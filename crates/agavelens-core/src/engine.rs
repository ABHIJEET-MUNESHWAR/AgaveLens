//! The analytics engine — composition of ports, guards, and the aggregator.

use std::sync::Arc;

use agavelens_types::{AnalyticsSnapshot, Epoch, EpochSummary, SlotSample, ValidatorId, ValidatorReport};

use crate::aggregate::{aggregate, report_for, summarize_epoch};
use crate::config::AnalyticsConfig;
use crate::error::{CoreError, PortError};
use crate::guard::{Clock, RateLimiter, SystemClock};
use crate::ports::SampleRepository;

/// Outcome of an ingest call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestSummary {
    /// Samples accepted in this batch.
    pub accepted: usize,
    /// Total samples retained in the store afterwards (post-bounding).
    pub total_stored: usize,
}

/// Dependencies injected into the engine (the ports it drives).
pub struct EngineDeps {
    /// Sample persistence.
    pub repo: Arc<dyn SampleRepository>,
    /// Time source for snapshot stamps.
    pub clock: Arc<dyn Clock>,
}

/// Ingests validator samples and computes analytics over them.
///
/// CPU-bound aggregation runs inside [`tokio::task::spawn_blocking`] so the
/// async runtime is never blocked, while `rayon` parallelises the work within.
pub struct AnalyticsEngine {
    repo: Arc<dyn SampleRepository>,
    clock: Arc<dyn Clock>,
    limiter: RateLimiter<SystemClock>,
    config: AnalyticsConfig,
}

impl AnalyticsEngine {
    /// Wire the engine from its dependencies and configuration.
    pub fn new(deps: EngineDeps, config: AnalyticsConfig) -> Self {
        let limiter = RateLimiter::new(
            config.ingest_capacity,
            config.ingest_refill_per_sec,
            SystemClock,
        );
        Self {
            repo: deps.repo,
            clock: deps.clock,
            limiter,
            config,
        }
    }

    /// Read-only view of the active configuration.
    pub fn config(&self) -> &AnalyticsConfig {
        &self.config
    }

    /// Ingest a batch of validated samples.
    ///
    /// # Errors
    /// - [`CoreError::BatchTooLarge`] if the batch exceeds `max_batch`.
    /// - [`CoreError::Throttled`] if the rate limiter sheds the request.
    /// - [`CoreError::Port`] if persistence fails.
    pub async fn ingest_batch(&self, samples: Vec<SlotSample>) -> Result<IngestSummary, CoreError> {
        if samples.len() > self.config.max_batch {
            metrics::counter!("agavelens_batches_rejected_total", "reason" => "too_large")
                .increment(1);
            return Err(CoreError::BatchTooLarge {
                size: samples.len(),
                max: self.config.max_batch,
            });
        }
        if !self.limiter.try_acquire() {
            metrics::counter!("agavelens_batches_rejected_total", "reason" => "throttled")
                .increment(1);
            return Err(CoreError::Throttled);
        }
        let accepted = samples.len();
        let total_stored = self.repo.save_batch(&samples).await?;
        metrics::counter!("agavelens_samples_ingested_total").increment(accepted as u64);
        Ok(IngestSummary {
            accepted,
            total_stored,
        })
    }

    /// Compute a full analytics snapshot over all retained samples.
    pub async fn snapshot(&self) -> Result<AnalyticsSnapshot, CoreError> {
        let samples = self.repo.all().await?;
        let now = self.clock.now();
        let threshold = self.config.parallel_threshold;
        let snapshot = tokio::task::spawn_blocking(move || aggregate(&samples, now, threshold))
            .await
            .map_err(|e| PortError::Internal(format!("aggregate task panicked: {e}")))?;
        metrics::counter!("agavelens_snapshots_total").increment(1);
        Ok(snapshot)
    }

    /// Analytics for a single validator, or `None` if it has no samples.
    pub async fn validator_report(
        &self,
        validator: &ValidatorId,
    ) -> Result<Option<ValidatorReport>, CoreError> {
        let samples = self.repo.by_validator(validator).await?;
        Ok(report_for(&samples, validator.clone()))
    }

    /// Roll-up for a single epoch, or `None` if it has no samples.
    pub async fn epoch_summary(&self, epoch: Epoch) -> Result<Option<EpochSummary>, CoreError> {
        let samples = self.repo.by_epoch(epoch).await?;
        Ok(summarize_epoch(&samples, epoch))
    }

    /// Number of retained samples.
    pub async fn sample_count(&self) -> Result<usize, CoreError> {
        Ok(self.repo.count().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::guard::ManualClock;
    use async_trait::async_trait;
    use chrono::Utc;
    use parking_lot::Mutex;

    /// A minimal bounded in-memory repository for engine tests.
    struct FakeRepo {
        samples: Mutex<Vec<SlotSample>>,
        cap: usize,
    }

    impl FakeRepo {
        fn new(cap: usize) -> Self {
            Self {
                samples: Mutex::new(Vec::new()),
                cap,
            }
        }
    }

    #[async_trait]
    impl SampleRepository for FakeRepo {
        async fn save_batch(&self, batch: &[SlotSample]) -> Result<usize, PortError> {
            let mut g = self.samples.lock();
            g.extend_from_slice(batch);
            if g.len() > self.cap {
                let drop = g.len() - self.cap;
                g.drain(0..drop);
            }
            Ok(g.len())
        }
        async fn all(&self) -> Result<Vec<SlotSample>, PortError> {
            Ok(self.samples.lock().clone())
        }
        async fn by_validator(&self, id: &ValidatorId) -> Result<Vec<SlotSample>, PortError> {
            Ok(self
                .samples
                .lock()
                .iter()
                .filter(|s| s.leader() == id)
                .cloned()
                .collect())
        }
        async fn by_epoch(&self, epoch: Epoch) -> Result<Vec<SlotSample>, PortError> {
            Ok(self
                .samples
                .lock()
                .iter()
                .filter(|s| s.epoch() == epoch)
                .cloned()
                .collect())
        }
        async fn count(&self) -> Result<usize, PortError> {
            Ok(self.samples.lock().len())
        }
    }

    fn engine_with(repo: Arc<dyn SampleRepository>, config: AnalyticsConfig) -> AnalyticsEngine {
        let clock = Arc::new(ManualClock::new(Utc::now()));
        AnalyticsEngine::new(EngineDeps { repo, clock }, config)
    }

    fn sample(slot: u64, leader: &str, skipped: bool) -> SlotSample {
        SlotSample::new(
            slot.into(),
            Epoch(0),
            ValidatorId::new(leader).unwrap(),
            400,
            100,
            skipped,
            Utc::now(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn ingest_then_snapshot() {
        let repo = Arc::new(FakeRepo::new(1000));
        let engine = engine_with(repo, AnalyticsConfig::default());
        let summary = engine
            .ingest_batch(vec![
                sample(1, "alice", false),
                sample(2, "bob", true),
                sample(3, "alice", false),
            ])
            .await
            .unwrap();
        assert_eq!(summary.accepted, 3);
        assert_eq!(summary.total_stored, 3);

        let snap = engine.snapshot().await.unwrap();
        assert_eq!(snap.total_samples, 3);
        assert_eq!(snap.validators_seen, 2);
        assert_eq!(snap.skip.skipped, 1);
    }

    #[tokio::test]
    async fn batch_too_large_is_rejected() {
        let repo = Arc::new(FakeRepo::new(1000));
        let config = AnalyticsConfig {
            max_batch: 2,
            ..AnalyticsConfig::default()
        };
        let engine = engine_with(repo, config);
        let err = engine
            .ingest_batch(vec![
                sample(1, "a", false),
                sample(2, "b", false),
                sample(3, "c", false),
            ])
            .await
            .unwrap_err();
        assert!(matches!(err, CoreError::BatchTooLarge { size: 3, max: 2 }));
    }

    #[tokio::test]
    async fn ingest_is_throttled_when_bucket_empty() {
        let repo = Arc::new(FakeRepo::new(1000));
        // capacity 1, no refill -> second ingest is shed
        let config = AnalyticsConfig {
            ingest_capacity: 1,
            ingest_refill_per_sec: 0.0,
            ..AnalyticsConfig::default()
        };
        let engine = engine_with(repo, config);
        assert!(engine.ingest_batch(vec![sample(1, "a", false)]).await.is_ok());
        let err = engine
            .ingest_batch(vec![sample(2, "b", false)])
            .await
            .unwrap_err();
        assert_eq!(err.code(), "throttled");
    }

    #[tokio::test]
    async fn bounded_store_evicts_oldest() {
        let repo = Arc::new(FakeRepo::new(2));
        let engine = engine_with(repo, AnalyticsConfig::default());
        engine
            .ingest_batch(vec![sample(1, "a", false), sample(2, "b", false), sample(3, "c", false)])
            .await
            .unwrap();
        assert_eq!(engine.sample_count().await.unwrap(), 2);
        let snap = engine.snapshot().await.unwrap();
        // oldest (slot 1, "a") evicted
        assert!(snap.per_validator.iter().all(|r| r.validator.as_str() != "a"));
    }

    #[tokio::test]
    async fn validator_and_epoch_lookups() {
        let repo = Arc::new(FakeRepo::new(1000));
        let engine = engine_with(repo, AnalyticsConfig::default());
        engine
            .ingest_batch(vec![sample(1, "alice", false), sample(2, "alice", true)])
            .await
            .unwrap();
        let report = engine
            .validator_report(&ValidatorId::new("alice").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.slots_led, 2);
        assert!(engine
            .validator_report(&ValidatorId::new("ghost").unwrap())
            .await
            .unwrap()
            .is_none());
        let epoch = engine.epoch_summary(Epoch(0)).await.unwrap().unwrap();
        assert_eq!(epoch.samples, 2);
        assert!(engine.epoch_summary(Epoch(9)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn repository_failure_propagates() {
        use crate::ports::MockSampleRepository;
        let mut mock = MockSampleRepository::new();
        mock.expect_all()
            .returning(|| Err(PortError::Unavailable("store down".into())));
        let engine = engine_with(Arc::new(mock), AnalyticsConfig::default());
        let err = engine.snapshot().await.unwrap_err();
        assert_eq!(err.code(), "unavailable");
    }
}
