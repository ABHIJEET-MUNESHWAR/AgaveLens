//! Computed analytics outputs: per-validator, per-epoch, and overall snapshots.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{Epoch, ValidatorId};
use crate::stats::{Percentiles, SkipStats};

/// Analytics for a single validator across all observed slots it led.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidatorReport {
    /// The validator's identity.
    pub validator: ValidatorId,
    /// Slots this validator led.
    pub slots_led: u64,
    /// How many of those it skipped.
    pub slots_skipped: u64,
    /// Skip rate in `[0.0, 1.0]`.
    pub skip_rate: f64,
    /// Slot-time distribution (produced slots only).
    pub slot_time: Percentiles,
    /// Vote-latency distribution (produced slots only).
    pub vote_latency: Percentiles,
}

/// A roll-up of one epoch.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EpochSummary {
    /// The epoch summarised.
    pub epoch: Epoch,
    /// Samples observed in the epoch.
    pub samples: u64,
    /// Epoch-wide skip rate.
    pub skip_rate: f64,
    /// Median slot time.
    pub slot_time_p50: u32,
    /// 99th-percentile slot time.
    pub slot_time_p99: u32,
    /// Median vote latency.
    pub vote_latency_p50: u32,
    /// 99th-percentile vote latency.
    pub vote_latency_p99: u32,
}

/// A complete analytics snapshot over a set of samples.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsSnapshot {
    /// When the snapshot was computed.
    pub generated_at: DateTime<Utc>,
    /// Total samples aggregated.
    pub total_samples: u64,
    /// Distinct validators seen.
    pub validators_seen: u64,
    /// Overall skip counters.
    pub skip: SkipStats,
    /// Overall slot-time distribution.
    pub slot_time: Percentiles,
    /// Overall vote-latency distribution.
    pub vote_latency: Percentiles,
    /// Per-validator reports, sorted worst-skip-rate first.
    pub per_validator: Vec<ValidatorReport>,
}

impl AnalyticsSnapshot {
    /// An empty snapshot stamped at `now`.
    pub fn empty(now: DateTime<Utc>) -> Self {
        Self {
            generated_at: now,
            total_samples: 0,
            validators_seen: 0,
            skip: SkipStats::default(),
            slot_time: Percentiles::empty(),
            vote_latency: Percentiles::empty(),
            per_validator: Vec::new(),
        }
    }

    /// Overall skip rate in `[0.0, 1.0]`.
    pub fn skip_rate(&self) -> f64 {
        self.skip.skip_rate()
    }

    /// The `n` validators with the highest skip rate (already sorted).
    pub fn worst_validators(&self, n: usize) -> &[ValidatorReport] {
        let end = n.min(self.per_validator.len());
        &self.per_validator[..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(id: &str, skip_rate: f64) -> ValidatorReport {
        ValidatorReport {
            validator: ValidatorId::new(id).unwrap(),
            slots_led: 100,
            slots_skipped: (skip_rate * 100.0) as u64,
            skip_rate,
            slot_time: Percentiles::empty(),
            vote_latency: Percentiles::empty(),
        }
    }

    #[test]
    fn empty_snapshot() {
        let now = Utc::now();
        let s = AnalyticsSnapshot::empty(now);
        assert_eq!(s.total_samples, 0);
        assert_eq!(s.skip_rate(), 0.0);
        assert!(s.worst_validators(5).is_empty());
        assert_eq!(s.generated_at, now);
    }

    #[test]
    fn worst_validators_clamps_to_len() {
        let mut s = AnalyticsSnapshot::empty(Utc::now());
        s.per_validator = vec![report("a", 0.3), report("b", 0.2), report("c", 0.1)];
        assert_eq!(s.worst_validators(2).len(), 2);
        assert_eq!(s.worst_validators(10).len(), 3);
        assert_eq!(s.worst_validators(2)[0].validator.as_str(), "a");
    }

    #[test]
    fn skip_rate_delegates_to_counter() {
        let mut s = AnalyticsSnapshot::empty(Utc::now());
        s.skip = SkipStats {
            observed: 200,
            skipped: 10,
        };
        assert!((s.skip_rate() - 0.05).abs() < f64::EPSILON);
    }
}
