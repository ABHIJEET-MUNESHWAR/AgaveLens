//! Parallel batch aggregation — the CPU-bound heart of AgaveLens.
//!
//! [`aggregate`] turns a slice of [`SlotSample`]s into a full
//! [`AnalyticsSnapshot`]. Grouping samples into per-validator buckets is a cheap,
//! cache-friendly linear pass, so it runs serially. The expensive, genuinely
//! CPU-bound work is the percentile **sorting** — independent per-validator sorts
//! plus the two fleet-wide sorts — which `rayon` distributes across the
//! work-stealing pool for inputs at or beyond a threshold. The function is pure
//! and deterministic (results are sorted by a total order), which keeps it
//! trivially testable and safe to run inside `spawn_blocking`.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rayon::prelude::*;

use agavelens_types::{
    AnalyticsSnapshot, Epoch, EpochSummary, Percentiles, SkipStats, SlotSample, ValidatorId,
    ValidatorReport,
};

/// Per-validator accumulator built during the grouping pass.
#[derive(Default)]
struct Bucket {
    skip: SkipStats,
    slot_times: Vec<u32>,
    vote_latencies: Vec<u32>,
}

impl Bucket {
    fn push(&mut self, sample: &SlotSample) {
        self.skip.record(sample.is_skipped());
        if sample.produced() {
            self.slot_times.push(sample.slot_time_ms());
            self.vote_latencies.push(sample.vote_latency_ms());
        }
    }

    fn into_report(mut self, validator: ValidatorId) -> ValidatorReport {
        self.slot_times.sort_unstable();
        self.vote_latencies.sort_unstable();
        ValidatorReport {
            validator,
            slots_led: self.skip.observed,
            slots_skipped: self.skip.skipped,
            skip_rate: self.skip.skip_rate(),
            slot_time: Percentiles::from_sorted(&self.slot_times),
            vote_latency: Percentiles::from_sorted(&self.vote_latencies),
        }
    }
}

type Buckets = HashMap<ValidatorId, Bucket>;

/// Group samples into per-validator buckets in a single linear pass.
///
/// Grouping is memory-bound (hashing string keys, pushing into vectors); a
/// parallel map-reduce here only adds partial-map merge overhead, so it stays
/// serial. The parallelism in [`aggregate`] is applied to the sort-heavy
/// percentile phase instead, where it actually pays off.
fn group(samples: &[SlotSample]) -> Buckets {
    let mut acc = Buckets::with_capacity(samples.len() / 64 + 1);
    for s in samples {
        acc.entry(s.leader().clone()).or_default().push(s);
    }
    acc
}

/// Aggregate a batch of samples into a full snapshot.
///
/// `parallel_threshold` selects the parallel path when `samples.len()` reaches
/// it; smaller inputs run serially to avoid thread-pool overhead.
pub fn aggregate(
    samples: &[SlotSample],
    now: DateTime<Utc>,
    parallel_threshold: usize,
) -> AnalyticsSnapshot {
    if samples.is_empty() {
        return AnalyticsSnapshot::empty(now);
    }

    let parallel = samples.len() >= parallel_threshold;
    let buckets = group(samples);

    // Overall skip counters fold across buckets before they are consumed.
    let mut overall_skip = SkipStats::default();
    for bucket in buckets.values() {
        overall_skip.merge(&bucket.skip);
    }
    let validators_seen = buckets.len() as u64;

    // Per-validator reports: each bucket sorts its own vectors independently, so
    // mapping in parallel is a clean win with no shared state or merge cost.
    let mut per_validator: Vec<ValidatorReport> = if parallel {
        buckets
            .into_par_iter()
            .map(|(id, bucket)| bucket.into_report(id))
            .collect()
    } else {
        buckets
            .into_iter()
            .map(|(id, bucket)| bucket.into_report(id))
            .collect()
    };
    // Worst skip-rate first; deterministic tie-breaks for stable output.
    per_validator.sort_by(|a, b| {
        b.skip_rate
            .partial_cmp(&a.skip_rate)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.slots_led.cmp(&a.slots_led))
            .then_with(|| a.validator.as_str().cmp(b.validator.as_str()))
    });

    // Overall percentiles over produced slots: a serial collect (memory-bound)
    // then a parallel sort (CPU-bound) of the two fleet-wide vectors.
    let (mut slot_times, mut vote_latencies) = collect_produced(samples);
    if parallel {
        slot_times.par_sort_unstable();
        vote_latencies.par_sort_unstable();
    } else {
        slot_times.sort_unstable();
        vote_latencies.sort_unstable();
    }

    AnalyticsSnapshot {
        generated_at: now,
        total_samples: samples.len() as u64,
        validators_seen,
        skip: overall_skip,
        slot_time: Percentiles::from_sorted(&slot_times),
        vote_latency: Percentiles::from_sorted(&vote_latencies),
        per_validator,
    }
}

fn collect_produced(samples: &[SlotSample]) -> (Vec<u32>, Vec<u32>) {
    let produced = samples.iter().filter(|s| s.produced());
    let mut slot_times = Vec::with_capacity(samples.len());
    let mut vote_latencies = Vec::with_capacity(samples.len());
    for s in produced {
        slot_times.push(s.slot_time_ms());
        vote_latencies.push(s.vote_latency_ms());
    }
    (slot_times, vote_latencies)
}

/// Build a single validator's report from its (already-filtered) samples.
///
/// Returns `None` if no samples are supplied.
pub fn report_for(samples: &[SlotSample], validator: ValidatorId) -> Option<ValidatorReport> {
    if samples.is_empty() {
        return None;
    }
    let mut bucket = Bucket::default();
    for s in samples {
        bucket.push(s);
    }
    Some(bucket.into_report(validator))
}

/// Summarise a single epoch from its (already-filtered) samples.
///
/// Returns `None` if no samples are supplied.
pub fn summarize_epoch(samples: &[SlotSample], epoch: Epoch) -> Option<EpochSummary> {
    if samples.is_empty() {
        return None;
    }
    let mut skip = SkipStats::default();
    let mut slot_times = Vec::new();
    let mut vote_latencies = Vec::new();
    for s in samples {
        skip.record(s.is_skipped());
        if s.produced() {
            slot_times.push(s.slot_time_ms());
            vote_latencies.push(s.vote_latency_ms());
        }
    }
    slot_times.sort_unstable();
    vote_latencies.sort_unstable();
    let st = Percentiles::from_sorted(&slot_times);
    let vl = Percentiles::from_sorted(&vote_latencies);
    Some(EpochSummary {
        epoch,
        samples: samples.len() as u64,
        skip_rate: skip.skip_rate(),
        slot_time_p50: st.p50,
        slot_time_p99: st.p99,
        vote_latency_p50: vl.p50,
        vote_latency_p99: vl.p99,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(slot: u64, leader: &str, slot_ms: u32, vote_ms: u32, skipped: bool) -> SlotSample {
        SlotSample::new(
            slot.into(),
            Epoch(0),
            ValidatorId::new(leader).unwrap(),
            slot_ms,
            vote_ms,
            skipped,
            Utc::now(),
        )
        .unwrap()
    }

    #[test]
    fn empty_input_yields_empty_snapshot() {
        let snap = aggregate(&[], Utc::now(), 1);
        assert_eq!(snap.total_samples, 0);
        assert!(snap.per_validator.is_empty());
    }

    #[test]
    fn aggregates_two_validators() {
        let samples = vec![
            sample(1, "alice", 400, 90, false),
            sample(2, "alice", 420, 110, false),
            sample(3, "bob", 800, 300, true),
            sample(4, "bob", 410, 100, false),
        ];
        let snap = aggregate(&samples, Utc::now(), 1);
        assert_eq!(snap.total_samples, 4);
        assert_eq!(snap.validators_seen, 2);
        assert_eq!(snap.skip.observed, 4);
        assert_eq!(snap.skip.skipped, 1);
        // bob has the higher skip rate -> sorts first
        assert_eq!(snap.per_validator[0].validator.as_str(), "bob");
        assert!((snap.per_validator[0].skip_rate - 0.5).abs() < f64::EPSILON);
        // overall produced slot times: 400,420,410 (bob's skipped slot excluded)
        assert_eq!(snap.slot_time.count, 3);
        assert_eq!(snap.slot_time.min, 400);
        assert_eq!(snap.slot_time.max, 420);
    }

    #[test]
    fn serial_and_parallel_paths_agree() {
        let samples: Vec<SlotSample> = (0..5_000)
            .map(|i| {
                let leader = if i % 3 == 0 { "alice" } else { "bob" };
                sample(
                    i,
                    leader,
                    400 + (i % 50) as u32,
                    80 + (i % 40) as u32,
                    i % 11 == 0,
                )
            })
            .collect();
        let serial = aggregate(&samples, Utc::now(), usize::MAX);
        let parallel = aggregate(&samples, Utc::now(), 1);
        assert_eq!(serial.total_samples, parallel.total_samples);
        assert_eq!(serial.validators_seen, parallel.validators_seen);
        assert_eq!(serial.skip, parallel.skip);
        assert_eq!(serial.slot_time, parallel.slot_time);
        assert_eq!(serial.vote_latency, parallel.vote_latency);
        assert_eq!(serial.per_validator, parallel.per_validator);
    }

    #[test]
    fn report_for_filters_and_summarizes() {
        let samples = vec![
            sample(1, "alice", 400, 90, false),
            sample(2, "alice", 600, 90, true),
        ];
        let r = report_for(&samples, ValidatorId::new("alice").unwrap()).unwrap();
        assert_eq!(r.slots_led, 2);
        assert_eq!(r.slots_skipped, 1);
        // only the produced slot contributes to the distribution
        assert_eq!(r.slot_time.count, 1);
        assert_eq!(r.slot_time.p50, 400);
    }

    #[test]
    fn report_for_empty_is_none() {
        assert!(report_for(&[], ValidatorId::new("x").unwrap()).is_none());
    }

    #[test]
    fn epoch_summary_computes_percentiles() {
        let samples: Vec<SlotSample> = (0..10)
            .map(|i| sample(i, "alice", 400 + i as u32, 100, false))
            .collect();
        let s = summarize_epoch(&samples, Epoch(0)).unwrap();
        assert_eq!(s.epoch, Epoch(0));
        assert_eq!(s.samples, 10);
        assert_eq!(s.vote_latency_p50, 100);
        assert!(s.slot_time_p99 >= s.slot_time_p50);
    }

    #[test]
    fn epoch_summary_empty_is_none() {
        assert!(summarize_epoch(&[], Epoch(3)).is_none());
    }
}
