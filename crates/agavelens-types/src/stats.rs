//! Aggregate statistics: percentile distributions and skip counters.

use serde::{Deserialize, Serialize};

/// A nearest-rank percentile summary of a set of millisecond latencies.
///
/// Computation is pure and deterministic — [`Percentiles::from_sorted`] takes an
/// already-sorted slice so the (parallel) caller controls the sort.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Percentiles {
    /// Number of samples summarised.
    pub count: u64,
    /// Smallest observed value.
    pub min: u32,
    /// 50th percentile (median).
    pub p50: u32,
    /// 90th percentile.
    pub p90: u32,
    /// 99th percentile.
    pub p99: u32,
    /// Largest observed value.
    pub max: u32,
    /// Arithmetic mean.
    pub mean: f64,
}

impl Percentiles {
    /// An empty distribution (all zero).
    pub const fn empty() -> Self {
        Self {
            count: 0,
            min: 0,
            p50: 0,
            p90: 0,
            p99: 0,
            max: 0,
            mean: 0.0,
        }
    }

    /// Whether the distribution summarises no samples.
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Compute percentiles from an **ascending-sorted** slice.
    ///
    /// Uses the nearest-rank method: the value at rank `ceil(q * n)`. An empty
    /// input yields [`Percentiles::empty`].
    pub fn from_sorted(sorted: &[u32]) -> Self {
        let n = sorted.len();
        if n == 0 {
            return Self::empty();
        }
        let pick = |q: f64| -> u32 {
            let rank = (q * n as f64).ceil() as usize;
            let idx = rank.saturating_sub(1).min(n - 1);
            sorted[idx]
        };
        let sum: u64 = sorted.iter().map(|&v| u64::from(v)).sum();
        Self {
            count: n as u64,
            min: sorted[0],
            p50: pick(0.50),
            p90: pick(0.90),
            p99: pick(0.99),
            max: sorted[n - 1],
            mean: sum as f64 / n as f64,
        }
    }
}

/// A running count of observed vs skipped slots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkipStats {
    /// Total slots observed.
    pub observed: u64,
    /// Slots the leader skipped.
    pub skipped: u64,
}

impl SkipStats {
    /// Record one observation.
    pub fn record(&mut self, skipped: bool) {
        self.observed += 1;
        if skipped {
            self.skipped += 1;
        }
    }

    /// Slots that were produced (`observed - skipped`, saturating).
    pub const fn produced(&self) -> u64 {
        self.observed.saturating_sub(self.skipped)
    }

    /// Fraction of observed slots that were skipped, in `[0.0, 1.0]`.
    pub fn skip_rate(&self) -> f64 {
        if self.observed == 0 {
            0.0
        } else {
            self.skipped as f64 / self.observed as f64
        }
    }

    /// Merge another counter into this one.
    pub fn merge(&mut self, other: &Self) {
        self.observed += other.observed;
        self.skipped += other.skipped;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_percentiles() {
        let p = Percentiles::from_sorted(&[]);
        assert!(p.is_empty());
        assert_eq!(p, Percentiles::empty());
    }

    #[test]
    fn single_value() {
        let p = Percentiles::from_sorted(&[400]);
        assert_eq!(p.count, 1);
        assert_eq!(p.min, 400);
        assert_eq!(p.p50, 400);
        assert_eq!(p.p99, 400);
        assert_eq!(p.max, 400);
        assert!((p.mean - 400.0).abs() < f64::EPSILON);
    }

    #[test]
    fn nearest_rank_percentiles() {
        // 1..=100 sorted.
        let data: Vec<u32> = (1..=100).collect();
        let p = Percentiles::from_sorted(&data);
        assert_eq!(p.count, 100);
        assert_eq!(p.min, 1);
        assert_eq!(p.max, 100);
        // ceil(0.5*100)=50 -> index 49 -> value 50
        assert_eq!(p.p50, 50);
        // ceil(0.9*100)=90 -> value 90
        assert_eq!(p.p90, 90);
        // ceil(0.99*100)=99 -> value 99
        assert_eq!(p.p99, 99);
        assert!((p.mean - 50.5).abs() < 1e-9);
    }

    #[test]
    fn skip_stats_record_and_rate() {
        let mut s = SkipStats::default();
        for skipped in [false, false, true, false] {
            s.record(skipped);
        }
        assert_eq!(s.observed, 4);
        assert_eq!(s.skipped, 1);
        assert_eq!(s.produced(), 3);
        assert!((s.skip_rate() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn skip_rate_of_empty_is_zero() {
        assert_eq!(SkipStats::default().skip_rate(), 0.0);
    }

    #[test]
    fn skip_stats_merge() {
        let mut a = SkipStats {
            observed: 10,
            skipped: 2,
        };
        a.merge(&SkipStats {
            observed: 5,
            skipped: 3,
        });
        assert_eq!(a.observed, 15);
        assert_eq!(a.skipped, 5);
    }
}
