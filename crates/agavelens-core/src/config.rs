//! Engine configuration with production-sane defaults.

/// Tunables for the analytics engine.
///
/// All values have defaults sized for a single validator's telemetry; override
/// via [`crate::AnalyticsEngine::new`] (the node crate wires these from the CLI).
#[derive(Debug, Clone)]
pub struct AnalyticsConfig {
    /// Slots per epoch, used to derive epochs from slots (mainnet: 432_000).
    pub slots_per_epoch: u64,

    /// Maximum samples retained in the store; oldest are evicted past this
    /// bound (memory safety — the store can never grow without limit).
    pub max_samples: usize,

    /// Maximum samples accepted in a single ingest batch.
    pub max_batch: usize,

    /// Token-bucket burst capacity for ingest (back-pressure).
    pub ingest_capacity: u32,

    /// Token-bucket refill rate per second for ingest.
    pub ingest_refill_per_sec: f64,

    /// Sample count at or above which aggregation runs in parallel (rayon).
    /// Below it, a serial pass is faster (no fork/join overhead). The default
    /// is the empirically measured crossover on a 1.5k-validator fleet — see
    /// `benches/aggregate_bench.rs` and the README performance section.
    pub parallel_threshold: usize,
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            slots_per_epoch: 432_000,
            max_samples: 200_000,
            max_batch: 10_000,
            ingest_capacity: 100_000,
            ingest_refill_per_sec: 100_000.0,
            parallel_threshold: 500_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let c = AnalyticsConfig::default();
        assert_eq!(c.slots_per_epoch, 432_000);
        assert!(c.max_batch <= c.max_samples);
        assert!(c.ingest_capacity > 0);
        assert!(c.parallel_threshold > 0);
    }
}
