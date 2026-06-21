//! # agavelens-infra
//!
//! Adapter implementations for the AgaveLens ports:
//! - [`MemorySampleRepository`] — a bounded in-memory [`SampleRepository`].
//! - [`SampleGenerator`] — deterministic synthetic validator telemetry.
//!
//! The production [`Clock`](agavelens_core::SystemClock) lives in the core crate
//! and is re-exported here for convenient wiring by the node.

#![forbid(unsafe_code)]

mod generator;
mod repo;

pub use agavelens_core::SystemClock;
pub use generator::SampleGenerator;
pub use repo::MemorySampleRepository;

#[cfg(test)]
mod tests {
    use super::*;
    use agavelens_core::{aggregate, SampleRepository};
    use chrono::Utc;

    #[tokio::test]
    async fn generator_through_repository_and_aggregate() {
        let gen = SampleGenerator::new(4, 432_000, 20);
        let repo = MemorySampleRepository::new(10_000);
        let samples = gen.generate(0, 4_000, Utc::now());
        repo.save_batch(&samples).await.unwrap();

        let stored = repo.all().await.unwrap();
        let snapshot = aggregate(&stored, Utc::now(), 1_024);
        assert_eq!(snapshot.total_samples, 4_000);
        assert_eq!(snapshot.validators_seen, 4);
        // worst validator sorts first
        let worst = snapshot.worst_validators(1);
        assert_eq!(worst.len(), 1);
        // some slots produced -> overall percentiles populated
        assert!(!snapshot.slot_time.is_empty());
    }
}
