//! Deterministic synthetic validator-telemetry generator.
//!
//! Produces reproducible [`SlotSample`]s without any RNG dependency — every
//! value is derived from a hash of the slot number, so the same parameters
//! always yield identical output (ideal for demos, benchmarks, and tests).
//! Validators are assigned differing skip rates and latency baselines so the
//! analytics produce a meaningful "worst validators" ranking.

use chrono::{DateTime, Utc};

use agavelens_types::{Epoch, Slot, SlotSample, ValidatorId};

/// MurmurHash3 64-bit finalizer — a cheap, well-distributed mixing function.
fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^= x >> 33;
    x
}

/// Generates synthetic slot samples for a fixed validator set.
pub struct SampleGenerator {
    validators: Vec<ValidatorId>,
    slots_per_epoch: u64,
    base_skip_pct: u8,
}

impl SampleGenerator {
    /// Build a generator with `num_validators` synthetic identities.
    ///
    /// `base_skip_pct` (clamped to 0..=100) is the baseline skip probability;
    /// individual validators deviate from it deterministically.
    pub fn new(num_validators: usize, slots_per_epoch: u64, base_skip_pct: u8) -> Self {
        let n = num_validators.max(1);
        let validators = (0..n)
            .map(|i| {
                ValidatorId::new(format!("val-{i:04}"))
                    .expect("synthetic validator id is within bounds")
            })
            .collect();
        Self {
            validators,
            slots_per_epoch: slots_per_epoch.max(1),
            base_skip_pct: base_skip_pct.min(100),
        }
    }

    /// The validator identities this generator produces.
    pub fn validators(&self) -> &[ValidatorId] {
        &self.validators
    }

    /// Generate `count` consecutive samples starting at `start_slot`.
    pub fn generate(
        &self,
        start_slot: u64,
        count: u64,
        observed_at: DateTime<Utc>,
    ) -> Vec<SlotSample> {
        let n = self.validators.len() as u64;
        let mut out = Vec::with_capacity(count as usize);
        for i in 0..count {
            let slot = start_slot + i;
            let leader_idx = (mix(slot) % n) as usize;
            let leader = self.validators[leader_idx].clone();

            // Per-validator skip bias: some validators are flakier than others.
            let bias = (leader_idx as u64 * 7) % 25;
            let threshold = (u64::from(self.base_skip_pct) + bias).min(100);
            let skipped = (mix(slot ^ 0xABCD) % 100) < threshold;

            let (slot_time_ms, vote_latency_ms) = if skipped {
                (0, 0)
            } else {
                // Per-validator latency baseline + deterministic jitter.
                let base_slot = 360 + (leader_idx as u32 % 7) * 18;
                let slot_jitter = (mix(slot ^ 0x1234) % 120) as u32;
                let base_vote = 60 + (leader_idx as u32 % 5) * 14;
                let vote_jitter = (mix(slot ^ 0x5678) % 80) as u32;
                (base_slot + slot_jitter, base_vote + vote_jitter)
            };

            let epoch: Epoch = Slot(slot).epoch(self.slots_per_epoch);
            out.push(
                SlotSample::new(
                    Slot(slot),
                    epoch,
                    leader,
                    slot_time_ms,
                    vote_latency_ms,
                    skipped,
                    observed_at,
                )
                .expect("synthetic sample values are within domain bounds"),
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_output() {
        let gen = SampleGenerator::new(4, 432_000, 10);
        let now = Utc::now();
        let a = gen.generate(0, 500, now);
        let b = gen.generate(0, 500, now);
        assert_eq!(a, b);
        assert_eq!(a.len(), 500);
    }

    #[test]
    fn produces_all_validators() {
        let gen = SampleGenerator::new(5, 432_000, 5);
        assert_eq!(gen.validators().len(), 5);
        let samples = gen.generate(0, 2_000, Utc::now());
        let mut seen = std::collections::HashSet::new();
        for s in &samples {
            seen.insert(s.leader().as_str().to_string());
        }
        assert_eq!(seen.len(), 5);
    }

    #[test]
    fn skip_rate_roughly_tracks_parameter() {
        let gen = SampleGenerator::new(3, 432_000, 50);
        let samples = gen.generate(0, 5_000, Utc::now());
        let skipped = samples.iter().filter(|s| s.is_skipped()).count();
        let rate = skipped as f64 / samples.len() as f64;
        // baseline 50% plus per-validator bias -> comfortably within this band
        assert!(rate > 0.4 && rate < 0.8, "rate was {rate}");
    }

    #[test]
    fn produced_samples_have_sane_latencies() {
        let gen = SampleGenerator::new(4, 432_000, 0);
        let samples = gen.generate(0, 1_000, Utc::now());
        for s in samples.iter().filter(|s| s.produced()) {
            assert!(s.slot_time_ms() >= 360);
            assert!(s.slot_time_ms() < 1_000);
            assert!(s.vote_latency_ms() >= 60);
        }
    }

    #[test]
    fn epochs_advance_with_slots() {
        let gen = SampleGenerator::new(2, 100, 0);
        let samples = gen.generate(0, 250, Utc::now());
        assert_eq!(samples[0].epoch(), Epoch(0));
        assert_eq!(samples[150].epoch(), Epoch(1));
        assert_eq!(samples[200].epoch(), Epoch(2));
    }
}
