//! A single slot-level validator observation — the unit of ingest.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::InvalidSample;
use crate::ids::{Epoch, Slot, ValidatorId};

/// Upper bound on a sane slot production/confirmation time in milliseconds.
///
/// A healthy Solana slot is ~400ms; 60s is an absurdly large ceiling that still
/// rejects obviously corrupt input (negative values can't occur with `u32`).
pub const MAX_SLOT_TIME_MS: u32 = 60_000;

/// Upper bound on a sane vote latency in milliseconds.
pub const MAX_VOTE_LATENCY_MS: u32 = 60_000;

/// One observation of a slot: who led it, how long it took, how quickly our
/// vote landed, and whether the leader skipped it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotSample {
    slot: Slot,
    epoch: Epoch,
    leader: ValidatorId,
    slot_time_ms: u32,
    vote_latency_ms: u32,
    skipped: bool,
    observed_at: DateTime<Utc>,
}

impl SlotSample {
    /// Construct a validated sample.
    ///
    /// # Errors
    /// Returns [`InvalidSample::ValueOutOfRange`] if either latency exceeds its
    /// ceiling ([`MAX_SLOT_TIME_MS`] / [`MAX_VOTE_LATENCY_MS`]).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slot: Slot,
        epoch: Epoch,
        leader: ValidatorId,
        slot_time_ms: u32,
        vote_latency_ms: u32,
        skipped: bool,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, InvalidSample> {
        if slot_time_ms > MAX_SLOT_TIME_MS {
            return Err(InvalidSample::ValueOutOfRange {
                field: "slot_time_ms",
                value: slot_time_ms as u64,
                max: MAX_SLOT_TIME_MS as u64,
            });
        }
        if vote_latency_ms > MAX_VOTE_LATENCY_MS {
            return Err(InvalidSample::ValueOutOfRange {
                field: "vote_latency_ms",
                value: vote_latency_ms as u64,
                max: MAX_VOTE_LATENCY_MS as u64,
            });
        }
        Ok(Self {
            slot,
            epoch,
            leader,
            slot_time_ms,
            vote_latency_ms,
            skipped,
            observed_at,
        })
    }

    /// The observed slot.
    pub const fn slot(&self) -> Slot {
        self.slot
    }

    /// The epoch the slot belongs to.
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The validator that led the slot.
    pub fn leader(&self) -> &ValidatorId {
        &self.leader
    }

    /// Slot production/confirmation time in milliseconds.
    pub const fn slot_time_ms(&self) -> u32 {
        self.slot_time_ms
    }

    /// Vote landing latency in milliseconds.
    pub const fn vote_latency_ms(&self) -> u32 {
        self.vote_latency_ms
    }

    /// Whether the leader skipped (failed to produce) the slot.
    pub const fn is_skipped(&self) -> bool {
        self.skipped
    }

    /// Whether the slot was produced (the inverse of [`Self::is_skipped`]).
    pub const fn produced(&self) -> bool {
        !self.skipped
    }

    /// When the observation was recorded.
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leader() -> ValidatorId {
        ValidatorId::new("val-1").unwrap()
    }

    #[test]
    fn valid_sample_exposes_fields() {
        let now = Utc::now();
        let s = SlotSample::new(Slot(100), Epoch(0), leader(), 410, 95, false, now).unwrap();
        assert_eq!(s.slot(), Slot(100));
        assert_eq!(s.epoch(), Epoch(0));
        assert_eq!(s.leader().as_str(), "val-1");
        assert_eq!(s.slot_time_ms(), 410);
        assert_eq!(s.vote_latency_ms(), 95);
        assert!(!s.is_skipped());
        assert!(s.produced());
        assert_eq!(s.observed_at(), now);
    }

    #[test]
    fn skipped_sample_is_not_produced() {
        let s = SlotSample::new(Slot(7), Epoch(0), leader(), 0, 0, true, Utc::now()).unwrap();
        assert!(s.is_skipped());
        assert!(!s.produced());
    }

    #[test]
    fn rejects_excessive_slot_time() {
        let err = SlotSample::new(
            Slot(1),
            Epoch(0),
            leader(),
            MAX_SLOT_TIME_MS + 1,
            10,
            false,
            Utc::now(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            InvalidSample::ValueOutOfRange {
                field: "slot_time_ms",
                value: (MAX_SLOT_TIME_MS + 1) as u64,
                max: MAX_SLOT_TIME_MS as u64,
            }
        );
    }

    #[test]
    fn rejects_excessive_vote_latency() {
        let err = SlotSample::new(
            Slot(1),
            Epoch(0),
            leader(),
            400,
            MAX_VOTE_LATENCY_MS + 1,
            false,
            Utc::now(),
        )
        .unwrap_err();
        assert_eq!(err.code(), "value_out_of_range");
    }
}
