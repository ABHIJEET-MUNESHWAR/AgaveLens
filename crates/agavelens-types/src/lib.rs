//! # agavelens-types
//!
//! Pure domain types for AgaveLens validator analytics. No I/O, no async — just
//! validated identifiers, slot samples, and the statistics they aggregate into.
//!
//! Layering: every other crate depends on this one; this crate depends on nothing
//! in the workspace (hexagonal core).

#![forbid(unsafe_code)]

mod error;
mod ids;
mod report;
mod sample;
mod stats;

pub use error::InvalidSample;
pub use ids::{Epoch, Slot, ValidatorId, MAX_VALIDATOR_ID_LEN};
pub use report::{AnalyticsSnapshot, EpochSummary, ValidatorReport};
pub use sample::{SlotSample, MAX_SLOT_TIME_MS, MAX_VOTE_LATENCY_MS};
pub use stats::{Percentiles, SkipStats};
