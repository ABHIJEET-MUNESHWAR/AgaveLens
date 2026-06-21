//! GraphQL anti-corruption types: inputs and output objects with `From`
//! conversions from the domain. Keeps the wire schema decoupled from core types.

use async_graphql::{InputObject, SimpleObject};
use chrono::{DateTime, Utc};

use agavelens_core::IngestSummary;
use agavelens_types::{
    AnalyticsSnapshot, EpochSummary, Percentiles, ValidatorReport,
};

/// One slot observation submitted for ingest.
#[derive(Debug, Clone, InputObject)]
pub struct SlotSampleInput {
    /// Slot number observed.
    pub slot: u64,
    /// Epoch the slot belongs to. If omitted, it is derived from the slot using
    /// the engine's configured epoch length.
    pub epoch: Option<u64>,
    /// Leader validator identity (e.g. base58 pubkey).
    pub leader: String,
    /// Slot production/confirmation time in milliseconds.
    pub slot_time_ms: u32,
    /// Vote landing latency in milliseconds.
    pub vote_latency_ms: u32,
    /// Whether the leader skipped the slot.
    #[graphql(default)]
    pub skipped: bool,
}

/// A nearest-rank percentile distribution.
#[derive(Debug, Clone, SimpleObject)]
pub struct PercentilesObject {
    /// Samples summarised.
    pub count: u64,
    /// Minimum value.
    pub min: u32,
    /// 50th percentile.
    pub p50: u32,
    /// 90th percentile.
    pub p90: u32,
    /// 99th percentile.
    pub p99: u32,
    /// Maximum value.
    pub max: u32,
    /// Arithmetic mean.
    pub mean: f64,
}

impl From<Percentiles> for PercentilesObject {
    fn from(p: Percentiles) -> Self {
        Self {
            count: p.count,
            min: p.min,
            p50: p.p50,
            p90: p.p90,
            p99: p.p99,
            max: p.max,
            mean: p.mean,
        }
    }
}

/// Analytics for a single validator.
#[derive(Debug, Clone, SimpleObject)]
pub struct ValidatorReportObject {
    /// Validator identity.
    pub validator: String,
    /// Slots led.
    pub slots_led: u64,
    /// Slots skipped.
    pub slots_skipped: u64,
    /// Skip rate in `[0.0, 1.0]`.
    pub skip_rate: f64,
    /// Slot-time distribution (produced slots).
    pub slot_time: PercentilesObject,
    /// Vote-latency distribution (produced slots).
    pub vote_latency: PercentilesObject,
}

impl From<ValidatorReport> for ValidatorReportObject {
    fn from(r: ValidatorReport) -> Self {
        Self {
            validator: r.validator.into_inner(),
            slots_led: r.slots_led,
            slots_skipped: r.slots_skipped,
            skip_rate: r.skip_rate,
            slot_time: r.slot_time.into(),
            vote_latency: r.vote_latency.into(),
        }
    }
}

/// A per-epoch roll-up.
#[derive(Debug, Clone, Copy, SimpleObject)]
pub struct EpochSummaryObject {
    /// Epoch number.
    pub epoch: u64,
    /// Samples observed.
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

impl From<EpochSummary> for EpochSummaryObject {
    fn from(e: EpochSummary) -> Self {
        Self {
            epoch: e.epoch.value(),
            samples: e.samples,
            skip_rate: e.skip_rate,
            slot_time_p50: e.slot_time_p50,
            slot_time_p99: e.slot_time_p99,
            vote_latency_p50: e.vote_latency_p50,
            vote_latency_p99: e.vote_latency_p99,
        }
    }
}

/// A complete analytics snapshot.
#[derive(Debug, Clone, SimpleObject)]
pub struct AnalyticsSnapshotObject {
    /// When the snapshot was computed.
    pub generated_at: DateTime<Utc>,
    /// Total samples aggregated.
    pub total_samples: u64,
    /// Distinct validators seen.
    pub validators_seen: u64,
    /// Slots observed overall.
    pub observed: u64,
    /// Slots skipped overall.
    pub skipped: u64,
    /// Overall skip rate.
    pub skip_rate: f64,
    /// Overall slot-time distribution.
    pub slot_time: PercentilesObject,
    /// Overall vote-latency distribution.
    pub vote_latency: PercentilesObject,
    /// Per-validator reports, worst skip-rate first.
    pub per_validator: Vec<ValidatorReportObject>,
}

impl From<AnalyticsSnapshot> for AnalyticsSnapshotObject {
    fn from(s: AnalyticsSnapshot) -> Self {
        Self {
            generated_at: s.generated_at,
            total_samples: s.total_samples,
            validators_seen: s.validators_seen,
            observed: s.skip.observed,
            skipped: s.skip.skipped,
            skip_rate: s.skip.skip_rate(),
            slot_time: s.slot_time.into(),
            vote_latency: s.vote_latency.into(),
            per_validator: s.per_validator.into_iter().map(Into::into).collect(),
        }
    }
}

/// Result of an ingest mutation.
#[derive(Debug, Clone, Copy, SimpleObject)]
pub struct IngestSummaryObject {
    /// Samples accepted in this batch.
    pub accepted: u64,
    /// Total samples retained afterwards.
    pub total_stored: u64,
}

impl From<IngestSummary> for IngestSummaryObject {
    fn from(s: IngestSummary) -> Self {
        Self {
            accepted: s.accepted as u64,
            total_stored: s.total_stored as u64,
        }
    }
}
