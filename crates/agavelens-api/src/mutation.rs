//! Mutation root — batch ingest of slot samples.

use async_graphql::{Context, Object, Result};
use chrono::Utc;

use agavelens_types::{Epoch, Slot, SlotSample, ValidatorId};

use crate::schema::{to_err, ApiContext};
use crate::types::{IngestSummaryObject, SlotSampleInput};

/// GraphQL mutation root.
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Ingest a batch of slot samples.
    ///
    /// Each input is validated into a domain `SlotSample`; the whole batch is
    /// then handed to the engine, which applies the batch-size guard, ingest
    /// rate limiter, and bounded store.
    async fn ingest_samples(
        &self,
        ctx: &Context<'_>,
        samples: Vec<SlotSampleInput>,
    ) -> Result<IngestSummaryObject> {
        let engine = &ctx.data_unchecked::<ApiContext>().engine;
        let slots_per_epoch = engine.config().slots_per_epoch;
        let now = Utc::now();

        let mut parsed = Vec::with_capacity(samples.len());
        for input in samples {
            let leader = ValidatorId::new(input.leader).map_err(to_err)?;
            let slot = Slot(input.slot);
            let epoch = input
                .epoch
                .map(Epoch)
                .unwrap_or_else(|| slot.epoch(slots_per_epoch));
            let sample = SlotSample::new(
                slot,
                epoch,
                leader,
                input.slot_time_ms,
                input.vote_latency_ms,
                input.skipped,
                now,
            )
            .map_err(to_err)?;
            parsed.push(sample);
        }

        let summary = engine.ingest_batch(parsed).await.map_err(to_err)?;
        Ok(summary.into())
    }
}
