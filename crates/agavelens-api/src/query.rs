//! Query root — read-only analytics over the ingested samples.

use async_graphql::{Context, Object, Result};

use agavelens_types::{Epoch, ValidatorId};

use crate::schema::{to_err, ApiContext};
use crate::types::{AnalyticsSnapshotObject, EpochSummaryObject, ValidatorReportObject};

/// GraphQL query root.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// The running API version (crate version).
    async fn api_version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    /// Number of samples currently retained in the store.
    async fn sample_count(&self, ctx: &Context<'_>) -> Result<u64> {
        let engine = &ctx.data_unchecked::<ApiContext>().engine;
        Ok(engine.sample_count().await.map_err(to_err)? as u64)
    }

    /// A full analytics snapshot computed over all retained samples.
    async fn snapshot(&self, ctx: &Context<'_>) -> Result<AnalyticsSnapshotObject> {
        let engine = &ctx.data_unchecked::<ApiContext>().engine;
        let snap = engine.snapshot().await.map_err(to_err)?;
        Ok(snap.into())
    }

    /// Overall skip rate in `[0.0, 1.0]`.
    async fn skip_rate(&self, ctx: &Context<'_>) -> Result<f64> {
        let engine = &ctx.data_unchecked::<ApiContext>().engine;
        let snap = engine.snapshot().await.map_err(to_err)?;
        Ok(snap.skip_rate())
    }

    /// The `limit` validators with the highest skip rate.
    async fn worst_validators(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 5)] limit: u32,
    ) -> Result<Vec<ValidatorReportObject>> {
        let engine = &ctx.data_unchecked::<ApiContext>().engine;
        let snap = engine.snapshot().await.map_err(to_err)?;
        Ok(snap
            .worst_validators(limit as usize)
            .iter()
            .cloned()
            .map(Into::into)
            .collect())
    }

    /// Analytics for a single validator, or `null` if it has no samples.
    async fn validator_report(
        &self,
        ctx: &Context<'_>,
        validator: String,
    ) -> Result<Option<ValidatorReportObject>> {
        let engine = &ctx.data_unchecked::<ApiContext>().engine;
        let id = ValidatorId::new(validator).map_err(to_err)?;
        let report = engine.validator_report(&id).await.map_err(to_err)?;
        Ok(report.map(Into::into))
    }

    /// Roll-up for a single epoch, or `null` if it has no samples.
    async fn epoch_summary(
        &self,
        ctx: &Context<'_>,
        epoch: u64,
    ) -> Result<Option<EpochSummaryObject>> {
        let engine = &ctx.data_unchecked::<ApiContext>().engine;
        let summary = engine.epoch_summary(Epoch(epoch)).await.map_err(to_err)?;
        Ok(summary.map(Into::into))
    }
}
