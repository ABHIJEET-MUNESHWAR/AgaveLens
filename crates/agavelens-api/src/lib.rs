//! # agavelens-api
//!
//! The GraphQL surface for AgaveLens. Read-mostly by design: a set of analytics
//! queries plus a single batch-ingest mutation, and **no subscriptions** — this
//! service answers analytical questions over accumulated samples rather than
//! streaming live events.

#![forbid(unsafe_code)]

mod mutation;
mod query;
mod schema;
mod types;

pub use mutation::MutationRoot;
pub use query::QueryRoot;
pub use schema::{build_schema, AgaveLensSchema, ApiContext};
pub use types::{
    AnalyticsSnapshotObject, EpochSummaryObject, IngestSummaryObject, PercentilesObject,
    SlotSampleInput, ValidatorReportObject,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use agavelens_core::{AnalyticsConfig, AnalyticsEngine, EngineDeps, SystemClock};
    use agavelens_infra::MemorySampleRepository;

    fn test_schema() -> AgaveLensSchema {
        let repo = Arc::new(MemorySampleRepository::new(10_000));
        let clock = Arc::new(SystemClock);
        let engine = Arc::new(AnalyticsEngine::new(
            EngineDeps { repo, clock },
            AnalyticsConfig::default(),
        ));
        build_schema(ApiContext::new(engine))
    }

    async fn ingest_three(schema: &AgaveLensSchema) {
        let m = r#"mutation {
            ingestSamples(samples: [
                { slot: 1, leader: "alice", slotTimeMs: 400, voteLatencyMs: 90, skipped: false },
                { slot: 2, leader: "alice", slotTimeMs: 420, voteLatencyMs: 110, skipped: false },
                { slot: 3, leader: "bob", slotTimeMs: 0, voteLatencyMs: 0, skipped: true }
            ]) { accepted totalStored }
        }"#;
        let resp = schema.execute(m).await;
        assert!(resp.errors.is_empty(), "ingest errors: {:?}", resp.errors);
        let json = resp.data.into_json().unwrap();
        assert_eq!(json["ingestSamples"]["accepted"], 3);
        assert_eq!(json["ingestSamples"]["totalStored"], 3);
    }

    #[tokio::test]
    async fn api_version_resolves() {
        let schema = test_schema();
        let resp = schema.execute("{ apiVersion }").await;
        assert!(resp.errors.is_empty());
        let json = resp.data.into_json().unwrap();
        assert_eq!(json["apiVersion"], env!("CARGO_PKG_VERSION"));
    }

    #[tokio::test]
    async fn ingest_then_snapshot() {
        let schema = test_schema();
        ingest_three(&schema).await;

        let q = r#"{ snapshot {
            totalSamples validatorsSeen observed skipped skipRate
            slotTime { count p50 max }
            perValidator { validator skipRate }
        } }"#;
        let resp = schema.execute(q).await;
        assert!(resp.errors.is_empty(), "snapshot errors: {:?}", resp.errors);
        let json = resp.data.into_json().unwrap();
        let snap = &json["snapshot"];
        assert_eq!(snap["totalSamples"], 3);
        assert_eq!(snap["validatorsSeen"], 2);
        assert_eq!(snap["skipped"], 1);
        // bob (skip rate 1.0) sorts ahead of alice (0.0)
        assert_eq!(snap["perValidator"][0]["validator"], "bob");
        // only the two produced slots contribute to slot-time percentiles
        assert_eq!(snap["slotTime"]["count"], 2);
    }

    #[tokio::test]
    async fn validator_and_epoch_queries() {
        let schema = test_schema();
        ingest_three(&schema).await;

        let resp = schema
            .execute(r#"{ validatorReport(validator: "alice") { slotsLed slotsSkipped } }"#)
            .await;
        let json = resp.data.into_json().unwrap();
        assert_eq!(json["validatorReport"]["slotsLed"], 2);
        assert_eq!(json["validatorReport"]["slotsSkipped"], 0);

        let resp = schema
            .execute(r#"{ epochSummary(epoch: 0) { samples skipRate } }"#)
            .await;
        let json = resp.data.into_json().unwrap();
        assert_eq!(json["epochSummary"]["samples"], 3);

        // unknown validator -> null
        let resp = schema
            .execute(r#"{ validatorReport(validator: "ghost") { slotsLed } }"#)
            .await;
        let json = resp.data.into_json().unwrap();
        assert!(json["validatorReport"].is_null());
    }

    #[tokio::test]
    async fn empty_leader_is_rejected() {
        let schema = test_schema();
        let m = r#"mutation {
            ingestSamples(samples: [
                { slot: 1, leader: "", slotTimeMs: 400, voteLatencyMs: 90, skipped: false }
            ]) { accepted }
        }"#;
        let resp = schema.execute(m).await;
        assert!(!resp.errors.is_empty());
    }

    #[tokio::test]
    async fn worst_validators_respects_limit() {
        let schema = test_schema();
        ingest_three(&schema).await;
        let resp = schema
            .execute(r#"{ worstValidators(limit: 1) { validator } }"#)
            .await;
        assert!(resp.errors.is_empty());
        let json = resp.data.into_json().unwrap();
        assert_eq!(json["worstValidators"].as_array().unwrap().len(), 1);
        assert_eq!(json["worstValidators"][0]["validator"], "bob");
    }
}
