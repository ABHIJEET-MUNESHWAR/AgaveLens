//! Telemetry wiring: structured tracing and a Prometheus metrics recorder.

use anyhow::Result;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

/// Initialise the global tracing subscriber.
///
/// Honours `RUST_LOG`; defaults to `info`. When `json` is set, logs are emitted
/// as structured JSON for ingestion by log pipelines.
pub fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let registry = tracing_subscriber::registry().with(filter);
    if json {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().compact())
            .init();
    }
}

/// Install the global Prometheus recorder and return a handle for `/metrics`.
///
/// # Errors
/// Fails if a global recorder is already installed.
pub fn init_metrics() -> Result<PrometheusHandle> {
    let handle = PrometheusBuilder::new().install_recorder()?;
    Ok(handle)
}
