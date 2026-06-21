//! # agavelens-node
//!
//! Composition root for AgaveLens. Wires the in-memory adapters into the
//! analytics engine, exposes the GraphQL API over axum, and provides a CLI with
//! two subcommands: `serve` (run the API) and `analyze` (one-shot batch report).

#![forbid(unsafe_code)]

pub mod analyze;
pub mod config;
pub mod startup;
pub mod telemetry;

pub use config::{Cli, Command};

/// Dispatch a parsed CLI invocation.
pub async fn run(cli: Cli) -> anyhow::Result<()> {
    telemetry::init_tracing(cli.log_json);
    match cli.command {
        Command::Serve(args) => startup::run_server(args).await,
        Command::Analyze(args) => analyze::run_analyze(args).await,
    }
}
