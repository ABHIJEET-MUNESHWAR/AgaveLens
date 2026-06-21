//! Command-line interface and argument types for the AgaveLens node.

use clap::{Args, Parser, Subcommand};

/// Validator-internals analytics node.
#[derive(Debug, Parser)]
#[command(name = "agavelens-node", version, about, long_about = None)]
pub struct Cli {
    /// Emit logs as JSON instead of human-readable text.
    #[arg(
        long,
        global = true,
        env = "AGAVELENS_LOG_JSON",
        default_value_t = false
    )]
    pub log_json: bool,

    /// Subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level node commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Serve the GraphQL analytics API over HTTP.
    Serve(ServeArgs),
    /// Run a one-shot batch analytics job over synthetic telemetry and print a report.
    Analyze(AnalyzeArgs),
}

/// Arguments for the `serve` command.
#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    /// Address to bind.
    #[arg(long, env = "AGAVELENS_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Port to bind.
    #[arg(long, env = "AGAVELENS_PORT", default_value_t = 8080)]
    pub port: u16,

    /// Maximum samples retained in the bounded store.
    #[arg(long, env = "AGAVELENS_MAX_SAMPLES", default_value_t = 200_000)]
    pub samples_capacity: usize,
}

impl Default for ServeArgs {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            samples_capacity: 200_000,
        }
    }
}

/// Arguments for the `analyze` command.
#[derive(Debug, Clone, Args)]
pub struct AnalyzeArgs {
    /// Number of consecutive slots of synthetic telemetry to generate.
    #[arg(long, default_value_t = 50_000)]
    pub slots: u64,

    /// Number of synthetic validators in the leader set.
    #[arg(long, default_value_t = 64)]
    pub validators: usize,

    /// Baseline skip-rate percentage (0..=100); validators deviate from it.
    #[arg(long, default_value_t = 8)]
    pub skip_rate: u8,

    /// How many worst-skip-rate validators to list in the report.
    #[arg(long, default_value_t = 10)]
    pub top: usize,

    /// Optionally also print the summary for a specific epoch.
    #[arg(long)]
    pub epoch: Option<u64>,
}

impl Default for AnalyzeArgs {
    fn default() -> Self {
        Self {
            slots: 50_000,
            validators: 64,
            skip_rate: 8,
            top: 10,
            epoch: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn defaults_are_populated() {
        let serve = ServeArgs::default();
        assert_eq!(serve.port, 8080);
        assert_eq!(serve.samples_capacity, 200_000);
        let analyze = AnalyzeArgs::default();
        assert_eq!(analyze.validators, 64);
        assert!(analyze.epoch.is_none());
    }

    #[test]
    fn parses_serve_with_flags() {
        let cli = Cli::try_parse_from(["agavelens-node", "serve", "--port", "9090"]).unwrap();
        match cli.command {
            Command::Serve(a) => assert_eq!(a.port, 9090),
            _ => panic!("expected serve"),
        }
    }

    #[test]
    fn parses_analyze_with_flags() {
        let cli = Cli::try_parse_from([
            "agavelens-node",
            "analyze",
            "--slots",
            "1000",
            "--validators",
            "8",
        ])
        .unwrap();
        match cli.command {
            Command::Analyze(a) => {
                assert_eq!(a.slots, 1000);
                assert_eq!(a.validators, 8);
            }
            _ => panic!("expected analyze"),
        }
    }
}
