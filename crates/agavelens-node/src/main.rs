//! AgaveLens node binary entry point.

use clap::Parser;

use agavelens_node::{run, Cli};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    run(cli).await
}
