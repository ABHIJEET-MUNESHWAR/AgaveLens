//! The `analyze` command: generate synthetic telemetry, run it through the
//! engine, and print a human-readable analytics report.

use std::time::Instant;

use anyhow::Result;
use chrono::Utc;

use agavelens_types::{AnalyticsSnapshot, Epoch};

use crate::config::AnalyzeArgs;
use crate::startup::build_engine;
use agavelens_infra::SampleGenerator;

/// Run a one-shot batch analytics job and print a report.
pub async fn run_analyze(args: AnalyzeArgs) -> Result<()> {
    // Size the store to retain every generated slot (no eviction during analysis).
    let capacity = (args.slots as usize).max(1);
    let engine = build_engine(capacity);
    let generator = SampleGenerator::new(args.validators, 432_000, args.skip_rate);

    let now = Utc::now();
    let samples = generator.generate(0, args.slots, now);
    let max_batch = engine.config().max_batch;

    let started = Instant::now();
    for chunk in samples.chunks(max_batch) {
        engine.ingest_batch(chunk.to_vec()).await?;
    }
    let snapshot = engine.snapshot().await?;
    let elapsed = started.elapsed();

    print_report(
        &snapshot,
        args.validators,
        args.skip_rate,
        args.top,
        elapsed,
    );

    if let Some(epoch) = args.epoch {
        if let Some(summary) = engine.epoch_summary(Epoch(epoch)).await? {
            println!();
            println!("epoch {epoch} summary:");
            println!("  samples          : {}", summary.samples);
            println!("  skip rate        : {:.2}%", summary.skip_rate * 100.0);
            println!(
                "  slot time p50/p99: {} / {} ms",
                summary.slot_time_p50, summary.slot_time_p99
            );
            println!(
                "  vote lat p50/p99 : {} / {} ms",
                summary.vote_latency_p50, summary.vote_latency_p99
            );
        } else {
            println!("\nepoch {epoch}: no samples");
        }
    }

    Ok(())
}

fn print_report(
    snapshot: &AnalyticsSnapshot,
    validators: usize,
    skip_rate: u8,
    top: usize,
    elapsed: std::time::Duration,
) {
    let secs = elapsed.as_secs_f64();
    let throughput = if secs > 0.0 {
        snapshot.total_samples as f64 / secs
    } else {
        0.0
    };

    println!(
        "AgaveLens analyze — {} slots across {validators} validators ({skip_rate}% baseline skip)",
        snapshot.total_samples
    );
    println!();
    println!("overall:");
    println!("  validators seen  : {}", snapshot.validators_seen);
    println!(
        "  skip rate        : {:.2}%  ({} of {} slots)",
        snapshot.skip_rate() * 100.0,
        snapshot.skip.skipped,
        snapshot.skip.observed
    );
    println!(
        "  slot time ms     : p50 {}  p90 {}  p99 {}  max {}",
        snapshot.slot_time.p50,
        snapshot.slot_time.p90,
        snapshot.slot_time.p99,
        snapshot.slot_time.max
    );
    println!(
        "  vote latency ms  : p50 {}  p90 {}  p99 {}  max {}",
        snapshot.vote_latency.p50,
        snapshot.vote_latency.p90,
        snapshot.vote_latency.p99,
        snapshot.vote_latency.max
    );
    println!();
    println!(
        "worst {} validators by skip rate:",
        top.min(snapshot.per_validator.len())
    );
    println!(
        "  {:<10}  {:>8}  {:>9}  {:>10}  {:>10}",
        "validator", "led", "skip", "slot p50", "vote p99"
    );
    for r in snapshot.worst_validators(top) {
        println!(
            "  {:<10}  {:>8}  {:>8.2}%  {:>9}  {:>10}",
            r.validator.as_str(),
            r.slots_led,
            r.skip_rate * 100.0,
            r.slot_time.p50,
            r.vote_latency.p99
        );
    }
    println!();
    println!("aggregation:");
    println!("  elapsed          : {:.3} ms", secs * 1_000.0);
    println!("  throughput       : {throughput:.0} samples/s");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn analyze_runs_end_to_end() {
        let args = AnalyzeArgs {
            slots: 2_000,
            validators: 8,
            skip_rate: 10,
            top: 5,
            epoch: Some(0),
        };
        // Should complete without error and print a report.
        run_analyze(args).await.unwrap();
    }

    #[tokio::test]
    async fn analyze_with_zero_slots_is_safe() {
        let args = AnalyzeArgs {
            slots: 0,
            validators: 4,
            skip_rate: 0,
            top: 3,
            epoch: None,
        };
        run_analyze(args).await.unwrap();
    }
}
