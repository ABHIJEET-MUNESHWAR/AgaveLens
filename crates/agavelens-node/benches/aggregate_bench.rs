//! Criterion benchmark for the parallel batch aggregator.
//!
//! Compares the `rayon` parallel path against the serial path across input
//! sizes, reporting per-element throughput.
//!
//! A flame graph of the hot path can be generated with the bundled `pprof`
//! sampling profiler (no `perf`/root required):
//!
//! ```text
//! cargo bench --bench aggregate_bench -- --profile-time 10 'parallel/1000000'
//! # -> target/criterion/aggregate/parallel/1000000/profile/flamegraph.svg
//! ```

use chrono::Utc;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pprof::criterion::{Output, PProfProfiler};

use agavelens_core::aggregate;
use agavelens_infra::SampleGenerator;

fn bench_aggregate(c: &mut Criterion) {
    let now = Utc::now();
    // A production-scale leader set: real Solana mainnet runs ~1.5k validators.
    let generator = SampleGenerator::new(1_500, 432_000, 10);

    let mut group = c.benchmark_group("aggregate");
    for &n in &[100_000u64, 1_000_000u64] {
        let samples = generator.generate(0, n, now);
        group.throughput(Throughput::Elements(n));

        group.bench_with_input(BenchmarkId::new("parallel", n), &samples, |b, s| {
            b.iter(|| aggregate(s, now, 50_000));
        });
        group.bench_with_input(BenchmarkId::new("serial", n), &samples, |b, s| {
            b.iter(|| aggregate(s, now, usize::MAX));
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(1_000, Output::Flamegraph(None)));
    targets = bench_aggregate
}
criterion_main!(benches);
