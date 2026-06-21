//! Criterion benchmark for the parallel batch aggregator.
//!
//! Compares the `rayon` parallel path against the serial path across input
//! sizes, reporting per-element throughput.

use chrono::Utc;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

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

criterion_group!(benches, bench_aggregate);
criterion_main!(benches);
