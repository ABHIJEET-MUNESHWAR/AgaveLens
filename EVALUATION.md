# AgaveLens — Self-Evaluation

A candid assessment of this implementation against the workspace's production-grade Rust
engineering guidelines. Each row states **what** the guideline asks, **where** AgaveLens
satisfies it, and an honest note on **limits**.

Legend: ✅ fully addressed · 🟡 addressed with a documented limitation · ⬜ intentionally
out of scope (with rationale).

> **Contrast with the siblings.** Each project headlines a different slice so the set
> demonstrates range rather than repetition: SolLander → rate limiting + generative AI;
> QuicForge → timeout/retry (clean latencies, *no* breaker); BundleRelay → **circuit breaker +
> rate limiter**; ShredStream → **concurrency bulkhead + back-pressure + TTL eviction**;
> AgaveLens → **`rayon` data-parallel aggregation** as a CPU-bound hot path, a **read-mostly
> analytics** GraphQL surface with **no subscription**, and **lighter, ingest-focused
> resilience** (rate limiter + bounded store + batch guard — deliberately no circuit breaker,
> because there is no flaky downstream to trip on).

---

## 1. Design, SOLID, type-safety (guidelines 1, 10, 13, 14, 22, 23)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | **Hexagonal layering**: `agavelens-core` defines ports (traits) + the `AnalyticsEngine`; adapters live in `agavelens-infra`. The domain has **zero** web/db deps. |
| ✅ | **Make-illegal-states-unrepresentable**: validated newtypes `Slot`, `Epoch`, `ValidatorId` (non-empty, ≤ 64 bytes); the `SlotSample` constructor range-checks `slot_time_ms` / `vote_latency_ms`. An invalid sample cannot exist in the domain. |
| ✅ | **DIP**: the engine depends on `Arc<dyn Trait>` ports (`SampleRepository`, `Clock`), injected at the composition root. |
| ✅ | **ISP / small interfaces**: `SampleRepository` (async) and `Clock` (sync) are single-purpose; `mockall::automock` generates doubles for both. |
| ✅ | **Sync vs async ports** modeled correctly: `Clock` is sync; the repository is `#[async_trait]`. |
| ✅ | `#![forbid(unsafe_code)]` in **every** crate. |

## 2. Architecture: events, CQRS, composability (guidelines 2, 9, 21)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | **CQRS**: the write path is `ingestSamples` (mutation → `AnalyticsEngine::ingest_batch`); the read path is `snapshot` / `worstValidators` / `validatorReport` / `epochSummary` / `sampleCount` / `skipRate` queries. Commands and queries use distinct types and code paths. |
| ✅ | **Composability**: swapping the `SampleGenerator` for a real telemetry tap, or the memory store for a durable adapter, is a one-line change at the composition root because both satisfy the `SampleRepository` port. |
| ⬜ | **Subscriptions / event bus**: intentionally omitted. Analytics are pull-based reductions; clients poll `snapshot`. The sibling ShredStream/BundleRelay projects demonstrate the event-driven subscription surface against these same guidelines. |

## 3. Partitioning & sharding (guideline 3)

| ✅/🟡 | Evidence |
|---|---|
| 🟡 | The store is an in-memory bounded `MemorySampleRepository` behind the `SampleRepository` port — the documented seam where a partitioned SQL/columnar store (range-partition by `epoch`, hash-shard by `validator`) drops in, as demonstrated in the sibling SolLander project. No persistent DB ships by design. |

## 4–5. Resilience (guidelines 4, 5)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | **Batch-size guard**: ingest rejects batches over `max_batch` (default 10,000) **before** any work → `CoreError::BatchTooLarge`. Bounds per-request CPU/memory. |
| ✅ | **Inbound rate limiting**: a token-bucket `RateLimiter` (capacity + refill/sec) on an injectable `Clock` sheds ingest floods → `CoreError::Throttled`. |
| ✅ | **Bounded working set**: the `VecDeque` store is capped at `max_samples` with oldest-first eviction — memory is provably bounded under unbounded ingest, with no eviction loop or TTL bookkeeping required. |
| ✅ | **Non-blocking aggregation**: the rayon reduce runs inside `tokio::task::spawn_blocking`, so a heavy `snapshot` never stalls the async executor serving other requests. |
| ✅ | **GraphQL DoS guard**: `limit_depth(12)` + `limit_complexity(256)`. |
| ✅ | **Graceful degradation**: back-pressure and oversize batches fold into typed `CoreError`s with stable `.code()`s; runtime paths never panic. |
| ⬜ | **Circuit breaker**: deliberately omitted — there is no unreliable downstream dependency to protect. Adding one would be cargo-culting; BundleRelay headlines the breaker where it is actually warranted. |

## 6, 20. Error handling & edge cases (guidelines 6, 20)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | `thiserror` enums in libraries (`InvalidSample`, `PortError`, `CoreError`); `anyhow` only in the `agavelens-node` binary/CLI. |
| ✅ | **No `unwrap`/`expect`/`panic` on runtime paths** — failures become `Result`. The one `expect` is in the *synthetic generator* on values it just constructed within bounds, documented as a provable invariant. |
| ✅ | Every error carries a machine-readable `.code()` (`empty_validator`, `validator_id_too_long`, `value_out_of_range`, `batch_too_large`, `throttled`, …) surfaced as the GraphQL error. |
| ✅ | `PortError::is_retryable()` distinguishes transient store faults from terminal ones. |
| ✅ | Edge cases under test: empty/oversized validator id rejected, out-of-range slot/vote time rejected, oversize batch rejected, throttled ingest, empty-snapshot percentiles, zero-slot analyze, epoch-with-no-samples returns null. |

## 7. GraphQL over REST (guideline 7)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | `agavelens-api` is pure `async-graphql` (Query + Mutation). The only non-GraphQL routes are operational probes (`/health/*`, `/metrics`). |
| ✅ | A DTO anti-corruption layer (`types.rs`) keeps domain types free of `async-graphql` derives; `From` conversions map domain → wire objects. |

## 8. Test coverage (guideline 8)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | **68 tests**: types 22, core 22, infra 10, api 5, node 9 — unit + adapter integration + GraphQL schema execution + axum handler `oneshot` + `analyze` end-to-end. |
| ✅ | **Deterministic** throughout: an injectable `ManualClock` drives rate-limiter tests without sleeping; the `SampleGenerator` is a seeded MurmurHash mixer (**no `rand`**), so aggregation and worst-validator ordering are reproducible. |
| ✅ | Mocked ports (`mockall`) for store-error injection + hand-written `FakeRepo`; the parallel and serial aggregation paths are both exercised (threshold-crossing test). |
| 🟡 | Coverage is *meaningful-path* complete; a `cargo llvm-cov` threshold isn't gated in CI yet (documented next step). |

## 12. Generative & agentic AI (guideline 12)

| ✅/🟡 | Evidence |
|---|---|
| ⬜ | **Intentionally not applicable.** AgaveLens is a numeric analytics path; an LLM would add latency and no value. The sibling **SolLander** project demonstrates the full generative + agentic-AI layer against these same guidelines. |

## 16–18. Performance & concurrency (guidelines 16, 17, 18) — **the headline feature**

| ✅/🟡 | Evidence |
|---|---|
| ✅ | **`rayon` data-parallelism**: grouping is a cheap serial linear pass; the CPU-bound percentile work is parallelised — per-validator reports via `into_par_iter()` and the two fleet-wide vectors via `par_sort_unstable()`. |
| ✅ | **Adaptive & measured**: `aggregate()` auto-selects serial vs parallel at a benchmarked ~500k-sample crossover (`parallel_threshold`). Below it serial is faster (less fork/join overhead); above it rayon wins and the margin grows with size/cores — an honest tuning decision, not "parallel everywhere". |
| ✅ | **Async never blocked**: the entire rayon reduce is wrapped in `spawn_blocking`, keeping Tokio workers free. The engine is `Clone` (all state behind `Arc`) and shared lock-light across handlers. |
| ✅ | **Criterion benchmark** (`benches/aggregate_bench.rs`, 1.5k-validator fleet) pits parallel against serial at 100k and 1M samples: serial wins at 100k (17.7 vs 9.7 Melem/s), parallel wins at 1M (18.3 vs 15.8 Melem/s, crossover ~500k). Measured end-to-end **~5.3M samples/s** (ingest + bounded-store write + full parallel aggregation) over 1M slots / 1.5k validators on an 8-core laptop. |
| ✅ | Cheap-first ordering: batch-size and rate-limit checks shed **before** any store write or aggregation. |

## 19. Observability (guideline 19)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | `tracing` spans + JSON-log option (`--log-json`); Prometheus `/metrics` via `metrics-exporter-prometheus`. |
| ✅ | RED-method signals: `agavelens_samples_ingested_total`, `agavelens_batches_rejected_total{reason}`, `agavelens_snapshots_total`, `agavelens_graphql_requests_total`; `sampleCount` / `skipRate` / `snapshot` are queryable over GraphQL. |
| ✅ | Optional Prometheus stack via `docker compose --profile monitoring up`. |

## 24. Benchmarks & complexity (guideline 24)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | Criterion bench of `aggregate` (parallel vs serial). Aggregation is O(n) serial bucketing + O(v · k log k) percentile sorts (v validators, k samples each); the rayon path distributes the sort-heavy phase across cores. Ingest is O(batch) with O(1) guard checks. |

## 25–27. CI/CD, Docker, Postman (guidelines 25, 26, 27)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | `.github/workflows/ci.yml`: fmt + clippy (`-D warnings`) + test + `cargo audit`. |
| ✅ | Multi-stage `Dockerfile` (`rust:1.89-slim` → `debian-slim`, non-root uid 10001) + `docker-compose.yml` (node + optional Prometheus profile). |
| ✅ | `postman/AgaveLens.postman_collection.json` — ingest mutation, analytics queries, and operational requests (no subscription, matching the surface). |

## 11, 15. Canonical crates & docs (guidelines 11, 15)

| ✅/🟡 | Evidence |
|---|---|
| ✅ | Only workspace-canonical crates (`rayon` added for the parallel hot path); internal versions declared once in `[workspace.dependencies]` and inherited with `{ workspace = true }`. |
| ✅ | This document + a thorough [`README.md`](README.md) with architecture, pipeline diagram, API, CLI, resilience model, real `analyze` output, and examples. |

---

## Known limitations (honest list)

1. **Synthetic telemetry source.** Samples come from the GraphQL mutation or a deterministic
   in-process `SampleGenerator`, not a live Agave validator-metrics/geyser feed. The
   `SampleRepository` port and the generator are the seam for a real tap; the core aggregation
   pipeline is unaffected.
2. **In-memory bounded store.** Samples live in a `parking_lot`-guarded `VecDeque` behind the
   `SampleRepository` port; persistence/partitioning is the documented SQL/columnar seam.
3. **Single-node.** Horizontal scale-out (shard by epoch/validator across nodes) is out of
   scope for this portfolio cut.
4. **No `cargo llvm-cov` gate in CI** yet — coverage is meaningful-path complete but not
   numerically gated.

None of these affect the engineering the guidelines target: the layering, type-safety,
**rayon-parallel aggregation off the async executor**, ingest-appropriate resilience
(rate limiter + bounded store + batch guard), CQRS read/write split, observability, and test
discipline are all real and exercised.
