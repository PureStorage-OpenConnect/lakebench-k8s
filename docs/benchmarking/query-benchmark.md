# Query benchmark

Reference: query categories, QpH modes and samples, cache modes, in-stream rounds and the in-stream QpH basis.

Flags: [cli-reference.md](../cli-reference.md#benchmark).

## Query categories

The 8 Customer 360 queries. The AML set is in [Query Reference](../benchmarks/aml/queries.md#6-query-set).

| Category | Queries | What it exercises |
|---|---|---|
| Scan | Q1 | Full scan of silver with global aggregates (records, unique customers, revenue). Raw I/O. |
| Filter/Prune | Q2, Q4 | Q2: silver over a 3-month window, by date and interaction type. Q4: churn-risk filters by journey stage and device category, with HAVING. |
| Aggregation | Q3, Q7 | Q3: customers by value tier and channel preference. Q7: per-channel funnel (awareness, consideration, conversion, retention) with `SUM(CASE)`. |
| Analytics | Q5, Q6 | Q5: 7-day moving average of daily revenue and DAU. Q6: RFM scoring with a CTE and multi-branch CASE. |
| Operational | Q9 | Dashboard read of the pre-aggregated gold table, with LAG for day-over-day revenue and DAU growth. |

## Modes

The three modes follow TPC methodology.

| Mode | What runs | Formula |
|---|---|---|
| Power (default) | Every query in sequence, one stream, each timed | `QpH = (num_queries / total_seconds) * 3600` |
| Throughput | N concurrent streams, each the full set in shuffled order (fewer correlated cache effects) | `QpH = (total_queries_across_all_streams / wall_clock_seconds) * 3600` |
| Composite | Power, then throughput | `composite_qph = sqrt(power_qph * throughput_qph)` |

Example: 8 queries in 40 s give (8 / 40) x 3600 = 720 QpH.

- Throughput and composite come from `lakebench benchmark --mode`.
- `lakebench run` measures one power pass, one stream, hot cache. It refuses a config whose `architecture.benchmark` asks for `throughput`, `composite`, `cache: cold` or `streams` above 1. `metrics.json` records the power pass it ran.

**Samples per query (power).**

- Each query is timed `architecture.benchmark.iterations` times (default 3) and scored by its median; QpH sums the medians. `iterations: 1` gives no spread.
- The first failed sample fails the query and stops its repeats.
- Each query keeps `samples`, `min_seconds`, `max_seconds`, `relative_range` ((max - min) / median).
- Each round's `spread`: `qph_low` and `qph_high` (slowest and fastest round the samples allow) and `samples_per_query` (smallest count over successful queries). The scorecard repeats them as `benchmark_samples_per_query` and `qph_spread`.
- Older records without `samples` read as one sample.
- Cost: each round takes three times as long, plus warm-up (Customer 360 scale 100: ~8 to ~24 minutes). Below scale 50 the pre-maintenance round adds about 2 x 2 x one round.
- Continuous rounds take one sample per query: gold changes under the round, and the rounds are the repeats.

## Cache modes

`hot` or `cold` (`lakebench benchmark --cold`; `run` uses hot). Cold flushes the engine's metadata cache (`CALL iceberg.system.flush_metadata_cache()` on Trino, equivalents on Spark Thrift and DuckDB): before each query in power mode, once before all streams in throughput mode.

## In-stream rounds

A continuous run benchmarks while the streams write, compact and change tables. Schedule keys: [running-pipelines.md](../running-pipelines.md#continuous-configuration).

- First round after `benchmark_warmup` (default 300 s), then every `benchmark_interval` (default 300 s), counted from a round's end.
- Each round flushes the metadata cache, probes gold freshness and runs the full power benchmark.
- The continuous `composite_qph` is the median across rounds.
- A run shorter than `benchmark_warmup + benchmark_interval` has no round and no QpH, with a warning.
- With `gold_refresh_interval` set, both are raised to it, with a warning (floor 300 s). Lower warmup hits an empty or stale gold and inflates QpH; a lower interval overlaps gold rewrites (Q9 contention). For more rounds, raise `run_duration`.

### Planning round counts

```
available = run_duration - warmup
rounds ≈ 1 + floor((available - round_time) / (interval + round_time))
```

Round time: 20-40 s at scale 10 with 3 Trino workers, 60-120 s at scale 100 with 10.

| Duration | Warmup | Interval | Round time | Rounds |
|---|---|---|---|---|
| 30 min | 300 s | 300 s | 40 s | 5 |
| 45 min | 300 s | 300 s | 40 s | 7 |
| 60 min | 300 s | 300 s | 40 s | 10 |

Five rounds or more for trend analysis: `run_duration >= warmup + 4 * (interval + round_time) + max(60, 1.2 * round_time)` (the fifth round must pass the end-of-window guard). At the defaults and 40 s rounds: 1720 s, so 1800 s (30 min) gives 5.

**End-of-window guard.** A final round starts only if it can finish: 60 s before any round completes, then 1.2 x the observed round time.

**Q9 contention.** Q9 alone reads gold, which `createOrReplace()` rewrites every refresh. A failed Q9 is retried up to twice (30 s, 60 s backoff). Each round records `q9_contention_observed` and `q9_retry_used`.

**Gold event age** (diagnostic, not freshness): query time minus `MAX(interaction_date)`, day resolution. It follows the corpus's event dates: a corpus dated 2024-12-18 to 2025-01-01 reads ~635 days in 2026.

- Median: `pipeline_benchmark.diagnostics.query_time_event_age_seconds`. Per round: `round_meta.gold_event_age_seconds`.
- Records before 1.6 wrote it as `query_time_freshness_seconds` / `gold_freshness_seconds` and printed it as the continuous Pipeline Score. The score line now shows `data_freshness_seconds`.

### In-stream QpH basis

Each round records `index`, `started_at`, `ended_at`, `executed_queries` (those that succeeded), `executed_query_set_id` and `investigator_queries` (`included`, `absent_no_cases` or `probe_failed`: whether an AML round with the TM operations layer ran IQ1-IQ4, which starts once the run has a case; null otherwise).

- A round with a failed query has QpH over a smaller set.
- `scores.composite_qph_basis`: whether the rounds with a QpH ran more than one set (`blended`), and rounds per set. `scores.composite_qph_by_set`: median per set.
- AML continuous: composite QpH uses only the rounds that ran the full 12-query set (`composite_qph_basis.composite_set`). Earlier 8-query rounds stay in `composite_qph_by_set`; `qph_degradation_pct` uses full-set rounds.
- Blended: the aggregate `query_set_id` reads `blended`, `composite_qph` and `in_stream_composite_qph` are not comparable with another run's and `qph_degradation_pct` is withheld (`scores.qph_degradation_withheld`). Otherwise `query_set_id` is every query name the rounds ran, even one that failed in every round.
- Every round missed the same query: medians cover the smaller set.
- Pre-1.7 rounds get their set from success flags; one with a query failing in some rounds reads blended.
