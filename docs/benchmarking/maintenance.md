# Maintenance and Lakebench caps

Reference: batch maintenance scoring, the storage settle wait, effective maintenance, compaction, and the Lakebench caps a run records.

## Maintenance scoring

With `pre_benchmark_maintenance: true` (the default), a batch run benchmarks twice:

1. **Pre-compaction benchmark** (below scale 50 only): the power benchmark on uncompacted data.
2. **Maintenance:** Iceberg `expire_snapshots`, `remove_orphan_files` (floor 24 h 10 min) and `rewrite_data_files` (silver and gold), or Delta `VACUUM` on Trino. Delta `OPTIMIZE` never runs. All statements share one 30-minute budget; the first statement timeout or the deadline stops the rest, and the benchmark runs anyway.
3. **Storage settle wait** (below).
4. **Post-compaction benchmark:** the same query set on compacted data.

Each round starts with one unmeasured warm-up pass (first touch of a snapshot). Before 1.6, batch maintenance was compaction only.

| Field | Meaning |
|---|---|
| `pre_compaction_qph` | QpH before maintenance. |
| `post_compaction_qph` | QpH after, over every query that succeeded in that round. The reported `composite_qph`. |
| `maintenance_value_pct` | QpH change over only the queries that succeeded in both rounds. Null when maintenance did not run, compaction changed no files, no query succeeded twice, one sample per query, or within noise. |
| `maintenance_value_reason` | Why it is null. |
| `maintenance_paired_queries` | Queries in that comparison. |
| `maintenance_elapsed_seconds` | Wall clock spent on maintenance. |
| `maintenance_pct_of_pipeline` | Maintenance seconds as a share of `time_to_value_seconds` plus maintenance. |
| `pre_compaction_file_count`, `post_compaction_file_count`, `compaction_ratio` | Data files before and after; their ratio (higher = more benefit). |
| `maintenance_settle_seconds` | Maintenance end to a stable settle probe. In no stage time or time to value. |
| `maintenance_settled`, `maintenance_settle_capped`, `maintenance_settle_verified` | False: post round on unsettled storage. True: wait reached `max_seconds`. False: no pre-maintenance probe time to check against. |

**Within noise.** The value is reported only when the rounds are distinguishable:

- Each round's paired total ranges from the sum of fastest to slowest samples. Overlap: null, reason `within noise: ...`, with the difference and both spreads.
- 11% or less is also within noise: drift between two unchanged rounds measured 3-11%.
- `iterations: 1`: null, reason `one sample per query`.
- The pre round is kept as `pre_compaction_benchmark`, samples included, for rechecking.

`lakebench run --skip-maintenance config.yaml` skips maintenance and runs only the post-pipeline benchmark.

### Storage settle wait

The object store keeps working off deletes and rewrites after maintenance SQL returns. Measured on FlashBlade, Customer 360 scale 10: compacted files read QpH 546 at ~2 min, 569 at +15 min and 841 at +35 min, against 828 before. AML scale 10 read 27% slow straight after.

- Lakebench times one storage-bound probe every `interval_seconds`. Default probe: the workload's first scan-class query (a full scan of the table compaction rewrote).
- Settled: two consecutive probes agree within `tolerance_pct` and, when the pre round ran, neither is slower than that query's pre-maintenance median by more. Agreement alone is not enough: the +2 and +15 min rounds above agreed within 4% while both were a third slow. The reference is a bound, not a target: when compaction speeds the probe up more than settling slows it, an unsettled probe can still pass.
- No pre round (scale 50 and above): three consecutive probes must agree, and `maintenance_settle_verified` is false.
- Stable but slower than before: waits to the cap; the reason says settling and a regression are not separable.
- At `max_seconds`, or after three failed probes in a row: the post round runs, and `maintenance_value_pct` is null with reason `storage did not settle within N s`.
- Probe times: `maintenance_settle`. The wait adds to wall clock only.

```yaml
architecture:
  benchmark:
    maintenance_settle:
      enabled: true          # false: post round straight after maintenance
      max_seconds: 2700      # recovery took ~35 min in the measured case
      interval_seconds: 60
      tolerance_pct: 10.0    # unsettled rounds were 27-34% slow
      probe_query: null      # default: first scan-class query
      probe_samples: 1       # timed runs per probe, median taken
```

Ranges: [configuration.md](../configuration.md).

**Continuous mode does not wait.** Maintenance and compaction run on timers during the stream (`architecture.pipeline.continuous.retention_interval`, `compaction_interval`).

- AML in-window compaction skips tables a stream rewrites every batch or [tick](../glossary.md#tick) (gold, and silver's entities, accounts, entity_profiles and silver_batch_versions): a rewrite committed under their MERGE fails it.
- Rounds just after maintenance can read slow; `in_stream_composite_qph` (a median) and `qph_degradation_pct` include them. The median limits the effect when only a few rounds land in a settling window.

## Effective maintenance

`experiment.effective_maintenance` labels each operation (Iceberg `expire_snapshots`, `remove_orphan_files`, `compaction`; Delta `vacuum`, `compaction`):

| Label | Meaning |
|---|---|
| `ran` | Executed. |
| `ran_no_effect` | Executed at a retention nothing in the window can meet (continuous Delta `VACUUM` at the 7-day default). |
| `not_supported` | The composition cannot run it (DuckDB, Delta `OPTIMIZE`, Delta `VACUUM` on Spark Thrift). |
| `skipped_by_user` | `--skip-maintenance`, `pre_benchmark_maintenance: false`, `--skip-benchmark`, or continuous compaction disabled. |
| `failed` | Attempted; no statement succeeded. |
| `not_run` | The run ended before maintenance. |

- Runs with different effective maintenance are comparable at best, not like-for-like.
- Exception: two runs that each skipped every operation by choice (`--skip-maintenance`, or batch `pre_benchmark_maintenance: false`) under one maintenance policy count as the same, even across table formats; their maintenance settings are not compared.
- `not_supported` is not a skip: a DuckDB run with maintenance on does not match a skipped run. With `--skip-maintenance` it records `skipped_by_user` like any run.

**Compaction operation** differs by engine and is an execution condition: Trino and Spark Thrift runs that both compacted are not like-for-like.

- Trino runs `optimize` with a 128 MB file size threshold. Spark Thrift runs Iceberg `rewrite_data_files` with its defaults. Delta compaction never runs.
- `effective_maintenance.detail.operations.compaction`: `{"operation": "trino_optimize", "params": {"file_size_threshold": "128MB"}}`, or `iceberg_rewrite_data_files`, or `mixed` (list in `operations`) when calls fell back to the other engine.
- In the exp2 id: `compaction=ran(trino_optimize:128MB)` or `compaction=ran(mixed(iceberg_rewrite_data_files+trino_optimize:128MB))`. Older records derive it from query engine and table format.

**Compaction is per table.** A table is compacted only when every statement for it succeeded.

- Trino, Customer 360 silver (partitioned by `interaction_date`) over 90 partitions: chunks of at most 90 partitions, one `optimize ... WHERE interaction_date ...` each, batch and continuous. Trino refuses an `optimize` across more than 100 partitions.
- AML `silver.transactions` and `silver.account_statements` (monthly): at most one month to merge (two or more files under the threshold) per statement; other months share a neighbour's. Form: `optimize ... WHERE txn_timestamp >= TIMESTAMP '<month> 00:00:00.000000 UTC' AND txn_timestamp < ...` (`book_ts` for statements), first open below, last open above. One statement over all months ran out of Trino per-node query memory.
- Partly compacted: `compaction=ran` in the id (`failed` only when nothing succeeded), `compaction=partial` in `detail_id`.
- Failed tables: `reasons` ("compaction failed on <table>: <error>"), `detail.compaction_failures`, `detail.compaction_statements`.

## Lakebench caps

- `experiment.limits`: the caps a run executed under, including the continuous [trickle](../glossary.md#trickle) (`max_files_per_trigger`, auto-capped at 50), benchmark iterations and in-stream rounds.
- `limits.bound`: the caps that bound it:
  - per-job executor caps (28 at most), a concurrent executor budget, auto-sizing cuts
  - TM alerts over capacity, AML rules skipped on a cap
  - the maintenance budget when it stopped maintenance early
  - the trickle line when the trickle held intake ([continuous.md](continuous.md#trickle-bound-throughput))
- `limits.bound_kinds`: the same without counts, trickle excepted.
- A number measured under a binding cap is a property of the cap, not the infrastructure.
- `limits.headroom_pct` (batch, diagnostic): each stage's headroom against the per-job timeout, and the benchmark's against its per-query timeout. See [aml-scoring.md](../aml-scoring.md#where-gold-finalize-spends-its-time).
