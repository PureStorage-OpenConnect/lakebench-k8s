# Customer 360 benchmark: metrics

## 8. Metrics

Units, directions and bands come from the metric registry
(`metrics/metric_registry.py`), which the report reads.
Direction `none`: a delta has no better side.

### 8.1 Batch

Primary: `time_to_value_seconds`, lower is better.

| Metric | Unit | Direction | Definition |
|---|---|---|---|
| `time_to_value_seconds` | s | lower | latest stage end minus earliest stage start over bronze-verify, silver-build, gold-finalize; details below the table |
| `time_to_value_datagen_excluded_seconds` | s | none | multi-cycle only: the cycles' datagen seconds inside the span, left out of `time_to_value_seconds` (0 when the run reused its corpus); absent when a cycle's datagen times are missing |
| `total_elapsed_seconds` | s | lower | sum of stage elapsed seconds, including a datagen stage when one ran and the query stage when a benchmark ran |
| `total_data_processed_gb` | GiB | none | sum of the GiB each job reported reading: bronze-verify and silver-build the bronze path size, gold-finalize the silver table size (table metadata); never a bucket listing; the query stage reports no input |
| `pipeline_throughput_gb_per_second` | GiB/s | higher | `total_data_processed_gb / time_to_value_seconds` |
| `total_core_hours` | core-h | lower | sum over batch stages of executors x executor cores x elapsed / 3600 (requested, not used; drivers excluded) |
| `compute_efficiency_gb_per_core_hour` | GiB/core-h | higher | `total_data_processed_gb / total_core_hours` |
| `scale_ratio` | ratio | target 1.0 | bronze input GB / `approx_bronze_gb` (10 x scale); a correctness figure |
| `composite_qph` | QpH | higher | QpH of the scored round: successful queries / sum of their median elapsed seconds x 3600; absent when no benchmark ran |
| `benchmark_samples_per_query`, `qph_spread` | count, struct | none | samples behind the medians; QpH of the slowest and fastest sample combination |
| `pre_compaction_qph`, `post_compaction_qph` | QpH | higher | the pre- and post-maintenance rounds |
| `maintenance_value_pct`, `maintenance_value_reason`, `maintenance_paired_queries` | pct, text, count | none | change from the pre to the post round over queries that succeeded in both; null with the reason in the cases below the table |
| `maintenance_elapsed_seconds` | s | lower | maintenance and compaction time |
| `maintenance_pct_of_pipeline` | pct | none | maintenance time as a share of `total_elapsed_seconds` (which includes datagen and the query stage) |
| `maintenance_stopped`, `maintenance_stop_reason` | bool, text | none | pre-benchmark maintenance stopped on a statement timeout or its budget, and why |
| `maintenance_live_streams`, `maintenance_live_streams_reason` | bool, text | none | stream apps were present, or unreadable, during pre-benchmark maintenance |
| `pre_compaction_file_count`, `post_compaction_file_count` | count | none | data files before and after (not published for Delta, whose OPTIMIZE never runs) |
| `compaction_ratio` | ratio | higher | pre over post file count; diagnostic |
| `maintenance_settle_seconds`, `maintenance_settled`, `maintenance_settle_capped`, `maintenance_settle_verified` | s, bool | none | the settle wait ([7.2](execution-rules.md#72-maintenance-policy-handling)) |
| `cycle_progression` | struct | none | per-cycle elapsed, QpH and table health (multi-cycle) |

`time_to_value_seconds`:

- A stage runs from its SparkApplication's creation to its driver
  container's finish (else `terminationTime`, else the 5 s status poll,
  labelled `timing_source: poll`), mapped to the host clock.
- The `[c360-check]` fact collection's `check_seconds` are subtracted. The
  gold-finalize non-degeneracy gate's two reads are not.
- It excludes datagen, maintenance, settle and benchmark.
- Multi-cycle: it spans every cycle, with each cycle's datagen interval
  (`cycles[].datagen_start`..`datagen_end`) subtracted. The table-health
  probe between cycles stays in.

`maintenance_value_pct` is null, with the reason, when:

- maintenance did not run, stopped or ran beside live streams;
- compaction did not reduce the data file count, or the count is
  unavailable;
- no query succeeded in both rounds;
- the settle wait did not settle;
- a round took one sample per query;
- the change is inside the within-round spread or the 11% round-to-round
  drift floor.

Batch time to value includes the non-degeneracy gate's gold count and
distinct silver date count, which grow with silver. Under workload version
`c360-2`, gold-finalize never picks its incremental strategy from table size
(gold with rows, silver over 1,000 GB), so a repeat run rebuilds every gold
day. A `c360-2` run compares with no run recorded
under `c360-1`.

### 8.2 Continuous

Primary: `data_freshness_seconds`, lower is better.

| Metric | Unit | Direction | Definition |
|---|---|---|---|
| `data_freshness_seconds` | s | lower | maximum over gold micro-batches inside the window of the gold write time minus the newest `ingest_ts` (file landing) among the rows the batch recomputed; when the corpus drained, trailing idle cycles are excluded. A `gold_refresh_interval` set in the config is labelled beside it (`limits.trigger_bound`) |
| `sustained_throughput_rps` | rows/s | higher | bronze rows written by batches that started inside the window / `arrival_seconds`. When `intake_limit` is `trickle_rate`, the configured offered load (a Lakebench cap), not a capacity |
| `window_seconds` | s | none | measured window length; follows `run_duration` |
| `arrival_seconds` | s | none | whole window while corpus remained; else bronze's last write inside the window plus one bronze cadence (its trigger, or back to back its median batch time) |
| `window_arrival_fraction` | ratio | none | `arrival_seconds / window_seconds` |
| `pre_window_rows` | rows | none | bronze rows before the window; in no window score |
| `released_rows` | rows | none | own datagen: rows datagen had written one bronze cadence before the window's end, at its mean rate. [Trickle](../../glossary.md#trickle): files per trigger x triggers since bronze's first write, at the corpus mean rows per file, capped at the corpus |
| `ingest_ratio` | ratio | target, guard | bronze rows by the window's end / `released_rows` (falls back to `corpus_ingest_ratio`); should sit in 0.95 to 1.05 |
| `corpus_ingest_ratio` | ratio | none | bronze rows / datagen rows; below 1 when datagen is ahead at the window's end (own datagen) or the corpus outlasts the window (trickle) |
| `pipeline_saturated` | bool | none | `ingest_ratio < 0.95`; under `intake_limit: trickle_rate`, whether silver failed to keep pace (null when silver's pace is unmeasured) |
| `intake_limit` | text | none | `none`, `trickle_rate`, `bronze_capacity` (bronze busy >= 80% of the window), `below_bronze_capacity`; can be null |
| `bronze_busy_fraction` | ratio | none | share of the window bronze spent in micro-batches |
| `corpus_drain_seconds` | s | none | datagen rows / rows per second when intake was trickle-bound |
| `corpus_drained` | bool | none | every datagen row in bronze and committed by silver before the window closed |
| `stage_latency_profile` | struct | none | mean micro-batch time per stream (`bronze_ms`, `silver_ms`, `gold_ms`) |
| `total_rows_processed` | rows | none | rows taken in inside the window across streams (gold re-reads of silver included) |
| `composite_qph`, `in_stream_composite_qph` | QpH | higher | median QpH over in-stream rounds with QpH > 0; rounds are described below the table |
| `composite_qph_rounds`, `benchmark_rounds_count` | count | none | rounds behind the median, and rounds executed |
| `composite_qph_basis`, `composite_qph_by_set` | struct | none | whether the median blends rounds over different query sets, rounds per set, median per set |
| `qph_degradation_pct` | pct | lower | `(1 - median(second half) / median(first half)) x 100`, 4 or more rounds |
| `qph_degradation_withheld` | text | none | why `qph_degradation_pct` is absent with 4 or more rounds: the rounds ran different query sets |
| `query_time_event_age_seconds` | s | none | median age of gold's newest event date at query time; tracks the corpus's position in time, not freshness |
| `total_elapsed_seconds` | s | none | wall clock from run start (before datagen and stream submission) to the record, including stream start-up and stream stop; shared with batch in name only |
| `pipeline_throughput_gb_per_second`, `compute_efficiency_gb_per_core_hour` | GB/s, GB/core-h | higher | `total_data_processed_gb` (stream input sizes, falling back to measured bucket sizes, plus the query stage's gold size when rounds ran) over the window; efficiency is the streams' input over their core-hours |
| `total_s3_objects` | count | none | objects across the three buckets at the window's end, before the streams stop; unbounded growth means maintenance is not keeping up. Not on batch records |
| `total_core_hours` | core-h | none | core-hours of the streams, which scale with the window (config-bound) |

In-stream rounds:

- Rounds run every `benchmark_interval` after `benchmark_warmup` (both at
  least `gold_refresh_interval`), 1 sample per query.
- Q9 is retried twice, after 30 s and 60 s.
- A round's QpH is over the queries that succeeded in it. After a successful
  Q9 retry it is recomputed over every query in the round, failed ones
  included.

Each round records the query set it executed; a round with a tolerated Q9
failure executed a 7-query set (`qs7-...`). When the rounds behind the median
executed different sets, `query_set_id` reads `blended`,
`composite_qph_basis.blended` is true, `composite_qph_by_set` gives the median
per set, and the median is not comparable with another run's. A published
continuous `composite_qph` must carry `composite_qph_basis`.

### 8.3 Both modes

| Metric | Unit | Direction | Definition |
|---|---|---|---|
| `snapshots_expired`, `orphan_files_removed`, `storage_reclaimed_mb` | count, count, MB | none | reserved for what table maintenance removed; no code path fills them in this release, so they never appear in a record |
| `storage_multiple_total` | ratio | lower, diagnostic (never a directional delta) | physical over logical table bytes at run end under the maintenance policy that ran (`storage_multiple.total.multiple`); a condition of the policy, not a system score |

`storage_multiple_total` is shown by the report and read by nothing that
gates. None of these is under `scores`. Per-stage and per-query figures
(`<stage>_seconds`, `query_qph_<query>`) are recorded beside these and follow the stage and query definitions above.
