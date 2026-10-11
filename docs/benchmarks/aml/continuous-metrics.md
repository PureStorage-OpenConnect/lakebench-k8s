# AML benchmark: continuous metrics

## 8.2 Continuous

Primary: `data_freshness_seconds`, lower is better.

| Metric | Unit | Direction | Definition |
|---|---|---|---|
| `data_freshness_seconds` | s | lower | the largest gold freshness sampled when a refresh completes (readers see data up to one refresh interval older): the maximum per-stage freshness, active freshness once the corpus drained; null when unmeasured |
| `data_freshness_seconds`, recorded | s | -- | 202 s (1.6 continuous, scale 1, n=1), under the definition used through 1.7.0 (from silver, not file landing; not comparable) |
| `time_to_detect_seconds`, `time_to_detect_p95_seconds`, `time_to_detect_max_seconds` | s | lower | median, 95th percentile and maximum time to detect (below), reported per rule. AML only. Recorded median under the definition used through 1.7.0 (before file landing; not comparable): 220 s (1.6 continuous, scale 1, n=1) |
| `time_to_detect_alerts`, `time_to_detect_late_alerts`, `time_to_detect_unmeasured_cycles` | count | none | alerts measured; alerts whose payments were all in silver before the previous pass of their rule (in the percentiles); gold cycles that logged no time-to-detect line |
| `sustained_throughput_rps` | rows/s | higher | bronze rows ingested inside the window / `arrival_seconds`; under a [trickle](../../glossary.md#trickle)-bound intake, the offered load the trickle set (a Lakebench cap), not a capacity |
| `window_seconds` | s | none | measured window length; follows `run_duration` |
| `arrival_seconds` | s | none | seconds of the window data was still arriving at bronze |
| `window_arrival_fraction` | ratio | none | `arrival_seconds / window_seconds` |
| `pre_window_rows` | rows | none | bronze rows ingested before the window; in no window score |
| `released_rows` | rows | none | own datagen: rows datagen had written one bronze cadence before the window's end, at its mean rate. Trickle: rows the trickle had made available by the window's end |
| `backlog_rows` | rows | none | datagen's rows bronze had not taken at the window's end |
| `datagen_ahead` | bool | none | bronze took under 0.95 of datagen's rows; true marks a capacity run |
| `pace_seconds_per_million_rows` | s/M rows | lower | window seconds per million rows through silver (end to end); a silver batch straddling the window's start or end counts by the share of its run time inside it |
| `bronze_pace_seconds_per_million_rows` | s/M rows | lower | window seconds per million rows bronze took |
| `ingest_ratio` | ratio | target, guard | estimate: bronze rows by the window's end / `released_rows` (falls back to `corpus_ingest_ratio`); should sit in 0.95 to 1.05 |
| `corpus_ingest_ratio` | ratio | none | bronze rows / datagen rows. Recorded under a trickle of 1 file per trigger, which a default run no longer sets: 0.4374 in a 1,800 s window (1.6 continuous, scale 1, n=1) |
| `pipeline_saturated` | bool | none | `ingest_ratio < 0.95`; false when the trickle, not the pipeline, bounded intake |
| `intake_limit` | text | none | `none`, `trickle_rate`, `bronze_capacity`, `below_bronze_capacity` ([7.4](execution-rules.md#74-lakebench-imposed-caps)) |
| `bronze_busy_fraction` | ratio | none | share of the window bronze spent in micro-batches |
| `corpus_drain_seconds` | s | none | seconds the trickle needs to ingest the whole corpus at the rate it held |
| `corpus_drained` | bool | none | every datagen row reached bronze and silver committed it before the window ended |
| `stage_latency_profile` | struct | none | mean micro-batch time per stream (`bronze_ms`, `silver_ms`, `gold_ms`) |
| `total_rows_processed` | rows | none | rows taken in inside the window across streams, gold's re-reads of silver included: not distinct rows |
| `composite_qph`, `in_stream_composite_qph` | QpH | higher | median QpH of in-stream rounds that ran the full 12-query set; 8-query rounds before the first case are only in `composite_qph_by_set`. Null when no full-set round measured one; the single post-stream benchmark when no round ran |
| `composite_qph_rounds`, `benchmark_rounds_count` | count | none | rounds behind the median (0 when `composite_qph` is the post-stream benchmark), and rounds executed |
| `composite_qph_basis`, `composite_qph_by_set` | struct | none | whether the median blends rounds over different query sets, rounds per set, median per set |
| `qph_degradation_pct` | pct | lower | QpH change from the first to the second half of the rounds (positive is slower), with 4 or more rounds |
| `qph_degradation_withheld` | text | none | why `qph_degradation_pct` is absent with 4 or more rounds: the rounds ran different query sets |
| `query_time_event_age_seconds` | s | none | not in the scorecard (`pipeline_benchmark.diagnostics`): median age of gold's newest event date at query time; tracks the corpus's position in time, not freshness |
| `total_elapsed_seconds` | s | none | the run's wall-clock seconds |
| `pipeline_throughput_gb_per_second`, `compute_efficiency_gb_per_core_hour` | GB/s, GB/core-h | higher | data processed over the window; the streams' input over their core-hours |
| `total_core_hours` | core-h | none | core-hours of the three streams, which run the whole window, so it follows the window length |

`qph_degradation_pct` needs four rounds to read as a trend; a typical run
produces five. Read a five-round value as a signal, not a conclusion.

With TM on, a round adds IQ1 to IQ4 once the run has a case, so rounds can
run different query sets. `composite_qph_basis.blended` then says so,
`composite_qph_by_set` gives the median per set, `qph_degradation_pct` is
withheld, and the round median is not comparable with another run's. A
published continuous `composite_qph` must carry `composite_qph_basis`.

**Time to detect** (`spark/scripts/gold_refresh_financial.py`,
`metrics/collector.py`). Batch runs report none; AML continuous runs always
carry the keys, null when nothing was measured.

- An alert is new on a [tick](../../glossary.md#tick) when its (rule, entity, sorted related payment
  ids) was not in `gold.alerts` at the snapshot before the tick. An alert
  whose content changes is new.
- Time to detect = the commit of its rule's INSERT into `gold.alerts` minus
  the newest bronze `ingest_ts` among its related payments.
- That spans from the last evidence's file landing in the raw zone to the
  alert being visible in gold. It includes the wait for bronze. For a corpus
  written before the run, landing is when bronze took the file.
- Every rule runs every tick.
- `detected_ts` is the first pass that wrote the alert's content.
- Each tick logs a histogram in 10 s bins. The collector merges every tick;
  the median and 95th percentile are read at the upper edge of the bin that
  reaches the quantile, capped at the measured maximum.
- Late alerts come from a re-raise after a rule error, or from evidence
  outside `related_txn_ids`. They stay in the percentiles, so they can only
  lengthen them.
- A W4 alert covers every matched pair of its account, so each pass that adds
  a pair raises it again; `time_to_detect_alerts` counts those re-raises.
- The lookup of each new alert's evidence reads all of silver's
  transactions: the one part of a tick that grows with the run (the tick's
  `ttd` phase).
- A tick that logged no measurement counts in
  `time_to_detect_unmeasured_cycles`; its alerts are measured one tick late.
- Ticks also log a pass-end histogram (every new alert measured at the end of
  the pass, the definition before per-rule commit times).
- They log one histogram per rule, kept with the gold-refresh stage metrics,
  so a rule that got slower is not hidden in the merged figure.

**Time-travel reads** (`continuous.time_travel`; reported, never gating).
Each tick records the `silver.transactions` snapshot it read, from snapshot
metadata only (no scan, so tick timings do not move).
`continuous.time_travel.ticks[]` holds:

- the driver start, the cycle, whether the tick completed;
- the snapshot id and its `committed_at` (UTC);
- from the Iceberg snapshot summary: `total_records` (record count of its
  live data files), `pos_deletes` and `eq_deletes` (deleted-row totals);
  with `count_source: "summary"`.

The delete totals are 0 on the copy-on-write tables Lakebench creates, when
`total_records` is the live row count. With no summary record count,
`total_records` is null and `count_source` is `"unavailable"`; the current
table's count never stands in. A tick with no transactions snapshot records
none.

After the score job of a run that passed its gates, `time-travel-financial`
(`spark/scripts/time_travel_financial.py`) reads those snapshots back, newest
first:

- A hash pass fingerprints each recorded snapshot still in the table over its
  business columns (`common.frame_fingerprint`: every column less `_batch_id`,
  `_stream_id`, `ingest_ts` and `committed_at`, listed in `hashed_columns`).
  It writes `scoring/<run_id>/tt_hashes.json` in the gold bucket, listing
  any snapshot it could not read.
- A read pass reads that file back, times a full scan `VERSION AS OF` each
  snapshot with the same fingerprint, and compares it with the tick's
  `total_records` (when a live-row count) and the hash pass.
- Each entry gains `state`, `read_s`, `rows`, `fp_match` and `count_match`.
  `expired_by` comes from `continuous.retention.rounds` (when each maintenance
  round ended, its applied expiry, its engine, and the tables whose
  `expire_snapshots` ran).
- The budget is a deadline on the cluster clock. A wait that ends before the
  job deletes the job.

Only ticks in the current gold-refresh driver pod's log are read. `verified`
shows that the snapshot id the tick read still holds the row count its
summary gave and reads identically twice. Snapshots are immutable, so the hash
comparison shows read determinism and an unaltered hashes file, not the
content the tick saw.

| Field | Unit | Definition |
|---|---|---|
| `ticks[].state` | text | one of the states in the next table |
| `ticks[].read_s` | s | the timed read-pass scan plus fingerprint (the snapshot's second read in the job); a snapshot several ticks read is read once per pass |
| `current_read_s` | s | the same scan of the current snapshot, for comparison only |
| `policy` | struct | configured retention and the expiry applied while streams ran (floored at 1 h), always stated |
| `budget` | struct | `budget_s`, the time-travel budget (a Lakebench cap), with its label: the per-job timeout less 120 s; the hash pass gets half |
| `budget`, scan rule | -- | after a pass's first scan, no scan starts unless the time left exceeds 1.5 times its longest scan. A pass's first scan always starts, so one scan longer than the budget ends the wait and reads `not_run` |
| `verdict` | text | `pass`, or the first that applies of the verdicts in the table after the states |

| State | Meaning |
|---|---|
| `verified` | scan rows equal the recorded count and the fingerprint equals the hash |
| `verified_hash_only` | recorded count is not a live-row count: no summary count, or delete files |
| `mismatch` | the read scan's rows, `fp` or column spec differ from the hash pass, or its rows differ from the tick's `total_records`; also when the tick recorded no snapshot id |
| `error` | a listed snapshot could not be read |
| `not_read` | the budget ran out |
| `expired` | expired by a Lakebench maintenance round, named in `expired_by` (below) |
| `missing_unexplained` | expired, and no Lakebench round could have |

`expired_by` is the earliest maintenance round that ran `expire_snapshots` on
the table and could have expired the snapshot.

- That is a snapshot committed before the round's latest possible cutoff
  minus its applied retention.
- The cutoff is the round's end moved to the cluster clock for Trino, and the
  round's start on this host's clock for Spark Thrift.
- It records the round, its end time on this host's clock, configured and
  applied retention, and the reason.

| Verdict | When |
|---|---|
| `fail` | any `mismatch`, `missing_unexplained`, `error` or `not_supported`; zero recorded snapshots; or no `verified` record (`verified_hash_only` alone compares nothing the tick recorded) |
| `incomplete` | the budget ran out |
| `not_run` | the job could not run, or the run failed its gates |
| `pass` | otherwise |

At the default 30 min retention, older tick snapshots are expected to expire
inside a 2 h window and the last ones to stay. Lakebench creates no tag or
branch, so expiry and destroy are unchanged. The check is shown beside the
verdict and never fails the run. No published record carries a time-travel
result yet.
