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
Each tick records the `silver.transactions` snapshot it read. After a passing
run, `time-travel-financial` reads those snapshots back and verifies that each
still holds the row count its summary gave and reads identically twice.
Snapshots are immutable, so the comparison shows read determinism, not the
content the tick saw. Snapshots expired by a Lakebench maintenance round are
attributed to the earliest round that could have expired them.

| State | Meaning |
|---|---|
| `verified` | scan rows equal the recorded count and the fingerprint equals the hash |
| `verified_hash_only` | recorded count is not a live-row count: no summary count, or delete files |
| `mismatch` | the read scan's rows, `fp` or column spec differ from the hash pass, or its rows differ from the tick's `total_records`; also when the tick recorded no snapshot id |
| `error` | a listed snapshot could not be read |
| `not_read` | the budget ran out |
| `expired` | expired by a Lakebench maintenance round, named in `expired_by` |
| `missing_unexplained` | expired, and no Lakebench round could have |

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
