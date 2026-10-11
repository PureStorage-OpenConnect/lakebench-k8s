# Continuous scores

Reference: the measurement window, continuous scores and formulas, offered load, capacity and steady-state runs, intake limits and throughput bounded by `max_files_per_trigger` (the [trickle](../glossary.md#trickle)).

Gate and result check: [verdict.md](verdict.md). Tuning: [continuous-tuning.md](continuous-tuning.md).

## Measurement window

- Freshness, throughput and row counts are measured inside it. Opens at datagen's first data file in bronze, after every stream is running (after 300 s with a warning when no file arrives). With `--skip-generate`, when every stream is running.
- Closes `run_duration` seconds later.
- Kept in cluster time (host clock plus the API server offset), matching pod logs. Start, end and offset: `continuous.window`.
- Rows a stream took before the window opened (it started while another retried its submission) are `pre_window_rows`, in no window score.
- `ingest_ratio` and `corpus_drained` count every row up to the window's end, pre-window rows included.
- `pipeline_throughput_gb_per_second` and `compute_efficiency_gb_per_core_hour` divide bucket sizes at the window's end by the window.

**Changed definitions.** Do not compare across these:

- Before 1.6 (2026-09-26), throughput, freshness, `corpus_drained` and `total_rows_processed` counted outside the window. `sustained_throughput_rps` was bronze rows / `run_duration`, pre-window rows included; `corpus_drained` also needed two idle gold cycles.
- 1.7.1: gold freshness counts from file landing (bronze `ingest_ts`), adding the datagen-to-bronze and bronze-to-silver lag; stages run back to back by default; `arrival_seconds`, `ingest_ratio` and silver's kept-pace check use the stage's cadence (trigger interval, or its median batch time). AML identity catches this (`aml-3`); Customer 360 (`c360-2.dev1`) does not.

## Scores

| Score | Formula | Meaning |
|---|---|---|
| `data_freshness_seconds` | max gold cycle freshness inside the window | Primary score; lower is better. See [Freshness](#freshness). Never a gate. Above half the run's duration it adds a warning. |
| `sustained_throughput_rps` | bronze rows inside the window / `arrival_seconds` | Unique rows/s entering bronze while data arrived; gold's re-reads excluded. Higher is better. With `intake_limit: trickle_rate`, the offered load, not a capacity. |
| `arrival_seconds` | whole window while corpus was left, else bronze's last write + one bronze cadence | Throughput is never averaged over idle time after the corpus ran out. |
| `window_seconds` | window end - window start | Window length. |
| `window_arrival_fraction` | `arrival_seconds / window_seconds` | Below 1: the corpus ran out inside the window. |
| `stage_latency_profile` | `[bronze_ms, silver_ms, gold_ms]` | Per-stage micro-batch latency. The much higher stage is the bottleneck. |
| `ingest_ratio` | `bronze_rows / released_rows` | Share of released rows bronze took by the window's end; 1.0 = kept up. A little above 1 is estimate error. Falls back to `corpus_ingest_ratio` without a corpus file count. |
| `corpus_ingest_ratio` | `bronze_rows / datagen_rows` | Share of everything datagen wrote. Own datagen: set by the backlog. `--skip-generate`: set by the trickle. Not a saturation signal. |
| `pipeline_saturated` | `ingest_ratio < 0.95`, unless `intake_limit` is `trickle_rate` and silver kept up | Bronze fell behind and the pipeline did not keep pace. Null when not measurable. |
| `intake_limit` | bronze trigger count, batch time, busy share | Why `ingest_ratio < 0.95`. See [Intake limits](#intake-limits). `none` at 0.95 or above. |
| `pace_seconds_per_million_rows` | `window_seconds / (silver window rows / 1e6)` | End-to-end pace; lower is better. A silver batch straddling a window edge counts by its run-time share inside. A capacity when `datagen_ahead` and `intake_limit: bronze_capacity`; else the arrival rate. |
| `bronze_pace_seconds_per_million_rows` | `window_seconds / (bronze window rows / 1e6)` | Bronze's pace, same terms. |
| `backlog_rows` | `datagen_rows - bronze_rows` | Rows datagen wrote by the window's end that bronze had not taken. |
| `datagen_ahead` | `bronze_rows < 0.95 x datagen_rows` | True: capacity run. False: steady state. Absent with `--skip-generate`. |
| `corpus_drain_seconds` | `datagen_rows / sustained_throughput_rps` | With `intake_limit: trickle_rate`: the window that would drain the corpus. |
| `compute_efficiency_gb_per_core_hour` | `total_data_processed_gb / total_core_hours` | Per core-hour of requested stream executors. Input: bronze's and silver's window rows at datagen's raw bytes per row; gold re-reads not counted. "not measured" without a datagen fleet record. Core hours follow the window. |
| `total_rows_processed` | rows each stage took inside the window (gold: its re-reads of silver) | Not distinct rows; distinct volume is bronze's and silver's window rows. |
| `total_s3_objects` | `sum(bucket_object_count)` | Objects in the three buckets at run end. Growth faster than retention slows metadata operations. |
| `qph_degradation_pct` | second-half vs first-half median QpH | Positive = slower. Needs 4+ rounds. Withheld when rounds ran different sets (`scores.qph_degradation_withheld`). |
| `composite_qph` | median in-stream QpH | Else the post-stream benchmark's (`composite_qph_rounds` 0). See [In-stream QpH basis](query-benchmark.md#in-stream-qph-basis). |

**`released_rows`** (an estimate at the mean rate):

- Trickle: `max_files_per_trigger` files per bronze trigger since bronze's first write, at mean rows per file (datagen rows / files), capped at the corpus.
- Own datagen: rows datagen had written one bronze cadence before the window's end, at its mean rate. Datagen's total also holds rows written after the window, before pods saw the stop marker.
- A run whose last batch was in flight can read up to one cycle short (0.983 at an 1800 s window and 30 s cycle).

## Freshness

- Each gold refresh measures, after its write, the age of the newest row it included, from that row's file landing (bronze `ingest_ts`). It is the handoff lags (datagen to bronze, bronze to silver, silver to gold) plus gold's read and write time.
- A configured `gold_refresh_interval` is not in it; it is labelled beside it (`experiment.limits.trigger_bound`). Readers see data up to one refresh interval older.
- With `--skip-generate`, a file written before bronze started counts from when bronze took it. `continuous.freshness.from` says which.
- `continuous.freshness`: `p50_s`, `p95_s`, `max_s` over cycles that saw new data, `cycles`, `from`, `store_clock_offset_s`.
- The balance gate, not freshness, decides whether gold kept up ([verdict.md](verdict.md#continuous-gate)).

## Offered load and regimes

Scale sets the offered load: AML 4 MB/s and Customer 360 10 MB/s of datagen files per scale unit, on every system, so runs at one scale are comparable. Sizing: [running-pipelines.md](../running-pipelines.md#datagen-and-offered-load).

- Datagen writes flat out for the whole window; the run stops it with the `_corpus/stop` marker.
- Each successive AML 24-month period has its own answer key.
- Bronze reads with no per-trigger limit. The run's rate is in its datagen fleet record.

| Regime | Record | Reading |
|---|---|---|
| Capacity | `datagen_ahead: true`, `intake_limit: bronze_capacity` | The paces are the pipeline's capacity; the backlog is not a failure. Freshness shows how far behind it fell. |
| Steady state | `datagen_ahead: false` | Freshness is the score; paces are the arrival rate. Use fewer datagen pods or less CPU per pod. |

- A backlog with bronze idle (`below_bronze_capacity`) is a stall or late start, and fails the run.
- A configured `max_files_per_trigger` is a Lakebench cap on intake, labelled as one.
- `--skip-generate`: bronze reads a finite corpus under the trickle; unset, the run derives the most files per trigger, up to 50, whose arrival lasts 1.2 x `run_duration`.

## Intake limits

When `ingest_ratio` is short, `intake_limit` says why:

- `trickle_rate`: bronze ran a micro-batch on at least 90% of the window's triggers and on all but 5% (at least one) between its first and last batch, each inside the trigger, with corpus left. The trickle bounded intake.
  - Silver also kept up (committed all but two silver cadences and one bronze cadence of bronze's rows and, on an interval, each batch inside it): not saturated; a report warning, and `corpus_drain_seconds`.
  - Silver logged batches but no commit: stuck, saturated. Silver logged nothing: `pipeline_saturated` null, with a warning.
- `bronze_capacity`: bronze ran back to back; rows/s is its capacity.
- `below_bronze_capacity`: bronze idle part of the window without keeping its trigger: a late start or stall (see the driver log).

Saturated: add executors to the stage that fell behind. More load: raise the scale, or set `workload.datagen.cpu` and `parallelism` and size the streams for it. Larger corpus at the same load: lengthen the window.

## Trickle-bound throughput

With `max_files_per_trigger` set (the trickle) and the pipeline keeping pace, or `intake_limit: trickle_rate`, throughput is the offered load, not capacity.

- **Kept pace:** `ingest_ratio` (offered = `released_rows`) at least 0.99, and lag at window end (window seconds minus bronze's last write) at most one trigger interval. Lag is not tested when bronze took the whole corpus.
- Shortfall within one cycle's batch and lag within one cycle, or an input not recorded: `kept_pace` null, the line says "the pipeline was not shown to keep pace", and throughput is still not shown as a capacity.
- `experiment.limits.trickle_bound`: `{kind, value, source, kept_pace, ratio, lag_s, trigger_s, offered_rows, ingested_rows}`, plus `not_measured` (null `kept_pace`) and `lag_note` (lag not tested). Null when the trickle did not hold intake.
- `limits.bound` line: `trickle: max_files_per_trigger N (auto), the pipeline kept pace`. Not in `limits.bound_kinds`, so not in the experiment identity.
- Capped rows (`capped_by` names the trickle): `sustained_throughput_rps`, `pipeline_throughput_gb_per_second`, compute efficiency, `corpus_drain_seconds`.
- The report labels those rows/s (headline card, Pipeline Stages summary, bronze stream and stage rows), GB/s and efficiency figures "BOUNDED BY: trickle (offered load, not capacity)". Tooltip: "trickle N files per trigger; this is the offered load, not infrastructure capacity".
- Pre-1.7 records get this when read.

AML continuous recall is scored over covered instances: planted cases whose every transaction and participant reached the scored [tick](../glossary.md#tick)'s tables. See [aml-scoring.md](../aml-scoring.md#continuous-recall-over-covered-instances).
