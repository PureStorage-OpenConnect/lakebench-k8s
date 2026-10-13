# HTML report layout

Reference: what each badge, panel, card and label in `report.html` means.

## Header

Deployment name, run id and a status badge:

| Badge | Meaning |
|---|---|
| **PASSED** (green) | Pipeline completed; batch scale ratio and continuous ingest ratio at least 0.95; all jobs succeeded; no failed queries; rows in every layer, the expected rules and non-empty answers ([What a PASSED verdict asserts](verdict.md#what-a-passed-verdict-asserts)). |
| **WARNING** (amber) | Completed with a non-fatal flag, for example a batch scale ratio above 1.05 (more data than the scale asks for). |
| **FAILED** (red) | A stage or query failed, or data completeness is below threshold. A run stopped by Ctrl-C or SIGTERM shows failed with the interrupt as reason; its verdict is INTERRUPTED ([Interrupting a run](../cli-reference.md#run)). |

A one-line banner shows pipeline mode, Customer 360 scale factor, the recipe (`catalog-format-engine-query_engine`) and wall-clock duration.

**Run record.** The panel at the bottom of the page, after every section. In order:

- **Verdict** and **headline**. The verdict is the stricter of the stored one and one recomputed today; a record is never promoted. A difference is shown, for example "FAILED (stored PASSED; recomputed FAILED)". The headline is a failed run's first reason, the first warning, or for a clean pass the workload, mode, scale, rules run and n.
- **Evidence class**: "development" for standard runs; registered evaluation runs show their role.
- **Corpus**, **support state**, **binding caps** (or "none"), **n** (runs, samples per query), **provenance** (version, commit, dirty tree), identity **digest**.
- Qualifiers: rules skipped on a cap, layers without row counts, Customer 360 checks failed outside the gating set.
- Limits: n=1, rules skipped or errored (and the rules continuous AML does not run), AML recall uncalibrated and in-sample, a dirty tree.

`lakebench report` prints the same record before its scores; `run` prints it after saving (not for `--local`).

The delivered `report.html` is written before any look can name the run, so it reads "development"; `lakebench report --render` afterwards reads the look.

## Views

A batch run of Customer 360 or AML opens on two tabs: **Workload replay** and **Scorecard**. The Scorecard is the page described below. A run with no replay (continuous mode) shows the Scorecard alone.

The replay plays the recorded timeline back, with play and pause, 1x/2x/4x speed, restart, a scrubber and a click on any stage:

- **Workload**: the five stages with their recorded row counts (datagen, bronze, silver, gold) and the number of successful benchmark queries, and the Spark jobs with their recorded durations.
- **Pipeline**: one bar per handoff, filling over the downstream stage's recorded window.
- **Platform**: the storage lane from the record's [`io` block](records.md#storage-io), MB/s read and written. Compute, Network and Kubernetes are listed as not measured.
- **Recorded** is a stage or job window, a row count, a byte count. **Derived** is the running count and the handoff bars, which grow over a recorded window because the record holds the window and the final figure, not the figure in between. Nothing is simulated.
- The page embeds its data as one JSON block, checked against the stored record by a test. Where the record has no window (datagen) or no figure (Spark stage bytes), the replay says so rather than drawing a curve.

## Scorecard tiles

The Scorecard tab opens on eight tiles, then the sections below; the run record closes the page. Each tile cites the record field it reads (hover it). A field the record does not hold shows NR with the reason, never zero.

| Tile | Batch | Continuous |
|---|---|---|
| 1 | Pace: the workload's unit of operation per second to gold ready. Customer 360 counts interactions cleaned (silver `output_rows`), AML transactions screened (gold `input_rows`), over `scores.time_to_value_seconds` (derived). A Count / Bytes switch on the tile shows the same stage's `input_size_gb` per second, stepping KiB to TiB | Pace, rows/s entering bronze (`scores.sustained_throughput_rps`), and whether the pipeline kept pace (`scores.pipeline_saturated`) |
| 2 | Time to value (`scores.time_to_value_seconds`) | Freshness (`scores.data_freshness_seconds`) |
| 3 | Query rate (`scores.composite_qph`) | Query rate (`scores.in_stream_composite_qph`) |
| 4 | Compute, core-hours (`scores.total_core_hours`) | same |
| 5 | Efficiency (`scores.compute_efficiency_gb_per_core_hour`) | same |
| 6 | Storage: Trino bytes read, from the queries' `io` fields; Spark stages are not measured | same |
| 7 | Gates: passed over total (`verdict.gates`, recomputed as the page's verdict is) | same |
| 8 | Support (`experiment.support.state`) | same |

`scores` is `pipeline_benchmark.scores`, read as the run stored it, not recomputed. The Summary cards below the tiles remain.

## Summary cards

| Mode | Cards |
|---|---|
| Batch | Time-to-Value, Data Processed (GB), Pipeline Throughput (GB/s), QpH, Job Status (pass/fail count) |
| Continuous | Data Freshness, Balance, Continuous Throughput (rows/s), Compute Efficiency (GB/core-hour), In-Stream QpH (median), Total CPU-hours |

- Balance reads balanced, not balanced or not measured. It names the bottleneck, each handoff's second-half lag growth against its allowance and, when datagen fell short, the MB/s offered against what the stages were sized for.
- Throughput and efficiency use stage inputs; the card shows the bronze corpus size beside that total (continuous: the bronze bucket at run end, landing files and table together).
- Not PASSED: no headline number; the page leads with the first actionable reason and failed jobs' errors, and cards read "-".
- The QpH tag counts runs separately from repetition: `n=1 run, 3 samples/query` (batch), `n=1 run, 4 rounds` (continuous). Samples and rounds are never counted as runs.

## Sections

| Section | Mode | Contents |
|---|---|---|
| Bottleneck Identification | both | Stacked bar of each stage's share of requested core-seconds (executors x cores x seconds; Trino pod cores x seconds for a Trino query stage). Bronze amber, silver indigo, gold gold, query cyan. |
| Bottleneck Identification table | both | Adds share of stage time (batch) or micro-batch latency (continuous). Spark Thrift and DuckDB query stages (no cores) and the continuous query stage (no latency) are left out. |
| Data Validity | both | Scale Ratio (batch): red below 0.95, amber above 1.05 (not shown as "Complete"). Ingest Ratio (continuous): red below 0.95 (amber when the [trickle](../glossary.md#trickle) held intake), amber above 1.05 (for example files from an earlier run). Job Success: passed and failed counts. Any red makes cross-run comparison unreliable. |
| Stability Over Time | continuous | QpH across rounds with the recorded `qph_degradation_pct`. The page computes no trend. |
| Table Maintenance | both | Policy, file counts and QpH before and after, each with its round's query count. The change is paired over queries both rounds ran ("QpH change, paired over 8 queries"); with different sets it says the unpaired figures are not the maintenance effect. |
| Q9 Contention | continuous | When Q9 collided with a gold rewrite and whether retries were needed. |
| Batch Job Performance | batch | Per Spark job: name, status, elapsed, input/output, throughput, executors, cores, total CPU seconds. |
| Continuous Pipeline | continuous | Per stream: type, status, rows, rows/s, freshness, executors, compute. |
| Pipeline Stages | both | Per-stage matrix. Batch: GB and rows in/out, GB/s, rows/s. Continuous: rows/s, micro-batch latency, freshness. |
| Query Performance | both | Benchmark table titled with the engine ("Trino query benchmark"): query, display name, category, elapsed, rows, pass/fail; summary row with mode, streams and QpH. |
| In-Stream Benchmark Rounds | continuous | Queries as rows, rounds as columns, with median, min, max. Each round header: QpH, gold freshness, contention. |
| Resources as run | both | Per job: executors as recorded (batch: every executor seen, replacements included, else the profile's count; continuous: the count submitted), cores and memory from the profile, scratch PVC size and class (`provenance.scratch_as_ran`; pre-1.7: "not recorded"). |
| Configuration | both | Scale, S3 endpoint, catalog, table format, query engine. |
| Platform Metrics | with observability | Per-stage CPU and memory average/max and pod counts; infrastructure pods (Hive, Polaris, Trino, Postgres) apart. Pods sum their containers only (no pod cgroup or pause container), a container scraped twice counts once, and max columns sum per-pod peaks (not a concurrent peak). Older records say they double-count. |

**Continuous intake cards**, after Pipeline Stages:

- Ingest ratio: bronze rows over released rows, or over corpus rows without a released count.
- Corpus coverage (`corpus_ingest_ratio`).
- The window.
- The trickle rate when set (a Lakebench cap, not a capacity). A default run has none, so it reads "not recorded"; the offered load is in `config_snapshot.offered_load`.
- AML: rules continuous mode does not run are named, and the detection table reads "excluded in continuous mode" for them, not "no data".

## AML results

The funnel, each step with its record path:

- Rule alerts (from scoring, and in `gold.alerts` as transaction monitoring read them).
- Alerts dispositioned on customers (of which over the per-customer cap, and withdrawn alerts carried from an earlier cycle) and on non-customers (of which not declared as counterparties).
- Customer alerts per customer, dispositions, escalations, alert cases, continuing-activity review cases, SARs filed.

A reconciliation checks, and sizes any unexplained difference in:

- scoring and TM alert totals agree;
- TM alerts = customer + non-customer dispositions - withdrawn carried alerts;
- dispositions sum to the customer plus non-customer count;
- SARs filed = alert-case SARs + continuing-activity SARs.

Detection table:

- Recall reads "uncalibrated, in-sample" unless a completed registered look names the run. Random-control chance sits beside it.
- Continuous: recall over covered instances with coverage beside it, and chance and off-target over them too ([AML spec 8.4](../benchmarks/aml/scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails)); otherwise why recall was not scored. Never plain "recall".
- Total alerts and off-target rate cover only rules that ran, labelled BOUNDED BY when a rule was skipped on a cap. Funnel totals carry the same label. Counts after the per-customer cap (escalated, alert cases, SARs filed) carry that cap when it held alerts back.
- A per-reason-code table when recorded; otherwise the producer's status or "reason codes not recorded".
- Leakage reads "not measured in this run": the AML fidelity gate (`scripts/aml_gate.py`) runs outside `lakebench run`.
- The planted-subject customer check.
- Unrenderable scoring or detection data shows "AML results could not be rendered" with the error.

## Expected results (Customer 360)

The `c360_correctness` checks ([gating rules](scorecard.md#customer-360-expected-results)):

- A chip: checks passed out of those recorded (unchecked included), and the gate as applied now (`GATING_CHECKS`): "N gating checks passed", or "fails the run" with the reason.
- Gating checks are tagged; one absent from the record is "not evaluated", since it fails the run.
- Checks not passed come first (failing the run, other failures, unchecked) with observed, expected and tolerance. Passed checks follow, collapsed: pipeline (invariant and reconcile), benchmark shapes, statistical.
- A note stored with an older record (for example "reporting only", from before the gating checks were approved) is shown as recorded, not as the current rule.

## Storage I/O

Bytes the engines reported reading from and writing to storage, from the record's [`io` block](records.md#storage-io); it sits before Storage multiple.

- A table with one row per source and round: Trino queries (the benchmark, the round before maintenance, each in-stream round) and datagen. Each row gives read and written bytes, the window (UTC, the engine's clock) and the basis of each figure; each total names its record field, so it can be checked against `metrics.json`.
- A rate chart of MB/s read and written per time bucket. Each interval's bytes are spread evenly over its window, so a bucket is no finer than the queries feeding it. Bytes with no recorded window (datagen) are listed under the chart as not on it.
- "Not measured" is stated, never shown as zero: Spark stages (executor I/O is not collected), a query engine other than Trino, and a Trino round whose record predates the block. "n/a" marks a direction a source does not have (datagen reads nothing). A figure that was measured as zero reads `0.00 MiB`.
- Pod network traffic and storage-array telemetry are not part of it, and bytes from different sources are not added together.

## Storage multiple

Physical bytes over logical bytes, per table, per layer and in total, measured once at run end: after the post-maintenance round (batch), or after the streams stop (continuous; AML after the gold-refresh [drain](../glossary.md#drain), stream stop and score job). It is a condition of the maintenance policy stated with it, not a system score; `storage_multiple_total` is diagnostic.

- **Physical**: object bytes under each table's location, one listing per bucket. **Logical**: current snapshot data files (Iceberg `$files`, Delta `DESCRIBE DETAIL`).
- Physical splits into current data, retained-snapshot data, metadata and other. When orphan removal ran, the unreferenced share is labelled as bounded by its 24 h 10 min floor.
- Excluded, with bytes: stream checkpoints, datagen markers and manifest, scoring output. Raw datagen files in bronze are physical only, outside the total.
- "Not measured", with reason, when the catalog does not know the table, its files are registered in place, or the engine cannot read its data size.
- A run that did not pass, and a `run --stage` run, record not measured.

Record field `storage_multiple`:

- `buckets`: per bucket, name, layers served, physical bytes, unattributed bytes, and the listing error.
- A bucket name is a value, never a key, so a record can be scrubbed into a test fixture.
