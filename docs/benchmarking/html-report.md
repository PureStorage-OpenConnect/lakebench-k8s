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

**Read this first.** The panel under the header comes before any metric. In order:

- **Verdict** and **headline**. The verdict is the stricter of the stored one and one recomputed today; a record is never promoted. A difference is shown, for example "FAILED (stored PASSED; recomputed FAILED)". The headline is a failed run's first reason, the first warning, or for a clean pass the workload, mode, scale, rules run and n.
- **Evidence class**, read only from the [registered-look](../glossary.md#look) file (`src/lakebench/spark/data/aml/aml_registered_looks.json`, AML evaluation runs recorded in advance), never the config.
  - A completed look whose `run_ids` names this run: "registered look: <role>", with the first 12 characters of its report sha256.
  - An AML calibration corpus: "development (calibration corpus: in-sample ...)": the numbers describe that corpus only, not held-out data.
  - Every other run, or a missing or unreadable file: "development".
- **Corpus**, **support state**, **binding caps** (or "none"), **n** (runs, samples per query), **provenance** (version, commit, dirty tree), identity **digest**.
- Qualifiers: rules skipped on a cap, layers without row counts, Customer 360 checks failed outside the gating set.
- Limits: n=1, rules skipped or errored (and the rules continuous AML does not run), AML recall uncalibrated and in-sample, a dirty tree.

`lakebench report` prints the same front matter before its scores; `run` prints it after saving (not for `--local`).

The delivered `report.html` predates any look naming the run, so it reads "development"; `lakebench report --render` afterwards reads the look.

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
- Continuous: recall over covered instances with coverage beside it, and chance and off-target over them too ([aml-scoring.md](../aml-scoring.md#continuous-recall-over-covered-instances)); otherwise why recall was not scored. Never plain "recall".
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

## Storage multiple

Physical bytes over logical bytes, per table, per layer and in total, measured once at run end: after the post-maintenance round (batch), or after the settle wait (continuous; AML after the gold-refresh [drain](../glossary.md#drain), stream stop and score job). It is a condition of the maintenance policy stated with it, not a system score (`storage_multiple_total` is diagnostic).

- **Physical**: object bytes under each table's location, one listing per bucket. **Logical**: current snapshot data files (Iceberg `$files`, Delta `DESCRIBE DETAIL`).
- Physical splits into current data, retained-snapshot data (files only an older retained snapshot references; Spark Thrift `all_files`, Trino `$all_entries`), metadata (`metadata/` or `_delta_log/`) and other. Without a retained split (Delta, Trino without `$all_entries`), "retained and unreferenced" is one figure. When orphan removal ran, the unreferenced share is labelled as bounded by its 24 h 10 min floor.
- Excluded, with bytes: stream checkpoints (any `checkpoints/` segment, and directories under `sustained.checkpoint_base`), datagen markers (`_corpus/`) and manifest, `<gold>/scoring/`, `<gold>/_ml_loop/`.
- Named without bytes (not in object storage): executor scratch PVCs, the dependency server's `lb-deps` PVC.
- Raw datagen files in bronze: physical only, outside the total. Continuous adds their growth per datagen hour and the space a 24-hour run needs.
- Incomplete multipart uploads are not listed or counted.
- "Not measured", with reason, when:
  - the catalog does not know the table
  - its files are registered in place outside its location (AML batch bronze, `add_files`)
  - the engine cannot read its data size (Delta on Trino)
  - physical bytes are below referenced bytes
- Customer 360 batch bronze is the raw corpus, not a table.
- Trino skips retained-snapshot bytes past 500 snapshots (`$all_entries` is expensive on the coordinator).
- An engine that cannot read table metadata (DuckDB, `none`): physical bytes per bucket only, with the reason.
- Listings and queries share a 10-minute budget; a failed bucket leaves its tables not measured, and the total says how many tables it covers.
- A run that did not pass, and a `run --stage` run (other layers' tables are an earlier run's), record not measured.

Record field `storage_multiple`:

- `buckets`: per bucket, name, layers served, physical bytes (every listed object), unattributed bytes, and the listing error (byte figures then null).
- Unattributed: objects under no exclusion, raw datagen prefix or measured table location; a table the catalog does not know lands here.
- A bucket name is a value, never a key, so a record can be scrubbed into a test fixture.
