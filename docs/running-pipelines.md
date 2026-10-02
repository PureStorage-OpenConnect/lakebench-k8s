# Running Pipelines

The `lakebench run` command executes the data pipeline: a sequence of Spark
jobs that transform raw bronze data into queryable gold tables, followed by
a query benchmark against the active engine. Metrics are automatically
collected at every stage and saved for reporting.

## Basic Usage

```bash
lakebench run my-config.yaml
```

This runs the full batch pipeline in order, waits for each stage to complete,
runs the query benchmark, and saves metrics. The output includes per-stage
timing, a QpH (queries per hour) score, and the path to the saved metrics
file.

## Batch Mode (Default)

The default pipeline executes four stages sequentially:

```
bronze-verify --> silver-build --> gold-finalize --> benchmark
```

### Stage 1: bronze-verify

Validates raw Parquet files in the bronze S3 bucket. Reads every file, checks
schema conformance, flags quality issues (nulls, format inconsistencies,
duplicates), and writes validation summary metrics. This stage is I/O-bound
and exercises the S3 read path.

**Job profile:** 2 cores per executor. Customer 360: 4g memory, 2g
overhead, 50Gi PVC. Financial (AML): 8g memory, 12g overhead, 500Gi PVC
(LB-118: financial trips the CTAS fallback above scale 5, which spills
roughly twice the per-executor input to local disk).

### Stage 2: silver-build

The core transformation stage. Reads validated bronze data and produces an
enriched Iceberg table in the silver bucket. Applies five transforms:
email normalization, phone normalization, geo enrichment, customer
segmentation, and quality flagging.

Silver-build selects a write strategy from the measured bronze data size.
These are silver-build write strategies, not pipeline modes:

| Strategy | Data Size | Description |
|---|---|---|
| **simple** | < 100 GB | Standard Spark processing. |
| **streaming** | >= 100 GB | Single-pass direct write with no shuffle (column transforms only); Iceberg hash distribution clusters rows by partition. |
| **salted** | override only | Retired: forcing it logs a no-op and runs `simple`. Never selected automatically. |

Automatic selection chooses only `simple` or `streaming`. To force a
strategy, set `spark.lb.silver.strategy` (or the `LB_SILVER_STRATEGY`
environment variable) to `simple` or `streaming` (`salted` runs `simple`).

**Job profile:** 4 cores, 48g memory, 12g overhead per executor, 300Gi PVC.

### Stage 3: gold-finalize

Aggregates the silver Iceberg table into daily KPI summaries for the executive
dashboard. Produces metrics like daily active customers, total revenue,
conversions, engagement scores, and churn risk counts. The output is a compact
gold-layer Iceberg table partitioned by date.

**Job profile:** 4 cores, 32g memory, 8g overhead per executor, 300Gi PVC.

### Stage 4: benchmark

Runs the workload's query set (8 queries for Customer 360, 12 for AML) on
the active query engine against the silver and gold tables and computes a QpH
(queries per hour) score. Queries span five categories:

| Category | Queries | Description |
|---|---|---|
| **scan** | Q1 | Full table scan with aggregation. I/O throughput bound. |
| **filter/prune** | Q2, Q4 | Date-range filtering and predicate pushdown with GROUP BY. |
| **aggregation** | Q3, Q7 | Hash aggregation, conditional SUM(CASE), conversion funnels. |
| **analytics** | Q5, Q6 | Window functions (MA7 revenue trend), CTEs (RFM scoring). |
| **operational** | Q9 | Gold-layer executive dashboard read with LAG window functions. |

The benchmark runs in power mode by default (single stream, hot cache). The
QpH score is `(number_of_queries / total_seconds) * 3600`.

## Multi-Cycle Batch

When `pipeline.cycles` is set to 2 or more, the batch pipeline runs N
iterations. Each cycle generates new data for its portion of the timestamp
range, then runs the full bronze-verify -> silver-build -> gold-finalize
sequence.

```yaml
architecture:
  pipeline:
    cycles: 3
    pre_benchmark_maintenance: true
```

Cycle 1 creates tables via full overwrite. Cycles 2+ run in incremental
mode -- silver appends new rows, gold merges based on a date watermark.
This simulates multi-day table growth where each cycle is a new "day" of
data.

The terminal output shows cycle headers (`Cycle 1/3`, `Cycle 2/3`, etc.)
and per-cycle timing. After all cycles complete, pre-benchmark maintenance
runs compaction and snapshot expiry (if enabled), then the benchmark
measures QpH against the accumulated table.

Metrics include a `cycles[]` array with per-cycle datagen timing, job
metrics, and table health (data file counts and snapshot counts from
Iceberg system tables). The `cycle_progression` score shows how elapsed
time and table state evolve across cycles.

## Command Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `--stage` | `-s` | (all) | Run a specific stage only: `bronze-verify`, `silver-build`, or `gold-finalize` |
| `--timeout` | `-t` | auto | Timeout per job in seconds. Defaults to `max(3600, scale * 120)` when omitted. The financial (AML) workload adds 900 s and never goes below the AML bronze-verify budget for the scale. |
| `--skip-benchmark` | | `false` | Skip the query benchmark after pipeline stages |
| `--continuous` | | `false` | Run in continuous mode. Overrides `pipeline.mode` in config. |
| `--duration` | | config value | Continuous run duration in seconds (continuous mode only) |
| `--generate` | | `false` | Run datagen before pipeline stages (batch mode only -- continuous mode always runs datagen automatically) |

### Examples

Run the full batch pipeline:

```bash
lakebench run my-config.yaml
```

Run only the silver-build stage:

```bash
lakebench run my-config.yaml --stage silver-build
```

Run with a longer per-job timeout (2 hours):

```bash
lakebench run my-config.yaml --timeout 7200
```

Run the pipeline without the query benchmark:

```bash
lakebench run my-config.yaml --skip-benchmark
```

Full end-to-end run including data generation:

```bash
lakebench run my-config.yaml --generate --timeout 7200
```

## Continuous Mode

Continuous mode runs the continuous pipeline instead of batch. Set it in the
config file or activate it with the `--continuous` CLI flag:

```yaml
# In your config YAML:
architecture:
  pipeline:
    mode: continuous              # batch | continuous
```

```bash
# Or as a one-off override:
lakebench run my-config.yaml --continuous
```

When `pipeline.mode` is set to `continuous` in the config, `lakebench run`
uses the continuous pipeline automatically -- no CLI flag needed. The
`--continuous` flag still works as an override for one-off runs.

In continuous mode, three Spark Structured Streaming jobs run concurrently:

```
bronze-ingest + silver-stream + gold-refresh  (concurrent)
```

- **bronze-ingest** -- Reads new Parquet files from S3 as they appear (via
  `maxFilesPerTrigger`), writes to a bronze Iceberg table.
- **silver-stream** -- Reads the bronze Iceberg table as a streaming source,
  applies silver transforms, writes to the silver Iceberg table.
- **gold-refresh** -- Periodically refreshes the gold aggregation table from
  the silver table.

Datagen starts with the streaming jobs and writes the scale's whole corpus
at full speed (about 2 minutes for 1 TB at scale 100). This is automatic -- no
`--generate` flag is needed. That flag only applies to batch mode.

Bronze reads that corpus as a trickle: at most `max_files_per_trigger` files
per `bronze_trigger_interval`. That rate is the offered load. By default it is
derived per run so the corpus keeps arriving for about 1.2 x the window
(c360 scale 1: 2 files per 30 s, about 2,400 s of arrival; scale 10: 22
files, about 2,190 s), capped at 50 files per 30 s (about 107 MB/s; a Lakebench-imposed cap, so
an auto-capped rate measures the cap, not the infrastructure). At the
cap, a 30-minute window takes about 19% of the scale-100 corpus. The larger
corpus is not a failure: a run the trickle held is recorded in
`experiment.limits.trickle_bound`, its throughput is labelled the offered
load, and the scorecard reads `intake_limit: none` (bronze kept up with what
was released). `intake_limit: trickle_rate` and `corpus_drain_seconds` appear
when the released rows are unknown or bronze took under 0.95 of them. See
[Scoring and Benchmarking](benchmarking.md#continuous-mode).

The measurement window opens when all three streams are running and lasts
the configured duration (default: 1800 seconds / 30 minutes). A stream whose
submission fails (for example a truncated Maven download) is reported on each
attempt while the Spark Operator retries it. During the window, Lakebench
runs periodic benchmark rounds to measure query performance while streaming
is active. After the window ends the continuous gate checks that data kept
arriving and that silver and gold committed continuously inside the window;
a run that passes lets the rest of the corpus settle, stops the streams, and
runs a result check over the settled tables so the run can be compared with
another (see "Continuous gate" and "Result check" in
[Scoring and Benchmarking](benchmarking.md)). A window much longer than the
time the trickle needs to offer the corpus measures an idle pipeline: the
gate fails it when data stopped arriving before half the window, and the run
warns about this at start. `run_duration` must be at least 3 x
`gold_refresh_interval`, or the run is refused before it starts.

### How Continuous Mode Works

The three streaming jobs behave differently:

- **bronze-ingest** and **silver-stream** are true streaming jobs. They
  process data incrementally -- each micro-batch appends new rows to the
  Iceberg table. Bronze reads Parquet files from S3; silver reads changes
  from the bronze Iceberg table.

- **gold-refresh** is a periodic batch aggregation. Every refresh cycle it
  reads the entire silver table, computes daily KPI aggregates, and
  **replaces** the gold table. It is not incremental -- the gold table is
  fully rewritten each cycle.

This means gold freshness depends on the refresh interval. With the default
5-minute cycle, gold can be up to 5 minutes stale even when bronze and
silver are seconds behind real-time.

Gold refresh also causes **Q9 contention**: if a benchmark query reads the
gold table while it's being rewritten, the query fails. Lakebench handles
this with automatic retries (30s/60s backoff). The scorecard records
contention events per round.

### Continuous Mode Configuration

```yaml
architecture:
  pipeline:
    continuous:
      bronze_trigger_interval: "30 seconds"
      silver_trigger_interval: "60 seconds"
      gold_refresh_interval: "5 minutes"
      run_duration: 1800              # 30 minutes
      # max_files_per_trigger: unset   # auto: arrival lasts ~1.2 x run_duration
      checkpoint_base: checkpoints
      benchmark_interval: 300         # Seconds between in-stream rounds
      benchmark_warmup: 300           # Seconds before first round
```

Override the run duration on the command line:

```bash
lakebench run my-config.yaml --continuous --duration 3600
```

### Tuning Reference

| Field | Default | What it controls | When to change |
|---|---|---|---|
| `bronze_trigger_interval` | 30s | How often bronze checks for new files. Lower = fresher data, higher CPU. | Reduce to 10-15s if freshness is critical. Increase to 60s+ for large scales where each batch is already large. |
| `silver_trigger_interval` | 60s | How often silver reads new bronze rows. Lower = fresher silver, more micro-batches. | Keep at 2x bronze interval. Reducing below bronze interval wastes cycles on empty batches. |
| `gold_refresh_interval` | 5 min | How often gold re-aggregates from silver. Sets the floor for gold freshness. | Reduce for fresher dashboards, but each cycle reads all of silver -- at large scales a refresh can take 30s+, so don't set the interval below the refresh duration. |
| `max_files_per_trigger` | auto | Files bronze reads per trigger (c360: 15,491 rows per 64 MB file). With `bronze_trigger_interval` it is the offered load. Auto derives it so data keeps arriving for about 1.2 x `run_duration`, capped at 50 (Lakebench-imposed). | Set it to offer a fixed load across scales, and size bronze-ingest and silver-stream for it. A value that would offer the corpus before the window ends is refused at start with the value to use. A short `ingest_ratio` with `intake_limit: trickle_rate` is not saturation. |
| `run_duration` | 1800 | Measurement window in seconds. At least 3 x `gold_refresh_interval` (900 s at defaults), or the run is refused: the continuous gate needs two gold refreshes on new data inside it. For 5 benchmark rounds: `gold_refresh_interval + 5 * (benchmark_interval + round_time)`. | Use 900 s or more for short tests (UAT included); see [Scoring and Benchmarking](benchmarking.md) for a planning table. |
| `benchmark_warmup` | 300s | Delay before first benchmark round. **Clamped to `gold_refresh_interval`** at runtime -- gold must complete at least one full refresh before benchmark rounds produce valid QpH. | Reduce only if you also reduce `gold_refresh_interval`. |
| `benchmark_interval` | 300s | Time between benchmark rounds (measured from completion of previous round). **Clamped to `gold_refresh_interval`** at runtime -- intervals shorter than the gold cycle cause Q9 contention as rounds overlap with gold rewrites. | To get more rounds, increase `run_duration` instead of lowering the interval. |
| `bronze_target_file_size_mb` | 512 | Target Iceberg data file size for bronze writes. | Reduce to 128-256 MB at small scales (< 10) where 512 MB files are never reached. |
| `silver_target_file_size_mb` | 512 | Target Iceberg data file size for silver writes. | Same guidance as bronze. |
| `gold_target_file_size_mb` | 128 | Target Iceberg data file size for gold writes. Smaller because gold is a compact aggregation table. | Rarely needs changing. |

### Iceberg Retention

Continuous-mode pipelines create new Iceberg snapshots every micro-batch.
Without periodic maintenance, snapshot metadata and orphan data files grow
unbounded -- a 24-hour run can produce over a million S3 objects.

Lakebench runs periodic `expire_snapshots` and `remove_orphan_files` (Delta:
`VACUUM`) during the continuous monitoring loop. Two config fields control the
schedule:

```yaml
architecture:
  pipeline:
    continuous:
      retention_interval: 600      # Seconds between maintenance rounds (300-7200; unset = run_duration / 3)
      retention_threshold: 30m     # Snapshot age to retain (e.g. 30m, 1h, 7d)
```

`retention_threshold` must be a whole number and one unit (`s`, `m`, `h` or
`d`); anything else is rejected when the config loads. The threshold is not
applied as-is everywhere:

- **Snapshot expiry** is floored at 1 h while streams are live, so a stream
  is never left without the snapshot it is reading.
- **Orphan-file removal** never uses less than 24 h 10 min, on any engine or
  path, so files a running writer has not yet committed are not deleted.
- **Delta VACUUM** keeps Delta's 7-day default retention while streams are
  live. A continuous Delta run shorter than 7 days therefore gets no effective
  cleanup, and `total_s3_objects` grows for the whole run.

Before v1.6 none of this maintenance worked. Trino refused every
`expire_snapshots` and `remove_orphan_files` below its 7-day system minimum,
the Spark Thrift form failed a parameter-binding error, and Delta VACUUM never
applied its retention (LB-172, LB-173, LB-174). Continuous numbers from v1.5
and earlier were measured with no snapshot expiry and no VACUUM.

Maintenance uses whichever query engine is deployed:

| Engine | Maintenance Support | Method |
|--------|:-------------------:|--------|
| Trino | Yes | `SET SESSION <catalog>.expire_snapshots_min_retention = ...; ALTER TABLE ... EXECUTE expire_snapshots(...)` in one submission (likewise for `remove_orphan_files`) |
| Spark Thrift | Iceberg only | `CALL catalog.system.expire_snapshots(table => ..., older_than => TIMESTAMP '...')` via beeline. Delta VACUUM is skipped (it runs Spark Thrift out of memory) |
| DuckDB | No | Read-only -- maintenance is skipped |

Each maintenance or compaction statement may run for min(600 s, half the
interval) and a whole round is capped at half the interval (and at the time
left in the run). The first statement that times out stops the rest of that
round; timeouts are journaled separately from failures, because the engine
may still be running the statement. The next round starts at the table after
the one that timed out, so a table that always times out cannot starve the
others, and compaction waits one statement timeout before it runs. A round
due with less than a minute of the run left is skipped. The bounds apply to
lakebench's wait, not to the engine: a statement that timed out near the end
of the run can still be running after the monitoring window closes.

`expire_snapshots` does not delete old `metadata.json` files; each commit
leaves one behind. Every Iceberg table lakebench creates therefore sets
`write.metadata.delete-after-commit.enabled=true` and
`write.metadata.previous-versions-max=50`, so each commit deletes metadata
files beyond the newest 50. This applies to tables created by the current
version. A table that already exists in a reused catalog keeps its old
properties until it is recreated (a fresh deployment, or a run that replaces
the table).

When `query_engine.type` is `duckdb` or `none`, maintenance is skipped with
a log message. Failures on individual tables (e.g., a table that doesn't exist
yet early in the run) are logged but do not abort the pipeline.

### In-Stream Benchmarking

During the measurement window, Lakebench runs the workload's full query benchmark (8 queries for Customer 360, 12 for AML)
at regular intervals using the active query engine. The default schedule is:
first round after 5 minutes of warmup, then every 5 minutes. Each round
measures QpH, per-query latency, and gold-table freshness at the moment of
query execution.

The final continuous QpH is the **median** of all in-stream rounds. The
terminal output shows a per-round summary table with QpH, per-query times,
freshness, and Q9 contention status. The HTML report includes an "In-Stream
Benchmark Rounds" section with the same data.

If the run is too short for in-stream rounds, no benchmark runs and no QpH
score is produced.

For round count planning and the adaptive end-of-window guard, see
[Scoring and Benchmarking](benchmarking.md).

### Continuous Mode Scoring

Continuous mode produces a different set of scores than batch:

| Score | Description |
|---|---|
| **data_freshness_seconds** | Worst-case gold table staleness from the stream job logs. |
| **query_time_event_age_seconds** | Diagnostic, not freshness: median age of gold's newest event date at query time (when in-stream rounds ran). Written as `query_time_freshness_seconds` before v1.6. |
| **sustained_throughput_rps** | Rows/sec bronze ingested inside the window, over the seconds data was arriving (`arrival_seconds`). |
| **composite_qph** | In-stream median QpH. |
| **in_stream_composite_qph** | Same as composite_qph (explicit label for in-stream origin). |
| **composite_qph_rounds** | In-stream rounds behind the composite_qph median (0 when it is the post-stream benchmark). Benchmark iterations are an execution condition, so two runs with different counts are comparable but not like-for-like, and `lakebench compare` says so. |
| **stage_latency_profile** | Average micro-batch processing time per stage (`bronze_ms`, `silver_ms`, `gold_ms`). |
| **total_rows_processed** | Total volume processed during the measurement window. |
| **pre_window_rows** | Bronze rows taken in before the window opened; not in any score. |

## Reading Output

After a pipeline run completes, Lakebench saves two outputs:

### Metrics JSON

A structured JSON file containing per-stage metrics (timing, throughput, data
volumes, resource allocation), benchmark query results, and pipeline-level
aggregate scores. Saved to `lakebench-output/runs/run-<id>/metrics.json`.

```bash
# View the latest run metrics
cat lakebench-output/runs/run-*/metrics.json | python3 -m json.tool
```

Key fields in the metrics JSON:

- `jobs[]` -- Per-stage metrics: elapsed time, input/output sizes, throughput,
  executor count, CPU-seconds, memory allocated.
- `benchmark` -- Query benchmark results: QpH, per-query timing and row counts.
- `pipeline_benchmark.scores` -- Aggregate scores: `time_to_value_seconds`
  (batch) or `data_freshness_seconds` (continuous), pipeline throughput.

### HTML Report

A self-contained HTML report with charts and tables. Includes a summary panel,
per-stage breakdown, storage metrics, resource utilization estimates, and
recommendations. Saved alongside the metrics JSON.

```bash
# Open the report in a browser
open lakebench-output/runs/run-*/report.html
```

To generate a report from a previous run:

```bash
lakebench report
```

## Executor Scaling

Executor counts for each Spark job scale with the scale factor unless
overridden in the config. Per-executor sizing (cores, memory, PVC) is fixed
from proven production profiles and does not change with scale. This keeps
data-per-executor constant as the dataset grows.

### Default Executor Counts by Scale

Each job's executor count comes from its job profile: a base count up to
scale 10, plus a per-profile number of executors per 100 scale units above
that, capped at the profile's maximum. The formula is
`min(base + int((scale - 10) * per_100 // 100), max)`.

| Job | Base (scale <= 10) | Added per 100 scale | Maximum |
|---|:---:|:---:|:---:|
| bronze-verify (c360) | 4 | 4 | 20 |
| bronze-verify (financial) | 4 | 8 | 28 |
| silver-build | 8 | 12 | 28 |
| gold-finalize | 4 | 8 | 28 |

At scale 100 that gives bronze-verify 7 (c360) or 11 (financial),
silver-build 18 and gold-finalize 11. The 28 maximum is a Lakebench-imposed
ceiling (K8s API polling, below), not a cluster limit. The auto-sizer does not
change these counts (v1.7 removed the `platform.compute.spark.executor`
block, which never did);
`lakebench info <config>` (hidden and deprecated, but the only command that prints the per-job counts) shows the resolved per-job values. In continuous
mode the streaming jobs have their own profiles and are capped to a
concurrent CPU budget when the cluster is smaller than the profiles need.

Override executor counts per job with the `platform.compute.spark.*_executors`
fields:

```yaml
platform:
  compute:
    spark:
      bronze_executors: 4
      silver_executors: 12
      gold_executors: 8
```

### High Executor Count Considerations

When overriding executor counts above 20, be aware of two scaling limits:

**Driver memory:** The Spark driver maintains K8s API watches for each
executor pod. Profile drivers are 4g for bronze-verify and 32g for
silver-build and gold-finalize on Spark 4 (24g on Spark 3). Above 20
executors, the 4g bronze-verify driver may be insufficient. If you see OOM errors in the driver pod, set a global
driver memory override. It applies to every job, so do not set it below
the 32g silver-build and gold-finalize default (24g OOMed those drivers on
Spark 4, BUG-005 and BUG-007):

```yaml
platform:
  compute:
    spark:
      driver_memory: "32g"
      silver_executors: 24
```

**K8s API polling storms:** At 32+ executors, the fabric8 Kubernetes
client (used by Spark on K8s) polls the API server once per executor per
heartbeat interval. This can overwhelm the API server, causing timeout
errors that look like network failures. This is a hard infrastructure
limit -- adding more driver memory does not help. Keep executor counts
at 28 or below (the Lakebench-imposed ceiling on computed counts).

**maxResultSize:** Lakebench automatically scales
`spark.driver.maxResultSize` based on the effective executor count. You
do not need to set this manually unless you see "serialized results"
errors in driver logs. Override it in `spark.conf` if needed.

## Troubleshooting

**Job fails with "No space left on device":** The Portworx PVC per executor is
the constraint. Silver-build requires 300Gi PVCs at scale. Ensure scratch
storage is configured with sufficient capacity.

**Job times out:** Increase the `--timeout` value. At scale 100 (~1 TB),
silver-build can take 30--60 minutes depending on cluster resources.

**Benchmark fails but pipeline succeeded:** The benchmark runs against a live
Trino cluster. If Trino pods are unhealthy, the benchmark may fail while
pipeline data is still valid. Run `lakebench status` to check Trino health,
then re-run the benchmark separately.
