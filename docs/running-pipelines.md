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

**Job profile:** 2 cores, 4g memory, 2g overhead per executor, 50Gi PVC.

### Stage 2: silver-build

The core transformation stage. Reads validated bronze data and produces an
enriched Iceberg table in the silver bucket. Applies five transforms:
email normalization, phone normalization, geo enrichment, customer
segmentation, and quality flagging.

Silver-build uses an **adaptive strategy** that selects the optimal processing
approach based on data size:

| Strategy | Data Size | Description |
|---|---|---|
| **simple** | < 100 GB | Standard Spark processing. |
| **streaming** | 100 GB -- 5 TB | Direct write with no shuffle (column transforms only). |
| **chunked** | >= 5 TB | Processes data in date-based chunks to limit memory. |
| **salted** | High skew (>100x) | Salts hot keys to distribute skewed partitions. |

The strategy is selected automatically based on the measured bronze data size.

**Job profile:** 4 cores, 48g memory, 12g overhead per executor, 150Gi PVC.

### Stage 3: gold-finalize

Aggregates the silver Iceberg table into daily KPI summaries for the executive
dashboard. Produces metrics like daily active customers, total revenue,
conversions, engagement scores, and churn risk counts. The output is a compact
gold-layer Iceberg table partitioned by date.

**Job profile:** 4 cores, 32g memory, 8g overhead per executor, 100Gi PVC.

### Stage 4: benchmark

Runs 8 Trino queries against the silver and gold tables and computes a QpH
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

## Command Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `--stage` | `-s` | (all) | Run a specific stage only: `bronze-verify`, `silver-build`, or `gold-finalize` |
| `--timeout` | `-t` | auto | Timeout per job in seconds. Defaults to `max(3600, scale * 60)` when omitted. |
| `--skip-benchmark` | | `false` | Skip the query benchmark after pipeline stages |
| `--continuous` | | `false` | Run in continuous streaming mode. Overrides `pipeline.mode` in config. |
| `--duration` | | config value | Streaming run duration in seconds (continuous mode only) |
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

Continuous mode runs a streaming pipeline instead of batch. Set it in the
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
uses the streaming pipeline automatically -- no CLI flag needed. The
`--continuous` flag still works as an override for one-off runs.

In continuous mode, three streaming Spark jobs run concurrently:

```
bronze-ingest + silver-stream + gold-refresh  (concurrent)
```

- **bronze-ingest** -- Reads new Parquet files from S3 as they appear (via
  `maxFilesPerTrigger`), writes to a bronze Iceberg table.
- **silver-stream** -- Reads the bronze Iceberg table as a streaming source,
  applies silver transforms, writes to the silver Iceberg table.
- **gold-refresh** -- Periodically refreshes the gold aggregation table from
  the silver table.

Datagen runs concurrently with the streaming jobs, continuously producing
new data for the pipeline to ingest. This is automatic -- no `--generate`
flag is needed. That flag only applies to batch mode.

The pipeline runs for the configured duration (default: 1800 seconds / 30
minutes). During this window, Lakebench runs periodic Trino benchmark rounds
to measure query performance while streaming is active. After the window ends,
streaming jobs are stopped and the in-stream results are aggregated.

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
      max_files_per_trigger: 50       # Files per micro-batch
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
| `max_files_per_trigger` | 50 | Files bronze processes per micro-batch (~122K rows/file, so 50 files = ~6.1M rows). Primary throughput cap. | Increase if `ingest_ratio < 0.95` (pipeline saturated). Decrease if bronze micro-batches are too large for executor memory. |
| `run_duration` | 1800 | Total streaming window in seconds. Minimum useful duration is `gold_refresh_interval + benchmark_interval + round_time` (~7 min at defaults). For 5 rounds: `gold_refresh_interval + 5 * (benchmark_interval + round_time)`. | See [Scoring and Benchmarking](benchmarking.md) for a planning table. |
| `benchmark_warmup` | 300s | Delay before first benchmark round. **Clamped to `gold_refresh_interval`** at runtime -- gold must complete at least one full refresh before benchmark rounds produce valid QpH. | Reduce only if you also reduce `gold_refresh_interval`. |
| `benchmark_interval` | 300s | Time between benchmark rounds (measured from completion of previous round). **Clamped to `gold_refresh_interval`** at runtime -- intervals shorter than the gold cycle cause Q9 contention as rounds overlap with gold rewrites. | To get more rounds, increase `run_duration` instead of lowering the interval. |
| `bronze_target_file_size_mb` | 512 | Target Iceberg data file size for bronze writes. | Reduce to 128-256 MB at small scales (< 10) where 512 MB files are never reached. |
| `silver_target_file_size_mb` | 512 | Target Iceberg data file size for silver writes. | Same guidance as bronze. |
| `gold_target_file_size_mb` | 128 | Target Iceberg data file size for gold writes. Smaller because gold is a compact aggregation table. | Rarely needs changing. |

### In-Stream Benchmarking

During the streaming window, Lakebench runs the full 8-query benchmark
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
| **data_freshness_seconds** | Worst-case gold table staleness from streaming logs. |
| **query_time_freshness_seconds** | Median gold staleness at Trino query time (when in-stream rounds ran). |
| **sustained_throughput_rps** | Aggregate sustained rows/sec across all streaming stages. |
| **composite_qph** | In-stream median QpH. |
| **in_stream_composite_qph** | Same as composite_qph (explicit label for in-stream origin). |
| **end_to_end_latency_ms** | Cumulative micro-batch processing latency bronze to gold. |
| **total_rows_processed** | Total volume processed during the monitoring window. |

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

Executor counts for each Spark job auto-scale based on the scale factor unless
overridden in the config. Per-executor sizing (cores, memory, PVC) is fixed
from proven production profiles and does not change with scale. This keeps
data-per-executor constant as the dataset grows.

### Default Executor Counts by Scale

Executor counts are derived from the scale factor using four tiers. The
`lakebench recommend` command shows these values for your chosen scale.

| Scale Range | Tier | Executors | Formula |
|:-----------:|:----:|:---------:|---------|
| 1--5 | minimal | 2 | fixed |
| 6--50 | balanced | 4--8 | `max(4, min(8, scale / 6))` |
| 51--500 | performance | 8--16 | `max(8, min(16, scale / 30))` |
| 501+ | extreme | 16--32 | `max(16, min(32, scale / 50))` |

These are starting values. When connected to a cluster, the autosizer adjusts
counts to fit available resources -- capping if the cluster is smaller than the
tier suggests, or scaling up at scale 51+ to use available capacity.

Override executor counts in the config if your cluster has specific capacity
constraints:

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
executor pod. Above 20 executors, the default driver memory may be
insufficient. If you see OOM errors in the driver pod, set a global
driver memory override:

```yaml
platform:
  compute:
    spark:
      driver_memory: "24g"
      silver_executors: 24
```

**K8s API polling storms:** At 32+ executors, the fabric8 Kubernetes
client (used by Spark on K8s) polls the API server once per executor per
heartbeat interval. This can overwhelm the API server, causing timeout
errors that look like network failures. This is a hard infrastructure
limit -- adding more driver memory does not help. Keep executor counts
at 28 or below.

**maxResultSize:** Lakebench automatically scales
`spark.driver.maxResultSize` based on the effective executor count. You
do not need to set this manually unless you see "serialized results"
errors in driver logs. Override it in `spark.conf` if needed.

## Troubleshooting

**Job fails with "No space left on device":** The Portworx PVC per executor is
the constraint. Silver-build requires 150Gi PVCs at scale. Ensure scratch
storage is configured with sufficient capacity.

**Job times out:** Increase the `--timeout` value. At scale 100 (~1 TB),
silver-build can take 30--60 minutes depending on cluster resources.

**Benchmark fails but pipeline succeeded:** The benchmark runs against a live
Trino cluster. If Trino pods are unhealthy, the benchmark may fail while
pipeline data is still valid. Run `lakebench status` to check Trino health,
then re-run the benchmark separately.
