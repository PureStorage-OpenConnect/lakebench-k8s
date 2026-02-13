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
| `--timeout` | `-t` | `3600` | Timeout per job in seconds |
| `--skip-benchmark` | | `false` | Skip the query benchmark after pipeline stages |
| `--continuous` | | `false` | Run in continuous streaming mode instead of batch |
| `--duration` | | config value | Streaming run duration in seconds (continuous mode only) |
| `--include-datagen` | | `false` | Run datagen before pipeline stages for full end-to-end measurement |

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
lakebench run my-config.yaml --include-datagen --timeout 7200
```

## Continuous Mode

Continuous mode runs a streaming pipeline instead of batch. Activate it with
the `--continuous` flag:

```bash
lakebench run my-config.yaml --continuous
```

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
new data for the pipeline to ingest.

The pipeline runs for the configured duration (default: 1800 seconds / 30
minutes), then stops all streaming jobs and optionally runs the Trino
benchmark.

### Continuous Mode Configuration

Tune streaming behavior in the config file:

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
```

Override the run duration on the command line:

```bash
lakebench run my-config.yaml --continuous --duration 3600
```

### Continuous Mode Scoring

Continuous mode produces a different set of scores than batch:

| Score | Description |
|---|---|
| **data_freshness_seconds** | Worst-case gold table staleness. The primary score. |
| **sustained_throughput_rps** | Aggregate sustained rows/sec across all streaming stages. |
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
lakebench report my-config.yaml
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
