# Benchmarking

Lakebench measures end-to-end pipeline performance and query throughput across
the Customer 360 medallion pipeline. The primary score is **time-to-value** --
how fast raw data becomes queryable gold. An 8-query benchmark suite then
measures analytical query performance against the gold layer using whichever
query engine your recipe specifies (Trino, Spark Thrift Server, or DuckDB).

## Overview

A lakebench benchmark has two phases:

1. **Pipeline scoring** -- measures the bronze-verify, silver-build, and
   gold-finalize stages for elapsed time, throughput, and compute efficiency.
2. **Query benchmark** -- executes 8 SQL queries against the silver and gold
   tables via the active query engine. The queries span five categories that
   mirror real analytical workloads: full-table scans, predicate filtering
   with grouping, hash aggregation, window function analytics, and operational
   gold-layer reads.

Both phases run automatically as part of `lakebench run`. The query benchmark
can also be triggered independently with `lakebench benchmark`.

## Pipeline Scoring

Lakebench computes pipeline-level scores that capture end-to-end performance.
The scoring is mode-conditional based on the pipeline mode.

### Batch Pipeline Scores

Batch scoring answers: "How fast do we get from raw data to queryable gold?"

| Score | Formula | Meaning |
|---|---|---|
| `time_to_value_seconds` | `max(end_time) - min(start_time)` | Wall-clock seconds from first stage start to last stage end. The primary batch score. Lower is better. |
| `total_elapsed_seconds` | `sum(stage.elapsed_seconds)` | Sum of all stage durations. May exceed time-to-value if stages overlap. |
| `total_data_processed_gb` | `sum(stage.input_size_gb)` | Total input data across all stages. |
| `pipeline_throughput_gb_per_second` | `total_data_processed_gb / time_to_value_seconds` | Composite throughput across the whole pipeline. Higher is better. |
| `compute_efficiency_gb_per_core_hour` | `total_data_processed_gb / total_core_hours` | GB processed per core-hour of allocated compute. |
| `composite_qph` | QpH from the query benchmark | Query throughput against the gold layer. |

### Continuous Pipeline Scores

Continuous scoring answers: "How fresh is gold, and how fast are we sustaining it?"

| Score | Formula | Meaning |
|---|---|---|
| `data_freshness_seconds` | `max(stage.freshness_seconds)` | Worst-case gold staleness. The primary continuous score. Lower is better. |
| `sustained_throughput_rps` | `bronze_input_rows / run_duration` | Aggregate sustained rows/sec the pipeline can maintain. Higher is better. |
| `stage_latency_profile` | `[bronze_ms, silver_ms, gold_ms]` | Per-stage micro-batch processing latency. Lower is better. |
| `ingestion_completeness_ratio` | `bronze_rows / datagen_rows` | Fraction of generated data that made it through bronze. Below 0.95 flags saturation. |
| `total_rows_processed` | `sum(stage.input_rows)` | Total volume processed during the monitoring window. |
| `composite_qph` | QpH from the query benchmark | Query throughput against the gold layer. |

## Query Categories

The 8 benchmark queries are organized into five categories:

**Scan (Q1)** -- Full table scan with aggregation. Exercises raw I/O
throughput by scanning the entire silver table and computing global
aggregates (total records, unique customers, total revenue).

**Filter/Prune (Q2, Q4)** -- Date-range and predicate filtering with
GROUP BY. Q2 filters the silver table to a 3-month window and groups by
date and interaction type. Q4 filters on churn risk indicators and groups by
journey stage and device category with a HAVING clause.

**Aggregation (Q3, Q7)** -- Hash aggregation and conditional SUM(CASE). Q3
segments customers by value tier and channel preference. Q7 builds a
per-channel conversion funnel using conditional aggregation to count
awareness, consideration, conversion, and retention stages.

**Analytics (Q5, Q6)** -- Window functions and CTEs with multi-branch CASE.
Q5 computes a 7-day moving average of daily revenue and DAU using window
frames. Q6 implements RFM (Recency, Frequency, Monetary) customer scoring
with a CTE and multi-branch CASE classification.

**Operational (Q9)** -- Gold layer executive dashboard read. Reads the
pre-aggregated gold table with LAG window functions to compute day-over-day
revenue change and DAU growth percentage. This tests the "last mile" read
path that dashboards use.

## Benchmark Modes

The benchmark runner supports three modes following TPC methodology:

### Power Run (default)

Executes all 8 queries sequentially in a single stream. Each query is timed
individually.

```
QpH = (num_queries / total_seconds) * 3600
```

For example, if 8 queries complete in 40 seconds total, QpH = (8 / 40) *
3600 = 720.

When `iterations > 1`, each query is run multiple times and the median time
is used for scoring.

### Throughput Run

Runs N concurrent query streams. Each stream executes the full 8-query suite
with shuffled query order to reduce correlated cache effects.

```
QpH = (total_queries_across_all_streams / wall_clock_seconds) * 3600
```

### Composite Run

Runs a power phase followed by a throughput phase. The composite QpH is the
geometric mean of the two:

```
composite_qph = sqrt(power_qph * throughput_qph)
```

### Cache Modes

Each benchmark mode supports `hot` or `cold` cache. In cold mode the query
engine's metadata cache is flushed before execution (e.g.
`CALL iceberg.system.flush_metadata_cache()` on Trino, or engine-specific
equivalents for Spark Thrift and DuckDB). In power mode with cold cache, the
cache is flushed before each individual query. In throughput mode, it is
flushed once before all streams start.

## Per-Stage Metrics

Each pipeline stage (datagen, bronze, silver, gold, query) produces a uniform
`StageMetrics` record containing:

- **Timing**: elapsed seconds, start/end timestamps, success/failure
- **Data volume**: input and output size in GB, input and output row counts
- **Throughput**: `input_size_gb / elapsed_seconds` and `input_rows / elapsed_seconds`
- **Resources** (allocated): executor count, cores per executor, memory per executor
- **Streaming** (zero for batch): latency in ms, freshness in seconds, batch count
- **Query** (zero for non-query stages): queries executed, queries per hour

These per-stage metrics feed into the pipeline-level scores and are exported
in the `stages` and `stage_matrix` sections of the metrics JSON.

## Reading Results

### Metrics JSON

After a run completes, metrics are saved to:

```
lakebench-output/runs/run-<id>/metrics.json
```

The JSON structure includes:

```json
{
  "run_id": "20260201-143052-a1b2c3",
  "pipeline_benchmark": {
    "pipeline_mode": "batch",
    "scorecard": {
      "time_to_value_seconds": 1842.5,
      "pipeline_throughput_gb_per_second": 0.5432,
      "composite_qph": 720.0
    },
    "stages": [ ... ],
    "stage_matrix": { ... },
    "query_benchmark": {
      "mode": "power",
      "qph": 720.0,
      "queries": [ ... ]
    }
  }
}
```

### HTML Reports

Generate a report from the latest run:

```bash
lakebench report
```

List all available runs:

```bash
lakebench report --list
```

Generate a report for a specific run:

```bash
lakebench report --run 20260201-143052-a1b2c3
```

Reports are written to `lakebench-output/runs/run-<id>/report.html`. The HTML
report includes summary cards (total time, QpH, time-to-value, pipeline
throughput), a job performance table, a pipeline benchmark stage matrix, a
per-query breakdown of the query benchmark, and the configuration snapshot.

### Viewing Results on the Command Line

Use `lakebench results` to display the stage-matrix view in the terminal:

```bash
lakebench results                      # latest run, table format
lakebench results --format json        # JSON output
lakebench results --run <id>           # specific run
```

## Comparing Runs

The metrics JSON is designed for diff and comparison across configurations.
Key fields for comparison:

- `scorecard.time_to_value_seconds` -- primary batch score
- `scorecard.pipeline_throughput_gb_per_second` -- throughput efficiency
- `scorecard.composite_qph` -- query performance
- `stage_matrix` -- per-stage breakdown for identifying bottlenecks
- `config_snapshot` -- captures scale factor, executor counts, memory,
  and all tuning parameters

To compare two runs, load both `metrics.json` files and diff the `scorecard`
and `stage_matrix` sections. The `config_snapshot` in each run records the
exact configuration used, so you can attribute performance differences to
specific changes (scale factor, executor count, memory, engine workers, etc.).

The `scale_verified_ratio` field (batch mode) confirms that the actual data
processed matches the expected volume for the configured scale factor. A ratio
below 0.95 indicates incomplete data generation or ingestion, which would
invalidate a cross-run comparison.
