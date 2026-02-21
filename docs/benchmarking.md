# Scoring and Benchmarking

Lakebench produces two distinct measurements:

1. **Pipeline scorecard** -- end-to-end scoring of the medallion pipeline
   (datagen, bronze, silver, gold). Answers "how fast and how efficiently
   does raw data become queryable gold?" in batch mode, or "how fresh is
   gold and can the pipeline keep up?" in continuous mode.

2. **Query engine benchmark** -- an 8-query SQL benchmark against the
   silver and gold tables using whichever engine your recipe specifies
   (Trino, Spark Thrift Server, or DuckDB). Produces a QpH (Queries per
   Hour) score that measures engine-level analytical performance.

The scorecard includes QpH as one of its scores (`composite_qph`), but they
are separate operations. `lakebench run` produces both automatically.
`lakebench benchmark` runs only the query engine benchmark.

---

## Pipeline Scorecard

The pipeline scorecard normalizes heterogeneous stages (Spark batch, Spark
streaming, query engines, datagen) into a single comparable view. Scoring
is mode-conditional -- batch and continuous pipelines produce different
score sets.

### Batch Scores

Batch scoring answers: "How fast do we get from raw data to queryable gold?"

| Score | Formula | Meaning |
|---|---|---|
| `time_to_value_seconds` | `max(end_time) - min(start_time)` | Wall-clock seconds from first stage start to last stage end. The primary batch score. Lower is better. |
| `total_elapsed_seconds` | `sum(stage.elapsed_seconds)` | Sum of all stage durations. May exceed time-to-value if stages overlap. |
| `total_data_processed_gb` | `sum(stage.input_size_gb)` | Total input data across all stages. |
| `pipeline_throughput_gb_per_second` | `total_data_processed_gb / time_to_value_seconds` | Composite throughput across the whole pipeline. Higher is better. |
| `compute_efficiency_gb_per_core_hour` | `total_data_processed_gb / total_core_hours` | GB processed per core-hour of allocated compute. Higher means better resource utilization. |
| `scale_ratio` | `total_gb / approx_bronze_gb` | Data completeness check. A ratio below 0.95 means data generation or ingestion was incomplete, which invalidates cross-run comparisons. |
| `composite_qph` | QpH from the query engine benchmark | Query throughput against the gold layer. |

### Continuous Scores

Continuous scoring answers: "How fresh is gold, and how fast are we sustaining it?"

| Score | Formula | Meaning |
|---|---|---|
| `data_freshness_seconds` | `max(stage.freshness_seconds)` | Worst-case gold staleness. The primary continuous score. Lower is better. |
| `sustained_throughput_rps` | `bronze_input_rows / run_duration` | Aggregate sustained rows/sec the pipeline can maintain. Higher is better. |
| `stage_latency_profile` | `[bronze_ms, silver_ms, gold_ms]` | Per-stage micro-batch processing latency. Lower is better. |
| `ingest_ratio` | `bronze_rows / datagen_rows` | Fraction of generated data that made it through bronze. Below 0.95 flags saturation. Above 1.0 means re-reads inflate the count. |
| `pipeline_saturated` | `ingest_ratio < 0.95` | Boolean flag. True when the pipeline cannot keep pace with incoming data. Indicates a bottleneck that needs investigation (see Interpreting Scores below). |
| `compute_efficiency_gb_per_core_hour` | `total_data_processed_gb / total_core_hours` | GB processed per core-hour of allocated compute. Shared with batch mode. |
| `total_rows_processed` | `sum(stage.input_rows)` | Total volume processed during the monitoring window. |
| `composite_qph` | QpH from the query engine benchmark | Query throughput against the gold layer. |

### Per-Stage Metrics

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

### Including Data Generation in Scoring

By default, `lakebench run` only measures the pipeline stages (bronze-verify,
silver-build, gold-finalize) and the query engine benchmark. Data generation is
treated as a separate preparation step.

In **batch** mode, to measure the full end-to-end pipeline including data
ingestion, use the `--generate` flag:

```bash
lakebench run --generate
```

This runs data generation first, then the pipeline stages, then the query
engine benchmark -- all in one invocation.

In **continuous** mode, datagen always runs automatically alongside the
streaming jobs -- no `--generate` flag needed. The datagen stage is included in the
pipeline scorecard as a `datagen` stage with `stage_type="datagen"` and
`engine="datagen"`. It contributes to:

- `time_to_value_seconds` -- wall-clock now starts from datagen begin
- `total_elapsed_seconds` -- includes datagen duration
- `total_data_processed_gb` -- includes datagen output size
- `scale_ratio` -- uses datagen output to verify completeness

The datagen output size is measured from the bronze S3 bucket after generation
completes. Row count is estimated from the scale factor (approximately 1.5M
rows per scale unit).

### Resource Metrics

All resource metrics use **requested** (allocated) values, not runtime
utilization. This is intentional -- it measures what you asked Kubernetes for,
which is what you pay for in a cloud environment. The key resource fields per
stage are:

- `executor_count` -- number of Spark executors
- `executor_cores` -- cores per executor
- `executor_memory_gb` -- memory per executor (not including overhead)

These come from the job profiles in `spark/job.py` and the scale-derived
executor count. They do not change between runs at the same scale unless you
override executor counts in your config.

---

## Query Engine Benchmark

The query engine benchmark measures analytical query performance independently
from the pipeline. It runs 8 SQL queries against the silver and gold tables
using the active query engine and produces a QpH (Queries per Hour) score.

### Query Categories

The 8 queries are organized into five categories:

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

### Benchmark Modes

The benchmark runner supports three modes following TPC methodology:

#### Power Run (default)

Executes all 8 queries sequentially in a single stream. Each query is timed
individually.

```
QpH = (num_queries / total_seconds) * 3600
```

For example, if 8 queries complete in 40 seconds total, QpH = (8 / 40) *
3600 = 720.

When `iterations > 1`, each query is run multiple times and the median time
is used for scoring.

#### Throughput Run

Runs N concurrent query streams. Each stream executes the full 8-query suite
with shuffled query order to reduce correlated cache effects.

```
QpH = (total_queries_across_all_streams / wall_clock_seconds) * 3600
```

#### Composite Run

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

### In-Stream Benchmarking (Continuous Mode)

In continuous mode, Lakebench runs query engine benchmark rounds at regular
intervals **while streaming jobs are active**. This measures engine
performance under realistic conditions -- concurrent streaming writes, active
compaction, and changing table state.

The first round starts after a configurable warmup period, then repeats at
a fixed interval. The interval is measured from round completion, not from
round start, so rounds don't pile up when queries take longer than expected.

**Configuration:**

```yaml
architecture:
  pipeline:
    continuous:
      benchmark_warmup: 300     # seconds before first round (default 300)
      benchmark_interval: 300   # seconds between rounds (default 300)
```

Each round:

1. Flushes the query engine metadata cache
2. Probes gold-table freshness at query time
3. Runs the full 8-query power benchmark
4. Records per-round QpH, per-query times, and freshness

The final QpH for the continuous pipeline scorecard is the **median** across
all in-stream rounds.

**Scheduling constraint:** Both `benchmark_warmup` and `benchmark_interval`
are clamped to `gold_refresh_interval` (default 5 min) at runtime. Gold
rewrites the entire table each refresh cycle via `createOrReplace()`.
Warmup below the gold interval produces inflated QpH from queries against an
empty or stale gold table. Intervals shorter than the gold cycle cause Q9
contention as benchmark rounds overlap with gold rewrites, producing
inconsistent QpH across rounds. Lakebench raises both values automatically
and logs a warning. To get more benchmark rounds, increase `run_duration` --
do not lower the interval below the gold refresh cycle.

**Planning round counts:** The number of rounds depends on run duration,
warmup, interval, and how long each round takes on your cluster. Use this
formula to estimate:

```
available = run_duration - warmup
rounds ≈ 1 + floor((available - round_time) / (interval + round_time))
```

Round execution time varies with scale factor and cluster size -- 20-40s at
scale 10 with 3 Trino workers, 60-120s at scale 100 with 10 workers.

| Duration | Warmup | Interval | Approx round time | Expected rounds |
|---|---|---|---|---|
| 30 min | 300s | 300s | 40s | 5 |
| 45 min | 300s | 300s | 40s | 8 |
| 60 min | 300s | 300s | 40s | 10 |

For at least 5 rounds (recommended for trend analysis), set
`run_duration >= warmup + 5 * (interval + round_time)`. With default
5-minute gold refresh, the shortest practical configuration for 5 rounds is
`warmup=300, interval=300, run_duration=1800` (30 min).

**Adaptive end-of-window guard:** Lakebench uses an adaptive guard to decide
whether to start a final round near the end of the streaming window. Before
any round has completed, a 60-second floor is used. After the first round
completes, the guard switches to 1.2x the observed round duration. This
scales with cluster performance -- fast clusters get more rounds, slow
clusters don't start rounds they can't finish.

**Q9 contention handling:** Q9 is the only query that reads the gold table.
Gold uses `createOrReplace()` which rewrites the entire table every refresh
cycle. If Q9 fails during a round, Lakebench retries up to twice with
30s/60s backoff. The contention status is recorded per-round as
`q9_contention_observed` and `q9_retry_used`.

**Query-time freshness:** Each round measures gold-table staleness at the
moment the engine queries it. This is more accurate than the streaming-log
freshness (which is averaged over the entire run). The median of per-round
freshness appears in the scorecard as `query_time_freshness_seconds`.

If the run duration is too short for at least one round (less than
`benchmark_warmup + benchmark_interval`), in-stream benchmarking is skipped
with a warning and no QpH score is produced.

---

## Interpreting Scores

### Batch Mode

**Time to Value** is the primary score. It measures wall-clock time from when
the first stage starts to when the last stage finishes. This is the number
that answers "how long until my data is queryable?"

- At scale 10 (~100 GB), expect 200--600s depending on cluster size
- At scale 100 (~1 TB), expect 1200--3600s

**Throughput** (GB/s) shows how fast the pipeline processes data overall.
Higher is better. This number scales with executor count and cluster capacity.

**Compute Efficiency** (GB/core-hour) measures how well you use allocated
resources. A higher number means less wasted compute. This metric uses
*requested* resources (what Spark asked Kubernetes for), not runtime
utilization. It penalizes over-provisioning: if you request 100 cores but only
use 20, efficiency drops.

**Scale Verified Ratio** validates that the benchmark ran on the expected data
volume. A ratio of 1.0 means the actual data matched the configured scale
factor. Below 0.95 indicates incomplete data -- the scorecard results are not
comparable to runs at the same nominal scale.

**QpH** (Queries per Hour) measures query engine performance against the gold
layer. This score depends on the query engine (Trino, Spark Thrift, DuckDB),
worker count, and memory allocation. It is independent of pipeline throughput.

### Continuous Mode

**Data Freshness** is the primary score. It measures how far behind real-time
the gold layer is -- the worst-case staleness across all streaming stages.
Lower is better.

- Under 30s is typical for a well-provisioned pipeline
- Over 60s suggests a bottleneck in one of the stages

**Sustained Throughput** (rows/sec) measures the steady-state ingestion rate
through bronze. This is unique rows only -- gold-stage re-reads of silver data
are excluded.

**Stage Latency Profile** is a three-element vector `[bronze_ms, silver_ms,
gold_ms]` showing per-stage micro-batch processing latency. If one stage has
significantly higher latency, it is the bottleneck.

**Ingestion Completeness** shows what fraction of generated data was actually
consumed. Below 0.95 means the pipeline is saturated -- it cannot keep up with
the data generation rate. When this happens, `pipeline_saturated` is set to
`true`.

**Pipeline Saturated** is a boolean flag derived from completeness. When true,
increase executor count or reduce datagen parallelism to bring the pipeline
back below capacity.

---

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
throughput), a pipeline scorecard stage matrix, a per-query breakdown of the
query engine benchmark, and the configuration snapshot.

### Viewing Results on the Command Line

Use `lakebench results` to display the stage-matrix view in the terminal:

```bash
lakebench results                      # latest run, table format
lakebench results --format json        # JSON output
lakebench results --run <id>           # specific run
```

Use `lakebench report --summary` to print key scores directly in the terminal
without opening the HTML report:

```bash
lakebench report --summary                        # latest run
lakebench report --summary --run <id>             # specific run
lakebench report --summary --metrics <dir>        # custom metrics dir
```

The summary output includes a per-stage table (elapsed time, data volume,
throughput, executor count) and mode-appropriate scores -- time-to-value and
throughput for batch, data freshness and sustained throughput for continuous.

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

Before comparing, check `scale_ratio` (batch) or
`ingest_ratio` (continuous) to confirm both runs processed
the expected data volume. Comparing runs with incomplete data gives misleading
results.
