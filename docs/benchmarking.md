# Scoring and Benchmarking

Lakebench produces two distinct measurements:

1. **Pipeline scorecard** -- end-to-end scoring of the medallion pipeline
   (datagen, bronze, silver, gold). Answers "how fast and how efficiently
   does raw data become queryable gold?" in batch mode, or "how fresh is
   gold and can the pipeline keep up?" in continuous mode.

2. **Query engine benchmark** -- a SQL benchmark against the silver and
   gold tables (8 queries for Customer 360; 12 for AML, FQ1-FQ8 plus the
   investigator queries IQ1-IQ4) using whichever engine your recipe specifies
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
| `time_to_value_seconds` | `max(end_time) - min(start_time)` | Wall-clock seconds from the first stage's submission to the moment gold is queryable (the gold Spark application's end). It includes lakebench's work between stages (noticing a stage ended, reading its driver log and output size, submitting the next). The primary batch score. Lower is better. |
| `total_elapsed_seconds` | `sum(stage.elapsed_seconds)` | Sum of all stage durations. May exceed time-to-value if stages overlap. |
| `total_data_processed_gb` | `sum(stage.input_size_gb)` | Total input data across all stages. |
| `pipeline_throughput_gb_per_second` | `total_data_processed_gb / time_to_value_seconds` | Composite throughput across the whole pipeline. Higher is better. |
| `compute_efficiency_gb_per_core_hour` | `total_data_processed_gb / total_core_hours` | GB processed per core-hour of allocated compute. Higher means better resource utilization. |
| `scale_ratio` | `bronze_input_gb / approx_bronze_gb` | Data completeness check. Uses only bronze input -- not the total across all stages. A ratio below 0.95 means data generation or ingestion was incomplete, which invalidates cross-run comparisons. |
| `composite_qph` | QpH from the query engine benchmark | Query throughput against the gold layer. |
| `cycle_progression` | Per-cycle elapsed, QpH, table health | (Multi-cycle only, when `cycles > 1`) Shows pipeline time and Iceberg metadata growth per cycle. |

Batch stage times (v1.6). A Spark stage runs from its SparkApplication's creation to the driver container's finish time (else the SparkApplication's `terminationTime`), mapped to the lakebench host's clock through the API server's clock offset. Each stage records `timing_source` (`driver_container`, `spark_application`, or `poll` when no consistent cluster time could be read) and `timing_resolution_seconds` (2 s from the cluster, the poll interval otherwise). Before v1.6 every stage ended on the 15 s job-monitor poll, so older stage seconds and time to value are rounded up by up to 15 s per stage and include the gold driver-log fetch; the perf gate refuses to compare the two (record a new baseline).

### Continuous Scores

Continuous scoring answers: "How fresh is gold, and what ingest rate does the pipeline hold?"

Freshness, throughput and the row counts are measured inside the
**measurement window**. The window opens when every stream's driver is
running and closes `run_duration` seconds later. It is kept in cluster time
(this host's clock shifted by the API server's, so pod log timestamps line
up with it); its UTC start and end and the offset are in `metrics.json`
under `continuous.window`. A stream that started early (because another was
still retrying its submission) takes rows in before the window opens; those
are recorded as `pre_window_rows` and left out of every window score.
`ingest_ratio` and `corpus_drained` count every row up to the window's end,
pre-window rows included. `pipeline_throughput_gb_per_second` and
`compute_efficiency_gb_per_core_hour` still divide the bucket sizes measured
at the window's end by the window.

Definitions changed on lane/continuous-cred (2026-09-26):
`sustained_throughput_rps` was bronze rows / `run_duration`, pre-window rows
included; `data_freshness_seconds` covered every gold cycle in the log;
`corpus_drained` also needed two idle gold cycles; `total_rows_processed`
counted whole logs. Continuous records made before this are not comparable
with later ones on those scores (their experiment identity and results
differ, so compare, the perf gate and reproduce refuse them).

| Score | Formula | Meaning |
|---|---|---|
| `data_freshness_seconds` | `max(gold cycle freshness inside the window)` | Worst-case gold staleness. The primary continuous score. Lower is better. |
| `sustained_throughput_rps` | `bronze rows ingested inside the window / arrival_seconds` | Rows/sec entering bronze while data was arriving. Higher is better. When `intake_limit` is `trickle_rate` it is the configured offered load, not a capacity. |
| `window_seconds` | window end - window start | Length of the measurement window. |
| `arrival_seconds` | the whole window while corpus was left, else bronze's last write inside the window + one bronze trigger | Seconds of the window data was still arriving. Throughput is never averaged over idle time after the corpus ran out. |
| `window_arrival_fraction` | `arrival_seconds / window_seconds` | Below 1 the corpus ran out inside the window. |
| `pre_window_rows` | bronze rows written before the window opened | Not part of any window score. |
| `stage_latency_profile` | `[bronze_ms, silver_ms, gold_ms]` | Per-stage micro-batch processing latency (a diagnostic: `compare` does not colour it). |
| `ingest_ratio` | `bronze_rows / released_rows` | Share of what the trickle had released that bronze took by the window's end. `released_rows` = `max_files_per_trigger` files per bronze trigger since bronze's first write, at the corpus's mean rows per file (datagen rows / files), capped at the corpus. 1.0 = bronze kept up with what arrived. Falls back to `corpus_ingest_ratio` when the corpus file count is unknown. |
| `corpus_ingest_ratio` | `bronze_rows / datagen_rows` | Share of the whole corpus taken by the window's end. About 0.8 on a default run, whose trickle is sized to outlast the window; not a saturation signal. |
| `pipeline_saturated` | `ingest_ratio < 0.95`, unless `intake_limit` is `trickle_rate` and silver kept up | Boolean flag, null when unmeasurable. True when bronze fell behind the rows the trickle released. Indicates a bottleneck that needs investigation (see Interpreting Scores below). |
| `intake_limit` | bronze trigger count, batch time and busy share | What bounded intake when `ingest_ratio < 0.95`: `trickle_rate` (the configured trickle; the pipeline kept pace), `bronze_capacity` (bronze busy most of the window), `below_bronze_capacity` (idle bronze without the trickle pattern: a late start or a stall), `none` (kept up: `ingest_ratio >= 0.95`). Whether the trickle held intake is `experiment.limits.trickle_bound`, below. |
| `corpus_drain_seconds` | `datagen_rows / sustained_throughput_rps` | Set when `intake_limit` is `trickle_rate`: the window that would drain the corpus at the rate held. |
| `compute_efficiency_gb_per_core_hour` | `total_data_processed_gb / total_core_hours` | GB processed per core-hour of allocated compute. Shared with batch mode. |
| `total_rows_processed` | `sum(stage rows taken in inside the window)` (gold: its re-reads of silver) | Total volume processed during the measurement window. |
| `total_s3_objects` | `sum(bucket_object_count)` | Total S3 objects across bronze/silver/gold at end of run. If this grows faster than retention can clean, metadata ops degrade. |
| `qph_degradation_pct` | first-half vs second-half median QpH | QpH trend across in-stream rounds (requires 4+ rounds). Positive = degradation. |
| `composite_qph` | QpH from the query engine benchmark | Query throughput against the gold layer. |

**BOUNDED BY trickle.** A continuous run feeds bronze at most
`max_files_per_trigger` files per trigger. When that trickle was set and the
pipeline kept pace, the throughput figures (`sustained_throughput_rps`,
`pipeline_throughput_gb_per_second` and compute efficiency in a continuous
run) are the offered load, not what the infrastructure can do. Kept pace
means ingested rows over offered rows is at least 0.99 and the lag at window
end (window seconds minus bronze's last write) is at most one trigger
interval. Offered rows are `released_rows`, so the ratio is the record's
`ingest_ratio`; when bronze took the whole corpus there was nothing left to
offer and the lag is not tested. A run whose `intake_limit` is
`trickle_rate` is labelled too. The experiment block records
`limits.trickle_bound` (`{kind, value, source, kept_pace, ratio, lag_s,
trigger_s, offered_rows, ingested_rows}`, with `not_measured` when
`kept_pace` is null and `lag_note` when the lag was not tested; null when
the trickle did not hold intake) and adds a line `trickle: max_files_per_trigger N (auto), the
pipeline kept pace` to `limits.bound`. When an input was not recorded,
`kept_pace` is null and the line says "the pipeline was not shown to keep
pace"; the throughput is still not shown as a capacity. The trickle is not one of `limits.bound_kinds`, so it is not
part of the experiment identity. The report labels the continuous rows/s
(headline card, Pipeline Stages summary, the bronze stream and stage rows),
GB/s and efficiency figures "BOUNDED BY: trickle (offered load, not
capacity)", with "trickle N files per trigger; this is the offered load,
not infrastructure capacity" as the tooltip, and counts the trickle among
the run's limits. `compare` marks the rows that depend on the trickle
(`sustained_throughput_rps`, `pipeline_throughput_gb_per_second`, compute
efficiency and `corpus_drain_seconds`) `capped`, with `capped_by` naming
the trickle, and each side's `bound` lists the trickle line. A record written before 1.7 gets the
same answer, computed when it is read. `released_rows` counts the trigger at
the window's edge, so a run whose last batch was still in flight can read up
to one trigger short (0.983 at an 1800 s window and a 30 s trigger); when the
shortfall is within one trigger's batch and the lag within one trigger, the
run is labelled with `kept_pace` null ("not shown to keep pace"), not shown
as a capacity.

### Per-Stage Metrics

Each pipeline stage (datagen, bronze, silver, gold, query) produces a uniform
`StageMetrics` record containing:

- **Timing**: elapsed seconds, start/end timestamps, success/failure
- **Data volume**: input and output size in GB, input and output row counts
- **Throughput**: `input_size_gb / elapsed_seconds` and `input_rows / elapsed_seconds`
- **Resources** (allocated): executor count, cores per executor, memory per executor
- **Continuous stream metrics** (zero for batch): latency in ms, freshness in seconds, batch count
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
stream jobs -- no `--generate` flag needed. The datagen stage is included in the
pipeline scorecard as a `datagen` stage with `stage_type="datagen"` and
`engine="datagen"`. It contributes to:

- `total_elapsed_seconds` -- wall-clock of the run, datagen included
- `total_data_processed_gb` -- includes datagen output size
- the denominator of `corpus_ingest_ratio` (datagen output rows)

Continuous scores do not include `time_to_value_seconds` or `scale_ratio`.

The datagen output size is measured from the bronze S3 bucket after generation
completes. There is no row estimate from the scale factor. In continuous mode
the row count is the sum of the rows every datagen pod reports writing, and
it is left unmeasured (0, with `ingest_ratio` and `pipeline_saturated` null)
when any pod did not report. In batch mode the datagen stage's row count is
not measured.

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

### Maintenance Scoring

When `pre_benchmark_maintenance: true` (the default), lakebench measures the
cost and value of table maintenance by running the benchmark twice:

1. **Pre-compaction benchmark** -- runs the 8-query power benchmark on
   uncompacted data (many small files from the pipeline).
2. **Maintenance** -- runs Iceberg `expire_snapshots` + `remove_orphan_files`
   (floor 24 h 10 min) + `rewrite_data_files` (compaction of silver and gold),
   or Delta `VACUUM` on Trino (Delta `OPTIMIZE` is never run). Every
   statement shares one 30-minute budget. The first statement timeout or the
   deadline stops the rest, and the benchmark runs anyway. Before v1.6 the
   expire and orphan statements always failed (LB-172, LB-174), so v1.5
   batch maintenance was compaction only.
3. **Storage settle wait** -- probes one query until storage has settled
   after the maintenance burst (see below).
4. **Post-compaction benchmark** -- runs the same query set on compacted data.

The scorecard then reports:

| Field | Description |
|-------|-------------|
| `pre_compaction_qph` | QpH before maintenance |
| `post_compaction_qph` | QpH after maintenance, over every query that succeeded in that run (this is the reported `composite_qph`) |
| `maintenance_value_pct` | QpH change from maintenance, computed over only the queries that succeeded in both runs. Null when maintenance did not run, compaction changed no files, no query succeeded twice, the rounds took one sample per query, or the difference is within noise (see below) |
| `maintenance_value_reason` | Why `maintenance_value_pct` is null, when it is |
| `maintenance_paired_queries` | Number of queries in that comparison |
| `maintenance_elapsed_seconds` | Wall-clock time spent on maintenance |
| `maintenance_pct_of_pipeline` | Maintenance time as a fraction of total pipeline time |
| `pre_compaction_file_count` | Data files before compaction |
| `post_compaction_file_count` | Data files after compaction |
| `compaction_ratio` | `pre / post` file count (higher = more compaction benefit) |
| `maintenance_settle_seconds` | Seconds from maintenance end until the storage settle probe was stable (see below). Not counted in `time_to_value_seconds` or any stage time |
| `maintenance_settled` | False when the post round ran on storage that had not settled |
| `maintenance_settle_capped` | True when the wait reached `max_seconds` |
| `maintenance_settle_verified` | False when there was no pre-maintenance probe time to check the probes against |

Both rounds are preceded by one unmeasured warm-up pass, which pays the first
touch of a snapshot (metadata and manifest reads, split planning caches).

The warm-up is not enough on its own. Maintenance returns when its SQL
returns, but the object store keeps working off the burst of deletes and
rewrites afterwards. On FlashBlade at c360 scale 10 the same compacted files
read QpH 546 about 2 minutes after maintenance, 569 at +15 minutes and 841 at
+35 minutes, against 828 before maintenance; AML scale 10 read 27% slow
straight after (LB-150). So between maintenance and the post round,
lakebench times one storage-bound probe query (by default the workload's
first scan-class query, a full scan of the table compaction rewrote) every
`interval_seconds` until two consecutive probes agree within
`tolerance_pct` and, when the pre round ran, neither is slower than that
query's pre-maintenance median by more than `tolerance_pct`. The second
condition matters: in the run above the +2 and +15 minute rounds agreed
within 4% while both were a third slow. Without a pre round (scale 50 and
above) there is no time to compare against: three consecutive probes must
agree, and the result is recorded with `maintenance_settle_verified: false`
because a slow plateau also agrees with itself. The reference is a bound,
not a target: when compaction speeds the probe up more than settling slows
it down, an unsettled probe can still pass it. When the probes are stable
but stay slower than the pre-maintenance time, the wait runs to the cap and
the reason says settling and a maintenance regression are not separable.

If the wait reaches `max_seconds` the post round still runs, and
`maintenance_value_pct` is null with the reason `storage did not settle
within N s`. Three failed probes in a row end the wait early with the same
effect. Every probe's offset and time is kept in `metrics.json` under
`maintenance_settle`. The wait adds to the run's wall clock, not to time to
value, stage times or `maintenance_elapsed_seconds`.

```yaml
architecture:
  benchmark:
    maintenance_settle:
      enabled: true          # false: run the post round straight after maintenance
      max_seconds: 2700      # 45 min: recovery took ~35 min in the measured case
      interval_seconds: 60
      tolerance_pct: 10.0    # unsettled rounds were 27-34% slow
      probe_query: null      # a query name; default is the first scan-class query
      probe_samples: 1       # timed runs per probe, median taken
```

Continuous mode does not wait. Its maintenance and compaction run on a
timer during the stream (`architecture.pipeline.continuous.retention_interval`,
`architecture.pipeline.continuous.compaction_interval`), and an in-stream benchmark round that
starts soon after one of them can read slow for the same reason. The
continuous scorecard does not separate those rounds: `in_stream_composite_qph`
is the median over all rounds and `qph_degradation_pct` compares the first
and second halves, so settling rounds are included in both. The median
limits the effect when only a few rounds land in a settling window.

Each in-stream round records `index`, `started_at`, `ended_at`, the queries
it executed (`executed_queries`, the ones that succeeded),
`executed_query_set_id` and `investigator_queries` (`included`,
`absent_no_cases`, `probe_failed`; null until the AML investigator rounds
set it, and for C360). A round whose query failed executed a smaller set
than the others, so its QpH is over different queries.
`scores.composite_qph_basis` says whether the rounds behind the in-stream
QpH (those with a QpH) executed more than one set (`blended`) and how many
rounds each set has; `scores.composite_qph_by_set` is the median per set. A
round recorded before 1.7 gets its set from its queries' success flags,
and `compare`, the perf gate and `reproduce` all read the basis from the
rounds, so an older run in which a query failed in some rounds and not
others reads blended too. When the rounds are blended, the aggregate
benchmark's `query_set_id` reads `blended` (otherwise it is, as before, the
set of every query name the rounds ran, even when one query failed in every
round). `compare` marks a continuous run's `composite_qph`,
`in_stream_composite_qph` and `qph_degradation_pct` `not_assessed` with the
hint "rounds ran different query sets (A)", and the perf gate and
`reproduce` leave the in-stream QpH out. When every round missed the same
query, the medians are over a smaller set than the run declared: `compare`
marks the same rows `not_assessed` ("every round missed a query"), and
`reproduce` reads the run's query set as the smaller set the rounds
executed (for a record from before 1.7, its pinned legacy id or `unknown`).

The value is reported only when the pre and post rounds are distinguishable
at the samples taken. Over the paired queries, each round's total seconds
can fall anywhere between the sum of per-query fastest samples and the sum
of per-query slowest samples. When the two ranges overlap, the value is null
and `maintenance_value_reason` reads `within noise: ...` with the raw
difference and both spreads. Samples inside a round run back to back, so
their range misses drift between rounds; a difference of 11% or less is also
reported as within noise, the drift measured between two rounds of the same
run with nothing changed between them (3-11%). With `iterations: 1` there is
no range and the value is null with the reason `one sample per query`. The pre round is kept
in `metrics.json` as `pre_compaction_benchmark`, samples included, so the
judgement can be rechecked.

To skip maintenance and run only the post-pipeline benchmark:

```bash
lakebench run --skip-maintenance config.yaml
```

### Effective Maintenance and Lakebench Limits

The experiment block in `metrics.json` records the maintenance a run
actually got (`experiment.effective_maintenance`), one label per operation
(Iceberg `expire_snapshots`, `remove_orphan_files` and `compaction`; Delta
`vacuum` and `compaction`):

| Label | Meaning |
|---|---|
| `ran` | The operation executed. |
| `ran_no_effect` | It executed at a retention nothing written in the window can meet (continuous Delta `VACUUM` at Delta's 7-day default). |
| `not_supported` | The composition cannot run it and it was not executed (DuckDB, Delta `OPTIMIZE`, Delta `VACUUM` on Spark Thrift). |
| `skipped_by_user` | Turned off: `--skip-maintenance`, `pre_benchmark_maintenance: false`, `--skip-benchmark`, or continuous compaction disabled. |
| `failed` | Attempted, and no statement succeeded. |
| `not_run` | The run ended before the maintenance phase. |

Two runs whose effective maintenance differs are comparable at best, not
like-for-like.

Compaction differs by engine: Trino runs `optimize` with a 128 MB file size
threshold, Spark Thrift runs Iceberg `rewrite_data_files` with its defaults.
A run whose compaction ran records the operation and its parameters in
`effective_maintenance.detail.operations.compaction` (`{"operation":
"trino_optimize", "params": {"file_size_threshold": "128MB"}}`, or
`iceberg_rewrite_data_files`; `mixed`, with the list in `operations`, when
compaction calls fell back to the other engine), taken from the code that
writes the statements. Delta compaction never runs. An exp2 block also names
it in the id (`compaction=ran(trino_optimize:128MB)`, or
`compaction=ran(mixed(iceberg_rewrite_data_files+trino_optimize:128MB))`); for a record from before it,
`compare` derives the operation from the query engine and table format. The
compaction operation is an execution condition: a Trino and a Spark Thrift
run that both compacted are not like-for-like.

Compaction is counted per table: a table counts as compacted only when
every statement for it succeeded. On Trino, a Customer 360 silver table
(partitioned by `interaction_date`) with more than 90 partitions is
compacted in chunks of at most 90 partitions, one `optimize ... WHERE
interaction_date ...` per chunk, in batch and continuous mode alike: Trino
refuses an `optimize` that rewrites files in more than 100 partitions, and
continuous silver has small files in every partition. A table whose chunks
partly succeeded is partial: it keeps `compaction=ran` in the id (which reads
`failed` only when no statement succeeded) and reads `compaction=partial` in
`detail_id`. `reasons` names each failed table as "compaction failed on
<table>: <error>", and `detail.compaction_failures` and
`detail.compaction_statements` list the failed tables and the statements
attempted.

`experiment.limits` records the Lakebench-imposed caps a run executed
under (among them the continuous trickle, `max_files_per_trigger`,
auto-capped at 50 files per trigger, the benchmark iterations and the
in-stream rounds), and `limits.bound` lists the ones that bound it: the
per-job executor caps (28 at most) and any concurrent executor budget,
auto-sizing cuts, TM alerts over capacity, AML rules skipped on a cap, the
pre-benchmark maintenance budget when it stopped maintenance early, and the
trickle line when the trickle held intake (BOUNDED BY trickle, above).
`limits.bound_kinds` names the same limits without their counts, the
trickle excepted. A number measured under a cap that bound is a property of
the cap, not of the infrastructure. `limits.headroom_pct` (batch,
diagnostic) gives each stage's headroom against the per-job timeout and the
timed benchmark's against its per-query timeout; see [aml-scoring.md](aml-scoring.md#where-gold-finalize-spends-its-time).

---

## Query Engine Benchmark

The query engine benchmark measures analytical query performance independently
from the pipeline. It runs the workload's query set (8 queries for Customer
360, 12 for AML: FQ1-FQ8 plus investigator queries IQ1-IQ4) against the
silver and gold tables using the active query engine and produces a QpH (Queries per Hour) score.

### Query Categories

The 8 Customer 360 queries are organized into five categories (the AML
set is listed in [Query Reference](query-reference.md)):

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

Executes every query in the set sequentially in a single stream. Each query is timed
individually.

```
QpH = (num_queries / total_seconds) * 3600
```

For example, if 8 queries complete in 40 seconds total, QpH = (8 / 40) *
3600 = 720.

Each query is timed `architecture.benchmark.iterations` times (default 3)
and scored by the median of its samples; the QpH above sums the medians.
`iterations: 1` is a quick run that measures no spread. The first failed
sample fails the query and stops its repeats, so a timeout is paid once.

`metrics.json` keeps every sample. Each query record carries `samples`,
`min_seconds`, `max_seconds` and `relative_range` ((max - min) / median), and
each round carries a `spread` block: `qph_low` and `qph_high` are the QpH of
the slowest and fastest round the samples allow, and `samples_per_query` is
the smallest sample count over the successful queries. The scorecard repeats
these as `benchmark_samples_per_query` and `qph_spread`. Records written
before per-query repeats have no `samples` and read as one sample.

Time cost at the default: each measured round takes three times as long,
plus the one warm-up pass. A c360 scale-100 round is about 8 queries x 60 s,
so the post-maintenance round goes from about 8 to about 24 minutes (+16
min). At scale below 50 the pre-maintenance round runs too, so the benchmark
phase adds about 2 x 2 x one round. Continuous runs keep one sample per query
in each in-stream round: gold changes under the round, and the rounds are
already the repeats.

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

Throughput and composite runs come from `lakebench benchmark --mode`.
`lakebench run` measures one power pass with one stream and a hot cache, and
refuses a config whose `architecture.benchmark` asks it for `throughput`,
`composite`, `cache: cold` or `streams` above 1, rather than recording a run
that did not happen; `metrics.json` records the power pass it ran.

### Cache Modes

Each benchmark mode supports `hot` or `cold` cache (`lakebench benchmark
--cold`; `lakebench run` uses hot). In cold mode the query
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
3. Runs the workload's full power benchmark (8 queries for Customer 360,
   12 for AML)
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
| 45 min | 300s | 300s | 40s | 7 |
| 60 min | 300s | 300s | 40s | 10 |

For at least 5 rounds (recommended for trend analysis), set
`run_duration >= warmup + 5 * (interval + round_time)`. With default
5-minute gold refresh, the shortest practical configuration for 5 rounds is
`warmup=300, interval=300, run_duration=1800` (30 min).

**Adaptive end-of-window guard:** Lakebench uses an adaptive guard to decide
whether to start a final round near the end of the measurement window. Before
any round has completed, a 60-second floor is used. After the first round
completes, the guard switches to 1.2x the observed round duration. This
scales with cluster performance -- fast clusters get more rounds, slow
clusters don't start rounds they can't finish.

**Q9 contention handling:** Q9 is the only query that reads the gold table.
Gold uses `createOrReplace()` which rewrites the entire table every refresh
cycle. If Q9 fails during a round, Lakebench retries up to twice with
30s/60s backoff. The contention status is recorded per-round as
`q9_contention_observed` and `q9_retry_used`.

**Gold event age (diagnostic, not freshness):** Each round also records how
old gold's newest event date is at query time (query time minus
`MAX(interaction_date)`, day resolution). That tracks where the corpus's event
timestamps sit: a corpus dated 2024-12-18 to 2025-01-01 reads about 635 days in
2026, however fresh gold is. The median appears as
`query_time_event_age_seconds` and each round's value as
`round_meta.gold_event_age_seconds`. Before v1.6 the same figure was written as
`query_time_freshness_seconds` / `gold_freshness_seconds` and printed as the
continuous Pipeline Score; the score line now always shows
`data_freshness_seconds`, the scored freshness.

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

**Customer 360 expected results.** A Customer 360 batch run on the
cluster checks silver and gold against what the generator wrote, and
records every check in `metrics.json` as `c360_correctness`. Sixteen checks
gate the run: the fifteen invariant and reconcile checks (bronze rows equal
to the rows datagen was sized to write, the silver invariants, bronze to
silver to gold reconciliation and the gold KPI identities) and the overall
average transaction value. The run fails when one of them fails or could
not be evaluated, or when gold-finalize logged no facts or the check itself
raised. The other
statistical checks and the benchmark row counts are printed and recorded but
do not fail the run; a failed one is also listed in `metrics.json` under
`verdict.qualifiers.c360_failed_not_gating`. A `--local` run and a
`--stage` run make no Customer 360 check.

### Continuous Mode

**Data Freshness** (`data_freshness_seconds`) is the gold staleness headline:
the maximum of the streams' measured freshness inside the window. Each gold
refresh measures, after its write, the age of the newest silver row it
read (`current_timestamp` minus the newest silver processing timestamp), so
the figure tracks the silver trigger and the gold write time rather than
`gold_refresh_interval`. Lower is better.

**Sustained Throughput** (rows/sec) measures the steady-state ingestion rate
through bronze. This is unique rows only -- gold-stage re-reads of silver data
are excluded.

**Stage Latency Profile** is a three-element vector `[bronze_ms, silver_ms,
gold_ms]` showing per-stage micro-batch processing latency. If one stage has
significantly higher latency, it is the bottleneck.

**Offered load.** Continuous mode trickles a finite corpus. Datagen writes
the whole scale's corpus at full speed (one measurement, n=1, on the
pre-v1.6 generator: 1 TB in 121 s on 44 pods at scale 100,
run-20260924-201745-cb354f)
and bronze reads it at a fixed rate: `max_files_per_trigger` files of about
64 MB per `bronze_trigger_interval`. A run measures sustained throughput and
freshness at that offered load.

**Data must keep arriving through the window.** `max_files_per_trigger` is
unset by default (auto): the run derives the most files per trigger, up to
50, whose arrival still lasts 1.2 x `run_duration`, from the nominal corpus
size, and prints the value and the arrival it gives. At the defaults (30 s
trigger, 1800 s window):

| Corpus | Files per trigger | Arrival |
|---|---|---|
| c360 scale 1 (~160 files) | 2 | ~2,400 s |
| c360 scale 10 (~1,600 files) | 22 | ~2,190 s |
| c360 scale 100 | 50 (the Lakebench-imposed cap) | ~9,600 s |
| AML scale 1 | 1 | ~4,050 s |
| AML scale 10 | 18 | ~2,250 s |

So the offered load now grows with scale up to 50 files per 30 s (about
107 MB/s, 25,818 rows/s for c360), where it stays. That ceiling is a
Lakebench-imposed cap, not an infrastructure limit: at scale 100 the window
takes about 19% of the corpus, which the scorecard reports as
`intake_limit: trickle_rate`, not saturation. A config that sets
`max_files_per_trigger` explicitly to a value that would offer the corpus in
less time than the window is refused at run start, with the value to set
(for c360 scale 1 and a 600 s window, 6 or lower). Before this change the
default was a fixed 50, which offered the c360 scale-1 corpus in about 96 s.
Whatever the estimate, the continuous gate decides on what the run did (see
Continuous Gate below).

**Continuous gate.** A continuous run passes only on continuous processing
inside the measurement window. It is refused before it starts when
`run_duration` is shorter than 3 x `gold_refresh_interval` (two gold
refreshes cannot be guaranteed inside it). It fails when:

- a stream never reached RUNNING, was not RUNNING when the window closed, or
  restarted inside it (a new driver pod or another submission);
- bronze ingested no rows inside the window (in particular when it drained
  the corpus before the window opened), wrote fewer than 2 batches inside
  it, or wrote its last batch before half the window had passed;
- silver committed fewer than 2 micro-batches with rows inside the window
  after bronze's first write in it (a backlog from before the window does
  not count);
- gold refreshed on new silver data fewer than 2 times inside the window
  after that first write (a cycle tagged `(silver idle)`, one that read no
  silver, or one that read no more silver rows than the cycle before, does
  not count);
- gold freshness was not measured inside the window;
- a stream's driver log could not be read.

When the corpus runs out between half and all of the window the run passes
and says so in `window_arrival_fraction`; freshness then leaves out the gold
cycles after silver stopped growing.

Submission failures (for example a Maven download that arrived truncated)
are printed and journaled as they happen and recorded per stream in
`metrics.json` (`streaming[].submission_failures`), and the time each stream
reached RUNNING is recorded as `running_at`.

**Result check.** In-stream rounds read tables still being written, so their
results are not compared. After a run that passed its gates, the CLI keeps
the streams running until the whole corpus has reached gold (every datagen
row in bronze, every bronze row committed by silver, and a gold refresh that
read silver after its last commit), for at most 1800 s, then stops the
streams and runs the query set once over the settled tables. A query that
fails there fails the run, as in batch. Its result
fingerprints are the run's results (`continuous.result_check` and the
experiment block's `results`), the same as a batch run's, so `compare`, the
perf gate and `reproduce` hold continuous runs to "no comparison without
equivalent results". A run whose corpus does not settle in time, or that ran
with `--skip-benchmark` or without a query engine, records why in
`results.not_checked` and is never presented as comparable. AML continuous
runs are not result-checked: detection and TM operations passes run on a
timer, so their tables depend on when the passes ran relative to arrival.
None of this time is part of the window.

**Ingestion Completeness** (`ingest_ratio`) is the share of the rows the
trickle had released to bronze by the window's end that bronze took
(`released_rows`, see the table above). Before 1.6 it was the share of the
whole corpus; that figure is now `corpus_ingest_ratio`, and a short
`corpus_ingest_ratio` says only that the corpus outlasted the window. When
`ingest_ratio` is short, `intake_limit` says why:

- `trickle_rate`: bronze ran a micro-batch on at least 90% of the window's
  triggers and all but 5% (at least one) of the triggers between its first
  and last batch (so a mid-window stall shows), each batch inside the trigger, with corpus
  left. The trickle, not the pipeline, bounded intake. If silver also kept up
  (its batches finished inside the silver trigger and it committed all but
  two silver triggers and one bronze trigger of what bronze took), the run is
  not saturated, the report shows a warning rather than a failure, and
  `corpus_drain_seconds` gives the window that would drain the corpus. A
  silver that logged batches but no commit is stuck and saturated. If silver
  logged nothing, `pipeline_saturated` is null (unknown) and the report warns.
- `bronze_capacity`: bronze ran back to back. Its processing is the limit and
  rows/s is its capacity.
- `below_bronze_capacity`: bronze was idle for part of the window but did not
  keep to its trigger: a late start or a stall. The driver log says which.

**Pipeline Saturated** is true when `ingest_ratio < 0.95` and the pipeline did
not keep pace with the load offered to it. When true, add executors to the
stage that fell behind. To offer more load, raise `max_files_per_trigger` (or
shorten `bronze_trigger_interval`) and size bronze-ingest and silver-stream for
it; to drain a larger corpus at the same load, lengthen the window.

---

## Tuning a Continuous Pipeline

The continuous scorecard reveals imbalances between pipeline stages. This
section walks through common patterns and how to fix them.

### Reading the Stage Latency Profile

The `stage_latency_profile` shows per-stage micro-batch processing latency
in milliseconds. Compare each stage's latency to its trigger interval:

| Stage | Trigger Interval | Healthy Latency |
|---|---|---|
| bronze | 30 seconds | < 30,000 ms |
| silver | 60 seconds | < 60,000 ms |
| gold | 5 minutes | < 300,000 ms |

If a stage's latency exceeds its trigger interval, micro-batches pile up
and freshness degrades. That stage is the bottleneck.

### Ingest Ratio Above 1.0

`released_rows` is computed from the corpus's mean rows per file, so a run
whose early files are larger than the mean can read slightly above 1.0; that
still means bronze kept up. A value well above 1.0 means bronze ingested more
rows than the trickle had released (for example, data left in the bronze
bucket from an earlier run). The HTML report shows a ratio above 1.05 as a
warning, and the perf gate refuses a run whose `ingest_ratio` is above 1.05. The pipeline is saturated only when
`ingest_ratio < 0.95` and `intake_limit` does not show the trickle bounding
intake.

### Gold Re-Read Amplification

Gold reads the entire silver table on every refresh cycle. With a 5-minute
`gold_refresh_interval` and a 30-minute `run_duration`, gold executes 6
refreshes. If silver has 93M rows, gold's `input_rows` will be
approximately `93M * 6 = 558M` (or whatever fraction of silver was available
at each refresh point).

This is expected with `createOrReplace()` -- gold rewrites the whole table
each cycle for consistency. The gold row count in the scorecard reflects
total rows read across all refresh cycles, not unique rows.

To reduce gold re-read amplification:
- Increase `gold_refresh_interval` (fewer rewrites, higher staleness)
- Decrease `run_duration` (fewer total cycles)

### Reducing Data Freshness

`data_freshness_seconds` is the worst gold staleness measured in the
window. Each gold refresh measures it right after its write, as the age of
the newest silver row it read, so it is made of the silver trigger delay
(how long a row waits in silver before gold can see it) plus the time gold
takes to read silver and write. `gold_refresh_interval` does not bound it:
the measurement is taken at the write, not between writes. What a consumer
sees between refreshes can be up to one `gold_refresh_interval` older.

To lower freshness:
1. Speed up each gold rewrite: add gold executors
   (`gold_refresh_executors`).
2. Decrease `silver_trigger_interval`, so silver rows are visible to gold
   sooner. This adds silver micro-batches and their commit cost.

### Stage-by-Stage Tuning

**Bronze latency too high (> trigger interval):**
- Increase `bronze_ingest_executors`. Bronze is I/O-bound (reading Parquet
  from S3). More executors = more parallel reads.
- Reduce `max_files_per_trigger` to process smaller batches (lower latency
  per batch, more batches total).
- Check if datagen `parallelism` is too high -- 16 pods writing at full
  speed can overwhelm 3 bronze executors.

**Silver latency too high (> trigger interval):**
- Increase `silver_stream_executors`. Silver applies 5 column transforms
  per micro-batch. It is CPU-bound at large batch sizes.
- Increase `silver_trigger_interval` to process larger, less frequent
  batches (better throughput, worse per-batch latency).

**Gold latency too high (> refresh interval):**
- Increase `gold_refresh_executors`. Gold reads the full silver table and
  aggregates it. At large silver tables this is the slowest stage.
- Increase `gold_refresh_interval` to give gold more time per cycle.
  Trade-off: higher data freshness (more stale).

### Example: Balancing a Scale-50 Continuous Run

Starting point (imbalanced):

```yaml
# Bronze: 3 executors, 111s/batch latency (trigger: 30s) -- bottleneck
# Silver: 10 executors, 53s/batch latency (trigger: 60s) -- healthy
# Gold: 3 executors, 124s/batch latency (refresh: 5 min) -- healthy
# Freshness: 275s (near the 5-min gold refresh floor)
```

Tuned configuration:

```yaml
platform:
  compute:
    spark:
      bronze_ingest_executors: 6   # was 3 (auto) -- fix bronze bottleneck
      silver_stream_executors: 10  # keep -- silver is balanced
      gold_refresh_executors: 4    # slight bump for headroom

architecture:
  pipeline:
    continuous:
      gold_refresh_interval: "3 minutes"   # was 5 min -- lower freshness
      benchmark_warmup: 300                # must be >= gold interval
      benchmark_interval: 300              # must be >= gold interval
      run_duration: 2700                   # 45 min -- more benchmark rounds
```

Expected result: bronze latency drops to ~55s/batch, data freshness
improves to ~150--180s, and the longer run duration yields more benchmark
rounds for trend analysis.

### Diagnostic Checklist

| Symptom | Likely Cause | Fix |
|---|---|---|
| `pipeline_saturated: true` | A stage could not keep pace with the trickle (`intake_limit` names bronze; otherwise silver) | Add executors to that stage |
| `corpus_ingest_ratio` < 1 with `ingest_ratio` near 1.0 | Corpus larger than trickle rate x window | Not saturation. Lengthen the window to `corpus_drain_seconds`, or raise `max_files_per_trigger` and size the streams for it |
| `ingest_ratio` < 0.95 | Bronze fell behind the rows the trickle released; `intake_limit` says whether bronze capacity or a stall bounded it | Add bronze-ingest executors, or check the driver log for a late start or stall |
| `ingest_ratio` well above 1.0 | Bronze took more rows than the trickle released (for example, data from an earlier run) | Empty bronze (`lakebench clean bronze`) and rerun; the report warns above 1.05 and the perf gate refuses the run |
| `data_freshness > 300s` | Gold refresh interval too long | Decrease `gold_refresh_interval` |
| Bronze latency >> 30s | Too few bronze executors | Increase `bronze_ingest_executors` |
| Silver latency >> 60s | Too few silver executors | Increase `silver_stream_executors` |
| Gold latency >> refresh interval | Silver table too large for gold executors | Increase `gold_refresh_executors` |
| QpH dropping across rounds | Table growth degrading queries | Add Trino workers or memory |
| Q9 contention > 20% | Benchmark rounds colliding with gold rewrites | Increase `gold_refresh_interval` or `benchmark_interval` |
| `total_s3_objects` growing unbounded | Maintenance not keeping pace, or failing | Iceberg: check the journal's Iceberg maintenance events for timed-out or failed statements, then decrease `retention_interval`. Lowering `retention_threshold` below `1h` does nothing while streams are live (expiry is floored at 1 h, orphan removal at 24 h 10 min). Tables created before metadata retention was added keep every `metadata.json`; recreate them with a fresh deployment. Delta: continuous mode has no effective table maintenance in v1.6 |

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
  "maintenance_policy_id": "m2-2026-09-26",
  "provenance": {
    "lakebench_version": "1.7.0",
    "git_sha": "<40-char commit, or null when unknown>",
    "git_dirty": false,
    "install": "checkout",
    "end_sample": { "git_sha": "...", "code_changed_during_run": false },
    "config_sha256": "<sha256 of the config file>",
    "config_path": "/abs/path/to/config.yaml",
    "scripts_sha256": "<sha256 over the Spark scripts ConfigMaps>",
    "scripts_maps": { "common": "<sha256>" },
    "deps": { "pinset_sha256": "<sha256>", "request_sha256": "<sha256>", "...": "...", "pods_checked": 1, "pod_mismatches": [] },
    "images_observed": {
      "spark_driver": "<registry>/spark@sha256:...",
      "spark_executor": "<registry>/spark@sha256:...",
      "trino_coordinator": "<registry>/trino@sha256:..."
    },
    "scratch_as_ran": {
      "silver-build": { "size_limit": "300Gi", "storage_class": "px-csi-scratch" }
    }
  },
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

`maintenance_policy_id` names the table-maintenance policy the run was
measured under (see `docs/perf-regression-gate.md`). `provenance` records
what produced the run. The experiment block's `lakebench` copy carries the
code fields, `deps` and `images_observed`: the dependency pinset
(`deps.pinset_sha256`) is part of the experiment identity, and `compare`
reads the observed image digests as an architecture key, comparing each
role both runs observed (a role seen by one run only, or a run that
observed none, is not a difference). The rest is provenance only:

- `lakebench_version`, `git_sha`, `git_dirty`, `install` and
  `tree_sha256`. From a git checkout (`install: checkout`) the commit and
  whether the package had uncommitted changes; from a pip-installed wheel
  (`install: wheel`) the commit and tree state the wheel was built from,
  which the build writes into the package. `unknown` is any other install
  (the single-file binary), with a null commit. `tree_sha256` hashes the
  package's files on disk.
- `end_sample`: the same fields read again when the run ends, and
  `code_changed_during_run`: true when the package files changed, or the
  commit, version or install did. An edit inside an installed wheel or an
  already-modified checkout counts. When the code changed, a support state
  of `supported` is withdrawn to `unverified`.
- `config_sha256` (the same value the perf gate checks in
  `config_snapshot`) and `config_path`, the config file as given, made
  absolute.
- `scripts_sha256`, `scripts_maps` and `scripts_files_sha256`: the Spark
  scripts ConfigMaps the run applied and read back.
- `deps`: the deployment's dependency set the run checked before anything
  was submitted: `pinset_sha256` (the set's identity, which compare reads),
  `request_sha256`, the repositories and index, the files per group with
  their sha256, `resolved_at` and the server pod. At run end the Spark
  Thrift or DuckDB pods are checked against it: `pods_checked` (how many),
  `pod_mismatches` (pods on another set, which fail the run),
  `pods_check_error` (the pods could not be read, which fails the run too),
  or `pods_check_skipped` (why the check did not read: interrupted, a
  prerequisite failed, the namespace went, no cluster). `"not_recorded"`
  for a `--local` run and for records made before 1.7.
- `images_observed`: the image digests this run's pods ran (`imageID` from
  the pod status): the Spark driver and executors of the run's own
  applications, and the running Trino coordinator and Spark Thrift server.
  A batch stage is read while it runs, until a driver and an executor
  digest are seen (at most four reads, 15 s apart, and once more after the
  stage when no driver digest was seen); a stage whose driver or executor
  was never seen
  is listed in `images_observed_missing`. A continuous run is read when
  the streams are running and again before they stop. The first digest
  seen per role is kept; a later different one is listed in
  `images_observed_changed`. When pods cannot be listed (RBAC) it reads
  `{"not_observed": "<reason>"}`. The image references the config asked
  for are in `config_snapshot.images`.
- `scratch_as_ran`: per Spark job, the executor scratch PVC size and
  storage class in the SparkApplication as the cluster held it (both null
  when the job had no scratch PVC).

### HTML Reports

Every `lakebench run` delivers `lakebench-output/runs/run-<id>/report.html`
once, at the end of the run. That file is the shareable artifact.

Print the summary of the latest run (does not modify `report.html`):

```bash
lakebench report
```

List all available runs:

```bash
lakebench report --list
```

Print the summary for a specific run:

```bash
lakebench report --run 20260201-143052-a1b2c3
```

Regenerate the HTML from saved metrics. This never overwrites the
delivered file; it writes a fresh copy under `lakebench-output/reports/`
with a UTC timestamp in the name:

```bash
lakebench report --run 20260201-143052-a1b2c3 --render
# writes lakebench-output/reports/report-20260201-143052-a1b2c3-<UTC>.html
```

The HTML report includes summary cards (total time, QpH, time-to-value,
pipeline throughput), a pipeline scorecard stage matrix, a per-query
breakdown of the query engine benchmark, and the configuration snapshot.

### Viewing Results on the Command Line

Use `lakebench results` to display the stage-matrix view in the terminal:

```bash
lakebench results                      # latest run, table format
lakebench results --format json        # JSON output
lakebench results --run <id>           # specific run
```

Use `lakebench report` (no flags) to print key scores directly in the terminal
without opening or regenerating the HTML report:

```bash
lakebench report                              # latest run
lakebench report --run <id>                   # specific run
lakebench report --metrics <dir>              # custom metrics dir
```

The summary output includes a per-stage table (elapsed time, data volume,
throughput, executor count) and mode-appropriate scores -- time-to-value and
throughput for batch, data freshness and sustained throughput for continuous.

## HTML Report Layout

The HTML scorecard (`lakebench report`) is organized in three layers: a verdict
at the top, diagnostic charts in the middle, and raw evidence at the bottom.
Some sections only appear in batch or continuous mode as noted below.

### Header

The header shows the deployment name, run ID, and an overall status badge:

- **PASSED** (green) -- pipeline completed, data complete (scale/ingest ratio
  0.95--1.05), all jobs succeeded, no failed queries.
- **WARNING** (amber) -- pipeline completed but a ratio or job raised a
  non-fatal flag.
- **FAILED** (red) -- a stage or query failed, or data completeness is below
  threshold. A run stopped by Ctrl-C or SIGTERM also shows as failed, with
  the interrupt as its reason; its verdict is INTERRUPTED (see
  [Interrupting a run](cli-reference.md#run)).

A one-line context banner below the header shows pipeline mode (Batch /
Continuous), Customer360 scale factor, the recipe string
(`catalog-format-engine-query_engine`), and wall-clock duration.

### Summary Cards

Five primary KPI cards. The cards change with pipeline mode:

**Batch:** Time-to-Value, Data Processed (GB), Pipeline Throughput (GB/s), QpH,
Job Status (pass/fail count).

**Continuous:** Data Freshness, Sustained Throughput (rows/s), Compute
Efficiency (GB/core-hour), In-Stream QpH (median across rounds), Total
CPU-hours.

### Bottleneck Identification (batch and continuous)

A stacked bar chart showing time and compute distribution across pipeline
stages. Each stage is color-coded (bronze = amber, silver = indigo, gold =
gold, query = cyan). The chart identifies which stage dominates elapsed time
or compute. In continuous mode the chart uses micro-batch latency instead of
elapsed seconds.

### Data Validity (batch and continuous)

Green/red status indicators for data quality checks:

- **Scale Ratio** (batch) or **Ingest Ratio** (continuous) -- confirms the run
  processed the expected data volume. Red when below 0.95 or above 1.05.
- **Job Success** -- counts of passed and failed batch and continuous jobs.

If any indicator is red, cross-run comparisons are unreliable.

### Stability Over Time (continuous only)

A line chart showing the QpH trend across in-stream benchmark rounds. Requires at least 5 rounds for trend analysis.
Helps identify performance degradation over time as table state grows.

### Q9 Contention (continuous only)

A table of Q9 contention events across benchmark rounds. Q9 reads the gold
table, which is rewritten every refresh cycle via `createOrReplace()`. This
section shows when Q9 collided with a gold rewrite and whether retries were
needed.

### Batch Job Performance (batch only)

A table with one row per Spark job (bronze-verify, silver-build,
gold-finalize). Columns: job name, status, elapsed time, input/output data,
throughput, executor count, cores, and total CPU seconds.

### Continuous Pipeline (continuous only)

A table with one row per stream job. Columns: job type, status, rows
processed, throughput (rows/s), freshness, executor count, and compute
resources.

### Pipeline Stages (batch and continuous)

Per-stage matrix table. In batch mode: GB in/out, rows in/out, GB/s, rows/s.
In continuous mode: rows/s, micro-batch latency, freshness.

### Query Performance (batch and continuous)

Performance table for the engine benchmark (8 queries for Customer 360,
12 for AML). Columns: query name,
display name, category, elapsed time, rows returned, and pass/fail status.
The benchmark mode (power, throughput, composite), stream count, and final QpH
appear in a summary row.

### In-Stream Benchmark Rounds (continuous only)

A transposed table with queries as rows and benchmark rounds as columns.
Shows per-query times across rounds plus statistical measures (median, min,
max). Each round header includes its QpH, gold freshness, and contention
status.

### Configuration

Key configuration parameters extracted from the run: scale factor, S3 endpoint,
executor specifications, catalog type, table format, and query engine settings.

### Platform Metrics (when observability is enabled)

Per-stage pod resource summary: CPU average/max, memory average/max, and pod
counts. Infrastructure pods (Hive, Polaris, Trino, Postgres) are shown
separately from pipeline pods.

---

## Comparing Runs

`lakebench compare SIDE_A SIDE_B` compares stored run records: each side is
run ids, run directories, a `series:<id>` or a config (its latest run, or
every member of that run's `run --repeat` series). It runs nothing; run each
side first with `lakebench run`. It checks the runs' experiment blocks and
results before it shows any number, and the exit code is the verdict:

| Verdict | When | Exit |
|---|---|---|
| LIKE-FOR-LIKE | Same experiment, equal benchmark results, same execution conditions | 0 |
| NOT COMPARABLE | Different experiments (workload, corpus, seed, scale, mode and so on), different benchmark results, a run that did not pass, a record without an experiment block, or a side whose runs are not one experiment | 10 |
| NOT ESTABLISHED | Nothing contradicts the pair, but a side has no checked results: `--skip-benchmark`, a `*-none` recipe, or a continuous run whose result check did not settle | 11 |
| NOT LIKE-FOR-LIKE | Comparable, but an execution condition differs | 12 |
| CONFOUNDED | Comparable, but the architecture and the system both differ | 13 |

The execution conditions are effective maintenance and the compaction
operation it ran (Trino `optimize` at 128MB and Spark Thrift Iceberg
`rewrite_data_files` are different operations), maintenance settings,
benchmark iterations and mode, in-stream rounds (continuous), and the
Lakebench limits that bound. A delta between runs whose conditions differ
may come from those conditions rather than the architecture. Each verdict
is printed with the one condition the pair is missing and, where one
exists, the command that supplies it. Medians, ranges and n are shown for every score, with the
delta of medians where the pair is comparable; no winner is named in this
release. See the [CLI reference](cli-reference.md#compare).

The architecture (the recipe, its components and versions, the query access
path, the dependency set) and the system (the cluster and object store,
recorded as `experiment.system_identity`) are not conditions; they are what
a comparison varies. A pair that differs in the architecture alone is an
architecture differential, and one that differs in the system alone is a
system differential. A pair that differs in both is **confounded**: no
difference can be put down to either, and the table lists "architecture
and system both differ" with the not like-for-like reasons. Two runs whose
only architecture difference is the dependency set (same composition,
different jars) are not like-for-like. Records written before 1.7 carry no
system identity, and two of them are assumed to share a system. A record
with a system identity against one without counts as a different system,
as does a 1.7 run whose system could not be sampled, or two observations
with no part in common; such a pair is confounded when the architecture
also differs, and is never called a system differential. A pair whose observations agree on every part both
read but cannot show one cluster (an API server CA that could not be read),
or two `--local` runs, which record no part, is treated as one system but
never as a repeat of the same experiment. Each
run also records the allocatable CPU and memory of the schedulable workers
and the CPU and memory other namespaces' pods requested, platform pods
included, at run start and when the record is saved
(`experiment.observed`), as evidence only.

For ad hoc analysis the metrics JSON can also be diffed directly. Key
fields:

- `scorecard.time_to_value_seconds` -- primary batch score
- `scorecard.pipeline_throughput_gb_per_second` -- throughput efficiency
- `scorecard.composite_qph` -- query performance
- `stage_matrix` -- per-stage breakdown for identifying bottlenecks
- `config_snapshot` -- captures scale factor, executor counts, memory,
  and all tuning parameters

To compare two runs by hand, load both `metrics.json` files and diff the
`scorecard` and `stage_matrix` sections, after checking that their
`experiment` blocks match and their results are equivalent. The `config_snapshot` in each run records the
exact configuration used, so you can attribute performance differences to
specific changes (scale factor, executor count, memory, engine workers, etc.).

Before comparing, check `scale_ratio` (batch) or
`ingest_ratio` (continuous) to confirm both runs processed
the expected data volume. Comparing runs with incomplete data gives misleading
results.
