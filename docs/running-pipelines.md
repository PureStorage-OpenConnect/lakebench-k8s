# Running Pipelines

Guide: run the pipeline with `lakebench run` and read what it measured.

`lakebench run` runs a sequence of Spark jobs that turn raw bronze data into
queryable gold tables, then a query benchmark on the active engine. It
records metrics at every stage. [CLI Reference](cli-reference.md#run) lists
every flag, refused combination and exit code.

## Basic usage

```bash
lakebench run my-config.yaml --generate   # first run: generate the corpus, then the pipeline
lakebench run my-config.yaml              # later runs: reuse the corpus in bronze
```

- `run` runs the stages in order, waits for each, runs the benchmark and
  saves metrics.
- Without `--generate`, `run` reuses the corpus already in bronze. On an
  empty bronze bucket it exits 4 and names `--generate`.
- The output shows per-stage timing, a QpH (queries per hour) score and the
  path to the metrics file.

Common variations:

```bash
lakebench run my-config.yaml --stage silver-build          # one stage only
lakebench run my-config.yaml --timeout 7200                # 2 h per job
lakebench run my-config.yaml --skip-benchmark              # no query benchmark
lakebench run my-config.yaml --generate --timeout 7200     # datagen, then everything
```

The per-job timeout defaults to `max(3600, scale * 120)` seconds. AML adds
900 s and never goes below its bronze-verify budget for the scale.

## Batch mode (default)

Four stages run in sequence:

```
bronze-verify --> silver-build --> gold-finalize --> benchmark
```

Per-executor cores, memory and scratch for each job are in
[Job Profiles](component-spark.md#batch-jobs).

### Stage 1: bronze-verify

- Reads every raw Parquet file in the bronze bucket and checks it against
  the schema.
- Flags nulls, format inconsistencies and duplicates, and writes summary
  metrics.
- I/O-bound: it exercises the S3 read path.
- Financial (AML) falls back to a CTAS rewrite when `add_files` fails or the
  source exceeds 1.5 TiB or 800,000 files; the profile is sized for it
  because AML sources cross these from about scale 5 (job.py profile note).
  The CTAS spills about twice the per-executor input to local disk. That is why its scratch PVC
  is larger.

### Stage 2: silver-build

The core transformation. It reads validated bronze and writes an enriched
Iceberg table to the silver bucket with five transforms: email
normalization, phone normalization, geo enrichment, customer segmentation
and quality flagging.

It picks a write strategy from the measured bronze size. These are
silver-build strategies, not pipeline modes:

| Strategy | Bronze size | What it does |
|---|---|---|
| `simple` | < 100 GB | Standard Spark processing. |
| `streaming` | >= 100 GB | One pass, direct write, no shuffle (column transforms only). Iceberg hash distribution clusters rows by partition. |

To force one, set `spark.lb.silver.strategy` (or the `LB_SILVER_STRATEGY`
environment variable) to `simple` or `streaming`. `salted` is refused: the
silver-build job stops at start and the stage fails.

### Stage 3: gold-finalize

Aggregates silver into daily KPI summaries for the executive dashboard:
daily active customers, total revenue, conversions, engagement scores and
churn risk counts. The output is a compact gold Iceberg table partitioned by
date.

### Stage 4: benchmark

Runs the workload's query set on the active engine against silver and gold:
8 queries for Customer 360, 12 for AML.

| Class | Customer 360 | AML | What it tests |
|---|---|---|---|
| scan | Q1 | FQ1 | Full table scan with aggregation. I/O bound. |
| filter/prune | Q2, Q4 | FQ2, FQ6 | Date-range filtering and predicate pushdown with GROUP BY. |
| aggregation | Q3, Q7 | FQ3, FQ7 | Hash aggregation, conditional SUM(CASE), funnels and concentration. |
| analytics | Q5, Q6 | FQ4 | Window functions and CTEs. |
| operational | Q9 | FQ5, FQ8 | Gold and alert reads, as a dashboard or triage screen runs them. |
| investigator | | IQ1-IQ4 | One investigator's case lookups: customer 360, 12-month activity, two-hop network, aged cases. |

The default is power mode: one stream, hot cache. QpH is
`(number_of_queries / total_seconds) * 3600`.

## Multi-Cycle Batch

Set `pipeline.cycles` to 2 or more to run N batch iterations. Each cycle
generates data for its part of the timestamp range, then runs bronze-verify,
silver-build and gold-finalize.

```yaml
architecture:
  pipeline:
    cycles: 3
    pre_benchmark_maintenance: true
```

- Cycle 1 creates the tables with a full overwrite.
- Later cycles are incremental: silver appends new rows, gold merges on a
  date watermark. Each cycle is a new "day" of table growth.
- The run generates on its own. `--generate` is refused (exit 2): a corpus
  generated first would be read again by cycle 0.
- A non-empty datagen prefix is refused (exit 3) unless you pass
  `--regenerate`, which clears it on a bucket this deployment created.
- To rerun the cycles over the same corpus, add `--skip-generate`. Each
  cycle then runs its stages over its own slice with no datagen. The corpus
  series marker must show the multi-cycle generate finished for this
  config's cycle count, windows and generation (exit 3 otherwise).
- An AML multi-cycle run cannot reuse its corpus: its stages read the whole
  bronze prefix every cycle.

The terminal shows cycle headers (`Cycle 1/3`, `Cycle 2/3`) and per-cycle
timing. After the last cycle, pre-benchmark maintenance runs compaction and
snapshot expiry (if enabled), then the benchmark measures QpH on the grown
table.

Metrics carry a `cycles[]` array: per-cycle datagen timing
(`datagen_skipped` under `--skip-generate`), job metrics, and table health
(data file and snapshot counts from Iceberg system tables). The
`cycle_progression` score shows how elapsed time and table state change
across cycles.

## Repeated batch runs

`lakebench run --repeat 3` runs the batch pipeline three times as one series
over one corpus. Repetition 1 may generate; repetitions 2 and 3 rebuild
silver and gold from the same bronze. If bronze changes between repetitions
the series stops (exit 3). Records carry `series {id, index, size}`, and the
manifest is `lakebench-output/series/<id>.json`. The refused combinations
and exit codes are under "Repeating a run" in
[CLI Reference](cli-reference.md#run).

## Continuous Mode

Set continuous mode in the config, or pass `--continuous` for one run:

```yaml
architecture:
  pipeline:
    mode: continuous              # batch | continuous
```

```bash
lakebench run my-config.yaml --continuous
```

With `pipeline.mode: continuous`, `run` needs no flag. Three Spark
Structured Streaming jobs run at once:

```
bronze-ingest + silver-stream + gold-refresh  (concurrent)
```

- **bronze-ingest** reads new Parquet files from S3 as they land and writes
  a bronze Iceberg table.
- **silver-stream** reads the bronze table as a stream, applies the silver
  transforms and writes the silver table.
- **gold-refresh** updates gold from new silver rows (see below).

Each stage reads only what the stage before it committed. The commit is the
handoff.

### Datagen and offered load

- Datagen starts once the three streams are running. No `--generate` is
  needed; that flag is for batch only.
- The measurement window opens at datagen's first file. Lakebench warns if
  none arrives within 300 s.
- Datagen writes for the whole window. Every pod writes as fast as it can
  until the window ends, then the run stops it.
  - AML: the 24-month history, then successive 24-month periods of the same
    bank.
  - Customer 360: successive time slices.
- A restarted datagen pod resumes where it stopped. An AML pod reports one
  metrics line covering every period it wrote.

Scale sets the **offered load**, the data rate datagen offers. It is part of
the workload definition and the same on every system:

| Workload | Offered load per scale unit | At scale 10 | Datagen MB/s per core |
|---|---|---|---|
| AML | 4 MB/s | 40 MB/s | 28 |
| Customer 360 | 10 MB/s | 100 MB/s | 104 |

- Unset, `workload.datagen.cpu` and `parallelism` are the cores that load
  needs, in pods of up to 8 cores. Set them to offer a different load.
- Bronze reads with no per-trigger limit unless `max_files_per_trigger` is
  set. A set value is a Lakebench cap and the record labels it.

### Sizing the streaming stages

Lakebench sizes bronze and silver to carry the offered load with 20%
headroom:

```
executors = load / (stage MB/s per core x 0.8) / executor cores
```

| Stage MB/s per core (measured at scale 10) | Bronze | Silver |
|---|---|---|
| AML | 4.3 | 0.7 |
| Customer 360 | 40.5 | 14.2 |

- A stage whose need is above its profile's executor cap (`max_executors`)
  first grows its executors to 8 cores, then 16.
- If it still cannot carry the load, `run` names it before the run starts
  ("cannot balance"). Its lag will grow and the run will FAIL the balance
  check. Lower the scale or give the stage more cores.
- Every stage's executor count and cores per executor can be set under
  `platform.compute.spark`.
- The record's `stage_capacity` gives, per stage (bronze, silver), the share
  of the window it was busy and the MB/s per core it would take busy all
  window, plus datagen's MB/s per core. A low busy share extrapolates
  further.

### Two kinds of run

- **Capacity run.** Datagen stays ahead of the pipeline
  (`datagen_ahead: true`). The run measures capacity, scored by pace in
  seconds per million rows.
- **Steady-state run.** With fewer datagen pods the pipeline keeps up. The
  run is scored by freshness.

Above bronze's capacity each batch takes in more than the last until one
batch fills the window, and the run fails saying so. See
[Continuous gate](benchmarking/verdict.md#continuous-gate).

With `--skip-generate`, bronze reads a finite corpus already in the bucket a
few files at a time (`max_files_per_trigger`, the most files bronze reads per
trigger; derived per run when unset).

### Storage growth

Continuous datagen's files are kept, as a bank keeps its raw payment
messages. A long run grows its bronze bucket at the datagen rate:

- AML at scale 10 wrote about 116 GiB of raw files per hour (measured,
  n=1), about 2.7 TiB for a 24-hour run, beside the tables built from them.
- The report's storage section gives the run's own rate and 24-hour size.

### The window and the checks after it

- The window lasts the configured duration (default 1800 s, 30 minutes).
- A stream whose submission fails (for example a truncated Maven download)
  is reported on each attempt while the Spark Operator retries it.
- Benchmark rounds run during the window (see In-stream benchmarking).
- After the window, the continuous gate checks that data kept arriving and
  that silver and gold committed continuously inside the window.
- The streams stop at window end. The in-stream rounds are the run's only
  query checks: failed queries (a Q9 failure is tolerated unless it fails in
  every round) and empty answers in the last round. See [Continuous gate](benchmarking/verdict.md#continuous-gate)
  and [After the window](benchmarking/verdict.md#after-the-window).
- A window much longer than a finite corpus needs to arrive measures an idle
  pipeline. The gate fails a run whose data stopped arriving before half the
  window, and the run warns about this at start.
- With gold on an interval, `run_duration` must be at least 3 x
  `gold_refresh_interval`, or the run is refused before it starts.

### How the streams work

- **bronze-ingest** and **silver-stream** are true streaming jobs. Each
  micro-batch appends new rows to its Iceberg table. Bronze reads Parquet
  files from S3; silver reads changes from the bronze table.
- **gold-refresh, Customer 360**, streams the silver table. Each micro-batch
  is the silver commits since gold's position (its checkpoint, so a restart
  resumes). It pins silver at one snapshot, recomputes the daily KPIs of
  every date the new rows touch from all of silver's rows on those dates, and
  replaces those dates.
- **gold-refresh, AML**, runs detection over the pinned silver on each
  refresh. It re-detects only what the new rows can change and keeps the
  rest, so the alerts equal a full recompute. Every rule runs on every
  refresh, over the new rows plus that rule's window.

Gold runs back to back by default (`gold_refresh_interval` `0 seconds`).
With an interval set, gold can be up to that interval stale even when bronze
and silver are seconds behind, and the run labels it.

### Lag and balance

Every two minutes inside the window the run prints each handoff's lag: how
long the oldest commit the next stage has not taken has waited
(`datagen->bronze`, `bronze->silver`, `silver->gold`).

**Clock offset.** Datagen's landing times come from the object store's
clock, which can be minutes off the cluster's. At start, bronze measures the
offset with a probe object and moves landing times onto the cluster clock
(`continuous.freshness.store_clock_offset_s`). Bronze retries the probe and
fails if it keeps failing.

**Files written before the run.** A file that landed before bronze first
started (a corpus written before the run, `--skip-generate`) counts from
when bronze takes it. Datagen-to-bronze lag is recorded but not judged with
`--skip-generate` or a set `max_files_per_trigger`: the corpus or the limit
sets that lag, not bronze.

**Keeping up.** When the window closes, a stage keeps up if its lag did not
climb through the window's second half:

- The lag is sampled once per batch at the same point: bronze at each
  commit, silver and gold as each batch starts.
- The trend of those samples must rise by less than one cadence. The
  cadence is the stage's trigger interval or, when it runs back to back, its
  median batch time in the window's first half. An AML gold refresh that
  found no new silver row is not counted. A stage whose batches grow as it
  falls behind therefore cannot widen its own allowance.
- The start-up ramp, and a lag that swings by one batch time, do not count.

The run is balanced if every stage keeps up. A run that is not balanced
FAILS. Its bottleneck line names the stage, its lag and busy share, and the
executors to raise.

**What the record keeps.** `continuous.balance` holds the lag samples and
`continuous.freshness` gold freshness p50, p95 and max. Both workloads
measure freshness from file landing (bronze's `ingest_ts`, which silver
carries). A stream on a trigger interval from the config is labelled beside
freshness (`experiment.limits.trigger_bound`): freshness is measured at each
write, so between writes gold can be up to that interval older.

**Q9 contention.** A benchmark query that reads gold while it is being
rewritten fails. Lakebench retries it (30 s, then 60 s backoff) and the
scorecard records contention events per round.

### Continuous configuration

```yaml
architecture:
  pipeline:
    continuous:
      bronze_trigger_interval: "0 seconds"   # back to back (default)
      silver_trigger_interval: "0 seconds"   # back to back (default)
      gold_refresh_interval: "0 seconds"     # back to back (default)
      run_duration: 1800              # 30 minutes
      # max_files_per_trigger: unset   # no limit while datagen generates
      checkpoint_base: checkpoints
      benchmark_interval: 300         # Seconds between in-stream rounds
      benchmark_warmup: 300           # Seconds before first round
```

Override the window on the command line:

```bash
lakebench run my-config.yaml --continuous --duration 3600
```

Each key's type, range and default is in
[Configuration](configuration.md#architecture----pipeline). When to change
them:

- **Trigger intervals**: leave at 0 to measure the pipeline. A positive
  interval holds the stage to that cadence and is labelled as a Lakebench
  cap; a timer only builds backlog or adds waiting.
- **`max_files_per_trigger`**: leave unset while datagen runs. With
  `--skip-generate`, unset is derived so a finite corpus keeps arriving for
  about 1.2 x `run_duration`, at most 50 files.
- **`run_duration`**: use 900 s or more for short tests (UAT included). For
  5 benchmark rounds:
  `benchmark_warmup + 4 * (benchmark_interval + round_time) + max(60, 1.2 * round_time)`.
  [Planning round counts](benchmarking/query-benchmark.md#planning-round-counts) has a planning table.
- **`benchmark_warmup`**: raise it for a slow first gold refresh.
- **`benchmark_interval`**: for more rounds, raise `run_duration` instead of
  lowering the interval. With gold on a longer interval, both are raised to
  it.
- **Target file sizes**: reduce `bronze_target_file_size_mb` and
  `silver_target_file_size_mb` to 128-256 MB below scale 10, where 512 MB
  files are never reached. `gold_target_file_size_mb` rarely needs changing.

### Iceberg Retention

Continuous pipelines create a new snapshot every micro-batch. Without
maintenance, snapshot metadata and orphan files grow without bound: a
24-hour run can produce over a million S3 objects.

Lakebench runs `expire_snapshots` and `remove_orphan_files` (Delta:
`VACUUM`) during the run. The `retention_interval` and
`retention_threshold` fields are in
[Configuration](configuration.md#architecture----pipeline).

`retention_threshold` is not applied as-is everywhere:

- **Snapshot expiry** is floored at 1 h while streams are live, so no stream
  loses the snapshot it is reading.
- **Orphan-file removal** never uses less than 24 h 10 min, on any engine or
  path, so files a running writer has not committed are not deleted.
- **Delta VACUUM** keeps Delta's 7-day default retention while streams are
  live. A continuous Delta run shorter than 7 days gets no effective cleanup,
  and `total_s3_objects` grows for the whole run.

Continuous numbers from v1.5 and earlier were measured with no snapshot
expiry and no VACUUM ([CHANGELOG](../CHANGELOG.md)).

Maintenance uses the deployed query engine:

| Engine | Maintenance | Method |
|--------|:-----------:|--------|
| Trino | Yes | `SET SESSION <catalog>.expire_snapshots_min_retention = ...; ALTER TABLE ... EXECUTE expire_snapshots(...)` in one submission (likewise for `remove_orphan_files`) |
| Spark Thrift | Iceberg only | `CALL catalog.system.expire_snapshots(table => ..., older_than => TIMESTAMP '...')` via beeline. Delta VACUUM is skipped (it runs Spark Thrift out of memory) |
| DuckDB | No | Read-only; maintenance is skipped |

With `query_engine.type` `duckdb` or `none`, maintenance is skipped with a
log message. A failure on one table (for example one that does not exist yet
early in the run) is logged and does not stop the pipeline.

Time limits:

- Each maintenance or compaction statement may run for min(600 s, half the
  interval). A round is capped at half the interval and at the time left in
  the run.
- The first statement that times out stops the rest of that round. Timeouts
  are journaled apart from failures, because the engine may still be running
  the statement.
- The next round starts at the table after the one that timed out, so one
  slow table cannot starve the others. Compaction waits one statement
  timeout before it runs.
- A round due with less than a minute of the run left is skipped.
- The limits bound Lakebench's wait, not the engine. A statement that timed
  out near the end can still be running after the window closes.

`expire_snapshots` does not delete old `metadata.json` files; each commit
leaves one. Every Iceberg table Lakebench creates sets
`write.metadata.delete-after-commit.enabled=true` and
`write.metadata.previous-versions-max=50`, so each commit deletes metadata
files beyond the newest 50. A table that already exists in a reused catalog
keeps its old properties until it is recreated (a fresh deployment, or a run
that replaces the table).

### In-stream benchmarking

During the window Lakebench runs the workload's full query benchmark (8
queries for Customer 360, 12 for AML) on the active engine. By default the
first round starts after 5 minutes and then every 5 minutes. Each round
measures QpH, per-query latency and gold freshness at query time.

- The continuous QpH is the **median** of the in-stream rounds.
- The terminal shows a per-round table: QpH, per-query times, freshness and
  Q9 contention. The HTML report's "In-Stream Benchmark Rounds" section has
  the same data.
- A run too short for a round produces no QpH.

Round planning and the end-of-window guard are in
[Planning round counts](benchmarking/query-benchmark.md#planning-round-counts).

### Continuous scores

Every continuous score and its formula: [Continuous scores](benchmarking/continuous.md).
The balance check: [Verdict](benchmarking/verdict.md#continuous-gate).

## Reading output

Each run saves a metrics JSON and an HTML report.

**Metrics JSON** is at `lakebench-output/runs/run-<id>/metrics.json`:

```bash
# View the newest run's metrics
python3 -m json.tool "$(ls -td lakebench-output/runs/run-*/ | head -1)metrics.json"
```

- `jobs[]`: per-stage elapsed time, input and output sizes, throughput,
  executor count, CPU-seconds, memory allocated.
- `benchmark`: QpH, per-query timing and row counts.
- `pipeline_benchmark.scores`: aggregate scores, such as
  `time_to_value_seconds` (batch) or `data_freshness_seconds` (continuous),
  and pipeline throughput.

**HTML report** sits beside the metrics JSON. It is self-contained: summary
panel, per-stage breakdown, storage metrics, resource estimates and
recommendations.

```bash
open lakebench-output/runs/run-*/report.html   # open in a browser
lakebench report                                # build a report from a previous run
```

## Executor scaling

- Formula, per-job table and AML overrides:
  [Auto-Scaling](component-spark.md#auto-scaling).
- Count overrides (`platform.compute.spark.<job>_executors`), driver memory
  and tested ranges: [Executor Override Guide](configuration.md#executor-override-guide).
- Counts grow more slowly than the data, so data per executor grows with
  scale.
- The cap of 28 is a hard Lakebench cap. From 32 executors the driver's
  fabric8 client floods the API server with polls, with timeouts that look
  like network failures. More driver memory does not help.
- Override `spark.driver.maxResultSize` in `spark.conf` only if driver logs
  show "serialized results" errors.
