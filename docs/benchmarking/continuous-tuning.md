# Tuning a continuous pipeline

Guide: reading the continuous scores for imbalances, the levers, and a diagnostic checklist.

Settings: [running-pipelines.md](../running-pipelines.md#continuous-configuration), [configuration.md](../configuration.md). Scores: [continuous.md](continuous.md).

## Reading the scores

- **Stage latency.** Bronze and silver run back to back by default, so a batch holds what arrived while the last ran. A stage whose batches keep growing is the bottleneck. With a trigger interval, a stage whose latency exceeds it piles up batches.
- **Ingest ratio.** Slightly above 1.0 is the error of `released_rows`, a mean-rate estimate; bronze kept up. The report warns above 1.05; see the checklist.
- **Gold re-reads.** AML gold re-detects each [tick](../glossary.md#tick) from the silver rows its rules' windows reach back to. Customer 360 gold recomputes only the dates each micro-batch touches. Both count gold rows as the whole pinned silver snapshot per cycle, summed over cycles, so gold rows and `total_rows_processed` grow with silver size times cycles and are neither rows read nor unique rows.
- **Freshness.** To lower `data_freshness_seconds`: add `gold_executors`, and leave `silver_trigger_interval` at 0 s so silver rows reach gold as each batch commits.

## Stage by stage

| Symptom | What to do |
|---|---|
| Bronze falls behind datagen | Raise `bronze_executors`. Bronze is I/O-bound (Parquet reads from S3). A configured `datagen.parallelism` or `cpu` can offer more than the stages were sized for; unset them. |
| Silver falls behind bronze | Raise `silver_executors` first. Customer 360 silver is per-row column transforms, CPU-bound at large batches. AML silver also runs the dimension MERGE, whose cost tracks the party population, not the batch: raise `silver_max_rows_per_trigger` and keep `silver_write_mode` at `merge_on_read` ([Levers](#levers)). |
| Gold falls behind silver | Raise `gold_executors`. AML gold re-runs each rule over the new rows' days plus that rule's window. |
| AML gold slow from TM | The TM operations pass runs inside a gold tick over the whole corpus, so its cost grows with the run. Raise `workload.tm_operations.continuous_interval_seconds`. The tick timing line's `tm=` and the balance line show its share. |

## Levers

Each setting is used as set. The balance line names the one to change (`continuous.balance.lever`).

| Setting | Changes | Lever when |
|---|---|---|
| `platform.compute.spark.{bronze,silver,gold}_executors` | the stage's executors in this mode, up to 28 | the stage falls behind below what the load needs |
| `platform.compute.spark.{bronze,silver,gold}_executor_cores` | cores per executor | the load needs more than 28 executors |
| `platform.compute.spark.{bronze,silver,gold}_executor_memory` | heap per executor | a stage needs more memory per executor without more cores, such as a larger silver batch |
| cluster capacity | the concurrent budget the streams share | the budget cut a stage below plan |
| `workload.tm_operations.continuous_interval_seconds` | how often AML gold runs the TM pass | TM takes a quarter or more of gold's time |
| `workload.datagen.parallelism`, `cpu` | the offered load | to offer more or less than the scale's load |
| `architecture.pipeline.continuous.{bronze,silver}_trigger_interval`, `gold_refresh_interval` | a stage's cadence (0 s: back to back) | a timer only adds waiting; leave at 0 s to measure capacity |
| `architecture.pipeline.continuous.silver_write_mode` | the Iceberg write mode of the update-heavy silver tables (AML); default `merge_on_read`, whose MERGE writes only the rows it changes | leave at the default. `copy_on_write` rewrites the files each MERGE touches, so silver's cost tracks table size and added executors sit idle. Moves recorded results |
| `architecture.pipeline.continuous.silver_max_rows_per_trigger` | the bronze rows a silver micro-batch reads; default 20M. AML continuous, unset: sized to the silver executors' heap, never below 20M: about 3.2M rows per 32g executor, at most about 4.5M per larger one. `null` lifts the cap | silver's per-batch fixed cost (the dimension MERGE) dominates: a bigger batch spreads it over more rows. It commits less often, so a short window may hold too few silver commits for the gate. Moves recorded results |
| `architecture.pipeline.continuous.silver_reuse_bronze_projection` | whether an AML silver batch reads one shared bronze projection for the entity and account dimensions instead of one scan each; default off | the per-batch bronze read dominates. Dimension rows are identical either way |
| scale | offered load and corpus | no lever above helps |

### Example: not balanced

```
Balance: not balanced: silver-stream fell behind bronze-ingest: its lag grew
412s across the window's second half (one cadence is 95s); 640s at the
window's end, busy 100%; raise platform.compute.spark.silver_executors
(has 12, the offered load needs ~19) or lower the scale
```

- Give silver the executors named, use a larger cluster (`plan`'s "cannot balance" warning names the same stage), or lower the scale.
- A stage that already has its full need and still falls behind ran below the sizing default rate: raise its executors past the need.

## Diagnostic checklist

| Symptom | Likely cause | Fix |
|---|---|---|
| `pipeline_saturated: true` | A stage could not keep pace (`intake_limit` names bronze; otherwise silver) | Add executors to that stage |
| `corpus_ingest_ratio` < 1, `ingest_ratio` near 1.0 | Own datagen: rows written after the window. `--skip-generate`: corpus larger than [trickle](../glossary.md#trickle) rate x window | Not saturation. Lengthen the window to `corpus_drain_seconds`, or raise `max_files_per_trigger` and size the streams |
| `ingest_ratio` < 0.95 | Bronze fell behind; `intake_limit` says capacity or stall | Add `bronze_executors`, or check the driver log |
| `ingest_ratio` well above 1.0 | Bronze took more than was released, such as an earlier run's data | Rerun without `--skip-generate`: a run that generates its own data clears earlier raw datagen files and stream checkpoints first (a Customer 360 rerun over existing tables also needs `--force-reset`) |
| `not balanced: <stage> fell behind` (advisory; the run is not failed) | The stage cannot carry the load on its executors | Raise the named setting, larger cluster, or lower scale |
| `cannot balance` from `plan` or `config recommend` | Need above the executor cap, cluster budget, or a configured count | Larger cluster or lower scale; otherwise the run reports not balanced |
| Balance card: `datagen offered X of Y sized ... fell short` | Datagen offered under 90% of the sized load | Check datagen pod CPU (throttling, eviction); rates are against a lighter load |
| High `data_freshness`, balanced | Gold write time, or a configured `gold_refresh_interval` | Add `gold_executors`; leave the interval at `0 seconds` |
| QpH dropping across rounds | Table growth | Add Trino workers or memory |
| Q9 shows `retry` or `FAIL` in the Q9 Contention table across rounds | Rounds colliding with gold rewrites | Raise `benchmark_interval` |
| `total_s3_objects` growing unbounded (Iceberg) | Maintenance not keeping pace, or failing | Check the journal's Iceberg maintenance events for timed-out or failed statements, then lower `retention_interval`. `retention_threshold` below `1h` does nothing while streams are live (expiry floored at 1 h, and at the snapshot the live silver stream still reads from, so the stream is never left without its offset; orphan removal at 24 h 10 min). |
| Same, Iceberg tables created before metadata retention | They keep every `metadata.json` | Recreate them with a fresh deployment. |
| Same, Delta | Runs under 7 days get no effective cleanup (VACUUM keeps the 7-day default while streams are live) | |
