# Batch scorecard

Reference: batch scores and formulas, stage timing, per-stage and resource metrics, datagen in scoring, Customer 360 expected results.

## Batch scores

| Score | Formula | Meaning |
|---|---|---|
| `time_to_value_seconds` | `max(end_time) - min(start_time)` | Primary batch score; lower is better. From the first stage's submission to the end of the gold Spark application. Includes Lakebench's work between stages (detecting the end, reading driver log and output size, submitting). |
| `time_to_value_seconds`, multi-cycle | | Customer 360 leaves out each cycle's datagen (between one cycle's gold and the next bronze) but keeps the table-health probe after each cycle. AML keeps its datagen. |
| `time_to_value_datagen_excluded_seconds` | sum over cycles of each `cycles[].datagen_start`..`datagen_end` overlap with the span | Multi-cycle Customer 360 only. Datagen seconds left out of time to value. 0 with `--skip-generate`. Absent when a cycle's datagen times are missing. Diagnostic. |
| `total_elapsed_seconds` | `end_time - start_time` | Whole run, maintenance and benchmark included. |
| `total_data_processed_gb` | `sum(stage.input_size_gb)` | GiB the jobs reported reading: bronze the raw files, silver the bronze input, gold silver's current snapshot (from table metadata). Never a bucket listing. Unreported stages and the query stage add nothing. |
| `pipeline_throughput_gb_per_second` | `total_data_processed_gb / time_to_value_seconds` | Higher is better; scales with executors and cluster capacity. |
| `compute_efficiency_gb_per_core_hour` | `total_data_processed_gb / total_core_hours` | GiB per core-hour of requested executor compute for bronze, silver and gold. Drivers, datagen, query engine and maintenance not counted. Higher is better. Request 100 cores and use 20, and it drops. |
| `scale_ratio` | `bronze_input_gb / approx_bronze_gb` | Data completeness, from bronze input only. Below 0.95: datagen or ingestion was incomplete; not comparable with other runs at that scale. |
| `composite_qph` | QpH from the [query benchmark](query-benchmark.md) | Engine performance against gold. Depends on engine, workers and memory, not on pipeline throughput. |
| `cycle_progression` | per-cycle elapsed, QpH, table health | Multi-cycle only (`cycles > 1`). Pipeline time and Iceberg metadata growth per cycle. |

Typical time to value: 200-600 s at scale 10 (~100 GB), 1200-3600 s at scale 100 (~1 TB).

## Stage times

- A Spark stage runs from its SparkApplication's creation to the driver container's finish (else the SparkApplication's `terminationTime`).
- Times are mapped to the Lakebench host's clock through the API server's clock offset.
- `timing_source`: `driver_container`, `spark_application`, or `poll` when no consistent cluster time could be read.
- `timing_resolution_seconds`: 2 s from the cluster, else the poll interval.
- Records before 1.6 ended every stage on the 15 s job-monitor poll: stage seconds and time to value read up to 15 s per stage late and include the gold driver-log fetch. Do not compare them with later records.

## Per-stage metrics

Each stage (datagen, bronze, silver, gold, query) writes a `StageMetrics` record, exported as `stages` and `stage_matrix` in `metrics.json`.

| Group | Fields |
|---|---|
| Timing | elapsed seconds, start and end, success or failure |
| Data volume | input and output GB and rows |
| Throughput | `input_size_gb / elapsed_seconds`, `input_rows / elapsed_seconds` |
| Resources (allocated) | executor count, cores and memory per executor |
| Continuous streams (zero for batch) | latency ms, freshness s, batch count |
| Query (zero for other stages) | queries executed, queries per hour |

## Datagen in scoring

By default `lakebench run` measures bronze-verify, silver-build, gold-finalize and the query benchmark; datagen is a separate step.

- Batch: `lakebench run --generate` adds datagen to the same invocation.
- Continuous: datagen runs beside the streams as a `datagen` stage. Continuous runs have no `time_to_value_seconds` or `scale_ratio`.

Datagen cost uses requested CPU, not utilization: CPU-seconds = pod CPU request x wall time.

- Example (illustrative, not a measurement): 8 pods at 8 CPU running 200 s = 12,800 CPU-seconds, about 3.6 CPU-hours. Writing 100 GB, that is 36 CPU-hr/TB. With 12 Spark core-hours the pipeline total is 15.6 core-hours.
- `$/TB` = `total_core_hours` x $/core-hour / TB, covering the whole pipeline.
- `total_core_hours` includes datagen.

`lakebench generate` writes a sidecar with the fleet record; a generating `run` writes its own. A replaced corpus's sidecar is removed first, so it is never attributed to a later run.

## Resource metrics

Resource metrics are **requested** values (what you asked Kubernetes for and pay for in a cloud), not utilization.

- `executor_count`, `executor_cores` (per executor), `executor_memory_gb` (per executor, overhead excluded).
- Source: the job profiles in `spark/job.py` and the scale-derived executor count. Batch values are fixed per scale unless overridden.
- Continuous streams are sized for the offered load and the cluster budget, so they can differ between clusters. The shape that ran is in `config_snapshot.spark.streaming_shape`.

## Customer 360 expected results

A Customer 360 batch run on the cluster checks silver and gold against what the generator wrote and records each check in `metrics.json` as `c360_correctness`.

- Sixteen checks gate the run: the fifteen invariant and reconcile checks (bronze rows equal to the rows datagen was sized for, silver invariants, bronze-to-silver-to-gold reconciliation, gold KPI identities) and the overall average transaction value.
- The run fails when a gating check fails or could not be evaluated, when gold-finalize logged no facts, or when the check raised.
- Other statistical checks and benchmark row counts are recorded but do not fail the run. A failed one is listed in `verdict.qualifiers.c360_failed_not_gating`.
- `--local` and `--stage` runs make no Customer 360 check.
