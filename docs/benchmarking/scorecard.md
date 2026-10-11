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
- Continuous: datagen always runs beside the streams as a `datagen` stage (`stage_type="datagen"`, `engine="datagen"`). It adds to `total_elapsed_seconds`, prices bronze's and silver's window rows at its raw bytes per row (fleet bytes written / rows written), and is the denominator of `corpus_ingest_ratio`. Gold's input is what gold reported reading, with no silver-bucket fallback; gold's re-reads and the query stage add none.
- Continuous runs have no `time_to_value_seconds` or `scale_ratio`.
- Datagen output size: its pods' bytes written, else a bronze bucket listing (which also holds the table and earlier runs' files). Rows are never estimated from scale.
- Continuous datagen rows are the sum every pod reports. If any pod did not report, rows are unmeasured (0, with `ingest_ratio` and `pipeline_saturated` null). Batch datagen rows are not measured.

Datagen cost:

- The scorecard reports CPU-hr/TB and total core-hours from datagen through gold, in the same units for every stage.
- Datagen wall time is the Job's, from submit to the last pod Succeeded, else the slowest pod's own timer (`wall_elapsed_max_s`).
- Datagen CPU-seconds are the pod's CPU request x wall time: a pod requesting 8 CPU for 100 s costs 800, whatever its threads did. A pod requesting 16 CPU that runs 8 threads is billed for 16.
- Example (illustrative, not a measurement): 8 pods at 8 CPU running 200 s = 12,800 CPU-seconds, about 3.6 CPU-hours. Writing 100 GB, that is 36 CPU-hr/TB. With 12 Spark core-hours the pipeline total is 15.6 core-hours.
- `$/TB` = `total_core_hours` x $/core-hour / TB, covering the whole pipeline.
- Each pod writes one `LB_METRICS_JSON` line to stderr at completion, with `cores_used` (thread pool) and `cpu_request_millicores` (the request). Cost uses the request.
- Threads: the pod's CPU rounded up to whole cores (1300m runs 2 threads, held to its share by the CPU quota). An explicit `datagen.generators` overrides it; the count drops when the memory limit cannot hold that many threads.
- Batch datagen stage: `executor_count` is `pods_reported`; `executor_cores` is effective cores / pods reported; `cpu_seconds_requested` uses the Spark formula; `total_core_hours` includes datagen.
- A fleet with no reporting pods counts no datagen cores. The stage is added only when the Job's elapsed time is above 0, with 0 executors.
- Datagen pods are not scraped by Prometheus; the stderr line is the record. With observability on, each pod pushes progress to the deployment's Pushgateway about every 10 s plus a final push at exit, best effort; a failed push changes nothing recorded.

The pod lines fold into a sidecar, `lakebench-output/datagen/<namespace>-datagen-metrics.json`. A run never reads another namespace's sidecar.

| Command | Fleet it records |
|---|---|
| `lakebench generate` | Writes the sidecar |
| `run` that generates (batch `--generate`, or continuous without `--skip-generate`) | Its own pods, as `datagen_fleet`; `experiment.corpus.datagen` reads the image digest from it |
| Batch `run` that does not generate | The namespace's sidecar, from the generate that wrote its corpus |
| Continuous `--skip-generate` | None |
| Batch `run` with `pipeline.cycles` above 1 that generates every cycle | None: it does not read those pods. It refuses `--generate`, and removes the sidecar once its bronze check lets it generate (before the check with `--regenerate`) |
| Multi-cycle `--skip-generate` | The sidecar |
| `run --local` | Reads the sidecar; writes none |

`lakebench generate` and a generating run remove the sidecar before they empty or regenerate the corpus, so a replaced corpus is never attributed to a later run. This holds even when their own pods cannot be read or the generate fails.

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
