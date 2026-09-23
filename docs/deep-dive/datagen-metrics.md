# How the datagen scorecard adds up

lakebench's pipeline scorecard reports CPU-hr/TB and total core-hours
for the full run: datagen through gold. Datagen used to contribute
only wall time and total bytes, which meant the numerator (bytes) and
denominator (compute) were reported in different units and the
scorecard understated the true cost of a run by exactly the cost of
the datagen step.

This article walks through what changed to close that gap and why
each piece exists.

## The two things you can measure

**Wall time** is what k8s observes: the interval between the datagen
Job being submitted and the last pod entering Succeeded.

**CPU-seconds** is what k8s bills: cores reserved by the pod, times
wall time. A pod with `resources.requests.cpu: 8` running for 100
seconds costs 800 CPU-seconds regardless of what its threads were
doing (encoding parquet, sleeping in an S3 PUT, throttled by the
cgroup, whatever).

These two are related but not the same. A slow-but-parallel run and
a fast-but-serial run can have identical wall time and very different
CPU-second counts. Any benchmark scorecard that reports one without
the other is misleading.

## What lakebench used to do

Before this change, the datagen contribution to the pipeline
scorecard was:

- `datagen_elapsed`: wall time from Job submit to last pod done
- `datagen_output_gb`: measured by counting bytes in the bronze
  bucket after the fact

CPU-seconds was implicitly zero. So a 60-minute datagen at 43 pods
x 8 CPU (~20,600 CPU-seconds of real compute) contributed nothing
to `total_core_hours`. Downstream `compute_efficiency_gb_per_core_hour`
denominators were derived from Spark jobs only. Datagen was a phantom
zero-cost step.

## What the change adds

Each datagen pod, on completion, emits one JSON line on stderr:

```
LB_METRICS_JSON {"schema":"customer360","node_id":0,"node_count":8,
  "cores_used":8,"cpu_request_millicores":8000,
  "bucket":"lb-bronze","prefix":"customer/interactions/",
  "target_tb":0.5,"customer_id_max":500000,"dirty_ratio":0.08,
  "file_size_mb":64,"rows_per_file":100000,"total_files":400,
  "files_written":50,"bytes_written":6553600000,"rows_written":5000000,
  "elapsed_s":100.4,"setup_s":0.4,"gen_s":100.0,
  "build_batch_s":440.0,"encode_parquet_s":320.0,"s3_put_s":40.0,
  "throughput_mbps":65.28,"cpu_seconds":803.2,"cpu_hr_per_tb":34.05}
```

The `LB_METRICS_JSON ` prefix is intentional: grep can extract the
line from the whole pod log without parsing. Everything after the
prefix is JSON per RFC 8259, so downstream can `json.loads` it
directly.

Lakebench's aggregator reads each pod's log after the Job succeeds,
extracts the line, folds them into a `FleetSummary`, and writes the
result to a namespace-keyed sidecar under
`lakebench-output/datagen/`.

`lakebench run`, when it builds the pipeline scorecard, loads the
sidecar for the current namespace and enriches the datagen
`StageMetrics` with `executor_count`, `executor_cores`, and derived
`cpu_seconds_requested`. Downstream `total_core_hours` now includes
datagen. Same formula as the Spark stages; same units.

## Two subtle bits

Two decisions deserve calling out because they took adversarial
review to get right.

**Cores used vs cores requested.** Rust's `available_parallelism()`
does not read the cgroup CPU quota on Linux. A pod requesting 16
CPU with rayon defaulted to 8 threads produces a datagen that runs
at 8-thread parallelism but k8s reserves 16 CPU. Cost accounting
must use the k8s request (what k8s bills), not the rayon pool (what
the code actually parallelized to). So the emit carries both:
`cores_used` (rayon pool) AND `cpu_request_millicores` (k8s
request, read from a downward-API env var populated by the Job
template). The aggregator prefers the request when set. Without
this the pipeline scorecard silently reports 2x-lower CPU cost
than the customer pays.

**Sidecar collision across namespaces.** UAT runs 4 tests in
parallel across 4 namespaces. The sidecar file used to be one
`latest-datagen-metrics.json` shared between all runs; whichever
`generate` finished last overwrote the file and every subsequent
`run` in any of the 4 namespaces enriched its scorecard with
someone else's fleet. Sidecar path is now
`<namespace>-datagen-metrics.json`, payload also carries a
`namespace` field, and `_load_latest_datagen_fleet` refuses a
sidecar whose payload namespace does not match the current run.

Both of these were caught by an adversarial subagent pass on the
first draft. Neither would have surfaced in "please review this"
review because the wrong numbers still looked plausible.

## The scorecard now says

For a scale-0.5 c360 run at 8 pods x 8 CPU snappy compression:

- Datagen wall time: ~197s
- Datagen bytes: 125 GB (bronze)
- Datagen throughput: 4.34 GB/s aggregate, 628 MB/s per pod
- Datagen CPU-hr: ~3.5
- Datagen CPU-hr/TB: 4.2
- Pipeline total wall (including bronze-verify, silver-build, gold-finalize): ~28 min at this scale
- Pipeline total CPU-hr: 15.6 (of which datagen is ~3.5, Spark stages ~12.1)

That last row is what changed. `total_core_hours` used to report
~12 because datagen was invisible; it now correctly reports 15.6.
Any `$/TB` derived from `total_core_hours * $/core-hour / TB` is
now correct across the whole pipeline.

## What isn't in this change

- No Prometheus scraping. Datagen pods are short-lived batch jobs;
  their metrics are terminal, not longitudinal. Polling adds noise
  without value.
- No new pod resources. The emit is on stderr, tiny (<1 KiB per
  pod), and free.
- No config surface. There's no flag to enable or disable this;
  every datagen run emits the line, and every `lakebench run` that
  finds a matching sidecar uses it.

## For the curious

The Rust side lives in `datagen_rs/src/metrics.rs` (struct + JSON
formatter). The Python aggregator is
`src/lakebench/metrics/datagen_aggregator.py`. The template env-var
plumbing is in `src/lakebench/templates/datagen/job.yaml.j2`
(look for `LB_POD_CPU_REQUEST_MILLI`). The tests exercising the
CPU-request preference and the empty-fleet gate are in
`tests/test_datagen_aggregator.py` and
`tests/test_metrics.py::TestBuildPipelineBenchmark`.

The design doc is
`dev-artifacts/METRICS-DESIGN.md`.
