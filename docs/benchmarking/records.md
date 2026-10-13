# Run records

Reference: `metrics.json`, provenance fields, benchmark records and the report commands.

## Metrics JSON

Each run saves `lakebench-output/runs/run-<id>/metrics.json`:

```json
{
  "run_id": "20260201-143052-a1b2c3",
  "maintenance_policy_id": "m2-2026-09-26",
  "provenance": {
    "lakebench_version": "1.7.3",
    "git_sha": "<40-char commit, or null when unknown>",
    "git_dirty": false,
    "install": "checkout",
    "deps": { "pinset_sha256": "<sha256>", "...": "..." },
    "images_observed": { "spark_driver": "<registry>/spark@sha256:...", "...": "..." },
    "scratch_as_ran": { "silver-build": { "size_limit": "300Gi", "storage_class": "px-csi-scratch" } },
    "...": "..."
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
    "query_benchmark": { "mode": "power", "qph": 720.0, "queries": [ ... ] }
  }
}
```

- `maintenance_policy_id` names the table-maintenance policy. The experiment block's `lakebench` copy carries the code fields, `deps` and `images_observed`; other `provenance` fields are provenance only.
- `deps.pinset_sha256` is part of the experiment identity.
- Observed image digests are an architecture key, compared per role both runs observed. A role seen by one run only, or a run that observed none, is not a difference.

| Field | Contents |
|---|---|
| `lakebench_version`, `git_sha`, `git_dirty`, `install`, `tree_sha256` | `install: checkout`: the commit and whether the package had uncommitted changes. `install: wheel` (pip): the commit and tree state written into the package at build. `unknown`: any other install (the single-file binary), null commit. `tree_sha256` hashes the package files on disk. |
| `end_sample` | The same fields read at run end, and `code_changed_during_run`: true when package files, commit, version or install changed (an edit inside an installed wheel or a modified checkout counts). Then a support state of `supported` drops to `unverified`. |
| `config_sha256`, `config_path` | Hash of `config_snapshot`; the config file as given, made absolute. |
| `scripts_sha256`, `scripts_maps`, `scripts_files_sha256` | The Spark scripts ConfigMaps applied and read back. |
| `deps` | The dependency set: `pinset_sha256` (its identity), repositories, per-group file hashes, the server pod. A run-end pod check verifies Spark Thrift or DuckDB pods are on the same set. |
| `images_observed` | Image digests the run's pods ran: Spark driver and executors, Trino coordinator, Spark Thrift server. A later different digest per role goes in `images_observed_changed`. |
| `scratch_as_ran` | Per Spark job, the executor scratch PVC size and storage class as the cluster held them (null without a scratch PVC). |

## Storage I/O

The `io` block holds the storage traffic the engines reported for the run, derived from the rest of the record when it is written; records from before 1.7.2 lack it. Bytes are what the storage clients count; pod network bytes are not added.

- `intervals`: one per benchmark query (any instrumented engine), one for datagen, and one per 15 s step of each Spark stage that gained bytes. Each has `source`, `stage`, `label` (`benchmark`, `pre_compaction` or `round_<n>`), the `query`, `start` and `end` (UTC, from the engine's clock; null when not recorded), `read_bytes` and `write_bytes`, the basis of each, and `complete`.
- `null` bytes mean not measured; `0` means measured and zero.
- `not_measured`: what has no figure and why. A Spark stage with no executor counters reads `executor_io_not_collected` (observability was off), a Spark stage's reads of an Iceberg table before Iceberg 1.12.0 `iceberg_reads_have_no_client_counter`, a query engine that reports no I/O `engine_not_instrumented` (Trino and DuckDB are instrumented; Spark Thrift is not), and a query record from before the block `io_not_recorded`.
- `totals`: bytes per source, summed over its intervals.
- `problems`: query samples whose report was missing, for example because the query had left the coordinator's history, and Spark stages whose write counters cover under 95% of the bytes that landed.

Where the figures come from:

- **Trino:** `physical_input_bytes` and `physical_written_bytes` from `system.runtime.tasks`, read once per query stream after its last query ends, so no timed query is affected. Summed over a query's tasks they equal its `physicalInputDataSize` and `physicalWrittenDataSize`. Decoded size, output size and inter-node exchange are not storage and are not read.
- **DuckDB:** the httpfs filesystem log (`duckdb_logs_parsed('FileSystem')`), summed over `S3FileSystem` READ rows for each query. Captured in the query's own session, so it is that query's actual bytes read from S3, with row-group and file pruning reflected. Queries do not write, so `write_bytes` is 0.
- **Datagen:** `datagen_fleet.total_bytes_written`. Single-run datagen records no start or end time, so its interval has no window and counts as unplaced in a time series.
- **Spark writes:** the driver's total of the stage's task output bytes, logged in the job's metrics block (`jobs[].extra_metrics.output_bytes_written`). It covers every task, so it is exact: in a Customer 360 run at scale 1 the silver total equalled the bucket's data bytes to the byte. It is used only when the job marked it complete (`output_bytes_complete: true`); a driver whose status store dropped events says so and its total is ignored. Works with observability off, and then the total is placed evenly over the stage window.
- **Spark timing and reads:** with `observability.enabled` the executors' exporter counters, read from Prometheus after the run (`platform_metrics.spark_io`, cumulative bytes per pod summed per stage every 15 s), time the writes and give the reads. The task output counter moves when a task finishes, so a stage shows its bytes late, not as a rate; and the executors are deleted when their job ends, so the last increments are lost (77% to 87% of silver's bytes in three runs). The part of the driver's total the counters did not see is placed in the last 15 s of the stage window (`driver_task_output_unseen_by_executors`). Reads are `executor.filesystem.s3a.read_bytes` (raw files and Delta tables) and `executor.filesystem.s3_read_bytes` (Iceberg tables, which S3FileIO counts under scheme `s3`, not `s3a`); a pod reads through one scheme, so the two are summed per pod without double counting. They were within 0.3% of the bytes read. Silver reads raw Parquet in a batch run and bronze's table in a continuous one; gold reads the silver table. The Iceberg `s3` counter is only correct from Iceberg 1.12.0 ([PR #17236](https://github.com/apache/iceberg/pull/17236)); before it the series is all zero, so an Iceberg read with no bytes is marked `iceberg_reads_have_no_client_counter` and a read with bytes is measured. Reads are trusted from the series, not from a version string. Streaming checkpoint and state bytes are in neither counter. Without a complete driver total, a stage whose counters cover under 95% of the bytes that landed is flagged in `problems` and its intervals are `complete: false`.
- **Not measured:** Iceberg table reads before Iceberg 1.12.0 (no correct client counter), driver-side I/O, and storage-side telemetry.

`lakebench.metrics.io.bucket_series` spreads the intervals over fixed time buckets. Each interval's bytes are spread evenly over its window, so a bucket is no finer than the queries feeding it. Bytes with no window, or outside the requested range, are `unplaced`.

## Benchmark records

`lakebench benchmark` saves its result under a new run id with `record_kind: "benchmark"` and `parent_run_id` (the run it measured).

- It copies the measured run's record: QpH, scores and query stage are new; pipeline stages, sizes and timings are the run's. In-stream rounds and the maintenance QpH pair are not copied.
- `provenance.benchmark` names the code that ran it, and when.
- The run's own record is never rewritten.
- A benchmark record is never a deployment's "latest run" for `report`; read it by run id. When nothing is recorded: [cli-reference.md](../cli-reference.md#benchmark).

`lakebench query` prints its result and writes no record.

## Report commands

- Every `lakebench run` writes `lakebench-output/runs/run-<id>/report.html` once, at the end; that file is the shareable artifact. Layout: [HTML report layout](html-report.md).
- `lakebench report` prints, without changing it, a per-stage table (elapsed, volume, throughput, executors) and the mode's scores (time to value and throughput for batch, data freshness and sustained throughput for continuous).
- `lakebench report --render` writes a fresh copy, for example `lakebench-output/reports/report-20260201-143052-a1b2c3-<UTC>.html`, never overwriting the delivered file.
- `lakebench report --format table|json|csv` prints the stage matrix.

Other flags (`--list`, `--run`, `--metrics`): [cli-reference.md](../cli-reference.md#report).
