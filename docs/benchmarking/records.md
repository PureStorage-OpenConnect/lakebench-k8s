# Run records

Reference: `metrics.json`, provenance fields, benchmark records and the report commands.

## Metrics JSON

Each run saves `lakebench-output/runs/run-<id>/metrics.json`:

```json
{
  "run_id": "20260201-143052-a1b2c3",
  "maintenance_policy_id": "m2-2026-09-26",
  "provenance": {
    "lakebench_version": "1.7.1",
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

## Benchmark records

`lakebench benchmark` saves its result under a new run id with `record_kind: "benchmark"` and `parent_run_id` (the run it measured).

- A copy of that run's record: QpH, scores and query stage are new; pipeline stages, sizes and timings are the run's.
- `provenance.benchmark` names the code that ran it, and when.
- In-stream rounds and the maintenance QpH pair are not copied.
- The run's own record is never rewritten.
- A benchmark record is never a deployment's "latest run" for `report`; read it by run id. When nothing is recorded: [cli-reference.md](../cli-reference.md#benchmark).

`lakebench query` prints its result and writes no record.

## Report commands

- Every `lakebench run` writes `lakebench-output/runs/run-<id>/report.html` once, at the end. That file is the shareable artifact. Layout: [HTML report layout](html-report.md).
- `lakebench report` prints, without changing it, a per-stage table (elapsed, volume, throughput, executors) and the mode's scores (time to value and throughput for batch, data freshness and sustained throughput for continuous).
- `lakebench report --render` writes a fresh copy, for example `lakebench-output/reports/report-20260201-143052-a1b2c3-<UTC>.html`, never overwriting the delivered file.
- `lakebench report --format table|json|csv` prints the stage matrix.

Other flags (`--list`, `--run`, `--metrics`): [cli-reference.md](../cli-reference.md#report).
