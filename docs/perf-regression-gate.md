# Performance-regression gate

The gate answers one question: did this change make a pinned benchmark
slower? It compares a new run of a pinned config against that config's
accepted baseline, metric by metric, and refuses to compare anything that is
not like for like. It runs offline over `metrics.json` files; it never talks
to Kubernetes or S3.

## Pieces

| Piece | Path | What it holds |
|---|---|---|
| Pinned configs | `benchmarks/perf/*.yaml` | One per workload and mode that matters, every sizing knob explicit |
| Baseline store | `benchmarks/perf/baselines.yaml` | Accepted numbers per pinned config, with run id, git sha, config hash |
| Gate logic | `src/lakebench/metrics/perf_gate.py` | Fingerprints, guards, compare, record |
| CLI | `scripts/perf_gate.py` | `status`, `compare`, `record`, `seed`, `gate` |
| Release check | `scripts/release_gate.py` check `perf-baselines` | Fails the release on a regression or a missing required baseline |

Pinned configs today:

| Name | Workload | Mode | Scale | Required by the release gate |
|---|---|---|---|---|
| `c360-batch-s10` | Customer 360 | batch | 10 | yes |
| `c360-continuous-s10` | Customer 360 | continuous | 10 | yes |
| `aml-batch-s1` | AML (financial) | batch | 1 | no, until the AML datagen is frozen |

AML batch at scale 10 joins as a required config once the AML datagen is
frozen.

## How "like for like" is enforced

Two hashes, both recorded with the baseline:

- **`config_hash`** is the sha256 of the pinned YAML as parsed. Comments do
  not count; any value does. If the pinned file changes after a baseline was
  recorded, compare refuses until a new baseline is recorded.
- **`fingerprint_hash`** is the sha256 of the sizing-relevant part of the
  `config_snapshot` a run records in `metrics.json`: scale, recipe, Spark
  driver, executor and per-job executor counts, datagen scale, mode,
  parallelism and file size, images, Trino coordinator and workers,
  continuous-mode trigger intervals, benchmark mode, and scratch storage.
  The snapshot is taken after autosizing and any cluster capping, so it is
  what actually ran. A run whose fingerprint differs from the pinned
  config's is refused, and the refusal names each differing field (for
  example `trino.worker.replicas: pinned 2, run 8`).

Defaults are not part of `config_hash`, which is why each pinned file sets
every knob itself; `tests/test_perf_gate.py` fails if one is left to a
default. The run also has to match on what it realised: each stage's
executor count and the number of datagen pods must equal the pinned values.

Baselines are specific to the reference cluster. The storage classes are
pinned, but a different cluster behind the same names produces different
numbers; do not compare across clusters.

## Guards

A run is refused, never compared, when:

- it did not succeed;
- it is a batch run with `scale_ratio` below 0.95 (0 means the bronze input
  volume was not measured, which is refused too);
- its datagen fleet reported `data_quality` other than `complete`.

Within a comparable run, two numbers are left out rather than trusted:

- `sustained_throughput_rps` of a continuous run whose corpus drained before
  the window ended (`corpus_drained: true`). That figure is corpus rows over
  the window, a lower bound, not a throughput (LB-145). A drained run is never
  recorded as the rows/s baseline either.
- `maintenance_value_pct` when it is null. It is reported as "not measured",
  never as zero.

## Metrics and tolerances

The metric set and each metric's direction come from
`lakebench.cli._reproduce` (`_METRIC_TABLE`, `_classify_direction`), the same
classification `lakebench reproduce` uses. Only the performance band is
compared.

| Metric | Direction | Default tolerance |
|---|---|---|
| `time_to_value_seconds`, `<stage>_seconds`, `data_freshness_seconds`, `datagen_cpu_hr_per_tb` | lower is better | 10% |
| `pipeline_throughput_gb_per_second`, `compute_efficiency_gb_per_core_hour`, `composite_qph`, `sustained_throughput_rps`, `datagen_aggregate_mbps`, `datagen_mbps_per_pod` | higher is better | 10% |
| `query_qph_<query>` (3600 / query seconds) | higher is better | 20% |
| `maintenance_value_pct` | higher is better | 10 percentage points |

Only drift in the bad direction fails. An improvement past the tolerance is
reported as `improved` and passes; record a new baseline if it should become
the bar. A metric in the baseline that the run lacks (a query that failed,
missing datagen telemetry) fails as `missing`.

Per-config overrides go in the store entry:

```yaml
  c360-batch-s10:
    tolerances:
      query_qph_Q1_full_aggregation_scan: {pct: 30}
      maintenance_value_pct: {abs: 5}
```

## Record a baseline

1. Run the pinned config as-is on the reference cluster. Identity and
   credentials come from the environment, so the file does not change:

   ```bash
   export LAKEBENCH_PERF_NAME=perf-c360-batch-s10
   export LAKEBENCH_S3_ENDPOINT=... LAKEBENCH_S3_ACCESS_KEY=... LAKEBENCH_S3_SECRET_KEY=...
   lakebench deploy   benchmarks/perf/c360-batch-s10.yaml
   lakebench generate benchmarks/perf/c360-batch-s10.yaml --wait
   lakebench run      benchmarks/perf/c360-batch-s10.yaml
   lakebench destroy  benchmarks/perf/c360-batch-s10.yaml --force
   ```

2. Record the run. `metrics.json` does not store the commit it came from, so
   `--git-sha` is required:

   ```bash
   python scripts/perf_gate.py record c360-batch-s10 \
       --run <run id> --git-sha "$(git rev-parse --short=12 HEAD)"
   ```

   `record` applies every guard above and refuses a run that is not
   comparable. Replacing an accepted baseline needs `--replace`; commit the
   updated `baselines.yaml` with a message that says why the bar moved.

## Run the check

```bash
python scripts/perf_gate.py status
python scripts/perf_gate.py compare c360-batch-s10 --run <run id or path/to/metrics.json>
python scripts/perf_gate.py gate
python scripts/release_gate.py --only perf-baselines
```

`compare` exits 0 on pass, 1 on a regression, 2 when refused or when there is
no baseline. `gate` and the release check pick, for each pinned config, the
newest successful run whose fingerprint matches, searching
`lakebench-output/runs` (or `$LAKEBENCH_PERF_RUNS_DIR`) and `uat/perf/`. Name
a run explicitly with `--perf-run NAME=RUN` on `release_gate.py` or
`--run NAME=RUN` on `perf_gate.py gate`.

The release check fails when a required config has no accepted baseline, no
run can be found for it, or its run is refused or regressed. An optional
config is reported but never fails the release. CI has no local runs
directory, so a release checks in the `metrics.json` of each required perf
run as `uat/perf/run-<id>/metrics.json` alongside `uat/results-<version>.md`.

## Seeding

`python scripts/perf_gate.py seed` scans the runs directory for runs that
match a pending config exactly and, with `--write`, records the newest one.
Against the 98 runs on the reference workstation on 2026-09-24 nothing
matched, so all three configs are "pending first run":

- every earlier run used the floating `lb-datagen:latest` image, left
  per-job executor counts and datagen mode to defaults, and all 22 c360
  scale-10 runs used 512 MB datagen files and predate the `workload_schema`
  snapshot field;
- every AML batch run records `scale_ratio` 0.0, because the bronze stage
  reports no input volume for the financial schema, so the scale guard
  refuses them even where the sizing is close.
