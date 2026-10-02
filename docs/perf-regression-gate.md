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
| Baseline store | `benchmarks/perf/baselines.yaml` | Accepted numbers per pinned config, with run id, git sha, config hash, fingerprint version and dependency pinset (store `schema_version` 2; a schema 1 store still loads, its entries read as fingerprint version 1, and the next `record` writes schema 2) |
| Gate logic | `src/lakebench/metrics/perf_gate.py` | Fingerprints, guards, compare, record |
| CLI | `scripts/perf_gate.py` | `status`, `compare`, `record`, `seed`, `gate` |
| Release check | `scripts/release_gate.py` check `perf-baselines` | Fails the release on a regression or a missing required baseline |

Pinned configs today:

| Name | Workload | Mode | Scale | Required by the release gate | Baseline |
|---|---|---|---|---|---|
| `c360-batch-s10` | Customer 360 | batch | 10 | no (v1.6) | accepted, but poll-timed (pre-v1.6), so every current run is refused against it |
| `c360-continuous-s10` | Customer 360 | continuous | 10 | no (v1.6) | pending first run |
| `aml-batch-s1` | AML (financial) | batch | 1 | no | pending first run |

v1.6 has no performance baselines. The re-baseline of the pinned configs on
the v1.6 tree is deferred to v1.7, so no config is required and the
`perf-baselines` release check reports each config as `warn` without failing
the release. Which configs become required again, and when AML joins them, is
set with the v1.7 re-baseline.

## How "like for like" is enforced

Two hashes, both recorded with the baseline:

- **`config_hash`** is the sha256 of the pinned YAML as parsed. Comments do
  not count; any value does. If the pinned file changes after a baseline was
  recorded, compare refuses until a new baseline is recorded.
- **`fingerprint_hash`** is the sha256 of the sizing-relevant part of the
  `config_snapshot` a run records in `metrics.json`. Fingerprint version 2
  (from v1.7) covers: scale, recipe, per-job executor count overrides,
  datagen scale, mode, parallelism and file size, images, Trino coordinator
  and workers, continuous-mode trigger intervals, the benchmark that ran
  (for `lakebench run`: one hot power pass with one stream, the config's
  `iterations` and the `maintenance_settle` settings), scratch storage with
  the per-job scratch sizes, the maintenance settings
  (`pre_benchmark_maintenance`, `retention_interval`, `retention_threshold`,
  `compaction_enabled`, `compaction_interval`), and `fingerprint_inputs`:
  - `job_profiles`: per Spark job of the run's mode, the driver cores and
    memory, executor cores, memory, overhead and count, and scratch size
    its manifest asks for (job profile, scale-derived count, overrides);
    the count is the one before the continuous concurrent budget;
  - `owned_conf`: per Spark job, the `sparkConf` its manifest writes,
    without the keys that name where a deployment lives (S3 endpoint,
    warehouse and catalog URIs, metastore URI, jar URLs) and without any
    key that can hold a credential;
  - the query engine's sizing block (Trino workers with spill and storage,
    Spark Thrift or DuckDB cores and memory) and the catalog's resources.

  The snapshot is taken after autosizing and any cluster capping, so it is
  what actually ran, and `fingerprint_version` and `fingerprint_inputs` are
  stamped into it when the run starts: the gate reads them and never
  rebuilds them, so a record keeps the fingerprint of the code that ran it.
  A run whose fingerprint differs from the pinned config's is refused, and
  the refusal names each differing field (for example
  `trino.worker.replicas: pinned 2, run 8`). A change to a job profile or
  to the conf Lakebench writes moves the pinned config's fingerprint too, so
  its baseline is refused until re-recorded.
- **Fingerprint version.** A run recorded before v1.7 has no
  `fingerprint_version` (version 1) and is refused by name ("run predates
  fingerprint v2"); a baseline recorded under version 1 is refused the same
  way ("baseline predates fingerprint v2") until the v1.7 re-baseline
  re-records it.
- **Dependency set.** Each baseline stores the dependency pinset of its run
  (`provenance.deps.pinset_sha256`). The gate compares a pinned config with
  itself, so a run on another set differs from the baseline in its jars
  alone, which is not like for like: it is refused ("dependency set differs
  from the baseline"). `record` refuses a run that records no pinset. Runs
  record the pinset once the in-deployment dependency server lands in v1.7;
  until then no run can be recorded as a baseline.

Defaults are not part of `config_hash`, which is why each pinned file sets
every knob itself; `tests/test_perf_gate.py` fails if one is left to a
default. A batch run also has to match on what it realised: each batch
stage's executor count (the peak the run observed) and the number of datagen
pods must equal the pinned values. Continuous stages have no realised count;
see "Known gaps".

The table-maintenance policy is matched too. Every `metrics.json` records
`maintenance_policy_id` (the constant in
`src/lakebench/metrics/maintenance_policy.py`, which lists what each policy
does), and each baseline stores the id of the run it was recorded from. Only
runs under the current policy are compared or recorded, and a run whose
policy differs from its baseline's is refused. A run or baseline without the
field is the legacy policy `m1-legacy` (which covers runs before and after
the LB-174 fix, so it cannot be compared at all), and a run made with
`--skip-maintenance` is stamped `<id>+skipped`. Every baseline recorded
before the id existed therefore refuses current runs until it is
re-recorded.

Baselines are specific to the reference cluster. The storage classes are
pinned, but a different cluster behind the same names produces different
numbers; do not compare across clusters.

## Guards

A run is refused, never compared, when:

- it did not succeed;
- it is a local run (`lakebench run --local`), which the fingerprint does not
  otherwise tell apart from a cluster run;
- it is a batch run with `scale_ratio` outside 0.95 to 1.10 (0 means the
  bronze input volume was not measured, which is refused too; above 1.10
  means extra data, which flatters GB/s);
- it is a batch run whose bronze, silver or gold stages differ from the
  baseline run's. A missing query stage is not a refusal: the QpH metrics
  report it as missing, a regression;
- it is a continuous run in which no data flowed (`ingest_ratio` or rows/s
  zero or missing), whose `ingest_ratio` is above 1.05, or whose freshness
  was not measured. Since 1.6 `ingest_ratio` is bronze rows over the rows
  the trickle had released by the window's end (`released_rows`), not over
  the whole corpus (that is `corpus_ingest_ratio`). Above 1.05, bronze took
  more rows than were released, for example data left from an earlier run,
  which inflates rows/s. Below 1 is saturation, a real performance signal,
  and stays comparable;
- it is a continuous run whose window (stage seconds) differs from the pinned
  `run_duration` by more than 10%. A `--duration` override is not recorded in
  the snapshot, so this is how it is caught;
- it is a continuous run whose `corpus_drained` differs from the baseline
  run's. A drained run's freshness covers only the cycles that saw data;
- its results are not established. A continuous run's results come from its
  end-of-run result check over the settled corpus (see "Continuous Gate" and
  "Result check" in `benchmarking.md`); a run whose corpus did not settle, or
  whose result fingerprints differ from the baseline's, is refused like a
  batch run;
- its datagen fleet reported `data_quality` other than `complete`;
- its snapshot records a `config_sha256` that is not the pinned file's, or
  (a v1.7 run) records none;
- it is a batch run whose time to value was taken differently from the
  baseline's (from stage timestamps in one, from the scorecard in the other),
  or whose stages carry no timestamps while its datagen stage is stale or
  present on one side only. Without timestamps, time to value is the run's
  wall clock and may or may not include a generate;
- its batch stages are timed on a different basis from the baseline's: from
  v1.6 stages end at the Spark application's real end (`timing_source`
  `driver_container` or `spark_application`), before that on the 15 s
  job-monitor poll. A baseline recorded before v1.6 (no `stage_timing`) is
  poll-timed; re-record it (`scripts/perf_gate.py record --replace`) from a
  v1.6 run;
- it is a multi-cycle batch run (`cycles` above 1). Cycles 2 onwards generate
  data between gold and the next bronze, inside the time-to-value span, and
  `cycles` is not in the snapshot for the fingerprint to catch;
- its batch stage timestamps span less time than the stages' own seconds add
  up to. Runs recorded before v1.6 carry naive local timestamps, so this is a
  clock change (a DST fall-back) during the run; a spring-forward lengthens the
  span instead and reads as a time-to-value regression. From v1.6 the
  timestamps are UTC with the offset recorded and a DST change cannot move them.

Within a comparable run, some numbers are left out rather than trusted:

- `sustained_throughput_rps` of a continuous run whose corpus drained while
  data arrived for less than 90% of the window (`corpus_drained: true` and
  `window_arrival_fraction` under 0.9), or of a drained record from before
  the window was recorded (corpus rows over the window, a lower bound,
  LB-145). A drained run that arrived through at least 90% of the window is
  gated like any other. An excluded rows/s is never recorded as the baseline
  either.
- continuous stage seconds, which are the window length, not a measurement.
- the datagen numbers (`datagen_*`) when the datagen metrics were written
  more than 24 hours before the run started (they came from an earlier
  `generate`) or when only one of the baseline and the run has a datagen
  stage. `datagen_seconds` stays when it is the run's own generate time
  (`lakebench run --generate` writes no sidecar but attaches the last one). Generate once and run several times is a normal workflow. Nothing
  else is dropped with them: for batch runs time to value and GB/s are
  recomputed from the pipeline stages' own timestamps with the datagen stage
  left out, and GB/core-hr counts batch or continuous stages only. From v1.6
  the run's `start_time` is UTC with its offset and the age is exact. An older
  run's `start_time` is naive local time with no zone recorded, so the age is taken
  at its smallest over every UTC offset (-12h to +14h): a sidecar is called
  stale only when it is more than 24 hours old wherever the run happened, and
  the answer does not depend on the gate host's zone. The cost is a wide
  bound: the naive start has to be more than 38 hours after `written_at` for
  the sidecar to be dropped, which in real time is between 24 and 50 hours
  depending on the run host's zone (44 hours on a UTC-6 host).

The recomputed GB/s is the scorecard's: GB is summed over every non-datagen
stage, the untimed query stage included, as the scorecard sums it.

A batch baseline has to carry datagen numbers: `record` refuses a batch run
whose datagen sidecar is stale or which has no datagen stage, because such a
baseline would leave every later run's datagen ungated ("present on one side
only").

QpH is the median of `architecture.benchmark.iterations` samples per query
(3 in the batch pinned configs). The gate refuses a batch run whose recorded
samples per query differ from the pinned config's `iterations`, and reads a
run written before per-query repeats (no `samples` in its query records) as
one sample whatever its config snapshot says, because `lakebench run` did not
pass `iterations` to the runner before then. A single sample and a median of
three are different estimators: with right-skewed query noise the median
reads faster, so the drift between them is a bias, and a gate that exits
nonzero on regression should refuse rather than warn. `lakebench reproduce`
applies the same rule against the package's `benchmark_samples_per_query`
(1 for packages recorded before it existed) and refuses before running the
pipeline when the config asks for a different count. `lakebench compare`
warns instead: it runs two configs the user chose, and the sample count may
be what is being compared.

`maintenance_value_pct` is reported by `lakebench run` but not gated. It is
(post - pre) / pre, and both halves are gated on their own
(`pre_compaction_qph`, `composite_qph`); it has no good direction, since a
better write layout raises pre-maintenance QpH and so lowers it.

The storage settle wait before the post-maintenance round
(`maintenance_settle_seconds`) is not gated and is not a stage, so it does
not enter the recomputed time to value. It does raise `composite_qph` on
storage that settles slowly: a baseline recorded before the wait existed
(LB-150) measured the post round on unsettled storage, and should be
re-recorded rather than compared against.

`gate` and the release check also fail a required config whose run is not
newer than the baseline run (run ids are timestamp-prefixed): a baseline
compared with itself, or with an older run, proves nothing.

## Metrics and tolerances

The metric set and each metric's direction come from the metric registry
(`metrics/metric_registry.reproduce_class`), the same classification
`lakebench reproduce` uses.
Only the performance band is compared.

| Metric | Direction | Default tolerance |
|---|---|---|
| `time_to_value_seconds`, `<stage>_seconds`, `data_freshness_seconds`, `datagen_cpu_hr_per_tb` | lower is better | 10% |
| `pipeline_throughput_gb_per_second`, `compute_efficiency_gb_per_core_hour`, `composite_qph`, `sustained_throughput_rps`, `datagen_aggregate_mbps`, `datagen_mbps_per_pod` | higher is better | 10% |
| `query_qph_<query>` (3600 / query seconds) | higher is better | 20% |
| `pre_compaction_qph` | higher is better | 10% |

Only drift in the bad direction fails. An improvement past the tolerance is
reported as `improved` and passes; record a new baseline if it should become
the bar. A metric in the baseline that the run lacks (a query that failed,
missing datagen telemetry) fails as `missing`.

Per-config overrides go in the store entry:

```yaml
  c360-batch-s10:
    tolerances:
      query_qph_Q1_full_aggregation_scan: {pct: 30}
      data_freshness_seconds: {abs: 30}
```

## Record a baseline

1. Run the pinned config as-is on the reference cluster. Identity and
   credentials come from the environment, so the file does not change:

   `LAKEBENCH_PERF_NAME` becomes the namespace. The pinned configs are Hive
   recipes, so it must be at most 23 characters; a longer one is refused at
   config load (LB-153). The defaults in the files fit (17 to 19 characters).
   Setting the variable does not change `config_hash`, which is taken before
   substitution.

   ```bash
   export LAKEBENCH_PERF_NAME=perf-c360-batch-s10
   export LAKEBENCH_S3_ENDPOINT=... LAKEBENCH_S3_ACCESS_KEY=... LAKEBENCH_S3_SECRET_KEY=...
   lakebench deploy   benchmarks/perf/c360-batch-s10.yaml
   lakebench generate benchmarks/perf/c360-batch-s10.yaml
   lakebench run      benchmarks/perf/c360-batch-s10.yaml
   lakebench destroy  benchmarks/perf/c360-batch-s10.yaml --force
   ```

2. Record the run. `scripts/perf_gate.py record` still requires `--git-sha`
   (it does not read the commit that newer `metrics.json` files record under
   `provenance.git_sha`):

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

`compare` exits 0 on pass, 1 on a regression, 2 when refused, when there is
no baseline, or when the verdict is `NOT_COMPARABLE`.

A run whose pre-benchmark maintenance stopped early (a statement timed out or
the 30 min cap was hit, `maintenance_stopped` in the scores), or ran while
stream apps were still present (`maintenance_live_streams`), has no clean
post-maintenance QpH: a rewrite may still have been running during the
benchmark, or the streams kept writing under it. The gate then leaves `composite_qph` and every `query_qph_*` out
(status `excluded`, with the stop reason) and always names the stop in the
comparison's reasons; `pre_compaction_qph` stays gated. If no QpH metric is
left to gate (always at scale 50 and above, where no pre-maintenance round
runs), the verdict is `NOT_COMPARABLE`: never a pass, exit 2, and the release
check does not report it as ok. Such a run can never be recorded as a
baseline. `gate` and the release check pick, for each pinned config, the
newest successful run whose fingerprint matches, searching
`lakebench-output/runs` (or `$LAKEBENCH_PERF_RUNS_DIR`) and `uat/perf/`. Name
a run explicitly with `--perf-run NAME=RUN` on `release_gate.py` or
`--run NAME=RUN` on `perf_gate.py gate`; a run id is looked up in both
directories (`compare` and `record` do the same). A run id present in both
with different contents is refused, for a named run and for one the gate
found itself.

The release check fails when a required config has no accepted baseline, no
run can be found for it, or its run is refused or regressed. An optional
config is reported but never fails the release. CI has no local runs
directory, so a release checks in the `metrics.json` of each required perf
run as `uat/perf/run-<id>/metrics.json` alongside `uat/results-<version>.md`.

## Known gaps

The fingerprint can only compare what `build_config_snapshot` records. Some
knobs that move numbers are not in it, such as datagen CPU and the datagen
timestamp range; the config-file sha256 covers them for runs of a pinned
file. Code-default changes outside the Spark manifest's conf and sizing are
not fingerprinted: the Spark pods' environment, the restart policy, and the
continuous bronze-verify preflight and AML scoring jobs, which are not
stages the gate times. A change there is compared, not refused, like any
other code change. The datagen sidecar does not record the image
that wrote it, so a run that reuses a recent sidecar from a different image is
not caught either.

Continuous runs are not checked for realised executor counts.
`build_pipeline_benchmark` fills a continuous stage's `executor_count` from the
snapshot's `executor_overrides` (or the profile default), not from the pods
that ran, so a check would compare the config with itself. The fingerprint
still pins the requested counts; a cluster that could not schedule them is not
caught until the continuous run path records observed executor counts.

The datagen staleness bound is wide: a sidecar written up to 50 hours
before the run (real time, depending on the run host's zone) can still be
attributed to it, because the run's zone is unknown. A run that reuses the
baseline's own sidecar inside that window compares datagen with itself.
Runs recorded from v1.6 carry `start_time` with its UTC offset, and the gate
uses the real 24-hour bound for them; the wide bound applies to older runs only.

## Seeding

`python scripts/perf_gate.py seed` scans the runs directory for runs that
match a pending config exactly and, with `--write`, records the newest one.
Runs whose pre-benchmark maintenance stopped are skipped (and listed); if the
newest remaining run is refused as a baseline, seed tries the next-newest, and
one config's failure does not stop the others.
Against the 98 runs on the reference workstation on 2026-09-24 nothing
matched, so all three configs were "pending first run" at that point
(`c360-batch-s10` was recorded on 2026-09-25, before v1.6 stage timing):

- every earlier run used the floating `lb-datagen:latest` image, left
  per-job executor counts and datagen mode to defaults, and all 22 c360
  scale-10 runs used 512 MB datagen files and predate the `workload_schema`
  snapshot field;
- every AML batch run records `scale_ratio` 0.0, because the bronze stage
  reports no input volume for the financial schema, so the scale guard
  refuses them even where the sizing is close.
