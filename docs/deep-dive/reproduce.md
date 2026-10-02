# The `lakebench reproduce` contract

lakebench's performance claims must be reproducible on request. A "we
saw 4.34 GB/s" that nobody else can verify is a marketing statement,
not a benchmark result. This article documents what reproducibility
means in this project and how the `reproduce` command implements it.

## The gap this closes

Before this change, "reproduce a published number" meant: read the run
config, deploy the same recipe, hope the cluster is roughly the same,
run the pipeline, eyeball the resulting numbers against the published
ones. Anyone who has tried to reproduce a benchmark knows how much
drift accumulates across the layers of "roughly."

The layers that matter:

- **Software**: commit SHA of the code that ran.
- **Container**: image tag and digest of the datagen and spark images.
- **Cluster**: OpenShift version, node count, node CPU/RAM, storage
  provisioner, network fabric.
- **Config**: the exact YAML that drove the run, including credentials
  and endpoints. (Credentials are redacted; endpoints and bucket names
  survive.)
- **Data**: scale factor, seed, corpus window, and the datagen
  version's realism knobs.
- **Storage**: which storage classes the PVCs bound to, and their
  replication factors.

A reproduce that reports "close enough" without checking each layer
is not reproducible. A reproduce that fails on the first version drift
is not usable.

What the command checks today is narrower than this list. It records
and checks the software layer (commit SHA), the config (a redacted
snapshot plus a reference to the YAML to run) and the experiment
(workload, corpus, seed, scale, mode, query set, maintenance policy,
per-query result fingerprints). It does not pull images or compare
image digests, and it does not inspect cluster topology or storage
classes.

## The contract

A reproduction package is one YAML file per published benchmark run,
written by `lakebench reproduce --record`. It carries
(`_build_package` in `src/lakebench/cli/_reproduce.py`):

```yaml
schema_version: 1
reproduction_metadata:
  commit_sha: "<7-character HEAD at record time>"
  recorded_at: "<UTC timestamp>"
  source_run_id: "<run id>"
  deployment_name: "<config name>"
  pipeline_mode: batch            # or continuous
  config_reference: "<path, relative to the package file>"
  expected_numbers: {}            # every populated metric of the source run
  query_set_id: "<QpH query-set id>"
  maintenance_policy_id: "<table-maintenance policy id>"
  benchmark_samples_per_query: 3
  experiment_identity: {}         # workload, corpus, seed, scale, mode
  result_fingerprints: {}         # what each benchmark query returned
  tolerance_pct:
    performance: 20.0             # DEFAULT_TOLERANCES
    correctness: 0.0
  config_snapshot: {}             # redacted
  datagen_fleet_summary: {}       # pods_reported, data_quality
```

Recording refuses a source run with no numbers, one measured under a
maintenance policy other than the current one, one without an
experiment block, one whose benchmark results are not established or
lack usable result fingerprints, and one with no `scale_ratio` (batch)
or `ingest_ratio` (continuous).

`lakebench reproduce <package.yaml>` then:

1. Loads and validates the package (schema version 1, finite numbers,
   a correctness tolerance of exactly 0).
2. Compares the current commit (`git rev-parse --short=7 HEAD`) with
   `commit_sha`. On a mismatch it exits 14 (requirement unmet) unless
   `--allow-commit-drift` is passed, which turns it into a warning.
3. Resolves the config: `--config PATH` if given, otherwise
   `config_reference` resolved relative to the package file's
   directory. A missing file exits 2.
4. Before running anything, exits 2 if the config's
   `architecture.benchmark.iterations` differs from
   `benchmark_samples_per_query` (batch packages with QpH), if the
   package's maintenance policy differs from the running version's,
   or if the package has no `experiment_identity`. `--dry-run` stops
   here.
5. Destroys any existing deployment, then deploys, generates and runs
   the pipeline, and destroys again unless `--keep` is set.
6. Exits 14 if the run took a different number of samples per query,
   ran under a different maintenance policy, or is not the package's
   experiment or returned different benchmark results.
7. Compares actual against expected per metric. Correctness metrics
   (`scale_ratio`, `ingest_ratio`) have zero tolerance in either
   direction; performance metrics use the performance band in the
   metric's bad direction. QpH across different query sets is
   reported as incomparable and counts as performance drift (a
   package recorded before query-set ids is not compared on QpH).
8. Exits 0 (pass) or 14 (performance drift, a missing performance
   metric, a correctness violation or a missing correctness metric).
   The refusals in steps 1, 3 and 4 exit 2, and a pipeline that could
   not run, or whose run cannot be found, exits 1. In 1.6 performance
   drift exited 1 and correctness drift exited 2.

## What the tolerance is for

Performance varies. Same code, same cluster, same day, back-to-back
runs will differ by a few percent because of network, storage cache
warmth, and Kubernetes scheduling. A 20% band is generous enough to
absorb this without lying about drift; it is tight enough that a
factor-of-two regression fails.

Correctness has no tolerance because there is no legitimate reason
for the pipeline to process a different share of the data than the
recorded run (`scale_ratio` in batch; `ingest_ratio`, bronze rows over
the rows the trickle had released, in continuous). Any correctness
drift means the pipeline has a bug or the config is different.

## What breaks reproducibility

Some things we cannot control:

- AML generator output does not depend on the thread pool or pod
  count: at seed 43 the e14d0fd image produced byte-identical objects
  thread-throttled and with 4 pods, and 034f998 (rebuilt unchanged as the 1.6.0 release image) is byte-identical to
  e14d0fd. This was not measured for Customer 360, where Snappy
  compression can vary by about 0.5% with the order rows reach the
  Parquet writer. The package does not record the pool size, so keep
  `datagen.cpu` and `datagen.generators` as the source run's config
  snapshot has them.
- FlashBlade S3 latency has a bimodal distribution; a full flash tray
  vs a mixed workload can add 5-10% wall time.
- OpenShift's control plane latency changes with etcd size; a heavily
  used cluster will schedule Spark executors 30-60 seconds slower.

The package captures these as environmental facts, not as expected
numbers. If a reproduce reports drift and every layer above is
matched, the difference is environmental and the CI should be tuned
to widen the tolerance rather than declare a regression.

## What "reproducible" does NOT mean

- **Not bit-identical output.** Snappy compression, executor
  scheduling, and Iceberg snapshot IDs vary. The Parquet _content_
  is bit-identical for the same (seed, scale, config, image); the
  wrapping is not.
- **Not identical wall-clock.** See tolerance discussion above.
- **Not identical resource usage.** CPU-seconds vary with CFS
  scheduler decisions; memory usage varies with JVM garbage-
  collection timing.

What IS reproducible: distribution shape of the data, per-stage row
counts, what each benchmark query returns (the package's result
fingerprints), and output size within a compression-noise band.

## Wiring

Package files live under `docs/reproductions/`. Each file is
generated from an actual `metrics.json` by
`lakebench reproduce --record <run-id> --write <path>`, so the
"expected numbers" cannot drift from a real run.

The reproduce command lives in
`src/lakebench/cli/_reproduce.py`. It shares the deploy / generate /
run / destroy plumbing with the main CLI; it does not reimplement
any of it. The default bands are `DEFAULT_TOLERANCES` in
`_reproduce.py`, and each metric's band and direction come from the
metric registry (`metrics/metric_registry.py`), not from the package.

## For contributors

If your PR changes a performance-affecting code path, run one
reproduce against a package your change should NOT regress and post
the output in the PR. The reproduce output is short and posts
cleanly: expected vs actual per metric, drift percentage, pass /
warn / fail.

If your PR intentionally changes a performance number, generate a
new package with the post-change metrics.json and reference the old
package in the commit message as "supersedes"; do not just delete
the old one.
