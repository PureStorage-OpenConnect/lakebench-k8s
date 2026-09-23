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

## The contract

A reproduction package is one YAML file per published benchmark run.
It carries:

```yaml
reproduction_metadata:
  commit_sha: "ead6722"
  image_digest:
    lb_datagen: "sha256:a57c2ce729ff..."
    apache_spark: "sha256:..."
    trinodb_trino: "sha256:..."
  cluster_topology:
    openshift_version: "4.19"
    node_count: 14
    node_cpu_cores: 32
    node_memory_gb: 128
  expected_numbers:
    datagen_aggregate_mbps: 154.4
    datagen_cpu_hr_per_tb: 14.36
    time_to_value_seconds: 405
    silver_build_seconds: 90
    qph: 1305
  tolerance_pct:
    performance: 20   # allow up to 20% wall-time / throughput drift
    correctness: 0    # zero tolerance for row-count / semantic drift
```

`lakebench reproduce <package.yaml>` then:

1. Loads the reproduction package.
2. Fetches the current commit SHA (`git rev-parse HEAD`) and warns
   if it does not match `commit_sha` in the package.
3. Pulls the images and reports their digest; warns on mismatch.
4. Runs `kubectl get nodes` and warns on cluster-topology drift.
5. Deploys, generates, runs the pipeline, destroys -- the standard
   four-step flow.
6. Extracts the actual numbers from `metrics.json`.
7. Compares actual vs expected under the tolerance bands.
8. Exits 0 (pass), 1 (drift exceeded), or 2 (correctness violation).

Correctness violations (row count off, missing stages, wrong scale)
fail with exit 2 regardless of tolerance. Performance drift under the
band exits 0. Performance drift over the band exits 1 with a
per-metric breakdown of what drifted and by how much.

## What the tolerance is for

Performance varies. Same code, same cluster, same day, back-to-back
runs will differ by a few percent because of network, storage cache
warmth, and Kubernetes scheduling. A 20% band is generous enough to
absorb this without lying about drift; it is tight enough that a
factor-of-two regression fails.

Correctness has no tolerance because there is no legitimate reason
for `bronze_rows != datagen_rows`. Any correctness drift means the
pipeline has a bug or the config is different.

## What breaks reproducibility

Some things we cannot control:

- Snappy compression ratio varies by ~0.5% run-to-run based on the
  order Rust hands rows to the parquet writer. That is deterministic
  in seed, but is a different determinism across a rayon pool size
  change. So a package must pin `rayon_pool_size`.
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

What IS reproducible: distribution shape of the data, precision and
recall of detection rules against planted typologies, per-stage row
counts, output size within a compression-noise band.

## Wiring

Package files live under `docs/reproductions/`. Each file is
generated from an actual `metrics.json` by
`lakebench reproduce --record <run-id> --write <path>`, so the
"expected numbers" cannot drift from a real run.

The reproduce command lives in
`src/lakebench/cli/_reproduce.py`. It shares the deploy / generate /
run / destroy plumbing with the main CLI; it does not reimplement
any of it. Comparison thresholds are documented in
`_reproduce.py::_TOLERANCES` as a single source of truth.

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
