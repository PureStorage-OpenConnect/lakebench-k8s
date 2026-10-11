# Lakebench

[![Python 3.10+](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/LICENSE)

Lakebench deploys a lakehouse stack on Kubernetes from one YAML file, runs a
data workload through it, and reports how it behaved.

Each run records what produced it: corpus, components, query set and the
Lakebench caps that applied. A reader can check that two runs did the same
work before comparing their numbers.

## What it is for

- **Compare stacks.** Swap the catalog (Hive, Polaris), query engine (Trino,
  Spark Thrift, DuckDB) or table format (Iceberg, Delta). Data and queries
  stay the same.
- **Test at scale.** Run the same workload at 10 GB, 100 GB and 1 TB to find
  where throughput stops growing on your hardware.
- **Measure freshness.** Continuous mode keeps data arriving and measures
  queries under ongoing ingest.

## Workloads

- **Customer 360** (default, `workload.schema: customer360`). Retail
  interactions from about 8 channels flow through bronze, silver and gold
  into an executive dashboard and an 8-query benchmark.
- **Financial crime, AML** (`workload.schema: financial`). pacs.008 wire
  messages from generator `datagen-v2-rs-0.4` flow through bronze, silver and
  gold. `lakebench run` scores nine detection rules for recall and precision
  against planted typologies. Iceberg recipes only. See
  [AML Scoring](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/aml-scoring.md)
  for what those numbers mean.

## Quick start

```bash
pip install lakebench-k8s
```

Binaries that need no Python are on
[GitHub Releases](https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases).

```bash
lakebench init                                          # writes lakebench.yaml; set the S3 endpoint and export the two key variables it names
lakebench admin install --component all lakebench.yaml  # once per cluster, by a cluster admin
lakebench plan lakebench.yaml                           # what the run needs and whether the cluster has room; changes nothing
lakebench run lakebench.yaml --generate --yes           # deploy, generate, pipeline, benchmark
lakebench report lakebench.yaml                         # print the scorecard
lakebench destroy lakebench.yaml                        # remove what this deployment created (asks first; --yes skips)
```

- `init --workload financial` writes an AML config.
- `--yes` lets `run` deploy the namespace and components. Without it, `run`
  stops and asks you to run `lakebench deploy` first.
- The Spark Operator, the Stackable operators (Hive recipes), the scratch
  StorageClass and the observability stack are shared by the cluster.
  `deploy` only checks them. A cluster admin installs them once with
  `admin install`.

The full walkthrough is
[Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md).

## Minimum config

```yaml
# lakebench.yaml, as `lakebench init` writes it
name: lb-alice-7f3c                # names the namespace and the buckets
recipe: polaris-iceberg-spark-trino
workload:
  schema: customer360
  datagen:
    scale: 1                       # 1 = ~10 GB, 10 = ~100 GB, 100 = ~1 TB
platform:
  storage:
    s3:
      endpoint: http://s3.example.com:80
      access_key: "${LAKEBENCH_S3_ACCESS_KEY}"
      secret_key: "${LAKEBENCH_S3_SECRET_KEY}"
```

- Every command that changes data needs `name`. Without it the command is
  refused and names the field to add.
- Buckets default to `<name>-bronze`, `<name>-silver` and `<name>-gold`.
  They stay unique on stores where bucket names are global (FlashBlade, AWS
  S3).
- The recipe sets the catalog, table format and engines. A component that
  contradicts it (say `catalog.type: hive` under a Polaris recipe) is
  refused at load.
- Older forms still load with a deprecation note: no `recipe:` (means
  `hive-iceberg-spark-trino`; v1.8 requires a recipe) and flat keys such as
  `endpoint:`
  ([Configuration](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/configuration.md#flat-fields-deprecated)).
- Polaris needs no client secret in the config. `deploy` generates one per
  deployment and stores it in the namespace.

There are eleven recipes. List them with `lakebench config recipes`, start
one with `lakebench init --recipe <name>`, or see
[`examples/`](https://github.com/PureStorage-OpenConnect/lakebench-k8s/tree/main/examples)
and [Recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md).

## Support states

Support is judged per workload, recipe and mode:

- **supported**: a live run on the release tree validated it end to end.
- **unverified**: valid, not release-validated.
- **unsupported**: refused before a run.

Every run records its state in `metrics.json`. The table for this release is
in the
[Compatibility Matrix](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/compatibility-matrix.md#support-states).

## Prerequisites

- `kubectl` and `helm` on PATH.
- Kubernetes 1.26+, with room for the minimum below.
- S3-compatible object storage. FlashBlade and Garage are validated; others
  are expected to work. Check yours with `lakebench config storage`. See
  [Storage Backends](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/storage-backends.md).
- [Kubeflow Spark Operator 2.5.1+](https://github.com/kubeflow/spark-operator),
  installed once with `lakebench admin install --component spark-operator`.
  `deploy` adds its namespace to the operator's watch list under the cluster
  lock. Do not edit `spark.jobNamespaces` by hand.
- [Stackable Hive Operator](https://docs.stackable.tech/home/stable/hive/)
  for Hive recipes only.

The minimum cluster for `hive-iceberg-spark-trino` is below. `lakebench run`
fails fast when the cluster is too small.

- **Batch:** datagen pods that do not fit wait their turn. The minimum is
  the Spark peak plus the always-on pods.
- **Continuous:** the figure carries the scale's offered load. A smaller
  cluster runs with fewer executors, warns "cannot balance" naming the
  stage, and fails the balance check.

[Sizing](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/sizing.md)
explains each figure.

<!-- BEGIN GENERATED: sizing-minimums -->
<!-- Generated from the code by `python3.11 scripts/gen_sizing_tables.py`; do not edit by hand. -->

| Workload | Mode | Scale | Minimum CPU | Minimum RAM | Scratch PVC (if enabled) | Largest pod |
|:---|:---|---:|---:|---:|---:|---:|
| Customer 360 | batch | 1 | 41 cores | 544 GB | 400 Gi | 8 cores / 60 GB |
| Customer 360 | batch | 10 | 48 cores | 572 GB | 600 Gi | 8 cores / 60 GB |
| Customer 360 | batch | 100 | 114 cores | 1,340 GB | 5,400 Gi | 8 cores / 60 GB |
| Customer 360 | continuous | 1 | 46 cores | 363 GB | 640 Gi | 4 cores / 40 GB |
| Customer 360 | continuous | 10 | 54 cores | 391 GB | 640 Gi | 4 cores / 40 GB |
| Customer 360 | continuous | 100 | 222 cores | 1,896 GB | 3,220 Gi | 8 cores / 80 GB |
| AML | batch | 1 | 41 cores | 544 GB | 400 Gi | 8 cores / 60 GB |
| AML | batch | 10 | 48 cores | 572 GB | 600 Gi | 8 cores / 60 GB |
| AML | batch | 100 | 114 cores | 1,340 GB | 5,400 Gi | 8 cores / 60 GB |
| AML | continuous | 1 | 135 cores | 1,254 GB | 2,300 Gi | 4 cores / 40 GB |
| AML | continuous | 10 | 183 cores | 1,682 GB | 3,200 Gi | 4 cores / 40 GB |
| AML | continuous | 100 | 817 cores | 7,815 GB | 14,600 Gi | 16 cores / 160 GB |

- AML continuous scale 100 cannot balance at any cluster size: silver-stream needs ~45 executors x 16 cores to carry the offered load; the executor cap allows 28, so its lag will grow and the run will fail the balance check. The minimum is the cluster that runs it at the executor cap.

<!-- END GENERATED: sizing-minimums -->

## Results

Each `lakebench run` prints a scorecard (seconds per stage and the run's
scores). It also writes `lakebench-output/runs/run-<id>/report.html` with
per-query latencies and bottleneck analysis. With observability on, the
report adds pod CPU, memory and S3 I/O.

`lakebench report` prints the scorecard and the HTML path; `--render` writes
a fresh copy. The `experiment` block in `metrics.json` records what produced
the run:

- workload and generator version, seed, recipe and component versions
- scale, mode and the maintenance that ran
- which Lakebench caps applied and whether they bound
- the support state, and the benchmark query set id

## Comparing two stacks

Run each config, then read the two reports side by side:

```bash
lakebench deploy config-hive.yaml && lakebench run config-hive.yaml --generate --yes
lakebench deploy config-polaris.yaml && lakebench run config-polaris.yaml --generate --yes
lakebench report config-hive.yaml
lakebench report config-polaris.yaml
```

The Experiment section of each HTML report lists:

- the corpus and datagen image
- the components and the maintenance that ran
- the stages and rules that ran
- the benchmark query set id
- for AML batch runs, the alert set

Compare performance only when the corpus, the query set id and (AML batch)
the alert set match. Lakebench does not compare query answers: check the
per-query row counts and numbers in both reports by eye. A number bounded by
a Lakebench cap is labelled as such.

## Upgrading

- From 1.6: every change that can break a config, command line or script,
  with the fix, is in
  [UPGRADING-1.7.md](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/UPGRADING-1.7.md).
  Redeploy each deployment once after upgrading.
- Numbers from 1.5 and earlier are not comparable with 1.7. Before 1.6 no
  Iceberg snapshot expiry or Delta VACUUM ran, and the v1.7 datagen image
  changes the corpus.
- What changed in each release:
  [CHANGELOG](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/CHANGELOG.md).

## Documentation

- [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md): install and first run
- [Running Pipelines](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/running-pipelines.md): batch and continuous modes
- [Troubleshooting](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/troubleshooting.md): errors and fixes
- Reference: [CLI](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/cli-reference.md),
  [Configuration](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/configuration.md),
  [Sizing](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/sizing.md),
  [Supported Components](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/compatibility-matrix.md#components),
  [Benchmarking](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/benchmarking.md),
  [AML Scoring](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/aml-scoring.md),
  [Architecture](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/architecture.md)
- [All docs](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/README.md)

## License

Apache 2.0
