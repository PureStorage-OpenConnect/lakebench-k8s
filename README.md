# Lakebench

[![Python 3.10+](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

**A/B testing for lakehouse architectures on Kubernetes.**

Deploy a complete lakehouse stack from a single YAML, run a medallion pipeline
at any scale, and get a scorecard you can compare across configurations.

<!-- TODO: Add terminal recording / screenshot of `lakebench run` output here -->

## Why Lakebench?

- **Compare stacks.** Swap catalogs (Hive, Polaris), query engines (Trino,
  Spark Thrift, DuckDB), and table formats -- same data, same queries,
  different architecture. Side-by-side scorecard comparison.
- **Test at scale.** Run the same workload at 10 GB, 100 GB, and 1 TB to find
  where throughput plateaus or resources saturate on your hardware.
- **Measure freshness.** Continuous mode keeps data arriving through the
  pipeline and benchmarks query performance under ongoing ingest load.

## Workloads

Lakebench ships two workload schemas. The stack is the same either way; the
schema flag routes datagen and the pipeline scripts.

- **Customer 360** (default). Retail interactions from ~8 channels flow
  through bronze / silver / gold into an executive dashboard and the
  8-query analytical benchmark. `workload.schema: customer360`.
- **Financial crime (AML).** pacs.008 wire messages flow through
  bronze / silver / gold. Six W-rule detectors score against planted
  typologies (structuring, gather-scatter, rapid-layering, dormant
  reactivation, corridor risk, high-velocity chains). Per-rule recall,
  precision, and pattern-span are joined against a scikit-learn
  reference detector and a distribution-leakage gate, so a rule cannot
  read high recall from a label proxy without being caught.
  `workload.schema: financial`. See
  [AML Scoring](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/aml-scoring.md)
  for what precision and recall measure here vs what an AML ops team
  cares about.

## Quick Start

```bash
pip install lakebench-k8s
```

> Pre-built binaries (no Python required) are available on
> [GitHub Releases](https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases).

```bash
pip install lakebench-k8s
lakebench init                                 # quick setup (4 questions), writes lakebench.yaml
lakebench run lakebench.yaml --generate --yes  # deploy + generate + pipeline + benchmark
lakebench results lakebench.yaml               # view scorecard
lakebench destroy lakebench.yaml               # tear down what this deployment owns
```

`--yes` lets `run` deploy the namespace and components when they do not
exist yet; without it `run` stops and asks you to run `lakebench deploy`
first. The Spark Operator is shared cluster infrastructure: a cluster admin
installs it once with `lakebench admin install-spark-operator` (see
[Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md)).

Minimum config -- 4 lines:

```yaml
# lakebench.yaml
endpoint: http://s3.example.com:80
access_key: YOUR_KEY
secret_key: YOUR_SECRET
scale: 10                          # 1 = ~10 GB, 10 = ~100 GB, 100 = ~1 TB
```

Name is auto-generated. Buckets default to `<name>-bronze`, `<name>-silver`
and `<name>-gold`, so they are unique on stores where bucket names are
global (FlashBlade, AWS S3). Recipe defaults to `hive-iceberg-spark-trino`.
Override anything with flat fields or nested YAML:

```yaml
# lakebench.yaml (with overrides)
name: flashblade-polaris
recipe: polaris-iceberg-spark-trino
endpoint: http://10.21.227.93:80
access_key: ${S3_ACCESS_KEY}       # env var substitution
secret_key: ${S3_SECRET_KEY}
scale: 50
mode: batch
spark_image: apache/spark:4.1.1-python3
```

Eleven recipes are available -- see [Recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md)
for the full list.

Compare two configurations side-by-side:

```bash
lakebench deploy config-hive.yaml && lakebench deploy config-polaris.yaml
lakebench compare config-hive.yaml config-polaris.yaml --generate
```

`compare` runs each config through the pipeline and benchmark in turn, then
destroys that deployment unless you pass `--keep`. Deploy both configs
first; `--generate` fills each bronze bucket before its run. The two configs
need different names and bucket names, or the first destroy empties the
second run's data.

For all recipes, see [`examples/`](examples/) or run `lakebench init --advanced`
for the full interactive wizard.

## What You Get

After `lakebench run` completes, the terminal prints a scorecard:

```
 ─ Pipeline Complete ──────────────────────────────
  bronze-verify         142.0 s
  silver-build          891.0 s
  gold-finalize         234.0 s
  benchmark              87.0 s

  Scores
    Time to Value:        1354.0 s
    Throughput:           0.782 GB/s
    Efficiency:           3.41 GB/core-hr
    Scale:                100.0% verified
    QpH:                  2847.3

  Full report: lakebench report
 ──────────────────────────────────────────────────
```

`lakebench report` generates an HTML report with per-query latencies,
bottleneck analysis, and optional platform metrics (CPU, memory, S3 I/O per
pod).

## How It Works

```
                    ┌──────────────────────────────────┐
                    │         lakebench.yaml           │
                    └────────────┬─────────────────────┘
                                 │
                    ┌────────────▼─────────────────────┐
                    │   deploy (Kubernetes namespace,   │
                    │   S3 secrets, PostgreSQL, catalog, │
                    │   query engine, observability)     │
                    └────────────┬─────────────────────┘
                                 │
     Raw Parquet ──► Bronze (validate) ──► Silver (enrich) ──► Gold (aggregate)
         S3              Spark                Spark               Spark
                                                                    │
                                                        ┌───────────▼──────────┐
                                                        │  8-query benchmark   │
                                                        │  (Trino / DuckDB /   │
                                                        │   Spark Thrift)      │
                                                        └──────────────────────┘
```

## Prerequisites

- `kubectl` and `helm` on PATH
- Kubernetes 1.26+ with capacity for the Spark job profiles. Customer360 or
  AML batch at scale 1 requests a peak of **36 cores and 512 GB RAM**
  (driven by `silver-build`), and a single executor pod needs 60 GB on one
  node. AML continuous at scale 1-10 requests **118 cores and 980 GB**,
  because its three streaming jobs run at once; AML batch at scale 100
  peaks at 76 cores for the pipeline, but its data generation ran about
  350 cores (44 pods x 8 cores, measured in run-20260925-104703-c02890).
  The request figures come from `compute_peak_requirements()`. See
  [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md)
  for the full table. `lakebench run` fails fast if the cluster is too small.
- S3-compatible object storage. FlashBlade and Garage are validated; others are
  expected to work. Run `lakebench config storage` to check yours. See
  [Storage Backends](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/storage-backends.md).
- [Kubeflow Spark Operator 2.5.1+](https://github.com/kubeflow/spark-operator),
  installed once per cluster with `lakebench admin install-spark-operator`.
  `deploy` adds its namespace to the operator's watch list itself, under the
  cluster lock; do not edit `spark.jobNamespaces` by hand.
- [Stackable Hive Operator](https://docs.stackable.tech/home/stable/hive/) for
  Hive recipes (not needed for Polaris)

## Commands

| Command | Description |
|---------|-------------|
| `init` | Generate a starter config file |
| `config validate` | Check config and cluster connectivity |
| `config storage` | Check the S3 backend supports what lakebench needs |
| `config show` | Show the resolved configuration and its peak requested resources |
| `deploy` | Deploy all infrastructure components |
| `generate` | Generate synthetic data at the configured scale |
| `run` | Execute the medallion pipeline and benchmark |
| `benchmark` | Run the 8-query benchmark standalone |
| `query` | Execute ad-hoc SQL against the active engine |
| `status` | Show deployment status |
| `results` | Show the latest run's scorecard in the terminal |
| `report` | Generate HTML scorecard report |
| `compare` | Run two configs in turn and compare their results |
| `config recommend` | Recommend a scale factor for the connected cluster |
| `admin` | Cluster-admin setup: `install-spark-operator`, `doctor`, `status`, `repair-operator` |
| `financial` | AML operator actions (`score`, `replay`, `reproduce`) |
| `destroy` | Tear down all deployed resources |

See [CLI Reference](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/cli-reference.md)
for flags and options.

## Component Versions

| Component | Version |
|-----------|---------|
| Apache Spark | 3.5.4, 4.0.2, 4.1.1 |
| Spark Operator | 2.5.1 (Kubeflow) |
| Apache Iceberg | 1.11.0 |
| Delta Lake | 4.0.0 / 4.1.0 (auto, by Spark version) |
| Hive Metastore | 3.1.3 (Stackable 25.7.0) |
| Apache Polaris | 1.6.0 |
| Trino | 483 |
| DuckDB | bundled (Python 3.11) |
| PostgreSQL | 16, 17, 18 |

All versions are overridable in the YAML config. See
[Supported Components](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/supported-components.md).

## Documentation

- [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md) -- prerequisites, install, first run
- [Configuration](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/configuration.md) -- full YAML reference
- [Recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md) -- catalog + format + engine combinations
- [Compatibility Matrix](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/compatibility-matrix.md) -- Spark, Iceberg, and Delta version support
- [Running Pipelines](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/running-pipelines.md) -- batch and continuous modes
- [Benchmarking](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/benchmarking.md) -- scorecard and query benchmark
- [AML Scoring](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/aml-scoring.md) -- financial-crime / AML workload: rule recall, reference detector, leakage gate
- [Architecture](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/architecture.md) -- system design
- [Storage Backends](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/storage-backends.md) -- validated S3 backends and conformance checks
- [Troubleshooting](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/troubleshooting.md) -- common errors and fixes

## License

Apache 2.0
