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
- **Measure freshness.** Continuous mode streams data through the pipeline and
  benchmarks query performance under sustained ingest load.

## Quick Start

```bash
pip install lakebench-k8s
```

> Pre-built binaries (no Python required) are available on
> [GitHub Releases](https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases).

```bash
lakebench init --interactive             # generate config with S3 prompts
lakebench validate lakebench.yaml        # check config + cluster connectivity
lakebench deploy lakebench.yaml          # deploy the stack
lakebench run lakebench.yaml --generate  # generate data + run pipeline + benchmark
lakebench report                         # view HTML scorecard
lakebench destroy lakebench.yaml         # tear down everything
```

The `recipe` field selects your architecture in one line. The `scale` field
controls data volume.

```yaml
# lakebench.yaml (minimal)
deployment_name: my-test
recipe: hive-iceberg-spark-trino   # or polaris-iceberg-spark-duckdb, etc.
scale: 10                          # 1 = ~10 GB, 10 = ~100 GB, 100 = ~1 TB
s3:
  endpoint: http://s3.example.com:80
  access_key: ...
  secret_key: ...
```

Eight recipes are available -- see [Recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md)
for the full list.

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
- Kubernetes 1.26+ (minimum 8 CPU / 32 GB RAM for scale 1)
- S3-compatible object storage (FlashBlade, MinIO, AWS S3, etc.)
- [Kubeflow Spark Operator 2.4.0+](https://github.com/kubeflow/spark-operator)
  (or set `spark.operator.install: true`)
- [Stackable Hive Operator](https://docs.stackable.tech/home/stable/hive/) for
  Hive recipes (not needed for Polaris)

## Commands

| Command | Description |
|---------|-------------|
| `init` | Generate a starter config file |
| `validate` | Check config and cluster connectivity |
| `info` | Show deployment configuration summary |
| `deploy` | Deploy all infrastructure components |
| `generate` | Generate synthetic data at the configured scale |
| `run` | Execute the medallion pipeline and benchmark |
| `benchmark` | Run the 8-query benchmark standalone |
| `query` | Execute ad-hoc SQL against the active engine |
| `status` | Show deployment status |
| `report` | Generate HTML scorecard report |
| `recommend` | Recommend cluster sizing for a scale factor |
| `destroy` | Tear down all deployed resources |

See [CLI Reference](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/cli-reference.md)
for flags and options.

## Component Versions

| Component | Version |
|-----------|---------|
| Apache Spark | 3.5.4 |
| Spark Operator | 2.4.0 (Kubeflow) |
| Apache Iceberg | 1.10.1 |
| Hive Metastore | 3.1.3 (Stackable 25.7.0) |
| Apache Polaris | 1.3.0-incubating |
| Trino | 479 |
| DuckDB | bundled (Python 3.11) |
| PostgreSQL | 17 |

All versions are overridable in the YAML config. See
[Supported Components](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/supported-components.md).

## Documentation

- [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md) -- prerequisites, install, first run
- [Configuration](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/configuration.md) -- full YAML reference
- [Recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md) -- catalog + format + engine combinations
- [Running Pipelines](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/running-pipelines.md) -- batch and continuous modes
- [Benchmarking](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/benchmarking.md) -- scorecard and query benchmark
- [Architecture](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/architecture.md) -- system design
- [Troubleshooting](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/troubleshooting.md) -- common errors and fixes

## License

Apache 2.0
