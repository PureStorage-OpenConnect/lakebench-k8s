# Lakebench

[![Python 3.10+](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

CLI tool for deploying and benchmarking lakehouse architectures on Kubernetes.

> **Note:** This package is published as `lakebench-k8s` on PyPI. Install with `pip install lakebench-k8s`. The CLI command is `lakebench`.

Choosing between Hive and Polaris, sizing Spark for 100 GB vs 10 TB, or
comparing batch and continuous pipelines shouldn't require weeks of manual setup. Lakebench deploys a complete
lakehouse stack from a single YAML file, generates realistic data at any scale,
runs the pipeline, benchmarks query performance, and tears everything down --so
you can focus on comparing architectures, not plumbing.

## Installation

```bash
pip install lakebench-k8s
```

Or with [pipx](https://pipx.pypa.io/): `pipx install lakebench-k8s`

Pre-built binaries (no Python required) are available on
[GitHub Releases](https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases).

### Prerequisites

- Python 3.10+
- `kubectl` and `helm` on PATH
- Kubernetes cluster (1.26+, minimum 8 CPU / 32 GB RAM for scale 1)
- S3-compatible object storage (FlashBlade, MinIO, AWS S3, etc.)
- [Kubeflow Spark Operator 2.4.0+](https://github.com/kubeflow/spark-operator) (or set `spark.operator.install: true` in config to auto-install)
- [Stackable Hive Operator](https://docs.stackable.tech/home/stable/hive/) if using a Hive recipe (the default). Not needed for Polaris recipes.

See [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md)
for detailed install instructions.

## Quick Start

A **quick-recipe** selects the catalog + table format + query engine
combination in one line (e.g. `recipe: hive-iceberg-spark-trino`). The **scale
factor** controls data volume: 1 = ~10 GB, 10 = ~100 GB, 100 = ~1 TB.
Every recipe default can be overridden individually -- see the
[Configuration Reference](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/configuration.md)
for advanced configuration options.

```bash
# 1. Generate config (interactive prompts for S3 details)
lakebench init --interactive

# 2. Validate config and cluster connectivity
lakebench validate lakebench.yaml

# 3. Deploy infrastructure
lakebench deploy lakebench.yaml

# 4. Generate test data
lakebench generate lakebench.yaml --wait

# 5. Run the pipeline + benchmark
lakebench run lakebench.yaml

# 6. View results
lakebench report

# 7. Tear down
lakebench destroy lakebench.yaml
```

> Deploy and generate will prompt for confirmation. Add `--yes` to skip
> (e.g. `lakebench deploy lakebench.yaml --yes`).

## Commands

| Command | Description |
|---------|-------------|
| `lakebench init` | Generate a starter configuration file |
| `lakebench validate <config>` | Validate config and test connectivity |
| `lakebench info <config>` | Show configuration summary |
| `lakebench recommend` | Recommend cluster sizing for a scale factor |
| `lakebench deploy <config>` | Deploy all infrastructure |
| `lakebench generate <config>` | Generate synthetic data to bronze bucket |
| `lakebench run <config>` | Execute the medallion pipeline with metrics |
| `lakebench benchmark <config>` | Run 8-query benchmark against the active engine |
| `lakebench query <config>` | Execute SQL queries against the active engine |
| `lakebench status [config]` | Show deployment status |
| `lakebench logs <component> [config]` | Stream logs from a component |
| `lakebench report` | Generate HTML benchmark report |
| `lakebench destroy <config>` | Tear down all resources |

## How It Works

Lakebench deploys a three-layer stack on Kubernetes:

1. **Platform** -- Kubernetes namespace, S3 secrets, PostgreSQL (metadata store)
2. **Data architecture** -- catalog (Hive or Polaris), table format (Iceberg),
   query engine (Trino, Spark Thrift, or DuckDB), all wired together via
   [recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md)
3. **Observability** -- optional Prometheus + Grafana stack for platform metrics

Once deployed, the pipeline runs three Spark jobs in sequence:

```
Raw Parquet (S3)  -->  Bronze (validate, deduplicate)
                  -->  Silver (normalize, enrich -- Iceberg table)
                  -->  Gold (aggregate -- Iceberg table)
                  -->  Benchmark (8 analytical queries via query engine)
```

The benchmark produces an HTML report with query latencies, throughput scores,
and optional platform metrics (CPU, memory, S3 I/O per pod). See the
[Architecture](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/architecture.md)
doc for the full picture.

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

All versions are configurable. See
[Supported Components](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/supported-components.md)
for the full matrix of components, recipes, and override options.

## Documentation

Full documentation is in the [docs/](https://github.com/PureStorage-OpenConnect/lakebench-k8s/tree/main/docs) directory:

- [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md) -- prerequisites, install, first deployment
- [Configuration](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/configuration.md) -- full YAML reference
- [CLI Reference](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/cli-reference.md) -- all commands and flags
- [Recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md) -- supported component combinations
- [Supported Components](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/supported-components.md) -- versions, images, and recipe matrix
- [Deployment](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/deployment.md) -- deploy lifecycle and status checks
- [Data Generation](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/data-generation.md) -- scale factors, parallelism, and monitoring
- [Running Pipelines](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/running-pipelines.md) -- batch and streaming modes
- [Scoring and Benchmarking](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/benchmarking.md) -- pipeline scorecard and query engine benchmark
- [Polaris Quick Start](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/quickstart-polaris.md) -- use Apache Polaris instead of Hive
- [Architecture](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/architecture.md) -- system design and component layers
- [Troubleshooting](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/troubleshooting.md) -- common errors and fixes

## License

Apache 2.0
