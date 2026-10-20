# Lakebench

[![Python 3.10+](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/LICENSE)

**Evaluate lakehouse architectures through the workloads they execute.**

Lakebench runs defined data workloads against configurable lakehouse
architectures on Kubernetes. It measures how the assembled system behaves,
where time and resources go, and how results change when the architecture or
operating conditions change. It produces measurements and supporting
context, not a verdict on the best architecture or its production
suitability.

## How Lakebench works

An experiment combines four elements:

- **Workload:** the data, processing, queries and outcomes that exercise the
  architecture.
- **Architecture:** the composition of processing engines, table formats,
  catalogs and query engines.
- **System:** the Kubernetes, compute, network and storage environment.
- **Execution conditions:** scale, resource allocations, execution mode and
  other controls.

`Workload x Architecture x System x Execution conditions -> Evidence`

Lakebench deploys the components, generates data, runs the workload,
collects measurements and writes reports.

## Workloads

- **Customer 360:** a retail analytics workload. Customer interactions are
  ingested, validated, transformed, aggregated and queried.
  ([overview](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/benchmarks/c360-overview.md))
- **Financial Crime / AML:** transaction-monitoring over synthetic financial
  transactions: ingestion, transformation, detection and analytical queries.
  It evaluates how the architecture executes the workload, not the
  effectiveness of a production AML system.
  ([overview](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/benchmarks/aml-overview.md))

## Running an experiment

### Install

```bash
pip install lakebench-k8s
```

Binaries that need no Python are on
[GitHub Releases](https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases).

### Run

```bash
lakebench init --endpoint http://your-s3-endpoint:80    # writes lakebench.yaml
export LAKEBENCH_S3_ACCESS_KEY=...
export LAKEBENCH_S3_SECRET_KEY=...
lakebench admin install --component all lakebench.yaml  # once per cluster
lakebench plan lakebench.yaml                           # what the run needs (read-only)
lakebench run lakebench.yaml --generate --yes           # deploy, generate, run workload
lakebench report lakebench.yaml                         # print results
lakebench destroy lakebench.yaml                        # remove what this deployment created
```

- `init` writes a minimal configuration. `--recipe <name>` selects the
  architecture; `--workload financial` selects the AML
  workload. `lakebench config recipes` lists the 11 supported recipes.
- `admin install` places shared cluster components (Spark Operator,
  Stackable operators for Hive recipes, scratch StorageClass). Run once
  per cluster.
- `plan` checks whether the cluster has room for the workload. It changes
  nothing.
- `--generate` fills the bronze bucket with synthetic data. Without it,
  `run` refuses (exit 4).
- `--yes` lets `run` deploy the namespace and components. Without it, run
  `lakebench deploy` first.
- `destroy` removes only what this deployment created. It asks for
  confirmation unless `--yes` is passed.

The full walkthrough is [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md).

## Results

Depending on the workload, a run records outcomes and correctness, stage
timings, throughput, query or detection performance, storage I/O, and
compute and memory use. Results describe behaviour under the conditions of
that run. Compare runs only when their execution conditions and workload
semantics match.

`lakebench report` prints a summary and the path to the HTML report.
`--render` writes a fresh copy; `--format json` or `csv` is for scripts.

## Prerequisites

Lakebench does not provision the Kubernetes cluster or the storage.

**Host tooling:**

- `kubectl` and `helm` on PATH.

**Kubernetes:**

- Kubernetes 1.26+.
- Permission to create a namespace.

**Storage:**

- S3-compatible object storage with an endpoint URL, access key and secret
  key. Validated on FlashBlade and Garage; expected to work on AWS S3,
  MinIO and other S3-compatible stores. `lakebench config storage` checks
  yours.

**Cluster capacity:**

- Room for the workload. Scale 1 batch needs about 41 cores and 544 GB;
  requirements grow with scale and differ by workload and mode.
  `lakebench plan` checks the cluster and [Sizing](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/sizing.md) lists
  every combination.

[Prerequisites](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/prerequisites.md) lists every check and its fix.

## Documentation

- [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md): install, first run, teardown
- [Documentation index](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/README.md): full navigation
- [Configuration](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/configuration.md): every YAML field
- [CLI Reference](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/cli-reference.md): every command and flag
- [Workloads](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/README.md#workloads): Customer 360 and AML
- [Recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md): supported architectural compositions
- [Results & Comparison](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/README.md#results--comparison): measurements
  and interpretation

## License

Apache 2.0
