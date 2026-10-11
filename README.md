# Lakebench

[![Python 3.10+](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/LICENSE)

**Evaluate lakehouse architectures through the workloads they execute.**

Lakebench runs defined data workloads against configurable lakehouse
architectures on Kubernetes. It measures how the assembled system behaves,
where time and resources are consumed, and how results change when
architectural choices or operating conditions change.

Rather than benchmarking individual components in isolation, Lakebench
evaluates the behaviour of the architecture as a whole.

## Why Lakebench?

A lakehouse is a composition of technologies. Its behaviour depends on how
those technologies work together, the workload being executed and the
infrastructure underneath them.

Component benchmarks cannot tell you how the complete architecture will
behave when ingesting, transforming, analyzing and maintaining data.

Lakebench provides a repeatable way to investigate three questions:

1. **How does the architecture behave?** Execute a defined workload and
   measure its outcomes, processing stages and resource consumption.
2. **Where are the constraints?** Examine where execution time is spent and
   which system resources contribute to observed behaviour.
3. **What changes when the architecture changes?** Repeat the workload with
   different supported components, configurations or operating conditions
   and compare the evidence.

Lakebench produces measurements and supporting context. It does not
automatically determine the best architecture or establish production
suitability.

## How Lakebench Works

Every Lakebench experiment brings together four elements:

**Workload:** The data, processing, queries and outcomes that exercise the
architecture.

**Architecture:** The supported composition of processing engines, table
formats, catalogs and query engines.

**System:** The Kubernetes, compute, network and storage environment on
which the architecture runs.

**Execution conditions:** The scale, resource allocations, execution mode
and other controls under which the workload executes.

Together, these produce evidence of how the workload behaved.

`Workload x Architecture x System x Execution conditions -> Evidence`

Lakebench automates supported deployment and execution activities, collects
measurements and produces reports for examining the results.

## Workloads

Lakebench currently provides two synthetic workloads.

### Customer 360

Customer 360 exercises a retail analytics workload. Customer interactions
are ingested, validated, transformed and aggregated into analytical datasets
that are queried for business insights.

It exercises the architecture through data preparation, transformation,
aggregation and analytical querying.

### Financial Crime / AML

The financial-crime workload exercises transaction-monitoring data
processing using synthetic financial transactions.

It combines ingestion, transformation, detection and analytical activity to
examine both processing behaviour and detection outcomes.

Its purpose is to evaluate how the architecture executes the workload, not
to establish the effectiveness of a production AML detection system.

The two workloads exercise different data-processing behaviours while using
the same underlying architectural composition model.

## Running an Experiment

Lakebench supports a workflow that:

1. Defines the workload, architecture and execution configuration.
2. Deploys the required supported components onto Kubernetes.
3. Generates synthetic workload data.
4. Executes the workload.
5. Collects measurements and produces results.
6. Supports subsequent investigation and comparison.

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

- `init` writes a minimal configuration. `--recipe <name>` selects an
  architectural composition; `--workload financial` selects the AML
  workload. There are 11 supported recipes. `lakebench config recipes`
  lists them.
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

The full walkthrough is [Getting Started](docs/getting-started.md).

## Understanding Results

Lakebench records how the workload executed under the specified conditions.

Depending on the workload and available instrumentation, this can include:

- Workload outcomes and correctness.
- Execution time and processing-stage behaviour.
- Throughput and resource efficiency.
- Query or detection performance.
- Compute, memory and storage observations.

Results describe observed behaviour under particular conditions. They are
not universal performance claims.

Comparing results requires understanding what changed between runs and
whether their execution conditions and workload semantics are equivalent.

`lakebench report` prints a summary and the path to the HTML report.
`--render` writes a fresh copy; `--format json` or `csv` for scripts.

## Prerequisites

Lakebench requires a supported Kubernetes environment and access to
compatible storage. It does not provision the underlying infrastructure.

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

- Room for the workload. Scale 1 batch needs about 41 cores and 544 GB.
  Requirements grow with scale and differ by workload and execution mode.
  `lakebench plan` checks the cluster;
  [Sizing](docs/sizing.md) has every combination.

The [Prerequisites](docs/prerequisites.md) page lists every check and its
fix.

## Documentation

- [Getting Started](docs/getting-started.md): install, first run, teardown
- [Documentation index](docs/README.md): full navigation
- [Configuration](docs/configuration.md): every YAML field
- [CLI Reference](docs/cli-reference.md): every command and flag
- [Workloads](docs/README.md#workloads): Customer 360 and AML
- [Recipes](docs/recipes.md): supported architectural compositions
- [Results & Comparison](docs/README.md#results--comparison): measurements
  and interpretation
- [Upgrading to 1.7](UPGRADING-1.7.md): every breaking change from 1.6

## License

Apache 2.0
