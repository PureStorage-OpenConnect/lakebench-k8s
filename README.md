# Lakebench

[![Python 3.10+](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/LICENSE)

Lakebench evaluates how composed lakehouse architectures behave when they
execute real data workloads on real infrastructure.

It deploys a configurable architecture onto Kubernetes, runs a defined
workload through it under controlled conditions, and produces evidence of
correctness, performance, throughput and resource consumption. Each run
records what produced it so a reader can judge whether two results are
comparable.

## Why Lakebench?

Architectural decisions need evidence from executing workloads, not
component specifications in isolation. Lakebench helps investigate three
questions:

1. **How does an architecture execute a workload?** Deploy a catalog, table
   format and query engine together, run a workload end to end, and observe
   where time goes, how resources are used, and whether results are correct.
2. **Where do constraints emerge?** Run the same workload at increasing
   scale to find where throughput stops growing, which stages dominate, and
   where the system saturates.
3. **What changes when the composition changes?** Swap one component (Hive
   for Polaris, Trino for DuckDB), rerun the same workload on the same
   system, and compare the evidence.

The value is repeatable evidence under disclosed conditions, not an
automated recommendation.

## How it works

Lakebench combines four inputs into one experiment:

```
Workload × Architecture × System × Execution conditions → Evidence
```

- **Workload.** The data, processing stages, queries and outcomes that
  exercise the architecture.
- **Architecture.** A composition of catalog, table format, pipeline engine
  and query engine, expressed as a recipe.
- **System.** The Kubernetes cluster, node hardware, network and
  S3-compatible object storage.
- **Execution conditions.** Scale factor, pipeline mode (batch or
  continuous), resource allocation, concurrency and maintenance.
- **Evidence.** Observed correctness, stage timing, throughput, query
  latencies, resource consumption, and the conditions and limits that apply.

A recipe names one supported composition. There are 11. `lakebench init
--recipe <name>` writes its config; `lakebench config recipes` lists them
all. See [Recipes](docs/recipes.md).

## Supported workloads

Workloads are the means of evaluating the architecture. Each defines its
data, processing stages, queries and measured outcomes.

### Customer 360

A synthetic customer-interaction analytics workload. Retail interactions
across multiple channels flow through bronze ingestion, silver enrichment
and gold aggregation into an executive dashboard. An 8-query analytical
benchmark measures query latency and throughput against the silver and gold
tables.

Batch mode runs the three stages in sequence. Continuous mode keeps data
arriving and measures the pipeline under ongoing ingest.

Set `workload.schema: customer360` (the default).

### Financial crime (AML)

A synthetic financial-transaction workload. pacs.008 wire messages flow
through the same medallion stages, then nine detection rules score recall
and precision against planted typologies. A 12-query benchmark covers
transactional and investigator queries.

Both pipeline execution and detection correctness are measured. Continuous
mode adds time-to-detect. Recall and precision from the default seed are
in-sample and uncalibrated; what those numbers mean and how to cite them:
[AML Scoring](docs/aml-scoring.md).

Set `workload.schema: financial`. Requires an Iceberg recipe.

### Shared and workload-specific behaviour

Both workloads share the same architectural components and medallion
pipeline. They differ in data model, processing logic, detection rules (AML
only), query sets and scored outcomes. The run's `experiment` block records
which workload, stages, rules and query set produced each result.

## Quick start

### Install

```bash
pip install lakebench-k8s
```

Binaries that need no Python are on
[GitHub Releases](https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases).

### Prerequisites

Lakebench runs on an existing Kubernetes cluster with S3-compatible storage.
It does not provision the underlying infrastructure.

- `kubectl` and `helm` on PATH.
- Kubernetes 1.26+.
- S3-compatible object storage with an endpoint, access key and secret key.
  Validated on FlashBlade and Garage; expected to work on AWS S3, MinIO and
  other S3-compatible stores. Check yours with `lakebench config storage`.
- Cluster room for the workload. Scale 1 batch needs about 41 cores and
  544 GB. `lakebench plan` checks your cluster; [Sizing](docs/sizing.md)
  has the minimum per workload, mode and scale.

The [Prerequisites](docs/prerequisites.md) page lists every cluster check
and its fix.

### First experiment

```bash
lakebench init --endpoint http://your-s3-endpoint:80    # writes lakebench.yaml
export LAKEBENCH_S3_ACCESS_KEY=...
export LAKEBENCH_S3_SECRET_KEY=...
lakebench admin install --component all lakebench.yaml  # once per cluster
lakebench plan lakebench.yaml                           # read-only: what the run needs
lakebench run lakebench.yaml --generate --yes           # deploy, generate, pipeline, benchmark
lakebench report lakebench.yaml                         # print the scorecard
lakebench destroy lakebench.yaml                        # remove what this deployment created
```

- `--yes` lets `run` deploy the namespace and components. Without it, run
  `lakebench deploy` first.
- `--generate` fills the bronze bucket. Without it, `run` refuses with
  exit 4.
- `admin install` places the Spark Operator, Stackable operators (for Hive
  recipes) and the scratch StorageClass. Run it once per cluster.

The full walkthrough is [Getting Started](docs/getting-started.md).

### Minimal configuration

`lakebench init` writes a config that is sufficient for a first run:

```yaml
name: lb-alice-7f3c
recipe: polaris-iceberg-spark-trino
workload:
  schema: customer360
  datagen:
    scale: 1                       # ~10 GB of bronze data
platform:
  storage:
    s3:
      endpoint: "http://your-s3-endpoint:80"
      access_key: "${LAKEBENCH_S3_ACCESS_KEY}"
      secret_key: "${LAKEBENCH_S3_SECRET_KEY}"
```

- `name` is required. It names the namespace and the buckets.
- The recipe sets the catalog, table format and engines. A component that
  contradicts it is refused at load.
- Credentials are environment-variable references, never plaintext.
- `init --workload financial` writes an AML config.

Every YAML field: [Configuration](docs/configuration.md).

## Understanding results

Each run records what happened and what produced it.

**Workload outcomes.** Stage completion, row counts (bronze, silver, gold)
and, for AML, detection recall and precision per rule. A run that silently
skips work or produces zero rows is not a pass.

**Execution time.** Per-stage duration, total pipeline time, and Time to
Value (deploy through benchmark).

**Throughput.** Pipeline throughput (GB/s), compute efficiency (GB per
core-hour) and, for continuous mode, sustained throughput (rows/s) and data
freshness (seconds of lag).

**Query behaviour.** Per-query latency and queries per hour (QpH). AML
continuous mode adds time-to-detect (p50 and p95).

**Resource consumption.** With observability enabled, the HTML report adds
pod CPU, memory and S3 I/O.

**Conditions and limits.** The `experiment` block in `metrics.json` records
the corpus, datagen image, recipe, component versions, scale, mode,
maintenance, Lakebench caps (and whether they bound), and the support state.
A number bounded by a cap is labelled as such.

A slow stage is an observation. Attributing it to a specific component or
resource requires supporting evidence.

`lakebench report` prints the scorecard and the path to `report.html`.
`--render` writes a fresh copy. `--format json` or `csv` for scripts.

## Comparing runs

Run each config, then read the two HTML reports side by side.

The Experiment section of each report lists the corpus, components,
maintenance, stages, rules, query set id and (for AML batch) the alert set.
Compare performance only when the corpus and query set match.

**Like-for-like:** same workload, architecture, system and execution
conditions. Differences in numbers reflect variance and measurement.

**Comparable:** same workload and system, different architecture or
execution conditions. The disclosed differences are the subject of the
comparison.

Lakebench does not compare query answers automatically. Check per-query row
counts and numbers in both reports.

## Support states

Each workload, recipe and mode combination has a support state:

- **supported:** validated end to end on the release tree.
- **unverified:** valid, not release-validated.
- **unsupported:** refused before a run.

Every run records its state. The table for this release is in the
[Compatibility Matrix](docs/compatibility-matrix.md#support-states).

## Documentation

- [Getting Started](docs/getting-started.md): install and first run
- [All documentation](docs/README.md): the full index
- [CLI Reference](docs/cli-reference.md): every command and flag
- [Configuration](docs/configuration.md): every YAML field
- [Sizing](docs/sizing.md): minimum cluster per workload, mode and scale
- [Supported Components](docs/compatibility-matrix.md#components): versions
- [Upgrading to 1.7](UPGRADING-1.7.md): every breaking change from 1.6
- [CHANGELOG](CHANGELOG.md)

## License

Apache 2.0
