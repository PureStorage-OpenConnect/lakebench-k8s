# Lakebench Documentation

Lakebench deploys a lakehouse stack to Kubernetes from a single YAML, runs
a workload end to end, and records evidence of what produced each number.

## Getting Started

- [Getting Started](getting-started.md) -- prerequisites, install, first deployment
- [Prerequisites](prerequisites.md) -- cluster checklist, generated from the checks `plan` runs
- [Recipes](recipes.md) -- the 11 recipes (plus the `default` alias) with decision guidance
- [Polaris Quickstart](quickstart-polaris.md) -- switch from Hive Metastore to Apache Polaris

## Core Workflow

- [Configuration](configuration.md) -- full YAML reference, generated from the schema
- [Deployment](deployment.md) -- deploy, status, and destroy lifecycle
- [Data Generation](data-generation.md) -- `generate` command, scale factors, monitoring
- [Running Pipelines](running-pipelines.md) -- pipeline stages, batch and continuous modes
- [Operations](operations.md) -- shared-cluster setup, ownership, parallel deployments, cleanup

## Data Generation

- [Datagen Schema](datagen-schema.md) -- Customer 360 schema, 41 columns, 7 realism features
- [Custom Datagen Images](datagen-custom-images.md) -- build, push, configure custom images

## Benchmarks and Scoring

- [Benchmarking](benchmarking.md) -- pipeline scorecard, query benchmark, QpH scoring
- [Query Reference](query-reference.md) -- per-query reference with categories and expected output
- [Customer 360 benchmark](benchmarks/C360.md) -- data model, pipeline, correctness checks, queries, metrics, comparability
- [AML benchmark](benchmarks/AML.md) -- data model, seed policy, pipeline, detection rules, scoring, comparability
- [AML Scoring](aml-scoring.md) -- what precision and recall measure, the leakage gate, the reference detector
- [Financial Benchmark Baselines](financial-benchmark-baselines.md) -- published Financial numbers per scale

## Components

### Query Engines

- [Trino](component-trino.md) -- distributed query engine, coordinator and worker sizing, catalog integration
- [Spark Thrift Server](component-spark-thrift.md) -- Spark-native query engine, single pod, beeline interface
- [DuckDB](component-duckdb.md) -- single-pod engine, development and small-scale runs

### Catalogs

- [Hive Metastore](component-hive.md) -- Stackable operator, thrift settings, PostgreSQL backend
- [Apache Polaris](component-polaris.md) -- REST catalog, OAuth2 authentication, bootstrap lifecycle
- [Operators and Catalogs](operators-and-catalogs.md) -- Spark Operator and Hive Metastore deep dive

### Infrastructure

- [Spark](component-spark.md) -- Spark Operator, driver and executor resources, per-job profiles, S3A tuning
- [S3 Storage](component-s3.md) -- endpoint, credentials, buckets, FlashBlade specifics
- [Storage Backends](storage-backends.md) -- validated S3 backends and conformance checks
- [PostgreSQL](component-postgres.md) -- metadata store, storage classes, deployment order
- [Observability](component-observability.md) -- Prometheus, Grafana, local metrics, HTML reports
- [Supported Components](supported-components.md) -- versions, images, recipe matrix
- [Compatibility Matrix](compatibility-matrix.md) -- Spark, Iceberg, Delta version support

## Architecture and Reference

- [Architecture](architecture.md) -- component topology, medallion layers, catalog pluggability
- [DESIGN.md](DESIGN.md) -- design authority: what Lakebench measures, invariants, owner decisions
- [Internals](internals.md) -- why the sizing, versions and catalog handling are built this way
- [CLI Reference](cli-reference.md) -- every command and flag
- [Exit Codes](exit-codes.md) -- what each exit code means and which paths produce it
- [Troubleshooting](troubleshooting.md) -- common errors and fixes, by symptom

## Reproductions and Upgrades

- [Reproduction Packages](reproductions/README.md) -- recorded packages, how `reproduce` runs one
- [Reproduce Deep Dive](deep-dive/reproduce.md) -- the reproduction contract
- [Datagen Metrics Deep Dive](deep-dive/datagen-metrics.md) -- how datagen numbers are measured
- [Upgrading to 1.7](../UPGRADING-1.7.md) -- every breaking change with its fix

## Development

- [Development Guide](development.md) -- architecture map, test and CI wiring, adding a recipe or workload
- [Design Notes](design/README.md) -- long-form design records (namespace isolation, others)
- [Contributing](../CONTRIBUTING.md) -- setup, test tiers, pull requests, review and style
