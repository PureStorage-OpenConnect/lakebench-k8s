# Lakebench Documentation

Lakebench is a CLI tool for deploying and benchmarking lakehouse architectures on Kubernetes. It automates the full lifecycle -- deploy infrastructure, generate data, run pipelines, benchmark query engines, and collect results -- from a single YAML config.

## Getting Started

- [Getting Started](getting-started.md) -- Prerequisites, installation, and first deployment
- [Recipes](recipes.md) -- All 8 supported component combinations with decision guidance
- [Polaris Quickstart](quickstart-polaris.md) -- Switch from Hive Metastore to Apache Polaris

## Core Workflow

- [Configuration](configuration.md) -- Full YAML reference with annotated examples
- [Deployment](deployment.md) -- Deploy, status, and destroy lifecycle
- [Data Generation](data-generation.md) -- Generate command, scale factors, and monitoring
- [Running Pipelines](running-pipelines.md) -- Pipeline stages, batch and continuous modes

## Data Generation

- [Datagen Schema](datagen-schema.md) -- Customer360 schema, 41 columns, 7 realism features
- [Custom Datagen Images](datagen-custom-images.md) -- Build, push, and configure custom images

## Scoring and Benchmarking

- [Scoring and Benchmarking](benchmarking.md) -- pipeline scorecard, query engine benchmark, QpH scoring
- [Query Reference](query-reference.md) -- Per-query reference with categories and expected output

## Component Reference

### Query Engines

- [Trino](component-trino.md) -- Distributed query engine, coordinator/worker sizing, catalog integration
- [Spark Thrift Server](component-spark-thrift.md) -- Spark-native query engine, single-pod, beeline interface
- [DuckDB](component-duckdb.md) -- Lightweight single-pod engine, development and small-scale runs

### Catalogs

- [Hive Metastore](component-hive.md) -- Stackable operator, thrift settings, PostgreSQL backend
- [Apache Polaris](component-polaris.md) -- REST catalog, OAuth2 authentication, bootstrap lifecycle

### Infrastructure

- [Spark](component-spark.md) -- Spark Operator, driver/executor resources, per-job profiles, S3A tuning
- [S3 Storage](component-s3.md) -- Endpoint, credentials, buckets, FlashBlade specifics
- [PostgreSQL](component-postgres.md) -- Metadata store, storage classes, deployment order
- [Observability](component-observability.md) -- Prometheus, Grafana, local metrics, HTML reports
- [Supported Components](supported-components.md) -- Versions, images, and recipe matrix

## Architecture & Reference

- [Architecture](architecture.md) -- Component topology, medallion layers, catalog pluggability
- [CLI Reference](cli-reference.md) -- All commands and flags
- [Troubleshooting](troubleshooting.md) -- Common errors and fixes

## Development

- [Development Guide](development.md) -- Dev setup, running tests, project structure
- [Contributing](contributing.md) -- PR process, code standards, test requirements

## Internal Reference

- [Operators and Catalogs](operators-and-catalogs.md) -- Spark Operator and Hive Metastore deep dive
