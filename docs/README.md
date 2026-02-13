# Lakebench Documentation

## Getting Started

- [Getting Started](getting-started.md) -- Prerequisites, installation, and first deployment
- [Recipes](recipes.md) -- All 10 supported component combinations with decision guidance
- [Polaris Quickstart](quickstart-polaris.md) -- Switch from Hive Metastore to Apache Polaris

## Core Workflow

- [Configuration](configuration.md) -- Full YAML reference with annotated examples
- [Deployment](deployment.md) -- Deploy, status, and destroy lifecycle
- [Data Generation](data-generation.md) -- Generate command, scale factors, and monitoring
- [Running Pipelines](running-pipelines.md) -- Pipeline stages, batch and continuous modes

## Data Generation

- [Datagen Schema](datagen-schema.md) -- Customer360 schema, 41 columns, 7 realism features
- [Custom Datagen Images](datagen-custom-images.md) -- Build, push, and configure custom images

## Benchmarking

- [Benchmarking](benchmarking.md) -- 8-query suite, QpH scoring, pipeline scores
- [Query Reference](query-reference.md) -- Per-query reference with categories and expected output

## Component Reference

- [Trino](component-trino.md) -- Query engine configuration, coordinator/worker sizing, catalog integration
- [Spark](component-spark.md) -- Spark Operator, driver/executor resources, per-job profiles, S3A tuning
- [Hive Metastore](component-hive.md) -- Stackable operator, thrift settings, PostgreSQL backend
- [S3 Storage](component-s3.md) -- Endpoint, credentials, buckets, FlashBlade specifics
- [PostgreSQL](component-postgres.md) -- Metadata store, storage classes, deployment order
- [Observability](component-observability.md) -- Prometheus, Grafana, local metrics, HTML reports
- [Polaris Reference](polaris-reference.md) -- Polaris REST catalog deployment and gotchas

## Architecture & Reference

- [Architecture](architecture.md) -- Component topology, medallion layers, catalog pluggability
- [CLI Reference](cli-reference.md) -- All commands and flags
- [Troubleshooting](troubleshooting.md) -- Common errors and fixes

## Development

- [Development Guide](development.md) -- Dev setup, running tests, project structure
- [Contributing](contributing.md) -- PR process, code standards, test requirements

## Internal Reference

- [Operators and Catalogs](operators-and-catalogs.md) -- Spark Operator and Hive Metastore deep dive
