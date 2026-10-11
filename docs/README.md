# Lakebench Documentation

Documentation for configuring, executing and interpreting Lakebench
workload experiments.

## Getting Started

- [Getting Started](getting-started.md): prerequisites, install, first run,
  first report, teardown
- [Deployment](deployment.md): installing the CLI, deploy, status, destroy
- [Prerequisites](prerequisites.md): every cluster check and its fix
- [Upgrading to 1.7](../UPGRADING-1.7.md): every breaking change from 1.6

## Workloads

- [Customer 360](benchmarks/C360.md): data model, pipeline stages, queries,
  metrics and comparability
- [AML](benchmarks/AML.md): data model, seed policy, detection rules,
  scoring and comparability
- [AML Scoring](aml-scoring.md): how to read recall and precision, what
  the numbers mean and how to cite them
- [Data Generation](data-generation.md): scale factors, batch and
  continuous corpora, re-running and reuse
- [Custom Datagen Images](datagen-custom-images.md): building, pushing and
  configuring a datagen image

## Architecture & Configuration

- [Recipes](recipes.md): the 11 supported architectural compositions and
  how to choose a catalog, table format and engine
- [Configuration](configuration.md): every YAML field and its default
- [Architecture](architecture.md): how catalogs, table formats, pipeline
  engines and query engines fit together
- [Compatibility Matrix](compatibility-matrix.md): support states, component
  versions and images
- [Sizing](sizing.md): minimum cluster per workload, mode and scale

## Running Experiments

- [Running Pipelines](running-pipelines.md): pipeline stages, batch and
  continuous modes
- [Operations](operations.md): shared clusters, shared components,
  ownership, parallel deployments, cleanup
- [Tuning a continuous pipeline](benchmarking/continuous-tuning.md): levers
  when a stage falls behind
- [Storage Backends](storage-backends.md): S3 configuration, path style,
  validated stores

## Results & Comparison

- [Benchmarking](benchmarking.md): overview of what Lakebench measures
- [Batch scorecard](benchmarking/scorecard.md): stage timing, throughput,
  efficiency
- [Continuous scores](benchmarking/continuous.md): freshness, sustained
  throughput, time-to-detect
- [Query benchmark](benchmarking/query-benchmark.md): per-query latency and
  QpH
- [Verdict](benchmarking/verdict.md): pass criteria and what they check
- [Maintenance and limits](benchmarking/maintenance.md): compaction, expiry,
  executor caps
- [Comparing runs](benchmarking/comparing.md): comparability rules and
  disclosed differences
- [Run records](benchmarking/records.md): provenance and the experiment
  block
- [HTML report layout](benchmarking/html-report.md): sections and how to
  read them

## Technical Reference

Commands and configuration:

- [CLI Reference](cli-reference.md): every command and flag
- [Exit Codes](exit-codes.md): what each exit code means
- [Glossary](glossary.md): terms used in reports and records

Components:

- Query engines: [Trino](component-trino.md),
  [Spark Thrift Server](component-spark-thrift.md),
  [DuckDB](component-duckdb.md)
- Catalogs: [Hive Metastore](component-hive.md),
  [Apache Polaris](component-polaris.md)
- Infrastructure: [Spark](component-spark.md),
  [PostgreSQL](component-postgres.md),
  [Observability](component-observability.md)
- [Troubleshooting](troubleshooting.md): errors and fixes, by symptom

## Development

- [Contributing](../CONTRIBUTING.md): setup, test tiers, pull requests and
  style
- [Development Guide](development.md): code map, CI, extension points
- [Releasing](../RELEASING.md): cutting a release
- [Security](../SECURITY.md): reporting a vulnerability
