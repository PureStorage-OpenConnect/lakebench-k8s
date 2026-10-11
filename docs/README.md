# Lakebench Documentation

Lakebench deploys a lakehouse stack to Kubernetes from one YAML file, runs a
workload end to end, and records what produced each number.

## Start here

- [Getting Started](getting-started.md): prerequisites, install, first run
- [Recipes](recipes.md): the 11 recipes and how to choose a catalog, format and engine
- [Upgrading to 1.7](../UPGRADING-1.7.md): every breaking change with its fix

## Guides

- [Deployment](deployment.md): install the CLI; deploy, status, destroy
- [Operations](operations.md): shared clusters, shared components, ownership, parallel deployments, cleanup
- [Data Generation](data-generation.md): `generate`, scale factors, batch and continuous corpora
- [Running Pipelines](running-pipelines.md): pipeline stages, batch and continuous modes
- [Tuning a continuous pipeline](benchmarking/continuous-tuning.md): levers when a stage falls behind
- [Custom Datagen Images](datagen-custom-images.md): build, push and configure a datagen image
- [Troubleshooting](troubleshooting.md): errors and fixes, by symptom

## Reference

Commands and configuration:

- [CLI Reference](cli-reference.md): every command and flag
- [Configuration](configuration.md): every YAML field
- [Exit Codes](exit-codes.md): what each exit code means
- [Prerequisites](prerequisites.md): cluster checks `plan` runs
- [Sizing](sizing.md): minimum cluster per workload, mode and scale; the capacity check
- [Compatibility Matrix](compatibility-matrix.md): support states, component versions and images
- [Glossary](glossary.md): terms used in reports and records

Components:

- Query engines: [Trino](component-trino.md), [Spark Thrift Server](component-spark-thrift.md), [DuckDB](component-duckdb.md)
- Catalogs: [Hive Metastore](component-hive.md), [Apache Polaris](component-polaris.md)
- Infrastructure: [Spark](component-spark.md), [S3 storage](storage-backends.md), [PostgreSQL](component-postgres.md), [Observability](component-observability.md)
- [Architecture](architecture.md): how the components fit together

Scores and reports ([overview](benchmarking.md)):

- [Batch scorecard](benchmarking/scorecard.md), [Continuous scores](benchmarking/continuous.md), [Query benchmark](benchmarking/query-benchmark.md)
- [Verdict](benchmarking/verdict.md), [Maintenance and limits](benchmarking/maintenance.md), [Comparing runs](benchmarking/comparing.md)
- [Run records](benchmarking/records.md), [HTML report layout](benchmarking/html-report.md)

Workload specifications:

- [Customer 360](benchmarks/C360.md): data model, pipeline, queries, metrics, comparability
- [AML](benchmarks/AML.md): data model, seed policy, rules, scoring, comparability
- [AML Scoring](aml-scoring.md): how to read AML precision and recall

## Contributors

- [Contributing](../CONTRIBUTING.md): setup, test tiers, pull requests, review and style
- [Development Guide](development.md): code map, CI, extension points, why it is built this way
- [DESIGN.md](DESIGN.md): design authority: what Lakebench measures, invariants, owner decisions
- [Releasing](../RELEASING.md): cutting a release
- [Security](../SECURITY.md): reporting a vulnerability
