# Lakebench

[![Python 3.10+](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/LICENSE)

**Run a real data workload through a composable lakehouse architecture on
Kubernetes, and get evidence of how it behaved.**

Deploy a complete lakehouse stack from a single YAML, generate a workload
corpus, run its pipeline and queries at any scale, and get a scorecard that
records exactly what produced it. Two runs are compared only when they
returned the same workload results.

<!-- TODO: Add terminal recording / screenshot of `lakebench run` output here -->

## Why Lakebench?

- **Compare stacks.** Swap catalogs (Hive, Polaris), query engines (Trino,
  Spark Thrift, DuckDB), and table formats (Iceberg, Delta) -- same data,
  same queries, different architecture. `lakebench compare` checks that both
  sides returned the same results before it shows any performance
  difference, and says NOT COMPARABLE when they did not.
- **Test at scale.** Run the same workload at 10 GB, 100 GB, and 1 TB to find
  where throughput plateaus or resources saturate on your hardware.
- **Measure freshness.** Continuous mode keeps data arriving through the
  pipeline and benchmarks query performance under ongoing ingest load.

## Workloads

Lakebench ships two workload schemas. The stack is the same either way; the
schema flag routes datagen and the pipeline scripts.

- **Customer 360** (default). Retail interactions from ~8 channels flow
  through bronze / silver / gold into an executive dashboard and the
  8-query analytical benchmark. `workload.schema: customer360`.
- **Financial crime (AML).** pacs.008 wire messages from a frozen
  generator (`datagen-v2-rs-0.3`) flow through bronze / silver / gold.
  Nine detection rules, including a fuzzy sanctions and PEP screen against
  a synthetic, dated watchlist, are scored for recall and precision against
  planted typologies (structuring, round-tripping, layering, dormant
  reactivation, high-risk corridors and others) by
  `spark/scripts/score_financial.py`, which `lakebench run` invokes
  inline. A pre-registered reference model (the fidelity gate) and a
  band leakage report ship separately as `lakebench financial
  reference-score`; they diagnose whether a rule could read high recall
  from a label proxy, and they are not run by `lakebench run`. Iceberg
  recipes only. `workload.schema: financial`. See
  [AML Scoring](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/aml-scoring.md)
  for what precision and recall measure here vs what an AML ops team
  cares about.

## Quick Start

```bash
pip install lakebench-k8s
```

> Pre-built binaries (no Python required) are available on
> [GitHub Releases](https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases).

```bash
pip install lakebench-k8s
lakebench init                                 # quick setup (4 questions), writes lakebench.yaml
lakebench run lakebench.yaml --generate --yes  # deploy + generate + pipeline + benchmark
lakebench results lakebench.yaml               # view scorecard
lakebench destroy lakebench.yaml               # tear down what this deployment owns
```

`--yes` lets `run` deploy the namespace and components when they do not
exist yet; without it `run` stops and asks you to run `lakebench deploy`
first. The Spark Operator is shared cluster infrastructure: a cluster admin
installs it once with `lakebench admin install-spark-operator` (see
[Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md)).

Minimum config:

```yaml
# lakebench.yaml
name: my-lakehouse                 # names the namespace and the buckets
platform:
  storage:
    s3:
      endpoint: http://s3.example.com:80
      access_key: YOUR_KEY
      secret_key: YOUR_SECRET
workload:
  datagen:
    scale: 10                      # 1 = ~10 GB, 10 = ~100 GB, 100 = ~1 TB
```

The name is required by every command that changes data (`deploy`,
`generate`, `run` and the rest); a config without one is refused with the
name to add. Buckets default to `<name>-bronze`, `<name>-silver`
and `<name>-gold`, so they are unique on stores where bucket names are
global (FlashBlade, AWS S3). Recipe defaults to `hive-iceberg-spark-trino`.
Override anything in the nested YAML:

```yaml
# lakebench.yaml (with overrides)
name: flashblade-polaris
recipe: polaris-iceberg-spark-trino
images:
  spark: apache/spark:4.1.1-python3
platform:
  storage:
    s3:
      endpoint: http://10.0.1.50:80
      access_key: ${S3_ACCESS_KEY}   # env var substitution
      secret_key: ${S3_SECRET_KEY}
architecture:
  catalog:
    polaris:
      client_secret: ${LAKEBENCH_POLARIS_CLIENT_SECRET}   # required for Polaris
  pipeline:
    mode: batch
workload:
  datagen:
    scale: 50
```

The flat top-level spellings (`endpoint:`, `scale:` and the others in
[Configuration](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/configuration.md#flat-fields-deprecated))
still load, with a deprecation note naming the nested key.

Eleven recipes are available -- see [Recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md)
for the full list. Support is judged per workload x recipe x mode:
**supported** means a live run on the release tree validated it end to end,
**unverified** means it is valid but not release-validated, and
**unsupported** is refused before a run
([rules](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/compatibility-matrix.md#support-states)).
Every run records its state in `metrics.json`. As computed by this release:

<!-- BEGIN GENERATED: support-states -->
<!-- Generated from the code by `python3.11 -m lakebench.config.support .`; do not edit by hand. -->

| Recipe | Customer 360 batch | Customer 360 continuous | AML (financial) batch | AML (financial) continuous |
|---|---|---|---|---|
| `hive-delta-spark-none` | unverified | unverified | unsupported | unsupported |
| `hive-delta-spark-thrift` | unverified | unverified | unsupported | unsupported |
| `hive-delta-spark-trino` | unverified | unverified | unsupported | unsupported |
| `hive-iceberg-spark-duckdb` | unverified | unverified | unverified | unverified |
| `hive-iceberg-spark-none` | unverified | unverified | unverified | unverified |
| `hive-iceberg-spark-thrift` | unverified | unverified | unverified | unverified |
| `hive-iceberg-spark-trino` | unverified | unverified | unverified | unverified |
| `polaris-iceberg-spark-duckdb` | unverified | unverified | unverified | unverified |
| `polaris-iceberg-spark-none` | unverified | unverified | unverified | unverified |
| `polaris-iceberg-spark-thrift` | unverified | unverified | unverified | unverified |
| `polaris-iceberg-spark-trino` | unverified | unverified | unverified | unverified |

- **unsupported**, refused at config load: AML (financial) on `hive-delta-spark-none`, `hive-delta-spark-thrift`, `hive-delta-spark-trino`. The financial (AML) workload supports table_format iceberg, not delta. Its stage scripts and table DDL are written for iceberg only, so this combination would not run the workload it names. Set architecture.table_format.type to iceberg (for example recipe: polaris-iceberg-spark-trino).
- Any catalog, table format and query engine combination that is not a recipe above is refused at config load for every workload.
- AML (financial) continuous: AML continuous runs detection rules W2, W3, W4, W17 each tick and records W1, W5, W6, W7, W8 as not run. Its results depend on when detection ran relative to arrival, so no end-of-run result check is recorded.

<!-- END GENERATED: support-states -->

Compare two configurations side-by-side:

```bash
lakebench deploy config-hive.yaml && lakebench deploy config-polaris.yaml
lakebench compare config-hive.yaml config-polaris.yaml --generate
```

`compare` runs each config through the pipeline and benchmark in turn, then
destroys that deployment unless you pass `--keep`. Deploy both configs
first; `--generate` fills each bronze bucket before its run. The two configs
need different names and bucket names, or the first destroy empties the
second run's data.

For all recipes, see [`examples/`](https://github.com/PureStorage-OpenConnect/lakebench-k8s/tree/main/examples) or run `lakebench init --advanced`
for the full interactive wizard.

## What You Get

After `lakebench run` completes, the terminal prints a scorecard (the
numbers below are illustrative, not a measurement):

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

Each run writes `lakebench-output/runs/run-<id>/report.html`; `lakebench
report` prints the run's summary and points at it (`--render` writes a fresh
copy). The report shows per-query latencies,
bottleneck analysis, and optional platform metrics (CPU, memory, S3 I/O per
pod). Every run's `metrics.json` carries an `experiment` block: workload and
generator version, seed, recipe and component versions, scale, mode, the
maintenance that actually ran, the Lakebench-imposed limits that applied
and whether they bound, the support state, and a fingerprint of each
query's result.

Upgrading from 1.5: most earlier numbers are not comparable with 1.6 (for
example, no Iceberg snapshot expiry or Delta VACUUM ever ran before 1.6.0).
See the [CHANGELOG](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/CHANGELOG.md).

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
- Kubernetes 1.26+ with room for the minimum below: what must fit at once,
  for the default recipe (`hive-iceberg-spark-trino`), from the sizing
  function behind `lakebench config show`, `config recommend` and the `run`
  capacity preflight. Batch datagen pods that do not fit wait their turn, so
  the batch minimum is the Spark peak plus the always-on pods. See
  [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md)
  for what each figure is built from. `lakebench run` fails fast if the
  cluster is too small.

<!-- BEGIN GENERATED: sizing-minimums -->
<!-- Generated from the code by `python3.11 scripts/gen_sizing_tables.py`; do not edit by hand. -->

| Workload | Mode | Scale | Minimum CPU | Minimum RAM | Scratch PVC (if enabled) | Largest pod |
|:---|:---|---:|---:|---:|---:|---:|
| Customer 360 | batch | 1 | 40 cores | 529 GB | 2,400 Gi | 8 cores / 60 GB |
| Customer 360 | batch | 10 | 47 cores | 557 GB | 2,400 Gi | 8 cores / 60 GB |
| Customer 360 | batch | 100 | 113 cores | 1,325 GB | 5,400 Gi | 8 cores / 60 GB |
| Customer 360 | continuous | 1 | 58 cores | 297 GB | 640 Gi | 8 cores / 40 GB |
| Customer 360 | continuous | 10 | 81 cores | 333 GB | 640 Gi | 8 cores / 40 GB |
| Customer 360 | continuous | 100 | 201 cores | 943 GB | 1,700 Gi | 8 cores / 48 GB |
| AML | batch | 1 | 40 cores | 529 GB | 2,400 Gi | 8 cores / 60 GB |
| AML | batch | 10 | 47 cores | 557 GB | 2,400 Gi | 8 cores / 60 GB |
| AML | batch | 100 | 113 cores | 1,325 GB | 5,500 Gi | 8 cores / 60 GB |
| AML | continuous | 1 | 138 cores | 1,011 GB | 2,300 Gi | 8 cores / 40 GB |
| AML | continuous | 10 | 161 cores | 1,053 GB | 2,300 Gi | 8 cores / 40 GB |
| AML | continuous | 100 | 339 cores | 2,241 GB | 4,660 Gi | 8 cores / 48 GB |

<!-- END GENERATED: sizing-minimums -->

- S3-compatible object storage. FlashBlade and Garage are validated; others are
  expected to work. Run `lakebench config storage` to check yours. See
  [Storage Backends](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/storage-backends.md).
- [Kubeflow Spark Operator 2.5.1+](https://github.com/kubeflow/spark-operator),
  installed once per cluster with `lakebench admin install-spark-operator`.
  `deploy` adds its namespace to the operator's watch list itself, under the
  cluster lock; do not edit `spark.jobNamespaces` by hand.
- [Stackable Hive Operator](https://docs.stackable.tech/home/stable/hive/) for
  Hive recipes (not needed for Polaris)

## Commands

| Command | Description |
|---------|-------------|
| `init` | Generate a starter config file |
| `config validate` | Check config and cluster connectivity |
| `config storage` | Check the S3 backend supports what lakebench needs |
| `config show` | Show the resolved configuration and its peak requested resources |
| `deploy` | Deploy all infrastructure components |
| `generate` | Generate synthetic data at the configured scale |
| `run` | Execute the medallion pipeline and benchmark |
| `benchmark` | Run the workload's query benchmark standalone |
| `query` | Execute ad-hoc SQL against the active engine |
| `status` | Show deployment status |
| `results` | Show the latest run's scorecard in the terminal |
| `report` | Generate HTML scorecard report |
| `compare` | Run two configs in turn; compare performance only when their results match |
| `config recommend` | Recommend a scale factor for the connected cluster |
| `config recipes` | List recipes and their support state per workload and mode |
| `admin` | Cluster-admin setup and repair: `install-spark-operator`, `install-scratch-storage-class`, `doctor`, `status`, `repair-operator`, `release-lock`, `migrate-deployment`, `reclaim-bucket` |
| `financial` | AML operator actions (`score`, `reference-score`, `replay`, `reproduce`) |
| `reproduce` | Record or verify a reproduction package from a run |
| `destroy` | Tear down the resources this deployment owns |

See [CLI Reference](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/cli-reference.md)
for flags and options.

## Component Versions

| Component | Version |
|-----------|---------|
| Apache Spark | 3.5.x, 4.0.x (default 4.0.2), 4.1.x (4.2 not supported) |
| Spark Operator | 2.5.1 (Kubeflow) |
| Apache Iceberg | 1.11.0 (1.10.1 with Spark 3.5.4 on its Java 11 image, and in local mode) |
| Delta Lake | 4.0.0 / 4.1.0 (auto, by Spark version) |
| Hive Metastore | 3.1.3 (Stackable 25.7.0) |
| Apache Polaris | 1.6.0 |
| Trino | 483 |
| DuckDB | 1.5.5 (Python 3.11 image) |
| PostgreSQL | 16, 17, 18 |

All versions are overridable in the YAML config. See
[Supported Components](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/supported-components.md).

## Documentation

- [Getting Started](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/getting-started.md) -- prerequisites, install, first run
- [Configuration](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/configuration.md) -- full YAML reference
- [Recipes](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/recipes.md) -- catalog + format + engine combinations
- [Compatibility Matrix](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/compatibility-matrix.md) -- Spark, Iceberg, and Delta version support
- [Running Pipelines](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/running-pipelines.md) -- batch and continuous modes
- [Benchmarking](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/benchmarking.md) -- scorecard and query benchmark
- [AML Scoring](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/aml-scoring.md) -- financial-crime / AML workload: rule recall, reference detector, leakage gate
- [Architecture](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/architecture.md) -- system design
- [Storage Backends](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/storage-backends.md) -- validated S3 backends and conformance checks
- [Troubleshooting](https://github.com/PureStorage-OpenConnect/lakebench-k8s/blob/main/docs/troubleshooting.md) -- common errors and fixes

## License

Apache 2.0
