# Recipes

The 11 recipes, how to choose one and its catalog, and where support states
are listed.

A **recipe** is a validated combination of catalog, table format, pipeline
engine and query engine. Config load rejects any architecture outside the
recipe list, with the reason. A recipe says nothing about which workloads and
modes it supports; see [Pipeline modes and support states](#pipeline-modes-and-support-states).

## Setting a recipe

Set `recipe:` instead of `catalog`, `table_format`, `pipeline_engine` and
`query_engine`:

```yaml
name: my-lakehouse
recipe: polaris-iceberg-spark-trino    # sets catalog, format, engine and query engine
```

- `lakebench init` writes `recipe: polaris-iceberg-spark-trino` unless
  `--recipe` names another.
- A config with no `recipe:`, or `recipe: default`, resolves to
  `hive-iceberg-spark-trino` with a deprecation note. 1.8 requires
  `recipe:`.
- Recipe defaults merge without overwriting: images,
  versions and engine resources set in the config take precedence.
- The four recipe-owned keys (`architecture.catalog.type`,
  `architecture.table_format.type`, `architecture.pipeline_engine`,
  `architecture.query_engine.type`) may be left out or set to the recipe's
  value. A different value is refused at load, naming both keys.

## Quick Reference

Every recipe uses Spark as the pipeline engine. Image defaults:
[Compatibility Matrix](compatibility-matrix.md#component-version-matrix).

| Name | `recipe:` | Catalog | Format | Query engine | Best for |
|---|---|---|---|---|---|
| **Standard** | `hive-iceberg-spark-trino` (`default`) | hive | iceberg | trino | Ad-hoc SQL analytics |
| **Standard Headless** | `hive-iceberg-spark-none` | hive | iceberg | none | ETL-only workloads |
| **Spark SQL** | `hive-iceberg-spark-thrift` | hive | iceberg | spark-thrift | Spark-native analytics |
| **DuckDB** | `hive-iceberg-spark-duckdb` | hive | iceberg | duckdb | Lightweight single-node analytics |
| **Polaris** | `polaris-iceberg-spark-trino` | polaris | iceberg | trino | Multi-engine catalog sharing, fine-grained access control |
| **Polaris Headless** | `polaris-iceberg-spark-none` | polaris | iceberg | none | REST catalog ETL |
| **Polaris Spark SQL** | `polaris-iceberg-spark-thrift` | polaris | iceberg | spark-thrift | Spark-native with REST catalog |
| **Polaris DuckDB** | `polaris-iceberg-spark-duckdb` | polaris | iceberg | duckdb | Lightweight analytics with REST catalog |
| **Hive Delta Trino** | `hive-delta-spark-trino` | hive | delta | trino | Databricks-comparable analytics |
| **Hive Delta Spark SQL** | `hive-delta-spark-thrift` | hive | delta | spark-thrift | Delta + Spark-native analytics |
| **Hive Delta Headless** | `hive-delta-spark-none` | hive | delta | none | Delta ETL-only workloads |

- No DuckDB + Delta recipe: DuckDB cannot read Delta on non-AWS S3.
- No Polaris + Delta recipe: Polaris is Iceberg-only.
- The Hive Delta recipes differ from their Iceberg twins in table format
  and run Customer 360 only.

## Choosing a Recipe

- **Ad-hoc SQL after the pipeline?** Use `trino` (Standard or Polaris).
- **REST catalog API, or Stackable Hive Operator already installed?** See
  [Choosing a catalog](#choosing-a-catalog).
- **ETL only, no interactive queries?** Use a `none` recipe (Standard
  Headless, Polaris Headless or Hive Delta Headless). No query engine is
  deployed.
- **AML (financial) workload?** Use a Trino + Iceberg recipe (Standard or
  Polaris). The financial workload scores recall and precision with SQL on
  Trino, so a config on Delta, or on a `none`, `duckdb` or `spark-thrift`
  recipe, is refused at config load.

## Choosing a catalog

| Consideration | Hive Metastore | Apache Polaris |
|---|---|---|
| Lakebench default | With no recipe set | Of `lakebench init` |
| Setup | Stackable operators (commons, secret, listener, hive) | None: Lakebench deploys Polaris |
| Protocol | Thrift (binary, port 9083) | Iceberg REST/HTTP (JSON, port 8181), OAuth2 client credentials |
| Table formats | Iceberg, Delta (the only catalog for Delta) | Iceberg |
| Multi-engine sharing | Spark and Trino in the same cluster | Any engine with Iceberg REST support |
| Governance | Basic | Access control |
| Multi-tenancy | Single catalog namespace | Namespace-level isolation |
| Credential vending | No; credentials injected via SecretClass | No: `stsUnavailable: true`, and Spark and Trino keep static S3 credentials |
| PostgreSQL backend | `hive` database | Separate `polaris` database |

- **Choose Hive** for the simplest deployment, a single compute cluster, or Delta.
- **Choose Polaris** for REST access to the catalog, or to share tables across engines or clusters. Lakebench does not use Polaris credential vending.
- Unity Catalog OSS (lineage, governance) is not supported: no Unity combination is accepted. Nessie is not implemented.
- Details: [Hive Metastore](component-hive.md), [Polaris](component-polaris.md).

## Recipe Details

Every recipe deploys PostgreSQL (the catalog backend) and Spark RBAC.

| Recipe | Also deploys | Does not deploy |
|---|---|---|
| Standard | Hive Metastore (Stackable HiveCluster), Trino coordinator + workers | Polaris |
| Standard Headless | Hive Metastore | Trino, Polaris |
| Spark SQL | Hive Metastore, Spark Thrift Server | Trino, Polaris |
| DuckDB | Hive Metastore, DuckDB pod | Trino, Polaris |
| Polaris | Polaris server + bootstrap Job (realm, catalog, roles), Trino with the REST catalog connector | Hive Metastore |
| Polaris Headless | Polaris server + bootstrap Job | Hive Metastore, Trino |
| Polaris Spark SQL | Polaris server + bootstrap Job, Spark Thrift Server | Hive Metastore, Trino |
| Polaris DuckDB | Polaris server + bootstrap Job, DuckDB pod | Hive Metastore, Trino |
| Hive Delta Trino | Hive Metastore, Trino coordinator + workers | Polaris |
| Hive Delta Spark SQL | Hive Metastore, Spark Thrift Server | Trino, Polaris |
| Hive Delta Headless | Hive Metastore | Trino, Polaris |

### Hive and Polaris recipes

- Hive recipes need the Stackable Hive Operator CRD (`hiveclusters.hive.stackable.tech`) and the commons, listener and secret operators. A cluster admin installs them once: [Stackable operator](component-hive.md#stackable-operator).
- Polaris recipes: version floors, the client secret and the STS settings are in [Polaris](component-polaris.md). Spark Thrift reaches Polaris through the REST catalog API.

### Headless recipes (`none`)

- The full bronze-silver-gold pipeline runs; downstream consumers read the
  tables directly.
- The query benchmark stage is skipped automatically, or pass
  `--skip-benchmark` to `lakebench run`.

### Spark Thrift and DuckDB recipes

- Defaults and auto-sizing: [Spark Thrift](component-spark-thrift.md#configuration-keys), [DuckDB](component-duckdb.md#configuration-keys).

## Pipeline Modes and Support States

The pipeline mode, `batch` or `continuous`, sets how data flows:

| Mode | Stages |
|---|---|
| `batch` | bronze-verify, silver-build, gold-finalize, then the benchmark. The default. |
| `continuous` | bronze-ingest, silver-stream and gold-refresh run concurrently over a corpus that keeps arriving, for `architecture.pipeline.continuous.run_duration`. |

```yaml
architecture:
  pipeline:
    mode: continuous   # or batch
```

- `lakebench run <config> --continuous` runs one config in continuous mode
  without editing it.
- `sustained` is a deprecated alias of `continuous`.

See [Compatibility matrix](compatibility-matrix.md) for support states and platform requirements.

## Using `lakebench config recommend`

`config recommend` shows the largest scale your cluster can hold for a
config, and what a larger scale needs.

```bash
lakebench config recommend
lakebench config recommend my-config.yaml
```

- It sizes the config as written (recipe, workload, mode, datagen settings)
  at each scale, with the same function as the `run` capacity check.
- It does not select a recipe.
- The config file is optional (default `lakebench.yaml`). A config that does
  not load is an error.
- The top-level `lakebench recommend` is deprecated and hidden from help.

## Advanced Configuration

A recipe sets the four architecture axes. Every other setting can be
overridden without leaving the recipe:

```yaml
name: my-lakehouse
recipe: polaris-iceberg-spark-trino     # recipe sets the architecture

platform:
  compute:
    spark:
      silver_executors: 16             # sizes silver-build (batch) or silver-stream (continuous)
      gold_executors: 12

architecture:
  query_engine:
    trino:
      worker:
        replicas: 4                    # more Trino workers than default
        memory: 32Gi

  pipeline:
    mode: continuous                    # continuous instead of batch

workload:
  datagen:
    scale: 100                       # 1 TB test

observability:
  enabled: true                        # per-deployment Prometheus, Grafana, Pushgateway
```

Full YAML schema: [Configuration Reference](configuration.md).

## See also

[Getting started](getting-started.md), [Trino](component-trino.md), [Spark](component-spark.md).
