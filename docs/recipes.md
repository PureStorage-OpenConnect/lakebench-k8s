# Recipes -- Component Combinations

A **recipe** (also called a **quick-recipe**) is the validated combination of catalog, table format, pipeline engine, and query engine that defines a Lakebench deployment's data architecture. Lakebench validates the architecture at config load time and rejects anything outside the recipe list with the reason. There are 11 recipes. A recipe is architecture only: whether a workload and mode are supported on it is a separate question, answered by the support table below.

## Quick-Recipes

Instead of setting `catalog`, `table_format`, `pipeline_engine`, and `query_engine` individually, use the `recipe:` field for one-line setup:

```yaml
name: my-lakehouse
recipe: polaris-iceberg-spark-trino    # sets catalog, format, engine, and query engine in one line
```

Polaris recipes need no `client_secret`: `deploy` generates one per
deployment and stores it in the namespace.

Recipe defaults are merged without overwriting: images, versions and engine resources you set always take precedence. The four components a recipe sets (`architecture.catalog.type`, `architecture.table_format.type`, `architecture.pipeline_engine`, `architecture.query_engine.type`) may be left out or written with the recipe's value; a different value is refused at load, naming both keys. A config with no `recipe:`, or `recipe: default`, still resolves to `hive-iceberg-spark-trino` (or to the components it sets) with a deprecation note; v1.8 requires `recipe:`. Available recipe names: `hive-iceberg-spark-trino` (`default` is a deprecated alias), `hive-iceberg-spark-thrift`, `hive-iceberg-spark-duckdb`, `hive-iceberg-spark-none`, `polaris-iceberg-spark-trino`, `polaris-iceberg-spark-thrift`, `polaris-iceberg-spark-duckdb`, `polaris-iceberg-spark-none`, `hive-delta-spark-trino`, `hive-delta-spark-thrift`, `hive-delta-spark-none`.

## Quick Reference

The short names in the first column are the section headings below; the
`recipe:` value is the second column.

| Name | `recipe:` | Catalog | Table Format | Query Engine | Best For | Prerequisites |
|---|---|---|---|---|---|---|
| **Standard** | `hive-iceberg-spark-trino` (`default`) | hive | iceberg | trino | Ad-hoc SQL analytics | Stackable Hive Operator |
| **Standard Headless** | `hive-iceberg-spark-none` | hive | iceberg | none | ETL-only workloads | Stackable Hive Operator |
| **Spark SQL** | `hive-iceberg-spark-thrift` | hive | iceberg | spark-thrift | Spark-native analytics | Stackable Hive Operator |
| **DuckDB** | `hive-iceberg-spark-duckdb` | hive | iceberg | duckdb | Lightweight single-node analytics | Stackable Hive Operator |
| **Polaris** | `polaris-iceberg-spark-trino` | polaris | iceberg | trino | Multi-engine catalog sharing, fine-grained access control | None (Lakebench deploys Polaris) |
| **Polaris Headless** | `polaris-iceberg-spark-none` | polaris | iceberg | none | REST catalog ETL | None (Lakebench deploys Polaris) |
| **Polaris Spark SQL** | `polaris-iceberg-spark-thrift` | polaris | iceberg | spark-thrift | Spark-native with REST catalog | None (Lakebench deploys Polaris) |
| **Polaris DuckDB** | `polaris-iceberg-spark-duckdb` | polaris | iceberg | duckdb | Lightweight analytics with REST catalog | None (Lakebench deploys Polaris) |
| **Hive Delta Trino** | `hive-delta-spark-trino` | hive | delta | trino | Databricks-comparable analytics | Stackable Hive Operator |
| **Hive Delta Spark SQL** | `hive-delta-spark-thrift` | hive | delta | spark-thrift | Delta + Spark-native analytics | Stackable Hive Operator |
| **Hive Delta Headless** | `hive-delta-spark-none` | hive | delta | none | Delta ETL-only workloads | Stackable Hive Operator |

Every recipe uses Spark as the pipeline engine. There is no DuckDB + Delta
recipe (DuckDB cannot read Delta on non-AWS S3) and no Polaris + Delta recipe
(Polaris is Iceberg-only).

## Choosing a Recipe

Use this decision tree to narrow down the right recipe:

- **Need ad-hoc SQL after the pipeline runs?** Use `trino` as the query engine (Standard or Polaris).
- **Need a REST catalog API?** Use `polaris` recipes. Polaris exposes an Iceberg REST API on port 8181, enabling multi-engine access and OAuth2 authentication.
- **Already have the Stackable Hive Operator installed?** The `hive` recipes are simpler to reason about and use the battle-tested Thrift protocol.
- **Only need ETL, no interactive queries?** Use a `none` query engine recipe (Standard Headless, Polaris Headless or Hive Delta Headless). This skips the query engine entirely.
- **Running the AML (financial) workload?** Use an Iceberg recipe. AML on Delta is refused at config load.

## Recipe Details

### Standard

The default recipe. Hive Metastore provides the catalog via Thrift, Iceberg manages table state, and Trino serves as the query engine for ad-hoc SQL analytics and the benchmark query suite.

```yaml
architecture:
  catalog:
    type: hive
  table_format:
    type: iceberg
  query_engine:
    type: trino
```

**Deploys:** PostgreSQL (Hive backend), Hive Metastore (Stackable HiveCluster), Trino coordinator + workers, Spark RBAC.

**Does not deploy:** Polaris.

**Caveats:** Requires the Stackable Hive Operator CRD (`hiveclusters.hive.stackable.tech`) and the commons, listener and secret operators it depends on. A cluster admin installs them once (lakebench does not):

```bash
for op in commons-operator listener-operator secret-operator hive-operator; do
  helm install $op oci://oci.stackable.tech/sdp-charts/$op \
    --version 25.7.0 --namespace stackable --create-namespace
done
```

---

### Standard Headless

Pipeline-only variant. Runs the full bronze-silver-gold Spark pipeline but does not deploy a query engine. Useful for ETL workloads where downstream consumers read Iceberg tables directly.

```yaml
architecture:
  catalog:
    type: hive
  table_format:
    type: iceberg
  query_engine:
    type: none
```

**Deploys:** PostgreSQL, Hive Metastore (Stackable HiveCluster), Spark RBAC.

**Does not deploy:** Trino, Polaris.

**Caveats:** The query benchmark stage is skipped when `query_engine` is `none`. Use `--skip-benchmark` with `lakebench run` or it will be skipped automatically.

---

### Spark SQL

Uses Spark Thrift Server as the query engine instead of Trino. Queries run through Spark's SQL engine, which can be advantageous when the analytics workload is tightly coupled with the Spark processing pipeline.

```yaml
architecture:
  catalog:
    type: hive
  table_format:
    type: iceberg
  query_engine:
    type: spark-thrift
```

**Deploys:** PostgreSQL, Hive Metastore (Stackable HiveCluster), Spark Thrift Server, Spark RBAC.

**Does not deploy:** Trino, Polaris.

**Caveats:** Requires the Stackable Hive Operator. The Spark Thrift Server uses 2 cores and a 4g heap by default (configurable via `architecture.query_engine.spark_thrift`); on `hive-delta-spark-thrift` the auto-sized default is 8 cores and a 16g heap, because Delta is not compacted before the benchmark.

---

### DuckDB

Lightweight single-node query engine. DuckDB runs as a single pod and executes benchmark queries via `kubectl exec`. Useful for small-scale benchmarks or environments where Trino's distributed overhead is not warranted.

```yaml
architecture:
  catalog:
    type: hive
  table_format:
    type: iceberg
  query_engine:
    type: duckdb
```

**Deploys:** PostgreSQL, Hive Metastore (Stackable HiveCluster), DuckDB pod, Spark RBAC.

**Does not deploy:** Trino, Polaris.

**Caveats:** Requires the Stackable Hive Operator. DuckDB uses 2 cores and 4g memory by default (configurable via `architecture.query_engine.duckdb`). Queries are adapted from Trino SQL to DuckDB-compatible syntax at runtime.

---

### Polaris

Uses Apache Polaris as a REST catalog instead of Hive Metastore. Polaris provides an Iceberg REST API (port 8181) with OAuth2 authentication, making it suitable for multi-engine environments and fine-grained access control. Lakebench deploys Polaris automatically -- no external operator required.

```yaml
architecture:
  catalog:
    type: polaris
  table_format:
    type: iceberg
  query_engine:
    type: trino
```

**Deploys:** PostgreSQL (Polaris backend), Polaris server + bootstrap Job (realm, catalog, roles), Trino coordinator + workers (with REST catalog connector), Spark RBAC.

**Does not deploy:** Hive Metastore.

**Requirements:** Polaris 1.3.0-incubating+ (lakebench defaults to 1.6.0) and Trino 454+. The client secret is generated per deployment unless `architecture.catalog.polaris.client_secret` sets one.

**Caveats:** The bootstrap Job always creates the catalog with `stsUnavailable=true` and `pathStyleAccess=true` (needed on FlashBlade and other non-AWS S3). Each client (Spark, Trino) maintains its own static S3 credentials rather than using credential vending.

---

### Polaris Headless

REST catalog pipeline without a query engine. Useful when Polaris is the catalog standard but queries happen outside Lakebench.

```yaml
architecture:
  catalog:
    type: polaris
  table_format:
    type: iceberg
  query_engine:
    type: none
```

**Deploys:** PostgreSQL, Polaris server + bootstrap Job, Spark RBAC.

**Does not deploy:** Hive Metastore, Trino.

**Caveats:** Benchmark stage is skipped.

---

### Polaris Spark SQL

Combines the Polaris REST catalog with Spark Thrift Server for querying. Best when the environment standardizes on both REST catalog APIs and Spark-native analytics.

```yaml
architecture:
  catalog:
    type: polaris
  table_format:
    type: iceberg
  query_engine:
    type: spark-thrift
```

**Deploys:** PostgreSQL, Polaris server + bootstrap Job, Spark Thrift Server, Spark RBAC.

**Does not deploy:** Hive Metastore, Trino.

**Caveats:** Polaris 1.3.0-incubating+ required (lakebench defaults to 1.6.0). Spark Thrift Server connects to Polaris via the REST catalog API.

---

### Polaris DuckDB

Combines the Polaris REST catalog with DuckDB for lightweight querying. Useful for small-scale benchmarks with REST catalog access.

```yaml
architecture:
  catalog:
    type: polaris
  table_format:
    type: iceberg
  query_engine:
    type: duckdb
```

**Deploys:** PostgreSQL, Polaris server + bootstrap Job, DuckDB pod, Spark RBAC.

**Does not deploy:** Hive Metastore, Trino.

**Caveats:** Polaris 1.3.0-incubating+ required (lakebench defaults to 1.6.0). DuckDB uses 2 cores and 4g memory by default.

---

## Pipeline Modes and Support States

Recipes define the data architecture. The pipeline mode, `batch` or
`continuous`, defines how data flows through it:

| Mode | Stages |
|---|---|
| `batch` | bronze-verify, silver-build, gold-finalize, then the benchmark. The default. |
| `continuous` | bronze-ingest, silver-stream and gold-refresh run concurrently over a corpus that keeps arriving, for `architecture.pipeline.continuous.run_duration`. |

```yaml
architecture:
  pipeline:
    mode: continuous   # or batch
```

`lakebench run <config> --continuous` runs one config in continuous mode
without editing it. `sustained` is accepted as a deprecated alias of
`continuous`.

Support is judged per workload x recipe x mode: **supported** (validated on the
release tree), **unverified** (valid, not release-validated) or
**unsupported** (refused before a run). See
[Compatibility Matrix](compatibility-matrix.md#support-states) for the rules.
This table is generated from the code:

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

- **unsupported**, refused at config load: AML (financial) on `hive-delta-spark-none`, `hive-delta-spark-thrift`, `hive-delta-spark-trino`. The financial (AML) workload supports table_format iceberg, not delta. Its stage scripts and table DDL are written for iceberg only, so this combination would not run the workload it names. Use an iceberg recipe (for example recipe: polaris-iceberg-spark-trino), or with no recipe set architecture.table_format.type to iceberg.
- Any catalog, table format and query engine combination that is not a recipe above is refused at config load for every workload.
- AML (financial) continuous: AML continuous runs detection rules W2, W3, W4, W17 each tick and records W1, W5, W6, W7, W8 as not run. Its results depend on when detection ran relative to arrival, so no end-of-run result check is recorded.

<!-- END GENERATED: support-states -->

## Using `lakebench config recommend`

The `config recommend` command shows sizing guidance for your cluster: the largest scale it can hold for your config, and what a larger scale needs. It sizes the config as written (its recipe, workload, mode and datagen settings) at each scale, with the same function the `run` capacity preflight uses. It does not select a recipe, but it helps size the deployment. It takes an optional config file (default `lakebench.yaml`); a config that does not load is an error:

```bash
lakebench config recommend
lakebench config recommend my-config.yaml
```

The older top-level `lakebench recommend` is deprecated and hidden from help; use `lakebench config recommend`.

Combine the output of `config recommend` with the recipe table above to configure your deployment. The recipe controls what gets deployed; the scale factor (informed by `config recommend`) controls how large the deployment is.

## Advanced Configuration

Quick-recipes set the architecture axes (catalog, table format, pipeline engine, query engine) to validated defaults. Every other setting -- component versions, Spark sizing, executor counts, worker replicas, pipeline mode, datagen parallelism, timeouts, and observability -- can be overridden selectively without abandoning the recipe.

Use a recipe as a starting point and override only what you need:

```yaml
name: my-lakehouse
recipe: polaris-iceberg-spark-trino     # quick-recipe sets the architecture

platform:
  compute:
    spark:
      silver_executors: 16             # override auto-scaled executor count
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
  enabled: true                        # deploy Prometheus + Grafana
```

Recipe defaults are merged via `_deep_setdefault`: your explicit values take precedence, except the four recipe-owned components, which must agree with the recipe. See the [Configuration Reference](configuration.md) for the full YAML schema.

## Cross-References

- [Getting Started](getting-started.md) -- Initial setup and first deployment
- [Quickstart: Polaris](quickstart-polaris.md) -- Step-by-step Polaris recipe walkthrough
- [Configuration Reference](configuration.md) -- Full YAML schema documentation
- [Component: Hive](component-hive.md) -- Hive Metastore deep dive
- [Component: Trino](component-trino.md) -- Trino deployment and tuning
- [Component: Spark](component-spark.md) -- Spark job profiles and resource sizing
- [Operators and Catalogs](operators-and-catalogs.md) -- Operator installation and catalog management
