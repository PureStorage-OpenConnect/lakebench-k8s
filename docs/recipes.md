# Recipes -- Component Combinations

A **recipe** is the validated combination of catalog, table format, and query engine that defines a Lakebench deployment's data architecture. Lakebench validates recipes at config load time against a whitelist of supported combinations and rejects anything unsupported with a clear error message. There are currently 10 supported recipes.

## Using the `recipe:` Shorthand

Instead of setting `catalog`, `table_format`, and `query_engine` individually, use the `recipe:` field for one-line setup:

```yaml
name: my-lakehouse
recipe: polaris-iceberg-trino    # sets catalog, format, and engine in one line
```

Recipe defaults are merged without overwriting -- any explicit values you set in `architecture:` always take precedence. Available recipe names: `hive-iceberg-trino` (or `default`), `hive-iceberg-spark`, `hive-iceberg-duckdb`, `hive-iceberg-none`, `hive-delta-trino`, `hive-delta-none`, `polaris-iceberg-trino`, `polaris-iceberg-spark`, `polaris-iceberg-duckdb`, `polaris-iceberg-none`.

## Quick Reference

| Recipe Name | Catalog | Table Format | Query Engine | Best For | Prerequisites |
|---|---|---|---|---|---|
| **Standard** | hive | iceberg | trino | Ad-hoc SQL analytics | Stackable Hive Operator |
| **Standard Headless** | hive | iceberg | none | ETL-only workloads | Stackable Hive Operator |
| **Spark SQL** | hive | iceberg | spark-thrift | Spark-native analytics | Stackable Hive Operator |
| **Delta Lake** | hive | delta | trino | Delta ecosystem compatibility | Stackable Hive Operator |
| **Delta Headless** | hive | delta | none | Delta ETL pipelines | Stackable Hive Operator |
| **Polaris** | polaris | iceberg | trino | Multi-engine catalog sharing, fine-grained access control | None (Lakebench deploys Polaris) |
| **DuckDB** | hive | iceberg | duckdb | Lightweight single-node analytics | Stackable Hive Operator |
| **Polaris Headless** | polaris | iceberg | none | REST catalog ETL | None (Lakebench deploys Polaris) |
| **Polaris Spark SQL** | polaris | iceberg | spark-thrift | Spark-native with REST catalog | None (Lakebench deploys Polaris) |
| **Polaris DuckDB** | polaris | iceberg | duckdb | Lightweight analytics with REST catalog | None (Lakebench deploys Polaris) |

## Choosing a Recipe

Use this decision tree to narrow down the right recipe:

- **Need ad-hoc SQL after the pipeline runs?** Use `trino` as the query engine (Standard, Delta Lake, or Polaris).
- **Need a REST catalog API?** Use `polaris` recipes. Polaris exposes an Iceberg REST API on port 8181, enabling multi-engine access and OAuth2 authentication.
- **Already have the Stackable Hive Operator installed?** The `hive` recipes are simpler to reason about and use the battle-tested Thrift protocol.
- **Only need ETL, no interactive queries?** Use a `none` query engine recipe (Standard Headless, Delta Headless, or Polaris Headless). This skips Trino deployment entirely.
- **Need Delta Lake compatibility?** Only `hive` + `delta` recipes are supported. Polaris does not support Delta tables.

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

**Caveats:** Requires the Stackable Hive Operator CRD (`hiveclusters.hive.stackable.tech`) to be installed on the cluster. Install it with:

```bash
helm install hive-operator oci://oci.stackable.tech/sdp-charts/hive-operator \
  --version 25.7.0 --namespace stackable
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

**Caveats:** Requires the Stackable Hive Operator. The Spark Thrift Server uses 2 cores and 4g memory by default (configurable via `architecture.query_engine.spark_thrift`).

---

### Delta Lake

Replaces Iceberg with Delta Lake as the table format. Trino reads Delta tables via its Delta Lake connector. Choose this when your ecosystem standardizes on Delta.

```yaml
architecture:
  catalog:
    type: hive
  table_format:
    type: delta
  query_engine:
    type: trino
```

**Deploys:** PostgreSQL, Hive Metastore (Stackable HiveCluster), Trino coordinator + workers (with Delta connector), Spark RBAC.

**Does not deploy:** Polaris.

**Caveats:** Requires the Stackable Hive Operator. Delta version defaults to 3.0.0 (configurable via `architecture.table_format.delta.version`). Polaris does not support Delta -- only `hive` catalog recipes work with Delta.

---

### Delta Headless

Delta Lake pipeline without a query engine. ETL-only.

```yaml
architecture:
  catalog:
    type: hive
  table_format:
    type: delta
  query_engine:
    type: none
```

**Deploys:** PostgreSQL, Hive Metastore (Stackable HiveCluster), Spark RBAC.

**Does not deploy:** Trino, Polaris.

**Caveats:** Same as Delta Lake, but the benchmark stage is skipped.

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

**Requirements:** Polaris 1.3.0-incubating+ (default), Trino 454+.

**Caveats:** On FlashBlade, Polaris runs with `stsUnavailable=true` and `pathStyleAccess=true`. Each client (Spark, Trino) maintains its own static S3 credentials rather than using credential vending.

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

**Caveats:** Polaris 1.3.0-incubating+ required. Spark Thrift Server connects to Polaris via the REST catalog API.

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

**Caveats:** Polaris 1.3.0-incubating+ required. DuckDB uses 2 cores and 4g memory by default.

---

## Pipeline Modes

Recipes define the data architecture (catalog + format + query engine). Pipeline modes define how data flows through it. The three supported patterns are:

| Pattern | Config Value | Description |
|---|---|---|
| **Medallion** | `medallion` | Sequential batch pipeline: bronze-verify, silver-build, gold-finalize. The default. |
| **Streaming** | `streaming` | Concurrent streaming jobs: bronze-ingest, silver-stream, gold-refresh running simultaneously. |
| **Batch** | `batch` | Alias for medallion with batch-oriented tuning. |

Pipeline mode is independent of recipe -- any recipe works with any pipeline pattern. Set the pattern in config:

```yaml
architecture:
  pipeline:
    pattern: medallion  # or streaming, batch
```

To run in continuous streaming mode:

```bash
lakebench run test-config.yaml --continuous
```

The `--continuous` flag launches bronze-ingest, silver-stream, and gold-refresh as concurrent streaming jobs that run for the configured duration (default 30 minutes, configurable via `architecture.pipeline.continuous.run_duration`).

> **Note:** The old field name `processing:` is still accepted with a deprecation warning.

## Using `lakebench recommend`

The `recommend` command helps you choose the right scale factor for your cluster, or determine the cluster size needed for a target scale. It does not select a recipe, but it helps size the deployment.

```bash
# Auto-detect cluster capacity and show max feasible scale
lakebench recommend

# Specify cluster resources manually
lakebench recommend --cores 64 --memory 256

# Check requirements for a specific scale
lakebench recommend --scale 100

# Check requirements for petabyte scale
lakebench recommend --scale 100000
```

Combine the output of `recommend` with the recipe table above to configure your deployment. The recipe controls what gets deployed; the scale factor (informed by `recommend`) controls how large the deployment is.

## Cross-References

- [Getting Started](getting-started.md) -- Initial setup and first deployment
- [Quickstart: Polaris](quickstart-polaris.md) -- Step-by-step Polaris recipe walkthrough
- [Configuration Reference](configuration.md) -- Full YAML schema documentation
- [Component: Hive](component-hive.md) -- Hive Metastore deep dive
- [Component: Trino](component-trino.md) -- Trino deployment and tuning
- [Component: Spark](component-spark.md) -- Spark job profiles and resource sizing
- [Operators and Catalogs](operators-and-catalogs.md) -- Operator installation and catalog management
