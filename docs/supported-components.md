# Supported Components

Lakebench deploys and manages the following components. The versions listed
below are **defaults** -- every component image and library version can be
overridden in your YAML config. See [Overriding Versions](#overriding-versions)
at the bottom of this page, or the [Configuration](configuration.md) reference
for the full YAML schema.

---

## Compute

| Component | Default Version | Image | Role |
|-----------|----------------|-------|------|
| Apache Spark | 3.5.x / 4.0.x / 4.1.x | `apache/spark:4.1.1-python3` (default for the Hive recipes), `4.0.2-python3` (default for the Polaris recipes and `hive-delta-spark-thrift`), or a Spark 3.5 image (a Java 11 tag such as `3.5.4-python3` gets Iceberg 1.10.1; a java17 tag gets 1.11.0) | Pipeline processing (bronze, silver, gold stages) |
| Spark Operator | 2.5.1 | Kubeflow Helm chart | Submits SparkApplication CRDs to Kubernetes |

Spark runs all data pipeline jobs. The Spark Operator manages job lifecycle
via Kubernetes CRDs. Operator v2.x is required (v1.x has issues with
ConfigMap volume injection). See [Spark Reference](component-spark.md) for
tuning, executor profiles, and version compatibility.

---

## Catalogs

| Component | Default Version | Image | Role |
|-----------|----------------|-------|------|
| Hive Metastore | 3.1.3 | Stackable Hive Operator 25.7.0 | Thrift-based catalog for Iceberg tables |
| Apache Polaris | 1.6.0 | `apache/polaris:1.6.0` | REST-based Iceberg catalog with OAuth2 |

Each recipe uses exactly one catalog. Both Hive and Polaris support Iceberg
tables; Delta tables use Hive. Unity Catalog is not supported. See
[Recipes](recipes.md) for valid combinations.

**Hive prerequisites:** Stackable operators (commons, secret, listener, hive)
must be installed cluster-wide. See [Getting Started](getting-started.md#catalog-operator-depends-on-your-recipe)
for Helm commands.

**Polaris:** No operator needed -- Lakebench deploys it directly as a
Kubernetes Deployment with a bootstrap Job. Its client secret and DB password
are generated per deployment unless the config sets
`architecture.catalog.polaris.client_secret`.

See [Hive Reference](component-hive.md) and
[Operators and Catalogs](operators-and-catalogs.md) for version compatibility
and troubleshooting.

---

## Table Formats

| Component | Default Version | Delivery | Role |
|-----------|----------------|----------|------|
| Apache Iceberg | 1.11.0 | Spark runtime JAR (`iceberg-spark-runtime-3.5_2.12`, `4.0_2.13`, or `4.1_2.13`) | Open table format with ACID transactions |
| Delta Lake | auto: 4.0.0 on Spark 4.0, 4.1.0 on Spark 4.1 | `io.delta:delta-spark_2.13` (4.0) or `delta-spark_4.1_2.13` (4.1) | Open table format; Spark 4.x only |

Iceberg works with both catalogs (Hive and Polaris). Delta works with Hive
only, is not readable by DuckDB on non-AWS object stores, and supports the
Customer 360 workload only: the AML workload is Iceberg-only.

---

## Query Engines

| Component | Default Version | Image | Role |
|-----------|----------------|-------|------|
| Trino | 483 | `trinodb/trino:483` | Distributed SQL engine for interactive analytics |
| Spark Thrift Server | 3.5.x / 4.0.x / 4.1.x (same as Spark) | Same image as `images.spark` (default per recipe: `4.1.1-python3` on Hive, `4.0.2-python3` on Polaris and `hive-delta-spark-thrift`) | Spark-native SQL via HiveServer2 JDBC |
| DuckDB | 1.5.5 (pinned by `duckdb.version`) | `python:3.11-slim`, DuckDB pip-installed at that version | Lightweight single-pod analytics engine |

Each recipe uses at most one query engine (or `none` for ETL-only
deployments). The benchmark suite runs against whichever engine is active.

**Trino** is the default -- distributed, multi-worker, best for ad-hoc SQL
at scale. Minimum version 454 required for Polaris (OAuth2 scope support).

**Spark Thrift Server** exposes Spark SQL over JDBC. Good for Spark-native
workloads and UDF support.

**DuckDB** runs in a single pod with no external dependencies. Best for
small-scale testing and quick iteration.

See [Trino Reference](component-trino.md) for worker scaling and connector
configuration.

---

## Infrastructure

| Component | Default Version | Image | Role |
|-----------|----------------|-------|------|
| PostgreSQL | 17 | `postgres:17` | Metadata backend for Hive and Polaris |

PostgreSQL stores catalog metadata. Deployed as a StatefulSet with a
persistent volume. See [PostgreSQL Reference](component-postgres.md) for
storage sizing and teardown.

---

## Observability (optional)

Deployed together via the `kube-prometheus-stack` Helm chart when
`observability.enabled: true`. The chart is pinned via
`observability.chart_version` (default `87.19.2`), which bundles Prometheus
v3.13.1 and Grafana v13.1.x -- Prometheus and Grafana versions are not
independently configurable; they come from whatever the pinned chart version
bundles. Includes kube-state-metrics, and node-exporter except on OpenShift,
where lakebench disables it (it needs host access the SCCs block). One
built-in Grafana dashboard, Lakebench Overview, shared by every deployment,
with datagen, bronze/silver/pipeline stage, Trino and node CPU panels, and a
per-deployment Pushgateway for live datagen and pipeline metrics. See
[Observability Reference](component-observability.md) for dashboard
configuration and metric details.

---

## Kubernetes and Storage

| Requirement | Minimum | Tested On |
|-------------|---------|-----------|
| Kubernetes | 1.26+ | OpenShift 4.x |
| S3 storage | Any S3-compatible that passes `lakebench config storage` | Pure Storage FlashBlade; Garage (local mode). AWS S3 and MinIO are not yet validated; see [Storage Backends](storage-backends.md). |

See [S3 Storage Reference](component-s3.md) for endpoint configuration,
path-style vs virtual-hosted addressing, and bucket layout.

---

## Recipe Matrix

Every recipe deploys PostgreSQL, its catalog (Hive Metastore or Polaris), its
query engine (none for the `-none` recipes) and runs Spark for the pipeline.

<!-- BEGIN GENERATED: recipe-components -->
<!-- Generated from the code by `python3.11 -m lakebench.config.support .`; do not edit by hand. -->

| Recipe | Catalog | Table Format | Pipeline Engine | Query Engine |
|---|---|---|---|---|
| `hive-delta-spark-none` | Hive | Delta | Spark | None |
| `hive-delta-spark-thrift` | Hive | Delta | Spark | Spark Thrift |
| `hive-delta-spark-trino` | Hive | Delta | Spark | Trino |
| `hive-iceberg-spark-duckdb` | Hive | Iceberg | Spark | DuckDB |
| `hive-iceberg-spark-none` | Hive | Iceberg | Spark | None |
| `hive-iceberg-spark-thrift` | Hive | Iceberg | Spark | Spark Thrift |
| `hive-iceberg-spark-trino` | Hive | Iceberg | Spark | Trino |
| `polaris-iceberg-spark-duckdb` | Polaris | Iceberg | Spark | DuckDB |
| `polaris-iceberg-spark-none` | Polaris | Iceberg | Spark | None |
| `polaris-iceberg-spark-thrift` | Polaris | Iceberg | Spark | Spark Thrift |
| `polaris-iceberg-spark-trino` | Polaris | Iceberg | Spark | Trino |

<!-- END GENERATED: recipe-components -->

Which workloads and modes each recipe supports is in
[Compatibility Matrix](compatibility-matrix.md#support-states).

---

## Overriding Versions

Lakebench is not locked to the default versions listed above. Component
images and library versions can be overridden in your YAML config, within
these limits:

- Spark must be a 3.5.x, 4.0.x or 4.1.x `-python3` image; Spark 4.2 and other
  minors are rejected at config load.
- An explicit Iceberg or Delta version must be in the compatibility list for
  the Spark minor, or config load fails. Iceberg 1.11+ needs a Java 17 Spark
  image.
- The Hive that runs is not configurable: the Stackable HiveCluster runs
  Hive 3.1.3 (see [Hive Reference](component-hive.md)).
- The Polaris version that runs is the tag of `images.polaris`.

For example:

```yaml
images:
  spark: apache/spark:4.0.2-python3
  postgres: postgres:17
  polaris: apache/polaris:1.6.0
  trino: trinodb/trino:483
  duckdb: python:3.11-slim

architecture:
  table_format:
    iceberg:
      version: "1.11.0"
```

See [Configuration](configuration.md) for the full YAML reference.
