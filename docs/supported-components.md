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
| Apache Spark | 3.5.4 | `apache/spark:3.5.4-python3` | Pipeline processing (bronze, silver, gold stages) |
| Spark Operator | 2.4.0 | Kubeflow Helm chart | Submits SparkApplication CRDs to Kubernetes |

Spark runs all data pipeline jobs. The Spark Operator manages job lifecycle
via Kubernetes CRDs. Operator v2.x is required (v1.x has issues with
ConfigMap volume injection). See [Spark Reference](component-spark.md) for
tuning, executor profiles, and version compatibility.

---

## Catalogs

| Component | Default Version | Image | Role |
|-----------|----------------|-------|------|
| Hive Metastore | 3.1.3 | Stackable Hive Operator 25.7.0 | Thrift-based catalog for Iceberg and Delta tables |
| Apache Polaris | 1.3.0-incubating | `apache/polaris:1.3.0-incubating` | REST-based Iceberg catalog with OAuth2 |

Each recipe uses exactly one catalog. Hive supports both Iceberg and Delta
table formats. Polaris supports Iceberg only. See
[Recipes](recipes.md) for valid combinations.

**Hive prerequisites:** Stackable operators (commons, secret, listener, hive)
must be installed cluster-wide. See [Getting Started](getting-started.md#catalog-operator-hive)
for Helm commands.

**Polaris:** No operator needed -- Lakebench deploys it directly as a
Kubernetes Deployment with a bootstrap Job.

See [Hive Reference](component-hive.md) and
[Operators and Catalogs](operators-and-catalogs.md) for version compatibility
and troubleshooting.

---

## Table Formats

| Component | Default Version | Delivery | Role |
|-----------|----------------|----------|------|
| Apache Iceberg | 1.10.1 | Spark runtime JAR (`iceberg-spark-runtime-3.5_2.12`) | Open table format with ACID transactions |
| Delta Lake | 3.0.0 | Spark runtime JAR | Open table format (Hive catalog only) |

Iceberg is the default and works with both catalogs. Delta is supported
with the Hive catalog only.

---

## Query Engines

| Component | Default Version | Image | Role |
|-----------|----------------|-------|------|
| Trino | 479 | `trinodb/trino:479` | Distributed SQL engine for interactive analytics |
| Spark Thrift Server | 3.5.4 (same as Spark) | `apache/spark:3.5.4-python3` | Spark-native SQL via HiveServer2 JDBC |
| DuckDB | Bundled in Python 3.11 | `python:3.11-slim` | Lightweight single-pod analytics engine |

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
storage sizing and backup.

---

## Observability (optional)

| Component | Default Version | Image | Role |
|-----------|----------------|-------|------|
| Prometheus | v2.48.0 | `prom/prometheus:v2.48.0` | Metrics collection and time-series storage |
| Grafana | 10.2.0 | `grafana/grafana:10.2.0` | Dashboards and visualization |

Deployed together via the `kube-prometheus-stack` Helm chart when
`observability.enabled: true`. Includes node-exporter and kube-state-metrics.
Three built-in Grafana dashboards: Pipeline Overview, Spark Detail,
Storage I/O. See [Observability Reference](component-observability.md) for
dashboard configuration and metric details.

---

## Kubernetes and Storage

| Requirement | Minimum | Tested On |
|-------------|---------|-----------|
| Kubernetes | 1.26+ | OpenShift 4.x, vanilla K8s (kubeadm, EKS, GKE, AKS) |
| S3 storage | Any S3-compatible | Pure Storage FlashBlade, MinIO, AWS S3 |

See [S3 Storage Reference](component-s3.md) for endpoint configuration,
path-style vs virtual-hosted addressing, and bucket layout.

---

## Recipe Matrix

Which components are deployed for each recipe:

| Recipe | Catalog | Format | Engine | Components Deployed |
|--------|---------|--------|--------|---------------------|
| `hive-iceberg-trino` | Hive | Iceberg | Trino | Postgres, Hive, Trino, Spark |
| `hive-iceberg-spark` | Hive | Iceberg | Spark Thrift | Postgres, Hive, Spark Thrift, Spark |
| `hive-iceberg-duckdb` | Hive | Iceberg | DuckDB | Postgres, Hive, DuckDB, Spark |
| `hive-iceberg-none` | Hive | Iceberg | -- | Postgres, Hive, Spark |
| `hive-delta-trino` | Hive | Delta | Trino | Postgres, Hive, Trino, Spark |
| `hive-delta-none` | Hive | Delta | -- | Postgres, Hive, Spark |
| `polaris-iceberg-trino` | Polaris | Iceberg | Trino | Postgres, Polaris, Trino, Spark |
| `polaris-iceberg-spark` | Polaris | Iceberg | Spark Thrift | Postgres, Polaris, Spark Thrift, Spark |
| `polaris-iceberg-duckdb` | Polaris | Iceberg | DuckDB | Postgres, Polaris, DuckDB, Spark |
| `polaris-iceberg-none` | Polaris | Iceberg | -- | Postgres, Polaris, Spark |

---

## Overriding Versions

Lakebench is not locked to the default versions listed above. Every component
image and library version can be overridden in your YAML config, so you can
test newer (or older) releases of any component without waiting for a
Lakebench update:

```yaml
images:
  spark: apache/spark:3.5.4-python3
  postgres: postgres:17
  hive: apache/hive:3.1.3
  polaris: apache/polaris:1.3.0-incubating
  trino: trinodb/trino:479
  duckdb: python:3.11-slim
  prometheus: prom/prometheus:v2.48.0
  grafana: grafana/grafana:10.2.0

architecture:
  table_format:
    iceberg:
      version: "1.10.1"
    delta:
      version: "3.0.0"
```

See [Configuration](configuration.md) for the full YAML reference.
