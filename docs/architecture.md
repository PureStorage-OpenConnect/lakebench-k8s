# Architecture

Lakebench deploys a complete lakehouse stack on Kubernetes and runs reproducible
benchmarks against it. The system is organized into three layers: platform
infrastructure, data architecture, and observability.

## System Layers

```
+-----------------------------------------------------------------------+
|                        Layer 3: Observability                         |
|   Prometheus  |  Grafana Dashboards  |  Local JSON Metrics + Reports  |
+-----------------------------------------------------------------------+
|                      Layer 2: Data Architecture                       |
|  Catalog: Hive / Polaris  |  Table Format: Iceberg  |  Query: Trino  |
|                     Processing: Apache Spark                          |
+-----------------------------------------------------------------------+
|                        Layer 1: Platform                              |
|       Kubernetes (vanilla or OpenShift)  |  S3-Compatible Storage     |
|                     PostgreSQL (metadata)                             |
+-----------------------------------------------------------------------+
```

**Layer 1 (Platform)** provides the compute substrate and persistent storage.
Lakebench runs on any Kubernetes distribution (vanilla, OpenShift, EKS, GKE)
and any S3-compatible object store (AWS S3, MinIO, Pure Storage FlashBlade).
PostgreSQL stores catalog metadata.

**Layer 2 (Data Architecture)** contains the lakehouse components. A catalog
service (Hive Metastore or Apache Polaris) manages table metadata. Apache
Iceberg provides the table format. Spark processes data through the medallion
pipeline. Trino executes analytical queries for benchmarking.

**Layer 3 (Observability)** captures performance data. Prometheus scrapes
Spark and Trino metrics. Grafana renders dashboards. The CLI also collects
local JSON metrics per run for offline analysis and HTML report generation.

## Medallion Pipeline

Lakebench implements the medallion architecture -- a three-layer data pipeline
that progressively refines raw data into business-ready tables.

```
  +------------------+      +--------------------+      +-------------------------+
  |      Bronze      |      |       Silver       |      |          Gold           |
  |   (Raw Parquet)  | ---> | (Cleaned Iceberg)  | ---> |  (Aggregated Iceberg)   |
  |                  |      |                    |      |                         |
  |  S3 bucket:      |      |  S3 bucket:        |      |  S3 bucket:             |
  |  lakebench-bronze|      |  lakebench-silver  |      |  lakebench-gold         |
  +------------------+      +--------------------+      +-------------------------+
       bronze-verify             silver-build              gold-finalize
       (Spark batch)             (Spark batch)             (Spark batch)
```

**Bronze** holds raw Parquet files written by the datagen stage. The
`bronze-verify` Spark job validates data integrity, checks schema conformance,
and registers an Iceberg table over the raw files.

**Silver** contains cleaned and enriched data. The `silver-build` Spark job
reads from bronze, applies normalization transforms (email, phone, geo
enrichment, customer segmentation, quality flags), and writes an Iceberg table
(`customer_interactions_enriched`).

**Gold** contains aggregated KPIs ready for analytics. The `gold-finalize`
Spark job reads from silver, computes business metrics (daily revenue,
engagement, churn indicators), and writes the executive dashboard Iceberg table
(`customer_executive_dashboard`).

After the pipeline completes, Lakebench runs an 8-query benchmark via the
active query engine (Trino, Spark Thrift, or DuckDB) against the gold layer
and computes Queries per Hour (QpH).

### Continuous Mode

In addition to batch processing, Lakebench supports a continuous streaming
pipeline using Spark Structured Streaming:

- `bronze-ingest` reads new Parquet files as they appear (via `maxFilesPerTrigger`)
- `silver-stream` incrementally transforms bronze to silver
- `gold-refresh` periodically recomputes gold aggregations

All three streaming jobs run concurrently alongside the datagen process. The
streaming run duration, trigger intervals, and checkpoint locations are
configurable.

## Component Topology

The following components are deployed into a single Kubernetes namespace:

```
Namespace: lakebench
+-----------------------------------------------------------+
|                                                           |
|  +------------+    +-----------------+    +--------+      |
|  | PostgreSQL |<---| Hive Metastore  |    | Spark  |      |
|  | (metadata) |    |   OR Polaris    |    | RBAC   |      |
|  +------------+    +-----------------+    +--------+      |
|        |                   |                  |           |
|        |                   v                  v           |
|        |           +----------------+   +-----------+     |
|        +---------->| Trino          |   | Spark     |     |
|                    | Coordinator    |   | Operator  |     |
|                    +----------------+   | (cluster) |     |
|                    | Trino Workers  |   +-----------+     |
|                    | (StatefulSet)  |        |            |
|                    +----------------+        v            |
|                                        +-----------+     |
|                                        | Spark     |     |
|                                        | Driver +  |     |
|                                        | Executors |     |
|                                        +-----------+     |
|                                                           |
+-----------------------------------------------------------+
                            |
                            v
                  +-------------------+
                  | S3 Object Storage |
                  | bronze | silver   |
                  | gold   buckets    |
                  +-------------------+
```

### PostgreSQL

Deployed as a StatefulSet with a persistent volume. Serves as the metadata
backend for both Hive Metastore and Polaris catalog. Uses a replicated storage
class (`px-csi-db` or cluster default) for data durability.

### Catalog Service (Hive Metastore or Polaris)

A single configuration field (`architecture.catalog.type`) switches between
catalog implementations:

- **Hive Metastore** -- Deployed via the Stackable Hive Operator as a
  `HiveCluster` CRD. Exposes a Thrift endpoint on port 9083. Both Spark and
  Trino connect to it for Iceberg table metadata.

- **Apache Polaris** -- Deployed as a Deployment with a REST API on port 8181.
  Implements the Iceberg REST Catalog specification. Spark connects via the
  `RESTCatalog` client; Trino connects via the Iceberg REST connector. Polaris
  uses OAuth2 for authentication and stores its catalog state in the shared
  PostgreSQL instance.

The catalog choice is transparent to the pipeline -- both implementations
register and serve Iceberg tables identically.

### Spark

Spark jobs are submitted as `SparkApplication` custom resources managed by the
Kubeflow Spark Operator (v2.x). The operator uses a mutating admission webhook
to inject volumes (scripts ConfigMap, scratch PVCs, Ivy cache) into driver and
executor pods.

Pipeline scripts are packaged into a `lakebench-spark-scripts` ConfigMap and
mounted at `/opt/spark/scripts` in every Spark pod. The driver and executor
pods run as UID 185 (the `spark` user in the `apache/spark` base image).
On OpenShift, an `anyuid` SCC is automatically bound to the
`lakebench-spark-runner` service account.

### Trino

Trino is deployed as a coordinator (Deployment) plus workers (StatefulSet).
The coordinator exposes a ClusterIP service. Workers use persistent volumes
for spill-to-disk when query memory pressure is high. Trino connects to
the catalog service (Hive or Polaris) for Iceberg metadata and reads data
directly from S3.

## Spark Executor Profiles

Each pipeline stage has a fixed per-executor resource profile derived from
production-proven configurations. These values are non-negotiable -- reducing
them causes OOM kills or disk-full failures at scale.

| Stage | Cores | Memory | Overhead | Scratch PVC |
|---|---|---|---|---|
| `bronze-verify` | 2 | 4g | 2g | 50Gi |
| `silver-build` | 4 | 48g | 12g | 150Gi |
| `gold-finalize` | 4 | 32g | 8g | 100Gi |

Per-executor sizing (cores, memory, overhead, PVC) is fixed. What scales with
data is the **executor count**. Executor count is derived automatically from
the scale factor using a linear formula:

- At scale <= 10: uses a base count (4 for bronze, 8 for silver, 4 for gold)
- Above scale 10: adds executors linearly (e.g., silver adds 12 per 100 scale units)
- Each job has a maximum executor cap (20 for bronze/gold, 30 for silver)

Per-job executor count can be overridden in the config for manual tuning:

```yaml
platform:
  compute:
    spark:
      silver_executors: 12   # override auto-scaling for silver-build
```

Streaming jobs (`bronze-ingest`, `silver-stream`, `gold-refresh`) have lighter
profiles since they process micro-batches rather than full table scans.

Scratch PVCs should use a single-replica storage class (`px-csi-scratch`,
repl=1). Using higher replication doubles storage with no benefit for
ephemeral shuffle data.

## Catalog Pluggability

Switching catalogs requires changing a single field in the configuration:

```yaml
architecture:
  catalog:
    type: hive     # or "polaris"
```

When `type: hive`, the deployment engine creates a Stackable `HiveCluster`
resource, and Spark/Trino are configured with Hive Metastore Thrift URIs.

When `type: polaris`, the engine deploys a Polaris REST catalog server and
a bootstrap job that creates the warehouse, principal, and grants. Spark and
Trino are configured with REST catalog endpoints and OAuth2 credentials.

The validated component combinations are:

| Catalog | Table Format | Query Engine |
|---|---|---|
| Hive | Iceberg | Trino |
| Hive | Iceberg | Spark Thrift |
| Hive | Delta | Trino |
| Polaris | Iceberg | Trino |
| Polaris | Iceberg | Spark Thrift |

## Storage

Lakebench uses S3-compatible object storage for all data. Three buckets
correspond to the three medallion layers:

| Bucket | Purpose |
|---|---|
| `lakebench-bronze` | Raw Parquet files from datagen |
| `lakebench-silver` | Cleaned Iceberg table |
| `lakebench-gold` | Aggregated KPI Iceberg table |

Bucket names are configurable. Path-style access is enabled by default for
compatibility with S3-compatible stores (FlashBlade, MinIO) that do not support
virtual-hosted bucket addressing.

All S3 access uses the `S3AFileSystem` Hadoop connector with tuned settings
for high throughput: fast upload with byte-buffer mode, 200 max connections,
256 MB multipart threshold, and 256 MB block size.

## Deployment Order

The deployment engine creates resources in a strict dependency order:

1. **Namespace** -- creates the target namespace if it does not exist
2. **Secrets** -- S3 credentials and PostgreSQL credentials
3. **Scratch StorageClass** -- Portworx repl=1 class for Spark PVCs (if enabled)
4. **PostgreSQL** -- StatefulSet with persistent volume
5. **Catalog** -- Hive Metastore or Polaris (the non-selected one is skipped)
6. **Trino** -- coordinator Deployment + worker StatefulSet
7. **Spark RBAC** -- ServiceAccount, Role, RoleBinding (plus SCC on OpenShift)
8. **Prometheus** -- metrics scraping (if observability is enabled)
9. **Grafana** -- dashboards (if observability is enabled)

Destruction follows the reverse order: Spark jobs and pods first, then Iceberg
table maintenance (expire snapshots, remove orphan files), DROP TABLEs, S3
bucket cleanup, infrastructure removal, and finally namespace deletion.
