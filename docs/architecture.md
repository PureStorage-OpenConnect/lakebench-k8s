# Architecture

Reference: the layers, pipeline, components and deploy order of a Lakebench deployment.

Lakebench deploys a lakehouse stack on Kubernetes and runs reproducible
benchmarks against it, in three layers.

## System Layers

```
+-----------------------------------------------------------------------+
|                        Layer 3: Observability                         |
|   Prometheus  |  Grafana Dashboards  |  Local JSON Metrics + Reports  |
+-----------------------------------------------------------------------+
|                      Layer 2: Data Architecture                       |
| Catalog: Hive / Polaris | Format: Iceberg / Delta | Query: Trino etc. |
|                     Processing: Apache Spark                          |
+-----------------------------------------------------------------------+
|                        Layer 1: Platform                              |
|       Kubernetes (vanilla or OpenShift)  |  S3-Compatible Storage     |
|                     PostgreSQL (metadata)                             |
+-----------------------------------------------------------------------+
```

| Layer | Contents |
|---|---|
| 1. Platform | Any Kubernetes distribution (vanilla, OpenShift, EKS, GKE); any S3-compatible store (AWS S3, MinIO, Pure Storage FlashBlade); PostgreSQL for catalog metadata |
| 2. Data architecture | Catalog: Hive Metastore or Apache Polaris. Table format: Iceberg or Delta (Delta only with Hive). Spark runs the medallion pipeline. Query engine: Trino, Spark Thrift Server or DuckDB (DuckDB only with Iceberg) |
| 3. Observability | Optional Prometheus and Grafana (see below). The CLI writes local JSON metrics per run, for offline analysis and HTML reports |

With observability on, Prometheus scrapes Trino JMX, per-pod CPU and memory, and the Pushgateway that datagen and the Spark stages push to. Grafana renders dashboards. Spark engine metrics are not exported: the Spark UI, which serves them, is off.

## Medallion Pipeline

```
  +------------------+      +--------------------+      +-------------------------+
  |      Bronze      |      |       Silver       |      |          Gold           |
  |   (Raw Parquet)  | ---> |  (Cleaned table)   | ---> |   (Aggregated table)    |
  |                  |      |                    |      |                         |
  |  S3 bucket:      |      |  S3 bucket:        |      |  S3 bucket:             |
  |  <name>-bronze   |      |  <name>-silver     |      |  <name>-gold            |
  +------------------+      +--------------------+      +-------------------------+
       bronze-verify             silver-build              gold-finalize
       (Spark batch)             (Spark batch)             (Spark batch)
```

Batch stages for Customer 360 (AML tables:
[AML spec, section 2](benchmarks/aml/data-model.md)):

| Stage | Reads | Does | Writes |
|---|---|---|---|
| datagen | -- | Generates raw data | Parquet in bronze |
| `bronze-verify` | bronze | Checks rows, required columns and types, and rows that pass the silver filter; records the bronze data clock; fails when bronze cannot produce a valid silver | -- |
| `silver-build` | bronze | Normalizes email and phone, geo enrichment, customer segmentation, quality flags | `customer_interactions_enriched` |
| `gold-finalize` | silver | Daily revenue, engagement, churn indicators | `customer_executive_dashboard` |

After the pipeline, the active query engine runs the workload's benchmark
queries against silver and gold and computes Queries per Hour (QpH): 8 queries
for Customer 360, 12 for AML (8 analytical plus 4 investigator). See
[Query Reference](benchmarks/c360/queries.md#6-query-set).

### Multi-Cycle Batch

`architecture.pipeline.cycles` runs N batch cycles to model daily table
growth. Cycle 1 creates the tables; later cycles append. See
[Configuration -- Multi-Cycle Batch](configuration.md#multi-cycle-batch).

### Continuous Mode

Spark Structured Streaming runs three jobs alongside datagen, which writes
until the window ends:

| Job | Does |
|---|---|
| `bronze-ingest` | Reads new Parquet files as they appear, with no per-trigger limit |
| `silver-stream` | Transforms new bronze rows to silver |
| `gold-refresh` | Recomputes the gold dates (Customer 360) or re-runs the rules over the new silver rows (AML) |

- The jobs run back to back by default (trigger interval 0 s).
- Run duration, trigger intervals and checkpoint locations are configurable.
- Offered load per scale unit: 4 MB/s for AML, 10 MB/s for Customer 360.
  Lakebench gives datagen the cores that produce it;
  `workload.datagen.parallelism` or `workload.datagen.cpu` overrides that.
- AML runs rules W2, W3, W4, W5, W6 and W17 on every gold refresh, and records
  W1, W7 and W8 as not run. W5 and W6 screen each payment as it arrives, with
  no rescreen.

## Component Topology

All components deploy into one Kubernetes namespace:

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

- A StatefulSet with a persistent volume.
- Metadata backend for Hive Metastore and Polaris.
- Storage class: the cluster default unless
  `platform.compute.postgres.storage_class` is set.

### Catalog Service (Hive Metastore or Polaris)

`architecture.catalog.type` selects the catalog. Both register and serve
tables the same way, so the pipeline does not change.

| | Hive Metastore (`hive`) | Apache Polaris (`polaris`) |
|---|---|---|
| Deployed as | Stackable `HiveCluster` CRD (Stackable Hive Operator) | Deployment, plus a bootstrap Job that creates the warehouse, principal and grants |
| Endpoint | Thrift, port 9083 | Iceberg REST Catalog, port 8181 |
| Spark connects with | Hive Metastore Thrift URI | `RESTCatalog` client, OAuth2 credentials |
| Trino connects with | Hive Metastore Thrift URI | Iceberg REST connector, OAuth2 credentials |
| State | Shared PostgreSQL | Shared PostgreSQL |

### Spark

- Jobs are `SparkApplication` resources managed by the Kubeflow Spark
  Operator (v2.x).
- Volumes do not rely on the operator's webhook, which does not inject them
  from the SparkApplication spec. The scripts ConfigMaps and emptyDir volumes
  are declared in the driver and executor pod templates. Scratch PVCs attach
  through `spark.kubernetes.*.volumes.persistentVolumeClaim.*` conf
  properties. See [component-spark.md](component-spark.md#spark-operator).
- Pipeline scripts ship in one ConfigMap per role (`lakebench-scripts-common`,
  `-c360`, `-aml-rules`, `-aml-jobs`, `-aml-gate`, `-aml-data`), projected
  together at `/opt/spark/scripts` in every Spark pod. `run` applies them
  before any job and refuses to start if a listed file is missing from the
  package or a map is over 80% of the 1 MiB ConfigMap limit. See
  [component-spark.md](component-spark.md).
- Driver and executor pods run as UID 185 (the `spark` user in the
  `apache/spark` base image).
- On OpenShift, `deploy` grants the `anyuid` SCC to the
  `lakebench-spark-runner` and `lakebench-postgres` service accounts through
  the Kubernetes API, and fails if the grant is refused.
- Executor sizes, counts and scaling:
  [component-spark.md -- Batch Jobs](component-spark.md#batch-jobs) and
  [Auto-Scaling](component-spark.md#auto-scaling). Why they are fixed:
  [Internals](development.md#spark-job-sizing).

### Trino

- Coordinator (Deployment, ClusterIP service) plus workers (StatefulSet).
- Workers spill to `emptyDir` by default, or to PVCs when
  `architecture.query_engine.trino.worker.storage_class` is set.
- Reads table metadata from the catalog and data directly from S3.

## Catalog Pluggability

```yaml
architecture:
  catalog:
    type: hive     # or "polaris"
```

Lakebench accepts only the recipes below; any other combination is refused at
config load with the reason. A listed recipe is a valid composition, not a
release-validated one: support is judged per workload x recipe x mode (see
[Compatibility Matrix](compatibility-matrix.md#support-states)).

<!-- BEGIN GENERATED: recipe-components -->
<!-- Generated from the code by `PYTHONPATH=src python3.11 -m lakebench.config.support .`; do not edit by hand. -->

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

## Storage

| Bucket | Holds |
|---|---|
| `<name>-bronze` | Raw Parquet files from datagen |
| `<name>-silver` | Cleaned table |
| `<name>-gold` | Aggregated KPI table |

- `<name>` is the deployment `name`. Names are configurable under
  `platform.storage.s3.buckets`.
- Path-style access is on by default, for stores without virtual-hosted
  bucket addressing (FlashBlade, MinIO).
- Spark uses the Hadoop `S3AFileSystem` connector with fast upload
  (byte-buffer), 200 max connections, 256 MB multipart threshold and 256 MB
  block size. See [Storage backends](storage-backends.md#spark-s3a).

## Deployment Order

- Deploy steps: [Deployment order](deployment.md#deployment-order).
- Destroy steps, roughly in reverse: [Destroy order](deployment.md#destroy-order).
