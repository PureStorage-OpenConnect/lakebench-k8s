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
| Catalog: Hive / Polaris | Format: Iceberg / Delta | Query: Trino etc. |
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
Iceberg or Delta Lake provides the table format (Delta runs only with Hive).
Spark processes data through the medallion pipeline. A query engine (Trino,
Spark Thrift Server or DuckDB; DuckDB with Iceberg only) executes analytical
queries for benchmarking.

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
  |  <name>-bronze   |      |  <name>-silver     |      |  <name>-gold            |
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

After the pipeline completes, Lakebench runs the workload's benchmark query
set via the active query engine (Trino, Spark Thrift, or DuckDB) against the
silver and gold tables and computes Queries per Hour (QpH): 8 queries for
Customer 360, 12 for AML (8 analytical plus 4 investigator queries).

### Multi-Cycle Batch (v1.1.0)

The `pipeline.cycles` field runs N batch iterations to simulate multi-day
table growth. Cycle 1 creates tables; cycles 2+ append incrementally.
Iceberg compaction and table health tracking run between cycles. See
[Configuration -- Multi-Cycle Batch](configuration.md#multi-cycle-batch).

### Continuous Mode

In addition to batch processing, Lakebench supports a continuous
pipeline using Spark Structured Streaming:

- `bronze-ingest` reads new Parquet files as they appear (via `maxFilesPerTrigger`)
- `silver-stream` incrementally transforms bronze to silver
- `gold-refresh` periodically recomputes gold aggregations

All three continuous jobs run concurrently. Datagen writes a fixed corpus
(it does not generate at a paced rate, although the streams can start before
it finishes), and `bronze-ingest` takes it at a Lakebench-imposed trickle rate (`max_files_per_trigger` files per trigger), so continuous
intake figures are bounded by that cap. The continuous run duration, trigger
intervals, and checkpoint locations are configurable. AML continuous runs
detection rules W2, W3, W4 and W17 each tick and records W1, W5, W6, W7 and
W8 as not run.

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
Kubeflow Spark Operator (v2.x). Lakebench does not rely on the operator's
webhook for volumes, because it does not inject them from the
SparkApplication spec. The scripts ConfigMaps and emptyDir volumes are
declared in driver and executor pod templates, and scratch PVCs are attached
through `spark.kubernetes.*.volumes.persistentVolumeClaim.*` conf properties.
See [component-spark.md](component-spark.md#spark-operator).

Pipeline scripts ship in one ConfigMap per role (`lakebench-scripts-common`,
`-c360`, `-aml-rules`, `-aml-jobs`, `-aml-gate` and `-aml-data`), projected
together at `/opt/spark/scripts` in every Spark pod. `run` applies them before
any job is submitted and refuses to start if a listed file is missing from the
package or a map is over 80% of the 1 MiB ConfigMap limit; see
[component-spark.md](component-spark.md). The driver and executor
pods run as UID 185 (the `spark` user in the `apache/spark` base image).
On OpenShift, `deploy` grants the `anyuid` SCC to the
`lakebench-spark-runner` and `lakebench-postgres` service accounts through the
Kubernetes API, and fails if the grant is refused.

### Trino

Trino is deployed as a coordinator (Deployment) plus workers (StatefulSet).
The coordinator exposes a ClusterIP service. By default, workers use ephemeral
storage (`emptyDir`) for spill-to-disk. When `storage_class` is set in the
config, workers use PVC-backed persistent volumes instead. Trino connects to
the catalog service (Hive or Polaris) for Iceberg metadata and reads data
directly from S3.

## Spark Executor Profiles

Each pipeline stage has a fixed per-executor resource profile derived from
production-proven configurations. These values are non-negotiable -- reducing
them causes OOM kills or disk-full failures at scale.

| Stage | Cores | Memory | Overhead | Scratch PVC |
|---|---|---|---|---|
| `bronze-verify` | 2 | 4g (8g financial) | 2g (12g financial) | 50Gi (c360) / 500Gi (financial) |
| `silver-build` | 4 | 48g | 12g | 300Gi |
| `gold-finalize` | 4 | 32g | 8g | 300Gi |

The financial workload's bronze-verify trips a CTAS fallback in
`bronze_verify_financial.py` above scale 5 (Iceberg `add_files` cannot
zero-copy-register the pacs.008 source once it exceeds the size/file
thresholds), which rewrites the full source through an Iceberg CTAS and
spills roughly twice the per-executor input to local disk. The c360
profile is a thin `add_files` register and never sees that spill. See
LB-118 and `_SCHEMA_PROFILE_OVERRIDES` in
`modules/pipeline_engines/spark/job.py`.

Per-executor sizing (cores, memory, overhead, PVC) is fixed. What scales with
data is the **executor count**. Executor count is derived automatically from
the scale factor using a linear formula:

- At scale <= 10: uses a base count (4 for bronze, 8 for silver, 4 for gold)
- Above scale 10: adds executors linearly (e.g., silver adds 12 per 100 scale units)
- Each job has a maximum executor cap: 20 for c360 bronze, 28 for silver / gold
  and for financial bronze (financial also bumps `executors_per_100_scale`
  from 4 to 8 so per-executor load halves at scale 100+).

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

The architecture compositions lakebench accepts are the recipes below. Any
other combination is refused at config load with the reason. A recipe being
listed says the composition is valid, not that it is release-validated for a
workload: support is judged per workload x recipe x mode (see
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

Lakebench uses S3-compatible object storage for all data. Three buckets
correspond to the three medallion layers:

| Bucket | Purpose |
|---|---|
| `<name>-bronze` | Raw Parquet files from datagen |
| `<name>-silver` | Cleaned Iceberg table |
| `<name>-gold` | Aggregated KPI Iceberg table |

`<name>` is the deployment `name`. Bucket names are configurable under
`platform.storage.s3.buckets`. Path-style access is enabled by default for
compatibility with S3-compatible stores (FlashBlade, MinIO) that do not support
virtual-hosted bucket addressing.

All S3 access uses the `S3AFileSystem` Hadoop connector with tuned settings
for high throughput: fast upload with byte-buffer mode, 200 max connections,
256 MB multipart threshold, and 256 MB block size.

## Deployment Order

The deployment engine creates resources in a strict dependency order:

1. **Namespace** -- creates the target namespace if it does not exist
2. **Secrets** -- S3 credentials and PostgreSQL credentials
3. **Silver-state ConfigMap** -- per-deployment rebuild-epoch counters and
   the bronze data clock that bronze-verify records for silver
4. **S3 buckets** -- creates the deployment's buckets
5. **Scratch StorageClass check** -- verifies the scratch class exists (if
   scratch is enabled); it never creates it. A cluster admin installs it once
   with `lakebench admin install --component scratch-storage-class`
6. **PostgreSQL** -- StatefulSet with persistent volume
7. **Hive Metastore** -- skipped unless the catalog is Hive
8. **Polaris** -- skipped unless the catalog is Polaris
9. **Spark RBAC** -- ServiceAccount, Role, RoleBinding (plus SCC on OpenShift)
10. **Unity Catalog** -- skipped unless the catalog is Unity (not a supported
    combination)
11. **Spark Operator** -- verifies the shared operator (deploy never installs
    it; `lakebench admin install --component spark-operator` does) and adds
    the namespace to its watch list under the cluster lease
12. **Dependency server** -- the `lb-deps` Deployment, Service and PVC in
    the namespace: resolves the jars and wheels once per request and serves
    them read-only; deploy waits until it is Ready and records the set
13. **Trino** -- coordinator Deployment + worker StatefulSet (if selected)
14. **Spark Thrift Server** -- if selected
15. **DuckDB** -- if selected
16. **Observability** -- checks the shared Prometheus and Grafana release
    (installed by `lakebench admin install --component observability`) and
    applies the deployment's PodMonitors and Pushgateway (if enabled)

Destruction follows the reverse order: an ownership check, Spark jobs and pods
first, then table removal from the catalog (metadata only, no table
maintenance), emptying the S3
buckets and deleting the ones this deployment created, infrastructure removal,
and finally namespace deletion. The namespace is kept when a recorded bucket
could not be deleted, and destroy waits for it to be NotFound before reporting
it deleted.
