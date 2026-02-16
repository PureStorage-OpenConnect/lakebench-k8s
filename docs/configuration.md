# Configuration Reference

Lakebench uses a single YAML file to describe the entire deployment: platform
resources, data architecture, workload parameters, and observability settings.
The configuration is validated at load time by Pydantic v2. Any invalid field
or unsupported combination produces a clear error before anything touches your
cluster.

The default config path is `./lakebench.yaml`. All commands accept an explicit
path as the first positional argument:

```bash
lakebench deploy my-config.yaml
lakebench run my-config.yaml
```

## Minimum Viable Config

The smallest working config requires only a name, an S3 endpoint, and S3
credentials. Everything else has working defaults (Hive + Iceberg + Trino,
scale 1, Spark operator auto-installed).

```yaml
name: my-lakehouse
platform:
  storage:
    s3:
      endpoint: http://your-s3-endpoint:80
      access_key: YOUR_KEY
      secret_key: YOUR_SECRET
```

When you omit optional sections, these defaults apply:

| Section | Default | Effect |
|---------|---------|--------|
| `recipe` | (none) | hive + iceberg + trino |
| `datagen.scale` | 10 | ~100 GB bronze data |
| `catalog.type` | hive | Stackable Hive Metastore |
| `query_engine.type` | trino | Trino coordinator + 2 workers |
| `spark.operator.install` | false | Must opt in to auto-install Spark Operator |
| `observability.enabled` | false | No Prometheus/Grafana |
| `scratch.enabled` | false | emptyDir for shuffle |

**What to tune first as you scale up:**

1. `datagen.scale` -- controls data volume
2. `compute.spark.executor` -- match instances/memory to your cluster
3. `query_engine.trino.worker` -- match replicas/memory to your cluster
4. `scratch` / `postgres` storage classes -- match to your storage provider

## Annotated Example

Below is a complete configuration with every section annotated. Only `name`
and `platform.storage.s3.endpoint` plus S3 credentials are strictly required;
everything else has sensible defaults.

```yaml
# REQUIRED: Unique name for this deployment. Also used as the default
# Kubernetes namespace if platform.kubernetes.namespace is empty.
name: my-lakehouse

# Optional human-readable description.
description: "Production benchmark at scale 100"

# Config schema version (always 1 for now).
version: 1

# Optional recipe shorthand. Sets catalog, table_format, and query_engine
# in one line. User overrides in architecture: always take precedence.
# recipe: hive-iceberg-trino

# ---------------------------------------------------------------------------
# IMAGES
# ---------------------------------------------------------------------------
# Container images for every component. Override these for air-gapped
# registries or custom builds.
images:
  datagen: docker.io/sillidata/lb-datagen:v2
  spark: apache/spark:3.5.4-python3
  postgres: postgres:17
  hive: apache/hive:3.1.3
  polaris: apache/polaris:1.3.0-incubating
  trino: trinodb/trino:479
  prometheus: prom/prometheus:v2.48.0
  grafana: grafana/grafana:10.2.0
  pull_policy: IfNotPresent           # Always | IfNotPresent | Never
  pull_secrets: []                    # List of imagePullSecret names

# ---------------------------------------------------------------------------
# LAYER 1: PLATFORM
# ---------------------------------------------------------------------------
platform:
  kubernetes:
    context: ""                       # Empty = current kubectl context
    namespace: ""                     # Empty = use deployment name
    create_namespace: true

  storage:
    s3:
      # REQUIRED: S3-compatible endpoint URL.
      # FlashBlade:  http://10.0.0.1:80
      # MinIO:       http://minio.minio.svc:9000
      # AWS S3:      https://s3.us-east-1.amazonaws.com
      endpoint: ""

      region: us-east-1
      path_style: true                # true for FlashBlade/MinIO, false for AWS

      # Credentials: provide inline OR reference an existing K8s Secret.
      access_key: ""
      secret_key: ""
      # secret_ref: "my-existing-secret"

      buckets:
        bronze: lakebench-bronze
        silver: lakebench-silver
        gold: lakebench-gold
      create_buckets: true

    scratch:
      enabled: false                  # Enable Portworx scratch StorageClass
      storage_class: px-csi-scratch
      size: 100Gi
      create_storage_class: true

  compute:
    spark:
      operator:
        install: true
        namespace: spark-operator
        version: "2.4.0"

      driver:
        cores: 4
        memory: 8g

      executor:
        instances: 8
        cores: 4
        memory: 48g
        memory_overhead: 12g

      # Per-job executor count overrides (null = auto from scale factor).
      # Per-executor sizing (cores, memory, PVC) is fixed from proven profiles.
      bronze_executors: null
      silver_executors: null
      gold_executors: null

      # Streaming job overrides (continuous mode).
      bronze_ingest_executors: null
      silver_stream_executors: null
      gold_refresh_executors: null

      # Global driver overrides (null = profile default).
      driver_memory: null
      driver_cores: null

    postgres:
      storage: 10Gi
      storage_class: ""               # Empty = cluster default

# ---------------------------------------------------------------------------
# LAYER 2: DATA ARCHITECTURE
# ---------------------------------------------------------------------------
architecture:
  catalog:
    type: hive                        # hive | polaris | none

  table_format:
    type: iceberg                     # Only iceberg is currently supported

  query_engine:
    type: trino                       # trino | spark-thrift | none
    trino:
      coordinator:
        cpu: "2"
        memory: 8Gi
      worker:
        replicas: 2
        cpu: "4"
        memory: 16Gi
        spill_enabled: true
        spill_max_per_node: 40Gi
        storage: 50Gi
        storage_class: ""
      catalog_name: lakehouse

  pipeline:
    pattern: medallion                # medallion | streaming | batch

    # Continuous mode tuning (used with `lakebench run --continuous`).
    continuous:
      bronze_trigger_interval: "30 seconds"
      silver_trigger_interval: "60 seconds"
      gold_refresh_interval: "5 minutes"
      run_duration: 1800              # Seconds (30 min default)
      max_files_per_trigger: 50
      checkpoint_base: checkpoints

  workload:
    schema: customer360               # customer360 | iot | financial
    datagen:
      scale: 10                       # 1 unit ~ 10 GB bronze
      mode: auto                      # auto | batch | continuous
      parallelism: 4
      file_size: 512mb
      dirty_data_ratio: 0.08

  benchmark:
    mode: power                       # power | standard | extended
    streams: 4
    cache: hot                        # hot | cold
    iterations: 1

  tables:
    bronze: "default.bronze_raw"
    silver: "silver.customer_interactions_enriched"
    gold: "gold.customer_executive_dashboard"

# ---------------------------------------------------------------------------
# LAYER 3: OBSERVABILITY
# ---------------------------------------------------------------------------
observability:
  metrics:
    local:
      output_dir: ./lakebench-output/runs
    prometheus:
      enabled: false
      deploy: false
      retention: 7d
      storage: 10Gi
    collect:
      spark_metrics: true
      trino_metrics: true
      s3_metrics: true
      kubernetes_metrics: true

  dashboards:
    grafana:
      enabled: false
      deploy: false
      dashboards:
        - spark-jobs
        - trino-queries
        - storage-throughput
        - cluster-resources

  reports:
    enabled: true
    output_dir: ./lakebench-output/runs
    format: html                      # html | json | both

# ---------------------------------------------------------------------------
# SPARK CONFIGURATION OVERRIDES
# ---------------------------------------------------------------------------
# S3A and shuffle settings proven at 1 TB+ in production.
spark:
  conf:
    spark.hadoop.fs.s3a.connection.maximum: "500"
    spark.hadoop.fs.s3a.threads.max: "200"
    spark.hadoop.fs.s3a.fast.upload: "true"
    spark.hadoop.fs.s3a.multipart.size: "268435456"
    spark.hadoop.fs.s3a.fast.upload.active.blocks: "16"
    spark.sql.shuffle.partitions: "200"
    spark.default.parallelism: "200"
    spark.memory.fraction: "0.8"
    spark.memory.storageFraction: "0.3"
```

## Complete Field Reference

Every field accepted in the YAML is listed below, organized by section.
Fields marked **(required)** must be provided; everything else has a default.

### Root

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | **(required)** | Unique deployment name. Also used as the K8s namespace when `namespace` is empty. |
| `description` | string | `""` | Optional human-readable description. |
| `version` | int | `1` | Config schema version. Always `1`. |
| `recipe` | string or null | `null` | Recipe shorthand (e.g., `hive-iceberg-trino`). Sets catalog, table format, and query engine defaults. See [Recipes](recipes.md). |

### Images

Container images for every deployed component. Override for air-gapped
registries or custom builds.

| Field | Type | Default | Description |
|---|---|---|---|
| `images.datagen` | string | `docker.io/sillidata/lb-datagen:v2` | Data generator image. |
| `images.spark` | string | `apache/spark:3.5.4-python3` | Spark runtime image. Spark 4.x images are auto-detected. |
| `images.postgres` | string | `postgres:17` | PostgreSQL image (metadata backend). |
| `images.hive` | string | `apache/hive:3.1.3` | Hive Metastore image (Stackable operator). |
| `images.polaris` | string | `apache/polaris:1.3.0-incubating` | Apache Polaris REST catalog image. |
| `images.trino` | string | `trinodb/trino:479` | Trino query engine image. |
| `images.prometheus` | string | `prom/prometheus:v2.48.0` | Prometheus image. |
| `images.grafana` | string | `grafana/grafana:10.2.0` | Grafana image. |
| `images.pull_policy` | enum | `IfNotPresent` | `Always`, `IfNotPresent`, or `Never`. |
| `images.pull_secrets` | list | `[]` | List of Kubernetes `imagePullSecret` names. |

### Platform -- Kubernetes

| Field | Type | Default | Description |
|---|---|---|---|
| `platform.kubernetes.context` | string | `""` | kubectl context name from `~/.kube/config`. Empty = use the currently active context (`kubectl config current-context`). Set this to target a specific cluster when you have multiple contexts configured. |
| `platform.kubernetes.namespace` | string | `""` | Kubernetes namespace for all resources. Empty = use the deployment `name`. |
| `platform.kubernetes.create_namespace` | bool | `true` | Create the namespace if it does not exist. |

### Platform -- S3 Storage

| Field | Type | Default | Description |
|---|---|---|---|
| `platform.storage.s3.endpoint` | string | **(required)** | S3-compatible endpoint URL (e.g., `http://minio:9000`). |
| `platform.storage.s3.region` | string | `us-east-1` | AWS region. Used by boto3 for signing. |
| `platform.storage.s3.path_style` | bool | `true` | Path-style access (`true` for FlashBlade/MinIO, `false` for AWS S3). |
| `platform.storage.s3.access_key` | string | `""` | Inline S3 access key. Provide this OR `secret_ref`. |
| `platform.storage.s3.secret_key` | string | `""` | Inline S3 secret key. Provide this OR `secret_ref`. |
| `platform.storage.s3.secret_ref` | string | `""` | Name of an existing K8s Secret containing S3 credentials. Alternative to inline keys. |
| `platform.storage.s3.buckets.bronze` | string | `lakebench-bronze` | Bronze layer S3 bucket name. |
| `platform.storage.s3.buckets.silver` | string | `lakebench-silver` | Silver layer S3 bucket name. |
| `platform.storage.s3.buckets.gold` | string | `lakebench-gold` | Gold layer S3 bucket name. |
| `platform.storage.s3.create_buckets` | bool | `true` | Create buckets if they do not exist. |

### Platform -- Scratch Storage

Scratch PVCs for Spark shuffle data. Only needed with Portworx or similar CSI.

| Field | Type | Default | Description |
|---|---|---|---|
| `platform.storage.scratch.enabled` | bool | `false` | Enable scratch StorageClass for Spark PVCs. |
| `platform.storage.scratch.storage_class` | string | `px-csi-scratch` | StorageClass name for scratch volumes. |
| `platform.storage.scratch.size` | string | `100Gi` | Default scratch PVC size. |
| `platform.storage.scratch.create_storage_class` | bool | `true` | Create the StorageClass if it does not exist (requires cluster-admin). Set to `false` if the SC already exists or is managed externally. |
| `platform.storage.scratch.provisioner` | string | `pxd.portworx.com` | CSI provisioner for the StorageClass. Use `rancher.io/local-path`, `ebs.csi.aws.com`, etc. for non-Portworx providers. |
| `platform.storage.scratch.parameters` | dict | `{"repl": "1", ...}` | Provider-specific StorageClass parameters. |

### Platform -- Spark Compute

| Field | Type | Default | Description |
|---|---|---|---|
| `platform.compute.spark.operator.install` | bool | `false` | Install the Kubeflow Spark Operator via Helm. Requires cluster-admin. |
| `platform.compute.spark.operator.namespace` | string | `spark-operator` | Namespace for the Spark Operator. |
| `platform.compute.spark.operator.version` | string | `2.4.0` | Spark Operator chart version. v2.x required. |
| `platform.compute.spark.driver.cores` | int | `4` | Spark driver CPU cores. |
| `platform.compute.spark.driver.memory` | string | `8g` | Spark driver memory. |
| `platform.compute.spark.executor.instances` | int | `8` | Default executor count (overridden by per-job settings). |
| `platform.compute.spark.executor.cores` | int | `4` | Executor CPU cores. |
| `platform.compute.spark.executor.memory` | string | `48g` | Executor memory. |
| `platform.compute.spark.executor.memory_overhead` | string | `12g` | Executor memory overhead (off-heap). |
| `platform.compute.spark.bronze_executors` | int or null | `null` | Override bronze-verify executor count. Null = auto from scale. |
| `platform.compute.spark.silver_executors` | int or null | `null` | Override silver-build executor count. Null = auto from scale. |
| `platform.compute.spark.gold_executors` | int or null | `null` | Override gold-finalize executor count. Null = auto from scale. |
| `platform.compute.spark.bronze_ingest_executors` | int or null | `null` | Override bronze-ingest (streaming) executor count. |
| `platform.compute.spark.silver_stream_executors` | int or null | `null` | Override silver-stream (streaming) executor count. |
| `platform.compute.spark.gold_refresh_executors` | int or null | `null` | Override gold-refresh (streaming) executor count. |
| `platform.compute.spark.driver_memory` | string or null | `null` | Global driver memory override (e.g., `16g`). Null = profile default. |
| `platform.compute.spark.driver_cores` | int or null | `null` | Global driver cores override. Null = profile default. |

### Platform -- PostgreSQL

| Field | Type | Default | Description |
|---|---|---|---|
| `platform.compute.postgres.storage` | string | `10Gi` | PVC size for PostgreSQL data. |
| `platform.compute.postgres.storage_class` | string | `""` | StorageClass for PostgreSQL PVC. Empty = cluster default. |

### Architecture -- Catalog

| Field | Type | Default | Description |
|---|---|---|---|
| `architecture.catalog.type` | enum | `hive` | Catalog service: `hive`, `polaris`, or `none`. |
| `architecture.catalog.hive.thrift.min_threads` | int | `10` | Hive Metastore thrift server minimum threads. |
| `architecture.catalog.hive.thrift.max_threads` | int | `50` | Hive Metastore thrift server maximum threads. |
| `architecture.catalog.hive.thrift.client_timeout` | string | `300s` | Hive Metastore client timeout. |
| `architecture.catalog.hive.resources.cpu_min` | string | `500m` | Hive Metastore minimum CPU request. |
| `architecture.catalog.hive.resources.cpu_max` | string | `2` | Hive Metastore CPU limit. |
| `architecture.catalog.hive.resources.memory` | string | `4Gi` | Hive Metastore memory. |
| `architecture.catalog.polaris.version` | string | `1.3.0-incubating` | Polaris version. Minimum 1.3.0-incubating. |
| `architecture.catalog.polaris.port` | int | `8181` | Polaris REST API port. |
| `architecture.catalog.polaris.resources.cpu` | string | `1` | Polaris CPU request/limit. |
| `architecture.catalog.polaris.resources.memory` | string | `2Gi` | Polaris memory. |

### Architecture -- Table Format

| Field | Type | Default | Description |
|---|---|---|---|
| `architecture.table_format.type` | enum | `iceberg` | Table format: `iceberg`, `delta`, or `hudi`. |
| `architecture.table_format.iceberg.version` | string | `1.10.1` | Apache Iceberg runtime JAR version. |
| `architecture.table_format.iceberg.file_format` | enum | `parquet` | Underlying file format: `parquet`, `orc`, or `avro`. |
| `architecture.table_format.iceberg.properties` | dict | `{}` | Additional Iceberg table properties (key-value pairs). |
| `architecture.table_format.delta.version` | string | `3.0.0` | Delta Lake version. |
| `architecture.table_format.delta.properties` | dict | `{}` | Additional Delta table properties. |

### Architecture -- Query Engine

| Field | Type | Default | Description |
|---|---|---|---|
| `architecture.query_engine.type` | enum | `trino` | Query engine: `trino`, `spark-thrift`, or `none`. |
| `architecture.query_engine.trino.coordinator.cpu` | string | `2` | Trino coordinator CPU. |
| `architecture.query_engine.trino.coordinator.memory` | string | `8Gi` | Trino coordinator memory. |
| `architecture.query_engine.trino.worker.replicas` | int | `2` | Number of Trino worker pods. |
| `architecture.query_engine.trino.worker.cpu` | string | `4` | Trino worker CPU. |
| `architecture.query_engine.trino.worker.memory` | string | `16Gi` | Trino worker memory. |
| `architecture.query_engine.trino.worker.spill_enabled` | bool | `true` | Enable query spill to disk. |
| `architecture.query_engine.trino.worker.spill_max_per_node` | string | `40Gi` | Maximum spill size per worker. |
| `architecture.query_engine.trino.worker.storage` | string | `50Gi` | Worker PVC size for spill and temp data. |
| `architecture.query_engine.trino.worker.storage_class` | string | `""` | Worker PVC StorageClass. Empty = cluster default. |
| `architecture.query_engine.trino.catalog_name` | string | `lakehouse` | Trino catalog name for the Iceberg connector. |
| `architecture.query_engine.spark_thrift.cores` | int | `2` | Spark Thrift Server CPU cores. |
| `architecture.query_engine.spark_thrift.memory` | string | `4g` | Spark Thrift Server memory. |
| `architecture.query_engine.spark_thrift.catalog_name` | string | `lakehouse` | Iceberg catalog name for Spark Thrift Server. |

### Architecture -- Pipeline

> The legacy name `processing` is still accepted with a deprecation warning.

| Field | Type | Default | Description |
|---|---|---|---|
| `architecture.pipeline.pattern` | enum | `medallion` | Pipeline pattern: `medallion`, `streaming`, `batch`, or `custom`. |
| `architecture.pipeline.continuous.bronze_trigger_interval` | string | `30 seconds` | Bronze streaming trigger interval. |
| `architecture.pipeline.continuous.silver_trigger_interval` | string | `60 seconds` | Silver streaming trigger interval. |
| `architecture.pipeline.continuous.gold_refresh_interval` | string | `5 minutes` | Gold refresh trigger interval. |
| `architecture.pipeline.continuous.run_duration` | int | `1800` | Streaming run duration in seconds. Minimum 60. |
| `architecture.pipeline.continuous.max_files_per_trigger` | int | `50` | Max Parquet files per micro-batch. Primary throughput cap. |
| `architecture.pipeline.continuous.checkpoint_base` | string | `checkpoints` | S3 prefix for streaming checkpoints. |

### Architecture -- Workload & Datagen

| Field | Type | Default | Description |
|---|---|---|---|
| `architecture.workload.schema` | enum | `customer360` | Workload schema: `customer360`, `iot`, or `financial`. |
| `architecture.workload.datagen.scale` | int | `10` | Scale factor (1 unit ~ 10 GB bronze). Range: 1--10000. |
| `architecture.workload.datagen.target_size` | string or null | `null` | **Deprecated.** Legacy size string (e.g., `100gb`). Converted to scale automatically. |
| `architecture.workload.datagen.mode` | enum | `auto` | Datagen mode: `auto`, `batch`, or `continuous`. Auto selects based on scale. |
| `architecture.workload.datagen.parallelism` | int | `4` | Number of parallel datagen pods. |
| `architecture.workload.datagen.file_size` | string | `512mb` | Target Parquet file size. |
| `architecture.workload.datagen.dirty_data_ratio` | float | `0.08` | Fraction of intentionally dirty records (0.0--1.0). |
| `architecture.workload.datagen.cpu` | string | `2` | CPU per datagen pod. **Hard-locked by mode** (batch=4, continuous=8). |
| `architecture.workload.datagen.memory` | string | `4Gi` | Memory per datagen pod. **Hard-locked by mode** (batch=4Gi, continuous=24Gi). |
| `architecture.workload.datagen.generators` | int | `0` | Generator processes per pod. 0 = auto (1 batch, 8 continuous). |
| `architecture.workload.datagen.uploaders` | int | `0` | Uploader threads per pod. 0 = auto (1 batch, 2 continuous). |
| `architecture.workload.datagen.checkpoint.enabled` | bool | `true` | Enable datagen checkpoint for resume. |
| `architecture.workload.datagen.checkpoint.path` | string | `.lakebench_checkpoint.json` | Checkpoint file path. |
| `architecture.workload.datagen.timestamp_start` | string or null | `null` | Start date for generated timestamps (ISO format). Default: `2024-01-01`. |
| `architecture.workload.datagen.timestamp_end` | string or null | `null` | End date for generated timestamps (ISO format). Default: `2025-12-31`. |

### Architecture -- Customer360 Overrides

Advanced overrides for the Customer360 workload schema. Most users should
leave these at defaults and control volume via `datagen.scale`.

| Field | Type | Default | Description |
|---|---|---|---|
| `architecture.workload.customer360.unique_customers` | int or null | `null` | Override customer count. Null = derived from scale. |
| `architecture.workload.customer360.date_range_days` | int or null | `null` | Override date range in days. Null = 365. |
| `architecture.workload.customer360.channels` | list | `[web, mobile, store, call_center, social_media]` | Interaction channels. |
| `architecture.workload.customer360.event_types` | list | `[purchase, browse, support, login, abandoned_cart]` | Event types. |
| `architecture.workload.customer360.quality_distribution.clean` | float | `0.92` | Fraction of clean records. |
| `architecture.workload.customer360.quality_distribution.duplicate_suspected` | float | `0.02` | Fraction of suspected duplicates. |
| `architecture.workload.customer360.quality_distribution.incomplete` | float | `0.03` | Fraction of incomplete records. |
| `architecture.workload.customer360.quality_distribution.format_inconsistent` | float | `0.03` | Fraction of format-inconsistent records. |

Note: quality distribution values must sum to 1.0.

### Architecture -- Benchmark

| Field | Type | Default | Description |
|---|---|---|---|
| `architecture.benchmark.mode` | enum | `power` | Benchmark mode: `power`, `standard`, `extended`, `throughput`, or `composite`. |
| `architecture.benchmark.streams` | int | `4` | Concurrent query streams for throughput mode. Range: 1--64. |
| `architecture.benchmark.cache` | enum | `hot` | Cache mode: `hot` (warm cache) or `cold` (cleared before each query). |
| `architecture.benchmark.iterations` | int | `1` | Iterations per query. >1 uses median timing. Range: 1--100. |

### Architecture -- Table Names

Fully-qualified Iceberg table names (`namespace.table`). The catalog prefix is
added at runtime.

| Field | Type | Default | Description |
|---|---|---|---|
| `architecture.tables.bronze` | string | `default.bronze_raw` | Bronze table name. |
| `architecture.tables.silver` | string | `silver.customer_interactions_enriched` | Silver table name. |
| `architecture.tables.gold` | string | `gold.customer_executive_dashboard` | Gold table name. |

### Observability

| Field | Type | Default | Description |
|---|---|---|---|
| `observability.enabled` | bool | `false` | Deploy the observability stack (Prometheus + Grafana). |
| `observability.prometheus_stack_enabled` | bool | `true` | Deploy kube-prometheus-stack when observability is enabled. |
| `observability.s3_metrics_enabled` | bool | `true` | Collect S3 operation metrics from CLI-side boto3 calls. |
| `observability.spark_metrics_enabled` | bool | `true` | Collect Spark job metrics via Prometheus servlet. |
| `observability.dashboards_enabled` | bool | `true` | Deploy Grafana dashboards. |
| `observability.retention` | string | `7d` | Prometheus data retention period. |
| `observability.storage` | string | `10Gi` | Prometheus PVC size. |
| `observability.storage_class` | string | `""` | Prometheus PVC StorageClass. Empty = cluster default. |

### Observability -- Reports

| Field | Type | Default | Description |
|---|---|---|---|
| `observability.reports.enabled` | bool | `true` | Generate benchmark reports after runs. |
| `observability.reports.output_dir` | string | `./lakebench-output/runs` | Report output directory. |
| `observability.reports.format` | enum | `html` | Report format: `html`, `json`, or `both`. |
| `observability.reports.include.summary` | bool | `true` | Include pipeline summary in report. |
| `observability.reports.include.stage_breakdown` | bool | `true` | Include per-stage breakdown. |
| `observability.reports.include.storage_metrics` | bool | `true` | Include storage throughput metrics. |
| `observability.reports.include.resource_utilization` | bool | `true` | Include resource utilization data. |
| `observability.reports.include.recommendations` | bool | `true` | Include sizing recommendations. |

### Spark Configuration Overrides

The `spark.conf` section accepts arbitrary Spark configuration key-value pairs.
These are passed directly to the SparkApplication manifest. The defaults below
are proven at 1 TB+ scale.

| Field | Type | Default | Description |
|---|---|---|---|
| `spark.conf` | dict | see annotated example | Map of Spark config keys to values. Any valid `spark.*` property is accepted. |

Common keys and their defaults:

| Key | Default | Description |
|---|---|---|
| `spark.hadoop.fs.s3a.connection.maximum` | `500` | Max S3 connections. |
| `spark.hadoop.fs.s3a.threads.max` | `200` | Max S3 threads. |
| `spark.hadoop.fs.s3a.fast.upload` | `true` | Enable fast multipart upload. |
| `spark.hadoop.fs.s3a.multipart.size` | `268435456` | Multipart upload part size (256 MB). |
| `spark.sql.shuffle.partitions` | `200` | Shuffle partition count. |
| `spark.default.parallelism` | `200` | Default RDD parallelism. |
| `spark.memory.fraction` | `0.8` | Fraction of heap for execution + storage. |
| `spark.memory.storageFraction` | `0.3` | Fraction of `memory.fraction` for storage. |

## Scale Factors

The `datagen.scale` field is an abstract multiplier. One scale unit produces
approximately 10 GB of on-disk bronze Parquet data. The table below shows the
Customer360 workload schema mapping (the default):

| Scale | Customers | Approximate Rows | Bronze Size |
|---|---|---|---|
| 1 | 100,000 | 2.4 M | ~10 GB |
| 10 | 1,000,000 | 24 M | ~100 GB |
| 100 | 10,000,000 | 240 M | ~1 TB |
| 1000 | 100,000,000 | 2.4 B | ~10 TB |

Each customer generates approximately 24 events across a 365-day date range.
Scaling is linear: doubling the scale factor doubles customers, rows, and data
volume.

Executor counts auto-scale with the scale factor unless overridden by the
`bronze_executors`, `silver_executors`, or `gold_executors` fields. Per-executor
sizing (cores, memory, PVC size) is fixed from proven production profiles and
does not change with scale.

## Auto-Sizing

When connected to a Kubernetes cluster, Lakebench auto-sizes compute resources
to fit available capacity. This happens transparently during `deploy`, `info`,
and `validate`.

The algorithm:

1. **Always-on pods** (Trino coordinator + workers, Hive/Polaris, PostgreSQL)
   are sized from tier guidance and capped to fit the cluster. They are never
   boosted beyond the tier recommendation.
2. **Datagen and Spark** share the remaining CPU budget.
   - In **batch mode** (default), they run sequentially -- each gets the full
     remaining budget.
   - In **streaming mode** (`--continuous`), they run concurrently -- the budget
     is split 40% datagen, 60% Spark.
3. **Small scales (1--50):** Resources are only capped downward to fit.
4. **Large scales (51+):** Executor counts are scaled up to use available
   cluster capacity.
5. **Per-pod memory** is capped to 85% of the largest node.

Use `lakebench info <config>` to see the resolved executor counts after
auto-sizing. Override any auto-sized value with explicit settings in the config
(e.g., `spark.bronze_executors: 4`).

## Supported Component Combinations

Not every catalog, table format, and query engine combination is valid.
Lakebench validates at config load and rejects unsupported combinations with a
clear error message.

| Catalog | Table Format | Query Engine | Supported |
|---|---|---|---|
| hive | iceberg | trino | Yes |
| hive | iceberg | spark-thrift | Yes |
| hive | iceberg | none | Yes |
| polaris | iceberg | trino | Yes |
| polaris | iceberg | spark-thrift | Yes |
| polaris | iceberg | none | Yes |
| hive | delta | trino | Yes |
| hive | delta | none | Yes |

Polaris is an Apache Iceberg REST catalog. It does not support Delta Lake.

## Image Overrides

All container images are configurable under the `images` section. This is
useful for air-gapped environments or when running custom builds:

```yaml
images:
  datagen: my-registry.internal/lakebench/datagen:v2
  spark: my-registry.internal/apache/spark:3.5.4-python3
  trino: my-registry.internal/trinodb/trino:479
  pull_policy: Always
  pull_secrets:
    - my-registry-pull-secret
```

Set `pull_policy: Always` after pushing a new image tag to ensure Kubernetes
pulls the latest version.

## Generating a Starter Config

Use `lakebench init` to generate a starter configuration file interactively:

```bash
lakebench init my-config.yaml
```

This creates a well-commented YAML file with all defaults that you can
customize for your environment.
