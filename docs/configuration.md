# Configuration Reference

Reference: every config key, its default, and how a config loads.

- One YAML file describes the whole deployment: platform resources, data
  architecture, workload and observability.
- Pydantic v2 validates it at load. An invalid field or an unsupported
  combination fails with an error before anything touches the cluster.
- The default path is `./lakebench.yaml`. Every command takes an explicit
  path as its first positional argument:

```bash
lakebench deploy my-config.yaml
lakebench run my-config.yaml
```

## Minimum viable config

```yaml
name: my-lakehouse
recipe: hive-iceberg-spark-trino
platform:
  storage:
    s3:
      endpoint: http://your-s3-endpoint:80
      access_key: "${LAKEBENCH_S3_ACCESS_KEY}"
      secret_key: "${LAKEBENCH_S3_SECRET_KEY}"
workload:
  datagen:
    scale: 10
```

- Export the two variables before you run a command:
  `export LAKEBENCH_S3_ACCESS_KEY=... LAKEBENCH_S3_SECRET_KEY=...`.
  `lakebench init` writes the same references.
- Without `recipe:` the config resolves to `hive-iceberg-spark-trino` and
  loads with a deprecation note (see
  [Recipes and components](#recipes-and-components)).
- `name` is required by every command that changes data: `deploy`,
  `generate`, `run`, `benchmark`, `query`, `clean`, `financial`
  and `validate` refuse a config without one, and the error offers a name to
  add.
- There is no `config upgrade` command (removed in 1.7; see the CHANGELOG).

## Removed keys

A key an earlier release accepted that now does nothing (for example
`images.pull_secrets` or `platform.storage.scratch.create_storage_class`):

- is refused, with what to do instead, by the commands that change data
  (the list above);
- is dropped by `destroy`, `stop`, `status`, `logs`, `report`, `info`,
  `config show` and `admin`, which print an "Upgrade notes" block on
  stderr. An old config can still be inspected, stopped and torn down.

The removed keys, with what to do instead (generated from each model's
`_removed_keys`):

<!-- BEGIN GENERATED: config-removed (scripts/gen_config_reference.py) -->
| Removed key | Instead |
|---|---|
| `architecture.catalog.hive.thrift` | the HiveCluster template sets hive.metastore.server.min.threads 10, max.threads 50 and hive.metastore.client.socket.timeout 300s. |
| `architecture.catalog.polaris.version` | the Polaris that runs, and is recorded, is the tag of images.polaris. |
| `architecture.catalog.unity.version` | the Unity Catalog that runs is the tag of images.unity. |
| `architecture.pipeline.medallion` | nothing read the medallion block except bronze.path_template, and the Spark stages read a fixed bronze layout (customer/interactions/, pacs008/ for the financial workload): a custom bronze layout is not supported. |
| `architecture.table_format.delta.properties` | no table property is applied from the config. |
| `architecture.table_format.hudi` | Hudi is not a supported table format; removed in v1.2. |
| `architecture.table_format.iceberg.file_format` | Iceberg tables are always written as Parquet. |
| `architecture.table_format.iceberg.properties` | no table property is applied from the config. |
| `description` | nothing read it; keep notes in a YAML comment. |
| `images.grafana` | Grafana deploys from the kube-prometheus-stack chart; pin it with observability.chart_version. |
| `images.hive` | the Stackable HiveCluster always runs Hive 3.1.3 (Hive 4 breaks Iceberg and Trino ANALYZE), whatever images.hive named, and run provenance records 3.1.3. |
| `images.prometheus` | Prometheus deploys from the kube-prometheus-stack chart; pin it with observability.chart_version. |
| `images.pull_secrets` | No deployer ever applied it; removed in v1.5. |
| `observability.prometheus_stack_enabled` | observability.enabled always installs or reuses the full stack, Prometheus included. |
| `observability.reports` | every run writes report.html into its run directory under lakebench-output/runs; 'lakebench report --render' writes a fresh copy to lakebench-output/reports/ without overwriting. |
| `observability.s3_metrics_enabled` | PodMonitor deployment is not gated on it. |
| `observability.spark_metrics_enabled` | PodMonitor deployment is not gated on it. |
| `observability.storage_class` | the Prometheus volume claim is created without a storageClassName, so it uses the cluster default StorageClass. |
| `platform.compute.spark.driver` | per-executor sizing is fixed in the job profiles; use platform.compute.spark.<job>_executors for counts and driver_memory/driver_cores for the driver. |
| `platform.compute.spark.executor` | per-executor sizing is fixed in the job profiles; use platform.compute.spark.<job>_executors for counts and driver_memory/driver_cores for the driver. |
| `platform.storage.s3.secret_ref` | lakebench never reads an existing Secret: deploy writes the S3 Secret from access_key and secret_key. Set those instead (use ${VAR} substitution, for example access_key: "${S3_ACCESS_KEY}", to keep the keys out of the file). |
| `platform.storage.scratch.create_storage_class` | lakebench no longer creates StorageClasses; a cluster admin runs 'lakebench admin install --component scratch-storage-class' once. |
| `platform.storage.scratch.size` | per-job scratch comes from the job profiles (silver-build: 60 GiB x scale / executors, 50-300Gi). |
| `secret_ref` | lakebench never reads an existing Secret: deploy writes the S3 Secret from access_key and secret_key. Set those instead. |
| `version` | there is one config schema and nothing read the number; a removed key is named in the upgrade notes instead. |
| `workload.customer360.channels` | The customer360 generator never read it; removed in v1.5. |
| `workload.customer360.date_range_days` | the customer360 generator never read it: the event window is datagen.timestamp_start and datagen.timestamp_end. |
| `workload.customer360.event_types` | The customer360 generator never read it; removed in v1.5. |
| `workload.customer360.quality_distribution` | The customer360 generator never read it; removed in v1.5. |
| `workload.datagen.checkpoint` | The Rust generator never implemented checkpoint-resume; removed 2026-09-28. Re-run generation from the start on failure. |
| `workload.datagen.uploaders` | Never forwarded to the Rust generator; removed 2026-09-28. Uploader concurrency is fixed inside the S3 sink. |
<!-- END GENERATED: config-removed -->

- A removed key still at its old default is inert and loads under every
  command with a note. For `description` that is any text; for
  `images.hive`, any value naming 3.1.3. Another value is refused as above.
- The bronze layout is fixed (`customer/interactions/`, `pacs008/` for the
  financial workload), so a `path_template` naming another layout is
  refused.

## Flat Fields (deprecated)

Flat top-level fields map to nested keys. They still load, and each adds a
deprecation note naming the nested key to write instead (for example "flat
'scale' is deprecated; write workload.datagen.scale"). Write the nested key
in new configs.

| Field | Maps to | Default |
|-------|---------|---------|
| `endpoint` | `platform.storage.s3.endpoint` | (required) |
| `access_key` | `platform.storage.s3.access_key` | (required) |
| `secret_key` | `platform.storage.s3.secret_key` | (required) |
| `scale` | `workload.datagen.scale` | 10 |
| `namespace` | `platform.kubernetes.namespace` | same as name |
| `mode` | `architecture.pipeline.mode` | batch |
| `cycles` | `architecture.pipeline.cycles` | 1 |
| `spark_image` | `images.spark` | from recipe |

If a flat and a nested key set the same thing, the flat one wins and the
note says so.

## Error messages

- An unknown key names the key it is closest to: first among the keys of
  its own section, then among every key in the schema. A key in the wrong
  section is pointed at the right one:

```text
  - platform.compute.spark.silver_executor: unknown key; did you mean `silver_executors`?
  - platform.storage.scale: unknown key; did you mean `workload.datagen.scale`?
```

- An unknown recipe names the nearest recipe, or lists them all. A name
  whose every part is a real component (`unity-delta-spark-trino`) is
  reported as a combination that is not a recipe, not corrected to another
  one.
- Counts are bounded: for example `trino.worker.replicas` 1 to 256,
  `datagen.generators` 0 to 1024, ports 1 to 65535, every core count at
  least 1.
- A setting that belongs to the other workload loads with a note saying
  the workload does not read it. Examples: `customer360.unique_customers`
  or `dirty_data_ratio` under `schema: financial`; `tm_operations` or
  `w1_max_vertices` under `schema: customer360`.
- Under `financial` the note does not ask you to delete it: the corpus id
  still hashes the `customer360` fields and `dirty_data_ratio`, so removing
  one changes the id of an otherwise identical corpus.
- `datagen.timestamp_*` gets no note. The financial generator ignores it,
  but it sets the silver data clock for every workload.

## Environment Variable Substitution

Use `${VAR}` or `${VAR:-default}` in any YAML value:

```yaml
name: my-lakehouse
platform:
  storage:
    s3:
      endpoint: ${S3_ENDPOINT}
      access_key: "${S3_ACCESS_KEY}"
      secret_key: "${S3_SECRET_KEY}"
workload:
  datagen:
    scale: ${LAKEBENCH_SCALE:-10}
```

- Unresolved variables without defaults produce one error naming all of
  them.
- Substitution runs value by value, not on the file text.
- An unquoted value is trimmed and typed as YAML types it, unless it
  carries a tag such as `!!str`: `0042` is octal 34, `true` a bool, an empty
  value null.
- The environment value itself is never parsed as YAML: ` #`, quotes or
  `a: b` inside it stay text, and its line breaks are not folded.
- A quoted value (`"${S3_SECRET}"`) arrives verbatim as a string, unless it
  carries a tag such as `!!int`. Quote every credential reference.
- A block scalar keeps the substituted text inside its own line breaks.
- A `${VAR}` in a comment is not read.
- An unclosed `${VAR:-default` (a default cut short by ` #`) is an error.
- Inside flow syntax (`[${A}, ${B}]`) each reference must be quoted.

## Deploy state and nameless teardown

Every deploy records the nonce it stamps on the namespace in
`.lakebench/<name>.json` beside the config. A nameless config can read or
tear down only a deployment its directory proves is its own, and
`--name NAME` names one. The rules, the v1.6 `.lakebench/state.json` and
moving a deployment's directory are in
[UPGRADING-1.7.md](../UPGRADING-1.7.md#nameless-configs-and-deploy-state).

## Defaults for omitted sections

| Section | Default | Effect |
|---------|---------|--------|
| `recipe` | (none) | hive + iceberg + trino |
| `datagen.scale` | 10 | ~100 GB bronze data |
| `catalog.type` | hive | Stackable Hive Metastore |
| `query_engine.type` | trino | Trino coordinator + 2 workers |
| `observability.enabled` | false | No Prometheus/Grafana |
| `scratch.enabled` | unset ([when it is on](operations.md#installing-the-shared-pieces)) | emptyDir for shuffle when off |

What to tune first as you scale up:

1. `datagen.scale` -- the data volume.
2. `compute.spark.*_executors` -- per-job executor counts. Batch executor
   size is fixed in the job profiles; continuous streams also take
   `*_executor_cores`. See [Executor Override Guide](#executor-override-guide).
3. `query_engine.trino.worker` -- match replicas and memory to the cluster.
4. `scratch` and `postgres` storage classes -- match them to the storage
   provider.

## Annotated Example

A complete config with every section annotated. Only `name`,
`platform.storage.s3.endpoint` and the S3 credentials are required;
everything else has a default.

```yaml
# REQUIRED: Unique name for this deployment. Also used as the default
# Kubernetes namespace if platform.kubernetes.namespace is empty.
# The namespace is limited to 23 characters on a Hive recipe and 38 on any
# other: Stackable derives a metastore pod volume name,
# lakebench-s3-credentials-<namespace>-s3-credentials, that Kubernetes caps
# at 63. Config load refuses a longer one and names the derived object.
name: my-lakehouse

# Optional recipe shorthand. Sets catalog, table_format, and query_engine
# in one line. User overrides in architecture: always take precedence.
# recipe: hive-iceberg-spark-trino

# ---------------------------------------------------------------------------
# IMAGES
# ---------------------------------------------------------------------------
# Container images for every component. Override these for air-gapped
# registries or custom builds.
images:
  datagen: docker.io/sillidata/lb-datagen:5d7ce61a@sha256:ed4057e097f09fdd3e37631bc37eb88e5fce561cb8ebe06cd6fa2fd7d23e4bfc
  spark: apache/spark:4.1.1-python3       # the recipe's default; 4.0.2 on Polaris recipes
  postgres: postgres:17
  polaris: apache/polaris:1.6.0
  trino: trinodb/trino:483
  pull_policy: Always                 # Always | IfNotPresent | Never

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
      # FlashBlade HTTP:  http://10.0.1.50:80
      # FlashBlade HTTPS: https://10.0.1.50:443
      # MinIO:            http://minio.minio.svc:9000
      # AWS S3:           https://s3.us-east-1.amazonaws.com
      endpoint: ""

      region: us-east-1
      path_style: true                # true for FlashBlade/MinIO, false for AWS

      # Credentials (use ${VAR} substitution to keep them out of the file).
      access_key: ""
      secret_key: ""

      # TLS / HTTPS settings (for HTTPS S3 endpoints).
      # ca_cert: "/path/to/ca-bundle.pem"  # PEM CA cert for self-signed endpoints
      # verify_ssl: true                    # Set false to skip SSL verification (dev only)

      buckets:                        # default: <name>-bronze, <name>-silver, <name>-gold
        bronze: my-lakehouse-bronze
        silver: my-lakehouse-silver
        gold: my-lakehouse-gold
      create_buckets: true

    scratch:
      # enabled: true                 # Portworx scratch PVCs; unset, it turns on for a batch run at scale 50 and above
      storage_class: px-csi-scratch   # PVC size per executor: the job profile's

  compute:
    spark:
      operator:
        namespace: spark-operator
        version: "2.5.1"

      # Each job takes its driver and executor sizing from its built-in job
      # profile. The v1.6 driver and executor blocks are refused (they sized
      # nothing).

      # Per-job executor count overrides (null = auto from scale factor).
      # Batch per-executor sizing (cores, memory, PVC) is fixed from proven
      # profiles. Continuous streams take bronze_ingest_executor_cores,
      # silver_stream_executor_cores and gold_refresh_executor_cores.
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

  # Dependency server (lb-deps). deploy resolves the jars and wheels this
  # deployment needs onto a 5Gi PVC in its namespace and serves them there.
  # Every key is optional; the URL keys point the resolve at a mirror for
  # clusters without egress to the public repositories.
  deps:
    maven_repository: ""              # Empty = Maven Central, then its Google mirror
    pypi_index: ""                    # Empty = https://pypi.org/simple/
    duckdb_extension_repository: ""   # Empty = http://extensions.duckdb.org
    storage_class: ""                 # PVC lb-deps-data; empty = cluster default
    postgres:
      storage: 10Gi
      storage_class: ""               # Empty = cluster default

# ---------------------------------------------------------------------------
# LAYER 2: DATA ARCHITECTURE
# ---------------------------------------------------------------------------
architecture:
  catalog:
    type: hive                        # hive | polaris (unity and none have no supported recipe)

  table_format:
    type: iceberg                     # iceberg | delta (delta: hive catalog only)

  query_engine:
    type: trino                       # trino | spark-thrift | duckdb | none
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
        storage_class: ""                 # Empty = emptyDir. Set a class for PVC.
      catalog_name: lakehouse

  pipeline:
    mode: batch                       # batch | continuous

    # Iterative batch cycles. Runs N batch iterations where cycle 1
    # is full overwrite and cycles 2-N are incremental append/merge. Simulates
    # multi-day lakehouse table growth. Datagen timestamp range is split evenly
    # across cycles so each cycle adds new data.
    cycles: 1                         # 1 = single batch (default). 2-50 = multi-cycle.
    pre_benchmark_maintenance: true   # Compact + expire before benchmark (recommended)

    # Continuous pipeline tuning (active when mode: continuous or the --continuous flag).
    continuous:
      bronze_trigger_interval: "0 seconds"   # back to back (default)
      silver_trigger_interval: "0 seconds"   # back to back (default)
      gold_refresh_interval: "0 seconds"     # back to back (default)
      run_duration: 1800              # Seconds (30 min default)
      # max_files_per_trigger: unset = no limit (datagen generates for the whole window)
      checkpoint_base: checkpoints
      benchmark_interval: 300         # 300-3600; raised to a longer gold interval
      benchmark_warmup: 300           # 300-1800; raised to a longer gold interval

  benchmark:
    mode: power                       # power | standard | extended; throughput and composite
                                      # are `lakebench benchmark --mode` only (run refuses them)
    # streams: 4                      # `lakebench benchmark` throughput streams; run refuses > 1
    cache: hot                        # hot; cold is `lakebench benchmark --cold` only
    iterations: 3                     # timed runs per query; QpH uses the median

  tables:
    bronze: "default.bronze_raw"
    silver: "silver.customer_interactions_enriched"
    gold: "gold.customer_executive_dashboard"

workload:
  schema: customer360               # customer360 | financial
  datagen:
    scale: 10                       # 1 unit ~ 10 GB bronze
    mode: auto                      # auto | batch | continuous
    # parallelism: auto             # datagen pods; unset = sized from scale
    file_size: 64mb
    dirty_data_ratio: 0.08

# ---------------------------------------------------------------------------
# LAYER 3: OBSERVABILITY
# ---------------------------------------------------------------------------
observability:
  enabled: false                      # Deploy Prometheus + Grafana stack
  dashboards_enabled: true            # Deploy Grafana dashboards
  retention: 7d                       # Prometheus data retention
  storage: 10Gi                       # Prometheus PVC size

# ---------------------------------------------------------------------------
# SPARK CONFIGURATION OVERRIDES
# ---------------------------------------------------------------------------
# Your own Spark keys, merged over the job defaults. Keys Lakebench owns
# (partitions, catalog, S3A connection pool, jars, UI) are refused.
spark:
  conf: {}
    # spark.speculation: "true"
    # spark.memory.fraction: "0.8"     # a default; set to change it
```

### Executor Override Guide

A per-job executor override (`silver_executors`, `gold_executors` and the
rest) replaces the scaling formula for one job.

- Each takes 1 to 28, the Lakebench cap. A larger value is
  refused by the commands that change data; `destroy`, `status` and the
  read-only commands drop it with a note.
- The capacity check, `deploy`, `config show` and `info` count the
  override, so a config sized past the cluster is refused before it runs.

An override changes what a run measures:

- It enters the experiment record (`architecture.spark_executor_overrides`;
  driver overrides as `architecture.spark_driver_overrides`) and the
  identity, as an architecture difference. Two runs with different
  overrides differ in architecture. They are like-for-like only when no
  override binds one run and not the other.
- An override below what the profile asks for at the run's scale binds the
  run. It is labelled in `limits.bound` ("silver-build: executor override 4
  (profile asks 8)") and enters "Lakebench limits that bound".
- A run with any override that differs from the profile's count, or with a
  driver override, is not the proven sizing. It cannot be release evidence
  or a perf-gate baseline, and a pinned perf-gate config must pin each count
  at the profile's count (or leave it unset).

Driver memory:

- More executors raise driver memory pressure: the Spark driver manages
  per-executor K8s API watches and aggregates serialized task results.
- Profile driver defaults are 4g for bronze-verify, and 32g for
  silver-build and gold-finalize on Spark 4 (24g on Spark 3).
- `driver_memory` overrides all jobs at once, so a value below 32g shrinks
  the silver and gold drivers on Spark 4 (below 24g on Spark 3).
- Lakebench logs a warning when a job has more than 24 executors and a
  driver below 24g. It does not warn about 24g to 31g on Spark 4.
- The capacity check, `config show` and `info` count the driver each job
  requests: these overrides, the Spark 3 size, and the overhead Spark adds
  to a Python driver pod (40% of the heap).

Tested boundaries (the 24g rows are Spark 3; see the Spark 4 note below):

| Executors | Driver Memory | Status |
|:---------:|:------------:|--------|
| 1--12     | 4g           | Proven stable |
| 13--20    | 4--24g       | Stable; driver_memory override may be needed |
| 21--24    | 24g          | Proven stable with 24g driver |
| 25--28    | 24g          | Safe range; recommended maximum (the job profiles cap auto counts at 28, a Lakebench cap) |
| 29+       | --           | Refused: K8s API polling storms from 32 up |

On Spark 4, `silver-build` and `gold-finalize` default to a 32g driver: 24g
ran both out of memory there. Above 20 executors the 4g `bronze-verify`
driver may also run out of memory. `driver_memory` applies to every job, so
set it to 32g or more (for example `"32g"`).

`spark.driver.maxResultSize` follows the effective executor count:
`min(16, max(8, count // 2))` GiB on Spark 4 and
`min(16, max(4, count // 3))` GiB on Spark 3. Override it in `spark.conf`
if needed.

Recommended overrides by cluster size, with the per-executor sizes of the
[batch job profiles](component-spark.md#batch-jobs):

| Cluster Cores | Silver Executors | Gold Executors |
|:------------:|:----------------:|:--------------:|
| 32           | 4--6             | 3--4           |
| 64           | 8--12            | 6--8           |
| 128          | 16--20           | 12--16         |
| 256+         | 24--28           | 20--28         |

## Complete Field Reference

Every field the YAML accepts, by section. Fields marked **(required)** must
be set; every other field has a default. **first day** marks the keys
`lakebench init` writes; the rest are advanced. This reference is generated
from the schema (`scripts/gen_config_reference.py`): edit a field's
description in `src/lakebench/config/schema.py`, then regenerate.

<!-- BEGIN GENERATED: config-reference (scripts/gen_config_reference.py) -->
### Root

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `name` | string | **(required)** | first day | Unique deployment name. Also used as the K8s namespace when `namespace` is empty. |
| `recipe` | string or null | `null` | first day | Recipe shorthand (e.g., `hive-iceberg-spark-trino`). Sets catalog, table format, and query engine defaults. See [Recipes](recipes.md). |

### Images

Container images for every deployed component. Override for air-gapped registries or custom builds.

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `images.datagen` | string | `docker.io/sillidata/lb-datagen:5d7ce61a@sha256:ed4057e097f09fdd3e37631bc37eb88e5fce561cb8ebe06cd6fa2fd7d23e4bfc` | advanced | Data generator image, pinned by tag and digest (the digest is what is pulled). In continuous mode it generates until the run window ends: AML as successive 24-month periods of the same bank, Customer360 as successive time slices. |
| `images.spark` | string | `apache/spark:4.1.1-python3` | advanced | Spark runtime image. Unset: the image of the config's recipe (or of the recipe its components name): `4.1.1-python3` on the Hive recipes, `4.0.2-python3` on the Polaris recipes, `hive-delta-spark-thrift` and `hive-delta-spark-none`; 4.0.2 also when the config writes a table format version Spark 4.1 cannot run (Delta 4.0.0). |
| `images.postgres` | string | `postgres:17` | advanced | PostgreSQL image (metadata backend). |
| `images.polaris` | string | `apache/polaris:1.6.0` | advanced | Apache Polaris REST catalog image. |
| `images.polaris_admin_tool` | string | `apache/polaris-admin-tool:1.6.0` | advanced | Polaris admin tool image; bootstraps the Polaris metastore on a Polaris recipe. |
| `images.unity` | string | `unitycatalog/unitycatalog:main` | advanced | Unity Catalog server image (Unity catalog only; no recipe uses it). |
| `images.trino` | string | `trinodb/trino:483` | advanced | Trino query engine image. |
| `images.duckdb` | string | `python:3.11-slim` | advanced | Python image the DuckDB query engine pod runs in; DuckDB itself is pinned by `architecture.query_engine.duckdb.version`. |
| `images.jmx_exporter` | string | `bitnami/jmx-exporter:latest@sha256:873527b34b55ca7b8f0b5f7efdf18c93e4337d06709da674ba7284fd3f6316de` | advanced | JMX exporter image for the metrics sidecars, used when observability is enabled. |
| `images.pull_policy` | one of `Always`, `IfNotPresent`, `Never` | `Always` | advanced | `Always`, `IfNotPresent`, or `Never`. |

### Platform -- Kubernetes

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `platform.kubernetes.context` | string | `""` | advanced | kubectl context name from the kubeconfig (`$KUBECONFIG`, else `~/.kube/config`). Empty = the kubeconfig's current context (`kubectl config current-context`; with several files in `$KUBECONFIG`, the first file that sets one), resolved by name at the command's first cluster call, API or `kubectl`/`helm`/`oc`; every later call in that `lakebench` process uses that context, even if the current context changes while it runs. The command stops at its next client load or tool call if the context's API server or CA changes in the kubeconfig. A name that is not in the kubeconfig is refused. In-cluster credentials are used only when no kubeconfig file exists. Set this to target a specific cluster when you have multiple contexts configured. |
| `platform.kubernetes.namespace` | string | `""` | first day | Kubernetes namespace for all resources. Empty = use the deployment `name`. |
| `platform.kubernetes.create_namespace` | boolean | `true` | advanced | Create the namespace if it does not exist. |

### Platform -- S3 Storage

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `platform.storage.s3.endpoint` | string | **(required)** | first day | S3-compatible endpoint URL (e.g., `http://minio:9000` or `https://s3.example.com:443`). HTTPS endpoints with self-signed CAs require `ca_cert`. |
| `platform.storage.s3.region` | string | `us-east-1` | advanced | AWS region. Used by boto3 for signing. |
| `platform.storage.s3.path_style` | boolean | `true` | advanced | Path-style access (`true` for FlashBlade/MinIO, `false` for AWS S3). Spark and the datagen pods both follow it; with `false` requests go to `<bucket>.<endpoint host>`. |
| `platform.storage.s3.access_key` | string | `""` | first day | S3 access key. Required for deploy. |
| `platform.storage.s3.secret_key` | string | `""` | first day | S3 secret key. Required for deploy. |
| `platform.storage.s3.ca_cert` | string | `""` | advanced | Path to a PEM CA certificate bundle for HTTPS endpoints with self-signed or private CAs. Empty = use system default CAs. The PEM content is read at deploy time and embedded into a Kubernetes Secret for all components. |
| `platform.storage.s3.verify_ssl` | boolean | `true` | advanced | Verify SSL certificates for HTTPS endpoints. Set `false` only for development with self-signed certs when you don't have the CA certificate file. |
| `platform.storage.s3.buckets.bronze` | string | `<name>-bronze` | advanced | Bronze layer S3 bucket name. Unset, it is derived from the deployment `name`. |
| `platform.storage.s3.buckets.silver` | string | `<name>-silver` | advanced | Silver layer S3 bucket name. Unset, it is derived from the deployment `name`. |
| `platform.storage.s3.buckets.gold` | string | `<name>-gold` | advanced | Gold layer S3 bucket name. Unset, it is derived from the deployment `name`. |
| `platform.storage.s3.create_buckets` | boolean | `true` | advanced | Create buckets if they do not exist. |

### Platform -- Scratch Storage

Scratch PVCs for Spark shuffle data. Only needed with Portworx or similar CSI.

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `platform.storage.scratch.enabled` | boolean | `false` | advanced | Enable scratch StorageClass for Spark PVCs. Unset, a batch run at scale 50 and above turns it on (Spark shuffle there outgrows pod ephemeral storage). |
| `platform.storage.scratch.storage_class` | string | `px-csi-scratch` | advanced | StorageClass name for scratch volumes. |
| `platform.storage.scratch.provisioner` | string | `pxd.portworx.com` | advanced | CSI provisioner for the StorageClass. Use `rancher.io/local-path`, `ebs.csi.aws.com`, etc. for non-Portworx providers. |
| `platform.storage.scratch.parameters` | mapping | `{"io_profile": "auto", "priority_io": "high", "repl": "1"}` | advanced | Provider-specific StorageClass parameters. |

### Platform -- Spark Compute

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `platform.compute.spark.operator.install` | boolean | `false` | advanced | Refused when `true`: deploy never installs the shared operator; a cluster admin runs `lakebench admin install --component spark-operator`. `false` loads as before. |
| `platform.compute.spark.operator.namespace` | string | `spark-operator` | advanced | Namespace for the Spark Operator. |
| `platform.compute.spark.operator.version` | string | `2.5.1` | advanced | Chart version a fresh `admin install` uses. v2.x required. An installed operator keeps its version. |
| `platform.compute.spark.bronze_executors` | integer or null | `null` | advanced | Override bronze-verify executor count (1--28). Null = auto from scale. |
| `platform.compute.spark.silver_executors` | integer or null | `null` | advanced | Override silver-build executor count (1--28). Null = auto from scale. |
| `platform.compute.spark.gold_executors` | integer or null | `null` | advanced | Override gold-finalize executor count (1--28). Null = auto from scale. |
| `platform.compute.spark.bronze_ingest_executors` | integer or null | `null` | advanced | Override bronze-ingest executor count (1--28). Null = auto from scale. |
| `platform.compute.spark.silver_stream_executors` | integer or null | `null` | advanced | Override silver-stream executor count (1--28). Null = auto from scale. |
| `platform.compute.spark.gold_refresh_executors` | integer or null | `null` | advanced | Override gold-refresh executor count (1--28). Null = auto from scale. |
| `platform.compute.spark.bronze_ingest_executor_cores` | integer or null | `null` | advanced | Override bronze-ingest cores per executor (1--16). Memory and scratch follow the profile's per-core share; the executor count is unchanged, so total cores grow with the size. Null = auto: the profile's, grown to 8 or 16 cores when the offered load needs more executors than the cap, unless `bronze_ingest_executors` is set. |
| `platform.compute.spark.silver_stream_executor_cores` | integer or null | `null` | advanced | Override silver-stream cores per executor (1--16). Memory and scratch follow the profile's per-core share; the executor count is unchanged, so total cores grow with the size. Null = auto: the profile's, grown to 8 or 16 cores when the offered load needs more executors than the cap, unless `silver_stream_executors` is set. |
| `platform.compute.spark.gold_refresh_executor_cores` | integer or null | `null` | advanced | Override gold-refresh cores per executor (1--16). Memory and scratch follow the profile's per-core share; the executor count is unchanged, so total cores grow with the size. Null = the profile's; gold-refresh is never grown automatically. |
| `platform.compute.spark.driver_memory` | string or null | `null` | advanced | Global driver memory override (e.g., `16g`). Null = profile default. |
| `platform.compute.spark.driver_cores` | integer or null | `null` | advanced | Override driver cores (1--16). Null = profile default (typically 4). |

### Platform -- PostgreSQL

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `platform.compute.postgres.storage` | string | `10Gi` | advanced | PVC size for PostgreSQL data. |
| `platform.compute.postgres.storage_class` | string | `""` | advanced | StorageClass for PostgreSQL PVC. Empty = cluster default StorageClass (requires one to exist -- see [Prerequisites](getting-started.md#default-storageclass)). |

### Platform -- Dependency server

What `lb-deps` is and when it resolves again: see [Dependency server](#dependency-server).

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `platform.deps.maven_repository` | string | `""` | advanced | The only Maven repository the resolve reads; replaces Maven Central and the Google mirror. An `http://` or `https://` base URL without credentials, query or fragment; stored with one trailing `/`. |
| `platform.deps.pypi_index` | string | `""` | advanced | PyPI simple index for the AML reference wheels and the DuckDB wheel. Empty = `https://pypi.org/simple/`. A plain-HTTP index is passed to pip as a trusted host. |
| `platform.deps.duckdb_extension_repository` | string | `""` | advanced | DuckDB extension repository. Empty = `http://extensions.duckdb.org`. |
| `platform.deps.storage_class` | string | `""` | advanced | StorageClass of the `lb-deps-data` PVC (5Gi, ReadWriteOnce). Empty = the cluster default StorageClass. Read only when the PVC is created; an existing PVC is never changed, so delete it to move the set. The volume must be writable by UID 185 through `fsGroup`. |

### Architecture -- Catalog

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `architecture.catalog.type` | one of `hive`, `polaris`, `unity`, `none` | `hive` | advanced | Catalog service: `hive` or `polaris`. `unity` and `none` have no supported recipe and are refused. |
| `architecture.catalog.hive.operator.install` | boolean | `false` | advanced | Refused when `true`: deploy never installs the Stackable operators; a cluster admin runs `lakebench admin install --component stackable`. `false` loads as before. |
| `architecture.catalog.hive.operator.namespace` | string | `stackable` | advanced | Namespace for Stackable operators. |
| `architecture.catalog.hive.operator.version` | string | `25.7.0` | advanced | Stackable SDP chart version a fresh `admin install` uses. |
| `architecture.catalog.hive.resources.cpu_min` | string | `500m` | advanced | Hive Metastore minimum CPU request. |
| `architecture.catalog.hive.resources.cpu_max` | string | `2` | advanced | Hive Metastore CPU limit. |
| `architecture.catalog.hive.resources.memory` | string | `4Gi` | advanced | Hive Metastore memory. |
| `architecture.catalog.polaris.port` | integer | `8181` | advanced | Polaris REST API port. |
| `architecture.catalog.polaris.client_secret` | string | `""` | advanced | OAuth2 secret of the `lakebench` Polaris client. Empty = deploy generates one and keeps it in the Secret `lakebench-polaris-client`; a value is written there on the first deploy. Use a `${VAR}` reference rather than a literal. |
| `architecture.catalog.polaris.resources.cpu` | string | `1` | advanced | Catalog server CPU request/limit. |
| `architecture.catalog.polaris.resources.memory` | string | `2Gi` | advanced | Catalog server memory. |
| `architecture.catalog.unity.spark_connector_version` | string | `0.4.0` | advanced | Unity Catalog Spark connector version the Spark jobs load. |
| `architecture.catalog.unity.port` | integer | `8080` | advanced | Unity Catalog REST API port. |
| `architecture.catalog.unity.resources.cpu` | string | `1` | advanced | Catalog server CPU request/limit. |
| `architecture.catalog.unity.resources.memory` | string | `2Gi` | advanced | Catalog server memory. |

### Architecture -- Table Format

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `architecture.table_format.type` | one of `iceberg`, `delta` | `iceberg` | advanced | Table format: `iceberg` or `delta`. Delta runs with the `hive` catalog and `trino`, `spark-thrift` or `none` query engines (the `hive-delta-*` recipes). The `financial` (AML) workload refuses Delta. |
| `architecture.table_format.iceberg.version` | string | `1.11.0` | advanced | Apache Iceberg runtime JAR version. |
| `architecture.table_format.delta.version` | string | `auto` | advanced | Delta Lake version. `auto` resolves a version that matches the Spark image. |

### Architecture -- Pipeline engine

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `architecture.pipeline_engine` | one of `spark` | `spark` | advanced | Pipeline engine; `spark` is the only one. |

### Architecture -- Query Engine

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `architecture.query_engine.type` | one of `trino`, `spark-thrift`, `duckdb`, `none` | `trino` | advanced | Query engine: `trino`, `spark-thrift`, `duckdb`, or `none`. |
| `architecture.query_engine.trino.coordinator.cpu` | string | `2` | advanced | Trino coordinator CPU. |
| `architecture.query_engine.trino.coordinator.memory` | string | `8Gi` | advanced | Trino coordinator memory. |
| `architecture.query_engine.trino.worker.replicas` | integer | `2` | advanced | Number of Trino worker pods. |
| `architecture.query_engine.trino.worker.cpu` | string | `4` | advanced | Trino worker CPU. |
| `architecture.query_engine.trino.worker.memory` | string | `16Gi` | advanced | Trino worker memory. |
| `architecture.query_engine.trino.worker.spill_enabled` | boolean | `true` | advanced | Enable query spill to disk. |
| `architecture.query_engine.trino.worker.spill_max_per_node` | string | `40Gi` | advanced | Maximum spill size per worker. |
| `architecture.query_engine.trino.worker.storage` | string | `50Gi` | advanced | Worker storage size for spill and temp data. |
| `architecture.query_engine.trino.worker.storage_class` | string | `""` | advanced | Worker StorageClass. Empty = emptyDir (ephemeral, no PVC needed). Set a class name to use PVC-backed persistent volumes instead. |
| `architecture.query_engine.trino.catalog_name` | string | `lakehouse` | advanced | Trino catalog name for the Iceberg connector. |
| `architecture.query_engine.spark_thrift.cores` | integer | `2` | advanced | Spark Thrift Server CPU cores. Auto-sized to `8` on Delta + Hive when unset. |
| `architecture.query_engine.spark_thrift.memory` | string | `4g` | advanced | Spark Thrift Server heap. The pod limit adds max(10% of heap, 1 GiB). Auto-sized to `16g` on Delta + Hive and `24g` on the financial schema when unset. |
| `architecture.query_engine.spark_thrift.catalog_name` | string | `lakehouse` | advanced | Iceberg catalog name for Spark Thrift Server. |
| `architecture.query_engine.duckdb.cores` | integer | `2` | advanced | DuckDB CPU cores. |
| `architecture.query_engine.duckdb.memory` | string | `4g` | advanced | DuckDB pod memory. Auto-sized to `16g` on the financial schema when unset (less on a node with under 24 GiB allocatable). |
| `architecture.query_engine.duckdb.catalog_name` | string | `lakehouse` | advanced | Iceberg catalog name for DuckDB. |
| `architecture.query_engine.duckdb.version` | string | `1.5.5` | advanced | DuckDB version installed at deploy time. Pinned deliberately: an unpinned install takes whatever is current, so two runs weeks apart can query with different engines and the difference would read as a result. |

### Architecture -- Pipeline

The legacy name `processing` is still accepted with a deprecation warning.

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `architecture.pipeline.pattern` | one of `medallion`, `streaming`, `batch`, `custom` | `medallion` | advanced | **Deprecated; removed in v1.7.** The stages are chosen by `pipeline.mode`. The only remaining effect is that `streaming` makes the auto-sizer give Spark 60% and datagen 40% of the CPU budget; any value other than `medallion` prints a warning. |
| `architecture.pipeline.mode` | one of `batch`, `continuous` | `batch` | advanced | Pipeline execution mode: `batch` (sequential medallion jobs) or `continuous` (concurrent jobs over arriving data). `sustained` is accepted as a deprecated alias. The `--continuous` CLI flag overrides this. |
| `architecture.pipeline.cycles` | integer | `1` | advanced | Batch iterations (1--50). Cycle 1 is full overwrite; cycles 2+ are incremental append/merge. Simulates multi-day lakehouse behavior. Only valid when `mode: batch`. See [Multi-Cycle Batch](#multi-cycle-batch). |
| `architecture.pipeline.pre_benchmark_maintenance` | boolean | `true` | advanced | Run table maintenance before the benchmark phase so QpH is measured against maintained tables. Iceberg: `expire_snapshots`, `remove_orphan_files` (never below 24 h 10 min) and compaction of silver and gold. Delta: `VACUUM` on Trino only; Delta `OPTIMIZE` is never run. All statements share one 30-minute budget; the first statement timeout or the deadline stops the rest, and the post-maintenance QpH is then not a measurement. |
| `architecture.pipeline.continuous.bronze_trigger_interval` | string | `0 seconds` | advanced | Bronze streaming trigger interval. "0 seconds" (the default) starts the next micro-batch as soon as the last one finishes and new files exist; a positive interval holds bronze to that cadence, labelled beside freshness. With a trickle (`max_files_per_trigger`, `--skip-generate`), "0 seconds" becomes 30 seconds, the trickle's cadence, and the run says so. A whole number and seconds, minutes or hours; anything else is refused at load. |
| `architecture.pipeline.continuous.silver_trigger_interval` | string | `0 seconds` | advanced | Silver streaming trigger interval. "0 seconds" (the default) starts the next micro-batch as soon as the last one finishes and bronze has committed more; a positive interval is labelled beside freshness. A whole number and seconds, minutes or hours. |
| `architecture.pipeline.continuous.gold_refresh_interval` | string | `0 seconds` | advanced | Gold refresh trigger interval. "0 seconds" (the default) starts the next refresh as soon as the last one finishes (Customer360: as soon as silver commits); a positive interval holds gold to that cadence, labelled beside freshness. |
| `architecture.pipeline.continuous.run_duration` | integer | `1800` | advanced | Measurement window in seconds. The schema accepts 60 and up; with gold on an interval a continuous run refuses less than 3 x `gold_refresh_interval`. Use 900 s or more, UAT included. |
| `architecture.pipeline.continuous.checkpoint_base` | string | `checkpoints` | advanced | S3 prefix for streaming checkpoints. |
| `architecture.pipeline.continuous.max_files_per_trigger` | integer or null | none | advanced | Max Parquet files bronze reads per trigger, a Lakebench cap on intake. Unset: no limit when the run starts its own datagen, which generates for the whole window. With --skip-generate (a finite corpus) unset is derived per run so data keeps arriving for about 1.2 x `run_duration`, capped at 50, and an explicit value that would offer the corpus before the window ends is refused at run start. |
| `architecture.pipeline.continuous.bronze_target_file_size_mb` | integer | `512` | advanced | Target Iceberg file size for bronze writes (MB) |
| `architecture.pipeline.continuous.silver_target_file_size_mb` | integer | `512` | advanced | Target Iceberg file size for silver writes (MB) |
| `architecture.pipeline.continuous.silver_bronze_wait_seconds` | integer or null | `null` | advanced | Customer360 only: seconds silver-stream waits for the bronze table to appear before it stops the run. Unset (auto): the run's window (`--duration` or run_duration) / 4, floored at 600 s. The wait runs before the window opens: datagen starts once the streams run, and bronze creates its table with its first batch. |
| `architecture.pipeline.continuous.gold_target_file_size_mb` | integer | `128` | advanced | Target Iceberg file size for gold writes (MB) |
| `architecture.pipeline.continuous.retention_interval` | integer or null | auto | advanced | Seconds between table maintenance rounds during a continuous run: Iceberg `expire_snapshots` + `remove_orphan_files`, or Delta `VACUUM` (Trino only). Unset: `run_duration / 3`, within 300--7200, resolved at run start (600 s for the default 1800 s window), so a default run maintains inside its window. An explicit value too long for the first round to run inside the window is refused at run start unless `--skip-maintenance` is given. While streams are live Delta `VACUUM` keeps Delta's 7-day default retention, so a continuous Delta run shorter than 7 days removes no files. Range: 300--7200. |
| `architecture.pipeline.continuous.retention_threshold` | string | `30m` | advanced | Iceberg snapshot retention threshold. Snapshots older than this are expired. A whole number and one unit, `s`, `m`, `h` or `d` (e.g., `30m`, `1h`, `7d`); anything else is rejected at load. While streams are live, Iceberg expiry is floored at `1h`, and a continuous Iceberg config on Trino or Spark Thrift that sets a lower value prints a warning when it loads. The `30m` default does not warn: every continuous maintenance round runs beside live streams, so it expires at `1h`, and the run records the applied values in `continuous.retention` of `metrics.json`. Delta has no effective table maintenance in continuous mode (v1.6). Orphan-file removal never uses less than 24 h 10 min, on any engine. |
| `architecture.pipeline.continuous.compaction_enabled` | boolean | `true` | advanced | Run periodic Iceberg compaction (`rewrite_data_files` / `optimize`) during continuous runs. |
| `architecture.pipeline.continuous.compaction_interval` | integer | `0` | advanced | Seconds between compaction rounds. `0` = 2x the effective `retention_interval` (1200 s for the default window). An explicit value that cannot run inside the window is refused at run start unless `compaction_enabled` is false or `--skip-maintenance` is given. Minimum 0; no upper bound in the schema. |
| `architecture.pipeline.continuous.benchmark_interval` | integer | `300` | advanced | Seconds between in-stream benchmark rounds, 300--3600. With gold on a longer interval, raised to that interval so rounds do not overlap gold rewrites. |
| `architecture.pipeline.continuous.benchmark_warmup` | integer | `300` | advanced | Seconds before the first in-stream benchmark round, 300--1800. With gold on a longer interval, raised to that interval so gold refreshes once first. |

### Workload & Datagen

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `workload.schema` | one of `customer360`, `financial`, `custom` | `customer360` | first day | Workload schema: `customer360` or `financial`. `custom` is refused at load in v1.6. `financial` requires `table_format: iceberg`. The block was `architecture.workload` before v1.6; that location still loads with a deprecation warning, and setting both with different values is an error. |
| `workload.datagen.scale` | number | `10` | first day | Scale factor (1 unit ~ 10 GB bronze). The schema accepts 0.01--10000, but datagen is banded per workload: Customer 360 supported to 300, unverified to 600, refused above; AML supported to 300, unverified to 800, refused above (see [Scale Factors](#scale-factors)). Values below 1 are intended for local mode. |
| `workload.datagen.target_size` | string or null | `null` | advanced | **Deprecated.** Legacy size string (e.g., `100gb`). Converted to scale automatically. |
| `workload.datagen.mode` | one of `batch`, `continuous`, `auto` | `auto` | advanced | S3 delivery pattern: `batch` = one PUT per file, `continuous` = S3 multipart upload as row-groups close, `auto` = `continuous` at every scale. Row content is byte-identical across modes at a fixed seed. Sizing is keyed on scale, not mode. |
| `workload.datagen.seed` | integer or null | `null` | advanced | Top-level generator seed, which names the corpus. Unset: the AML pre-registration's calibration seed for `financial`, 42 for other schemas. A `financial` seed listed in the pre-registration's `corpora.spent_seeds` is refused at config load, so a retired corpus is never regenerated by accident. The AML reference job reports the seed it scored. The held-out evaluation and robustness seeds are refused unless `corpus_role` declares that role, and even then every command that reads or scores data refuses them: their corpus is generated only by `generate --registered-corpus` and scored only by `scripts/aml_gate.py --registered`. They are known only as salted hashes in `spark/data/aml/heldout_hashes.json`, and the check hashes the configured seed. |
| `workload.datagen.corpus_role` | one of `calibration`, `evaluation`, `robustness` or null | `null` | advanced | `financial` only: `calibration`, `evaluation` or `robustness`. Declares this deployment as the registered corpus for that role. For `evaluation` and `robustness`, `seed` must be set and must hash to that role's entry in `heldout_hashes.json`; an unset `seed` is refused, `generate --registered-corpus` generates the corpus, and every other data command refuses it; its seed reaches the cluster only through a Secret in the deployment's namespace, never as a Job argument. For `calibration`, an unset `seed` uses the calibration seed. Only set it for the one registered gate run of that role. |
| `workload.datagen.robustness_perturbation` | boolean | `false` | advanced | `financial` only. Generates the robustness corpus: the pre-registration's `corpora.robustness_perturbation` multipliers shift the nuisance parameters in natural units (median amount x1.2, persona activity and amount log-sds x1.2, dormancy lengths x1.2). Instances, participants and row counts are unchanged. Required with `corpus_role: robustness`, refused with `calibration` or `evaluation`; the generator also refuses the robustness seed without it. Off, the corpus is byte-identical to a run without the option. |
| `workload.datagen.parallelism` | integer | auto (by scale and cluster) | advanced | Number of parallel datagen pods. A value you set is used exactly, with a warning when the cluster cannot fit it (batch pods queue; a continuous run, whose pods all run beside the streams, is refused at preflight) or it is under 8 for financial above scale 100. Unset: in batch the auto-sizer derives it from the scale, caps it to fit the cluster and raises financial above scale 100 to at least 8 pods; in continuous, with `cpu` also unset, it is the pods (of up to 8 cores) that offer the scale's load, still raised to the financial floor and capped to fit the cluster (a cap lowers the offered load). The schema fallback without auto-sizing is 4. |
| `workload.datagen.file_size` | one of `64mb` | `64mb` | advanced | Fixed at `64mb` for every workload and mode; any other value is refused. One size keeps row content identical across delivery modes. |
| `workload.datagen.dirty_data_ratio` | number | `0.08` | advanced | Fraction of intentionally dirty records (0.0--1.0). Applies to the `customer360` schema only; the `financial` (AML) generator ignores it. |
| `workload.datagen.cpu` | string | auto (by scale and cluster) | advanced | CPU per datagen pod. A value you set is used as given. Unset: 8 in batch, and 8 in continuous when `parallelism` is set; in continuous with both unset, the cores that offer the scale's load (in 100m steps, at least 200m), split over the pods. |
| `workload.datagen.memory` | string | auto (by workload, scale and cpu) | advanced | Memory per datagen pod. A value you set is used as given; unset, the auto-sizer derives it from the measured peak RSS model for the schema, scale, pod CPU (thread count) at the fixed 64mb file size, with a 4Gi floor. |
| `workload.datagen.generators` | integer | `0` | advanced | Generator threads per pod. 0 = auto: one thread per started core of the pod's CPU (Lakebench sets `CPU_LIMIT`), held to the CPU by its quota. |
| `workload.datagen.timestamp_start` | string or null | `null` | advanced | Start date for generated timestamps (ISO format). Default: `2024-01-01`. See [Timestamp Range Impact](#timestamp-range-impact). |
| `workload.datagen.timestamp_end` | string or null | `null` | advanced | End date for generated timestamps (ISO format, exclusive). Default: `2025-01-01` for single-cycle runs (Rust generator built-in). Multi-cycle runs (`cycles > 1`) split a wider `2024-01-01` to `2025-12-31` default window across cycles (`config/c360_run.py` `cycle_windows`, which the datagen deployer and `metrics/c360_correctness.py` both read). See [Timestamp Range Impact](#timestamp-range-impact). |

### Workload -- Customer 360 and AML

Advanced workload overrides. Most users should leave these at defaults and control volume via `datagen.scale`. The AML TM operations layer (`workload.tm_operations.*`) is described in [TM operations](benchmarks/aml/tm-operations.md#85-tm-operations).

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `workload.customer360.unique_customers` | integer or null | `null` | advanced | Override: unique customer count. If None, derived from scale. |
| `workload.retention_workload` | boolean | `false` | advanced | AML: keep the snapshots time-travel reproduction needs. Pre-benchmark maintenance then retains `retention_months` plus headroom instead of expiring every snapshot. |
| `workload.retention_months` | integer | `60` | advanced | AML: months of snapshots kept when `retention_workload` is true (1--120). |
| `workload.w1_max_vertices` | integer | `8000000` | advanced | AML: vertex cap of the W1 connected-components rule, a Lakebench-imposed cap. The default covers scale 10 (1.1M entities); raise it for larger scales that have the executor budget. |
| `workload.tm_operations.enabled` | boolean | `true` | advanced | Run the TM operations layer. When it cannot run (no manifest, an error) the run reports it as not run; only violated workflow invariants fail a run |
| `workload.tm_operations.seed` | integer | `20260924` | advanced | Seed for every simulated decision |
| `workload.tm_operations.analyst_accuracy` | number | `0.9` | advanced | Share of alerts the simulated L1 analyst dispositions correctly (0.5--1.0). |
| `workload.tm_operations.investigator_accuracy` | number | `0.95` | advanced | Share of cases the simulated L2 investigator decides correctly (0.5--1.0). |
| `workload.tm_operations.qa_sample_rate` | number | `0.05` | advanced | Share of L1 decisions QA re-reviews |
| `workload.tm_operations.alert_sla_days` | integer | `60` | advanced | Policy SLA from alert to final decision |
| `workload.tm_operations.case_lookback_months` | integer | `12` | advanced | Activity a case pulls in before its opening |
| `workload.tm_operations.late_filing_rate` | number | `0.03` | advanced | Share of SARs filed after the deadline |
| `workload.tm_operations.no_suspect_rate` | number | `0.05` | advanced | Share of new cases with no suspect identified (60-day filing clock) |
| `workload.tm_operations.max_alerts_per_customer` | integer | `50000` | advanced | Alerts replayed per customer; a hub customer's alerts past this are dispositioned over_capacity and counted in the report |
| `workload.tm_operations.continuous_interval_seconds` | integer | `1800` | advanced | Continuous mode: seconds between operations passes. One pass costs minutes at scale 10, so running it every gold-refresh tick would wreck freshness |
| `workload.tm_operations.counterparty_scenarios` | list | `["W1_connected_components", "W3_round_tripping", "W4_risk_propagation", "W17_layering_chain"]` | advanced | Scenarios declared to alert on counterparties as well as customers (graph overlays). An alert on a non-customer from any other scenario fails an invariant |

### Architecture -- Benchmark

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `architecture.benchmark.mode` | one of `standard`, `extended`, `power`, `throughput`, `composite` | `power` | advanced | Benchmark mode: `power`, `standard`, `extended`, `throughput`, or `composite`. `lakebench run` measures one power pass (`standard` and `extended` are power) and refuses `throughput` and `composite`, with or without `--skip-benchmark`; `lakebench benchmark --mode` runs them. |
| `architecture.benchmark.streams` | integer | `4` | advanced | Concurrent query streams for `lakebench benchmark` throughput mode. Range: 1--64. `lakebench run` uses one stream and refuses an explicit value above 1. |
| `architecture.benchmark.cache` | string | `hot` | advanced | Cache mode: `hot` (warm cache) or `cold` (cleared before each query). `lakebench run` measures a hot cache and refuses `cold`; use `lakebench benchmark --cold`. |
| `architecture.benchmark.iterations` | integer | `3` | advanced | Timed runs of each query per benchmark round. QpH is scored from the per-query median and every sample plus the spread is recorded in `metrics.json`. `1` is a quick run with no measured spread; the maintenance value is then not reported. Range: 1--100. |
| `architecture.benchmark.maintenance_settle.enabled` | boolean | `true` | advanced | Batch mode: probe until storage settles between maintenance and the post-maintenance round. See [Storage settle wait](benchmarking/maintenance.md#storage-settle-wait). |
| `architecture.benchmark.maintenance_settle.max_seconds` | integer | `2700` | advanced | Longest wait. When reached, the post round still runs and `maintenance_value_pct` is null. Range: 60--14400. |
| `architecture.benchmark.maintenance_settle.interval_seconds` | integer | `60` | advanced | Seconds between the starts of consecutive probes. Range: 5--3600. |
| `architecture.benchmark.maintenance_settle.tolerance_pct` | number | `10.0` | advanced | Settled when two consecutive probes differ by at most this percent and neither is slower than the pre-maintenance time by more. Range: above 0, up to 100. |
| `architecture.benchmark.maintenance_settle.probe_query` | string or null | `null` | advanced | Benchmark query name to probe with. Default: the workload's first scan-class query (a full scan of the table compaction rewrote) |
| `architecture.benchmark.maintenance_settle.probe_samples` | integer | `1` | advanced | Timed runs per probe; the probe time is their median. Range: 1--10. |
| `architecture.benchmark.investigator_sessions` | integer or null | `null` | advanced | AML continuous only: concurrent investigator sessions run once, as an extra round after the first in-stream round that had a case, each working one case of the run (IQ1 to IQ4). Range: 1--32. Unset runs no such round. Refused at load unless the workload is `financial`, `tm_operations.enabled` is true and the query engine is `trino` or `spark-thrift`, and by `run` unless the run is continuous. The sessions that ran (fewer when the run has fewer cases) are an outcome condition: two runs that differ in it are not like-for-like. |

### Architecture -- Table Names

Fully-qualified table names (`namespace.table`). The catalog prefix is added at runtime.

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `architecture.tables.bronze` | string | `default.bronze_raw` | advanced | Bronze table: namespace.table (e.g. default.bronze_raw) |
| `architecture.tables.silver` | string | `silver.customer_interactions_enriched` | advanced | Silver table: namespace.table |
| `architecture.tables.gold` | string | `gold.customer_executive_dashboard` | advanced | Gold table: namespace.table |
| `architecture.tables.silver_entities` | string | `silver.entities` | advanced | Silver entities table (Financial): namespace.table |
| `architecture.tables.silver_accounts` | string | `silver.accounts` | advanced | Silver accounts table (Financial): namespace.table |
| `architecture.tables.silver_account_statements` | string | `silver.account_statements` | advanced | Silver camt.053-shaped statement-line table with running balance (Financial): namespace.table |
| `architecture.tables.silver_counterparty_edges` | string | `silver.counterparty_edges` | advanced | Silver entity-to-entity edge table (Financial): namespace.table |
| `architecture.tables.silver_entity_profiles` | string | `silver.entity_profiles` | advanced | Silver per-entity behavioural baseline table (Financial, C-PROFILES): namespace.table |
| `architecture.tables.silver_batch_versions` | string | `silver.silver_batch_versions` | advanced | Silver sealed-batch marker sidecar (Financial): one row per (stream_id, batch_id) written last so downstream consumers hide mid-batch crashes |
| `architecture.tables.silver_counterparty_pairs` | string | `silver.counterparty_pairs` | advanced | Silver distinct (originator, beneficiary) pairs (Financial, continuous only): one row per pair, so the stream counts a batch's new counterparties without re-reading the history |
| `architecture.tables.gold_alerts` | string | `gold.alerts` | advanced | Gold alerts table (Financial): namespace.table |
| `architecture.tables.gold_risk_scores` | string | `gold.risk_scores` | advanced | Gold entity risk-score table (Financial): namespace.table |
| `architecture.tables.gold_entity_clusters` | string | `gold.entity_clusters` | advanced | Gold synthetic-id / community-detection cluster table (Financial): namespace.table |
| `architecture.tables.gold_daily_dashboards` | string | `gold.daily_dashboards` | advanced | Gold daily-aggregate dashboard table (Financial): namespace.table |
| `architecture.tables.gold_tm_reconciliation` | string | `gold.tm_reconciliation` | advanced | Per-cycle monitoring completeness and funnel ledger (Financial) |
| `architecture.tables.gold_scenario_coverage` | string | `gold.scenario_coverage` | advanced | Scenario-to-typology coverage matrix (Financial) |
| `architecture.tables.gold_alert_dispositions` | string | `gold.alert_dispositions` | advanced | L1 triage priority and disposition per alert (Financial) |
| `architecture.tables.gold_cases` | string | `gold.cases` | advanced | Customer-keyed L2 cases with SAR decisions (Financial) |

### Observability

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `observability.enabled` | boolean | `false` | advanced | Deploy the observability stack (Prometheus + Grafana). |
| `observability.dashboards_enabled` | boolean | `true` | advanced | Deploy Grafana dashboards. |
| `observability.retention` | string | `7d` | advanced | Prometheus data retention period. |
| `observability.storage` | string | `10Gi` | advanced | Prometheus PVC size. |
| `observability.chart_version` | string | `87.19.2` | advanced | `kube-prometheus-stack` Helm chart version. Bundles Prometheus and Grafana as one unit -- there is no separate Prometheus/Grafana version field. The version a fresh `lakebench admin install --component observability` uses; an installed release keeps its version. |
| `observability.pushgateway_enabled` | boolean | `true` | advanced | Deploy a Prometheus Pushgateway for Spark job metrics when observability is enabled. A live view only; `metrics.json` stays the record. |
| `observability.pushgateway_image` | string | `prom/pushgateway:v1.11.1` | advanced | Pushgateway image. |
| `observability.pushgateway_storage` | string | `1Gi` | advanced | Pushgateway persistent volume size. |
| `observability.pushgateway_storage_class` | string | `px-csi-scratch` | advanced | StorageClass of the Pushgateway volume. |

### Spark

How `spark.conf` layers over the job defaults and what it refuses: see [Spark Configuration Overrides](#spark-configuration-overrides).

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `spark.conf` | mapping | `{}` | advanced | Your Spark keys, merged over the job defaults. Enters the experiment record (`architecture.spark_conf_user`) when it changes something. |
<!-- END GENERATED: config-reference -->

## Dependency server

`deploy` starts one dependency server, `lb-deps`, in the deployment's
namespace. What it resolves, how deploy checks it and what fails are in
[Deployment](deployment.md#deployment-order), step 11. In addition:

- The set is resolved again only when the request changes. That is the
  Spark or DuckDB image tag, the table format or its version, the workload
  (AML adds the reference wheels), the query engine or the DuckDB version, a
  mirror key, or the resolver shipped with Lakebench.
- The request names images by tag. The manifest records the digest each
  container actually ran.
- Mirrors are read anonymously, over plain HTTP or over HTTPS with a
  publicly trusted certificate. A mirror that serves the same bytes yields
  the same dependency set hash.

## Spark Configuration Overrides

`spark.conf` holds your own Spark keys. Each job's conf has three layers,
later ones winning:

1. the job defaults (`SPARK_CONF_DEFAULTS`, below);
2. `spark.conf`;
3. the keys Lakebench sets for the job: per-job shuffle partitions and
   result size, the catalog and S3A connection settings, jars, adaptive
   execution, UI and Kubernetes settings.

Refused keys:

- A `spark.conf` key Lakebench sets for the job is refused at load by the
  commands that change data, naming the setting that controls it instead.
  For example `spark.sql.shuffle.partitions` follows the executor count,
  `platform.compute.spark.<job>_executors`. `destroy`, `status` and the
  read-only commands drop it with a note.
- `spark.driver.maxResultSize` is the one Lakebench-set key a user value
  replaces.
- Also refused: keys a job script sets (`spark.sql.session.timeZone` and
  others), and the reserved `spark.kubernetes.*`, `spark.jars.*` and
  pod-sizing keys (executor and driver memory, overhead, cores, off-heap,
  PySpark memory).
- The jar keys are all Lakebench's, because every job takes its jars, in
  order, from the deployment's dependency set: `spark.jars`,
  `spark.jars.*`, `spark.submit.pyFiles`, `spark.files`,
  `spark.driver.extraClassPath`, `spark.executor.extraClassPath`,
  `spark.driver.userClassPathFirst` and `spark.executor.userClassPathFirst`.
- The full set is in
  `src/lakebench/modules/pipeline_engines/spark/conf_keys.py`.

`spark.conf` reaches the pipeline's Spark jobs only, not the Spark Thrift
server or `--local` runs.

The run record keeps it as `architecture.spark_conf_user`:

- Every key is named. Only tuning keys (SQL execution, shuffle, memory,
  speculation and the like) keep their values.
- A key that names a credential, a secret, an endpoint, a location or an
  environment variable (`spark.executorEnv.*`), or whose value names a
  location (a URI or an IP address), is recorded as `<redacted>`.
- Any other value is recorded as `<redacted sha256:...>`, a digest of the
  value. A short value is not protected by the digest: put secrets under a
  secret-named key or an environment variable.
- A per-bucket S3A key records its bucket's layer (`<bronze>`, `<silver>`,
  `<gold>`) or `<other-bucket>` instead of its name.
- The recorded map is the `spark conf` key of the experiment identity. Two
  runs whose recorded maps differ differ in architecture. Two deployments
  that differ only in buckets and endpoints do not differ there. A
  difference in a value recorded as `<redacted>` (an environment
  variable's value, for example) is not in the identity.

Job defaults (`SPARK_CONF_DEFAULTS`), which a `spark.conf` value replaces:

| Key | Default | Description |
|---|---|---|
| `spark.hadoop.fs.s3a.multipart.size` | `268435456` | Multipart upload part size (256 MB). |
| `spark.hadoop.fs.s3a.fast.upload.active.blocks` | `16` | Upload blocks in flight per stream. |
| `spark.hadoop.fs.s3a.attempts.maximum` | `20` | S3 request attempts. |
| `spark.hadoop.fs.s3a.retry.limit` | `10` | S3A retries. |
| `spark.hadoop.fs.s3a.retry.interval` | `500ms` | Wait between retries. |
| `spark.memory.fraction` | `0.8` | Fraction of heap for execution + storage. |
| `spark.memory.storageFraction` | `0.3` | Fraction of `memory.fraction` for storage. |

How 1.6 handled these defaults is in
[UPGRADING-1.7.md](../UPGRADING-1.7.md#sparkconf-defaults-before-17).

## Scale Factors

See [Data generation](data-generation.md) for scale factor details.

`spark.lb.gold.strategy` picks how Customer 360 gold-finalize aggregates
silver:

- `auto` (the default): `simple_agg` below 500 GB of silver, else
  `two_phase_agg`. Or set `simple_agg` or `two_phase_agg`.
- Both rebuild every gold day from all of silver.
- `incremental` and any other value are refused when a command that changes
  data (`deploy`, `run` and the others) loads a Customer 360 config.
  Incremental gold runs only for cycles 2 and later of a multi-cycle run
  (see [Multi-Cycle Batch](#multi-cycle-batch)), and those cycles use it
  whatever the key says.
- Only batch gold-finalize on the cluster reads the key. `--local` runs do
  not pass `spark.conf` and use `auto`.
- The strategy that ran, and why (`auto`, `override` or `cycle`), is
  recorded per gold-finalize job in `metrics.json` as
  `jobs[].extra_metrics.gold_strategy` and `gold_strategy_source`.

Executor counts are covered under [Auto-Sizing](#auto-sizing).

## Multi-Cycle Batch

See [Running pipelines](running-pipelines.md#multi-cycle-batch) for
multi-cycle batch details.

Config fields:

- `architecture.pipeline.cycles` (1 to 50): number of batch iterations.
- `architecture.pipeline.pre_benchmark_maintenance` (boolean, default true):
  run compaction and snapshot expiry before the benchmark after the last
  cycle.

## Timestamp Range Impact

`timestamp_start` and `timestamp_end` set the range of `event_timestamp`
values in generated data. The silver table is partitioned by
`interaction_date` (`to_date(event_timestamp)`), so the range sets the
Iceberg partition count.

| Mode | Recommended Range | Reason |
|------|-------------------|--------|
| Continuous | Days to weeks | Fewer partitions per micro-batch, larger files |
| Batch | Months to years | Single compaction pass handles small files |

- **Continuous: use a narrow range.** Each micro-batch writes a small file
  into every date partition in its data. A 3-year range is about 1,095 date
  partitions, which reaches hundreds of thousands of data files within
  hours and slows Iceberg metadata operations and query planning.
- **Batch: wider ranges are fine.** Compaction runs once after the pipeline
  and merges the small files.

The range also sets the data clock for the `customer_recency_score`
column: `30 - days between the event date and the last day of the range`.

- The last day is the day before `timestamp_end` (the end is exclusive). An
  event on the newest day scores 30, and one 30 days older scores 0.
- With `timestamp_end` unset, the clock is the newest event date in the
  data.
- The score does not depend on the date the pipeline runs, so reruns of the
  same corpus reproduce it.

## Auto-Sizing

When connected to a cluster, Lakebench sizes compute to fit the available
capacity during `deploy`, `info` and `validate`.

1. **Always-on pods** (Trino coordinator and workers, Hive or Polaris,
   PostgreSQL, and `lb-deps` at its 1 CPU / 2 GiB reservation) are sized
   from tier guidance and capped to fit the cluster. They are never raised
   above the tier recommendation.
2. **Datagen** gets the remaining CPU budget.
   - Batch mode (the default) runs datagen and Spark one after the other,
     so datagen gets the full remaining budget.
   - Only the deprecated `pipeline.pattern: streaming` gives datagen 40% of
     it.
   - Continuous mode, with `datagen.cpu` and `parallelism` unset: datagen
     gets the cores that produce the scale's offered load (AML 4 MB/s,
     Customer 360 10 MB/s per scale unit), in pods of up to 8 cores. The
     continuous Spark jobs are capped to a concurrent budget when their
     manifests are built.
   - A `datagen.parallelism` you set is never cut to this budget. The run
     warns, batch pods that do not fit queue, and a continuous run (whose
     datagen pods all run beside the streams) is refused at preflight.
3. **Scales 1 to 50:** resources are only capped downward to fit. Above
   scale 50: [Sizing](sizing.md#reading-the-table).
4. **Trino worker memory** is capped to 85% of the largest node.

Batch Spark executor and driver sizes come from the job profiles; this
sizing does not change them. Counts: [Auto-Scaling](component-spark.md#auto-scaling).
Overrides: [Executor Override Guide](#executor-override-guide).

Continuous streams ([Continuous sizing](sizing.md#continuous-sizing)):

- A stage that needs more than the profile's cap grows its executors to 8,
  then 16 cores, within the largest node. Lakebench then sets its
  `*_executor_cores` and says so.
- gold-refresh is never grown automatically.

`lakebench info <config>` prints the resolved per-job counts. It is hidden
and deprecated, but the only command that prints them.

## Example: Scale 100 (~1 TB)

A config for 1 TB benchmarking. The cluster it needs is the scale-100 row
of the sizing table in [Sizing](sizing.md); `lakebench config show` prints
it for your config.

```yaml
name: lakebench-1tb
recipe: hive-iceberg-spark-trino

workload:
  datagen:
    scale: 100

platform:
  storage:
    s3:
      endpoint: http://<flashblade-data-vip>:80
      access_key: "${LAKEBENCH_S3_ACCESS_KEY}"
      secret_key: "${LAKEBENCH_S3_SECRET_KEY}"
    scratch:
      enabled: true
      storage_class: px-csi-scratch
  compute:
    postgres:
      storage_class: px-csi-db
```

No executor tuning is needed: the job profiles scale executor counts with
the data, and Lakebench sizes datagen and Trino to the cluster. At scale
100 the resolved resources are about:

| Component | Instances | Per-Instance Resources |
|---|---|---|
| Datagen pods | 10+ | 8 CPU, 4Gi (c360) / 8Gi (financial) |
| Bronze-verify executors | 7 (c360) / 11 (financial) | 2 cores, 4g+2g overhead, 50Gi PVC (c360) / 2 cores, 8g+12g overhead, 455Gi PVC (financial: 50 GiB x 100 / 11) |
| Silver-build executors | 18 | 4 cores, 48g+12g overhead, 300Gi PVC (the cap) |
| Gold-finalize executors | 11 | 4 cores, 32g+8g overhead, 300Gi PVC (33 GiB x 100 / 11) |
| Trino workers | 4 | 8 cores, 48Gi |

Recommended timeouts:

```bash
lakebench generate lakebench.yaml --timeout 14400  # 4 hours
lakebench run lakebench.yaml --timeout 7200               # 2 hours
```

Check that the cluster can hold this scale with
`lakebench config recommend lakebench.yaml` before deploying.

## Example: HTTPS Endpoint with Self-Signed CA

For an HTTPS endpoint with a self-signed or private CA (common with
FlashBlade, MinIO and other on-prem object stores), give the CA certificate
PEM file:

```yaml
name: lakebench-https
recipe: polaris-iceberg-spark-trino

platform:
  storage:
    s3:
      endpoint: https://10.0.1.50:443
      access_key: "${LAKEBENCH_S3_ACCESS_KEY}"
      secret_key: "${LAKEBENCH_S3_SECRET_KEY}"
      ca_cert: ./flashblade-ca.pem
      # verify_ssl: true  # default; set false only for dev

```

Polaris recipes need no `client_secret`: `deploy` generates one per
deployment (see [Polaris](component-polaris.md)).

How it works:

- At deploy time, Lakebench reads the PEM file and creates the Kubernetes
  Secret `lakebench-ca-certificate` holding the certificate.
- Each JVM component (Spark, Trino, Polaris, Hive) gets an init container
  that imports the CA into a JKS truststore.
- Python components (datagen, the S3 client) get the PEM path through
  environment variables for boto3.

To get the certificate of a self-signed endpoint:

```bash
openssl s_client -connect 10.0.1.50:443 -showcerts </dev/null 2>/dev/null \
  | openssl x509 -outform PEM > flashblade-ca.pem
```

For a corporate CA, your infrastructure team can provide the PEM file.

AWS S3 and other public endpoints use well-known CAs that the system CA
bundle already trusts. They need no `ca_cert`; set the HTTPS endpoint:

```yaml
platform:
  storage:
    s3:
      endpoint: https://s3.us-east-1.amazonaws.com
```

## Supported Component Combinations

Config load refuses a catalog, table format and query engine combination
that is not supported. The valid recipes are in [Recipes](recipes.md). Each
one's support state per workload and mode, with the refused combinations
and why, is in the [Compatibility Matrix](compatibility-matrix.md).

## Image Overrides

Every image is set under `images` (see the [field reference](#images)).
The limits on Spark, Iceberg, Delta, Hive and Polaris versions are in
[Overriding Versions](compatibility-matrix.md#overriding-versions).

## Generating a Starter Config

`lakebench init` writes a starter config: see
[Getting Started, step 1](getting-started.md#1-generate-a-configuration-file)
for what it writes and [CLI Reference](cli-reference.md#init) for the flags.
Every key it leaves out keeps its default and is described on this page.

### Converting an older config

`lakebench init --from OLD.yaml -o NEW.yaml` rewrites a 1.6 config in the
current format:

- It keeps the name and buckets.
- It moves plaintext secrets to `${VAR}` references.
- It lists every moved or dropped key.
- It writes nothing unless the new file loads to the same settings.

The full rules are in
[UPGRADING-1.7.md](../UPGRADING-1.7.md#converting-a-16-config-with-init---from).

### Recipes and components

A recipe sets `architecture.catalog.type`, `architecture.table_format.type`,
`architecture.pipeline_engine` and `architecture.query_engine.type`.

- A config may leave them out or write the value the recipe sets.
- Any other value is refused at load with both keys named, for example
  `architecture.catalog.type is 'hive' but recipe 'polaris-iceberg-spark-trino'
  sets 'polaris'; delete one of them`.
- The message also names the recipe a 1.6 deployment from that file used
  (1.6 let the written value win). Write that recipe to keep that
  deployment.
- `deploy`, `run` and the other commands that change data refuse such a
  config. `destroy`, `status`, `report` and the inspect commands load it
  with the written value winning, with a note.
- Images and engine resources stay overridable under a recipe.

A config with no `recipe:`, or `recipe: default`, resolves to
`hive-iceberg-spark-trino` when it sets no component, otherwise to the
components it sets. It loads with a deprecation note naming the recipe to
write. v1.8 requires `recipe:`.
