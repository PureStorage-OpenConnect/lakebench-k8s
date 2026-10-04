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

## Minimum Viable Config (v1.3)

The smallest working config:

```yaml
name: my-lakehouse
platform:
  storage:
    s3:
      endpoint: http://your-s3-endpoint:80
      access_key: YOUR_KEY
      secret_key: YOUR_SECRET
workload:
  datagen:
    scale: 10
```

Recipe defaults to `hive-iceberg-spark-trino`.

The name is required by every command that changes data: `deploy`,
`generate`, `run`, `benchmark`, `query`, `clean`, `compare`, `reproduce`,
`financial` and `validate` refuse a config without one, and the error
offers a name to add. (`config upgrade` is removed in v1.7 for every
config; see the CHANGELOG.)

Before v1.7 a nameless config got a time-based name (`lb-YYYYMMDD-HHMMSS`)
written to `.lakebench/state.json` in the config's directory, which every
nameless config in that directory shared. That file is now only read, and
nothing ties the name in it to any one config. The teardown commands
(`destroy`, `stop`, `admin`) and the read-only commands that look at a
deployment (`status`, `logs`, `report`) therefore refuse a
nameless config in a directory that has the file, unless `--name NAME` is
given to `destroy`, `stop`, `status` or `logs`, which then check the
namespace's own stamps (check 3 under "Deploy state and nameless teardown"
below). The error gives the v1.6 name and lists the other nameless `*.yaml`
and `*.yml` configs beside it. To inspect or tear down a deployment v1.6
made, pass `--name` with that name, or add `name:` with that name to the
config that deployed it and use that config. `info`, `config show`,
`config storage` and `config recommend` look at no deployment, so they
still load a nameless config under the v1.6 name.

Without the file, `destroy`, `stop` and `admin` refuse a nameless config
with no `--name`, because no deployment can be its own, and the read-only
commands use a suggested name, `lb-<user>-<6 hex>`, which the error for the
other commands also offers. No command writes `.lakebench/state.json` any
more, and the read-only commands create no files.

The v1.6 name is read from `.lakebench/state.json` beside the path given,
as v1.6 read it, with symbolic links not followed; `--name` is checked
against that name. When a nameless config is reached through a link and the
directory of the file it points to records a different v1.6 name (or the
link's directory records none), every command that may look at a
deployment refuses it without `--name`, so one config cannot act on, or
report, the other directory's deployment. `info`, `config show`, `config
storage` and `config recommend` load it under the link directory's name (a
suggested name when it records none) with a note. Because both directories
share the one file, the fix is not to add `name:` to it: pass `--name`
(when the link's directory records a name, only that name is accepted
through the link; reach the other deployment through the file's own path;
otherwise the namespace's stamps decide), or replace the link with a
copy and name each copy. `init --overwrite` without `--name` refuses such a
file when either directory records a v1.6 name, and `relocate` refuses to
run through a link when either directory records one. The v1.7 deploy
state (`.lakebench/<name>.json`, below) stays with the file the link points
to.

### Deploy state and nameless teardown

Every `deploy` (named configs included) records the per-deploy nonce it is
about to stamp on the namespace in `.lakebench/<name>.json` beside the
config, before the namespace gets it, under a lock file
`.lakebench/<name>.lock`. The file keeps the last five nonces; the one the
namespace carries is never dropped, and the next deploy confirms it. A
`deploy --dry-run` writes no state, lock or `.lakebench/` directory (the
command journal is written as for any command). The directory must be on a
local disk, or deployed from one host only: the lock is host-local, and a
deploy waits at most 120 s for another deploy from the same directory. If
the state cannot be written or read, deploy stops before changing the
cluster (exit 4). A state written for another directory or host (a copied
directory, including a `cp -r` copy at the same path) stops deploy with
exit 3 (`deploy.state_copied`): if the directory was only renamed with
`mv`, run `relocate` (below) from it; if the copy is meant to be a new
deployment directory, remove only its `.lakebench/<name>.json` (the other
files there may be the only record of other deployments). When the config's
`platform.kubernetes.namespace` changes, the next deploy starts a fresh
nonce list for the new namespace. Deployment names that are not plain file
names, and the name `state`, cannot be recorded (exit 4).

`destroy`, `stop`, `status` and `logs` with a nameless config act only on a
deployment the directory can prove is its own, and refuse otherwise
(exit 3), pointing at `lakebench init --from`:

1. It is the only nameless config in its directory, or `--name NAME` is
   given.
2. If `.lakebench/<name>.json` exists, it was written for this directory
   (the same directory, not a copy at the same path) on this host, for the
   config's namespace, has not been moved, and the namespace carries one of
   its nonces. A copied directory, or a namespace redeployed from elsewhere,
   is refused.
3. Otherwise (a v1.6 directory, at most `.lakebench/state.json`): `--name`
   is required and must equal the name in `state.json` when there is one;
   the namespace must carry `lakebench.deployment/name: NAME`, must not
   carry the v1.7 `lakebench.deployment/state-schema` annotation, and its
   `lakebench.deployment/created-buckets` record (or each bucket's ownership
   tag) must name all three of the config's buckets.

Destroy then acts only on the namespace incarnation the check proved; a
redeploy in between stops it before any delete (exit 3,
`destroy.incarnation_mismatch`). For `status` and `logs` a namespace that
does not exist passes the check, and the command reports what it finds as
for any missing deployment. Named configs skip these checks and rely on the
ownership stamps alone.

To move a deployment's config to another directory without breaking check
2, use `python -m lakebench.config.deploy_state relocate CONFIG NEWDIR`
(add `--name NAME` for a nameless config). It copies the config (and a v1.6
`state.json`), writes the state for the new directory and marks the old one
as moved, so only the new directory is accepted from then on. Only the
directory that wrote the state, or that directory renamed with `mv`, can
move it: a copy, or a move to another host, is refused. A v1.6 directory has no v1.7 state to move, so after
relocate both directories can still tear the deployment down with
`--name` through the namespace's own stamps (check 3).

### Removed keys

A key that an earlier release accepted and that now does nothing (for
example `images.pull_secrets` or `platform.storage.scratch.create_storage_class`)
is refused by the commands that change data (the list above), with what
to do instead. `destroy`, `stop`, `status`, `logs`, `report`, `info`,
`config show` and `admin` drop it and print an "Upgrade notes" block on
stderr, so an old config can still be inspected, stopped and torn down.

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
| `platform.storage.scratch.size` | per-job scratch comes from the job profiles (silver-build 300Gi). |
| `secret_ref` | lakebench never reads an existing Secret: deploy writes the S3 Secret from access_key and secret_key. Set those instead. |
| `version` | there is one config schema and nothing read the number; a removed key is named in the upgrade notes instead. |
| `workload.customer360.channels` | The customer360 generator never read it; removed in v1.5. |
| `workload.customer360.date_range_days` | the customer360 generator never read it: the event window is datagen.timestamp_start and datagen.timestamp_end. |
| `workload.customer360.event_types` | The customer360 generator never read it; removed in v1.5. |
| `workload.customer360.quality_distribution` | The customer360 generator never read it; removed in v1.5. |
| `workload.datagen.checkpoint` | The Rust generator never implemented checkpoint-resume; removed 2026-09-28. Re-run generation from the start on failure. |
| `workload.datagen.uploaders` | Never forwarded to the Rust generator; removed 2026-09-28. Uploader concurrency is fixed inside the S3 sink. |
<!-- END GENERATED: config-removed -->

A config that still carries one of them at its old default
(for `description` any text, for `images.hive` any value naming 3.1.3) is
inert and loads under every command with a note; another value is refused as
above. The bronze layout is
fixed (`customer/interactions/`, `pacs008/` for the financial workload), so a
`path_template` naming another layout is refused.

### Flat Fields (deprecated)

v1.3 added flat top-level fields that map to nested locations. They still
load in v1.7, and each one adds a deprecation note naming the nested key to
write instead (for example "flat 'scale' is deprecated; write
workload.datagen.scale"). Write the nested key in new configs:

| Field | Maps to | Default |
|-------|---------|---------|
| `endpoint` | `platform.storage.s3.endpoint` | (required) |
| `access_key` | `platform.storage.s3.access_key` | (required) |
| `secret_key` | `platform.storage.s3.secret_key` | (required) |
| `secret_ref` | `platform.storage.s3.secret_ref` | (removed in v1.7: set `access_key` and `secret_key`) |
| `scale` | `workload.datagen.scale` | 10 |
| `name` | root `name` | (required to change data) |
| `recipe` | root `recipe` | default (hive-iceberg-spark-trino) |
| `namespace` | `platform.kubernetes.namespace` | same as name |
| `mode` | `architecture.pipeline.mode` | batch |
| `cycles` | `architecture.pipeline.cycles` | 1 |
| `spark_image` | `images.spark` | from recipe |

If both flat and nested are present for the same setting, flat takes
precedence and the note says so.

### Error messages

An unknown key names the key it is closest to, first among the keys of its
own section and then among every key in the schema, so a key written in the
wrong section is pointed at the right one:

```text
  - platform.compute.spark.silver_executor: unknown key; did you mean `silver_executors`?
  - platform.storage.scale: unknown key; did you mean `workload.datagen.scale`?
```

An unknown recipe names the nearest recipe, or lists them all; a name whose
every part is a real component (`unity-delta-spark-trino`) is reported as a
combination that is not a recipe, not corrected to a different one. Counts
are bounded (for example `trino.worker.replicas` 1 to 256,
`datagen.generators` 0 to 1024, ports 1 to 65535, every core count at
least 1). A setting that belongs to the other workload,
such as `customer360.unique_customers` or `dirty_data_ratio` under `schema:
financial`, or `tm_operations` or `w1_max_vertices` under `schema:
customer360`, loads with a note saying the workload does not read it. Under
`financial` the note does not ask you to delete it: the corpus id still
hashes the `customer360` fields and `dirty_data_ratio`, so removing one
changes the id of an otherwise identical corpus. `datagen.timestamp_*` gets
no note: the financial generator ignores it, but it sets the silver data
clock for every workload.

### Environment Variable Substitution

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

Unresolved variables without defaults produce one error naming all of them.
Substitution runs value by value, not on the file text. An unquoted value
is trimmed and, unless it carries a tag such as `!!str`, typed as YAML types
it (`0042` is octal 34, `true` a bool, an empty value null), as in v1.6. The
environment value itself is never parsed as YAML: ` #`, quotes or `a: b`
inside it stay text, and its line breaks are not folded. A quoted value
(`"${S3_SECRET}"`) arrives verbatim as a string unless it carries a tag
such as `!!int`, so quote every credential reference. A block scalar keeps the
substituted text inside its own line breaks. A `${VAR}` in a comment is not
read. An unclosed `${VAR:-default` (a default cut short by ` #`) is an
error, and inside flow syntax (`[${A}, ${B}]`) each reference must be
quoted.

### Nested Config (v1.2 Compatible)

The full nested structure still works unchanged:

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
| `observability.enabled` | false | No Prometheus/Grafana |
| `scratch.enabled` | false | emptyDir for shuffle |

**What to tune first as you scale up:**

1. `datagen.scale` -- controls data volume
2. `compute.spark.*_executors` -- per-job executor counts (per-executor
   sizing is fixed in the job profiles; see [Executor Override Guide](#executor-override-guide))
3. `query_engine.trino.worker` -- match replicas/memory to your cluster
4. `scratch` / `postgres` storage classes -- match to your storage provider

## Annotated Example

Below is a complete configuration with every section annotated. Only `name`
and `platform.storage.s3.endpoint` plus S3 credentials are strictly required;
everything else has sensible defaults.

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
  datagen: docker.io/sillidata/lb-datagen:a592385@sha256:48e18a417bf85528392afeb9b8222bfd3cc1d5f3db3bf1d7d0623e6a4f6ea4b1
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
      # FlashBlade HTTP:  http://10.0.0.1:80
      # FlashBlade HTTPS: https://10.0.0.1:443
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
      enabled: false                  # Enable Portworx scratch StorageClass
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

  # Dependency server (lb-deps). deploy resolves the jars and wheels this
  # deployment needs onto a 5Gi PVC in its namespace and serves them there.
  # Every key is optional; the URL keys point the resolve at a mirror for
  # clusters without egress to the public repositories.
  deps:
    maven_repository: ""              # Empty = Maven Central, then its Google mirror
    pypi_index: ""                    # Empty = https://pypi.org/simple/
    duckdb_extension_repository: ""   # Empty = http://extensions.duckdb.org
    storage_class: ""                 # PVC lb-deps-data; empty = cluster default
```

### Executor Override Guide

The per-job executor overrides (`silver_executors`, `gold_executors`, etc.)
replace the auto-scaling formula for one job. Each takes 1 to 28 (the
proven executor ceiling); a larger value is refused by the commands that
change data, and `destroy`, `status` and the read-only commands drop it with
a note. The capacity check, `deploy`, `config show` and `info` count the
override, so a config sized past the cluster is refused before it runs.

An override changes what a run measures:

- It enters the experiment record (`architecture.spark_executor_overrides`;
  the driver overrides as `architecture.spark_driver_overrides`) and the
  identity, as an architecture difference: compare reports two runs with
  different overrides as differing in architecture. They are like-for-like
  only when no override binds one run and not the other (below).
- An override below what the profile asks for at the run's scale binds the
  run. It is labelled in `limits.bound` ("silver-build: executor override 4
  (profile asks 8)") and enters "Lakebench limits that bound".
- A run with any override that differs from the profile's count, or with a
  driver override, is not the proven sizing: it cannot be release evidence or
  a perf-gate baseline, and a pinned perf-gate config must pin each count at
  the profile's count (or leave it unset).

Higher executor counts increase driver memory pressure because the Spark
driver manages per-executor K8s API watches and aggregates serialized task
results. The table below shows tested boundaries:

Profile driver defaults are 4g for bronze-verify and 32g for silver-build
and gold-finalize on Spark 4 (24g on Spark 3). `driver_memory` overrides
all jobs at once, so setting it below 24g shrinks the silver and gold
drivers. Lakebench logs a warning when a job has more than 24 executors and a driver
below 24g.

The capacity check, `config show` and `info` count the driver each job
requests: these overrides, the Spark 3 size, and the overhead Spark adds
to a Python driver pod (40% of the heap).

| Executors | Driver Memory | Status |
|:---------:|:------------:|--------|
| 1--12     | 4g           | Proven stable |
| 13--20    | 4--24g       | Stable; driver_memory override may be needed |
| 21--24    | 24g          | Proven stable with 24g driver |
| 25--28    | 24g          | Safe range; recommended maximum (the job profiles cap auto counts at 28, a Lakebench-imposed ceiling) |
| 29+       | --           | Refused: K8s API polling storms from 32 up |

`spark.driver.maxResultSize` is set automatically based on the effective
executor count: `min(16, max(8, count // 2))` GiB on Spark 4 and
`min(16, max(4, count // 3))` GiB on Spark 3. Override it in `spark.conf`
if needed.

Recommended overrides by cluster size:

| Cluster Cores | Silver Executors | Gold Executors |
|:------------:|:----------------:|:--------------:|
| 32           | 4--6             | 3--4           |
| 64           | 8--12            | 6--8           |
| 128          | 16--20           | 12--16         |
| 256+         | 24--28           | 20--28         |

These assume per-executor sizing from the proven profiles: silver uses
4 cores + 60g (48g + 12g overhead) per executor, gold uses 4 cores +
40g (32g + 8g overhead) per executor.

```yaml
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

    # Continuous pipeline tuning (active when mode: continuous or the --continuous flag).
    # Iterative batch cycles (v1.1.0). Runs N batch iterations where cycle 1
    # is full overwrite and cycles 2-N are incremental append/merge. Simulates
    # multi-day lakehouse table growth. Datagen timestamp range is split evenly
    # across cycles so each cycle adds new data.
    cycles: 1                         # 1 = single batch (default). 2-50 = multi-cycle.
    pre_benchmark_maintenance: true   # Compact + expire before benchmark (recommended)

    continuous:
      bronze_trigger_interval: "30 seconds"
      silver_trigger_interval: "60 seconds"
      gold_refresh_interval: "5 minutes"
      run_duration: 1800              # Seconds (30 min default)
      # max_files_per_trigger: auto (unset) keeps data arriving through the window
      checkpoint_base: checkpoints
      benchmark_interval: 300         # Clamped to gold_refresh_interval at runtime
      benchmark_warmup: 300           # Clamped to gold_refresh_interval at runtime

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

## Complete Field Reference

Every field accepted in the YAML is listed below, organized by section.
Fields marked **(required)** must be provided; everything else has a default.
**first day** marks the keys `lakebench init` writes; the rest are
advanced. This reference is generated from the schema
(`scripts/gen_config_reference.py`); edit a field's description in
`src/lakebench/config/schema.py`, then regenerate.

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
| `images.datagen` | string | `docker.io/sillidata/lb-datagen:a592385@sha256:48e18a417bf85528392afeb9b8222bfd3cc1d5f3db3bf1d7d0623e6a4f6ea4b1` | advanced | Data generator image, pinned by tag and digest (the digest is what is pulled). Output is byte-identical to the v1.6 AML generator freeze (`datagen-v2-rs-0.3`) on the five byte-compare cases; this build adds the held-out seed check, strict argument parsing and per-node corpus markers. |
| `images.spark` | string | `apache/spark:4.1.1-python3` | advanced | Spark runtime image. Unset: the image of the config's recipe (or of the recipe its components name): `4.1.1-python3` on the Hive recipes, `4.0.2-python3` on the Polaris recipes, `hive-delta-spark-thrift` and `hive-delta-spark-none`; 4.0.2 also when the config writes a table format version Spark 4.1 cannot run (Delta 4.0.0). |
| `images.postgres` | string | `postgres:17` | advanced | PostgreSQL image (metadata backend). |
| `images.polaris` | string | `apache/polaris:1.6.0` | advanced | Apache Polaris REST catalog image. |
| `images.polaris_admin_tool` | string | `apache/polaris-admin-tool:1.6.0` | advanced | Polaris admin tool image; bootstraps the Polaris metastore on a Polaris recipe. |
| `images.unity` | string | `unitycatalog/unitycatalog:main` | advanced | Unity Catalog server image (Unity catalog only; no recipe uses it). |
| `images.trino` | string | `trinodb/trino:483` | advanced | Trino query engine image. |
| `images.duckdb` | string | `python:3.11-slim` | advanced | Python image the DuckDB query engine pod runs in; DuckDB itself is pinned by `architecture.query_engine.duckdb.version`. |
| `images.jmx_exporter` | string | `bitnami/jmx-exporter:latest` | advanced | JMX exporter image for the metrics sidecars, used when observability is enabled. |
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
| `platform.storage.s3.create_buckets` | boolean | `true` | advanced | Create buckets if they do not exist. `reproduce` refuses `false`. |

### Platform -- Scratch Storage

Scratch PVCs for Spark shuffle data. Only needed with Portworx or similar CSI.

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `platform.storage.scratch.enabled` | boolean | `false` | advanced | Enable scratch StorageClass for Spark PVCs. |
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
| `architecture.catalog.type` | one of `hive`, `polaris`, `unity`, `none` | `hive` | advanced | Catalog service: `hive`, `polaris`, or `none`. |
| `architecture.catalog.hive.operator.install` | boolean | `false` | advanced | Refused when `true`: deploy never installs the Stackable operators; a cluster admin runs `lakebench admin install --component stackable`. `false` loads as before. |
| `architecture.catalog.hive.operator.namespace` | string | `stackable` | advanced | Namespace for Stackable operators. |
| `architecture.catalog.hive.operator.version` | string | `25.7.0` | advanced | Stackable SDP chart version a fresh `admin install` uses. |
| `architecture.catalog.hive.resources.cpu_min` | string | `500m` | advanced | Hive Metastore minimum CPU request. |
| `architecture.catalog.hive.resources.cpu_max` | string | `2` | advanced | Hive Metastore CPU limit. |
| `architecture.catalog.hive.resources.memory` | string | `4Gi` | advanced | Hive Metastore memory. |
| `architecture.catalog.polaris.port` | integer | `8181` | advanced | Polaris REST API port. |
| `architecture.catalog.polaris.client_secret` | string | `""` | advanced | OAuth2 secret of the `lakebench` Polaris client. Empty = deploy generates one and keeps it in the Secret `lakebench-polaris-client`; a value is written there on the first deploy. Use a `${VAR}` reference rather than a literal. |
| `architecture.catalog.polaris.resources.cpu` | string | `1` | advanced | Polaris CPU request/limit. |
| `architecture.catalog.polaris.resources.memory` | string | `2Gi` | advanced | Polaris memory. |
| `architecture.catalog.unity.spark_connector_version` | string | `0.4.0` | advanced | Unity Catalog Spark connector version the Spark jobs load. |
| `architecture.catalog.unity.port` | integer | `8080` | advanced | Unity Catalog REST API port. |
| `architecture.catalog.unity.resources.cpu` | string | `1` | advanced | Polaris CPU request/limit. |
| `architecture.catalog.unity.resources.memory` | string | `2Gi` | advanced | Polaris memory. |

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
| `architecture.query_engine.duckdb.memory` | string | `4g` | advanced | DuckDB memory. |
| `architecture.query_engine.duckdb.catalog_name` | string | `lakehouse` | advanced | Iceberg catalog name for DuckDB. |
| `architecture.query_engine.duckdb.version` | string | `1.5.5` | advanced | DuckDB version installed at deploy time. Pinned deliberately: an unpinned install takes whatever is current, so two runs weeks apart can query with different engines while `compare` reports the difference as a result. |

### Architecture -- Pipeline

The legacy name `processing` is still accepted with a deprecation warning.

| Field | Type | Default | Tier | Description |
|---|---|---|---|---|
| `architecture.pipeline.pattern` | one of `medallion`, `streaming`, `batch`, `custom` | `medallion` | advanced | **Deprecated; removed in v1.7.** The stages are chosen by `pipeline.mode`. The only remaining effect is that `streaming` makes the auto-sizer give Spark 60% and datagen 40% of the CPU budget; any value other than `medallion` prints a warning. |
| `architecture.pipeline.mode` | one of `batch`, `continuous` | `batch` | advanced | Pipeline execution mode: `batch` (sequential medallion jobs) or `continuous` (concurrent jobs over arriving data). `sustained` is accepted as a deprecated alias. The `--continuous` CLI flag overrides this. |
| `architecture.pipeline.cycles` | integer | `1` | advanced | Batch iterations (1--50). Cycle 1 is full overwrite; cycles 2+ are incremental append/merge. Simulates multi-day lakehouse behavior. Only valid when `mode: batch`. See [Multi-Cycle Batch](#multi-cycle-batch). |
| `architecture.pipeline.pre_benchmark_maintenance` | boolean | `true` | advanced | Run table maintenance before the benchmark phase so QpH is measured against maintained tables. Iceberg: `expire_snapshots`, `remove_orphan_files` (never below 24 h 10 min) and compaction of silver and gold. Delta: `VACUUM` on Trino only; Delta `OPTIMIZE` is never run. All statements share one 30-minute budget; the first statement timeout or the deadline stops the rest, and the perf gate then treats post-maintenance QpH as not a measurement. |
| `architecture.pipeline.continuous.bronze_trigger_interval` | string | `30 seconds` | advanced | Bronze streaming trigger interval. |
| `architecture.pipeline.continuous.silver_trigger_interval` | string | `60 seconds` | advanced | Silver streaming trigger interval. |
| `architecture.pipeline.continuous.gold_refresh_interval` | string | `5 minutes` | advanced | Gold refresh trigger interval. |
| `architecture.pipeline.continuous.run_duration` | integer | `1800` | advanced | Measurement window in seconds. The schema accepts 60 and up; a continuous run refuses less than 3 x `gold_refresh_interval` (900 s at defaults). Use 900 s or more, UAT included. |
| `architecture.pipeline.continuous.checkpoint_base` | string | `checkpoints` | advanced | S3 prefix for streaming checkpoints. |
| `architecture.pipeline.continuous.max_files_per_trigger` | integer or null | auto | advanced | Max Parquet files bronze reads per trigger; with `bronze_trigger_interval` it sets the offered load. Unset: derived per run so data keeps arriving for about 1.2 x `run_duration`, capped at 50 (a Lakebench-imposed cap; 50 files per 30 s is about 107 MB/s, so an auto-capped ingest rate measures the cap, not the infrastructure). An explicit value that would offer the corpus before the window ends is refused at run start. |
| `architecture.pipeline.continuous.bronze_target_file_size_mb` | integer | `512` | advanced | Target Iceberg file size for bronze writes (MB) |
| `architecture.pipeline.continuous.silver_target_file_size_mb` | integer | `512` | advanced | Target Iceberg file size for silver writes (MB) |
| `architecture.pipeline.continuous.silver_bronze_wait_seconds` | integer or null | `null` | advanced | Seconds silver-stream waits for the bronze table to appear before it stops the run. Unset (auto): run_duration / 4, floored at 10 s, so a short run cannot spend its whole window on the wait. The old fixed 1800 was longer than a default run_duration, so the check for a stalled bronze-ingest never fired. |
| `architecture.pipeline.continuous.gold_target_file_size_mb` | integer | `128` | advanced | Target Iceberg file size for gold writes (MB) |
| `architecture.pipeline.continuous.retention_interval` | integer or null | auto | advanced | Seconds between table maintenance rounds during a continuous run: Iceberg `expire_snapshots` + `remove_orphan_files`, or Delta `VACUUM` (Trino only). Unset: `run_duration / 3`, within 300--7200, resolved at run start (600 s for the default 1800 s window), so a default run maintains inside its window. An explicit value too long for the first round to run inside the window is refused at run start unless `--skip-maintenance` is given. While streams are live Delta `VACUUM` keeps Delta's 7-day default retention, so continuous Delta has no effective table maintenance in v1.6. Range: 300--7200. |
| `architecture.pipeline.continuous.retention_threshold` | string | `30m` | advanced | Iceberg snapshot retention threshold. Snapshots older than this are expired. A whole number and one unit, `s`, `m`, `h` or `d` (e.g., `30m`, `1h`, `7d`); anything else is rejected at load. While streams are live, Iceberg expiry is floored at `1h`, and a continuous Iceberg config on Trino or Spark Thrift that sets a lower value prints a warning when it loads. The `30m` default does not warn: every continuous maintenance round runs beside live streams, so it expires at `1h`, and the run records the applied values in `continuous.retention` of `metrics.json`. Delta has no effective table maintenance in continuous mode (v1.6). Orphan-file removal never uses less than 24 h 10 min, on any engine. |
| `architecture.pipeline.continuous.compaction_enabled` | boolean | `true` | advanced | Run periodic Iceberg compaction (`rewrite_data_files` / `optimize`) during continuous runs. |
| `architecture.pipeline.continuous.compaction_interval` | integer | `0` | advanced | Seconds between compaction rounds. `0` = 2x the effective `retention_interval` (1200 s for the default window). An explicit value that cannot run inside the window is refused at run start unless `compaction_enabled` is false or `--skip-maintenance` is given. Minimum 0; no upper bound in the schema. |
| `architecture.pipeline.continuous.benchmark_interval` | integer | `300` | advanced | Seconds between in-stream benchmark rounds. Clamped to `gold_refresh_interval` at runtime -- intervals shorter than the gold cycle cause Q9 contention. Range: 300--3600. |
| `architecture.pipeline.continuous.benchmark_warmup` | integer | `300` | advanced | Seconds before first in-stream benchmark round. Clamped to `gold_refresh_interval` at runtime -- rounds before the first gold refresh produce inflated QpH. Range: 300--1800. |

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
| `workload.datagen.parallelism` | integer | auto (by scale and cluster) | advanced | Number of parallel datagen pods. A value you set is used as given, except that it is capped to fit the cluster and financial above scale 100 is raised to at least 8 pods; unset, the auto-sizer derives it from the scale. The schema fallback without auto-sizing is 4. |
| `workload.datagen.file_size` | one of `64mb` | `64mb` | advanced | Fixed at `64mb` for every workload and mode; any other value is refused. One size keeps row content identical across delivery modes. |
| `workload.datagen.dirty_data_ratio` | number | `0.08` | advanced | Fraction of intentionally dirty records (0.0--1.0). Applies to the `customer360` schema only; the `financial` (AML) generator ignores it. |
| `workload.datagen.cpu` | string | auto (by scale and cluster) | advanced | CPU per datagen pod. A value you set is used as given; unset, the auto-sizer sets 8 in both modes. |
| `workload.datagen.memory` | string | auto (by workload, scale and cpu) | advanced | Memory per datagen pod. A value you set is used as given; unset, the auto-sizer derives it from the measured peak RSS model for the schema, scale, pod CPU (thread count) at the fixed 64mb file size, with a 4Gi floor. |
| `workload.datagen.generators` | integer | `0` | advanced | Generator threads per pod. 0 = auto: the entrypoint sizes threads from the pod's CPU request. |
| `workload.datagen.timestamp_start` | string or null | `null` | advanced | Start date for generated timestamps (ISO format). Default: `2024-01-01`. See [Timestamp Range Impact](#timestamp-range-impact). |
| `workload.datagen.timestamp_end` | string or null | `null` | advanced | End date for generated timestamps (ISO format, exclusive). Default: `2025-01-01` for single-cycle runs (Rust generator built-in). Multi-cycle runs (`cycles > 1`) split a wider `2024-01-01` to `2025-12-31` default window across cycles (`config/c360_run.py` `cycle_windows`, which the datagen deployer and `metrics/c360_correctness.py` both read). See [Timestamp Range Impact](#timestamp-range-impact). |

### Workload -- Customer 360 and AML

Advanced workload overrides. Most users should leave these at defaults and control volume via `datagen.scale`. The AML TM operations layer (`workload.tm_operations.*`) is described in [aml-scoring.md](aml-scoring.md#the-transaction-monitoring-operations-layer).

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
| `architecture.benchmark.maintenance_settle.enabled` | boolean | `true` | advanced | Batch mode: probe until storage settles between maintenance and the post-maintenance round. See [benchmarking.md](benchmarking.md). |
| `architecture.benchmark.maintenance_settle.max_seconds` | integer | `2700` | advanced | Longest wait. When reached, the post round still runs and `maintenance_value_pct` is null. Range: 60--14400. |
| `architecture.benchmark.maintenance_settle.interval_seconds` | integer | `60` | advanced | Seconds between the starts of consecutive probes. Range: 5--3600. |
| `architecture.benchmark.maintenance_settle.tolerance_pct` | number | `10.0` | advanced | Settled when two consecutive probes differ by at most this percent and neither is slower than the pre-maintenance time by more. Range: above 0, up to 100. |
| `architecture.benchmark.maintenance_settle.probe_query` | string or null | `null` | advanced | Benchmark query name to probe with. Default: the workload's first scan-class query (a full scan of the table compaction rewrote) |
| `architecture.benchmark.maintenance_settle.probe_samples` | integer | `1` | advanced | Timed runs per probe; the probe time is their median. Range: 1--10. |
| `architecture.benchmark.investigator_sessions` | integer or null | `null` | advanced | AML continuous only: concurrent investigator sessions run once, as an extra round after the first in-stream round that had a case, each working one case of the run (IQ1 to IQ4). Range: 1--32. Unset runs no such round. Refused at load unless the workload is `financial`, `tm_operations.enabled` is true and the query engine is `trino` or `spark-thrift`, and by `run` unless the run is continuous. The sessions that ran (fewer when the run has fewer cases) are an outcome condition: two runs that differ in it compare as not like-for-like. |

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
namespace. It resolves the Maven jars (and, for the AML workload, the
reference detector's Python wheels; for DuckDB, its wheel and extension
files) onto the PVC `lb-deps-data`, records a sha256 for every file, and
serves the set read-only inside the namespace. The set is resolved again
only when the request changes: the Spark or DuckDB image tag, the table
format or its version, the workload (AML adds the reference wheels), the
query engine or the DuckDB version, a mirror key, or the resolver shipped
with Lakebench. The request names images by tag; the manifest records the
digest each container actually ran.

Mirrors are read anonymously, over plain HTTP or over HTTPS with a publicly
trusted certificate. A mirror that serves the same bytes yields the same
dependency set hash.

## Spark Configuration Overrides

`spark.conf` holds your own Spark keys. Each job's conf is built in three
layers, later ones winning: the job defaults below, then `spark.conf`, then
the keys Lakebench sets for the job (per-job shuffle partitions and result
size, the catalog and S3A connection settings, jars, adaptive execution, UI
and Kubernetes settings). A `spark.conf` key Lakebench sets for the job is
refused at load by the commands that change data, naming the setting that
controls it instead (for example `spark.sql.shuffle.partitions` follows the
executor count, `platform.compute.spark.<job>_executors`); `destroy`, `status`
and the read-only commands drop it with a note. `spark.driver.maxResultSize`
is the one Lakebench-set key a user value replaces. Also refused: keys a
job script sets (`spark.sql.session.timeZone` and others), and the reserved
`spark.kubernetes.*`, `spark.jars.*` and pod-sizing keys (executor and
driver memory, overhead, cores, off-heap, PySpark memory). The jar keys
are all Lakebench's, because every job takes its jars, in order, from the
deployment's dependency set: `spark.jars`, `spark.jars.*`,
`spark.submit.pyFiles`, `spark.files`, `spark.driver.extraClassPath`,
`spark.executor.extraClassPath`, `spark.driver.userClassPathFirst` and
`spark.executor.userClassPathFirst`. The full set is in
`src/lakebench/modules/pipeline_engines/spark/conf_keys.py`. `spark.conf`
reaches the pipeline's Spark jobs only, not the Spark Thrift server or
`--local` runs. The run record keeps it as `architecture.spark_conf_user`:
every key is named, but only tuning keys (SQL execution, shuffle, memory,
speculation and the like) keep their values. A key that names a
credential, a secret, an endpoint, a location or an environment variable
(`spark.executorEnv.*`), or whose value names a location (a URI or an IP
address), is recorded as `<redacted>`. Any other value is recorded as
`<redacted sha256:...>`, a digest of the value, which a short value does
not protect: put secrets under a secret-named key or an environment
variable. A per-bucket S3A key records its bucket's layer (`<bronze>`,
`<silver>`, `<gold>`) or `<other-bucket>` instead of its name. The
recorded map is the `spark conf` key of the experiment identity, so compare
reports two runs whose recorded maps differ as an architecture difference,
while two deployments that differ only in buckets and endpoints do not
differ there. A difference
in a value recorded as `<redacted>` (an environment variable's value, for
example) is not seen by compare.


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

Before v1.7 these defaults were the schema default of `spark.conf`, so
setting any key there dropped all of them; they now stay. A config that still
carries the v1.6 default map loads with a note for the Lakebench-set keys in
it (they were overwritten then too).

## Scale Factors

The `datagen.scale` field is an abstract multiplier: one scale unit is about
10 GB of bronze Parquet. What a scale unit generates is per workload: for
Customer 360 the customer id space, file count and row count at each scale
are in [the Customer 360 spec, section 3](benchmarks/C360.md#3-data-generation),
and [Data Generation](data-generation.md) covers the `generate` command and
both workloads.

Datagen scale is banded per workload. Customer 360 is supported up to scale
300 and unverified up to 600; AML (financial) is supported up to 300 and
unverified up to 800. Above the ceiling `deploy` and `generate` refuse the
config, because a datagen pod would exceed the 16 GiB per-pod memory cap
(a Lakebench-imposed cap); in the unverified range they warn. The run's
support state records the band.

`spark.lb.gold.strategy` picks how Customer 360 gold-finalize aggregates
silver: `auto` (the default: `simple_agg` below 500 GB of silver, else
`two_phase_agg`), `simple_agg` or `two_phase_agg`. Both rebuild every gold
day from all of silver. `incremental` and any other value are refused
when a command that changes data (`deploy`, `run` and the others) loads a
Customer 360 config: incremental gold runs only for cycles 2 and later of
a multi-cycle run (see [Multi-Cycle Batch](#multi-cycle-batch)), and those
cycles use it whatever the key says. The key is read by batch
gold-finalize on the cluster only; `--local` runs do not pass `spark.conf`
and use `auto`. The strategy that ran, and why (`auto`, `override` or
`cycle`), is recorded per gold-finalize job in `metrics.json` as
`jobs[].extra_metrics.gold_strategy` and `gold_strategy_source`.

Executor counts auto-scale with the scale factor unless overridden by the
`bronze_executors`, `silver_executors`, or `gold_executors` fields. Per-executor
sizing (cores, memory, PVC size) is fixed from proven production profiles and
does not change with scale.

## Multi-Cycle Batch

`architecture.pipeline.cycles` (1 to 50) runs a batch run as N cycles, each
over its own slice of the event window, to model a table that receives daily
loads. It is refused with continuous mode, and `run --generate` (except with
`--local`), `run --generate-only` and `lakebench generate` are refused with
it, because each cycle generates its own slice; `run --skip-generate` reuses a finished
multi-cycle corpus of the same config (checked against its corpus series
marker), except for AML. Cycle 1 creates silver
and gold; cycles 2 and later append to silver and run gold-finalize
incrementally (`LB_SILVER_INCREMENTAL` and `LB_GOLD_INCREMENTAL`), the only
case in which gold-finalize runs incrementally. Table health is probed after
each cycle and recorded in `cycles[].table_health` as
`silver_data_file_count`, `gold_data_file_count`, `silver_snapshot_count`
and `gold_snapshot_count`. Delta records the file counts only; the record
is empty on DuckDB, with no query engine, or where the engine cannot count
Delta files, and a probe query that fails leaves its key out. How a multi-cycle run proceeds and what it records
is in [Running Pipelines](running-pipelines.md#multi-cycle-batch); what it
does to the corpus is in
[the Customer 360 spec, section 3](benchmarks/C360.md#3-data-generation).

## Timestamp Range Impact

The `timestamp_start` and `timestamp_end` fields control the range of
`event_timestamp` values in generated data. This range directly affects
Iceberg partition count because the silver table is partitioned by
`interaction_date` (derived from `event_timestamp` via `to_date()`).

**In continuous mode, use a narrow range (days to weeks).** Each streaming
micro-batch writes small files across every date partition that appears in
its data. A wide range (e.g., 3 years = ~1,095 date partitions) causes
massive small-file proliferation -- each micro-batch creates a tiny file
per date partition, leading to hundreds of thousands of data files within
hours. This degrades Iceberg metadata operations and query planning.

**In batch mode, wider ranges are fine.** Compaction runs once after the
pipeline completes, consolidating small files.

The range also sets the data clock for the `customer_recency_score` derived
column: `30 - days between the event date and the last day of the range`.
The last day is the day before `timestamp_end` (the end is exclusive), so
an event on the newest day scores 30 and one 30 days older scores 0. When
`timestamp_end` is unset, the clock is the newest event date in the data.
The score no longer depends on the date the pipeline runs, so reruns of the
same corpus reproduce it.

| Mode | Recommended Range | Reason |
|------|-------------------|--------|
| Continuous | Days to weeks | Fewer partitions per micro-batch, larger files |
| Batch | Months to years | Single compaction pass handles small files |

## Auto-Sizing

When connected to a Kubernetes cluster, Lakebench auto-sizes compute resources
to fit available capacity. This happens transparently during `deploy`, `info`,
and `validate`.

The algorithm:

1. **Always-on pods** (Trino coordinator + workers, Hive/Polaris, PostgreSQL,
   and the `lb-deps` dependency server at its 1 CPU / 2 GiB reservation)
   are sized from tier guidance and capped to fit the cluster. They are never
   boosted beyond the tier recommendation.
2. **Datagen** gets the remaining CPU budget.
   - In **batch mode** (default), datagen and Spark run sequentially, so
     datagen gets the full remaining budget.
   - Only the deprecated `pipeline.pattern: streaming` gives datagen 40% of
     it. Continuous mode (`--continuous`) does not change that; the
     continuous Spark jobs are capped to a concurrent budget when their
     manifests are built.
3. **Small scales (1--50):** Resources are only capped downward to fit.
4. **Large scales (51+):** datagen parallelism is scaled up to use
   available cluster capacity.
5. **Trino worker memory** is capped to 85% of the largest node.

Spark executor and driver sizing is not auto-sized: it is the job profiles.

Per-job executor counts do not come from the auto-sizer. Each job takes a
base count from its job profile and adds executors linearly above scale 10,
up to the profile's maximum (28 at most, a Lakebench-imposed ceiling, not a
cluster limit). A `platform.compute.spark.*_executors` value replaces the
computed count for that job (e.g., `spark.bronze_executors: 4`). Use
`lakebench info <config>` (hidden and deprecated, but the only command that prints the per-job counts) to see the resolved per-job counts.

## Example: Scale 100 (~1 TB)

A production-scale config for 1 TB benchmarking. The cluster it needs is
the scale-100 row of the sizing table in
[Getting Started](getting-started.md#kubernetes-cluster) (`lakebench config show`
prints it for your config):

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
      access_key: <key>
      secret_key: <secret>
    scratch:
      enabled: true
      storage_class: px-csi-scratch
  compute:
    postgres:
      storage_class: px-csi-db
```

No executor tuning needed: the job profiles scale executor counts with the
data, and the auto-sizer sizes datagen and Trino. At scale 100 the resolved
resources are approximately:

| Component | Instances | Per-Instance Resources |
|---|---|---|
| Datagen pods | 10+ | 8 CPU, 4Gi (c360) / 8Gi (financial) |
| Bronze-verify executors | 7 (c360) / 11 (financial) | 2 cores, 4g+2g overhead, 50Gi PVC (c360) / 2 cores, 8g+12g overhead, 500Gi PVC (financial, LB-118) |
| Silver-build executors | 18 | 4 cores, 48g+12g overhead, 300Gi PVC |
| Gold-finalize executors | 11 | 4 cores, 32g+8g overhead, 300Gi PVC |
| Trino workers | 4 | 8 cores, 48Gi |

Recommended timeouts:

```bash
lakebench generate lakebench.yaml --timeout 14400  # 4 hours
lakebench run lakebench.yaml --timeout 7200               # 2 hours
```

Use `lakebench config recommend lakebench.yaml` to verify your cluster can handle
this scale before deploying.

## Example: HTTPS Endpoint with Self-Signed CA

When your S3 endpoint uses HTTPS with a self-signed or private CA certificate
(common with FlashBlade, MinIO, and on-prem object stores), provide the CA
certificate PEM file:

```yaml
name: lakebench-https
recipe: polaris-iceberg-spark-trino

platform:
  storage:
    s3:
      endpoint: https://10.0.1.50:443
      access_key: <key>
      secret_key: <secret>
      ca_cert: ./flashblade-ca.pem
      # verify_ssl: true  # default; set false only for dev

```

Polaris recipes need no `client_secret`: `deploy` generates one per
deployment (see [Polaris](component-polaris.md)).

**How it works:** At deploy time, lakebench reads the PEM file and creates a
Kubernetes Secret (`lakebench-ca-certificate`) containing the certificate.
Each JVM component (Spark, Trino, Polaris, Hive) gets an init container that
imports the CA into a JKS truststore. Python components (datagen, S3 client)
receive the PEM path via environment variables for boto3.

**Obtaining the certificate:** For self-signed endpoints, extract the CA
certificate using `openssl`:

```bash
openssl s_client -connect 10.0.1.50:443 -showcerts </dev/null 2>/dev/null \
  | openssl x509 -outform PEM > flashblade-ca.pem
```

For corporate CAs, your infrastructure team can provide the PEM file.

**AWS S3 and public endpoints** use well-known CAs that are already trusted
by the system CA bundle. No `ca_cert` is needed -- just use the HTTPS endpoint:

```yaml
platform:
  storage:
    s3:
      endpoint: https://s3.us-east-1.amazonaws.com
```

## Supported Component Combinations

Config load refuses a catalog, table format and query engine combination
that is not supported. The valid recipes are in [Recipes](recipes.md), and
each one's support state per workload and mode, with the refused
combinations and why, is in the
[Compatibility Matrix](compatibility-matrix.md).

## Image Overrides

Every image is set under `images` (see the [field reference](#images));
the limits on Spark, Iceberg, Delta, Hive and Polaris versions are in
[Overriding Versions](supported-components.md#overriding-versions).

## Generating a Starter Config

`lakebench init` writes a starter config: see
[Getting Started, step 1](getting-started.md#1-generate-a-configuration-file) for what it writes and
[CLI Reference](cli-reference.md#init) for the flags. Every key it leaves
out keeps its default and is described on this page.

### Converting an older config

`lakebench init --from OLD.yaml -o NEW.yaml` rewrites a 1.6 config in the
current format. It reads OLD as text, so a `${VAR}` reference is copied as
written (quoted or not) and never expanded, and it never writes over OLD
(`-o` naming OLD, or a link to it, exits 2). It:

- keeps the deployment's name: OLD's `name:`, or for a nameless config the
  name 1.6 recorded in `.lakebench/state.json` beside the path given, where
  1.6 read it. For a link, the file beside its target is read too: two
  different names, or a name only beside the target, exit 3, as `destroy`
  and `status` refuse that config without `--name`. When other nameless configs share that directory, 1.6 gave all
  of them that name, so it exits 3 until `--name` says which deployment
  this file made. A `--name` that differs from the recorded name is
  printed with it. With neither, the new file gets a new name, and the
  output says to convert again with `--name` if the config deployed
  something;
- writes out the bucket names 1.6 and 1.7 derive from the name
  (`<name>-bronze` and so on), so a later rename cannot move them. A config
  last deployed by 1.5 or earlier, with no buckets set, used
  `lakebench-bronze`, `-silver` and `-gold`: set those in NEW to keep them;
- moves the old spellings to the current keys: flat top-level keys
  (`endpoint:`, `scale:` and the rest), `architecture.workload` to
  `workload`, `architecture.processing` to `architecture.pipeline`,
  `pipeline.sustained` to `pipeline.continuous` and `mode: sustained` to
  `continuous`. A recipe that contradicts the components written becomes
  the recipe of those components, which is what 1.6 deployed, and a config
  with no recipe gets the one it resolves to, unless that recipe's
  defaults would change a setting;
- drops every removed key, both `operator.install` keys and the
  `spark.conf` keys at the 1.6 defaults Lakebench overwrote anyway, each
  with what to do instead, and drops `benchmark.streams` when it holds the
  default 4 (1.6 saved configs wrote it, and `run` refuses it written out).
  A `medallion` block that moved the bronze layout is kept: 1.6 read it,
  1.7 cannot, and `deploy` and `run` refuse it;
- replaces a plaintext credential (`access_key`, `secret_key`,
  `client_secret`, or a `spark.conf` password, token or key) with a
  `${VAR}` reference: `${LAKEBENCH_S3_ACCESS_KEY}` and
  `${LAKEBENCH_S3_SECRET_KEY}` for the S3 keys (`--credentials-env`
  renames them) and `${LAKEBENCH_POLARIS_CLIENT_SECRET}` for Polaris. The
  value is never printed, and NEW is no more readable than OLD. When one
  of those variables is already set in the shell, the output says so.

Before writing, it loads OLD (under the name above) and the new file the
way `status` would, every referenced variable set to its own placeholder
(or, when that cannot load, to its value in this shell), and writes nothing
(exit 3) unless the two give the same settings and the same planned
experiment, apart from the moved secrets. This checks the rewrite, not the
name it chose. It prints every moved, dropped or derived key, and then
anything `run` still refuses in the new file (an executor override above
28, `benchmark.mode: throughput`; `deploy` refuses all but the benchmark
settings), which it leaves for you to change. `--overwrite` replaces an
existing NEW, and refuses (exit 3) when that file resolves to the same
deployment in another namespace, bucket, endpoint or recipe, or is a
nameless config in a directory 1.6 recorded a name for. Comments in OLD
are not carried over.

### Recipes and components

A recipe sets `architecture.catalog.type`, `architecture.table_format.type`,
`architecture.pipeline_engine` and `architecture.query_engine.type`. A
config may leave them out or write the value the recipe sets; any other
value is refused at load with both keys named, for example
`architecture.catalog.type is 'hive' but recipe 'polaris-iceberg-spark-trino'
sets 'polaris'; delete one of them`. The message also names the recipe a
v1.6 deployment from that file used (v1.6 let the written value win), so a
deployment made from it can be kept by writing that recipe. `deploy`, `run`
and the other commands that change data refuse such a config; `destroy`,
`status`, `report` and the inspect commands load it as v1.6 did, with a
note. Images and engine resources stay overridable under a recipe.

A config with no `recipe:`, or `recipe: default`, resolves as in v1.6: to
`hive-iceberg-spark-trino` when it sets no component, otherwise to the
components it sets. It loads with a deprecation note naming the recipe to
write; v1.8 requires `recipe:`.
