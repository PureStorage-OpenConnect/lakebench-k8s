# Spark

Reference: configure and size the Apache Spark pipeline engine: Spark Operator, config keys, job profiles, executor scaling, scratch storage, RBAC and OpenShift SCC.

## What it does

Spark is the compute engine for the medallion pipeline in every recipe ([Recipes](recipes.md)). Lakebench submits `SparkApplication` resources through the Kubeflow Spark Operator v2.x. Spark runs PySpark scripts:

- **Batch mode:** `bronze-verify`, `silver-build`, `gold-finalize` run one after another.
- **Continuous mode:** `bronze-ingest`, `silver-stream`, `gold-refresh` run at the same time as structured streaming jobs with configurable trigger intervals.

### Script ConfigMaps

Pipeline scripts are deployed as ConfigMaps (one per role), projected together at `/opt/spark/scripts` in every Spark pod. `run` validates them before submitting jobs and refuses to start if a listed file is missing or a map exceeds the ~840KB ConfigMap limit. `destroy` deletes all the maps.

## Spark Operator

Lakebench requires **Kubeflow Spark Operator v2.x** (default: [version matrix](compatibility-matrix.md#component-version-matrix)). The v1.x line breaks volume injection and is not supported.

- v1.x does not inject volumes from `spec.volumes` into pods.
- The operator version is not the Spark runtime version.
- `mainApplicationFile` uses `local://` URIs, so scripts must be in the container filesystem.

`lakebench deploy` checks the operator. It never installs the operator; a missing one fails the deploy.

- `deploy` adds the deployment's namespace to the operator's watch list (`spark.jobNamespaces`) and `destroy` removes it, both under the `lakebench-cluster-lock` lease in `lakebench-system`. Do not edit `spark.jobNamespaces` by hand.
- An installed operator keeps its version whatever the config says.
- `operator.install: true` is refused by the commands that change data; `destroy`, `status` and `admin` load it as false.

A cluster admin installs the shared operator once with Lakebench, not raw Helm:

```bash
lakebench admin install --component spark-operator lakebench.yaml \
  --version spark-operator=<version> \
  --controller-tmp-size 8Gi
```

- `--version` defaults to `platform.compute.spark.operator.version`. The namespace is `platform.compute.spark.operator.namespace`.
- `--controller-tmp-size` and the version rules: [CLI reference](cli-reference.md) (`admin install`, `admin repair-operator`).
- On OpenShift, Lakebench grants the `anyuid` SCC to the `spark-operator-controller` and `spark-operator-webhook` service accounts.
- It also patches `fsGroup` and `seccompProfile` out of the operator Deployments. The chart hardcodes them and Helm values cannot remove them.

```yaml
platform:
  compute:
    spark:
      operator:
        namespace: "spark-operator"  # Where the operator runs
        version: "2.5.1"            # Chart a fresh admin install uses; must be v2.x
```

## Version and image

`images.spark` defaults to the recipe's image ([version matrix](compatibility-matrix.md#component-version-matrix)).

- A config with no recipe takes the image of the recipe its components name, so a recipe-less Polaris config gets the Polaris recipes' image.
- The Spark 4.0 image is also chosen when the config sets a table format version Spark 4.1 cannot run (Delta 4.0.0).
- Supported: Spark 3.5.x, 4.0.x and 4.1.x. Spark 4.2 is not: no Iceberg release ships a runtime that works with it.
- The tag must contain `-python3`. Unsupported versions, non-Python images and unparseable tags are rejected at config load.
- Format versions, Hadoop AWS, runtime jars and the Java 17 rule for Iceberg 1.11.0: [Spark + Table Format Version Matrix](compatibility-matrix.md#spark--table-format-version-matrix).

The Spark version parsed from the tag also picks:

| Spark | AWS SDK | Driver memory (silver/gold) |
|-------|---------|---|
| 3.5.x | 1.12.262 | 24g |
| 4.0.x, 4.1.x | 1.12.720 | 32g |

Spark 4 drivers get 32g: `hadoop-aws:3.4.1` pulls a 558MB AWS SDK v2 bundle (280MB SDK v1 on 3.5.x), and the driver's netty file server serves those jars to executors.

`lb-deps` resolves the runtime and Hadoop AWS jars and serves them by URL. Example for a Java 11 Spark 3.5 image such as `apache/spark:3.5.4-python3` on Hive: it gets `iceberg-spark-runtime-3.5_2.12:1.10.1` and `hadoop-aws:3.3.4`. Other Spark minors: [which runtime jar](compatibility-matrix.md#which-runtime-jar-gets-requested).

```yaml
spark.jars: http://lb-deps.<namespace>.svc.cluster.local:8080/sets/<pinset>/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar,...
spark.sql.catalog.lakehouse: org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type: hive
spark.sql.catalog.lakehouse.uri: thrift://lakebench-hive-metastore:9083
```

## Configuration keys

Spark settings live under `platform.compute.spark`, `platform.storage.scratch`, `images.spark` and the top-level `spark` section.

| Key | Default | Effect |
|---|---|---|
| `platform.compute.spark.driver_cores` | `null` | Replaces the profile's driver cores for every job. At most 16. |
| `platform.compute.spark.driver_memory` | `null` | Replaces the profile's driver memory for every job (e.g. `16g`). Must be a whole number with unit k, m, g or t. Use on small nodes or at scale 500+, where the driver holds more Iceberg commit metadata. |
| `bronze_executors`, `silver_executors`, `gold_executors` | `null` | Batch executor count. Set, it bypasses the scaling formula for that job. |
| `bronze_ingest_executors`, `silver_stream_executors`, `gold_refresh_executors` | `null` | Continuous executor count. Set, it bypasses the scaling formula and the shared-cluster Lakebench cap. |
| `bronze_ingest_executor_cores`, `silver_stream_executor_cores`, `gold_refresh_executor_cores` | `null` | Continuous executor size, 1-16 cores. Memory and scratch per core follow the profile; the count is unchanged, so total cores grow. |
| `platform.storage.scratch.enabled` | `false` | Portworx scratch PVCs for executors. Unset, a batch run at scale 50 and above turns it on. |
| `platform.storage.scratch.storage_class` | `px-csi-scratch` | Scratch StorageClass. Must be `repl=1`. |
| `spark.conf` | `{}` | Spark conf merged over the job defaults (below). |

- Executor count overrides above 28 are refused by the commands that change data; `destroy` and read commands drop them with a note.
- Unset `*_executor_cores`: bronze-ingest and silver-stream grow to 8 or 16 cores when the offered load needs more executors than the cap (keeping total cores), unless their `*_executors` count is set. gold-refresh is never grown.
- Batch jobs take only a count override. The removed `platform.compute.spark.driver` and `.executor` blocks and `scratch.size` are listed in [UPGRADING-1.7.md](../UPGRADING-1.7.md#sizing-settings-that-sized-nothing-are-removed).

### Spark conf (S3A, shuffle, memory)

```yaml
spark:
  conf:
    spark.speculation: "true"            # your keys, merged over the defaults
    spark.hadoop.fs.s3a.retry.limit: "20"  # replaces a job default
```

Each job's conf is the job defaults, then `spark.conf`, then the keys Lakebench sets. Lakebench defaults, which `spark.conf` can replace:

| Key | Default |
|---|---|
| `spark.hadoop.fs.s3a.multipart.size` | `268435456` (256 MB) |
| `spark.hadoop.fs.s3a.fast.upload.active.blocks` | `16` |
| `spark.hadoop.fs.s3a.attempts.maximum` | `20` |
| `spark.hadoop.fs.s3a.retry.limit` | `10` |
| `spark.hadoop.fs.s3a.retry.interval` | `500ms` |
| `spark.memory.fraction` | `0.8` |
| `spark.memory.storageFraction` | `0.3` |

Keys Lakebench sets for every job (`LAKEBENCH_OWNED_SPARK_KEYS`). The commands that change data refuse a `spark.conf` that sets one, naming what controls it:

| Key | Value that runs |
|---|---|
| `spark.hadoop.fs.s3a.connection.maximum` | `200` |
| `spark.hadoop.fs.s3a.threads.max` | `100` |
| `spark.hadoop.fs.s3a.fast.upload` | `true` |
| `spark.hadoop.fs.s3a.fast.upload.buffer` | `bytebuffer` |
| `spark.hadoop.fs.s3a.multipart.threshold` | `268435456` |
| `spark.hadoop.fs.s3a.max.total.tasks` | `200` |
| `spark.hadoop.fs.s3a.block.size` | `268435456` |
| `spark.hadoop.fs.s3a.connection.timeout` | `60000` |
| `spark.sql.shuffle.partitions`, `spark.default.parallelism` | at scale 10 or below the profile's `base_partitions`, unless an executor override raises the count above the profile default; otherwise `executor_count * cores * 2` |
| catalog, jar (`spark.jars`, `spark.jars.*`, `spark.submit.pyFiles`), adaptive-execution, stability, Parquet, Spark UI and `spark.kubernetes.*` keys | set per job |

Two keys a user may set (`USER_OVERRIDABLE_SPARK_KEYS`): Lakebench sets `spark.driver.maxResultSize` from the executor count only when `spark.conf` does not; `spark.redaction.regex` keeps a user value with Lakebench's terms in front.

## Job Profiles

The built-in profiles set per-executor sizing, proven at 1TB+ scale. Reducing these values causes OOM kills or "No space left on device" failures.

### Batch Jobs

| Job | Executor cores | Executor memory | Overhead | Scratch PVC | Driver (Spark 3) | Driver (Spark 4) |
|---|---|---|---|---|---|---|
| `bronze-verify` | 2 | 4g (8g financial) | 2g (12g financial) | 50Gi (c360); financial: 50 GiB x scale / executors, 50-500Gi | 4g | 4g |
| `silver-build` | 4 | 48g | 12g | 60 GiB x scale / executors, 50-300Gi | 24g | 32g |
| `gold-finalize` | 4 | 32g | 8g | 33 GiB x scale / executors, 50-300Gi | 24g | 32g |

- Financial `bronze-verify` also raises `executors_per_100_scale` 4 -> 8 and `max_executors` 20 -> 28 for its CTAS fallback, which rewrites the full pacs.008 source. It registers bronze with zero-copy `add_files` at every scale; CTAS runs only when `add_files` fails or the source exceeds 1.5 TiB or 800,000 files (`LB_BRONZE_ADD_FILES_MAX_BYTES`, `LB_BRONZE_ADD_FILES_MAX_FILES`).
- Customer 360 `bronze-verify` only validates the data and records the data clock; it registers no table.
- Per-executor memory is fixed at every scale. Executor counts are fixed up to scale 10, grow linearly above it and stop at `max_executors`, so data per executor grows up to scale 10 and again past the cap.
- silver-build is the bottleneck: at scale 100 (~1TB) it requests 18 executors at 60g each (48g + 12g overhead).
- Scratch rules are under [Scratch Storage](#scratch-storage-shuffle-pvcs).

### Continuous-mode jobs

| Job | Executor cores | Executor memory | Overhead | Scratch PVC | Driver memory |
|---|---|---|---|---|---|
| `bronze-ingest` | 2 | 4g | 2g | 20Gi | 4g |
| `silver-stream` | 4 | 32g | 8g | 100Gi | 8g |
| `gold-refresh` | 4 | 32g | 8g | 100Gi | 8g |

Financial overrides: `bronze-ingest` runs 4 cores, 8g memory and 8g overhead per executor; all three jobs get different executor counts ([Auto-Scaling](#auto-scaling)); base partitions are 80 for `silver-stream` and 96 for `gold-refresh`. Per-executor cores, memory and scratch for `silver-stream` and `gold-refresh` are unchanged.

## Auto-Scaling

Executor count comes from the scale factor unless a `*_executors` key overrides it:

- **Scale <= 10:** the job's base count.
- **Scale > 10:** `base + ((scale - 10) * rate) // 100`, capped at the job's maximum.

| Job | Base | Rate per 100 scale | Maximum |
|---|---|---|---|
| `bronze-verify` | 4 | 4 | 20 |
| `silver-build` | 8 | 12 | 28 |
| `gold-finalize` | 4 | 8 | 28 |
| `bronze-ingest` | 2 | 4 | 10 |
| `silver-stream` | 4 | 8 | 20 |
| `gold-refresh` | 2 | 4 | 10 |

Financial (AML) overrides: `bronze-verify` 4 / 8 / 28, `bronze-ingest` 5 / 4 / 20, `silver-stream` 10 / 8 / 28, `gold-refresh` 12 / 120 / 28.

In continuous mode:

- bronze-ingest and silver-stream take the larger of this count and what the offered load needs, up to the maximum.
- The three jobs share the cluster with datagen. When the cluster cannot hold every stream (`_streaming_concurrent_budget()`, after Trino, the catalog, PostgreSQL and datagen), each executor goes to the stream holding the smallest share of what it needs. The slowest stage keeps the largest share the cluster allows.
- An explicit per-job executor override wins over this Lakebench cap.

## Scratch Storage (Shuffle PVCs)

```yaml
platform:
  storage:
    scratch:
      # enabled: true                # unset, it turns on for a batch run at scale 50 and above
      storage_class: "px-csi-scratch"  # Must be repl=1
```

When enabled, each executor gets a dynamically provisioned PVC at `/tmp/spark-local` for shuffle spill:

- **Batch, per-scale profile:** the stage's `scratch_gib_per_scale` x scale / executors, rounded up, between 50Gi and the profile's `scratch_size` (the ceiling). Examples: Customer 360 silver-build gets 8 x 50Gi at scale 1, 8 x 75Gi at scale 10 and 18 x 300Gi at scale 100; AML bronze-verify with 16 executors at scale 10 gets 50Gi each.
- **No per-scale need** (c360 bronze-verify, 50Gi, and the streaming jobs): `scratch_size`. A widened streaming executor's PVC grows with its cores.
- To change scratch, change the executor count (`platform.compute.spark.<job>_executors`). There is no size field.
- `metrics.json` records the size per job as `config_snapshot.scratch.size_per_job`.
- The StorageClass must use `repl=1`: `repl=2+` doubles storage with no benefit for recomputable shuffle data.
- `lakebench deploy` only checks that the StorageClass exists. A cluster admin creates it once with `lakebench admin install --component scratch-storage-class`.

## RBAC

Deploy creates a namespace-scoped Role with pod and PVC lifecycle verbs; `deletecollection` is required for Spark 3.5.x cleanup.

## OpenShift Security Context Constraints (SCC)

Lakebench detects OpenShift when the `security.openshift.io` API group is served. Otherwise it treats the cluster as vanilla Kubernetes.

- Spark pods run as UID 185 (the `spark` user in `apache/spark` images), so `lakebench-spark-runner` needs the `anyuid` SCC.
- `deploy` grants `anyuid` to `lakebench-spark-runner` and `lakebench-postgres` in its RBAC and PostgreSQL steps. It uses the RBAC API (the RoleBinding `system:openshift:scc:anyuid`), so no `oc` is needed. A refused grant fails the step. Vanilla Kubernetes gets no grant. See [Prerequisites](prerequisites.md#openshift-scc-clusterrole).
- `lakebench validate` prints the platform and version and, on OpenShift, whether each SCC is assigned. With `--verbose` it checks the security requirements and shows the `oc adm policy` fix for a missing grant.
- Manual grant and how to verify it: [OpenShift SCC permission denied](troubleshooting.md#openshift-scc-permission-denied). `oc get events -n <namespace> --sort-by='.lastTimestamp'` shows SCC errors.

## Troubleshooting

- [SparkApplication stuck with no status](troubleshooting.md#sparkapplication-stuck-with-no-status)
- [Spark Operator volume mounting fails](troubleshooting.md#spark-operator-volume-mounting-fails)
- ["No space left on device" on silver-build](troubleshooting.md#no-space-left-on-device-on-silver-build)
- [A stage times out](troubleshooting.md#a-stage-times-out)
- [OpenShift SCC permission denied](troubleshooting.md#openshift-scc-permission-denied)

| Symptom | Cause | Fix |
|---|---|---|
| PVC provisioning failed | Storage class does not exist | Use one that exists. A cluster admin installs the scratch default `px-csi-scratch` (Portworx, repl=1) once with `lakebench admin install --component scratch-storage-class`. |
| deletecollection forbidden | Role lacks `deletecollection` | Add the verb to the Role. |

## See also

[Recipes](recipes.md), [Architecture](architecture.md), [Running Pipelines](running-pipelines.md), [Configuration](configuration.md), [Sizing](sizing.md).
