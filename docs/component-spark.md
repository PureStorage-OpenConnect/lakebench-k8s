# Component: Apache Spark

Apache Spark is the compute engine for the Lakebench medallion pipeline. Lakebench
uses the Kubeflow Spark Operator v2.x to submit `SparkApplication` custom resources
on Kubernetes. Spark runs PySpark scripts that move data through three layers:

- **Batch mode:** `bronze-verify`, `silver-build`, `gold-finalize` -- run sequentially,
  each job starts after the previous one completes.
- **Continuous mode:** `bronze-ingest`, `silver-stream`, `gold-refresh` -- run
  concurrently as structured streaming jobs with configurable trigger intervals.

The scripts are deployed as one ConfigMap per role and projected together, as
one flat directory, at `/opt/spark/scripts` in both driver and executor pods:

| ConfigMap | Contents |
|---|---|
| `lakebench-scripts-common` | `common.py` |
| `lakebench-scripts-c360` | the Customer 360 stage scripts, Iceberg and Delta |
| `lakebench-scripts-aml-rules` | `detection_rules.py`, `tm_operations.py` |
| `lakebench-scripts-aml-jobs` | the AML stage and operator-action scripts |
| `lakebench-scripts-aml-gate` | the reference scorer and the pre-registered gate modules |
| `lakebench-scripts-aml-data` | the AML reference and pre-registration JSON |

The file list is `SCRIPT_MAPS` in `modules/pipeline_engines/spark/scripts_maps.py`.
`run` (and `lakebench financial`) applies every map, reads each back and checks
that its data still hashes to its `lakebench.io/scripts-sha256` annotation, and
only then submits jobs. It refuses to change a map that another deployment
owns, or one that a still-running SparkApplication mounts (Kubernetes would
swap the files under the running pods), and it re-checks the maps before each
later job, so no stage is submitted on scripts other than the ones its run
applied. A listed file missing from the installed
package, or a map over 838,860 bytes (80% of the 1 MiB ConfigMap limit, counting
key and value bytes), stops the run with one line naming the file or map. The
single `lakebench-spark-scripts` map used by 1.6 and earlier is deleted on the
first 1.7 run, unless a SparkApplication that is still running mounts it, and
`destroy` deletes all of them, including when `create_namespace: false` keeps
the namespace.

## Spark Operator

Lakebench requires **Kubeflow Spark Operator v2.x** (2.5.1 is the current
default). ConfigMap volumes cannot use Spark's native
`spark.kubernetes.*.volumes.*` conf properties, because Spark's
`KubernetesVolumeUtils` has no `configMap` volume type, so lakebench defines
its volumes (the projected scripts volume, the work-dir emptyDir and, on the
driver, the `lb-deps-dl` emptyDir the jars are downloaded into)
in `driver.template`/`executor.template` pod templates; see `_build_manifest()`
in `modules/pipeline_engines/spark/job.py`. Only the executor scratch PVC uses
the conf-property path. The operator's webhook injection was checked against
its source through 2.5.1 and is unchanged from 2.4.0, so the pod-template
route stays. The v1.x line has broken volume
injection entirely and is not supported.

`lakebench deploy` always checks the operator and adds the deployment's
namespace to the operator's watch list (`spark.jobNamespaces`). It never
installs the operator: a missing one fails the deploy, and a cluster admin
installs the shared operator once with `lakebench admin install --component
spark-operator`, at `version` in `namespace`. An installed operator keeps its
version whatever the config says. `install: true` (v1.6) is refused by the
commands that change data (`destroy`, `status` and `admin` load it as false).

```yaml
platform:
  compute:
    spark:
      operator:
        namespace: "spark-operator"  # Where the operator runs
        version: "2.5.1"            # Chart a fresh admin install uses; must be v2.x
```

## YAML Configuration

All Spark-related settings live under `platform.compute.spark`, `platform.storage.scratch`,
`images.spark`, and the top-level `spark` section. Below is every configurable field
with its default value.

### Image

```yaml
images:
  spark: "apache/spark:4.1.1-python3"   # Spark 4.1.x (default for the Hive recipes)
  # spark: "apache/spark:4.0.2-python3" # Spark 4.0.x (default for Polaris, hive-delta-spark-thrift)
  # spark: "apache/spark:3.5.8-python3" # Spark 3.5.x (also supported)
```

The default image follows each recipe's release-matrix row: the Hive recipes
(`hive-iceberg-*`, `hive-delta-spark-trino`) default to Spark 4.1.1; the
Polaris recipes, `hive-delta-spark-thrift` and `hive-delta-spark-none`
default to 4.0.2. A config with no recipe takes the image of the recipe its
components name, so a recipe-less Polaris config also runs 4.0.2. The table
format version follows the Spark minor when left at `auto`: Delta 4.1.0 on
4.1 and 4.0.0 on 4.0; Iceberg 1.11.0 on both, with its native 4.1 runtime on
Spark 4.1.

**Supported versions:** Spark 3.5.x, 4.0.x, and 4.1.x. Spark 4.2 is not
supported: no Iceberg release ships a runtime that works with it. The image
tag must contain `-python3` (PySpark scripts require a Python-enabled image).
Unsupported versions, non-Python images, and unparseable tags are rejected at
config load time with a clear error.

The default Iceberg version is 1.11.0, which needs Java 17. The plain
`apache/spark:3.5.x-python3` images ship Java 11, so on those lakebench falls
back to Iceberg 1.10.1 and logs a warning; use a `-java17-python3` 3.5 tag to
run Iceberg 1.11.0. An explicitly configured Iceberg 1.11+ on a Java 11 image
is rejected.

The Spark version is parsed from the image tag to resolve the correct
dependency coordinates and driver resource profiles:

| Spark | Scala | Hadoop AWS | AWS SDK | Iceberg runtime | Driver memory (silver/gold) |
|-------|-------|-----------|---------|------------------------|---|
| 3.5.x | 2.12 | 3.3.4 | 1.12.262 | `iceberg-spark-runtime-3.5_2.12` | 24g |
| 4.0.x | 2.13 | 3.4.1 | 1.12.720 | `iceberg-spark-runtime-4.0_2.13` | 32g |
| 4.1.x | 2.13 | 3.4.2 | 1.12.720 | `iceberg-spark-runtime-4.1_2.13` (1.11.0+) or `-4.0_2.13` (1.10.x) | 32g |

Spark 4.0.x needs more driver memory because `hadoop-aws:3.4.1` pulls a
558MB AWS SDK v2 bundle (vs 280MB SDK v1 on Spark 3.5.x). The extra heap
pressure from serving larger jars to executors via the driver's netty file
server requires the bump from 24g to 32g. This is handled automatically --
the driver memory column above shows the default for each version.

### Operator

```yaml
platform:
  compute:
    spark:
      operator:
        namespace: "spark-operator"  # Operator namespace
        version: "2.5.1"            # Chart a fresh `admin install` uses (v2.x required)
```

### Driver Resources

Each job's driver is sized by its job profile (see
[Job Profiles](#job-profiles) below). Two global overrides apply to every
Spark job:

```yaml
platform:
  compute:
    spark:
      driver_cores: null             # Override driver cores (e.g. 2)
      driver_memory: null            # Override driver memory (e.g. "16g")
```

When `driver_cores` or `driver_memory` is set, the override replaces the
per-job profile default for every job. Use this when cluster nodes have
limited resources or at extreme scales (500+) where the driver needs more
memory to handle Iceberg commit metadata.

### Executor Resources

Per-executor sizing (cores, memory, overhead, scratch PVC) is fixed per job
in the job profiles; only the count can be overridden (below). The v1.6
`platform.compute.spark.driver` and `.executor` blocks sized nothing (the
manifests never read them), so v1.7 removed them: a command that changes
data refuses a config that sets either, with the fix, and `destroy`,
`status` and the read-only commands load it and say the block is ignored.

### Per-Job Executor Count Overrides

```yaml
platform:
  compute:
    spark:
      # Batch jobs (null = auto-derived from scale factor)
      bronze_executors: null         # Override bronze-verify executor count
      silver_executors: null         # Override silver-build executor count
      gold_executors: null           # Override gold-finalize executor count

      # Continuous-mode jobs (null = auto-derived from scale factor)
      bronze_ingest_executors: null  # Override bronze-ingest executor count
      silver_stream_executors: null  # Override silver-stream executor count
      gold_refresh_executors: null   # Override gold-refresh executor count
```

When set, these values bypass the auto-scaling formula entirely for that job.
Per-executor sizing (cores, memory, overhead, PVC) is never overridden -- only
the count changes.

### Scratch Storage (Shuffle PVCs)

```yaml
platform:
  storage:
    scratch:
      enabled: false                 # Enable Portworx scratch PVCs
      storage_class: "px-csi-scratch"  # Must be repl=1
```

When enabled, each executor gets a dynamically provisioned PVC mounted at
`/tmp/spark-local` for shuffle spill. The PVC size comes from the per-job
profile (`scratch_size`); `metrics.json` records it per job as
`config_snapshot.scratch.size_per_job`. `scratch.size` sized nothing and is
refused like the removed executor block. The StorageClass must use `repl=1` --
using `repl=2+` doubles storage consumption with zero benefit for
recomputable shuffle data. `lakebench deploy` only verifies that the
StorageClass exists; it never creates it. A cluster admin creates it once with
`lakebench admin install --component scratch-storage-class`.

### Spark Configuration Overrides (S3A, Shuffle, Memory)

```yaml
spark:
  conf:
    spark.speculation: "true"            # your keys, merged over the defaults
    spark.hadoop.fs.s3a.retry.limit: "20"  # replaces a job default
```

Each job's conf is the job defaults, then `spark.conf`, then the keys
Lakebench sets for the job. The defaults (`SPARK_CONF_DEFAULTS` in
`src/lakebench/modules/pipeline_engines/spark/conf_keys.py`), which a
`spark.conf` value replaces:

| Key | Default |
|---|---|
| `spark.hadoop.fs.s3a.multipart.size` | `268435456` (256 MB) |
| `spark.hadoop.fs.s3a.fast.upload.active.blocks` | `16` |
| `spark.hadoop.fs.s3a.attempts.maximum` | `20` |
| `spark.hadoop.fs.s3a.retry.limit` | `10` |
| `spark.hadoop.fs.s3a.retry.interval` | `500ms` |
| `spark.memory.fraction` | `0.8` |
| `spark.memory.storageFraction` | `0.3` |

Lakebench then sets its own keys for every job, and `spark.conf` cannot
change them: the commands that change data refuse a config that sets one,
naming what controls it. They include:

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
| `spark.sql.shuffle.partitions`, `spark.default.parallelism` | per job: at scale 10 or below the job profile's `base_partitions`, unless an executor override raises the count above the profile default; otherwise `executor_count * cores * 2` |

and the catalog, jar (`spark.jars`, `spark.jars.*`, `spark.submit.pyFiles`),
adaptive-execution, stability, Parquet, Spark UI and `spark.kubernetes.*`
keys (`LAKEBENCH_OWNED_SPARK_KEYS`). `spark.driver.maxResultSize` is the
exception: Lakebench sets it from the executor count only when `spark.conf`
does not.

## Job Profiles

Per-executor sizing is **fixed** and proven at 1TB+ scale. These values are not
user-configurable. They live in
`_JOB_PROFILES` in `src/lakebench/modules/pipeline_engines/spark/job.py`.

### Batch Jobs

| Job | Executor Cores | Executor Memory | Overhead | Scratch PVC | Driver (Spark 3) | Driver (Spark 4) |
|---|---|---|---|---|---|---|
| `bronze-verify` | 2 | 4g (8g financial) | 2g (12g financial) | 50Gi (c360) / 500Gi (financial) | 4g | 4g |
| `silver-build` | 4 | 48g | 12g | 300Gi | 24g | 32g |
| `gold-finalize` | 4 | 32g | 8g | 300Gi | 24g | 32g |

For financial workloads (LB-118) `bronze-verify` also overrides executor
memory (4g -> 8g), overhead (2g -> 12g), `executors_per_100_scale` (4 -> 8)
and `max_executors` (20 -> 28), since the CTAS fallback in
`bronze_verify_financial.py` rewrites the full pacs.008 source above scale 5.
Overrides live in `_SCHEMA_PROFILE_OVERRIDES` alongside the base profiles.

### Continuous-Mode Jobs

| Job | Executor Cores | Executor Memory | Overhead | Scratch PVC | Driver Memory |
|---|---|---|---|---|---|
| `bronze-ingest` | 2 | 4g | 2g | 20Gi | 4g |
| `silver-stream` | 4 | 32g | 8g | 100Gi | 8g |
| `gold-refresh` | 4 | 32g | 8g | 100Gi | 8g |

For financial workloads `bronze-ingest` runs 4 cores, 8g memory and 8g
overhead per executor. The financial overrides also change the executor
counts of all three continuous jobs (see the Auto-Scaling section), and set
base partitions to 80 for `silver-stream` and 96 for `gold-refresh`. Per-executor
cores, memory and scratch for `silver-stream` and `gold-refresh` are unchanged.

**Why are these fixed?** Adding executors keeps data-per-executor constant, so
per-executor memory and PVC requirements do not change with scale. The silver-build
job is the bottleneck -- at scale 100 (~1TB) it requests 18 executors at 60g
each (48g + 12g overhead) plus 300Gi scratch PVCs. Reducing these values causes
OOM kills or "No space left on device" failures.

## Auto-Scaling

Executor count is automatically derived from the scale factor unless overridden.
The formula in `_scale_executor_count()`:

- **Scale <= 10:** Use the base executor count (varies per job).
- **Scale > 10:** `base + ((scale - 10) * rate) // 100`, capped at a per-job maximum.

| Job | Base | Rate per 100 scale units | Maximum |
|---|---|---|---|
| `bronze-verify` | 4 | 4 | 20 |
| `silver-build` | 8 | 12 | 28 |
| `gold-finalize` | 4 | 8 | 28 |
| `bronze-ingest` | 2 | 4 | 10 |
| `silver-stream` | 4 | 8 | 20 |
| `gold-refresh` | 2 | 4 | 10 |

Financial (AML) overrides: `bronze-verify` 4 / 8 / 28, `bronze-ingest`
5 / 4 / 20, `silver-stream` 10 / 8 / 28, `gold-refresh` 12 / 120 / 28.

In continuous mode, the three jobs share the cluster concurrently with datagen.
A budget calculation (`_streaming_concurrent_budget()`) proportionally caps each
job's executor count based on available cluster CPU after subtracting
Trino, Hive, PostgreSQL, and datagen while it is still running. An explicit
per-job executor override wins over this cap.

To override the auto-derived count for any job:

```yaml
platform:
  compute:
    spark:
      silver_executors: 12           # Force 12 executors for silver-build
      gold_refresh_executors: 4      # Force 4 executors for gold-refresh
```

## OpenShift

Spark pods run as **UID 185** (the `spark` user in official `apache/spark`
images). On OpenShift, this requires the `anyuid` Security Context Constraint
(SCC) bound to the `lakebench-spark-runner` service account.

Lakebench handles this automatically. During `lakebench deploy`, the RBAC
deployer detects OpenShift and makes the grant that `oc adm policy
add-scc-to-user anyuid -z lakebench-spark-runner -n <namespace>` makes, through
the Kubernetes API: the RoleBinding `system:openshift:scc:anyuid` in the
deployment's namespace. `oc` is not needed. If the grant is refused (the
deploying user cannot bind that ClusterRole), the RBAC step fails and prints
the `oc adm policy` command for a cluster admin; see
[Prerequisites](prerequisites.md#openshift-scc-clusterrole). On vanilla
Kubernetes, the SCC step is skipped.

Spark is used by all recipes. See the [Recipes Guide](recipes.md) for all
supported component combinations.

## See Also

- [Recipes](recipes.md) -- all supported component combinations
- [Architecture](architecture.md) -- system layers and medallion pipeline overview
- [Running Pipelines](running-pipelines.md) -- step-by-step `lakebench run` usage
- [Configuration](configuration.md) -- full YAML configuration reference
- [Troubleshooting](troubleshooting.md) -- common Spark failure modes and fixes
