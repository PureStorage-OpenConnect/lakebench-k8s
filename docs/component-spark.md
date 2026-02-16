# Component: Apache Spark

Apache Spark is the compute engine for the Lakebench medallion pipeline. Lakebench
uses the Kubeflow Spark Operator v2.x to submit `SparkApplication` custom resources
on Kubernetes. Spark runs PySpark scripts that move data through three layers:

- **Batch mode:** `bronze-verify`, `silver-build`, `gold-finalize` -- run sequentially,
  each job starts after the previous one completes.
- **Continuous mode:** `bronze-ingest`, `silver-stream`, `gold-refresh` -- run
  concurrently as structured streaming jobs with configurable trigger intervals.

All scripts are deployed as a ConfigMap (`lakebench-spark-scripts`) and mounted
at `/opt/spark/scripts` in both driver and executor pods.

## Spark Operator

Lakebench requires **Kubeflow Spark Operator v2.4.0** (or any v2.x release).
The v2.x line uses webhook-based pod mutation for volume injection, which is
required for ConfigMap and PVC mounts to work correctly. The v1.x line has
broken ConfigMap volume injection and is not supported.

Lakebench can auto-install the operator into its own namespace, or skip
installation if the operator is already present on the cluster:

```yaml
platform:
  compute:
    spark:
      operator:
        install: false              # Set true to auto-install (requires cluster-admin)
        namespace: "spark-operator"  # Where the operator runs
        version: "2.4.0"            # Must be v2.x
```

## YAML Configuration

All Spark-related settings live under `platform.compute.spark`, `platform.storage.scratch`,
`images.spark`, and the top-level `spark` section. Below is every configurable field
with its default value.

### Image

```yaml
images:
  spark: "apache/spark:3.5.4-python3"   # Spark container image
```

The Spark version is parsed from the image tag to resolve Iceberg runtime JAR
coordinates (e.g., `iceberg-spark-runtime-3.5_2.12`).

### Operator

```yaml
platform:
  compute:
    spark:
      operator:
        install: false               # Set true to auto-install via Helm (requires cluster-admin)
        namespace: "spark-operator"  # Operator namespace
        version: "2.4.0"            # Operator chart version (v2.x required)
```

### Driver Resources

```yaml
platform:
  compute:
    spark:
      driver:
        cores: 4                     # Driver CPU cores (default for all jobs)
        memory: "8g"                 # Driver memory (default for all jobs)

      # Global driver overrides (apply to ALL Spark jobs)
      driver_cores: null             # Override driver cores (e.g. 2)
      driver_memory: null            # Override driver memory (e.g. "16g")
```

When `driver_cores` or `driver_memory` is set, the override replaces the
per-job profile default for every job. Use this when cluster nodes have
limited resources or at extreme scales (500+) where the driver needs more
memory to handle Iceberg commit metadata.

### Executor Resources (Defaults)

```yaml
platform:
  compute:
    spark:
      executor:
        instances: 8                 # Default executor count
        cores: 4                     # Cores per executor
        memory: "48g"                # Memory per executor
        memory_overhead: "12g"       # JVM overhead per executor
```

These defaults are used by the auto-sizer for tier guidance. At runtime,
the actual per-executor sizing comes from the fixed job profiles (see
[Job Profiles](#job-profiles) below), not from these fields.

### Per-Job Executor Count Overrides

```yaml
platform:
  compute:
    spark:
      # Batch jobs (null = auto-derived from scale factor)
      bronze_executors: null         # Override bronze-verify executor count
      silver_executors: null         # Override silver-build executor count
      gold_executors: null           # Override gold-finalize executor count

      # Streaming jobs (null = auto-derived from scale factor)
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
      size: "100Gi"                  # Default PVC size (overridden per job)
      create_storage_class: true     # Auto-create the StorageClass
```

When enabled, each executor gets a dynamically provisioned PVC mounted at
`/tmp/spark-local` for shuffle spill. The PVC size comes from the per-job
profile, not from `scratch.size`. The StorageClass must use `repl=1` --
using `repl=2+` doubles storage consumption with zero benefit for
recomputable shuffle data.

### Spark Configuration Overrides (S3A, Shuffle, Memory)

```yaml
spark:
  conf:
    # S3A tuning (proven defaults for FlashBlade / S3-compatible stores)
    spark.hadoop.fs.s3a.connection.maximum: "500"
    spark.hadoop.fs.s3a.threads.max: "200"
    spark.hadoop.fs.s3a.fast.upload: "true"
    spark.hadoop.fs.s3a.multipart.size: "268435456"       # 256 MB
    spark.hadoop.fs.s3a.fast.upload.active.blocks: "16"
    spark.hadoop.fs.s3a.attempts.maximum: "20"
    spark.hadoop.fs.s3a.retry.limit: "10"
    spark.hadoop.fs.s3a.retry.interval: "500ms"

    # Shuffle settings
    spark.sql.shuffle.partitions: "200"
    spark.default.parallelism: "200"

    # Memory settings
    spark.memory.fraction: "0.8"
    spark.memory.storageFraction: "0.3"
```

These are applied as the base Spark configuration for every job. Per-job
shuffle partitions are then overridden to `executor_count * cores * 2`
(the standard convention for partition sizing).

## Job Profiles

Per-executor sizing is **fixed** and proven at 1TB+ scale. These values are not
user-configurable. They live in
`_JOB_PROFILES` in `src/lakebench/spark/job.py`.

### Batch Jobs

| Job | Executor Cores | Executor Memory | Overhead | Scratch PVC | Driver Memory |
|---|---|---|---|---|---|
| `bronze-verify` | 2 | 4g | 2g | 50Gi | 4g |
| `silver-build` | 4 | 48g | 12g | 150Gi | 16g |
| `gold-finalize` | 4 | 32g | 8g | 100Gi | 16g |

### Streaming Jobs

| Job | Executor Cores | Executor Memory | Overhead | Scratch PVC | Driver Memory |
|---|---|---|---|---|---|
| `bronze-ingest` | 2 | 4g | 2g | 20Gi | 4g |
| `silver-stream` | 4 | 32g | 8g | 100Gi | 8g |
| `gold-refresh` | 4 | 32g | 8g | 100Gi | 8g |

**Why are these fixed?** Adding executors keeps data-per-executor constant, so
per-executor memory and PVC requirements do not change with scale. The silver-build
job is the bottleneck -- at scale 100 (~1TB) it requests 19 executors at 60g
each (48g + 12g overhead) plus 150Gi scratch PVCs. Reducing these values causes
OOM kills or "No space left on device" failures.

## Auto-Scaling

Executor count is automatically derived from the scale factor unless overridden.
The formula in `_scale_executor_count()`:

- **Scale <= 10:** Use the base executor count (varies per job).
- **Scale > 10:** `base + ((scale - 10) * rate) // 100`, capped at a per-job maximum.

| Job | Base | Rate per 100 scale units | Maximum |
|---|---|---|---|
| `bronze-verify` | 4 | 4 | 20 |
| `silver-build` | 8 | 12 | 30 |
| `gold-finalize` | 4 | 8 | 20 |
| `bronze-ingest` | 2 | 4 | 10 |
| `silver-stream` | 4 | 8 | 20 |
| `gold-refresh` | 2 | 4 | 10 |

In continuous mode, streaming jobs share the cluster concurrently with datagen.
A budget calculation (`_streaming_concurrent_budget()`) proportionally caps each
streaming job's executor count based on available cluster CPU after subtracting
Trino, Hive, PostgreSQL, and datagen.

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
deployer detects OpenShift and runs:

```
oc adm policy add-scc-to-user anyuid -z lakebench-spark-runner -n <namespace>
```

No manual intervention is needed. On vanilla Kubernetes, the SCC step is
skipped.

Spark is used by all recipes. See the [Recipes Guide](recipes.md) for all
supported component combinations.

## See Also

- [Recipes](recipes.md) -- all supported component combinations
- [Architecture](architecture.md) -- system layers and medallion pipeline overview
- [Running Pipelines](running-pipelines.md) -- step-by-step `lakebench run` usage
- [Configuration](configuration.md) -- full YAML configuration reference
- [Troubleshooting](troubleshooting.md) -- common Spark failure modes and fixes
