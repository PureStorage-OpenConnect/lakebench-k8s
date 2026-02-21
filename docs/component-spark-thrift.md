# Component Reference: Spark Thrift Server

## Overview

Spark Thrift Server (HiveThriftServer2) is a Spark-native query engine option
for Lakebench. It exposes Spark SQL over the Thrift protocol on port 10000,
allowing Lakebench to run the benchmark query suite using the same Spark
runtime that processes the pipeline stages.

When `architecture.query_engine.type` is set to `spark-thrift`, Lakebench
deploys the Spark Thrift Server during `lakebench deploy` and tears it down
during `lakebench destroy`.

## Architecture

Lakebench deploys Spark Thrift Server as a standard Kubernetes Deployment
(not a Spark Operator SparkApplication). This is because HiveThriftServer2
requires `--deploy-mode client`, which the Spark Operator does not support.

- **Deployment** -- `lakebench-spark-thrift` (1 replica). Runs `spark-submit`
  in client mode with the HiveThriftServer2 main class.
- **Service** -- `lakebench-spark-thrift` (ClusterIP). Exposes the Thrift
  protocol at `lakebench-spark-thrift.<namespace>.svc.cluster.local:10000`.

Benchmark queries are executed via `kubectl exec` into the pod using
`beeline` (Spark's JDBC CLI) with connection string `jdbc:hive2://localhost:10000`.

### Init containers

Two init containers run before the Spark Thrift Server starts:

1. **Catalog wait** -- blocks until the catalog backend is reachable:
   - Hive: waits for `lakebench-hive-metastore:9083` (TCP)
   - Polaris: waits for `lakebench-polaris:8181` (TCP)

2. **JAR download** -- downloads Iceberg runtime, AWS SDK, and Hadoop S3
   JARs from Maven Central. These are required for reading Iceberg tables
   on S3-compatible storage.

### Health checks

- **Readiness/Liveness**: TCP socket probe on port 10000
- Startup can take 3-5 minutes due to JAR downloads and JVM initialization

### Cache behavior

Spark refreshes Iceberg table metadata automatically on each query. The
`flush_cache()` operation is a no-op -- no explicit cache management is
needed.

## YAML Configuration

Spark Thrift Server settings live under `architecture.query_engine` in the
config file. The image is shared with Spark pipeline jobs under `images.spark`.

```yaml
images:
  spark: "apache/spark:3.5.4-python3"    # Shared with pipeline Spark jobs

architecture:
  query_engine:
    type: spark-thrift
    spark_thrift:
      cores: 2                       # CPU request and limit
      memory: "4g"                   # Memory request and limit (driver JVM)
      catalog_name: "lakehouse"      # Iceberg catalog name in queries
```

### Field reference

| Field | Default | Description |
|---|---|---|
| `query_engine.type` | `trino` | Set to `spark-thrift` to deploy Spark Thrift Server instead of Trino. |
| `spark_thrift.cores` | `2` | CPU request and limit for the Spark Thrift pod. |
| `spark_thrift.memory` | `"4g"` | Memory request and limit. This is the Spark driver memory -- all query processing happens in this single JVM. |
| `spark_thrift.catalog_name` | `"lakehouse"` | The Iceberg catalog name used in SQL queries. Must match the catalog registered in Hive or Polaris. |

### What the overrides do

| Override | Effect | When to change |
|---|---|---|
| `spark_thrift.cores` | Controls CPU available for the Spark driver JVM. Spark SQL parallelizes query stages within this single process. | Increase to `4` at scale 50+ where queries process larger tables. |
| `spark_thrift.memory` | Sets the Spark driver heap. All query data (shuffle, aggregation, sort) must fit in this allocation plus spill-to-disk. | Increase to `8g` or `16g` at scale 50+ for analytics queries that build large intermediate results. |
| `spark_thrift.catalog_name` | Changes the catalog prefix in SQL. | Only change if you registered the catalog under a different name. |

## Limitations

- **Single pod.** Runs as a driver-only process with no executor distribution.
  All query work happens in one JVM.
- **Startup time.** 3-5 minutes due to Maven JAR downloads and JVM warmup.
  Subsequent runs are faster if the pod is already running.
- **No concurrent query streams.** Benchmark throughput mode (concurrent
  streams) runs serially through the single Thrift connection.
- **Shared image with pipeline.** Uses the same `apache/spark` image as
  pipeline Spark jobs. Changing `images.spark` affects both.

## Sizing Guidance

Spark Thrift Server is a single-JVM query engine. At large scales, Trino's
distributed workers provide better query performance.

| Scale factor | Recommended cores | Recommended memory | Notes |
|---|---|---|---|
| 1--10 | 2 | 4g | Default config works |
| 10--50 | 4 | 8g | Increase memory for analytics queries |
| 50+ | -- | -- | Consider switching to Trino |

## Recipes

Spark Thrift Server is used by these recipes:

- `hive-iceberg-spark-thrift`
- `polaris-iceberg-spark-thrift`

See the [Recipes Guide](recipes.md) for all combinations.

## See Also

- [Trino](component-trino.md) -- distributed query engine (recommended for production)
- [DuckDB](component-duckdb.md) -- lightweight single-pod query engine
- [Spark](component-spark.md) -- Spark Operator and pipeline job configuration
- [Scoring and Benchmarking](benchmarking.md) -- query engine benchmark methodology
- [Recipes](recipes.md) -- all supported component combinations
- [Configuration](configuration.md) -- full YAML schema reference
