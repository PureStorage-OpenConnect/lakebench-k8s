# Spark Thrift Server

Reference: configure and size the Spark Thrift Server query engine: config keys, limits and deploy behaviour.

## What it does

Spark Thrift Server (HiveThriftServer2) is a Spark-native query engine. It exposes Spark SQL over Thrift on port 10000 and runs the benchmark queries on the same Spark runtime as the pipeline.

- Set `architecture.query_engine.type: spark-thrift`.
- Queries run through `kubectl exec` into the pod with `beeline` and `jdbc:hive2://localhost:10000`.
- Spark refreshes Iceberg metadata on each query; `flush_cache()` is a no-op.
- Recipes: `hive-iceberg-spark-thrift`, `polaris-iceberg-spark-thrift`, `hive-delta-spark-thrift` ([Recipes](recipes.md)).

## Version and image

The server uses `images.spark`, shared with the pipeline jobs: changing it changes both. Defaults per recipe: [version matrix](compatibility-matrix.md#component-version-matrix).

## Configuration keys

Defaults from `SparkThriftConfig` in `config/schema.py`; the Delta and financial values are set by `config/autosizer.py` on fields left unset.

| Key | Default | Effect |
|---|---|---|
| `query_engine.type` | `trino` | `spark-thrift` deploys Spark Thrift Server instead of Trino. |
| `spark_thrift.cores` | `2` (`8` on Delta + Hive) | CPU request and limit. The server runs Spark in local mode, so this is the query parallelism. Delta + Hive fits down to min(8, largest node cores - 2), floor 2. |
| `spark_thrift.memory` | `"4g"` (`"16g"` on Delta + Hive, `"24g"` on the financial schema) | Driver heap; all query data (shuffle, aggregation, sort) must fit here plus spill. See below. |
| `spark_thrift.catalog_name` | `"lakehouse"` | Catalog prefix in SQL. Must match the catalog registered in Hive or Polaris. |

`spark_thrift.memory`:

- Fit-down on the largest node: Delta + Hive min(16, node - 8) GiB; financial below 36 GiB allocatable min(20g, node - 8g); floor 4g.
- Delta + Hive gets more because Delta OPTIMIZE is skipped for Thrift, so queries scan uncompacted silver.
- Pod request and limit are the heap plus max(10% of heap, 1 GiB), so `4g` gives a 5 GiB pod.

## Sizing

At large scale Trino's distributed workers are faster.

| Scale factor | Cores | Memory | Notes |
|---|---|---|---|
| 1-10 | 2 | 4g | Defaults work |
| 10-50 | 4 | 8g | More memory for analytics queries |
| 50+ | -- | -- | Consider Trino; if staying, 4 cores and 8g or 16g |

## Limitations

- **Single pod.** A driver-only process with no executors; all query work runs in one JVM.
- **Serial queries.** Throughput mode streams run one at a time through the single Thrift connection.
- **Startup time.** Jars come from the in-namespace dependency server, then the JVM warms up. A new dependency set (a changed image or format version) restarts the pod once. Startup time is not measured on this release.

## Deploy and destroy

The server runs as a plain Kubernetes Deployment, not a SparkApplication: HiveThriftServer2 needs `--deploy-mode client`, which the Spark Operator does not support.

- **Deployment** `lakebench-spark-thrift` (1 replica): `spark-submit` in client mode with the HiveThriftServer2 main class.
- **Service** `lakebench-spark-thrift` (ClusterIP): `lakebench-spark-thrift.<namespace>.svc.cluster.local:10000`.
- **Probes:** TCP socket on port 10000 for readiness and liveness.

Init containers, plus a CA-import container when `platform.storage.s3.ca_cert` is set:

1. **Catalog wait:** TCP check on `lakebench-hive-metastore:9083` (Hive) or `lakebench-polaris:8181` (Polaris).
2. **`lb-deps-fetch`:** copies the table-format runtime (Iceberg or Delta), AWS SDK and Hadoop S3 jars that the Spark jobs load from the `lb-deps` server, checking each sha256 against the `lb-deps-manifest` ConfigMap. The server puts them on its driver classpath after the image's own jars, in the jobs' order. Nothing downloads from Maven at start.

`lakebench destroy` removes the Deployment and Service.

## Troubleshooting

- [Delta + Spark Thrift limits](compatibility-matrix.md#delta--spark-thrift)
- [The benchmark fails but the pipeline succeeded](troubleshooting.md#the-benchmark-fails-but-the-pipeline-succeeded)

## See also

[Trino](component-trino.md), [DuckDB](component-duckdb.md), [Spark](component-spark.md), [Benchmarking](benchmarking.md), [Configuration](configuration.md).
