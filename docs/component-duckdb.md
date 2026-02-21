# Component Reference: DuckDB

## Overview

DuckDB is a lightweight, single-pod query engine option for Lakebench. It
runs the benchmark query suite against Iceberg tables without requiring a
distributed query cluster. DuckDB is best for small-scale runs, development,
and environments where deploying Trino workers is impractical.

When `architecture.query_engine.type` is set to `duckdb`, Lakebench deploys
a DuckDB pod during `lakebench deploy` and tears it down during
`lakebench destroy`.

## Architecture

Lakebench deploys DuckDB as a single Kubernetes resource:

- **Deployment** -- `lakebench-duckdb` (1 replica). Runs a Python 3.11
  container with the `duckdb` module installed at startup. The pod sleeps
  indefinitely and serves as a query execution target.
- **Service** -- `lakebench-duckdb` (headless). Used for pod discovery.

DuckDB has no coordinator/worker split, no external JDBC port, and no
persistent state. Each benchmark query is executed via `kubectl exec` into
the pod, which creates a fresh DuckDB connection, runs the SQL, and returns
results as JSON.

### Health checks

- **Readiness/Liveness**: exec probe running `python -c "import duckdb; print('ok')"`
- Verifies the Python module loaded correctly

### Cache behavior

Each query creates a fresh DuckDB connection. There is no metadata cache
to flush between queries -- every query reads Iceberg metadata from scratch.
The `flush_cache()` operation is a no-op.

## YAML Configuration

DuckDB settings live under `architecture.query_engine` in the config file.

```yaml
architecture:
  query_engine:
    type: duckdb
    duckdb:
      cores: 2                       # CPU request and limit
      memory: "4g"                   # Memory request and limit
      catalog_name: "lakehouse"      # Iceberg catalog name in queries
```

### Field reference

| Field | Default | Description |
|---|---|---|
| `query_engine.type` | `trino` | Set to `duckdb` to deploy DuckDB instead of Trino. |
| `duckdb.cores` | `2` | CPU request and limit for the DuckDB pod. |
| `duckdb.memory` | `"4g"` | Memory request and limit for the DuckDB pod. |
| `duckdb.catalog_name` | `"lakehouse"` | The catalog name used in SQL queries (e.g. `SELECT ... FROM lakehouse.silver.table`). Must match the catalog registered in Hive or Polaris. |

### What the overrides do

| Override | Effect | When to change |
|---|---|---|
| `duckdb.cores` | Controls CPU available for query execution. DuckDB parallelizes within a single process using all available cores. | Increase to `4` at scale 50+ where queries scan larger tables. |
| `duckdb.memory` | Limits total memory for the DuckDB process. Queries that exceed this fail with OOM. | Increase to `8g` or `16g` at scale 50+ for analytics queries (Q5, Q6) that build large intermediate results. |
| `duckdb.catalog_name` | Changes the catalog prefix in SQL. | Only change if you registered the catalog under a different name. |

## Limitations

- **Single pod.** No horizontal scaling. All query work runs in one process.
- **No concurrent query support.** Benchmark queries run serially. Throughput
  mode (concurrent streams) is not practical.
- **No persistent cache.** Each query re-reads Iceberg metadata from S3.
  At large scales this adds latency compared to Trino's cached metadata.
- **kubectl exec overhead.** Each query invocation has ~1-2s overhead from
  the kubectl exec round-trip.

## Sizing Guidance

DuckDB is designed for small to mid-scale runs. At large scales, Trino is
significantly faster due to distributed execution.

| Scale factor | Recommended cores | Recommended memory | Notes |
|---|---|---|---|
| 1--10 | 2 | 4g | Default config works |
| 10--50 | 4 | 8g | Increase memory for analytics queries |
| 50+ | -- | -- | Consider switching to Trino |

## Recipes

DuckDB is used by these recipes:

- `hive-iceberg-spark-duckdb`
- `polaris-iceberg-spark-duckdb`

See the [Recipes Guide](recipes.md) for all combinations.

## See Also

- [Trino](component-trino.md) -- distributed query engine (recommended for production)
- [Spark Thrift Server](component-spark-thrift.md) -- Spark-native query engine
- [Scoring and Benchmarking](benchmarking.md) -- query engine benchmark methodology
- [Recipes](recipes.md) -- all supported component combinations
- [Configuration](configuration.md) -- full YAML schema reference
