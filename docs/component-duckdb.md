# DuckDB

Reference: configure the single-pod DuckDB query engine: config keys, query translation, limits and deploy behaviour.

## What it does

DuckDB is a single-pod query engine. It runs the benchmark query suite against Iceberg tables without a distributed query cluster. Use it for small runs, development, and where Trino workers are impractical.

- Set `architecture.query_engine.type: duckdb`.
- DuckDB reads Iceberg only. DuckDB with Delta is rejected at config load: its delta extension ignores DuckDB's S3 settings and cannot reach non-AWS S3.
- Recipes: `hive-iceberg-spark-duckdb`, `polaris-iceberg-spark-duckdb` ([Recipes](recipes.md)).

## Version and image

`images.duckdb` sets the pod image; `duckdb.version` pins DuckDB. Defaults: [version matrix](compatibility-matrix.md#component-version-matrix).

## Configuration keys

Defaults from `DuckDBConfig` in `config/schema.py`.

| Key | Default | Effect |
|---|---|---|
| `query_engine.type` | `trino` | `duckdb` deploys DuckDB instead of Trino. |
| `duckdb.cores` | `2` | CPU request and limit. DuckDB parallelizes across all cores in one process. |
| `duckdb.memory` | `"4g"` | Memory request and limit; a query over it fails with OOM. On the financial schema, an unset memory becomes `16g`; on a node under 24 GiB allocatable, node minus 8 GiB (floor 4g). |
| `duckdb.catalog_name` | `"lakehouse"` | Catalog prefix in SQL (`lakehouse.silver.table`). Must match the catalog registered in Hive or Polaris. |
| `duckdb.version` | [matrix](compatibility-matrix.md#component-version-matrix) | `duckdb` Python package version. Pinned so runs weeks apart use the same engine. |

## Sizing

At large scale Trino is much faster through distributed execution.

| Scale factor | Cores | Memory | Notes |
|---|---|---|---|
| 1-10 | 2 | 4g | Defaults work |
| 10-50 | 4 | 8g | More memory for analytics queries (Q5, Q6) with large intermediate results |
| 50+ | -- | -- | Consider Trino; if staying on DuckDB, 4 cores and 8g or 16g |

## How queries run

- Each query runs through `kubectl exec` into the pod, which opens a fresh DuckDB connection, runs the SQL and returns JSON. The round-trip adds ~1-2 s per query.
- DuckDB does not connect to Hive or Polaris. It reads Iceberg tables from S3 with `iceberg_scan()`:

  ```sql
  SELECT * FROM iceberg_scan('s3://bucket/warehouse/namespace.db/table',
                             allow_moved_paths := true)
  ```

- `adapt_query()` in `DuckDBExecutor` rewrites catalog-qualified names (e.g. `lakehouse.silver.customer_interactions_enriched`) to `iceberg_scan()` calls on the table's S3 path. Hive uses the `namespace.db/` warehouse layout. The auxiliary AML tables (`silver_entities`, `gold_alerts` and the others) resolve to the bucket of the layer that writes them.
- Queries are written in Trino SQL. `adapt_query()` rewrites Trino functions:
  - `date_add('month', N, expr)` -> `(expr + INTERVAL N MONTH)`
  - `date_diff(...)` -> `datediff(...)`
  - `cardinality(array)` -> `len(array)` (outside string literals, quoted identifiers and comments)
- COUNT, SUM, AVG, LAG and window frames work unchanged.
- No metadata cache: every query reads Iceberg metadata from S3. `flush_cache()` is a no-op.

## Limitations

- **Single pod.** No horizontal scaling.
- **Serial queries.** Throughput mode (concurrent streams) is not practical.
- **No cache.** At large scale, re-reading metadata adds latency against Trino's cached metadata.
- **No Iceberg maintenance.** DuckDB is read-only. `expire_snapshots`, `remove_orphan_files` and compaction never run on a DuckDB recipe; the run's maintenance record marks them `not_supported`. Table health probing still works.

## Deploy and destroy

`lakebench deploy` creates:

- **Deployment** `lakebench-duckdb` (1 replica). The main container sleeps and serves as the query target.
- **Service** `lakebench-duckdb` (headless), for pod discovery.

No coordinator/worker split, no JDBC port, no persistent state.

Extensions and wheel:

- The DuckDB wheel and the `httpfs`, `iceberg` and `avro` extensions are part of the deployment's dependency set. Deploy resolves them once onto the `lb-deps` server.
- The `lb-deps-fetch` init container copies them, checking every file's sha256 (`pip install --no-index --require-hashes`, and `lb_deps.py fetch` for the extensions).
- Queries run with `autoinstall_known_extensions=false`, so a missing extension fails instead of downloading.
- The pod needs no internet access; only the deploy-time resolve does, or a mirror (`platform.deps`).

Probes:

- **Startup:** `import duckdb; c=duckdb.connect(); c.execute('SET autoinstall_known_extensions=false'); c.load_extension('iceberg'); c.load_extension('httpfs')`.
- **Readiness and liveness:** `python -c "import duckdb; print('ok')"`.

Destroy: DuckDB cannot drop catalog tables, so destroy does not drop them on a DuckDB recipe. The catalog database goes with the namespace (when Lakebench created it). Table files go with the cleanup of buckets the deployment owns.

## Troubleshooting

- [DuckDB pod never becomes ready](troubleshooting.md#duckdb-pod-never-becomes-ready)

## See also

[Trino](component-trino.md), [Spark Thrift Server](component-spark-thrift.md), [Benchmarking](benchmarking.md), [Configuration](configuration.md).
