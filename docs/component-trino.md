# Trino

Reference: configure and size the Trino query engine: config keys, catalog wiring, memory limits, timeouts and deploy behaviour.

## What it does

Trino is the default query engine. It runs the benchmark query suite against Iceberg or Delta tables and is the ad-hoc SQL interface for every medallion layer.

- `architecture.query_engine.type: trino` (the default).
- Lakebench connects Trino to the recipe's catalog, Hive Metastore or Polaris, so every table the pipeline registers is queryable at once. Delta runs with Hive only.
- Recipes: `hive-iceberg-spark-trino` (the default), `polaris-iceberg-spark-trino`, `hive-delta-spark-trino` ([Recipes](recipes.md)).

## Version and image

`images.trino` sets the coordinator and worker image (default: [version matrix](compatibility-matrix.md#component-version-matrix)). Another release can be pinned, with two limits:

- **Polaris needs Trino 454 or later** for the `oauth2.scope` property (Trino PR #22961).
- **Native S3:** Lakebench generates `fs.native-s3.enabled=true` with `s3.*` properties. Trino 483 removed the legacy `hive.s3.*` properties. On an older image, check that its S3 syntax matches.

## Configuration keys

Defaults from `TrinoConfig`, `TrinoCoordinatorConfig` and `TrinoWorkerConfig` in `config/schema.py`.

| Key | Default | Effect |
|---|---|---|
| `query_engine.type` | `trino` | `trino`, `spark-thrift`, `duckdb` or `none`. Other values skip Trino. |
| `trino.coordinator.cpu` | `"2"` | Coordinator CPU request and limit |
| `trino.coordinator.memory` | `"8Gi"` | Coordinator memory request and limit. JVM `-Xmx` is 80% of it. |
| `trino.worker.replicas` | `2` | Worker pods, 1 to 256. `0` (coordinator-only) is refused at load. |
| `trino.worker.cpu` | `"4"` | CPU request and limit per worker |
| `trino.worker.memory` | `"16Gi"` | Memory request and limit per worker. JVM `-Xmx` is 80% of it. |
| `trino.worker.spill_enabled` | `true` | Spill to disk when queries exceed memory |
| `trino.worker.spill_max_per_node` | `"40Gi"` | Spill per worker before the query fails. Keep it at or below the spill volume (the PVC `storage` when a class is set). |
| `trino.worker.storage` | `"50Gi"` | PVC size per worker (spill and data directory) |
| `trino.worker.storage_class` | `""` | Worker PVC StorageClass. Empty: an `emptyDir`, no PVC. |
| `trino.catalog_name` | `"lakehouse"` | Catalog name in Trino (`SELECT ... FROM lakehouse.silver.table`) |

## Catalog wiring

Lakebench writes `lakehouse.properties` from `architecture.catalog.type`. S3 credentials come from the `lakebench-s3-credentials` Secret as environment variables.

Hive (`catalog.type: hive`):

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://lakebench-hive-metastore:9083
fs.native-s3.enabled=true
s3.endpoint=<from config>
s3.path-style-access=true
s3.region=us-east-1
```

Polaris (`catalog.type: polaris`), Iceberg REST with OAuth2. The `PRINCIPAL_ROLE:ALL` scope gives Trino read and write on Polaris tables:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakebench-polaris.<namespace>.svc.cluster.local:8181/api/catalog
iceberg.rest-catalog.warehouse=lakehouse
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=lakebench:<secret>
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
fs.native-s3.enabled=true
s3.endpoint=<from config>
s3.path-style-access=true
s3.region=us-east-1
```

## Sizing

When `trino.worker.replicas`, `cpu` and `memory` are at their defaults, Lakebench sets them from the scale factor. Explicit values are kept.

| Scale factor | Workers | Worker CPU | Worker memory | Coordinator CPU | Coordinator memory |
|---|---|---|---|---|---|
| 1-5 | 1 | 2 | 8Gi | 1 | 4Gi |
| 6-50 | 2 | 4 | 16Gi | 2 | 8Gi |
| 51-500 | max(4, scale / 25) | 8 | 48Gi | 4 | 16Gi |
| 501+ | max(10, scale / 50) | 8 | 64Gi | 4 | 16Gi |

- **Scale out before scaling up.** More workers spread query fragments and raise `query.max-memory` with them.
- **The coordinator processes no data.** 2 CPU / 8Gi suits most workloads.

### Query memory limits

Lakebench sets Trino's memory properties from the deployed heaps and worker count.

| Property | Value | Scale 1 | Scale 10 | Scale 100 |
|---|---|---|---|---|
| Worker `-Xmx` | 80% of the pod limit | 6553m | 13107m | 39321m |
| `query.max-memory-per-node` | 35% of that node's heap | 2293MB | 4587MB | 13762MB |
| `memory.heap-headroom-per-node` | 30% of that node's heap (Trino's default) | 1965MB | 3932MB | 11796MB |
| `query.max-memory` | workers x worker per-node | 2293MB | 9174MB | 55048MB |
| `query.max-total-memory` | Trino's default, 2 x `query.max-memory` | 4586MB | 18348MB | 110096MB |

These values are fixed at deploy and scale with the worker count. Stock Trino's 20GB cap is raised at every autosized scale.

### Spill

- With `spill_enabled: true`, joins, `ORDER BY`, window functions and plain aggregations can spill. Spilled state is revocable memory and does not count toward `query.max-memory`.
- An aggregate still `DISTINCT` (or with an `ORDER BY` inside it) in the final plan, and the `MarkDistinct` operator, cannot spill in Trino 483. Their hash tables stay in user memory and hit the limits above.
- The optimizer can rewrite a `DISTINCT` aggregate into a spillable `GROUP BY` (the `pre_aggregate` distinct-aggregation strategy, chosen from statistics under the default `automatic`). AML FQ3 (`COUNT(DISTINCT target_entity_id)` with `SUM`s, grouped by entity) can take the non-spillable shape; check with `EXPLAIN`.

### Query timeouts

- Each benchmark query has a client timeout: 300 s, or 900 s for the financial workload.
- Killing the local `kubectl exec` does not stop the `trino` CLI or its query in the pod. The executor therefore passes `--session query_max_run_time=<timeout - 5>s`, so Trino fails the query just before the client gives up.
- On a client timeout it also cancels anything still running under the query's unique `--source` tag with `system.runtime.kill_query`, so a timed-out query stops holding worker memory.
- There is no cluster-wide `query.max-execution-time`: Iceberg and Delta maintenance runs through the Trino CLI and can take longer than any benchmark query.
- `lakebench query` gets the same session limit. Its default `--timeout` is 120 s, so Trino ends a long ad-hoc statement (a manual `OPTIMIZE`) at 115 s. Pass a larger `--timeout` for such statements.

## Deploy and destroy

`lakebench deploy` creates:

- **Coordinator:** Deployment `lakebench-trino-coordinator` (1 replica). Query planning, scheduling and HTTP on port 8080. Its init container waits for `lakebench-hive-metastore:9083` (Hive) or `lakebench-polaris:8181` (Polaris), by TCP.
- **Workers:** StatefulSet `lakebench-trino-worker`. Spill goes to an `emptyDir` unless `trino.worker.storage_class` gives each worker a PVC. Their init container waits for the coordinator on port 8080.
- **Services:** `lakebench-trino` at `lakebench-trino.<namespace>.svc.cluster.local:8080`; headless `lakebench-trino-worker` for stable worker pod DNS.
- **Probes:** `/v1/info` for readiness and liveness on both. The deployer also runs `SHOW CATALOGS` to confirm the Iceberg catalog answers.

`lakebench destroy`, before removing Trino:

- Clears the pipeline tables from the catalog with `CALL <catalog>.system.unregister_table(...)`, for Iceberg and Delta, not `DROP TABLE`. A Trino `DROP TABLE` would also delete data files, including datagen files registered in place.
- On Polaris, when the namespace is deleted too, runs no statement: the catalog's only state is its PostgreSQL database, which goes with the namespace.
- Table files are removed only by the bucket step, and only from buckets the deployment owns.

## Troubleshooting

- [Trino coordinator stays in Init](troubleshooting.md#trino-coordinator-stays-in-init)
- [Trino queries fail with Polaris: "scope not valid"](troubleshooting.md#trino-queries-fail-with-polaris-scope-not-valid)
- [Trino metrics show only JVM metrics](troubleshooting.md#trino-metrics-show-only-jvm-metrics)
- [Iceberg compaction fails on open writers or per-node memory](troubleshooting.md#iceberg-compaction-fails-on-open-writers-or-per-node-memory)

## See also

[Benchmarking](benchmarking.md), [Query reference](benchmarks/c360/queries.md#6-query-set), [Architecture](architecture.md), [Configuration](configuration.md), [Polaris](component-polaris.md).
