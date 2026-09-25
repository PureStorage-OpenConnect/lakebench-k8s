# Component Reference: Trino

## Overview

Trino is the default query engine in a Lakebench recipe. It executes the benchmark query suite against Iceberg tables in the gold layer and serves as the ad-hoc SQL interface for exploring data across all medallion layers. Lakebench connects Trino to whichever catalog the recipe specifies -- Hive Metastore or Apache Polaris REST catalog -- so that every Iceberg table registered during the pipeline run is immediately queryable.

When `architecture.query_engine.type` is set to `trino` (the default), Lakebench deploys Trino automatically during `lakebench deploy` and tears it down during `lakebench destroy`.

## Architecture

Lakebench deploys Trino as two Kubernetes workloads:

- **Coordinator** -- a single-replica `Deployment` (`lakebench-trino-coordinator`). Handles query planning, scheduling, and the HTTP endpoint on port 8080.
- **Workers** -- a `StatefulSet` (`lakebench-trino-worker`) with configurable replica count. Each worker gets a PVC for spill-to-disk storage, allowing large queries to exceed available memory.

A `Service` named `lakebench-trino` exposes the coordinator at `lakebench-trino.<namespace>.svc.cluster.local:8080`.

### Init containers

The coordinator runs an init container that blocks until the catalog backend is reachable:

- **Hive catalog**: waits for `lakebench-hive-metastore:9083` (TCP check).
- **Polaris catalog**: waits for `lakebench-polaris:8181` (TCP check).

Workers run their own init container that waits for the coordinator to become available on port 8080 before joining the cluster.

### Health checks

Both coordinator and worker pods use Trino's `/v1/info` HTTP endpoint for readiness and liveness probes. The deployer additionally runs `SHOW CATALOGS` against the coordinator to verify that the Iceberg catalog is registered and responding before reporting success.

## YAML Configuration

All Trino settings live under `architecture.query_engine` in the Lakebench config file. The image tag is set separately under `images`.

```yaml
# Image override
images:
  trino: "trinodb/trino:483"          # Default: trinodb/trino:483

# Query engine selection and tuning
architecture:
  query_engine:
    type: trino                        # trino | spark-thrift | duckdb | none
    trino:
      coordinator:
        cpu: "2"                       # CPU request/limit for coordinator
        memory: "8Gi"                  # Memory request/limit for coordinator
      worker:
        replicas: 2                    # Number of worker pods
        cpu: "4"                       # CPU request/limit per worker
        memory: "16Gi"                 # Memory request/limit per worker
        spill_enabled: true            # Enable spill-to-disk for large queries
        spill_max_per_node: "40Gi"     # Max spill data per worker node
        storage: "50Gi"               # PVC size per worker (data + spill)
        storage_class: ""              # StorageClass (empty = cluster default)
      catalog_name: "lakehouse"        # Iceberg catalog name exposed in Trino
```

### Field reference

| Field | Default | Description |
|---|---|---|
| `images.trino` | `trinodb/trino:483` | Container image for coordinator and workers. |
| `query_engine.type` | `trino` | Set to `trino` to deploy Trino. Other values skip Trino deployment. |
| `trino.coordinator.cpu` | `"2"` | CPU request and limit for the coordinator pod. |
| `trino.coordinator.memory` | `"8Gi"` | Memory request and limit for the coordinator pod. JVM `-Xmx` is 80% of this limit, leaving room for non-heap memory. |
| `trino.worker.replicas` | `2` | Number of worker pods. Set to `0` for coordinator-only mode (dev/debug). |
| `trino.worker.cpu` | `"4"` | CPU request and limit per worker pod. |
| `trino.worker.memory` | `"16Gi"` | Memory request and limit per worker pod. JVM `-Xmx` is 80% of this limit, leaving room for non-heap memory. |
| `trino.worker.spill_enabled` | `true` | Enable spill-to-disk when queries exceed memory. |
| `trino.worker.spill_max_per_node` | `"40Gi"` | Maximum spill data written per worker before the query fails. |
| `trino.worker.storage` | `"50Gi"` | PVC size for each worker (used for spill and data directory). |
| `trino.worker.storage_class` | `""` | StorageClass for worker PVCs. Empty = emptyDir (ephemeral, no PVC needed). |
| `trino.catalog_name` | `"lakehouse"` | The catalog name registered in Trino. Queries reference it as `SELECT ... FROM lakehouse.silver.table`. |

## Version Flexibility

The default image is **Trino 483**. You can override it via `images.trino` to use a newer or older release, with these constraints:

- **Minimum version for Polaris**: Trino 454. Earlier versions lack the `oauth2.scope` property needed for Polaris REST catalog authentication (added in Trino PR #22961).
- **Native S3 filesystem**: Trino 483 uses `fs.native-s3.enabled=true` with `s3.*` properties. The legacy `hive.s3.*` properties were removed in this release. If you pin an older image, verify that its S3 configuration syntax matches what Lakebench generates.

## Catalog Integration

Lakebench automatically configures the Iceberg connector based on `architecture.catalog.type`:

### Hive Metastore (`catalog.type: hive`)

The generated `lakehouse.properties` uses the Hive Metastore Iceberg connector:

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://lakebench-hive-metastore:9083
fs.native-s3.enabled=true
s3.endpoint=<from config>
s3.path-style-access=true
s3.region=us-east-1
```

### Polaris REST Catalog (`catalog.type: polaris`)

The generated `lakehouse.properties` uses the Iceberg REST connector with OAuth2:

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

The `PRINCIPAL_ROLE:ALL` scope grants Trino the permissions it needs to read and write Iceberg tables managed by Polaris. S3 credentials are injected via environment variables from the `lakebench-s3-credentials` Kubernetes Secret.

## Sizing Guidance

When `trino.worker.replicas`, `cpu` and `memory` are left at their defaults, the autosizer sets them from the scale factor (`full_compute_guidance()` in `config/scale.py`). Explicit values are kept.

| Scale factor | Workers | Worker memory | Coordinator memory |
|---|---|---|---|
| 1-5 | 1 | 8Gi | 4Gi |
| 6-50 | 2 | 16Gi | 8Gi |
| 51-500 | max(4, scale / 25) | 48Gi | 16Gi |
| 501+ | max(10, scale / 50) | 64Gi | 16Gi |

### Query memory limits

lakebench sets Trino's memory properties from the deployed heaps and worker count (`trino_memory_properties()` in `deploy/engine.py`). Trino's own defaults do not follow the cluster: `query.max-memory` is a flat 20GB, so before this change four 48Gi workers at AML scale 100 still failed FQ3 with `Query exceeded distributed user memory limit of 20GB` while about 107 GB of memory pool sat unused.

| Property | Value | Scale 1 | Scale 10 | Scale 100 |
|---|---|---|---|---|
| Worker `-Xmx` | 80% of the pod limit | 6553m | 13107m | 39321m |
| `query.max-memory-per-node` | 50% of that node's heap | 3276MB | 6553MB | 19660MB |
| `memory.heap-headroom-per-node` | 30% of that node's heap (Trino's default) | 1965MB | 3932MB | 11796MB |
| `query.max-memory` | workers x worker per-node | 3276MB | 13106MB | 78640MB |
| `query.max-total-memory` | workers x (worker heap - headroom) | 4588MB | 18350MB | 110100MB |

The per-node values shown are the worker's; the coordinator gets the same fractions of its own heap. Per-node plus headroom is 80% of the heap, inside Trino's startup check (the two may not exceed the heap). Half the heap is about 71% of the node's memory pool: one query in a power run can use most of it, and throughput runs (4 streams) still have room for a second query. When the pool fills, Trino blocks and its low-memory killer ends the largest query rather than the JVM running out of heap. `query.max-total-memory` is the physical pool across the workers; Trino's default (twice `query.max-memory`) is above that and would never bind first either.

At every autosized scale the new cluster cap is higher than the old effective cap, which was the smaller of 20GB and workers x 30% of the heap (scale 1: 1.9 GB to 3.2 GB; scale 10: 7.7 GB to 12.8 GB; scale 100: 20 GB to 76.8 GB). The values are fixed at deploy time; changing the worker count by hand afterwards does not update them.

General principles:

- **Scale out before scaling up.** Adding worker replicas distributes query fragments across more nodes and raises `query.max-memory` with them.
- **Spill does not cover every query.** With `spill_enabled: true` (the default), joins, `ORDER BY`, window functions and plain aggregations can spill to disk. Spilled state is revocable memory, which does not count toward `query.max-memory`. Aggregations with `DISTINCT` (or an `ORDER BY` inside the aggregate) and the `MarkDistinct` operator cannot spill in Trino 483, so their hash tables stay in user memory and hit the limits above. AML FQ3 (`COUNT(DISTINCT target_entity_id)` grouped by entity) is this shape. Keep `spill_max_per_node` at or below the PVC `storage` size.
- **Coordinator sizing is modest.** The coordinator does not process data. The defaults of 2 CPU / 8Gi are sufficient for most workloads.

**Recipes using Trino:** Standard, Polaris.
See the [Recipes Guide](recipes.md) for all combinations.

## See Also

- [Recipes](recipes.md) -- all supported component combinations
- [Scoring and Benchmarking](benchmarking.md) -- query engine benchmark and pipeline scorecard
- [Query Reference](query-reference.md) -- the benchmark queries Trino runs
- [Architecture](architecture.md) -- how Trino fits into the overall Lakebench stack
- [Configuration](configuration.md) -- full YAML schema reference
- [Quickstart: Polaris](quickstart-polaris.md) -- deploying with Polaris REST catalog
- [Troubleshooting](troubleshooting.md) -- common Trino deployment issues
