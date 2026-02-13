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
  trino: "trinodb/trino:479"          # Default: trinodb/trino:479

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
| `images.trino` | `trinodb/trino:479` | Container image for coordinator and workers. |
| `query_engine.type` | `trino` | Set to `trino` to deploy Trino. Other values skip Trino deployment. |
| `trino.coordinator.cpu` | `"2"` | CPU request and limit for the coordinator pod. |
| `trino.coordinator.memory` | `"8Gi"` | Memory request and limit for the coordinator pod. Also sets JVM `-Xmx`. |
| `trino.worker.replicas` | `2` | Number of worker pods. Set to `0` for coordinator-only mode (dev/debug). |
| `trino.worker.cpu` | `"4"` | CPU request and limit per worker pod. |
| `trino.worker.memory` | `"16Gi"` | Memory request and limit per worker pod. Also sets JVM `-Xmx`. |
| `trino.worker.spill_enabled` | `true` | Enable spill-to-disk when queries exceed memory. |
| `trino.worker.spill_max_per_node` | `"40Gi"` | Maximum spill data written per worker before the query fails. |
| `trino.worker.storage` | `"50Gi"` | PVC size for each worker (used for spill and data directory). |
| `trino.worker.storage_class` | `""` | StorageClass for worker PVCs. Empty uses the cluster default. |
| `trino.catalog_name` | `"lakehouse"` | The catalog name registered in Trino. Queries reference it as `SELECT ... FROM lakehouse.silver.table`. |

## Version Flexibility

The default image is **Trino 479**. You can override it via `images.trino` to use a newer or older release, with these constraints:

- **Minimum version for Polaris**: Trino 454. Earlier versions lack the `oauth2.scope` property needed for Polaris REST catalog authentication (added in Trino PR #22961).
- **Native S3 filesystem**: Trino 479 uses `fs.native-s3.enabled=true` with `s3.*` properties. The legacy `hive.s3.*` properties were removed in this release. If you pin an older image, verify that its S3 configuration syntax matches what Lakebench generates.

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

Worker sizing depends on query complexity, data volume, and concurrency:

| Scale factor | Approximate data | Recommended workers | Worker memory | Notes |
|---|---|---|---|---|
| 10 (~100 GB) | ~100 GB bronze | 1-2 | 8-16Gi | Minimal setup, suitable for development |
| 100 (~1 TB) | ~1 TB bronze | 2 | 16Gi | Default config handles the 8-query benchmark |
| 500+ (~5 TB+) | 5+ TB bronze | 4-8 | 32Gi | Increase replicas before increasing per-worker memory |

General principles:

- **Scale out before scaling up.** Adding worker replicas distributes query fragments across more nodes and is more effective than increasing memory on fewer workers.
- **Spill storage is your safety net.** With `spill_enabled: true` (the default), queries that exceed worker memory spill intermediate data to disk rather than failing with OOM. Keep `spill_max_per_node` at or below the PVC `storage` size.
- **Coordinator sizing is modest.** The coordinator does not process data. The defaults of 2 CPU / 8Gi are sufficient for most workloads.

**Recipes using Trino:** Standard, Delta Lake, Polaris, Polaris Spark SQL.
See the [Recipes Guide](recipes.md) for all combinations.

## See Also

- [Recipes](recipes.md) -- all supported component combinations
- [Benchmarking](benchmarking.md) -- query suite execution and scoring
- [Query Reference](query-reference.md) -- the benchmark queries Trino runs
- [Architecture](architecture.md) -- how Trino fits into the overall Lakebench stack
- [Configuration](configuration.md) -- full YAML schema reference
- [Quickstart: Polaris](quickstart-polaris.md) -- deploying with Polaris REST catalog
- [Troubleshooting](troubleshooting.md) -- common Trino deployment issues
