# Lakebench v1.2 Compatibility Matrix

## Supported Recipe Combinations

| Catalog | Table Format | Pipeline Engine | Query Engine | Recipe Name | Status |
|---------|-------------|----------------|-------------|-------------|--------|
| Hive | Iceberg | Spark | Trino | `hive-iceberg-spark-trino` | Stable (v1.0) |
| Hive | Iceberg | Spark | Spark Thrift | `hive-iceberg-spark-thrift` | Stable (v1.0) |
| Hive | Iceberg | Spark | DuckDB | `hive-iceberg-spark-duckdb` | Stable (v1.0) |
| Hive | Iceberg | Spark | None | `hive-iceberg-spark-none` | Stable (v1.0) |
| Polaris | Iceberg | Spark | Trino | `polaris-iceberg-spark-trino` | Stable (v1.1) |
| Polaris | Iceberg | Spark | Spark Thrift | `polaris-iceberg-spark-thrift` | Stable (v1.1) |
| Polaris | Iceberg | Spark | DuckDB | `polaris-iceberg-spark-duckdb` | Stable (v1.1) |
| Polaris | Iceberg | Spark | None | `polaris-iceberg-spark-none` | Stable (v1.1) |
| Hive | Delta | Spark | Trino | `hive-delta-spark-trino` | **New (v1.2)** |
| Hive | Delta | Spark | Spark Thrift | `hive-delta-spark-thrift` | **New (v1.2)** |
| Hive | Delta | Spark | None | `hive-delta-spark-none` | **New (v1.2)** |

## Pipeline Mode Support

All 11 recipes support all three pipeline modes:

| Mode | Description | Tested With |
|------|-------------|-------------|
| `batch` | Single-pass: bronze-verify -> silver-build -> gold-finalize -> benchmark | All recipes |
| `sustained` | Continuous streaming with periodic benchmarks | hive-delta-spark-trino |
| `iterative` | Repeated batch cycles with accumulating data | All recipes (v1.1) |

## Excluded Combinations

| Combination | Reason | Planned |
|------------|--------|---------|
| **Polaris + Delta** | Polaris is an Iceberg-native REST catalog. Delta requires Hive or Unity. | No |
| **Unity + Iceberg** | OSS Unity Catalog v0.4.0 Iceberg REST API is read-only (GET only). `UCSingleCatalog` v0.4.0 incompatible with Spark 4.0 for Iceberg writes (`ClassCastException`). | Pending Unity release |
| **Delta + DuckDB** | DuckDB's `delta` extension uses `delta-kernel-rs` which bypasses `httpfs` S3 settings and tries AWS IMDS (169.254.169.254) for credentials. Hangs on non-AWS Kubernetes. Only works on AWS with IAM instance profiles. | No (upstream) |
| **Unity + Delta (all engines)** | UCSingleCatalog 0.4.0 always calls `generateTemporaryTableCredentials` (STS) even for EXTERNAL tables (`CREATE TABLE ... LOCATION`). FlashBlade has no STS. Requires upstream UCSingleCatalog fix or direct REST API table registration. | v1.3 |
| **Unity + Delta + Trino** | Even with EXTERNAL tables, Trino 479 Delta connector requires Hive Metastore. Unity doesn't deploy one. | Pending Trino release |

## Component Version Matrix

| Component | Iceberg Recipes | Delta Recipes | Notes |
|-----------|----------------|--------------|-------|
| Apache Spark | 3.5.4, 4.0.2, 4.1.1 | 4.0.2 or 4.1.1 | Delta requires Spark 4.x |
| Spark Operator | 2.4.0 | 2.4.0 | Kubeflow Spark Operator |
| Apache Iceberg | 1.10.1 (auto) | -- | Auto-selected based on Spark version |
| Delta Lake | -- | 4.0.0 or 4.1.0 (auto) | Auto-selected based on Spark version |
| Hive Metastore | 3.1.3 | 3.1.3 | Stackable 25.7.0 |
| Apache Polaris | 1.3.0-incubating | -- | Iceberg-only |
| Trino | 479 | 479 | Iceberg or Delta connector |
| DuckDB | 1.5.0 | -- | Delta not supported (see above) |
| PostgreSQL | 16, 17, 18 | 16, 17, 18 | Metadata backend |

## Spark + Table Format Version Matrix

Format versions are **auto-selected** based on the Spark image version. Users can override
with an explicit version -- incompatible combinations are rejected at config load time.

| Spark | Delta 4.0.0 | Delta 4.1.0 | Iceberg 1.10.1 | Iceberg 1.9.1 | Iceberg 1.8.1 |
|-------|-------------|-------------|----------------|---------------|---------------|
| 3.5.x | -- | -- | **Default** | OK | OK |
| 4.0.x (default) | **Default** | -- | **Default** | OK | OK |
| 4.1.x | OK | **Default** | **Default** | OK | OK |

**Default** = auto-selected when no version specified. **OK** = accepted if user overrides. **--** = rejected.

Example:
```yaml
images:
  spark: apache/spark:4.1.1-python3    # Opt into Spark 4.1
architecture:
  table_format:
    type: delta
    # delta.version auto-resolves to 4.1.0 (matches Spark 4.1)
    # Or set explicitly: delta: { version: "4.0.0" }  -- backward compat OK
```

## Known Limitations (v1.2)

### Delta + Trino

- **Q2 benchmark query**: `MIN(interaction_date)` subquery triggers delta-spark 4.0.0
  `OptimizeMetadataOnlyDeltaQuery` bug (`ClassCastException: LocalDate -> java.sql.Date`).
  Q2 returns 0 rows via Spark Thrift. Not reproducible via Trino (different optimizer).
  Upstream bug in delta-spark 4.0.0.

- **OPTIMIZE OOM**: `ALTER TABLE ... EXECUTE optimize` rewrites the entire table in one pass.
  Exhausts Trino worker (8Gi) and Spark Thrift (4Gi) memory at scale 1+.
  Pre-benchmark OPTIMIZE is skipped for Delta. VACUUM still runs.

- **VACUUM requires catalog prefix**: `CALL {catalog}.system.vacuum(...)`, not
  `CALL system.vacuum(...)`. Also needs `SET SESSION {catalog}.vacuum_min_retention = '0s'`
  for retention below the 7-day default.

### Delta + Hive

- Tables are registered in the **session catalog** (`spark_catalog`), not a named catalog.
  `DeltaCatalog` is a `CatalogExtension` that must override `spark_catalog`.
  Benchmark queries use `spark_catalog.schema.table` references.

### Platform Requirements

- **S3**: Path-style access required (FlashBlade, MinIO). Virtual-hosted style not tested.
- **OpenShift**: Requires `anyuid` SCC for Spark pods (UID 185).
- **Portworx**: `px-csi-scratch` (repl=1) for Spark shuffle, `px-csi-db` (repl=3) for PostgreSQL.
- **PostgreSQL auth**: v1.2 uses SCRAM-SHA-256 authentication (replacing MD5). PostgreSQL 16+ defaults to SCRAM-SHA-256. Ensure `pg_hba.conf` uses `scram-sha-256` method, not `md5`.

## Catalog + Table Format Behavior

| Catalog | Format | Mechanism | Notes |
|---------|--------|-----------|-------|
| Hive | Iceberg | SparkCatalog with Hive Thrift backend | Tables registered via Thrift. Trino reads via Iceberg connector. |
| Hive | Delta | DeltaCatalog as session catalog extension | Tables in Hive "Spark SQL specific format". Trino reads via Delta connector. |
| Polaris | Iceberg | SparkCatalog with REST backend | OAuth2 auth. Trino reads via Iceberg REST connector. |
| Unity | Delta | UCSingleCatalog with EXTERNAL tables | Data written via S3A credentials. Metadata registered via Unity REST API. Trino not supported. |
