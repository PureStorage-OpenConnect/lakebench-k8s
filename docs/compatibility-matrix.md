# Lakebench Compatibility Matrix

## Supported Recipe Combinations

| Catalog | Table Format | Pipeline Engine | Query Engine | Recipe Name |
|---------|-------------|----------------|-------------|-------------|
| Hive | Iceberg | Spark | Trino | `hive-iceberg-spark-trino` |
| Hive | Iceberg | Spark | Spark Thrift | `hive-iceberg-spark-thrift` |
| Hive | Iceberg | Spark | DuckDB | `hive-iceberg-spark-duckdb` |
| Hive | Iceberg | Spark | None | `hive-iceberg-spark-none` |
| Polaris | Iceberg | Spark | Trino | `polaris-iceberg-spark-trino` |
| Polaris | Iceberg | Spark | Spark Thrift | `polaris-iceberg-spark-thrift` |
| Polaris | Iceberg | Spark | DuckDB | `polaris-iceberg-spark-duckdb` |
| Polaris | Iceberg | Spark | None | `polaris-iceberg-spark-none` |
| Hive | Delta | Spark | Trino | `hive-delta-spark-trino` |
| Hive | Delta | Spark | Spark Thrift | `hive-delta-spark-thrift` |
| Hive | Delta | Spark | None | `hive-delta-spark-none` |

## Pipeline Mode Support

All 11 recipes support all three pipeline modes:

| Mode | Description | Tested With |
|------|-------------|-------------|
| `batch` | Single-pass: bronze-verify -> silver-build -> gold-finalize -> benchmark | All recipes |
| `sustained` | Sustained streaming with periodic benchmarks | hive-delta-spark-trino |
| `iterative` | Repeated batch cycles with accumulating data | All recipes |

## Excluded Combinations

| Combination | Reason | Planned |
|------------|--------|---------|
| **Polaris + Delta** | Polaris is an Iceberg-native REST catalog. Delta requires Hive or Unity. | No |
| **Unity + Iceberg** | OSS Unity Catalog v0.4.0 Iceberg REST API is read-only (GET only). `UCSingleCatalog` v0.4.0 incompatible with Spark 4.0 for Iceberg writes (`ClassCastException`). | Pending Unity release |
| **Delta + DuckDB** | DuckDB's `delta` extension uses `delta-kernel-rs` which bypasses `httpfs` S3 settings and tries AWS IMDS (169.254.169.254) for credentials. Hangs on non-AWS Kubernetes. Only works on AWS with IAM instance profiles. | No (upstream) |
| **Unity + Delta (all engines)** | UCSingleCatalog 0.4.0 always calls `generateTemporaryTableCredentials` (STS) even for EXTERNAL tables (`CREATE TABLE ... LOCATION`). FlashBlade has no STS. Requires upstream UCSingleCatalog fix or direct REST API table registration. | Pending upstream fix |
| **Unity + Delta + Trino** | Even with EXTERNAL tables, Trino's Delta connector requires a Hive Metastore. Unity doesn't deploy one. | No |

## Component Version Matrix

| Component | Iceberg Recipes | Delta Recipes | Notes |
|-----------|----------------|--------------|-------|
| Apache Spark | 3.5.4, 4.0.2, 4.1.1 | 4.0.2 or 4.1.1 | Delta requires Spark 4.x |
| Spark Operator | 2.5.1 | 2.5.1 | Kubeflow Spark Operator |
| Apache Iceberg | 1.11.0 (auto) | -- | Auto-selected based on Spark version. 1.11.0 requires Java 17 -- see below. |
| Delta Lake | -- | 4.0.0 or 4.1.0 (auto) | Auto-selected based on Spark version |
| Hive Metastore | 3.1.3 | 3.1.3 | Stackable 25.7.0 |
| Apache Polaris | 1.6.0 | -- | Iceberg-only |
| Trino | 483 | 483 | Iceberg or Delta connector |
| DuckDB | 1.5.0 | -- | Delta not supported (see above) |
| PostgreSQL | 16, 17, 18 | 16, 17, 18 | Metadata backend |

## Spark + Table Format Version Matrix

Format versions are **auto-selected** based on the Spark image version. Users can override
with an explicit version -- incompatible combinations are rejected at config load time.

| Spark | Delta 4.0.0 | Delta 4.1.0 | Iceberg 1.11.0 | Iceberg 1.10.1 | Iceberg 1.10.0 |
|-------|-------------|-------------|----------------|----------------|----------------|
| 3.5.x | -- | -- | **Default** (needs java17 image) | OK | OK |
| 4.0.x (default) | **Default** | -- | **Default** | OK | OK |
| 4.1.x | -- | **Default** | **Default** | OK | OK |

**Default** = auto-selected when no version specified. **OK** = accepted if user overrides. **--** = rejected.

Spark 4.0/4.1 runtime artifacts only exist for Iceberg 1.10.0+. Older Iceberg
versions (1.5.x--1.9.x) are compatible with Spark 3.5.x only.

### Iceberg 1.11.0 requires Java 17

Iceberg 1.11.0 is compiled to Java 17 bytecode; 1.10.x was Java 11. Every
Spark 4.x image already ships Java 17, so only Spark 3.5 is affected --
`apache/spark:3.5.4-python3` ships Java 11 and will fail at class load with
`UnsupportedClassVersionError`.

On Spark 3.5, either use a java17 image tag:

```yaml
images:
  spark: apache/spark:3.5.9-java17-python3
```

or pin Iceberg to the last Java 11 release:

```yaml
architecture:
  table_format:
    iceberg:
      version: "1.10.1"
```

Lakebench rejects the bad pairing at config load rather than letting it fail
inside the Spark driver.

### Which runtime jar gets requested

Iceberg does not publish the same set of Spark runtimes in every release, so
the artifact depends on both versions:

| Spark | Iceberg 1.10.x | Iceberg 1.11.0 |
|-------|----------------|----------------|
| 3.5.x | `3.5_2.12` | `3.5_2.12` |
| 4.0.x | `4.0_2.13` | `4.0_2.13` |
| 4.1.x | `4.0_2.13` (borrowed) | `4.1_2.13` (native) |

Spark 4.1 borrows the 4.0 runtime on Iceberg 1.10.x because no 4.1 artifact
exists there. 1.11.0 publishes one, so 4.1 uses it directly.

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

## Known Limitations

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
