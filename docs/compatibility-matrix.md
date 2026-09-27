# Lakebench Compatibility Matrix

## Recipes

Every recipe maps to one entry of the architecture list lakebench validates at
config load. Any other catalog, table format, pipeline engine and query engine
combination is refused at load with the reason.

<!-- BEGIN GENERATED: recipe-components -->
<!-- Generated from the code by `python3.11 -m lakebench.config.support .`; do not edit by hand. -->

| Recipe | Catalog | Table Format | Pipeline Engine | Query Engine |
|---|---|---|---|---|
| `hive-delta-spark-none` | Hive | Delta | Spark | None |
| `hive-delta-spark-thrift` | Hive | Delta | Spark | Spark Thrift |
| `hive-delta-spark-trino` | Hive | Delta | Spark | Trino |
| `hive-iceberg-spark-duckdb` | Hive | Iceberg | Spark | DuckDB |
| `hive-iceberg-spark-none` | Hive | Iceberg | Spark | None |
| `hive-iceberg-spark-thrift` | Hive | Iceberg | Spark | Spark Thrift |
| `hive-iceberg-spark-trino` | Hive | Iceberg | Spark | Trino |
| `polaris-iceberg-spark-duckdb` | Polaris | Iceberg | Spark | DuckDB |
| `polaris-iceberg-spark-none` | Polaris | Iceberg | Spark | None |
| `polaris-iceberg-spark-thrift` | Polaris | Iceberg | Spark | Spark Thrift |
| `polaris-iceberg-spark-trino` | Polaris | Iceberg | Spark | Trino |

<!-- END GENERATED: recipe-components -->

## Support States

Support is judged per workload x recipe x mode, not per recipe:

- **supported**: the release validation record
  (`src/lakebench/config/validated_combinations.yaml`) lists run ids that
  completed this workload x recipe x mode end to end on the release tree with
  the correctness contract passing.
- **unverified**: valid for the workload and mode, but no release validation
  run is listed. It runs, and its evidence and `compare` output carry the
  state; an unverified run is not proof that the combination is supported.
- **unsupported**: refused before a run, at config load (or by `run` when
  `--continuous` selects a mode the workload does not declare).

Every run's `metrics.json` records the state in `experiment.support`, and
`lakebench config show`, `lakebench config recipes`, the HTML report and
`lakebench compare` show it. The table below is generated from the code.

<!-- BEGIN GENERATED: support-states -->
<!-- Generated from the code by `python3.11 -m lakebench.config.support .`; do not edit by hand. -->

| Recipe | Customer 360 batch | Customer 360 continuous | AML (financial) batch | AML (financial) continuous |
|---|---|---|---|---|
| `hive-delta-spark-none` | unverified | unverified | unsupported | unsupported |
| `hive-delta-spark-thrift` | unverified | unverified | unsupported | unsupported |
| `hive-delta-spark-trino` | unverified | unverified | unsupported | unsupported |
| `hive-iceberg-spark-duckdb` | unverified | unverified | unverified | unverified |
| `hive-iceberg-spark-none` | unverified | unverified | unverified | unverified |
| `hive-iceberg-spark-thrift` | unverified | unverified | unverified | unverified |
| `hive-iceberg-spark-trino` | unverified | unverified | unverified | unverified |
| `polaris-iceberg-spark-duckdb` | unverified | unverified | unverified | unverified |
| `polaris-iceberg-spark-none` | unverified | unverified | unverified | unverified |
| `polaris-iceberg-spark-thrift` | unverified | unverified | unverified | unverified |
| `polaris-iceberg-spark-trino` | unverified | unverified | unverified | unverified |

- **unsupported**, refused at config load: AML (financial) on `hive-delta-spark-none`, `hive-delta-spark-thrift`, `hive-delta-spark-trino`. The financial (AML) workload supports table_format iceberg, not delta. Its stage scripts and table DDL are written for iceberg only, so this combination would not run the workload it names. Set architecture.table_format.type to iceberg (for example recipe: polaris-iceberg-spark-trino).
- Any catalog, table format and query engine combination that is not a recipe above is refused at config load for every workload.
- AML (financial) continuous: AML continuous runs detection rules W2, W3, W4, W17 each tick and records W1, W5, W6, W7, W8 as not run. Its results depend on when detection ran relative to arrival, so no end-of-run result check is recorded.

<!-- END GENERATED: support-states -->

Multi-cycle batch is available via the `cycles: N` field on a `batch` config; there is no separate `iterative` mode.

## Excluded Combinations

All of these are refused at config load.

| Combination | Reason |
|------------|--------|
| **AML (financial) + Delta** | The AML stage scripts and table DDL write Iceberg tables only. |
| **Polaris + Delta** | Polaris is an Iceberg-native REST catalog. Delta requires Hive. |
| **Delta + DuckDB** | DuckDB's `delta` extension uses `delta-kernel-rs` which bypasses `httpfs` S3 settings and tries AWS IMDS (169.254.169.254) for credentials. Hangs on non-AWS Kubernetes. |
| **Unity (any format, any engine)** | Not supported. The `unity` catalog value is accepted by the schema but no Unity combination is in the supported list. OSS Unity Catalog 0.4.0's Iceberg REST API is read-only; `UCSingleCatalog` 0.4.0 calls `generateTemporaryTableCredentials` (STS) even for EXTERNAL Delta tables, and object stores without STS (FlashBlade) cannot serve it; Trino's Delta connector needs a Hive Metastore, which Unity deployments do not include. |

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

### Delta + Spark Thrift

- **Q2 and Q6 (RFM) benchmark queries**: `MIN(interaction_date)` (Q2) and
  `MAX(interaction_date)` (Q6) trigger the delta-spark
  `OptimizeMetadataOnlyDeltaQuery` bug (`ClassCastException: LocalDate -> java.sql.Date`)
  through Spark. Trino is not affected (different optimizer). lakebench sets
  `spark.databricks.delta.optimizeMetadataQuery.enabled=false` for Delta + Hive on the
  Thrift server and Spark jobs (LB-148), so these queries run, at the cost of scanning
  instead of answering MIN/MAX/COUNT from the Delta log. A Q2 failure on Delta + Thrift
  is now treated as a regression, not a known failure.

### Delta + Trino and Delta + Spark Thrift

- **OPTIMIZE OOM**: `ALTER TABLE ... EXECUTE optimize` rewrites the entire table in one pass
  and has exhausted Trino worker and Spark Thrift memory at scale 1+.
  Lakebench never runs Delta OPTIMIZE, neither before the benchmark nor in the
  continuous loop. VACUUM still runs on Trino.

- **VACUUM requires catalog prefix**: `CALL {catalog}.system.vacuum(...)`, not
  `CALL system.vacuum(...)`. Retention below the 7-day default also needs
  `SET SESSION {catalog}.vacuum_min_retention = '0s'`, and it must travel in the
  same `trino --execute` submission as the `CALL`: a session property set in a
  separate submission is gone before the `CALL` runs. Before v1.6 lakebench sent
  them separately, so no Delta VACUUM ever applied its requested retention
  (LB-173).
- **No effective VACUUM in short continuous runs**: while streams are live,
  VACUUM keeps Delta's 7-day default retention so a lagging stream never reads
  a vacuumed file. A continuous run shorter than 7 days therefore removes
  nothing.
- **Delta + Spark Thrift runs no maintenance**: VACUUM is skipped because it
  runs Spark Thrift out of memory at 4 GiB.

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
| Unity | Delta | Not supported | Refused at config load. See Excluded Combinations. |
