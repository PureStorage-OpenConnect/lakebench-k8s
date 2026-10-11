# Lakebench Compatibility Matrix

Reference: the components Lakebench deploys, recipes, support states, excluded combinations, and every component version and override.

## Components

| Component | Role | Page |
|---|---|---|
| Apache Spark | Pipeline jobs (bronze, silver, gold) | [Spark](component-spark.md) |
| Kubeflow Spark Operator | Submits SparkApplication CRDs and manages the job lifecycle. v1.x breaks ConfigMap volume injection. | [Spark](component-spark.md#spark-operator) |
| Hive Metastore | Thrift catalog for Iceberg and Delta tables, run by the Stackable Hive Operator (HiveCluster) | [Hive](component-hive.md) |
| Apache Polaris | REST Iceberg catalog with OAuth2 | [Polaris](component-polaris.md) |
| Apache Iceberg, Delta Lake | Open table formats, loaded as Spark runtime jars. Delta needs Spark 4.x and Hive. | [Version matrix](#spark--table-format-version-matrix) |
| Trino | Distributed SQL engine; the default | [Trino](component-trino.md) |
| Spark Thrift Server | Spark SQL over HiveServer2 JDBC, on `images.spark` | [Spark Thrift](component-spark-thrift.md) |
| DuckDB | Single-pod SQL engine | [DuckDB](component-duckdb.md) |
| PostgreSQL | Metadata backend for Hive and Polaris; a StatefulSet with a persistent volume | [PostgreSQL](component-postgres.md) |
| Observability (optional) | Prometheus, Grafana, Pushgateway | [Observability](component-observability.md) |
| S3 object store | All pipeline data | [Storage backends](storage-backends.md) |

- Hive needs the Stackable operators (commons, secret, listener, hive) installed cluster-wide: [Operations](operations.md#installing-the-shared-pieces).
- Which recipe to choose: [Recipes](recipes.md).

## Recipes

Each recipe is one entry of the architecture list Lakebench validates at
config load. Any other catalog, table format, pipeline engine and query
engine combination is refused at load with the reason.

- Every recipe deploys PostgreSQL and its catalog (Hive Metastore or Polaris), and runs Spark for the pipeline.
- Each recipe has at most one query engine (`none` for ETL only). The benchmark runs against that engine.

<!-- BEGIN GENERATED: recipe-components -->
<!-- Generated from the code by `PYTHONPATH=src python3.11 -m lakebench.config.support .`; do not edit by hand. -->

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

Support is judged per workload x recipe x mode and per component version,
not per recipe.

- **supported**: the release validation record
  (`src/lakebench/config/validated_combinations.yaml`) lists runs that
  completed this workload x recipe x mode end to end on the release tree,
  with the correctness contract passing.
  - The record is keyed by workload, recipe, mode, Spark minor (from the
    Spark image tag) and table format version (the resolved Iceberg or Delta
    version). An entry validated on Spark 4.1 does not make a Spark 4.0 run
    of the same recipe supported.
  - A supported cell names the version pairs its runs used.
  - The Spark minor is read only from an `apache/spark` image (any registry
    prefix) with a release tag such as `4.1.1-python3`. A run on any other
    Spark image is never stamped supported.
  - The record is generated from the release-matrix run records, never
    edited by hand. Lakebench refuses to load an entry that is not a
    release-matrix row at the matrix's versions. The record is empty until
    the release runs fill it.
- **unverified**: valid for the workload and mode, but no release validation
  run is listed. It runs, and its evidence and report carry the state. An
  unverified run is not proof that the combination is supported.
- **unsupported**: refused before a run, at config load (or by `run` when
  `--continuous` selects a mode the workload does not declare).

Scale caps the state. Datagen is banded per workload:

| Workload | Supported up to | Unverified up to | Refused above |
|---|---|---|---|
| Customer 360 | scale 300 | scale 600 | scale 600 |
| AML (financial) | scale 300 | scale 800 | scale 800 |

- Above the supported maximum a run is at most unverified.
- Above the ceiling a datagen pod would exceed the 16 GiB per-pod memory
  cap (a Lakebench cap), and `deploy` and `generate` refuse the config.
- `lakebench config show` prints the band note for the config's scale.

How the state is recorded:

- It is computed when the run starts and stored in `metrics.json` as
  `experiment.support`. Rendering the record later does not re-stamp it.
- A run from a Lakebench checkout with local changes is never stamped
  supported.
- `--local` runs Customer 360 in batch mode only and refuses anything else.
  Local runs are at most unverified.
- `lakebench config show`, `lakebench config recipes` and the HTML report
  show it.

The table below is generated from the code.

<!-- BEGIN GENERATED: support-states -->
<!-- Generated from the code by `PYTHONPATH=src python3.11 -m lakebench.config.support .`; do not edit by hand. -->

| Recipe | Customer 360 batch | Customer 360 continuous | AML (financial) batch | AML (financial) continuous |
|---|---|---|---|---|
| `hive-delta-spark-none` | unverified [1] | unverified [1] | unsupported | unsupported |
| `hive-delta-spark-thrift` | supported (Spark 4.0, Delta 4.0.0) | unverified [1] | unsupported | unsupported |
| `hive-delta-spark-trino` | supported (Spark 4.1, Delta 4.1.0) | supported (Spark 4.1, Delta 4.1.0) | unsupported | unsupported |
| `hive-iceberg-spark-duckdb` | supported (Spark 4.1, Iceberg 1.11.0) | unverified [1] | unverified [1] | unverified [1] |
| `hive-iceberg-spark-none` | supported (Spark 4.1, Iceberg 1.11.0) | unverified [1] | unverified [1] | unverified [1] |
| `hive-iceberg-spark-thrift` | supported (Spark 4.1, Iceberg 1.11.0) | unverified [1] | unverified [1] | unverified [1] |
| `hive-iceberg-spark-trino` | supported (Spark 4.1, Iceberg 1.11.0) | supported (Spark 4.1, Iceberg 1.11.0) | supported (Spark 4.1, Iceberg 1.11.0) | supported (Spark 4.1, Iceberg 1.11.0) |
| `polaris-iceberg-spark-duckdb` | supported (Spark 4.0, Iceberg 1.11.0) | unverified [1] | unverified [1] | unverified [1] |
| `polaris-iceberg-spark-none` | unverified [1] | unverified [1] | unverified [1] | unverified [1] |
| `polaris-iceberg-spark-thrift` | supported (Spark 4.0, Iceberg 1.11.0) | unverified [1] | unverified [1] | unverified [1] |
| `polaris-iceberg-spark-trino` | supported (Spark 4.0, Iceberg 1.11.0) | unverified [1] | supported (Spark 4.0, Iceberg 1.11.0) | supported (Spark 4.0, Iceberg 1.11.0) |

- [1] unverified: not in this release's validation matrix.
- **unsupported**, refused at config load: AML (financial) on `hive-delta-spark-none`, `hive-delta-spark-thrift`, `hive-delta-spark-trino`. The financial (AML) workload supports table_format iceberg, not delta. Its stage scripts and table DDL are written for iceberg only, so this combination would not run the workload it names. Use an iceberg recipe (for example recipe: polaris-iceberg-spark-trino), or with no recipe set architecture.table_format.type to iceberg.
- Any catalog, table format and query engine combination that is not a recipe above is refused at config load for every workload.
- A supported cell names the Spark minor and table format version its validation runs used; the same cell on any other Spark minor or format version is unverified. Spark 3.5 is unverified and gets no v1.7 features.
- AML (financial) continuous: AML continuous runs detection rules W2, W3, W4, W5, W6, W17 each tick and records W1, W7, W8 as not run (from 1.7.1; earlier records name the rules they ran). W4 raises one alert per entity per week, and W5 screens payments as they arrive, with no rescreen of earlier payments when a list version is published. Its results depend on when detection ran relative to arrival, so no end-of-run result check is recorded.

<!-- END GENERATED: support-states -->

Multi-cycle batch is the `cycles: N` field on a `batch` config. There is no
separate `iterative` mode.

## Excluded Combinations

All of these are refused at config load.

| Combination | Reason |
|------------|--------|
| **AML (financial) + Delta** | The AML stage scripts and table DDL write Iceberg tables only. |
| **Polaris + Delta** | Polaris is an Iceberg-native REST catalog. Delta requires Hive. |
| **Delta + DuckDB** | DuckDB's `delta` extension uses `delta-kernel-rs`, which bypasses the `httpfs` S3 settings and asks AWS IMDS (169.254.169.254) for credentials. It hangs on non-AWS Kubernetes. |
| **Unity (any format, any engine)** | The schema accepts `unity`, but no Unity combination is in the supported list. See below. |

Why Unity is excluded:

- OSS Unity Catalog 0.4.0's Iceberg REST API is read-only.
- `UCSingleCatalog` 0.4.0 calls `generateTemporaryTableCredentials` (STS) even for EXTERNAL Delta tables. Object stores without STS (FlashBlade) cannot serve it.
- Trino's Delta connector needs a Hive Metastore, which Unity deployments do not include.

## Component Version Matrix

Defaults come from `src/lakebench/config/schema.py` and
`src/lakebench/config/recipes.py`. Override them as in
[Overriding Versions](#overriding-versions).

| Component | Default | Accepted | Set by |
|---|---|---|---|
| Apache Spark | `apache/spark:4.1.1-python3` on the Hive recipes except below; `4.0.2-python3` on the Polaris recipes, `hive-delta-spark-thrift` and `hive-delta-spark-none` | Any 3.5.x, 4.0.x or 4.1.x image with a `-python3` tag. Delta needs 4.x. 4.2 and other minors are refused at load. | `images.spark` |
| Kubeflow Spark Operator | 2.5.1 (Helm chart `spark-operator/spark-operator`, namespace `spark-operator`) | v2.x. v1.x is not supported. | `platform.compute.spark.operator.version` (fresh install only) |
| Apache Iceberg | 1.11.0; 1.10.1 on a Java 11 Spark 3.5 image; 1.10.1 in local mode | See [Spark + Table Format Version Matrix](#spark--table-format-version-matrix) | `architecture.table_format.iceberg.version` |
| Delta Lake | `auto`: 4.0.0 on Spark 4.0, 4.1.0 on Spark 4.1 | Only the version matching the Spark minor | `architecture.table_format.delta.version` |
| Hadoop AWS | 3.3.4 on Spark 3.5, 3.4.1 on 4.0, 3.4.2 on 4.1 | Fixed by Spark minor | Not configurable |
| Hive Metastore | 3.1.3 | 3.1.3 only (Hive 4 breaks Iceberg and Trino ANALYZE) | Fixed; the Stackable HiveCluster `productVersion` |
| Stackable operators (commons, secret, listener, hive) | SDP 25.7.0, from `oci://oci.stackable.tech/sdp-charts` | An installed SDP keeps its version | `architecture.catalog.hive.operator.version` (fresh install only) |
| Apache Polaris | `apache/polaris:1.6.0`, `apache/polaris-admin-tool:1.6.0` | 1.3.0-incubating or later | `images.polaris`, `images.polaris_admin_tool` |
| Trino | `trinodb/trino:483` | 454 or later with Polaris | `images.trino` |
| DuckDB | 1.5.5, pip-installed on `python:3.11-slim` (wheel resolved by `lb-deps`) | Iceberg only | `architecture.query_engine.duckdb.version`, `images.duckdb` |
| PostgreSQL | `postgres:17` | Tested with 16, 17, 18 | `images.postgres` |
| kube-prometheus-stack | Chart 87.19.2 (Prometheus v3.13.1, Grafana v13.1.x) | Prometheus and Grafana come from the chart | `observability.chart_version` |
| Pushgateway | `prom/pushgateway:v1.11.1` | | `observability.pushgateway_image` |

The deployment's dependency server (`lb-deps`) resolves the Iceberg or Delta
runtime, Hadoop AWS and DuckDB jars once at deploy.

## Overriding Versions

Every component image and library version can be overridden in the YAML
config, within these limits:

- Spark must be a 3.5.x, 4.0.x or 4.1.x `-python3` image. Spark 4.2 and other
  minors are rejected at config load.
- An explicit Iceberg or Delta version must be in the compatibility list for
  the Spark minor, or config load fails. Iceberg 1.11+ needs a Java 17 Spark
  image.
- The Hive version is not configurable
  ([Hive Reference](component-hive.md#version-and-image)).
- The Polaris version that runs is the tag of `images.polaris`.

```yaml
images:
  spark: apache/spark:4.0.2-python3
  postgres: postgres:17
  polaris: apache/polaris:1.6.0
  trino: trinodb/trino:483
  duckdb: python:3.11-slim

architecture:
  table_format:
    iceberg:
      version: "1.11.0"
```

Image pulls:

- `images.pull_policy` (default `Always`) applies to the pods Lakebench
  renders with it, Spark, Trino and datagen among them. A re-pushed tag is
  pulled again there.
- Lakebench sets no pull policy on the Unity, Pushgateway and Stackable Hive
  pods.
- A private registry needs an `imagePullSecret` on the namespace's service
  accounts. Lakebench does not set one.

Full YAML schema: [Configuration](configuration.md).

## Spark + Table Format Version Matrix

Format versions are **auto-selected** from the Spark image version. An
explicit version overrides it. An incompatible combination is rejected at
config load.

| Spark | Delta 4.0.0 | Delta 4.1.0 | Iceberg 1.11.0 | Iceberg 1.10.1 | Iceberg 1.10.0 | Iceberg 1.5.2-1.9.1 |
|-------|-------------|-------------|----------------|----------------|----------------|------|
| 3.5.x | -- | -- | **Default** (java17 image) | OK (default on a Java 11 image) | -- | OK |
| 4.0.x | **Default** | -- | **Default** | OK | OK | -- |
| 4.1.x | -- | **Default** | **Default** | OK | OK | -- |

**Default** = auto-selected. **OK** = accepted as an override. **--** = rejected.
Spark 3.5 accepts Iceberg 1.5.2, 1.6.1, 1.7.1, 1.8.1 and 1.9.1. Spark 4.0 and
4.1 runtimes exist only for Iceberg 1.10.0 and later.

### Iceberg 1.11.0 requires Java 17

- Iceberg 1.11.0 is Java 17 bytecode; 1.10.x was Java 11.
- Every Spark 4.x image ships Java 17, so only Spark 3.5 is affected.
- `apache/spark:3.5.4-python3` ships Java 11. Iceberg 1.11.0 on it fails at
  class load with `UnsupportedClassVersionError`.
- With `table_format.iceberg.version` at its default, Lakebench detects a
  Java 11 Spark 3.5 image and uses Iceberg 1.10.1, with a warning.
- An explicitly chosen Iceberg 1.11.x on a Java 11 image is rejected at
  config load, not inside the Spark driver.

On Spark 3.5, use a java17 image tag:

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

### Which runtime jar gets requested

| Spark | Iceberg 1.10.x | Iceberg 1.11.0 | Delta |
|-------|----------------|----------------|-------|
| 3.5.x | `iceberg-spark-runtime-3.5_2.12` | `3.5_2.12` | -- |
| 4.0.x | `4.0_2.13` | `4.0_2.13` | `io.delta:delta-spark_2.13` |
| 4.1.x | `4.0_2.13` (borrowed) | `4.1_2.13` (native) | `delta-spark_4.1_2.13` |

- Spark 4.1 borrows the 4.0 runtime on Iceberg 1.10.x, which has no 4.1
  artifact. 1.11.0 publishes one.
- The Spark jobs and the Spark Thrift server choose the runtime the same
  way, so both load one runtime jar.

```yaml
images:
  spark: apache/spark:4.1.1-python3    # the Hive recipes' default
architecture:
  table_format:
    type: delta
    # delta.version auto-resolves to 4.1.0. An explicit delta.version must
    # match the Spark minor: only 4.1.0 on Spark 4.1, 4.0.0 on Spark 4.0.
```

## Known Limitations

### Delta + Spark Thrift

- **Q2 and Q6 (RFM)**: `MIN(interaction_date)` (Q2) and
  `MAX(interaction_date)` (Q6) hit the delta-spark
  `OptimizeMetadataOnlyDeltaQuery` bug
  (`ClassCastException: LocalDate -> java.sql.Date`) through Spark. Trino is
  not affected. Lakebench sets
  `spark.databricks.delta.optimizeMetadataQuery.enabled=false` for Delta +
  Hive on the Thrift server and Spark jobs. The queries then scan instead of
  answering MIN/MAX/COUNT from the Delta log. A Q2 failure on Delta + Thrift
  is a regression.
- **Silver file layout**: the Delta silver build clusters rows by
  `interaction_date` before the write (a `REBALANCE` hint, the Delta
  counterpart of Iceberg's `write.distribution-mode=hash`). The driver log
  reports the commit's file count. `spark.lb.silver.distribution_mode=none`
  restores the direct write: one file per day per write task, tens of
  thousands of small files at scale 1, and Q3 and Q6 over the 300 s query
  timeout on Spark Thrift.

### Delta + Trino and Delta + Spark Thrift

- **No OPTIMIZE**: `ALTER TABLE ... EXECUTE optimize` rewrites the whole
  table in one pass and has exhausted Trino worker and Spark Thrift memory
  at scale 1+. Lakebench never runs Delta OPTIMIZE, before the benchmark or
  in the continuous loop. VACUUM still runs on Trino.
- **VACUUM needs the catalog prefix**: `CALL {catalog}.system.vacuum(...)`,
  not `CALL system.vacuum(...)`. Retention below the 7-day default also
  needs `SET SESSION {catalog}.vacuum_min_retention = '0s'` in the same
  `trino --execute` submission as the `CALL`. A session property set in a
  separate submission is gone before the `CALL` runs. Lakebench releases
  before v1.6 sent them separately, so their Delta VACUUM never applied the
  requested retention.
- **No effective VACUUM in short continuous runs**: while streams are live,
  VACUUM keeps Delta's 7-day default retention so a lagging stream never
  reads a vacuumed file. A continuous run shorter than 7 days removes
  nothing.
- **Delta + Spark Thrift runs no maintenance**: neither VACUUM nor OPTIMIZE,
  before the benchmark or in the continuous loop. The run records both as
  not supported. VACUUM is skipped because it ran Spark Thrift out of memory
  at 4 GiB. Lakebench builds the Spark form
  (`SET spark.databricks.delta.retentionDurationCheck.enabled=false; VACUUM
  <table> RETAIN <n> HOURS` in one beeline submission) but does not run it.

### Delta + Hive

- Tables live in the **session catalog** (`spark_catalog`), not a named
  catalog. `DeltaCatalog` is a `CatalogExtension` and must override
  `spark_catalog`. Benchmark queries use `spark_catalog.schema.table`.

### Platform Requirements

- **Kubernetes**: 1.26+. Tested on OpenShift 4.x.
- **S3**: any S3-compatible store that passes `lakebench config storage`.
  Tested on Pure Storage FlashBlade and Garage (local mode). AWS S3 and MinIO
  are not yet validated: [Storage Backends](storage-backends.md).
- **S3 addressing**: path-style access required (FlashBlade, MinIO).
  Virtual-hosted style is not tested.
- **OpenShift**: Spark pods (UID 185) need the `anyuid` SCC. See
  [Spark](component-spark.md#openshift-security-context-constraints-scc).
- **Portworx** (tested): `px-csi-scratch` (repl=1, the scratch default) for
  Spark shuffle, `px-csi-db` (repl=3) for PostgreSQL.
- **PostgreSQL auth**: the Lakebench PostgreSQL container starts with
  SCRAM-SHA-256 (`POSTGRES_HOST_AUTH_METHOD=scram-sha-256`,
  `--auth-host=scram-sha-256`), not MD5. Nothing to configure. The JDBC drivers in Hive, Polaris and Unity support it.

## Catalog + Table Format Behavior

| Catalog | Format | Mechanism | Notes |
|---------|--------|-----------|-------|
| Hive | Iceberg | SparkCatalog with Hive Thrift backend | Tables registered via Thrift. Trino reads via the Iceberg connector. |
| Hive | Delta | DeltaCatalog as session catalog extension | Tables in Hive "Spark SQL specific format". Trino reads via the Delta connector. |
| Polaris | Iceberg | SparkCatalog with REST backend | OAuth2 auth. Trino reads via the Iceberg REST connector. |
| Unity | Delta | Not supported | Refused at config load. See [Excluded Combinations](#excluded-combinations). |
