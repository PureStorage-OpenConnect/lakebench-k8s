# Internals

Maintainer background: why parts of Lakebench are built the way they are,
and what not to change without a live test. Every statement points at the
code or test that holds it, so it can be checked against the tree you are
reading. User-facing symptoms and fixes are in
[Troubleshooting](troubleshooting.md).

Numbers that matter (scratch sizes, cluster minimums) are not repeated
here. They come from the job profiles and are published in the generated
sizing tables in [Getting Started](getting-started.md) and the
[README](../README.md), which a drift test keeps equal to the code
(`tests/test_sizing_tables_drift.py`).

---

## Spark job sizing

**Per-executor sizing is fixed and is not autosized.** Each Spark job's
driver and executor cores, memory, overhead and scratch size are set per
job type in `src/lakebench/modules/pipeline_engines/spark/job.py:_JOB_PROFILES`,
with per-workload changes in
`src/lakebench/modules/pipeline_engines/spark/job.py:_SCHEMA_PROFILE_OVERRIDES`
(the AML bronze-verify CTAS fallback and the continuous AML jobs) merged by
`src/lakebench/modules/pipeline_engines/spark/job.py:_resolve_job_profile`.
The values were raised after live failures: silver-build and gold-finalize
scratch after "No space left on device" at scale 100, the silver and gold
driver after out-of-memory errors on Spark 4. The comments beside each
entry record why it was raised. Do not reduce them; a change needs a live
run at the scale that set the value. The tables in
[Job Profiles](component-spark.md#job-profiles) show the current values.

**Executor counts scale with data, up to a cap.**
`src/lakebench/modules/pipeline_engines/spark/job.py:executor_count` keeps
the base count at scale 10 and below, adds a per-job rate above it, and
caps the result at the profile's `max_executors`, which is never above
`src/lakebench/modules/pipeline_engines/spark/job.py:_MAX_EXECUTORS_SAFE`:
at 32 or more executors the fabric8 Kubernetes client in the driver causes
API polling storms. Because the base count is flat up to scale 10, scale 1
and scale 10 have the same Spark peak (datagen and the always-on pods still
grow), and data per executor grows with scale inside the flat and the
capped ranges.

**Cluster minimums come from `compute_peak_requirements()`, never from an
estimate.** `src/lakebench/modules/pipeline_engines/spark/job.py:compute_peak_requirements`
is the one Spark source. Batch jobs run one after another
(`src/lakebench/modules/pipeline_engines/spark/job.py:BATCH_JOB_TYPES`), so
the batch peak is the largest single job, not the sum; continuous jobs run
at once, so the continuous peak is their sum. Driver pods count Spark's own
driver overhead, which the manifests do not set
(`src/lakebench/modules/pipeline_engines/spark/job.py:_driver_pod_bytes`).
`src/lakebench/config/sizing.py:plan_requirements` adds datagen and the
always-on pods, and is what `config show`, `config recommend`, the `run`
capacity preflight and the generated tables use. Regenerate the tables with
`python3.11 scripts/gen_sizing_tables.py` rather than editing a number;
`tests/test_sizing.py` holds hand-derived floors for the formula.

**`compute_guidance()` does not size Spark.**
`src/lakebench/config/scale.py:compute_guidance` maps a scale to a tier.
For Spark only its tier name is shown (`validate`, and the hidden `info`);
its executor and memory hints are not what the jobs request. Through
`src/lakebench/config/scale.py:full_compute_guidance` it does feed the
autosizer's Trino and datagen defaults
(`src/lakebench/config/autosizer.py:resolve_auto_sizing`).

---

## Spark Operator

**Volumes go through pod templates.** Spark's
`spark.kubernetes.*.volumes.*` properties have no `configMap` type, so the
scripts volume, the work-dir emptyDir and the driver's dependency download
volume are declared in the driver and executor pod templates; only the
executor scratch PVC uses the conf properties. Whether the operator's
webhook would inject a ConfigMap volume declared on the SparkApplication
has never been tested live, so the template route stays until a live test
shows it does.
Code: `src/lakebench/modules/pipeline_engines/spark/job.py:_build_manifest`,
`src/lakebench/modules/pipeline_engines/spark/scripts_maps.py:scripts_volume`.

**`helm upgrade --reuse-values` does not backfill new chart values.** It
carries forward only the keys the stored release already has. A value a
newer chart added renders empty, and the operator can crash-loop on it (the
2.4.0 to 2.5.1 upgrade did, on an empty
`--metrics-job-submit-latency-buckets`). Every `--reuse-values` upgrade that
pins a version must set such values itself.
`src/lakebench/modules/pipeline_engines/spark/operator.py:SparkOperatorManager`
does this in `_reuse_values_backfill` (the latency buckets when a version is
pinned, and the controller `/tmp` size) and in `_watch_list_pin` for
watch-list upgrades. Helm's own `--set` parser splits on unescaped commas,
even when argv is built as a Python list, so a comma inside a value is
written `\,` (`_JOB_SUBMIT_LATENCY_BUCKETS_DEFAULT`).

---

## Versions and artifacts

**Hive 3.1.3 is deliberate.** The Stackable HiveCluster takes a
`productVersion`, not an image, and Lakebench renders the constant
`src/lakebench/config/schema.py:STACKABLE_HIVE_VERSION`; deploy and run
provenance record it. Stackable resolves the real image
(`oci.stackable.tech/sdp/hive:<version>-stackable<sdp version>`). Do not
move to Hive 4: Iceberg fails against it with `TApplicationException:
Invalid method name: 'get_table'`
([apache/iceberg#12878](https://github.com/apache/iceberg/issues/12878)) and
Trino's repeated ANALYZE fails against metastore 4.0
([trinodb/trino#26214](https://github.com/trinodb/trino/issues/26214));
revisit when both are fixed and a live Iceberg and Trino run passes on
Hive 4. `images.hive` is a removed key for the same reason
(`src/lakebench/config/schema.py:ImagesConfig`).

**Polaris server and admin tool move together.** `images.polaris` and
`images.polaris_admin_tool` publish the same version set and default to the
same tag (`src/lakebench/config/schema.py:ImagesConfig`). Polaris dropped
the `-incubating` suffix at 1.4.0; never filter a tag listing on
`incubating`, which hides every release after 1.3.0.

**The Iceberg runtime artifact depends on both versions.** Iceberg does not
publish the same Spark runtimes in every release: 1.11.0 added a native
Spark 4.1 runtime, 1.10.x has none, so Spark 4.1 on 1.10.x borrows the 4.0
jar. Choosing on the Spark version alone would request an artifact that
does not exist, and the failure would arrive as a Maven resolution error in
the driver.
`src/lakebench/modules/pipeline_engines/spark/job.py:iceberg_runtime_suffix_for`
reads `_ICEBERG_NATIVE_RUNTIME_FROM` first and falls back to
`_ICEBERG_RUNTIME_SUFFIX`.

**Iceberg 1.11 needs Java 17.** Its jars carry class-file major 61 (1.10.x
carried 55), and the plain Spark 3.5 images ship Java 11. When the user did
not choose a version, `src/lakebench/config/schema.py:resolve_format_versions`
falls back to Iceberg 1.10.1 with a warning; when they did,
`src/lakebench/modules/pipeline_engines/spark/job.py:validate_iceberg_java_runtime`
refuses the pairing at load, before it can fail as
`UnsupportedClassVersionError` inside the driver.

**Spark 4.2 is excluded on purpose.**
`src/lakebench/modules/pipeline_engines/spark/job.py:_SUPPORTED_SPARK_VERSIONS`
has no 4.2 entry: no Iceberg release ships a 4.2 runtime (as of 2026-09), and the borrowed
4.1 runtime throws `IncompatibleClassChangeError` loading Iceberg's
`SparkView` at the first table write. Adding 4.2 needs a released
`iceberg-spark-runtime-4.2_2.13` on Maven Central, a 4.2 entry in the
runtime-suffix tables, and a live write-path run through the whole
pipeline; a version-bump smoke test cannot catch this kind of binary break.

**Delta 4.1 renamed its artifact.** Delta 4.0.x publishes
`delta-spark_2.13`; 4.1.0 and later put the Spark minor in the name,
`delta-spark_4.1_2.13`.
`src/lakebench/modules/pipeline_engines/spark/job.py:_delta_spark_artifact`
builds both forms. Check an artifact by fetching its POM, not with the
Maven search API, which misses some (see
[Troubleshooting](troubleshooting.md#a-maven-artifact-seems-not-to-exist)).

**`table_format.delta.version` defaults to `auto`.**
`src/lakebench/config/schema.py:DeltaConfig` declares it, and
`src/lakebench/modules/pipeline_engines/spark/job.py:resolve_format_version`
resolves it to the Delta built for the Spark image (`_FORMAT_VERSION_DEFAULTS`);
`_FORMAT_VERSION_COMPAT` refuses an explicit version built for another
Spark minor, so a config that names no Delta version keeps working when the
Spark image changes minor.

---

## Catalogs and PostgreSQL

**The Polaris admin tool is a jar.** The admin-tool image has no `polaris`
binary; the bootstrap Job runs `java -jar
/deployments/polaris-admin-tool.jar bootstrap ...`
(`src/lakebench/templates/polaris/bootstrap-job.yaml.j2`).

**Bootstrap is made safe to repeat whatever upstream does.** Polaris
1.3.0's admin tool threw `already been bootstrapped` on a second run;
whether 1.6.0 still does has not been checked live here.
The Job's script treats that message as success either way, and because
Kubernetes Jobs are immutable the deployer deletes the old Job before it
creates a new one
(`src/lakebench/modules/catalogs/polaris/deployer.py:PolarisDeployer`,
`_delete_old_bootstrap_job`).

**PostgreSQL DDL through `kubectl exec` checks before it creates.** The deployers
do not use psql's `\gexec`, and PostgreSQL does not allow `CREATE DATABASE`
inside a `DO` block, so the Polaris deployer checks for its role
and database with a query first, creates what is missing, and sets the role
password from a SCRAM verifier rather than a plaintext password
(`src/lakebench/modules/catalogs/polaris/deployer.py:PolarisDeployer`,
`_create_polaris_db`;
`src/lakebench/deploy/deployment_secrets.py:scram_sha256_verifier`).

**Delta with Hive uses the session catalog.** Delta's `DeltaCatalog` is a
catalog extension, not a standalone catalog, so it must replace
`spark_catalog` and tables are addressed as `spark_catalog.<schema>.<table>`.
The job conf sets it in
`src/lakebench/modules/pipeline_engines/spark/job.py:_build_manifest`, the
job environment passes `LB_ICEBERG_CATALOG=spark_catalog` to the scripts,
and the scripts read it through
`src/lakebench/spark/scripts/common.py:pipeline_catalog`.

**The Delta scripts avoid REPLACE TABLE AS SELECT.** Unity's
`UCSingleCatalog` 0.4.0 does not support it, so the Delta scripts check
whether a table exists and append to create it, overwriting only an
existing table (`src/lakebench/spark/scripts/gold_finalize_delta.py:_safe_write_mode`,
`src/lakebench/spark/scripts/silver_build_delta.py:_table_exists`). Unity
0.4.1 says it fixed this, but no Unity combination is in
`src/lakebench/config/schema.py:_SUPPORTED_COMBINATIONS`, so the workaround
stays until a Unity recipe is supported and RTAS is proven live on it.

---

## Storage conformance

**`config storage` reports; it does not gate.** S3 implementations differ
in behaviour that mocked tests cannot see (SeaweedFS returns an empty
bucket list while `head_bucket` succeeds, Garage checks the signing
region), so `lakebench config storage <config>` runs graded checks against
the real backend. A REQUIRED failure (connectivity, bucket enumeration,
object operations, multipart abort) means Lakebench's code paths break
there; an ADVISORY result (region strictness) changes Spark config and is
recorded, not treated as a defect; the bucket-tagging check is advisory
too and reports whether the backend supports bucket tags, which destroy
uses to prove bucket ownership. The command never blocks `deploy` or
`run`, so a store that works is never refused for being unlisted, and an
account without `CreateBucket` gets read-only checks with the write checks
reported as skipped rather than failed. The runner and the known-backend
list are `src/lakebench/s3/conformance.py:ConformanceRunner` and
`src/lakebench/s3/conformance.py:KNOWN_BACKENDS`; the mocked tests are
`tests/test_s3_conformance_runner.py` and the live suite
`tests/test_s3_conformance.py`. Keep those, the command in
`src/lakebench/cli/_config.py` and [Storage Backends](storage-backends.md)
in step when a check changes.

---

## Table maintenance

**Iceberg expiry and orphan removal are built per engine.**
`src/lakebench/modules/table_formats/iceberg/maintenance.py:build_maintenance_sql`
builds the statements and
`src/lakebench/modules/table_formats/iceberg/maintenance.py:exec_sql` runs
each one through `kubectl exec` on the Trino coordinator or the Spark
Thrift pod, raising on a non-zero exit and with `ExecSqlTimeout` when the
client times out (the statement may still be running in the engine).
DuckDB is read-only for Iceberg and runs none.

- **Trino** refuses a retention under its system minimum unless the
  session property is set in the same CLI process, so each statement is
  one `--execute` string: `SET SESSION <catalog>.expire_snapshots_min_retention
  = '...'; ALTER TABLE ... EXECUTE expire_snapshots(...)`, and the same for
  `remove_orphan_files`.
- **Spark** takes `CALL <catalog>.system.expire_snapshots(...)` with a
  `TIMESTAMP` literal; a computed numeric expression fails argument binding
  on Spark 4.

**Two retention floors.** Orphan removal never uses a retention below
`src/lakebench/modules/table_formats/iceberg/maintenance.py:ORPHAN_MIN_RETENTION_SECONDS`
(24 h 10 min), because removing orphans beside a live writer can delete
files a commit has not yet referenced; the builder enforces it whatever the
caller passes. While streams are live, snapshot expiry is floored at
`src/lakebench/modules/table_formats/iceberg/maintenance.py:LIVE_EXPIRE_MIN_RETENTION_SECONDS`,
so a stream that falls behind does not lose the snapshots it still needs.
The builder does not know about live streams: that floor (and Delta's
7-day floor while streams are live) is applied by
`src/lakebench/cli/_sustained.py:applied_retentions`, which is also what
the run records, so a new call site must take its retentions from it.
`retention_threshold` must be a whole number and one unit (`30m`, `1h`,
`7d`) and is checked at load
(`src/lakebench/config/schema.py:SustainedConfig`).

**Trino compaction is chunked by partition.**
`src/lakebench/modules/table_formats/iceberg/maintenance.py:build_compaction_plan`
splits one `optimize` into several for the tables on
`src/lakebench/modules/table_formats/iceberg/maintenance.py:_COMPACTION_PARTITIONING`,
after `src/lakebench/cli/_sustained.py:_compaction_partitions` reads the
table's partition values from `$partitions`. Each partition an `optimize`
rewrites keeps open Parquet writers, so what one statement can take is
bounded by partitions, not rows:

- **Customer 360 silver** (identity on `interaction_date`): at most 90
  partitions per statement
  (`src/lakebench/modules/table_formats/iceberg/maintenance.py:COMPACTION_CHUNK_PARTITIONS`),
  below Trino's limit of 100 open writers.
- **AML silver** `transactions` and `account_statements` (`months()` on
  `txn_timestamp` and `book_ts`): at most one month with files to merge
  per statement
  (`src/lakebench/modules/table_formats/iceberg/maintenance.py:COMPACTION_CHUNK_MONTHS`).
  A writer buffers up to a row group (128 MB) per partition, and 12 to 13
  months of small continuous files in one statement exceeded the 2.24 GB
  per-node query memory on the single scale-1 Trino worker, about 160 MB a
  month. The read takes each month's data file count; a month of one file
  is not rewritten (Trino 483 skips a partition's only file when it has no
  deletes), so it is not counted and shares a statement with its
  neighbour, which keeps batch silver near one statement (the batch s1
  record counts 65 data files across `silver.transactions` and the gold
  dashboard table over a 60-month corpus). A NULL month (a file under the pre-1.6
  `days()` spec, or a NULL timestamp) makes Trino rewrite every file in
  range, so then every month counts. The bound is sized at scale 1; on
  larger workers Trino may scale one month over more local writers, which
  has not been measured.

Trino accepts `optimize ... WHERE` only when the connector applies the
whole predicate to partitions; anything else fails with "Unexpected
FilterNode found in plan; probably connector was not able to handle
provided WHERE expression". For a `months()` field that means a range on
the source column whose bounds sit on month starts
(`IcebergUtil.canEnforceRangeWithPartitioningField` in Trino 483), so the
bounds are `TIMESTAMP 'YYYY-MM-01 00:00:00.000000 UTC'`: the month
transform of a `timestamp with time zone` is taken in UTC, and an explicit
UTC literal does not depend on the session time zone. The chunks are
contiguous and the first and last are open-ended, so a partition written
after the read, or missing from it, is still compacted by exactly one
statement. A failed partition read falls back to one statement and is
named in the record. Trino settings are not changed; the commits are one
snapshot per chunk, and the record names the operation (`trino_optimize`
with its threshold) as before.

**Where maintenance runs.** In a continuous run,
`src/lakebench/cli/_sustained.py:_run_iceberg_maintenance` runs a round
every `retention_interval` seconds (unset means a third of the window,
clamped to the field's range, `SustainedConfig.effective_retention_interval`); a table that fails is
logged and the round goes on to the next, and the run does not stop for
it. Before a batch benchmark,
`run` runs compaction and maintenance under one shared budget
(`src/lakebench/cli/_run.py:PRE_BENCHMARK_MAINTENANCE_CAP`). A round that
stops on that budget, or runs beside live streams, is recorded
(`maintenance_stopped`, `maintenance_live_streams`), and the perf gate does
not treat the post-maintenance QpH as a measurement
(`src/lakebench/metrics/perf_gate.py:post_qph_unmeasured`). Destroy runs no
maintenance. Delta maintenance is covered in
[Troubleshooting](troubleshooting.md#delta-no-compaction-and-vacuum-only-on-trino).

---

## Metrics and output

**Requested resources, not usage.** Per-stage executor counts, cores and
memory in the record are what the job profiles requested from Kubernetes,
and the core-hour and efficiency figures are built from them; they are
named "requested" for that reason. When a job finishes too fast for the
progress callback to see its executors, the count comes from the config's
per-job executor override when set, otherwise from the profile and the
scale (`src/lakebench/cli/_run.py`, using
`src/lakebench/modules/pipeline_engines/spark/job.py:get_executor_count`).

**Size fallbacks.** When a job reports no output size, the measured S3 size
of that layer is used; when gold's input size is zero, the measured silver
size is used. Both are in
`src/lakebench/metrics/collector.py:build_pipeline_benchmark`.

**The continuous mode is stored as `sustained`.** The user-facing mode is
`continuous`, but records keep `pipeline_mode: "sustained"` so older
records still read. `src/lakebench/config/schema.py:is_continuous_mode`
accepts both spellings; new code should use it rather than compare with
`"sustained"`.

**One output root.** Journals, run records and reports all derive their
paths from `src/lakebench/_constants.py:DEFAULT_OUTPUT_DIR`, so moving the
output means changing one constant. The layout is in
[Operations](operations.md#where-the-output-goes).
