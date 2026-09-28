"""Common utilities for Spark pipeline scripts.

Bridge module: provides lakebench helper functions (log, env) and
shared transformation logic used by both batch and streaming scripts.

Environment variables set by lakebench job.py:
  BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET, CATALOG_NAME,
  LB_BRONZE_URI, LB_SILVER_URI, LB_ICEBERG_CATALOG, LB_CATALOG_TYPE
"""

import os
import re
from datetime import datetime


class SilverAbort(RuntimeError):
    """A silver-stage run refused to succeed for a defined reason.

    Raised in silver mains (batch or stream) when a hard invariant fails
    that would otherwise let a zero-row or corrupted run exit 0 -- the
    LB-044 class. The message names the invariant and, where useful, the
    remediation. Callers (job.py, the K8s driver wrapper) surface the
    exception verbatim: exit 0 is not a pass.
    """


def log(msg):
    """Timestamped log line."""
    print(f"[lb] {datetime.utcnow().isoformat()} - {msg}", flush=True)


def env(name, default=None):
    """Read required env var."""
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing env var: {name}")
    return v


def assert_progress(rows_written, job_type):
    """Refuse an exit-0 pass when a silver job wrote zero rows (LB-044).

    ``rows_written`` is the primary output-row count the caller tracked
    (silver.transactions rows for AML; the sole silver table for c360;
    the accumulated micro-batch total for streams). The escape hatch
    LB_SILVER_TEST_ALLOW_EMPTY=1 is honoured only when LB_TESTING=1 is
    also set -- both must come from a test harness, never from job.py.
    Any other zero-row silver run raises SilverAbort so the K8s Job
    ends non-zero and the collector records the failure.
    """
    if int(rows_written) >= 1:
        return
    bypass = os.getenv("LB_SILVER_TEST_ALLOW_EMPTY") == "1"
    testing = os.getenv("LB_TESTING") == "1"
    if bypass and testing:
        log(f"{job_type}: zero rows written; bypassed by LB_SILVER_TEST_ALLOW_EMPTY (test-only)")
        return
    raise SilverAbort(f"{job_type}: zero rows written; refusing exit-0 pass (LB-044 gate)")


def emit_stream_scale_admission(measured_envelope_scale=10):
    """Emit the G5 scale cap and admission labels (invariant 6).

    ``silver_stream_scale_cap`` names the measured envelope this profile
    was tuned against (v1.6: scale 10 for all three silver streams, see
    the ``_JOB_PROFILES["silver-stream"]`` docstring in
    ``modules/pipeline_engines/spark/job.py``). ``silver_stream_scale_admission``
    is a decision, not a static string:

    - ``ok`` when the deployment scale (LB_SCALE, threaded from
      job.py._build_env_vars) is <= the measured envelope;
    - ``labelled_beyond_measured_envelope`` when it exceeds it. The stream
      still runs -- refusal is the responsibility of D-safe (AML) or a
      future config gate -- but downstream reports MUST NOT read the
      numbers as infrastructure performance without the label.

    A parse error on LB_SCALE (unset, non-numeric) falls to ``labelled``
    on the safe side: an unknown scale should not silently look like an
    in-envelope run.
    """
    raw_scale = os.getenv("LB_SCALE")
    try:
        scale = float(raw_scale) if raw_scale is not None else None
    except ValueError:
        scale = None
    log(f"silver_stream_scale_cap: measured_up_to_scale_{int(measured_envelope_scale)}")
    if scale is None:
        log("silver_stream_scale_admission: labelled_scale_unknown")
    elif scale <= measured_envelope_scale:
        log("silver_stream_scale_admission: ok")
    else:
        log("silver_stream_scale_admission: labelled_beyond_measured_envelope")


def pipeline_catalog():
    """The Spark catalog every pipeline job reads and writes c360 tables in.

    LB_ICEBERG_CATALOG, which job.py sets to the recipe's named catalog for
    Iceberg and to ``spark_catalog`` for Delta + Hive (DeltaCatalog is the
    session catalog; no named catalog exists). CATALOG_NAME is the Trino
    catalog name: for Delta + Hive it names no Spark catalog, and a table
    ``lakehouse.default.bronze_raw`` resolves as the two-part namespace
    ``lakehouse.default`` inside spark_catalog, which Spark refuses
    (REQUIRES_SINGLE_PART_NAMESPACE).
    """
    return env("LB_ICEBERG_CATALOG", os.getenv("CATALOG_NAME") or "lakehouse")


def pipeline_table(key, default):
    """``pipeline_catalog()`` + the table name in env var *key* (or *default*)."""
    return f"{pipeline_catalog()}.{env(key, default)}"


def one_line(text, limit=200):
    """Collapse whitespace so a value stays on one log line.

    The driver-log parser reads per-rule status one line at a time; Spark
    exception messages are usually multi-line, and a rule whose error text
    spilled onto the next line vanished from rule_errors entirely instead of
    being reported as an error.
    """
    return " ".join(str(text).split())[:limit]


# Iceberg metadata retention, set at creation on every Iceberg table lakebench
# creates. Each commit writes a new metadata.json; expire_snapshots prunes
# snapshots but never deletes old metadata.json files, and Iceberg keeps them
# all unless delete-after-commit is on. A continuous run commits every
# micro-batch on several tables, so without this the metadata objects grow
# linearly for the whole run. With it, each commit deletes metadata files
# beyond the newest ICEBERG_PREVIOUS_VERSIONS_MAX (Iceberg's default is 100);
# 50 keeps about 25 min of history at a 30 s trigger, which only the
# metadata_log_entries diagnostic table reads. Snapshots, time travel,
# streaming offsets and replay dedup live in the current metadata.json and
# are unaffected; data files are never touched by this setting.
# Tables that already exist keep their properties (CREATE TABLE IF NOT EXISTS
# is a no-op), so this applies to fresh deployments and to tables a run
# recreates (createOrReplace, continuous reset).
ICEBERG_PREVIOUS_VERSIONS_MAX = 50
METADATA_DELETE_AFTER_COMMIT = ("write.metadata.delete-after-commit.enabled", "true")
METADATA_PREVIOUS_VERSIONS_MAX = (
    "write.metadata.previous-versions-max",
    str(ICEBERG_PREVIOUS_VERSIONS_MAX),
)
ICEBERG_METADATA_PROPS_SQL = ", ".join(
    f"'{k}' = '{v}'" for k, v in (METADATA_DELETE_AFTER_COMMIT, METADATA_PREVIOUS_VERSIONS_MAX)
)
# The TBLPROPERTIES body shared by the financial DDL (v2, snappy, retention).
ICEBERG_V2_SNAPPY_PROPS_SQL = (
    "'format-version' = '2', 'write.parquet.compression-codec' = 'snappy', "
    + ICEBERG_METADATA_PROPS_SQL
)


def table_exists(spark, table_name):
    """True if a catalog table exists, False only if it definitely does not.

    Use this, not DeltaTable.isDeltaTable, for catalog names: isDeltaTable's
    identifier is a FILE PATH, so "catalog.schema.table" is always False
    (Delta docs). Callers branch to create-with-overwrite on False, so any
    error other than a genuine not-found is re-raised: treating a transient
    catalog failure as "missing" would overwrite a live table.
    """
    try:
        spark.table(table_name).schema  # noqa: B018 -- forces resolution
        return True
    except Exception as e:  # noqa: BLE001
        text = str(e)
        if (
            "TABLE_OR_VIEW_NOT_FOUND" in text
            or "Table or view not found" in text
            or "NoSuchTableException" in type(e).__name__
            or "SCHEMA_NOT_FOUND" in text
        ):
            return False
        raise


def ensure_column(spark, fq_table, name, sql_type):
    """Add a nullable column to an existing table when it is missing.

    Reads the live schema first: Spark has no ``ADD COLUMN IF NOT EXISTS``
    for columns (that clause is for partitions), so the unconditional form
    failed with a parse error on every run and was silently swallowed. For a
    reused catalog whose table predates the column, that left the next write
    to fail on a schema mismatch. Returns True when the column was added.
    """
    if name in spark.table(fq_table).columns:
        return False
    spark.sql(f"ALTER TABLE {fq_table} ADD COLUMNS ({name} {sql_type})")
    log(f"[startup] added {name} to {fq_table} (reused-catalog upgrade)")
    return True


def _partition_transforms(spark, fq_table):
    """The table's partition transforms as DESCRIBE reports them, spaces
    removed (for example ``days(txn_timestamp)``)."""
    out, in_parts = [], False
    for r in spark.sql(f"DESCRIBE TABLE {fq_table}").collect():
        name = (r[0] or "").strip()
        if name == "# Partitioning":
            in_parts = True
            continue
        if in_parts:
            if not name or name.startswith("#"):
                break
            out.append((r[1] or "").replace(" ", ""))
    return out


def ensure_partition_transform(spark, fq_table, old, new):
    """Evolve a reused Iceberg table's partition field from ``old`` to ``new``.

    ``CREATE TABLE IF NOT EXISTS`` is a no-op on a reused catalog, so a table
    created under an older DDL keeps its old spec. Silver and gold AML tables
    moved from ``days()`` to ``months()``: at scale 1 the daily layout left
    silver in 1,339 files of 3 MB, one per day, which compaction could not
    merge. Existing files keep their spec; the next full overwrite or
    delete-and-insert rewrites them under the new one. Returns True when the
    spec was changed; logs and returns False when it cannot be read or changed.
    """
    try:
        parts = _partition_transforms(spark, fq_table)
        if old.replace(" ", "") not in parts:
            return False
        spark.sql(f"ALTER TABLE {fq_table} REPLACE PARTITION FIELD {old} WITH {new}")
    except Exception as e:  # noqa: BLE001
        log(f"[startup] could not evolve {fq_table} from {old} to {new}: {one_line(e)}")
        return False
    log(f"[startup] {fq_table}: partition field {old} -> {new} (reused-catalog upgrade)")
    return True


def _path_size_gb_impl(spark, uri):
    """Total bytes under a Hadoop-FS path or glob, in GiB.

    The file system comes from ``Path.getFileSystem``: ``java.net.URI(uri)``
    rejects glob characters such as ``[0-9]`` (common.c360_bronze_path).
    Raises the underlying Hadoop exception on listing failure; wrappers decide
    whether to swallow it (path_size_gb) or propagate it (path_size_gb_strict).
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hconf)
    if any(ch in uri for ch in "*?[{"):
        total = 0
        for st in fs.globStatus(path) or []:
            total += fs.getContentSummary(st.getPath()).getLength()
        return total / (1024**3)
    if not fs.exists(path):
        return 0.0
    return fs.getContentSummary(path).getLength() / (1024**3)


def path_size_gb(spark, uri):
    """Total bytes under a Hadoop-FS path or glob, in GiB; 0.0 if it cannot be
    measured.

    Callers that must distinguish "path empty" from "listing failed" should use
    ``path_size_gb_strict`` instead.
    """
    try:
        return _path_size_gb_impl(spark, uri)
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] size of {uri} unavailable: {one_line(e)}")
        return 0.0


def path_size_gb_strict(spark, uri):
    """Total bytes under a Hadoop-FS path or glob, in GiB.

    A6 (silver-plan): silver mains call this variant so an S3 outage surfaces
    as a retryable driver error rather than as an "empty bronze" exit-1. The
    non-strict ``path_size_gb`` returns 0.0 on any exception, which made a
    transient listing failure indistinguishable from a truly empty bronze
    path and led to a silent no-op silver run.
    """
    return _path_size_gb_impl(spark, uri)


def _describe_table(spark, fq_table):
    """(location, provider) of a catalog table from DESCRIBE TABLE EXTENDED.

    Only rows after the "# Detailed Table Information" header count: a column
    named Location or Provider comes earlier and would be read as the value.
    """
    location = provider = None
    detail = False
    for row in spark.sql(f"DESCRIBE TABLE EXTENDED {fq_table}").collect():
        name = (row["col_name"] or "").strip()
        if name.startswith("# Detailed Table Information"):
            detail = True
            continue
        if not detail:
            continue
        value = (row["data_type"] or "").strip() or None
        if name == "Location" and location is None:
            location = value
        elif name == "Provider" and provider is None:
            provider = value
    return location, provider


def _norm_uri(uri):
    """One spelling per location: s3a:// and s3:// compare equal, and so do
    file:/x and file:///x (Hadoop reports either)."""
    import re

    m = re.match(r"^([A-Za-z][A-Za-z0-9+.-]*):/*(.*)$", uri.strip())
    if not m:
        return uri.rstrip("/") + "/"
    scheme, rest = m.group(1).lower(), m.group(2).rstrip("/")
    if scheme in ("s3", "s3a", "s3n"):
        return f"s3://{rest}/"
    return f"{scheme}:/{rest}/"


def owned_table_dir(location, owned_uris, keep_uris, table_name=None):
    """True when ``location`` may be deleted as a table's own directory.

    It must sit strictly below one of ``owned_uris`` (the deployment's
    bucket roots; a bucket root itself is never a table directory), must
    neither contain nor lie inside any of ``keep_uris`` (the raw datagen
    landing zone, which the next stream reads), and, when ``table_name`` is
    given, its last path segment must be the table name (or ``name-<suffix>``,
    Iceberg's unique-location form), so a table that resolves to a namespace
    or warehouse root never takes its sibling tables with it.
    """
    loc = _norm_uri(location)
    if not any(loc.startswith(_norm_uri(o)) and loc != _norm_uri(o) for o in owned_uris):
        return False
    for k in keep_uris:
        k = _norm_uri(k)
        if loc.startswith(k) or k.startswith(loc):
            return False
    if table_name:
        last = loc.rstrip("/").rsplit("/", 1)[-1]
        if last != table_name and not last.startswith(table_name + "-"):
            return False
    return True


def _hadoop_fs(spark, uri):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    target = uri.replace("s3://", "s3a://", 1)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(target), hconf)
    return fs, jvm.org.apache.hadoop.fs.Path(target)


def _delete_children(spark, location, keep=()):
    """Delete every child of ``location`` except the names in ``keep``."""
    fs, path = _hadoop_fs(spark, location)
    if not fs.exists(path):
        return 0
    n = 0
    for st in fs.listStatus(path):
        if st.getPath().getName() in keep:
            continue
        fs.delete(st.getPath(), True)
        n += 1
    return n


def reset_stream_tables(spark, tables, *, owned_uris, keep_uris):
    """Drop the tables a continuous run writes, with their data. Returns the
    tables that existed and were dropped.

    Order per table, so an interrupted reset can be re-run to completion:
      1. Delete the table's data (every child of its directory except the
         table log: ``metadata`` for Iceberg, ``_delta_log`` for Delta). The
         catalog still loads the table during DROP, so the log must survive
         until then; if the job dies here the table still exists and the
         next reset finds it again.
      2. DROP TABLE, with PURGE for Iceberg, or a plain DROP when the
         catalog refuses purge (Polaris defaults drop-with-purge to off).
      3. Delete what is left of the directory.
    Directories are touched only when ``owned_table_dir`` allows it. A table
    whose location cannot be read is still dropped and its directory kept.
    """
    dropped = []
    for fq in tables:
        if not table_exists(spark, fq):
            log(f"Continuous reset: {fq} does not exist")
            continue
        try:
            location, provider = _describe_table(spark, fq)
        except Exception as e:  # noqa: BLE001
            log(f"Continuous reset: no location for {fq} ({one_line(e)})")
            location, provider = None, None
        iceberg = (provider or "").lower() == "iceberg"
        name = fq.rsplit(".", 1)[-1]
        owned = bool(location) and owned_table_dir(location, owned_uris, keep_uris, name)
        if location and not owned:
            log(f"Continuous reset: kept {location} (outside this deployment or not its own dir)")
        if owned:
            n = _delete_children(spark, location, keep=("metadata", "_delta_log"))
            log(f"Continuous reset: deleted {n} data entries under {location}")
        how = "DROP"
        if iceberg:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {fq} PURGE")
                how = "DROP PURGE"
            except Exception as e:  # noqa: BLE001
                log(f"Continuous reset: PURGE of {fq} refused ({one_line(e)}); plain DROP")
        if how == "DROP":
            spark.sql(f"DROP TABLE IF EXISTS {fq}")
        dropped.append(fq)
        log(f"Continuous reset: {how} {fq} ({provider or 'unknown provider'})")
        if owned:
            fs, path = _hadoop_fs(spark, location)
            if fs.exists(path):
                fs.delete(path, True)
                log(f"Continuous reset: deleted {location}")
    return dropped


def estimate_distinct_from_sample(sample_rows, distinct, singletons, doubletons, population_rows):
    """Distinct values in the population from a uniform row sample (Chao1).

    Dividing the sample's distinct count by the sampling fraction assumes
    every sampled value is unseen elsewhere, which overstates a key that
    repeats: a 0.1% sample of 24.8M rows over 1M customers holds about 24K
    customers, and 24K / 0.001 reported 14.7M (LB-144). Chao1 adds the
    unseen values implied by how many sampled values appear once versus
    twice. It is a lower-bound estimator, capped here at the population
    row count, and exact when the sample is the population.
    """
    if distinct <= 0 or population_rows <= 0:
        return 0
    if sample_rows >= population_rows:
        return int(distinct)
    if doubletons > 0:
        unseen = singletons * singletons / (2.0 * doubletons)
    else:
        unseen = singletons * (singletons - 1) / 2.0
    return int(min(distinct + unseen, population_rows))


def sample_key_profile(sample_df, key, population_rows):
    """(estimated distinct ``key`` values, skew factor) from a row sample.

    One aggregation over the sample's per-key counts gives both the
    frequency profile Chao1 needs and the max/avg ratio used as the skew
    factor.
    """
    from pyspark.sql.functions import avg, col, count, lit, when
    from pyspark.sql.functions import max as max_
    from pyspark.sql.functions import sum as sum_

    r = (
        sample_df.groupBy(key)
        .count()
        .agg(
            count(lit(1)).alias("distinct"),
            sum_("count").alias("rows"),
            sum_(when(col("count") == 1, 1).otherwise(0)).alias("f1"),
            sum_(when(col("count") == 2, 1).otherwise(0)).alias("f2"),
            max_("count").alias("max_count"),
            avg("count").alias("avg_count"),
        )
        .collect()[0]
    )
    estimate = estimate_distinct_from_sample(
        int(r["rows"] or 0),
        int(r["distinct"] or 0),
        int(r["f1"] or 0),
        int(r["f2"] or 0),
        population_rows,
    )
    skew = (r["max_count"] or 1) / max(r["avg_count"] or 1, 1)
    return estimate, skew


def iceberg_table_stats(spark, fq_table):
    """(row_count, size_gb) of an Iceberg table from its ``data_files`` metadata.

    Reads manifest metadata only, so it costs no data scan. ``data_files``
    rather than ``files``: the latter includes delete files, which would
    overcount a merge-on-read table. Returns (0, 0.0)
    when the metadata table is unavailable (non-Iceberg table, catalog
    error); the collector treats zero input as unmeasured.
    """
    try:
        r = spark.sql(
            f"SELECT COALESCE(SUM(record_count), 0) AS n, "
            f"COALESCE(SUM(file_size_in_bytes), 0) AS b FROM {fq_table}.data_files"
        ).collect()[0]
        return int(r["n"]), float(r["b"]) / (1024**3)
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] table stats for {fq_table} unavailable: {one_line(e)}")
        return 0, 0.0


def log_job_metrics(job, *, input_size_gb, input_rows, output_rows, elapsed_seconds, **extra):
    """Emit the ``=== JOB METRICS ===`` block the metrics collector parses.

    ``extra`` carries per-table row counts (A2) and other numeric metrics
    silver writes alongside the standard four. Keys land in JobMetrics
    via `_apply_metric`'s `silver_*_rows` allowlist; other keys are stored
    on JobMetrics.extra_metrics. Integer values are emitted as integers,
    floats as three-decimal, strings are passed verbatim.
    """
    log(f"=== JOB METRICS: {job} ===")
    log(f"input_size_gb: {input_size_gb:.3f}")
    log(f"input_rows: {int(input_rows)}")
    log(f"output_rows: {int(output_rows)}")
    log(f"elapsed_seconds: {elapsed_seconds:.1f}")
    for key, value in extra.items():
        if isinstance(value, bool):
            log(f"{key}: {'true' if value else 'false'}")
        elif isinstance(value, int):
            log(f"{key}: {value}")
        elif isinstance(value, float):
            log(f"{key}: {value:.3f}")
        else:
            log(f"{key}: {value}")
    log("=" * 60)


def stream_batch_lines(batch_id, rows, seconds, table):
    """Per-micro-batch lines in the format ``parse_streaming_logs`` reads.

    Empty batches produce nothing, matching c360's bronze_ingest: an idle
    trigger is not a processed batch.
    """
    if rows <= 0:
        return []
    return [
        f"Batch {batch_id}: writing {rows:,} rows to {table}",
        f"Batch {batch_id}: committed in {seconds:.1f}s",
    ]


def ttd_line(cycle, stats):
    """The per-cycle time-to-detect line ``parse_streaming_logs`` reads.

    ``stats`` is gold_refresh_financial.ttd_stats: ``alerts`` measured,
    ``late`` (of those, alerts whose related transactions were all in silver
    before the previous detection pass read it), ``unmatched`` (no related
    transaction found in silver), ``max_s``, and a histogram ``bins`` of
    {bin index: count} at ``bin_s`` seconds per bin. The collector merges
    the bins of every cycle into run-wide percentiles.
    """
    bins = ",".join(f"{b}:{n}" for b, n in sorted(stats["bins"].items()))
    mx = stats["max_s"]
    return (
        f"Cycle {cycle}: time to detect alerts={stats['alerts']} late={stats['late']} "
        f"unmatched={stats['unmatched']} max={'-' if mx is None else f'{mx:.1f}'}s "
        f"bin={stats['bin_s']}s bins={bins}"
    )


# A gold.alerts snapshot lookup that failed (as opposed to None: the table
# has no snapshot yet, so every alert is new).
TTD_SNAPSHOT_UNKNOWN = "unknown"


class TtdBaseline:
    """Which gold.alerts snapshot a tick's alerts are compared against.

    Normally the snapshot just before the tick. When a tick's measurement
    does not happen (lookup failure, exception, a tick that raised), its
    newly raised alerts would sit in the next tick's snapshot and never be
    measured, and the ticks lost that way are the slow ones. So the baseline
    of an unmeasured tick is carried to the next tick, whose alerts are then
    measured against it: late by the lost tick, never dropped. A baseline is
    carried at most ``max_carry`` times in a row, since in-stream maintenance
    may expire the snapshot; then the fresh one is used.

    A failed lookup (TTD_SNAPSHOT_UNKNOWN) falls back to the snapshot read
    right after the last measured tick, which is the same point in the
    table's history unless something else wrote to it since.

    Each baseline carries ``late_before_s``: the newest silver ingest time
    the tick before the baseline had read. An alert whose evidence was all
    older than that could have been raised on that tick.
    """

    def __init__(self, max_carry=3):
        self.max_carry = max_carry
        self._carry = None
        self._carries = 0
        self._last_good = None

    def begin(self, prior, late_before_s):
        """Start a tick: ``prior`` is its pre-detection snapshot id (None:
        no snapshot; TTD_SNAPSHOT_UNKNOWN: lookup failed). Returns the
        (snapshot, late_before_s) to measure against."""
        carry = self._carry
        if carry is not None and self._carries >= self.max_carry:
            # The fallback is as old as the carried snapshot: it may be
            # expired too.
            self._last_good = None
        if prior == TTD_SNAPSHOT_UNKNOWN and self._last_good is not None:
            prior = self._last_good
        if (
            carry is not None
            and carry[0] != TTD_SNAPSHOT_UNKNOWN
            and self._carries < self.max_carry
        ):
            base = carry
            self._carries += 1
        else:
            base = (prior, late_before_s)
            self._carries = 0
        self._carry = base
        return base

    def measured(self, after_snapshot=TTD_SNAPSHOT_UNKNOWN):
        """The tick logged its measurement: the next tick starts fresh.
        ``after_snapshot`` is gold.alerts' snapshot once the tick's alerts
        were written, kept as the fallback for a failed lookup."""
        self._carry = None
        self._carries = 0
        self._last_good = None if after_snapshot == TTD_SNAPSHOT_UNKNOWN else after_snapshot


def parse_size_gb(s):
    """Parse size string to GB."""
    return float(s)


# ============================================================
# Shared transformation logic (used by both batch and streaming)
# ============================================================


def apply_silver_transformations(df_bronze):
    """Apply Silver layer transformations - cleaning, standardization, enrichment.

    Pure column-level transforms: no joins, no shuffles. Each row is
    processed independently. Shared between silver_build.py (batch) and
    silver_stream.py (streaming via foreachBatch).
    """
    from pyspark.sql.functions import (
        col,
        concat_ws,
        current_timestamp,
        expr,
        lower,
        regexp_replace,
        to_date,
        trim,
        upper,
        when,
    )

    return (
        df_bronze
        # Light filtering - only remove truly bad data (~2% removed)
        .filter(col("data_quality_flag") != "duplicate_suspected")
        # === STANDARDIZED/CLEANED VERSIONS (keep originals) ===
        .withColumn(
            "email_clean", regexp_replace(lower(trim(col("email_raw"))), "\\.duplicate", "")
        )
        .withColumn(
            "phone_clean",
            regexp_replace(regexp_replace(col("phone_raw"), "[^0-9]", ""), "^1?(\\d{10})$", "+1$1"),
        )
        # Geographic standardization
        .withColumn(
            "state_standardized",
            when(upper(col("state_raw")).isin("CA", "CALIFORNIA"), "CA")
            .when(upper(col("state_raw")).isin("TX", "TEXAS"), "TX")
            .when(upper(col("state_raw")).isin("NY", "NEW YORK"), "NY")
            .when(upper(col("state_raw")) == "FL", "FL")
            .otherwise(upper(col("state_raw"))),
        )
        .withColumn(
            "city_standardized",
            when(upper(col("city_raw")).isin("NEW YORK", "NYC"), "New York")
            .when(upper(col("city_raw")) == "LA", "Los Angeles")
            .otherwise(col("city_raw")),
        )
        # === DERIVED TIME DIMENSIONS ===
        .withColumn("interaction_date", to_date(col("event_timestamp")))
        .withColumn("interaction_hour", expr("hour(event_timestamp)"))
        .withColumn("interaction_day_of_week", expr("dayofweek(event_timestamp)"))
        .withColumn("interaction_week_of_year", expr("weekofyear(event_timestamp)"))
        .withColumn("interaction_month", expr("month(event_timestamp)"))
        .withColumn("interaction_year", expr("year(event_timestamp)"))
        .withColumn("is_weekend", expr("dayofweek(event_timestamp) in (1, 7)"))
        .withColumn("is_business_hours", expr("hour(event_timestamp) between 9 and 17"))
        .withColumn(
            "is_peak_hours",
            expr("""
                hour(event_timestamp) between 12 and 14 or
                hour(event_timestamp) between 18 and 20
            """),
        )
        # === CUSTOMER VALUE SEGMENTATION ===
        .withColumn(
            "customer_value_tier",
            when(col("transaction_amount") > 500, "high_value")
            .when(col("transaction_amount") > 100, "medium_value")
            .when(col("transaction_amount") > 0, "low_value")
            .otherwise("browser_only"),
        )
        .withColumn(
            "transaction_size_category",
            when(col("transaction_amount") > 1000, "large")
            .when(col("transaction_amount") > 250, "medium")
            .when(col("transaction_amount") > 0, "small")
            .otherwise("none"),
        )
        # === BEHAVIORAL ANALYTICS ===
        .withColumn(
            "engagement_score",
            expr("""
                case when page_views = 0 then 0
                     when page_views <= 2 then 1
                     when page_views <= 5 then 2
                     when page_views <= 10 then 3
                     else 4 end
            """),
        )
        .withColumn(
            "session_depth_category",
            when(col("page_views") > 10, "deep")
            .when(col("page_views") > 3, "medium")
            .when(col("page_views") > 0, "shallow")
            .otherwise("bounce"),
        )
        .withColumn(
            "time_spent_category",
            when(col("time_on_site_seconds") > 1800, "long")
            .when(col("time_on_site_seconds") > 300, "medium")
            .when(col("time_on_site_seconds") > 0, "short")
            .otherwise("none"),
        )
        .withColumn(
            "channel_preference",
            when(col("channel") == "mobile_app", "mobile_first")
            .when(col("channel") == "web", "web_first")
            .when(col("channel") == "store", "physical_first")
            .otherwise("omnichannel"),
        )
        # === ADVANCED ANALYTICS (ML Features) ===
        .withColumn(
            "lifetime_value_estimate",
            expr("round(transaction_amount * (1 + points_earned/1000.0), 2)"),
        )
        .withColumn(
            "customer_recency_score",
            expr("30 - datediff(current_date(), to_date(event_timestamp))"),
        )
        .withColumn(
            "engagement_velocity",
            expr("round(page_views / greatest(time_on_site_seconds/60.0, 1.0), 4)"),
        )
        .withColumn(
            "churn_risk_indicator",
            when(col("satisfaction_score") <= 2, "high_risk")
            .when(col("satisfaction_score") <= 3, "medium_risk")
            .when(col("satisfaction_score").isNull(), "unknown_risk")
            .otherwise("low_risk"),
        )
        # === MARKETING ATTRIBUTION ===
        .withColumn(
            "attribution_channel",
            when(col("utm_source").isNotNull(), col("utm_source")).otherwise("direct"),
        )
        .withColumn(
            "attribution_quality",
            when(col("utm_source").isNotNull() & col("utm_medium").isNotNull(), "high")
            .when(col("utm_source").isNotNull(), "medium")
            .otherwise("low"),
        )
        .withColumn(
            "customer_journey_stage",
            when(col("interaction_type") == "browse", "awareness")
            .when(col("interaction_type") == "abandoned_cart", "consideration")
            .when(col("interaction_type") == "purchase", "conversion")
            .when(col("interaction_type") == "support", "retention")
            .otherwise("other"),
        )
        # === DEVICE CONTEXT ===
        .withColumn(
            "device_category",
            when(col("device_type") == "mobile", "mobile")
            .when(col("device_type") == "tablet", "tablet")
            .otherwise("desktop"),
        )
        .withColumn(
            "browser_family",
            when(col("browser").isin("chrome", "edge"), "chromium")
            .when(col("browser") == "safari", "webkit")
            .when(col("browser") == "firefox", "gecko")
            .otherwise("other"),
        )
        # === COMPOSITE FEATURES ===
        .withColumn(
            "interaction_context",
            concat_ws("|", col("device_type"), col("browser"), col("channel")),
        )
        .withColumn(
            "customer_segment_key",
            concat_ws(
                ":", col("customer_value_tier"), col("channel_preference"), col("loyalty_tier")
            ),
        )
        # === DATA LINEAGE ===
        .withColumn("silver_processing_timestamp", current_timestamp())
        .withColumn(
            "data_quality_score",
            when(col("data_quality_flag") == "clean", 1.0)
            .when(col("data_quality_flag") == "format_inconsistent", 0.8)
            .when(col("data_quality_flag") == "incomplete_data", 0.6)
            .otherwise(0.5),
        )
    )


def set_utc_session(spark):
    """Pin the Spark session time zone to UTC.

    Datagen writes ``event_timestamp`` as a UTC-adjusted instant, and every
    derived calendar field (``interaction_date``, hour, weekday, week, month,
    year) is computed in the session time zone. Left at the JVM default, the
    same data lands on different dates on a pod whose TZ is not UTC.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")


def data_clock_date(df, ts_col="event_timestamp"):
    """Latest event date in ``df`` (UTC), or None when it has no timestamps.

    A full scan of one column. The fallback data clock when LB_DATA_CLOCK is
    not set (see ``resolve_data_clock``). Call with the session already
    pinned to UTC (``set_utc_session``).
    """
    from pyspark.sql.functions import col, to_date
    from pyspark.sql.functions import max as max_

    return df.agg(max_(to_date(col(ts_col))).alias("d")).collect()[0]["d"]


def configured_data_clock(value=None):
    """Last day of the configured datagen window, or None when unset.

    LB_DATA_CLOCK is ``datagen.timestamp_end`` (job.py). Datagen treats the
    end as exclusive, so the newest possible event date is the day before.
    """
    from datetime import date, timedelta

    raw = os.getenv("LB_DATA_CLOCK", "") if value is None else value
    raw = raw.strip()
    if not raw:
        return None
    return date.fromisoformat(raw[:10]) - timedelta(days=1)


def resolve_data_clock(df_fallback=None):
    """The c360 data clock: the date recency is measured from.

    One clock for the whole run: from LB_DATA_CLOCK when set, so batch,
    every multi-cycle cycle and every micro-batch use the same anchor and a
    rerun reproduces the same scores (GOALS P4.2). Only when it is unset is
    it measured as max(event date) of ``df_fallback``. Logs which was used.
    """
    anchor = configured_data_clock()
    if anchor is not None:
        log(f"Data clock (recency anchor): {anchor} from LB_DATA_CLOCK")
        return anchor
    if df_fallback is None:
        log("Data clock: LB_DATA_CLOCK unset and no data to measure; recency is NULL")
        return None
    anchor = data_clock_date(df_fallback)
    log(f"Data clock (recency anchor): {anchor} from max(event_timestamp); LB_DATA_CLOCK unset")
    return anchor


def c360_bronze_path(bronze_uri, appending=False):
    """The bronze files a c360 silver-build reads.

    Single-cycle runs (``LB_BRONZE_CYCLE`` unset) read every file under
    ``customer/interactions/``. In a multi-cycle run the CLI passes the
    cycle index as ``LB_BRONZE_CYCLE`` and datagen_rs names the files
    ``part-{fid:06}.parquet`` for cycle 0 and ``part-c{cycle:03}-{fid:06}``
    for cycle n (``datagen_rs::cycle::c360_key``):

    - cycle 0 reads only cycle-0 names, so ``part-c*`` files left by an
      earlier multi-cycle run in the same bucket are not rebuilt into it;
    - cycles 2+ appending to silver read only their own files. Reading the
      whole prefix and appending re-added every earlier cycle's rows, so
      every gold count and revenue KPI was inflated.

    A later cycle that finds no silver table to append to rebuilds from the
    whole prefix.
    """
    base = bronze_uri + "customer/interactions/"
    cycle = os.environ.get("LB_BRONZE_CYCLE", "").strip()
    if not cycle:
        return base
    n = int(cycle)
    if n == 0:
        return base + "part-[0-9]*.parquet"
    if appending:
        return base + f"part-c{n:03d}-*.parquet"
    return base


def c360_bronze_run_path(bronze_uri):
    """Every bronze file this run's cycles wrote, for bronze-verify.

    The whole prefix for a single-cycle run. In a multi-cycle run at cycle n,
    the cycle-0 names plus ``part-c001`` .. ``part-c{n:03}``: the files silver
    holds after cycle n (``c360_bronze_path``), so bronze-verify counts what
    silver must hold and not files an earlier run left in the bucket.
    """
    base = bronze_uri + "customer/interactions/"
    cycle = os.environ.get("LB_BRONZE_CYCLE", "").strip()
    if not cycle:
        return base
    names = ["part-[0-9]*.parquet"] + [f"part-c{i:03d}-*.parquet" for i in range(1, int(cycle) + 1)]
    if len(names) == 1:
        return base + names[0]
    return base + "{" + ",".join(names) + "}"


def apply_silver_transformations_anchored(df_bronze, anchor_date):
    """``apply_silver_transformations`` with recency anchored to the data clock.

    ``customer_recency_score`` is ``30 - days between the event date and
    anchor_date``: 30 for an event on the newest day of the data, 0 for one
    30 days older, negative beyond. The shared transform measures from
    ``current_date()``, which made the score a function of the run date. ``anchor_date`` is a ``datetime.date``; when it is
    None (no timestamps at all) the score is NULL rather than run-dated.
    """
    from pyspark.sql.functions import col, datediff, lit

    out = apply_silver_transformations(df_bronze)
    if anchor_date is None:
        score = lit(None).cast("int")
    else:
        score = lit(30) - datediff(lit(anchor_date), col("interaction_date"))
    return out.withColumn("customer_recency_score", score)


def streaming_query_id(spark):
    """Id of the streaming query running the current ``foreachBatch`` call.

    Stable across driver restarts from the same checkpoint and new for a
    fresh checkpoint, so it scopes a batch id to one logical stream. Spark
    sets it as a local property on the micro-batch thread. Raises when it is
    absent: an idempotency key without it would be unsound.
    """
    qid = spark.sparkContext.getLocalProperty("sql.streaming.queryId")
    if not qid:
        raise RuntimeError("sql.streaming.queryId is not set; call from inside foreachBatch")
    return qid


def delta_idempotent_options(spark, app, batch_id):
    """Delta writer options that make a ``foreachBatch`` write exactly-once.

    Delta records (txnAppId, txnVersion) in the commit and skips any later
    write whose txnVersion is not greater than the recorded one, so a
    micro-batch replayed after a driver restart commits nothing the second
    time. The app id includes the streaming query id: with a fixed app id a
    fresh checkpoint (batch ids restart at 0) would have every write skipped,
    a silent zero-row run.
    """
    return {
        "txnAppId": f"{app}-{streaming_query_id(spark)}",
        "txnVersion": str(int(batch_id)),
    }


def stream_run_id(spark):
    """Run id of the streaming query in the current ``foreachBatch`` call.

    A new run id every time a query starts, including a restart from the
    same checkpoint. Spark uses it as the job group of the micro-batch
    thread. None when it cannot be read.
    """
    return spark.sparkContext.getLocalProperty("spark.jobGroup.id") or None


_RUNS_STARTED = set()


def replay_possible(spark):
    """True for the first micro-batch of each query run, False after.

    A batch that fails stops its query and the driver exits (``await_stream``),
    so only the first batch a run executes can repeat a batch an earlier run
    committed. Writers do their replay check (a DELETE, a snapshot or version
    lookup) only then instead of on every batch. When the run id is not
    readable every batch is treated as a possible replay: slower, never wrong.
    """
    run = stream_run_id(spark)
    if run is None:
        return True
    if run in _RUNS_STARTED:
        return False
    _RUNS_STARTED.add(run)
    return True


def delta_table_version(spark, fq_table):
    """Latest commit version of a Delta table."""
    return int(spark.sql(f"DESCRIBE HISTORY {fq_table} LIMIT 1").collect()[0]["version"])


def checkpoint_is_fresh(spark, checkpoint_location):
    """True when a streaming checkpoint has no committed offsets yet."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    offsets = jvm.org.apache.hadoop.fs.Path(checkpoint_location.rstrip("/") + "/offsets")
    fs = offsets.getFileSystem(hconf)
    return not fs.exists(offsets) or len(fs.listStatus(offsets)) == 0


def refuse_fresh_checkpoint_over_data(spark, checkpoint_location, fq_table):
    """Exit when a fresh checkpoint would re-read the source into a full table.

    A new checkpoint starts the source from the beginning, so every row
    already in ``fq_table`` would be written a second time. That happens when
    checkpoints are deleted but tables are not. Clear both, or neither.
    """
    if not checkpoint_is_fresh(spark, checkpoint_location):
        return
    if not table_exists(spark, fq_table) or spark.table(fq_table).limit(1).count() == 0:
        return
    log(
        f"ERROR: checkpoint {checkpoint_location} is empty but {fq_table} already has "
        "rows; starting would re-read the whole source and duplicate them. Drop the "
        "table or restore the checkpoint."
    )
    raise SystemExit(1)


def await_stream(spark, query):
    """Block until ``query`` stops; re-raise its failure.

    SIGTERM and SIGINT only set a flag; the loop below stops the query, so
    the handler never calls into py4j while the main thread may be inside
    it. A query that died with an exception fails the driver, so the
    Kubernetes job reports the real outcome instead of a pass with no data
    (LB-044).
    """
    import signal
    import time

    stop = {"signal": None}

    def _shutdown_handler(signum, frame):  # noqa: ARG001
        stop["signal"] = signum

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _shutdown_handler)
        except ValueError:
            pass

    while query.isActive:
        if stop["signal"] is not None:
            log(f"Signal {stop['signal']} received; stopping stream cleanly")
            try:
                query.stop()
            except Exception as e:  # noqa: BLE001
                log(f"query.stop failed: {e}")
            break
        time.sleep(1)

    exc = query.exception()
    if exc is not None:
        log(f"Streaming query failed: {one_line(exc, 2000)}")
        spark.stop()
        raise exc
    log("Streaming query stopped")


def get_daily_kpi_aggregations():
    """Return the list of aggregation expressions for daily KPIs.

    Shared by every c360 gold writer: gold_finalize.py and
    gold_finalize_delta.py (batch), gold_refresh.py and gold_refresh_delta.py
    (continuous), so the Iceberg and Delta adapters publish identical KPIs.
    Produces 30 KPI columns (plus the ``interaction_date`` group key).

    Averages are taken over the rows the KPI is about, not over every
    interaction. The generator (datagen_rs customer360.rs) writes
    ``transaction_amount = 0.0`` on every non-purchase row and
    ``page_views = time_on_site_seconds = 0`` on every row that is not a
    purchase or browse, so an average over all rows mixes in structural
    zeros: avg_transaction_value was about 5.5x low (the 18% purchase share),
    avg_page_views and avg_time_on_site_seconds about 1.9x low (the 53%
    visit share). A transaction is a row with ``transaction_amount > 0``
    (the same test as ``total_transactions``); a site visit is a row with
    ``page_views > 0``. A day with no transactions has a NULL average, not 0.
    """
    from pyspark.sql.functions import (
        avg,
        col,
        count,
        countDistinct,
        when,
    )
    from pyspark.sql.functions import (
        max as max_,
    )
    from pyspark.sql.functions import (
        round as round_,
    )
    from pyspark.sql.functions import (
        sum as sum_,
    )

    return [
        # Customer metrics
        countDistinct("customer_id").alias("daily_active_customers"),
        countDistinct("email_clean").alias("unique_emails"),
        countDistinct("session_id").alias("total_sessions"),
        # Revenue metrics
        round_(sum_("transaction_amount"), 2).alias("total_daily_revenue"),
        # Mean value of a transaction (purchase rows), not of an interaction.
        round_(avg(when(col("transaction_amount") > 0, col("transaction_amount"))), 2).alias(
            "avg_transaction_value"
        ),
        round_(max_("transaction_amount"), 2).alias("largest_transaction"),
        sum_(when(col("transaction_amount") > 0, 1).otherwise(0)).alias("total_transactions"),
        # Channel revenue breakdown
        round_(
            sum_(when(col("channel") == "web", col("transaction_amount")).otherwise(0)), 2
        ).alias("web_revenue"),
        round_(
            sum_(when(col("channel") == "mobile_app", col("transaction_amount")).otherwise(0)), 2
        ).alias("mobile_revenue"),
        round_(
            sum_(when(col("channel") == "store", col("transaction_amount")).otherwise(0)), 2
        ).alias("store_revenue"),
        round_(
            sum_(when(col("channel") == "call_center", col("transaction_amount")).otherwise(0)), 2
        ).alias("call_center_revenue"),
        # Engagement metrics
        round_(avg("engagement_score"), 2).alias("avg_engagement_score"),
        # Per site visit (page_views > 0); support calls and logins have none.
        round_(avg(when(col("page_views") > 0, col("time_on_site_seconds"))), 0).alias(
            "avg_time_on_site_seconds"
        ),
        round_(avg(when(col("page_views") > 0, col("page_views"))), 1).alias("avg_page_views"),
        # Conversion funnel
        sum_(when(col("customer_journey_stage") == "awareness", 1).otherwise(0)).alias(
            "awareness_interactions"
        ),
        sum_(when(col("customer_journey_stage") == "consideration", 1).otherwise(0)).alias(
            "consideration_interactions"
        ),
        sum_(when(col("customer_journey_stage") == "conversion", 1).otherwise(0)).alias(
            "conversions"
        ),
        sum_(when(col("customer_journey_stage") == "retention", 1).otherwise(0)).alias(
            "retention_interactions"
        ),
        # Loyalty metrics
        sum_(when(col("loyalty_member") == True, 1).otherwise(0)).alias(  # noqa: E712
            "loyalty_member_interactions"
        ),
        sum_("points_earned").alias("total_points_earned"),
        sum_("points_redeemed").alias("total_points_redeemed"),
        # Customer satisfaction
        # One ticket per support interaction. The generator draws ticket ids
        # at random from 90,000 values (TKT10000-TKT99999), so a distinct
        # count merged unrelated tickets that collided: about 5% low at
        # 10,000 tickets a day and capped at 90,000 however many were opened.
        count("support_ticket_id").alias("support_tickets_created"),
        round_(avg("satisfaction_score"), 2).alias("avg_satisfaction_score"),
        # Risk indicators
        sum_(when(col("churn_risk_indicator") == "high_risk", 1).otherwise(0)).alias(
            "high_churn_risk_count"
        ),
        sum_(when(col("churn_risk_indicator") == "medium_risk", 1).otherwise(0)).alias(
            "medium_churn_risk_count"
        ),
        # Value metrics
        round_(sum_("lifetime_value_estimate"), 2).alias("total_estimated_ltv"),
        # Per transaction, like avg_transaction_value: the estimate is 0 on
        # every row without a transaction amount.
        round_(avg(when(col("transaction_amount") > 0, col("lifetime_value_estimate"))), 2).alias(
            "avg_estimated_ltv"
        ),
        # Channel distribution
        sum_(when(col("channel") == "web", 1).otherwise(0)).alias("web_interactions"),
        sum_(when(col("channel") == "mobile_app", 1).otherwise(0)).alias("mobile_interactions"),
        sum_(when(col("channel") == "store", 1).otherwise(0)).alias("store_interactions"),
    ]


# ---------------------------------------------------------------------------
# c360 correctness facts (reported after gold-finalize, D6: reporting only)
# ---------------------------------------------------------------------------

C360_CHECK_TAG = "[c360-check]"

# Gold columns that count rows or sum non-negative amounts: never negative,
# never NULL on a day that exists.
C360_GOLD_NONNEG_COLUMNS = (
    "daily_active_customers",
    "unique_emails",
    "total_sessions",
    "total_daily_revenue",
    "largest_transaction",
    "total_transactions",
    "web_revenue",
    "mobile_revenue",
    "store_revenue",
    "call_center_revenue",
    "awareness_interactions",
    "consideration_interactions",
    "conversions",
    "retention_interactions",
    "loyalty_member_interactions",
    "total_points_earned",
    "total_points_redeemed",
    "support_tickets_created",
    "high_churn_risk_count",
    "medium_churn_risk_count",
    "total_estimated_ltv",
    "web_interactions",
    "mobile_interactions",
    "store_interactions",
)

# Gold columns summed over days for the silver-to-gold reconciliation.
C360_GOLD_SUM_COLUMNS = (
    "total_daily_revenue",
    "total_transactions",
    "awareness_interactions",
    "consideration_interactions",
    "conversions",
    "retention_interactions",
    "support_tickets_created",
    "high_churn_risk_count",
    "medium_churn_risk_count",
    "total_points_earned",
)


def _num(v):
    """A JSON-safe number: None stays None, Decimal/int/float become float."""
    return None if v is None else float(v)


def c360_silver_facts(silver_df):
    """One aggregation pass over silver: the counts the checks reconcile to.

    Reads only the columns named here (columnar), no join, one distinct
    count (customers). Every value is exact.
    """
    from pyspark.sql.functions import col, count, countDistinct, lit, when
    from pyspark.sql.functions import max as max_
    from pyspark.sql.functions import min as min_
    from pyspark.sql.functions import sum as sum_

    it = col("interaction_type")
    amt = col("transaction_amount")
    pv = col("page_views")

    def n(cond):
        return sum_(when(cond, 1).otherwise(0))

    row = silver_df.agg(
        count(lit(1)).alias("rows"),
        n(col("data_quality_flag") == "duplicate_suspected").alias("duplicate_flag_rows"),
        n(it == "purchase").alias("purchase_rows"),
        n(it == "browse").alias("browse_rows"),
        n(it == "support").alias("support_rows"),
        n(it == "login").alias("login_rows"),
        n(it == "abandoned_cart").alias("abandoned_cart_rows"),
        n(amt > 0).alias("transaction_rows"),
        n((amt > 0) & ((it != "purchase") | it.isNull())).alias("non_purchase_amount_rows"),
        n(amt.isNull()).alias("null_amount_rows"),
        sum_(amt).alias("revenue"),
        min_(when(it == "purchase", amt)).alias("purchase_amount_min"),
        max_(when(it == "purchase", amt)).alias("purchase_amount_max"),
        n(pv > 0).alias("visit_rows"),
        sum_(when(pv > 0, pv)).alias("visit_page_views"),
        sum_(when(pv > 0, col("time_on_site_seconds"))).alias("visit_time_on_site"),
        count("satisfaction_score").alias("satisfaction_rows"),
        sum_("satisfaction_score").alias("satisfaction_sum"),
        count("support_ticket_id").alias("ticket_rows"),
        n(col("churn_risk_indicator") == "high_risk").alias("high_churn_rows"),
        n(col("churn_risk_indicator") == "medium_risk").alias("medium_churn_rows"),
        sum_("points_earned").alias("points_earned"),
        n(col("customer_id").isNull()).alias("null_customer_rows"),
        min_("customer_id").alias("customer_id_min"),
        max_("customer_id").alias("customer_id_max"),
        countDistinct("customer_id").alias("distinct_customers"),
        n(col("interaction_date").isNull()).alias("null_date_rows"),
        min_("interaction_date").alias("date_min"),
        max_("interaction_date").alias("date_max"),
        countDistinct("interaction_date").alias("distinct_dates"),
    ).collect()[0]
    out = {}
    for k, v in row.asDict().items():
        if k in ("date_min", "date_max"):
            out[k] = v.isoformat() if v is not None else None
        elif k in ("revenue", "purchase_amount_min", "purchase_amount_max"):
            out[k] = _num(v)
        else:
            out[k] = None if v is None else int(v)
    return out


def c360_gold_facts(gold_df):
    """Facts over the gold table as written: totals, and per-day identities.

    Gold is one row per day (a few hundred), so it is collected and checked
    on the driver. ``days`` carries the per-day values the statistical
    checks need; the identities are counted here as violations.
    """
    rows = [r.asDict() for r in gold_df.collect()]
    dates = [r.get("interaction_date") for r in rows]
    present = [d for d in dates if d is not None]
    sums = dict.fromkeys(C360_GOLD_SUM_COLUMNS, 0.0)
    neg = {}
    nulls = {}
    viol = {
        "avg_transaction_value_mismatch": 0,
        "avg_transaction_value_null_mismatch": 0,
        "avg_estimated_ltv_mismatch": 0,
        "transactions_ne_conversions": 0,
        "tickets_ne_support": 0,
        "channel_revenue_exceeds_total": 0,
        "churn_exceeds_support": 0,
        "largest_transaction_out_of_range": 0,
        "ltv_below_revenue": 0,
    }
    examples = {}

    def flag(name, day):
        viol[name] += 1
        examples.setdefault(name, str(day))

    days = []
    max_dau = 0
    for r in rows:
        d = r.get("interaction_date")
        for c in C360_GOLD_NONNEG_COLUMNS:
            v = r.get(c)
            if v is None:
                nulls[c] = nulls.get(c, 0) + 1
            elif float(v) < 0:
                neg[c] = neg.get(c, 0) + 1
        for c in C360_GOLD_SUM_COLUMNS:
            sums[c] += float(r.get(c) or 0)
        tx = int(r.get("total_transactions") or 0)
        rev = float(r.get("total_daily_revenue") or 0)
        atv = r.get("avg_transaction_value")
        if (tx == 0) != (atv is None):
            flag("avg_transaction_value_null_mismatch", d)
        # Both sides are rounded to cents: 0.005 each, plus float slack.
        if tx > 0 and atv is not None and abs(float(atv) - rev / tx) > 0.011:
            flag("avg_transaction_value_mismatch", d)
        # Non-transaction rows carry an LTV estimate of 0, so the per-
        # transaction average is total / transactions.
        altv = r.get("avg_estimated_ltv")
        if (tx == 0) != (altv is None) or (
            tx > 0
            and altv is not None
            and abs(float(altv) - float(r.get("total_estimated_ltv") or 0) / tx) > 0.011
        ):
            flag("avg_estimated_ltv_mismatch", d)
        if tx != int(r.get("conversions") or 0):
            flag("transactions_ne_conversions", d)
        support = int(r.get("retention_interactions") or 0)
        if int(r.get("support_tickets_created") or 0) != support:
            flag("tickets_ne_support", d)
        channels = sum(
            float(r.get(c) or 0)
            for c in ("web_revenue", "mobile_revenue", "store_revenue", "call_center_revenue")
        )
        if channels > rev + 0.05:
            flag("channel_revenue_exceeds_total", d)
        churn = int(r.get("high_churn_risk_count") or 0) + int(
            r.get("medium_churn_risk_count") or 0
        )
        if churn > support:
            flag("churn_exceeds_support", d)
        largest = r.get("largest_transaction")
        if tx > 0 and atv is not None and largest is not None:
            if float(largest) > 9999.99 + 1e-6 or float(largest) < float(atv) - 0.011:
                flag("largest_transaction_out_of_range", d)
        ltv = float(r.get("total_estimated_ltv") or 0)
        if ltv < rev - 0.02:
            flag("ltv_below_revenue", d)
        max_dau = max(max_dau, int(r.get("daily_active_customers") or 0))
        days.append(
            [
                d.isoformat() if d is not None else None,
                tx,
                _num(atv),
                int(r.get("awareness_interactions") or 0) + int(r.get("conversions") or 0),
                _num(r.get("avg_page_views")),
                _num(r.get("avg_time_on_site_seconds")),
                support,
                _num(r.get("avg_satisfaction_score")),
            ]
        )
    days.sort(key=lambda x: x[0] or "")
    return {
        "rows": len(rows),
        "null_dates": len(dates) - len(present),
        "distinct_dates": len(set(present)),
        "date_min": min(present).isoformat() if present else None,
        "date_max": max(present).isoformat() if present else None,
        "sums": {k: round(v, 2) for k, v in sums.items()},
        "negative_values": neg,
        "null_values": nulls,
        "violations": viol,
        "violation_examples": examples,
        "max_daily_active_customers": max_dau,
        # [date, transactions, avg_transaction_value, visits, avg_page_views,
        #  avg_time_on_site_seconds, support_interactions, avg_satisfaction]
        "days_columns": [
            "date",
            "transactions",
            "avg_transaction_value",
            "visits",
            "avg_page_views",
            "avg_time_on_site_seconds",
            "support",
            "avg_satisfaction_score",
        ],
        "days": days,
    }


def c360_check_facts(silver_df, gold_df):
    """Silver and gold facts for the c360 expected-result checks.

    The checks themselves (what a correct corpus must produce) live in
    ``lakebench.metrics.c360_correctness`` on the CLI side, which reads the
    ``[c360-check]`` line this produces.
    """
    return {"version": 1, "silver": c360_silver_facts(silver_df), "gold": c360_gold_facts(gold_df)}


def log_c360_check(spark, silver_tbl, gold_tbl):
    """Log the ``[c360-check] {json}`` line after gold is written.

    Reporting only (owner decision D6): an error here is logged in the line
    and never fails the stage. It runs inside the gold-finalize pod, so the
    pod's wall clock includes it; the line records ``check_seconds`` and the
    CLI takes that off the stage's elapsed time and end time
    (``cli/_run.py``), so time to value measures the pipeline, not
    lakebench's own check. ``LB_C360_CHECK=false`` skips it.
    """
    import json
    import time

    if os.environ.get("LB_C360_CHECK", "true").lower() == "false":
        log(C360_CHECK_TAG + " " + json.dumps({"version": 1, "skipped": "LB_C360_CHECK=false"}))
        return
    t0 = time.time()
    try:
        facts = c360_check_facts(spark.table(silver_tbl), spark.table(gold_tbl))
    except Exception as e:  # noqa: BLE001 -- reporting only, never fail the stage
        facts = {"version": 1, "error": one_line(e, 500)}
    facts["check_seconds"] = round(time.time() - t0, 1)
    # One physical line: the parser anchors on the tag and reads to the end.
    log(C360_CHECK_TAG + " " + json.dumps(facts, separators=(",", ":"), default=str))


# ---------------------------------------------------------------------------
# Delta table write helper -- handles managed vs EXTERNAL tables
# ---------------------------------------------------------------------------


def clear_unregistered_table_dirs(spark, targets, *, owned_uris, keep_uris):
    """Delete the directory of each (table, location) whose table is not in
    the catalog. Returns the locations deleted.

    For tables created at an explicit path (the Delta continuous bronze
    table): DROP leaves their files, so a reset interrupted between its DROP
    and its directory delete, or a first commit whose catalog registration
    failed, leaves a _delta_log that the next create refuses to adopt
    (refuse_orphan_delta_log). reset_stream_tables skips a table that is not
    registered, so without this the deployment stayed wedged. The same
    ``owned_table_dir`` guard applies.
    """
    cleared = []
    for fq, location in targets:
        if table_exists(spark, fq):
            continue
        name = fq.rsplit(".", 1)[-1]
        if not owned_table_dir(location, owned_uris, keep_uris, name):
            log(f"Continuous reset: kept {location} (outside this deployment or not its own dir)")
            continue
        fs, path = _hadoop_fs(spark, location)
        if fs.exists(path):
            fs.delete(path, True)
            cleared.append(location)
            log(f"Continuous reset: deleted {location} ({fq} is not in the catalog)")
    return cleared


def _is_unity_catalog():
    """Check if the current catalog is Unity (requires EXTERNAL table writes)."""
    return os.getenv("LB_CATALOG_TYPE", "hive") == "unity"


def ensure_namespaces(spark, catalog, tables):
    """CREATE NAMESPACE IF NOT EXISTS for every namespace in ``tables``.

    Polaris creates the medallion namespaces at bootstrap; the Hive catalog
    does not, so a first CREATE TABLE there fails with NoSuchNamespace. No
    LOCATION: Iceberg's HiveCatalog then derives one from the catalog's S3
    warehouse, as silver_build.py and gold_finalize.py already rely on.
    """
    for ns in sorted({t.split(".", 1)[0] for t in tables if "." in t}):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}")


_DDL_TABLE = re.compile(r"CREATE TABLE IF NOT EXISTS\s+[\w`]+\.([\w`]+)\.([\w`]+)", re.IGNORECASE)


def ensure_namespaces_for_ddl(spark, catalog, ddls):
    """ensure_namespaces for every table the given CREATE TABLE DDLs create.

    Reads the names out of the DDL itself, so a table overridden into another
    namespace is covered without keeping a second list in step.
    """
    tables = []
    for ddl in ddls:
        m = _DDL_TABLE.search(ddl)
        if m:
            tables.append(f"{m.group(1)}.{m.group(2)}".replace("`", ""))
    ensure_namespaces(spark, catalog, tables)


def _s3_table_path(bucket_uri, fq_table):
    """Build the S3 path for an EXTERNAL Delta table.

    Args:
        bucket_uri: S3A URI for the layer bucket (e.g. "s3a://lb-silver/")
        fq_table: Fully-qualified table name WITHOUT catalog prefix
                  (e.g. "silver.customer_interactions_enriched")

    Returns:
        S3 path like "s3a://lb-silver/warehouse/silver.db/customer_interactions_enriched"
    """
    parts = fq_table.split(".", 1)
    if len(parts) == 2:
        schema, table = parts
    else:
        schema, table = "default", fq_table
    return f"{bucket_uri.rstrip('/')}/warehouse/{schema}.db/{table}"


_REGISTERED_DELTA_TABLES: set[str] = set()


def refuse_orphan_delta_log(spark, fq_table, location=None):
    """Refuse to create a Delta table over an unregistered _delta_log.

    Destroy unregisters tables whose files sit in a bucket it does not own
    and leaves the files (LB-186). A later run with the same names would find
    the table missing from the catalog and create it at the same location,
    appending to or adopting the old log. Stop instead. *location* is the
    table's explicit path when it has one; otherwise the managed location
    under the namespace is checked.
    """
    if fq_table in _REGISTERED_DELTA_TABLES:
        return  # a micro-batch loop: checked once per table and process
    if table_exists(spark, fq_table):
        _REGISTERED_DELTA_TABLES.add(fq_table)
        return
    if location:
        _refuse_existing_delta_log(spark, fq_table, f"{location.rstrip('/')}/_delta_log")
        return
    parts = fq_table.split(".")
    name = parts[-1]
    ns_ref = ".".join(parts[:-1]) or "default"
    try:
        rows = spark.sql(f"DESCRIBE NAMESPACE EXTENDED {ns_ref}").collect()
    except Exception as e:  # noqa: BLE001
        text = str(e)
        if "SCHEMA_NOT_FOUND" in text or "NoSuchNamespace" in text or "not found" in text:
            return  # no namespace yet, so no location either
        raise
    location = None
    for row in rows:
        d = row.asDict() if hasattr(row, "asDict") else dict(row)
        key = str(d.get("info_name") or d.get("database_description_item") or "")
        if key.strip().lower() == "location":
            location = str(d.get("info_value") or d.get("database_description_value") or "")
            break
    if not location:
        return
    _refuse_existing_delta_log(spark, fq_table, f"{location.rstrip('/')}/{name.lower()}/_delta_log")


def _refuse_existing_delta_log(spark, fq_table, log_dir):
    fs, path = _hadoop_fs(spark, log_dir)
    if fs.exists(path):
        raise RuntimeError(
            f"{fq_table} is not in the catalog but {log_dir} already holds a Delta log. "
            "It is left by a destroy that kept the files (--keep-buckets, or a bucket "
            "it could not prove it owned), by a continuous reset interrupted between "
            "its DROP and its directory delete, or by a write that committed but never "
            "registered. Refusing to append to or adopt it: delete that table "
            "directory if its data may go, or point this deployment at other buckets."
        )


_CLUSTER_VIEW_SEQ = [0]
_PLAIN_COLUMN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def cluster_by_partition(spark, df, partition_cols, mode="hash"):
    """Group rows by their partition values before a partitioned file write.

    A partitioned write without this writes one file per partition value per
    task. c360 silver has one partition per day (366 at the default range),
    so every input task wrote a file into almost every day: tens of
    thousands of sub-megabyte files at scale 1, which a Spark Thrift server
    then plans and opens one at a time. Iceberg silver avoids this with
    write.distribution-mode=hash; this is the Delta equivalent.

    REBALANCE (Spark 3.2+) hash-partitions by the columns and lets AQE split
    a partition larger than the advisory size and merge small ones, so a
    large day still spreads over several tasks at high scale instead of
    landing on one. ``mode`` uses the Iceberg distribution-mode vocabulary:
    "none" returns ``df`` unchanged (spark.lb.silver.distribution_mode=none,
    the same escape hatch as for Iceberg); anything else clusters.
    """
    if not partition_cols or str(mode).strip().lower() == "none":
        return df
    for c in partition_cols:
        if not _PLAIN_COLUMN.fullmatch(str(c)):
            raise ValueError(f"cluster_by_partition: not a plain column name: {c!r}")
    _CLUSTER_VIEW_SEQ[0] += 1
    view = f"lb_cluster_by_partition_{_CLUSTER_VIEW_SEQ[0]}"
    df.createOrReplaceTempView(view)
    cols = ", ".join(partition_cols)
    return spark.sql(f"SELECT /*+ REBALANCE({cols}) */ * FROM {view}")


def files_added_by_last_commit(spark, fq_table):
    """Data files the table's latest Delta commit wrote, or None if unknown.

    Commit metadata only (DESCRIBE HISTORY operationMetrics.numFiles).
    """
    try:
        r = spark.sql(f"DESCRIBE HISTORY {fq_table} LIMIT 1").collect()
        n = (r[0]["operationMetrics"] or {}).get("numFiles") if r else None
        return int(n) if n is not None else None
    except Exception as e:  # noqa: BLE001 -- feeds a log line only; never fails the write
        log(f"Warning: commit file count unavailable ({one_line(e)})")
        return None


def write_delta_table(
    spark, df, fq_table, bucket_uri, mode="append", partition_cols=None, options=None, location=None
):
    """Write a DataFrame as a Delta table, handling managed vs EXTERNAL.

    When LB_CATALOG_TYPE is "unity", writes data directly to S3 via
    df.write.save(path) and registers the table with CREATE TABLE ...
    LOCATION. This bypasses Unity's STS credential vending which fails
    on non-AWS S3 (FlashBlade, MinIO).

    When LB_CATALOG_TYPE is "hive" (default), uses saveAsTable() which
    registers through the session catalog (DeltaCatalog over Hive).

    Args:
        spark: SparkSession
        df: DataFrame to write
        fq_table: Catalog-qualified table name (e.g. "lakehouse.silver.table")
        bucket_uri: S3A URI for the layer (e.g. "s3a://lb-silver/")
        mode: Write mode -- "append" or "overwrite"
        partition_cols: List of partition column names, or None
        options: Dict of writer options (e.g. Delta table properties)
        location: Explicit table path for the Hive catalog (the table is then
            EXTERNAL); None keeps the namespace's managed location. Unity
            always writes to the _s3_table_path location.
    """
    options = options or {}
    writer = df.write.format("delta").mode(mode)
    for k, v in options.items():
        writer = writer.option(k, v)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    if _is_unity_catalog():
        # EXTERNAL table path -- bypass credential vending
        # Strip catalog prefix to get schema.table for path construction
        parts = fq_table.split(".", 1)
        schema_table = parts[1] if len(parts) > 1 else fq_table
        table_path = _s3_table_path(bucket_uri, schema_table)

        log(f"Writing EXTERNAL Delta table to {table_path} (mode={mode})")
        writer.save(table_path)

        # Register table in Unity catalog (idempotent)
        partition_clause = ""
        if partition_cols:
            partition_clause = f" PARTITIONED BY ({', '.join(partition_cols)})"
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {fq_table} "
            f"USING DELTA{partition_clause} "
            f"LOCATION '{table_path}'"
        )
    else:
        # Managed table path -- saveAsTable registers via DeltaCatalog/Hive
        refuse_orphan_delta_log(spark, fq_table, location)
        if location:
            writer = writer.option("path", location)
            log(f"Writing Delta table {fq_table} at {location} (mode={mode})")
        else:
            log(f"Writing managed Delta table {fq_table} (mode={mode})")
        writer.saveAsTable(fq_table)
