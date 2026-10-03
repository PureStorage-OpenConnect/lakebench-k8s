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


def assert_preflight_rows(df, name, minimum=1):
    """A1-atomic + F2: assert a silver DataFrame has at least ``minimum`` rows
    BEFORE any silver table has been written for the cycle.

    ``df`` is a lazily-built Spark DataFrame; ``name`` is the target silver
    table (used verbatim in the abort message). ``minimum`` defaults to 1 so
    the assertion enforces the F2 ">= 1 rows per silver frame" contract; a
    caller with a known lower bound (e.g. entities >= unique parties count
    from bronze) may pass a tighter minimum.

    Raises ``SilverAbort`` if the row count is below ``minimum``. The caller
    is expected to run this against every bronze-derived silver frame in a
    pre-flight pass so that any failure aborts the run before the first
    ``_replace_data`` write -- the whole point of A1-atomic is that a run
    that would produce an empty silver table for any frame refuses to
    write ANY silver table, so downstream consumers never see a partial
    silver set.

    Returns the observed row count so the caller can log a per-table
    "staged N rows" line before the writes begin.
    """
    rows = int(df.count())
    if rows < minimum:
        raise SilverAbort(
            f"pre-flight row-count check failed for {name}: "
            f"{rows} rows, expected >= {minimum} (A1-atomic + F2 gate)"
        )
    return rows


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


# ---------------------------------------------------------------------------
# E1: shared entity_type derivation used by both batch and stream silver.
# ---------------------------------------------------------------------------
#
# The regex is a corporate-suffix heuristic applied to the END of the upper-
# cased name only (via $). Short two-letter tokens (AG, BV, SA) must not
# false-positive anywhere in the middle: "MARIA SA" is a person, "ACME SA" a
# company. Corporate names put the suffix at the end by convention. "L.L.C."
# is intentionally not detected -- the dotted form is rare in pacs.008
# dbtr/cdtr fields.
#
# One constant, two call sites: ``derive_entity_type`` returns a pyspark
# Column expression (batch ``build_entities``); ``entity_type_from_name_sql``
# returns a SQL fragment (the stream MERGE that re-derives entity_type from
# ``LEAST(target.name, source.name)``). Sharing the constant is what E1's
# unit test asserts, so batch and stream cannot drift.
_ENTITY_TYPE_COMPANY_SUFFIX_REGEX = (
    r"(LTD|LIMITED|INC|CORP|LLC|GMBH|AG|PLC|SA|SARL|BV|BANK|CAPITAL|"
    r"HOLDINGS|GROUP|INTERNATIONAL|COMPANY|CO)$"
)


def derive_entity_type(name_col):
    """Person-vs-Company entity_type Column, derived from the reported name.

    A common heuristic: a name whose upper-cased form ends with a corporate
    suffix (LTD/INC/GMBH/PLC etc.) is Company, else Person. Callers pass a
    Column that evaluates to the entity's reported name. Both silver_build
    (batch) and silver_stream (per-batch MERGE re-derivation) must call this
    helper, not inline the regex, so the two paths cannot drift.
    """
    from pyspark.sql.functions import lit as _lit
    from pyspark.sql.functions import upper as _upper
    from pyspark.sql.functions import when as _when

    return (
        _when(
            _upper(name_col).rlike(_ENTITY_TYPE_COMPANY_SUFFIX_REGEX),
            _lit("Company"),
        )
        .otherwise(_lit("Person"))
        .alias("entity_type")
    )


def entity_type_from_name_sql(name_sql_expr):
    """SQL fragment: entity_type from a name expression, shared regex.

    ``name_sql_expr`` is any SQL expression that evaluates to the entity's
    name (e.g. ``LEAST(t.name, s.name)`` inside a MERGE UPDATE). The
    returned string is a CASE expression that reduces to 'Company' or
    'Person' using the same regex ``derive_entity_type`` applies. E1's
    unit test asserts both helpers reference ``_ENTITY_TYPE_COMPANY_SUFFIX_REGEX``.
    """
    regex = _ENTITY_TYPE_COMPANY_SUFFIX_REGEX
    return f"CASE WHEN upper({name_sql_expr}) RLIKE '{regex}' THEN 'Company' ELSE 'Person' END"


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


def _is_concurrent_modification(exc):
    """Recognise the Iceberg/Delta concurrent-schema-modification error surface.

    Both engines wrap the underlying commit conflict in a Py4J/Java exception
    class whose string form carries a stable substring. ``ensure_column`` is
    the only in-process caller that races on ``ALTER TABLE``, so scoping the
    match tightly (either the JVM class name or the well-known message
    substring) avoids treating unrelated ``AnalysisException``/parse errors as
    retryable.
    """
    text = f"{type(exc).__name__}: {exc}"
    return (
        "ConcurrentModificationException" in text
        or "CommitFailedException" in text
        or "ConcurrentAppendException" in text
        or "ConcurrentDeleteReadException" in text
        or "MetadataChangedException" in text
    )


def ensure_column_with_retry(
    spark, fq_table, name, sql_type, *, max_attempts=3, backoff_seconds=0.5
):
    """I9: ``ensure_column`` with bounded retry across concurrent writers.

    Multiple silver drivers (batch + stream, or several stream instances on a
    reused catalog) race on ``ALTER TABLE ... ADD COLUMNS`` at startup. The
    loser's commit fails with a Java ``ConcurrentModificationException``
    (Iceberg) or a Delta metadata-conflict exception, and the whole silver run
    then crashes before it processes a batch.

    Retries up to ``max_attempts`` times on that error surface only, with
    ``backoff_seconds`` (linear) between attempts. Between attempts we re-read
    the schema: if the concurrent writer already added the column, we return
    ``False`` (column-already-present branch) rather than re-issuing the
    ``ALTER``. Any other exception surfaces immediately -- a wrong-type ALTER
    is a code bug, not a race.
    """
    import time as _time

    if name in spark.table(fq_table).columns:
        return False
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            spark.sql(f"ALTER TABLE {fq_table} ADD COLUMNS ({name} {sql_type})")
            log(f"[startup] added {name} to {fq_table} (reused-catalog upgrade)")
            return True
        except Exception as e:  # noqa: BLE001
            if not _is_concurrent_modification(e):
                raise
            last_exc = e
            # Re-check the schema: the concurrent writer may have added the
            # column for us, in which case we are done -- no further ALTER.
            try:
                if name in spark.table(fq_table).columns:
                    log(
                        f"[startup] {name} on {fq_table}: concurrent writer added it "
                        f"(attempt {attempt}); continuing"
                    )
                    return False
            except Exception as reread_exc:  # noqa: BLE001
                log(f"[startup] {name} on {fq_table}: schema re-read failed: {reread_exc}")
            if attempt < max_attempts:
                log(
                    f"[startup] {name} on {fq_table}: ALTER conflict "
                    f"(attempt {attempt}/{max_attempts}); retrying"
                )
                if backoff_seconds > 0:
                    _time.sleep(backoff_seconds)
    # Bounded retries exhausted; propagate the last conflict so the caller
    # sees a real failure instead of a silent no-op.
    assert last_exc is not None
    raise last_exc


def ensure_column(spark, fq_table, name, sql_type):
    """Add a nullable column to an existing table when it is missing.

    Reads the live schema first: Spark has no ``ADD COLUMN IF NOT EXISTS``
    for columns (that clause is for partitions), so the unconditional form
    failed with a parse error on every run and was silently swallowed. For a
    reused catalog whose table predates the column, that left the next write
    to fail on a schema mismatch. Returns True when the column was added.

    Delegates to ``ensure_column_with_retry`` so every runtime caller inherits
    the I9 race guard: two silver drivers starting against the same catalog
    (batch + stream, or two streams post-restart) can both call this and only
    one issues the ``ALTER``. The retry is bounded (3 attempts, 0.5s backoff),
    then propagates.
    """
    return ensure_column_with_retry(spark, fq_table, name, sql_type)


# ---------------------------------------------------------------------------
# B5: bronze-checkpoint-reset guard (C360 silver streams)
# ---------------------------------------------------------------------------
#
# A Structured Streaming checkpoint stores the source-side snapshot id of every
# committed micro-batch. When an operator wipes bronze and re-populates it
# (DROP + re-ingest, or truncate-and-re-ingest), a brand-new snapshot lineage
# appears under the same table name, and the OLD checkpoint's saved snapshot
# id is no longer on the current ancestor chain. Iceberg then either fails
# with "Cannot find snapshot" or, worse in the Delta case, silently starts
# from version 0 and re-writes every row silver already wrote. Upstream
# Iceberg PR #17599 tracks the same class of bug; no native helper today.
#
# The guard writes a small JSON sidecar next to the checkpoint on first start
# with the current bronze snapshot fingerprint (Iceberg snapshot_id or Delta
# commit version); on every subsequent start it re-reads the fingerprint and
# compares:
#
# * MATCH -> continue silently. The stream is resuming from the same lineage.
# * MISMATCH -> raise SilverAbort telling the operator to also reset silver.
#   A checkpoint whose bronze source snapshot no longer exists cannot resume
#   safely, and continuing would double-write or crash.
# * SIDECAR MISSING -> fail-open (write it, log a warning, continue). This is
#   the upgrade path from an older release. Refusing here would break every
#   running deployment on first restart; the operator can rely on the guard
#   from the next start onward.
#
# The helper is shared across silver_stream.py (Iceberg) and
# silver_stream_delta.py (Delta) so one bug fix covers both formats.


def _bronze_fingerprint_sidecar_path(checkpoint_location):
    """Absolute path of the sidecar JSON file next to a streaming checkpoint."""
    return checkpoint_location.rstrip("/") + "/bronze_fingerprint.json"


def _read_bronze_snapshot_fingerprint(spark, bronze_tbl, source_format):
    """Current bronze snapshot fingerprint as a string, or None if unavailable.

    * Iceberg: current-ancestor snapshot id from ``<table>.history``.
    * Delta: latest commit version from ``DESCRIBE HISTORY``.
    Any catalog error returns None -- the caller falls back to the fail-open
    policy (no sidecar written, warning logged) so a transient catalog failure
    never crashes a running stream.
    """
    try:
        if source_format == "delta":
            rows = spark.sql(f"DESCRIBE HISTORY {bronze_tbl} LIMIT 1").collect()
            if not rows:
                return None
            r = rows[0]
            # Delta history row exposes ``version``; support both index and key.
            try:
                v = r["version"]
            except Exception:  # noqa: BLE001
                v = r[0]
            return None if v is None else str(v)
        # Default: Iceberg.
        rows = spark.sql(
            f"SELECT snapshot_id FROM {bronze_tbl}.history "
            "WHERE is_current_ancestor ORDER BY made_current_at DESC LIMIT 1"
        ).collect()
        if not rows:
            return None
        v = rows[0][0]
        return None if v is None else str(v)
    except Exception as e:  # noqa: BLE001
        log(f"[b5] bronze snapshot lookup failed on {bronze_tbl}: {one_line(e)}")
        return None


class _HadoopSidecarFS:
    """Read/write/exists over a Hadoop-FS path, wrapping ``_hadoop_fs``."""

    def __init__(self, spark):
        self._spark = spark

    def exists(self, uri):
        fs, path = _hadoop_fs(self._spark, uri)
        return bool(fs.exists(path))

    def read(self, uri):
        fs, path = _hadoop_fs(self._spark, uri)
        in_ = fs.open(path)
        try:
            jvm = self._spark._jvm
            baos = jvm.java.io.ByteArrayOutputStream()
            jvm.org.apache.hadoop.io.IOUtils.copyBytes(in_, baos, 4096, False)
            return bytes(baos.toByteArray())
        finally:
            try:
                in_.close()
            except Exception:  # noqa: BLE001
                pass

    def write(self, uri, data):
        fs, path = _hadoop_fs(self._spark, uri)
        out = fs.create(path, True)  # overwrite=True
        try:
            out.write(bytearray(data))
        finally:
            out.close()

    def delete(self, uri):
        fs, path = _hadoop_fs(self._spark, uri)
        if not fs.exists(path):
            return False
        return bool(fs.delete(path, False))  # recursive=False


def check_bronze_fingerprint(
    spark,
    bronze_tbl,
    checkpoint_location,
    *,
    source_format="iceberg",
    fs=None,
):
    """B5 guard: refuse to resume a silver stream when bronze was reset.

    ``source_format`` is ``"iceberg"`` or ``"delta"`` -- the format of the
    bronze streaming source, not silver's own format. ``fs`` is a sidecar
    filesystem stub (used by tests); the default writes the JSON next to the
    Spark checkpoint through the same Hadoop-FS the checkpoint uses. Raises
    ``SilverAbort`` on a real fingerprint mismatch; every other failure mode
    is fail-open with a warning so a transient catalog error never crashes
    a running stream.
    """
    import json as _json

    sidecar_path = _bronze_fingerprint_sidecar_path(checkpoint_location)
    sidecar_fs = fs if fs is not None else _HadoopSidecarFS(spark)

    current = _read_bronze_snapshot_fingerprint(spark, bronze_tbl, source_format)

    exists = False
    try:
        exists = sidecar_fs.exists(sidecar_path)
    except Exception as e:  # noqa: BLE001
        log(f"[b5] sidecar existence probe failed at {sidecar_path}: {one_line(e)}")
        return  # fail-open

    if exists:
        try:
            payload = _json.loads(sidecar_fs.read(sidecar_path).decode("utf-8"))
        except Exception as e:  # noqa: BLE001
            log(f"[b5] sidecar unreadable at {sidecar_path}: {one_line(e)}; fail-open")
            return
        saved = payload.get("bronze_snapshot_fingerprint")
        saved_s = None if saved is None else str(saved)
        if current is None:
            # Cannot compute a fingerprint right now; do not condemn the
            # stream on an incomplete comparison.
            log(
                f"[b5] bronze fingerprint currently unavailable for {bronze_tbl}; "
                "keeping sidecar untouched"
            )
            return
        if saved_s != current:
            raise SilverAbort(
                "B5: bronze snapshot fingerprint mismatch at "
                f"{sidecar_path}: sidecar={saved_s!r} vs current={current!r} "
                f"(source_format={source_format}, bronze={bronze_tbl}). Bronze was "
                "reset while this checkpoint retained the old lineage; also reset "
                "silver (drop the silver table and this checkpoint) before restarting."
            )
        log(
            f"[b5] bronze fingerprint match on {bronze_tbl} "
            f"(fingerprint={current}); resuming stream"
        )
        return

    # Sidecar missing: fail-open. Only write it when we have a real
    # fingerprint; a None-sidecar would poison the next comparison.
    if current is None:
        log(
            f"[b5] no bronze fingerprint sidecar at {sidecar_path} and no snapshot "
            f"yet on {bronze_tbl}; deferring sidecar write"
        )
        return
    try:
        payload = {
            "bronze_table": bronze_tbl,
            "source_format": source_format,
            "bronze_snapshot_fingerprint": current,
            "written_by": "check_bronze_fingerprint",
        }
        sidecar_fs.write(sidecar_path, _json.dumps(payload).encode("utf-8"))
        log(
            f"[b5] WARNING: sidecar {sidecar_path} was missing; wrote current "
            f"fingerprint ({current}). Guard is active from next start onward."
        )
    except Exception as e:  # noqa: BLE001
        log(f"[b5] sidecar write failed at {sidecar_path}: {one_line(e)}; fail-open")


# H4: silver-batch fat-finger guard.
#
# silver_build_financial's batch overwrite of silver.transactions et al. wipes
# every row silver_stream_financial has written so far (docstring warning at
# ``silver_stream_financial.py:46-53``). H4 converts that warning into a
# runtime refusal: the stream writes a ``_STARTED`` marker into its
# checkpoint directory on start-up and removes it on clean shutdown; the
# batch main checks for the marker at start-up and raises ``SilverAbort``
# when it is present, unless ``--force-rebuild`` (LB_FORCE_REBUILD=1) is
# passed.
#
# Design choice: a plain marker file, not an extension of the B5 sidecar
# with a ``last_batch_at`` heartbeat. Reasoning:
#
# * A heartbeat needs a threshold, per-micro-batch writes into a JSON
#   sidecar shared with B5's fingerprint contract, and separate reader
#   logic that reasons about "still running vs. crashed a while ago".
#   Three moving parts (writer + threshold + reader) instead of one.
# * The marker file is a single existence probe. Stream writes on
#   startup, removes on clean shutdown; anything else (crash, kill -9,
#   pod eviction) leaves it in place, which is the desired semantics --
#   the batch operator should not silently overwrite tables the stream
#   was in the middle of writing. ``--force-rebuild`` is the escape
#   hatch after the operator has cleaned up.
# * Independent of B5's semantics: extending the sidecar would couple
#   the fingerprint check (whether bronze was reset) with the fat-finger
#   check (whether a stream is live) in one file, and every change to
#   either would touch the other.
#
# Fail-open policy (mirrors B5):
#
# * marker present -> ``SilverAbort`` naming the checkpoint path.
# * marker absent, or checkpoint directory absent -> proceed. Fresh
#   deployment, or the stream shut down cleanly.
# * probe raises (transient FS failure) -> log a warning and proceed.
#   H4 is defense-in-depth, not the only guard; a broken S3 listing
#   during startup MUST NOT convert a batch outage into a chain of
#   batch outages.


def _stream_started_marker_path(checkpoint_location):
    """Absolute path of the marker file next to a streaming checkpoint."""
    return checkpoint_location.rstrip("/") + "/_STARTED"


def mark_stream_started(spark, checkpoint_location, *, fs=None, payload=None):
    """Write the ``_STARTED`` marker on stream start-up. Fail-open on error.

    Called from the stream main after the Structured Streaming query has
    started so a subsequent ``silver_build_financial`` refuses to run
    against the same deployment. Any FS failure is logged and swallowed:
    the stream must not fail to start because the marker could not be
    written.
    """
    if not checkpoint_location:
        return
    sidecar_fs = fs if fs is not None else _HadoopSidecarFS(spark)
    marker_path = _stream_started_marker_path(checkpoint_location)
    body = payload if payload is not None else b"lakebench-silver-stream-financial"
    try:
        sidecar_fs.write(marker_path, body)
        log(f"[h4] wrote stream-started marker at {marker_path}")
    except Exception as e:  # noqa: BLE001
        log(f"[h4] marker write failed at {marker_path}: {one_line(e)}; continuing")


def clear_stream_started_marker(spark, checkpoint_location, *, fs=None):
    """Remove the ``_STARTED`` marker on clean shutdown. Fail-open on error.

    Called from the stream main's shutdown path (SIGTERM / SIGINT handler
    or the normal exit after ``query.stop()``). A crash or kill -9 skips
    this call, leaving the marker in place -- that is the intended
    semantics: batch must not silently overwrite tables a crashed stream
    was mid-writing.
    """
    if not checkpoint_location:
        return
    sidecar_fs = fs if fs is not None else _HadoopSidecarFS(spark)
    marker_path = _stream_started_marker_path(checkpoint_location)
    try:
        removed = sidecar_fs.delete(marker_path)
        if removed:
            log(f"[h4] cleared stream-started marker at {marker_path}")
    except Exception as e:  # noqa: BLE001
        log(f"[h4] marker delete failed at {marker_path}: {one_line(e)}; continuing")


def refuse_batch_while_stream_active(
    *,
    spark,
    checkpoint_location,
    force_rebuild,
    fs=None,
):
    """H4 fat-finger guard called from ``silver_build_financial.main()``.

    Raises ``SilverAbort`` when the AML stream's ``_STARTED`` marker is
    present at ``checkpoint_location`` and ``force_rebuild`` is False.
    Fails open on a missing checkpoint / missing marker / transient FS
    error so a fresh batch-only deployment is never blocked.
    """
    if not checkpoint_location:
        return
    sidecar_fs = fs if fs is not None else _HadoopSidecarFS(spark)
    marker_path = _stream_started_marker_path(checkpoint_location)
    try:
        exists = sidecar_fs.exists(marker_path)
    except Exception as e:  # noqa: BLE001
        log(
            f"[h4] WARNING: marker probe failed at {marker_path}: "
            f"{one_line(e)}; fail-open (batch proceeds)"
        )
        return
    if not exists:
        return
    if force_rebuild:
        log(
            f"[h4] stream-started marker present at {marker_path}; "
            "LB_FORCE_REBUILD=1 -> operator opted in, batch proceeds"
        )
        return
    raise SilverAbort(
        "H4: refusing silver_build_financial while an AML stream appears "
        f"active. Streaming checkpoint carries a _STARTED marker at "
        f"{marker_path}. Overwriting silver.transactions et al. now would "
        "wipe every row the stream has written. Stop the continuous "
        "deployment first, delete the marker if the stream is genuinely "
        "gone (kill -9 / pod eviction leaves it), or re-run with "
        "--force-rebuild (LB_FORCE_REBUILD=1) to opt in explicitly."
    )


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
        # LB-188: PURGE deletes every file the table metadata references,
        # wherever it sits, so it runs only for a table whose directory this
        # deployment owns. A table whose location is unreadable or outside this
        # deployment is dropped catalog-only (its files kept), matching the
        # "kept ..." log above; an owned table's files are already removed by
        # the _delete_children and location delete around this drop.
        if iceberg and owned:
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
    # Emit-then-push: the log block above is the authoritative metrics.json
    # source and is always written first. The push is best-effort live-view
    # observability and never affects the stage.
    _push_stage_metrics(job, input_size_gb, input_rows, output_rows, elapsed_seconds, extra)


def _push_stage_metrics(job, input_size_gb, input_rows, output_rows, elapsed_seconds, extra):
    """Best-effort push of bronze/silver/gold stage metrics to the per-deployment
    Pushgateway for live Grafana visibility. No-op when LB_PUSHGATEWAY_URL is
    unset (observability off). Driver-only. metrics.json stays the authoritative
    artifact.

    The payload is built on the caller thread (cheap, no I/O), but the network
    PUT runs fire-and-forget on a daemon thread. In continuous mode this is
    called from the streaming micro-batch thread per tick, and urllib's timeout
    does not bound DNS resolution (getaddrinfo) -- an off-thread push ensures a
    slow or unresolvable gateway can never stall the streaming trigger. All
    errors are swallowed.
    """
    import os

    url = os.environ.get("LB_PUSHGATEWAY_URL", "").strip()
    if not url:
        return
    try:
        run_id = os.environ.get("LB_RUN_ID", "").strip() or "unknown"
        stage = str(job).replace("/", "_")
        lines = [
            f"lakebench_stage_input_rows {float(input_rows)}",
            f"lakebench_stage_output_rows {float(output_rows)}",
            f"lakebench_stage_input_size_gb {float(input_size_gb)}",
            f"lakebench_stage_elapsed_seconds {float(elapsed_seconds)}",
        ]
        for key, value in extra.items():
            # Per-silver-table row counts (silver_<table>_rows) become a labeled
            # gauge so one panel shows every table.
            if (
                key.startswith("silver_")
                and key.endswith("_rows")
                and isinstance(value, (int, float))
                and not isinstance(value, bool)
            ):
                table = key[len("silver_") : -len("_rows")]
                lines.append(f'lakebench_silver_table_rows{{table="{table}"}} {float(value)}')
        body = "\n".join(lines) + "\n"
        target = url.rstrip("/") + f"/metrics/job/spark_stage/stage/{stage}/run_id/{run_id}"

        def _send():
            try:
                import urllib.request

                req = urllib.request.Request(
                    target,
                    data=body.encode(),
                    method="PUT",
                    headers={"Content-Type": "text/plain"},
                )
                urllib.request.urlopen(req, timeout=2).close()  # noqa: S310 (in-cluster http)
            except Exception:
                pass  # best-effort; the JOB METRICS log block is authoritative

        import threading

        threading.Thread(target=_send, daemon=True).start()
    except Exception:
        pass  # never let observability affect the stage


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


SILVER_STATE_CONFIGMAP_NAME = "lakebench-silver-state"
"""C2 (silver-plan): per-deployment ConfigMap that carries the bronze-side
data clock (``bronze_data_clock``) that bronze-verify writes and job.py
reads to build the silver env bundle. The deployer creates it on greenfield
deploy so bronze-verify's write is a plain update, not a first create."""


def write_bronze_data_clock(namespace, bronze_max_ts):
    """C2 (silver-plan): record bronze's newest observed event date to the
    ``lakebench-silver-state`` ConfigMap so job.py can resolve LB_DATA_CLOCK
    for silver jobs without a Spark session in the submit path.

    ``bronze_max_ts`` is the ``max(event_timestamp)`` value verified bronze
    holds (``datetime.datetime`` or ``datetime.date``); this function
    computes the ISO date and stores it under the ``bronze_data_clock`` key.

    A failure to write is logged and swallowed. Silver's C2 resolver falls
    back through ``datagen.timestamp_start`` and today at 00:00 UTC when the
    key is missing, so a transient K8s error at bronze-verify time never
    blocks a silver run.
    """
    if bronze_max_ts is None:
        log("[silver-state] bronze_data_clock: no timestamp to write; skipping")
        return
    from datetime import date as _date
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    # LB_DATA_CLOCK follows the datagen ``timestamp_end`` (EXCLUSIVE) convention:
    # ``configured_data_clock`` subtracts one day to recover the newest possible
    # event date. To keep ConfigMap-sourced anchors consistent with the
    # ``timestamp_end`` rung, we store ``max(event_ts) + 1 day`` -- so silver
    # resolves back to exactly ``max(event_ts)`` as the recency anchor.
    if isinstance(bronze_max_ts, _dt):
        d = bronze_max_ts.date()
    elif isinstance(bronze_max_ts, _date):
        d = bronze_max_ts
    else:
        d = _date.fromisoformat(str(bronze_max_ts)[:10])
    iso = (d + _td(days=1)).isoformat()
    ns = namespace or os.environ.get("LAKEBENCH_NAMESPACE")
    if not ns:
        log("[silver-state] LAKEBENCH_NAMESPACE unset; skipping bronze_data_clock write")
        return
    try:
        from kubernetes import client as _kclient
        from kubernetes import config as _kconfig
        from kubernetes.client.exceptions import ApiException
    except ImportError as e:  # pragma: no cover -- k8s client always present on driver
        log(f"[silver-state] kubernetes client not importable: {e}; skipping")
        return
    try:
        try:
            _kconfig.load_incluster_config()
        except Exception:  # noqa: BLE001 -- fallback for out-of-cluster runs
            try:
                _kconfig.load_kube_config()
            except Exception as e:  # noqa: BLE001
                log(f"[silver-state] no kube config available: {e}; skipping")
                return
        core = _kclient.CoreV1Api()
        try:
            core.patch_namespaced_config_map(
                name=SILVER_STATE_CONFIGMAP_NAME,
                namespace=ns,
                body={"data": {"bronze_data_clock": iso}},
            )
            log(
                f"[silver-state] bronze_data_clock={iso} written to {ns}/{SILVER_STATE_CONFIGMAP_NAME}"
            )
            return
        except ApiException as e:
            if e.status != 404:
                log(f"[silver-state] patch failed ({e.status}): {e.reason}; skipping")
                return
        # Not found: create it. The deployer normally seeds this on greenfield;
        # a create here is a safety net for pre-C2 deployments.
        from kubernetes.client.models import V1ConfigMap, V1ObjectMeta

        body = V1ConfigMap(
            metadata=V1ObjectMeta(name=SILVER_STATE_CONFIGMAP_NAME, namespace=ns),
            data={"bronze_data_clock": iso},
        )
        try:
            core.create_namespaced_config_map(namespace=ns, body=body)
            log(
                f"[silver-state] bronze_data_clock={iso} created in {ns}/{SILVER_STATE_CONFIGMAP_NAME}"
            )
        except ApiException as e:
            log(f"[silver-state] create failed ({e.status}): {e.reason}; skipping")
    except Exception as e:  # noqa: BLE001
        log(f"[silver-state] unexpected write error: {type(e).__name__}: {e}; skipping")


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


def resolve_data_clock(df_fallback=None, strict=False):
    """The c360 data clock: the date recency is measured from.

    One clock for the whole run: from LB_DATA_CLOCK when set, so batch,
    every multi-cycle cycle and every micro-batch use the same anchor and a
    rerun reproduces the same scores (GOALS P4.2). Only when it is unset is
    it measured as max(event date) of ``df_fallback``. Logs which was used.

    ``strict=True`` (C1, silver-plan): C360 silver mains use this. C2 sets
    LB_DATA_CLOCK unconditionally in the silver env bundle (with a today
    fallback), so seeing it unset here means the env plumbing broke -- a
    silent fallback would ship a NULL or measured-per-batch anchor,
    invalidating cross-cycle recency scoring. Raise ``SilverAbort`` instead.
    AML mains keep ``strict=False``: they do not use customer_recency_score,
    so a missing anchor is not a correctness hazard for them.
    """
    anchor = configured_data_clock()
    if anchor is not None:
        log(f"Data clock (recency anchor): {anchor} from LB_DATA_CLOCK")
        return anchor
    if strict:
        raise SilverAbort(
            "LB_DATA_CLOCK is unset. Silver C360 mains require a resolved "
            "data clock so recency scores stay reproducible across cycles "
            "and batch/stream. job.py._build_env_vars should always export "
            "LB_DATA_CLOCK for silver jobs (see silver-plan C2)."
        )
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


def welford_merge(n_a, mean_a, M2_a, n_b, mean_b, M2_b):
    """Parallel Welford merge of two (n, mean, M2) blocks.

    D-full-profiles: ``silver.entity_profiles.avg_amount_usd`` and
    ``stddev_amount_usd`` are maintained incrementally in continuous mode.
    Each micro-batch computes its own (n_b, mean_b, M2_b) over the
    originator-side amounts for the entities it touches; this helper folds
    that batch block into the profile row's target block using the parallel
    recurrence (Chan et al., 1979; the Wikipedia
    "Algorithms for calculating variance -- Parallel algorithm").

    Given target block ``a`` and batch block ``b``::

        n   = n_a + n_b
        d   = mean_b - mean_a
        mean = mean_a + d * n_b / n
        M2  = M2_a + M2_b + d * d * n_a * n_b / n

    Sample stddev is then ``sqrt(M2 / (n - 1))`` -- pyspark's ``stddev`` is
    ``stddev_samp``, so the batch main writes M2 as ``variance * (n - 1)``
    for parity.

    Returns ``(n, mean, M2)`` with the empty-block identity: merging with
    ``(0, 0.0, 0.0)`` on either side returns the other block unchanged.
    Callers must feed 0.0 (not None) for M2 when n <= 1 (variance is
    undefined for a single point; M2 is 0 by definition).
    """
    n_a = int(n_a or 0)
    n_b = int(n_b or 0)
    if n_a == 0:
        return n_b, float(mean_b or 0.0), float(M2_b or 0.0)
    if n_b == 0:
        return n_a, float(mean_a or 0.0), float(M2_a or 0.0)
    n = n_a + n_b
    delta = float(mean_b) - float(mean_a)
    mean = float(mean_a) + delta * n_b / n
    M2 = float(M2_a) + float(M2_b) + delta * delta * n_a * n_b / n
    return n, mean, M2


def delta_batch_txn_options(app, rebuild_epoch, cycle):
    """Delta writer options that make a batch cycle-append exactly-once (B1).

    Extends the ``txnAppId`` / ``txnVersion`` protocol Delta uses in
    ``foreachBatch`` to batch-mode multi-cycle appends. ``rebuild_epoch``
    partitions the app id so a ``--force-rebuild`` cycles the id space and
    cycle 0 of the new epoch is not skipped as a duplicate of the last epoch's
    cycle 0. ``cycle`` is the txnVersion: Delta short-circuits any later
    commit whose (appId, version) it has already seen, so re-submitting the
    same cycle within the same epoch is a no-op at the transaction log --
    the second submission's DESCRIBE HISTORY shows ``SET TRANSACTION`` with
    no data commit.

    Do NOT reuse ``delta_idempotent_options``: that helper calls
    ``streaming_query_id`` which raises outside ``foreachBatch`` and would
    crash the batch main at import.
    """
    return {
        "txnAppId": f"{app}-rebuild-{int(rebuild_epoch)}",
        "txnVersion": str(int(cycle)),
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

    ``fq_table`` accepts a single fully-qualified table name (str) or a list
    of them; any populated table on the list triggers the refusal. B3
    uniform silver exit contract: the refusal raises ``SilverAbort`` so the
    driver terminates with the same exception class every other silver
    invariant does (A1, F1). Existing callers already let unhandled
    exceptions terminate main().
    """
    if not checkpoint_is_fresh(spark, checkpoint_location):
        return
    tables = [fq_table] if isinstance(fq_table, str) else list(fq_table)
    populated: list[str] = []
    for t in tables:
        if not table_exists(spark, t):
            continue
        if spark.table(t).limit(1).count() == 0:
            continue
        populated.append(t)
    if not populated:
        return
    log(
        f"ERROR: checkpoint {checkpoint_location} is empty but populated silver "
        f"table(s) {', '.join(populated)} already hold rows; starting would re-read "
        "the whole source and duplicate them. Drop the tables or restore the checkpoint."
    )
    raise SilverAbort(
        f"fresh checkpoint over populated silver: {checkpoint_location} vs {', '.join(populated)}"
    )


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


def gold_date_coverage_problem(gold_rows: int, distinct_silver_dates: int) -> str | None:
    """Non-degeneracy check for c360 gold (invariant 3): the daily-KPI gold table
    must hold exactly one row per distinct silver ``interaction_date``.

    Returns an error string when they differ, None when they match. Pure, so it
    is unit-tested without Spark. Holds for every strategy: a full rebuild writes
    one KPI row per date, and the incremental strategy's watermark-inclusive
    recompute plus the kept older rows also cover every date. A mismatch means
    gold dropped or duplicated dates and the run must fail rather than report a
    degenerate gold as success (LB batch runs once passed with 0 rows, LB-044).
    """
    if gold_rows != distinct_silver_dates:
        return (
            f"gold has {gold_rows} KPI row(s) but silver has {distinct_silver_dates} "
            "distinct interaction_date bucket(s)"
        )
    return None


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


def aml_opening_balance(account_id_col):
    """Deterministic per-account opening balance for silver.account_statements.

    ``abs(account_id) % 200_000 + 10_000`` in the (10000, 210000] range, cast
    to decimal(18, 2). Shared between ``silver_build_financial.build_statements``
    (batch) and ``silver_stream_financial._maintain_statements`` (D-full) so a
    future formula change cannot drift between the two write paths silently.
    Batch mode's ``xxhash64`` returns a signed BIGINT and Spark's ``%``
    preserves sign, so ``abs()`` is required to keep the result in the
    intended positive range.
    """
    from pyspark.sql.functions import abs as _abs
    from pyspark.sql.functions import lit as _lit

    return ((_abs(account_id_col) % _lit(200_000)) + _lit(10_000)).cast("decimal(18,2)")


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


def sealed_txns_filter(spark, txns_df, catalog, versions_table):
    """Semi-join ``txns_df`` against ``silver_batch_versions`` on
    ``(_stream_id, _batch_id)`` (I10).

    A driver crash between the AML stream's transactions/edges commits and
    the sealed-marker commit leaves rows visible in silver.transactions
    with no matching row in ``silver_batch_versions``. The semi-join hides
    that partial batch from every gold-side consumer, so a mid-batch crash
    window is invisible to detection, scoring, replay, and reproduction.

    Batch mode stamps rows with ``(_stream_id='batch', _batch_id=cycle)``
    and writes a matching versions row last, so the same filter applies
    uniformly to batch and stream reads.

    ``txns_df`` is passed in as an already-materialised DataFrame: callers
    that read via ``spark.table`` supply that; callers that pin at an
    Iceberg snapshot (``read_at_snapshot`` / ``FOR TIMESTAMP AS OF``)
    supply that pinned frame. The versions table is read at CURRENT state
    (never pinned), so a batch sealed AFTER the txn snapshot but BEFORE
    this call correctly becomes visible on a later reader; a batch not yet
    sealed anywhere in the versions table stays hidden.

    Fail-open on a missing versions table: the helper logs and returns
    ``txns_df`` unchanged so a legacy catalog that predates I10 still
    reads (the next silver run bootstraps the sidecar). Callers do not
    have to catch this: the filter degrades to the pre-I10 behaviour.
    """
    from pyspark.sql.functions import col as _col

    versions_fq = f"{catalog}.{versions_table}"
    try:
        versions = spark.table(versions_fq).select(
            _col("stream_id").alias("_sv_stream_id"),
            _col("batch_id").alias("_sv_batch_id"),
        )
    except Exception as e:  # noqa: BLE001
        log(
            f"[i10] {versions_fq} not readable ({e}); "
            "falling back to unfiltered read. A silver run bootstraps the sidecar."
        )
        return txns_df
    return txns_df.join(
        versions,
        (txns_df["_stream_id"] == versions["_sv_stream_id"])
        & (txns_df["_batch_id"] == versions["_sv_batch_id"]),
        "left_semi",
    )


class materialised_source:  # noqa: N801 -- used like a function: with materialised_source(...)
    """``with materialised_source(spark, df, view_name) as view:`` gives a
    temp view over ``df`` with its lineage cut, for use as a MERGE source.

    On Spark 4.1 with Iceberg, ``MERGE ... USING <temp view>`` fails with an
    internal error ("No plan for TableReference") when the view's plan reads
    an Iceberg table. A view over a local checkpoint of the frame reads only
    the checkpointed blocks (tested on Spark 4.0 and 4.1). The content is
    the frame's rows at the moment of entry, computed once.

    On exit the view is dropped and the checkpoint's blocks are freed. They
    belong to the checkpoint's own RDD, which ``DataFrame.unpersist`` does
    not reach (a checkpoint is not in the cache manager), so the RDD under
    the plan is unpersisted directly. Both run when the body raises.

    Each entry logs ``[merge-source] <view>: materialised``.

    The blocks live on the executors that computed them: an executor lost
    between entry and the MERGE fails that micro-batch, and the query
    restarts through the replay path.
    """

    def __init__(self, spark, df, view_name):
        self._spark = spark
        self._df = df
        self._view = view_name
        self._checkpoint = None

    def __enter__(self):
        self._checkpoint = self._df.localCheckpoint(eager=True)
        try:
            self._checkpoint.createOrReplaceTempView(self._view)
        except BaseException:
            self._free()
            raise
        # One line per MERGE source and batch, so a run's log shows each
        # site materialised. No row count: counting is one more Spark job
        # per site per micro-batch, which would land inside the stream's
        # published merge timings (the entity and account counts are in the
        # [dim-merge] lines already).
        log(f"[merge-source] {self._view}: materialised")
        return self._view

    def __exit__(self, *_exc):
        try:
            self._spark.catalog.dropTempView(self._view)
        except Exception as e:  # noqa: BLE001
            log(f"[merge-source] dropping view {self._view} failed: {type(e).__name__}: {e}")
        self._free()
        return False

    def _free(self):
        try:
            self._checkpoint._jdf.logicalPlan().rdd().unpersist(False)
        except Exception as e:  # noqa: BLE001
            log(f"[merge-source] freeing {self._view} blocks failed: {type(e).__name__}: {e}")


class SealedFilterError(RuntimeError):
    """``sealed_txns_filter_at`` could not read the versions table at the
    requested snapshot (the snapshot is expired or never existed, or the
    table is missing or unreadable). The caller decides the fallback; the
    helper never returns the transactions unfiltered."""


def sealed_txns_filter_at(spark, txns_df, catalog, versions_table, versions_snapshot):
    """``sealed_txns_filter`` with the versions table read ``VERSION AS OF
    versions_snapshot`` instead of at current state.

    A detection tick and a scorer that pass the same versions snapshot see
    the same sealed set, whatever is committed to the versions table later:
    the snapshot id is part of the plan, so every action on the returned
    frame reads the same versions rows.

    Fails closed, unlike ``sealed_txns_filter``:

    - ``versions_snapshot`` must be an ``int`` (not a bool). ``None``, the
      string ``TTD_SNAPSHOT_UNKNOWN`` or any other value raises
      ``TypeError`` before anything is read.
    - The versions frame is built and analysed inside this call, so a
      snapshot id the table does not have, or a missing or unreadable
      versions table, raises ``SealedFilterError`` here, before the caller
      runs any action.

    It never returns ``txns_df`` unfiltered.
    """
    if isinstance(versions_snapshot, bool) or not isinstance(versions_snapshot, int):
        raise TypeError(
            "sealed_txns_filter_at needs an int versions snapshot id, got "
            f"{type(versions_snapshot).__name__} {versions_snapshot!r}"
        )
    from pyspark.sql.functions import col as _col

    versions_fq = f"{catalog}.{versions_table}"
    try:
        versions = spark.sql(
            f"SELECT stream_id, batch_id FROM {versions_fq} VERSION AS OF {versions_snapshot}"
        ).select(
            _col("stream_id").alias("_sv_stream_id"),
            _col("batch_id").alias("_sv_batch_id"),
        )
        # Classic pyspark analyses spark.sql eagerly; reading the schema
        # forces analysis on a lazy client too, so the snapshot and the
        # table are resolved here and not at the caller's first action.
        versions.schema  # noqa: B018
    except Exception as e:  # noqa: BLE001
        raise SealedFilterError(
            f"{versions_fq} not readable at snapshot {versions_snapshot}: {e}"
        ) from e
    return txns_df.join(
        versions,
        (txns_df["_stream_id"] == versions["_sv_stream_id"])
        & (txns_df["_batch_id"] == versions["_sv_batch_id"]),
        "left_semi",
    )


# Bumped when the fingerprint definition changes, so fingerprints made by two
# definitions never compare equal. Hashed into every row.
_FINGERPRINT_VERSION = 1


def frame_fingerprint(df, cols):
    """Order-independent fingerprint of ``df`` over the named columns.

    Returns ``(rows, fp, cols_sha)``: the row count, the sum of one xxhash64
    per row as a decimal string, and the first 16 hex digits of the sha256
    of ``name:type`` for each column in the given order. Two frames match
    only when all three match. One Spark action (one aggregate).

    Each row's hash covers the definition version, every named column in
    order, and a null mask (bit ``i`` set when column ``i`` is NULL). Spark's
    xxhash64 skips NULL arguments, so without the mask ``(x, NULL)`` and
    ``(NULL, x)`` would hash the same. The sum is taken as decimal(38,0), so
    it is exact and cannot overflow under ANSI mode. Row order does not
    matter; a duplicated row changes both ``rows`` and ``fp``.

    Array, struct and map columns are hashed through a canonical form,
    because xxhash64 chains nested values without their lengths or NULL
    positions (``(["a","b"], [])`` and ``(["a"], ["b"])`` would collide, and
    so would ``[a, NULL]`` and ``[NULL, a]``, or a struct whose value moves
    to a NULL neighbour). Every nested value is paired with its own NULL
    flag, every array and map carries its size, and a map becomes its
    entries in sorted order (Spark refuses to hash a map, and a map has no
    defined entry order). ``-0.0`` and ``0.0`` hash the same, and so do all
    NaNs; a collated string hashes by its bytes, so ``'A'`` and ``'a'``
    differ under ``UTF8_LCASE``. A struct with two fields of one name (only
    an in-memory frame can have one) raises at analysis. The nested form
    costs higher-order functions per row: measure it before fingerprinting
    a full large table on a hot path.

    Callers name the columns; AML time travel and reproduction pass every
    column of the snapshot schema, including ``_stream_id``, ``_batch_id``
    and ``ingest_ts``, which decide sealed visibility.

    Raises ``ValueError`` for no columns, more than 63 (the mask is a
    signed long), a repeated name, or a name that is not a top-level
    column of ``df``.
    """
    import hashlib

    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, MapType, StructField, StructType

    def _flagged(c, dt):
        return F.struct(c.isNull(), _canonical(c, dt))

    def _canonical_array(c, element_type, sort):
        elements = F.transform(c, lambda x: _flagged(x, element_type))
        if sort:
            elements = F.array_sort(elements)
        size = F.when(c.isNull(), F.lit(-1)).otherwise(F.size(c))
        return F.struct(size, elements)

    def _canonical(c, dt):
        if isinstance(dt, MapType):
            entry = StructType([StructField("key", dt.keyType), StructField("value", dt.valueType)])
            return _canonical_array(F.map_entries(c), entry, sort=True)
        if isinstance(dt, ArrayType):
            return _canonical_array(c, dt.elementType, sort=False)
        if isinstance(dt, StructType):
            return F.struct(*[_flagged(c.getField(f.name), f.dataType) for f in dt.fields])
        return c

    cols = list(cols)
    if not cols:
        raise ValueError("frame_fingerprint needs at least one column")
    if len(cols) > 63:
        raise ValueError(f"frame_fingerprint takes at most 63 columns, got {len(cols)}")
    if len(set(cols)) != len(cols):
        raise ValueError(f"frame_fingerprint: repeated column in {cols}")
    fields = {f.name: f for f in df.schema.fields}
    missing = [c for c in cols if c not in fields]
    if missing:
        raise ValueError(f"frame_fingerprint: {missing} not in {sorted(fields)}")

    def _ref(name):
        return F.col("`" + name.replace("`", "``") + "`")

    hashed = []
    mask = F.lit(0).cast("long")
    for i, name in enumerate(cols):
        c = _ref(name)
        hashed.append(_canonical(c, fields[name].dataType))
        bit = F.lit(1 << i).cast("long")
        mask = mask + F.when(c.isNull(), bit).otherwise(F.lit(0).cast("long"))
    h = F.xxhash64(F.lit(_FINGERPRINT_VERSION), *hashed, mask)
    row = df.agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(h.cast("decimal(38,0)")).alias("s"),
    ).collect()[0]
    rows = int(row["n"])
    fp = str(int(row["s"])) if row["s"] is not None else "0"
    spec = ",".join(f"{name}:{fields[name].dataType.simpleString()}" for name in cols)
    cols_sha = hashlib.sha256(spec.encode("utf-8")).hexdigest()[:16]
    return rows, fp, cols_sha


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


def _status_events_dropped(jsc):
    """Events the driver's status listener queue has dropped so far."""
    registry = jsc.listenerBus().metrics().metricRegistry()
    return int(registry.counter("queue.appStatus.numDroppedEvents").getCount())


def rule_profile_mark(spark):
    """Where the application stands when a rule's job group is set: jobs
    submitted so far (DAGScheduler.numTotalJobs) and status events dropped
    so far. Pass it to ``rule_stage_profile``. None when it cannot be read."""
    try:
        jsc = spark.sparkContext._jsc.sc()
        return {
            "jobs": int(jsc.dagScheduler().numTotalJobs()),
            "dropped": _status_events_dropped(jsc),
        }
    except Exception:  # noqa: BLE001 -- diagnostic only
        return None


def rule_stage_profile(spark, group, rule_id, *, mark=None, top=3, wait_s=5.0):
    """Log the ``top`` stages of job group ``group`` by executor run time.

    Reads the driver's AppStatusStore through py4j. The store is live with
    ``spark.ui.enabled=false`` (only the UI server is off). It is filled by
    an asynchronous listener, so this first waits up to ``wait_s`` for the
    listener bus to drain. One line per stage, heaviest first::

        [stage-profile] rule=<id> group=<g> stage=<n> attempt=<a> status=<s>
            tasks=<t> wall_s=<s> exec_s=<s> shuffle_read_mb=<m> max_task_s=<s>
            stages=<k> truncated=<b> complete=<b> lossy=<b> profile_s=<s>
            name=<stage name>

    ``wall_s`` is submission to completion, ``exec_s`` the summed executor
    run time of the stage's tasks, ``max_task_s`` its longest task,
    ``stages`` the number of the group's stages the store holds (skipped
    stages, whose output was reused, are not counted). The three flags say
    how far the numbers can be trusted:

    - ``complete=false``: the listener bus had not drained within
      ``wait_s``, so the rule's last jobs or task ends may be missing. The
      wait covers every listener queue (an enabled event log too), so the
      flag can be false while the status store itself had caught up;
    - ``truncated=true``: the store had already dropped some of the
      group's jobs or stages (it keeps ``spark.ui.retainedJobs`` jobs and
      ``spark.ui.retainedStages`` stages). Dropped jobs are not listed under
      the group at all, so they are found by count against ``mark``;
    - ``lossy=true``: the listener queue dropped events during the rule
      (against ``mark``), so the stored task totals are low.

    Without a ``mark`` neither can be checked, and both are logged true.
    ``profile_s`` is the time this call took, wait included: Lakebench
    overhead inside the gold-finalize job's time. A group with no stage in
    the store logs ``stages=0`` with the same flags and no stage line. Any other failure logs ``[stage-profile] rule=<id>
    group=<g> unavailable reason=<one line>`` and returns None. Never
    raises, so detection cannot fail because of it. Returns the logged
    stage rows.
    """
    import time

    started = time.time()
    try:
        sc = spark.sparkContext
        jsc = sc._jsc.sc()
        complete = True
        try:
            jsc.listenerBus().waitUntilEmpty(int(wait_s * 1000))
        except Exception:  # noqa: BLE001 -- TimeoutException, or no such call
            complete = False
        tracker = sc.statusTracker()
        store = jsc.statusStore()
        job_ids = list(tracker.getJobIdsForGroup(group))
        truncated = lossy = mark is None
        if mark is not None:
            truncated = len(job_ids) < int(jsc.dagScheduler().numTotalJobs()) - mark["jobs"]
            lossy = _status_events_dropped(jsc) > mark["dropped"]
        stage_ids = set()
        for job_id in job_ids:
            info = tracker.getJobInfo(job_id)
            if info is None:
                truncated = True
                continue
            stage_ids.update(int(s) for s in info.stageIds)
        stages = []
        for sid in sorted(stage_ids):
            try:
                sd = store.lastStageAttempt(sid)
            except Exception as e:  # noqa: BLE001
                if "NoSuchElementException" not in str(e):
                    raise
                truncated = True  # dropped from the store
                continue
            status = sd.status().toString()
            if status in ("SKIPPED", "PENDING"):
                continue
            sub, comp = sd.submissionTime(), sd.completionTime()
            wall = (
                round((comp.get().getTime() - sub.get().getTime()) / 1000.0, 1)
                if sub.isDefined() and comp.isDefined()
                else None
            )
            stages.append(
                {
                    "stage": sid,
                    "attempt": int(sd.attemptId()),
                    "status": status,
                    "tasks": int(sd.numTasks()),
                    "wall_s": wall,
                    "exec_s": round(int(sd.executorRunTime()) / 1000.0, 1),
                    "shuffle_read_mb": round(int(sd.shuffleReadBytes()) / 1048576.0, 1),
                    "name": one_line(sd.name(), limit=120),
                }
            )
        stages.sort(key=lambda r: (-r["exec_s"], r["stage"]))
        rows = stages[:top]
        for r in rows:
            gw = sc._gateway
            quantile = gw.new_array(gw.jvm.double, 1)
            quantile[0] = 1.0
            summary = store.taskSummary(r["stage"], r["attempt"], quantile)
            r["max_task_s"] = (
                round(summary.get().executorRunTime().apply(0) / 1000.0, 1)
                if summary.isDefined()
                else None
            )
    except Exception as e:  # noqa: BLE001 -- diagnostic only
        log(
            f"[stage-profile] rule={rule_id} group={group} unavailable "
            f"reason={one_line(f'{type(e).__name__}: {e}')}"
        )
        return None
    flags = " ".join(
        f"{k}={'true' if v else 'false'}"
        for k, v in (("truncated", truncated), ("complete", complete), ("lossy", lossy))
    )
    flags += f" profile_s={time.time() - started:.2f}"
    if not rows:
        log(f"[stage-profile] rule={rule_id} group={group} stages=0 {flags}")
    for r in rows:
        log(
            f"[stage-profile] rule={rule_id} group={group} stage={r['stage']} "
            f"attempt={r['attempt']} status={r['status']} tasks={r['tasks']} "
            f"wall_s={r['wall_s']} exec_s={r['exec_s']} "
            f"shuffle_read_mb={r['shuffle_read_mb']} max_task_s={r['max_task_s']} "
            f"stages={len(stages)} {flags} name={r['name']}"
        )
    return rows
