"""
Adaptive Silver Build - Scale-aware transformation pipeline

Selects the processing strategy from the bronze size:
- SIMPLE: < 100GB, standard processing
- STREAMING: >= 100GB, single pass, hash distribution (Iceberg clusters by partition)
"""

from __future__ import annotations

import os
import re
import time

from common import (
    METADATA_DELETE_AFTER_COMMIT,
    METADATA_PREVIOUS_VERSIONS_MAX,
    SilverAbort,
    apply_silver_dynamic_config,
    apply_silver_transformations_anchored,
    batch_id_last,
    c360_bronze_path,
    c360_bronze_run_path,
    determine_silver_strategy,
    ensure_column,
    env,
    finish_silver_build,
    log,
    one_line,
    refuse_unforced_rebuild,
    resolve_data_clock,
    set_utc_session,
    silver_input,
    table_exists,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    regexp_extract,
    when,
)

# Rows counted during the build.
_COUNTED: dict[str, int] = {}


def rows_added_by_last_commit(spark, silver_tbl):
    """Rows the table's latest snapshot added (this cycle's write).

    Snapshot metadata only. Returns None when the summary is missing, never
    the table's row count: that is cumulative and would hide a cycle that
    wrote nothing. Callers refuse to publish None as ``output_rows``.
    """
    try:
        # The snapshot main points at, not the latest committed_at (a
        # writer's clock), so the answer is exact.
        r = spark.sql(
            f"SELECT s.summary['added-records'] AS n FROM {silver_tbl}.snapshots s "
            f"JOIN {silver_tbl}.refs r ON s.snapshot_id = r.snapshot_id WHERE r.name = 'main'"
        ).collect()
        if r and r[0]["n"] is not None:
            return int(r[0]["n"])
    except Exception as e:  # noqa: BLE001
        log(f"Warning: snapshot summary unavailable ({e}); output_rows will be unknown")
    return None


# Properties of the Iceberg silver table other than the distribution mode and
# fanout, which silver_iceberg_props reads from spark.conf on every write.
_SILVER_ICEBERG_STATIC_PROPS: tuple[tuple[str, str], ...] = (
    ("write.format.default", "parquet"),
    ("write.parquet.compression-codec", "snappy"),
    METADATA_DELETE_AFTER_COMMIT,
    METADATA_PREVIOUS_VERSIONS_MAX,
    ("write.target-file-size-bytes", "134217728"),  # 128MB target
)


def silver_iceberg_props(spark) -> list[tuple[str, str]]:
    """The silver table's properties: the static set plus the distribution
    mode and fanout from ``spark.lb.silver.distribution_mode`` (default
    hash) and ``spark.lb.silver.fanout_enabled`` (default false)."""
    dist_mode = spark.conf.get("spark.lb.silver.distribution_mode", "hash")
    fanout = spark.conf.get("spark.lb.silver.fanout_enabled", "false")
    props = [*_SILVER_ICEBERG_STATIC_PROPS, ("write.distribution-mode", dist_mode)]
    if str(fanout).lower() == "true":
        props.append(("write.spark.fanout.enabled", "true"))
    return props


def reassert_silver_iceberg_props(spark, silver_tbl) -> None:
    """Set the silver table's properties again before an append.

    ``writeTo(...).append()`` does not carry TBLPROPERTIES, so a table that
    lost a property (an ALTER, a table an older script created) would keep
    the wrong defaults for the life of the deployment. The ALTER is
    idempotent and touches metadata only.
    """
    props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in silver_iceberg_props(spark))
    spark.sql(f"ALTER TABLE {silver_tbl} SET TBLPROPERTIES ({props_sql})")


def write_silver(spark, silver_df, silver_tbl, appending, cycle) -> None:
    """Append *silver_df* as cycle *cycle*, or replace the table with it.

    An append first deletes the rows an earlier attempt of the same cycle
    wrote (none on a first attempt), so a resubmitted cycle replaces them.
    A full build replaces the table, partitioned by day, with
    ``silver_iceberg_props``.
    """
    if appending:
        log(f"Appending to existing table (incremental mode, cycle={cycle})")
        ensure_column(spark, silver_tbl, "_batch_id", "BIGINT")
        spark.sql(f"DELETE FROM {silver_tbl} WHERE _batch_id = {int(cycle)}")
        reassert_silver_iceberg_props(spark, silver_tbl)
        silver_df.writeTo(silver_tbl).append()
        return
    drop_if_columns_move(spark, silver_tbl, silver_df)
    writer = silver_df.writeTo(silver_tbl).partitionedBy("interaction_date")
    for key, value in silver_iceberg_props(spark):
        writer = writer.tableProperty(key, value)
    writer.createOrReplace()


def tag_batch(df_bronze, cycle, appending):
    """``df_bronze`` with ``_batch_id``: the cycle whose rows these are.

    An append reads one cycle's files, so every row is that cycle's. A full
    build can read several cycles' files (a later cycle that found no table
    rebuilds from cycle 0 up), so each row takes the cycle in its file's name
    (``part-c<n>-*`` is cycle n, any other name cycle 0; common.c360_bronze_path).
    Tagging the whole rebuild with the current cycle made an operator retry,
    which deletes that cycle's rows before re-appending them, delete all of it.
    """
    if appending:
        return df_bronze.withColumn("_batch_id", lit(int(cycle)).cast("bigint"))
    if int(cycle) > 0:
        # A rebuild at a later cycle whose own files the name pattern does not
        # find would tag every row 0, and a retry would then delete nothing
        # and append the cycle twice. Refuse instead.
        own = re.compile(rf"/part-c0*{int(cycle)}-[^/]*$")
        if not any(own.search(f) for f in df_bronze.inputFiles()):
            raise SilverAbort(
                f"silver-build: rebuilding at cycle {cycle}, but no bronze file is named "
                f"part-c{int(cycle):03d}-*; cannot tag rows by cycle"
            )
    n = regexp_extract(col("_metadata.file_path"), r"/part-c(\d+)-[^/]*$", 1)
    return df_bronze.withColumn("_batch_id", when(n == "", lit(0)).otherwise(n).cast("bigint"))


def drop_if_columns_move(spark, silver_tbl, silver_df):
    """Drop an existing silver table whose columns are not *silver_df*'s, in
    order, just before a full build writes it.

    A Hive Metastore refuses a createOrReplace whose columns change type by
    position (a table a continuous run wrote, with _stream_id; a table a 1.7
    dev build wrote), and each refused attempt left its data files in the
    bucket. On Hive and Polaris the drop is the catalog entry only: no file
    is deleted, so no bucket ownership is at stake, and the old table's
    files stay in the bucket (local mode's Hadoop catalog deletes the table
    directory). A write that fails after the drop leaves no table; the next
    run finds none and builds silver without --force-rebuild. A table with
    the same columns (one 1.6 or this version wrote) keeps the atomic
    replace, so a write that fails leaves the old table.
    """
    if not table_exists(spark, silver_tbl):
        return
    try:
        existing = spark.table(silver_tbl).columns
    except Exception as e:  # noqa: BLE001 -- unreadable: replace it from scratch
        log(f"Full rebuild: cannot read {silver_tbl} columns ({one_line(e)})")
        existing = None
    if existing == silver_df.columns:
        return
    spark.sql(f"DROP TABLE IF EXISTS {silver_tbl}")
    log(
        f"Full rebuild: dropped {silver_tbl} (catalog entry only; its files stay in the "
        "bucket): its columns differ"
    )


def silver_simple(spark, source, silver_tbl, appending=False, cycle=0):
    """SIMPLE strategy: one pass over bronze. For < 100GB.

    Every row carries the ``_batch_id`` of its cycle (``tag_batch``), so a
    resubmitted cycle deletes the earlier attempt's rows before appending,
    as silver_stream.py does. Cycle 0 is the full build; cycles 1+ append.
    """
    log("Executing SIMPLE strategy...")

    df_bronze = spark.read.parquet(source)
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")
    _COUNTED["bronze_rows"] = bronze_count
    # strict: the job always exports LB_DATA_CLOCK, so a missing one is a
    # configuration error.
    anchor = resolve_data_clock(df_bronze, strict=True)

    silver_df = batch_id_last(
        apply_silver_transformations_anchored(tag_batch(df_bronze, cycle, appending), anchor)
    )
    silver_count = silver_df.count()

    log(f"Writing {silver_count:,} records to {silver_tbl}")
    write_silver(spark, silver_df, silver_tbl, appending, cycle)
    return silver_count


def silver_streaming(spark, source, silver_tbl, input_size_gb, appending=False, cycle=0):
    """STREAMING strategy: one pass, no repartition and no count of the
    output. For >= 100GB.

    The transforms are per-row column operations, so no redistribution is
    needed. ``write.distribution-mode=hash`` makes Iceberg cluster rows by
    the partition column before the write, so each task writes one
    partition: about 9K files at scale 100 (1 TB, 365 days) instead of 836K
    with ``none``, and the Hive Metastore commit no longer times out. The
    hash shuffle once filled 150Gi scratch PVCs at 5 TB and above; it has
    been run without PVC pressure up to 1 TB with 28 executors. Above that,
    set ``spark.lb.silver.distribution_mode=none``.
    """
    log("Executing STREAMING strategy (single pass)...")
    log(f"Input size: {input_size_gb:.1f} GB")

    df_bronze = spark.read.parquet(source)

    # Parquet count() reads row-group footers only (about one S3 request
    # per file), so bronze_rows costs no data scan.
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")
    _COUNTED["bronze_rows"] = bronze_count

    anchor = resolve_data_clock(df_bronze, strict=True)
    silver_df = batch_id_last(
        apply_silver_transformations_anchored(tag_batch(df_bronze, cycle, appending), anchor)
    )

    dist_mode = spark.conf.get("spark.lb.silver.distribution_mode", "hash")
    fanout = spark.conf.get("spark.lb.silver.fanout_enabled", "false")
    log(f"Writing to {silver_tbl} (single pass, distribution-mode={dist_mode}, fanout={fanout})...")
    write_silver(spark, silver_df, silver_tbl, appending, cycle)

    silver_count = rows_added_by_last_commit(spark, silver_tbl)
    log(
        f"Wrote {silver_count:,} records"
        if silver_count is not None
        else "Wrote unknown records (snapshot summary unavailable)"
    )

    return silver_count


# ============================================================
# MAIN EXECUTION
# ============================================================

catalog = env("LB_ICEBERG_CATALOG", "ice")
bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")

log("=" * 60)
log("Customer 360 Silver Build - Adaptive Transformation Pipeline")
log("=" * 60)

spark = SparkSession.builder.appName("lb-silver-build").getOrCreate()
set_utc_session(spark)

log("Creating Iceberg namespaces...")
try:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
    log(f"Created namespace {catalog}.silver")
except Exception as e:
    log(f"Namespace creation note: {str(e)}")

start_time = time.time()

silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"
log(f"Target table: {silver_tbl}")

# Cycles 2+ of a multi-cycle run append to the table (LB_SILVER_INCREMENTAL).
incremental_mode = os.environ.get("LB_SILVER_INCREMENTAL", "false").lower() == "true"
if incremental_mode:
    log("INCREMENTAL MODE: will append to existing table")

# 0 on the full build, 1..N on later cycles (unset in a single-cycle run).
_cycle = int(os.environ.get("LB_BRONZE_CYCLE", "0"))

_exists = table_exists(spark, silver_tbl)
appending = incremental_mode and _exists
# Later cycles of a multi-cycle run append only their own bronze files. A
# full build reads this run's files: every file in a single-cycle run, else
# cycle 0's and cycles 1..k's (a later cycle that found no table), as
# bronze-verify counts them, not files of later cycles an earlier run with
# more cycles left under the prefix. Profile, size and read the same path.
bronze_source = (
    c360_bronze_path(bronze_uri, True) if appending else c360_bronze_run_path(bronze_uri)
)
log(f"Bronze source: {bronze_source}")

if not appending and _exists:
    refuse_unforced_rebuild(spark, silver_tbl)

input_size_gb, skew_factor, input_size_gb_source = silver_input(spark, bronze_source)
strategy = determine_silver_strategy(spark, input_size_gb)
apply_silver_dynamic_config(spark, input_size_gb, skew_factor)

if strategy == "simple":
    silver_count = silver_simple(
        spark, bronze_source, silver_tbl, appending=appending, cycle=_cycle
    )
else:
    silver_count = silver_streaming(
        spark, bronze_source, silver_tbl, input_size_gb, appending=appending, cycle=_cycle
    )

total_time = time.time() - start_time

finish_silver_build(
    spark,
    title="Customer 360 Silver Build",
    strategy=strategy,
    silver_tbl=silver_tbl,
    silver_count=silver_count,
    bronze_rows=_COUNTED.get("bronze_rows", "unknown"),
    input_size_gb=input_size_gb,
    input_size_gb_source=input_size_gb_source,
    total_time=total_time,
    unknown_reason="snapshot summary unavailable",
)
