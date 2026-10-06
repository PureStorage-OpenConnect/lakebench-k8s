"""
Adaptive Silver Build - Scale-aware transformation pipeline

Automatically selects the optimal processing strategy based on data size:
- SIMPLE: < 100GB, standard processing
- STREAMING: >= 100GB, single pass, hash distribution (Iceberg clusters by partition)
- SALTED: High skew (>100x) on small datasets, salt hot keys
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from common import (
    METADATA_DELETE_AFTER_COMMIT,
    METADATA_PREVIOUS_VERSIONS_MAX,
    SilverAbort,
    apply_silver_transformations_anchored,
    assert_progress,
    batch_id_last,
    c360_bronze_path,
    c360_bronze_run_path,
    ensure_column,
    env,
    log,
    one_line,
    path_size_gb_strict,
    resolve_data_clock,
    sample_key_profile,
    set_utc_session,
    table_exists,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    regexp_extract,
    to_date,
    when,
)
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import min as min_

# ============================================================
# STRATEGY FRAMEWORK
# ============================================================


class SilverStrategy(Enum):
    """Transform strategy for silver layer, selected based on data profile."""

    SIMPLE = "simple"
    STREAMING = "streaming"  # Was BROADCAST - no shuffle, direct write
    SALTED = "salted"


@dataclass
class DataProfile:
    """Bronze data characteristics used to select the silver transform strategy."""

    total_size_gb: float
    transaction_count: int
    customer_count: int
    customers_size_gb: float
    skew_factor: float
    date_range_days: int
    min_date: datetime
    max_date: datetime
    hot_keys: list[str] | None = field(default=None)


# Rows actually counted during the build (the profile only estimates them).
_COUNTED: dict[str, int] = {}


def get_path_size_gb(spark, path: str) -> float:
    """Size of a path or glob in GB (common.path_size_gb_strict).

    A6 (silver-plan): the strict variant re-raises listing errors so an S3
    outage cannot silently look like an empty bronze path.
    """
    return path_size_gb_strict(spark, path)


def profile_bronze_data(spark, txn_path: str) -> DataProfile:
    """Lightweight profiling - no shuffles, no full scans, uses filesystem metadata.

    This replaces the expensive profiling that caused OOM at 1TB+ scale.
    Key changes:
    - Use filesystem API for size (no Spark scan)
    - Estimate row count from size (no count())
    - Estimate distinct customers from the sample's frequency profile (Chao1)
    - Single aggregation pass on sample for all stats
    """
    log("Profiling Bronze data (lightweight)...")

    # txn_path: the files this build reads (common.c360_bronze_path).
    # 1. Get size from filesystem (no Spark scan)
    txn_size_gb = get_path_size_gb(spark, txn_path)
    log(f"  Size from filesystem: {txn_size_gb:.1f} GB")

    # 2. Read schema only - lazy, no data scan
    transactions = spark.read.parquet(txn_path)

    # 3. Sample sizing needs an approximate row count; a size-based estimate
    # is good enough because it only controls sample_fraction, and factor-2
    # misses do not change the sample_key_profile Chao1 estimate meaningfully.
    # A5 (silver-plan) drops the historical `estimated_rows` metric emission
    # entirely; the real bronze_rows is counted downstream (SIMPLE in
    # silver_simple, STREAMING before the transform) and unified as
    # `bronze_rows` in the JOB METRICS block.
    sample_estimate = int(txn_size_gb * 250_000)

    # 4. Sample: 0.1% or enough for 10M rows, whichever is smaller
    sample_fraction = min(0.001, 10_000_000 / max(sample_estimate, 1))
    # G3: seed the sampler so re-runs at the same LB_SEED produce identical
    # profile numbers. LB_SEED is exported by job.py:_build_env_vars alongside
    # LB_DATA_CLOCK; the default of 0 keeps the historical behaviour when the
    # env var is not set.
    sample_seed = int(os.getenv("LB_SEED", "0"))
    sample_df = transactions.sample(sample_fraction, seed=sample_seed)

    sample_stats = sample_df.agg(
        min_(to_date(col("event_timestamp"))).alias("min_date"),
        max_(to_date(col("event_timestamp"))).alias("max_date"),
    ).collect()[0]

    # Customers and skew from the sample's per-customer counts. The distinct
    # count is not scaled by 1 / sample_fraction: that treated every sampled
    # customer as unique to the sample and reported 14.7M customers at
    # scale 10, where there are 1M.
    approx_customer_count, skew_factor = sample_key_profile(
        sample_df, "customer_id", sample_estimate
    )
    min_date = sample_stats.min_date
    max_date = sample_stats.max_date
    date_range_days = (max_date - min_date).days if min_date and max_date else 1

    log(f"  Approx customers: {approx_customer_count:,}")
    log(f"  Date range: {min_date} to {max_date} ({date_range_days} days)")

    log(f"  Skew factor: {skew_factor:.1f}")

    profile = DataProfile(
        total_size_gb=txn_size_gb,
        transaction_count=0,  # A5: no longer estimated; bronze_rows is counted downstream.
        customer_count=approx_customer_count,
        customers_size_gb=0.0,
        skew_factor=skew_factor,
        date_range_days=date_range_days,
        min_date=min_date,
        max_date=max_date,
        hot_keys=[],  # Skip hot key detection for performance
    )

    log("Profile complete (lightweight, no shuffles)")
    return profile


def get_strategy_override(spark) -> SilverStrategy | None:
    """Check for user-specified strategy override.

    G1: SALTED is deferred to v1.7 (see plan Block J). Accepting it silently
    dispatched to SIMPLE while metrics claimed `Strategy: salted`, violating
    invariant 5 (published evidence identifies what produced it). Refuse
    at parse time before any log line names the strategy.
    """
    override = spark.conf.get("spark.lb.silver.strategy", None)
    if override is None:
        override = os.environ.get("LB_SILVER_STRATEGY", None)

    if override and override.lower() != "auto":
        if override.lower() == SilverStrategy.SALTED.value:
            raise SilverAbort("SALTED strategy is deferred to v1.7; see plan Block J")
        try:
            return SilverStrategy(override.lower())
        except ValueError:
            log(f"Warning: Invalid strategy override '{override}', using auto")
    return None


def get_size_override(spark) -> float | None:
    """Check for user-specified size override (skip profiling entirely)."""
    override = spark.conf.get("spark.lb.silver.size_gb", None)
    if override is None:
        override = os.environ.get("LB_SILVER_SIZE_GB", None)

    if override:
        try:
            return float(override)
        except ValueError:
            log(f"Warning: Invalid size override '{override}', using profiling")
    return None


def select_silver_strategy(profile: DataProfile) -> SilverStrategy:
    """Select optimal strategy based on data profile.

    Size-based selection takes priority over skew because silver-build only
    performs column-level transforms (casting, null handling, derived columns).
    These are skew-agnostic -- each row is processed independently regardless
    of its customer_id distribution.

    SALTED is only used for SIMPLE-range datasets (<100GB) with extreme skew,
    where the count() and hash distribution write could be affected.

    BUG-001: Previously, skew >100x was checked first, which caused SALTED
    to be selected for all Zipf-distributed data (v2 datagen). SALTED performs
    repartition() + count() + hash writes, filling 150Gi PVCs with shuffle
    spill at scale 50+.

    BUG-002: CHUNKED was previously selected at >= 5TB. Despite the comment
    claiming "no shuffle", CHUNKED does repartition() per chunk -- causing a
    full shuffle of each chunk's data. At scale 500 (5 TB, 90 date chunks),
    each chunk re-scans the entire 5 TB bronze dataset to filter down to one
    day (~55 GB), then shuffles that 55 GB. This resulted in 30+ hour runtimes.
    STREAMING (single pass, hash distribution) handles 1 TB+ correctly --
    column transforms don't require data redistribution, and Iceberg's
    write.distribution-mode=hash clusters rows by partition before writing,
    producing ~9K files at scale 100 instead of 836K with mode=none.
    """
    # STREAMING for all datasets >= 100 GB -- single pass, hash distribution.
    # Column transforms are row-independent; Iceberg handles file clustering.
    if profile.total_size_gb >= 100:
        return SilverStrategy.STREAMING

    return SilverStrategy.SIMPLE


def determine_silver_strategy(spark, profile: DataProfile) -> SilverStrategy:
    """Determine strategy with override support."""
    override = get_strategy_override(spark)
    if override:
        log(f"Using override strategy: {override.value}")
        return override

    strategy = select_silver_strategy(profile)
    log(f"Auto-selected strategy: {strategy.value}")
    return strategy


def calculate_shuffle_partitions(input_size_gb: float, target_mb: int = 256) -> int:
    """Calculate optimal shuffle partition count."""
    input_mb = input_size_gb * 1024
    partitions = int(input_mb / target_mb)
    return max(200, min(10000, partitions))


def calculate_output_partitions(
    input_size_gb: float, target_mb: int = 256, compression: float = 0.3
) -> int:
    """Calculate partition count for optimal output file sizes."""
    estimated_output_mb = input_size_gb * 1024 * compression
    partitions = int(estimated_output_mb / target_mb)
    return max(10, min(5000, partitions))


def apply_dynamic_config(spark, profile: DataProfile):
    """Apply dynamic Spark configuration based on data profile."""
    shuffle_partitions = calculate_shuffle_partitions(profile.total_size_gb)
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)
    log(f"Dynamic shuffle partitions: {shuffle_partitions}")

    # Adjust AQE settings for scale
    if profile.total_size_gb > 1000:
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64m")

    # Enable skew handling if detected
    if profile.skew_factor > 10:
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        log("Enabled AQE skew join handling")


# ============================================================
# TRANSFORMATION LOGIC
# ============================================================
# apply_silver_transformations_anchored() is imported from common.py
# (shared between batch silver_build.py and continuous silver_stream.py)

# ============================================================
# STRATEGY IMPLEMENTATIONS
# ============================================================


def _table_exists(spark, table_name: str) -> bool:
    """Check if a catalog table exists (see common.table_exists).

    Only a genuine not-found is False: any other error (a metastore timeout,
    an unreadable metadata file) raises, so it cannot send a populated table
    down the full-rebuild path.
    """
    return table_exists(spark, table_name)


def rows_added_by_last_commit(spark, silver_tbl):
    """Rows the table's latest snapshot added (this cycle's write).

    Snapshot metadata only; returns None when the summary is missing.

    A4 (silver-plan): the previous fallback ``spark.table(silver_tbl).count()``
    returned the cumulative table row count, which masked a zero-write cycle
    in incremental mode. Callers now treat None as "unknown"
    and refuse to publish it as ``output_rows``.
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


# G4: shared property set for the Iceberg C360 silver table. CREATE below sets
# these on cycle 0; cycles 1+ (append path) re-assert them before the write so
# an in-place ALTER (or an older table missing a property) does not silently
# fall back to defaults. Cheap; metadata only, no data touched. Excludes
# `write.distribution-mode` and `write.spark.fanout.enabled` because those are
# operator-overridable per Spark conf -- the helper reads them at
# call time so a scale-5TB+ deployment that set
# `spark.lb.silver.distribution_mode=none` on cycle 0 keeps that setting on
# cycle 1+ instead of being silently reverted to `hash`.
_SILVER_ICEBERG_STATIC_PROPS: tuple[tuple[str, str], ...] = (
    ("write.format.default", "parquet"),
    ("write.parquet.compression-codec", "snappy"),
    METADATA_DELETE_AFTER_COMMIT,
    METADATA_PREVIOUS_VERSIONS_MAX,
    ("write.target-file-size-bytes", "134217728"),  # 128MB target
)


def reassert_silver_iceberg_props(spark, silver_tbl) -> None:
    """G4: re-run the create-time TBLPROPERTIES before an append cycle.

    Iceberg's ``writeTo(...).append()`` does not carry TBLPROPERTIES, so a
    table that lost a property (via an ALTER, a fresh CREATE by an older
    script, or a schema evolution) would keep the wrong defaults for the
    lifetime of the deployment. This ALTER is idempotent and cheap.

    Distribution mode and fanout are honoured from ``spark.conf`` (escape
    hatch), so a cycle-1+ append cannot silently overwrite an
    operator's scale-5TB+ ``distribution_mode=none`` choice with the
    ``hash`` default.
    """
    dist_mode = spark.conf.get("spark.lb.silver.distribution_mode", "hash")
    fanout = spark.conf.get("spark.lb.silver.fanout_enabled", "false")
    props: list[tuple[str, str]] = list(_SILVER_ICEBERG_STATIC_PROPS)
    props.append(("write.distribution-mode", dist_mode))
    if str(fanout).lower() == "true":
        props.append(("write.spark.fanout.enabled", "true"))
    props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props)
    spark.sql(f"ALTER TABLE {silver_tbl} SET TBLPROPERTIES ({props_sql})")


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


def silver_simple(spark, source, silver_tbl, catalog, appending=False, cycle=0):
    """SIMPLE strategy: Standard shuffle joins, single pass. For < 100GB.

    B1: every row carries the ``_batch_id`` of its cycle (``tag_batch``) so a
    re-submission of the same cycle DELETEs the earlier attempt's rows before
    re-inserting them, matching the DELETE + APPEND pattern silver_stream.py
    already uses. Cycle 0 is the full rebuild (createOrReplace); cycles 1+
    append.
    """
    log("Executing SIMPLE strategy...")

    df_bronze = spark.read.parquet(source)
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")
    _COUNTED["bronze_rows"] = bronze_count
    # C1 (silver-plan): C360 silver mains use strict=True. C2 always exports
    # LB_DATA_CLOCK in the silver env bundle (with a today fallback), so a
    # missing env here is a plumbing break, not a legitimate greenfield.
    anchor = resolve_data_clock(df_bronze, strict=True)

    silver_df = batch_id_last(
        apply_silver_transformations_anchored(tag_batch(df_bronze, cycle, appending), anchor)
    )
    silver_count = silver_df.count()

    log(f"Writing {silver_count:,} records to {silver_tbl}")
    if appending:
        log(f"Appending to existing table (incremental mode, cycle={cycle})")
        # B1: DELETE this cycle's rows first so a re-submission of the same
        # cycle is idempotent. First attempt: DELETE matches nothing.
        ensure_column(spark, silver_tbl, "_batch_id", "BIGINT")
        spark.sql(f"DELETE FROM {silver_tbl} WHERE _batch_id = {int(cycle)}")
        # G4: re-assert table properties before the append so a stale table
        # cannot silently degrade the write (invariant 5).
        reassert_silver_iceberg_props(spark, silver_tbl)
        silver_df.writeTo(silver_tbl).append()
    else:
        drop_if_columns_move(spark, silver_tbl, silver_df)
        (
            silver_df.writeTo(silver_tbl)
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
            .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
            .tableProperty("write.target-file-size-bytes", "134217728")  # 128MB target
            .tableProperty("write.distribution-mode", "hash")
            .partitionedBy("interaction_date")
            .createOrReplace()
        )

    return silver_count


def silver_streaming(spark, source, silver_tbl, catalog, profile, appending=False, cycle=0):
    """STREAMING strategy: Direct write, single pass. For >= 100GB.

    Key insight: Column transformations don't require data redistribution.
    - No repartition() -- avoid explicit shuffle that caused OOM at 1TB+
    - No intermediate count() -- avoid extra data passes
    - distribution-mode=hash: Iceberg clusters rows by partition column
      before writing, so each task writes to one partition. Produces ~N
      files (N = partition count) instead of tasks x partitions files.
      At scale 100 (1TB, 365 date partitions): ~9K files vs 836K with
      mode=none. The Hive Metastore commit drops from timeout to 340ms.

    BUG-003 history: mode was changed from hash to none in v1.0 because
    hash shuffle filled 150Gi scratch PVCs at 5TB+. Testing at 1TB with
    28 executors x 150Gi scratch shows no PVC pressure. The none->hash
    revert is safe up to at least 1TB. For 5TB+ scales, override via
    spark.lb.silver.distribution_mode=none in spark.conf.
    """
    log("Executing STREAMING strategy (single pass)...")
    log(f"Input size: {profile.total_size_gb:.1f} GB")

    df_bronze = spark.read.parquet(source)

    # A5 (silver-plan): count bronze rows before the transform so the metrics
    # block emits a real `bronze_rows` for STREAMING, matching SIMPLE. Parquet
    # .count() uses per-row-group footers -- roughly one S3 HEAD per file, no
    # data scan.
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")
    _COUNTED["bronze_rows"] = bronze_count

    # C1: strict=True; LB_DATA_CLOCK is always exported by C2's env builder.
    anchor = resolve_data_clock(df_bronze, strict=True)

    # Apply transformations - all column operations, no joins
    silver_df = batch_id_last(
        apply_silver_transformations_anchored(tag_batch(df_bronze, cycle, appending), anchor)
    )

    # Distribution-mode is overridable via Spark conf for scale
    # testing. Default changed from "none" to "hash" -- see docstring.
    dist_mode = spark.conf.get("spark.lb.silver.distribution_mode", "hash")
    fanout = spark.conf.get("spark.lb.silver.fanout_enabled", "false")
    log(f"Writing to {silver_tbl} (single pass, distribution-mode={dist_mode}, fanout={fanout})...")

    if appending:
        log(f"Appending to existing table (incremental mode, cycle={cycle})")
        # B1: DELETE this cycle's rows first so a re-submission of the same
        # cycle is idempotent (same pattern as SIMPLE and silver_stream).
        ensure_column(spark, silver_tbl, "_batch_id", "BIGINT")
        spark.sql(f"DELETE FROM {silver_tbl} WHERE _batch_id = {int(cycle)}")
        # G4: re-assert table properties before the append. STREAMING is only
        # picked when incremental is fresh in practice, but the append path
        # still needs the same guarantee as SIMPLE.
        reassert_silver_iceberg_props(spark, silver_tbl)
        silver_df.writeTo(silver_tbl).append()
    else:
        drop_if_columns_move(spark, silver_tbl, silver_df)
        writer = (
            silver_df.writeTo(silver_tbl)
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
            .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
            .tableProperty("write.target-file-size-bytes", "134217728")  # 128MB target
            .tableProperty("write.distribution-mode", dist_mode)
            .partitionedBy("interaction_date")
        )
        if fanout.lower() == "true":
            writer = writer.tableProperty("write.spark.fanout.enabled", "true")
        writer.createOrReplace()

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
silver_uri = env("LB_SILVER_URI", "s3a://lb-silver/")

log("=" * 60)
log("Customer 360 Silver Build - Adaptive Transformation Pipeline")
log("=" * 60)

spark = SparkSession.builder.appName("lb-silver-build").getOrCreate()
set_utc_session(spark)

# Check for legacy shuffle partition override
shuffle_override = os.getenv("LB_SILVER_SHUFFLE_PARTITIONS")
if shuffle_override:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_override)
    log(f"Legacy shuffle partitions override: {shuffle_override}")

# Create Iceberg namespaces
log("Creating Iceberg namespaces...")
try:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
    log(f"Created namespace {catalog}.silver")
except Exception as e:
    log(f"Namespace creation note: {str(e)}")

# Profile data and select strategy
import time  # noqa: E402

start_time = time.time()

silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"
log(f"Target table: {silver_tbl}")

# Incremental mode: append to existing table instead of overwriting.
# Controlled by LB_SILVER_INCREMENTAL env var set by lakebench for
# batch cycles 2+ in multi-cycle runs.
incremental_mode = os.environ.get("LB_SILVER_INCREMENTAL", "false").lower() == "true"
if incremental_mode:
    log("INCREMENTAL MODE: will append to existing table")

# B1: cycle number is 0 on the full rebuild and 1..N on subsequent appends.
# LB_BRONZE_CYCLE is 0-indexed from cli/_run.py; it is unset in a single-cycle
# run. A repeated submission of the same cycle is idempotent because the
# writer DELETEs on the _batch_id key first.
_cycle = int(os.environ.get("LB_BRONZE_CYCLE", "0"))

appending = incremental_mode and _table_exists(spark, silver_tbl)
# Later cycles of a multi-cycle run append only their own bronze files. A
# full build reads this run's files: every file in a single-cycle run, else
# cycle 0's and cycles 1..k's (a later cycle that found no table), as
# bronze-verify counts them, not files of later cycles an earlier run with
# more cycles left under the prefix. Profile, size and read the same path.
bronze_source = (
    c360_bronze_path(bronze_uri, True) if appending else c360_bronze_run_path(bronze_uri)
)
log(f"Bronze source: {bronze_source}")

# B1 full-rebuild epoch guard: cycle 0 with an already-populated silver
# table is an unintended rebuild that would drop rows this deployment has
# already written. --force-rebuild (LB_FORCE_REBUILD=1) opts in explicitly,
# and job.py bumps LB_REBUILD_EPOCH before submitting so downstream idempotency
# keys move to a new namespace. Without it, refuse.
_force_rebuild = os.environ.get("LB_FORCE_REBUILD", "0") == "1"
if not appending and _table_exists(spark, silver_tbl):
    try:
        _has_rows = spark.table(silver_tbl).limit(1).count() > 0
    except Exception as e:  # noqa: BLE001
        # A read that fails says nothing about the rows: counting it as
        # empty rebuilt a populated table without --force-rebuild.
        if not _force_rebuild:
            raise SilverAbort(
                f"silver-build: cannot tell whether {silver_tbl} holds rows ({one_line(e)}); "
                "refusing a full rebuild without --force-rebuild"
            ) from e
        _has_rows = True
    if _has_rows and not _force_rebuild:
        raise SilverAbort(
            f"silver-build: refusing full rebuild of populated {silver_tbl}; "
            "re-run with --force-rebuild to opt in"
        )

# Check for size override first (skip profiling entirely for faster startup)
size_override = get_size_override(spark)
if size_override and size_override > 0:
    log(f"Using size override: {size_override:.1f} GB (skipping profiling)")
    # G2: label the emitted input_size_gb as an operator override so
    # downstream reports never present it as a measured value (invariant 5).
    input_size_gb_source = "operator_override"
    # Create minimal profile with overridden size
    profile = DataProfile(
        total_size_gb=size_override,
        transaction_count=int(size_override * 250_000),  # ~250K rows per GB
        customer_count=100_000,  # Conservative estimate
        customers_size_gb=0.0,
        skew_factor=1.0,
        date_range_days=30,
        min_date=datetime.now() - timedelta(days=30),
        max_date=datetime.now(),
        hot_keys=[],
    )
else:
    input_size_gb_source = "filesystem"
    profile = profile_bronze_data(spark, bronze_source)

# A5/A6 (silver-plan): the size comes from path_size_gb_strict, which
# raises on listing failures rather than returning 0.0 on error; a zero size
# now really means empty bronze.
if profile.total_size_gb == 0 and size_override is None:
    log("ERROR: Bronze dataset is empty - run Bronze job first")
    spark.stop()
    sys.exit(1)

strategy = determine_silver_strategy(spark, profile)
apply_dynamic_config(spark, profile)


# Execute selected strategy
if strategy == SilverStrategy.SIMPLE:
    silver_count = silver_simple(
        spark, bronze_source, silver_tbl, catalog, appending=appending, cycle=_cycle
    )
elif strategy == SilverStrategy.STREAMING:
    silver_count = silver_streaming(
        spark, bronze_source, silver_tbl, catalog, profile, appending=appending, cycle=_cycle
    )
elif strategy == SilverStrategy.SALTED:
    # G1: SALTED is deferred to v1.7 (plan Block J). `get_strategy_override`
    # refuses the override at parse time; this branch is a defense-in-depth
    # trap if any future auto-selector reaches SALTED.
    raise SilverAbort("SALTED strategy is deferred to v1.7; see plan Block J")
else:
    log(f"ERROR: Unknown strategy {strategy}")
    spark.stop()
    sys.exit(1)

total_time = time.time() - start_time

log("=" * 60)
log("Customer 360 Silver Build COMPLETED")
log("=" * 60)
log(f"Strategy: {strategy.value}")
log(f"Records written: {silver_count if silver_count is not None else 'unknown'}")
log(f"Table: {silver_tbl}")
log("Partitioned by: interaction_date")
log(f"Duration: {total_time:.1f}s ({total_time / 60:.1f} min)")
log("=== JOB METRICS: silver-build ===")
log(f"input_size_gb: {profile.total_size_gb:.3f}")
# G2: source label so a downstream reader can tell an operator-asserted
# size from a filesystem-measured one (invariant 5).
log(f"input_size_gb_source: {input_size_gb_source}")
# C2 (silver-plan): label which rung of the resolution ladder produced
# LB_DATA_CLOCK, so metrics.json records `datagen_timestamp_end`,
# `bronze_data_clock`, `datagen_timestamp_start` or `fallback_default`
# for every silver run.
log(f"data_clock_source: {env('LB_DATA_CLOCK_SOURCE', 'unknown')}")
# A5 (silver-plan): unify on `bronze_rows` across SIMPLE and STREAMING; the
# earlier `estimated_rows` emission is dropped.
log(f"bronze_rows: {_COUNTED.get('bronze_rows', 'unknown')}")
# A4 (silver-plan): output_rows is `unknown` when the snapshot fallback
# fired; the gate below then refuses the run rather than publishing
# a cumulative table count as this cycle's output.
if silver_count is None:
    log("output_rows: unknown")
else:
    log(f"output_rows: {silver_count}")
log(f"elapsed_seconds: {total_time:.1f}")
log("=" * 60)
# A4 (silver-plan): an unknown output_rows is a real silent-corruption
# surface (a zero-write cycle was masked by cumulative counts), so the run
# must fail rather than exit 0 with `output_rows: unknown`.
if silver_count is None:
    spark.stop()
    raise SilverAbort(
        "silver-build: output_rows unknown (snapshot summary unavailable); refusing exit-0 pass"
    )
# A1 gate. A zero-row silver run refuses to exit 0 so the K8s Job
# reports failure and the collector records it. Runs after metrics emission
# so a failing gate still leaves the metrics block on stdout.
assert_progress(silver_count, "silver-build")
spark.stop()
