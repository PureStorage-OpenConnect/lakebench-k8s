"""
Adaptive Silver Build (Delta Lake) - Scale-aware transformation pipeline

Delta Lake variant of silver_build.py. Uses Delta write APIs instead of
Iceberg's DataFrameWriterV2.

Automatically selects the optimal processing strategy based on data size:
- SIMPLE: < 100GB, standard processing
- STREAMING: >= 100GB, single pass, rows clustered by day before the write
- SALTED: High skew (>100x) on small datasets, salt hot keys
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from common import (
    SilverAbort,
    apply_silver_transformations_anchored,
    assert_progress,
    c360_bronze_path,
    cluster_by_partition,
    env,
    files_added_by_last_commit,
    log,
    path_size_gb_strict,
    resolve_data_clock,
    sample_key_profile,
    set_utc_session,
    table_exists,
    write_delta_table,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
)
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import min as min_

# ============================================================
# STRATEGY FRAMEWORK
# ============================================================


class SilverStrategy(Enum):
    """Transform strategy for silver layer, selected based on data profile."""

    SIMPLE = "simple"
    STREAMING = "streaming"  # Was BROADCAST - single pass, no intermediate counts
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

    # 3. Sample sizing needs an approximate row count; see silver_build.py
    # for the A5 rationale for dropping the published `estimated_rows`.
    sample_estimate = int(txn_size_gb * 250_000)

    # 4. Sample: 0.1% or enough for 10M rows, whichever is smaller
    sample_fraction = min(0.001, 10_000_000 / max(sample_estimate, 1))
    sample_df = transactions.sample(sample_fraction)

    sample_stats = sample_df.agg(
        min_(to_date(col("event_timestamp"))).alias("min_date"),
        max_(to_date(col("event_timestamp"))).alias("max_date"),
    ).collect()[0]

    # Customers and skew from the sample's per-customer counts. The distinct
    # count is not scaled by 1 / sample_fraction: that treated every sampled
    # customer as unique to the sample and reported 14.7M customers at
    # scale 10, where there are 1M (LB-144).
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
    """Check for user-specified strategy override."""
    override = spark.conf.get("spark.lb.silver.strategy", None)
    if override is None:
        override = os.environ.get("LB_SILVER_STRATEGY", None)

    if override and override.lower() != "auto":
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

    SALTED is never auto-selected (retired, see the dispatch below). It was
    picked for any small dataset with skew over 100x, which the Zipf
    customer distribution always has.
    """
    # STREAMING for all datasets >= 100 GB -- single pass, one shuffle to
    # cluster rows by day before the write (cluster_silver).
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
    """Check if a catalog table exists (see common.table_exists)."""
    return table_exists(spark, table_name)


def _delta_write_props() -> dict[str, str]:
    """Common Delta table properties for silver writes."""
    return {
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    }


def rows_added_by_last_commit(spark, silver_tbl):
    """Rows the table's latest Delta commit wrote (this cycle's write).

    Commit metadata only; returns None when the metric is missing.

    A4 (silver-plan): the previous fallback ``spark.table(silver_tbl).count()``
    returned the cumulative table row count, which masked a zero-write cycle
    in incremental mode (LB-044 class). Callers now treat None as "unknown"
    and refuse to publish it as ``output_rows``.
    """
    try:
        r = spark.sql(f"DESCRIBE HISTORY {silver_tbl} LIMIT 1").collect()
        n = (r[0]["operationMetrics"] or {}).get("numOutputRows") if r else None
        if n is not None:
            return int(n)
    except Exception as e:  # noqa: BLE001
        log(f"Warning: commit metrics unavailable ({e}); output_rows will be unknown")
    return None


def cluster_silver(spark, silver_df):
    """Cluster silver rows by interaction_date before the Delta write.

    Without it every write task put a file into almost every day's partition
    (tasks x days files). spark.lb.silver.distribution_mode=none skips it,
    matching the Iceberg silver override.
    """
    dist_mode = spark.conf.get("spark.lb.silver.distribution_mode", "hash")
    log(f"Silver write distribution: {dist_mode} (clustered by interaction_date unless none)")
    return cluster_by_partition(spark, silver_df, ["interaction_date"], dist_mode)


def log_silver_files(spark, silver_tbl):
    """Log how many data files this build's commit wrote."""
    n = files_added_by_last_commit(spark, silver_tbl)
    if n is not None:
        log(f"Silver data files written by this commit: {n:,}")


def silver_simple(spark, source, silver_tbl, catalog, appending=False):
    """SIMPLE strategy: Standard shuffle joins, single pass. For < 100GB."""
    log("Executing SIMPLE strategy...")

    df_bronze = spark.read.parquet(source)
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")
    _COUNTED["bronze_rows"] = bronze_count
    anchor = resolve_data_clock(df_bronze)

    silver_df = apply_silver_transformations_anchored(df_bronze, anchor)
    silver_count = silver_df.count()
    silver_df = cluster_silver(spark, silver_df)

    log(f"Writing {silver_count:,} records to {silver_tbl}")
    silver_bucket = env("LB_SILVER_URI", "s3a://lb-silver/")
    if appending:
        log("Appending to existing table (incremental mode)")
        write_delta_table(spark, silver_df, silver_tbl, silver_bucket, mode="append")
    else:
        table_exists = _table_exists(spark, silver_tbl)
        write_mode = "overwrite" if table_exists else "append"
        opts = {"overwriteSchema": "true", "compression": "snappy"}
        opts.update(_delta_write_props())
        write_delta_table(
            spark,
            silver_df,
            silver_tbl,
            silver_bucket,
            mode=write_mode,
            partition_cols=["interaction_date"],
            options=opts,
        )

    log_silver_files(spark, silver_tbl)
    return silver_count


def silver_streaming(spark, source, silver_tbl, catalog, profile, appending=False):
    """STREAMING strategy: single pass, no intermediate counts. For >= 100GB.

    - No repartition(N) and no intermediate count(): one pass over bronze
    - One shuffle, REBALANCE by interaction_date (cluster_silver), the
      Delta counterpart of Iceberg silver's distribution-mode=hash: without
      it each task writes a file into every day it holds (tasks x days
      files). spark.lb.silver.distribution_mode=none restores the direct
      write.
    """
    log("Executing STREAMING strategy (single pass)...")
    log(f"Input size: {profile.total_size_gb:.1f} GB")

    df_bronze = spark.read.parquet(source)

    # A5 (silver-plan): count bronze rows before the transform so the metrics
    # block emits a real `bronze_rows` for STREAMING, matching SIMPLE.
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")
    _COUNTED["bronze_rows"] = bronze_count

    # LB_DATA_CLOCK when set; a one-column pass over bronze only without it.
    anchor = resolve_data_clock(df_bronze)

    # Apply transformations - all column operations, no joins
    silver_df = apply_silver_transformations_anchored(df_bronze, anchor)
    silver_df = cluster_silver(spark, silver_df)

    silver_bucket = env("LB_SILVER_URI", "s3a://lb-silver/")
    log(f"Writing to {silver_tbl} (single pass, no intermediate counts)...")
    if appending:
        log("Appending to existing table (incremental mode)")
        write_delta_table(spark, silver_df, silver_tbl, silver_bucket, mode="append")
    else:
        table_exists = _table_exists(spark, silver_tbl)
        write_mode = "overwrite" if table_exists else "append"
        opts = {"overwriteSchema": "true", "compression": "snappy"}
        opts.update(_delta_write_props())
        write_delta_table(
            spark,
            silver_df,
            silver_tbl,
            silver_bucket,
            mode=write_mode,
            partition_cols=["interaction_date"],
            options=opts,
        )

    silver_count = rows_added_by_last_commit(spark, silver_tbl)
    log(
        f"Wrote {silver_count:,} records"
        if silver_count is not None
        else "Wrote unknown records (commit metrics unavailable)"
    )
    log_silver_files(spark, silver_tbl)

    return silver_count


# ============================================================
# MAIN EXECUTION
# ============================================================

catalog = env("LB_ICEBERG_CATALOG", "ice")
bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")
silver_uri = env("LB_SILVER_URI", "s3a://lb-silver/")

log("=" * 60)
log("Customer 360 Silver Build (Delta) - Adaptive Transformation Pipeline")
log("=" * 60)

spark = SparkSession.builder.appName("lb-silver-build-delta").getOrCreate()
set_utc_session(spark)

# Check for legacy shuffle partition override
shuffle_override = os.getenv("LB_SILVER_SHUFFLE_PARTITIONS")
if shuffle_override:
    spark.conf.set("spark.sql.shuffle.partitions", shuffle_override)
    log(f"Legacy shuffle partitions override: {shuffle_override}")

# Create Delta schema
log("Creating Delta schema...")
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    log(f"Created schema {catalog}.silver")
except Exception as e:
    log(f"Schema creation note: {str(e)}")

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

# Cycles 2+ of a multi-cycle run append only their own bronze files; a full
# build reads every file. Profile, size and read the same path.
appending = incremental_mode and _table_exists(spark, silver_tbl)
bronze_source = c360_bronze_path(bronze_uri, appending)
log(f"Bronze source: {bronze_source}")

# Check for size override first (skip profiling entirely for faster startup)
size_override = get_size_override(spark)
if size_override and size_override > 0:
    log(f"Using size override: {size_override:.1f} GB (skipping profiling)")
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
    silver_count = silver_simple(spark, bronze_source, silver_tbl, catalog, appending=appending)
elif strategy == SilverStrategy.STREAMING:
    silver_count = silver_streaming(
        spark, bronze_source, silver_tbl, catalog, profile, appending=appending
    )
elif strategy == SilverStrategy.SALTED:
    # SALTED is retired, as in silver_build.py: silver-build is row-independent
    # column transforms, so salting did nothing, and it overwrote the table
    # regardless of incremental mode, wiping earlier cycles' silver.
    log("SALTED strategy is a no-op for row transforms; running SIMPLE")
    silver_count = silver_simple(spark, bronze_source, silver_tbl, catalog, appending=appending)
else:
    log(f"ERROR: Unknown strategy {strategy}")
    spark.stop()
    sys.exit(1)

total_time = time.time() - start_time

log("=" * 60)
log("Customer 360 Silver Build (Delta) COMPLETED")
log("=" * 60)
log(f"Strategy: {strategy.value}")
log(f"Records written: {silver_count if silver_count is not None else 'unknown'}")
log(f"Table: {silver_tbl}")
log("Partitioned by: interaction_date")
log(f"Duration: {total_time:.1f}s ({total_time / 60:.1f} min)")
log("=== JOB METRICS: silver-build ===")
log(f"input_size_gb: {profile.total_size_gb:.3f}")
# A5 (silver-plan): unify on `bronze_rows` across SIMPLE and STREAMING; the
# earlier `estimated_rows` emission is dropped.
log(f"bronze_rows: {_COUNTED.get('bronze_rows', 'unknown')}")
# A4 (silver-plan): output_rows is `unknown` when the commit-metrics
# fallback fired; the LB-044 gate below then refuses the run rather than
# publishing a cumulative table count as this cycle's output.
if silver_count is None:
    log("output_rows: unknown")
else:
    log(f"output_rows: {silver_count}")
log(f"elapsed_seconds: {total_time:.1f}")
log("=" * 60)
# A4 (silver-plan): see silver_build.py for the rationale.
if silver_count is None:
    spark.stop()
    raise SilverAbort(
        "silver-build: output_rows unknown (commit metrics unavailable); refusing exit-0 pass"
    )
# A1: LB-044 gate; see silver_build.py for the rationale.
assert_progress(silver_count, "silver-build")
spark.stop()
