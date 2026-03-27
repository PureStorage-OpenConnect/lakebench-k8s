"""
Adaptive Silver Build (Delta Lake) - Scale-aware transformation pipeline

Delta Lake variant of silver_build.py. Uses Delta write APIs instead of
Iceberg's DataFrameWriterV2.

Automatically selects the optimal processing strategy based on data size:
- SIMPLE: < 100GB, standard processing
- STREAMING: >= 100GB, direct write, no shuffle (column transforms only)
- SALTED: High skew (>100x) on small datasets, salt hot keys
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from common import apply_silver_transformations, env, log, write_delta_table
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    concat_ws,
    floor,
    lit,
    rand,
    to_date,
    udf,
    when,
)
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import min as min_
from pyspark.sql.types import BooleanType

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


def get_path_size_gb(spark, path: str) -> float:
    """Get size of a path in GB using Hadoop FileSystem API."""
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        uri = sc._jvm.java.net.URI(path)
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)

        if fs.exists(hadoop_path):
            status = fs.getContentSummary(hadoop_path)
            return status.getLength() / (1024**3)
        return 0.0
    except Exception as e:
        log(f"Warning: Could not get path size for {path}: {e}")
        return 0.0


def profile_bronze_data(spark, bronze_path: str) -> DataProfile:
    """Lightweight profiling - no shuffles, no full scans, uses filesystem metadata.

    This replaces the expensive profiling that caused OOM at 1TB+ scale.
    Key changes:
    - Use filesystem API for size (no Spark scan)
    - Estimate row count from size (no count())
    - Use approx_count_distinct on sample (no distinct().count() shuffle)
    - Single aggregation pass on sample for all stats
    """
    log("Profiling Bronze data (lightweight)...")

    txn_path = bronze_path + "customer/interactions/"

    # 1. Get size from filesystem (no Spark scan)
    txn_size_gb = get_path_size_gb(spark, txn_path)
    log(f"  Size from filesystem: {txn_size_gb:.1f} GB")

    # 2. Read schema only - lazy, no data scan
    transactions = spark.read.parquet(txn_path)

    # 3. Estimate row count from size (avoid full count)
    # ~4KB per row based on schema analysis of customer/interactions data
    estimated_row_count = int(txn_size_gb * 1024 * 1024 * 1024 / 4096)
    log(f"  Estimated rows: {estimated_row_count:,}")

    # 4. Use approx_count_distinct on SAMPLE (no shuffle, no full scan)
    # Sample fraction: 0.1% or enough for 10M rows, whichever is smaller
    sample_fraction = min(0.001, 10_000_000 / max(estimated_row_count, 1))
    sample_df = transactions.sample(sample_fraction)

    # Single aggregation pass for all stats
    sample_stats = sample_df.agg(
        approx_count_distinct("customer_id").alias("approx_customers"),
        min_(to_date(col("event_timestamp"))).alias("min_date"),
        max_(to_date(col("event_timestamp"))).alias("max_date"),
    ).collect()[0]

    # Scale up approximate customer count
    approx_customer_count = int(sample_stats.approx_customers / sample_fraction)
    min_date = sample_stats.min_date
    max_date = sample_stats.max_date
    date_range_days = (max_date - min_date).days if min_date and max_date else 1

    log(f"  Approx customers: {approx_customer_count:,}")
    log(f"  Date range: {min_date} to {max_date} ({date_range_days} days)")

    # 5. Skew detection from sample (small local shuffle, not full data)
    key_stats = (
        sample_df.groupBy("customer_id")
        .count()
        .agg(max_("count").alias("max_count"), avg("count").alias("avg_count"))
        .collect()[0]
    )
    max_count = key_stats.max_count or 1
    avg_count = key_stats.avg_count or 1
    skew_factor = max_count / max(avg_count, 1)
    log(f"  Skew factor: {skew_factor:.1f}")

    profile = DataProfile(
        total_size_gb=txn_size_gb,
        transaction_count=estimated_row_count,  # Estimated, not counted
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

    SALTED is only used for SIMPLE-range datasets (<100GB) with extreme skew,
    where the count() and hash distribution write could be affected.
    """
    # STREAMING for all datasets >= 100 GB -- single pass, no shuffle.
    # Column transforms are row-independent; Delta handles file layout.
    if profile.total_size_gb >= 100:
        return SilverStrategy.STREAMING

    # Small datasets: check skew for SALTED vs SIMPLE
    if profile.skew_factor > 100:
        return SilverStrategy.SALTED

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
# apply_silver_transformations() is imported from common.py
# (shared between batch silver_build.py and streaming silver_stream.py)

# ============================================================
# STRATEGY IMPLEMENTATIONS
# ============================================================


def _table_exists(spark, table_name: str) -> bool:
    """Check if a Delta table exists."""
    try:
        return DeltaTable.isDeltaTable(spark, table_name)
    except Exception:
        # isDeltaTable may not work with catalog-qualified names;
        # fall back to trying to read the table
        try:
            spark.table(table_name)
            return True
        except Exception:
            return False


def _delta_write_props() -> dict[str, str]:
    """Common Delta table properties for silver writes."""
    return {
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    }


def silver_simple(spark, bronze_uri, silver_tbl, catalog, incremental=False):
    """SIMPLE strategy: Standard shuffle joins, single pass. For < 100GB."""
    log("Executing SIMPLE strategy...")

    df_bronze = spark.read.parquet(bronze_uri + "customer/interactions/")
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")

    silver_df = apply_silver_transformations(df_bronze)
    silver_count = silver_df.count()

    log(f"Writing {silver_count:,} records to {silver_tbl}")
    silver_bucket = env("LB_SILVER_URI", "s3a://lb-silver/")
    if incremental and _table_exists(spark, silver_tbl):
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

    return silver_count


def silver_streaming(spark, bronze_uri, silver_tbl, catalog, profile, incremental=False):
    """STREAMING strategy: Direct write, no shuffle, single pass. For >= 100GB.

    Key insight: Column transformations don't require data redistribution.
    - No repartition() - avoid shuffle that caused OOM at 1TB+
    - No intermediate count() - avoid extra data passes
    - overwriteSchema=true: Delta equivalent of distribution-mode=none
      (no extra sort/shuffle imposed by the writer)
    - Let AQE handle partition coalescing, Delta handle file sizing
    """
    log("Executing STREAMING strategy (no shuffle, single pass)...")
    log(
        f"Input size: {profile.total_size_gb:.1f} GB (estimated {profile.transaction_count:,} rows)"
    )

    df_bronze = spark.read.parquet(bronze_uri + "customer/interactions/")

    # Apply transformations - all column operations, no joins
    silver_df = apply_silver_transformations(df_bronze)

    silver_bucket = env("LB_SILVER_URI", "s3a://lb-silver/")
    log(f"Writing to {silver_tbl} (single pass, no intermediate counts)...")
    if incremental and _table_exists(spark, silver_tbl):
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

    silver_count = spark.table(silver_tbl).count()
    log(f"Wrote {silver_count:,} records")

    return silver_count


def silver_salted(spark, bronze_uri, silver_tbl, catalog, profile):
    """SALTED strategy: Salt hot keys for skewed data. For skew > 100x."""
    log("Executing SALTED strategy...")
    log(f"Hot keys detected: {len(profile.hot_keys)}")

    SALT_BUCKETS = 10

    df_bronze = spark.read.parquet(bronze_uri + "customer/interactions/")
    bronze_count = df_bronze.count()
    log(f"Bronze records: {bronze_count:,}")

    # Broadcast hot keys for UDF
    hot_keys_set = set(profile.hot_keys)
    hot_keys_bc = spark.sparkContext.broadcast(hot_keys_set)

    @udf(BooleanType())
    def is_hot_key(customer_id):
        return customer_id in hot_keys_bc.value

    # Add salt to hot customer IDs
    df_salted = df_bronze.withColumn(
        "salt",
        when(is_hot_key(col("customer_id")), floor(rand() * SALT_BUCKETS).cast("string")).otherwise(
            lit("0")
        ),
    ).withColumn("salted_customer_id", concat_ws("_", col("customer_id"), col("salt")))

    # Apply transformations
    silver_df = apply_silver_transformations(df_salted).drop("salt", "salted_customer_id")

    # Repartition to balance load
    output_partitions = calculate_output_partitions(profile.total_size_gb)
    silver_df = silver_df.repartition(output_partitions, "customer_id")

    silver_count = silver_df.count()

    log(f"Writing {silver_count:,} records to {silver_tbl}")
    silver_bucket = env("LB_SILVER_URI", "s3a://lb-silver/")
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
    profile = profile_bronze_data(spark, bronze_uri)

if profile.transaction_count == 0:
    log("ERROR: Bronze dataset is empty - run Bronze job first")
    spark.stop()
    sys.exit(1)

strategy = determine_silver_strategy(spark, profile)
apply_dynamic_config(spark, profile)

silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"
log(f"Target table: {silver_tbl}")

# Incremental mode: append to existing table instead of overwriting.
# Controlled by LB_SILVER_INCREMENTAL env var set by lakebench for
# batch cycles 2+ in multi-cycle runs.
incremental_mode = os.environ.get("LB_SILVER_INCREMENTAL", "false").lower() == "true"
if incremental_mode:
    log("INCREMENTAL MODE: will append to existing table")

# Execute selected strategy
if strategy == SilverStrategy.SIMPLE:
    silver_count = silver_simple(
        spark, bronze_uri, silver_tbl, catalog, incremental=incremental_mode
    )
elif strategy == SilverStrategy.STREAMING:
    silver_count = silver_streaming(
        spark, bronze_uri, silver_tbl, catalog, profile, incremental=incremental_mode
    )
elif strategy == SilverStrategy.SALTED:
    silver_count = silver_salted(spark, bronze_uri, silver_tbl, catalog, profile)
else:
    log(f"ERROR: Unknown strategy {strategy}")
    spark.stop()
    sys.exit(1)

total_time = time.time() - start_time

log("=" * 60)
log("Customer 360 Silver Build (Delta) COMPLETED")
log("=" * 60)
log(f"Strategy: {strategy.value}")
log(f"Records written: {silver_count:,}")
log(f"Table: {silver_tbl}")
log("Partitioned by: interaction_date")
log(f"Duration: {total_time:.1f}s ({total_time / 60:.1f} min)")
log("=== JOB METRICS: silver-build ===")
log(f"input_size_gb: {profile.total_size_gb:.3f}")
log(f"estimated_rows: {profile.transaction_count}")
log(f"output_rows: {silver_count}")
log(f"elapsed_seconds: {total_time:.1f}")
log("=" * 60)
spark.stop()
