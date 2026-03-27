"""
Adaptive Gold Finalize (Delta Lake) - Scale-aware aggregation pipeline

Delta Lake variant of gold_finalize.py. Uses Delta write APIs instead of
Iceberg's DataFrameWriterV2 and DESCRIBE DETAIL instead of $files metadata.

Automatically selects the optimal aggregation strategy based on Silver size:
- SIMPLE_AGG: < 500GB, standard single-pass aggregation
- TWO_PHASE_AGG: 500GB - 10TB, pre-aggregate then final aggregate
- INCREMENTAL: > 10TB or repeat runs, process only new data
"""

from __future__ import annotations

import os
import sys
import time
from enum import Enum

from common import env, get_daily_kpi_aggregations, log, write_delta_table
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
)
from pyspark.sql.functions import max as max_

# ============================================================
# STRATEGY FRAMEWORK
# ============================================================


class GoldStrategy(Enum):
    """Aggregation strategy for gold layer, selected based on silver table size."""

    SIMPLE_AGG = "simple_agg"
    TWO_PHASE_AGG = "two_phase_agg"
    INCREMENTAL = "incremental"


def get_table_size_gb(spark, table_name: str) -> float:
    """Get approximate size of a Delta table in GB using DESCRIBE DETAIL."""
    try:
        detail_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
        total_bytes = detail_df.select("sizeInBytes").collect()[0][0]
        return (total_bytes or 0) / (1024**3)
    except Exception as e:
        log(f"Warning: Could not get table size: {e}")
        return 0.0


def get_strategy_override(spark) -> GoldStrategy | None:
    """Check for user-specified strategy override."""
    override = spark.conf.get("spark.lb.gold.strategy", None)
    if override is None:
        override = os.environ.get("LB_GOLD_STRATEGY", None)

    if override and override.lower() != "auto":
        try:
            return GoldStrategy(override.lower())
        except ValueError:
            log(f"Warning: Invalid strategy override '{override}', using auto")
    return None


def check_gold_exists(spark, gold_tbl: str) -> bool:
    """Check if Gold table already exists with data."""
    try:
        count = spark.table(gold_tbl).count()
        return count > 0
    except Exception:
        return False


def select_gold_strategy(silver_size_gb: float, gold_exists: bool) -> GoldStrategy:
    """Select optimal Gold strategy based on Silver size."""
    if gold_exists and silver_size_gb > 1000:
        return GoldStrategy.INCREMENTAL

    if silver_size_gb < 500:
        return GoldStrategy.SIMPLE_AGG

    return GoldStrategy.TWO_PHASE_AGG


def determine_gold_strategy(spark, silver_tbl: str, gold_tbl: str) -> GoldStrategy:
    """Determine strategy with override support."""
    override = get_strategy_override(spark)
    if override:
        log(f"Using override strategy: {override.value}")
        return override

    silver_size_gb = get_table_size_gb(spark, silver_tbl)
    gold_exists = check_gold_exists(spark, gold_tbl)

    log(f"Silver size: {silver_size_gb:.1f} GB")
    log(f"Gold exists: {gold_exists}")

    strategy = select_gold_strategy(silver_size_gb, gold_exists)
    log(f"Auto-selected strategy: {strategy.value}")
    return strategy


# ============================================================
# AGGREGATION EXPRESSIONS
# ============================================================
# get_daily_kpi_aggregations() is imported from common.py
# (shared between batch gold_finalize.py and streaming gold_refresh.py)

# ============================================================
# STRATEGY IMPLEMENTATIONS
# ============================================================


def _delta_write_props() -> dict[str, str]:
    """Common Delta table properties for gold writes."""
    return {
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    }


def _table_exists(spark, table_name: str) -> bool:
    """Check if a table exists in the catalog."""
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False


def _safe_write_mode(spark, table_name: str) -> str:
    """Return 'overwrite' if table exists, 'append' otherwise.

    UCSingleCatalog (Unity) does not support REPLACE TABLE AS SELECT.
    When the table doesn't exist, use 'append' which creates it implicitly.
    """
    return "overwrite" if _table_exists(spark, table_name) else "append"


def gold_simple_agg(spark, silver_tbl: str, gold_tbl: str) -> int:
    """SIMPLE_AGG strategy: Standard single-pass aggregation. For < 500GB."""
    log("Executing SIMPLE_AGG strategy...")

    df = spark.table(silver_tbl)
    silver_count = df.count()
    log(f"Silver records: {silver_count:,}")

    daily_kpis = (
        df.groupBy("interaction_date")
        .agg(*get_daily_kpi_aggregations())
        .orderBy("interaction_date")
    )

    kpi_count = daily_kpis.count()
    log(f"Generated {kpi_count:,} daily KPI records")

    # Coalesce to single file - Gold is small (daily aggregates)
    # No partitioning needed for such a small table
    daily_kpis_consolidated = daily_kpis.coalesce(1)

    gold_bucket = env("LB_GOLD_URI", "s3a://lb-gold/")
    log(f"Writing to: {gold_tbl}")
    opts = {"overwriteSchema": "true", "compression": "snappy"}
    opts.update(_delta_write_props())
    write_delta_table(
        spark,
        daily_kpis_consolidated,
        gold_tbl,
        gold_bucket,
        mode=_safe_write_mode(spark, gold_tbl),
        options=opts,
    )

    return kpi_count


def gold_two_phase_agg(spark, silver_tbl: str, gold_tbl: str) -> int:
    """TWO_PHASE_AGG strategy: Pre-aggregate then final aggregate. For 500GB - 10TB."""
    log("Executing TWO_PHASE_AGG strategy...")

    df = spark.table(silver_tbl)

    silver_size_gb = get_table_size_gb(spark, silver_tbl)
    agg_partitions = max(200, int(silver_size_gb / 2))  # ~2GB per partition
    log(f"Using {agg_partitions} aggregation partitions")

    # Phase 1: Partial aggregation with repartition
    log("Phase 1: Partial aggregation...")
    df_repartitioned = df.repartition(agg_partitions, "interaction_date")

    # Standard aggregation on repartitioned data
    daily_kpis = (
        df_repartitioned.groupBy("interaction_date")
        .agg(*get_daily_kpi_aggregations())
        .orderBy("interaction_date")
    )

    kpi_count = daily_kpis.count()
    log(f"Generated {kpi_count:,} daily KPI records")

    # Coalesce to single file - Gold is small (daily aggregates)
    daily_kpis_consolidated = daily_kpis.coalesce(1)

    gold_bucket = env("LB_GOLD_URI", "s3a://lb-gold/")
    log(f"Phase 2: Writing to {gold_tbl}")
    opts = {"overwriteSchema": "true", "compression": "snappy"}
    opts.update(_delta_write_props())
    write_delta_table(
        spark,
        daily_kpis_consolidated,
        gold_tbl,
        gold_bucket,
        mode=_safe_write_mode(spark, gold_tbl),
        options=opts,
    )

    return kpi_count


def gold_incremental(spark, silver_tbl: str, gold_tbl: str) -> int:
    """INCREMENTAL strategy: Process only new Silver data. For > 10TB or repeat runs."""
    log("Executing INCREMENTAL strategy...")

    # Get high watermark from existing Gold table
    try:
        existing_gold = spark.table(gold_tbl)
        last_date = existing_gold.agg(max_("interaction_date")).collect()[0][0]
        log(f"Last processed date: {last_date}")
    except Exception:
        log("No existing Gold table, will process all data")
        last_date = None
        existing_gold = None

    # Read Silver, filter to new data if watermark exists
    silver_df = spark.table(silver_tbl)
    if last_date:
        silver_df = silver_df.filter(col("interaction_date") > last_date)

    new_count = silver_df.count()
    if new_count == 0:
        log("No new records to process")
        if existing_gold:
            return existing_gold.count()
        return 0

    log(f"Processing {new_count:,} new records")

    # Aggregate new data
    new_kpis = (
        silver_df.groupBy("interaction_date")
        .agg(*get_daily_kpi_aggregations())
        .withColumn("last_updated", current_timestamp())
    )

    new_kpi_count = new_kpis.count()
    log(f"Generated {new_kpi_count:,} new KPI records")

    # Coalesce to minimize file count - Gold is small
    new_kpis_consolidated = new_kpis.coalesce(1)

    gold_bucket = env("LB_GOLD_URI", "s3a://lb-gold/")
    if existing_gold is None:
        # First run - create table
        log("Creating new Gold table...")
        opts = {"overwriteSchema": "true", "compression": "snappy"}
        opts.update(_delta_write_props())
        write_delta_table(
            spark,
            new_kpis_consolidated,
            gold_tbl,
            gold_bucket,
            mode=_safe_write_mode(spark, gold_tbl),
            options=opts,
        )
    else:
        # Append new records (dates don't overlap due to filter)
        log("Appending to existing Gold table...")
        write_delta_table(
            spark,
            new_kpis_consolidated,
            gold_tbl,
            gold_bucket,
            mode="append",
        )

    total_count = spark.table(gold_tbl).count()
    return total_count


# ============================================================
# MAIN EXECUTION
# ============================================================

catalog = env("LB_ICEBERG_CATALOG", "ice")

log("=" * 60)
log("Customer 360 Gold Finalize (Delta) - Adaptive Aggregation Pipeline")
log("=" * 60)

spark = SparkSession.builder.appName("lb-gold-finalize-delta").getOrCreate()

start_time = time.time()

# Create gold schema
log("Creating Delta schema...")
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
    log(f"Created schema {catalog}.gold")
except Exception as e:
    log(f"Schema creation note: {str(e)}")

silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"
gold_tbl = f"{catalog}.{env('LB_GOLD_TABLE', 'gold.customer_executive_dashboard')}"

# Verify Silver table exists
log(f"Checking Silver table: {silver_tbl}")
try:
    silver_count = spark.table(silver_tbl).count()
    log(f"Silver table contains {silver_count:,} records")
except Exception as e:
    log(f"ERROR: Cannot read Silver table - {str(e)}")
    log("Make sure Silver job completed successfully first")
    spark.stop()
    sys.exit(1)

if silver_count == 0:
    log("ERROR: Silver table is empty - run Silver job first")
    spark.stop()
    sys.exit(1)

# Determine and execute strategy
strategy = determine_gold_strategy(spark, silver_tbl, gold_tbl)

# Incremental mode override: forces INCREMENTAL strategy regardless of
# data size.  Set by lakebench for batch cycles 2+ in multi-cycle runs.
if os.environ.get("LB_GOLD_INCREMENTAL", "false").lower() == "true":
    log("INCREMENTAL MODE: forced by LB_GOLD_INCREMENTAL env var")
    strategy = GoldStrategy.INCREMENTAL

if strategy == GoldStrategy.SIMPLE_AGG:
    kpi_count = gold_simple_agg(spark, silver_tbl, gold_tbl)
elif strategy == GoldStrategy.TWO_PHASE_AGG:
    kpi_count = gold_two_phase_agg(spark, silver_tbl, gold_tbl)
elif strategy == GoldStrategy.INCREMENTAL:
    kpi_count = gold_incremental(spark, silver_tbl, gold_tbl)
else:
    log(f"ERROR: Unknown strategy {strategy}")
    spark.stop()
    sys.exit(1)

total_time = time.time() - start_time

# Show sample output
log("Sample Gold KPIs:")
spark.table(gold_tbl).select(
    "interaction_date",
    "daily_active_customers",
    "total_daily_revenue",
    "conversions",
    "avg_engagement_score",
).orderBy("interaction_date").show(5, truncate=False)

silver_size_gb = get_table_size_gb(spark, silver_tbl)

log("=" * 60)
log("Customer 360 Gold Finalize (Delta) COMPLETED")
log("=" * 60)
log(f"Strategy: {strategy.value}")
log(f"KPI records: {kpi_count:,}")
log(f"Table: {gold_tbl}")
log(f"Duration: {total_time:.1f}s ({total_time / 60:.1f} min)")
log("=== JOB METRICS: gold-finalize ===")
log(f"input_size_gb: {silver_size_gb:.3f}")
log(f"estimated_rows: {silver_count}")
log(f"output_rows: {kpi_count}")
log(f"elapsed_seconds: {total_time:.1f}")
log("=" * 60)
spark.stop()
