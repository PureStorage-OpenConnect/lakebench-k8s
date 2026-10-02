"""
Adaptive Gold Finalize - Scale-aware aggregation pipeline

Automatically selects the optimal aggregation strategy based on Silver size:
- SIMPLE_AGG: < 500GB, standard single-pass aggregation
- TWO_PHASE_AGG: 500GB - 10TB, pre-aggregate then final aggregate
- INCREMENTAL: only for cycles 2+ of a multi-cycle run, which lakebench
  marks with LB_GOLD_INCREMENTAL=true; never chosen from table size or from
  a gold table that already has rows, so a repeat run does the same work
"""

from __future__ import annotations

import os
import sys
import time
from enum import Enum

from common import (
    METADATA_DELETE_AFTER_COMMIT,
    METADATA_PREVIOUS_VERSIONS_MAX,
    env,
    get_daily_kpi_aggregations,
    gold_date_coverage_problem,
    log,
    log_c360_check,
    log_job_metrics,
    set_utc_session,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
)
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import sum as sum_

# ============================================================
# STRATEGY FRAMEWORK
# ============================================================


class GoldStrategy(Enum):
    """Aggregation strategy for gold layer, selected based on silver table size."""

    SIMPLE_AGG = "simple_agg"
    TWO_PHASE_AGG = "two_phase_agg"
    INCREMENTAL = "incremental"


def get_table_size_gb(spark, table_name: str) -> float:
    """Get approximate size of an Iceberg table in GB."""
    try:
        files_df = spark.sql(f"SELECT file_size_in_bytes FROM {table_name}$files")
        total_bytes = files_df.agg(sum_("file_size_in_bytes")).collect()[0][0]
        return (total_bytes or 0) / (1024**3)
    except Exception as e:
        log(f"Warning: Could not get table size: {e}")
        return 0.0


#: Strategies a user may name in ``spark.lb.gold.strategy``; INCREMENTAL is
#: lakebench's choice for multi-cycle cycles 2+ only.
OVERRIDABLE_STRATEGIES = (GoldStrategy.SIMPLE_AGG, GoldStrategy.TWO_PHASE_AGG)


class GoldStrategyRefused(Exception):
    """A ``spark.lb.gold.strategy`` value this script will not run."""


def get_strategy_override(spark) -> GoldStrategy | None:
    """The strategy named in ``spark.lb.gold.strategy``, or None for auto.

    Raises ``GoldStrategyRefused`` for ``incremental`` (lakebench chooses it
    for multi-cycle cycles 2+ only) and for a value that names no strategy.
    """
    override = spark.conf.get("spark.lb.gold.strategy", None)
    if override is None or override.strip().lower() in ("", "auto"):
        return None
    value = override.strip().lower()
    if value == GoldStrategy.INCREMENTAL.value:
        raise GoldStrategyRefused(
            "spark.lb.gold.strategy=incremental is refused: incremental gold is chosen "
            "by lakebench for multi-cycle cycles 2+ only"
        )
    for strategy in OVERRIDABLE_STRATEGIES:
        if strategy.value == value:
            return strategy
    raise GoldStrategyRefused(
        f"spark.lb.gold.strategy={override!r} names no strategy: use auto, "
        + " or ".join(s.value for s in OVERRIDABLE_STRATEGIES)
    )


def select_gold_strategy(silver_size_gb: float) -> GoldStrategy:
    """The automatic strategy, from silver's size alone: SIMPLE_AGG below
    500 GB, else TWO_PHASE_AGG. Both rebuild gold from all of silver."""
    if silver_size_gb < 500:
        return GoldStrategy.SIMPLE_AGG
    return GoldStrategy.TWO_PHASE_AGG


def determine_gold_strategy(spark, silver_tbl: str) -> tuple[GoldStrategy, str]:
    """``(strategy, source)``, source being ``cycle``, ``override`` or
    ``auto``.

    ``cycle``: LB_GOLD_INCREMENTAL=true, which the CLI sets for cycles 2+ of
    a multi-cycle run only. ``override``: ``spark.lb.gold.strategy``.
    ``auto``: ``select_gold_strategy`` on silver's size. A refused
    override raises ``GoldStrategyRefused`` here too, though ``main()``
    checks it first, before any write.
    """
    override = get_strategy_override(spark)
    if os.environ.get("LB_GOLD_INCREMENTAL", "false").lower() == "true":
        log("INCREMENTAL MODE: multi-cycle cycle 2+ (LB_GOLD_INCREMENTAL)")
        return GoldStrategy.INCREMENTAL, "cycle"
    if override is not None:
        log(f"Using override strategy: {override.value}")
        return override, "override"

    silver_size_gb = get_table_size_gb(spark, silver_tbl)
    log(f"Silver size: {silver_size_gb:.1f} GB")
    strategy = select_gold_strategy(silver_size_gb)
    log(f"Auto-selected strategy: {strategy.value}")
    return strategy, "auto"


# ============================================================
# AGGREGATION EXPRESSIONS
# ============================================================
# get_daily_kpi_aggregations() is imported from common.py
# (shared between batch gold_finalize.py and streaming gold_refresh.py)

# ============================================================
# STRATEGY IMPLEMENTATIONS
# ============================================================


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

    log(f"Writing to: {gold_tbl}")
    (
        daily_kpis_consolidated.writeTo(gold_tbl)
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
        .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
        .tableProperty("write.target-file-size-bytes", "134217728")  # 128MB target
        .createOrReplace()
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

    log(f"Phase 2: Writing to {gold_tbl}")
    (
        daily_kpis_consolidated.writeTo(gold_tbl)
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
        .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
        .tableProperty("write.target-file-size-bytes", "134217728")  # 128MB target
        .createOrReplace()
    )

    return kpi_count


def _merge_gold(existing_gold, new_kpis, last_date):
    """Gold rows before ``last_date`` plus the recomputed rows, materialized.

    Materialized (gold is a few hundred rows) so the overwrite does not read
    the table it is replacing.
    """
    kept = existing_gold
    if last_date is not None:  # an empty gold table has no watermark
        kept = existing_gold.filter(col("interaction_date") < lit(last_date))
    return kept.unionByName(new_kpis).coalesce(1).localCheckpoint(eager=True)


def gold_incremental(spark, silver_tbl: str, gold_tbl: str) -> int:
    """INCREMENTAL strategy: recompute gold from the last gold date on.
    Multi-cycle cycles 2+ only (LB_GOLD_INCREMENTAL)."""
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

    # Recompute from the watermark date INCLUSIVE and replace those gold rows.
    # A strict > dropped any silver rows that landed on the last processed
    # date (a cycle boundary day), silently leaving that day's KPIs short.
    silver_df = spark.table(silver_tbl)
    if last_date:
        silver_df = silver_df.filter(col("interaction_date") >= last_date)

    new_count = silver_df.count()
    if new_count == 0:
        log("No new records to process")
        if existing_gold:
            return existing_gold.count()
        return 0

    log(f"Processing {new_count:,} new records")

    # Aggregate new data
    # Same columns as SIMPLE_AGG / TWO_PHASE_AGG. An extra update-time column
    # here made the append fail on cycle 2 of every multi-cycle run (schema
    # mismatch against a gold table the other strategies created).
    new_kpis = silver_df.groupBy("interaction_date").agg(*get_daily_kpi_aggregations())

    new_kpi_count = new_kpis.count()
    log(f"Generated {new_kpi_count:,} new KPI records")

    # Coalesce to minimize file count - Gold is small
    new_kpis_consolidated = new_kpis.coalesce(1)

    if existing_gold is None:
        # First run - create table
        log("Creating new Gold table...")
        (
            new_kpis_consolidated.writeTo(gold_tbl)
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
            .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
            .tableProperty("write.target-file-size-bytes", "134217728")  # 128MB target
            .create()
        )
    else:
        # One commit: gold is one small unpartitioned file of daily rows, so
        # rewrite it whole (rows before the watermark plus the recomputed
        # ones). DELETE-then-append was two commits and a failure between
        # them left gold missing days; overwrite(filter) cannot be used
        # because the filter never aligns with whole files here.
        log(f"Replacing gold rows from {last_date} on...")
        merged = _merge_gold(existing_gold, new_kpis_consolidated, last_date)
        merged.writeTo(gold_tbl).overwrite(lit(True))

    total_count = spark.table(gold_tbl).count()
    return total_count


# ============================================================
# MAIN EXECUTION
# ============================================================


def main() -> None:
    """Gold finalize. A function so a test can run it in-process; the Spark
    Operator runs this file as ``__main__``."""
    catalog = env("LB_ICEBERG_CATALOG", "ice")

    log("=" * 60)
    log("Customer 360 Gold Finalize - Adaptive Aggregation Pipeline")
    log("=" * 60)

    spark = SparkSession.builder.appName("lb-gold-finalize").getOrCreate()
    set_utc_session(spark)

    start_time = time.time()

    # A refused spark.lb.gold.strategy stops the job before it writes anything.
    try:
        get_strategy_override(spark)
    except GoldStrategyRefused as e:
        log(f"ERROR: {e}")
        spark.stop()
        sys.exit(1)

    # Create gold namespace
    log("Creating Iceberg namespace...")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.gold")
        log(f"Created namespace {catalog}.gold")
    except Exception as e:
        log(f"Namespace creation note: {str(e)}")

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

    # Determine and execute strategy (the override was checked above).
    strategy, strategy_source = determine_gold_strategy(spark, silver_tbl)

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

    # Non-degeneracy gate (invariant 3): gold must hold one KPI row per distinct
    # silver interaction_date, else the run did not produce a valid gold.
    _gold_rows = spark.table(gold_tbl).count()
    _distinct_dates = spark.table(silver_tbl).select("interaction_date").distinct().count()
    _gate = gold_date_coverage_problem(_gold_rows, _distinct_dates)
    if _gate:
        log(f"ERROR: gold non-degeneracy gate FAILED ({strategy.value}): {_gate}")
        spark.stop()
        sys.exit(1)
    log(
        f"Gold non-degeneracy gate PASSED: {_gold_rows} gold rows == {_distinct_dates} silver dates"
    )

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
    log("Customer 360 Gold Finalize COMPLETED")
    log("=" * 60)
    log(f"Strategy: {strategy.value}")
    log(f"KPI records: {kpi_count:,}")
    log(f"Table: {gold_tbl}")
    log(f"Duration: {total_time:.1f}s ({total_time / 60:.1f} min)")
    # log_job_metrics both writes the JOB METRICS block (input_rows aliases
    # estimated_rows in the collector, collector.py:2778) and pushes the stage
    # gauges to the Pushgateway, so c360 gold now reaches the live dashboard like
    # silver and AML gold already do (Gate 3 observability).
    log_job_metrics(
        "gold-finalize",
        input_size_gb=silver_size_gb,
        input_rows=silver_count,
        output_rows=kpi_count,
        elapsed_seconds=total_time,
        gold_strategy=strategy.value,
        gold_strategy_source=strategy_source,
    )
    # Expected-result facts (metrics/c360_correctness.py), after the timing above.
    log_c360_check(spark, silver_tbl, gold_tbl)
    spark.stop()


if __name__ == "__main__":
    main()
