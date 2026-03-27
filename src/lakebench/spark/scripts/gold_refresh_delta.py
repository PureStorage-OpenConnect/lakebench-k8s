"""
Gold Refresh (Delta Lake) - Periodic re-aggregation of Silver data into Gold KPIs.

Delta Lake variant of gold_refresh.py. Uses Delta write APIs instead of
Iceberg's DataFrameWriterV2.

Uses a rate source with foreachBatch to periodically read the full Silver
Delta table, compute daily KPI aggregations, and overwrite the Gold table.
Each cycle produces a complete, consistent Gold snapshot.

This is the sustained-pipeline equivalent of gold_finalize_delta.py. Where
gold_finalize runs once as a batch job, gold_refresh re-aggregates on
a timer (default every 5 minutes) so the Gold layer stays fresh.

Environment variables (set by job.py):
    LB_ICEBERG_CATALOG   - Catalog name (e.g., "lakehouse")
    CATALOG_NAME         - same as LB_ICEBERG_CATALOG
    CHECKPOINT_LOCATION  - s3a://gold-bucket/checkpoints/gold-refresh/
    TRIGGER_INTERVAL     - e.g., "5 minutes"
"""

from __future__ import annotations

import time

from common import env, get_daily_kpi_aggregations, log, write_delta_table
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
catalog = env("LB_ICEBERG_CATALOG", "ice")
gold_uri = env("LB_GOLD_URI", "s3a://lb-gold/")
checkpoint_location = env("CHECKPOINT_LOCATION")
trigger_interval = env("TRIGGER_INTERVAL", "5 minutes")

silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"
gold_tbl = f"{catalog}.{env('LB_GOLD_TABLE', 'gold.customer_executive_dashboard')}"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("lb-gold-refresh-delta").getOrCreate()

log("=" * 60)
log("Gold Refresh (Delta) (Periodic Re-aggregation)")
log("=" * 60)
log(f"Source table: {silver_tbl}")
log(f"Target table: {gold_tbl}")
log(f"Checkpoint:   {checkpoint_location}")
log(f"Trigger:      {trigger_interval}")

# ---------------------------------------------------------------------------
# Ensure target schema exists
# ---------------------------------------------------------------------------
log("Creating Delta schema...")
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
    log(f"Created schema {catalog}.gold")
except Exception as e:
    log(f"Schema creation note: {str(e)}")

# Track refresh cycles and incremental state
_refresh_count = 0
_last_max_date = None  # Track last-seen max interaction_date for incremental reads
_incremental = env("LB_GOLD_INCREMENTAL", "false").lower() == "true"

if _incremental:
    log("Incremental gold refresh enabled -- only new/changed partitions per cycle")


def _delta_write_props() -> dict[str, str]:
    """Common Delta table properties for gold writes."""
    return {
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    }


# ---------------------------------------------------------------------------
# foreachBatch writer -- incremental or full re-aggregation each cycle
# ---------------------------------------------------------------------------
def refresh_gold(trigger_df, batch_id):
    """Re-aggregate Silver into Gold KPIs.

    The trigger_df (from rate source) is ignored -- it only drives timing.
    In incremental mode, only new Silver partitions since the last cycle are
    read and merged into Gold. In full mode, the entire Silver table is
    re-aggregated and Gold is overwritten.
    """
    global _refresh_count, _last_max_date
    _refresh_count += 1
    cycle_start = time.time()

    log(f"Refresh cycle {_refresh_count} (batch {batch_id})")

    # Read current Silver table
    try:
        silver_df = spark.table(silver_tbl)
    except Exception as e:
        log(f"Cycle {_refresh_count}: Silver table not ready yet: {e}")
        return

    # Incremental: only read partitions newer than what we last processed
    if _incremental and _last_max_date is not None:
        from pyspark.sql.functions import col

        silver_df = silver_df.filter(col("interaction_date") >= _last_max_date)
        log(f"Cycle {_refresh_count}: incremental read from {_last_max_date}")

    silver_count = silver_df.count()
    if silver_count == 0:
        log(f"Cycle {_refresh_count}: no new Silver data, skipping")
        return

    log(f"Cycle {_refresh_count}: aggregating {silver_count:,} Silver records")

    # Compute daily KPIs using shared aggregation expressions
    daily_kpis = (
        silver_df.groupBy("interaction_date")
        .agg(*get_daily_kpi_aggregations())
        .orderBy("interaction_date")
    )

    kpi_count = daily_kpis.count()
    log(f"Cycle {_refresh_count}: generated {kpi_count:,} daily KPI records")

    # Track max date for next incremental cycle
    if _incremental:
        from pyspark.sql.functions import max as max_

        max_date_row = daily_kpis.agg(max_("interaction_date").alias("max_date")).collect()[0]
        if max_date_row.max_date is not None:
            _last_max_date = max_date_row.max_date
            log(f"Cycle {_refresh_count}: updated max date to {_last_max_date}")

    # Coalesce to single file -- Gold is small (daily aggregates)
    daily_kpis_consolidated = daily_kpis.coalesce(1)

    if _incremental and _refresh_count > 1:
        # Incremental: merge new KPIs into existing Gold table.
        # For dates that appear in both old Gold and new aggregation, the new
        # aggregation wins (it's computed from the latest Silver data).
        try:
            existing_gold = spark.table(gold_tbl)

            # Keep existing Gold rows for dates NOT in the new batch
            new_dates = daily_kpis_consolidated.select("interaction_date")
            merged = (
                existing_gold.join(new_dates, on="interaction_date", how="left_anti")
                .unionByName(daily_kpis_consolidated)
                .orderBy("interaction_date")
            )
            gold_bucket = env("LB_GOLD_URI", "s3a://lb-gold/")
            opts = {"overwriteSchema": "true", "compression": "snappy"}
            opts.update(_delta_write_props())
            write_delta_table(
                spark, merged.coalesce(1), gold_tbl, gold_bucket,
                mode="overwrite", options=opts,
            )
        except Exception:
            # Gold table doesn't exist yet -- fall through to overwrite
            gold_bucket = env("LB_GOLD_URI", "s3a://lb-gold/")
            opts = {"overwriteSchema": "true", "compression": "snappy"}
            opts.update(_delta_write_props())
            write_delta_table(
                spark, daily_kpis_consolidated, gold_tbl, gold_bucket,
                mode="overwrite", options=opts,
            )
    else:
        # Full mode or first cycle: overwrite Gold table completely
        gold_bucket = env("LB_GOLD_URI", "s3a://lb-gold/")
        opts = {"overwriteSchema": "true", "compression": "snappy"}
        opts.update(_delta_write_props())
        write_delta_table(
            spark, daily_kpis_consolidated, gold_tbl, gold_bucket,
            mode="overwrite", options=opts,
        )

    # Compute data freshness: how old is the most recent Silver data
    try:
        from pyspark.sql.functions import col, current_timestamp
        from pyspark.sql.functions import max as max_

        freshness_row = silver_df.agg(
            (
                current_timestamp().cast("long")
                - max_(col("silver_processing_timestamp")).cast("long")
            ).alias("freshness_s")
        ).collect()[0]
        freshness = freshness_row.freshness_s or 0
        log(f"Cycle {_refresh_count}: data freshness {freshness:.0f}s")
    except Exception as e:
        log(f"Cycle {_refresh_count}: could not compute freshness: {e}")

    cycle_time = time.time() - cycle_start
    log(
        f"Cycle {_refresh_count}: refreshed {gold_tbl} in {cycle_time:.1f}s ({kpi_count:,} KPI records)"
    )


# ---------------------------------------------------------------------------
# Streaming query -- rate source drives periodic refresh
# ---------------------------------------------------------------------------
# The rate source emits one row per trigger interval. We use it purely
# as a timer -- the actual data comes from reading the Silver table in
# the foreachBatch function.
stream = spark.readStream.format("rate").option("rowsPerSecond", "1").load()

query = (
    stream.writeStream.foreachBatch(refresh_gold)
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime=trigger_interval)
    .start()
)

log("Streaming query started, awaiting termination...")
query.awaitTermination()

spark.stop()
