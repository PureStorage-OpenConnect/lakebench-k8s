"""
Gold Refresh - Periodic re-aggregation of Silver data into Gold KPIs.

Uses a rate source with foreachBatch to periodically read the full Silver
Iceberg table, compute daily KPI aggregations, and overwrite the Gold table.
Each cycle produces a complete, consistent Gold snapshot.

This is the continuous-pipeline equivalent of gold_finalize.py. Where
gold_finalize runs once as a batch job, gold_refresh re-aggregates on
a timer (default every 5 minutes) so the Gold layer stays fresh.

Environment variables (set by job.py):
    LB_ICEBERG_CATALOG   - Iceberg catalog name (e.g., "lakehouse")
    CATALOG_NAME         - same as LB_ICEBERG_CATALOG
    CHECKPOINT_LOCATION  - s3a://gold-bucket/checkpoints/gold-refresh/
    TRIGGER_INTERVAL     - e.g., "5 minutes"
"""

import time

from common import env, get_daily_kpi_aggregations, log
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
catalog = env("LB_ICEBERG_CATALOG", "ice")
gold_uri = env("LB_GOLD_URI", "s3a://lb-gold/")
checkpoint_location = env("CHECKPOINT_LOCATION")
trigger_interval = env("TRIGGER_INTERVAL", "5 minutes")
target_file_size_bytes = env("TARGET_FILE_SIZE_BYTES", "134217728")

silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"
gold_tbl = f"{catalog}.{env('LB_GOLD_TABLE', 'gold.customer_executive_dashboard')}"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("lb-gold-refresh").getOrCreate()

log("=" * 60)
log("Gold Refresh (Periodic Re-aggregation)")
log("=" * 60)
log(f"Source table: {silver_tbl}")
log(f"Target table: {gold_tbl}")
log(f"Checkpoint:   {checkpoint_location}")
log(f"Trigger:      {trigger_interval}")

# ---------------------------------------------------------------------------
# Ensure target namespace exists
# ---------------------------------------------------------------------------
log("Creating Iceberg namespace...")
try:
    gold_warehouse = gold_uri + "warehouse/"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.gold LOCATION '{gold_warehouse}'")
    log(f"Created namespace {catalog}.gold")
except Exception as e:
    log(f"Namespace creation note: {str(e)}")

# Track refresh cycles
_refresh_count = 0


# ---------------------------------------------------------------------------
# foreachBatch writer -- full re-aggregation each cycle
# ---------------------------------------------------------------------------
def refresh_gold(trigger_df, batch_id):
    """Re-aggregate Silver into Gold KPIs.

    The trigger_df (from rate source) is ignored -- it only drives timing.
    Each cycle reads the full Silver table and overwrites Gold.
    """
    global _refresh_count
    _refresh_count += 1
    cycle_start = time.time()

    log(f"Refresh cycle {_refresh_count} (batch {batch_id})")

    # Read current Silver table
    try:
        silver_df = spark.table(silver_tbl)
    except Exception as e:
        log(f"Cycle {_refresh_count}: Silver table not ready yet: {e}")
        return

    silver_count = silver_df.count()
    if silver_count == 0:
        log(f"Cycle {_refresh_count}: Silver table is empty, skipping")
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

    # Coalesce to single file -- Gold is small (daily aggregates)
    daily_kpis_consolidated = daily_kpis.coalesce(1)

    # Overwrite Gold table with fresh aggregation
    (
        daily_kpis_consolidated.writeTo(gold_tbl)
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
        .createOrReplace()
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
