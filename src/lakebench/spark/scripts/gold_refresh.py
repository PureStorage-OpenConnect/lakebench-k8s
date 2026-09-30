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

from common import (
    METADATA_DELETE_AFTER_COMMIT,
    METADATA_PREVIOUS_VERSIONS_MAX,
    await_stream,
    env,
    get_daily_kpi_aggregations,
    log,
    set_utc_session,
    table_exists,
)
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
set_utc_session(spark)

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
    gold_warehouse = gold_uri + "warehouse/gold.db/"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.gold LOCATION '{gold_warehouse}'")
    log(f"Created namespace {catalog}.gold")
except Exception as e:
    log(f"Namespace creation note: {str(e)}")

# Track refresh cycles and incremental state
_refresh_count = 0
_read_failures = 0  # consecutive cycles whose silver lookup errored
_MAX_READ_FAILURES = 5
_last_max_date = None  # Track last-seen max interaction_date for incremental reads
_last_silver_max_ts = None  # newest silver_processing_timestamp seen last cycle (LB-145)
_incremental = env("LB_GOLD_INCREMENTAL", "false").lower() == "true"

if _incremental:
    log("Incremental gold refresh enabled -- only new/changed partitions per cycle")


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
    global _refresh_count, _last_max_date, _read_failures, _last_silver_max_ts
    _refresh_count += 1
    cycle_start = time.time()

    log(f"Refresh cycle {_refresh_count} (batch {batch_id})")

    # Read current Silver table. Not-found means "not ready yet". Any other
    # catalog error skips this cycle (a transient metastore hiccup should not
    # cost a driver restart), but _MAX_READ_FAILURES in a row fail the stream
    # instead of leaving gold stale for the whole run.
    try:
        ready = table_exists(spark, silver_tbl)
    except Exception as e:  # noqa: BLE001
        _read_failures += 1
        log(
            f"Cycle {_refresh_count}: Silver table unreadable "
            f"({_read_failures}/{_MAX_READ_FAILURES}): {e}"
        )
        if _read_failures >= _MAX_READ_FAILURES:
            raise
        return
    _read_failures = 0
    if not ready:
        log(f"Cycle {_refresh_count}: Silver table not ready yet")
        return
    silver_df = spark.table(silver_tbl)
    silver_all = silver_df  # unfiltered, for the freshness/idle check

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
            (
                merged.coalesce(1)
                .writeTo(gold_tbl)
                .tableProperty("write.format.default", "parquet")
                .tableProperty("write.parquet.compression-codec", "snappy")
                .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
                .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
                .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
                .createOrReplace()
            )
        except Exception:
            # Gold table doesn't exist yet -- fall through to createOrReplace
            (
                daily_kpis_consolidated.writeTo(gold_tbl)
                .tableProperty("write.format.default", "parquet")
                .tableProperty("write.parquet.compression-codec", "snappy")
                .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
                .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
                .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
                .createOrReplace()
            )
    else:
        # Full mode or first cycle: overwrite Gold table completely
        (
            daily_kpis_consolidated.writeTo(gold_tbl)
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
            .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
            .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
            .createOrReplace()
        )

    # Compute data freshness: how old is the most recent Silver data.
    # A cycle whose newest silver row is the same as the previous cycle's saw
    # no new data, so its value only measures wall-clock since silver last
    # moved. It is tagged "(silver idle)". The collector drops only the
    # trailing idle run, and only when the corpus was fully ingested and
    # committed (a drained finite corpus, LB-145); an idle stretch that new
    # data later ends is a stall and keeps its staleness.
    try:
        from pyspark.sql.functions import col, current_timestamp
        from pyspark.sql.functions import max as max_

        # Whole table, not the incremental slice: late rows whose event dates
        # fall before _last_max_date still move silver and must not read idle.
        freshness_row = silver_all.agg(
            max_(col("silver_processing_timestamp")).alias("newest_ts"),
            (
                current_timestamp().cast("long")
                - max_(col("silver_processing_timestamp")).cast("long")
            ).alias("freshness_s"),
        ).collect()[0]
        freshness = freshness_row.freshness_s or 0
        idle = _last_silver_max_ts is not None and freshness_row.newest_ts == _last_silver_max_ts
        _last_silver_max_ts = freshness_row.newest_ts
        suffix = " (silver idle)" if idle else ""
        log(f"Cycle {_refresh_count}: data freshness {freshness:.0f}s{suffix}")
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
await_stream(spark, query)

spark.stop()
