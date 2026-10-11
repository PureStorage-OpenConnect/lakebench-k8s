"""
Gold Refresh - Continuous re-aggregation of Silver data into Gold KPIs.

Streams the Silver Iceberg table: each micro-batch is the silver commits
since gold's position, which the stream's checkpoint keeps across restarts.
For each batch gold pins silver at one snapshot, recomputes the daily KPIs of
every date the new rows touch from all of that snapshot's rows on them, and
replaces those dates in Gold. A replayed batch recomputes the same dates, so
a restart never double counts. The next batch starts as soon as silver has
committed more (TRIGGER_INTERVAL "0 seconds"); a set interval is a timer.

This is the continuous-pipeline equivalent of gold_finalize.py.

Environment variables (set by job.py):
    LB_ICEBERG_CATALOG   - Iceberg catalog name (e.g., "lakehouse")
    CATALOG_NAME         - same as LB_ICEBERG_CATALOG
    CHECKPOINT_LOCATION  - s3a://gold-bucket/checkpoints/gold-refresh/
    TRIGGER_INTERVAL     - "0 seconds" (back to back) or a timer, e.g. "5 minutes"
"""

from common import (
    METADATA_DELETE_AFTER_COMMIT,
    METADATA_PREVIOUS_VERSIONS_MAX,
    env,
    log,
    run_c360_gold_stream,
    set_utc_session,
)
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
catalog = env("LB_ICEBERG_CATALOG", "ice")
gold_uri = env("LB_GOLD_URI", "s3a://lb-gold/")
checkpoint_location = env("CHECKPOINT_LOCATION")
trigger_interval = env("TRIGGER_INTERVAL", "0 seconds")
target_file_size_bytes = env("TARGET_FILE_SIZE_BYTES", "134217728")

silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"
gold_tbl = f"{catalog}.{env('LB_GOLD_TABLE', 'gold.customer_executive_dashboard')}"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("lb-gold-refresh").getOrCreate()
set_utc_session(spark)

log("=" * 60)
log("Gold Refresh (continuous re-aggregation)")
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


def _write(df):
    """Replace gold with *df* in one commit (gold is small: one file)."""
    (
        df.coalesce(1)
        .writeTo(gold_tbl)
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
        .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
        .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
        .createOrReplace()
    )


run_c360_gold_stream(
    spark, silver_tbl, gold_tbl, "iceberg", _write, checkpoint_location, trigger_interval
)

spark.stop()
