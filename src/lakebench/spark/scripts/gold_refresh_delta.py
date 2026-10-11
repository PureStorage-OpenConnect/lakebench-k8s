"""
Gold Refresh (Delta Lake) - Continuous re-aggregation of Silver into Gold KPIs.

Delta Lake variant of gold_refresh.py: streams the Silver Delta table, pins
each batch's read at one silver version, recomputes the dates the new rows
touch, and writes Gold with the Delta write APIs.

This is the continuous-pipeline equivalent of gold_finalize_delta.py.

Environment variables (set by job.py):
    LB_ICEBERG_CATALOG   - Catalog name (e.g., "lakehouse")
    CATALOG_NAME         - same as LB_ICEBERG_CATALOG
    CHECKPOINT_LOCATION  - s3a://gold-bucket/checkpoints/gold-refresh/
    TRIGGER_INTERVAL     - "0 seconds" (back to back) or a timer, e.g. "5 minutes"
"""

from __future__ import annotations

from common import (
    env,
    log,
    run_c360_gold_stream,
    set_utc_session,
    write_delta_table,
)
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
catalog = env("LB_ICEBERG_CATALOG", "ice")
gold_uri = env("LB_GOLD_URI", "s3a://lb-gold/")
checkpoint_location = env("CHECKPOINT_LOCATION")
trigger_interval = env("TRIGGER_INTERVAL", "0 seconds")

silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"
gold_tbl = f"{catalog}.{env('LB_GOLD_TABLE', 'gold.customer_executive_dashboard')}"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("lb-gold-refresh-delta").getOrCreate()
set_utc_session(spark)

log("=" * 60)
log("Gold Refresh (Delta, continuous re-aggregation)")
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


def _write(df):
    """Replace gold with *df* in one commit."""
    opts = {
        "overwriteSchema": "true",
        "compression": "snappy",
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    }
    write_delta_table(spark, df.coalesce(1), gold_tbl, gold_uri, mode="overwrite", options=opts)


run_c360_gold_stream(
    spark, silver_tbl, gold_tbl, "delta", _write, checkpoint_location, trigger_interval
)

spark.stop()
