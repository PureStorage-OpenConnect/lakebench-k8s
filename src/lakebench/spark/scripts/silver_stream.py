"""
Silver Stream - Structured Streaming transformation from Bronze to Silver.

Reads incrementally from the Iceberg bronze_raw table, applies the same
Silver transformations as the batch silver_build.py job, and writes to the
managed Iceberg table `customer_interactions_enriched`.

This is the continuous-pipeline equivalent of silver_build.py. Where silver_build
processes all data in a single batch pass, silver_stream processes micro-batches
as new rows arrive in bronze_raw.

Environment variables (set by job.py):
    LB_BRONZE_URI        - s3a://bronze-bucket/
    LB_SILVER_URI        - s3a://silver-bucket/
    LB_ICEBERG_CATALOG   - Iceberg catalog name (e.g., "lakehouse")
    CATALOG_NAME         - same as LB_ICEBERG_CATALOG
    CHECKPOINT_LOCATION  - s3a://silver-bucket/checkpoints/silver-stream/
    TRIGGER_INTERVAL     - e.g., "60 seconds"
"""

import time

from common import apply_silver_transformations, env, log
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
catalog = env("LB_ICEBERG_CATALOG", "ice")
silver_uri = env("LB_SILVER_URI", "s3a://lb-silver/")
checkpoint_location = env("CHECKPOINT_LOCATION")
trigger_interval = env("TRIGGER_INTERVAL", "60 seconds")
target_file_size_bytes = env("TARGET_FILE_SIZE_BYTES", "536870912")

bronze_tbl = f"{catalog}.{env('LB_BRONZE_TABLE', 'default.bronze_raw')}"
silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("lb-silver-stream").getOrCreate()

log("=" * 60)
log("Silver Stream (Structured Streaming)")
log("=" * 60)
log(f"Source table: {bronze_tbl}")
log(f"Target table: {silver_tbl}")
log(f"Checkpoint:   {checkpoint_location}")
log(f"Trigger:      {trigger_interval}")

# ---------------------------------------------------------------------------
# Ensure target namespace and table exist
# ---------------------------------------------------------------------------
log("Creating Iceberg namespace...")
try:
    silver_warehouse = silver_uri + "warehouse/"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver LOCATION '{silver_warehouse}'")
    log(f"Created namespace {catalog}.silver")
except Exception as e:
    log(f"Namespace creation note: {str(e)}")


# ---------------------------------------------------------------------------
# foreachBatch writer -- transform and append micro-batch to Silver
# ---------------------------------------------------------------------------
def write_silver_batch(batch_df, batch_id):
    """Transform and write a micro-batch from Bronze to Silver Iceberg table."""
    batch_start = time.time()
    count = batch_df.count()
    if count == 0:
        log(f"Batch {batch_id}: empty, skipping")
        return

    log(f"Batch {batch_id}: transforming {count:,} rows")

    # Apply the same Silver transformations as batch silver_build.py
    enriched = apply_silver_transformations(batch_df)

    enriched_count = enriched.count()
    log(
        f"Batch {batch_id}: {enriched_count:,} rows after transforms (filtered ~{(1 - enriched_count / count) * 100 if count > 0 else 0:.0f}%)"
    )

    # Write to Silver Iceberg table
    # First batch creates the table; subsequent batches append
    try:
        spark.table(silver_tbl)
        # Table exists -- append
        enriched.writeTo(silver_tbl).append()
    except Exception:
        # Table does not exist -- create with partitioning
        log(f"Batch {batch_id}: creating Silver table with partitioning")
        (
            enriched.writeTo(silver_tbl)
            .partitionedBy("interaction_date", "channel")
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
            .create()
        )

    batch_time = time.time() - batch_start
    log(f"Batch {batch_id}: committed to {silver_tbl} in {batch_time:.1f}s")


# ---------------------------------------------------------------------------
# Streaming query -- read from Iceberg bronze_raw
# ---------------------------------------------------------------------------
stream = spark.readStream.format("iceberg").load(bronze_tbl)

query = (
    stream.writeStream.foreachBatch(write_silver_batch)
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime=trigger_interval)
    .start()
)

log("Streaming query started, awaiting termination...")
query.awaitTermination()

spark.stop()
