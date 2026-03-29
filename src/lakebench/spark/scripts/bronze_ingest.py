"""
Bronze Ingest - Structured Streaming ingestion from landing zone to Iceberg.

Monitors the landing zone path in S3 for new Parquet files written by datagen,
and appends rows to the managed Iceberg table `bronze_raw`.

This is the sustained-pipeline equivalent of bronze_verify.py. Where bronze_verify
reads all data in a single batch pass, bronze_ingest consumes files incrementally
as they arrive.

Environment variables (set by job.py):
    LB_BRONZE_URI        - s3a://bronze-bucket/
    BRONZE_BUCKET        - bucket name (for checkpoint path)
    CATALOG_NAME         - Iceberg catalog name (e.g., "lakehouse")
    CHECKPOINT_LOCATION  - s3a://bronze-bucket/checkpoints/bronze-ingest/
    TRIGGER_INTERVAL     - e.g., "30 seconds"
"""

import time

from common import env, log
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------
bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")
catalog_name = env("CATALOG_NAME", "lakehouse")
checkpoint_location = env("CHECKPOINT_LOCATION")
trigger_interval = env("TRIGGER_INTERVAL", "30 seconds")
max_files_per_trigger = env("MAX_FILES_PER_TRIGGER", "50")
target_file_size_bytes = env("TARGET_FILE_SIZE_BYTES", "536870912")

landing_zone = bronze_uri + "customer/interactions/"
bronze_table_path = env("LB_BRONZE_TABLE", "default.bronze_raw")
table_name = f"{catalog_name}.{bronze_table_path}"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder.appName("lb-bronze-ingest").getOrCreate()

log("=" * 60)
log("Bronze Ingest (Structured Streaming)")
log("=" * 60)
log(f"Landing zone: {landing_zone}")
log(f"Target table: {table_name}")
log(f"Checkpoint:   {checkpoint_location}")
log(f"Trigger:      {trigger_interval}")

# Set the default namespace location to S3 so Iceberg tables are created
# in S3, not the Hive Metastore default warehouse (file:/stackable/warehouse/).
# Silver and gold scripts do this for their namespaces; bronze uses "default".
bronze_warehouse = bronze_uri + "warehouse/"
log(f"Namespace location: {bronze_warehouse}")
try:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.default LOCATION '{bronze_warehouse}'")
    log(f"Created namespace {catalog_name}.default")
except Exception as e:
    log(f"Namespace creation note: {str(e)}")


# ---------------------------------------------------------------------------
# Wait for Parquet files in the landing zone, then infer schema.
# Datagen v2 writes Parquet with self-describing schemas -- we read one
# file to get the schema, then use it for the streaming reader.
# In sustained mode, datagen starts concurrently and may not have written
# any files yet when bronze-ingest starts.
# ---------------------------------------------------------------------------
_LANDING_WAIT_INTERVAL = 15  # seconds between checks
_LANDING_WAIT_MAX = 1800  # 30 minutes

_waited = 0
while True:
    try:
        sample_df = spark.read.parquet(landing_zone)
        inferred_schema = sample_df.schema
        if len(inferred_schema) > 0:
            log(f"Inferred schema with {len(inferred_schema)} columns (waited {_waited}s)")
            break
    except Exception as e:
        if _waited == 0 or _waited % 60 == 0:
            log(f"Landing zone probe error: {type(e).__name__}: {e}")
    if _waited >= _LANDING_WAIT_MAX:
        log(f"No Parquet files in {landing_zone} after {_waited}s, giving up")
        spark.stop()
        raise SystemExit(1)
    log(f"Waiting for Parquet files in landing zone ({_waited}s elapsed)...")
    time.sleep(_LANDING_WAIT_INTERVAL)
    _waited += _LANDING_WAIT_INTERVAL


# ---------------------------------------------------------------------------
# foreachBatch writer -- append micro-batch to Iceberg
# ---------------------------------------------------------------------------
_table_created = False


def write_bronze_batch(batch_df, batch_id):
    """Write a micro-batch of Parquet files to the bronze_raw Iceberg table."""
    global _table_created
    batch_start = time.time()
    count = batch_df.count()
    if count == 0:
        log(f"Batch {batch_id}: empty, skipping")
        return

    log(f"Batch {batch_id}: writing {count:,} rows to {table_name}")

    if not _table_created:
        # First batch -- create or verify the table exists
        try:
            spark.table(table_name)
            _table_created = True
        except Exception:
            # Create the table with an explicit S3 LOCATION to avoid the
            # Hive Metastore default warehouse (file:/stackable/warehouse/).
            from common import _s3_table_path
            table_location = _s3_table_path(bronze_uri, bronze_table_path)
            log(f"Batch {batch_id}: creating bronze table {table_name} at {table_location}")
            (
                batch_df.writeTo(table_name)
                .tableProperty("write.format.default", "parquet")
                .tableProperty("write.parquet.compression-codec", "snappy")
                .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
                .tableProperty("location", table_location)
                .create()
            )
            _table_created = True
            batch_time = time.time() - batch_start
            log(f"Batch {batch_id}: committed in {batch_time:.1f}s")
            return

    batch_df.writeTo(table_name).append()
    batch_time = time.time() - batch_start
    log(f"Batch {batch_id}: committed in {batch_time:.1f}s")


# ---------------------------------------------------------------------------
# Streaming query
# ---------------------------------------------------------------------------
stream = (
    spark.readStream.schema(inferred_schema)
    .option("maxFilesPerTrigger", max_files_per_trigger)
    .parquet(landing_zone)
)

query = (
    stream.writeStream.foreachBatch(write_bronze_batch)
    .option("checkpointLocation", checkpoint_location)
    .trigger(processingTime=trigger_interval)
    .start()
)

log("Streaming query started, awaiting termination...")
query.awaitTermination()

spark.stop()
