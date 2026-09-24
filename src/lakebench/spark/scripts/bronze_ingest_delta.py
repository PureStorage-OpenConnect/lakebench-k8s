"""
Bronze Ingest (Delta Lake) - Structured Streaming ingestion from landing zone to Delta.

Monitors the landing zone path in S3 for new Parquet files written by datagen,
and appends rows to the managed Delta table `bronze_raw`.

This is the continuous-pipeline equivalent of bronze_verify.py for Delta Lake.
Where bronze_verify reads all data in a single batch pass, bronze_ingest
consumes files incrementally as they arrive.

Replay idempotency (E3): each micro-batch is one Delta commit written with
Delta's idempotent-write options (txnAppId = stream + query id, txnVersion =
batch id), so a batch replayed after a driver restart commits nothing the
second time instead of appending a duplicate copy to bronze_raw.

Environment variables (set by job.py):
    LB_BRONZE_URI        - s3a://bronze-bucket/
    BRONZE_BUCKET        - bucket name (for checkpoint path)
    CATALOG_NAME         - catalog name (e.g., "lakehouse")
    CHECKPOINT_LOCATION  - s3a://bronze-bucket/checkpoints/bronze-ingest/
    TRIGGER_INTERVAL     - e.g., "30 seconds"
"""

from __future__ import annotations

import time

from common import (
    await_stream,
    delta_idempotent_options,
    env,
    log,
    set_utc_session,
    table_exists,
    write_delta_table,
)
from pyspark.sql import SparkSession

_LANDING_WAIT_INTERVAL = 15  # seconds between checks
_LANDING_WAIT_MAX = 1800  # 30 minutes


def write_bronze_batch(batch_df, batch_id, table_name, bronze_bucket):
    """Append one micro-batch to the bronze Delta table exactly once.

    Returns the rows in the batch (0 when empty). A replay is skipped by
    Delta itself inside the write, so it is logged like any other batch;
    a restarted driver is a new pod, and its log is all the collector reads.
    """
    spark = batch_df.sparkSession
    batch_start = time.time()
    count = batch_df.count()
    if count == 0:
        log(f"Batch {batch_id}: empty, skipping")
        return 0

    log(f"Batch {batch_id}: writing {count:,} rows to {table_name}")
    txn = delta_idempotent_options(spark, "lb-bronze-ingest", batch_id)
    if table_exists(spark, table_name):
        write_delta_table(spark, batch_df, table_name, bronze_bucket, mode="append", options=txn)
    else:
        log(f"Batch {batch_id}: creating bronze table {table_name}")
        write_delta_table(
            spark,
            batch_df,
            table_name,
            bronze_bucket,
            mode="overwrite",
            options={
                "overwriteSchema": "true",
                "delta.logRetentionDuration": "interval 30 days",
                **txn,
            },
        )
    batch_time = time.time() - batch_start
    log(f"Batch {batch_id}: committed in {batch_time:.1f}s")
    return count


def main() -> None:
    bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")
    catalog_name = env("CATALOG_NAME", "lakehouse")
    checkpoint_location = env("CHECKPOINT_LOCATION")
    trigger_interval = env("TRIGGER_INTERVAL", "30 seconds")
    max_files_per_trigger = env("MAX_FILES_PER_TRIGGER", "50")

    landing_zone = bronze_uri + "customer/interactions/"
    bronze_table_path = env("LB_BRONZE_TABLE", "default.bronze_raw")
    table_name = f"{catalog_name}.{bronze_table_path}"

    spark = SparkSession.builder.appName("lb-bronze-ingest-delta").getOrCreate()
    set_utc_session(spark)

    log("=" * 60)
    log("Bronze Ingest - Delta Lake (Structured Streaming)")
    log("=" * 60)
    log(f"Landing zone: {landing_zone}")
    log(f"Target table: {table_name}")
    log(f"Checkpoint:   {checkpoint_location}")
    log(f"Trigger:      {trigger_interval}")

    # Wait for Parquet files in the landing zone, then infer schema. Datagen
    # writes self-describing Parquet; in continuous mode it starts
    # concurrently and may not have written any files yet.
    waited = 0
    while True:
        try:
            inferred_schema = spark.read.parquet(landing_zone).schema
            if len(inferred_schema) > 0:
                log(f"Inferred schema with {len(inferred_schema)} columns (waited {waited}s)")
                break
        except Exception as e:
            if waited == 0 or waited % 60 == 0:
                log(f"Landing zone probe error: {type(e).__name__}: {e}")
        if waited >= _LANDING_WAIT_MAX:
            log(f"No Parquet files in {landing_zone} after {waited}s, giving up")
            spark.stop()
            raise SystemExit(1)
        log(f"Waiting for Parquet files in landing zone ({waited}s elapsed)...")
        time.sleep(_LANDING_WAIT_INTERVAL)
        waited += _LANDING_WAIT_INTERVAL

    stream = (
        spark.readStream.schema(inferred_schema)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .parquet(landing_zone)
    )
    query = (
        stream.writeStream.foreachBatch(
            lambda df, bid: write_bronze_batch(df, bid, table_name, bronze_uri)
        )
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    log("Streaming query started, awaiting termination...")
    await_stream(spark, query)
    spark.stop()


if __name__ == "__main__":
    main()
