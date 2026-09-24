"""
Bronze Ingest - Structured Streaming ingestion from landing zone to Iceberg.

Monitors the landing zone path in S3 for new Parquet files written by datagen,
and appends rows to the managed Iceberg table `bronze_raw`.

This is the continuous-pipeline equivalent of bronze_verify.py. Where bronze_verify
reads all data in a single batch pass, bronze_ingest consumes files incrementally
as they arrive.

Replay idempotency (E3): a micro-batch replayed after a driver restart (the
table commit landed, the checkpoint commit did not) appended its rows a
second time, and silver-stream then carried the duplicate forward. Each
append tags its Iceberg snapshot with the streaming query id and batch id
(``snapshot-property.lb.*``). Only the first micro-batch of a query run can
be a replay, so only that batch looks the tag up in the table's snapshots
and, when found, is skipped and logged as "skipped (already committed)", a
line the metrics collector does not count as rows. Only this stream appends
to bronze_raw, so nothing it needs commits between the lookup and the append.

What the lookup relies on: the replayed batch's snapshot is still in the
snapshot history. Compaction (rewrite_data_files) may add a replace snapshot
on top of it, which does not remove it from history, but expire_snapshots
does once it is older than the retention threshold. A driver that stays
down longer than the retention (sustained.retention_threshold, 30m by
default) and then replays would append the batch again.

Startup refuses a fresh checkpoint over a non-empty bronze_raw: the new
query would re-read the whole landing zone into it.

Environment variables (set by job.py):
    LB_BRONZE_URI        - s3a://bronze-bucket/
    BRONZE_BUCKET        - bucket name (for checkpoint path)
    CATALOG_NAME         - Iceberg catalog name (e.g., "lakehouse")
    CHECKPOINT_LOCATION  - s3a://bronze-bucket/checkpoints/bronze-ingest/
    TRIGGER_INTERVAL     - e.g., "30 seconds"
"""

import os
import time

from common import (
    _s3_table_path,
    await_stream,
    env,
    log,
    refuse_fresh_checkpoint_over_data,
    replay_possible,
    set_utc_session,
    streaming_query_id,
    table_exists,
)
from pyspark.sql import SparkSession

_LANDING_WAIT_INTERVAL = 15  # seconds between checks
_LANDING_WAIT_MAX = 1800  # 30 minutes

_QUERY_PROP = "lb.query-id"
_BATCH_PROP = "lb.batch-id"


def batch_already_committed(spark, table_name, query_id, batch_id):
    """True if a snapshot of ``table_name`` was written by this query and batch."""
    rows = spark.sql(
        f"SELECT 1 FROM {table_name}.snapshots "
        f"WHERE summary['{_QUERY_PROP}'] = '{query_id}' "
        f"AND summary['{_BATCH_PROP}'] = '{int(batch_id)}' LIMIT 1"
    ).collect()
    return bool(rows)


def _table_location(bronze_uri, bronze_table_path):
    """Explicit S3 LOCATION for the bronze table.

    Hive: s3a://bucket/warehouse/default.db/bronze_raw
    Polaris: s3://bucket/default/bronze_raw (must match the namespace's
    allowed location set during bootstrap, using the s3:// scheme)
    """
    if os.getenv("LB_CATALOG_TYPE", "hive") == "polaris":
        bucket = bronze_uri.replace("s3a://", "").replace("s3://", "").rstrip("/")
        return f"s3://{bucket}/default/bronze_raw"
    return _s3_table_path(bronze_uri, bronze_table_path)


def write_bronze_batch(
    batch_df,
    batch_id,
    table_name,
    table_location=None,
    target_file_size_bytes="536870912",
):
    """Append one micro-batch to the bronze Iceberg table exactly once.

    Returns the rows written, 0 for an empty or already-committed batch.
    """
    spark = batch_df.sparkSession
    check_replay = replay_possible(spark)
    batch_start = time.time()
    count = batch_df.count()
    if count == 0:
        log(f"Batch {batch_id}: empty, skipping")
        return 0

    query_id = streaming_query_id(spark)
    props = {
        f"snapshot-property.{_QUERY_PROP}": query_id,
        f"snapshot-property.{_BATCH_PROP}": str(int(batch_id)),
    }

    exists = table_exists(spark, table_name)
    if check_replay and exists and batch_already_committed(spark, table_name, query_id, batch_id):
        # Not a "writing" line: the collector counts those as ingested rows.
        log(f"Batch {batch_id}: skipped (already committed to {table_name})")
        return 0

    log(f"Batch {batch_id}: writing {count:,} rows to {table_name}")
    writer = batch_df.writeTo(table_name).options(**props)
    if exists:
        writer.append()
    else:
        log(f"Batch {batch_id}: creating bronze table {table_name} at {table_location}")
        writer = (
            writer.tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
        )
        if table_location:
            writer = writer.tableProperty("location", table_location)
        writer.create()
    batch_time = time.time() - batch_start
    log(f"Batch {batch_id}: committed in {batch_time:.1f}s")
    return count


def main() -> None:
    bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")
    catalog_name = env("CATALOG_NAME", "lakehouse")
    checkpoint_location = env("CHECKPOINT_LOCATION")
    trigger_interval = env("TRIGGER_INTERVAL", "30 seconds")
    max_files_per_trigger = env("MAX_FILES_PER_TRIGGER", "50")
    target_file_size_bytes = env("TARGET_FILE_SIZE_BYTES", "536870912")

    landing_zone = bronze_uri + "customer/interactions/"
    bronze_table_path = env("LB_BRONZE_TABLE", "default.bronze_raw")
    table_name = f"{catalog_name}.{bronze_table_path}"

    spark = SparkSession.builder.appName("lb-bronze-ingest").getOrCreate()
    set_utc_session(spark)

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
    bronze_warehouse = bronze_uri + "warehouse/default.db/"
    log(f"Namespace location: {bronze_warehouse}")
    try:
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.default LOCATION '{bronze_warehouse}'"
        )
        log(f"Created namespace {catalog_name}.default")
    except Exception as e:
        log(f"Namespace creation note: {str(e)}")

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

    refuse_fresh_checkpoint_over_data(spark, checkpoint_location, table_name)
    table_location = _table_location(bronze_uri, bronze_table_path)
    stream = (
        spark.readStream.schema(inferred_schema)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .parquet(landing_zone)
    )
    query = (
        stream.writeStream.foreachBatch(
            lambda df, bid: write_bronze_batch(
                df, bid, table_name, table_location, target_file_size_bytes
            )
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
