"""
Silver Stream - Structured Streaming transformation from Bronze to Silver.

Reads incrementally from the Iceberg bronze_raw table, applies the same
Silver transformations as the batch silver_build.py job, and writes to the
managed Iceberg table `customer_interactions_enriched`.

This is the continuous-pipeline equivalent of silver_build.py. Where
silver_build processes all data in a single batch pass, silver_stream
processes micro-batches as new rows arrive in bronze_raw.

Replay idempotency (E3, same protocol as silver_stream_financial, LB-109):
Structured Streaming re-runs a micro-batch with the same batchId when the
driver dies after the table commit but before the checkpoint commit. A plain
append then wrote the batch twice. Every silver row now carries
`_batch_id BIGINT`; each batch first deletes rows tagged with its id, then
appends. The delete is a no-op the first time and removes the earlier
attempt on a replay, so a replayed batch leaves exactly one copy. Batch-mode
silver_build writes no `_batch_id`; the column is added on startup when a
reused table lacks it. Batch and continuous modes do not share a table.

Environment variables (set by job.py):
    LB_BRONZE_URI        - s3a://bronze-bucket/
    LB_SILVER_URI        - s3a://silver-bucket/
    LB_ICEBERG_CATALOG   - Iceberg catalog name (e.g., "lakehouse")
    CATALOG_NAME         - same as LB_ICEBERG_CATALOG
    CHECKPOINT_LOCATION  - s3a://silver-bucket/checkpoints/silver-stream/
    TRIGGER_INTERVAL     - e.g., "60 seconds"
"""

from __future__ import annotations

import time

from common import (
    apply_silver_transformations_anchored,
    await_stream,
    data_clock_date,
    ensure_column,
    env,
    log,
    set_utc_session,
    table_exists,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

_TABLE_WAIT_INTERVAL = 15  # seconds between checks
_TABLE_WAIT_MAX = 1800  # 30 minutes -- generous for large datagen


def write_silver_batch(batch_df, batch_id, silver_tbl, target_file_size_bytes="536870912"):
    """Transform one micro-batch and write it to Silver exactly once.

    Returns the number of silver rows the batch holds after the write.
    """
    spark = batch_df.sparkSession
    batch_start = time.time()
    count = batch_df.count()
    if count == 0:
        log(f"Batch {batch_id}: empty, skipping")
        return 0

    log(f"Batch {batch_id}: transforming {count:,} rows")

    # Recency is measured from the newest event in this batch. A pure
    # function of the batch contents, so a replay writes identical scores.
    anchor = data_clock_date(batch_df)
    enriched = (
        apply_silver_transformations_anchored(batch_df, anchor)
        .withColumn("_batch_id", lit(int(batch_id)).cast("bigint"))
        .cache()
    )
    try:
        enriched_count = enriched.count()
        log(
            f"Batch {batch_id}: {enriched_count:,} rows after transforms "
            f"(filtered ~{(1 - enriched_count / count) * 100:.0f}%)"
        )

        if table_exists(spark, silver_tbl):
            # DELETE first: removes the rows of an earlier attempt of this
            # batch (replay after a driver restart); no-op otherwise.
            spark.sql(f"DELETE FROM {silver_tbl} WHERE _batch_id = {int(batch_id)}")
            enriched.writeTo(silver_tbl).append()
        else:
            log(f"Batch {batch_id}: creating Silver table with partitioning")
            (
                enriched.writeTo(silver_tbl)
                .partitionedBy("interaction_date")
                .tableProperty("write.format.default", "parquet")
                .tableProperty("write.parquet.compression-codec", "snappy")
                .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
                .create()
            )
    finally:
        enriched.unpersist(blocking=False)

    batch_time = time.time() - batch_start
    log(f"Batch {batch_id}: committed to {silver_tbl} in {batch_time:.1f}s")
    return enriched_count


def _exists_or_false(spark, table):
    try:
        return table_exists(spark, table)
    except Exception as e:  # noqa: BLE001
        log(f"Bronze table probe error: {type(e).__name__}: {e}")
        return False


def main() -> None:
    catalog = env("LB_ICEBERG_CATALOG", "ice")
    silver_uri = env("LB_SILVER_URI", "s3a://lb-silver/")
    checkpoint_location = env("CHECKPOINT_LOCATION")
    trigger_interval = env("TRIGGER_INTERVAL", "60 seconds")
    target_file_size_bytes = env("TARGET_FILE_SIZE_BYTES", "536870912")

    bronze_tbl = f"{catalog}.{env('LB_BRONZE_TABLE', 'default.bronze_raw')}"
    silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"

    spark = SparkSession.builder.appName("lb-silver-stream").getOrCreate()
    set_utc_session(spark)

    log("=" * 60)
    log("Silver Stream (Structured Streaming)")
    log("=" * 60)
    log(f"Source table: {bronze_tbl}")
    log(f"Target table: {silver_tbl}")
    log(f"Checkpoint:   {checkpoint_location}")
    log(f"Trigger:      {trigger_interval}")

    log("Creating Iceberg namespace...")
    try:
        silver_warehouse = silver_uri + "warehouse/silver.db/"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver LOCATION '{silver_warehouse}'")
        log(f"Created namespace {catalog}.silver")
    except Exception as e:
        log(f"Namespace creation note: {str(e)}")

    # Reused table from before the idempotency key: add it before the first
    # micro-batch, whose DELETE references it.
    if table_exists(spark, silver_tbl):
        ensure_column(spark, silver_tbl, "_batch_id", "BIGINT")

    # Bronze-ingest creates bronze_raw on its first batch; silver-stream
    # starts concurrently and would crash with NoSuchTableException.
    # Any error here only means "wait longer", so a transient catalog error
    # is retried rather than fatal.
    waited = 0
    while not _exists_or_false(spark, bronze_tbl):
        if waited >= _TABLE_WAIT_MAX:
            log(f"Bronze table {bronze_tbl} not found after {waited}s, giving up")
            spark.stop()
            raise SystemExit(1)
        log(f"Waiting for {bronze_tbl} to be created ({waited}s elapsed)...")
        time.sleep(_TABLE_WAIT_INTERVAL)
        waited += _TABLE_WAIT_INTERVAL
    log(f"Bronze table {bronze_tbl} exists (waited {waited}s)")

    stream = spark.readStream.format("iceberg").load(bronze_tbl)
    query = (
        stream.writeStream.foreachBatch(
            lambda df, bid: write_silver_batch(df, bid, silver_tbl, target_file_size_bytes)
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
