"""
Silver Stream - Structured Streaming transformation from Bronze to Silver.

Reads incrementally from the Iceberg bronze_raw table, applies the same
Silver transformations as the batch silver_build.py job, and writes to the
managed Iceberg table `customer_interactions_enriched`.

This is the continuous-pipeline equivalent of silver_build.py. Where
silver_build processes all data in a single batch pass, silver_stream
processes micro-batches as new rows arrive in bronze_raw.

Replay idempotency (E3, the LB-109 protocol of silver_stream_financial,
scoped to the stream): Structured Streaming re-runs a micro-batch with the
same batchId when the driver dies after the table commit but before the
checkpoint commit, and a plain append wrote it twice. Every silver row
carries `_stream_id` (the streaming query id) and `_batch_id`. Only the
first micro-batch of a query run can be such a replay (a failed batch stops
the query and the driver), so only that batch probes for rows with its
(stream, batch) key and deletes them before appending. Later batches just
append, so the table gets no empty delete snapshot per trigger. The key
includes the query id: a new checkpoint restarts batch ids at 0, and a bare
batch id would delete the previous stream's batch 0.

Batch silver_build writes the same table. Its full rebuild (createOrReplace)
replaces the table, stream columns included; on startup this job adds the
two columns to a table that lacks them (ensure_column), and rows without a
stream id are never matched by the replay delete. Startup refuses a fresh
checkpoint over a non-empty silver table, which would re-read all of bronze
into it a second time.

Recency is anchored to LB_DATA_CLOCK (the configured datagen end) when set,
so every micro-batch uses one clock; otherwise to each micro-batch's newest
event.

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
    configured_data_clock,
    data_clock_date,
    ensure_column,
    env,
    log,
    refuse_fresh_checkpoint_over_data,
    replay_possible,
    set_utc_session,
    streaming_query_id,
    table_exists,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

_TABLE_WAIT_INTERVAL = 15  # seconds between checks
_TABLE_WAIT_MAX = 1800  # 30 minutes -- generous for large datagen


def write_silver_batch(
    batch_df, batch_id, silver_tbl, target_file_size_bytes="536870912", data_clock=None
):
    """Transform one micro-batch and write it to Silver exactly once.

    ``data_clock`` is the recency anchor; None measures it from the batch.
    Returns the number of silver rows the batch wrote.
    """
    spark = batch_df.sparkSession
    check_replay = replay_possible(spark)
    batch_start = time.time()
    count = batch_df.count()
    if count == 0:
        log(f"Batch {batch_id}: empty, skipping")
        return 0

    log(f"Batch {batch_id}: transforming {count:,} rows")

    stream_id = streaming_query_id(spark)
    anchor = data_clock if data_clock is not None else data_clock_date(batch_df)
    enriched = (
        apply_silver_transformations_anchored(batch_df, anchor)
        .withColumn("_stream_id", lit(stream_id))
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
            if check_replay:
                _delete_earlier_attempt(spark, silver_tbl, stream_id, batch_id)
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


def _delete_earlier_attempt(spark, silver_tbl, stream_id, batch_id):
    """Remove rows an earlier attempt of this (stream, batch) committed.

    Probes first so a first attempt commits no empty delete snapshot; file
    statistics on the two key columns prune the probe to the batch's files.
    Delete-then-append is two commits: a failure between them leaves the
    batch missing until the next replay, never duplicated.
    """
    key = f"_stream_id = '{stream_id}' AND _batch_id = {int(batch_id)}"
    if spark.sql(f"SELECT 1 FROM {silver_tbl} WHERE {key} LIMIT 1").collect():
        log(f"Batch {batch_id}: replay; removing the earlier attempt's rows")
        spark.sql(f"DELETE FROM {silver_tbl} WHERE {key}")


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

    # Reused table without the idempotency key (batch-built, or an older
    # release): add it before the first micro-batch references it.
    if table_exists(spark, silver_tbl):
        ensure_column(spark, silver_tbl, "_stream_id", "STRING")
        ensure_column(spark, silver_tbl, "_batch_id", "BIGINT")
    refuse_fresh_checkpoint_over_data(spark, checkpoint_location, silver_tbl)

    data_clock = configured_data_clock()
    log(
        f"Data clock (recency anchor): {data_clock} from LB_DATA_CLOCK"
        if data_clock is not None
        else "Data clock: LB_DATA_CLOCK unset; recency anchored per micro-batch"
    )

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
            lambda df, bid: write_silver_batch(
                df, bid, silver_tbl, target_file_size_bytes, data_clock
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
