"""
Silver Stream - Structured Streaming transformation from Bronze to Silver.

Reads incrementally from the Iceberg bronze_raw table, applies the same
Silver transformations as the batch silver_build.py job, and writes to the
managed Iceberg table `customer_interactions_enriched`.

This is the continuous-pipeline equivalent of silver_build.py. Where
silver_build processes all data in a single batch pass, silver_stream
processes micro-batches as new rows arrive in bronze_raw.

Replay idempotency: Structured Streaming re-runs a micro-batch with the
same batchId when the driver dies after the table commit but before the
checkpoint commit, and a plain append wrote it twice. Every silver row
carries `_stream_id` (the streaming query id) and `_batch_id`. Only the
first micro-batch of a query run can be such a replay (a failed batch stops
the query and the driver), so only that batch probes for rows with its
(stream, batch) key and deletes them before appending. Later batches just
append, so the table gets no empty delete snapshot per trigger. The key
includes the query id: a new checkpoint restarts batch ids at 0, and a bare
batch id would delete the previous stream's batch 0.

Batch silver_build writes the same table. Its full rebuild drops (catalog
entry only on Hive and Polaris) a table whose columns differ from its own (a
stream-written table has _stream_id) and creates it without the stream
columns; on startup this job adds the
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
    CHECKPOINT_LOCATION  - s3a://silver-bucket/checkpoints/silver-stream/
    TRIGGER_INTERVAL     - e.g., "0 seconds" (back to back)
"""

from __future__ import annotations

import time

from common import (
    METADATA_DELETE_AFTER_COMMIT,
    METADATA_PREVIOUS_VERSIONS_MAX,
    apply_silver_transformations_anchored,
    assert_progress,
    await_stream,
    check_bronze_fingerprint,
    create_namespace,
    data_clock_date,
    emit_stream_scale_admission,
    ensure_column,
    env,
    log,
    refuse_fresh_checkpoint_over_data,
    replay_possible,
    resolve_data_clock,
    set_utc_session,
    silver_stream_accumulator,
    streaming_query_id,
    table_exists,
    wait_for_bronze_table,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


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
                .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
                .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
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


def main() -> None:
    catalog = env("LB_ICEBERG_CATALOG", "ice")
    silver_uri = env("LB_SILVER_URI", "s3a://lb-silver/")
    checkpoint_location = env("CHECKPOINT_LOCATION")
    trigger_interval = env("TRIGGER_INTERVAL", "0 seconds")
    target_file_size_bytes = env("TARGET_FILE_SIZE_BYTES", "536870912")
    # Soft cap on bronze rows read per trigger (Iceberg
    # streaming-max-rows-per-micro-batch); 0 lifts it.
    max_rows_per_trigger = int(env("SILVER_MAX_ROWS_PER_TRIGGER", "0"))

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
    # Label the imposed scale envelope so a report never presents a
    # Lakebench-bounded number as infrastructure performance. Measured up to
    # scale 10; larger scales run, but the cap is labelled, not hidden.
    emit_stream_scale_admission(measured_envelope_scale=10)

    log("Creating Iceberg namespace...")
    try:
        silver_warehouse = silver_uri + "warehouse/silver.db/"
        create_namespace(spark, catalog, "silver", silver_warehouse)
        log(f"Created namespace {catalog}.silver")
    except Exception as e:
        log(f"Namespace creation note: {str(e)}")

    # Reused table without the idempotency key (batch-built, or an older
    # release): add it before the first micro-batch references it.
    if table_exists(spark, silver_tbl):
        ensure_column(spark, silver_tbl, "_stream_id", "STRING")
        ensure_column(spark, silver_tbl, "_batch_id", "BIGINT")
    refuse_fresh_checkpoint_over_data(spark, checkpoint_location, silver_tbl)

    # strict: the job always exports LB_DATA_CLOCK (with a today fallback),
    # so a missing clock here is a configuration error. Anchoring per
    # micro-batch instead would break same-bronze reproducibility across
    # restarts, which recency scoring must not do.
    data_clock = resolve_data_clock(df_fallback=None, strict=True)

    wait_for_bronze_table(spark, bronze_tbl)

    # Refuse resume when the bronze snapshot lineage was replaced under the
    # old checkpoint. The first start writes the sidecar; later starts
    # compare and raise on a mismatch. After the wait, so the snapshot
    # lookup is not defeated by the "bronze not created yet" race.
    check_bronze_fingerprint(spark, bronze_tbl, checkpoint_location, source_format="iceberg")

    # Fold each batch's committed rows into a total read after await_stream,
    # so a stream that saw no non-empty batch is caught.
    _foreach_batch, read_total = silver_stream_accumulator(
        lambda df, bid: write_silver_batch(df, bid, silver_tbl, target_file_size_bytes, data_clock)
    )

    reader = spark.readStream.format("iceberg")
    # A micro-batch takes the next whole data file, then stops adding files
    # once max_rows_per_trigger would be exceeded, so a slow batch cannot make
    # the next one unboundedly larger. 0 lifts the cap (take every snapshot).
    if max_rows_per_trigger > 0:
        reader = reader.option("streaming-max-rows-per-micro-batch", str(max_rows_per_trigger))
    stream = reader.load(bronze_tbl)
    query = (
        stream.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    log("Streaming query started, awaiting termination...")
    await_stream(spark, query)
    total = read_total()
    log(f"Silver stream stopped; total rows written across all batches: {total}")
    assert_progress(total, "silver-stream")
    spark.stop()


if __name__ == "__main__":
    main()
