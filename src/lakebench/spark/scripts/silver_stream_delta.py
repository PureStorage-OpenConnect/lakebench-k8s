"""
Silver Stream (Delta Lake) - Structured Streaming transformation from Bronze to Silver.

Reads incrementally from the Delta bronze_raw table, applies the same
Silver transformations as the batch silver_build.py job, and writes to the
managed Delta table `customer_interactions_enriched`.

This is the continuous-pipeline equivalent of silver_build.py for Delta Lake.
Where silver_build processes all data in a single batch pass, silver_stream
processes micro-batches as new rows arrive in bronze_raw.

Replay idempotency (E3): each micro-batch is one Delta commit written with
Delta's idempotent-write options (txnAppId = stream + query id, txnVersion =
batch id). A batch replayed after a driver restart carries a txnVersion Delta
has already recorded, so the second write commits nothing. Only the first
micro-batch of a query run can be a replay; for that batch the table version
is compared before and after the write, and a skipped write is logged as
"skipped (txn already applied)", a line the metrics collector does not count
as processed rows. Startup refuses a fresh checkpoint over a non-empty silver
table, which would re-read all of bronze into it a second time.

Recency is anchored to LB_DATA_CLOCK (the configured datagen end) when set,
otherwise to each micro-batch's newest event.

Environment variables (set by job.py):
    LB_BRONZE_URI        - s3a://bronze-bucket/
    LB_SILVER_URI        - s3a://silver-bucket/
    LB_ICEBERG_CATALOG   - catalog name (e.g., "lakehouse")
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
    delta_idempotent_options,
    delta_table_version,
    env,
    log,
    refuse_fresh_checkpoint_over_data,
    replay_possible,
    set_utc_session,
    table_exists,
    write_delta_table,
)
from pyspark.sql import SparkSession

_TABLE_WAIT_INTERVAL = 15  # seconds between checks
_TABLE_WAIT_MAX = 1800  # 30 minutes -- generous for large datagen


def write_silver_batch(batch_df, batch_id, silver_tbl, silver_bucket, data_clock=None):
    """Transform one micro-batch and write it to the Silver Delta table once.

    ``data_clock`` is the recency anchor; None measures it from the batch.
    Returns the rows this call committed: 0 for an empty batch or a replay
    Delta skipped.
    """
    spark = batch_df.sparkSession
    check_replay = replay_possible(spark)
    batch_start = time.time()
    count = batch_df.count()
    if count == 0:
        log(f"Batch {batch_id}: empty, skipping")
        return 0

    log(f"Batch {batch_id}: read {count:,} bronze rows")
    anchor = data_clock if data_clock is not None else data_clock_date(batch_df)
    enriched = apply_silver_transformations_anchored(batch_df, anchor).cache()
    try:
        enriched_count = enriched.count()
        txn = delta_idempotent_options(spark, "lb-silver-stream", batch_id)
        if table_exists(spark, silver_tbl):
            before = delta_table_version(spark, silver_tbl) if check_replay else None
            write_delta_table(
                spark, enriched, silver_tbl, silver_bucket, mode="append", options=txn
            )
            if before is not None and delta_table_version(spark, silver_tbl) == before:
                log(f"Batch {batch_id}: skipped (txn already applied)")
                return 0
        else:
            log(f"Batch {batch_id}: creating Silver table with partitioning")
            write_delta_table(
                spark,
                enriched,
                silver_tbl,
                silver_bucket,
                mode="overwrite",
                partition_cols=["interaction_date"],
                options={"delta.logRetentionDuration": "interval 30 days", **txn},
            )
    finally:
        enriched.unpersist(blocking=False)

    # Logged after the commit so a skipped replay is never counted as rows.
    log(f"Batch {batch_id}: transforming {count:,} rows")
    log(
        f"Batch {batch_id}: {enriched_count:,} rows after transforms "
        f"(filtered ~{(1 - enriched_count / count) * 100:.0f}%)"
    )
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

    bronze_tbl = f"{catalog}.{env('LB_BRONZE_TABLE', 'default.bronze_raw')}"
    silver_tbl = f"{catalog}.{env('LB_SILVER_TABLE', 'silver.customer_interactions_enriched')}"

    spark = SparkSession.builder.appName("lb-silver-stream-delta").getOrCreate()
    set_utc_session(spark)

    log("=" * 60)
    log("Silver Stream - Delta Lake (Structured Streaming)")
    log("=" * 60)
    log(f"Source table: {bronze_tbl}")
    log(f"Target table: {silver_tbl}")
    log(f"Checkpoint:   {checkpoint_location}")
    log(f"Trigger:      {trigger_interval}")

    log("Creating namespace...")
    try:
        silver_warehouse = silver_uri + "warehouse/"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver LOCATION '{silver_warehouse}'")
        log(f"Created namespace {catalog}.silver")
    except Exception as e:
        log(f"Namespace creation note: {str(e)}")

    refuse_fresh_checkpoint_over_data(spark, checkpoint_location, silver_tbl)
    data_clock = configured_data_clock()
    log(
        f"Data clock (recency anchor): {data_clock} from LB_DATA_CLOCK"
        if data_clock is not None
        else "Data clock: LB_DATA_CLOCK unset; recency anchored per micro-batch"
    )

    # Bronze-ingest creates bronze_raw on its first batch; silver-stream
    # starts concurrently. Any probe error only means "wait longer".
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

    stream = spark.readStream.format("delta").table(bronze_tbl)
    query = (
        stream.writeStream.foreachBatch(
            lambda df, bid: write_silver_batch(df, bid, silver_tbl, silver_uri, data_clock)
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
