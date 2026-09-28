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
has already recorded, so the second write commits nothing. The count
returned for the write is read from THIS attempt's own row in
``DeltaTable.forName(...).history(20)`` (I8), matched by a per-attempt
unique ``userMetadata`` tag: original and replay share the same
``(txnAppId, txnVersion)``, so a lookup by ``txnAppId`` alone would find
the original's row on a replay and wrongly report non-zero. If no row
matches this attempt's tag, the write did not commit -- either Delta
short-circuited the ``(appId, version)`` or the driver crashed before
commit -- and we return 0. This is immune to interleaved compaction /
vacuum commits that would displace the write from the head of history
and break a before / after-version bracket. Startup refuses a fresh
checkpoint over a non-empty silver table, which would re-read all of
bronze into it a second time.

Startup race (B4): the not-exists branch is split into a metadata-only
``CREATE TABLE IF NOT EXISTS`` (Delta serialises this at its log; the loser
sees the table on the next check) and the same ``append`` write the exists
branch uses. Both racers converge at CREATE and their appends both land, so
neither's rows are lost.

Debug columns (I6): every write projects ``_stream_id STRING`` and
``_batch_id BIGINT`` in the silver row shape, matching the Iceberg stream
(``silver_stream.py``). Delta's native ``txnAppId``/``txnVersion`` handles
exactly-once; the columns give operators the same by-eye debugging signal
the Iceberg path already emits.

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

import os
import threading
import time
import uuid

from common import (
    SilverAbort,
    apply_silver_transformations_anchored,
    assert_progress,
    await_stream,
    configured_data_clock,
    data_clock_date,
    delta_idempotent_options,
    emit_stream_scale_admission,
    ensure_column,
    env,
    log,
    refuse_fresh_checkpoint_over_data,
    set_utc_session,
    streaming_query_id,
    table_exists,
    write_delta_table,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

_TABLE_WAIT_INTERVAL = 15  # seconds between checks
# A3 (silver-plan): see silver_stream.py for rationale.
_TABLE_WAIT_MAX = int(os.environ.get("LB_SILVER_BRONZE_WAIT_SECONDS", "1800"))


def _rows_from_own_commit(spark, silver_tbl, attempt_tag):
    """Rows THIS write attempt committed, read from its own row in
    ``DeltaTable.forName(spark, silver_tbl).history(100)``.

    ``attempt_tag`` is a per-attempt unique string, set as the write's
    ``userMetadata``. Uniqueness matters: two attempts of the same
    ``(txnAppId, txnVersion)`` -- one original, one replay -- share the
    same ``txnAppId``, so a lookup by ``txnAppId`` alone finds the original
    commit's row even for a replay Delta short-circuited, wrongly returning
    non-zero. A per-attempt tag pins each history lookup to THIS attempt.

    Returns:
      - ``int(operationMetrics.numOutputRows)`` when the matching row is
        found in the last 100 commits (this attempt committed rows).
      - ``None`` when no matching row is found: either Delta short-circuited
        this attempt via its ``(appId, version)`` dedup, or the write did
        not reach the log; either way, this attempt committed nothing.

    Window is 100 rather than 20: concurrent OPTIMIZE / VACUUM / a second
    writer to the same table can push our commit past a small window
    between our write and our history read, and returning ``None`` on a
    real commit would silently under-count against A1's LB-044 accumulator
    (invariant 5 label drift, not corruption -- rows are still written).
    100 is a cheap Delta metadata scan and covers realistic maintenance
    burst rates.

    Immune to interleaved compaction/vacuum commits that would displace the
    write from the head of history and break a before/after-version bracket.
    """
    from delta.tables import DeltaTable

    hist = DeltaTable.forName(spark, silver_tbl).history(100)
    if "userMetadata" not in set(hist.columns):
        # Older Delta build without userMetadata in history; the caller
        # cannot distinguish a replay from a real commit, so refuse.
        raise RuntimeError(
            "DeltaTable.history() has no userMetadata column; the deployment is "
            "on a Delta build too old for the I8 read-own-commit guard"
        )
    matches = hist.where(hist["userMetadata"] == attempt_tag).limit(1).collect()
    if not matches:
        return None
    metrics = matches[0]["operationMetrics"] or {}
    n = metrics.get("numOutputRows")
    return int(n) if n is not None else 0


def _create_silver_table_if_not_exists(spark, schema, silver_tbl, silver_bucket):
    """Metadata-only ``CREATE TABLE IF NOT EXISTS`` for the silver Delta table.

    Delta serialises this commit at its log; a startup race between two
    ``silver_stream_delta`` mains sees exactly one racer commit the metadata
    and the other's ``IF NOT EXISTS`` no-op. Both then take the append branch
    with their own ``txnAppId``, so both racers' rows land -- neither is lost
    to an overwrite (which is what the pre-B4 not-exists branch used to do,
    and which would have removed the winner's data files on the loser's second
    call).
    """
    from delta.tables import DeltaTable

    builder = (
        DeltaTable.createIfNotExists(spark)
        .tableName(silver_tbl)
        .addColumns(schema)
        .partitionedBy("interaction_date")
        .property("delta.logRetentionDuration", "interval 30 days")
    )
    if os.environ.get("LB_CATALOG_TYPE", "hive").lower() == "unity":
        # Match write_delta_table's Unity path: EXTERNAL table at the
        # warehouse-derived S3 path, so credential vending is bypassed.
        parts = silver_tbl.split(".", 1)
        schema_table = parts[1] if len(parts) > 1 else silver_tbl
        sub = schema_table.split(".", 1)
        s, t = (sub[0], sub[1]) if len(sub) == 2 else ("default", schema_table)
        table_path = f"{silver_bucket.rstrip('/')}/warehouse/{s}.db/{t}"
        builder = builder.location(table_path)
    builder.execute()


def write_silver_batch(batch_df, batch_id, silver_tbl, silver_bucket, data_clock=None):
    """Transform one micro-batch and write it to the Silver Delta table once.

    ``data_clock`` is the recency anchor; None measures it from the batch.
    Returns the rows this call committed: 0 for an empty batch or a replay
    Delta short-circuited.
    """
    spark = batch_df.sparkSession
    batch_start = time.time()
    count = batch_df.count()
    if count == 0:
        log(f"Batch {batch_id}: empty, skipping")
        return 0

    log(f"Batch {batch_id}: read {count:,} bronze rows")
    anchor = data_clock if data_clock is not None else data_clock_date(batch_df)
    stream_id = streaming_query_id(spark)
    # I6: project the same debug columns the Iceberg path emits so operators
    # can trace a row back to its stream+batch, even though Delta's own
    # (txnAppId, txnVersion) is authoritative for replay correctness.
    enriched = (
        apply_silver_transformations_anchored(batch_df, anchor)
        .withColumn("_stream_id", lit(stream_id))
        .withColumn("_batch_id", lit(int(batch_id)).cast("bigint"))
        .cache()
    )
    try:
        enriched_count = enriched.count()
        txn = delta_idempotent_options(spark, "lb-silver-stream", batch_id)
        # A per-attempt userMetadata tag lets ``_rows_from_own_commit``
        # tell an actual commit from a replay Delta dedup'd by
        # ``(txnAppId, txnVersion)``. Sharing ``txnAppId`` between the
        # original and the replay is what makes Delta's short-circuit work
        # -- but it also means a lookup by ``txnAppId`` alone finds the
        # original's row for a replay, wrongly reporting non-zero.
        attempt_tag = f"{txn['txnAppId']}:v{txn['txnVersion']}:{uuid.uuid4().hex}"
        write_options = {"userMetadata": attempt_tag, **txn}

        if not table_exists(spark, silver_tbl):
            # B4: split the not-exists branch into a metadata-only CREATE IF
            # NOT EXISTS followed by the same append the exists branch uses.
            # Two racers converge at CREATE; the loser sees the table and
            # falls through to append. Never do an overwrite here: the
            # loser's overwrite would delete the winner's data files.
            log(f"Batch {batch_id}: creating Silver table with partitioning (metadata-only)")
            _create_silver_table_if_not_exists(spark, enriched.schema, silver_tbl, silver_bucket)

        write_delta_table(
            spark,
            enriched,
            silver_tbl,
            silver_bucket,
            mode="append",
            options=write_options,
        )
        committed = _rows_from_own_commit(spark, silver_tbl, attempt_tag)
        if committed is None:
            log(f"Batch {batch_id}: skipped (txn already applied)")
            return 0
    finally:
        enriched.unpersist(blocking=False)

    # Logged after the commit so a skipped replay is never counted as rows.
    log(f"Batch {batch_id}: transforming {count:,} rows")
    log(
        f"Batch {batch_id}: {enriched_count:,} rows after transforms "
        f"(filtered ~{(1 - enriched_count / count) * 100:.0f}%)"
    )
    batch_time = time.time() - batch_start
    log(f"Batch {batch_id}: committed to {silver_tbl} in {batch_time:.1f}s ({committed:,} rows)")
    return committed


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
    # G5: label the imposed scale envelope so downstream reports never
    # present a Lakebench-bounded number as infrastructure performance
    # (invariant 6). v1.6 profile: measured up to scale 10; larger scales
    # run but the cap is labelled, not hidden.
    emit_stream_scale_admission(measured_envelope_scale=10)

    log("Creating namespace...")
    try:
        silver_warehouse = silver_uri + "warehouse/"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver LOCATION '{silver_warehouse}'")
        log(f"Created namespace {catalog}.silver")
    except Exception as e:
        log(f"Namespace creation note: {str(e)}")

    # I6: reused silver table without the debug columns (batch-built, or an
    # older release): add them before the first micro-batch references them.
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
    # starts concurrently. Any probe error only means "wait longer".
    waited = 0
    while not _exists_or_false(spark, bronze_tbl):
        if waited >= _TABLE_WAIT_MAX:
            log(f"Bronze table {bronze_tbl} not found after {waited}s, giving up")
            spark.stop()
            raise SilverAbort(f"bronze did not appear in wait window ({_TABLE_WAIT_MAX}s)")
        log(f"Waiting for {bronze_tbl} to be created ({waited}s elapsed)...")
        time.sleep(_TABLE_WAIT_INTERVAL)
        waited += _TABLE_WAIT_INTERVAL
    log(f"Bronze table {bronze_tbl} exists (waited {waited}s)")

    # A1: driver-side accumulator; see silver_stream.py for the rationale.
    rows_written_total = 0
    rows_written_lock = threading.Lock()

    def _foreach_batch(df, bid):
        written = write_silver_batch(df, bid, silver_tbl, silver_uri, data_clock)
        with rows_written_lock:
            nonlocal rows_written_total
            rows_written_total += int(written or 0)

    stream = spark.readStream.format("delta").table(bronze_tbl)
    query = (
        stream.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    log("Streaming query started, awaiting termination...")
    await_stream(spark, query)
    with rows_written_lock:
        total = rows_written_total
    log(f"Silver stream stopped; total rows written across all batches: {total}")
    assert_progress(total, "silver-stream")
    spark.stop()


if __name__ == "__main__":
    main()
