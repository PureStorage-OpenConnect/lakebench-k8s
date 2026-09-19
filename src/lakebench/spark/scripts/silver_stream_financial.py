"""Silver Stream (Financial, sustained) -- incremental bronze -> silver + edge merge.

Structured-Streaming variant of silver_build_financial. For each micro-batch:
1. Read new bronze rows via readStream from the Iceberg bronze table.
2. Reuse the same flatten logic (build_transactions from silver_build_financial)
   to produce silver.transactions rows.
3. Append to silver.transactions AND merge the (originator, beneficiary)
   aggregates into silver.counterparty_edges in a SINGLE foreachBatch handler.
4. Cache the per-batch txns DataFrame so build_transactions + edges don't
   recompute; the previous design paid ~2x per-batch by re-running the
   flatten from bronze inside the edges MERGE.

At-most-once vs exactly-once caveat (silver_stream #F7):
foreachBatch guarantees each batch's output is produced from the same
batchId, but the two writes inside are NOT atomic across a retry. If the
append() commits and the MERGE fails, Structured Streaming replays the
same batchId and both writes re-execute -- duplicating the txns rows and
double-adding to cumulative_amount_usd on surviving MERGE rows. The
correct fix requires either (a) a single Iceberg statement that writes
both tables, which Iceberg V2 doesn't expose, or (b) a two-phase
protocol keyed by batchId. Tracked as follow-up. For today, sustained
mode should be run against non-critical benchmark buckets; batch mode
(silver_build_financial) is the authoritative silver.

Entities/accounts are refreshed lazily by the batch silver_build;
streaming mode does not update the dimension tables to keep the
per-batch cost low.
"""

from __future__ import annotations

import signal
import time

from common import env, log
from pyspark.sql import SparkSession
from silver_build_financial import build_edges, build_transactions

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_EDGES = env("LB_FINANCIAL_SILVER_EDGES", "silver.counterparty_edges")
CHECKPOINT_URI = env(
    "LB_FINANCIAL_SILVER_CHECKPOINT", "s3a://lb-bronze/_checkpoints/silver_stream_financial/"
)
TRIGGER_S = int(env("LB_FINANCIAL_SILVER_TRIGGER_S", "30"))


def _merge_edges(batch_df, batch_id):
    """MERGE per-batch edge aggregates into the cumulative silver edges table.

    Caches the flattened txns for the same batch so build_transactions and
    build_edges don't scan the bronze rows twice (once for the txns.append
    and again inside the edges MERGE build path). Cast the MERGE arithmetic
    to decimal(38,2) rather than the DDL's (18,2): SUM of decimal columns
    widens to (38,2), and truncating back to (18,2) silently NULLs on
    overflow (ANSI-off default) which then fails the NOT NULL constraint
    on the second batch. Silver_build_financial has the matching fix.
    """
    spark = batch_df.sparkSession
    txns = build_transactions(batch_df).cache()
    try:
        n_txns = txns.count()
        log(f"[batch {batch_id}] {n_txns} txns")
        txns.writeTo(f"{CATALOG}.{SILVER_TXNS}").append()

        edges = build_edges(txns)
        edges.createOrReplaceTempView("_edge_updates")
        spark.sql(
            f"""
            MERGE INTO {CATALOG}.{SILVER_EDGES} t
            USING _edge_updates u
            ON t.source_entity_id = u.source_entity_id
               AND t.target_entity_id = u.target_entity_id
            WHEN MATCHED THEN UPDATE SET
                t.last_seen_ts = greatest(t.last_seen_ts, u.last_seen_ts),
                t.cumulative_amount_usd = CAST(
                    t.cumulative_amount_usd + u.cumulative_amount_usd
                    AS DECIMAL(38, 2)
                ),
                t.txn_count = t.txn_count + u.txn_count
            WHEN NOT MATCHED THEN INSERT (
                source_entity_id, target_entity_id,
                first_seen_ts, last_seen_ts,
                cumulative_amount_usd, txn_count
            ) VALUES (
                u.source_entity_id, u.target_entity_id,
                u.first_seen_ts, u.last_seen_ts,
                u.cumulative_amount_usd, u.txn_count
            )
            """
        )
        log(f"[batch {batch_id}] merged edges (source txns: {n_txns})")
    finally:
        txns.unpersist(blocking=False)


def main() -> None:
    spark = SparkSession.builder.appName("lb-silver-stream-financial").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    log("=" * 60)
    log("Silver Stream (Financial)")
    log(f"Source: {CATALOG}.{BRONZE_TABLE}")
    log(f"Sinks: {CATALOG}.{SILVER_TXNS} (append), {CATALOG}.{SILVER_EDGES} (merge)")
    log(f"Checkpoint: {CHECKPOINT_URI}")
    log(f"triggerSeconds={TRIGGER_S}")
    log("=" * 60)

    stream = spark.readStream.format("iceberg").load(f"{CATALOG}.{BRONZE_TABLE}")
    query = (
        stream.writeStream.foreachBatch(_merge_edges)
        .option("checkpointLocation", CHECKPOINT_URI)
        .trigger(processingTime=f"{TRIGGER_S} seconds")
        .start()
    )

    def _shutdown_handler(signum, frame):  # noqa: ARG001
        log(f"Signal {signum} received; stopping stream cleanly")
        try:
            query.stop()
        except Exception as e:  # noqa: BLE001
            log(f"query.stop failed: {e}")

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _shutdown_handler)
        except ValueError:
            pass

    while query.isActive:
        time.sleep(1)

    # Re-raise any streaming exception so silent PASS-with-zero-rows
    # can't happen; K8s Job status must reflect the real outcome.
    exc = query.exception()
    if exc is not None:
        log(f"Streaming query failed: {exc}")
        spark.stop()
        raise exc

    log("Silver stream stopped")
    if query.lastProgress:
        log(f"  batchId: {query.lastProgress.get('batchId')}")
        log(f"  processedRowsPerSecond: {query.lastProgress.get('processedRowsPerSecond')}")
    spark.stop()


if __name__ == "__main__":
    main()
