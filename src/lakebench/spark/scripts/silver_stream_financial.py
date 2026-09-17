"""Silver Stream (Financial, sustained) -- incremental bronze -> silver + edge merge.

Structured-Streaming variant of silver_build_financial. For each micro-batch:
1. Read new bronze rows via readStream.
2. Reuse the same flatten logic (build_transactions from silver_build_financial)
   to produce silver.transactions rows.
3. Append to silver.transactions.
4. MERGE the (originator, beneficiary) aggregates into silver.counterparty_edges
   in a foreachBatch handler so cumulative_amount_usd and last_seen_ts stay
   monotonic across micro-batches.

Entities/accounts are refreshed lazily by the batch silver_build; streaming
mode does not update the dimension tables to keep the per-batch cost low.
"""

from __future__ import annotations

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
    """MERGE per-batch edge aggregates into the cumulative silver edges table."""
    spark = batch_df.sparkSession
    txns = build_transactions(batch_df)
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
            t.cumulative_amount_usd = t.cumulative_amount_usd + u.cumulative_amount_usd,
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
    log(f"[batch {batch_id}] merged {edges.count()} edge deltas")


def main() -> None:
    spark = SparkSession.builder.appName("lb-silver-stream-financial").getOrCreate()

    log("=" * 60)
    log("Silver Stream (Financial)")
    log(f"Source: {CATALOG}.{BRONZE_TABLE}")
    log(f"Sinks: {CATALOG}.{SILVER_TXNS} (append), {CATALOG}.{SILVER_EDGES} (merge)")
    log(f"triggerSeconds={TRIGGER_S}")
    log("=" * 60)

    stream = spark.readStream.format("iceberg").load(f"{CATALOG}.{BRONZE_TABLE}")
    query = (
        stream.writeStream.foreachBatch(_merge_edges)
        .option("checkpointLocation", CHECKPOINT_URI)
        .trigger(processingTime=f"{TRIGGER_S} seconds")
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
