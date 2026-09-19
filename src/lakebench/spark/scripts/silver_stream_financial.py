"""Silver Stream (Financial, sustained) -- incremental bronze -> silver + edges.

Structured-Streaming variant of silver_build_financial. For each micro-batch:
1. Read new bronze rows via readStream from the Iceberg bronze table.
2. Reuse the same flatten logic (build_transactions from silver_build_financial)
   to produce silver.transactions rows.
3. Idempotently append per-batch txns AND per-batch counterparty edges,
   tagging every row with the Structured Streaming batchId.
4. Cache the per-batch txns DataFrame so build_transactions + edges don't
   recompute; the previous design paid ~2x per-batch by re-running the
   flatten from bronze inside the edges MERGE.

Retry idempotency (LB-109 fix):
Structured Streaming retries the entire `foreachBatch` handler on failure,
re-running with the same batchId. The prior design did an `append()` to
silver.transactions plus a MERGE into silver.counterparty_edges as two
separate Iceberg commits; a failure between them replayed both, silently
duplicating txn rows and double-counting cumulative_amount_usd.

The two-phase batchId protocol used here removes that risk:
  * Both target tables carry a nullable `_batch_id BIGINT` column
    (added to the CREATE TABLE in silver_build_financial and expected
    to be present in existing deployments; ALTER-add is idempotent).
  * On every batch we DELETE FROM <table> WHERE _batch_id = <batchId>
    then INSERT the new rows tagged with the same _batch_id.
  * DELETE is a no-op the first time and cleans up any prior half-written
    replay attempt; the subsequent INSERT is deterministic in (batchId,
    source data). Net effect: a retry produces exactly one clean copy of
    the batch's rows for that _batch_id, regardless of where the previous
    attempt failed.

Cumulative aggregates (previously computed by the MERGE) are now derived
on read via SUM(cumulative_amount_usd), SUM(txn_count) GROUP BY
source_entity_id, target_entity_id -- benchmark query FQ3 already reads
this way, and detection rules do not consume counterparty_edges directly.

Semantics of per-batch rows: `first_seen_ts` and `last_seen_ts` on a
streamed row are the min/max WITHIN the writing micro-batch, not the
lifetime min/max for the (source, target) pair. Consumers that need
lifetime bounds must aggregate: MIN(first_seen_ts), MAX(last_seen_ts)
GROUP BY source_entity_id, target_entity_id. The column name
`cumulative_amount_usd` is preserved for schema compatibility with
batch mode, but under streaming it is a per-batch total, not
cumulative -- callers still SUM to get the cumulative.

Mode boundary (important operational contract): silver_build_financial
in batch mode overwrites silver.counterparty_edges (and silver.transactions)
via `.writeTo(...).overwrite(lit(True))`. Running silver_build after
silver_stream has been running WILL WIPE every streamed row. Batch and
sustained modes are mutually exclusive per deployment; do not mix them
against the same table. Batch-mode silver_build writes with _batch_id
= NULL (one row per pair, unchanged from before), so a fresh batch
deployment sees no change.

Entities/accounts are refreshed lazily by the batch silver_build;
streaming mode does not update the dimension tables to keep the
per-batch cost low.
"""

from __future__ import annotations

import signal
import time

from common import env, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from silver_build_financial import build_edges, build_transactions

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_EDGES = env("LB_FINANCIAL_SILVER_EDGES", "silver.counterparty_edges")
CHECKPOINT_URI = env(
    "LB_FINANCIAL_SILVER_CHECKPOINT", "s3a://lb-bronze/_checkpoints/silver_stream_financial/"
)
TRIGGER_S = int(env("LB_FINANCIAL_SILVER_TRIGGER_S", "30"))


def _ensure_batch_id_column(spark, table: str) -> None:
    """Add the `_batch_id` column if the target table was created by an
    older silver_build_financial (pre-LB-109). Iceberg's ADD COLUMN IF
    NOT EXISTS is idempotent, so running this every stream startup costs
    one metadata read on subsequent restarts.
    """
    try:
        spark.sql(
            f"ALTER TABLE {CATALOG}.{table} ADD COLUMN IF NOT EXISTS _batch_id BIGINT"
        )
    except Exception as e:  # noqa: BLE001
        # Some catalog implementations reject ADD COLUMN IF NOT EXISTS
        # against a table that already has the column. Fall through: if
        # the column really is missing the first INSERT below will fail
        # loud, which is the desired behavior for a stale table.
        log(f"[startup] ADD COLUMN _batch_id on {table} skipped: {e}")


def _merge_batch(batch_df, batch_id: int) -> None:
    """Idempotently write per-batch txns and edges tagged with batch_id.

    Caches the flattened txns so build_transactions and build_edges don't
    scan the bronze rows twice. Casts the per-batch aggregate to
    decimal(38,2) matching the DDL (SUM of decimal columns widens; the
    old (18,2) would silently NULL on overflow under ANSI-off).
    """
    spark = batch_df.sparkSession
    tagged_txns = (
        build_transactions(batch_df)
        # Overwrite the batch column with the real batchId. Cheaper than
        # projecting the schema again, and mirrors the same pattern the
        # per-batch edges do below.
        .withColumn("_batch_id", lit(int(batch_id)).cast("bigint"))
        .cache()
    )
    try:
        n_txns = tagged_txns.count()
        log(f"[batch {batch_id}] {n_txns} txns")

        # PHASE 1: silver.transactions.
        # DELETE first so a retry after a partial append doesn't leave
        # ghost rows behind; INSERT then re-materializes the batch. On a
        # first attempt DELETE is a no-op (nothing matches). Iceberg V2
        # supports row-level DELETE; both COW and MoR configurations work.
        spark.sql(
            f"DELETE FROM {CATALOG}.{SILVER_TXNS} WHERE _batch_id = {int(batch_id)}"
        )
        tagged_txns.writeTo(f"{CATALOG}.{SILVER_TXNS}").append()

        # PHASE 2: silver.counterparty_edges.
        # Per-batch aggregates only; cumulative sums are computed on read
        # by consumers via SUM(cumulative_amount_usd) GROUP BY
        # source_entity_id, target_entity_id (FQ3 already does this).
        edges_batch = (
            build_edges(tagged_txns.drop("_batch_id"))
            .withColumn("_batch_id", lit(int(batch_id)).cast("bigint"))
        )
        spark.sql(
            f"DELETE FROM {CATALOG}.{SILVER_EDGES} WHERE _batch_id = {int(batch_id)}"
        )
        edges_batch.writeTo(f"{CATALOG}.{SILVER_EDGES}").append()

        log(f"[batch {batch_id}] appended edges idempotently (source txns: {n_txns})")
    finally:
        tagged_txns.unpersist(blocking=False)


def main() -> None:
    spark = SparkSession.builder.appName("lb-silver-stream-financial").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    log("=" * 60)
    log("Silver Stream (Financial)")
    log(f"Source: {CATALOG}.{BRONZE_TABLE}")
    log(f"Sinks: {CATALOG}.{SILVER_TXNS} (delete+append), {CATALOG}.{SILVER_EDGES} (delete+append)")
    log(f"Checkpoint: {CHECKPOINT_URI}")
    log(f"triggerSeconds={TRIGGER_S}")
    log("=" * 60)

    # LB-109: guarantee the idempotency-key column exists on the target
    # tables before the first micro-batch fires. Idempotent on re-runs.
    _ensure_batch_id_column(spark, SILVER_TXNS)
    _ensure_batch_id_column(spark, SILVER_EDGES)

    stream = spark.readStream.format("iceberg").load(f"{CATALOG}.{BRONZE_TABLE}")
    query = (
        stream.writeStream.foreachBatch(_merge_batch)
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
