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

Dimensions (entities, accounts): continuous mode never runs the batch
silver_build, so each micro-batch appends the entities and accounts it
introduces (anti-join on entity_id / iban against the table), carrying
country and the monitored-population / KYC columns from the party and
account masters. The anti-join makes a retried batch a no-op for rows it
already wrote. The masters are read once, when they appear: the datagen
writes them before its first bronze file, and a batch that arrives first
waits for them (LB_FINANCIAL_KYC_WAIT_S), so no entity is written with
NULL KYC that a moment later would have had it; if they never appear,
the stream fails unless the manifest proves a pre-KYC corpus.

Known differences from batch mode: an entity's name, type and (for
LEI-keyed entities) country, and an account's holder and opened_date, come
from the first micro-batch that sees them rather than from the whole
corpus. The datagen gives each entity one name and country, so the
difference is limited to opened_date (first date the stream saw).
"""

from __future__ import annotations

import signal
import time

from common import ensure_column, env, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from silver_build_financial import (
    DDL_ACCOUNTS,
    DDL_EDGES,
    DDL_ENTITIES,
    DDL_STATEMENTS,
    DDL_TXNS,
    KYC_ACCOUNT_COLUMNS,
    KYC_ENTITY_COLUMNS,
    _read_reference,
    build_accounts,
    build_edges,
    build_entities,
    build_kyc,
    build_transactions,
    reference_frames,
)

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_EDGES = env("LB_FINANCIAL_SILVER_EDGES", "silver.counterparty_edges")
CHECKPOINT_URI = env(
    "LB_FINANCIAL_SILVER_CHECKPOINT", "s3a://lb-bronze/_checkpoints/silver_stream_financial/"
)
TRIGGER_S = int(env("LB_FINANCIAL_SILVER_TRIGGER_S", "30"))
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_ACCOUNTS = env("LB_FINANCIAL_SILVER_ACCOUNTS", "silver.accounts")
KYC_WAIT_S = int(env("LB_FINANCIAL_KYC_WAIT_S", "900"))

# Party/account masters joined into the dimensions: loaded once (see
# _kyc), None until then. _KYC_LOADED marks a completed attempt, since a
# pre-KYC corpus legitimately has no masters.
_KYC = None
_KYC_LOADED = False


def _kyc(spark):
    """The KYC-by-IBAN frame, read once. Waits up to KYC_WAIT_S for both
    masters to be visible (a dedicated reference pod, or other pods' bronze,
    can beat them; the datagen writes party last, so a visible party means
    the rest is there). After the wait _read_reference decides, and raises
    unless the manifest proves a pre-KYC corpus: the stream fails loudly
    rather than write a run's dimensions with NULL KYC."""
    global _KYC, _KYC_LOADED
    if _KYC_LOADED:
        return _KYC
    deadline = time.time() + KYC_WAIT_S
    while True:
        party, account = reference_frames(spark)
        if party is not None and account is not None:
            break
        if time.time() >= deadline:
            party, account = _read_reference(spark)
            break
        log(f"[kyc] party/account masters not there yet; waiting (up to {KYC_WAIT_S}s)")
        time.sleep(10)
    kyc = build_kyc(party, account)
    _KYC = kyc.cache() if kyc is not None else None
    _KYC_LOADED = True
    log(f"[kyc] masters {'loaded' if _KYC is not None else 'absent (pre-KYC corpus): KYC NULL'}")
    return _KYC


def append_new_dimensions(spark, batch_df, txns, kyc) -> tuple[int, int]:
    """Append the entities and accounts this batch introduces. Anti-join on
    the key against the table, so a replayed batch writes nothing twice."""
    from pyspark.sql.functions import col

    ents = build_entities(txns.drop("_batch_id"), batch_df, kyc)
    have = spark.table(f"{CATALOG}.{SILVER_ENTITIES}").select(col("entity_id").alias("_have"))
    new_ents = ents.join(have, ents["entity_id"] == have["_have"], "left_anti")
    accts = build_accounts(batch_df, kyc)
    have_a = spark.table(f"{CATALOG}.{SILVER_ACCOUNTS}").select(col("iban").alias("_have"))
    new_accts = accts.join(have_a, accts["iban"] == have_a["_have"], "left_anti")
    # One small file per table per batch, not one per shuffle partition: the
    # dimensions are append-only in continuous mode and nothing compacts them.
    new_ents = new_ents.coalesce(1).cache()
    new_accts = new_accts.coalesce(1).cache()
    try:
        n_e, n_a = new_ents.count(), new_accts.count()
        if n_e:
            new_ents.writeTo(f"{CATALOG}.{SILVER_ENTITIES}").append()
        if n_a:
            new_accts.writeTo(f"{CATALOG}.{SILVER_ACCOUNTS}").append()
        return n_e, n_a
    finally:
        new_ents.unpersist(blocking=False)
        new_accts.unpersist(blocking=False)


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
        if n_txns == 0:
            # Collector line format (LB-136); counts the batch, adds no rows.
            log(f"Batch {batch_id}: empty, skipping")
            return
        log(f"Batch {batch_id}: transforming {n_txns:,} rows")
        t0 = time.time()

        # PHASE 1: silver.transactions.
        # DELETE first so a retry after a partial append doesn't leave
        # ghost rows behind; INSERT then re-materializes the batch. On a
        # first attempt DELETE is a no-op (nothing matches). Iceberg V2
        # supports row-level DELETE; both COW and MoR configurations work.
        spark.sql(f"DELETE FROM {CATALOG}.{SILVER_TXNS} WHERE _batch_id = {int(batch_id)}")
        tagged_txns.writeTo(f"{CATALOG}.{SILVER_TXNS}").append()

        # PHASE 2: silver.counterparty_edges.
        # Per-batch aggregates only; cumulative sums are computed on read
        # by consumers via SUM(cumulative_amount_usd) GROUP BY
        # source_entity_id, target_entity_id (FQ3 already does this).
        edges_batch = build_edges(tagged_txns.drop("_batch_id")).withColumn(
            "_batch_id", lit(int(batch_id)).cast("bigint")
        )
        spark.sql(f"DELETE FROM {CATALOG}.{SILVER_EDGES} WHERE _batch_id = {int(batch_id)}")
        edges_batch.writeTo(f"{CATALOG}.{SILVER_EDGES}").append()

        log(f"[batch {batch_id}] appended edges idempotently (source txns: {n_txns})")

        # PHASE 3: dimensions this batch introduces (entities, accounts).
        n_e, n_a = append_new_dimensions(spark, batch_df, tagged_txns, _kyc(spark))
        log(f"[batch {batch_id}] appended {n_e} new entities, {n_a} new accounts")
        log(f"Batch {batch_id}: committed to {SILVER_TXNS} in {time.time() - t0:.1f}s")
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

    # LB-127: create the silver tables if absent. In CONTINUOUS mode
    # silver_build never runs, so nothing else creates silver.transactions /
    # silver.counterparty_edges (and the dimensions) -- the per-batch
    # writeTo(...).append() below requires them to exist, and without this
    # every micro-batch failed, leaving silver empty and the gold detection
    # stage with nothing to scan. Mirrors silver_build_financial.main()'s
    # bootstrap loop (same DDL constants) so both modes converge on one
    # schema. All CREATE TABLE IF NOT EXISTS -- idempotent on restart.
    for _name, _ddl in (
        ("transactions", DDL_TXNS),
        ("entities", DDL_ENTITIES),
        ("accounts", DDL_ACCOUNTS),
        ("account_statements", DDL_STATEMENTS),
        ("edges", DDL_EDGES),
    ):
        spark.sql(_ddl)
        log(f"[startup] bootstrapped silver.{_name}")

    # LB-109: guarantee the idempotency-key column exists on the target
    # tables before the first micro-batch fires. Idempotent on re-runs.
    ensure_column(spark, f"{CATALOG}.{SILVER_TXNS}", "_batch_id", "BIGINT")
    ensure_column(spark, f"{CATALOG}.{SILVER_EDGES}", "_batch_id", "BIGINT")
    ensure_column(spark, f"{CATALOG}.{SILVER_TXNS}", "ingest_ts", "TIMESTAMP")
    # P10 stage 0/2 columns on a reused catalog (same list as silver_build).
    for table, columns in (
        (SILVER_ENTITIES, KYC_ENTITY_COLUMNS),
        (SILVER_ACCOUNTS, KYC_ACCOUNT_COLUMNS),
    ):
        for name, sql_type in columns:
            ensure_column(spark, f"{CATALOG}.{table}", name, sql_type.upper())

    # LB-127: the bronze table carries an OVERWRITE snapshot from the
    # bronze-verify preflight (LB_REGISTER_TABLE=1 does a full CTAS/register
    # of the pacs.008 corpus). Iceberg's streaming source refuses overwrite
    # (and delete) snapshots by default and throws
    # "Cannot process overwrite snapshot", crash-looping silver-stream so
    # silver never fills. Skip non-append snapshots: bronze-ingest writes
    # append-only micro-batches, which is what silver must consume.
    stream = (
        spark.readStream.format("iceberg")
        .option("streaming-skip-overwrite-snapshots", "true")
        .option("streaming-skip-delete-snapshots", "true")
        .load(f"{CATALOG}.{BRONZE_TABLE}")
    )
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
