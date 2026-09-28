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
corpus. The datagen gives each entity (world entities and the screening
track's external accounts alike) one name and country, so the difference is
limited to opened_date (first date the stream saw).
"""

from __future__ import annotations

import signal
import threading
import time

from common import (
    assert_progress,
    clear_stream_started_marker,
    emit_stream_scale_admission,
    ensure_column,
    ensure_namespaces_for_ddl,
    ensure_partition_transform,
    env,
    log,
    log_job_metrics,
    mark_stream_started,
    refuse_fresh_checkpoint_over_data,
    replay_possible,
    streaming_query_id,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from silver_build_financial import (
    DDL_ACCOUNTS,
    DDL_BATCH_VERSIONS,
    DDL_EDGES,
    DDL_ENTITIES,
    DDL_PROFILES,
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
SILVER_STATEMENTS = env("LB_FINANCIAL_SILVER_STATEMENTS", "silver.account_statements")
SILVER_BATCH_VERSIONS = env("LB_FINANCIAL_SILVER_BATCH_VERSIONS", "silver.silver_batch_versions")
KYC_WAIT_S = int(env("LB_FINANCIAL_KYC_WAIT_S", "900"))
# I7: the KYC frame is reloaded on the first micro-batch after this many
# seconds have passed since the previous successful load. Default 1 hour;
# 0 disables the refresh (the frame is loaded once per process).
KYC_REFRESH_S = int(env("LB_STREAM_KYC_REFRESH_SECONDS", "3600"))

# Party/account masters joined into the dimensions: loaded once (see
# _kyc), None until then. _KYC_LOADED marks a completed attempt, since a
# pre-KYC corpus legitimately has no masters. _KYC_LOADED_AT is the unix
# time of the last successful load (0.0 while _KYC_LOADED is False) so a
# micro-batch after KYC_REFRESH_S has elapsed re-reads the masters.
_KYC = None
_KYC_LOADED = False
_KYC_LOADED_AT = 0.0


def _kyc(spark):
    """The KYC-by-IBAN frame. Loaded on the first call and re-read when
    KYC_REFRESH_S seconds have elapsed since the previous load, so a stream
    that runs for days picks up party/account master updates without a
    restart. Waits up to KYC_WAIT_S for both masters to be visible on the
    first load (a dedicated reference pod, or other pods' bronze, can beat
    them; the datagen writes party last, so a visible party means the rest
    is there). After the wait _read_reference decides, and raises unless
    the manifest proves a pre-KYC corpus: the stream fails loudly rather
    than write a run's dimensions with NULL KYC. A refresh reload skips
    the wait loop and keeps the previous cached frame on any transient
    read failure so a stream never stalls a micro-batch on KYC
    availability after the initial load succeeded; the next micro-batch
    retries."""
    global _KYC, _KYC_LOADED, _KYC_LOADED_AT
    now = time.time()
    if _KYC_LOADED and (KYC_REFRESH_S <= 0 or (now - _KYC_LOADED_AT) < KYC_REFRESH_S):
        return _KYC
    is_refresh = _KYC_LOADED
    if is_refresh:
        # I7 refresh: read once, keep the current cache on any failure
        # so the micro-batch is not blocked on a transient reference-file
        # read. _KYC_LOADED_AT is left unchanged so the next batch retries.
        try:
            party, account = reference_frames(spark)
        except Exception as e:  # noqa: BLE001
            log(f"[kyc] refresh read failed: {type(e).__name__}: {e}; keeping cached KYC")
            return _KYC
        if party is None or account is None:
            log("[kyc] refresh read incomplete (party or account missing); keeping cached KYC")
            return _KYC
    else:
        deadline = now + KYC_WAIT_S
        extended = False
        while True:
            party, account = reference_frames(spark)
            if party is not None and account is not None:
                break
            if time.time() >= deadline:
                if account is not None and not extended:
                    # Account is written before party: party is still uploading
                    # (5-10 GB at scale 1000). Give it one more wait.
                    deadline, extended = time.time() + KYC_WAIT_S, True
                    continue
                party, account = _read_reference(spark)
                break
            log(f"[kyc] party/account masters not there yet; waiting (up to {KYC_WAIT_S}s)")
            time.sleep(10)
    previous_kyc = _KYC
    kyc = build_kyc(party, account)
    # Drop the previous cached frame's blocks before the new load takes its
    # place; a leaked cache accumulates over hours of stream uptime.
    if is_refresh and previous_kyc is not None:
        try:
            previous_kyc.unpersist(blocking=False)
        except Exception as e:  # noqa: BLE001
            log(f"[kyc] previous frame unpersist failed: {type(e).__name__}: {e}")
    _KYC = kyc.cache() if kyc is not None else None
    _KYC_LOADED = True
    _KYC_LOADED_AT = now
    action = "reloaded" if is_refresh else "loaded"
    log(f"[kyc] masters {action if _KYC is not None else 'absent (pre-KYC corpus): KYC NULL'}")
    # Publish a per-refresh timestamp so operators see when the stream
    # last picked up KYC updates. Emitted twice: (i) as a plain labelled
    # `[lb] ... - kyc_refreshed_at: <unix>` line so a follow-on collector
    # regex extension in parse_streaming_logs can lift the value into
    # metrics.json without any wire-not-connected step; (ii) as a
    # `=== JOB METRICS: silver-stream-kyc-refresh ===` block for symmetry
    # with the plan's stated emit path and the extra_metrics allowlist in
    # parse_driver_logs. silver_stream_financial does not emit any other
    # JOB METRICS block today, so no shadowing risk on the current
    # streaming path; a future author adding a final-metrics block must
    # place it before the first refresh or key it under a distinct job
    # name (parse_driver_logs' regex is first-match).
    log(f"kyc_refreshed_at: {int(now)} kyc_refresh_kind: {'refresh' if is_refresh else 'initial'}")
    log_job_metrics(
        "silver-stream-kyc-refresh",
        input_size_gb=0.0,
        input_rows=0,
        output_rows=0,
        elapsed_seconds=0.0,
        kyc_refreshed_at=int(now),
        kyc_refresh_kind=("refresh" if is_refresh else "initial"),
    )
    return _KYC


def append_new_dimensions(spark, batch_df, txns, kyc) -> tuple[int, int]:
    """Append the entities and accounts this batch introduces. Anti-join on
    the key against the table, so a replayed batch writes nothing twice."""
    from pyspark.sql.functions import col

    ents = build_entities(txns.drop("_batch_id", "_stream_id"), batch_df, kyc)
    have = spark.table(f"{CATALOG}.{SILVER_ENTITIES}").select(col("entity_id").alias("_have"))
    new_ents = ents.join(have, ents["entity_id"] == have["_have"], "left_anti")
    accts = build_accounts(batch_df, kyc)
    have_a = spark.table(f"{CATALOG}.{SILVER_ACCOUNTS}").select(col("iban").alias("_have"))
    new_accts = accts.join(have_a, accts["iban"] == have_a["_have"], "left_anti")
    # One small file per table per batch, not one per shuffle partition: the
    # dimensions are append-only in continuous mode and nothing compacts them.
    # repartition, not coalesce: coalesce would pull the anti-join itself into
    # one task, and the first batch carries nearly every entity.
    new_ents = new_ents.repartition(1).cache()
    new_accts = new_accts.repartition(1).cache()
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


def _merge_batch(batch_df, batch_id: int) -> int:
    """Idempotently write per-batch txns and edges tagged with batch_id.

    Caches the flattened txns so build_transactions and build_edges don't
    scan the bronze rows twice. Casts the per-batch aggregate to
    decimal(38,2) matching the DDL (SUM of decimal columns widens; the
    old (18,2) would silently NULL on overflow under ANSI-off).

    Returns the silver.transactions row count this batch committed
    (0 for an empty batch). The A1 driver-side accumulator in main() sums
    these across batches to feed assert_progress.
    """
    spark = batch_df.sparkSession
    # B2: compose the DELETE key with the streaming query id so a fresh
    # checkpoint's batch 0 does not stomp the previous stream's batch 0.
    # streaming_query_id() must be read inside foreachBatch: Spark sets
    # it as a local property on the micro-batch thread.
    sid = streaming_query_id(spark)
    # I5: only the first micro-batch of a query run can be a replay of a
    # batch a previous run committed (a failed batch stops its query and
    # the driver exits), so the DELETE that cleans a partial replay runs
    # only then. Post-first batches skip it and the Iceberg snapshot log
    # gets one delete per restart instead of one per trigger. Called
    # before the empty-batch short-circuit so the run is registered even
    # when its first micro-batch reads zero rows -- mirrors silver_stream.
    check_replay = replay_possible(spark)
    tagged_txns = (
        build_transactions(batch_df)
        # Overwrite the batch column with the real batchId. Cheaper than
        # projecting the schema again, and mirrors the same pattern the
        # per-batch edges do below.
        .withColumn("_batch_id", lit(int(batch_id)).cast("bigint"))
        # Every stream-written row also carries its streaming query id
        # so the DELETE key is (stream, batch); batch-mode writes stamp
        # 'batch' and are never matched.
        .withColumn("_stream_id", lit(sid))
        .cache()
    )
    try:
        n_txns = tagged_txns.count()
        log(f"[batch {batch_id}] {n_txns} txns")
        if n_txns == 0:
            # Collector line format (LB-136); counts the batch, adds no rows.
            log(f"Batch {batch_id}: empty, skipping")
            return 0
        log(f"Batch {batch_id}: transforming {n_txns:,} rows")
        t0 = time.time()

        # PHASE 1: silver.transactions.
        # DELETE first so a retry after a partial append doesn't leave
        # ghost rows behind; INSERT then re-materializes the batch. On a
        # first attempt DELETE is a no-op (nothing matches). Iceberg V2
        # supports row-level DELETE; both COW and MoR configurations work.
        # B2 + I5: predicate is (stream, batch), never bare batch id; the
        # DELETE only runs on the first micro-batch after a restart so the
        # Iceberg snapshot log stays at one delete per restart.
        if check_replay:
            spark.sql(
                f"DELETE FROM {CATALOG}.{SILVER_TXNS} "
                f"WHERE _stream_id = '{sid}' AND _batch_id = {int(batch_id)}"
            )
        tagged_txns.writeTo(f"{CATALOG}.{SILVER_TXNS}").append()

        # PHASE 2: silver.counterparty_edges.
        # Per-batch aggregates only; cumulative sums are computed on read
        # by consumers via SUM(cumulative_amount_usd) GROUP BY
        # source_entity_id, target_entity_id (FQ3 already does this).
        edges_batch = (
            build_edges(tagged_txns.drop("_batch_id", "_stream_id"))
            .withColumn("_batch_id", lit(int(batch_id)).cast("bigint"))
            .withColumn("_stream_id", lit(sid))
        )
        # B2 + I5: same shape as the transactions DELETE above.
        if check_replay:
            spark.sql(
                f"DELETE FROM {CATALOG}.{SILVER_EDGES} "
                f"WHERE _stream_id = '{sid}' AND _batch_id = {int(batch_id)}"
            )
        edges_batch.writeTo(f"{CATALOG}.{SILVER_EDGES}").append()

        log(f"[batch {batch_id}] appended edges idempotently (source txns: {n_txns})")

        # PHASE 3: dimensions this batch introduces (entities, accounts).
        n_e, n_a = append_new_dimensions(spark, batch_df, tagged_txns, _kyc(spark))
        log(f"[batch {batch_id}] appended {n_e} new entities, {n_a} new accounts")

        # PHASE 4 (I10 sealed marker): only after transactions AND edges have
        # committed do we write the (stream_id, batch_id) row that gold-side
        # consumers semi-join against. A driver crash between phase 1 and this
        # phase leaves txns and edges rows visible in silver, but no matching
        # versions row, so consumers hide the partial batch. On the retry the
        # phase-1 DELETE (guarded by replay_possible) cleans the ghost rows
        # before the same (sid, batch_id) is re-materialised; the MERGE below
        # then seals the retry idempotently.
        #
        # MERGE (not INSERT): a crash after this write but before Structured
        # Streaming durably records the batch id would replay the whole
        # foreachBatch on restart; a plain INSERT would then write a second
        # row for the same (sid, batch_id). The semi-join tolerates
        # duplicates today, but any future COUNT(*) or per-batch join against
        # silver_batch_versions would double-count. The MERGE keeps the
        # sidecar at exactly one row per sealed batch.
        spark.sql(
            f"MERGE INTO {CATALOG}.{SILVER_BATCH_VERSIONS} v "
            f"USING (SELECT '{sid}' AS stream_id, "
            f"CAST({int(batch_id)} AS BIGINT) AS batch_id, "
            f"current_timestamp() AS committed_at) s "
            f"ON v.stream_id = s.stream_id AND v.batch_id = s.batch_id "
            f"WHEN NOT MATCHED THEN INSERT *"
        )
        log(f"[batch {batch_id}] sealed marker written to {SILVER_BATCH_VERSIONS}")

        log(f"Batch {batch_id}: committed to {SILVER_TXNS} in {time.time() - t0:.1f}s")
        return int(n_txns)
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
    # G5: label the imposed scale envelope so downstream reports never
    # present a Lakebench-bounded number as infrastructure performance
    # (invariant 6). v1.6 profile: measured up to scale 10; larger scales
    # run but the cap is labelled, not hidden.
    emit_stream_scale_admission(measured_envelope_scale=10)

    # LB-127: create the silver tables if absent. In CONTINUOUS mode
    # silver_build never runs, so nothing else creates silver.transactions /
    # silver.counterparty_edges (and the dimensions) -- the per-batch
    # writeTo(...).append() below requires them to exist, and without this
    # every micro-batch failed, leaving silver empty and the gold detection
    # stage with nothing to scan. Mirrors silver_build_financial.main()'s
    # bootstrap loop (same DDL constants) so both modes converge on one
    # schema. All CREATE TABLE IF NOT EXISTS -- idempotent on restart.
    # H3: bootstrap entity_profiles too, even though D-safe stream does not
    # write to it. A mixed batch+stream deployment (or a continuous-only
    # deployment on a fresh catalog) that never runs silver_build_financial
    # would otherwise leave silver.entity_profiles absent, and any gold-side
    # consumer that reads it fails with "table not found".
    ensure_namespaces_for_ddl(
        spark,
        CATALOG,
        (
            DDL_TXNS,
            DDL_ENTITIES,
            DDL_ACCOUNTS,
            DDL_STATEMENTS,
            DDL_EDGES,
            DDL_PROFILES,
            DDL_BATCH_VERSIONS,
        ),
    )
    for _name, _ddl in (
        ("transactions", DDL_TXNS),
        ("entities", DDL_ENTITIES),
        ("accounts", DDL_ACCOUNTS),
        ("account_statements", DDL_STATEMENTS),
        ("edges", DDL_EDGES),
        ("entity_profiles", DDL_PROFILES),
        # I10: sidecar seals every (stream_id, batch_id) after phases 1+2
        # commit; every gold/score reader semi-joins against it.
        ("silver_batch_versions", DDL_BATCH_VERSIONS),
    ):
        spark.sql(_ddl)
        log(f"[startup] bootstrapped silver.{_name}")

    # LB-109: guarantee the idempotency-key column exists on the target
    # tables before the first micro-batch fires. Idempotent on re-runs.
    ensure_column(spark, f"{CATALOG}.{SILVER_TXNS}", "_batch_id", "BIGINT")
    ensure_column(spark, f"{CATALOG}.{SILVER_EDGES}", "_batch_id", "BIGINT")
    # B2: _stream_id scopes _batch_id per streaming query so a fresh
    # checkpoint's batch 0 does not collide with the previous stream's.
    # Idempotent on re-runs; adds the column on a reused catalog.
    ensure_column(spark, f"{CATALOG}.{SILVER_TXNS}", "_stream_id", "STRING")
    ensure_column(spark, f"{CATALOG}.{SILVER_EDGES}", "_stream_id", "STRING")
    ensure_column(spark, f"{CATALOG}.{SILVER_TXNS}", "ingest_ts", "TIMESTAMP")

    # B3: refuse a fresh checkpoint over populated silver -- the source
    # would start from the beginning and every existing row would be
    # duplicated. Uniform SilverAbort exit contract (see common.py).
    refuse_fresh_checkpoint_over_data(
        spark,
        CHECKPOINT_URI,
        [f"{CATALOG}.{SILVER_TXNS}", f"{CATALOG}.{SILVER_EDGES}"],
    )
    # H2: match silver_build_financial's partition evolution so a
    # continuous-only deployment on a reused catalog does not drift on the
    # old days() spec. Iceberg partition evolution is metadata-only (~1s)
    # and idempotent; safe to run at every startup.
    ensure_partition_transform(
        spark, f"{CATALOG}.{SILVER_TXNS}", "days(txn_timestamp)", "months(txn_timestamp)"
    )
    ensure_partition_transform(
        spark,
        f"{CATALOG}.{SILVER_STATEMENTS}",
        "days(book_ts)",
        "months(book_ts)",
    )
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
    # A1: driver-side accumulator. _merge_batch returns the silver.transactions
    # rows committed for the batch; the wrapper folds each into a
    # lock-protected counter so a stream that saw only empty batches raises
    # SilverAbort after await. Spark's async ListenerBus swallows Python
    # listener exceptions, so the check runs on the main thread that blocks
    # on `query.isActive`, not inside a StreamingQueryListener.
    rows_written_total = 0
    rows_written_lock = threading.Lock()

    def _foreach_batch(df, bid):
        written = _merge_batch(df, bid)
        with rows_written_lock:
            nonlocal rows_written_total
            rows_written_total += int(written or 0)

    query = (
        stream.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", CHECKPOINT_URI)
        .trigger(processingTime=f"{TRIGGER_S} seconds")
        .start()
    )

    # H4: write the stream-started marker after the query has actually
    # started so silver_build_financial refuses to run against this
    # deployment. The marker is cleared on clean shutdown below; a crash
    # or kill -9 leaves it in place, which is what we want (batch must
    # not silently overwrite a mid-flight stream's tables).
    mark_stream_started(spark, CHECKPOINT_URI)

    def _shutdown_handler(signum, frame):  # noqa: ARG001
        log(f"Signal {signum} received; stopping stream cleanly")
        try:
            query.stop()
        except Exception as e:  # noqa: BLE001
            log(f"query.stop failed: {e}")
        # H4: clean shutdown removes the marker so a subsequent batch
        # run against a decommissioned continuous deployment is not
        # blocked. A crash bypasses this handler entirely.
        clear_stream_started_marker(spark, CHECKPOINT_URI)

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
    with rows_written_lock:
        total = rows_written_total
    log(f"Silver stream total rows written across all batches: {total}")
    # H4: clean shutdown removes the marker. The signal handler above
    # also calls this; the double-call is idempotent (delete-if-exists).
    # Both paths matter: process exits via SIGTERM in K8s Job termination,
    # but the query can also drain naturally at end of run_duration.
    clear_stream_started_marker(spark, CHECKPOINT_URI)
    assert_progress(total, "silver-stream")
    spark.stop()


if __name__ == "__main__":
    main()
