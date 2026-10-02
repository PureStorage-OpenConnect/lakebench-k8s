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
silver_build, so each micro-batch merges the entities and accounts it
introduces into the silver dimensions, carrying country and the
monitored-population / KYC columns from the party and account masters.
The MERGE (E1) preserves the LEAST() of the target row and the batch row
on the dimension columns batch mode collapses with min() (entities:
name, entity_type re-derived from the merged name, legal_name, country;
accounts: holder_entity_id, bank_bic, currency, opened_date). This
closes the pre-E1 first-batch-wins parity gap between stream and batch
against identical bronze. The masters are read once, when they appear:
the datagen writes them before its first bronze file, and a batch that
arrives first waits for them (LB_FINANCIAL_KYC_WAIT_S), so no entity is
written with NULL KYC that a moment later would have had it; if they
never appear, the stream fails unless the manifest proves a pre-KYC
corpus. KYC columns are set at first insert and left alone on later
batches (they are the same across a stream's lifetime); a KYC refresh
that changes a value picks up on the NEXT run, matching pre-E1
behaviour.
"""

from __future__ import annotations

import os
import signal
import threading
import time

from common import (
    SilverAbort,
    aml_opening_balance,
    assert_progress,
    clear_stream_started_marker,
    emit_stream_scale_admission,
    ensure_column,
    ensure_namespaces_for_ddl,
    ensure_partition_transform,
    entity_type_from_name_sql,
    env,
    log,
    log_job_metrics,
    mark_stream_started,
    materialised_source,
    refuse_fresh_checkpoint_over_data,
    replay_possible,
    sealed_txns_filter,
    streaming_query_id,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg as avg_,
)
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    greatest,
    lit,
)
from pyspark.sql.functions import (
    count as count_,
)
from pyspark.sql.functions import (
    countDistinct as count_distinct_,
)
from pyspark.sql.functions import (
    max as max_,
)
from pyspark.sql.functions import (
    min as min_,
)
from pyspark.sql.functions import (
    sum as sum_,
)
from pyspark.sql.functions import (
    variance as variance_,
)
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
# D-full-profiles: silver.entity_profiles is now maintained by the stream via
# an Iceberg MERGE with Welford + additive updates (see _merge_profiles). The
# MERGE is self-idempotent on (t._stream_id, t._batch_id) so a retry after a
# post-MERGE crash does not double-apply the additive deltas; the versions
# sidecar is I10's (silver.silver_batch_versions), owned there and consumed
# here for the sealed-txns filter in the distinct-counterparty recompute.
SILVER_PROFILES = env("LB_FINANCIAL_SILVER_PROFILES", "silver.entity_profiles")
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

# E1 (dimension parity): temp view names the per-batch MERGE reuses.
# foreachBatch runs sequentially per streaming query on the driver, so
# fixed names are safe within a single query. Two concurrent stream
# queries in the same JVM (not a supported deployment mode today) would
# need per-query naming.
_ENTITIES_MERGE_VIEW = "_silver_stream_dim_merge_entities"
_ACCOUNTS_MERGE_VIEW = "_silver_stream_dim_merge_accounts"
_BALANCES_MERGE_VIEW = "_d_full_balances"


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
    # One label per log() call so the collector's per-line regex does not
    # swallow the second key into the first key's value. Reviewer-caught
    # silent-drop shape (2026-09-28): `<k1>: <v1> <k2>: <v2>` matched only
    # k1 because the value pattern was greedy; k2's rows were lost.
    log(f"kyc_refreshed_at: {int(now)}")
    log(f"kyc_refresh_kind: {'refresh' if is_refresh else 'initial'}")
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


STATEMENTS_STRICT_PARITY_ENV = "LB_SILVER_STATEMENTS_STRICT_PARITY"


def _maintain_statements(
    batch_df,
    batch_id: int,
    sid: str,
    check_replay: bool,
) -> tuple[int, int]:
    """D-full-simple: idempotently maintain silver.account_statements +
    roll up silver.accounts.current_balance for this micro-batch.

    Reads the batch's touched iban set from bronze, looks up each iban's
    latest ``bal_after`` from silver.account_statements (or the deterministic
    opening_balance formula for a first-touch account), computes new
    statement rows with ``bal_after = prior + cumsum(signed_amt)``, tags
    each row with ``(_stream_id, _batch_id)`` for replay idempotency,
    DELETE+INSERTs into the table, and MERGEs the latest per-iban bal_after
    into silver.accounts.current_balance.

    Parity with batch build (``build_statements`` / ``update_accounts_balance``):
    byte-identical ONLY when bronze rows arrive per-iban strictly monotone in
    book_ts across micro-batches -- the associativity of the running sum
    reproduces batch mode's global cumsum under the same (book_ts, txn_id,
    _cdt_dbt_ord) tie-break. When a batch delivers a row whose book_ts is
    earlier than the previous highest committed book_ts for that iban
    (typical under multi-pod datagen -- files land out of write-time order
    on the object store), the stream cannot reorder rows already committed,
    so ``entry_seq`` picks up in arrival order and ``bal_after`` lands out
    of book_ts order. That is silently correct as arrival-order running
    balance, and silently WRONG as batch-mode strict parity: the two write
    paths diverge.

    Late arrivals are detected per batch (min this-batch book_ts per iban
    vs silver.account_statements' max prior book_ts per iban) and reported
    as invariant-6 labels through the caller-returned tuple:
    ``(entries_written, late_iban_count)``. The caller emits the labels
    per micro-batch. When ``LB_SILVER_STATEMENTS_STRICT_PARITY=1`` AND any
    late iban is seen, ``SilverAbort`` fires so a live gate that WANTS
    byte-identical parity refuses rather than publishes a mis-labelled
    number.

    Returns ``(entries_written, late_iban_count)``.
    """
    from pyspark.sql import Window
    from pyspark.sql.functions import coalesce, col, expr, row_number, when, xxhash64
    from pyspark.sql.functions import min as min_
    from pyspark.sql.functions import sum as sum_

    spark = batch_df.sparkSession

    # 1. Explode bronze -> two statement lines per pacs.008 (DBIT + CRDT),
    #    filter iban NULLs, derive account_id + signed_amt + tie-break helper.
    common_cols = [
        col("cre_dt_tm").alias("book_ts"),
        col("cre_dt_tm").alias("val_ts"),
        col("intr_bk_sttlm_amt").cast("decimal(18,2)").alias("amt"),
        col("intr_bk_sttlm_ccy").alias("ccy"),
        col("txn_id"),
        col("uetr"),
    ]
    dbit = batch_df.select(
        col("dbtr_acct.iban").alias("iban"),
        lit("DBIT").alias("cdt_dbt_ind"),
        *common_cols,
    )
    crdt = batch_df.select(
        col("cdtr_acct.iban").alias("iban"),
        lit("CRDT").alias("cdt_dbt_ind"),
        *common_cols,
    )
    entries = (
        dbit.unionByName(crdt)
        .filter(col("iban").isNotNull())
        .withColumn("account_id", xxhash64(col("iban")))
        .withColumn(
            "signed_amt",
            when(col("cdt_dbt_ind") == lit("CRDT"), col("amt")).otherwise(-col("amt")),
        )
        .withColumn(
            "_cdt_dbt_ord",
            when(col("cdt_dbt_ind") == lit("DBIT"), lit(0)).otherwise(lit(1)),
        )
        .cache()
    )
    try:
        n_entries = entries.count()
        if n_entries == 0:
            return 0, 0

        # 2. Per touched account: prior latest bal_after, prior max entry_seq,
        #    and prior max book_ts (used for the late-arrival label). Also
        #    compute this batch's min book_ts per iban for the same reason.
        touched = entries.select("account_id", "iban").distinct().cache()
        this_batch_min_ts = entries.groupBy("account_id").agg(
            min_(col("book_ts")).alias("_this_min_book_ts")
        )
        try:
            stmts_tbl = spark.table(f"{CATALOG}.{SILVER_STATEMENTS}")
            # Inner join to restrict the aggregate to touched accounts;
            # groupBy + max/max_by is one shuffle vs a full-table window.
            prior_state = (
                stmts_tbl.join(
                    touched.select("account_id"),
                    on="account_id",
                    how="inner",
                )
                .groupBy("account_id")
                .agg(
                    # Spark 3.5+ max_by: bal_after at the maximum entry_seq per
                    # account. entry_seq is monotone per (account_id, book_ts),
                    # so max_by(entry_seq) gives the latest row deterministically.
                    expr("cast(max_by(bal_after, entry_seq) as decimal(38,2)) as _prev_bal_after"),
                    expr("cast(max(entry_seq) as bigint) as _prev_max_seq"),
                    expr("max(book_ts) as _prev_max_book_ts"),
                )
            )

            # Late-arrival detection: any account_id whose THIS-batch min
            # book_ts is strictly less than the PRIOR max book_ts is a late
            # arrival (a row landed out of write-time order). Count distinct
            # such account_ids; one late iban is enough to label the batch.
            late_df = (
                this_batch_min_ts.join(
                    prior_state.select("account_id", "_prev_max_book_ts"),
                    on="account_id",
                    how="inner",
                )
                .filter(col("_this_min_book_ts") < col("_prev_max_book_ts"))
                .select("account_id")
                .distinct()
            )
            late_iban_count = int(late_df.count())

            # STRICT parity opt-in: refuse to publish an arrival-order
            # running_balance when the operator has asserted the pipeline
            # was configured for strict-monotone bronze arrival. This is
            # the invariant-2 gate: a batch/stream comparison is invalid
            # under multi-pod (non-monotone) datagen, so a run that means
            # to be compared MUST fail loud rather than emit divergent
            # numbers.
            if late_iban_count > 0 and os.getenv(STATEMENTS_STRICT_PARITY_ENV) == "1":
                raise SilverAbort(
                    f"silver.account_statements: {late_iban_count} iban(s) received "
                    f"a late-arriving bronze row in batch {batch_id} "
                    f"(this-batch min book_ts < prior max book_ts). "
                    f"{STATEMENTS_STRICT_PARITY_ENV}=1 is set: refusing to "
                    f"write an arrival-order running_balance that would "
                    f"silently diverge from batch mode. Remedies: single-pod "
                    f"datagen (DatagenConfig.parallelism=1), or wait for the "
                    f"out-of-order-tolerant D-full slated for v1.7."
                )

            # 3. Join, fill first-touch defaults, cumulative sum per account.
            deterministic_open = aml_opening_balance(col("account_id"))
            with_prior = (
                entries.join(prior_state, on="account_id", how="left")
                .withColumn(
                    "_opening",
                    deterministic_open,
                )
                .withColumn(
                    "_prev_bal_after",
                    coalesce(
                        col("_prev_bal_after"),
                        col("_opening").cast("decimal(38,2)"),
                    ),
                )
                .withColumn(
                    "_prev_max_seq",
                    coalesce(col("_prev_max_seq"), lit(0).cast("bigint")),
                )
            )

            w = Window.partitionBy("account_id").orderBy(
                col("book_ts"),
                col("txn_id"),
                col("_cdt_dbt_ord"),
            )
            with_prior = (
                with_prior.withColumn(
                    "_batch_cumsum",
                    sum_(col("signed_amt")).over(w).cast("decimal(38,2)"),
                )
                .withColumn(
                    "bal_after",
                    (col("_prev_bal_after") + col("_batch_cumsum")).cast("decimal(38,2)"),
                )
                .withColumn(
                    "bal_before",
                    (col("bal_after") - col("signed_amt")).cast("decimal(38,2)"),
                )
                .withColumn(
                    "entry_seq",
                    (col("_prev_max_seq") + row_number().over(w)).cast("bigint"),
                )
            )

            out = with_prior.select(
                col("account_id"),
                col("iban"),
                col("entry_seq"),
                col("book_ts"),
                col("val_ts"),
                col("cdt_dbt_ind"),
                col("amt"),
                col("ccy"),
                col("bal_before"),
                col("bal_after"),
                col("txn_id"),
                col("uetr"),
                lit("PMNT-ICDT").alias("bk_tx_cd"),
                lit(int(batch_id)).cast("bigint").alias("_batch_id"),
                lit(sid).alias("_stream_id"),
            )

            # 4. DELETE-INSERT idempotency. Same protocol as silver.transactions.
            if check_replay:
                spark.sql(
                    f"DELETE FROM {CATALOG}.{SILVER_STATEMENTS} "
                    f"WHERE _stream_id = '{sid}' AND _batch_id = {int(batch_id)}"
                )
            out.writeTo(f"{CATALOG}.{SILVER_STATEMENTS}").append()

            # 5. Roll up silver.accounts.current_balance for touched IBANs only.
            # A separate temp-view keeps the merge source narrow to the
            # batch's touched IBANs (typically << the accounts table).
            safe_sid = _sanitize_for_view(sid)
            view_name = f"_d_full_touched_ibans_{safe_sid}_{int(batch_id)}"
            touched.select("iban").createOrReplaceTempView(view_name)
            try:
                # The MERGE reads a materialised source: on Spark 4.1 a MERGE
                # whose source plan reads an Iceberg table can fail.
                balances = spark.sql(
                    f"""
SELECT iban, running_balance
FROM (
    SELECT s.iban,
           s.bal_after AS running_balance,
           row_number() OVER (
               PARTITION BY s.iban
               ORDER BY s.book_ts DESC, s.entry_seq DESC
           ) AS rn
    FROM {CATALOG}.{SILVER_STATEMENTS} s
    JOIN {view_name} touched ON s.iban = touched.iban
) ranked
WHERE rn = 1
""".strip()
                )
                with materialised_source(
                    spark, balances, f"{_BALANCES_MERGE_VIEW}_{safe_sid}_{int(batch_id)}"
                ) as src_view:
                    spark.sql(
                        f"""
MERGE INTO {CATALOG}.{SILVER_ACCOUNTS} t
USING {src_view} src
ON t.iban = src.iban
WHEN MATCHED THEN UPDATE SET current_balance = src.running_balance
""".strip()
                    )
            finally:
                try:
                    spark.catalog.dropTempView(view_name)
                except Exception:  # noqa: BLE001
                    pass

            return int(n_entries), late_iban_count
        finally:
            touched.unpersist(blocking=False)
    finally:
        entries.unpersist(blocking=False)


def _sanitize_for_view(text: str) -> str:
    """Streaming query ids include hyphens (UUID form); Spark temp view names
    disallow them, so map to identifier-safe underscores. Idempotent."""
    return "".join(c if c.isalnum() else "_" for c in text)


def _sanitize_view_suffix(stream_id):
    """Turn a streaming query id (UUID with hyphens) into a temp-view suffix.

    Non-blocker defense: two concurrent stream queries in the same JVM
    (not a supported deployment mode today) share a session, so a fixed
    temp-view name would collide across their MERGEs. Suffixing with a
    per-query id keeps them isolated. Non-alphanumerics collapse to
    underscore; the empty case (unit-test path with stream_id=None)
    returns the empty string so tests keep the fixed name.
    """
    if not stream_id:
        return ""
    safe = "".join(c if c.isalnum() else "_" for c in str(stream_id))
    return f"_{safe}"


def append_new_dimensions(spark, batch_df, txns, kyc, stream_id=None) -> tuple[int, int]:
    """Merge this batch's entities and accounts into the silver dimensions.

    E1: replaces the previous first-seen-wins append with per-table MERGE
    INTOs that end each batch with the same silver.entities and
    silver.accounts row shape a single-pass batch build would.

    ``silver.entities`` -- per-column LEAST is safe on the entity
    dimension columns because each column collapses independently under
    batch's ``build_entities`` (``_min("name")`` on the reported name,
    then a left join to ``_entity_countries`` for country; there is no
    cross-column tiebreak). Semantics per column (matches batch's
    ``min()`` skip-NULL behaviour via ``coalesce(least(t, s), t, s)``):

    - name updated to ``LEAST(target.name, source.name)``.
    - legal_name updated to the SAME expression as name -- batch enforces
      ``legal_name = col("name")`` at build_entities so the invariant
      ``legal_name == name`` must survive stream updates. Using an
      independent LEAST on legal_name would drift on a batch where the
      target's name and legal_name disagreed for any reason (they never
      should, but the invariant is what the DDL semantics rely on).
    - entity_type re-derived from the merged name via the shared
      ``common.entity_type_from_name_sql`` helper so batch and stream
      cannot drift on the corporate-suffix regex.
    - country updated to ``LEAST(target.country, source.country)``.
    - KYC and screening columns are set at first insert and left alone
      on later batches (they are the same across a stream's lifetime).

    ``silver.accounts`` -- per-column LEAST WOULD MIX FIELDS across
    debtor and creditor sides on the same iban: batch's ``build_accounts``
    already fixed this exact class of hazard (I3 at
    silver_build_financial.py:632-644 -- ``row_number() over (iban)
    order by (holder_entity_id, bank_bic, opened_date) asc_nulls_last``,
    filter rn=1) so the four dimension columns come from ONE OBSERVED
    ROW. Reopening per-column LEAST here would synthesise a row that
    appears on neither side (debtor's holder + creditor's bank_bic + a
    third row's opened_date). Instead the stream computes the same
    row_number winner across (target existing row + this batch's
    candidates) and UPDATEs the four columns as a unit. For a
    row_number-1-by-lex-tuple the operation is associative:
    ``winner(A, B, C) == winner(winner(A, B), C)`` -- so a stream that
    sees the corpus in chunks converges on the same winner a single-pass
    batch would.

    Structure: TWO MERGEs on silver.accounts to keep the source cleanly
    typed for each path -- one INSERT-only source (batch anti-join,
    full-shape rows carrying KYC), one UPDATE-only source (winners
    resolved over target + batch candidates; only the four dimension
    columns are UPDATEd so target KYC is preserved). One MERGE on
    silver.entities (source shape is DDL-shaped, INSERT * safe).

    Returns ``(entities_inserted, accounts_inserted)``, the count of
    genuinely new rows (WHEN NOT MATCHED path). The A1 driver-side
    progress gate treats a batch that inserted zero new dimensions as
    non-progress on the dimensions themselves; ``_merge_batch``'s
    return value (silver.transactions row count) is the LB-044 gate.

    Instrumentation: publishes ``dim_merge_elapsed_ms`` for entities and
    accounts separately via ``log_job_metrics`` so the E1/E2 live gate
    can measure whether the MERGE cost at scale 10 stays within one
    trigger interval. If not, the block-E fallback (E2: label + defer,
    v1.7) is triggered.

    ``stream_id`` scopes the per-batch temp views used by the MERGE
    sources so two concurrent stream queries in the same JVM never
    collide on the fixed view name. ``_merge_batch`` passes it in from
    ``streaming_query_id(spark)``; the unit-test paths pass None and
    fall back to the fixed name.
    """
    from pyspark.sql import Window
    from pyspark.sql.functions import col, row_number

    ents = build_entities(txns.drop("_batch_id", "_stream_id"), batch_df, kyc)
    accts = build_accounts(batch_df, kyc)
    # One small file per source per batch, not one per shuffle partition:
    # the dimensions are dozens to thousands of rows per micro-batch and a
    # 200-partition shuffle would produce as many empty files.
    ents = ents.repartition(1).cache()
    accts = accts.repartition(1).cache()
    try:
        n_ents_total = ents.count()
        n_accts_total = accts.count()
        # Anti-join gives the count of rows that WILL insert (WHEN NOT
        # MATCHED). We compute it before the MERGE so the return value
        # matches the "new entities/accounts introduced" contract the
        # previous append-based API published.
        have_e = spark.table(f"{CATALOG}.{SILVER_ENTITIES}").select(col("entity_id").alias("_have"))
        n_new_ents = (
            ents.join(have_e, ents["entity_id"] == have_e["_have"], "left_anti").count()
            if n_ents_total
            else 0
        )
        have_a_full = spark.table(f"{CATALOG}.{SILVER_ACCOUNTS}")
        have_a = have_a_full.select(col("iban").alias("_have"))
        n_new_accts = (
            accts.join(have_a, accts["iban"] == have_a["_have"], "left_anti").count()
            if n_accts_total
            else 0
        )

        # BLOCKER 3 fix: batch enforces ``legal_name = col("name")``. Use
        # the SAME expression for both so the invariant survives the
        # UPDATE. entity_type re-derivation reads the same expression.
        merged_name_sql = "coalesce(least(t.name, s.name), t.name, s.name)"
        entity_type_expr = entity_type_from_name_sql(merged_name_sql)

        # Non-blocker: sid-scope the temp views so two concurrent stream
        # queries in the same JVM cannot clobber each other's MERGE
        # source. Falls back to the fixed name when stream_id is None
        # (unit-test path).
        suffix = _sanitize_view_suffix(stream_id)
        ent_view = f"{_ENTITIES_MERGE_VIEW}{suffix}"
        acct_view_ins = f"{_ACCOUNTS_MERGE_VIEW}{suffix}"
        acct_view_upd = f"{_ACCOUNTS_MERGE_VIEW}{suffix}_upd"

        ent_elapsed_ms = 0
        if n_ents_total:
            t0 = time.time()
            with materialised_source(spark, ents, ent_view) as src_view:
                spark.sql(
                    f"""
                    MERGE INTO {CATALOG}.{SILVER_ENTITIES} t
                    USING {src_view} s
                    ON t.entity_id = s.entity_id
                    WHEN MATCHED THEN UPDATE SET
                        name = {merged_name_sql},
                        entity_type = {entity_type_expr},
                        legal_name = {merged_name_sql},
                        country = coalesce(least(t.country, s.country), t.country, s.country)
                    WHEN NOT MATCHED THEN INSERT *
                    """
                )
            ent_elapsed_ms = int((time.time() - t0) * 1000)

        acct_elapsed_ms = 0
        if n_accts_total:
            # BLOCKER 1 fix: pick ONE coherent winning row per iban
            # across (target existing row + this batch's candidates)
            # using the same row_number tiebreak as batch's
            # build_accounts. Per-column LEAST would mix the debtor's
            # holder with the creditor's bank_bic on the same iban --
            # exactly the I3 hazard build_accounts already fixed.
            tiebreak_window = Window.partitionBy("iban").orderBy(
                col("holder_entity_id").asc_nulls_last(),
                col("bank_bic").asc_nulls_last(),
                col("opened_date").asc_nulls_last(),
            )
            dim_cols = ["iban", "holder_entity_id", "bank_bic", "currency", "opened_date"]
            # Path (i): WHEN NOT MATCHED THEN INSERT * -- full-shape
            # rows for the ibans this batch introduces for the first
            # time. KYC columns carried through from build_accounts's
            # kyc join.
            new_accts = accts.join(have_a, accts["iban"] == have_a["_have"], "left_anti")
            # Path (ii): WHEN MATCHED THEN UPDATE the four dimension
            # columns from a winning row. Compute the winner over the
            # touched target rows plus this batch's candidates for the
            # same ibans; row_number-1 by (holder, bic, opened) asc
            # nulls-last. Winner comes from ONE actual row (debtor or
            # creditor side, or a previously-committed row), never a
            # cross-side synthesis.
            touched_existing_ibans = (
                accts.join(have_a, accts["iban"] == have_a["_have"], "left_semi")
                .select("iban")
                .distinct()
            )
            existing_candidates = have_a_full.select(*dim_cols).join(
                touched_existing_ibans, "iban", "inner"
            )
            batch_candidates_for_update = accts.join(
                touched_existing_ibans, "iban", "left_semi"
            ).select(*dim_cols)
            winners = (
                existing_candidates.unionByName(batch_candidates_for_update)
                .withColumn("_rn", row_number().over(tiebreak_window))
                .filter("_rn = 1")
                .drop("_rn")
            )

            t0 = time.time()

            if n_new_accts:
                with materialised_source(spark, new_accts, acct_view_ins) as src_view:
                    spark.sql(
                        f"""
                        MERGE INTO {CATALOG}.{SILVER_ACCOUNTS} t
                        USING {src_view} s
                        ON t.iban = s.iban
                        WHEN NOT MATCHED THEN INSERT *
                        """
                    )
            if n_accts_total > n_new_accts:
                with materialised_source(spark, winners, acct_view_upd) as src_view:
                    spark.sql(
                        f"""
                        MERGE INTO {CATALOG}.{SILVER_ACCOUNTS} t
                        USING {src_view} s
                        ON t.iban = s.iban
                        WHEN MATCHED THEN UPDATE SET
                            holder_entity_id = s.holder_entity_id,
                            bank_bic = s.bank_bic,
                            currency = s.currency,
                            opened_date = s.opened_date
                        """
                    )

            acct_elapsed_ms = int((time.time() - t0) * 1000)

        # E1 instrumentation: per-batch merge cost, split entities vs
        # accounts. Emitted through ``log_job_metrics`` so the collector's
        # existing regex lifts the JOB METRICS block into metrics.json,
        # and also as a plain labelled line for a follow-on parser.
        log(
            f"[dim-merge] entities: total={n_ents_total} inserted={n_new_ents} "
            f"updated={n_ents_total - n_new_ents} elapsed_ms={ent_elapsed_ms}"
        )
        log(
            f"[dim-merge] accounts: total={n_accts_total} inserted={n_new_accts} "
            f"updated={n_accts_total - n_new_accts} elapsed_ms={acct_elapsed_ms}"
        )
        # One label per log() call so the collector's per-line regex does not
        # swallow the second key into the first key's value.
        log(f"dim_merge_elapsed_ms_entities: {ent_elapsed_ms}")
        log(f"dim_merge_elapsed_ms_accounts: {acct_elapsed_ms}")
        log_job_metrics(
            "silver-stream-dim-merge",
            input_size_gb=0.0,
            input_rows=n_ents_total + n_accts_total,
            output_rows=n_new_ents + n_new_accts,
            elapsed_seconds=(ent_elapsed_ms + acct_elapsed_ms) / 1000.0,
            dim_merge_entities_total=n_ents_total,
            dim_merge_entities_inserted=n_new_ents,
            dim_merge_entities_updated=n_ents_total - n_new_ents,
            dim_merge_accounts_total=n_accts_total,
            dim_merge_accounts_inserted=n_new_accts,
            dim_merge_accounts_updated=n_accts_total - n_new_accts,
            dim_merge_elapsed_ms_entities=ent_elapsed_ms,
            dim_merge_elapsed_ms_accounts=acct_elapsed_ms,
        )
        return n_new_ents, n_new_accts
    finally:
        ents.unpersist(blocking=False)
        accts.unpersist(blocking=False)


def _merge_profiles(spark, tagged_txns, sid: str, batch_id: int) -> None:
    """Maintain silver.entity_profiles incrementally for the touched entities.

    Aggregate strategies per column:

    - Additive (txn_count_out/in, total_sent/received_usd): MERGE UPDATE
      SET target = target + batch_delta. Iceberg MERGE evaluates every
      UPDATE SET RHS against the pre-update row, so composing several
      counters in one statement is safe.
    - LEAST / GREATEST (first/last_seen_ts, _first_out_ts, _last_out_ts,
      profile_updated_ts): Spark LEAST/GREATEST skip NULLs, so an
      entity that appears on only one side of the batch still resolves.
    - Welford parallel merge (avg_amount_usd, stddev_amount_usd via _m2):
      combines the target block (n, mean, _m2) with the batch block
      (batch_n_out, batch_mean_out, batch_m2_out). Sample stddev is
      SQRT(_m2 / (n - 1)); NULL when n < 2.
    - Derived on write (txn_count_total, active_span_days, avg_gap_days,
      passthrough_ratio): recomputed from the freshly merged base columns
      using the merged LEAST/GREATEST/SUM values.
    - Per-batch recompute (distinct_counterparties_out/in): exact
      count_distinct cannot be maintained incrementally without a
      per-entity seen-set (a stream-safe HLL sketch would be additive but
      is not what the DDL declares). The merge reads silver.transactions
      through I10's common.sealed_txns_filter (semi-joined against
      silver.silver_batch_versions) so a mid-crash batch's ghost txns
      cannot inflate the count; the current batch's own tagged_txns are
      UNIONed in because PHASE 5's sealed marker has not yet been
      written when this MERGE runs.

    Self-idempotent MERGE (blocker fix): the WHEN MATCHED branch is
    guarded by ``(t._stream_id, t._batch_id) = (s.batch_stream_id,
    s.batch_batch_id)`` -- a re-apply on the same (sid, batch_id) is a
    no-op (UPDATE SET entity_id = entity_id). This keeps the MERGE
    self-contained: a driver crash between the profiles commit and
    PHASE 5's sealed-marker commit does NOT double-apply the additive
    deltas when the batch is retried on restart. The prior version
    guarded only via a cross-table probe of silver_batch_versions,
    which loses to the crash window between phase 4 (this MERGE) and
    phase 5 (I10 marker); the self-idempotent branch closes that gap.
    """
    amt_d = col("txn_amount_usd").cast("double")

    # Out-side per-entity batch aggregates. batch_m2_out = variance * (n-1),
    # NULL when n <= 1 -> coalesce to 0.0 (Welford identity).
    out_delta = tagged_txns.groupBy(col("originator_id").alias("entity_id")).agg(
        min_(col("txn_timestamp")).alias("batch_first_out_ts"),
        max_(col("txn_timestamp")).alias("batch_last_out_ts"),
        count_(lit(1)).alias("batch_n_out"),
        sum_(col("txn_amount_usd")).cast("decimal(38,2)").alias("batch_sum_sent"),
        avg_(amt_d).alias("batch_mean_out"),
        coalesce(variance_(amt_d) * (count_(lit(1)) - lit(1)), lit(0.0)).alias("batch_m2_out"),
    )
    in_delta = tagged_txns.groupBy(col("beneficiary_id").alias("entity_id")).agg(
        min_(col("txn_timestamp")).alias("batch_first_in_ts"),
        max_(col("txn_timestamp")).alias("batch_last_in_ts"),
        count_(lit(1)).alias("batch_n_in"),
        sum_(col("txn_amount_usd")).cast("decimal(38,2)").alias("batch_sum_recv"),
    )
    delta = out_delta.join(in_delta, on="entity_id", how="fullouter").cache()
    try:
        touched = delta.select(col("entity_id"))
        # Per-batch recompute of distinct counterparties. Reads
        # silver.transactions through I10's sealed_txns_filter so a
        # partial batch's ghost rows (committed to silver.transactions
        # but not yet sealed in silver_batch_versions after a mid-crash
        # window) cannot inflate the count. sealed_txns_filter DOES NOT
        # include the current batch's rows because PHASE 5's sealed
        # marker MERGE runs AFTER this function returns; the UNION with
        # tagged_txns adds them back so the count reflects the current
        # batch's contribution. Both frames project the (originator_id,
        # beneficiary_id) pair used by count_distinct; unionByName
        # aligns on those two columns.
        silver_txns = spark.table(f"{CATALOG}.{SILVER_TXNS}")
        sealed = sealed_txns_filter(spark, silver_txns, CATALOG, SILVER_BATCH_VERSIONS)
        pairs_sealed = sealed.select(col("originator_id"), col("beneficiary_id"))
        pairs_current = tagged_txns.select(col("originator_id"), col("beneficiary_id"))
        pairs = pairs_sealed.unionByName(pairs_current, allowMissingColumns=False)
        touched_l = broadcast(touched.withColumnRenamed("entity_id", "_t"))
        touched_r = broadcast(touched.withColumnRenamed("entity_id", "_t2"))
        recomputed_out = (
            pairs.join(touched_l, pairs["originator_id"] == col("_t"), "inner")
            .groupBy(pairs["originator_id"].alias("entity_id"))
            .agg(count_distinct_(pairs["beneficiary_id"]).alias("recomputed_dco"))
        )
        recomputed_in = (
            pairs.join(touched_r, pairs["beneficiary_id"] == col("_t2"), "inner")
            .groupBy(pairs["beneficiary_id"].alias("entity_id"))
            .agg(count_distinct_(pairs["originator_id"]).alias("recomputed_dci"))
        )
        # Assemble the source frame for the MERGE, filling counts with 0
        # for entities only present on one side and computing batch-wide
        # profile min / max (for LEAST/GREATEST against the target).
        final_delta = (
            delta.join(recomputed_out, on="entity_id", how="left")
            .join(recomputed_in, on="entity_id", how="left")
            .select(
                col("entity_id"),
                coalesce(col("batch_first_out_ts"), col("batch_first_in_ts")).alias(
                    "batch_first_seen_ts"
                ),
                greatest(col("batch_last_out_ts"), col("batch_last_in_ts")).alias(
                    "batch_last_seen_ts"
                ),
                col("batch_first_out_ts"),
                col("batch_last_out_ts"),
                coalesce(col("batch_n_out"), lit(0)).cast("bigint").alias("batch_n_out"),
                coalesce(col("batch_n_in"), lit(0)).cast("bigint").alias("batch_n_in"),
                coalesce(col("batch_sum_sent"), lit(0).cast("decimal(38,2)")).alias(
                    "batch_sum_sent"
                ),
                coalesce(col("batch_sum_recv"), lit(0).cast("decimal(38,2)")).alias(
                    "batch_sum_recv"
                ),
                col("batch_mean_out"),
                coalesce(col("batch_m2_out"), lit(0.0)).alias("batch_m2_out"),
                coalesce(col("recomputed_dco"), lit(0)).cast("bigint").alias("recomputed_dco"),
                coalesce(col("recomputed_dci"), lit(0)).cast("bigint").alias("recomputed_dci"),
                greatest(col("batch_last_out_ts"), col("batch_last_in_ts")).alias(
                    "batch_profile_updated_ts"
                ),
                lit(sid).alias("batch_stream_id"),
                lit(int(batch_id)).cast("bigint").alias("batch_batch_id"),
            )
        )

        # SQL block below: MERGE UPDATE SET evaluates every RHS against
        # the pre-update target, so composing several counter and Welford
        # expressions in one statement is safe. Sample stddev is derived
        # from the newly-merged _m2 in the SAME statement, using the
        # inlined Welford expression (not a reference to t._m2 which is
        # still the pre-update value at RHS eval time).
        # Self-idempotent MERGE (blocker fix): the first WHEN MATCHED
        # branch catches an already-applied (stream_id, batch_id) and
        # emits a no-op UPDATE (entity_id -> entity_id). This closes
        # the crash window between phase 4 (this MERGE commits) and
        # phase 5 (the sealed-marker MERGE commits): a restart-replay
        # of the same batch cannot double-apply the additive deltas
        # because the guarded branch fires first and the substantive
        # branch is short-circuited by MERGE's first-match semantics.
        merge_sql = f"""
        MERGE INTO {CATALOG}.{SILVER_PROFILES} t
        USING _lb_profiles_delta s
        ON t.entity_id = s.entity_id
        WHEN MATCHED AND (t._stream_id = s.batch_stream_id
                          AND t._batch_id = s.batch_batch_id) THEN UPDATE SET
            t.entity_id = t.entity_id
        WHEN MATCHED THEN UPDATE SET
            t.first_seen_ts = LEAST(t.first_seen_ts, s.batch_first_seen_ts),
            t.last_seen_ts = GREATEST(t.last_seen_ts, s.batch_last_seen_ts),
            t.active_span_days = CAST(DATEDIFF(
                GREATEST(t.last_seen_ts, s.batch_last_seen_ts),
                LEAST(t.first_seen_ts, s.batch_first_seen_ts)
            ) AS DOUBLE),
            t.txn_count_out = t.txn_count_out + s.batch_n_out,
            t.txn_count_in = t.txn_count_in + s.batch_n_in,
            t.txn_count_total =
                t.txn_count_out + s.batch_n_out + t.txn_count_in + s.batch_n_in,
            t.total_sent_usd =
                COALESCE(t.total_sent_usd, CAST(0 AS DECIMAL(38,2))) + s.batch_sum_sent,
            t.total_received_usd =
                COALESCE(t.total_received_usd, CAST(0 AS DECIMAL(38,2)))
                + s.batch_sum_recv,
            t.avg_amount_usd = CASE
                WHEN (t.txn_count_out + s.batch_n_out) = 0 THEN NULL
                WHEN t.txn_count_out = 0 THEN s.batch_mean_out
                WHEN s.batch_n_out = 0 THEN t.avg_amount_usd
                ELSE t.avg_amount_usd
                     + (s.batch_mean_out - t.avg_amount_usd)
                       * CAST(s.batch_n_out AS DOUBLE)
                       / CAST(t.txn_count_out + s.batch_n_out AS DOUBLE)
            END,
            t._m2 = CASE
                WHEN (t.txn_count_out + s.batch_n_out) = 0 THEN 0.0
                WHEN t.txn_count_out = 0 THEN s.batch_m2_out
                WHEN s.batch_n_out = 0 THEN COALESCE(t._m2, 0.0)
                ELSE COALESCE(t._m2, 0.0) + s.batch_m2_out
                     + POWER(s.batch_mean_out - t.avg_amount_usd, 2.0)
                       * CAST(t.txn_count_out AS DOUBLE)
                       * CAST(s.batch_n_out AS DOUBLE)
                       / CAST(t.txn_count_out + s.batch_n_out AS DOUBLE)
            END,
            t.stddev_amount_usd = CASE
                WHEN (t.txn_count_out + s.batch_n_out) < 2 THEN NULL
                ELSE SQRT(
                    (CASE
                        WHEN (t.txn_count_out + s.batch_n_out) = 0 THEN 0.0
                        WHEN t.txn_count_out = 0 THEN s.batch_m2_out
                        WHEN s.batch_n_out = 0 THEN COALESCE(t._m2, 0.0)
                        ELSE COALESCE(t._m2, 0.0) + s.batch_m2_out
                             + POWER(s.batch_mean_out - t.avg_amount_usd, 2.0)
                               * CAST(t.txn_count_out AS DOUBLE)
                               * CAST(s.batch_n_out AS DOUBLE)
                               / CAST(t.txn_count_out + s.batch_n_out AS DOUBLE)
                     END)
                    / CAST(t.txn_count_out + s.batch_n_out - 1 AS DOUBLE)
                )
            END,
            t._first_out_ts = LEAST(t._first_out_ts, s.batch_first_out_ts),
            t._last_out_ts = GREATEST(t._last_out_ts, s.batch_last_out_ts),
            t.avg_gap_days = CASE
                WHEN (t.txn_count_out + s.batch_n_out) < 2 THEN NULL
                ELSE CAST(DATEDIFF(
                    GREATEST(t._last_out_ts, s.batch_last_out_ts),
                    LEAST(t._first_out_ts, s.batch_first_out_ts)
                ) AS DOUBLE)
                / CAST(t.txn_count_out + s.batch_n_out - 1 AS DOUBLE)
            END,
            t.distinct_counterparties_out = s.recomputed_dco,
            t.distinct_counterparties_in = s.recomputed_dci,
            t.passthrough_ratio = CASE
                WHEN (COALESCE(t.total_received_usd, CAST(0 AS DECIMAL(38,2)))
                      + s.batch_sum_recv) > 0
                THEN CAST(
                    COALESCE(t.total_sent_usd, CAST(0 AS DECIMAL(38,2))) + s.batch_sum_sent
                    AS DOUBLE)
                    / CAST(
                    COALESCE(t.total_received_usd, CAST(0 AS DECIMAL(38,2)))
                    + s.batch_sum_recv AS DOUBLE)
                ELSE NULL
            END,
            t.profile_updated_ts = GREATEST(
                COALESCE(t.profile_updated_ts, s.batch_profile_updated_ts),
                s.batch_profile_updated_ts
            ),
            t._stream_id = s.batch_stream_id,
            t._batch_id = s.batch_batch_id
        WHEN NOT MATCHED THEN INSERT (
            entity_id, first_seen_ts, last_seen_ts, active_span_days,
            txn_count_out, txn_count_in, txn_count_total,
            total_sent_usd, total_received_usd,
            avg_amount_usd, stddev_amount_usd, avg_gap_days,
            distinct_counterparties_out, distinct_counterparties_in,
            passthrough_ratio, profile_updated_ts,
            _m2, _first_out_ts, _last_out_ts, _stream_id, _batch_id
        ) VALUES (
            s.entity_id, s.batch_first_seen_ts, s.batch_last_seen_ts,
            CAST(DATEDIFF(s.batch_last_seen_ts, s.batch_first_seen_ts) AS DOUBLE),
            s.batch_n_out, s.batch_n_in, s.batch_n_out + s.batch_n_in,
            s.batch_sum_sent, s.batch_sum_recv,
            CASE WHEN s.batch_n_out = 0 THEN NULL ELSE s.batch_mean_out END,
            CASE WHEN s.batch_n_out < 2 THEN NULL
                 ELSE SQRT(s.batch_m2_out / CAST(s.batch_n_out - 1 AS DOUBLE))
            END,
            CASE
                WHEN s.batch_n_out < 2 THEN NULL
                ELSE CAST(DATEDIFF(s.batch_last_out_ts, s.batch_first_out_ts) AS DOUBLE)
                     / CAST(s.batch_n_out - 1 AS DOUBLE)
            END,
            s.recomputed_dco, s.recomputed_dci,
            CASE WHEN s.batch_sum_recv > 0
                 THEN CAST(s.batch_sum_sent AS DOUBLE) / CAST(s.batch_sum_recv AS DOUBLE)
                 ELSE NULL
            END,
            s.batch_profile_updated_ts,
            s.batch_m2_out, s.batch_first_out_ts, s.batch_last_out_ts,
            s.batch_stream_id, s.batch_batch_id
        )
        """
        # The MERGE reads a materialised source: on Spark 4.1 a MERGE whose
        # source plan reads an Iceberg table can fail.
        with materialised_source(spark, final_delta, "_lb_profiles_delta"):
            spark.sql(merge_sql)
    finally:
        delta.unpersist(blocking=False)


def _merge_batch(batch_df, batch_id: int) -> tuple[int, int]:
    """Idempotently write per-batch txns and edges tagged with batch_id.

    Caches the flattened txns so build_transactions and build_edges don't
    scan the bronze rows twice. Casts the per-batch aggregate to
    decimal(38,2) matching the DDL (SUM of decimal columns widens; the
    old (18,2) would silently NULL on overflow under ANSI-off).

    Returns ``(silver.transactions row count, late_iban_count)`` for this
    batch. The A1 driver-side accumulator in main() sums the transactions
    count across batches for ``assert_progress``, and accumulates the late
    count for the run-total invariant-6 label.
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
            return 0, 0
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
        n_e, n_a = append_new_dimensions(spark, batch_df, tagged_txns, _kyc(spark), stream_id=sid)
        log(f"[batch {batch_id}] appended {n_e} new entities, {n_a} new accounts")

        # PHASE 4 (D-full-simple): silver.account_statements per-batch MERGE and
        # silver.accounts.current_balance roll-up for this batch's touched IBANs.
        n_stmts, late_ibans = _maintain_statements(batch_df, int(batch_id), sid, check_replay)
        log(f"[batch {batch_id}] wrote {n_stmts} statement entries and updated current_balance")

        # D-full-simple invariant-6 label: publish per-batch so downstream
        # reports and the collector can lift the value into metrics.json
        # via parse_streaming_logs' key: value regex. `strict_monotone` is
        # the parity-with-batch mode; any late iban downgrades this batch
        # to `arrival_order_running_balance` (silently correct as arrival
        # order, but NOT byte-identical to batch mode).
        parity_mode = "strict_monotone" if late_ibans == 0 else "arrival_order_running_balance"
        # One label per log() call so the collector's per-line regex does not
        # swallow the second key into the first key's value.
        log(f"silver_statements_parity_mode: {parity_mode}")
        log(f"silver_statements_late_arrivals_this_batch: {late_ibans}")
        # A JOB METRICS block per batch so the collector's driver-log parser
        # ingests these into metrics.json without a wire-not-connected step
        # (same mechanism KYC refresh uses in _kyc above).
        log_job_metrics(
            "silver-stream-statements",
            input_size_gb=0.0,
            input_rows=int(n_txns),
            output_rows=int(n_stmts),
            elapsed_seconds=0.0,
            silver_statements_parity_mode=parity_mode,
            silver_statements_late_arrivals_this_batch=int(late_ibans),
            silver_statements_batch_id=int(batch_id),
        )

        # PHASE 5 (D-full-profiles): incremental MERGE into
        # silver.entity_profiles. The MERGE is self-idempotent via a
        # WHEN MATCHED AND (t._stream_id = s.batch_stream_id AND
        # t._batch_id = s.batch_batch_id) THEN no-op branch, so a retry
        # after a post-MERGE crash does not double-apply the additive
        # deltas. The distinct-counterparty recompute inside _merge_profiles
        # reads silver.transactions through common.sealed_txns_filter
        # (I10), UNIONed with the current batch's tagged_txns so the
        # current batch's contribution is included (its versions row is
        # not written until PHASE 6 below).
        _merge_profiles(spark, tagged_txns, sid, batch_id)
        log(f"[batch {batch_id}] merged entity_profiles")

        # PHASE 6 (I10 sealed marker): must be LAST. Only after
        # transactions, edges, dimensions, statements AND profiles have
        # committed do we write the (stream_id, batch_id) row that
        # gold-side consumers semi-join against. A driver crash between
        # any earlier phase and this phase leaves partial silver rows
        # visible but no matching versions row, so consumers hide the
        # partial batch. On retry, the phase-1 DELETE (guarded by
        # replay_possible) cleans ghost txns/edges rows; phase-4 MERGE
        # on statements is idempotent by (_stream_id, _batch_id) key;
        # phase-5 profiles MERGE is self-idempotent via the same key;
        # this MERGE seals the retry idempotently.
        #
        # MERGE (not INSERT): a crash after this write but before
        # Structured Streaming durably records the batch id would replay
        # the whole foreachBatch on restart; a plain INSERT would then
        # write a second row for the same (sid, batch_id). The semi-join
        # tolerates duplicates today, but any future COUNT(*) or
        # per-batch join against silver_batch_versions would
        # double-count. The MERGE keeps the sidecar at exactly one row
        # per sealed batch.
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
        return int(n_txns), int(late_ibans)
    finally:
        tagged_txns.unpersist(blocking=False)


def main() -> None:
    # D-full complete: silver.transactions, silver.counterparty_edges,
    # silver.entities, silver.accounts, silver.account_statements,
    # silver.accounts.current_balance and silver.entity_profiles are all
    # maintained by the continuous pipeline. The previous D-safe
    # residual refusal (LB_ALLOW_PARTIAL_SILVER=1 opt-in) is removed.
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
        # I10: sidecar seals every (stream_id, batch_id) after phases 1+2+3+4
        # commit; every gold/score reader semi-joins against it. D-full-profiles
        # consumes it read-only for the sealed-txns filter in the
        # distinct-counterparty recompute.
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
    # D-full-simple: silver.account_statements gains the same idempotency-key
    # columns. Batch and stream both write them; a reused catalog whose
    # statements table predates the columns receives them here.
    ensure_column(spark, f"{CATALOG}.{SILVER_STATEMENTS}", "_batch_id", "BIGINT")
    ensure_column(spark, f"{CATALOG}.{SILVER_STATEMENTS}", "_stream_id", "STRING")
    ensure_column(spark, f"{CATALOG}.{SILVER_TXNS}", "ingest_ts", "TIMESTAMP")
    # D-full-profiles: bring silver.entity_profiles forward on a reused
    # catalog whose DDL predates the internal Welford / originator-side
    # accumulators. Idempotent on re-runs.
    ensure_column(spark, f"{CATALOG}.{SILVER_PROFILES}", "_m2", "DOUBLE")
    ensure_column(spark, f"{CATALOG}.{SILVER_PROFILES}", "_first_out_ts", "TIMESTAMP")
    ensure_column(spark, f"{CATALOG}.{SILVER_PROFILES}", "_last_out_ts", "TIMESTAMP")
    ensure_column(spark, f"{CATALOG}.{SILVER_PROFILES}", "_stream_id", "STRING")
    # BLOCKER 6: the MERGE writes t._batch_id = s.batch_batch_id; a
    # legacy catalog whose entity_profiles DDL predates the _batch_id
    # column would fail the MERGE with column-not-found. Idempotent on
    # re-runs; matches the ensure_column pattern used above for
    # silver.transactions and silver.counterparty_edges.
    ensure_column(spark, f"{CATALOG}.{SILVER_PROFILES}", "_batch_id", "BIGINT")

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
    #
    # D-full-simple: also accumulate the run-total late-arrival iban count
    # so the summary line emits `silver_statements_total_late_arrivals` for
    # the collector to lift into metrics.json (per-batch labels above give
    # the incremental value; this one is the cumulative that a live gate
    # or dashboard reads).
    rows_written_total = 0
    late_ibans_total = 0
    rows_written_lock = threading.Lock()

    def _foreach_batch(df, bid):
        written, late = _merge_batch(df, bid)
        with rows_written_lock:
            nonlocal rows_written_total, late_ibans_total
            rows_written_total += int(written or 0)
            late_ibans_total += int(late or 0)

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
        total_late = late_ibans_total
    log(f"Silver stream total rows written across all batches: {total}")
    # D-full-simple: publish the run-total late-arrival count as an
    # invariant-6 label. A run with zero late arrivals produced silver
    # statements byte-identical to batch mode; a non-zero count means
    # the arrival-order fallback fired on that many iban * batch touches.
    run_parity_mode = "strict_monotone" if total_late == 0 else "arrival_order_running_balance"
    # One label per log() call so the collector's per-line regex does not
    # swallow the second key into the first key's value.
    log(f"silver_statements_total_late_arrivals: {total_late}")
    log(f"silver_statements_parity_mode_run: {run_parity_mode}")
    # H4: clean shutdown removes the marker. The signal handler above
    # also calls this; the double-call is idempotent (delete-if-exists).
    # Both paths matter: process exits via SIGTERM in K8s Job termination,
    # but the query can also drain naturally at end of run_duration.
    clear_stream_started_marker(spark, CHECKPOINT_URI)
    assert_progress(total, "silver-stream")
    spark.stop()


if __name__ == "__main__":
    main()
