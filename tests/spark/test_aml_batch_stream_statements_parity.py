"""D-full-simple: batch and stream produce the SAME silver.account_statements
and silver.accounts.current_balance for the same bronze corpus.

Runs the same 10-message bronze corpus twice against separate Iceberg
tables:

* batch: ``silver_build_financial.build_statements`` +
  ``update_accounts_balance`` (the same helpers ``main`` calls).
* stream: five micro-batches through
  ``silver_stream_financial._merge_batch``.

Asserts a row-hash of every entry (iban, entry_seq, cdt_dbt_ind, amt,
bal_before, bal_after, txn_id, uetr) matches, and per-iban
current_balance matches. Sentinel columns (_batch_id, _stream_id) are
excluded from the hash because batch mode writes NULL / 'batch' while
stream writes the streaming_query_id + batchId; they compare separately.

Byte-identical parity holds ONLY under strict-monotone bronze arrival
(single-pod datagen: DatagenConfig.parallelism=1). Multi-pod datagen
lets an earlier-time file from pod A land on FlashBlade AFTER a
later-time file from pod B, so a stream batch can carry a row whose
book_ts is earlier than a previously committed one for the same iban.
The stream cannot rewrite past rows and therefore assigns entry_seq in
arrival order, which diverges from batch mode's globally-sorted
entry_seq -- silently correct as arrival-order running balance, silently
wrong as batch-mode parity. Under LB_SILVER_STATEMENTS_STRICT_PARITY=1
the stream refuses to publish that arrival-order state; the run fails
loud with SilverAbort (see test_late_arrival_strict_refuses.py). Under
the default posture the stream labels the batch as
`silver_statements_parity_mode=arrival_order_running_balance` (see
test_late_arrival_labelled.py).

This parity test therefore constructs micro-batches with strictly
monotonic book_ts per iban across batches (batch N > batch N-1). This
is the ordering assumption D-full-simple's per-batch cumsum needs to
reproduce batch mode's global cumsum by associativity of addition and
matching Window tie-breaks (book_ts, txn_id, _cdt_dbt_ord).
"""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from datetime import timedelta

import pytest
from _foreach_batch import foreach_batch_harness

pytest.importorskip("pyspark")

pytestmark = [
    pytest.mark.requires_jars("iceberg"),
    pytest.mark.usefixtures("load_script"),
    pytest.mark.aml_parity,
]


def test_batch_stream_parity_in_a_fresh_jvm(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=900)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    assert not problems(out), out


def problems(out):
    """The guard's checks on the child's JSON, as named failures (the parity
    mutation check reads them too)."""
    found = []
    if out["statements_row_hash_batch"] != out["statements_row_hash_stream"]:
        found.append("statements_row_hash")
    if out["current_balance_batch"] != out["current_balance_stream"]:
        found.append("current_balance")
    if out["batch_row_count"] != out["stream_row_count"]:
        found.append("row_count")
    if not out["batch_row_count"] > 0:
        found.append("no_rows")
    return found


def _run(jars):
    from _d_full_helpers import (
        BASE_TS,
        STATEMENTS_DDL,
        bind_stream_module,
        bootstrap_catalog,
        bronze_batch,
        build_spark,
        seed_account,
    )

    with tempfile.TemporaryDirectory() as work:
        spark = build_spark(work, jars)
        bootstrap_catalog(spark)

        # Five micro-batches; strictly monotonic book_ts per iban across
        # batches. Two IBANs share the whole trace so parity is checked on
        # both sides of each payment.
        batches = [
            [("B0T0", BASE_TS + timedelta(hours=0), "GB01", "US02", "100.00")],
            [("B1T0", BASE_TS + timedelta(hours=24), "GB01", "US02", "50.00")],
            [("B2T0", BASE_TS + timedelta(hours=48), "GB01", "US02", "25.00")],
            [("B3T0", BASE_TS + timedelta(hours=72), "GB01", "US02", "10.00")],
            [("B4T0", BASE_TS + timedelta(hours=96), "GB01", "US02", "5.00")],
        ]
        all_rows = [row for b in batches for row in b]

        # Seed silver.accounts for BOTH the stream and the batch scoped tables.
        for iban in ("GB01", "US02"):
            seed_account(spark, iban)

        # --- STREAM pass on lh.silver.account_statements + lh.silver.accounts.
        ss = bind_stream_module(spark)
        spark.sparkContext.setJobGroup("stream-run-1", "test")
        for bid, batch in enumerate(batches):
            foreach_batch_harness(spark, ss._merge_batch, bronze_batch(spark, batch), bid)

        stream_stmts = _stmt_rows(spark, "lh.silver.account_statements")
        stream_cb = _current_balance_map(spark, "lh.silver.accounts")

        # --- BATCH pass: create a separate table, run build_statements against
        # the full bronze corpus once, update accounts.current_balance.
        spark.sql(STATEMENTS_DDL.replace("lh.silver.account_statements", "lh.silver.stmts_batch"))
        spark.sql(
            """
            CREATE TABLE lh.silver.accounts_batch (
                account_id BIGINT NOT NULL,
                iban STRING,
                holder_entity_id BIGINT NOT NULL,
                bank_bic STRING NOT NULL,
                currency STRING NOT NULL,
                opened_date DATE NOT NULL,
                closed_date DATE,
                current_balance DECIMAL(38, 2),
                home_fi STRING,
                is_customer BOOLEAN
            ) USING iceberg
            """
        )
        # Seed the batch-scoped accounts table with the same rows.
        _copy_accounts(spark, "lh.silver.accounts", "lh.silver.accounts_batch")

        import silver_build_financial as sbf
        from pyspark.sql.functions import lit

        bronze_all = bronze_batch(spark, all_rows)
        accts_batch = spark.table("lh.silver.accounts_batch")
        stmts_df = sbf.build_statements(bronze_all, accts_batch)
        stmts_df.writeTo("lh.silver.stmts_batch").append()

        stmts_written = spark.table("lh.silver.stmts_batch")
        updated = sbf.update_accounts_balance(accts_batch, stmts_written)
        updated.select(*accts_batch.columns).writeTo("lh.silver.accounts_batch").overwrite(
            lit(True)
        )

        batch_stmts = _stmt_rows(spark, "lh.silver.stmts_batch")
        batch_cb = _current_balance_map(spark, "lh.silver.accounts_batch")

        out = {
            "statements_row_hash_batch": _row_hash(batch_stmts),
            "statements_row_hash_stream": _row_hash(stream_stmts),
            "batch_row_count": len(batch_stmts),
            "stream_row_count": len(stream_stmts),
            "current_balance_batch": batch_cb,
            "current_balance_stream": stream_cb,
        }
        print(json.dumps(out, default=str))
        spark.stop()


def _stmt_rows(spark, fq_table):
    return spark.sql(
        f"SELECT iban, entry_seq, cdt_dbt_ind, amt, bal_before, bal_after, "
        f"txn_id, uetr, bk_tx_cd, book_ts "
        f"FROM {fq_table} ORDER BY iban, entry_seq"
    ).collect()


def _current_balance_map(spark, fq_table):
    return {
        r["iban"]: str(r["current_balance"])
        for r in spark.sql(f"SELECT iban, current_balance FROM {fq_table}").collect()
    }


def _row_hash(rows):
    return hashlib.sha256(
        "|".join(
            f"{r['iban']}:{r['entry_seq']}:{r['cdt_dbt_ind']}:{r['amt']}:"
            f"{r['bal_before']}:{r['bal_after']}:{r['txn_id']}:{r['uetr']}:"
            f"{r['bk_tx_cd']}:{r['book_ts']}"
            for r in rows
        ).encode("utf-8")
    ).hexdigest()


def _copy_accounts(spark, src, dst):
    spark.sql(f"INSERT INTO {dst} SELECT * FROM {src}")


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts and tests/spark on
    # PYTHONPATH and passes the jar classpath.
    import _parity_mutation

    _parity_mutation.install()
    _run(sys.argv[1])
