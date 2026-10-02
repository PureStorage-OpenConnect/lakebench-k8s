"""D-full-simple: replaying the same micro-batch twice is a no-op on
silver.account_statements. Row counts and per-iban running_balance must
be identical to a single-run of the batch.

Structured Streaming retries a failed foreachBatch handler with the same
batchId. Without the (_stream_id, _batch_id) DELETE-INSERT the append
would silently double-count every payment. This test simulates that path
directly: it drives one batch through _merge_batch, snapshots the state,
then invokes _merge_batch a second time with the same (bronze, batch_id)
and asserts the snapshot is unchanged.
"""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile

import pytest
from _foreach_batch import foreach_batch_harness

pytest.importorskip("pyspark")

pytestmark = [
    pytest.mark.requires_jars("iceberg"),
    pytest.mark.usefixtures("load_script"),
    pytest.mark.aml_parity,
]


def test_replay_is_idempotent_in_a_fresh_jvm(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    assert not problems(out), out


def problems(out):
    """The guard's checks on the child's JSON, as named failures: the row
    count, the row hash of every entry (iban, entry_seq, bal_before,
    bal_after, txn_id, cdt_dbt_ind) and current_balance are unchanged by the
    replay. The parity mutation check reads them too."""
    found = []
    for name in ("row_count", "row_hash", "current_balance"):
        if out[f"{name}_after_first"] != out[f"{name}_after_replay"]:
            found.append(name)
    if not out["row_count_after_first"]:
        found.append("no_rows")
    return found


def _run(jars):
    from datetime import timedelta

    from _d_full_helpers import (
        BASE_TS,
        bind_stream_module,
        bootstrap_catalog,
        bronze_batch,
        build_spark,
        seed_account,
    )

    with tempfile.TemporaryDirectory() as work:
        spark = build_spark(work, jars)
        bootstrap_catalog(spark)
        for iban in ("GB01", "US02"):
            seed_account(spark, iban)

        ss = bind_stream_module(spark)

        # Two prior batches so the replay of batch 2 has to preserve the
        # earlier running-balance state, not just restore a first-batch case.
        rows_by_batch = {
            0: [("B0T0", BASE_TS + timedelta(hours=0), "GB01", "US02", "100.00")],
            1: [("B1T0", BASE_TS + timedelta(hours=24), "GB01", "US02", "50.00")],
            2: [
                ("B2T0", BASE_TS + timedelta(hours=48), "GB01", "US02", "25.00"),
                ("B2T1", BASE_TS + timedelta(hours=49), "GB01", "US02", "10.00"),
            ],
        }

        spark.sparkContext.setJobGroup("stream-run-1", "test")

        for bid in (0, 1, 2):
            df = bronze_batch(spark, rows_by_batch[bid])
            foreach_batch_harness(spark, ss._merge_batch, df, bid)

        snapshot_after_first = _snapshot(spark)

        # Simulate Structured Streaming's retry: a failed batch stops its
        # query, and the restarted query (same query id, new run id, which
        # Spark uses as the job group) runs batch 2 again as its first batch.
        # Within one run no batch repeats, so the replay check runs only on a
        # run's first batch (common.replay_possible).
        spark.sparkContext.setJobGroup("stream-run-2", "test")
        df_replay = bronze_batch(spark, rows_by_batch[2])
        foreach_batch_harness(spark, ss._merge_batch, df_replay, 2)

        snapshot_after_replay = _snapshot(spark)

        out = {
            "row_count_after_first": snapshot_after_first["row_count"],
            "row_count_after_replay": snapshot_after_replay["row_count"],
            "row_hash_after_first": snapshot_after_first["row_hash"],
            "row_hash_after_replay": snapshot_after_replay["row_hash"],
            "current_balance_after_first": snapshot_after_first["current_balance"],
            "current_balance_after_replay": snapshot_after_replay["current_balance"],
        }
        print(json.dumps(out, default=str))
        spark.stop()


def _snapshot(spark):
    """Row hash + row count of silver.account_statements + current_balance map."""
    stmts = spark.sql(
        "SELECT iban, entry_seq, cdt_dbt_ind, bal_before, bal_after, txn_id "
        "FROM lh.silver.account_statements ORDER BY iban, entry_seq"
    ).collect()
    row_hash = hashlib.sha256(
        "|".join(
            f"{r['iban']}:{r['entry_seq']}:{r['cdt_dbt_ind']}:"
            f"{r['bal_before']}:{r['bal_after']}:{r['txn_id']}"
            for r in stmts
        ).encode("utf-8")
    ).hexdigest()
    cb = {
        r["iban"]: (None if r["current_balance"] is None else str(r["current_balance"]))
        for r in spark.sql("SELECT iban, current_balance FROM lh.silver.accounts").collect()
    }
    return {"row_count": len(stmts), "row_hash": row_hash, "current_balance": cb}


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts and tests/spark on
    # PYTHONPATH and passes the jar classpath.
    import _parity_mutation

    _parity_mutation.install()
    _run(sys.argv[1])
