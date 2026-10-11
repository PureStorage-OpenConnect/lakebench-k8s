"""D-full-simple: after stream micro-batches complete, silver.accounts
`current_balance` for every touched iban equals silver.account_statements
`bal_after` at row_number()=1 OVER (PARTITION BY iban ORDER BY book_ts
DESC, entry_seq DESC).

Runs three micro-batches with two payments each (four distinct IBANs
across all three batches, some IBANs appearing multiple times), asserts:

* every touched iban's current_balance == its latest statement's
  bal_after (query the same row_number()=1 window as production);
* an untouched iban keeps its NULL current_balance (the MERGE is scoped
  to the batch's touched set, never overwriting an inactive account).
"""

from __future__ import annotations

import json
import sys
import tempfile

import pytest
from _foreach_batch import foreach_batch_harness

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script")]


# Net movement per iban over the three batches, debits negative:
# GB01: -100 -50 -25 +5; US02: +100 -10 -3; DE03: +50 +10; FR04: +25 -5 +3.
NET_MOVEMENT = {"GB01": -170.0, "US02": 87.0, "DE03": 60.0, "FR04": 23.0}


def test_current_balance_matches_last_statement_in_a_fresh_jvm(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # The latest statements are the opening balance plus the hand-computed
    # net movement; the table under test is not its own oracle.
    assert set(out["expected_map"]) == set(NET_MOVEMENT), out
    for iban, net in NET_MOVEMENT.items():
        assert out["expected_map"][iban] == out["opening"][iban] + net, (iban, out)
    # Every touched iban's current_balance matches its latest statement.
    assert out["mismatches"] == [], out
    # Untouched iban keeps its NULL current_balance -- the MERGE never
    # visited it.
    assert out["untouched_current_balance_is_null"] is True, out


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

    # Three micro-batches, each with two payments. Use varied iban pairs so
    # some IBANs appear only in some batches and current_balance reflects
    # the most-recent-touching batch.
    batches = [
        [
            ("B0T0", BASE_TS + timedelta(hours=0), "GB01", "US02", "100.00"),
            ("B0T1", BASE_TS + timedelta(hours=1), "GB01", "DE03", "50.00"),
        ],
        [
            ("B1T0", BASE_TS + timedelta(hours=24), "GB01", "FR04", "25.00"),
            ("B1T1", BASE_TS + timedelta(hours=25), "US02", "DE03", "10.00"),
        ],
        [
            ("B2T0", BASE_TS + timedelta(hours=48), "FR04", "GB01", "5.00"),
            ("B2T1", BASE_TS + timedelta(hours=49), "US02", "FR04", "3.00"),
        ],
    ]

    with tempfile.TemporaryDirectory() as work:
        spark = build_spark(work, jars)
        bootstrap_catalog(spark)
        # Seed silver.accounts for the touched IBANs and one untouched
        # control iban (IT99). The stream must never touch IT99.
        for iban in ("GB01", "US02", "DE03", "FR04", "IT99"):
            seed_account(spark, iban)

        ss = bind_stream_module(spark)
        from common import aml_opening_balance
        from pyspark.sql.functions import lit, xxhash64

        opening = {
            iban: float(
                spark.range(1)
                .select(aml_opening_balance(xxhash64(lit(iban))).alias("o"))
                .first()["o"]
            )
            for iban in ("GB01", "US02", "DE03", "FR04")
        }
        spark.sparkContext.setJobGroup("stream-run-1", "test")

        for bid, rows in enumerate(batches):
            df = bronze_batch(spark, rows)
            foreach_batch_harness(spark, ss._merge_batch, df, bid)

        # Expected: query the same row_number=1 window the production MERGE
        # source uses.
        expected = spark.sql(
            """
            SELECT iban, bal_after AS expected FROM (
                SELECT iban, bal_after,
                    row_number() OVER (PARTITION BY iban ORDER BY book_ts DESC, entry_seq DESC) AS rn
                FROM lh.silver.account_statements
            ) WHERE rn = 1
            """
        ).collect()
        expected_map = {r["iban"]: float(r["expected"]) for r in expected}

        actual_map = {
            r["iban"]: (None if r["current_balance"] is None else float(r["current_balance"]))
            for r in spark.sql("SELECT iban, current_balance FROM lh.silver.accounts").collect()
        }

        mismatches = []
        for iban, exp in expected_map.items():
            act = actual_map.get(iban)
            if act is None or act != exp:
                mismatches.append({"iban": iban, "expected": exp, "actual": act})

        untouched = actual_map.get("IT99")
        out = {
            "expected_map": expected_map,
            "opening": opening,
            "actual_map": actual_map,
            "mismatches": mismatches,
            "untouched_current_balance_is_null": untouched is None,
        }
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts and tests/spark on
    # PYTHONPATH and passes the jar classpath.
    _run(sys.argv[1])
