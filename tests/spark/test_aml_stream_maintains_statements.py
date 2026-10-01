"""D-full-simple: silver_stream_financial._merge_batch writes
silver.account_statements rows for every touched account and the
running_balance grows as a strictly monotonic sum of the batch's signed
amounts.

Test drives five micro-batches through _merge_batch (skipping the KYC
and dimension paths that need separate seed data) and asserts:

* silver.account_statements has 2 rows per input pacs.008 message
  (one DBIT on the debtor side, one CRDT on the creditor side);
* both touched IBANs have rows;
* per-iban running balance (bal_after) is monotonic in entry_seq;
* the first batch's opening bal_after uses the deterministic
  ``abs(xxhash64(iban)) % 200_000 + 10_000`` formula, matching batch mode.

Runs in a subprocess so a fresh JVM picks up the Iceberg jar.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

_HERE = Path(__file__).resolve().parent
_SCRIPTS = _HERE.parents[1] / "src/lakebench/spark/scripts"

pytestmark = pytest.mark.usefixtures("load_script")
from _d_full_helpers import iceberg_jar  # noqa: E402


def test_stream_maintains_statements_in_a_fresh_jvm():
    jar = iceberg_jar()
    if jar is None:
        pytest.skip("no iceberg-spark-runtime-4.0 jar available (set LB_TEST_ICEBERG_JAR)")
    res = subprocess.run(
        [sys.executable, __file__, jar], capture_output=True, text=True, timeout=600
    )
    assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-4000:]
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # 5 micro-batches x 1 payment each = 5 payments = 10 statement entries
    # (one DBIT + one CRDT per payment). Both touched IBANs have 5 rows.
    assert out["total_statement_rows"] == 10, out
    assert out["rows_per_iban"] == {"GB01": 5, "US02": 5}, out
    # bal_after is monotonic in entry_seq per iban (strictly monotonic for
    # non-zero signed_amt, which the test uses).
    assert out["monotonic_per_iban"] is True, out
    # Deterministic opening balance parity with batch mode: entry_seq=1 has
    # bal_after = opening + signed_amt.
    assert out["opening_balance_parity_gb01"] is True, out
    assert out["opening_balance_parity_us02"] is True, out
    # current_balance on silver.accounts matches the last bal_after per iban.
    assert out["current_balance_matches_last_bal_after_gb01"] is True, out
    assert out["current_balance_matches_last_bal_after_us02"] is True, out


def _run(jar):
    from _d_full_helpers import (
        BASE_TS,
        batch_rows,
        bind_stream_module,
        bootstrap_catalog,
        bronze_batch,
        build_spark,
        opening_balance_sql,
        seed_account,
    )

    with tempfile.TemporaryDirectory() as work:
        spark = build_spark(work, jar)
        bootstrap_catalog(spark)
        for iban in ("GB01", "US02"):
            seed_account(spark, iban)

        ss = bind_stream_module(spark)

        spark.sparkContext.setJobGroup("stream-run-1", "test")
        for bid in range(5):
            rows = batch_rows(bid, count=1)
            df = bronze_batch(spark, rows)
            ss._merge_batch(df, bid)

        stmts = spark.sql(
            "SELECT iban, entry_seq, cdt_dbt_ind, amt, bal_before, bal_after "
            "FROM lh.silver.account_statements ORDER BY iban, entry_seq"
        ).collect()

        rows_per_iban = {"GB01": 0, "US02": 0}
        for r in stmts:
            rows_per_iban[r["iban"]] += 1

        # Monotonicity per iban: entry_seq is 1..N, bal_after either strictly
        # rising (CRDT flow) or strictly falling (DBIT flow) at each step
        # in this test. With unique signed_amt sign per iban, the sequence
        # is strictly monotonic.
        def _monotonic(sequence):
            if len(sequence) < 2:
                return True
            direction = None
            for a, b in zip(sequence, sequence[1:], strict=False):
                d = 1 if b > a else -1 if b < a else 0
                if d == 0:
                    return False  # equal values are not strictly monotonic
                if direction is None:
                    direction = d
                elif direction != d:
                    return False
            return True

        gb01 = [r["bal_after"] for r in stmts if r["iban"] == "GB01"]
        us02 = [r["bal_after"] for r in stmts if r["iban"] == "US02"]
        monotonic_per_iban = _monotonic(gb01) and _monotonic(us02)

        # Opening balance parity: at entry_seq=1, bal_after = opening + signed_amt.
        # GB01 is debtor (DBIT, negative signed_amt); US02 is creditor (CRDT).
        first_gb01 = next(r for r in stmts if r["iban"] == "GB01" and r["entry_seq"] == 1)
        first_us02 = next(r for r in stmts if r["iban"] == "US02" and r["entry_seq"] == 1)

        expected_open_gb01 = spark.sql(f"SELECT {opening_balance_sql('GB01')} AS ob").collect()[0][
            "ob"
        ]
        expected_open_us02 = spark.sql(f"SELECT {opening_balance_sql('US02')} AS ob").collect()[0][
            "ob"
        ]

        # first_gb01 is DBIT of 100.00, so bal_after = opening - 100
        # first_us02 is CRDT of 100.00, so bal_after = opening + 100
        parity_gb01 = float(first_gb01["bal_after"]) == float(expected_open_gb01) - 100.0
        parity_us02 = float(first_us02["bal_after"]) == float(expected_open_us02) + 100.0

        # current_balance on silver.accounts must match the last bal_after per iban.
        last_gb01 = gb01[-1]
        last_us02 = us02[-1]
        cb = {
            r["iban"]: r["current_balance"]
            for r in spark.sql("SELECT iban, current_balance FROM lh.silver.accounts").collect()
        }
        cb_match_gb01 = float(cb["GB01"]) == float(last_gb01)
        cb_match_us02 = float(cb["US02"]) == float(last_us02)

        out = {
            "total_statement_rows": len(stmts),
            "rows_per_iban": rows_per_iban,
            "monotonic_per_iban": monotonic_per_iban,
            "opening_balance_parity_gb01": parity_gb01,
            "opening_balance_parity_us02": parity_us02,
            "current_balance_matches_last_bal_after_gb01": cb_match_gb01,
            "current_balance_matches_last_bal_after_us02": cb_match_us02,
            "base_ts_iso": BASE_TS.isoformat(),
        }
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    sys.path[:0] = [str(_SCRIPTS), str(_HERE)]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    _run(sys.argv[1])
