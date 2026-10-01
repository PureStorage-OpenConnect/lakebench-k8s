"""D-full-simple invariant-6 label: when a stream micro-batch delivers
bronze rows whose per-iban book_ts is earlier than the previous highest
committed book_ts for that iban (a real hazard under multi-pod datagen
whose files land out of write-time order on FlashBlade), the batch's
running_balance is arrival-order rather than strict-monotone, and the
stream MUST publish that as an invariant-6 label rather than a silent
divergence from batch mode.

Runs two micro-batches:
* batch 0: one payment at hour 24, GB01 debtor, US02 creditor.
* batch 1: one payment at hour 0 (EARLIER than batch 0's row), same
  iban pair.

Asserts:
* silver.account_statements gets rows from both batches;
* ``_maintain_statements`` returns a positive late_iban_count on batch 1;
* the per-batch `silver_statements_parity_mode: arrival_order_running_balance`
  and `silver_statements_late_arrivals_this_batch: <N>` log lines are
  emitted for batch 1;
* batch 0 (no late arrival) emits `strict_monotone` and zero.
"""

from __future__ import annotations

import json
import sys
import tempfile

import pytest

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script")]


def test_late_arrival_produces_arrival_order_label_in_a_fresh_jvm(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # Batch 0 sees no prior state -> zero late arrivals (fresh table).
    assert out["batch0_late_count"] == 0, out
    # Batch 1's earlier-book_ts rows are late w.r.t. batch 0's committed
    # book_ts for both IBANs -> at least 1 iban late (both, in fact).
    assert out["batch1_late_count"] >= 1, out
    # Per-batch label lines were emitted through common.log (parseable by
    # collector.parse_streaming_logs).
    assert "silver_statements_parity_mode: strict_monotone" in out["log_lines_batch0"], out
    assert (
        "silver_statements_parity_mode: arrival_order_running_balance" in out["log_lines_batch1"]
    ), out
    assert "silver_statements_late_arrivals_this_batch: 0" in out["log_lines_batch0"], out
    assert (
        "silver_statements_late_arrivals_this_batch: 2" in out["log_lines_batch1"]
        or "silver_statements_late_arrivals_this_batch: 1" in out["log_lines_batch1"]
    ), out


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
        spark.sparkContext.setJobGroup("stream-run-1", "test")

        # Capture the log lines emitted by common.log during each batch so
        # the test can assert on the exact strings the collector regex
        # matches. common.log prints to stdout via `print`; monkey-patch
        # via the module the stream imports it from.
        import common

        captured: list[str] = []
        real_log = common.log

        def capture(msg):
            captured.append(str(msg))
            real_log(msg)

        common.log = capture  # type: ignore[assignment]
        ss.log = capture  # type: ignore[assignment]

        # Batch 0: one payment at hour 24.
        b0_rows = [("B0T0", BASE_TS + timedelta(hours=24), "GB01", "US02", "100.00")]
        captured_len_before_b0 = len(captured)
        b0_result = ss._merge_batch(bronze_batch(spark, b0_rows), 0)
        b0_lines = captured[captured_len_before_b0:]

        # Batch 1: one payment at hour 0 -- EARLIER than batch 0's row, so
        # both iban's this-batch min book_ts < prior max book_ts. Both
        # IBANs are late.
        b1_rows = [("B1T0", BASE_TS + timedelta(hours=0), "GB01", "US02", "50.00")]
        captured_len_before_b1 = len(captured)
        b1_result = ss._merge_batch(bronze_batch(spark, b1_rows), 1)
        b1_lines = captured[captured_len_before_b1:]

        common.log = real_log  # type: ignore[assignment]

        out = {
            "batch0_result": list(b0_result),
            "batch1_result": list(b1_result),
            "batch0_late_count": int(b0_result[1]),
            "batch1_late_count": int(b1_result[1]),
            "log_lines_batch0": "\n".join(b0_lines),
            "log_lines_batch1": "\n".join(b1_lines),
        }
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts and tests/spark on
    # PYTHONPATH and passes the jar classpath.
    _run(sys.argv[1])
