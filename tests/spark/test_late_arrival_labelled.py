"""D-full-simple invariant-6 label: when a stream micro-batch delivers
bronze rows whose per-iban book_ts is earlier than the previous highest
committed book_ts for that iban (a real hazard under multi-pod datagen
whose files land out of write-time order on FlashBlade), the batch's
running_balance is arrival-order rather than strict-monotone, and the
stream MUST publish that as an invariant-6 label rather than a silent
divergence from batch mode.

Runs three micro-batches:
* batch 0: one payment at hour 24, GB01 debtor, US02 creditor.
* batch 1: one payment at hour 0 (EARLIER than batch 0's row), same
  iban pair.
* batch 2 (labelled posture only): one payment at hour 1 -- later than
  batch 1 but still earlier than batch 0, so the prior max book_ts must
  remain hour 24 and both ibans stay late. Guards the last_book_ts
  running-max against a per-batch-only value.

Without ``LB_SILVER_STATEMENTS_STRICT_PARITY``:
* ``_maintain_statements`` reports exactly 2 late ibans on batch 1 and 0 on
  batch 0;
* batch 1 emits `silver_statements_parity_mode: arrival_order_running_balance`
  and `silver_statements_late_arrivals_this_batch: 2`; batch 0 emits
  `strict_monotone` and zero.

With ``LB_SILVER_STATEMENTS_STRICT_PARITY=1`` batch 0 commits cleanly and
batch 1 raises ``SilverAbort`` instead of publishing an arrival-order
running_balance as batch-mode parity (invariant 2).
"""

from __future__ import annotations

import json
import sys
import tempfile

import pytest
from _foreach_batch import foreach_batch_harness

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script")]


_STRICT_ENV = "LB_SILVER_STATEMENTS_STRICT_PARITY"


@pytest.mark.parametrize("strict", [False, True], ids=["labelled", "strict"])
def test_late_arrival_in_a_fresh_jvm(spark_subprocess, spark_jars, strict):
    res = spark_subprocess(
        __file__,
        spark_jars.classpath,
        env={_STRICT_ENV: "1" if strict else "0"},
        timeout=600,
    )
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # Batch 0 sees no prior state: zero late arrivals, strict-monotone label.
    assert out["batch0_late_count"] == 0, out
    assert "silver_statements_parity_mode: strict_monotone" in out["log_lines_batch0"], out
    assert "silver_statements_late_arrivals_this_batch: 0" in out["log_lines_batch0"], out
    assert out["batch1_raised_silver_abort"] is strict, out
    if strict:
        return
    # Batch 1's earlier book_ts is late for both IBANs.
    assert out["batch1_late_count"] == 2, out
    assert (
        "silver_statements_parity_mode: arrival_order_running_balance" in out["log_lines_batch1"]
    ), out
    assert "silver_statements_late_arrivals_this_batch: 2" in out["log_lines_batch1"], out
    # Batch 2 (hour 1) is later than batch 1 (hour 0) but still earlier than
    # batch 0 (hour 24). The prior max book_ts must remain hour 24 across all
    # prior batches, so both IBANs are still late. Guards the last_book_ts
    # running-max (greatest) against a per-batch-only value that batch 1 would
    # have pushed back to hour 0, which would silently miss these late rows.
    assert out["batch2_late_count"] == 2, out
    assert (
        "silver_statements_parity_mode: arrival_order_running_balance" in out["log_lines_batch2"]
    ), out
    assert "silver_statements_late_arrivals_this_batch: 2" in out["log_lines_batch2"], out


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
        from common import SilverAbort

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
        b0_result = foreach_batch_harness(spark, ss._merge_batch, bronze_batch(spark, b0_rows), 0)
        b0_lines = captured[captured_len_before_b0:]

        # Batch 1: one payment at hour 0 -- EARLIER than batch 0's row, so
        # both iban's this-batch min book_ts < prior max book_ts. Both
        # IBANs are late.
        b1_rows = [("B1T0", BASE_TS + timedelta(hours=0), "GB01", "US02", "50.00")]
        captured_len_before_b1 = len(captured)
        raised = False
        b1_result = None
        try:
            b1_result = foreach_batch_harness(
                spark, ss._merge_batch, bronze_batch(spark, b1_rows), 1
            )
        except SilverAbort:
            raised = True
        b1_lines = captured[captured_len_before_b1:]

        # Batch 2: one payment at hour 1 -- later than batch 1's hour-0 row
        # but STILL earlier than batch 0's hour-24 row. This is the
        # last_book_ts regression guard: the stored last_book_ts must be the
        # running max over ALL prior batches (hour 24), not just the previous
        # batch's max (which batch 1 would have pushed back to hour 0). If
        # last_book_ts were taken from the latest batch's own rows it would
        # read hour 0 here, and batch 2's hour-1 min would NOT be < prior, so
        # both IBANs would be silently missed. With greatest(stored, batch)
        # the prior max stays hour 24 and both IBANs count as late.
        b2_rows = [("B2T0", BASE_TS + timedelta(hours=1), "GB01", "US02", "25.00")]
        captured_len_before_b2 = len(captured)
        b2_result = None
        b2_raised = False
        try:
            b2_result = foreach_batch_harness(
                spark, ss._merge_batch, bronze_batch(spark, b2_rows), 2
            )
        except SilverAbort:
            b2_raised = True
        b2_lines = captured[captured_len_before_b2:]

        common.log = real_log  # type: ignore[assignment]

        out = {
            "batch0_late_count": int(b0_result[1]),
            "batch1_late_count": None if raised else int(b1_result[1]),
            "batch1_raised_silver_abort": raised,
            "batch2_late_count": None if b2_raised else int(b2_result[1]),
            "log_lines_batch0": "\n".join(b0_lines),
            "log_lines_batch1": "\n".join(b1_lines),
            "log_lines_batch2": "\n".join(b2_lines),
        }
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts and tests/spark on
    # PYTHONPATH and passes the jar classpath.
    _run(sys.argv[1])
