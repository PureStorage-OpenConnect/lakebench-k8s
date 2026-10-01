"""D-full-simple: when ``LB_SILVER_STATEMENTS_STRICT_PARITY=1`` is set and
a stream micro-batch delivers late-arriving bronze rows for any iban,
``_maintain_statements`` raises ``SilverAbort`` so the run refuses to
publish an arrival-order running_balance masquerading as batch-mode
parity. Invariant 2 gate: batch/stream comparison is invalid under
non-monotone bronze arrival.

Runs the same "hour 24 then hour 0" sequence as
``test_late_arrival_labelled.py`` but with the strict env var set and
asserts ``SilverAbort`` fires on batch 1 (not batch 0, where the fresh
table has no prior state to compare against).
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


def test_strict_env_var_refuses_late_arrival_in_a_fresh_jvm():
    jar = iceberg_jar()
    if jar is None:
        pytest.skip("no iceberg-spark-runtime-4.0 jar available (set LB_TEST_ICEBERG_JAR)")
    res = subprocess.run(
        [sys.executable, __file__, jar], capture_output=True, text=True, timeout=600
    )
    assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-4000:]
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # Batch 0 commits cleanly (no prior state -> no late arrivals).
    assert out["batch0_committed"] is True, out
    # Batch 1 raises SilverAbort because the strict env var is set AND
    # the row is late w.r.t. batch 0's committed book_ts.
    assert out["batch1_raised_silver_abort"] is True, out
    # The exception message names the strict env var so an operator can
    # find the flag to unset without reading source.
    assert "LB_SILVER_STATEMENTS_STRICT_PARITY" in out["batch1_exception_message"], out


def _run(jar):
    from datetime import timedelta

    from _d_full_helpers import (
        BASE_TS,
        bind_stream_module,
        bootstrap_catalog,
        bronze_batch,
        build_spark,
        seed_account,
    )

    # Set the strict env var BEFORE any Spark or module import so the
    # module's ``os.getenv`` check sees it in the same process.
    os.environ["LB_SILVER_STATEMENTS_STRICT_PARITY"] = "1"

    with tempfile.TemporaryDirectory() as work:
        spark = build_spark(work, jar)
        bootstrap_catalog(spark)
        for iban in ("GB01", "US02"):
            seed_account(spark, iban)

        ss = bind_stream_module(spark)
        from common import SilverAbort

        spark.sparkContext.setJobGroup("stream-run-1", "test")

        # Batch 0: no prior state -> no late arrivals -> commits cleanly.
        b0_rows = [("B0T0", BASE_TS + timedelta(hours=24), "GB01", "US02", "100.00")]
        try:
            ss._merge_batch(bronze_batch(spark, b0_rows), 0)
            batch0_committed = True
        except SilverAbort:
            batch0_committed = False

        # Batch 1: earlier book_ts -> late arrival -> strict mode raises.
        b1_rows = [("B1T0", BASE_TS + timedelta(hours=0), "GB01", "US02", "50.00")]
        try:
            ss._merge_batch(bronze_batch(spark, b1_rows), 1)
            batch1_raised = False
            b1_msg = ""
        except SilverAbort as exc:
            batch1_raised = True
            b1_msg = str(exc)
        except Exception as exc:  # noqa: BLE001
            # Any other exception should NOT stop the assertions from
            # differentiating clearly: record the type so the failure
            # message names it.
            batch1_raised = False
            b1_msg = f"unexpected {type(exc).__name__}: {exc}"

        out = {
            "batch0_committed": batch0_committed,
            "batch1_raised_silver_abort": batch1_raised,
            "batch1_exception_message": b1_msg,
        }
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    sys.path[:0] = [str(_SCRIPTS), str(_HERE)]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    _run(sys.argv[1])
