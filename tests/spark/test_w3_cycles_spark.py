"""Executed: W3 finds planted-style multi-hop cycles once, respects the hop
window, and excludes hubs as intermediaries (D2: the old A->B->A self-join
could not detect cycle or cross_border_cycle at all)."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


def _df(spark, rows):
    t0 = datetime(2024, 3, 1)
    return spark.createDataFrame(
        [(u, a, b, t0 + timedelta(hours=h)) for u, a, b, h in rows],
        "uetr string, originator_id long, beneficiary_id long, txn_timestamp timestamp",
    )


def test_cycles_found_once_and_windows_respected(spark):
    from detection_rules import w3_round_tripping

    rows = [
        # 4-hop cycle 1->2->3->4->1, one day per hop.
        ("c1", 1, 2, 0),
        ("c2", 2, 3, 24),
        ("c3", 3, 4, 48),
        ("c4", 4, 1, 72),
        # 2-hop return 10->11->10.
        ("r1", 10, 11, 0),
        ("r2", 11, 10, 5),
        # Slow cycle: third hop 9 days after the second (hop window 7 d).
        ("s1", 20, 21, 0),
        ("s2", 21, 22, 24),
        ("s3", 22, 20, 24 + 9 * 24),
        # Listed out of order, but 31->30 at 5 h then 30->31 at 10 h is a
        # genuine round trip that starts at account 31.
        ("o1", 30, 31, 10),
        ("o2", 31, 30, 5),
    ]
    out = w3_round_tripping(_df(spark, rows), run_id="r").collect()
    found = {tuple(a["related_txn_ids"]) for a in out}
    assert ("c1", "c2", "c3", "c4") in found
    assert ("r1", "r2") in found
    assert ("o2", "o1") in found
    assert not any("s1" in f for f in found)
    assert len(out) == 3  # each cycle exactly once


def test_hub_is_not_an_intermediary(spark):
    from detection_rules import w3_round_tripping

    rows = [("h0", 1, 99, 0), ("h1", 99, 1, 1)]
    # Account 99 sends 300 payments in the same week: a hub.
    rows += [(f"x{i}", 99, 1000 + i, 2) for i in range(300)]
    out = w3_round_tripping(_df(spark, rows), max_out_degree=200, run_id="r").collect()
    assert not out
