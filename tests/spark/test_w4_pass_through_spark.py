"""Executed: W4 finds a rapid pass-through, including one that straddles a
velocity-window bucket boundary (its join is keyed on the bucket), and
ignores a forward that comes too late or carries too little."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
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
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def _df(spark, rows):
    """rows: (uetr, originator, beneficiary, hours after t0, usd amount)."""
    t0 = datetime(2024, 3, 1, tzinfo=timezone.utc)  # 00:00 UTC, a 6 h bucket boundary
    return spark.createDataFrame(
        [(u, a, b, t0 + timedelta(hours=h), float(amt)) for u, a, b, h, amt in rows],
        "uetr string, originator_id long, beneficiary_id long, "
        "txn_timestamp timestamp, txn_amount_usd double",
    )


def test_pass_through_found_across_bucket_boundary(spark):
    from detection_rules import w4_risk_propagation

    rows = [
        # Credit at 05:00, forward at 07:00: crosses the 06:00 boundary.
        ("i1", 1, 2, 5, 1000),
        ("o1", 2, 3, 7, 950),
        # Forward 7 h after the credit: too late.
        ("i2", 10, 11, 1, 1000),
        ("o2", 11, 12, 8, 990),
        # Forward in time but only half the amount.
        ("i3", 20, 21, 1, 1000),
        ("o3", 21, 22, 2, 500),
    ]
    out = w4_risk_propagation(_df(spark, rows), run_id="r").collect()
    assert [(a["entity_id"], sorted(a["related_txn_ids"])) for a in out] == [(2, ["i1", "o1"])]
