"""Executed: W4 finds a rapid pass-through, including one that straddles a
velocity-window bucket boundary (its join is keyed on the bucket), and
ignores a forward that comes too late or carries too little."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")


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


def test_hub_alert_is_capped_sorted_and_says_so(spark):
    """AML-2 W4 evidence cap: a hub's related arrays are sorted and cut to
    max_txns_per_alert (1000), and the evidence map carries the full counts
    and the truncation flags. 1500 credits from 1500 senders, each forwarded
    to one of 1500 receivers within the hour: 3000 related txns and 3000
    related entities."""
    from detection_rules import w4_risk_propagation

    rows = []
    for i in range(1500):
        rows.append((f"in{i:05d}", 10_000 + i, 2, 1, 1000))
        rows.append((f"out{i:05d}", 2, 20_000 + i, 1.5, 950))
    rows.append(("si", 7, 8, 1, 1000))  # a small pass-through at entity 8
    rows.append(("so", 8, 9, 2, 990))
    out = {a["entity_id"]: a for a in w4_risk_propagation(_df(spark, rows), run_id="r").collect()}

    hub = out[2]
    every_txn = sorted([f"in{i:05d}" for i in range(1500)] + [f"out{i:05d}" for i in range(1500)])
    assert hub["related_txn_ids"] == every_txn[:1000]
    every_entity = sorted([10_000 + i for i in range(1500)] + [20_000 + i for i in range(1500)])
    assert hub["related_entity_ids"] == every_entity[:1000]
    assert hub["evidence"]["txn_total"] == "3000"
    assert hub["evidence"]["txns_truncated"] == "true"
    assert hub["evidence"]["entity_total"] == "3000"
    assert hub["evidence"]["entities_truncated"] == "true"

    small = out[8]
    assert small["related_txn_ids"] == ["si", "so"]
    assert small["related_entity_ids"] == [7, 9]
    assert small["evidence"]["txn_total"] == "2"
    assert small["evidence"]["txns_truncated"] == "false"
    assert small["evidence"]["entities_truncated"] == "false"


def test_cap_is_a_rule_parameter(spark):
    """The cap is a keyword of the rule, visible to a caller that builds
    the rule's arguments from its signature (gold_finalize today, and the
    shared rule parameters of reproduction later)."""
    from detection_rules import w4_risk_propagation

    rows = [("a1", 1, 2, 1, 1000), ("a2", 3, 2, 1, 1000), ("b1", 2, 4, 2, 990)]
    (alert,) = w4_risk_propagation(_df(spark, rows), run_id="r", max_txns_per_alert=2).collect()
    assert alert["related_txn_ids"] == ["a1", "a2"]
    assert alert["evidence"]["txn_total"] == "3"
    assert alert["evidence"]["txns_truncated"] == "true"
