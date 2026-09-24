"""Executed: W2's beneficiary aggregation catches several senders structuring
into one account (micro_structuring plants 8 distinct senders, which the
per-originator count can never see), and the originator form is unchanged."""

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
    """rows: (uetr, originator, beneficiary, hours after t0, amount, currency)."""
    t0 = datetime(2024, 3, 1, tzinfo=timezone.utc)
    return spark.createDataFrame(
        [(u, a, b, t0 + timedelta(hours=h), float(amt), c) for u, a, b, h, amt, c in rows],
        "uetr string, originator_id long, beneficiary_id long, "
        "txn_timestamp timestamp, txn_amount double, txn_currency string",
    )


def _alerts(spark, rows):
    from detection_rules import w2_structuring

    return w2_structuring(_df(spark, rows), run_id="r").collect()


def test_several_senders_into_one_beneficiary(spark):
    rows = [
        # Three senders, each in its own currency's band, same day.
        ("m1", 1, 9, 1, 9500, "USD"),
        ("m2", 2, 9, 5, 14_000, "EUR"),
        ("m3", 3, 9, 9, 9800, "USD"),
        # Out of band: does not count.
        ("m4", 4, 9, 10, 2000, "USD"),
    ]
    out = _alerts(spark, rows)
    assert len(out) == 1
    a = out[0]
    assert a["alert_type"] == "structuring_beneficiary"
    assert a["evidence"]["aggregation"] == "beneficiary"
    assert a["entity_id"] == 9
    assert sorted(a["related_txn_ids"]) == ["m1", "m2", "m3"]
    assert sorted(a["related_entity_ids"]) == [1, 2, 3]


def test_two_credits_or_different_days_do_not_fire(spark):
    rows = [
        ("d1", 1, 9, 1, 9500, "USD"),
        ("d2", 2, 9, 5, 9600, "USD"),
        # Next UTC day.
        ("d3", 3, 9, 30, 9700, "USD"),
    ]
    assert _alerts(spark, rows) == []


def test_originator_form_unchanged(spark):
    rows = [
        ("o1", 5, 11, 1, 9500, "USD"),
        ("o2", 5, 12, 2, 9600, "USD"),
        ("o3", 5, 13, 3, 9700, "USD"),
    ]
    out = _alerts(spark, rows)
    assert [a["alert_type"] for a in out] == ["structuring"]
    assert out[0]["entity_id"] == 5
    assert out[0]["evidence"]["aggregation"] == "originator"
    assert out[0]["alert_score"] == pytest.approx(0.5)


def test_single_sender_is_only_the_originator_kind(spark):
    """One smurf paying one account three times raised both kinds on the
    same transactions; the beneficiary kind now needs two senders."""
    rows = [(f"u{i}", 7, 9, i, 9500 + i, "USD") for i in range(3)]
    out = _alerts(spark, rows)
    assert [a["alert_type"] for a in out] == ["structuring"]


def test_burst_across_day_boundaries_is_caught(spark):
    """Eight senders, two credits per UTC day across four days: no tumbling
    day holds three, but three fall within 24 h of each other."""
    hours = [20, 23, 25, 46, 49, 70, 73, 94]
    rows = [(f"b{i}", 100 + i, 9, h, 9500, "USD") for i, h in enumerate(hours)]
    out = [a for a in _alerts(spark, rows) if a["alert_type"] == "structuring_beneficiary"]
    sets = [set(a["related_txn_ids"]) for a in out]
    assert {"b0", "b1", "b2"} in sets
    # No reported window is contained in another.
    assert not any(a < b for a in sets for b in sets)
    for a in out:
        assert a["entity_id"] == 9
        ts = sorted(hours[int(u[1:])] for u in a["related_txn_ids"])
        assert ts[-1] - ts[0] < 24


def test_window_is_shorter_than_24_hours(spark):
    """Credits exactly 24 h apart are not within one window."""
    rows = [("w0", 1, 9, 0, 9500, "USD"), ("w1", 2, 9, 12, 9500, "USD")]
    rows.append(("w2", 3, 9, 24, 9500, "USD"))
    assert _alerts(spark, rows) == []


def test_score_is_capped(spark):
    rows = [(f"c{i}", 5, 20 + i, i * 0.5, 9500, "USD") for i in range(13)]
    out = _alerts(spark, rows)
    assert [a["alert_type"] for a in out] == ["structuring"]
    assert out[0]["alert_score"] == pytest.approx(0.95)
