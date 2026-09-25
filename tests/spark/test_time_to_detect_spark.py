"""Executed: continuous time to detect counts only newly raised alerts, keys
them by content (alert_id is a uuid redrawn every tick), and measures each
from the newest bronze ingest of its related transactions."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))

_ALERTS = "alert_id string, rule_id string, entity_id long, related_txn_ids array<string>"


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


def _epoch(s):
    # Timezone-aware, so PySpark stores the instant regardless of the host's
    # local zone (a naive value is read as local time).
    return datetime.fromtimestamp(s, tz=timezone.utc)


def _txns(spark):
    # ingest_ts in epoch seconds from 1_000_000.
    rows = [("t1", 100), ("t2", 160), ("t3", 50), ("t4", 300), ("t5", None)]
    return spark.createDataFrame(
        [(u, _epoch(1_000_000 + s) if s is not None else None) for u, s in rows],
        "uetr string, ingest_ts timestamp",
    )


def test_only_new_content_is_measured_from_its_newest_transaction(spark):
    from gold_refresh_financial import new_alert_arrivals, new_alert_txns, ttd_stats

    prior = spark.createDataFrame(
        [
            ("old-uuid-a", "W2_structuring", 1, ["t1", "t3"]),
            ("old-uuid-b", "W3_round_tripping", 2, ["t2"]),
        ],
        _ALERTS,
    )
    current = spark.createDataFrame(
        [
            # Same content as prior alert a, new uuid, txns in another order.
            ("new-uuid-a", "W2_structuring", 1, ["t3", "t1"]),
            # Prior alert b grew: t4 joined, so it is raised again.
            ("new-uuid-b", "W3_round_tripping", 2, ["t2", "t4"]),
            # Brand new alert, newest txn t2 (ingest 160).
            ("new-uuid-c", "W17_layering_chain", 3, ["t1", "t2"]),
            # Its twin (same content): counted once.
            ("new-uuid-c2", "W17_layering_chain", 3, ["t2", "t1"]),
            # Related transaction absent from silver.
            ("new-uuid-d", "W4_risk_propagation", 4, ["missing"]),
            # Only a transaction with no ingest_ts.
            ("new-uuid-e", "W4_risk_propagation", 5, ["t5"]),
        ],
        _ALERTS,
    )
    arrivals = {
        r["_key"]: r["arrival_ts"]
        for r in new_alert_arrivals(new_alert_txns(current, prior), _txns(spark)).collect()
    }
    assert len(arrivals) == 4
    # collect() returns naive local times; timestamp() reads them as local.
    got = sorted(v.timestamp() for v in arrivals.values() if v is not None)
    assert got == [1_000_160, 1_000_300]
    assert sum(v is None for v in arrivals.values()) == 2

    # Broadcast and shuffled lookups agree. late_before_s = 200: alert c's
    # newest transaction (160) was in silver before the previous pass.
    for small in (True, False):
        arrivals = new_alert_arrivals(new_alert_txns(current, prior), _txns(spark), small=small)
        stats = ttd_stats(arrivals, 1_000_425.0, late_before_s=1_000_200.0, bin_s=10)
        # ttd: c = 425 - 160 = 265 -> bin 26; b = 425 - 300 = 125 -> bin 12.
        assert stats == {
            "alerts": 2,
            "late": 1,
            "unmatched": 2,
            "max_s": pytest.approx(265.0),
            "bin_s": 10,
            "bins": {12: 1, 26: 1},
        }


def test_no_prior_snapshot_means_every_alert_is_new(spark):
    from gold_refresh_financial import new_alert_arrivals, new_alert_txns, ttd_stats

    current = spark.createDataFrame([("u", "W2_structuring", 1, ["t4"])], _ALERTS)
    stats = ttd_stats(new_alert_arrivals(new_alert_txns(current, None), _txns(spark)), 1_000_290.0)
    # Bronze and gold driver clocks can disagree by a little: clamped at 0.
    assert stats["alerts"] == 1 and stats["max_s"] == 0.0 and stats["bins"] == {0: 1}


def test_empty_tick_logs_a_parseable_line(spark):
    from common import ttd_line
    from gold_refresh_financial import (
        _empty_ttd_stats,
        new_alert_arrivals,
        new_alert_txns,
        ttd_stats,
    )

    empty = spark.createDataFrame([], _ALERTS)
    stats = ttd_stats(new_alert_arrivals(new_alert_txns(empty, empty), _txns(spark)), 1.0)
    assert stats == _empty_ttd_stats()
    assert ttd_line(7, stats) == (
        "Cycle 7: time to detect alerts=0 late=0 unmatched=0 max=-s bin=10s bins="
    )
