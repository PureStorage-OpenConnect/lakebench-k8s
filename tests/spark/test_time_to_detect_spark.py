"""Executed: continuous time to detect counts only newly raised alerts, keys
them by content (alert_id is a uuid redrawn every tick), measures each from
the newest arrival of its related transactions, and calls it late only when
all of them were in silver for the previous pass."""

from __future__ import annotations

import sys
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")

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
    # ingest_ts in epoch seconds from 1_000_000, and the silver batch each
    # row came in. t6 landed early but reached silver in a later batch.
    rows = [
        ("t1", 100, 1),
        ("t2", 160, 2),
        ("t3", 50, 1),
        ("t4", 300, 3),
        ("t5", None, 3),
        ("t6", 90, 3),
    ]
    return spark.createDataFrame(
        [(u, _epoch(1_000_000 + s) if s is not None else None, "s", b) for u, s, b in rows],
        "uetr string, ingest_ts timestamp, _stream_id string, _batch_id long",
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
            # Its evidence landed early but reached silver after the
            # previous pass: not late.
            ("new-uuid-f", "W2_structuring", 6, ["t6"]),
        ],
        _ALERTS,
    )
    arrivals = {
        r["_key"]: r["arrival_ts"]
        for r in new_alert_arrivals(new_alert_txns(current, prior), _txns(spark)).collect()
    }
    assert len(arrivals) == 5
    # collect() returns naive local times; timestamp() reads them as local.
    got = sorted(v.timestamp() for v in arrivals.values() if v is not None)
    assert got == [1_000_090, 1_000_160, 1_000_300]
    assert sum(v is None for v in arrivals.values()) == 2

    # Broadcast and shuffled lookups agree. The previous pass read up to
    # batch 2: alert c's evidence (batches 1 and 2) was all in silver then.
    for small in (True, False):
        arrivals = new_alert_arrivals(
            new_alert_txns(current, prior), _txns(spark), small=small, seen={"s": 2}
        )
        stats = ttd_stats(arrivals, 1_000_425.0, bin_s=10)
        # ttd: c = 425 - 160 = 265 -> bin 26; b = 425 - 300 = 125 -> bin 12;
        # f = 425 - 90 = 335 -> bin 33.
        assert stats == {
            "alerts": 3,
            "late": 1,
            "unmatched": 2,
            "max_s": pytest.approx(335.0),
            "bin_s": 10,
            "bins": {12: 1, 26: 1, 33: 1},
        }


def test_no_prior_snapshot_means_every_alert_is_new(spark):
    from gold_refresh_financial import new_alert_arrivals, new_alert_txns, ttd_stats

    current = spark.createDataFrame([("u", "W2_structuring", 1, ["t4"])], _ALERTS)
    stats = ttd_stats(new_alert_arrivals(new_alert_txns(current, None), _txns(spark)), 1_000_290.0)
    # Bronze and gold driver clocks can disagree by a little: clamped at 0.
    assert stats["alerts"] == 1 and stats["max_s"] == 0.0 and stats["bins"] == {0: 1}


def test_each_alert_is_measured_at_its_own_rule_commit(spark):
    """A rule's alerts are visible when its INSERT commits, so a cheap rule
    run first is measured at its own commit, not at the end of the pass."""
    from gold_refresh_financial import new_alert_arrivals, new_alert_txns, ttd_stats

    current = spark.createDataFrame(
        [
            ("a", "W4_risk_propagation", 1, ["t4"]),
            ("b", "W3_round_tripping", 2, ["t2"]),
            ("c", "W2_structuring", 3, ["t1"]),
        ],
        _ALERTS,
    )
    arrivals = new_alert_arrivals(new_alert_txns(current, None), _txns(spark))
    stats = ttd_stats(
        arrivals,
        1_000_900.0,
        detected_by_rule={
            "W4_risk_propagation": 1_000_320.0,
            "W3_round_tripping": 1_000_800.0,
            # A rule that did not commit falls back to the pass end.
            "W2_structuring": None,
        },
        bin_s=10,
    )
    # W4: 320 - 300 = 20 -> bin 2; W3: 800 - 160 = 640 -> bin 64;
    # W2: 900 - 100 = 800 -> bin 80.
    assert stats["bins"] == {2: 1, 64: 1, 80: 1}
    assert stats["max_s"] == pytest.approx(800.0)
    # The pass-end histogram measures all three at 900: 600, 740, 800.
    assert stats["pass_end"]["bins"] == {60: 1, 74: 1, 80: 1}
    assert stats["by_rule"]["W4_risk_propagation"] == {
        "alerts": 1,
        "max_s": pytest.approx(20.0),
        "bins": {2: 1},
    }
    from gold_refresh_financial import ttd_detail_lines

    from lakebench.metrics.collector import _TTD_DETAIL_LINE

    # The collector's parser reads these lines: what it recovers is what the report shows.
    parsed = [m for m in map(_TTD_DETAIL_LINE.search, ttd_detail_lines(4, stats)) if m]
    by_rule = {m["rule"]: m for m in parsed}
    pass_end = by_rule[None]
    assert (pass_end["cycle"], pass_end["alerts"], float(pass_end["max"])) == ("4", "3", 800.0)
    assert pass_end["bins"] == "60:1,74:1,80:1"
    w4 = by_rule["W4_risk_propagation"]
    assert (w4["alerts"], float(w4["max"]), w4["bins"]) == ("1", 20.0, "2:1")
