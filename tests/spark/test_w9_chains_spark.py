"""Executed: W9 finds an open layering chain once, requires the amount to carry
through each hop and each hop to follow within the window, excludes hubs as
intermediaries, leaves cycles to W3, and skips honestly at its path cap (D2:
no rule could detect `stack`)."""

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
    t0 = datetime(2024, 3, 1, tzinfo=timezone.utc)
    return spark.createDataFrame(
        [(u, a, b, t0 + timedelta(hours=h), float(amt)) for u, a, b, h, amt in rows],
        "uetr string, originator_id long, beneficiary_id long, "
        "txn_timestamp timestamp, txn_amount_usd double",
    )


def _found(spark, rows, **kw):
    from detection_rules import w9_layering_chain

    out = w9_layering_chain(_df(spark, rows), run_id="r", **kw).collect()
    return out, [tuple(a["related_txn_ids"]) for a in out]


def test_planted_chain_found_once(spark):
    rows = [
        # Unrelated earlier credit into the chain's start: 1000 is 20% of
        # it, so it cannot be the chain's previous hop.
        ("pre", 7, 1, 0, 5000),
        # 4-hop stack 1->2->3->4->5, one day per hop, 1-9% skim per hop.
        ("s1", 1, 2, 10, 1000),
        ("s2", 2, 3, 34, 950),
        ("s3", 3, 4, 58, 900),
        ("s4", 4, 5, 82, 860),
    ]
    out, found = _found(spark, rows)
    assert found == [("s1", "s2", "s3", "s4")]
    assert out[0]["entity_id"] == 1
    assert out[0]["rule_id"] == "W9_layering_chain"
    assert list(out[0]["related_entity_ids"]) == [1, 2, 3, 4, 5]


def test_amount_must_carry_through(spark):
    rows = [
        # Second hop forwards only half: no chain of 3 carrying hops.
        ("a1", 10, 11, 0, 1000),
        ("a2", 11, 12, 24, 500),
        ("a3", 12, 13, 48, 480),
        # Forwarding more than was received is not pass-through either.
        ("b1", 20, 21, 0, 1000),
        ("b2", 21, 22, 24, 1200),
        ("b3", 22, 23, 48, 1100),
    ]
    _, found = _found(spark, rows)
    assert found == []


def test_slow_hop_breaks_chain(spark):
    rows = [
        ("t1", 30, 31, 0, 1000),
        ("t2", 31, 32, 24, 950),
        # Eight days after the previous hop; the window is seven.
        ("t3", 32, 33, 24 + 8 * 24, 900),
    ]
    _, found = _found(spark, rows)
    assert found == []


def test_hub_is_not_an_intermediary(spark):
    rows = [
        ("h1", 40, 99, 0, 1000),
        ("h2", 99, 41, 1, 950),
        ("h3", 41, 42, 2, 900),
    ]
    # Account 99 sends 300 payments in the same week: a hub.
    rows += [(f"x{i}", 99, 1000 + i, 3, 50) for i in range(300)]
    _, found = _found(spark, rows, max_out_degree=200)
    assert not any("h2" in f for f in found)


def test_cycle_is_left_to_w3(spark):
    rows = [
        ("c1", 50, 51, 0, 1000),
        ("c2", 51, 52, 24, 950),
        ("c3", 52, 53, 48, 900),
        ("c4", 53, 50, 72, 860),
    ]
    _, found = _found(spark, rows)
    assert found == []


def test_path_cap_skips_honestly(spark):
    from detection_rules import RuleSkipped, w9_layering_chain

    rows = [("s1", 1, 2, 0, 1000), ("s2", 2, 3, 24, 950), ("s3", 3, 4, 48, 900)]
    with pytest.raises(RuleSkipped) as exc:
        w9_layering_chain(_df(spark, rows), max_paths=0, run_id="r")
    assert exc.value.reason == "path-cap"


def test_hops_across_a_bucket_boundary_are_joined(spark):
    """The extension join is keyed on 7-day buckets aligned to the epoch
    (Thursdays 00:00 UTC). 2024-03-07 is such a boundary, 144 h after t0; a
    hop just after it must still extend a path ending just before it."""
    from detection_rules import w3_round_tripping

    chain = [
        ("k1", 60, 61, 100, 1000),
        ("k2", 61, 62, 143.5, 950),
        ("k3", 62, 63, 144.5, 900),
    ]
    _, found = _found(spark, chain)
    assert found == [("k1", "k2", "k3")]

    cycle = [("q1", 70, 71, 143, 1), ("q2", 71, 72, 145, 1), ("q3", 72, 70, 150, 1)]
    out = w3_round_tripping(_df(spark, cycle), run_id="r").collect()
    assert [tuple(a["related_txn_ids"]) for a in out] == [("q1", "q2", "q3")]


def test_w3_path_cap_skips_honestly(spark):
    from detection_rules import RuleSkipped, w3_round_tripping

    rows = [("r1", 1, 2, 0, 1), ("r2", 2, 1, 5, 1)]
    with pytest.raises(RuleSkipped) as exc:
        w3_round_tripping(_df(spark, rows), max_paths=0, run_id="r")
    assert exc.value.reason == "path-cap"
