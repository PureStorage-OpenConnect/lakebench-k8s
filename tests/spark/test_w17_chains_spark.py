"""Executed: W17 finds an open layering chain once, requires the amount to carry
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
    from detection_rules import w17_layering_chain

    out = w17_layering_chain(_df(spark, rows), run_id="r", **kw).collect()
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
    # Attributed to the first intermediary, the first account that received
    # and passed the funds on; the first sender is kept as a related entity.
    assert out[0]["entity_id"] == 2
    assert out[0]["rule_id"] == "W17_layering_chain"
    assert list(out[0]["related_entity_ids"]) == [1, 2, 3, 4, 5]


STACK = [
    ("s1", 1, 2, 10, 1000),
    ("s2", 2, 3, 34, 950),
    ("s3", 3, 4, 58, 900),
    ("s4", 4, 5, 82, 860),
]
STACK_IDS = {"s1", "s2", "s3", "s4"}


def test_revisiting_predecessor_does_not_hide_chain(spark):
    """Review a1: t0 (3 -> 1) carries into the chain's first sender, but its
    own chain dies when s2 returns to 3. The planted chain must survive."""
    _, found = _found(spark, [("t0", 3, 1, 0, 1100)] + STACK)
    assert ("s1", "s2", "s3", "s4") in found


def test_long_coincidental_run_keeps_the_stack(spark):
    """Review b: six carrying transfers into account 1 before the stack push
    the whole run past max_hops. Some alert must still carry every stack
    transfer."""
    back = [(f"b{i}", 100 + i, 100 + i + 1, i, 1100 * (1 / 0.95) ** (6 - i)) for i in range(6)]
    back[-1] = ("b5", 105, 1, 5, 1100)
    _, found = _found(spark, back + STACK)
    assert any(STACK_IDS <= set(f) for f in found)


def test_feeding_credits_merge_into_one_alert(spark):
    """Review c: two unrelated credits into account 1 both carry the amount.
    One alert, on account 1 (the first intermediary), not one per payer."""
    out, _ = _found(spark, [("p1", 70, 1, 0, 1100), ("p2", 71, 1, 1, 1050)] + STACK)
    assert len(out) == 1
    a = out[0]
    assert a["entity_id"] == 1
    assert set(a["related_txn_ids"]) == {"p1", "p2"} | STACK_IDS
    assert a["evidence"]["feeders"] == "2"


def test_hub_feeder_is_not_the_alerted_entity(spark):
    hub = [(f"h{i}", 999, 500 + i, 0.01 * i, 10) for i in range(250)]
    out, _ = _found(spark, hub + [("hp", 999, 1, 2, 1100)] + STACK)
    assert len(out) == 1
    assert out[0]["entity_id"] == 1
    assert STACK_IDS <= set(out[0]["related_txn_ids"])


def test_dense_neighbours(spark):
    """Review dense-corpus cases: an unrelated carrying credit into the first
    sender, and an unrelated carrying onward transfer from the last account.
    The stack is reported in one alert either way."""
    for extra in ([("in", 80, 1, 0, 1080)], [("out", 5, 81, 90, 830)]):
        out, _ = _found(spark, extra + STACK)
        assert len(out) == 1
        assert STACK_IDS <= set(out[0]["related_txn_ids"])


def test_hop_bounds_do_not_crash(spark):
    out, _ = _found(spark, STACK, min_hops=5, max_hops=4)
    assert out == []
    out, _ = _found(spark, STACK, max_hops=1)
    assert out == []


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


def test_cycle_start_not_reported_open_rotation_is(spark):
    """The chain from a cycle's first transfer returns to its start, so it is
    W3's and is not reported. Its rotation from the second transfer is an
    open chain (51 -> 52 -> 53 -> 50) and is reported, so a cycle longer than
    W3's max_hops still reaches an alert."""
    rows = [
        ("c1", 50, 51, 0, 1000),
        ("c2", 51, 52, 24, 950),
        ("c3", 52, 53, 48, 900),
        ("c4", 53, 50, 72, 860),
    ]
    _, found = _found(spark, rows)
    assert found == [("c2", "c3", "c4")]


def test_six_transfer_cycle_reaches_an_alert(spark):
    amt, rows = 1000.0, []
    for i in range(6):
        rows.append((f"k{i}", 200 + i, 200 + (i + 1) % 6, 24 * i, amt))
        amt *= 0.95
    _, found = _found(spark, rows)
    assert found and all(len(f) >= 3 for f in found)


def test_feeder_whose_chain_returns_does_not_hide_stack(spark):
    """Independent review C: 9 -> 1 feeds the stack and the stack's end pays
    5 -> 9, so the chain from 9 returns to its start and is dropped. The
    planted transfers must still be reported."""
    rows = [("p", 9, 1, 0, 1050)] + STACK + [("r", 5, 9, 100, 820)]
    _, found = _found(spark, rows)
    assert any(STACK_IDS <= set(f) for f in found)


def test_ntz_input_ignores_session_time_zone(spark):
    """TIMESTAMP_NTZ input is read as UTC wall-clock. Under America/New_York,
    2024-03-10 02:30 does not exist; a cast would move that hop and break
    the order."""
    from datetime import datetime

    from detection_rules import w17_layering_chain

    rows = [
        ("n1", 1, 2, datetime(2024, 3, 10, 2, 10), 1000.0),
        ("n2", 2, 3, datetime(2024, 3, 10, 2, 50), 950.0),
        ("n3", 3, 4, datetime(2024, 3, 10, 3, 5), 900.0),
    ]
    df = spark.createDataFrame(
        rows,
        "uetr string, originator_id long, beneficiary_id long, "
        "txn_timestamp timestamp_ntz, txn_amount_usd double",
    )
    old = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "America/New_York")
    try:
        out = w17_layering_chain(df, run_id="r").collect()
    finally:
        spark.conf.set("spark.sql.session.timeZone", old)
    assert [tuple(a["related_txn_ids"]) for a in out] == [("n1", "n2", "n3")]


def test_path_cap_skips_honestly(spark):
    from detection_rules import RuleSkipped, w17_layering_chain

    rows = [("s1", 1, 2, 0, 1000), ("s2", 2, 3, 24, 950), ("s3", 3, 4, 48, 900)]
    with pytest.raises(RuleSkipped) as exc:
        w17_layering_chain(_df(spark, rows), max_paths=0, run_id="r")
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


def test_budget_counts_edges_and_step_and_estimates_first(spark):
    """Edges (3) and step (6) are held rows; a budget of 9 admits them, and
    the level-2 estimate trips the skip before the level is built."""
    from detection_rules import RuleSkipped, w17_layering_chain

    with pytest.raises(RuleSkipped) as exc:
        w17_layering_chain(_df(spark, STACK[:3]), max_paths=9, run_id="r")
    assert exc.value.reason == "path-cap"
    assert "estimates level 2" in exc.value.detail


def test_budget_from_env_and_job_scratch(spark, monkeypatch):
    from detection_rules import _parse_size_bytes, path_search_budget_rows

    assert _parse_size_bytes("100Gi") == 100 * 2**30
    assert _parse_size_bytes("500G") == 500 * 10**9
    assert _parse_size_bytes("bogus") is None
    monkeypatch.setenv("LB_PATH_SEARCH_MAX_ROWS", "12345")
    assert path_search_budget_rows(spark) == 12345
    monkeypatch.setenv("LB_PATH_SEARCH_MAX_ROWS", "not-a-number")
    assert path_search_budget_rows(spark) > 0


def test_alert_ts_is_when_the_chain_first_qualifies(spark):
    """Review: two carrying onward hops about six days apart pushed alert_ts
    a week past the planted window, which scores as a miss. alert_ts is the
    time of the chain's third transfer."""
    from datetime import datetime, timedelta, timezone

    rows = STACK + [("x1", 5, 81, 82 + 140, 820), ("x2", 81, 82, 82 + 280, 790)]
    out, _ = _found(spark, rows)
    assert len(out) == 1
    assert STACK_IDS <= set(out[0]["related_txn_ids"])
    t0 = datetime(2024, 3, 1, tzinfo=timezone.utc)
    # collect() returns naive local time; astimezone reads it as local.
    assert out[0]["alert_ts"].astimezone(timezone.utc) == t0 + timedelta(hours=58)
