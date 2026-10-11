"""Executed: the W3/W17 path search at scale (live scale-10 failure: gold
executors OOM-killed during W3, then CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND on a
local checkpoint).

Covers the three changes: levels written under LB_GOLD_URI instead of local
checkpoints (same results, files removed), the extension join sized from the
edge count rather than the job's shuffle partitions (bounded rows per
partition on a hub-heavy graph), and the budget refusing a level before it is
built. W3 is also checked against a brute-force cycle enumeration.
"""

from __future__ import annotations

import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")

T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
HOUR_US = 3_600_000_000


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def _rows(seed, n, entities, hubs=0, hub_in=0.0, hub_out=0.0, weeks=8):
    """Random transfers: (uetr, originator, beneficiary, hours after T0, usd).
    Accounts below ``hubs`` receive ``hub_in`` and send ``hub_out`` of all
    transfers (busy pass-through accounts, the case that multiplies paths)."""
    rng = random.Random(seed)
    out = []
    for i in range(n):
        a = rng.randrange(hubs) if hubs and rng.random() < hub_out else rng.randrange(entities)
        b = rng.randrange(hubs) if hubs and rng.random() < hub_in else rng.randrange(entities)
        if a == b:
            continue
        amt = float(rng.choice([1000, 950, 900, 850, rng.randrange(100, 5000)]))
        out.append((f"u{i:06d}", a, b, rng.randrange(24 * 7 * weeks), amt))
    return out


def _df(spark, rows):
    return spark.createDataFrame(
        [(u, a, b, T0 + timedelta(hours=h), amt) for u, a, b, h, amt in rows],
        "uetr string, originator_id long, beneficiary_id long, "
        "txn_timestamp timestamp, txn_amount_usd double",
    )


def _sig(df):
    cols = [
        "rule_id",
        "entity_id",
        "related_txn_ids",
        "related_entity_ids",
        "alert_ts",
        "alert_score",
        "priority",
        "narrative",
        "evidence",
    ]
    return sorted(repr(tuple(r)) for r in df.select(*cols).collect())


def _w3_brute_force(rows, max_hops=5, hop_h=168, total_d=30, max_out_degree=200):
    """Every simple temporal cycle of 2..max_hops transfers, found from its
    first transfer; an account does not forward in a hop bucket where it
    sends more than max_out_degree transfers (a hub that week)."""
    hop_us, total_us = hop_h * HOUR_US, total_d * 24 * HOUR_US
    epoch0 = int(T0.timestamp()) * 1_000_000
    edges = [(u, a, b, epoch0 + int(h * HOUR_US)) for u, a, b, h, _ in rows]
    per_bucket = {}
    for _, a, _, t in edges:
        per_bucket[(a, t // hop_us)] = per_bucket.get((a, t // hop_us), 0) + 1
    hub_weeks = {k for k, n in per_bucket.items() if n > max_out_degree}
    out_by = {}
    for e in edges:
        if (e[1], e[3] // hop_us) not in hub_weeks:
            out_by.setdefault(e[1], []).append(e)
    found = set()

    def walk(start, t_first, t_last, nodes, uetrs):
        for u, _, b, t in out_by.get(nodes[-1], ()):
            if not (t_last < t <= t_last + hop_us and t <= t_first + total_us):
                continue
            if b == start:
                found.add(tuple(uetrs + [u]))
            elif b not in nodes and len(uetrs) + 1 < max_hops:
                walk(start, t_first, t, nodes + [b], uetrs + [u])

    for u, a, b, t in edges:
        walk(a, t, t, [a, b], [u])
    return found


def test_w3_matches_brute_force_in_both_modes(spark, tmp_path, monkeypatch):
    from detection_rules import cleanup_path_search_spill, w3_round_tripping

    rows = _rows(1, 3000, 250, hubs=3, hub_in=0.05, hub_out=0.1)
    expected = _w3_brute_force(rows, max_out_degree=12)
    assert len(expected) > 20  # the graph has cycles of several lengths
    df = _df(spark, rows).cache()

    monkeypatch.delenv("LB_GOLD_URI", raising=False)
    local = w3_round_tripping(df, max_out_degree=12, run_id="r")
    assert {tuple(a["related_txn_ids"]) for a in local.collect()} == expected
    assert local.count() == len(expected)

    monkeypatch.setenv("LB_GOLD_URI", f"file://{tmp_path}/gold/")
    spilled = w3_round_tripping(df, max_out_degree=12, run_id="r")
    assert _sig(spilled) == _sig(local)
    cleanup_path_search_spill(spark)


def test_w17_spill_mode_matches_local_checkpoint(spark, tmp_path, monkeypatch):
    """Same alerts whether levels are local checkpoints or written files; the
    level files go as the search advances, the result files at cleanup."""
    from detection_rules import cleanup_path_search_spill, w17_layering_chain

    df = _df(spark, _rows(2, 4000, 300, hubs=3, hub_in=0.05, hub_out=0.03)).cache()
    monkeypatch.delenv("LB_GOLD_URI", raising=False)
    local = _sig(w17_layering_chain(df, max_out_degree=12, run_id="r"))
    assert len(local) > 20

    monkeypatch.setenv("LB_GOLD_URI", f"file://{tmp_path}/gold/")
    spilled = w17_layering_chain(df, max_out_degree=12, run_id="r")
    assert _sig(spilled) == local
    root = tmp_path / "gold/_checkpoints/paths"
    written = {p.name.split("-")[0] for p in root.glob("*/*/*") if p.is_dir()}
    assert written == {"complete"}  # every level-N directory is gone already
    cleanup_path_search_spill(spark)
    assert not any(root.glob("*/*"))


def test_budget_refuses_a_level_before_writing_it(spark, tmp_path, monkeypatch, capsys):
    """A budget that holds level 2 but not level 3 skips on the level-3
    estimate: level 3 is never built, and the files the search did write
    are removed with the skip."""
    import re

    import detection_rules as dr

    monkeypatch.setenv("LB_GOLD_URI", f"file://{tmp_path}/gold/")
    rows = _rows(4, 4000, 200, hubs=10, hub_in=0.15, hub_out=0.1)
    df = _df(spark, rows).cache()
    dr.w3_round_tripping(df, max_hops=4, run_id="r").count()
    dr.cleanup_path_search_spill(spark)
    lines = re.findall(r"\[W3\] (.+?) rows=\d+ estimate=(\S+) held=(\d+)", capsys.readouterr().out)
    at = [label for label, _, _ in lines].index("level 3")
    est3 = int(lines[at][1])
    assert est3 > 0
    before3 = max(int(held) for _, _, held in lines[:at])
    # Everything before level 3 fits; level 3 on top of the level 2 rows it
    # extends (at least that frame's own count) does not.
    budget = before3 + est3 // 2

    capsys.readouterr()
    with pytest.raises(dr.RuleSkipped) as exc:
        dr.w3_round_tripping(df, max_hops=4, max_paths=budget, run_id="r")
    assert exc.value.reason == "path-cap"
    built = capsys.readouterr().out
    assert "level 2 rows=" in built and "level 3 rows=" not in built
    assert not any((tmp_path / "gold/_checkpoints/paths").glob("*/W3-*"))


def test_sweep_removes_only_stale_foreign_spill(spark, tmp_path, monkeypatch):
    """A driver killed mid-search leaves its levels; the next run removes
    directories whose newest file is old, and nothing recent or its own."""
    import time

    import detection_rules as dr

    monkeypatch.setenv("LB_GOLD_URI", f"file://{tmp_path}/gold/")
    own = dr._path_spill_root(spark).removeprefix("file://")
    base = Path(own).parent
    old_file = base / "spark-dead" / "W3-x" / "level-3" / "part-0.parquet"
    new_file = base / "spark-live" / "W17-y" / "level-2" / "part-0.parquet"
    own_file = Path(own) / "W3-z" / "cycles-2" / "part-0.parquet"
    for f in (old_file, new_file, own_file):
        f.parent.mkdir(parents=True)
        f.write_bytes(b"x")
    stale = time.time() - 48 * 3600
    os.utime(old_file, (stale, stale))
    os.utime(own_file, (stale, stale))
    # A write just starting: its part files are not visible yet.
    (base / "spark-starting" / "W3-w" / "level-2" / "_temporary").mkdir(parents=True)
    # A long-running driver whose level is old but whose next write began.
    alive = base / "spark-busy" / "_alive"
    old_level = base / "spark-busy" / "W3-v" / "level-2" / "part-0.parquet"
    old_level.parent.mkdir(parents=True)
    old_level.write_bytes(b"x")
    os.utime(old_level, (stale, stale))
    alive.write_bytes(b"")
    assert dr.sweep_stale_path_spill(spark) == 1
    assert not (base / "spark-dead").exists()
    assert new_file.exists() and own_file.exists() and old_level.exists()
    assert (base / "spark-starting").exists()


def test_sweep_spares_a_searching_drivers_spill_until_its_liveness_file_goes_stale(
    spark, tmp_path, monkeypatch
):
    """The liveness file a real search writes is what keeps another driver's
    sweep from deleting that search's old level files."""
    import time

    import detection_rules as dr

    monkeypatch.setenv("LB_GOLD_URI", f"file://{tmp_path}/gold/")
    dr.w3_round_tripping(_df(spark, _rows(6, 300, 30)), run_id="r").count()
    root = Path(dr._path_spill_root(spark).removeprefix("file://"))
    alive = root / "_alive"
    levels = [f for f in root.rglob("*") if f.is_file() and f != alive]
    assert alive.exists() and levels
    stale = time.time() - 48 * 3600
    for f in levels:
        os.utime(f, (stale, stale))

    # Another driver of the same deployment runs the sweep.
    monkeypatch.setattr(dr, "_path_spill_root", lambda spark: f"{root.parent}/other-driver")
    assert dr.sweep_stale_path_spill(spark) == 0
    assert all(f.exists() for f in levels)

    os.utime(alive, (stale, stale))
    assert dr.sweep_stale_path_spill(spark) == 1
    assert not root.exists()
