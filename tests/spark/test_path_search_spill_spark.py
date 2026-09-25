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
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))

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
    first transfer; hubs (more than max_out_degree sends in a hop bucket) do
    not forward."""
    hop_us, total_us = hop_h * HOUR_US, total_d * 24 * HOUR_US
    epoch0 = int(T0.timestamp()) * 1_000_000
    edges = [(u, a, b, epoch0 + int(h * HOUR_US)) for u, a, b, h, _ in rows]
    per_bucket = {}
    for _, a, _, t in edges:
        per_bucket[(a, t // hop_us)] = per_bucket.get((a, t // hop_us), 0) + 1
    hubs = {a for (a, _), n in per_bucket.items() if n > max_out_degree}
    out_by = {}
    for e in edges:
        if e[1] not in hubs:
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


def test_join_partitions_bound_rows_on_hub_graph(spark, monkeypatch):
    """Busy pass-through accounts make the levels grow hop by hop. The join
    is sized from the edge count, so no level lands in the job's 4 shuffle
    partitions: each holds a bounded slice."""
    import detection_rules as dr
    from pyspark.sql.functions import count, lit, spark_partition_id
    from pyspark.sql.functions import max as max_

    monkeypatch.delenv("LB_GOLD_URI", raising=False)
    monkeypatch.setattr(dr, "PATH_SEARCH_ROWS_PER_PARTITION", 1000)
    rows = _rows(3, 6000, 300, hubs=10, hub_in=0.15, hub_out=0.1)
    df = _df(spark, rows).cache()
    n_edges = df.count()
    parts = dr.path_search_partitions(spark, n_edges)
    assert parts == -(-4 * n_edges // 1000) > 4

    seen = []
    admit = dr._PathBudget.admit

    def spy(self, frame, estimate, label):
        cut, n = admit(self, frame, estimate, label)
        by_part = cut.groupBy(spark_partition_id().alias("p")).agg(count(lit(1)).alias("c"))
        stats = by_part.agg(max_("c").alias("mx"), count(lit(1)).alias("np")).collect()[0]
        seen.append((label, n, stats["np"], stats["mx"] or 0))
        return cut, n

    monkeypatch.setattr(dr._PathBudget, "admit", spy)
    alerts = dr.w3_round_tripping(df, run_id="r").count()
    assert alerts == len(_w3_brute_force(rows))
    sizes = [n for _, n, _, _ in seen]
    assert sizes[1] > sizes[0]  # the frontier grows
    for label, n, np_, mx in seen:
        assert np_ <= parts, label
        # Hash placement is uneven, but no partition takes more than a small
        # multiple of its share (with one partition it would take all n).
        assert mx <= max(50, 3 * n / parts), (label, n, mx)


def test_budget_refuses_a_level_before_writing_it(spark, tmp_path, monkeypatch):
    """A budget that holds level 2 but not level 3 skips on the level-3
    estimate: level 3 is never written, and the files the search did write
    are removed with the skip."""
    import detection_rules as dr

    monkeypatch.setenv("LB_GOLD_URI", f"file://{tmp_path}/gold/")
    rows = _rows(4, 4000, 200, hubs=10, hub_in=0.15, hub_out=0.1)
    df = _df(spark, rows).cache()
    written = []
    cut = dr._PathBudget.cut

    def spy(self, frame, label, **kw):
        written.append(label)
        return cut(self, frame, label, **kw)

    monkeypatch.setattr(dr._PathBudget, "cut", spy)
    checks = {}
    check = dr._PathBudget.check

    def record(self, estimate, label):
        checks[label] = (self.live, estimate)
        return check(self, estimate, label)

    monkeypatch.setattr(dr._PathBudget, "check", record)
    dr.w3_round_tripping(df, max_hops=4, run_id="r").count()
    dr.cleanup_path_search_spill(spark)
    live2, est2 = checks["level 2"]
    live3, est3 = checks["level 3"]
    assert est3 > 0
    budget = int(live3 + est3) - 1  # level 3 alone does not fit
    assert live2 + est2 <= budget  # level 2 does
    written.clear()
    with pytest.raises(dr.RuleSkipped) as exc:
        dr.w3_round_tripping(df, max_hops=4, max_paths=budget, run_id="r")
    assert exc.value.reason == "path-cap"
    assert "estimates level 3" in exc.value.detail
    assert "level 3" not in written and "level 2" in written
    assert not any((tmp_path / "gold/_checkpoints/paths").glob("*/W3-*"))


def test_partition_count_scales_with_edges_within_bounds(spark):
    import detection_rules as dr

    assert dr.path_search_partitions(spark, 1_000) == 4  # the job's own count
    assert dr.path_search_partitions(spark, 266_700_000) == 534  # scale 10
    assert dr.path_search_partitions(spark, 2_667_000_000) == dr.PATH_SEARCH_MAX_PARTITIONS


def test_last_hop_joins_the_step_frame_in_place(spark, monkeypatch):
    """Review: on the last hop the closing test ``e_dst == start`` became a
    third join key, the step frame's (e_src, _b) partitioning no longer
    counted, and Spark reshuffled the whole step frame at the job's shuffle
    partitions. Under the search's setting it is joined in place."""
    import re

    import detection_rules as dr
    from pyspark.sql.functions import array, col

    monkeypatch.delenv("LB_GOLD_URI", raising=False)
    monkeypatch.setattr(dr, "PATH_SEARCH_ROWS_PER_PARTITION", 100)
    old = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        df = _df(spark, _rows(5, 500, 50))
        hop_us = 168 * HOUR_US
        _, edges, n_edges, step, _, parts = dr._search_frames(
            df, "W3", hop_us, 200, 10**9, 10**9, with_amount=False
        )
        paths = edges.select(
            col("src").alias("start"),
            col("dst").alias("end"),
            col("t").alias("t_last"),
            array(col("uetr")).alias("uetrs"),
        )

        def last_hop_plan():
            closing = dr._extend_paths(paths, step, hop_us, parts).filter(
                col("e_dst") == col("start")
            )
            return closing._jdf.queryExecution().executedPlan().toString()

        assert "hashpartitioning(e_dst" in last_hop_plan()  # the defect
        assert parts > 4
        with dr._copartition_on_key_subset(spark):
            plan = last_hop_plan()
        assert "hashpartitioning(e_dst" not in plan
        assert re.search(rf"hashpartitioning\(end#\d+L, _pb#\d+L, {parts}\)", plan)
        assert spark.conf.get("spark.sql.requireAllClusterKeysForCoPartition") == "true"
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", old)
        spark.catalog.clearCache()


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


def test_cut_touches_the_liveness_file(spark, tmp_path, monkeypatch):
    import detection_rules as dr

    monkeypatch.setenv("LB_GOLD_URI", f"file://{tmp_path}/gold/")
    dr.w3_round_tripping(_df(spark, _rows(6, 300, 30)), run_id="r").count()
    root = Path(dr._path_spill_root(spark).removeprefix("file://"))
    assert (root / "_alive").exists()
    dr.cleanup_path_search_spill(spark)
