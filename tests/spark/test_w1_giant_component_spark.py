"""Executed: W1 does not emit components above max_cluster_size and caps
related_txn_ids (D8: the giant baseline component was one alert naming
every transaction, a row past Spark's 2 GB limit at scale 10)."""

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
    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def _graph(spark, n_rings, chain_len):
    t0 = datetime(2024, 1, 1)
    rows = []
    for r in range(n_rings):
        a, b, c = 3 * r + 1, 3 * r + 2, 3 * r + 3
        for i, (x, y) in enumerate([(a, b), (b, c), (c, a), (a, c), (b, a)]):
            rows.append((x, y, f"s{r}-{i}", t0 + timedelta(hours=i)))
    for i in range(1000, 1000 + chain_len - 1):
        rows.append((i, i + 1, f"g{i}", t0 + timedelta(hours=i)))
    return spark.createDataFrame(
        rows, "originator_id long, beneficiary_id long, uetr string, txn_timestamp timestamp"
    )


def test_minor_giant_component_not_emitted_and_txns_capped(spark, capsys):
    from detection_rules import w1_connected_components

    # 12 rings of 3 (36 vertices) + a 30-vertex chain: the giant is 45%.
    out = w1_connected_components(
        _graph(spark, 12, 30), min_cluster_size=3, max_cluster_size=10, max_txns_per_alert=2
    ).collect()
    assert len(out) == 12
    a = min(out, key=lambda r: r["entity_id"])
    assert sorted(a["related_entity_ids"]) == [1, 2, 3]
    assert a["related_txn_ids"] == ["s0-0", "s0-1"]  # earliest first, capped
    assert a["evidence"]["txn_total"] == "5"
    assert a["evidence"]["txns_truncated"] == "true"
    assert "above max_cluster_size=10" in capsys.readouterr().out


def test_dominant_giant_component_is_a_skip_not_zero_recall(spark):
    from detection_rules import RuleSkipped, w1_connected_components

    # 1 ring (3 vertices) + a 30-vertex chain: the giant is 91% of vertices.
    with pytest.raises(RuleSkipped) as e:
        w1_connected_components(
            _graph(spark, 1, 30), min_cluster_size=3, max_cluster_size=10
        ).collect()
    assert e.value.reason == "giant-component"
