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


def test_giant_component_skipped_and_txns_capped(spark, capsys):
    from detection_rules import w1_connected_components

    t0 = datetime(2024, 1, 1)
    rows = []
    # Small ring: 1-2-3 with 5 transactions.
    for i, (a, b) in enumerate([(1, 2), (2, 3), (3, 1), (1, 3), (2, 1)]):
        rows.append((a, b, f"s{i}", t0 + timedelta(hours=i)))
    # Giant chain: 100..129 (30 entities).
    for i in range(100, 129):
        rows.append((i, i + 1, f"g{i}", t0 + timedelta(hours=i)))
    df = spark.createDataFrame(
        rows, "originator_id long, beneficiary_id long, uetr string, txn_timestamp timestamp"
    )
    out = w1_connected_components(
        df, min_cluster_size=3, max_cluster_size=10, max_txns_per_alert=2, run_id="r"
    ).collect()
    assert len(out) == 1
    a = out[0]
    assert sorted(a["related_entity_ids"]) == [1, 2, 3]
    assert a["related_txn_ids"] == ["s0", "s1"]  # earliest first, capped
    assert a["evidence"]["txn_total"] == "5"
    assert a["evidence"]["txns_truncated"] == "true"
    assert "above max_cluster_size=10" in capsys.readouterr().out
