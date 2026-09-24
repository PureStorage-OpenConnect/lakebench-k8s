"""Executed: the reference feature frame groups by entity-day only, and the
day is positive when any of its transactions is planted (D7 self-scoring)."""

from __future__ import annotations

import sys
from datetime import datetime
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


def test_mixed_entity_day_is_one_positive_row(spark, monkeypatch, tmp_path):
    import score_financial_reference as ref

    t = datetime(2024, 3, 5, 10)
    spark.createDataFrame(
        [("u1", 7, 100.0, t), ("u2", 7, 9900.0, t), ("u3", 8, 50.0, t)],
        "uetr string, originator_id long, txn_amount double, txn_timestamp timestamp",
    ).createOrReplaceTempView("ref_txns")
    manifest = spark.createDataFrame(
        [("fan_in", ["u2"])], "typology_type string, participant_uetrs array<string>"
    )
    monkeypatch.setattr(ref, "CATALOG", "spark_catalog")
    spark.sql("CREATE DATABASE IF NOT EXISTS default")
    spark.sql("DROP TABLE IF EXISTS default.ref_txns_t")
    spark.table("ref_txns").write.option("path", str(tmp_path / "t")).saveAsTable(
        "default.ref_txns_t"
    )
    rows = {
        r["originator_id"]: r
        for r in ref._build_reference_feature_frame(spark, "default.ref_txns_t", manifest).collect()
    }
    assert set(rows) == {7, 8}  # one row per entity-day, not per (entity, day, label)
    assert rows[7]["label"] == "fan_in"
    assert rows[8]["label"] == "baseline"
    assert "amount_pct_of_ceiling" not in rows[7].asDict()
