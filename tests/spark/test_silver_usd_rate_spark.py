"""Executed: silver's USD conversion uses the settlement currency (LB-137)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


def test_usd_rate_by_currency():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql.functions import col

    spark = (
        SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    )
    from silver_build_financial import _usd_rate

    df = spark.createDataFrame(
        [("USD", 5000.0), ("JPY", 735_000.0), ("KRW", 6_666_667.0), ("XXX", 10.0)],
        "c string, a double",
    )
    got = {
        r["c"]: r["u"]
        for r in df.select("c", (col("a") * _usd_rate(col("c"))).alias("u")).collect()
    }
    assert got["USD"] == pytest.approx(5000.0)
    assert got["JPY"] == pytest.approx(4998.0)
    assert got["KRW"] == pytest.approx(5000.0)
    assert got["XXX"] == pytest.approx(10.0)
