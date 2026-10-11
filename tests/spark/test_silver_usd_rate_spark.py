"""Executed: silver's USD conversion uses the settlement currency."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")


def test_usd_rate_by_currency(spark_session):
    from pyspark.sql.functions import col
    from silver_build_financial import _FX_TO_USD, _usd_rate

    amount = 1000.0
    df = spark_session.createDataFrame(
        [("USD", amount), ("JPY", amount), ("XXX", amount)], "c string, a double"
    )
    got = {
        r["c"]: r["u"]
        for r in df.select("c", (col("a") * _usd_rate(col("c"))).alias("u")).collect()
    }
    assert got["USD"] == pytest.approx(amount * _FX_TO_USD["USD"])
    assert got["JPY"] == pytest.approx(amount * _FX_TO_USD["JPY"])
    assert got["JPY"] != pytest.approx(amount)
    assert "XXX" not in _FX_TO_USD
    assert got["XXX"] == pytest.approx(amount)
