"""Continuous c360 gold-refresh recomputes only the dates silver changed on
(common.refresh_daily_kpis). Gold after each refresh must equal a full
aggregation of silver, including when a later commit adds rows to a date
older than the newest one already seen: continuous datagen pods drift
apart, so rows land out of date order."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")

import gold_repeat_scenarios as sc  # noqa: E402

pytestmark = pytest.mark.usefixtures("load_script")

COMMITS = 3


def test_incremental_refresh_equals_a_full_aggregation(spark_session):
    from common import get_daily_kpi_aggregations, refresh_daily_kpis
    from pyspark.sql.functions import abs as abs_
    from pyspark.sql.functions import col, hash, lit

    spark = spark_session
    t0 = datetime(2026, 1, 1)
    # Every commit has rows on every date, so commits 2 and 3 land on dates
    # older than the newest one commit 1 brought.
    base = sc.silver_df(spark).withColumn(
        "_commit", abs_(hash(col("customer_id"), col("session_id"))) % COMMITS
    )
    silver = None
    gold = None
    since = None
    for k in range(COMMITS):
        part = base.filter(col("_commit") == k).withColumn(
            "silver_processing_timestamp", lit(t0 + timedelta(seconds=k))
        )
        silver = part if silver is None else silver.unionByName(part)
        out, since, dates = refresh_daily_kpis(silver.drop("_commit"), gold, since)
        assert out is not None
        if k > 0:
            assert dates and len(dates) > sc.DAYS // 2  # late rows on old dates
        # Materialise this refresh's gold, as the job's table write does.
        gold = spark.createDataFrame(out.collect(), out.schema).cache()
        want = {
            r["interaction_date"]: r.asDict()
            for r in silver.drop("_commit")
            .groupBy("interaction_date")
            .agg(*get_daily_kpi_aggregations())
            .collect()
        }
        got = {r["interaction_date"]: r.asDict() for r in gold.collect()}
        assert got.keys() == want.keys()
        bad = [d for d in want if not sc._same(got[d], want[d])]
        assert not bad, f"commit {k}: {len(bad)} date(s) differ from a full aggregation"
    # No row newer than the last refresh: nothing to recompute.
    out, again, dates = refresh_daily_kpis(silver.drop("_commit"), gold, since)
    assert out is None and again == since and dates == []
