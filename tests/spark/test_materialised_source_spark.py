"""common.materialised_source: a temp view over a local checkpoint of the
frame, with its lineage cut, dropped and freed on exit, also when the body
raises. Iceberg is not needed: the plan shape and the blocks are what is
checked here; the Spark 4.1 MERGE it exists for is covered by the parity
guards."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.usefixtures("load_script")


def _persistent(spark):
    return set(spark.sparkContext._jsc.getPersistentRDDs().keySet())


def test_view_reads_a_checkpoint_and_is_freed(spark_session):
    from common import materialised_source

    spark = spark_session
    base = _persistent(spark)
    df = spark.range(100).selectExpr("id", "id * 2 AS x")
    with materialised_source(spark, df, "_lb_ms_ok") as view:
        assert view == "_lb_ms_ok"
        plan = spark.table(view)._jdf.queryExecution().analyzed().toString()
        assert "LogicalRDD" in plan and "Range" not in plan, plan
        assert spark.sql(f"SELECT sum(x) FROM {view}").first()[0] == 9900
        assert len(_persistent(spark) - base) == 1
    assert not spark.catalog.tableExists("_lb_ms_ok")
    assert _persistent(spark) - base == set()


def test_content_fixed_at_entry(spark_session):
    """Rows are computed once, at entry: a later change to the source does
    not reach the view."""
    from common import materialised_source

    spark = spark_session
    spark.createDataFrame([(1,)], "n int").createOrReplaceTempView("_lb_ms_src")
    try:
        with materialised_source(spark, spark.table("_lb_ms_src"), "_lb_ms_fixed") as view:
            spark.createDataFrame([(1,), (2,)], "n int").createOrReplaceTempView("_lb_ms_src")
            assert spark.table(view).count() == 1
    finally:
        spark.catalog.dropTempView("_lb_ms_src")


def test_dropped_and_freed_when_the_body_raises(spark_session):
    from common import materialised_source

    spark = spark_session
    base = _persistent(spark)
    with pytest.raises(ValueError, match="merge failed"):
        with materialised_source(spark, spark.range(10), "_lb_ms_raise"):
            raise ValueError("merge failed")
    assert not spark.catalog.tableExists("_lb_ms_raise")
    assert _persistent(spark) - base == set()
