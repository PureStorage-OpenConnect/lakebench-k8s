"""B1 (Iceberg): repeat cycle 1 produces one copy; cycle 2 grows once.

Runs the c360 silver-build Iceberg SIMPLE strategy against a local Hadoop
Iceberg warehouse. Repeats cycle 1 (DELETE-then-APPEND on _batch_id) and
asserts the final row count equals a single cycle-1 run's count. Then
runs cycle 2 twice and asserts only one copy lands.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg")


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, "ice", tmp_path_factory.mktemp("ice-wh"))
    return spark_session


def _rows(spark, n, start=0):
    from datetime import datetime, timedelta

    return spark.createDataFrame(
        [
            (
                i,
                1 + i % 5,
                datetime(2024, 6, 1) + timedelta(hours=i),
                "purchase",
                100.0 + i,
            )
            for i in range(start, start + n)
        ],
        "id bigint, customer_id bigint, event_timestamp timestamp, "
        "interaction_type string, transaction_amount double",
    )


def _silver_write(spark, tbl, df, cycle, appending):
    """Mimic silver_build.silver_simple's DELETE + APPEND cycle path."""
    from pyspark.sql.functions import lit

    df2 = df.withColumn("_batch_id", lit(int(cycle)).cast("bigint"))
    if appending:
        spark.sql(f"DELETE FROM {tbl} WHERE _batch_id = {int(cycle)}")
        df2.writeTo(tbl).append()
    else:
        df2.writeTo(tbl).createOrReplace()


def test_repeat_cycle_1_yields_single_copy(spark):
    tbl = "ice.silver.c360_iceberg_repeat"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    _silver_write(spark, tbl, _rows(spark, 10), cycle=0, appending=False)
    single = spark.table(tbl).count()

    # Cycle 1 twice: same bronze, DELETE-then-APPEND collapses to one copy.
    for _ in range(2):
        _silver_write(spark, tbl, _rows(spark, 10, start=10), cycle=1, appending=True)
    assert spark.table(tbl).count() == single + 10, (
        "cycle 1 repeated with the same bronze must not duplicate rows"
    )


def test_cycle_2_grows_by_its_rows_exactly_once(spark):
    tbl = "ice.silver.c360_iceberg_grow"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    _silver_write(spark, tbl, _rows(spark, 10), cycle=0, appending=False)
    # Cycle 1: 5 new rows.
    _silver_write(spark, tbl, _rows(spark, 5, start=100), cycle=1, appending=True)
    after_c1 = spark.table(tbl).count()
    # Cycle 2: 7 new rows, submitted twice; DELETE by _batch_id collapses.
    for _ in range(2):
        _silver_write(spark, tbl, _rows(spark, 7, start=200), cycle=2, appending=True)
    assert spark.table(tbl).count() == after_c1 + 7


def test_cycle_column_is_carried(spark):
    """Every row carries a non-null _batch_id matching its cycle."""
    tbl = "ice.silver.c360_iceberg_batch_col"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    _silver_write(spark, tbl, _rows(spark, 3), cycle=0, appending=False)
    _silver_write(spark, tbl, _rows(spark, 3, start=100), cycle=1, appending=True)
    counts = {
        r["_batch_id"]: r["n"]
        for r in spark.sql(f"SELECT _batch_id, count(*) n FROM {tbl} GROUP BY _batch_id").collect()
    }
    assert counts == {0: 3, 1: 3}
