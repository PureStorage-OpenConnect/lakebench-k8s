"""Executed: c360 bronze-verify fails on unusable bronze, recency is anchored
to the data clock, and calendar fields are derived in UTC (E3, E4b, E4c).
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parents[1] / "src/lakebench/spark/scripts"))
sys.path.insert(0, str(_HERE))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    prior = s.conf.get("spark.sql.session.timeZone")
    yield s
    s.conf.set("spark.sql.session.timeZone", prior)
    s.stop()


def _bronze(spark, n=20):
    from c360_stream_scenarios import bronze_df

    return bronze_df(spark, n)


# ---------------------------------------------------------------- bronze_verify


def test_good_bronze_passes(spark):
    from bronze_verify import verify_bronze

    stats, problems, warnings = verify_bronze(_bronze(spark))
    assert problems == []
    assert warnings == []
    assert stats["rows"] == 20
    assert stats["silver_rows"] == 18


def test_zero_rows_fails(spark):
    from bronze_verify import verify_bronze

    _, problems, _ = verify_bronze(_bronze(spark).limit(0))
    assert problems == ["bronze has 0 rows"]


def test_missing_column_fails(spark):
    from bronze_verify import verify_bronze

    _, problems, _ = verify_bronze(_bronze(spark).drop("transaction_amount", "channel"))
    assert any("missing required columns" in p and "transaction_amount" in p for p in problems)


def test_wrong_type_fails(spark):
    """A string event_timestamp would give NULL dates in silver."""
    from bronze_verify import verify_bronze
    from pyspark.sql.functions import col

    df = _bronze(spark).withColumn("event_timestamp", col("event_timestamp").cast("string"))
    _, problems, _ = verify_bronze(df)
    assert any("event_timestamp has type string" in p for p in problems)


def test_all_null_key_fails_partial_null_warns(spark):
    from bronze_verify import verify_bronze
    from pyspark.sql.functions import col, lit, when

    all_null = _bronze(spark).withColumn("customer_id", lit(None).cast("bigint"))
    _, problems, _ = verify_bronze(all_null)
    assert "key column customer_id is null in every row" in problems

    some_null = _bronze(spark).withColumn(
        "customer_id", when(col("id") < 3, None).otherwise(col("customer_id"))
    )
    _, problems, warnings = verify_bronze(some_null)
    assert problems == []
    assert warnings == ["key column customer_id is null in 3 of 20 rows"]


def test_nothing_survives_silver_filter_fails(spark):
    from bronze_verify import verify_bronze
    from pyspark.sql.functions import lit

    df = _bronze(spark).withColumn("data_quality_flag", lit("duplicate_suspected"))
    _, problems, _ = verify_bronze(df)
    assert "no row survives the silver quality filter" in problems


def test_timestamp_ntz_is_accepted(spark):
    from bronze_verify import schema_problems
    from pyspark.sql.functions import col

    df = _bronze(spark).withColumn("event_timestamp", col("event_timestamp").cast("timestamp_ntz"))
    assert schema_problems(df.schema) == []


# ---------------------------------------------------------------- recency (E4b)


def _with_ts(spark, instants):
    """Bronze rows whose event_timestamp values are the given instants."""
    base = _bronze(spark, len(instants)).drop("event_timestamp")
    data = [tuple(r) + (t,) for r, t in zip(base.collect(), instants, strict=True)]
    return spark.createDataFrame(data, base.schema.add("event_timestamp", "timestamp"))


def test_recency_anchored_to_newest_event(spark):
    from common import apply_silver_transformations_anchored, data_clock_date, set_utc_session

    set_utc_session(spark)
    df = _with_ts(
        spark,
        [
            datetime(2024, 3, 31, 23, 0, tzinfo=timezone.utc),
            datetime(2024, 3, 21, 1, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        ],
    )

    anchor = data_clock_date(df)
    assert anchor == date(2024, 3, 31)
    got = {
        r["interaction_date"]: r["customer_recency_score"]
        for r in apply_silver_transformations_anchored(df, anchor).collect()
    }
    assert got == {date(2024, 3, 31): 30, date(2024, 3, 21): 20, date(2024, 1, 1): -60}


def test_recency_does_not_depend_on_run_date(spark):
    """Same data, same anchor, same scores; the old formula used current_date()."""
    from common import apply_silver_transformations, apply_silver_transformations_anchored

    df = _bronze(spark)
    a = [
        r["customer_recency_score"]
        for r in apply_silver_transformations_anchored(df, date(2024, 6, 6)).orderBy("id").collect()
    ]
    b = [
        r["customer_recency_score"]
        for r in apply_silver_transformations_anchored(df, date(2024, 6, 6)).orderBy("id").collect()
    ]
    old = [
        r["customer_recency_score"]
        for r in apply_silver_transformations(df).orderBy("id").collect()
    ]
    assert a == b
    assert max(a) == 30
    # 2024 data scored against today's date is hundreds of days negative.
    assert max(old) < -500


def test_recency_null_without_clock(spark):
    from common import apply_silver_transformations_anchored

    got = {
        r["customer_recency_score"]
        for r in apply_silver_transformations_anchored(_bronze(spark), None).collect()
    }
    assert got == {None}


# ---------------------------------------------------------------- UTC (E4c)


def test_calendar_fields_follow_utc_not_jvm_zone(spark):
    """02:00Z on 1 March is still 29 February in Los Angeles."""
    from common import apply_silver_transformations, set_utc_session

    df = _with_ts(spark, [datetime(2024, 3, 1, 2, 0, tzinfo=timezone.utc)])

    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    la = apply_silver_transformations(df).select("interaction_date", "interaction_hour").first()
    assert la["interaction_date"] == date(2024, 2, 29)

    set_utc_session(spark)
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    utc = apply_silver_transformations(df).select("interaction_date", "interaction_hour").first()
    assert utc["interaction_date"] == date(2024, 3, 1)
    assert utc["interaction_hour"] == 2


@pytest.mark.parametrize(
    "script",
    [
        "bronze_verify.py",
        "bronze_ingest.py",
        "bronze_ingest_delta.py",
        "silver_build.py",
        "silver_build_delta.py",
        "silver_stream.py",
        "silver_stream_delta.py",
        "gold_finalize.py",
        "gold_finalize_delta.py",
        "gold_refresh.py",
        "gold_refresh_delta.py",
    ],
)
def test_every_c360_job_pins_utc(script):
    src = (_HERE.parents[1] / "src/lakebench/spark/scripts" / script).read_text()
    assert "set_utc_session(spark)" in src
    assert "current_date()" not in src
