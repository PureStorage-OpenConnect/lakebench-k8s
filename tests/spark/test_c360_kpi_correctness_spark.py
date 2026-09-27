"""Executed: c360 gold KPIs mean what their names say, and the expected-result
checks pass a correct corpus and fail a wrong one.

The corpus is ``c360_generator_model``, a Python model of the Rust
generator's c360 semantics. The KPIs under test are computed by the shared
``get_daily_kpi_aggregations`` both the Iceberg and Delta gold adapters call,
so one test covers both.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parents[1] / "src/lakebench/spark/scripts"))
sys.path.insert(0, str(_HERE))

# 40,000 rows over 20 days: about 2,000 rows and 360 purchases a day, dense
# enough for every per-day and every-day-present check to apply.
ROWS = 40_000
DAYS = 20
CUSTOMERS = 5_000
START = date(2024, 3, 1)


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    prior = s.conf.get("spark.sql.session.timeZone")
    s.conf.set("spark.sql.session.timeZone", "UTC")
    yield s
    s.conf.set("spark.sql.session.timeZone", prior)
    s.stop()


def _bronze(spark, rows=ROWS, seed=7, ticket_space=90_000):
    from c360_generator_model import generate
    from c360_stream_scenarios import BRONZE_DDL

    cols = [c.split()[0] for c in BRONZE_DDL.split(", ")]
    data = generate(rows, CUSTOMERS, START, DAYS, seed=seed, ticket_space=ticket_space)
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in data], BRONZE_DDL)


@pytest.fixture(scope="module")
def pipeline(spark):
    from common import apply_silver_transformations_anchored, get_daily_kpi_aggregations

    bronze = _bronze(spark).cache()
    silver = apply_silver_transformations_anchored(bronze, date(2024, 3, 20)).cache()
    gold = silver.groupBy("interaction_date").agg(*get_daily_kpi_aggregations()).cache()
    return bronze, silver, gold


def _ctx():
    return {
        "window_start": START.isoformat(),
        "window_end": date(2024, 3, 21).isoformat(),
        "customers": CUSTOMERS,
        "scale": 0.05,
        "cycles": 1,
        # The model writes exactly ROWS rows (datagen_rows sizing in a run).
        "bronze_rows_expected": {"snappy": ROWS},
    }


def _bronze_counts(bronze):
    from pyspark.sql.functions import col

    return {
        "rows": bronze.count(),
        "silver_filter_rows": bronze.filter(
            col("data_quality_flag") != "duplicate_suspected"
        ).count(),
    }


# ------------------------------------------------------------------- KPI fixes


def test_avg_transaction_value_is_per_transaction(pipeline):
    from pyspark.sql.functions import avg, col

    _, silver, gold = pipeline
    want = {
        r["interaction_date"]: r["v"]
        for r in silver.filter(col("interaction_type") == "purchase")
        .groupBy("interaction_date")
        .agg(avg("transaction_amount").alias("v"))
        .collect()
    }
    got = {r["interaction_date"]: r["avg_transaction_value"] for r in gold.collect()}
    assert got.keys() == want.keys()
    for d, v in want.items():
        assert got[d] == pytest.approx(round(v, 2), abs=0.006)
    # About 151 per purchase; the old all-row average was about 27.
    overall = sum(got.values()) / len(got)
    assert 120 < overall < 185


def test_visit_averages_exclude_non_visits(pipeline):
    from pyspark.sql.functions import avg, col

    _, silver, gold = pipeline
    want = {
        r["interaction_date"]: (r["pv"], r["tos"])
        for r in silver.filter(col("page_views") > 0)
        .groupBy("interaction_date")
        .agg(avg("page_views").alias("pv"), avg("time_on_site_seconds").alias("tos"))
        .collect()
    }
    for r in gold.collect():
        pv, tos = want[r["interaction_date"]]
        assert r["avg_page_views"] == pytest.approx(round(pv, 1), abs=0.051)
        assert r["avg_time_on_site_seconds"] == pytest.approx(round(tos, 0), abs=0.51)
        assert 9 < r["avg_page_views"] < 12


def test_support_tickets_count_every_support_interaction(spark):
    """Ticket ids collide in a small id space; each support row is a ticket."""
    from common import apply_silver_transformations_anchored, get_daily_kpi_aggregations

    # About 120 support rows a day over 50 possible ticket ids.
    bronze = _bronze(spark, rows=20_000, seed=3, ticket_space=50)
    silver = apply_silver_transformations_anchored(bronze, date(2024, 3, 20))
    gold = silver.groupBy("interaction_date").agg(*get_daily_kpi_aggregations()).collect()
    for r in gold:
        assert r["support_tickets_created"] == r["retention_interactions"]
        # A distinct count could never exceed the 50 ids.
        assert r["support_tickets_created"] > 50


def test_day_without_transactions_has_null_average(spark):
    from c360_stream_scenarios import bronze_df
    from common import apply_silver_transformations_anchored, get_daily_kpi_aggregations
    from pyspark.sql.functions import col, lit, when

    df = (
        bronze_df(spark, 6)
        .withColumn("interaction_type", when(col("id") < 3, lit("browse")).otherwise(lit("login")))
        .withColumn("transaction_amount", lit(0.0))
    )
    silver = apply_silver_transformations_anchored(df, date(2024, 6, 3))
    rows = silver.groupBy("interaction_date").agg(*get_daily_kpi_aggregations()).collect()
    assert rows
    for r in rows:
        assert r["total_transactions"] == 0
        assert r["avg_transaction_value"] is None
        assert r["avg_estimated_ltv"] is None


# ------------------------------------------------------- expected-result checks


def _statuses(record):
    return {c["id"]: c["status"] for c in record["checks"]}


def test_correct_corpus_passes_every_check(pipeline):
    from common import c360_check_facts

    from lakebench.metrics.c360_correctness import pipeline_checks, verdict

    bronze, silver, gold = pipeline
    facts = c360_check_facts(silver, gold)
    rec = verdict(pipeline_checks(facts, _bronze_counts(bronze), _ctx()))
    st = _statuses(rec)
    assert rec["status"] == "pass", [c for c in rec["checks"] if c["status"] == "fail"]
    assert rec["gating"] is False
    # The checks that matter here actually ran.
    for cid in (
        "bronze_rows_match_datagen",
        "avg_transaction_value_daily",
        "avg_transaction_value_overall",
        "gold_days_cover_window",
        "bronze_to_silver_rows",
        "silver_to_gold_counts",
        "distinct_customers",
    ):
        assert st[cid] == "pass", cid


def test_old_all_rows_average_fails_the_check(pipeline):
    """The pre-fix KPI (average over every interaction) is caught."""
    from common import c360_check_facts
    from pyspark.sql.functions import avg
    from pyspark.sql.functions import round as round_

    from lakebench.metrics.c360_correctness import pipeline_checks, verdict

    bronze, silver, gold = pipeline
    old = gold.drop("avg_transaction_value").join(
        silver.groupBy("interaction_date").agg(
            round_(avg("transaction_amount"), 2).alias("avg_transaction_value")
        ),
        "interaction_date",
    )
    rec = verdict(pipeline_checks(c360_check_facts(silver, old), _bronze_counts(bronze), _ctx()))
    st = _statuses(rec)
    assert st["avg_transaction_value_daily"] == "fail"
    assert st["gold_daily_identities"] == "fail"  # avg != revenue / transactions


def test_old_all_rows_ltv_average_fails_the_check(pipeline):
    from common import c360_check_facts
    from pyspark.sql.functions import avg
    from pyspark.sql.functions import round as round_

    from lakebench.metrics.c360_correctness import pipeline_checks, verdict

    bronze, silver, gold = pipeline
    old = gold.drop("avg_estimated_ltv").join(
        silver.groupBy("interaction_date").agg(
            round_(avg("lifetime_value_estimate"), 2).alias("avg_estimated_ltv")
        ),
        "interaction_date",
    )
    rec = verdict(pipeline_checks(c360_check_facts(silver, old), _bronze_counts(bronze), _ctx()))
    chk = next(c for c in rec["checks"] if c["id"] == "gold_daily_identities")
    assert chk["status"] == "fail"
    assert chk["observed"]["violations"]["avg_estimated_ltv_mismatch"] == DAYS


def test_double_counted_cycle_fails_reconciliation(pipeline):
    """Silver holding a cycle twice (the multi-cycle append bug) is caught."""
    from common import c360_check_facts, get_daily_kpi_aggregations

    from lakebench.metrics.c360_correctness import pipeline_checks, verdict

    bronze, silver, _ = pipeline
    doubled = silver.unionByName(silver.limit(4_000))
    gold = doubled.groupBy("interaction_date").agg(*get_daily_kpi_aggregations())
    rec = verdict(pipeline_checks(c360_check_facts(doubled, gold), _bronze_counts(bronze), _ctx()))
    assert _statuses(rec)["bronze_to_silver_rows"] == "fail"


def test_dropped_day_fails_coverage(pipeline):
    from common import c360_check_facts
    from pyspark.sql.functions import col

    from lakebench.metrics.c360_correctness import pipeline_checks, verdict

    bronze, silver, gold = pipeline
    gold_short = gold.filter(col("interaction_date") != date(2024, 3, 5))
    rec = verdict(
        pipeline_checks(c360_check_facts(silver, gold_short), _bronze_counts(bronze), _ctx())
    )
    st = _statuses(rec)
    assert st["gold_days_cover_window"] == "fail"
    assert st["silver_to_gold_days"] == "fail"
    assert st["silver_to_gold_counts"] == "fail"


# ------------------------------------------------------- multi-cycle bronze read


def test_appending_cycle_reads_only_its_own_files(spark, tmp_path, monkeypatch):
    from c360_stream_scenarios import bronze_df
    from common import c360_bronze_path

    base = tmp_path / "customer" / "interactions"
    base.mkdir(parents=True)

    def put(df, name):
        out = tmp_path / "w" / name
        df.coalesce(1).write.parquet(str(out))
        next(out.glob("part-*.parquet")).rename(base / name)

    # datagen_rs::cycle::c360_key names: cycle 0 plain, cycle n with c{n:03}.
    put(bronze_df(spark, 10), "part-000000.parquet")
    put(bronze_df(spark, 4, start=100), "part-c001-000000.parquet")
    uri = f"file://{tmp_path}/"
    monkeypatch.setenv("LB_BRONZE_CYCLE", "1")
    assert spark.read.parquet(c360_bronze_path(uri, appending=True)).count() == 4
    # A full build, or a first cycle, reads every file.
    assert spark.read.parquet(c360_bronze_path(uri, appending=False)).count() == 14
    # Cycle 0 of a multi-cycle run reads only cycle-0 names: a part-c001 file
    # left by an earlier run in the same bucket is not rebuilt into silver.
    monkeypatch.setenv("LB_BRONZE_CYCLE", "0")
    assert spark.read.parquet(c360_bronze_path(uri, appending=False)).count() == 10
    # A single-cycle run reads the whole prefix, as before.
    monkeypatch.delenv("LB_BRONZE_CYCLE")
    assert spark.read.parquet(c360_bronze_path(uri, appending=False)).count() == 14
