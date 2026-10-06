"""Shared scenarios for the Customer 360 gold repeat tests (V16-3).

Each format's test module runs the real ``gold_finalize`` (or
``gold_finalize_delta``) ``main()`` in-process against a silver table it
writes, changes silver on the first and the last day, runs ``main()`` again
with silver reported at 2,000 GB, and compares every gold day with a fresh
aggregation of the changed silver. Before V16-3 the second run chose the
incremental strategy (gold had rows and silver was over 1,000 GB) and
recomputed only the last day, so day 1 kept its old KPIs.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

ROWS = 40_000
DAYS = 200
CUSTOMERS = 500
START = date(2024, 1, 1)
LAST = START + timedelta(days=DAYS - 1)
SILVER = "silver.customer_interactions_enriched"
GOLD = "gold.customer_executive_dashboard"


def silver_df(spark):
    """Silver as the product builds it, from the generator model."""
    from c360_generator_model import generate
    from c360_stream_scenarios import BRONZE_DDL
    from common import apply_silver_transformations_anchored

    cols = [c.split()[0] for c in BRONZE_DDL.split(", ")]
    data = generate(ROWS, CUSTOMERS, START, DAYS, seed=11)
    bronze = spark.createDataFrame([tuple(r[c] for c in cols) for r in data], BRONZE_DDL)
    return apply_silver_transformations_anchored(bronze, LAST)


def changed(df):
    """Silver with every purchase amount on the first and last day doubled."""
    from pyspark.sql.functions import col, lit, when

    hit = col("interaction_date").isin(lit(START), lit(LAST)) & (
        col("interaction_type") == "purchase"
    )
    return df.withColumn(
        "transaction_amount",
        when(hit, col("transaction_amount") * 2).otherwise(col("transaction_amount")),
    )


def fresh_gold(spark, silver_tbl):
    """Gold computed from scratch over *silver_tbl*: {date: row dict}."""
    from common import get_daily_kpi_aggregations

    rows = spark.table(silver_tbl).groupBy("interaction_date").agg(*get_daily_kpi_aggregations())
    return {r["interaction_date"]: r.asDict() for r in rows.collect()}


def gold_rows(spark, gold_tbl):
    return {r["interaction_date"]: r.asDict() for r in spark.table(gold_tbl).collect()}


def _same(a, b):
    """Row equality, floats within one cent: the KPIs are rounded to cents
    (``get_daily_kpi_aggregations``), and a repartitioned aggregation sums
    in another order, so a rounded average can land one cent away. The
    doubled amounts move a changed day's revenue by far more."""
    if a is None or b is None or a.keys() != b.keys():
        return a == b
    for k, v in a.items():
        w = b[k]
        if isinstance(v, float) and isinstance(w, float):
            if not math.isclose(v, w, rel_tol=1e-9, abs_tol=0.0101):
                return False
        elif v != w:
            return False
    return True


def stale_days(spark, silver_tbl, gold_tbl):
    """``(day, field)`` pairs where gold differs from a fresh aggregation of
    silver (field None for a day missing on one side)."""
    want = fresh_gold(spark, silver_tbl)
    got = gold_rows(spark, gold_tbl)
    out = []
    for d in sorted(set(want) | set(got)):
        a, b = want.get(d), got.get(d)
        if a is None or b is None:
            out.append((d, None))
        elif not _same(a, b):
            out.append((d, sorted(k for k in a if not _same({k: a[k]}, {k: b.get(k)}))))
    return out


def run_main(mod, capsys):
    """Run the script's main() in-process; return its log output."""
    capsys.readouterr()
    mod.main()
    return capsys.readouterr().out
