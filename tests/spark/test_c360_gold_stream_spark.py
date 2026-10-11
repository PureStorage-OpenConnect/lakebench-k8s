"""Continuous c360 gold streams the silver table (common.run_c360_gold_stream).
After every silver commit gold must equal a full aggregation of silver: rows
landing on dates older than the newest one seen, a silver replay that deletes
and rewrites a batch's rows, and a gold restart that resumes from its
checkpoint must neither drop nor double count a date."""

from __future__ import annotations

import threading
import time

import pytest

pytest.importorskip("pyspark")

import gold_repeat_scenarios as sc  # noqa: E402

pytestmark = [
    pytest.mark.requires_jars("iceberg"),
    pytest.mark.usefixtures("load_script"),
    pytest.mark.slow,
]

PARTS = 4


def _start(spark, silver, gold, checkpoint):
    from common import run_c360_gold_stream

    def write(df):
        df.coalesce(1).writeTo(gold).createOrReplace()

    t = threading.Thread(
        target=run_c360_gold_stream,
        args=(spark, silver, gold, "iceberg", write, checkpoint, "0 seconds"),
        daemon=True,
    )
    t.start()
    return t


def _stop(spark, t):
    for q in spark.streams.active:
        q.stop()
    t.join(60)


def _await_full(spark, silver, gold, timeout=120):
    deadline = time.time() + timeout
    while True:
        try:
            got = sc.gold_rows(spark, gold)
        except Exception:  # noqa: BLE001 -- gold not created yet
            got = {}
        want = sc.fresh_gold(spark, silver)
        if got.keys() == want.keys() and all(sc._same(got[d], want[d]) for d in want):
            return
        if time.time() > deadline:
            bad = [d for d in want if d not in got or not sc._same(got[d], want[d])]
            raise AssertionError(f"gold differs from a full aggregation on {len(bad)} date(s)")
        time.sleep(2)


def test_gold_stream_equals_a_full_aggregation(spark_session, iceberg_catalog, tmp_path):
    from pyspark.sql.functions import abs as abs_
    from pyspark.sql.functions import col, hash

    spark = spark_session
    cat = iceberg_catalog(spark, "lh", tmp_path / "wh", cache_enabled=False)
    silver, gold = f"{cat}.silver.t", f"{cat}.gold.g"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.silver")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.gold")
    # Every part has rows on every date: later parts land on old dates.
    base = sc.silver_df(spark).withColumn(
        "_part", abs_(hash(col("customer_id"), col("session_id"))) % PARTS
    )
    parts = [base.filter(col("_part") == k).drop("_part").cache() for k in range(PARTS)]
    parts[0].writeTo(silver).create()
    checkpoint = str(tmp_path / "ckpt")

    t = _start(spark, silver, gold, checkpoint)
    _await_full(spark, silver, gold)
    parts[1].writeTo(silver).append()
    _await_full(spark, silver, gold)
    # A silver replay: some of a batch's rows are deleted, then written again.
    ids = [r[0] for r in parts[1].select("customer_id").distinct().limit(50).collect()]
    in_ids = ", ".join(repr(i) for i in ids)
    spark.sql(
        f"DELETE FROM {silver} WHERE customer_id IN ({in_ids}) "
        f"AND abs(hash(customer_id, session_id)) % {PARTS} = 1"
    )
    parts[1].filter(col("customer_id").isin(ids)).writeTo(silver).append()
    _await_full(spark, silver, gold)
    parts[2].writeTo(silver).append()
    _await_full(spark, silver, gold)
    _stop(spark, t)

    # A restart resumes from the checkpoint and takes only the new commit.
    parts[3].writeTo(silver).append()
    t = _start(spark, silver, gold, checkpoint)
    _await_full(spark, silver, gold)
    _stop(spark, t)
