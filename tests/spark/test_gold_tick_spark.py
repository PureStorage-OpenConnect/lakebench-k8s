"""Executed against a local Iceberg catalog: one continuous gold tick
(gold_refresh_financial.run_tick) over a pinned silver snapshot.

- The tick's alerts are the batch result on the same silver, rule by rule
  (same content: rule_id, entity_id, sorted related_txn_ids).
- A tick over unchanged silver rewrites the same content and measures no new
  alert (idempotent), and a tick after new evidence measures only the new
  alerts.
- The pinned frame does not move when silver takes an append mid-tick.
- The tick logs a phase breakdown the collector parses, with one phase per
  rule, in the continuous rule order.

Needs the Iceberg Spark runtime jar (LB_TEST_ICEBERG_JAR or
LB_SPARK_TEST_JARS), otherwise skipped.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

_ROOT = Path(__file__).resolve().parents[2]


def _find_jar() -> str | None:
    cands = [os.environ.get("LB_TEST_ICEBERG_JAR", "")]
    cands += os.environ.get("LB_SPARK_TEST_JARS", "").split(",")
    for c in (c.strip() for c in cands):
        if c and "iceberg-spark-runtime" in Path(c).name and Path(c).is_file():
            return c
    return None


_JAR = _find_jar()
pytestmark = pytest.mark.skipif(
    _JAR is None, reason="no Iceberg runtime jar in LB_TEST_ICEBERG_JAR / LB_SPARK_TEST_JARS"
)


def test_gold_tick_matches_batch_and_is_idempotent(tmp_path):
    """Runs in a fresh interpreter: spark.jars only takes effect in a JVM that
    has not started yet, and other Spark tests share this process."""
    env = dict(
        os.environ,
        PYSPARK_PYTHON=sys.executable,
        LB_TM_ENABLED="false",
        LB_RUN_ID="run-tick",
        PYTHONPATH=os.pathsep.join(
            [str(_ROOT / "src"), str(_ROOT / "src/lakebench/spark/scripts")]
            + ([os.environ["PYTHONPATH"]] if os.environ.get("PYTHONPATH") else [])
        ),
    )
    proc = subprocess.run(
        [sys.executable, __file__, str(tmp_path)],
        capture_output=True,
        text=True,
        env=env,
        timeout=900,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    assert "CHECK OK" in proc.stdout


def _session(warehouse):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", _JAR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", str(warehouse))
        .getOrCreate()
    )


_TXN_COLS = (
    "uetr string, originator_id bigint, beneficiary_id bigint, txn_timestamp timestamp, "
    "txn_amount decimal(18,2), txn_currency string, txn_amount_usd decimal(18,2), "
    "ingest_ts timestamp"
)


def _rows(tick):
    """Silver rows (uetr, from, to, hours after t0, usd) and the tick whose
    append brings them. Tick 1 holds a structuring burst (W2), a pass-through
    (W4), a round trip (W3) and a layering chain (W17); tick 2 adds a second
    pass-through and a fourth in-band payment to the burst."""
    t1 = [
        ("s1", 1, 9, 1, 9500),
        ("s2", 1, 9, 2, 9400),
        ("s3", 1, 9, 3, 9300),
        ("p1", 30, 2, 5, 1000),
        ("p2", 2, 3, 7, 950),
        ("c1", 10, 11, 10, 500),
        ("c2", 11, 12, 20, 480),
        ("c3", 12, 10, 30, 460),
        ("l1", 40, 41, 10, 2000),
        ("l2", 41, 42, 20, 1900),
        ("l3", 42, 43, 30, 1800),
        ("l4", 43, 44, 40, 1700),
    ]
    t2 = [
        ("s4", 1, 9, 4, 9200),
        ("q1", 50, 51, 100, 3000),
        ("q2", 51, 52, 101, 2900),
    ]
    return t1 if tick == 1 else t2


def _append(spark, tick, ingest_epoch):
    from datetime import datetime, timedelta, timezone
    from decimal import Decimal

    t0 = datetime(2024, 3, 1, tzinfo=timezone.utc)
    ing = datetime.fromtimestamp(ingest_epoch, tz=timezone.utc)
    df = spark.createDataFrame(
        [
            (u, a, b, t0 + timedelta(hours=h), Decimal(amt), "USD", Decimal(amt), ing)
            for u, a, b, h, amt in _rows(tick)
        ],
        _TXN_COLS,
    )
    df.writeTo("lakehouse.silver.transactions").append()


def _content(spark, run_id):
    return sorted(
        (r["rule_id"], r["entity_id"], tuple(sorted(r["related_txn_ids"])))
        for r in spark.table("lakehouse.gold.alerts").where(f"run_id = '{run_id}'").collect()
    )


def _check(spark):
    import time

    import gold_finalize_financial as gf
    import gold_refresh_financial as g

    from lakebench.metrics.collector import MetricsCollector, parse_tick_timing

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql(f"CREATE TABLE lakehouse.silver.transactions ({_TXN_COLS}) USING iceberg")
    spark.sql(
        "CREATE TABLE lakehouse.silver.entities (entity_id BIGINT, is_customer BOOLEAN) "
        "USING iceberg"
    )
    spark.sql(
        "INSERT INTO lakehouse.silver.entities VALUES "
        + ",".join(f"({e}, true)" for e in (1, 2, 10, 11, 12, 40, 41, 42, 43, 44, 50, 51, 52))
    )
    g._bootstrap_gold_tables(spark)

    lines = []
    real_log = g.log

    def capture(msg):
        lines.append(str(msg))
        real_log(msg)

    g.log = capture
    state = g.TickState(time.time(), manifest_ready=True)
    now = time.time()
    _append(spark, 1, now - 100)

    # The pin: an append after the pin is not in the pinned frame.
    txns, sid, rows, newest = g._pin_silver(spark)
    assert sid is not None and rows == len(_rows(1)), (sid, rows)
    assert abs(newest - (now - 100)) < 1e-3, newest
    _append(spark, 2, now - 50)
    assert txns.count() == len(_rows(1))
    spark.sql("DELETE FROM lakehouse.silver.transactions WHERE ingest_ts > timestamp_seconds(0)")
    _append(spark, 1, now - 100)

    phases1 = g.run_tick(spark, state, 1)
    tick1 = _content(spark, "run-tick")
    rules1 = {c[0] for c in tick1}
    for rule in g.CONTINUOUS_RULES:
        assert rule in rules1, (rule, tick1)

    # Phase line: one phase per rule in the continuous order, and the parts
    # add up to the total.
    timing = [parse_tick_timing(ln) for ln in lines]
    timing = [t for t in timing if t is not None]
    assert len(timing) == 1, lines
    names = list(timing[0]["phases"])
    assert timing[0]["silver_rows"] == len(_rows(1))
    assert [n for n in names if n in g.CONTINUOUS_RULES] == list(g.CONTINUOUS_RULES)
    assert names[0] == "probe" and names[-1] == "total"
    # The baseline runs after every rule, so alerts never wait on it.
    assert names.index("baseline") > max(names.index(r) for r in g.CONTINUOUS_RULES)
    # Alerts are measured at their own rule's commit: every alert here has the
    # same arrival, so W4 (first) reads shorter than W3 (last), and the
    # pass-end histogram is logged for comparison with earlier runs.
    per_rule = [ln for ln in lines if "time to detect rule=W4_risk_propagation" in ln]
    pass_end = [ln for ln in lines if "time to detect at pass end" in ln]
    assert len(per_rule) == 1 and len(pass_end) == 1, lines
    g_first = MetricsCollector().parse_streaming_logs("\n".join(lines), "gold-refresh")
    by_rule = g_first.ttd_by_rule
    assert (
        by_rule["W4_risk_propagation"]["max_seconds"] < by_rule["W3_round_tripping"]["max_seconds"]
    ), by_rule
    assert g_first.ttd_pass_end_p50_seconds is not None
    parts = sum(v for k, v in phases1.items() if k != "total")
    assert abs(parts - phases1["total"]) < 0.5, phases1
    g_metrics = MetricsCollector().parse_streaming_logs("\n".join(lines), "gold-refresh")
    assert [t["cycle"] for t in g_metrics.tick_timings] == [1]

    # Tick 2 over unchanged silver: same content, nothing new to measure.
    lines.clear()
    g.run_tick(spark, state, 2)
    assert _content(spark, "run-tick") == tick1
    ttd2 = [ln for ln in lines if "time to detect alerts=" in ln]
    assert len(ttd2) == 1 and "alerts=0 " in ttd2[0], ttd2

    # Tick 3 after new evidence: only the new or changed alerts are measured.
    _append(spark, 2, time.time() - 30)
    lines.clear()
    g.run_tick(spark, state, 3)
    tick3 = _content(spark, "run-tick")
    new = set(tick3) - set(tick1)
    assert new, tick3
    ttd3 = [ln for ln in lines if "time to detect alerts=" in ln]
    assert len(ttd3) == 1 and f"alerts={len(new)} " in ttd3[0], (ttd3, new)

    # A failing baseline does not cost the tick its alerts or measurements,
    # but one that keeps failing still fails the tick.
    real_baseline, real_max = g.build_baseline_dashboards, g.MAX_CONSECUTIVE_FAILURES

    def broken(*_a, **_k):
        raise RuntimeError("dashboards down")

    g.build_baseline_dashboards, g.MAX_CONSECUTIVE_FAILURES = broken, 2
    try:
        lines.clear()
        g.run_tick(spark, state, 4)
        assert any("time to detect alerts=0 " in ln for ln in lines), lines
        assert _content(spark, "run-tick") == tick3
        try:
            g.run_tick(spark, state, 5)
            raise AssertionError("second consecutive baseline failure did not raise")
        except RuntimeError as e:
            assert "consecutive" in str(e), e
    finally:
        g.build_baseline_dashboards, g.MAX_CONSECUTIVE_FAILURES = real_baseline, real_max

    # Equivalence: the batch driver over the same silver writes the same
    # content, rule by rule.
    gf.run_detection_rules(
        spark,
        spark.table("lakehouse.silver.transactions"),
        "run-batch",
        rules=g.CONTINUOUS_RULES,
    )
    assert _content(spark, "run-batch") == tick3


if __name__ == "__main__":
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    _spark = _session(sys.argv[1])
    try:
        _check(_spark)
    finally:
        _spark.stop()
    print("CHECK OK")
