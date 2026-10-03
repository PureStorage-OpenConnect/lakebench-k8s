"""Executed against a local Iceberg catalog: the gold detection driver computes
each rule's alerts exactly once (F2: it used to count the frame and then
INSERT from an unmaterialised temp view, so every rule ran twice), writes
them, and records the count in gold.detection_status. Also: earlier runs'
alerts are cleared only after this run's 'pending' status is written, and a
rule that skips leaves no rows behind.

Needs the Iceberg Spark runtime jar for the installed Spark in
LB_SPARK_TEST_JARS (see tests/spark/conftest.py).
"""

from __future__ import annotations

import sys

import pytest

pytest.importorskip("pyspark")


pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script")]


def _session(warehouse, jars):
    """*jars*: the comma-separated test jar classpath."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", jars)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", str(warehouse))
        .getOrCreate()
    )


def test_each_rule_computed_once(tmp_path, spark_subprocess, spark_jars):
    """Runs in a fresh interpreter: a JVM with its own static Spark conf
    (catalogs, extensions), apart from the other Spark tests in this
    process."""
    proc = spark_subprocess(__file__, tmp_path, spark_jars.classpath, timeout=600)
    assert "CHECK OK" in proc.stdout


def _check(spark):
    from datetime import datetime

    import detection_rules
    import gold_finalize_financial as gf
    from pyspark.sql.functions import col, udf

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    for ddl in (gf.DDL_ALERTS, gf.DDL_RISK, gf.DDL_CLUSTERS, gf.DDL_DASH, gf.DDL_STATUS):
        spark.sql(ddl)

    calls = spark.sparkContext.accumulator(0)

    def _tick(u):
        calls.add(1)
        return True

    # A filter, not a projected column: count() prunes unused projections,
    # but every computation of the frame must evaluate its filters.
    tick = udf(_tick, "boolean").asNondeterministic()
    template = detection_rules._empty_alerts_df(spark, "x")

    def counting_rule(silver_txns, run_id="unknown"):
        from pyspark.sql.functions import array, concat, current_timestamp, lit

        base = silver_txns.filter(tick(col("uetr"))).select(col("uetr"))
        cols = {
            "alert_id": concat(lit("alert-"), col("uetr")),
            "rule_id": lit("WX_counting"),
            "rule_version": lit("1"),
            "model_id": lit("m"),
            "model_version": lit("1"),
            "entity_id": lit(1).cast("bigint"),
            "related_txn_ids": array(col("uetr")),
            "related_entity_ids": array(lit(1).cast("bigint")),
            "alert_ts": lit(datetime(2024, 3, 1)).cast("timestamp"),
            "alert_score": lit(0.5),
            "priority": lit("LOW"),
            "status": lit("OPEN"),
            "disposition": lit(None).cast("string"),
            "alert_type": lit("test"),
            "run_id": lit(run_id),
            "narrative": lit("n"),
            "evidence": lit(None).cast("map<string,string>"),
            "detected_ts": current_timestamp(),
        }
        return base.select(*[cols[f.name].alias(f.name) for f in template.schema.fields])

    detection_rules._RULE_DISPATCH["WX_counting"] = counting_rule
    try:
        txns = spark.createDataFrame([(f"u{i}",) for i in range(5)], "uetr string")
        gf.run_detection_rules(spark, txns, "run-1", rules=("WX_counting",))
    finally:
        del detection_rules._RULE_DISPATCH["WX_counting"]

    assert calls.value == 5  # once per row: counted and written from one computation
    written = spark.table("lakehouse.gold.alerts").where("run_id = 'run-1'").collect()
    assert sorted(r["alert_id"] for r in written) == [f"alert-u{i}" for i in range(5)]
    status = spark.table("lakehouse.gold.detection_status").collect()
    assert [(r["rule_id"], r["status"], r["alert_count"]) for r in status] == [
        ("WX_counting", "ran", 5)
    ]

    # Ordering: run-2's 'pending' status is written while run-1's alerts
    # still exist; they are cleared only after it.
    seen = []
    real_status = gf._write_detection_status

    def spy(sp, rows, rid):
        old = sp.table("lakehouse.gold.alerts").where("run_id = 'run-1'").count()
        seen.append((rows[0][1], old))
        return real_status(sp, rows, rid)

    def skipping_rule(silver_txns, run_id="unknown"):
        raise detection_rules.RuleSkipped("test-skip")

    detection_rules._RULE_DISPATCH["WX_skip"] = skipping_rule
    gf._write_detection_status = spy
    try:
        # A skipped rule's rows from an earlier tick of the same run go too.
        spark.sql(
            "INSERT INTO lakehouse.gold.alerts SELECT alert_id, 'WX_skip', rule_version, "
            "model_id, model_version, entity_id, related_txn_ids, related_entity_ids, "
            "alert_ts, alert_score, priority, status, disposition, alert_type, 'run-2', "
            "narrative, evidence, detected_ts FROM lakehouse.gold.alerts LIMIT 1"
        )
        gf.run_detection_rules(spark, txns, "run-2", rules=("WX_skip",))
    finally:
        gf._write_detection_status = real_status
        del detection_rules._RULE_DISPATCH["WX_skip"]
    assert seen[0] == ("pending", 5)
    assert spark.table("lakehouse.gold.alerts").count() == 0
    status = spark.table("lakehouse.gold.detection_status").collect()
    assert [(r["rule_id"], r["status"], r["reason"]) for r in status] == [
        ("WX_skip", "skipped", "test-skip")
    ]


def _check_stage_profile(spark):
    """AML-1: with profile_stages, each rule runs in its own job group, logs a
    [stage-profile] line (stages, none, or unavailable) whatever its outcome,
    and the caller's job group is back after the pass. Without it nothing
    changes."""
    import common
    import detection_rules
    import gold_finalize_financial as gf

    from lakebench.metrics.stage_profile import parse_stage_profile

    sc = spark.sparkContext
    sc.setJobGroup("caller-group", "caller description", interruptOnCancel=False)
    template = detection_rules._empty_alerts_df(spark, "x")
    groups = {}

    def ran_rule(silver_txns, run_id="unknown"):
        groups["WX_ran"] = sc.getLocalProperty("spark.jobGroup.id")
        # A shuffle, so the rule's group runs more than one stage.
        silver_txns.groupBy("uetr").count().collect()
        return template

    def skip_rule(silver_txns, run_id="unknown"):
        groups["WX_skip"] = sc.getLocalProperty("spark.jobGroup.id")
        raise detection_rules.RuleSkipped("test-skip")

    def error_rule(silver_txns, run_id="unknown"):
        groups["WX_error"] = sc.getLocalProperty("spark.jobGroup.id")
        raise RuntimeError("boom")

    rules = {"WX_ran": ran_rule, "WX_skip": skip_rule, "WX_error": error_rule}
    logged = []
    real = (gf.log, common.log)

    def capture(m):
        logged.append(m)
        real[1](m)

    detection_rules._RULE_DISPATCH.update(rules)
    gf.log = common.log = capture
    try:
        txns = spark.createDataFrame([(f"u{i}",) for i in range(5)], "uetr string")
        # Default (the continuous tick): no group, no profile.
        gf.run_detection_rules(spark, txns, "run-sp", rules=("WX_ran",))
        assert groups.pop("WX_ran") == "caller-group"
        assert not [m for m in logged if m.startswith("[stage-profile]")], logged
        gf.run_detection_rules(spark, txns, "run-sp", rules=tuple(rules), profile_stages=True)
    finally:
        gf.log, common.log = real
        for r in rules:
            del detection_rules._RULE_DISPATCH[r]

    assert sc.getLocalProperty("spark.jobGroup.id") == "caller-group"
    assert sc.getLocalProperty("spark.job.description") == "caller description"
    for rule, group in groups.items():
        assert group.startswith(f"lb-rule-{rule}-"), groups
    assert len(set(groups.values())) == 3, groups
    lines = [m for m in logged if m.startswith("[stage-profile]")]
    for rule in rules:
        assert any(f"rule={rule} group={groups[rule]} " in m for m in lines), (rule, lines)
    profile, unavailable, _cost = parse_stage_profile("\n".join(lines))
    assert unavailable == {}, unavailable
    assert profile["WX_ran"], lines
    top = profile["WX_ran"][0]
    assert top["tasks"] >= 1 and top["exec_s"] >= 0 and top["stages"] >= 2, top
    assert profile["WX_skip"] == [] and profile["WX_error"] == [], profile
    sc.setLocalProperty("spark.jobGroup.id", None)
    sc.setLocalProperty("spark.job.description", None)
    sc.setLocalProperty("spark.job.interruptOnCancel", None)


def _check_late_entity(spark):
    """Continuous mode: silver_stream commits a batch's transactions before its
    entities, so a gold tick can see a structuring subject's payments while the
    subject is not yet in silver.entities. That tick drops the W2 alert (the
    subject is not a known customer); the next tick re-detects the full corpus
    and raises it, because nothing about a drop is carried between ticks."""
    from datetime import datetime, timedelta
    from decimal import Decimal

    import gold_finalize_financial as gf

    t0 = datetime(2024, 3, 1)
    txns = spark.createDataFrame(
        [(f"s{i}", 1, 9, t0 + timedelta(hours=i), Decimal("9500.00"), "USD") for i in range(3)],
        "uetr string, originator_id bigint, beneficiary_id bigint, "
        "txn_timestamp timestamp, txn_amount decimal(18,2), txn_currency string",
    )
    spark.sql(
        "CREATE TABLE lakehouse.silver.entities (entity_id BIGINT, is_customer BOOLEAN) "
        "USING iceberg"
    )
    # Tick 1: another customer is known, the subject (1) is not yet.
    spark.sql("INSERT INTO lakehouse.silver.entities VALUES (5, true)")

    def w2_alerts():
        gf.run_detection_rules(spark, txns, "run-c", rules=("W2_structuring",))
        return [
            r["entity_id"]
            for r in spark.table("lakehouse.gold.alerts")
            .where("run_id = 'run-c' AND rule_id = 'W2_structuring'")
            .collect()
        ]

    assert w2_alerts() == []
    # The stream's dimension append lands; tick 2 raises the alert.
    spark.sql("INSERT INTO lakehouse.silver.entities VALUES (1, true), (9, false)")
    assert w2_alerts() == [1]


def _cached(df):
    """Whether Spark's cache manager holds *df* (DataFrame.is_cached is a
    Python-side flag that clearCache does not reset)."""
    level = df.storageLevel
    return bool(level.useMemory or level.useDisk)


def _check_screen_base(spark):
    """AML-3: when W5 and W6 both run, the driver builds their screening
    input once, passes the same persisted frame to both, keeps it cached
    from W5 to W6 (only W5's alerts frame is dropped), and the cache is
    cleared after W6. silver.entities exists here (_check_late_entity made
    it)."""
    import detection_rules as dr
    import gold_finalize_financial as gf

    txns = spark.createDataFrame([(f"u{i}",) for i in range(5)], "uetr string")
    template = dr._empty_alerts_df(spark, "x")
    built, seen = [], []

    def fake_base(silver_txns, silver_entities):
        built.append(silver_entities is not None)
        return silver_txns.select("uetr")

    def screening(rule):
        def fn(silver_txns, silver_entities=None, run_id="unknown", screen_base=None):
            seen.append((rule, screen_base, screen_base is not None and _cached(screen_base)))
            return template

        return fn

    real = (dr.screen_base_frame, dict(dr._RULE_DISPATCH))
    dr.screen_base_frame = fake_base
    dr._RULE_DISPATCH["W5_sanctions_match"] = screening("W5")
    dr._RULE_DISPATCH["W6_pep_counterparty"] = screening("W6")
    try:
        gf.run_detection_rules(
            spark, txns, "run-sb", rules=("W5_sanctions_match", "W6_pep_counterparty")
        )
    finally:
        dr.screen_base_frame = real[0]
        dr._RULE_DISPATCH.clear()
        dr._RULE_DISPATCH.update(real[1])
    assert built == [True]
    (r5, b5, cached5), (r6, b6, cached6) = seen
    assert (r5, r6) == ("W5", "W6")
    assert b5 is b6 and b5 is not None
    assert cached5 and cached6, seen
    assert not _cached(b5)

    # One screening rule alone: no shared base.
    seen.clear()
    dr._RULE_DISPATCH["W5_sanctions_match"] = screening("W5")
    try:
        gf.run_detection_rules(spark, txns, "run-sb", rules=("W5_sanctions_match",))
    finally:
        dr._RULE_DISPATCH.clear()
        dr._RULE_DISPATCH.update(real[1])
    assert seen == [("W5", None, False)]


if __name__ == "__main__":
    # Run by spark_subprocess (argv: <warehouse> <jars>), which puts the
    # scripts on PYTHONPATH.
    _spark = _session(sys.argv[1], sys.argv[2])
    try:
        _check(_spark)
        _check_stage_profile(_spark)
        _check_late_entity(_spark)
        _check_screen_base(_spark)
    finally:
        _spark.stop()
    print("CHECK OK")
