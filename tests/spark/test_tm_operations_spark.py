"""Executed against a local Iceberg catalog: the P10 operations layer
(tm_operations.run_tm_operations) on top of gold.alerts.

Covers each stage and each invariant on the tables as written: stage 1
reconciliation and scenario coverage, stage 6 dispositions (priority,
suppression, out-of-population alerts), stage 7 cases with the activity
pull, stage 8 SAR clocks, the reconciliation history across cycles and runs,
and the invariant gate failing when the written tables are inconsistent.

Needs the Iceberg Spark runtime jar (LB_TEST_ICEBERG_JAR or
LB_SPARK_TEST_JARS); skipped otherwise.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

_SCRIPTS = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"


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
sys.path.insert(0, str(_SCRIPTS))


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


def test_tm_operations_end_to_end(tmp_path):
    """Fresh interpreter: spark.jars only applies to a JVM not yet started.
    The scripts directory goes on PYTHONPATH so executor Python workers can
    import tm_operations (the per-customer replay runs there)."""
    import subprocess

    env = dict(os.environ, PYSPARK_PYTHON=sys.executable)
    env["PYTHONPATH"] = os.pathsep.join(p for p in (str(_SCRIPTS), env.get("PYTHONPATH")) if p)
    proc = subprocess.run(
        [sys.executable, __file__, str(tmp_path)],
        capture_output=True,
        text=True,
        env=env,
        timeout=900,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    assert "CHECK OK" in proc.stdout


# Entities: 1-3 customers (CRR high, medium, low), 4-5 not customers.
_ENTITIES = [
    (1, True, "high"),
    (2, True, "medium"),
    (3, True, "low"),
    (4, False, None),
    (5, False, None),
]


def _setup(spark):
    from datetime import datetime, timedelta
    from decimal import Decimal

    import gold_finalize_financial as gf

    for ns in ("gold", "silver", "bronze", "default"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")
    for ddl in (gf.DDL_ALERTS, gf.DDL_STATUS):
        spark.sql(ddl)

    spark.createDataFrame(
        [
            (e, f"party {e}", "person" if c else None, "US", None, c, t, None, None, False, None)
            for e, c, t in _ENTITIES
        ],
        "entity_id BIGINT, name STRING, customer_type STRING, country STRING, "
        "customer_since DATE, is_customer BOOLEAN, crr_tier STRING, crr_score INT, "
        "crr_factors STRING, pep_status BOOLEAN, expected_monthly_volume_usd DECIMAL(18,2)",
    ).writeTo("lakehouse.silver.entities").create()
    spark.createDataFrame(
        [(e * 10, e, Decimal("1000.00")) for e, _, _ in _ENTITIES],
        "account_id BIGINT, holder_entity_id BIGINT, current_balance DECIMAL(38,2)",
    ).writeTo("lakehouse.silver.accounts").create()
    spark.createDataFrame(
        [(1, 4, Decimal("2000.00"), 20), (4, 5, Decimal("250.00"), 5), (2, 3, Decimal("2700"), 3)],
        "source_entity_id BIGINT, target_entity_id BIGINT, cumulative_amount_usd DECIMAL(38,2), "
        "txn_count BIGINT",
    ).writeTo("lakehouse.silver.counterparty_edges").create()

    t0 = datetime(2024, 1, 1)
    rows = []
    # u0..u19: customer 1 <-> non-customer 4, one every 5 days.
    for i in range(20):
        rows.append((f"u{i}", 1, 4, 100.0, 100.0, t0 + timedelta(days=5 * i)))
    # u20..u24: two non-customers only (not the reporting FI's payment).
    for i in range(20, 25):
        rows.append((f"u{i}", 4, 5, 50.0, 50.0, t0 + timedelta(days=i)))
    # u25: customer 2, amount not convertible to USD.
    rows.append(("u25", 2, 5, 70.0, None, t0 + timedelta(days=30)))
    # u26: customer 3 self-transfer; u27..u29: customer 2 activity.
    rows.append(("u26", 3, 3, 10.0, 10.0, t0 + timedelta(days=40)))
    for i in range(27, 30):
        rows.append((f"u{i}", 2, 3, 900.0, 900.0, t0 + timedelta(days=100 + i)))
    # The newest payment fixes the as-of date: 2024-12-31 -> 2025-01-01.
    rows.append(("u30", 3, 5, 5.0, 5.0, datetime(2024, 12, 31, 12)))
    txns = spark.createDataFrame(
        [
            (
                u,
                u,
                o,
                b,
                "BANKAAAA",
                "BANKBBBB",
                Decimal(str(a)),
                None if usd is None else Decimal(str(usd)),
                ts,
                False,
            )
            for u, o, b, a, usd, ts in rows
        ],
        "txn_id STRING, uetr STRING, originator_id BIGINT, beneficiary_id BIGINT, "
        "originator_bank_bic STRING, beneficiary_bank_bic STRING, txn_amount DECIMAL(18,2), "
        "txn_amount_usd DECIMAL(18,2), txn_timestamp TIMESTAMP, cross_border BOOLEAN",
    )
    txns.writeTo("lakehouse.silver.transactions").create()
    txns.select("uetr").writeTo("lakehouse.default.pacs008_raw").create()

    spark.createDataFrame(
        [
            ("t1", "cycle", ["u1", "u2"]),
            ("t2", "random", ["u27"]),
            ("t3", "fan_in", ["u28"]),
        ],
        "typology_id STRING, typology_type STRING, participant_uetrs ARRAY<STRING>",
    ).writeTo("lakehouse.bronze.manifest").create()
    return spark.table("lakehouse.silver.transactions"), len(rows)


def _alerts(spark, run_id):
    from datetime import datetime

    from pyspark.sql.functions import lit

    specs = [
        # (alert_id, rule, entity, alert_ts, related)
        ("A1", "W3_round_tripping", 1, datetime(2024, 1, 11), ["u1", "u2"]),  # planted: true
        ("A2", "W4_risk_propagation", 1, datetime(2024, 1, 20), ["u4"]),  # arrives into A1's case
        ("A3", "W2_structuring", 2, datetime(2024, 5, 10), ["u27"]),  # random control: false
        ("A4", "W7_cross_border_high_risk", 4, datetime(2024, 1, 21), ["u20"]),  # non-customer
        ("A5", "W4_risk_propagation", 3, datetime(2024, 12, 30), ["u30"]),  # still open at as-of
    ]
    rows = [
        (
            a,
            r,
            "1",
            "rule",
            "1",
            e,
            rel,
            [e],
            ts,
            0.5,
            "HIGH",
            "new",
            None,
            "t",
            run_id,
            None,
            None,
            ts,
        )
        for a, r, e, ts, rel in specs
    ]
    spark.createDataFrame(rows, spark.table("lakehouse.gold.alerts").schema).writeTo(
        "lakehouse.gold.alerts"
    ).append()
    spark.createDataFrame(
        [
            ("W3_round_tripping", "ran", None, "cycle", 1),
            ("W4_risk_propagation", "ran", None, "rapid_layering", 2),
            ("W2_structuring", "ran", None, "micro_structuring", 1),
            ("W7_cross_border_high_risk", "ran", None, "corridor_high_risk", 1),
            ("W1_connected_components", "skipped", "vertex-cap", "gather_scatter", None),
        ],
        "rule_id STRING, status STRING, reason STRING, target_typology STRING, alert_count BIGINT",
    ).selectExpr("*", f"'{run_id}' AS run_id", "current_timestamp() AS computed_ts").writeTo(
        "lakehouse.gold.detection_status"
    ).overwrite(lit(True))


PARAMS = {
    "seed": 11,
    "analyst_accuracy": 1.0,
    "investigator_accuracy": 1.0,
    "qa_sample_rate": 1.0,
    "alert_sla_days": 60,
    "lookback_months": 12,
    "late_filing_rate": 0.0,
}


def _check(spark):
    from datetime import date

    import tm_operations as tm
    from pyspark.sql.functions import col

    txns, n_rows = _setup(spark)
    _alerts(spark, "run-x-c1")
    inv = tm.run_tm_operations(
        spark, txns, "run-x-c1", params=PARAMS, source_rows_fn=lambda s: n_rows
    )
    status = {n: (s, d) for n, s, d in inv}
    assert all(s == "pass" for s, _ in status.values()), status
    assert "workflow" not in status

    # Stage 1: reconciliation. 31 payments: 5 between non-customers, 1 with no
    # USD amount (customer 2), 25 monitored.
    rec = {
        (r["section"], r["item"]): r["item_count"]
        for r in spark.table("lakehouse.gold.tm_reconciliation").collect()
    }
    assert rec[("completeness", "source")] == n_rows == 31
    assert rec[("completeness", "bronze")] == 31
    assert rec[("completeness", "monitored")] == 25
    assert rec[("exclusion", "no_customer_party")] == 5
    assert rec[("exclusion", "dq_unconvertible_currency")] == 1
    assert rec[("completeness", "excluded")] == 6
    assert rec[("dq", "dq_self_transfer")] == 1
    assert rec[("dq", "dq_unconvertible_currency")] == 1
    assert rec[("funnel", "alerts")] == 4  # A4 is on a non-customer
    assert rec[("funnel", "alerts_out_of_scope")] == 1

    # Scenario coverage: designated rows with this run's status, a gap row
    # for the planted fan_in no scenario targets, no row for the control.
    cov = {
        (r["typology"], r["rule_id"]): r
        for r in spark.table("lakehouse.gold.scenario_coverage").collect()
    }
    assert cov[("cycle", "W3_round_tripping")]["coverage"] == "designated"
    assert cov[("cycle", "W3_round_tripping")]["planted_instances"] == 1
    assert cov[("gather_scatter", "W1_connected_components")]["rule_status"] == "skipped"
    assert cov[("fan_in", None)]["coverage"] == "gap"
    assert ("random", None) not in cov
    assert cov[(None, "W5_sanctions_match")]["coverage"] == "attribute"

    # Stage 6: dispositions, one per alert.
    d = {r["alert_id"]: r for r in spark.table("lakehouse.gold.alert_dispositions").collect()}
    assert set(d) == {"A1", "A2", "A3", "A4", "A5"}
    assert d["A1"]["triage_priority"] == "critical"  # W3 weight 3 x CRR high 3
    assert d["A1"]["priority_score"] == 9.0
    assert d["A3"]["triage_priority"] == "high"  # W2 weight 2 x CRR medium 2
    assert d["A5"]["triage_priority"] == "low"  # W4 weight 1 x CRR low 1
    assert d["A1"]["simulated_truth"] is True and d["A3"]["simulated_truth"] is False
    assert d["A1"]["disposition"] == "escalated"
    assert d["A2"]["disposition"] == "attached"  # suppression into A1's open case
    assert d["A2"]["case_id"] == d["A1"]["case_id"]
    assert d["A3"]["disposition"] == "closed_nfa"
    assert d["A4"]["disposition"] == "out_of_scope" and d["A4"]["is_customer"] is False
    assert d["A5"]["disposition"] is None and d["A5"]["queue_status"] == "open"
    assert d["A5"]["aging_days"] == (date(2025, 1, 1) - date(2024, 12, 31)).days
    # QA at rate 1 samples every L1 decision; a perfect QA agrees with a
    # perfect analyst.
    assert d["A1"]["qa_sampled"] and d["A3"]["qa_sampled"] and not d["A2"]["qa_sampled"]
    assert not d["A1"]["qa_disagrees"]

    # Stage 7 and 8: one alert-driven case on customer 1, SAR filed inside
    # 30 days, activity pulled from the 12 months before opening, and the
    # 90-day continuing review opened.
    cases = spark.table("lakehouse.gold.cases").collect()
    alert_cases = [c for c in cases if c["case_type"] == "alert_escalation"]
    assert len(alert_cases) == 1
    c = alert_cases[0]
    assert c["customer_id"] == 1 and c["alert_count"] == 2 and c["escalated_alert_count"] == 1
    assert c["priority"] == "critical"
    assert sorted(c["rule_ids"]) == ["W3_round_tripping", "W4_risk_propagation"]
    assert c["sar_decision"] == "sar_filed" and c["case_status"] == "closed"
    assert c["filing_deadline_date"] == c["determination_date"].fromordinal(
        c["determination_date"].toordinal() + 30
    )
    assert c["filed_late"] is False
    # Customer 1's payments before the case opened (u0..u2 at most, by
    # 2024-01-11 plus the L1 turnaround).
    assert c["activity_txn_count"] >= 3
    assert c["activity_window_start"].year == c["opened_date"].year - 1
    reviews = [r for r in cases if r["case_type"] == "continuing_activity"]
    assert reviews and reviews[0]["parent_case_id"] == c["case_id"]
    assert c["continuing_review_case_id"] == reviews[0]["case_id"]
    assert all(r["as_of_date"] == date(2025, 1, 1) for r in cases)

    # Stage 9: the investigator queries run on these tables through the
    # Spark Thrift dialect adapter, as the benchmark runs them.
    from lakebench.benchmark.queries import INVESTIGATOR_QUERIES
    from lakebench.modules.query_engines.spark_thrift.executor import SparkThriftExecutor

    adapt = SparkThriftExecutor.__new__(SparkThriftExecutor).adapt_query
    got = {}
    for q in INVESTIGATOR_QUERIES:
        sql = q.sql.format(
            catalog="lakehouse",
            silver_table="silver.transactions",
            gold_table="gold.daily_dashboards",
            silver_entities="silver.entities",
            silver_accounts="silver.accounts",
            silver_counterparty_edges="silver.counterparty_edges",
            gold_alert_dispositions="gold.alert_dispositions",
            gold_cases="gold.cases",
        )
        got[q.name] = spark.sql(adapt(sql)).collect()
    (c360,) = got["IQ1_customer_360"]
    assert c360["entity_id"] == 1 and c360["alert_count"] == 2 and c360["sars"] >= 1
    assert c360["accounts"] == 1
    # Customer 1 paid u0, u1, u2 in the 12 months before its case opened.
    assert sum(r["txns"] for r in got["IQ2_case_activity_12m"]) >= 3
    assert {r["hop2_entity_id"] for r in got["IQ3_counterparty_two_hop"]} == {5}
    assert got["IQ4_open_cases_over_60_days"] == []

    # Reconciliation history: cycle 2 of the same run keeps cycle 1's rows; a
    # rerun of cycle 2 replaces its own; a new run clears the old run's.
    spark.sql("UPDATE lakehouse.gold.alerts SET run_id = 'run-x-c2'")
    spark.sql("UPDATE lakehouse.gold.detection_status SET run_id = 'run-x-c2'")
    for _ in range(2):
        tm.run_tm_operations(
            spark, txns, "run-x-c2", params=PARAMS, source_rows_fn=lambda s: n_rows
        )
    cyc = spark.table("lakehouse.gold.tm_reconciliation").groupBy("cycle").count().collect()
    per = {r["cycle"]: r["count"] for r in cyc}
    assert set(per) == {1, 2} and per[1] == per[2]
    # The projections are rebuilt per cycle, so they hold one cycle only.
    assert {r["run_id"] for r in spark.table("lakehouse.gold.cases").collect()} == {"run-x-c2"}

    # Continuous: payments datagen wrote that silver does not hold yet are
    # excluded as in_flight; silver holding more than the source is not.
    spark.sql("UPDATE lakehouse.gold.alerts SET run_id = 'run-cont'")
    spark.sql("UPDATE lakehouse.gold.detection_status SET run_id = 'run-cont'")
    inv = tm.run_tm_operations(
        spark,
        txns,
        "run-cont",
        cycle=3,
        continuous=True,
        params=PARAMS,
        source_rows_fn=lambda s: n_rows + 4,
    )
    assert all(s == "pass" for _, s, _ in inv), inv
    rec = {
        (r["section"], r["item"]): r["item_count"]
        for r in spark.table("lakehouse.gold.tm_reconciliation").where("cycle = 3").collect()
    }
    assert rec[("exclusion", "in_flight")] == 4
    inv = tm.run_tm_operations(
        spark,
        txns,
        "run-cont",
        cycle=4,
        continuous=True,
        params=PARAMS,
        source_rows_fn=lambda s: n_rows - 2,
    )
    assert {n: s for n, s, _ in inv}["reconciliation"] == "fail"

    # Invariant failures. (a) Source says more payments than silver holds.
    spark.sql("UPDATE lakehouse.gold.alerts SET run_id = 'run-x-c2'")
    spark.sql("UPDATE lakehouse.gold.detection_status SET run_id = 'run-x-c2'")
    spark.sql("UPDATE lakehouse.gold.alerts SET run_id = 'run-y-c1'")
    spark.sql("UPDATE lakehouse.gold.detection_status SET run_id = 'run-y-c1'")
    inv = tm.run_tm_operations(
        spark, txns, "run-y-c1", params=PARAMS, source_rows_fn=lambda s: n_rows + 5
    )
    st = {n: s for n, s, _ in inv}
    assert st["reconciliation"] == "fail" and st["funnel_monotone"] == "pass"
    assert {r["run_id"] for r in spark.table("lakehouse.gold.tm_reconciliation").collect()} == {
        "run-y"
    }
    # (b) A second open case on one customer, written behind the workflow's
    # back, is caught on read-back.
    spark.sql(
        "INSERT INTO lakehouse.gold.cases SELECT concat(case_id, '-dup'), customer_id, case_type, "
        "parent_case_id, opened_date, crr_tier, priority, alert_count, escalated_alert_count, "
        "rule_ids, first_alert_date, activity_window_start, activity_window_end, "
        "activity_txn_count, activity_amount_usd, 'open', determination, determination_date, "
        "sar_decision, suspect_identified, filing_deadline_date, filing_date, "
        "determination_to_filing_days, filed_late, alert_to_decision_days, sla_breached, "
        "continuing_review_due_date, continuing_review_case_id, simulated_truth, as_of_date, "
        "run_id, computed_ts FROM lakehouse.gold.cases"
    )
    counts = tm.read_back_counts(spark, "run-y-c1", "run-y", 1, date(2025, 1, 1))
    st = {n: s for n, s, _ in tm.evaluate_invariants(counts)}
    assert st["one_open_case_per_customer"] == "fail"
    assert st["cases_le_escalated"] == "fail"  # the duplicate alert-driven case had no escalation

    # (c) No manifest: dispositions cannot be simulated; reported, not raised.
    spark.sql("DROP TABLE lakehouse.bronze.manifest")
    inv = tm.run_tm_operations(spark, txns, "run-y-c1", params=PARAMS, source_rows_fn=lambda s: 1)
    assert [(n, s) for n, s, _ in inv] == [("workflow", "error")]
    assert spark.table("lakehouse.gold.alerts").where(col("run_id") == "run-y-c1").count() == 5

    # The continuous reset drops every TM table, so a new continuous run
    # never shows the previous run's queue before its first tick.
    import bronze_verify_financial as bvf
    from common import table_exists

    bvf._continuous_reset(spark, spark.table("lakehouse.default.pacs008_raw"))
    for t in ("tm_reconciliation", "scenario_coverage", "alert_dispositions", "cases"):
        assert not table_exists(spark, f"lakehouse.gold.{t}"), t


if __name__ == "__main__":
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # Keep the continuous reset's raw-path handling on local disk.
    os.environ["LB_BRONZE_URI"] = f"file://{sys.argv[1]}/raw/"
    _spark = _session(sys.argv[1])
    try:
        _check(_spark)
    finally:
        _spark.stop()
    print("CHECK OK")
