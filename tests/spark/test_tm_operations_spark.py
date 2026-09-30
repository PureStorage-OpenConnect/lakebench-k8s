"""Executed against a local Iceberg catalog: the P10 operations layer
(tm_operations.run_tm_operations) on top of gold.alerts.

Covers each stage and each invariant on the tables as written: stage 1
reconciliation and scenario coverage, stage 6 dispositions (priority,
suppression, out-of-population alerts), stage 7 cases with the activity
pull, stage 8 SAR clocks, alert identity and decided history across
cycles, the reconciliation ledger across cycles, ticks and runs, the silver
snapshot pinned against a concurrent stream commit, the not-run and disabled
paths, the Polaris PURGE fallback, and each read-back invariant failing when
the written tables are inconsistent.

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


# Stream ingest time of every bronze and silver row in the fixture.
INGEST_TS = __import__("datetime").datetime(2025, 1, 2, 3, 0)


def _src(ckpt, ingested, pending=None):
    """Raw files as count_source_rows reports them, plus the bronze stream's
    checkpoint source log naming the files batch 0 took. ``ingested`` and
    ``pending``: {file name: rows}."""
    import json as _json

    d = os.path.join(ckpt, "sources", "0")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "0"), "w") as f:
        f.write("v1\n")
        for name in sorted(ingested):
            path = f"file:///raw/{name}.parquet"
            f.write(_json.dumps({"path": path, "timestamp": 1, "batchId": 0}) + "\n")
    by_path = {f"/raw/{k}.parquet": v for k, v in {**ingested, **(pending or {})}.items()}
    return lambda s: {"rows": sum(by_path.values()), "by_path": by_path}


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
                INGEST_TS,
            )
            for u, o, b, a, usd, ts in rows
        ],
        "txn_id STRING, uetr STRING, originator_id BIGINT, beneficiary_id BIGINT, "
        "originator_bank_bic STRING, beneficiary_bank_bic STRING, txn_amount DECIMAL(18,2), "
        "txn_amount_usd DECIMAL(18,2), txn_timestamp TIMESTAMP, cross_border BOOLEAN, "
        "ingest_ts TIMESTAMP",
    )
    txns.writeTo("lakehouse.silver.transactions").create()
    # Bronze as the stream writes it: its snapshot carries the batch id.
    # Bronze as the continuous stream writes it (a real structured-streaming
    # batch, so its snapshot carries spark.sql.streaming.epochId).
    stage = os.path.join(os.environ["LB_TEST_WAREHOUSE"], "bronze-stage")
    txns.select("uetr", "ingest_ts").coalesce(1).write.mode("overwrite").parquet(stage)
    txns.select("uetr", "ingest_ts").limit(0).writeTo("lakehouse.default.pacs008_raw").create()
    q = (
        spark.readStream.format("parquet")
        .schema("uetr STRING, ingest_ts TIMESTAMP")
        .load(stage)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", stage + "-ckpt")
        .trigger(availableNow=True)
        .toTable("lakehouse.default.pacs008_raw")
    )
    q.awaitTermination()
    epochs = spark.sql(
        "SELECT summary['spark.sql.streaming.epochId'] AS e "
        "FROM lakehouse.default.pacs008_raw.snapshots"
    ).collect()
    assert [r["e"] for r in epochs if r["e"] is not None] == ["0"], epochs

    spark.createDataFrame(
        [
            ("t1", "cycle", ["u1", "u2"]),
            ("t2", "random", ["u27"]),
            ("t3", "fan_in", ["u28"]),
        ],
        "typology_id STRING, typology_type STRING, participant_uetrs ARRAY<STRING>",
    ).writeTo("lakehouse.bronze.manifest").create()
    return spark.table("lakehouse.silver.transactions"), len(rows)


_BASE_SPECS = [
    # (alert_id, rule, entity, alert_ts, related)
    ("A1", "W3_round_tripping", 1, (2024, 1, 11), ["u1", "u2"]),  # planted: true
    ("A2", "W4_risk_propagation", 1, (2024, 1, 20), ["u4"]),  # arrives into A1's case
    ("A3", "W2_structuring", 2, (2024, 5, 10), ["u27"]),  # random control: false
    # Non-customer, from a scenario declared customer-and-counterparty.
    ("A4", "W4_risk_propagation", 4, (2024, 1, 21), ["u20"]),
    ("A5", "W4_risk_propagation", 3, (2024, 12, 30), ["u30"]),  # still open at as-of
]


def _alerts(spark, run_id, specs=None):
    from datetime import datetime

    from pyspark.sql.functions import lit

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
            datetime(*ts),
            0.5,
            "HIGH",
            "new",
            None,
            "t",
            run_id,
            None,
            None,
            datetime(*ts),
        )
        for a, r, e, ts, rel in (specs or _BASE_SPECS)
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
    "enabled": True,
    "seed": 11,
    "analyst_accuracy": 1.0,
    "investigator_accuracy": 1.0,
    "qa_sample_rate": 1.0,
    "alert_sla_days": 60,
    "lookback_months": 12,
    "late_filing_rate": 0.0,
    "no_suspect_rate": 0.0,
    "max_alerts_per_customer": 50_000,
    "continuous_interval_seconds": 0,
}


def _st(inv):
    return {n: s for n, s, _ in inv}


def _disp(spark):
    return {r["alert_id"]: r for r in spark.table("lakehouse.gold.alert_dispositions").collect()}


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
    assert {
        "monitored_population",
        "noncustomer_alerts_declared",
        "no_null_disposition",
        "review_not_folded_into_determined",
    } <= set(status)
    assert "history_stable" not in status  # first cycle: nothing to compare

    # Stage 1: reconciliation. 31 payments: 5 between non-customers, 1 with no
    # USD amount (customer 2), 25 monitored.
    rec = {
        (r["section"], r["item"]): r["item_count"]
        for r in spark.table("lakehouse.gold.tm_reconciliation").collect()
    }
    assert rec[("completeness", "customers")] == 3
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
    assert rec[("funnel", "alerts_noncustomer_undeclared")] == 0

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

    # Stage 6: dispositions, one per alert, none NULL.
    d = _disp(spark)
    assert set(d) == {"A1", "A2", "A3", "A4", "A5"}
    assert all(r["disposition"] for r in d.values())
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
    assert d["A4"]["declared_counterparty"] is True
    assert d["A5"]["disposition"] == "pending_l1" and d["A5"]["queue_status"] == "open"
    assert d["A5"]["aging_days"] == (date(2025, 1, 1) - date(2024, 12, 31)).days
    assert d["A1"]["first_seen_cycle"] == 1 and d["A1"]["in_current_detection"]
    assert str(d["A1"]["alert_ts"]) == "2024-01-11 00:00:00"
    # QA at rate 1 samples every L1 decision; a perfect QA agrees with a
    # perfect analyst.
    assert d["A1"]["qa_sampled"] and d["A3"]["qa_sampled"] and not d["A2"]["qa_sampled"]
    assert not d["A1"]["qa_disagrees"]
    keys_c1 = {a: r["alert_key"] for a, r in d.items()}

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
    assert c["regulatory_limit"] == "30_day" and c["suspect_identified"] is True
    assert c["filing_deadline_date"] == c["determination_date"].fromordinal(
        c["determination_date"].toordinal() + 30
    )
    assert c["filed_late"] is False
    # Customer 1's payments before the case opened (u0..u2 at most, by
    # 2024-01-11 plus the L1 turnaround), counted once each.
    assert 3 <= c["activity_txn_count"] <= 3 + 1
    assert c["activity_window_start"].year == c["opened_date"].year - 1
    reviews = [r for r in cases if r["case_type"] == "continuing_activity"]
    assert reviews and reviews[0]["parent_case_id"] == c["case_id"]
    assert c["continuing_review_case_id"] == reviews[0]["case_id"]
    assert c["continuing_review_status"] == "opened"
    assert all(r["as_of_date"] == date(2025, 1, 1) for r in cases)
    case_c1 = {r["case_id"]: r for r in cases}

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
            tm_run_id="run-x",
        )
        got[q.name] = spark.sql(adapt(sql)).collect()
    assert len(got) == 4
    (c360,) = got["IQ1_customer_360"]
    assert c360["entity_id"] == 1 and c360["alert_count"] == 2 and c360["sars"] >= 1
    assert c360["accounts"] == 1
    # Customer 1 paid u0, u1, u2 in the 12 months before its case opened.
    assert sum(r["txns"] for r in got["IQ2_case_activity_12m"]) >= 3
    assert {r["hop2_entity_id"] for r in got["IQ3_counterparty_two_hop"]} == {5}
    assert got["IQ4_open_cases_over_60_days"] == []

    # Cycle 2 of the same run. A1's window grew (later last payment, one more
    # payment); A3 is no longer emitted; A6 is new but its payment predates
    # cycle 1. Identity and decided history carry over.
    specs2 = [s for s in _BASE_SPECS if s[0] not in ("A1", "A3")]
    specs2.append(("A1b", "W3_round_tripping", 1, (2024, 1, 16), ["u1", "u2", "u3"]))
    specs2.append(("A6", "W2_structuring", 3, (2024, 6, 1), ["u26"]))
    _alerts(spark, "run-x-c2", specs2)
    inv = tm.run_tm_operations(
        spark, txns, "run-x-c2", params=PARAMS, source_rows_fn=lambda s: n_rows
    )
    st = _st(inv)
    assert all(v == "pass" for v in st.values()), inv
    assert st["history_stable"] == "pass"
    d2 = {r["alert_key"]: r for r in spark.table("lakehouse.gold.alert_dispositions").collect()}
    a1 = d2[keys_c1["A1"]]
    assert a1["alert_id"] == "A1b" and a1["first_seen_cycle"] == 1
    assert a1["generated_date"] == date(2024, 1, 12)  # as first seen, not re-dated
    assert a1["disposition"] == "escalated" and a1["case_id"] == d["A1"]["case_id"]
    a3 = d2[keys_c1["A3"]]
    assert a3["in_current_detection"] is False and a3["disposition"] == "closed_nfa"
    a6 = [r for r in d2.values() if r["alert_id"] == "A6"][0]
    assert a6["first_seen_cycle"] == 2 and a6["generated_date"] == date(2025, 1, 1)
    case_c2 = {r["case_id"]: r for r in spark.table("lakehouse.gold.cases").collect()}
    for cid, old in case_c1.items():
        assert case_c2[cid]["sar_decision"] == old["sar_decision"]
        assert case_c2[cid]["filing_date"] == old["filing_date"]
    # A rerun of cycle 2 reproduces it and replaces its own ledger rows.
    inv = tm.run_tm_operations(
        spark, txns, "run-x-c2", params=PARAMS, source_rows_fn=lambda s: n_rows
    )
    assert _st(inv)["history_stable"] == "pass", inv
    per = {
        r["cycle"]: r["count"]
        for r in spark.table("lakehouse.gold.tm_reconciliation").groupBy("cycle").count().collect()
    }
    assert set(per) == {1, 2} and per[1] == per[2]
    # The projections are rebuilt per cycle, so they hold one cycle only.
    assert {r["run_id"] for r in spark.table("lakehouse.gold.cases").collect()} == {"run-x-c2"}

    # history_stable can fail: a decided alert whose recorded outcome differs
    # from what the replay reproduces is history rewritten.
    # The recorded cycle-2 state is edited and the ledger pointed at it.
    spark.sql(
        "UPDATE lakehouse.gold.alert_dispositions SET disposition = 'closed_nfa' "
        f"WHERE alert_key = '{keys_c1['A1']}'"
    )
    sid = tm.current_snapshot_id(spark, "lakehouse.gold.alert_dispositions")
    spark.sql(
        f"UPDATE lakehouse.gold.tm_reconciliation SET item_count = {sid} "
        "WHERE cycle = 2 AND section = 'snapshot' AND item = 'alert_dispositions'"
    )
    _alerts(spark, "run-x-c3", specs2)
    inv = tm.run_tm_operations(
        spark, txns, "run-x-c3", params=PARAMS, source_rows_fn=lambda s: n_rows
    )
    assert _st(inv)["history_stable"] == "fail", inv

    # noncustomer_alerts_declared can fail: a customer-only scenario (W7)
    # alerting on a non-customer is reported, not relabelled away.
    _alerts(
        spark, "run-x-c4", specs2 + [("A7", "W7_cross_border_high_risk", 5, (2024, 3, 1), ["u21"])]
    )
    inv = tm.run_tm_operations(
        spark, txns, "run-x-c4", params=PARAMS, source_rows_fn=lambda s: n_rows
    )
    detail = {n: (s, dd) for n, s, dd in inv}["noncustomer_alerts_declared"]
    assert detail[0] == "fail" and detail[1].startswith("1 alerts"), detail

    # Continuous: one ledger set per operations pass, appended. Payments
    # datagen wrote that bronze does not hold yet (the newest raw file) are
    # in flight.
    ckpt = os.path.join(os.environ["LB_TEST_WAREHOUSE"], "bronze-ckpt")
    tm.BRONZE_CHECKPOINT = ckpt
    _alerts(spark, "run-cont")
    inv = tm.run_tm_operations(
        spark,
        txns,
        "run-cont",
        cycle=7,
        continuous=True,
        params=PARAMS,
        source_rows_fn=_src(ckpt, {"f1": n_rows}, {"f2": 4}),
    )
    assert all(s == "pass" for _, s, _ in inv), inv
    rec = {
        (r["section"], r["item"]): r["item_count"]
        for r in spark.table("lakehouse.gold.tm_reconciliation").where("cycle = 1").collect()
    }
    assert rec[("exclusion", "in_flight_to_bronze")] == 4
    assert rec[("exclusion", "in_flight_to_silver")] == 0
    assert rec[("completeness", "unaccounted")] == 0
    assert rec[("status", "complete")] is None and rec[("snapshot", "cases")] > 0

    # Race: the stream commits more payments to silver while the source is
    # being counted. Silver was pinned before the count, so the ledger
    # balances instead of showing a negative in_flight.
    def count_then_stream(s):
        extra = s.table("lakehouse.silver.transactions").limit(3)
        extra = extra.selectExpr(
            "concat(txn_id, '-late') AS txn_id",
            "concat(uetr, '-late') AS uetr",
            *[c for c in extra.columns if c not in ("txn_id", "uetr")],
        )
        extra.writeTo("lakehouse.silver.transactions").append()
        return _src(ckpt, {"f1": n_rows})(s)

    inv = tm.run_tm_operations(
        spark,
        spark.table("lakehouse.silver.transactions"),
        "run-cont",
        cycle=8,
        continuous=True,
        params=PARAMS,
        source_rows_fn=count_then_stream,
    )
    assert _st(inv)["reconciliation"] == "pass", inv
    assert _st(inv)["history_stable"] == "pass", inv
    cyc = {
        r["cycle"]: r["cycle_run_id"]
        for r in spark.table("lakehouse.gold.tm_reconciliation").collect()
    }
    assert cyc == {1: "run-cont-t1", 2: "run-cont-t2"}
    spark.sql("DELETE FROM lakehouse.silver.transactions WHERE txn_id LIKE '%-late'")

    # Permanent loss fails instead of passing as in flight. (a) A raw file
    # the stream's log says it took, but bronze does not hold.
    inv = tm.run_tm_operations(
        spark,
        txns,
        "run-cont",
        cycle=9,
        continuous=True,
        params=PARAMS,
        source_rows_fn=_src(ckpt, {"f0": 3, "f1": n_rows}),
    )
    assert _st(inv)["reconciliation"] == "fail", inv
    # (b) A bronze row older than silver's watermark that silver never took.
    spark.sql(
        "INSERT INTO lakehouse.default.pacs008_raw VALUES "
        "('lost-1', TIMESTAMP '2025-01-01 00:00:00')"
    )
    inv = tm.run_tm_operations(
        spark,
        txns,
        "run-cont",
        cycle=10,
        continuous=True,
        params=PARAMS,
        source_rows_fn=_src(ckpt, {"f1": n_rows + 1}),
    )
    assert _st(inv)["reconciliation"] == "fail", inv
    # ... while a bronze row newer than the watermark is in flight to silver.
    spark.sql("DELETE FROM lakehouse.default.pacs008_raw WHERE uetr = 'lost-1'")
    spark.sql(
        "INSERT INTO lakehouse.default.pacs008_raw VALUES "
        "('new-1', TIMESTAMP '2025-01-03 00:00:00')"
    )
    inv = tm.run_tm_operations(
        spark,
        txns,
        "run-cont",
        cycle=11,
        continuous=True,
        params=PARAMS,
        source_rows_fn=_src(ckpt, {"f1": n_rows + 1}),
    )
    assert _st(inv)["reconciliation"] == "pass", inv
    spark.sql("DELETE FROM lakehouse.default.pacs008_raw WHERE uetr = 'new-1'")
    # (c) Silver holding more than the source is a duplication.
    inv = tm.run_tm_operations(
        spark,
        txns,
        "run-cont",
        cycle=12,
        continuous=True,
        params=PARAMS,
        source_rows_fn=_src(ckpt, {"f1": n_rows - 2}),
    )
    assert _st(inv)["reconciliation"] == "fail"

    # A crash after the TM tables were written fails the pass (not "not
    # run"); the next pass takes a new cycle number and carries from the
    # last completed cycle, not from the half-written one.
    ledger_before = tm._ledger_cycles(spark, "run-cont")
    real = tm.case_activity
    tm.case_activity = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("executor lost"))
    try:
        inv = tm.run_tm_operations(
            spark,
            txns,
            "run-cont",
            cycle=13,
            continuous=True,
            params=PARAMS,
            source_rows_fn=_src(ckpt, {"f1": n_rows}),
        )
    finally:
        tm.case_activity = real
    assert [(n, s) for n, s, _ in inv] == [("workflow", "fail")], inv
    crashed = max(tm._ledger_cycles(spark, "run-cont"))
    assert crashed == max(ledger_before) + 1
    assert not tm._ledger_cycles(spark, "run-cont")[crashed]["complete"]
    # A restarted driver (fresh file cache) with the same run id appends.
    tm._SOURCE_SEEN.clear()
    inv = tm.run_tm_operations(
        spark,
        txns,
        "run-cont",
        cycle=1,
        continuous=True,
        params=PARAMS,
        source_rows_fn=_src(ckpt, {"f1": n_rows}),
    )
    st = _st(inv)
    assert st["history_stable"] == "pass" and st["one_row_per_alert_identity"] == "pass", inv
    ledger = tm._ledger_cycles(spark, "run-cont")
    assert max(ledger) == crashed + 1 and ledger[crashed + 1]["complete"]
    assert set(ledger) >= set(ledger_before)

    # An unreadable ledger stops the pass before it writes anything.
    class NoLedger:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def table(self, name):
            if name.endswith("tm_reconciliation"):
                raise RuntimeError("metastore timeout")
            return self._inner.table(name)

    snap = tm.current_snapshot_id(spark, "lakehouse.gold.alert_dispositions")
    inv = tm.run_tm_operations(
        NoLedger(spark),
        txns,
        "run-cont",
        cycle=2,
        continuous=True,
        params=PARAMS,
        source_rows_fn=_src(ckpt, {"f1": n_rows}),
    )
    assert [(n, s) for n, s, _ in inv] == [("workflow", "not_run")], inv
    assert tm.current_snapshot_id(spark, "lakehouse.gold.alert_dispositions") == snap
    assert tm._ledger_cycles(spark, "run-cont") == ledger

    # A new run clears the old run's ledger. (a) Source says more payments
    # than silver holds.
    _alerts(spark, "run-y-c1")
    inv = tm.run_tm_operations(
        spark, txns, "run-y-c1", params=PARAMS, source_rows_fn=lambda s: n_rows + 5
    )
    st = _st(inv)
    assert st["reconciliation"] == "fail" and st["funnel_monotone"] == "pass"
    assert {r["run_id"] for r in spark.table("lakehouse.gold.tm_reconciliation").collect()} == {
        "run-y"
    }
    # (b) A second open case on one customer, written behind the workflow's
    # back, is caught on read-back.
    cols = ", ".join(spark.table("lakehouse.gold.cases").columns)
    dup = cols.replace("case_id, customer_id", "concat(case_id, '-dup'), customer_id", 1)
    dup = dup.replace("case_status", "'open'", 1)
    spark.sql(f"INSERT INTO lakehouse.gold.cases SELECT {dup} FROM lakehouse.gold.cases")
    counts = tm.read_back(spark, "run-y-c1", date(2025, 1, 1))
    counts.update(source=31, customers=3, silver=31, monitored=25, excluded=6)
    st = _st(tm.evaluate_invariants(counts))
    assert st["one_open_case_per_customer"] == "fail"
    assert st["cases_le_escalated"] == "fail"  # the duplicate alert-driven case had no escalation
    # (c) A review credited to a case determined before the review was due.
    spark.sql("DELETE FROM lakehouse.gold.cases WHERE case_id LIKE '%-dup'")
    sar = spark.table("lakehouse.gold.cases").where("sar_decision = 'sar_filed'").first()
    spark.sql(
        "UPDATE lakehouse.gold.cases SET continuing_review_status = 'folded', "
        f"continuing_review_case_id = '{sar['case_id']}' WHERE case_id = '{sar['case_id']}'"
    )
    counts = tm.read_back(spark, "run-y-c1", date(2025, 1, 1))
    assert counts["reviews_folded_into_determined"] == 1

    # (d) The per-customer cap never drops an alert already in the workflow:
    # a new backdated alert on customer 1 under cap 2 must not push A2 out
    # while its prior row is carried (it would be written twice).
    capped = dict(PARAMS, max_alerts_per_customer=2)
    _alerts(spark, "run-z-c1")
    tm.run_tm_operations(spark, txns, "run-z-c1", params=capped, source_rows_fn=lambda s: n_rows)
    _alerts(spark, "run-z-c2", _BASE_SPECS + [("A0", "W2_structuring", 1, (2024, 1, 2), ["u0"])])
    st = _st(
        tm.run_tm_operations(
            spark, txns, "run-z-c2", params=capped, source_rows_fn=lambda s: n_rows
        )
    )
    assert st["one_row_per_alert_identity"] == "pass" and st["history_stable"] == "pass", st
    # The Spark-side memory bound (4 x cap) never drops a grown alert whose
    # prior is carried (reviewer probe p5: it was written twice).
    one = dict(PARAMS, max_alerts_per_customer=1)
    _alerts(spark, "run-h-c1", [("H1", "W2_structuring", 1, (2024, 1, 11), ["u1"])])
    tm.run_tm_operations(spark, txns, "run-h-c1", params=one, source_rows_fn=lambda s: n_rows)
    hub = [("H1g", "W2_structuring", 1, (2024, 1, 12), ["u1", "u2"])]
    hub += [(f"HB{i}", "W3_round_tripping", 1, (2024, 1, 2 + i), [f"u{5 + i}"]) for i in range(5)]
    _alerts(spark, "run-h-c2", hub)
    st = _st(
        tm.run_tm_operations(spark, txns, "run-h-c2", params=one, source_rows_fn=lambda s: n_rows)
    )
    assert st["one_row_per_alert_identity"] == "pass" and st["history_stable"] == "pass", st
    rows = spark.table("lakehouse.gold.alert_dispositions").collect()
    assert len(rows) == 6 and len({r["alert_key"] for r in rows}) == 6
    h1 = [r for r in rows if r["alert_id"] == "H1g"][0]
    assert h1["disposition"] != "over_capacity" and h1["first_seen_cycle"] == 1

    # Neighbours sharing one payment with a carried alert (other rules, and
    # the same rule below the growth threshold) do not rank as known, so they
    # cannot push the carried alert past the bound (reviewer probe R4/p2).
    a1 = ("N1", "W2_structuring", 1, (2024, 3, 11), ["u9", "u10"])
    _alerts(spark, "run-n-c1", [a1])
    tm.run_tm_operations(spark, txns, "run-n-c1", params=one, source_rows_fn=lambda s: n_rows)
    nb = [a1]
    nb += [(f"NW3{i}", "W3_round_tripping", 1, (2024, 1, 2 + i), ["u9", f"u{i}"]) for i in range(3)]
    nb += [(f"NW2{i}", "W2_structuring", 1, (2024, 1, 2 + i), ["u9", f"u1{i}"]) for i in range(3)]
    _alerts(spark, "run-n-c2", nb)
    st = _st(
        tm.run_tm_operations(spark, txns, "run-n-c2", params=one, source_rows_fn=lambda s: n_rows)
    )
    assert st["one_row_per_alert_identity"] == "pass" and st["history_stable"] == "pass", st
    rows = spark.table("lakehouse.gold.alert_dispositions").collect()
    n1 = [r for r in rows if r["alert_id"] == "N1"]
    assert len(n1) == 1 and n1[0]["disposition"] != "over_capacity"

    # The continuous window start survives a driver restart.
    wdir = os.path.join(os.environ["LB_TEST_WAREHOUSE"], "gold-ckpt")
    assert tm.window_start_marker(spark, "file://" + wdir, 100.0) == 100.0
    assert tm.window_start_marker(spark, "file://" + wdir, 500.0) == 100.0

    # ... and the identity check fails when a row is duplicated.
    spark.sql(
        "INSERT INTO lakehouse.gold.alert_dispositions SELECT * FROM "
        "lakehouse.gold.alert_dispositions WHERE alert_id = 'NW30'"
    )
    counts = tm.read_back(spark, "run-n-c2", date(2025, 1, 1))
    counts.update(source=31, customers=3, silver=31, monitored=25, excluded=6)
    assert _st(tm.evaluate_invariants(counts))["one_row_per_alert_identity"] == "fail"

    # (e) No manifest: the layer reports not run, with the reason, and
    # leaves detection's alerts alone.
    spark.sql("DROP TABLE lakehouse.bronze.manifest")
    inv = tm.run_tm_operations(spark, txns, "run-y-c1", params=PARAMS, source_rows_fn=lambda s: 1)
    assert [(n, s) for n, s, _ in inv] == [("workflow", "not_run")]
    assert "manifest" in inv[0][2]
    assert spark.table("lakehouse.gold.alerts").where(col("run_id") == "run-y-c1").count() == 5
    # (f) Disabled: says so and does nothing else.
    inv = tm.run_tm_operations(spark, txns, "run-y-c1", params=dict(PARAMS, enabled=False))
    assert [(n, s) for n, s, _ in inv] == [("workflow", "disabled")]

    # Polaris refuses DROP ... PURGE: the reset falls back to a plain DROP and
    # removes only the table's own directory.
    import bronze_verify_financial as bvf
    from common import table_exists

    class NoPurge:
        def __init__(self, inner):
            self._inner = inner
            self._jvm = inner._jvm
            self._jsc = inner._jsc

        def sql(self, q):
            if "PURGE" in q:
                raise RuntimeError("403 Forbidden: DROP_WITH_PURGE_ENABLED is false")
            return self._inner.sql(q)

    loc = bvf._table_location(spark, "lakehouse.gold.cases")
    assert loc and os.path.isdir(loc.replace("file:", "", 1))
    bvf._drop_owned_table(NoPurge(spark), "gold.cases")
    assert not table_exists(spark, "lakehouse.gold.cases")
    assert not os.path.exists(loc.replace("file:", "", 1))
    assert os.path.isdir(os.path.dirname(loc.replace("file:", "", 1)))  # the namespace stays

    # The continuous reset drops every TM table, so a new continuous run
    # never shows the previous run's queue before its first tick.
    bvf._continuous_reset(spark, spark.table("lakehouse.default.pacs008_raw"))
    for t in ("tm_reconciliation", "scenario_coverage", "alert_dispositions", "cases"):
        assert not table_exists(spark, f"lakehouse.gold.{t}"), t


if __name__ == "__main__":
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # A path-based (hadoop) catalog places tables itself, like Polaris; the
    # Hive-only explicit bronze location does not apply.
    os.environ["LB_CATALOG_TYPE"] = "polaris"
    os.environ["LB_TEST_WAREHOUSE"] = sys.argv[1]
    # Keep the continuous reset's raw-path handling on local disk.
    os.environ["LB_BRONZE_URI"] = f"file://{sys.argv[1]}/raw/"
    _spark = _session(sys.argv[1])
    try:
        _check(_spark)
    finally:
        _spark.stop()
    print("CHECK OK")
