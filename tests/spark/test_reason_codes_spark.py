"""AML-5 reason codes, executed: each code's condition at its boundary, the
per-code scores against values computed by hand, the per-code union
identity, and the reused-catalog upgrade of an alerts table."""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.requires_jars("iceberg"), pytest.mark.usefixtures("load_script_module")]


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, "lakehouse", tmp_path_factory.mktemp("reason-codes-wh"))
    spark_session.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    return spark_session


def _codes(spark, key, rows, schema):
    from aml_reason_codes import reason_expr

    df = spark.createDataFrame(rows, schema)
    return [list(r["c"]) for r in df.select(reason_expr(key).alias("c")).collect()]


# (projection key, rows, schema, expected codes per row): a row on each side
# of every cut, and a NULL.
CASES = [
    (
        "W1_connected_components",
        [(8,), (7,), (None,)],
        "component_size long",
        [["W1_LARGE_COMPONENT"], [], []],
    ),
    (
        "W2_structuring",
        [("beneficiary", 6), ("originator", 5), ("originator", 6)],
        "_aggregation string, suspicious_count long",
        [["W2_BENEFICIARY_FAN_IN", "W2_HIGH_COUNT"], [], ["W2_HIGH_COUNT"]],
    ),
    ("W3_round_tripping", [(4,), (3,)], "hops int", [["W3_LONG_CYCLE"], []]),
    ("W17_layering_chain", [(5,), (4,)], "hops int", [["W17_LONG_CHAIN"], []]),
    ("W4_risk_propagation", [(3,), (2,)], "chain_count long", [["W4_MULTI_CHAIN"], []]),
    ("W5_sanctions_match", [(1.0,), (0.9,)], "similarity double", [["W5_EXACT"], ["W5_FUZZY"]]),
    (
        "W5_sanctions_match:rescreen",
        [(1.0,), (0.86,)],
        "similarity double",
        [["W5_EXACT", "W5_RESCREEN"], ["W5_FUZZY", "W5_RESCREEN"]],
    ),
    ("W6_pep_counterparty", [(1.0,), (0.9,)], "similarity double", [["W6_EXACT"], ["W6_FUZZY"]]),
    (
        "W7_cross_border_high_risk",
        [("black",), ("grey",), ("synthetic_corridor",), (None,)],
        "risk_tier string",
        [["W7_FATF_BLACK"], ["W7_FATF_GREY"], ["W7_SYNTHETIC_CORRIDOR"], []],
    ),
    ("W8_dormant_reactivation", [(1,)], "x int", [[]]),
]


@pytest.mark.parametrize(("key", "rows", "schema", "want"), CASES, ids=[c[0] for c in CASES])
def test_conditional_codes_at_their_cuts(spark, key, rows, schema, want):
    assert _codes(spark, key, rows, schema) == want


def test_alert_frame_puts_the_base_code_first(spark):
    import detection_rules as dr
    from pyspark.sql.functions import array, col, lit

    df = spark.createDataFrame([(1, 8), (2, 3)], "e long, component_size long")
    out = dr._alert_frame(
        df,
        rule_id="W1_connected_components",
        entity_id=col("e"),
        related_txn_ids=array(lit("u")),
        related_entity_ids=array(col("e")),
        alert_ts=lit("2024-01-01 00:00:00").cast("timestamp"),
        alert_score=lit(0.5),
        priority=lit("LOW"),
        alert_type=lit("cluster"),
        run_id="r",
        narrative=lit("n"),
        evidence=lit(None).cast("map<string,string>"),
    )
    got = {r["entity_id"]: list(r["reason_codes"]) for r in out.collect()}
    assert got == {1: ["W1_COMPONENT", "W1_LARGE_COMPONENT"], 2: ["W1_COMPONENT"]}
    with pytest.raises(TypeError):
        dr._alert_frame(
            df,
            rule_id=lit("W1_connected_components"),
            entity_id=col("e"),
            related_txn_ids=array(lit("u")),
            related_entity_ids=array(col("e")),
            alert_ts=lit(None).cast("timestamp"),
            alert_score=lit(0.5),
            priority=lit("LOW"),
            alert_type=lit("cluster"),
            run_id="r",
            narrative=lit("n"),
            evidence=lit(None).cast("map<string,string>"),
        )


def _manifest(spark):
    # Four W4 instances (stack) and one random-control instance.
    return spark.createDataFrame(
        [
            ("s1", "stack", "x", ["a"]),
            ("s2", "stack", "x", ["b"]),
            ("s3", "stack", "x", ["c"]),
            ("s4", "stack", "x", ["d"]),
            ("r1", "random", "x", ["z"]),
        ],
        "typology_id STRING, typology_type STRING, expected_workload STRING, "
        "participant_uetrs ARRAY<STRING>",
    )


W4 = "W4_risk_propagation"
STATUS = [{"rule_id": W4, "status": "ran", "target_typology": "stack"}]


def _alerts(spark, rows):
    return spark.createDataFrame(
        rows,
        "alert_id STRING, rule_id STRING, related_txn_ids ARRAY<STRING>, "
        "reason_codes ARRAY<STRING>",
    )


# Six alerts, three codes, one alert with two conditional codes. By hand:
# - W4_FAST_PASS_THROUGH (every alert): instances s1, s2, s3 hit -> 3/4;
#   alerts on target: a1, a2, a3, a5 of 6 -> FP 2/6.
# - W4_MULTI_CHAIN (a2, a3, a6): s2, s3 -> 2/4; on target a2, a3 -> FP 1/3.
# - X_EXTRA (a3 only, an unlisted code): s3 -> 1/4; FP 0.
ROWS = [
    ("a1", W4, ["a", "q"], ["W4_FAST_PASS_THROUGH"]),
    ("a2", W4, ["b"], ["W4_FAST_PASS_THROUGH", "W4_MULTI_CHAIN"]),
    ("a3", W4, ["c"], ["W4_FAST_PASS_THROUGH", "W4_MULTI_CHAIN", "X_EXTRA"]),
    ("a4", W4, ["q2"], ["W4_FAST_PASS_THROUGH"]),
    ("a5", W4, ["a"], ["W4_FAST_PASS_THROUGH"]),
    ("a6", W4, ["z"], ["W4_FAST_PASS_THROUGH", "W4_MULTI_CHAIN"]),
]


def test_per_code_scores_by_hand(spark):
    from score_financial import compute_scores

    per, s = compute_scores(spark, _manifest(spark), _alerts(spark, ROWS), STATUS)
    assert s["by_code_status"] == "scored"
    rec = s["recall_by_code"][W4]
    assert rec == pytest.approx(
        {"W4_FAST_PASS_THROUGH": 0.75, "W4_MULTI_CHAIN": 0.5, "X_EXTRA": 0.25}
    )
    assert s["fp_by_code"][W4] == pytest.approx(
        {"W4_FAST_PASS_THROUGH": 2 / 6, "W4_MULTI_CHAIN": 1 / 3, "X_EXTRA": 0.0}
    )
    assert s["alerts_by_code"][W4] == {"W4_FAST_PASS_THROUGH": 6, "W4_MULTI_CHAIN": 3, "X_EXTRA": 1}
    # The base code's recall is the rule's recall (the union identity).
    stack = [r for r in per.collect() if r["typology_type"] == "stack"][0]
    assert rec["W4_FAST_PASS_THROUGH"] == pytest.approx(stack["recall"])
    assert s["fp_rate_by_rule"][W4] == pytest.approx(s["fp_by_code"][W4]["W4_FAST_PASS_THROUGH"])
    assert len(s["reason_code_vocabulary"]) == 16


def test_an_alert_without_a_code_stops_per_code_scores(spark):
    from score_financial import compute_scores

    rows = ROWS[:-1] + [("a6", W4, ["d"], [])]
    _, s = compute_scores(spark, _manifest(spark), _alerts(spark, rows), STATUS)
    assert s["by_code_status"].startswith("not_scored: 1 alerts carry no reason code")
    assert s["recall_by_code"] == {} and s["fp_by_code"] == {}


def test_a_rule_that_did_not_run_gets_no_per_code_scores(spark):
    from score_financial import compute_scores

    status = [{"rule_id": W4, "status": "skipped", "target_typology": "stack"}]
    _, s = compute_scores(spark, _manifest(spark), _alerts(spark, ROWS), status)
    assert W4 not in s["recall_by_code"] and W4 not in s["fp_by_code"]


def test_alerts_without_the_column_are_not_recorded(spark):
    from score_financial import compute_scores

    alerts = _alerts(spark, ROWS).drop("reason_codes")
    _, s = compute_scores(spark, _manifest(spark), alerts, STATUS)
    assert s["by_code_status"].startswith("not_recorded")


def test_ensure_alert_columns_appends_trailing_and_refuses_reordered(spark):
    import common
    from detection_rules import ALERT_COLUMNS

    fq = "lakehouse.gold.alerts_upgrade"
    spark.sql(f"DROP TABLE IF EXISTS {fq}")
    old = ", ".join(f"{n} {t}" for n, t, _ in ALERT_COLUMNS[:17])  # a v1.5 table
    spark.sql(f"CREATE TABLE {fq} ({old}) USING iceberg")
    assert common.ensure_alert_columns(spark, fq, ALERT_COLUMNS) == ["detected_ts", "reason_codes"]
    assert spark.table(fq).columns == [n for n, _, _ in ALERT_COLUMNS]
    assert common.ensure_alert_columns(spark, fq, ALERT_COLUMNS) == []

    bad = "lakehouse.gold.alerts_reordered"
    spark.sql(f"DROP TABLE IF EXISTS {bad}")
    cols = list(ALERT_COLUMNS)
    cols[10], cols[11] = cols[11], cols[10]  # priority and status swapped, both STRING
    spark.sql(f"CREATE TABLE {bad} ({', '.join(f'{n} {t}' for n, t, _ in cols)}) USING iceberg")
    with pytest.raises(common.AlertColumnsError):
        common.ensure_alert_columns(spark, bad, ALERT_COLUMNS)
