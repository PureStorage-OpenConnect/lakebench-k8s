"""AML-5 reason codes, executed: each code's condition at its boundary, and
the reused-catalog upgrade of an alerts table."""

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

    # A table missing a middle column is refused before anything is altered.
    gap = "lakehouse.gold.alerts_gap"
    spark.sql(f"DROP TABLE IF EXISTS {gap}")
    cols = [c for c in ALERT_COLUMNS[:17] if c[0] != "alert_score"]
    spark.sql(f"CREATE TABLE {gap} ({', '.join(f'{n} {t}' for n, t, _ in cols)}) USING iceberg")
    with pytest.raises(common.AlertColumnsError):
        common.ensure_alert_columns(spark, gap, ALERT_COLUMNS)
    assert len(spark.table(gap).columns) == 16

    bad = "lakehouse.gold.alerts_reordered"
    spark.sql(f"DROP TABLE IF EXISTS {bad}")
    cols = list(ALERT_COLUMNS)
    cols[10], cols[11] = cols[11], cols[10]  # priority and status swapped, both STRING
    spark.sql(f"CREATE TABLE {bad} ({', '.join(f'{n} {t}' for n, t, _ in cols)}) USING iceberg")
    with pytest.raises(common.AlertColumnsError):
        common.ensure_alert_columns(spark, bad, ALERT_COLUMNS)
