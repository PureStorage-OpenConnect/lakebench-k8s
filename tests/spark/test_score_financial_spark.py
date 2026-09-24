"""Executed (not AST) tests of the AML scorer, on a local Spark session.

Skipped unless pyspark and a JVM are available (CI runs them in a dedicated
job). Covers the 2026-09-24 audit finding: recall credited any alert from any
rule, so one broad W1 alert touching every typology made recall 1.0 for all
of them, the random control included.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

SCRIPTS = Path(__file__).resolve().parents[2] / "src" / "lakebench" / "spark" / "scripts"
sys.path.insert(0, str(SCRIPTS))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    # Python UDF/collect workers must use this interpreter, not whatever
    # `python3` is on PATH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


def _manifest(spark):
    rows = [
        # typology_id, typology_type, expected_workload, participant_uetrs
        ("fo-1", "fan_out", "W2", ["u1", "u2"]),
        ("fo-2", "fan_out", "W2", ["u3"]),
        ("dr-1", "dormant_reactivation", "W8", ["u4"]),
        ("gs-1", "gather_scatter", "W1", ["u5"]),
        ("rl-1", "rapid_layering", "W3", ["u6"]),
        ("rnd-1", "random", "W2", ["u7"]),
        ("rnd-2", "random", "W2", ["u8"]),
    ]
    return spark.createDataFrame(
        rows,
        "typology_id STRING, typology_type STRING, expected_workload STRING, "
        "participant_uetrs ARRAY<STRING>",
    )


def _alerts(spark, rows):
    return spark.createDataFrame(
        rows, "alert_id STRING, rule_id STRING, related_txn_ids ARRAY<STRING>"
    )


STATUS = [
    {"rule_id": "W2_structuring", "status": "ran", "target_typology": "fan_out"},
    {
        "rule_id": "W8_dormant_reactivation",
        "status": "ran",
        "target_typology": "dormant_reactivation",
    },
    {
        "rule_id": "W1_connected_components",
        "status": "skipped",
        "target_typology": "gather_scatter",
    },
    {"rule_id": "W3_round_tripping", "status": "error", "target_typology": "rapid_layering"},
    {"rule_id": "W4_risk_propagation", "status": "ran", "target_typology": "stack"},
]


def _by_type(df):
    return {r["typology_type"]: r.asDict() for r in df.collect()}


def test_broad_rule_does_not_credit_other_typologies(spark):
    from score_financial import compute_scores

    # W4 fires one giant alert touching every planted txn; W2 catches one of
    # two fan_out instances; W8 catches nothing.
    alerts = _alerts(
        spark,
        [
            ("a-w4", "W4_risk_propagation", ["u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8", "zz"]),
            ("a-w2", "W2_structuring", ["u1"]),
        ],
    )
    per, summary = compute_scores(spark, _manifest(spark), alerts, STATUS)
    t = _by_type(per)
    assert t["fan_out"]["recall"] == pytest.approx(0.5)
    assert t["fan_out"]["incidental_recall"] == pytest.approx(1.0)
    assert t["dormant_reactivation"]["recall"] == pytest.approx(0.0)
    assert t["dormant_reactivation"]["detection_status"] == "scored"
    assert summary["random_control_floor"] == pytest.approx(1.0)


def test_skipped_error_and_no_rule_are_not_zero(spark):
    from score_financial import compute_scores

    alerts = _alerts(spark, [("a-w4", "W4_risk_propagation", ["u5", "u6", "u7"])])
    per, _ = compute_scores(spark, _manifest(spark), alerts, STATUS)
    t = _by_type(per)
    assert t["gather_scatter"]["detection_status"] == "rule_skipped"
    assert t["gather_scatter"]["recall"] is None
    assert t["rapid_layering"]["detection_status"] == "rule_error"
    assert t["rapid_layering"]["recall"] is None
    assert t["random"]["detection_status"] == "no_rule"
    assert t["random"]["recall"] is None
    assert t["random"]["incidental_recall"] == pytest.approx(0.5)


def test_fp_is_none_without_alerts_and_per_rule_otherwise(spark):
    from score_financial import compute_scores

    empty = _alerts(spark, [])
    _, s0 = compute_scores(spark, _manifest(spark), empty, STATUS)
    assert s0["fp_rate"] is None and s0["total_alerts"] == 0

    alerts = _alerts(
        spark,
        [
            ("a1", "W2_structuring", ["u1"]),  # on target (fan_out)
            ("a2", "W2_structuring", ["u4"]),  # planted, but not W2's target
            ("a3", "W2_structuring", ["zz"]),  # touches nothing planted
        ],
    )
    _, s = compute_scores(spark, _manifest(spark), alerts, STATUS)
    assert s["fp_rate"] == pytest.approx(1 / 3)  # global: only a3 touches nothing
    assert s["fp_rate_by_rule"]["W2_structuring"] == pytest.approx(2 / 3)
