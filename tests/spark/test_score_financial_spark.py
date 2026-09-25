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


def test_giant_alert_has_low_txn_precision_and_high_chance(spark):
    """W1-style: one alert over every txn. Alert-level it touches its target,
    so its alert FP is 0; txn-level precision and the per-rule chance expose
    it (review finding on 95f3605)."""
    from score_financial import compute_scores

    status = [
        {"rule_id": "W4_risk_propagation", "status": "ran", "target_typology": "fan_out"},
    ]
    everything = ["u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8"] + [f"b{i}" for i in range(92)]
    alerts = _alerts(spark, [("giant", "W4_risk_propagation", everything)])
    per, s = compute_scores(spark, _manifest(spark), alerts, status)
    assert _by_type(per)["fan_out"]["recall"] == pytest.approx(1.0)
    assert s["fp_rate_by_rule"]["W4_risk_propagation"] == pytest.approx(0.0)
    assert s["txn_precision_by_rule"]["W4_risk_propagation"] == pytest.approx(3 / 100)
    assert s["chance_by_rule"]["W4_risk_propagation"] == pytest.approx(1.0)


def test_rules_without_target_are_not_given_an_fp(spark):
    from score_financial import compute_scores

    status = STATUS + [{"rule_id": "W5_sanctions_match", "status": "ran", "target_typology": None}]
    alerts = _alerts(
        spark, [("s1", "W5_sanctions_match", ["u1"]), ("a1", "W2_structuring", ["u1"])]
    )
    _, s = compute_scores(spark, _manifest(spark), alerts, status)
    assert "W5_sanctions_match" not in s["fp_rate_by_rule"]
    assert s["fp_rate_by_rule"]["W2_structuring"] == pytest.approx(0.0)


def test_partial_when_one_of_two_designated_rules_errors(spark):
    from score_financial import compute_scores

    status = STATUS + [{"rule_id": "W9_extra", "status": "error", "target_typology": "fan_out"}]
    alerts = _alerts(spark, [("a1", "W2_structuring", ["u1"])])
    per, _ = compute_scores(spark, _manifest(spark), alerts, status)
    row = _by_type(per)["fan_out"]
    assert row["detection_status"] == "partial"
    assert row["recall"] == pytest.approx(0.5)


def _subject_frames(spark, customer_of):
    """Manifest (typology, participants, seed), id map dg_id -> key = dg_id +
    1000, and silver.entities with is_customer from ``customer_of(key)``."""
    manifest = spark.createDataFrame(
        [
            ("micro_structuring", [1, 2, 3, 4], 11),  # subject: last (4)
            ("dormant_reactivation", [5, 6], 12),  # subject: first (5)
            ("stack", [7, 8, 9], 13),  # subject: 8 (W17, a counterparty scenario)
            ("random", [10, 11], 14),
            # subject: 12 or 13 by the instance seed's flip (typology.rs)
            ("corridor_high_risk", [12, 13], 15),
        ],
        "typology_type string, participant_entity_ids array<long>, seed long",
    )
    id_map = spark.createDataFrame([(i, i + 1000) for i in range(1, 14)], "dg_id long, key long")
    ents = spark.createDataFrame(
        [(k, customer_of(k)) for k in range(1001, 1014)], "entity_id long, is_customer boolean"
    )
    return manifest, id_map, ents


SCOPED = {"micro_structuring", "dormant_reactivation", "corridor_high_risk"}


def _corridor_subject():
    from aml_features import subject_index

    return 1012 + subject_index("corridor_high_risk", 2, 15)


def test_subject_check_passes_when_every_subject_is_a_customer(spark):
    from score_financial import subject_customer_check

    subjects = {1004, 1005, 1008, 1010, _corridor_subject()}
    m, idm, e = _subject_frames(spark, lambda k: k in subjects)
    out = subject_customer_check(spark, m, e, idm, SCOPED)
    assert out["status"] == "ok", out
    assert out["subjects"] == 5 and out["not_customer"] == 0 and out["unmapped"] == 0


def test_subject_check_follows_the_corridor_flip(spark):
    """The other corridor participant being the customer is a failure: W7
    alerts on the originator, which the seed's flip makes the subject."""
    from score_financial import subject_customer_check

    other = 2025 - _corridor_subject()
    m, idm, e = _subject_frames(spark, lambda k: k in {1004, 1005, 1010, other})
    out = subject_customer_check(spark, m, e, idm, SCOPED)
    assert out["failing_typologies"] == ["corridor_high_risk"], out


def test_subject_check_duplicate_entity_rows(spark):
    """An entity listed twice counts as a customer if any row says so, the
    same answer the rules' semi join gives."""
    from score_financial import subject_customer_check

    subjects = {1004, 1005, 1010, _corridor_subject()}
    m, idm, e = _subject_frames(spark, lambda k: k in subjects)
    dup = e.unionByName(spark.createDataFrame([(1004, False)], e.schema))
    assert subject_customer_check(spark, m, dup, idm, SCOPED)["status"] == "ok"


def test_subject_check_only_counts_scoped_typologies(spark):
    """A typology whose customer-scoped rule did not run (W7, W8 in
    continuous mode) cannot fail the check."""
    from score_financial import subject_customer_check

    m, idm, e = _subject_frames(spark, lambda k: k in {1004})
    out = subject_customer_check(spark, m, e, idm, {"micro_structuring"})
    assert out["status"] == "ok", out


def test_subject_check_fails_on_a_non_customer_subject(spark):
    """An IBAN-join miss that leaves a micro_structuring subject a
    non-customer: W2 cannot alert on it, and the check says so."""
    from score_financial import subject_customer_check

    m, idm, e = _subject_frames(spark, lambda k: k in {1005, 1010, _corridor_subject()})
    out = subject_customer_check(spark, m, e, idm, SCOPED)
    assert out["status"] == "fail"
    assert out["failing_typologies"] == ["micro_structuring"]
    assert out["by_typology"]["micro_structuring"]["not_customer"] == 1
    # stack's subject is not a customer either, but W17 is a counterparty
    # scenario: counted, not failed.
    assert out["by_typology"]["stack"]["not_customer"] == 1


def test_subject_check_fails_on_an_unmapped_subject(spark):
    from score_financial import subject_customer_check

    m, idm, e = _subject_frames(spark, lambda k: True)
    out = subject_customer_check(spark, m, e, idm.where("dg_id <> 5"), SCOPED)
    assert out["status"] == "incomplete", out
    assert out["failing_typologies"] == ["dormant_reactivation"]
    assert out["by_typology"]["dormant_reactivation"]["unmapped"] == 1


def test_subject_check_unchecked_without_participants(spark):
    from score_financial import subject_customer_check

    out = subject_customer_check(spark, _manifest(spark), None, None, SCOPED)
    assert out["status"] == "unchecked" and "participant_entity_ids" in out["reason"]
