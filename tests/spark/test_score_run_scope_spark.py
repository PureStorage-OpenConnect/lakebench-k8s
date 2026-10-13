"""Executed: score_financial's batch score counts only the alerts of the run
gold.detection_status names.

On a reused catalog gold.alerts can still hold an earlier run's alerts (a
rule that skipped this run leaves its old rows). Scoring them would credit
this run with the old run's recall and add them to its alert count and
false positives. Runs ``main()`` end to end on parquet tables in the
session catalog.
"""

from __future__ import annotations

import json
import sys

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")

NOW = "run-now"
OLD = "run-old"
DB = "scorescope"


def test_score_ignores_another_runs_alerts(spark_session, tmp_path, monkeypatch):
    import score_financial as sf

    spark = spark_session
    spark.sql(f"DROP DATABASE IF EXISTS {DB} CASCADE")
    spark.sql(f"CREATE DATABASE {DB}")
    spark.createDataFrame(
        [
            # This run's only alert misses the planted payment.
            ("a-now", "W2_structuring", ["u9"], NOW),
            # An earlier run's alert that would hit it.
            ("a-old", "W2_structuring", ["u1"], OLD),
        ],
        "alert_id STRING, rule_id STRING, related_txn_ids ARRAY<STRING>, run_id STRING",
    ).write.saveAsTable(f"{DB}.alerts")
    spark.createDataFrame(
        [("W2_structuring", "ran", None, "fan_out", 1, NOW)],
        "rule_id STRING, status STRING, reason STRING, target_typology STRING, "
        "alert_count BIGINT, run_id STRING",
    ).write.saveAsTable(f"{DB}.detection_status")
    manifest = tmp_path / "manifest.parquet"
    spark.createDataFrame(
        [("fo-1", "fan_out", "W2", ["u1"])],
        "typology_id STRING, typology_type STRING, expected_workload STRING, "
        "participant_uetrs ARRAY<STRING>",
    ).write.parquet(str(manifest))

    monkeypatch.setattr(sf, "CATALOG", "spark_catalog")
    monkeypatch.setattr(sf, "GOLD_ALERTS", f"{DB}.alerts")
    monkeypatch.setattr(sf, "GOLD_STATUS", f"{DB}.detection_status")
    # The silver tables are not under test: the subject check and customer
    # count report themselves unavailable.
    monkeypatch.setattr(sf, "SILVER_ENTITIES", f"{DB}.missing_entities")
    monkeypatch.setattr(sf, "SILVER_ACCOUNTS", f"{DB}.missing_accounts")
    monkeypatch.setattr(sf, "ACCOUNT_PATH", f"file://{tmp_path}/missing.parquet")
    monkeypatch.setattr(sf, "refuse_protected_corpus", lambda manifest: None)
    monkeypatch.setattr(spark, "stop", lambda: None)  # main() stops the shared session
    monkeypatch.setenv("LB_RUN_ID", NOW)
    out = tmp_path / "out"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "score_financial.py",
            "--manifest",
            f"file://{manifest}",
            "--output",
            f"file://{out}/recall.parquet",
        ],
    )

    sf.main()

    summary = json.loads((out / "recall.json").read_text())
    assert summary["run_id"] == NOW
    assert summary["total_alerts"] == 1, summary
    assert summary["fp_alerts"] == 1, summary
    (fan_out,) = [t for t in summary["typologies"] if t["typology_type"] == "fan_out"]
    assert fan_out["recall"] == 0.0, fan_out
