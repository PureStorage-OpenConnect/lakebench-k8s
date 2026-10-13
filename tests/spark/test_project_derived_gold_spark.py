"""Executed: gold_finalize's projection of gold.alerts into
gold.entity_clusters (W1) and gold.risk_scores (W4) on a reused catalog.

gold.alerts can still hold an earlier run's rows (a rule that skipped this
run leaves its old alerts behind). The projection reads only this run's
alerts, so a skipped rule projects nothing instead of re-emitting the old
clusters as freshly detected. An alert with no related entities or no score
is left out: both derived tables declare those columns NOT NULL, and one
such row would otherwise fail the whole write and leave the table empty.

Runs on a local Hadoop Iceberg catalog named like the scripts' default.
"""

from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg")

CAT = "lakehouse"
NOW = "run-now"
OLD = "run-old"


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, CAT, tmp_path_factory.mktemp("derived-gold-wh"))
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.gold")
    return spark_session


def _alert(alert_id, rule_id, run_id, entity_id, related_entity_ids=None, score=None):
    return {
        "alert_id": alert_id,
        "rule_id": rule_id,
        "rule_version": "1",
        "model_id": "lakebench-aml",
        "model_version": "1",
        "entity_id": entity_id,
        "related_entity_ids": related_entity_ids,
        "alert_ts": datetime(2024, 5, 1),
        "alert_score": score,
        "priority": "MED",
        "run_id": run_id,
    }


def _write_alerts(spark, gf, rows):
    from detection_rules import ALERT_COLUMNS
    from pyspark.sql.types import StructField, StructType, _parse_datatype_string

    schema = StructType(
        [StructField(n, _parse_datatype_string(t), nullable) for n, t, nullable in ALERT_COLUMNS]
    )
    data = [tuple(r.get(n) for n, _, _ in ALERT_COLUMNS) for r in rows]
    spark.createDataFrame(data, schema).writeTo(f"{CAT}.{gf.GOLD_ALERTS}").append()


def test_projection_takes_only_this_runs_non_null_alerts(spark, load_script):
    gf = load_script("gold_finalize_financial")
    for t in (gf.GOLD_ALERTS, gf.GOLD_CLUSTERS, gf.GOLD_RISK):
        spark.sql(f"DROP TABLE IF EXISTS {CAT}.{t}")
    for ddl in (gf.DDL_ALERTS, gf.DDL_CLUSTERS, gf.DDL_RISK):
        spark.sql(ddl)
    _write_alerts(
        spark,
        gf,
        [
            _alert("w1-now", "W1_connected_components", NOW, 1, [1, 2], 0.8),
            _alert("w1-now-no-members", "W1_connected_components", NOW, 3, None, 0.8),
            _alert("w1-old", "W1_connected_components", OLD, 4, [4, 5], 0.8),
            _alert("w4-now", "W4_risk_propagation", NOW, 10, [11], 0.7),
            _alert("w4-now-no-score", "W4_risk_propagation", NOW, 12, [13], None),
            _alert("w4-old", "W4_risk_propagation", OLD, 14, [15], 0.9),
        ],
    )

    gf._project_derived_gold(spark, NOW)

    clusters = spark.table(f"{CAT}.{gf.GOLD_CLUSTERS}").collect()
    assert [(r["cluster_id"], r["detection_run_id"]) for r in clusters] == [("w1-now", NOW)]
    assert list(clusters[0]["member_entity_ids"]) == [1, 2]
    risk = spark.table(f"{CAT}.{gf.GOLD_RISK}").collect()
    assert [(r["entity_id"], r["risk_score"], r["run_id"]) for r in risk] == [(10, 0.7, NOW)]
