"""Executed against a local Iceberg catalog: the gold detection driver computes
each rule's alerts exactly once (F2: it used to count the frame and then
INSERT from an unmaterialised temp view, so every rule ran twice), writes
them, and records the count in gold.detection_status.

Needs the Iceberg Spark runtime jar for the installed Spark; set
LB_TEST_ICEBERG_JAR to its path, otherwise the test is skipped.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
_JAR = os.environ.get("LB_TEST_ICEBERG_JAR")
pytestmark = pytest.mark.skipif(
    not (_JAR and Path(_JAR).is_file()), reason="LB_TEST_ICEBERG_JAR not set"
)
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    wh = tmp_path_factory.mktemp("wh")
    s = (
        SparkSession.builder.master("local[1]")
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
        .config("spark.sql.catalog.lakehouse.warehouse", str(wh))
        .getOrCreate()
    )
    yield s
    s.stop()


def test_each_rule_computed_once(spark):
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
