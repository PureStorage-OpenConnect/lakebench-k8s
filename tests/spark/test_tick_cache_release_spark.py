"""Executed against a local Iceberg catalog: frames the gold tick caches are
released even when their step fails, and releasing them never raises.

- ``gold_refresh_financial.release_frames`` drops a frame pinned at a
  gold.alerts snapshot and a dependent frame that was never materialised,
  after maintenance expired the snapshot. Dropping the pinned frame first
  makes Spark rebuild the dependent, which fails on the expired snapshot.
- ``gold_finalize_financial._keep_first_detection`` drops the frame it
  persisted when its count fails, since the caller never gets a handle to it.

Both end with an empty Spark cache: a frame left cached is replanned by every
later write to gold.alerts.

Runs in a Spark child (``spark_subprocess``) with the Iceberg jar on the
driver classpath at JVM launch.
"""

from __future__ import annotations

import json
import sys

import pytest

pytest.importorskip("pyspark")


@pytest.mark.requires_jars("iceberg")
def test_tick_frames_are_released_on_failure(tmp_path, spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, tmp_path, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    assert out["release_error"] is None, out
    assert out["release_cache_empty"], out
    assert out["merge_raised"], out
    assert out["merge_cache_empty"], out


def _session(warehouse, jars):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", f"file://{warehouse}/wh")
        .getOrCreate()
    )


def _cache_empty(spark):
    return bool(spark._jsparkSession.sharedState().cacheManager().isEmpty())


def _run(warehouse, jars):
    import time

    import gold_finalize_financial as gf
    import gold_refresh_financial as gr
    from pyspark import StorageLevel
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import LongType

    spark = _session(warehouse, jars)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(gf.DDL_ALERTS)
    table = "lakehouse.gold.alerts"
    row = spark.sql("SELECT * FROM lakehouse.gold.alerts").schema

    def add(entity_id):
        frame = spark.range(1).selectExpr(
            f"'a{entity_id}' AS alert_id",
            "'W2_structuring' AS rule_id",
            "'v1' AS rule_version",
            "'m' AS model_id",
            "'v1' AS model_version",
            f"CAST({entity_id} AS BIGINT) AS entity_id",
            f"array('t{entity_id}') AS related_txn_ids",
            "CAST(NULL AS ARRAY<BIGINT>) AS related_entity_ids",
            "current_timestamp() AS alert_ts",
            "CAST(NULL AS DOUBLE) AS alert_score",
            "CAST(NULL AS STRING) AS priority",
            "CAST(NULL AS STRING) AS status",
            "CAST(NULL AS STRING) AS disposition",
            "CAST(NULL AS STRING) AS alert_type",
            "'run-1' AS run_id",
            "CAST(NULL AS STRING) AS narrative",
            "CAST(NULL AS MAP<STRING, STRING>) AS evidence",
            "current_timestamp() AS detected_ts",
            "CAST(NULL AS ARRAY<STRING>) AS reason_codes",
        )
        frame.select(*(col(f.name).cast(f.dataType) for f in row)).writeTo(table).append()

    # A pinned frame (the time-to-detect baseline) and a dependent frame that
    # a failure stopped before it was materialised.
    add(1)
    add(2)
    sid = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").first()[0]
    add(3)
    prior = spark.sql(f"SELECT * FROM {table} VERSION AS OF {sid}")
    new_rows = spark.table(table).join(prior.select("alert_id"), "alert_id", "left_anti")
    new_rows = new_rows.persist(StorageLevel.MEMORY_AND_DISK)
    new_rows.count()
    dependent = new_rows.join(spark.range(10).withColumnRenamed("id", "entity_id"), "entity_id")
    dependent = dependent.persist(StorageLevel.MEMORY_AND_DISK)
    # In-stream maintenance expires the pinned snapshot. Nothing writes
    # gold.alerts between the step's persist and its release.
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    )
    iceberg.expireSnapshots().expireOlderThan(int(time.time() * 1000) + 1000).retainLast(1).commit()
    release_error = None
    try:
        gr.release_frames([new_rows, dependent])
    except Exception as e:  # noqa: BLE001
        release_error = f"{type(e).__name__}: {e}"[:300]
    release_cache_empty = _cache_empty(spark)
    spark.catalog.clearCache()

    # A first-detection merge whose count fails: the rule's frame raises.
    @udf(LongType())
    def boom(v):
        raise ValueError("rule failed")

    alerts = spark.table(table).withColumn("entity_id", boom(col("entity_id")))
    merge_raised = False
    try:
        gf._keep_first_detection(spark, alerts, "W2_structuring")
    except Exception:  # noqa: BLE001
        merge_raised = True
    merge_cache_empty = _cache_empty(spark)

    print(
        json.dumps(
            {
                "release_error": release_error,
                "release_cache_empty": release_cache_empty,
                "merge_raised": merge_raised,
                "merge_cache_empty": merge_cache_empty,
            }
        )
    )
    spark.stop()


if __name__ == "__main__":
    _run(sys.argv[1], sys.argv[2])
