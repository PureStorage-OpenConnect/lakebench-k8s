"""Executed against a local Iceberg catalog:
``gold_refresh_financial.release_frames`` drops a frame pinned at a snapshot
and a dependent frame that was never materialised, after maintenance expired
the snapshot, without raising and leaving nothing cached. Dropping the pinned
frame first makes Spark rebuild the dependent, which fails on the expired
snapshot.

Runs in a Spark child (``spark_subprocess``) with the Iceberg jar on the
driver classpath at JVM launch.
"""

from __future__ import annotations

import json
import sys

import pytest

pytest.importorskip("pyspark")


@pytest.mark.requires_jars("iceberg")
def test_frames_are_released_after_their_snapshot_expires(tmp_path, spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, tmp_path, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    assert out["error"] is None, out
    assert out["cache_empty"], out


def _run(warehouse, jars):
    import time

    import gold_refresh_financial as gr
    from pyspark import StorageLevel
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", f"file://{warehouse}/wh")
        .getOrCreate()
    )
    table = "lakehouse.gold.t"
    spark.sql(f"CREATE TABLE {table} (k BIGINT) USING iceberg")
    spark.sql(f"INSERT INTO {table} VALUES (1)")
    sid = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").first()[0]
    spark.sql(f"INSERT INTO {table} VALUES (2)")

    # The time-to-detect shape: a frame pinned at the baseline snapshot, and
    # a dependent that a failure stopped before it was materialised.
    prior = spark.sql(f"SELECT * FROM {table} VERSION AS OF {sid}")
    pinned = spark.table(table).join(prior, "k", "left_anti").persist(StorageLevel.MEMORY_AND_DISK)
    pinned.count()
    dependent = pinned.join(spark.range(10).withColumnRenamed("id", "k"), "k")
    dependent = dependent.persist(StorageLevel.MEMORY_AND_DISK)
    # In-stream maintenance expires the pinned snapshot.
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table
    )
    iceberg.expireSnapshots().expireOlderThan(int(time.time() * 1000) + 1000).retainLast(1).commit()

    error = None
    try:
        gr.release_frames([pinned, dependent])
    except Exception as e:  # noqa: BLE001
        error = f"{type(e).__name__}: {e}"[:300]
    cache_empty = bool(spark._jsparkSession.sharedState().cacheManager().isEmpty())
    print(json.dumps({"error": error, "cache_empty": cache_empty}))
    spark.stop()


if __name__ == "__main__":
    _run(sys.argv[1], sys.argv[2])
