"""Delta silver-stream mechanics scenarios (B4 startup-race, I8 version-race).

Not collected by pytest (no ``test_`` prefix). The pytest wrapper modules run
this in a subprocess, so the scenarios get a JVM with their own static Spark
conf, apart from the other Spark tests in the pytest process.

Usage: python delta_stream_mech_scenarios.py <scenario> <jars> <work_dir>  (jars: comma-separated)

Scenarios:
    startup_race   -- two threads race the not-exists branch of
                      ``silver_stream_delta.write_silver_batch`` on the same
                      table / warehouse. Prints B4-relevant counts.
    version_race   -- one thread commits ten silver micro-batches while
                      another thread commits OPTIMIZE and a second
                      writer's appends to the same table between them.
                      Prints per-batch ``write_silver_batch`` return values.

Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import json
import os
import sys
import threading

from _foreach_batch import inside_foreach_batch
from c360_stream_scenarios import bronze_df


def _session(jars, work):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[4]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", f"file://{work}/ice-wh")
        .config("spark.sql.warehouse.dir", f"{work}/spark-wh")
        .config("spark.sql.session.timeZone", "America/Los_Angeles")
        .getOrCreate()
    )


def _run_startup_race(spark, work):
    """Simulate two concurrent silver_stream_delta writers landing on the same
    empty table. Each thread runs inside a foreachBatch of its own query id
    (``inside_foreach_batch`` sets the local properties a real
    ``foreachBatch`` sets, on that thread).
    """
    import silver_stream_delta as ss

    dtbl = "spark_catalog.silver.customer_interactions_enriched"
    spark.sql("DROP TABLE IF EXISTS " + dtbl)
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")

    errors: list[str] = []
    written: dict[str, int] = {}

    barrier = threading.Barrier(2)

    def _writer(name, start, qid):
        try:
            df = bronze_df(spark, 5, start=start)
            barrier.wait(timeout=30)
            with inside_foreach_batch(spark, 0, qid):
                n = ss.write_silver_batch(df, 0, dtbl, f"file://{work}/")
            written[name] = int(n)
        except Exception as e:  # noqa: BLE001 -- surfaced as JSON
            errors.append(f"{name}: {type(e).__name__}: {e}")

    t1 = threading.Thread(target=_writer, args=("A", 0, "qA"))
    t2 = threading.Thread(target=_writer, args=("B", 100, "qB"))
    t1.start()
    t2.start()
    t1.join(timeout=180)
    t2.join(timeout=180)

    if errors:
        return {"scenario": "startup_race", "errors": errors, "written": written}

    total_rows = spark.table(dtbl).count()
    per_stream = {
        r["_stream_id"]: int(r["cnt"])
        for r in spark.sql(
            f"SELECT _stream_id, COUNT(*) as cnt FROM {dtbl} GROUP BY _stream_id"
        ).collect()
    }
    return {
        "scenario": "startup_race",
        "written": written,
        "table_rows": int(total_rows),
        "per_stream_rows": per_stream,
        "errors": [],
    }


def _run_version_race(spark, work, batches=10):
    """Commit ``batches`` silver micro-batches from one thread while another
    thread commits to the same Delta table between them: OPTIMIZE (the
    compaction stand-in) and appends from a second writer with its own
    stream id. Both are commits Delta lets run beside a blind append. They
    land between this writer's batches (the test asserts the version moved
    by more than one commit), where a before/after-version bracket around a
    write would count them; the I8 read of the write's own commit
    (userMetadata tag) does not.
    """
    import silver_stream_delta as ss

    dtbl = "spark_catalog.silver.customer_interactions_enriched_vr"
    spark.sql("DROP TABLE IF EXISTS " + dtbl)
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")

    # One fixed stream id for this writer; only txnVersion varies per batch.
    qid = "qvr"
    other = "qother"

    stop_ev = threading.Event()
    landed = threading.Event()  # set after each churn commit
    churn = {"optimize": 0, "foreign_appends": 0, "errors": []}

    def _own_rows():
        return int(spark.table(dtbl).where(f"_stream_id = '{qid}'").count())

    def _churn():
        i = 0
        while not stop_ev.is_set():
            try:
                if i % 2 == 0:
                    spark.sql(f"OPTIMIZE {dtbl}")
                    churn["optimize"] += 1
                else:
                    df = bronze_df(spark, 3, start=50_000 + i * 3)
                    with inside_foreach_batch(spark, i, other):
                        ss.write_silver_batch(df, i, dtbl, f"file://{work}/")
                    churn["foreign_appends"] += 1
            except Exception as e:  # noqa: BLE001 -- reported, the test asserts none
                churn["errors"].append(f"{type(e).__name__}: {str(e)[:300]}")
            i += 1
            landed.set()

    # Seed the table before the churn starts.
    df0 = bronze_df(spark, 5, start=0)
    with inside_foreach_batch(spark, 0, qid):
        n0 = ss.write_silver_batch(df0, 0, dtbl, f"file://{work}/")

    ch = threading.Thread(target=_churn, daemon=True)
    ch.start()

    returns: list[int] = [int(n0)]
    own_rows: list[int] = [_own_rows()]
    versions: list[int] = [int(spark.sql(f"DESCRIBE HISTORY {dtbl} LIMIT 1").collect()[0][0])]
    try:
        for bid in range(1, batches):
            df = bronze_df(spark, 5, start=1000 + bid * 5)
            landed.clear()
            with inside_foreach_batch(spark, bid, qid):
                n = ss.write_silver_batch(df, bid, dtbl, f"file://{work}/")
            # At least one churn commit lands between consecutive batches.
            landed.wait(timeout=60)
            returns.append(int(n))
            own_rows.append(_own_rows())
            versions.append(int(spark.sql(f"DESCRIBE HISTORY {dtbl} LIMIT 1").collect()[0][0]))
    finally:
        stop_ev.set()
        ch.join(timeout=30)

    return {
        "scenario": "version_race",
        "errors": [],
        "returns": returns,
        "row_counts": own_rows,
        "versions": versions,
        "churn": churn,
        "batches": batches,
    }


def main():
    if len(sys.argv) < 4:
        print(
            json.dumps(
                {"error": "usage: delta_stream_mech_scenarios.py <scenario> <jars> <work_dir>"}
            )
        )
        sys.exit(2)
    scenario, jars, work = sys.argv[1], sys.argv[2], sys.argv[3]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # Delta's hive-catalog path is what silver_stream_delta uses in this test.
    os.environ.setdefault("LB_CATALOG_TYPE", "hive")
    spark = _session(jars, work)
    from common import set_utc_session

    set_utc_session(spark)

    if scenario == "startup_race":
        out = _run_startup_race(spark, work)
    elif scenario == "version_race":
        try:
            out = _run_version_race(spark, work, batches=10)
        except Exception as e:  # noqa: BLE001 -- surfaced as JSON, like startup_race
            out = {"scenario": "version_race", "errors": [f"{type(e).__name__}: {e}"]}
    else:
        out = {"error": f"unknown scenario {scenario!r}"}
    spark.stop()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts and tests/spark on PYTHONPATH.
    main()
