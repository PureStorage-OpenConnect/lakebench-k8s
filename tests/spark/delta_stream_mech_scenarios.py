"""Delta silver-stream mechanics scenarios (B4 startup-race, I8 version-race).

Not collected by pytest (no ``test_`` prefix). The pytest wrapper modules run
this in a subprocess so the Delta jars sit on the driver classpath at JVM
launch and no earlier test's plain JVM interferes.

Usage: python delta_stream_mech_scenarios.py <scenario> <jar_dir> <work_dir>

Scenarios:
    startup_race   -- two threads race the not-exists branch of
                      ``silver_stream_delta.write_silver_batch`` on the same
                      table / warehouse. Prints B4-relevant counts.
    version_race   -- one thread commits ten silver micro-batches while
                      another thread issues frequent metadata commits to
                      the same table (a compaction/vacuum stand-in).
                      Prints per-batch ``write_silver_batch`` return values.

Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import glob
import json
import os
import sys
import threading
import time
from pathlib import Path

from c360_stream_scenarios import bronze_df  # noqa: E402


def _session(jar_dir, work):
    from pyspark.sql import SparkSession

    jars = ",".join(sorted(glob.glob(os.path.join(jar_dir, "*.jar"))))
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


_THREAD_QID = threading.local()


def _thread_streaming_query_id(_spark):
    """A per-Python-thread streaming query id.

    Spark's ``sparkContext.setLocalProperty`` binds a value to the JVM
    thread the current Py4J call happened to grab from the connection pool,
    which is not stable across a Python thread's own subsequent calls. The
    startup-race scenario needs each racer to see its own query id inside
    the same shared session, so we replace ``silver_stream_delta``'s bound
    ``streaming_query_id`` with a threading.local lookup for the duration
    of the scenario.
    """
    return _THREAD_QID.qid


def _run_startup_race(spark, work):
    """Simulate two concurrent silver_stream_delta writers landing on the same
    empty table. Each thread pretends to be inside a foreachBatch with a
    distinct ``streaming_query_id``, matching the state Spark sets inside
    a real ``foreachBatch``.
    """
    import silver_stream_delta as ss

    ss.streaming_query_id = _thread_streaming_query_id

    dtbl = "spark_catalog.silver.customer_interactions_enriched"
    spark.sql("DROP TABLE IF EXISTS " + dtbl)
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")

    errors: list[str] = []
    written: dict[str, int] = {}

    barrier = threading.Barrier(2)

    def _writer(name, start, qid):
        try:
            _THREAD_QID.qid = qid
            df = bronze_df(spark, 5, start=start)
            barrier.wait(timeout=30)
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
    thread continually issues metadata commits (ALTER TBLPROPERTIES) against
    the same Delta table. Each ALTER lands as its own log commit, so the
    before/after-version bracket in the old code sees a non-zero delta
    even for a Delta-deduped write. The I8 fix reads the write's own commit
    from history via txnAppId/userMetadata and is immune to these.
    """
    import silver_stream_delta as ss

    ss.streaming_query_id = _thread_streaming_query_id

    dtbl = "spark_catalog.silver.customer_interactions_enriched_vr"
    spark.sql("DROP TABLE IF EXISTS " + dtbl)
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")

    # One fixed stream id for the whole scenario; only txnVersion varies per batch.
    _THREAD_QID.qid = "qvr"

    stop_ev = threading.Event()

    def _churn():
        i = 0
        while not stop_ev.is_set():
            try:
                spark.sql(f"ALTER TABLE {dtbl} SET TBLPROPERTIES ('lb.churn.{i}' = '{i}')")
            except Exception:  # noqa: BLE001 -- table may not exist yet
                pass
            i += 1
            time.sleep(0.02)

    # Seed the table before starting churn so ALTER doesn't fail forever.
    df0 = bronze_df(spark, 5, start=0)
    n0 = ss.write_silver_batch(df0, 0, dtbl, f"file://{work}/")

    ch = threading.Thread(target=_churn, daemon=True)
    ch.start()

    returns: list[int] = [int(n0)]
    row_counts: list[int] = [spark.table(dtbl).count()]
    try:
        for bid in range(1, batches):
            df = bronze_df(spark, 5, start=1000 + bid * 5)
            n = ss.write_silver_batch(df, bid, dtbl, f"file://{work}/")
            returns.append(int(n))
            row_counts.append(int(spark.table(dtbl).count()))
    finally:
        stop_ev.set()
        ch.join(timeout=10)

    return {
        "scenario": "version_race",
        "returns": returns,
        "row_counts": row_counts,
        "batches": batches,
    }


def main():
    if len(sys.argv) < 4:
        print(
            json.dumps(
                {"error": "usage: delta_stream_mech_scenarios.py <scenario> <jar_dir> <work_dir>"}
            )
        )
        sys.exit(2)
    scenario, jar_dir, work = sys.argv[1], sys.argv[2], sys.argv[3]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    # Delta's hive-catalog path is what silver_stream_delta uses in this test.
    os.environ.setdefault("LB_CATALOG_TYPE", "hive")
    spark = _session(jar_dir, work)
    from common import set_utc_session

    set_utc_session(spark)

    if scenario == "startup_race":
        out = _run_startup_race(spark, work)
    elif scenario == "version_race":
        out = _run_version_race(spark, work, batches=10)
    else:
        out = {"error": f"unknown scenario {scenario!r}"}
    spark.stop()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))
    main()
