"""Replay scenarios for the c360 continuous writers, run in a fresh JVM.

Not collected by pytest (no ``test_`` prefix). ``test_c360_stream_replay_spark``
runs it as a subprocess because the Iceberg and Delta jars must be on the
driver classpath at JVM launch, and another test module may already have
started a plain JVM in the pytest process.

Each scenario runs a real Structured Streaming query whose foreachBatch
commits to the table and then raises, the state a driver killed between the
table commit and the checkpoint commit leaves behind. The query is restarted
from the same checkpoint, Spark replays the batch with the same id, and the
scenario reports how many rows the table holds. A naive-append control
proves the harness does produce a replay.

Usage: python c360_stream_scenarios.py <jar_dir> <work_dir>
Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import glob
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


class SimulatedCrash(RuntimeError):
    pass


def bronze_rows(n, day0=datetime(2024, 6, 1)):
    """Bronze-shaped rows: every column silver and gold read."""
    rows = []
    for i in range(n):
        rows.append(
            {
                "id": i,
                "row_id": i,
                "event_timestamp": day0 + timedelta(hours=7 * i),
                "event_id": f"e{i}",
                "session_id": f"s{i // 3}",
                "customer_id": 1 + i % 17,
                "email_raw": f"u{i}@x.com",
                "phone_raw": "5551234567",
                "interaction_type": ["purchase", "browse", "support"][i % 3],
                "product_id": f"p{i % 5}",
                "product_category": "books",
                "transaction_amount": float(i),
                "currency": "USD",
                "channel": ["web", "mobile_app", "store"][i % 3],
                "device_type": "mobile",
                "browser": "chrome",
                "ip_address": "10.0.0.1",
                "city_raw": "NYC",
                "state_raw": "NY",
                "zip_code": "10001",
                "page_views": i % 12,
                "time_on_site_seconds": 60 * (i % 40),
                "support_ticket_id": None,
                "satisfaction_score": 1 + i % 5,
                "utm_source": "google",
                "utm_medium": "cpc",
                "loyalty_member": i % 2 == 0,
                "loyalty_tier": "gold",
                "points_earned": i,
                "points_redeemed": 0,
                # Every 10th row is filtered out by silver.
                "data_quality_flag": "duplicate_suspected" if i % 10 == 9 else "clean",
                "interaction_payload": "ab",
            }
        )
    return rows


BRONZE_DDL = (
    "id bigint, row_id bigint, event_timestamp timestamp, event_id string, "
    "session_id string, customer_id bigint, email_raw string, phone_raw string, "
    "interaction_type string, product_id string, product_category string, "
    "transaction_amount double, currency string, channel string, device_type string, "
    "browser string, ip_address string, city_raw string, state_raw string, "
    "zip_code string, page_views int, time_on_site_seconds int, "
    "support_ticket_id string, satisfaction_score int, utm_source string, "
    "utm_medium string, loyalty_member boolean, loyalty_tier string, "
    "points_earned int, points_redeemed int, data_quality_flag string, "
    "interaction_payload string"
)


def bronze_df(spark, n):
    cols = [c.split()[0] for c in BRONZE_DDL.split(", ")]
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in bronze_rows(n)], BRONZE_DDL)


def session(jar_dir, work):
    from pyspark.sql import SparkSession

    jars = ",".join(sorted(glob.glob(os.path.join(jar_dir, "*.jar"))))
    return (
        SparkSession.builder.master("local[2]")
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
        # The foreachBatch session is a clone with its own catalog instance;
        # a cached table in the driver session would read a stale snapshot.
        .config("spark.sql.catalog.ice.cache-enabled", "false")
        .config("spark.sql.catalog.ice.warehouse", f"file://{work}/ice-wh")
        .config("spark.sql.warehouse.dir", f"{work}/spark-wh")
        .config("spark.sql.session.timeZone", "America/Los_Angeles")
        .getOrCreate()
    )


def stage_source(spark, work, name, n):
    """Write n bronze rows as Parquet files for a file-source stream."""
    src = f"{work}/src-{name}"
    bronze_df(spark, n).repartition(1).write.parquet(src)
    return src


def crash_then_replay(spark, work, name, n, write):
    """Run ``write(df, batch_id)`` under a crash-after-commit, then restart.

    Returns the batch ids foreachBatch saw, in order.
    """
    src = stage_source(spark, work, name, n)
    schema = spark.read.parquet(src).schema
    ckpt = f"{work}/ckpt-{name}"
    seen = []

    def run(crash):
        def fb(df, bid):
            seen.append(int(bid))
            write(df, bid)
            if crash:
                raise SimulatedCrash("driver died after the table commit")

        q = (
            spark.readStream.schema(schema)
            .parquet(src)
            .writeStream.foreachBatch(fb)
            .option("checkpointLocation", ckpt)
            .trigger(availableNow=True)
            .start()
        )
        try:
            q.awaitTermination()
        except Exception as e:  # noqa: BLE001
            if "SimulatedCrash" not in str(e) and "driver died" not in str(e):
                raise
        return q.id

    qid1 = run(crash=True)
    qid2 = run(crash=False)
    return seen, str(qid1), str(qid2)


def main():
    jar_dir, work = sys.argv[1], sys.argv[2]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = session(jar_dir, work)
    from common import set_utc_session

    set_utc_session(spark)
    out = {}
    n = 40
    expected_silver = sum(
        1 for r in bronze_rows(n) if r["data_quality_flag"] != "duplicate_suspected"
    )
    out["n"] = n
    out["expected_silver"] = expected_silver

    # Control: a naive append duplicates the replayed batch.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.ctl")
    spark.sql("CREATE TABLE ice.ctl.t (id BIGINT) USING iceberg")
    seen, _, _ = crash_then_replay(
        spark, work, "control", n, lambda df, bid: df.select("id").writeTo("ice.ctl.t").append()
    )
    out["control_batches"] = seen
    out["control_rows"] = spark.table("ice.ctl.t").count()

    # Iceberg silver stream: _batch_id delete-then-append.
    import silver_stream

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    tbl = "ice.silver.customer_interactions_enriched"
    seen, _, _ = crash_then_replay(
        spark, work, "ice-silver", n, lambda df, bid: silver_stream.write_silver_batch(df, bid, tbl)
    )
    out["ice_silver_batches"] = seen
    out["ice_silver_rows"] = spark.table(tbl).count()
    out["ice_silver_distinct_ids"] = spark.table(tbl).select("id").distinct().count()
    out["ice_silver_batch_ids"] = sorted(
        r[0] for r in spark.table(tbl).select("_batch_id").distinct().collect()
    )
    out["ice_silver_recency_max"] = (
        spark.table(tbl).agg({"customer_recency_score": "max"}).first()[0]
    )

    # Upgrade path: a silver table created without _batch_id (batch mode or
    # an older release) gains it through ensure_column, then accepts batches.
    from common import apply_silver_transformations, ensure_column

    old = "ice.silver.legacy"
    apply_silver_transformations(bronze_df(spark, 5)).writeTo(old).partitionedBy(
        "interaction_date"
    ).create()
    added = ensure_column(spark, old, "_batch_id", "BIGINT")
    again = ensure_column(spark, old, "_batch_id", "BIGINT")
    silver_stream.write_silver_batch(bronze_df(spark, 10), 3, old)
    silver_stream.write_silver_batch(bronze_df(spark, 10), 3, old)
    out["legacy_added"], out["legacy_added_again"] = added, again
    out["legacy_rows"] = spark.table(old).count()
    out["legacy_rows_batch3"] = spark.table(old).where("_batch_id = 3").count()

    # Iceberg bronze ingest: snapshot tag check.
    import bronze_ingest

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.default")
    btbl = "ice.default.bronze_raw"
    seen, q1, q2 = crash_then_replay(
        spark,
        work,
        "ice-bronze",
        n,
        lambda df, bid: bronze_ingest.write_bronze_batch(df, bid, btbl),
    )
    out["ice_bronze_batches"] = seen
    out["ice_bronze_rows"] = spark.table(btbl).count()
    out["ice_bronze_same_query_id"] = q1 == q2
    # A fresh checkpoint is a new query id: its batch 0 must be written, not
    # mistaken for the earlier query's batch 0.
    ckpt_seen, _, _ = crash_then_replay(
        spark,
        work,
        "ice-bronze-fresh",
        10,
        lambda df, bid: bronze_ingest.write_bronze_batch(df, bid, btbl),
    )
    out["ice_bronze_rows_after_fresh"] = spark.table(btbl).count()

    # Delta silver stream: txnAppId/txnVersion.
    import silver_stream_delta

    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")
    dtbl = "spark_catalog.silver.customer_interactions_enriched"
    seen, _, _ = crash_then_replay(
        spark,
        work,
        "delta-silver",
        n,
        lambda df, bid: silver_stream_delta.write_silver_batch(df, bid, dtbl, f"file://{work}/"),
    )
    out["delta_silver_batches"] = seen
    out["delta_silver_rows"] = spark.table(dtbl).count()
    crash_then_replay(
        spark,
        work,
        "delta-silver-fresh",
        10,
        lambda df, bid: silver_stream_delta.write_silver_batch(df, bid, dtbl, f"file://{work}/"),
    )
    out["delta_silver_rows_after_fresh"] = spark.table(dtbl).count()

    # Delta bronze ingest.
    import bronze_ingest_delta

    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.default")
    dbtbl = "spark_catalog.default.bronze_raw"
    seen, _, _ = crash_then_replay(
        spark,
        work,
        "delta-bronze",
        n,
        lambda df, bid: bronze_ingest_delta.write_bronze_batch(df, bid, dbtbl, f"file://{work}/"),
    )
    out["delta_bronze_batches"] = seen
    out["delta_bronze_rows"] = spark.table(dbtbl).count()

    spark.stop()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
