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
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


class SimulatedCrash(RuntimeError):
    pass


def bronze_rows(n, day0=datetime(2024, 6, 1), start=0):
    """Bronze-shaped rows: every column silver and gold read."""
    rows = []
    for i in range(start, start + n):
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


def bronze_df(spark, n, start=0):
    cols = [c.split()[0] for c in BRONZE_DDL.split(", ")]
    rows = bronze_rows(n, start=start)
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], BRONZE_DDL)


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


def stage_files(spark, work, name, files, rows_per_file, start=0):
    """Write ``files`` Parquet files of bronze rows, one micro-batch each."""
    src = f"{work}/src-{name}"
    for k in range(files):
        bronze_df(spark, rows_per_file, start + k * rows_per_file).coalesce(1).write.mode(
            "append"
        ).parquet(src)
        time.sleep(0.05)  # distinct mtimes keep the file source's order stable
    return src


def run_stream(spark, src, ckpt, write, crash_at=None, log=None):
    """One query run over ``src``: one file per micro-batch, until caught up.

    With ``crash_at``, foreachBatch raises right after ``write`` commits that
    batch, the state a driver killed before its checkpoint commit leaves.
    ``log`` collects (batch_id, write's return value). Returns the query id.
    """
    schema = spark.read.parquet(src).schema

    def fb(df, bid):
        ret = write(df, bid)
        if log is not None:
            log.append([int(bid), ret])
        if crash_at is not None and int(bid) == crash_at:
            raise SimulatedCrash("driver died after the table commit")

    q = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 1)
        .parquet(src)
        .writeStream.foreachBatch(fb)
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)
        .start()
    )
    try:
        q.awaitTermination()
    except Exception as e:  # noqa: BLE001
        if "driver died" not in str(e):
            raise
    return str(q.id)


def crash_then_replay(spark, work, name, write, files=3, rows_per_file=10, crash_at=1):
    """Crash after committing batch ``crash_at``, restart from the checkpoint.

    Batch 0 creates the table, so ``crash_at=1`` replays onto an existing
    table. Returns (log of (batch_id, return value), first query id, second).
    """
    src = stage_files(spark, work, name, files, rows_per_file)
    ckpt = f"{work}/ckpt-{name}"
    log = []
    q1 = run_stream(spark, src, ckpt, write, crash_at=crash_at, log=log)
    q2 = run_stream(spark, src, ckpt, write, log=log)
    return log, q1, q2


def _merge_gold_from(script):
    """``_merge_gold`` from a gold script, which runs a job at import."""
    import ast

    from pyspark.sql.functions import col, lit

    path = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts" / script
    tree = ast.parse(path.read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_merge_gold")
    ns = {"col": col, "lit": lit}
    exec(compile(ast.Module([fn], []), str(path), "exec"), ns)  # noqa: S102
    return ns["_merge_gold"]


def gold_merge(spark, script):
    """Five gold days, recompute from day 4 with three days: one commit."""
    from datetime import date

    days = [date(2024, 6, d) for d in range(1, 7)]
    old = spark.createDataFrame([(d, 1) for d in days[:5]], "interaction_date date, v int")
    new = spark.createDataFrame([(d, 2) for d in days[3:]], "interaction_date date, v int")
    return old, new, days, _merge_gold_from(script)


def main():
    jar_dir, work = sys.argv[1], sys.argv[2]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = session(jar_dir, work)
    from common import set_utc_session

    set_utc_session(spark)
    out = {}
    from common import refuse_fresh_checkpoint_over_data

    # 3 files x 10 rows; silver drops id % 10 == 9, so 9 per file survive.
    out["bronze_rows"] = 30
    out["silver_rows"] = 27

    # Control: a naive append duplicates the replayed batch (batch 1).
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.ctl")
    spark.sql("CREATE TABLE ice.ctl.t (id BIGINT) USING iceberg")
    log, _, _ = crash_then_replay(
        spark, work, "control", lambda df, bid: df.select("id").writeTo("ice.ctl.t").append()
    )
    out["control_batches"] = [b for b, _ in log]
    out["control_rows"] = spark.table("ice.ctl.t").count()

    # Iceberg silver stream.
    import silver_stream

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    tbl = "ice.silver.customer_interactions_enriched"
    w = lambda df, bid: silver_stream.write_silver_batch(df, bid, tbl)  # noqa: E731
    log, q1, q2 = crash_then_replay(spark, work, "ice-silver", w)
    t = spark.table(tbl)
    out["ice_silver_batches"] = [b for b, _ in log]
    out["ice_silver_rows"] = t.count()
    out["ice_silver_distinct_ids"] = t.select("id").distinct().count()
    out["ice_silver_same_query"] = q1 == q2
    out["ice_silver_recency_max"] = t.agg({"customer_recency_score": "max"}).first()[0]

    def deletes(table):
        return spark.sql(
            f"SELECT count(*) FROM {table}.snapshots WHERE operation IN ('delete', 'overwrite')"
        ).first()[0]

    out["ice_silver_delete_snapshots"] = deletes(tbl)

    # A fresh checkpoint (new query id, batch ids from 0) over the reused
    # table: three one-row batches. The earlier stream's batch 0 must survive
    # and no batch after the first may commit a delete snapshot.
    src = stage_files(spark, work, "ice-silver-fresh", 3, 1, start=1000)
    ckpt_fresh = f"{work}/ckpt-ice-silver-fresh"
    q3 = run_stream(spark, src, ckpt_fresh, w)
    t = spark.table(tbl)
    out["ice_silver_rows_after_fresh"] = t.count()
    out["ice_silver_old_batch0_rows"] = t.where(f"_stream_id = '{q1}' AND _batch_id = 0").count()
    out["ice_silver_new_stream_rows"] = t.where(f"_stream_id = '{q3}'").count()
    out["ice_silver_delete_snapshots_after_fresh"] = deletes(tbl)

    # Startup guard: an empty checkpoint over a non-empty table is refused;
    # a used checkpoint, or an empty table, is not.
    try:
        refuse_fresh_checkpoint_over_data(spark, f"{work}/ckpt-never-used", tbl)
        out["refuse_fresh"] = False
    except SystemExit:
        out["refuse_fresh"] = True
    refuse_fresh_checkpoint_over_data(spark, ckpt_fresh, tbl)
    spark.sql("CREATE TABLE ice.silver.empty_t (id BIGINT) USING iceberg")
    refuse_fresh_checkpoint_over_data(spark, f"{work}/ckpt-never-used", "ice.silver.empty_t")
    out["refuse_allows_used_or_empty"] = True

    # Upgrade path: a silver table created without the key columns (batch
    # mode or an older release) gains them, then a replay is still exact.
    from common import apply_silver_transformations, ensure_column

    old = "ice.silver.legacy"
    apply_silver_transformations(bronze_df(spark, 5)).writeTo(old).partitionedBy(
        "interaction_date"
    ).create()
    added = [
        ensure_column(spark, old, c, t)
        for c, t in (("_stream_id", "STRING"), ("_batch_id", "BIGINT"))
    ]
    again = ensure_column(spark, old, "_batch_id", "BIGINT")
    wl = lambda df, bid: silver_stream.write_silver_batch(df, bid, old)  # noqa: E731
    crash_then_replay(spark, work, "legacy", wl, files=1, crash_at=0)
    out["legacy_added"], out["legacy_added_again"] = all(added), again
    out["legacy_rows"] = spark.table(old).count()

    # Iceberg bronze ingest: snapshot tag check on the replayed batch 1.
    import bronze_ingest

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.default")
    btbl = "ice.default.bronze_raw"
    wb = lambda df, bid: bronze_ingest.write_bronze_batch(df, bid, btbl)  # noqa: E731
    log, q1, q2 = crash_then_replay(spark, work, "ice-bronze", wb)
    out["ice_bronze_log"] = log
    out["ice_bronze_rows"] = spark.table(btbl).count()
    out["ice_bronze_same_query_id"] = q1 == q2
    src = stage_files(spark, work, "ice-bronze-fresh", 1, 10, start=500)
    run_stream(spark, src, f"{work}/ckpt-ice-bronze-fresh", wb)
    out["ice_bronze_rows_after_fresh"] = spark.table(btbl).count()

    # Delta silver stream: txnAppId/txnVersion, skip detected by version.
    import silver_stream_delta

    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")
    dtbl = "spark_catalog.silver.customer_interactions_enriched"
    wd = lambda df, bid: silver_stream_delta.write_silver_batch(  # noqa: E731
        df, bid, dtbl, f"file://{work}/"
    )
    log, _, _ = crash_then_replay(spark, work, "delta-silver", wd)
    out["delta_silver_log"] = log
    out["delta_silver_rows"] = spark.table(dtbl).count()
    src = stage_files(spark, work, "delta-silver-fresh", 1, 10, start=2000)
    run_stream(spark, src, f"{work}/ckpt-delta-silver-fresh", wd)
    out["delta_silver_rows_after_fresh"] = spark.table(dtbl).count()

    # Delta bronze ingest.
    import bronze_ingest_delta

    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.default")
    dbtbl = "spark_catalog.default.bronze_raw"
    wdb = lambda df, bid: bronze_ingest_delta.write_bronze_batch(  # noqa: E731
        df, bid, dbtbl, f"file://{work}/"
    )
    log, _, _ = crash_then_replay(spark, work, "delta-bronze", wdb)
    out["delta_bronze_log"] = log
    out["delta_bronze_rows"] = spark.table(dbtbl).count()

    # Gold INCREMENTAL boundary replace: one commit, earlier days kept.
    from pyspark.sql.functions import lit

    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.gold")
    g = "ice.gold.dash"
    old_g, new_g, days, merge = gold_merge(spark, "gold_finalize.py")
    old_g.writeTo(g).create()
    before = spark.sql(f"SELECT count(*) FROM {g}.snapshots").first()[0]
    merge(spark.table(g), new_g, days[3]).writeTo(g).overwrite(lit(True))
    out["ice_gold"] = sorted([str(r[0]), r[1]] for r in spark.table(g).collect())
    out["ice_gold_commits"] = spark.sql(f"SELECT count(*) FROM {g}.snapshots").first()[0] - before

    from common import delta_table_version, write_delta_table

    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.gold")
    dg = "spark_catalog.gold.dash"
    old_g, new_g, days, merge = gold_merge(spark, "gold_finalize_delta.py")
    write_delta_table(spark, old_g, dg, f"file://{work}/", mode="append")
    v0 = delta_table_version(spark, dg)
    merged = merge(spark.table(dg), new_g, days[3])
    write_delta_table(spark, merged, dg, f"file://{work}/", mode="overwrite")
    out["delta_gold"] = sorted([str(r[0]), r[1]] for r in spark.table(dg).collect())
    out["delta_gold_commits"] = delta_table_version(spark, dg) - v0

    spark.stop()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
