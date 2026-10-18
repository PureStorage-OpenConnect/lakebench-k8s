"""expire_snapshots must not remove a snapshot a live streaming reader is
positioned on.

Silver reads bronze as an Iceberg streaming source and tracks its position
by snapshot id in its checkpoint offset. A maintenance round that expires
that snapshot kills the stream with "Cannot load current offset at snapshot
... the snapshot was expired or removed". This reproduces the race: a
streaming reader commits an offset, newer snapshots then age the referenced
one past the retention horizon, and expiry at that horizon would remove it.
With the stream's committed snapshot read back (``streaming_source_protect_from``)
and passed as the expiry floor (``build_maintenance_sql(protect_from=...)``),
the referenced snapshot survives.

Runs in a Spark child (``spark_subprocess``) with the Iceberg jar on the
driver classpath at JVM launch.
"""

from __future__ import annotations

import json
import sys
import tempfile

import pytest

pytest.importorskip("pyspark")


@pytest.mark.requires_jars("iceberg")
def test_expiry_floored_at_live_stream_offset(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # The checkpoint offset the stream committed points at a real source
    # snapshot, discovered from the checkpoint files.
    assert out["committed_sid"] is not None, out
    assert out["committed_sid"] in out["all_source_sids"], out
    # Unprotected expiry at "now" removes the referenced snapshot: the crash.
    assert out["unprotected_removed_referenced"] is True, out
    # Protected expiry (floored at the referenced snapshot's commit time)
    # keeps it, and still expires something older.
    assert out["protected_kept_referenced"] is True, out
    assert out["protected_expired_older"] is True, out


def _run(jars):
    import time
    from datetime import datetime, timezone

    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as work:
        spark = (
            SparkSession.builder.master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jars)
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lh.type", "hadoop")
            .config("spark.sql.catalog.lh.cache-enabled", "false")
            .config("spark.sql.catalog.lh.warehouse", f"file://{work}/wh")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        import common

        from lakebench.modules.table_formats.iceberg.maintenance import build_maintenance_sql

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.db")
        spark.sql("CREATE TABLE lh.db.src (id BIGINT) USING iceberg")

        def _append(n0, n1):
            spark.range(n0, n1).write.format("iceberg").mode("append").saveAsTable("lh.db.src")

        # A few source snapshots; the stream will read through these.
        _append(0, 10)
        _append(10, 20)
        _append(20, 30)

        checkpoint = f"file://{work}/ckpt"
        sink = f"file://{work}/sink"
        q = (
            spark.readStream.format("iceberg")
            .load("lh.db.src")
            .writeStream.format("parquet")
            .option("checkpointLocation", checkpoint)
            .option("path", sink)
            .trigger(availableNow=True)
            .start()
        )
        q.awaitTermination()

        committed_sid = common.committed_source_snapshot_id(spark, checkpoint)
        all_source_sids = [
            int(r["snapshot_id"])
            for r in spark.sql("SELECT snapshot_id FROM lh.db.src.snapshots").collect()
        ]
        protect_from = common.streaming_source_protect_from(spark, checkpoint, "lh.db.src")

        # Newer snapshots land after the stream's committed offset: the
        # referenced snapshot is now behind the newest and would be expired by
        # a cutoff at "now".
        _append(30, 40)
        _append(40, 50)

        def _sids():
            return {
                int(r["snapshot_id"])
                for r in spark.sql("SELECT snapshot_id FROM lh.db.src.snapshots").collect()
            }

        def _run_expire(sql_list):
            for stmt in sql_list:
                spark.sql(stmt)

        # A short sleep so a cutoff at "now" is strictly after the latest
        # commit; then expiry at a tiny retention removes everything older
        # than now, including the stream's referenced snapshot: the crash.
        time.sleep(1.0)
        now = datetime.now(timezone.utc)
        unprotected_sql = build_maintenance_sql(
            "spark-thrift", "lh", "db.src", "1s", orphan_retention="48h", now=now
        )
        # Only the expire_snapshots statement (skip orphan removal in the test).
        _run_expire([s for s in unprotected_sql if "expire_snapshots" in s])
        after_unprotected = _sids()
        unprotected_removed_referenced = committed_sid not in after_unprotected

        # Rebuild the source to test the protected path. Commit one snapshot
        # that is genuinely older than the stream's offset (a two-second gap),
        # so there is something strictly older than the protection floor for
        # expiry to remove. ``old_sid`` is captured by id, since Iceberg
        # snapshot ids are random and not ordered by commit time.
        spark.sql("DROP TABLE lh.db.src")
        spark.sql("CREATE TABLE lh.db.src (id BIGINT) USING iceberg")
        _append(0, 10)
        old_sid = int(
            spark.sql(
                "SELECT snapshot_id FROM lh.db.src.snapshots ORDER BY committed_at DESC LIMIT 1"
            ).collect()[0]["snapshot_id"]
        )
        time.sleep(2.0)
        # The snapshots the stream reads through land after the gap.
        _append(10, 20)
        _append(20, 30)
        checkpoint2 = f"file://{work}/ckpt2"
        sink2 = f"file://{work}/sink2"
        q2 = (
            spark.readStream.format("iceberg")
            .load("lh.db.src")
            .writeStream.format("parquet")
            .option("checkpointLocation", checkpoint2)
            .option("path", sink2)
            .trigger(availableNow=True)
            .start()
        )
        q2.awaitTermination()
        committed_sid2 = common.committed_source_snapshot_id(spark, checkpoint2)
        protect_from2 = common.streaming_source_protect_from(spark, checkpoint2, "lh.db.src")
        # More snapshots land after the offset, so the referenced one is behind
        # the newest and a cutoff at "now" would reach it without protection.
        _append(30, 40)
        _append(40, 50)
        time.sleep(1.0)
        now2 = datetime.now(timezone.utc)
        protected_sql = build_maintenance_sql(
            "spark-thrift",
            "lh",
            "db.src",
            "1s",
            orphan_retention="48h",
            now=now2,
            protect_from=protect_from2,
        )
        _run_expire([s for s in protected_sql if "expire_snapshots" in s])
        after_protected = {
            int(r["snapshot_id"])
            for r in spark.sql("SELECT snapshot_id FROM lh.db.src.snapshots").collect()
        }
        protected_kept_referenced = committed_sid2 in after_protected
        # The genuinely-older snapshot (committed two seconds before the
        # stream's offset, so strictly before the floor) is still expired:
        # protection keeps the referenced snapshot and newer, not everything.
        protected_expired_older = old_sid not in after_protected

        out = {
            "committed_sid": committed_sid,
            "all_source_sids": all_source_sids,
            "protect_from": protect_from.isoformat() if protect_from else None,
            "unprotected_removed_referenced": bool(unprotected_removed_referenced),
            "protected_kept_referenced": bool(protected_kept_referenced),
            "protected_expired_older": bool(protected_expired_older),
            "old_sid": old_sid,
            "committed_sid2": committed_sid2,
        }
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    _run(sys.argv[1])
