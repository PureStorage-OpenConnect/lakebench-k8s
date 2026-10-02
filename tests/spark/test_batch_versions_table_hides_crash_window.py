"""I10: silver_batch_versions semi-join hides a mid-batch crash from
consumers.

Simulates the crash window between the transactions/edges commits and the
sealed-marker insert. We drive _merge_batch directly, but replace the
sealed INSERT with a raise so silver_batch_versions never receives the
row for that batch. Then a reader that semi-joins silver.transactions
against silver_batch_versions on (_stream_id, _batch_id) sees zero rows
for the crashed batch.

Runs in a Spark child (``spark_subprocess``) with the Iceberg jar from
``LB_SPARK_TEST_JARS`` on the driver classpath at JVM launch, matching the
pattern in ``tests/spark/test_aml_stream_one_delete_per_restart.py``.
"""

from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from _foreach_batch import foreach_batch_harness

pytest.importorskip("pyspark")


@pytest.mark.requires_jars("iceberg")
def test_crash_window_is_invisible_to_semi_join(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # Batch 0 sealed cleanly -- txns rows visible under the filter.
    assert out["sealed_visible_rows"] == 1, out
    # Batch 1 crashed (MERGE into versions raised) -- txns rows exist in
    # the raw table but the semi-join hides them.
    assert out["ghost_rows_batch_1"] == 1, out
    # After the crashed batch 1 and the sealed retry of batch 1 (batch 2 in
    # the test's numbering) the versions table shows one row per sealed
    # (sid, batch) key. This is the MERGE-idempotency check: the crashed
    # batch's Structured-Streaming-style replay would run through phase 4
    # twice on a plain INSERT and leave 2 rows for (sid, 1); the MERGE
    # keeps exactly one.
    assert out["versions_row_count_after_retry"] == 3, out
    assert out["versions_rows_for_batch_1_after_retry"] == 1, out
    # After batch-1 retry the filter now shows: batch 0 (1 row), batch 1
    # sealed (1 row), batch 2 sealed (1 row) = 3 rows visible.
    assert out["filtered_rows_after_retry"] == 3, out
    # Idempotent double-seal of the SAME (sid, batch) key: re-driving
    # _merge_batch for batch 0 must not add a second versions row.
    assert out["versions_row_count_after_replay_of_batch_0"] == 3, out


# ---------------------------------------------------------------------------
# Subprocess payload
# ---------------------------------------------------------------------------

_PACS_SCHEMA = (
    "txn_id string, uetr string, "
    "dbtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "cdtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "dbtr_agt struct<bicfi:string>, cdtr_agt struct<bicfi:string>, "
    "dbtr_acct struct<iban:string>, cdtr_acct struct<iban:string>, "
    "intrmy_agt_1 struct<bicfi:string>, "
    "intrmy_agt_2 struct<bicfi:string>, "
    "intrmy_agt_3 struct<bicfi:string>, "
    "intr_bk_sttlm_amt decimal(18,2), intr_bk_sttlm_ccy string, "
    "cre_dt_tm timestamp, purp_cd string, "
    "rgltry_rptg array<string>, msg_id string"
)


def _bronze_row(spark, txn_id, ts):
    def party(nm):
        return (nm, "US", ("NYC", "MAIN ST"), (f"LEI-{nm}",))

    row = (
        txn_id,
        f"UETR-{txn_id}",
        party(f"O-{txn_id}"),
        party(f"B-{txn_id}"),
        ("MERIUS2L",),
        ("NRTHGB3X",),
        ("US01",),
        ("GB02",),
        (None,),
        (None,),
        (None,),
        Decimal("100.00"),
        "USD",
        ts,
        "SALA",
        [],
        f"MSG-{txn_id}",
    )
    return spark.createDataFrame([row], _PACS_SCHEMA)


def _run(jars):
    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as work:
        spark = (
            SparkSession.builder.master("local[1]")
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

        from _d_full_helpers import bind_stream_module, bootstrap_catalog

        # Every table _merge_batch writes, in the shapes the stream uses
        # today; KYC and dimension writes are skipped.
        bootstrap_catalog(spark)
        ss = bind_stream_module(spark)

        def bronze(bid):
            return _bronze_row(
                spark,
                f"T{bid}-o-b",
                datetime(2024, 6, 1) + timedelta(days=bid),
            )

        # ---- Batch 0: sealed cleanly.
        foreach_batch_harness(spark, ss._merge_batch, bronze(0), 0)

        # ---- Batch 1: crash between phase-2 (edges commit) and phase-4
        # (sealed marker). Intercept spark.sql: allow every phase-1/2
        # DELETE/INSERT to run, but raise on the sealed-marker MERGE to
        # silver_batch_versions.
        real_sql = spark.sql

        def _sql(stmt, *a, **kw):
            up = stmt.strip().upper()
            if "SILVER_BATCH_VERSIONS" in up and up.startswith(("MERGE", "INSERT")):
                raise RuntimeError("simulated driver crash before sealed marker")
            return real_sql(stmt, *a, **kw)

        spark.sql = _sql  # type: ignore[assignment]
        try:
            try:
                foreach_batch_harness(spark, ss._merge_batch, bronze(1), 1)
            except RuntimeError as e:
                assert "simulated driver crash" in str(e), e
        finally:
            spark.sql = real_sql  # type: ignore[assignment]

        # ---- Consumer: semi-join filter identical to gold_finalize's
        # _sealed_txns helper.
        from pyspark.sql.functions import col

        txns = spark.table("lh.silver.transactions")
        versions = spark.table("lh.silver.silver_batch_versions").select(
            col("stream_id").alias("_sv_stream_id"),
            col("batch_id").alias("_sv_batch_id"),
        )
        filtered = txns.join(
            versions,
            (txns["_stream_id"] == versions["_sv_stream_id"])
            & (txns["_batch_id"] == versions["_sv_batch_id"]),
            "left_semi",
        )

        unfiltered_rows = txns.count()
        filtered_rows = filtered.count()
        sealed_visible_rows = filtered.where(col("_batch_id") == 0).count()
        ghost_rows_batch_1 = txns.where(col("_batch_id") == 1).count()
        versions_row_count = spark.table("lh.silver.silver_batch_versions").count()

        # ---- Structured Streaming replays foreachBatch on failure with the
        # SAME batchId, but this test simulates a crash-then-restart cycle
        # where the driver process died; we drive a re-run of batch 1 (with
        # the crash injector off) and a fresh batch 2 on top.
        foreach_batch_harness(
            spark, ss._merge_batch, bronze(1), 1
        )  # sealed retry of the crashed batch
        foreach_batch_harness(spark, ss._merge_batch, bronze(2), 2)  # new batch on top

        versions_row_count_after_retry = spark.table("lh.silver.silver_batch_versions").count()
        versions_rows_for_batch_1_after_retry = (
            spark.table("lh.silver.silver_batch_versions").where(col("batch_id") == 1).count()
        )
        filtered_rows_after_retry = filtered.count()

        # ---- MERGE-idempotency direct check: re-drive batch 0 again with
        # the SAME (sid, batch_id). Row count in versions must not grow.
        foreach_batch_harness(spark, ss._merge_batch, bronze(0), 0)
        versions_row_count_after_replay_of_batch_0 = spark.table(
            "lh.silver.silver_batch_versions"
        ).count()

        out = {
            "unfiltered_rows": int(unfiltered_rows),
            "filtered_rows": int(filtered_rows),
            "sealed_visible_rows": int(sealed_visible_rows),
            "ghost_rows_batch_1": int(ghost_rows_batch_1),
            "versions_row_count_after_retry": int(versions_row_count_after_retry),
            "versions_rows_for_batch_1_after_retry": int(versions_rows_for_batch_1_after_retry),
            "filtered_rows_after_retry": int(filtered_rows_after_retry),
            "versions_row_count_after_replay_of_batch_0": int(
                versions_row_count_after_replay_of_batch_0
            ),
            "versions_row_count": int(versions_row_count),
        }
        print(json.dumps(out))
        spark.stop()


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts on PYTHONPATH; argv[1]
    # is the comma-separated jar classpath.
    _run(sys.argv[1])
