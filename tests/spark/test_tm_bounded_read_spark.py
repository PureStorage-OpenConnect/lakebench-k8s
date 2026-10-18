"""The TM operations pass reads bounded silver and pins its sealed set.

Two properties of ``tm_operations.run_tm_operations``' pinned read:

- Case activity reads only the span the case windows cover. ``_bound_to_window``
  prunes the payments outside [min window start, max window end); the per-case
  window join is unchanged, so the activity counts are identical to a scan of
  the whole pinned frame.
- The sealed set is pinned at one versions snapshot, so every action in the
  pass (the in-flight watermark, the reconciliation count, the activity scan)
  sees the same sealed rows. A batch that seals mid-pass does not change the
  frame, so reconciliation cannot double-count it. Reading the versions table
  at current state instead lets a mid-pass seal be counted twice.

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
def test_bounded_read_matches_full_and_pin_is_stable(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # Bounding the silver read to the case windows drops the out-of-window
    # payments...
    assert out["bounded_rows"] < out["full_rows"], out
    # ...but produces exactly the same per-case activity as scanning the whole
    # frame: the per-case window join already excluded them.
    assert out["activity_full"] == out["activity_bounded"], out
    # Before the mid-pass seal both filters agree (batch 2 unsealed).
    assert out["pinned_before"] == out["current_before"], out
    # The pinned sealed filter does not see a batch that seals after the pin:
    # its row count is the same before and after the mid-pass seal.
    assert out["pinned_before"] == out["pinned_after"], out
    # The current-state filter does see it on the same transactions snapshot,
    # which is the overshoot the pin removes.
    assert out["current_after"] == out["current_before"] + out["sealed_batch_rows"], out


_TXNS_DDL = """
CREATE TABLE lakehouse.silver.transactions (
    uetr             STRING NOT NULL,
    originator_id    BIGINT,
    beneficiary_id   BIGINT,
    txn_amount_usd   DECIMAL(18,2),
    txn_timestamp    TIMESTAMP,
    ingest_ts        TIMESTAMP,
    _stream_id       STRING,
    _batch_id        BIGINT
) USING iceberg PARTITIONED BY (months(txn_timestamp))
"""

_VERSIONS_DDL = """
CREATE TABLE lakehouse.silver.silver_batch_versions (
    stream_id      STRING NOT NULL,
    batch_id       BIGINT NOT NULL,
    committed_at   TIMESTAMP NOT NULL
) USING iceberg
"""


def _run(jars):
    from datetime import date, datetime
    from decimal import Decimal

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
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "hadoop")
            .config("spark.sql.catalog.lakehouse.cache-enabled", "false")
            .config("spark.sql.catalog.lakehouse.warehouse", f"file://{work}/wh")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        import tm_operations as tm
        from pyspark.sql.functions import col

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
        spark.sql(_TXNS_DDL)
        spark.sql(_VERSIONS_DDL)

        schema = (
            "uetr STRING, originator_id BIGINT, beneficiary_id BIGINT, "
            "txn_amount_usd DECIMAL(18,2), txn_timestamp TIMESTAMP, ingest_ts TIMESTAMP, "
            "_stream_id STRING, _batch_id BIGINT"
        )
        ingest = datetime(2025, 1, 2, 3, 0)

        def _write(uetr, o, b, ts, stream_id, batch_id):
            spark.createDataFrame(
                [(uetr, o, b, Decimal("100.00"), ts, ingest, stream_id, batch_id)], schema
            ).writeTo("lakehouse.silver.transactions").append()

        def _seal(stream_id, batch_id):
            spark.sql(
                "INSERT INTO lakehouse.silver.silver_batch_versions "
                f"VALUES ('{stream_id}', {batch_id}, current_timestamp())"
            )

        # In-window payments (both near 2024-12): batch 0, sealed. One
        # customer (id 1) pays non-customer 2.
        _write("w0", 1, 2, datetime(2024, 12, 10, 12), "S1", 0)
        _write("w1", 2, 1, datetime(2024, 12, 20, 12), "S1", 0)
        _seal("S1", 0)
        # Out-of-window payment far in the past: batch 1, sealed. A case whose
        # window is the 12 months before 2024-12 never reaches 2020.
        _write("old0", 1, 2, datetime(2020, 1, 5, 12), "S1", 1)
        _seal("S1", 1)

        txns = tm.sealed_txns_filter(
            spark,
            tm.read_at_snapshot(
                spark,
                "lakehouse.silver.transactions",
                tm.current_snapshot_id(spark, "lakehouse.silver.transactions"),
            ),
            "lakehouse",
            "silver.silver_batch_versions",
        )
        full_rows = txns.count()

        # One case: customer 1, window [2024-01-01, 2025-01-01). It covers the
        # two 2024-12 payments and not the 2020 payment.
        cases = spark.createDataFrame(
            [("C1", 1, date(2024, 1, 1), date(2025, 1, 1))],
            "case_id STRING, customer_id BIGINT, activity_window_start DATE, "
            "activity_window_end DATE",
        )
        bounds = tm._case_window_bounds(cases)
        bounded = tm._bound_to_window(txns, *bounds)
        bounded_rows = bounded.count()

        def _activity(frame):
            return {
                r["case_id"]: int(r["activity_txn_count"])
                for r in tm.case_activity(frame, cases).collect()
            }

        activity_full = _activity(txns)
        activity_bounded = _activity(bounded)

        # The race: a batch's transactions are in the pinned transactions
        # snapshot but its versions row seals DURING the pass. Write batch 2's
        # rows now (so they are in the snapshot), pin the transactions
        # snapshot and the versions snapshot, then seal batch 2 after the pin.
        _write("w2", 1, 2, datetime(2024, 12, 25, 12), "S1", 2)
        _write("w3", 2, 1, datetime(2024, 12, 26, 12), "S1", 2)
        sealed_batch_rows = 2
        snap = tm.current_snapshot_id(spark, "lakehouse.silver.transactions")
        vsid = tm.current_snapshot_id(spark, "lakehouse.silver.silver_batch_versions")
        pinned = tm.sealed_txns_filter_at(
            spark,
            tm.read_at_snapshot(spark, "lakehouse.silver.transactions", snap),
            "lakehouse",
            "silver.silver_batch_versions",
            vsid,
        )
        current = tm.sealed_txns_filter(
            spark,
            tm.read_at_snapshot(spark, "lakehouse.silver.transactions", snap),
            "lakehouse",
            "silver.silver_batch_versions",
        )
        pinned_before = pinned.count()
        current_before = current.count()
        # Batch 2 seals mid-pass.
        _seal("S1", 2)
        # The pinned frame's sealed set is fixed at vsid: unchanged.
        pinned_after = pinned.count()
        # The current-state frame re-reads the versions table each action, so
        # the same transactions snapshot now shows the just-sealed batch: the
        # overshoot the pin removes.
        current_after = current.count()

        out = {
            "full_rows": full_rows,
            "bounded_rows": bounded_rows,
            "activity_full": activity_full,
            "activity_bounded": activity_bounded,
            "pinned_before": pinned_before,
            "pinned_after": pinned_after,
            "current_before": current_before,
            "current_after": current_after,
            "sealed_batch_rows": sealed_batch_rows,
        }
        assert col is not None
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    _run(sys.argv[1])
