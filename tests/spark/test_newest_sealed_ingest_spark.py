"""``gold_refresh_financial`` reports the newest ingest_ts over the SEALED
rows detection saw, not over the whole pinned snapshot.

ingest_ts is the bronze file-landing time carried into silver; a later
micro-batch can carry an older ingest_ts than an earlier one, and the
in-flight unsealed batch can carry a NEWER ingest_ts than every sealed row.
The freshness metric must name a row detection processed, so it is the true
max over the sealed set, whatever the unsealed tail carries. The probe
computes that value from the snapshot's file-metadata max and the small
unsealed tail without a full scan of the sealed set, and this test pins that
the value is identical to a direct max over the sealed frame in both the
common case (newest row sealed) and the case the cheap path cannot answer
(newest row in the unsealed tail).

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
def test_newest_sealed_ingest_ignores_unsealed_tail(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # Case A: the newest ingest_ts is in a sealed batch, so the sealed max is
    # the snapshot max; the probe returns it, and _pin_silver reports it.
    assert out["a_newest"] == out["a_sealed_direct"], out
    assert out["a_newest"] == out["a_snapshot_max"], out
    assert out["a_pin_newest"] == out["a_newest"], out
    # Case B: the unsealed in-flight batch carries a strictly newer ingest_ts
    # than any sealed row. The probe must still report the sealed max, never
    # the snapshot/unsealed max, or freshness names a row detection did not
    # process. This is the path the cheap bounds cannot answer, so it also
    # proves the exact fallback is taken.
    assert out["b_newest"] == out["b_sealed_direct"], out
    assert out["b_newest"] < out["b_snapshot_max"], out
    assert out["b_pin_newest"] == out["b_newest"], out


_TXNS_DDL = """
CREATE TABLE lh.silver.transactions (
    txn_id           STRING NOT NULL,
    _stream_id       STRING,
    _batch_id        BIGINT,
    ingest_ts        TIMESTAMP
) USING iceberg
"""

_VERSIONS_DDL = """
CREATE TABLE lh.silver.silver_batch_versions (
    stream_id      STRING NOT NULL,
    batch_id       BIGINT NOT NULL,
    committed_at   TIMESTAMP NOT NULL
) USING iceberg
"""


def _run(jars):
    from datetime import datetime

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

        import gold_refresh_financial as gr

        gr.CATALOG = "lh"
        gr.SILVER_TXNS = "silver.transactions"
        gr.SILVER_BATCH_VERSIONS = "silver.silver_batch_versions"

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
        spark.sql(_TXNS_DDL)
        spark.sql(_VERSIONS_DDL)

        schema = "txn_id STRING, _stream_id STRING, _batch_id BIGINT, ingest_ts TIMESTAMP"

        def _write(stream_id, batch_id, ingest):
            rows = [(f"T{stream_id}-{batch_id}-{i}", stream_id, batch_id, ingest) for i in range(3)]
            spark.createDataFrame(rows, schema).writeTo("lh.silver.transactions").append()

        def _seal(stream_id, batch_id):
            spark.sql(
                "INSERT INTO lh.silver.silver_batch_versions "
                f"VALUES ('{stream_id}', {batch_id}, current_timestamp())"
            )

        # Batch 0 sealed (older landing). Batch 1 sealed but lands LATER than
        # batch 0 and later than the unsealed batch 2: a later batch carrying
        # a newer ingest_ts, and it is the newest sealed row.
        b0 = datetime(2024, 6, 1, 0, 0, 0)
        b1 = datetime(2024, 6, 1, 9, 0, 0)  # newest sealed
        _write("S1", 0, b0)
        _seal("S1", 0)
        # Case A: only batch 0 and batch 1 exist, both sealed, no unsealed
        # tail yet -> newest sealed is batch 1.
        _write("S1", 1, b1)
        _seal("S1", 1)

        from pyspark.sql.functions import max as _max
        from pyspark.sql.functions import unix_micros as _um

        def _direct(df):
            return df.agg(_um(_max("ingest_ts")).alias("t")).collect()[0]["t"] / 1e6

        def _pinned():
            sid = gr._current_snapshot(spark, "lh.silver.transactions")
            return gr.read_at_snapshot(spark, "lh.silver.transactions", sid)

        txns_a, _sid, _vu, _rows, a_pin_newest, _tt = gr._pin_silver(spark)
        pinned_a = _pinned()
        # The probe under test, called directly so the fix's own helper is
        # exercised (and its absence fails this child).
        a_newest = gr._newest_sealed_ingest_epoch_s(spark, pinned_a, txns_a)
        a_sealed_direct = _direct(txns_a)
        a_snapshot_max = _direct(pinned_a)

        # Case B: add the unsealed in-flight batch 2, whose ingest_ts (b2) is
        # newer than batch 0 but older than the newest sealed (b1). Make b2
        # the snapshot-wide newest by bumping it past b1 so snapshot_max comes
        # from an UNSEALED row and the cheap path must not use it.
        b2_newest = datetime(2024, 6, 1, 12, 0, 0)
        _write("S1", 2, b2_newest)  # NOT sealed

        txns_b, _sidb, _vub, _rowsb, b_pin_newest, _ttb = gr._pin_silver(spark)
        pinned_b = _pinned()
        b_newest = gr._newest_sealed_ingest_epoch_s(spark, pinned_b, txns_b)
        b_sealed_direct = _direct(txns_b)
        b_snapshot_max = _direct(pinned_b)

        out = {
            "a_newest": a_newest,
            "a_pin_newest": a_pin_newest,
            "a_sealed_direct": a_sealed_direct,
            "a_snapshot_max": a_snapshot_max,
            "b_newest": b_newest,
            "b_sealed_direct": b_sealed_direct,
            "b_snapshot_max": b_snapshot_max,
            "b_pin_newest": b_pin_newest,
        }
        print(json.dumps(out))
        spark.stop()


if __name__ == "__main__":
    _run(sys.argv[1])
