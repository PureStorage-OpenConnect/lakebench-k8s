"""I10: ``gold_finalize_financial._sealed_txns`` returns zero rows for a
batch whose (_stream_id, _batch_id) has no matching row in
``silver_batch_versions``.

Direct test of the filter helper the finalize main() applies before
passing txns to the detection rules and the baseline builder. If the
sidecar table has no row for a given (sid, bid), every txn tagged with
that key must be invisible to the semi-join.

Also asserts the fallback: when silver_batch_versions is absent (a legacy
catalog that predates I10) the helper returns the unfiltered table
rather than raising; the log records that operators should redeploy /
restart silver to bootstrap the sidecar.

Runs in a Spark child (``spark_subprocess``) with the Iceberg jar from
``LB_SPARK_TEST_JARS`` on the driver classpath at JVM launch.
"""

from __future__ import annotations

import json
import sys
import tempfile

import pytest

pytest.importorskip("pyspark")


@pytest.mark.requires_jars("iceberg")
def test_sealed_txns_hides_unsealed_batches(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # 2 sealed batches (10 rows total) + 1 unsealed batch (5 rows) written raw.
    assert out["raw_row_count"] == 15, out
    # Semi-join keeps only sealed batches.
    assert out["sealed_row_count"] == 10, out
    # Legacy catalog fallback: silver_batch_versions dropped, filter passes
    # through with an ``[i10] ... not readable`` log line.
    assert out["fallback_row_count"] == 15, out


_TXNS_DDL = """
CREATE TABLE lh.silver.transactions (
    txn_id           STRING NOT NULL,
    _stream_id       STRING,
    _batch_id        BIGINT,
    txn_amount       DECIMAL(18, 2) NOT NULL,
    txn_currency     STRING NOT NULL,
    txn_timestamp    TIMESTAMP NOT NULL
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
    from decimal import Decimal

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

        import gold_finalize_financial as gf

        gf.CATALOG = "lh"
        gf.SILVER_TXNS = "silver.transactions"
        gf.SILVER_BATCH_VERSIONS = "silver.silver_batch_versions"

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
        spark.sql(_TXNS_DDL)
        spark.sql(_VERSIONS_DDL)

        # Seed 3 batches of 5 txns each with distinct (_stream_id, _batch_id).
        def _rows(stream_id, batch_id):
            return [
                (
                    f"T{stream_id}-{batch_id}-{i}",
                    stream_id,
                    batch_id,
                    Decimal("100.00"),
                    "USD",
                    datetime(2024, 6, 1),
                )
                for i in range(5)
            ]

        schema = "txn_id STRING, _stream_id STRING, _batch_id BIGINT, txn_amount DECIMAL(18,2), txn_currency STRING, txn_timestamp TIMESTAMP"
        # Sealed batches: ('S1', 0), ('S1', 1). Unsealed batch: ('S1', 2).
        for sid, bid in (("S1", 0), ("S1", 1), ("S1", 2)):
            spark.createDataFrame(_rows(sid, bid), schema).writeTo(
                "lh.silver.transactions"
            ).append()

        # Only the first two batches are sealed.
        spark.sql(
            "INSERT INTO lh.silver.silver_batch_versions VALUES "
            "('S1', 0, current_timestamp()), ('S1', 1, current_timestamp())"
        )

        raw_row_count = spark.table("lh.silver.transactions").count()
        sealed_row_count = gf._sealed_txns(spark, "silver.transactions").count()

        # Fallback: drop the sidecar, assert the helper returns unfiltered.
        spark.sql("DROP TABLE lh.silver.silver_batch_versions")
        fallback_row_count = gf._sealed_txns(spark, "silver.transactions").count()

        out = {
            "raw_row_count": int(raw_row_count),
            "sealed_row_count": int(sealed_row_count),
            "fallback_row_count": int(fallback_row_count),
        }
        print(json.dumps(out))
        spark.stop()


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts on PYTHONPATH; argv[1]
    # is the comma-separated jar classpath.
    _run(sys.argv[1])
