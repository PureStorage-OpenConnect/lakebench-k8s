"""I10: tm_operations.run_tm_operations semi-joins silver.transactions
against silver_batch_versions before any P10 monitoring stage runs.

The P10 monitoring layer (stages 1, 6, 7, 8) reads silver.transactions
at a pinned snapshot via ``read_at_snapshot``. Without a filter, a batch
whose transactions committed but whose versions row never landed would
appear in the alert-dispositions and cases layers as a "real" batch,
contaminating triage counts and case_status derivation.

This test drives ``sealed_txns_filter`` (the shared helper tm_operations
uses) against a table seeded with a mix of sealed and unsealed batches,
via a pinned Iceberg snapshot, and asserts the unsealed rows are hidden.

Runs in a child process because Iceberg jars must be on the driver
classpath at JVM launch.
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parents[1] / "src/lakebench/spark/scripts"


def _iceberg_jar() -> str | None:
    env = os.environ.get("LB_TEST_ICEBERG_JAR")
    if env and Path(env).exists():
        return env
    hits = sorted(
        glob.glob(str(Path.home() / ".lakebench/local/*/ivy/cache/org.apache.iceberg/*/jars/*.jar"))
        + glob.glob(str(Path.home() / ".ivy2*/cache/org.apache.iceberg/*/jars/*.jar"))
    )
    return next((h for h in hits if "spark-runtime-4.0" in h), None)


def test_pinned_snapshot_read_hides_unsealed_batches():
    jar = _iceberg_jar()
    if jar is None:
        pytest.skip("no iceberg-spark-runtime-4.0 jar available (set LB_TEST_ICEBERG_JAR)")
    res = subprocess.run(
        [sys.executable, __file__, jar], capture_output=True, text=True, timeout=600
    )
    assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-4000:]
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # 2 sealed batches (10 rows) + 1 unsealed batch (5 rows) at a pinned
    # snapshot.
    assert out["raw_row_count"] == 15, out
    assert out["pinned_raw_row_count"] == 15, out
    assert out["pinned_filtered_row_count"] == 10, out


_TXNS_DDL = """
CREATE TABLE lh.silver.transactions (
    txn_id           STRING NOT NULL,
    _stream_id       STRING,
    _batch_id        BIGINT,
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


def _run(jar):
    from datetime import datetime

    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as work:
        spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jar)
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

        import tm_operations as tm
        from common import sealed_txns_filter

        tm.CATALOG = "lh"
        tm.SILVER_TXNS = "silver.transactions"
        tm.SILVER_BATCH_VERSIONS = "silver.silver_batch_versions"

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
        spark.sql(_TXNS_DDL)
        spark.sql(_VERSIONS_DDL)

        def _rows(stream_id, batch_id):
            return [
                (f"T{stream_id}-{batch_id}-{i}", stream_id, batch_id, datetime(2024, 6, 1))
                for i in range(5)
            ]

        schema = "txn_id STRING, _stream_id STRING, _batch_id BIGINT, txn_timestamp TIMESTAMP"
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

        # Pin at the current snapshot (mirrors tm_operations' read path).
        pinned_sid = tm.current_snapshot_id(spark, "lh.silver.transactions")
        assert pinned_sid is not None
        pinned_raw = tm.read_at_snapshot(spark, "lh.silver.transactions", pinned_sid)
        pinned_raw_row_count = pinned_raw.count()

        pinned_filtered = sealed_txns_filter(
            spark, pinned_raw, "lh", "silver.silver_batch_versions"
        )
        pinned_filtered_row_count = pinned_filtered.count()

        out = {
            "raw_row_count": int(raw_row_count),
            "pinned_raw_row_count": int(pinned_raw_row_count),
            "pinned_filtered_row_count": int(pinned_filtered_row_count),
        }
        print(json.dumps(out))
        spark.stop()


if __name__ == "__main__":
    sys.path[:0] = [str(SCRIPTS)]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    _run(sys.argv[1])
