"""I10: ``gold_refresh_financial._pin_silver`` and its fallback path apply
the sealed-batch semi-join.

Continuous mode's per-tick silver read is the highest-frequency exposure
to the mid-batch crash window: every refresh interval calls _pin_silver.
The pinned Iceberg snapshot may include ghost rows from a batch whose
transactions committed but whose versions row never landed; the semi-join
must hide those.

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


def test_pin_silver_and_fallback_hide_ghost_rows():
    jar = _iceberg_jar()
    if jar is None:
        pytest.skip("no iceberg-spark-runtime-4.0 jar available (set LB_TEST_ICEBERG_JAR)")
    res = subprocess.run(
        [sys.executable, __file__, jar], capture_output=True, text=True, timeout=600
    )
    assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-4000:]
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # 2 sealed batches (10 rows) + 1 unsealed batch (5 rows).
    assert out["raw_row_count"] == 15, out
    # Pinned snapshot path (has a current snapshot) semi-joins.
    assert out["pinned_row_count"] == 10, out
    # Fallback path (snapshot lookup returns None) also semi-joins.
    assert out["fallback_row_count"] == 10, out


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

        import gold_refresh_financial as gr

        gr.CATALOG = "lh"
        gr.SILVER_TXNS = "silver.transactions"
        gr.SILVER_BATCH_VERSIONS = "silver.silver_batch_versions"

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
        spark.sql(_TXNS_DDL)
        spark.sql(_VERSIONS_DDL)

        def _rows(stream_id, batch_id):
            return [
                (f"T{stream_id}-{batch_id}-{i}", stream_id, batch_id, datetime(2024, 6, 1))
                for i in range(5)
            ]

        schema = "txn_id STRING, _stream_id STRING, _batch_id BIGINT, ingest_ts TIMESTAMP"
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

        # Pinned-snapshot path.
        txns_pinned, _sid, _rows, _newest = gr._pin_silver(spark)
        pinned_row_count = txns_pinned.count()

        # Force the fallback path by pointing gr at a table that has no
        # snapshots yet: create a fresh copy, seed same rows, but mock
        # _current_snapshot to return None so the fallback branch runs.
        real_current_snapshot = gr._current_snapshot
        gr._current_snapshot = lambda _s, _fq: None
        try:
            txns_fallback, sid_fb, _rows_fb, _newest_fb = gr._pin_silver(spark)
            fallback_row_count = txns_fallback.count()
            assert sid_fb is None, sid_fb
        finally:
            gr._current_snapshot = real_current_snapshot

        out = {
            "raw_row_count": int(raw_row_count),
            "pinned_row_count": int(pinned_row_count),
            "fallback_row_count": int(fallback_row_count),
        }
        print(json.dumps(out))
        spark.stop()


if __name__ == "__main__":
    sys.path[:0] = [str(SCRIPTS)]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    _run(sys.argv[1])
