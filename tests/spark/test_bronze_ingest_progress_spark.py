"""Executed tests: bronze_ingest_financial logs each micro-batch from a real
StreamingQuery.recentProgress in the collector's format (LB-136), including
across an idle gap, where Spark reports the next batch id with zero rows."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.streaming.noDataProgressEventInterval", "100")
        .getOrCreate()
    )
    yield s
    s.stop()


def _drop_file(spark, n, flat, tag):
    tmp = flat.parent / f"stage-{tag}"
    spark.range(n).coalesce(1).write.parquet(str(tmp))
    for f in tmp.glob("*.parquet"):
        f.rename(flat / f"{tag}-{f.name}")


def _wait(pred, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.2)
    return False


def test_batches_after_an_idle_gap_are_logged(spark, tmp_path, capsys):
    import bronze_ingest_financial as bi

    from lakebench.metrics.collector import MetricsCollector

    flat = tmp_path / "flat"
    flat.mkdir()
    _drop_file(spark, 5, flat, "a")
    q = (
        spark.readStream.format("parquet")
        .schema("id LONG")
        .load(str(flat))
        .writeStream.format("memory")
        .queryName("bronze_idle_t")
        .option("checkpointLocation", str(tmp_path / "ckpt"))
        .trigger(processingTime="500 milliseconds")
        .start()
    )
    try:
        assert _wait(lambda: any((p.get("numInputRows") or 0) > 0 for p in q.recentProgress))
        logged = bi._log_new_progress(q, -1)
        # Let idle triggers happen; they report the next batch id, 0 rows.
        assert _wait(
            lambda: any((p.get("numInputRows") or 0) == 0 for p in q.recentProgress), timeout=30
        )
        logged = bi._log_new_progress(q, logged)
        _drop_file(spark, 7, flat, "b")
        assert _wait(lambda: sum((p.get("numInputRows") or 0) for p in q.recentProgress) >= 12)
        bi._log_new_progress(q, logged)
    finally:
        q.stop()
    out = capsys.readouterr().out
    m = MetricsCollector().parse_streaming_logs(out, "bronze-ingest")
    assert m.total_rows_processed == 12
    assert m.total_batches == 2


def test_ensure_column_adds_only_when_missing(spark, tmp_path):
    from common import ensure_column

    spark.range(2).write.format("parquet").option("path", str(tmp_path / "t")).saveAsTable(
        "ensure_col_t"
    )
    assert ensure_column(spark, "ensure_col_t", "ingest_ts", "TIMESTAMP") is True
    assert "ingest_ts" in spark.table("ensure_col_t").columns
    assert ensure_column(spark, "ensure_col_t", "ingest_ts", "TIMESTAMP") is False
