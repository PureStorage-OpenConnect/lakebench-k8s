"""Executed test: bronze_ingest_financial logs each micro-batch from a real
StreamingQuery.recentProgress in the collector's format (LB-136)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def test_progress_lines_from_a_real_stream(spark, tmp_path, capsys):
    import bronze_ingest_financial as bi

    from lakebench.metrics.collector import MetricsCollector

    src = tmp_path / "src"
    for i, n in enumerate((5, 7)):
        spark.range(n).write.parquet(str(src / f"part-{i}"))
    # Flatten into one directory so maxFilesPerTrigger=1 yields two batches.
    flat = tmp_path / "flat"
    flat.mkdir()
    for i, d in enumerate(sorted(src.iterdir())):
        for f in d.glob("*.parquet"):
            f.rename(flat / f"f{i}-{f.name}")

    df = (
        spark.readStream.format("parquet")
        .schema("id LONG")
        .option("maxFilesPerTrigger", 1)
        .load(str(flat))
    )
    q = (
        df.writeStream.format("memory")
        .queryName("bronze_progress_t")
        .option("checkpointLocation", str(tmp_path / "ckpt"))
        .start()
    )
    q.processAllAvailable()
    last = bi._log_new_progress(q, -1)
    again = bi._log_new_progress(q, last)
    q.stop()
    out = capsys.readouterr().out

    assert again == last, "a batch must be logged once"
    m = MetricsCollector().parse_streaming_logs(out, "bronze-ingest")
    assert m.total_rows_processed == 12
    assert m.total_batches == 2
