"""Tests for the Trino query benchmark module."""

from dataclasses import asdict
from datetime import datetime, timedelta

import pytest

from lakebench.metrics import BenchmarkMetrics, MetricsStorage, PipelineMetrics

_QUERY = {
    "name": "Q1_full_aggregation_scan",
    "class": "scan",
    "elapsed_seconds": 2.92,
    "rows_returned": 1,
    "success": True,
}


@pytest.mark.parametrize(
    "benchmark",
    [
        BenchmarkMetrics(
            mode="extended",
            cache="cold",
            scale=50,
            qph=1234.5,
            total_seconds=29.2,
            queries=[_QUERY],
            iterations=5,
        ),
        BenchmarkMetrics(
            mode="throughput",
            cache="hot",
            scale=100,
            qph=2880.0,
            total_seconds=50.0,
            queries=[_QUERY],
            iterations=1,
            streams=4,
            stream_results=[
                {"stream_id": i, "total_seconds": t, "success": True, "queries": []}
                for i, t in enumerate((48.0, 49.0, 50.0, 47.5))
            ],
        ),
    ],
    ids=["extended", "throughput_streams"],
)
def test_benchmark_metrics_roundtrip(tmp_path, benchmark):
    """BenchmarkMetrics survive MetricsStorage save and load unchanged."""
    storage = MetricsStorage(tmp_path / "metrics")
    now = datetime.now()
    pm = PipelineMetrics(
        run_id="roundtrip-bench",
        deployment_name="test",
        start_time=now,
        end_time=now + timedelta(seconds=300),
        total_elapsed_seconds=300.0,
        success=True,
        benchmark=benchmark,
    )

    storage.save_run(pm)
    loaded = storage.load_run("roundtrip-bench")

    assert loaded is not None and loaded.benchmark is not None
    assert asdict(loaded.benchmark) == asdict(benchmark)


def test_adapt_query_iceberg_scan():
    from lakebench.benchmark.executor import DuckDBExecutor

    executor = DuckDBExecutor(
        namespace="test-ns",
        catalog_name="lakehouse",
        table_format="iceberg",
        s3_buckets={"silver": "lb-silver"},
        table_names={"silver": "silver_ns.my_table"},
    )
    adapted = executor.adapt_query("SELECT * FROM lakehouse.silver_ns.my_table")
    assert adapted == (
        "SELECT * FROM iceberg_scan("
        "'s3://lb-silver/warehouse/silver_ns.db/my_table', allow_moved_paths := true)"
    )
