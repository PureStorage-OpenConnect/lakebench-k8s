"""Shared test helpers moved from tests/test_record_writers.py (imported by several test files)."""

from __future__ import annotations


def _bench_result():
    from lakebench.benchmark.queries import BenchmarkQuery
    from lakebench.benchmark.runner import BenchmarkResult, QueryResult

    q = BenchmarkQuery(
        name="Q1_full_aggregation_scan", display_name="Q1", query_class="scan", sql="SELECT 1"
    )
    return BenchmarkResult(
        mode="power",
        cache="hot",
        scale=1.0,
        queries=[QueryResult(query=q, elapsed_seconds=1.5, rows_returned=3, success=True)],
        total_seconds=1.5,
        qph=2400.0,
        iterations=1,
        streams=1,
        stream_results=[],
        engine="trino",
    )
