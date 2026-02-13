"""Functional tests that exercise the full BenchmarkRunner flow with a mock executor.

These tests patch ``get_executor`` so that no real Kubernetes / Trino / Spark
infrastructure is required, while still running the full ``BenchmarkRunner``
code paths end-to-end.
"""

from __future__ import annotations

import math
from unittest.mock import MagicMock, patch

import pytest

from lakebench.benchmark.executor import QueryExecutorResult
from lakebench.benchmark.queries import BENCHMARK_QUERIES
from lakebench.benchmark.runner import BenchmarkRunner
from tests.conftest import make_config

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_executor():
    """A mock QueryExecutor that returns a successful result for every query."""
    executor = MagicMock()
    executor.engine_name.return_value = "trino"
    executor.catalog_name = "lakehouse"
    executor.adapt_query.side_effect = lambda sql: sql  # identity
    executor.execute_query.return_value = QueryExecutorResult(
        sql="SELECT 1",
        engine="trino",
        duration_seconds=0.5,
        rows_returned=10,
        raw_output="...",
    )
    return executor


@pytest.fixture
def config():
    """A default config suitable for benchmark runner tests."""
    return make_config()


@pytest.fixture
def runner(config, mock_executor):
    """A BenchmarkRunner with the real executor replaced by mock_executor."""
    with patch(
        "lakebench.benchmark.executor.get_executor",
        return_value=mock_executor,
    ):
        return BenchmarkRunner(config)


# ---------------------------------------------------------------------------
# 1. Power Mode
# ---------------------------------------------------------------------------


class TestBenchmarkRunnerPowerMode:
    """Full power-mode flow through BenchmarkRunner."""

    def test_power_runs_all_8_queries(self, runner, mock_executor):
        result = runner.run_power()

        assert len(result.queries) == 8
        assert result.mode == "power"
        assert mock_executor.execute_query.call_count == 8

    def test_power_qph_calculation(self, runner, mock_executor):
        """QpH should equal (num_queries / total_seconds) * 3600."""
        mock_executor.execute_query.return_value = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=2.0,
            rows_returned=10,
            raw_output="...",
        )

        result = runner.run_power()

        # 8 queries * 2s each = 16s total
        assert result.total_seconds == pytest.approx(16.0)
        expected_qph = (8 / 16.0) * 3600  # 1800.0
        assert result.qph == pytest.approx(expected_qph)

    def test_power_cold_cache_calls_flush(self, runner, mock_executor):
        """Running with cache='cold' must call flush_cache before each query."""
        runner.run_power(cache="cold")

        assert mock_executor.flush_cache.call_count == 8

    def test_power_hot_cache_skips_flush(self, runner, mock_executor):
        """Running with cache='hot' must NOT call flush_cache."""
        runner.run_power(cache="hot")

        mock_executor.flush_cache.assert_not_called()

    def test_power_query_failure_still_completes(self, runner, mock_executor):
        """A single query failure should not abort the run."""
        success_result = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=0.5,
            rows_returned=10,
            raw_output="...",
        )
        failure_result = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=1.0,
            rows_returned=0,
            raw_output="",
            error="Table not found",
        )

        # First call fails, rest succeed
        mock_executor.execute_query.side_effect = [
            failure_result,
            *[success_result] * 7,
        ]

        result = runner.run_power()

        assert len(result.queries) == 8
        assert result.queries[0].success is False
        assert result.queries[0].error_message == "Table not found"
        assert all(q.success for q in result.queries[1:])

    def test_power_category_qph(self, runner, mock_executor):
        """compute_category_qph() should return a per-class breakdown."""
        mock_executor.execute_query.return_value = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=1.0,
            rows_returned=10,
            raw_output="...",
        )

        result = runner.run_power()
        cat_qph = result.compute_category_qph()

        # The 8 queries span 5 classes
        expected_classes = {"scan", "filter_prune", "aggregation", "analytics", "operational"}
        assert set(cat_qph.keys()) == expected_classes

        # scan: 1 query @ 1s -> (1/1)*3600 = 3600
        assert cat_qph["scan"] == pytest.approx(3600.0)

        # filter_prune: 2 queries @ 1s each -> (2/2)*3600 = 3600
        assert cat_qph["filter_prune"] == pytest.approx(3600.0)

        # aggregation: 2 queries @ 1s each -> (2/2)*3600 = 3600
        assert cat_qph["aggregation"] == pytest.approx(3600.0)

        # analytics: 2 queries @ 1s each -> (2/2)*3600 = 3600
        assert cat_qph["analytics"] == pytest.approx(3600.0)

        # operational: 1 query @ 1s -> (1/1)*3600 = 3600
        assert cat_qph["operational"] == pytest.approx(3600.0)


# ---------------------------------------------------------------------------
# 2. Throughput Mode
# ---------------------------------------------------------------------------


class TestBenchmarkRunnerThroughputMode:
    """Concurrent-stream throughput tests."""

    def test_throughput_4_streams(self, runner, mock_executor):
        result = runner.run_throughput(streams=4)

        assert result.mode == "throughput"
        assert result.streams == 4
        assert len(result.stream_results) == 4

    def test_throughput_total_queries(self, runner, mock_executor):
        """4 streams * 8 queries = 32 total execute_query calls."""
        runner.run_throughput(streams=4)

        assert mock_executor.execute_query.call_count == 32

    def test_throughput_qph_formula(self, runner, mock_executor):
        """QpH = (streams * num_queries / wall_clock_seconds) * 3600."""
        result = runner.run_throughput(streams=4)

        # Wall clock is measured live, so just verify the formula is consistent
        total_queries = sum(len(s.queries) for s in result.stream_results)
        assert total_queries == 32
        if result.total_seconds > 0:
            expected_qph = (total_queries / result.total_seconds) * 3600
            assert result.qph == pytest.approx(expected_qph, rel=1e-2)

    def test_throughput_each_stream_has_all_queries(self, runner, mock_executor):
        result = runner.run_throughput(streams=4)

        for stream in result.stream_results:
            assert len(stream.queries) == 8


# ---------------------------------------------------------------------------
# 3. Composite Mode
# ---------------------------------------------------------------------------


class TestBenchmarkRunnerCompositeMode:
    """Power + throughput composite tests."""

    def test_composite_returns_three_results(self, runner, mock_executor):
        power, throughput, composite = runner.run_composite(streams=2)

        assert power.mode == "power"
        assert throughput.mode == "throughput"
        assert composite.mode == "composite"

    def test_composite_qph_is_geometric_mean(self, runner, mock_executor):
        """composite.qph must equal sqrt(power.qph * throughput.qph)."""
        mock_executor.execute_query.return_value = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=1.0,
            rows_returned=10,
            raw_output="...",
        )

        power, throughput, composite = runner.run_composite(streams=2)

        expected = math.sqrt(power.qph * throughput.qph)
        assert composite.qph == pytest.approx(expected, rel=1e-6)


# ---------------------------------------------------------------------------
# 4. Query Filtering
# ---------------------------------------------------------------------------


class TestBenchmarkRunnerQueryFiltering:
    """Tests that query_class filtering works correctly."""

    def test_filter_by_query_class_scan(self, runner, mock_executor):
        result = runner.run_power(query_class="scan")

        scan_queries = [q for q in BENCHMARK_QUERIES if q.query_class == "scan"]
        assert len(result.queries) == len(scan_queries)
        assert all(q.query.query_class == "scan" for q in result.queries)

    def test_filter_by_query_class_analytics(self, runner, mock_executor):
        result = runner.run_power(query_class="analytics")

        analytics_queries = [q for q in BENCHMARK_QUERIES if q.query_class == "analytics"]
        assert len(result.queries) == len(analytics_queries)
        assert all(q.query.query_class == "analytics" for q in result.queries)


# ---------------------------------------------------------------------------
# 5. Result Serialization
# ---------------------------------------------------------------------------


class TestBenchmarkResultSerialization:
    """Verify to_dict() output structure for various run modes."""

    def test_benchmark_result_to_dict(self, runner, mock_executor):
        result = runner.run_power()
        d = result.to_dict()

        expected_keys = {
            "benchmark_type",
            "mode",
            "cache",
            "scale",
            "qph",
            "category_qph",
            "total_seconds",
            "iterations",
            "streams",
            "queries",
        }
        assert expected_keys.issubset(set(d.keys()))
        assert d["mode"] == "power"
        assert d["benchmark_type"] == "trino_query"
        assert isinstance(d["queries"], list)
        assert len(d["queries"]) == 8
        assert isinstance(d["category_qph"], dict)

    def test_stream_results_serialized(self, runner, mock_executor):
        result = runner.run_throughput(streams=3)
        d = result.to_dict()

        assert "stream_results" in d
        assert len(d["stream_results"]) == 3
        for sr in d["stream_results"]:
            assert "stream_id" in sr
            assert "total_seconds" in sr
            assert "queries" in sr
            assert "success" in sr
