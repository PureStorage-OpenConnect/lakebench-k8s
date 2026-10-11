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


def _result(seconds: float, error: str | None = None) -> QueryExecutorResult:
    return QueryExecutorResult(
        sql="SELECT 1",
        engine="trino",
        duration_seconds=seconds,
        rows_returned=0 if error else 10,
        raw_output="" if error else "...",
        error=error,
    )


class TestBenchmarkRunnerPowerMode:
    """Full power-mode flow through BenchmarkRunner."""

    def test_power_qph_calculation(self, runner, mock_executor):
        """QpH should equal (num_queries / total_seconds) * 3600."""
        n = len(BENCHMARK_QUERIES)
        mock_executor.execute_query.return_value = _result(2.0)

        result = runner.run_power()

        assert result.mode == "power"
        assert len(result.queries) == n
        assert result.total_seconds == pytest.approx(2.0 * n)
        assert result.qph == pytest.approx((n / (2.0 * n)) * 3600)

    @pytest.mark.parametrize("cache,flushes_per_query", [("cold", 1), ("hot", 0)])
    def test_power_cache_mode_controls_flush(self, runner, mock_executor, cache, flushes_per_query):
        """cache='cold' flushes before each query; 'hot' never flushes."""
        runner.run_power(cache=cache)

        assert mock_executor.flush_cache.call_count == flushes_per_query * len(BENCHMARK_QUERIES)

    def test_power_query_failure_still_completes(self, runner, mock_executor):
        """A failed query is recorded and does not abort the run. QpH counts
        only successful queries over successful time; total_seconds keeps the
        failed query's wall time."""
        n = len(BENCHMARK_QUERIES)
        mock_executor.execute_query.side_effect = [
            _result(10.0, error="Table not found"),
            *[_result(0.5)] * (n - 1),
        ]

        result = runner.run_power()

        assert len(result.queries) == n
        assert result.queries[0].success is False
        assert result.queries[0].error_message == "Table not found"
        assert all(q.success for q in result.queries[1:])
        assert result.total_seconds == pytest.approx(10.0 + 0.5 * (n - 1))
        assert result.qph == pytest.approx(((n - 1) / (0.5 * (n - 1))) * 3600)

    def test_power_category_qph(self, runner, mock_executor):
        """Per-class QpH is that class's query count over that class's own time."""
        # Each query's duration is its 1-based position, so classes differ.
        durations = {q.name: float(i + 1) for i, q in enumerate(BENCHMARK_QUERIES)}
        mock_executor.execute_query.side_effect = [
            _result(durations[q.name]) for q in BENCHMARK_QUERIES
        ]

        cat_qph = runner.run_power().compute_category_qph()

        expected: dict[str, list[float]] = {}
        for q in BENCHMARK_QUERIES:
            expected.setdefault(q.query_class, []).append(durations[q.name])
        assert set(cat_qph) == set(expected)
        for cls, times in expected.items():
            assert cat_qph[cls] == pytest.approx(len(times) / sum(times) * 3600)


# ---------------------------------------------------------------------------
# 2. Throughput Mode
# ---------------------------------------------------------------------------


class TestBenchmarkRunnerThroughputMode:
    """Concurrent-stream throughput tests."""

    def test_throughput_streams_each_run_every_query(self, runner, mock_executor):
        result = runner.run_throughput(streams=4)

        assert result.mode == "throughput"
        assert result.streams == 4
        assert len(result.stream_results) == 4
        for stream in result.stream_results:
            assert len(stream.queries) == len(BENCHMARK_QUERIES)
            assert all(q.success for q in stream.queries)


# ---------------------------------------------------------------------------
# 3. Composite Mode
# ---------------------------------------------------------------------------


class TestBenchmarkRunnerCompositeMode:
    """Power + throughput composite tests."""

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

    @pytest.mark.parametrize("query_class", ["scan", "analytics"])
    def test_filter_by_query_class(self, runner, mock_executor, query_class):
        result = runner.run_power(query_class=query_class)

        expected = [q for q in BENCHMARK_QUERIES if q.query_class == query_class]
        assert len(result.queries) == len(expected)
        assert all(q.query.query_class == query_class for q in result.queries)
