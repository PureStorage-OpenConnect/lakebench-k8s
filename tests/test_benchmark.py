"""Tests for the Trino query benchmark module."""

import math
from datetime import datetime, timedelta

import pytest

from lakebench.benchmark.queries import BENCHMARK_QUERIES
from lakebench.benchmark.runner import BenchmarkResult, QueryResult, StreamResult
from lakebench.metrics import BenchmarkMetrics, MetricsStorage, PipelineMetrics

# ---------------------------------------------------------------------------
# BenchmarkQuery
# ---------------------------------------------------------------------------


class TestBenchmarkQueries:
    """Tests for benchmark query definitions."""

    def test_query_count(self):
        assert len(BENCHMARK_QUERIES) == 8

    def test_all_queries_have_catalog_placeholder(self):
        for q in BENCHMARK_QUERIES:
            assert "{catalog}" in q.sql, f"{q.name} missing {{catalog}} placeholder"

    def test_all_queries_have_required_fields(self):
        valid_classes = ("scan", "filter_prune", "aggregation", "analytics", "operational")
        for q in BENCHMARK_QUERIES:
            assert q.name, "Query missing name"
            assert q.display_name, f"{q.name} missing display_name"
            assert q.query_class in valid_classes, f"{q.name} has invalid class: {q.query_class}"
            assert q.sql.strip(), f"{q.name} has empty SQL"

    def test_query_class_distribution(self):
        classes = [q.query_class for q in BENCHMARK_QUERIES]
        assert classes.count("scan") == 1
        assert classes.count("filter_prune") == 2
        assert classes.count("aggregation") == 2
        assert classes.count("analytics") == 2
        assert classes.count("operational") == 1

    def test_query_names_unique(self):
        names = [q.name for q in BENCHMARK_QUERIES]
        assert len(names) == len(set(names)), "Duplicate query names"

    def test_query_names_start_with_q(self):
        for q in BENCHMARK_QUERIES:
            assert q.name.startswith("Q"), f"Query name should start with Q, got {q.name}"

    def test_queries_are_frozen(self):
        q = BENCHMARK_QUERIES[0]
        with pytest.raises(AttributeError):
            q.name = "modified"


# ---------------------------------------------------------------------------
# QueryResult
# ---------------------------------------------------------------------------


class TestQueryResult:
    """Tests for QueryResult dataclass."""

    def test_basic_query_result(self):
        q = BENCHMARK_QUERIES[0]
        r = QueryResult(
            query=q,
            elapsed_seconds=4.23,
            rows_returned=1,
            success=True,
        )
        assert r.elapsed_seconds == 4.23
        assert r.rows_returned == 1
        assert r.error_message == ""

    def test_query_result_to_dict(self):
        q = BENCHMARK_QUERIES[0]
        r = QueryResult(
            query=q,
            elapsed_seconds=4.23,
            rows_returned=1,
            success=True,
        )
        d = r.to_dict()
        assert d["name"] == "Q1_full_aggregation_scan"
        assert d["display_name"] == "Full aggregation scan"
        assert d["class"] == "scan"
        assert d["elapsed_seconds"] == 4.23
        assert d["rows_returned"] == 1
        assert d["success"] is True
        assert d["error_message"] == ""

    def test_failed_query_result(self):
        q = BENCHMARK_QUERIES[0]
        r = QueryResult(
            query=q,
            elapsed_seconds=0.5,
            rows_returned=0,
            success=False,
            error_message="Table not found",
        )
        d = r.to_dict()
        assert d["success"] is False
        assert d["error_message"] == "Table not found"


# ---------------------------------------------------------------------------
# BenchmarkResult
# ---------------------------------------------------------------------------


class TestBenchmarkResult:
    """Tests for BenchmarkResult dataclass."""

    def _make_results(self) -> list[QueryResult]:
        """Build a mock result set with predictable times."""
        results = []
        for q in BENCHMARK_QUERIES:
            results.append(
                QueryResult(
                    query=q,
                    elapsed_seconds=2.0,  # 2s per query
                    rows_returned=10,
                    success=True,
                )
            )
        return results

    def test_qph_calculation(self):
        results = self._make_results()
        total_seconds = sum(r.elapsed_seconds for r in results)
        qph = (len(results) / total_seconds) * 3600

        br = BenchmarkResult(
            mode="standard",
            cache="hot",
            scale=102,
            queries=results,
            total_seconds=total_seconds,
            qph=qph,
        )

        # 8 queries * 2s = 16s total
        assert br.total_seconds == 16.0
        # QpH = (8 / 16) * 3600 = 1800
        assert br.qph == 1800.0

    def test_benchmark_result_to_dict(self):
        results = self._make_results()
        br = BenchmarkResult(
            mode="standard",
            cache="hot",
            scale=102,
            queries=results,
            total_seconds=16.0,
            qph=1800.0,
            iterations=1,
        )
        d = br.to_dict()
        assert d["benchmark_type"] == "trino_query"
        assert d["mode"] == "standard"
        assert d["cache"] == "hot"
        assert d["scale"] == 102
        assert d["qph"] == 1800.0
        assert d["total_seconds"] == 16.0
        assert d["iterations"] == 1
        assert len(d["queries"]) == 8
        assert d["queries"][0]["name"] == "Q1_full_aggregation_scan"
        # category_qph should be present
        assert "category_qph" in d
        assert isinstance(d["category_qph"], dict)

    def test_empty_benchmark_result(self):
        br = BenchmarkResult(
            mode="standard",
            cache="hot",
            scale=10,
        )
        assert br.queries == []
        assert br.total_seconds == 0.0
        assert br.qph == 0.0


# ---------------------------------------------------------------------------
# BenchmarkMetrics integration
# ---------------------------------------------------------------------------


class TestBenchmarkMetricsIntegration:
    """Tests for BenchmarkMetrics in the metrics system."""

    def test_benchmark_metrics_to_dict(self):
        bm = BenchmarkMetrics(
            mode="standard",
            cache="hot",
            scale=102,
            qph=820.4,
            total_seconds=43.88,
            queries=[
                {
                    "name": "Q1_full_aggregation_scan",
                    "class": "scan",
                    "elapsed_seconds": 4.23,
                    "rows_returned": 1,
                    "success": True,
                }
            ],
            iterations=1,
        )
        d = bm.to_dict()
        assert d["benchmark_type"] == "trino_query"
        assert d["qph"] == 820.4
        assert d["scale"] == 102
        assert len(d["queries"]) == 1

    def test_pipeline_metrics_with_benchmark(self):
        now = datetime.now()
        pm = PipelineMetrics(
            run_id="bench-test",
            deployment_name="test",
            start_time=now,
            success=True,
            benchmark=BenchmarkMetrics(
                mode="standard",
                cache="hot",
                scale=102,
                qph=820.4,
                total_seconds=43.88,
                queries=[],
            ),
        )
        d = pm.to_dict()
        assert "benchmark" in d
        assert d["benchmark"]["qph"] == 820.4
        assert d["benchmark"]["benchmark_type"] == "trino_query"

    def test_pipeline_metrics_without_benchmark(self):
        now = datetime.now()
        pm = PipelineMetrics(
            run_id="no-bench",
            deployment_name="test",
            start_time=now,
            success=True,
        )
        d = pm.to_dict()
        assert "benchmark" not in d

    def test_benchmark_metrics_roundtrip(self, tmp_path):
        """Save and load PipelineMetrics with benchmark data."""
        storage = MetricsStorage(tmp_path / "metrics")

        now = datetime.now()
        pm = PipelineMetrics(
            run_id="roundtrip-bench",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=300),
            total_elapsed_seconds=300.0,
            success=True,
            benchmark=BenchmarkMetrics(
                mode="extended",
                cache="cold",
                scale=50,
                qph=1234.5,
                total_seconds=29.2,
                queries=[
                    {
                        "name": "Q1_full_aggregation_scan",
                        "class": "scan",
                        "elapsed_seconds": 2.92,
                        "rows_returned": 1,
                        "success": True,
                    }
                ],
                iterations=5,
            ),
        )

        storage.save_run(pm)
        loaded = storage.load_run("roundtrip-bench")

        assert loaded is not None
        assert loaded.benchmark is not None
        assert loaded.benchmark.mode == "extended"
        assert loaded.benchmark.cache == "cold"
        assert loaded.benchmark.scale == 50
        assert loaded.benchmark.qph == 1234.5
        assert loaded.benchmark.total_seconds == 29.2
        assert loaded.benchmark.iterations == 5
        assert len(loaded.benchmark.queries) == 1
        assert loaded.benchmark.queries[0]["name"] == "Q1_full_aggregation_scan"


# ---------------------------------------------------------------------------
# StreamResult
# ---------------------------------------------------------------------------


class TestStreamResult:
    """Tests for StreamResult dataclass."""

    def _make_stream_queries(self) -> list[QueryResult]:
        results = []
        for q in BENCHMARK_QUERIES[:3]:
            results.append(
                QueryResult(
                    query=q,
                    elapsed_seconds=1.5,
                    rows_returned=5,
                    success=True,
                )
            )
        return results

    def test_stream_result_to_dict(self):
        queries = self._make_stream_queries()
        sr = StreamResult(
            stream_id=0,
            queries=queries,
            total_seconds=4.5,
            success=True,
        )
        d = sr.to_dict()
        assert d["stream_id"] == 0
        assert d["total_seconds"] == 4.5
        assert d["success"] is True
        assert len(d["queries"]) == 3

    def test_stream_result_defaults(self):
        sr = StreamResult(stream_id=1)
        assert sr.queries == []
        assert sr.total_seconds == 0.0
        assert sr.success is True

    def test_failed_stream(self):
        sr = StreamResult(
            stream_id=2,
            queries=[],
            total_seconds=10.0,
            success=False,
        )
        d = sr.to_dict()
        assert d["success"] is False


# ---------------------------------------------------------------------------
# Throughput and Composite QpH
# ---------------------------------------------------------------------------


class TestThroughputQpH:
    """Tests for throughput QpH formula verification."""

    def _make_results(self) -> list[QueryResult]:
        results = []
        for q in BENCHMARK_QUERIES:
            results.append(
                QueryResult(
                    query=q,
                    elapsed_seconds=2.0,
                    rows_returned=10,
                    success=True,
                )
            )
        return results

    def test_throughput_qph_formula(self):
        """QpH = (total_queries_all_streams / wall_clock_seconds) * 3600."""
        queries = self._make_results()
        streams = 4
        wall_seconds = 50.0
        total_queries = streams * len(queries)
        qph = (total_queries / wall_seconds) * 3600

        br = BenchmarkResult(
            mode="throughput",
            cache="hot",
            scale=100,
            queries=queries,
            total_seconds=wall_seconds,
            qph=qph,
            streams=streams,
        )

        # 4 streams * 8 queries / 50s * 3600 = 2304
        assert br.qph == 2304.0
        assert br.streams == 4

    def test_throughput_result_with_stream_results(self):
        queries = self._make_results()
        stream_results = [
            StreamResult(stream_id=i, queries=queries, total_seconds=48.0 + i, success=True)
            for i in range(4)
        ]
        br = BenchmarkResult(
            mode="throughput",
            cache="hot",
            scale=100,
            queries=queries,
            total_seconds=51.0,
            qph=2823.5,
            streams=4,
            stream_results=stream_results,
        )
        d = br.to_dict()
        assert d["streams"] == 4
        assert "stream_results" in d
        assert len(d["stream_results"]) == 4
        assert d["stream_results"][0]["stream_id"] == 0

    def test_benchmark_result_no_stream_results_omitted(self):
        """stream_results key should be absent for power runs."""
        br = BenchmarkResult(
            mode="power",
            cache="hot",
            scale=100,
            total_seconds=20.0,
            qph=1800.0,
        )
        d = br.to_dict()
        assert d["streams"] == 1
        assert "stream_results" not in d


class TestCompositeQpH:
    """Tests for composite QpH formula (geometric mean)."""

    def test_composite_qph_formula(self):
        power_qph = 1800.0
        throughput_qph = 2880.0
        composite_qph = math.sqrt(power_qph * throughput_qph)

        # sqrt(1800 * 2880) = sqrt(5184000) ≈ 2276.8
        assert round(composite_qph, 1) == 2276.8

    def test_composite_zero_handling(self):
        """Composite should be 0 if either component is 0."""
        assert math.sqrt(0 * 2880.0) == 0.0
        assert math.sqrt(1800.0 * 0) == 0.0


# ---------------------------------------------------------------------------
# Throughput Metrics Roundtrip
# ---------------------------------------------------------------------------


class TestThroughputMetricsRoundtrip:
    """Tests for throughput metrics serialization/deserialization."""

    def test_save_load_with_streams(self, tmp_path):
        """Roundtrip throughput benchmark data through MetricsStorage."""
        storage = MetricsStorage(tmp_path / "metrics")

        now = datetime.now()
        pm = PipelineMetrics(
            run_id="throughput-roundtrip",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=60),
            total_elapsed_seconds=60.0,
            success=True,
            benchmark=BenchmarkMetrics(
                mode="throughput",
                cache="hot",
                scale=100,
                qph=2880.0,
                total_seconds=50.0,
                queries=[
                    {
                        "name": "Q1_full_aggregation_scan",
                        "class": "scan",
                        "elapsed_seconds": 5.0,
                        "rows_returned": 1,
                        "success": True,
                    }
                ],
                iterations=1,
                streams=4,
                stream_results=[
                    {"stream_id": 0, "total_seconds": 48.0, "success": True, "queries": []},
                    {"stream_id": 1, "total_seconds": 49.0, "success": True, "queries": []},
                    {"stream_id": 2, "total_seconds": 50.0, "success": True, "queries": []},
                    {"stream_id": 3, "total_seconds": 47.5, "success": True, "queries": []},
                ],
            ),
        )

        storage.save_run(pm)
        loaded = storage.load_run("throughput-roundtrip")

        assert loaded is not None
        assert loaded.benchmark is not None
        assert loaded.benchmark.mode == "throughput"
        assert loaded.benchmark.streams == 4
        assert len(loaded.benchmark.stream_results) == 4
        assert loaded.benchmark.stream_results[0]["stream_id"] == 0
        assert loaded.benchmark.stream_results[3]["total_seconds"] == 47.5

    def test_backward_compat_no_streams(self, tmp_path):
        """Old metrics without streams/stream_results load with defaults."""
        storage = MetricsStorage(tmp_path / "metrics")

        now = datetime.now()
        pm = PipelineMetrics(
            run_id="old-format",
            deployment_name="test",
            start_time=now,
            success=True,
            benchmark=BenchmarkMetrics(
                mode="power",
                cache="hot",
                scale=50,
                qph=900.0,
                total_seconds=40.0,
                queries=[],
            ),
        )

        storage.save_run(pm)
        loaded = storage.load_run("old-format")

        assert loaded is not None
        assert loaded.benchmark is not None
        assert loaded.benchmark.streams == 1
        assert loaded.benchmark.stream_results == []


# ---------------------------------------------------------------------------
# BenchmarkMetrics streams serialization
# ---------------------------------------------------------------------------


class TestBenchmarkMetricsStreams:
    """Tests for BenchmarkMetrics streams fields."""

    def test_to_dict_with_streams(self):
        bm = BenchmarkMetrics(
            mode="throughput",
            cache="hot",
            scale=100,
            qph=2880.0,
            total_seconds=50.0,
            streams=4,
            stream_results=[
                {"stream_id": 0, "total_seconds": 48.0, "success": True},
            ],
        )
        d = bm.to_dict()
        assert d["streams"] == 4
        assert "stream_results" in d
        assert len(d["stream_results"]) == 1

    def test_to_dict_no_stream_results_omitted(self):
        bm = BenchmarkMetrics(
            mode="power",
            cache="hot",
            scale=50,
            qph=1800.0,
            total_seconds=20.0,
        )
        d = bm.to_dict()
        assert d["streams"] == 1
        assert "stream_results" not in d
