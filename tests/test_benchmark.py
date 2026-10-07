"""Tests for the Trino query benchmark module."""

from datetime import datetime, timedelta

from lakebench.benchmark.queries import BENCHMARK_QUERIES
from lakebench.metrics import BenchmarkMetrics, MetricsStorage, PipelineMetrics

# ---------------------------------------------------------------------------
# BenchmarkQuery
# ---------------------------------------------------------------------------


class TestBenchmarkQueries:
    """Tests for benchmark query definitions."""

    def test_all_queries_have_catalog_placeholder(self):
        for q in BENCHMARK_QUERIES:
            assert "{catalog}" in q.sql, f"{q.name} missing {{catalog}} placeholder"


class TestBenchmarkQueriesByDomain:
    """Tests for the schema-keyed dispatch introduced in ENG-2C.4.6-7."""

    def test_custom_dispatch_falls_back_to_customer360(self):
        from lakebench.benchmark.queries import (
            BENCHMARK_QUERIES,
            get_benchmark_queries,
        )
        from lakebench.config.schema import WorkloadSchema

        assert get_benchmark_queries(WorkloadSchema.CUSTOM) == BENCHMARK_QUERIES


# ---------------------------------------------------------------------------
# QueryResult
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# BenchmarkResult
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# BenchmarkMetrics integration
# ---------------------------------------------------------------------------


class TestBenchmarkMetricsIntegration:
    """Tests for BenchmarkMetrics in the metrics system."""

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


# ---------------------------------------------------------------------------
# Throughput and Composite QpH
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Executor Format Awareness (v1.2)
# ---------------------------------------------------------------------------


class TestDuckDBExecutorFormatAwareness:
    """Tests for DuckDB executor table_format-dependent behavior."""

    def test_build_python_script_loads_delta_extension(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(
            namespace="test-ns",
            catalog_name="lakehouse",
            table_format="delta",
        )
        script = executor._build_python_script("SELECT 1")
        assert "load_extension('delta')" in script

    def test_build_python_script_loads_iceberg_extension(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(
            namespace="test-ns",
            catalog_name="lakehouse",
            table_format="iceberg",
        )
        script = executor._build_python_script("SELECT 1")
        assert "load_extension('iceberg')" in script

    def test_adapt_query_delta_scan(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(
            namespace="test-ns",
            catalog_name="lakehouse",
            table_format="delta",
            s3_buckets={"silver": "lb-silver"},
            table_names={"silver": "silver_ns.my_table"},
        )
        sql = "SELECT * FROM lakehouse.silver_ns.my_table"
        adapted = executor.adapt_query(sql)
        assert "delta_scan(" in adapted
        assert "iceberg_scan(" not in adapted

    def test_adapt_query_iceberg_scan(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(
            namespace="test-ns",
            catalog_name="lakehouse",
            table_format="iceberg",
            s3_buckets={"silver": "lb-silver"},
            table_names={"silver": "silver_ns.my_table"},
        )
        sql = "SELECT * FROM lakehouse.silver_ns.my_table"
        adapted = executor.adapt_query(sql)
        assert "iceberg_scan(" in adapted
        assert "delta_scan(" not in adapted
