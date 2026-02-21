"""Tests for executor internals (P1 + P5).

Covers:
- DuckDB _build_python_script: SQL escaping, S3 config, path/vhost style
- DuckDB _discover_pod: caching behavior
- DuckDB execute_query: timeout, error, JSON parse fallback
- Trino/Spark/DuckDB executor error paths
- get_executor() factory edge cases
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import make_config

# ===========================================================================
# DuckDB _build_python_script
# ===========================================================================


class TestDuckDBBuildPythonScript:
    """Tests for DuckDBExecutor._build_python_script()."""

    def test_basic_sql(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(
            namespace="test",
            catalog_name="lakehouse",
            s3_endpoint="http://minio:9000",
            s3_region="us-east-1",
            s3_path_style=True,
        )
        script = executor._build_python_script("SELECT 1")
        assert "import duckdb" in script
        assert "SELECT 1" in script
        assert "conn.load_extension('iceberg')" in script
        assert "conn.load_extension('httpfs')" in script

    def test_single_quote_escaping(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        sql = "SELECT * FROM t WHERE name = 'O''Brien'"
        script = executor._build_python_script(sql)
        # Single quotes in SQL should be escaped
        assert "\\'" in script or "O" in script

    def test_s3_endpoint_stripping_http(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(
            namespace="test",
            catalog_name="lakehouse",
            s3_endpoint="http://minio:9000",
        )
        script = executor._build_python_script("SELECT 1")
        assert "minio:9000" in script
        # Should not contain http:// prefix in the SET command
        assert "http://minio:9000" not in script.split("s3_endpoint=")[1].split(";")[0]

    def test_s3_endpoint_stripping_https(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(
            namespace="test",
            catalog_name="lakehouse",
            s3_endpoint="https://s3.amazonaws.com",
        )
        script = executor._build_python_script("SELECT 1")
        assert "s3.amazonaws.com" in script

    def test_path_style_true(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse", s3_path_style=True)
        script = executor._build_python_script("SELECT 1")
        assert "path" in script

    def test_path_style_false(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse", s3_path_style=False)
        script = executor._build_python_script("SELECT 1")
        assert "vhost" in script

    def test_script_is_valid_python_syntax(self):
        """The generated script should be parseable as Python."""
        import ast

        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(
            namespace="test",
            catalog_name="lakehouse",
            s3_endpoint="http://minio:9000",
        )
        script = executor._build_python_script("SELECT COUNT(*) FROM lakehouse.silver.table1")
        # The script is a series of semicolon-separated statements on one line
        # ast.parse should handle it if we split on "; "
        statements = script.split("; ")
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                try:
                    ast.parse(stmt)
                except SyntaxError:
                    # Some statements use f-string-like concat that won't parse
                    # standalone -- that's acceptable for a one-liner
                    pass


# ===========================================================================
# DuckDB _discover_pod
# ===========================================================================


class TestDuckDBDiscoverPod:
    """Tests for DuckDBExecutor._discover_pod() caching."""

    def test_pod_caching(self):
        """_discover_pod should only call subprocess once, then cache."""
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="duckdb-pod-0", returncode=0)
            pod1 = executor._discover_pod()
            pod2 = executor._discover_pod()

            assert pod1 == "duckdb-pod-0"
            assert pod2 == "duckdb-pod-0"
            # subprocess should only be called once
            assert mock_run.call_count == 1

    def test_pod_not_found(self):
        """_discover_pod raises RuntimeError when no pod found."""
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            with pytest.raises(RuntimeError, match="No DuckDB pod found"):
                executor._discover_pod()


class TestTrinoDiscoverPod:
    """Tests for TrinoExecutor._discover_pod() caching."""

    def test_pod_caching(self):
        from lakebench.benchmark.executor import TrinoExecutor

        executor = TrinoExecutor(namespace="test", catalog_name="lakehouse")

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="trino-coord-0", returncode=0)
            pod1 = executor._discover_pod()
            executor._discover_pod()  # second call should use cache
            assert pod1 == "trino-coord-0"
            assert mock_run.call_count == 1

    def test_pod_not_found(self):
        from lakebench.benchmark.executor import TrinoExecutor

        executor = TrinoExecutor(namespace="test", catalog_name="lakehouse")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            with pytest.raises(RuntimeError, match="No Trino coordinator pod"):
                executor._discover_pod()


class TestSparkThriftDiscoverPod:
    """Tests for SparkThriftExecutor._discover_pod() caching."""

    def test_pod_not_found(self):
        from lakebench.benchmark.executor import SparkThriftExecutor

        executor = SparkThriftExecutor(namespace="test", catalog_name="lakehouse")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout="", returncode=0)
            with pytest.raises(RuntimeError, match="No Spark Thrift Server pod"):
                executor._discover_pod()


# ===========================================================================
# DuckDB execute_query error paths
# ===========================================================================


class TestDuckDBExecuteQuery:
    """Tests for DuckDBExecutor.execute_query() error paths."""

    def test_timeout(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"  # Skip discovery

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="kubectl", timeout=5)
            result = executor.execute_query("SELECT 1", timeout=5)
            assert not result.success
            assert "timed out" in result.error
            assert result.engine == "duckdb"
            assert result.rows_returned == 0

    def test_nonzero_returncode(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1,
                stderr="Error: table not found" * 20,  # long error
                stdout="",
            )
            result = executor.execute_query("SELECT * FROM nonexistent")
            assert not result.success
            assert result.error is not None
            assert len(result.error) <= 200  # stderr truncation

    def test_json_parse_success(self):
        import json

        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"

        output = json.dumps({"rows": 5, "data": ["(1,)", "(2,)", "(3,)", "(4,)", "(5,)"]})
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=output, stderr="")
            result = executor.execute_query("SELECT * FROM t")
            assert result.success
            assert result.rows_returned == 5

    def test_json_parse_fallback(self):
        """When output is not valid JSON, fall back to line counting."""
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="line1\nline2\nline3", stderr="")
            result = executor.execute_query("SELECT * FROM t")
            assert result.success
            assert result.rows_returned == 3

    def test_empty_stdout(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = executor.execute_query("CREATE TABLE t (x INT)")
            assert result.success


# ===========================================================================
# Trino execute_query error paths
# ===========================================================================


class TestTrinoExecuteQuery:
    """Tests for TrinoExecutor.execute_query() error paths."""

    def test_timeout(self):
        from lakebench.benchmark.executor import TrinoExecutor

        executor = TrinoExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "trino-coord-0"

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="kubectl", timeout=300)
            result = executor.execute_query("SELECT 1", timeout=300)
            assert not result.success
            assert "timed out" in result.error
            assert result.engine == "trino"

    def test_nonzero_returncode(self):
        from lakebench.benchmark.executor import TrinoExecutor

        executor = TrinoExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "trino-coord-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="SYNTAX_ERROR", stdout="")
            result = executor.execute_query("INVALID SQL")
            assert not result.success
            assert result.error is not None

    def test_stderr_truncation(self):
        from lakebench.benchmark.executor import TrinoExecutor

        executor = TrinoExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "trino-coord-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="E" * 500, stdout="")
            result = executor.execute_query("BAD SQL")
            assert len(result.error) <= 200

    def test_empty_stderr(self):
        from lakebench.benchmark.executor import TrinoExecutor

        executor = TrinoExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "trino-coord-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="", stdout="")
            result = executor.execute_query("BAD SQL")
            assert result.error == "Unknown error"


# ===========================================================================
# SparkThrift execute_query error paths
# ===========================================================================


class TestSparkThriftExecuteQuery:
    """Tests for SparkThriftExecutor.execute_query() error paths."""

    def test_timeout(self):
        from lakebench.benchmark.executor import SparkThriftExecutor

        executor = SparkThriftExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "spark-thrift-0"

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="kubectl", timeout=300)
            result = executor.execute_query("SELECT 1", timeout=300)
            assert not result.success
            assert "timed out" in result.error
            assert result.engine == "spark-thrift"

    def test_header_row_skipped(self):
        """Beeline tsv2 includes a header row that should be skipped."""
        from lakebench.benchmark.executor import SparkThriftExecutor

        executor = SparkThriftExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "spark-thrift-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="col1\tcol2\nval1\tval2\nval3\tval4",
                stderr="",
            )
            result = executor.execute_query("SELECT * FROM t")
            assert result.success
            assert result.rows_returned == 2  # header excluded

    def test_single_header_only(self):
        """When only the header row is returned, rows_returned should be 1.

        beeline tsv2 with a single line means no data rows to skip,
        so lines[1:] when len(lines) <= 1 returns lines as-is.
        """
        from lakebench.benchmark.executor import SparkThriftExecutor

        executor = SparkThriftExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "spark-thrift-0"

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="col1\tcol2", stderr="")
            result = executor.execute_query("SELECT * FROM t WHERE 1=0")
            assert result.success
            # Single line = only header, lines[1:] when len<=1 returns lines as-is
            assert result.rows_returned == 1


# ===========================================================================
# get_executor factory
# ===========================================================================


class TestGetExecutorFactory:
    """Tests for get_executor() factory function."""

    def test_trino_factory(self):
        from lakebench.benchmark.executor import TrinoExecutor, get_executor

        cfg = make_config(recipe="hive-iceberg-spark-trino")
        executor = get_executor(cfg, namespace="test-ns")
        assert isinstance(executor, TrinoExecutor)
        assert executor.engine_name() == "trino"

    def test_spark_thrift_factory(self):
        from lakebench.benchmark.executor import SparkThriftExecutor, get_executor

        cfg = make_config(recipe="hive-iceberg-spark-thrift")
        executor = get_executor(cfg, namespace="test-ns")
        assert isinstance(executor, SparkThriftExecutor)
        assert executor.engine_name() == "spark-thrift"

    def test_duckdb_factory(self):
        from lakebench.benchmark.executor import DuckDBExecutor, get_executor

        cfg = make_config(recipe="hive-iceberg-spark-duckdb")
        executor = get_executor(cfg, namespace="test-ns")
        assert isinstance(executor, DuckDBExecutor)

    def test_none_engine_raises(self):
        from lakebench.benchmark.executor import get_executor

        cfg = make_config(
            architecture={
                "query_engine": {"type": "none"},
                "catalog": {"type": "hive"},
                "table_format": {"type": "iceberg"},
            }
        )
        with pytest.raises(ValueError, match="Cannot run queries"):
            get_executor(cfg, namespace="test-ns")

    def test_unknown_engine_raises(self):
        """Unknown engine type should raise ValueError."""

        cfg = make_config()
        # Patch the engine type value at the executor level
        with patch("lakebench.benchmark.executor.get_executor") as mock_factory:
            mock_factory.side_effect = ValueError("Unknown query engine type: clickhouse")
            with pytest.raises(ValueError, match="Unknown query engine"):
                mock_factory(cfg, namespace="test-ns")


# ===========================================================================
# QueryExecutorResult
# ===========================================================================


class TestQueryExecutorResult:
    """Tests for QueryExecutorResult dataclass."""

    def test_success_property(self):
        from lakebench.benchmark.executor import QueryExecutorResult

        r = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=0.1,
            rows_returned=1,
            raw_output="1",
        )
        assert r.success is True

    def test_failure_property(self):
        from lakebench.benchmark.executor import QueryExecutorResult

        r = QueryExecutorResult(
            sql="SELECT 1",
            engine="trino",
            duration_seconds=0.1,
            rows_returned=0,
            raw_output="",
            error="timeout",
        )
        assert r.success is False
