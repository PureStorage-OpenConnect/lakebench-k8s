"""Tests for executor internals (P1 + P5).

Covers:
- DuckDB _build_python_script: SQL escaping, S3 config, path/vhost style
- DuckDB _discover_pod: caching behavior
- DuckDB execute_query: timeout, error, JSON parse fallback
- Trino/Spark/DuckDB executor error paths
- get_executor() factory edge cases
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

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


# ===========================================================================
# DuckDB _discover_pod
# ===========================================================================


# ===========================================================================
# DuckDB execute_query error paths
# ===========================================================================


class TestDuckDBExecuteQuery:
    """Tests for DuckDBExecutor.execute_query() error paths."""

    @pytest.mark.parametrize(
        ("stdout", "success", "rows"),
        [
            ('{"rows": 5, "data": ["(1,)", "(2,)", "(3,)", "(4,)", "(5,)"]}', True, 5),
            # DuckDB prints a progress bar to a pipe past 2 s; the payload is last
            ("\r100% ▕███▏\n" + '{"rows": 882697, "data": []}\n', True, 882697),
            # output without the payload is an error, never a line count (invariant 3)
            ("line1\nline2\nline3", False, 0),
            # the script always prints a payload; none means it did not run
            ("", False, None),
        ],
    )
    def test_duckdb_rows_come_from_the_payload(self, stdout, success, rows):
        from lakebench.benchmark.executor import DuckDBExecutor

        executor = DuckDBExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "duckdb-pod-0"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=stdout, stderr="")
            result = executor.execute_query("SELECT * FROM t")
        assert result.success is success
        if rows is not None:
            assert result.rows_returned == rows

    def test_script_disables_the_progress_bar_and_pins_utc(self):
        from lakebench.benchmark.executor import DuckDBExecutor

        script = DuckDBExecutor(namespace="t", catalog_name="c")._build_python_script("SELECT 1")
        assert "enable_progress_bar = false" in script
        assert "TimeZone = 'UTC'" in script
        assert script.index("enable_progress_bar") < script.index("conn.sql(")


# ===========================================================================
# Trino execute_query error paths
# ===========================================================================


# ===========================================================================
# SparkThrift execute_query error paths
# ===========================================================================


class TestSparkThriftExecuteQuery:
    """Tests for SparkThriftExecutor.execute_query() error paths."""

    @pytest.mark.parametrize(
        ("stdout", "rows"),
        [
            ("col1\tcol2\nval1\tval2\nval3\tval4", 2),  # tsv2 header excluded
            ("col1\tcol2", 0),  # header alone is an empty result
        ],
    )
    def test_thrift_rows_exclude_the_header(self, stdout, rows):
        from lakebench.benchmark.executor import SparkThriftExecutor

        executor = SparkThriftExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "spark-thrift-0"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=stdout, stderr="")
            result = executor.execute_query("SELECT * FROM t")
        assert result.success
        assert result.rows_returned == rows

    def test_options_precede_dash_e(self):
        """beeline's -e takes several values: options after it were read as
        more statements and dropped, so a 250-row result counted 259."""
        from lakebench.benchmark.executor import SparkThriftExecutor

        executor = SparkThriftExecutor(namespace="test", catalog_name="lakehouse")
        executor._pod = "spark-thrift-0"
        tsv = "id\n" + "\n".join(str(i) for i in range(250))
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=tsv, stderr="")
            result = executor.execute_query("SELECT id FROM range(250)")
            argv = mock_run.call_args[0][0]
        e = argv.index("-e")
        assert argv[e + 1] == "SELECT id FROM range(250)" and len(argv) == e + 2
        for opt in ("--silent=true", "--outputformat=tsv2", "--nullemptystring=false"):
            assert argv.index(opt) < e
        assert result.rows_returned == 250


# ===========================================================================
# get_executor factory
# ===========================================================================


# ===========================================================================
# QueryExecutorResult
# ===========================================================================
