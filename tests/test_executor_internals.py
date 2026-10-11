"""Executor internals: DuckDB payload parsing, Spark Thrift row counting and
the Trino session zone."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestDuckDBExecuteQuery:
    """DuckDBExecutor.execute_query() payload handling."""

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


class TestSparkThriftExecuteQuery:
    """Tests for SparkThriftExecutor.execute_query() error paths."""

    @pytest.mark.parametrize(
        ("stdout", "rows"),
        [
            ("col1\tcol2\nval1\tval2\nval3\tval4", 2),  # tsv2 header excluded
            ("col1\tcol2", 0),  # header alone is an empty result
            # a one-column empty-string row is an empty line and still a row
            ("name\n\n", 1),
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


class TestTrinoSession:
    def test_every_trino_call_pins_utc(self):
        """timestamptz rendering, date_trunc and timestamp arithmetic follow
        the session zone; unpinned they follow the coordinator's default."""
        from lakebench.modules.query_engines.trino.executor import TrinoExecutor

        ex = TrinoExecutor("ns", "lakehouse")
        ex._pod = "coord-0"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='"1"\n', stderr="")
            ex.execute_query("SELECT 1")
        cmd = mock_run.call_args[0][0]
        assert cmd[cmd.index("--timezone") + 1] == "UTC"
