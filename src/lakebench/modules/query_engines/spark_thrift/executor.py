"""Spark Thrift Server query executor for Lakebench benchmarks.

Executes SQL queries via ``kubectl exec`` into beeline.
"""

from __future__ import annotations

import logging
import re
import subprocess
import time

from lakebench.benchmark.result import QueryExecutorResult, summarise_engine_error
from lakebench.modules.query_engines import PodExecutor, call_args_end

logger = logging.getLogger(__name__)


def beeline_argv(sql: str, url: str = "jdbc:hive2://localhost:10000") -> list[str]:
    """beeline argv that runs *sql* with tsv2 output.

    Every option goes before ``-e``. ``-e`` takes several values, so options
    after it were read as more ``-e`` statements (each starting ``--``, an
    SQL comment): silent mode and tsv2 were dropped, beeline printed its
    table format with a 3-line header every 100 rows, and the row count
    read n + 3 x ceil(n/100). ``--nullemptystring=false`` prints NULL as
    ``NULL``, so it stays distinct from an empty string.
    """
    return [
        "/opt/spark/bin/beeline",
        "-u",
        url,
        "--silent=true",
        "--outputformat=tsv2",
        "--nullemptystring=false",
        "-e",
        sql,
    ]


def _drop_terminal_newline(text: str) -> str:
    if text.endswith("\n"):
        text = text[:-1]
    if text.endswith("\r"):
        text = text[:-1]
    return text


class SparkThriftExecutor(PodExecutor):
    """Executes queries via ``kubectl exec`` into beeline on the Spark Thrift Server."""

    pod_component = "spark-thrift-server"
    pod_label = "Spark Thrift Server"
    health_timeout = 30

    def __init__(
        self,
        namespace: str,
        catalog_name: str,
        kube_context: str | None = None,
    ):
        self.namespace = namespace
        self.catalog_name = catalog_name
        self.kube_context = kube_context or None
        self._pod: str | None = None

    def engine_name(self) -> str:
        return "spark-thrift"

    def _beeline_cmd(self, pod: str, sql: str) -> list[str]:
        return [
            *self._kubectl_prefix(),
            "exec",
            pod,
            "-c",
            "spark-thrift",
            "-n",
            self.namespace,
            "--",
            *beeline_argv(sql),
        ]

    def execute_query(self, sql: str, timeout: int = 300) -> QueryExecutorResult:
        pod = self._discover_pod()
        cmd = self._beeline_cmd(pod, sql)

        start = time.monotonic()
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            elapsed = time.monotonic() - start
        except subprocess.TimeoutExpired:
            elapsed = time.monotonic() - start
            return QueryExecutorResult(
                sql=sql,
                engine="spark-thrift",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output="",
                error=f"Query timed out ({timeout}s)",
                timed_out=True,
            )

        if result.returncode != 0:
            error = summarise_engine_error(result.stderr or "")
            return QueryExecutorResult(
                sql=sql,
                engine="spark-thrift",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output=result.stdout or "",
                error=error,
            )

        output = _drop_terminal_newline(result.stdout or "")
        # tsv2 prints one header line and then a line per row, and prints
        # the header for an empty result too: a header alone is 0 rows. Only
        # the terminal newline is dropped: a one-column empty-string row is
        # an empty line and still a row.
        lines = output.split("\n") if output else []
        data_rows = lines[1:]
        return QueryExecutorResult(
            sql=sql,
            engine="spark-thrift",
            duration_seconds=elapsed,
            rows_returned=len(data_rows),
            raw_output=output,
        )

    def query_rows(self, sql: str, timeout: int = 300) -> list[list[str | None]]:
        """Not supported: financial scoring runs on Trino, the validated
        financial query engine (config load refuses financial on spark-thrift).
        The Trino->Spark dialect of the scoring statements and beeline tsv2
        quoting are not handled here; add them with a spark-thrift financial
        validation row if that engine is ever validated for financial."""
        raise NotImplementedError("financial scoring runs on Trino, not spark-thrift")

    def flush_cache(self) -> None:
        """Spark Thrift Server has no explicit metadata cache flush."""
        pass

    def adapt_query(self, sql: str) -> str:
        """Translate Trino SQL dialect to Spark SQL where they differ."""
        sql = self._rewrite_date_add(sql)
        sql = self._rewrite_date_diff(sql)
        return sql

    @staticmethod
    def _rewrite_date_add(sql: str) -> str:
        """Rewrite Trino ``date_add('month', N, expr)`` to Spark ``add_months(expr, N)``."""
        pattern = re.compile(
            r"date_add\(\s*'month'\s*,\s*(\d+)\s*,\s*",
            re.IGNORECASE,
        )
        m = pattern.search(sql)
        if not m:
            return sql
        n = m.group(1)
        start = m.end()
        i = call_args_end(sql, start)
        if i is None:
            return sql
        expr = sql[start : i - 1]
        replacement = f"add_months({expr}, {n})"
        return sql[: m.start()] + replacement + sql[i:]

    @staticmethod
    def _rewrite_date_diff(sql: str) -> str:
        """Rewrite Trino ``DATE_DIFF('day', start, end)`` to Spark ``DATEDIFF(end, start)``."""
        pattern = re.compile(
            r"DATE_DIFF\(\s*'day'\s*,\s*",
            re.IGNORECASE,
        )
        m = pattern.search(sql)
        if not m:
            return sql
        start = m.end()
        depth = 0
        args: list[str] = []
        arg_start = start
        i = start
        while i < len(sql):
            ch = sql[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth == 0:
                    args.append(sql[arg_start:i].strip())
                    break
                depth -= 1
            elif ch == "," and depth == 0:
                args.append(sql[arg_start:i].strip())
                arg_start = i + 1
            i += 1
        if len(args) != 2:
            return sql
        replacement = f"DATEDIFF({args[1]}, {args[0]})"
        return sql[: m.start()] + replacement + sql[i + 1 :]
