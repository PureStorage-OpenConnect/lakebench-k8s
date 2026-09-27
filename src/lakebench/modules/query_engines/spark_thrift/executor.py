"""Spark Thrift Server query executor for Lakebench benchmarks.

Executes SQL queries via ``kubectl exec`` into beeline.
"""

from __future__ import annotations

import logging
import re
import subprocess
import time

from lakebench.benchmark.fingerprint import (
    Unsupported,
    fingerprint_rows,
    rows_from_beeline_tsv2,
    unusable,
)
from lakebench.benchmark.result import QueryExecutorResult, summarise_engine_error

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


class SparkThriftExecutor:
    """Executes queries via ``kubectl exec`` into beeline on the Spark Thrift Server."""

    def __init__(self, namespace: str, catalog_name: str):
        self.namespace = namespace
        self.catalog_name = catalog_name
        self._pod: str | None = None

    def engine_name(self) -> str:
        return "spark-thrift"

    def _discover_pod(self) -> str:
        """Find the Spark Thrift Server driver pod."""
        if self._pod:
            return self._pod
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                self.namespace,
                "-l",
                "app.kubernetes.io/component=spark-thrift-server",
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        pod = result.stdout.strip()
        if not pod:
            raise RuntimeError(f"No Spark Thrift Server pod found in namespace {self.namespace}")
        self._pod = pod
        return pod

    def _beeline_cmd(self, pod: str, sql: str) -> list[str]:
        return [
            "kubectl",
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

        output = result.stdout.strip()
        # tsv2 prints one header line and then a line per row, and prints
        # the header for an empty result too: a header alone is 0 rows.
        lines = output.split("\n") if output else []
        data_rows = lines[1:]
        return QueryExecutorResult(
            sql=sql,
            engine="spark-thrift",
            duration_seconds=elapsed,
            rows_returned=len(data_rows),
            raw_output=output,
        )

    def fingerprint_query(
        self, sql: str, timeout: int = 300, approx_columns: dict[int, float] | None = None
    ) -> QueryExecutorResult:
        """Run *sql* once, untimed, and fingerprint the tsv2 rows
        (benchmark.fingerprint)."""
        pod = self._discover_pod()
        start = time.monotonic()
        try:
            result = subprocess.run(
                self._beeline_cmd(pod, sql), capture_output=True, text=True, timeout=timeout
            )
        except subprocess.TimeoutExpired:
            timed_out = f"fingerprint query timed out ({timeout}s)"
            return QueryExecutorResult(
                sql=sql,
                engine="spark-thrift",
                duration_seconds=time.monotonic() - start,
                rows_returned=0,
                raw_output="",
                error=timed_out,
                fingerprint=unusable("error", timed_out, "spark-thrift"),
            )
        error: str | None = None
        if result.returncode != 0:
            error = summarise_engine_error(result.stderr or "")
            fp = unusable("error", error, "spark-thrift")
        else:
            try:
                fp = fingerprint_rows(
                    rows_from_beeline_tsv2(result.stdout or ""),
                    approx_columns,
                    engine="spark-thrift",
                    adapted_sql=sql,
                )
            except Unsupported as e:
                fp = unusable("unsupported", str(e), "spark-thrift")
        return QueryExecutorResult(
            sql=sql,
            engine="spark-thrift",
            duration_seconds=time.monotonic() - start,
            rows_returned=int(fp.get("rows") or 0),
            raw_output="",
            error=error,
            fingerprint=fp,
        )

    def health_check(self) -> bool:
        try:
            result = self.execute_query("SELECT 1", timeout=30)
            return result.success
        except Exception:
            return False

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
        depth = 1
        i = start
        while i < len(sql) and depth > 0:
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
            i += 1
        if depth != 0:
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
