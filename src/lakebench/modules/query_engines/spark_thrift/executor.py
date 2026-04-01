"""Spark Thrift Server query executor for Lakebench benchmarks.

Executes SQL queries via ``kubectl exec`` into beeline.
"""

from __future__ import annotations

import logging
import re
import subprocess
import time

from lakebench.benchmark.result import QueryExecutorResult

logger = logging.getLogger(__name__)


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

    def execute_query(self, sql: str, timeout: int = 300) -> QueryExecutorResult:
        pod = self._discover_pod()
        cmd = [
            "kubectl",
            "exec",
            pod,
            "-c",
            "spark-thrift",
            "-n",
            self.namespace,
            "--",
            "/opt/spark/bin/beeline",
            "-u",
            "jdbc:hive2://localhost:10000",
            "-e",
            sql,
            "--silent=true",
            "--outputformat=tsv2",
        ]

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
            error = result.stderr.strip()[:200] if result.stderr else "Unknown error"
            return QueryExecutorResult(
                sql=sql,
                engine="spark-thrift",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output=result.stdout or "",
                error=error,
            )

        output = result.stdout.strip()
        lines = output.split("\n") if output else []
        data_rows = lines[1:] if len(lines) > 1 else lines
        return QueryExecutorResult(
            sql=sql,
            engine="spark-thrift",
            duration_seconds=elapsed,
            rows_returned=len(data_rows),
            raw_output=output,
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
