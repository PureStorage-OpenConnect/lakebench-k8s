"""DuckDB query executor for Lakebench benchmarks.

Executes SQL queries via ``kubectl exec`` into the DuckDB pod
using a Python one-liner with the duckdb module.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time

from lakebench.benchmark.result import QueryExecutorResult

logger = logging.getLogger(__name__)


class DuckDBExecutor:
    """Executes queries via ``kubectl exec`` into the DuckDB pod."""

    def __init__(
        self,
        namespace: str,
        catalog_name: str,
        s3_endpoint: str = "",
        s3_region: str = "us-east-1",
        s3_path_style: bool = True,
        s3_buckets: dict[str, str] | None = None,
        table_names: dict[str, str] | None = None,
        table_format: str = "iceberg",
        catalog_type: str = "hive",
    ):
        self.namespace = namespace
        self.catalog_name = catalog_name
        self.table_format = table_format
        self.catalog_type = catalog_type
        self.s3_endpoint = s3_endpoint
        self.s3_region = s3_region
        self.s3_path_style = s3_path_style
        self.s3_buckets = s3_buckets or {}
        self.table_names = table_names or {}
        self._pod: str | None = None

    def engine_name(self) -> str:
        return "duckdb"

    def _discover_pod(self) -> str:
        """Find the DuckDB pod."""
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
                "app.kubernetes.io/component=duckdb",
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        pod = result.stdout.strip()
        if not pod:
            raise RuntimeError(f"No DuckDB pod found in namespace {self.namespace}")
        self._pod = pod
        return pod

    def _build_python_script(self, sql: str) -> str:
        """Build a Python one-liner that executes SQL via duckdb."""
        sql = " ".join(sql.split())
        escaped_sql = sql.replace("'", "\\'")
        return (
            "import duckdb, json, os; "
            "conn = duckdb.connect(); "
            f"conn.load_extension('{'delta' if self.table_format == 'delta' else 'iceberg'}'); "
            "conn.load_extension('httpfs'); "
            f"conn.execute(\"SET s3_endpoint='{self.s3_endpoint.replace('http://', '').replace('https://', '')}'\"); "
            f"conn.execute(\"SET s3_region='{self.s3_region}'\"); "
            f"conn.execute(\"SET s3_url_style='{'path' if self.s3_path_style else 'vhost'}'\"); "
            f'conn.execute("SET s3_use_ssl={"true" if self.s3_endpoint.startswith("https://") else "false"}"); '
            "conn.execute(\"SET s3_access_key_id='\" + os.environ['AWS_ACCESS_KEY_ID'] + \"'\"); "
            "conn.execute(\"SET s3_secret_access_key='\" + os.environ['AWS_SECRET_ACCESS_KEY'] + \"'\"); "
            "conn.execute('SET unsafe_enable_version_guessing = true'); "
            f"result = conn.execute('{escaped_sql}'); "
            "rows = result.fetchall(); "
            "print(json.dumps({'rows': len(rows), 'data': [str(r) for r in rows[:100]]}))"
        )

    def execute_query(self, sql: str, timeout: int = 300) -> QueryExecutorResult:
        pod = self._discover_pod()
        script = self._build_python_script(sql)
        cmd = [
            "kubectl",
            "exec",
            pod,
            "-n",
            self.namespace,
            "--",
            "python",
            "-c",
            script,
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
                engine="duckdb",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output="",
                error=f"Query timed out ({timeout}s)",
            )

        if result.returncode != 0:
            error = result.stderr.strip()[:200] if result.stderr else "Unknown error"
            return QueryExecutorResult(
                sql=sql,
                engine="duckdb",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output=result.stdout or "",
                error=error,
            )

        output = result.stdout.strip()
        try:
            parsed = json.loads(output)
            row_count = parsed.get("rows", 0)
        except (json.JSONDecodeError, KeyError):
            lines = output.split("\n") if output else []
            row_count = len(lines)

        return QueryExecutorResult(
            sql=sql,
            engine="duckdb",
            duration_seconds=elapsed,
            rows_returned=row_count,
            raw_output=output,
        )

    def health_check(self) -> bool:
        try:
            result = self.execute_query("SELECT 1", timeout=15)
            return result.success
        except Exception:
            return False

    def flush_cache(self) -> None:
        """DuckDB has no persistent cache between connections."""
        pass

    def adapt_query(self, sql: str) -> str:
        """Rewrite Trino SQL to DuckDB dialect."""
        for layer, fq_name in self.table_names.items():
            bucket = self.s3_buckets.get(layer, "")
            if not bucket or not fq_name:
                continue
            parts = fq_name.split(".", 1)
            if len(parts) != 2:
                continue
            namespace, table = parts
            catalog_ref = f"{self.catalog_name}.{fq_name}"
            # Hive: tables at s3://bucket/warehouse/namespace.db/table
            # Polaris: tables at s3://bucket/namespace/table
            if self.catalog_type == "polaris":
                warehouse_path = f"s3://{bucket}/{namespace}/{table}"
            else:
                warehouse_path = f"s3://{bucket}/warehouse/{namespace}.db/{table}"
            if self.table_format == "delta":
                scan_expr = f"delta_scan('{warehouse_path}')"
            else:
                scan_expr = f"iceberg_scan('{warehouse_path}', allow_moved_paths := true)"
            sql = sql.replace(catalog_ref, scan_expr)

        sql = self._rewrite_date_add(sql)
        sql = self._rewrite_date_diff(sql)
        return sql

    @staticmethod
    def _rewrite_date_diff(sql: str) -> str:
        """Rewrite Trino ``DATE_DIFF(...)`` to DuckDB ``DATEDIFF(...)``."""
        return re.sub(r"\bDATE_DIFF\s*\(", "DATEDIFF(", sql, flags=re.IGNORECASE)

    @staticmethod
    def _rewrite_date_add(sql: str) -> str:
        """Rewrite Trino ``date_add('month', N, expr)`` to ``(expr + INTERVAL N MONTH)``."""
        pattern = re.compile(r"date_add\(\s*'(\w+)'\s*,\s*(\d+)\s*,\s*", re.IGNORECASE)
        m = pattern.search(sql)
        if not m:
            return sql
        unit = m.group(1).upper()
        n = m.group(2)
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
        replacement = f"({expr} + INTERVAL {n} {unit})"
        return sql[: m.start()] + replacement + sql[i:]
