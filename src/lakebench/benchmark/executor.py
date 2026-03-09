"""Query executor abstraction for Lakebench benchmarks.

Provides a protocol for executing SQL queries against different engines
(Trino, Spark Thrift Server, DuckDB) via ``kubectl exec``.

Usage::

    from lakebench.benchmark.executor import get_executor

    executor = get_executor(config, namespace)
    result = executor.execute_query("SELECT 1", timeout=30)
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from lakebench.config.schema import LakebenchConfig

logger = logging.getLogger(__name__)


@dataclass
class QueryExecutorResult:
    """Result of a single query execution via an engine executor."""

    sql: str
    engine: str
    duration_seconds: float
    rows_returned: int
    raw_output: str
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


class QueryExecutor(Protocol):
    """Protocol for query engine executors."""

    catalog_name: str

    def execute_query(self, sql: str, timeout: int = 300) -> QueryExecutorResult: ...

    def engine_name(self) -> str: ...

    def health_check(self) -> bool: ...

    def flush_cache(self) -> None: ...

    def adapt_query(self, sql: str) -> str: ...


class TrinoExecutor:
    """Executes queries via ``kubectl exec`` into the Trino CLI."""

    def __init__(self, namespace: str, catalog_name: str):
        self.namespace = namespace
        self.catalog_name = catalog_name
        self._pod: str | None = None

    def engine_name(self) -> str:
        return "trino"

    def _discover_pod(self) -> str:
        """Find the Trino coordinator pod."""
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
                "app.kubernetes.io/component=trino-coordinator",
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        pod = result.stdout.strip()
        if not pod:
            raise RuntimeError(f"No Trino coordinator pod found in namespace {self.namespace}")
        self._pod = pod
        return pod

    def execute_query(self, sql: str, timeout: int = 300) -> QueryExecutorResult:
        pod = self._discover_pod()
        cmd = [
            "kubectl",
            "exec",
            pod,
            "-c",
            "trino",
            "-n",
            self.namespace,
            "--",
            "trino",
            "--execute",
            sql,
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
                engine="trino",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output="",
                error=f"Query timed out ({timeout}s)",
            )

        if result.returncode != 0:
            error = result.stderr.strip()[:200] if result.stderr else "Unknown error"
            return QueryExecutorResult(
                sql=sql,
                engine="trino",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output=result.stdout or "",
                error=error,
            )

        output = result.stdout.strip()
        rows = output.split("\n") if output else []
        return QueryExecutorResult(
            sql=sql,
            engine="trino",
            duration_seconds=elapsed,
            rows_returned=len(rows),
            raw_output=output,
        )

    def health_check(self) -> bool:
        try:
            result = self.execute_query("SELECT 1", timeout=15)
            return result.success
        except Exception:
            return False

    def flush_cache(self) -> None:
        """Flush Trino's Iceberg metadata cache."""
        try:
            pod = self._discover_pod()
            subprocess.run(
                [
                    "kubectl",
                    "exec",
                    pod,
                    "-c",
                    "trino",
                    "-n",
                    self.namespace,
                    "--",
                    "trino",
                    "--execute",
                    "CALL iceberg.system.flush_metadata_cache()",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            logger.warning("Failed to flush Trino metadata cache")

    def adapt_query(self, sql: str) -> str:
        """Trino SQL is the canonical dialect; no adaptation needed."""
        return sql


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
        # beeline tsv2 includes a header row; skip it for row count
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
        """Spark Thrift Server has no explicit metadata cache flush.

        Spark refreshes table metadata on each query. This is a no-op.
        """
        pass

    def adapt_query(self, sql: str) -> str:
        """Translate Trino SQL dialect to Spark SQL where they differ.

        Trino is the canonical query dialect for benchmark queries.  Two
        functions use incompatible signatures:

        - ``date_add('month', N, expr)`` (Trino 3-arg) -> ``add_months(expr, N)``
        - ``DATE_DIFF('day', expr1, expr2)`` (Trino 3-arg) -> ``DATEDIFF(expr2, expr1)``
          Note: Trino DATE_DIFF arg order is (unit, start, end) while Spark
          DATEDIFF is (end, start).
        """
        sql = self._rewrite_date_add(sql)
        sql = self._rewrite_date_diff(sql)

        return sql

    @staticmethod
    def _rewrite_date_add(sql: str) -> str:
        """Rewrite Trino ``date_add('month', N, expr)`` to Spark ``add_months(expr, N)``."""
        import re

        pattern = re.compile(
            r"date_add\(\s*'month'\s*,\s*(\d+)\s*,\s*",
            re.IGNORECASE,
        )
        m = pattern.search(sql)
        if not m:
            return sql
        n = m.group(1)
        # Find the matching closing paren for this call
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
        import re

        pattern = re.compile(
            r"DATE_DIFF\(\s*'day'\s*,\s*",
            re.IGNORECASE,
        )
        m = pattern.search(sql)
        if not m:
            return sql
        # Parse the two comma-separated arguments after the match
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
        # Spark DATEDIFF(end, start) -- note reversed arg order
        replacement = f"DATEDIFF({args[1]}, {args[0]})"
        return sql[: m.start()] + replacement + sql[i + 1 :]


class DuckDBExecutor:
    """Executes queries via ``kubectl exec`` into the DuckDB pod.

    DuckDB runs as a single pod with ``sleep infinity``. Each query invocation
    creates a fresh ``duckdb.connect()`` via a Python one-liner executed through
    ``kubectl exec``.
    """

    def __init__(
        self,
        namespace: str,
        catalog_name: str,
        s3_endpoint: str = "",
        s3_region: str = "us-east-1",
        s3_path_style: bool = True,
        s3_buckets: dict[str, str] | None = None,
        table_names: dict[str, str] | None = None,
    ):
        self.namespace = namespace
        self.catalog_name = catalog_name
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
        # Collapse multi-line SQL into a single line (the script is a one-liner,
        # so newlines inside the SQL string literal would break Python parsing).
        sql = " ".join(sql.split())
        # Escape single quotes in SQL for embedding in Python string
        escaped_sql = sql.replace("'", "\\'")
        return (
            "import duckdb, json, os; "
            "conn = duckdb.connect(); "
            "conn.load_extension('iceberg'); "
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
        # Parse JSON output from the Python script
        try:
            import json

            parsed = json.loads(output)
            row_count = parsed.get("rows", 0)
        except (json.JSONDecodeError, KeyError):
            # Fallback: count lines
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
        """DuckDB has no persistent cache between connections.

        Each kubectl exec creates a fresh connection, so cache is
        implicitly flushed. This is a no-op.
        """
        pass

    def adapt_query(self, sql: str) -> str:
        """Rewrite Trino SQL to DuckDB dialect.

        Two kinds of rewrites:
        1. Table references: ``catalog.namespace.table`` -> ``iceberg_scan(...)``
        2. Dialect: Trino ``date_add('month', N, expr)`` -> DuckDB ``(expr + INTERVAL N MONTH)``
        """
        # -- Table reference rewrites --
        for layer, fq_name in self.table_names.items():
            bucket = self.s3_buckets.get(layer, "")
            if not bucket or not fq_name:
                continue
            parts = fq_name.split(".", 1)
            if len(parts) != 2:
                continue
            namespace, table = parts
            catalog_ref = f"{self.catalog_name}.{fq_name}"
            warehouse_path = f"s3://{bucket}/warehouse/{namespace}.db/{table}"
            scan_expr = f"iceberg_scan('{warehouse_path}', allow_moved_paths := true)"
            sql = sql.replace(catalog_ref, scan_expr)

        # -- Dialect rewrites --
        sql = self._rewrite_date_add(sql)
        return sql

    @staticmethod
    def _rewrite_date_add(sql: str) -> str:
        """Rewrite Trino ``date_add('month', N, expr)`` to ``(expr + INTERVAL N MONTH)``.

        DuckDB's date_add macro does not accept the 3-arg Trino form.
        """
        import re

        pattern = re.compile(r"date_add\(\s*'(\w+)'\s*,\s*(\d+)\s*,\s*", re.IGNORECASE)
        m = pattern.search(sql)
        if not m:
            return sql
        unit = m.group(1).upper()
        n = m.group(2)
        # Find the matching closing paren for the 3rd argument
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


def get_executor(config: LakebenchConfig, namespace: str | None = None) -> QueryExecutor:
    """Factory: return the appropriate QueryExecutor for the configured engine.

    Args:
        config: Lakebench configuration.
        namespace: Override namespace (default: from config).

    Returns:
        A QueryExecutor instance.

    Raises:
        ValueError: If ``query_engine.type`` is ``none``.
    """
    ns = namespace or config.get_namespace()
    engine_type = config.architecture.query_engine.type.value

    if engine_type == "trino":
        catalog = config.architecture.query_engine.trino.catalog_name
        return TrinoExecutor(namespace=ns, catalog_name=catalog)
    elif engine_type == "spark-thrift":
        catalog = config.architecture.query_engine.spark_thrift.catalog_name
        return SparkThriftExecutor(namespace=ns, catalog_name=catalog)
    elif engine_type == "duckdb":
        s3 = config.platform.storage.s3
        catalog = config.architecture.query_engine.duckdb.catalog_name
        tables = config.architecture.tables
        return DuckDBExecutor(
            namespace=ns,
            catalog_name=catalog,
            s3_endpoint=s3.endpoint,
            s3_region=s3.region,
            s3_path_style=s3.path_style,
            s3_buckets={
                "silver": s3.buckets.silver,
                "gold": s3.buckets.gold,
            },
            table_names={
                "silver": tables.silver,
                "gold": tables.gold,
            },
        )
    elif engine_type == "none":
        raise ValueError("Cannot run queries without a query engine (type=none)")
    else:
        raise ValueError(f"Unknown query engine type: {engine_type}")
