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
        """Spark SQL is compatible with the benchmark queries; no adaptation needed."""
        return sql


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
    ):
        self.namespace = namespace
        self.catalog_name = catalog_name
        self.s3_endpoint = s3_endpoint
        self.s3_region = s3_region
        self.s3_path_style = s3_path_style
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
            'conn.execute("SET s3_use_ssl=false"); '
            "conn.execute(\"SET s3_access_key_id='\" + os.environ['AWS_ACCESS_KEY_ID'] + \"'\"); "
            "conn.execute(\"SET s3_secret_access_key='\" + os.environ['AWS_SECRET_ACCESS_KEY'] + \"'\"); "
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
        """Translate Trino-style table references to DuckDB Iceberg syntax.

        Trino:  SELECT * FROM catalog.schema.table
        DuckDB: SELECT * FROM catalog.schema.table
                (when using DuckDB Iceberg catalog attach)

        For now, DuckDB's Iceberg extension catalog mode uses the same
        dotted notation. If iceberg_scan() is needed, this method handles
        the rewrite.
        """
        return sql


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
        return DuckDBExecutor(
            namespace=ns,
            catalog_name=catalog,
            s3_endpoint=s3.endpoint,
            s3_region=s3.region,
            s3_path_style=s3.path_style,
        )
    elif engine_type == "none":
        raise ValueError("Cannot run queries without a query engine (type=none)")
    else:
        raise ValueError(f"Unknown query engine type: {engine_type}")
