"""Trino query executor for Lakebench benchmarks.

Executes SQL queries via ``kubectl exec`` into the Trino CLI.
"""

from __future__ import annotations

import logging
import subprocess
import time

from lakebench.benchmark.result import QueryExecutorResult

logger = logging.getLogger(__name__)


class TrinoExecutor:
    """Executes queries via ``kubectl exec`` into the Trino CLI."""

    def __init__(self, namespace: str, catalog_name: str, table_format: str = "iceberg"):
        self.namespace = namespace
        self.catalog_name = catalog_name
        self.table_format = table_format
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
        if self.table_format == "delta":
            return
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
