"""Trino query executor for Lakebench benchmarks.

Executes SQL queries via ``kubectl exec`` into the Trino CLI.
"""

from __future__ import annotations

import logging
import re
import subprocess
import time
import uuid

from lakebench.benchmark.result import QueryExecutorResult, summarise_engine_error

logger = logging.getLogger(__name__)

# The server ends a query this many seconds before the client gives up on it.
# Killing the local ``kubectl exec`` on a client timeout leaves the trino CLI
# and its query running in the pod, holding worker memory and slowing every
# later query; with query_max_run_time just below the client timeout the
# server fails the query first and the client reads a clean error.
SERVER_TIMEOUT_MARGIN_SECONDS = 5
# Bound on each cleanup call after a client timeout (lookup, then one kill
# per id), so a saturated coordinator cannot stretch a timed-out probe loop.
_CLEANUP_TIMEOUT_SECONDS = 10

_QUERY_ID_RE = re.compile(r"^[0-9]{8}_[0-9]{6}_[0-9]{5}_[0-9a-z]{5}$")


def server_run_time_limit(timeout: int) -> int:
    """Seconds for ``query_max_run_time`` given the client timeout."""
    return max(1, int(timeout) - SERVER_TIMEOUT_MARGIN_SECONDS)


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

    def _exec_cmd(self, pod: str, *trino_args: str) -> list[str]:
        return [
            "kubectl",
            "exec",
            pod,
            "-c",
            "trino",
            "-n",
            self.namespace,
            "--",
            "trino",
            *trino_args,
        ]

    def _kill_by_source(self, pod: str, source: str) -> None:
        """Cancel, server side, every live query submitted with *source*.

        Backstop for the session run-time limit: the client timed out, so
        whatever still runs under this source is an orphan. If the CLI had
        not registered the query yet, the lookup finds nothing and the
        session limit still ends it.
        """
        try:
            listed = subprocess.run(
                self._exec_cmd(
                    pod,
                    "--session",
                    f"query_max_run_time={server_run_time_limit(_CLEANUP_TIMEOUT_SECONDS)}s",
                    "--output-format",
                    "TSV",
                    "--execute",
                    "SELECT query_id FROM system.runtime.queries "
                    f"WHERE source = '{source}' AND state NOT IN ('FINISHED', 'FAILED')",
                ),
                capture_output=True,
                text=True,
                timeout=_CLEANUP_TIMEOUT_SECONDS,
            )
            ids = [q for q in listed.stdout.split() if _QUERY_ID_RE.match(q)]
            for query_id in ids:
                subprocess.run(
                    self._exec_cmd(
                        pod,
                        "--execute",
                        f"CALL system.runtime.kill_query(query_id => '{query_id}', "
                        "message => 'lakebench client timeout')",
                    ),
                    capture_output=True,
                    text=True,
                    timeout=_CLEANUP_TIMEOUT_SECONDS,
                )
            if ids:
                logger.warning("Cancelled %d orphaned Trino query(s): %s", len(ids), ids)
        except (subprocess.SubprocessError, OSError) as e:
            logger.warning("Could not cancel timed-out Trino query (source %s): %s", source, e)

    def execute_query(self, sql: str, timeout: int = 300) -> QueryExecutorResult:
        pod = self._discover_pod()
        # A unique source tags this query so a timeout can find and cancel it.
        source = f"lakebench-{uuid.uuid4().hex[:16]}"
        cmd = self._exec_cmd(
            pod,
            "--source",
            source,
            "--session",
            f"query_max_run_time={server_run_time_limit(timeout)}s",
            "--execute",
            sql,
        )

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
            self._kill_by_source(pod, source)
            return QueryExecutorResult(
                sql=sql,
                engine="trino",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output="",
                error=f"Query timed out ({timeout}s)",
            )

        if result.returncode != 0:
            error = summarise_engine_error(result.stderr or "")
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
