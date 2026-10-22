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
from lakebench.k8s import pinned_kubectl

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

# The tag each benchmark query carries (``execute_query``), and the tag of the
# lookup query itself, which is never one of the queries it reports on.
_SOURCE_RE = re.compile(r"^lakebench-[0-9a-f]{16}$")
_IO_LOOKUP_SOURCE = "lakebench-io"
# Sources per lookup query: keeps the kubectl argv short.
_IO_LOOKUP_BATCH = 40


def _io_number(cell: str, kind: type) -> float | int | None:
    """*cell* as a number, or None for the CLI's empty/NULL spellings."""
    text = cell.strip()
    if text in ("", "\\N", "NULL", "null"):
        return None
    try:
        return kind(float(text)) if kind is int else float(text)
    except ValueError:
        return None


def _parse_io_row(line: str) -> tuple[str, dict[str, float | int | None]] | None:
    """One TSV row of the lookup in ``TrinoExecutor.query_io``, or None."""
    cells = line.rstrip("\n").split("\t")
    if len(cells) != 6 or not _SOURCE_RE.match(cells[0].strip()):
        return None
    return cells[0].strip(), {
        "tasks": _io_number(cells[1], int),
        "read_bytes": _io_number(cells[2], int),
        "write_bytes": _io_number(cells[3], int),
        "start": _io_number(cells[4], float),
        "end": _io_number(cells[5], float),
    }


def server_run_time_limit(timeout: int) -> int:
    """Seconds for ``query_max_run_time`` given the client timeout."""
    return max(1, int(timeout) - SERVER_TIMEOUT_MARGIN_SECONDS)


def _is_time_limit_error(stderr: str) -> bool:
    """True when Trino failed the query for exceeding its run-time cap.

    ``query_max_run_time`` kills the query with EXCEEDED_TIME_LIMIT; the CLI
    also prints the client-side 'maximum time limit' phrasing. Either means a
    time-limit crossing rather than a defect."""
    low = (stderr or "").lower()
    return "exceeded_time_limit" in low or "exceeded maximum time limit" in low


class TrinoExecutor:
    """Executes queries via ``kubectl exec`` into the Trino CLI."""

    def __init__(
        self,
        namespace: str,
        catalog_name: str,
        table_format: str = "iceberg",
        kube_context: str | None = None,
    ):
        self.namespace = namespace
        self.catalog_name = catalog_name
        self.table_format = table_format
        self.kube_context = kube_context or None
        self._pod: str | None = None

    def engine_name(self) -> str:
        return "trino"

    def _discover_pod(self) -> str:
        """Find the Trino coordinator pod."""
        if self._pod:
            return self._pod
        result = pinned_kubectl(
            self.kube_context,
            [
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

    def _kubectl_prefix(self) -> list[str]:
        """The ``kubectl`` argv prefix with the configured context pinned.

        With no configured context, the process's active cluster target
        supplies it (``k8s/target.py``), so a current-context switch
        mid-run does not move the queries.
        """
        from lakebench.k8s.target import cli_args

        return ["kubectl", *cli_args("kubectl", self.kube_context)]

    def _exec_cmd(self, pod: str, *trino_args: str) -> list[str]:
        return [
            *self._kubectl_prefix(),
            "exec",
            pod,
            "-c",
            "trino",
            "-n",
            self.namespace,
            "--",
            "trino",
            # Pin the session zone: timestamptz rendering, date_trunc and
            # timestamp arithmetic otherwise follow the coordinator's default.
            "--timezone",
            "UTC",
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
                        "--session",
                        f"query_max_run_time={server_run_time_limit(_CLEANUP_TIMEOUT_SECONDS)}s",
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
                engine_source=source,
                timed_out=True,
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
                engine_source=source,
                # query_max_run_time fires server-side ~5s before the client
                # timeout, so a time-limit crossing usually comes back here as
                # a non-zero exit with EXCEEDED_TIME_LIMIT, not as
                # TimeoutExpired above. Both are the same SLA crossing.
                timed_out=_is_time_limit_error(result.stderr or ""),
            )

        output = result.stdout.strip()
        rows = output.split("\n") if output else []
        return QueryExecutorResult(
            sql=sql,
            engine="trino",
            duration_seconds=elapsed,
            rows_returned=len(rows),
            raw_output=output,
            engine_source=source,
        )

    def query_io(self, sources: list[str]) -> dict[str, dict[str, float | int | None]]:
        """Storage bytes and time window of the queries run under *sources*.

        One query against ``system.runtime.tasks`` for all of them, run after
        the timed queries have finished. ``physical_input_bytes`` and
        ``physical_written_bytes`` summed over a query's tasks equal the
        query's ``physicalInputDataSize`` and ``physicalWrittenDataSize``;
        the other byte columns (processed, output, internal network) are not
        storage and are not read. A source whose query has left the
        coordinator's history is absent from the result. Never raises: a
        failed lookup returns what was read so far.
        """
        found: dict[str, dict[str, float | int | None]] = {}
        wanted = [s for s in dict.fromkeys(sources) if _SOURCE_RE.match(s)]
        try:
            pod = self._discover_pod()
        except (RuntimeError, subprocess.SubprocessError, OSError) as e:
            logger.warning("Trino I/O lookup skipped: %s", e)
            return found
        for i in range(0, len(wanted), _IO_LOOKUP_BATCH):
            batch = wanted[i : i + _IO_LOOKUP_BATCH]
            names = ", ".join(f"'{s}'" for s in batch)
            sql = (
                "SELECT q.source, count(*), sum(t.physical_input_bytes), "
                "sum(t.physical_written_bytes), to_unixtime(min(t.created)), "
                'to_unixtime(max(t."end")) '
                "FROM system.runtime.tasks t JOIN system.runtime.queries q "
                "ON t.query_id = q.query_id "
                f"WHERE q.source IN ({names}) GROUP BY q.source"
            )
            try:
                out = subprocess.run(
                    self._exec_cmd(
                        pod,
                        "--source",
                        _IO_LOOKUP_SOURCE,
                        "--session",
                        f"query_max_run_time={server_run_time_limit(_CLEANUP_TIMEOUT_SECONDS)}s",
                        "--output-format",
                        "TSV",
                        "--execute",
                        sql,
                    ),
                    capture_output=True,
                    text=True,
                    timeout=_CLEANUP_TIMEOUT_SECONDS,
                )
            except (subprocess.SubprocessError, OSError) as e:
                logger.warning("Trino I/O lookup failed: %s", e)
                continue
            if out.returncode != 0:
                logger.warning("Trino I/O lookup failed: %s", summarise_engine_error(out.stderr))
                continue
            for line in out.stdout.splitlines():
                parsed = _parse_io_row(line)
                if parsed is not None:
                    found[parsed[0]] = parsed[1]
        return found

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
            pinned_kubectl(
                self.kube_context,
                [
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
