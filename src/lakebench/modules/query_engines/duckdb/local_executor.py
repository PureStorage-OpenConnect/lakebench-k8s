"""DuckDB query execution for local mode.

Reuses the Kubernetes executor's SQL rewriting -- the Trino-dialect fixes and
``iceberg_scan`` substitution are identical -- and swaps ``kubectl exec`` for a
container run. Two things genuinely differ:

- **Table paths.** The hadoop catalog lays tables out as
  ``warehouse/<namespace>/<table>``, with no ``.db`` suffix on the namespace
  that the Hive layout uses.
- **Where the query runs.** There is no pod, so DuckDB runs in a throwaway
  container against the host-published Garage port.
"""

from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path

from lakebench.benchmark.result import QueryExecutorResult
from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor

logger = logging.getLogger(__name__)

# DuckDB with the iceberg and httpfs extensions preinstalled would be ideal,
# but the official image ships neither, so the script installs them on first
# use into a mounted directory that persists between queries.
DEFAULT_DUCKDB_IMAGE = "docker.io/python:3.11-slim"

# Kept in step with DuckDBConfig.version. A floating install means two runs
# weeks apart can query with different engines, which `compare` would report
# as a result rather than as drift.
DEFAULT_DUCKDB_VERSION = "1.5.5"


class LocalDuckDBExecutor(DuckDBExecutor):
    """Runs benchmark queries against local Iceberg tables via DuckDB."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        warehouse_bucket: str,
        region: str = "us-east-1",
        catalog_name: str = "lb",
        table_names: dict[str, str] | None = None,
        cli: str = "podman",
        image: str = DEFAULT_DUCKDB_IMAGE,
        workdir: str = "",
        duckdb_version: str = DEFAULT_DUCKDB_VERSION,
    ) -> None:
        super().__init__(
            namespace="",
            catalog_name=catalog_name,
            s3_endpoint=endpoint,
            s3_region=region,
            s3_path_style=True,
            s3_buckets={},
            table_names=table_names,
            table_format="iceberg",
            catalog_type="hadoop",
        )
        self.access_key = access_key
        self.secret_key = secret_key
        self.warehouse_bucket = warehouse_bucket
        self.cli = cli
        self.image = image
        self.workdir = workdir
        self.duckdb_version = duckdb_version
        # Filled in from the query payload: what the container actually loaded,
        # as opposed to what was asked for.
        self._reported_version = ""

    def engine_name(self) -> str:
        return "duckdb"

    def health_check(self) -> bool:
        result = self.execute_query("SELECT 1")
        return result.success

    def running_version(self) -> str:
        """The DuckDB version the container actually loaded.

        Asked of the engine rather than read from config: the pin is a request,
        and only the running process can confirm it was honoured. This is the
        check that would have caught the engine drifting under an unpinned
        install.

        Returns an empty string if the query failed, so callers can distinguish
        "could not ask" from a version mismatch.
        """
        result = self.execute_query("SELECT 1")
        if not result.success:
            return ""
        return self._reported_version

    def adapt_query(self, sql: str) -> str:
        """Rewrite catalog-qualified names to iceberg_scan over the local paths.

        Overridden rather than reusing the parent: the parent maps each layer to
        its own bucket, but local mode keeps one warehouse root, and the hadoop
        layout has no ``.db`` suffix.
        """
        for fq_name in self.table_names.values():
            parts = fq_name.split(".", 1)
            if len(parts) != 2:
                continue
            namespace, table = parts
            catalog_ref = f"{self.catalog_name}.{fq_name}"
            path = f"s3://{self.warehouse_bucket}/warehouse/{namespace}/{table}"
            scan = f"iceberg_scan('{path}', allow_moved_paths := true)"
            sql = sql.replace(catalog_ref, scan)

        sql = self._rewrite_date_add(sql)
        sql = self._rewrite_date_diff(sql)
        return sql

    def _build_local_script(self, sql: str) -> str:
        """Build the Python program DuckDB runs inside the container.

        The SQL is read from a sibling file rather than embedded. Benchmark
        queries contain both quote styles, and every attempt to escape them
        through a shell ``python -c`` argument produces a syntax error whose
        only visible symptom is a bare caret.
        """
        host = self.s3_endpoint.replace("http://", "").replace("https://", "")
        return "\n".join(
            [
                "import duckdb, json",
                "sql = open('/duckdb/query.sql').read()",
                "conn = duckdb.connect()",
                "conn.execute('INSTALL iceberg'); conn.load_extension('iceberg')",
                "conn.execute('INSTALL httpfs'); conn.load_extension('httpfs')",
                f"conn.execute(\"SET s3_endpoint='{host}'\")",
                f"conn.execute(\"SET s3_region='{self.s3_region}'\")",
                "conn.execute(\"SET s3_url_style='path'\")",
                'conn.execute("SET s3_use_ssl=false")',
                f"conn.execute(\"SET s3_access_key_id='{self.access_key}'\")",
                f"conn.execute(\"SET s3_secret_access_key='{self.secret_key}'\")",
                "conn.execute('SET unsafe_enable_version_guessing = true')",
                "rows = conn.execute(sql).fetchall()",
                # Report the engine version alongside every result. The pin is
                # only a request; this is what proves the container honoured it.
                "ver = conn.execute('SELECT version()').fetchone()[0]",
                "print(json.dumps({'rows': len(rows), 'version': ver, "
                "'data': [str(r) for r in rows[:100]]}))",
            ]
        )

    def execute_query(self, sql: str, timeout: int = 300) -> QueryExecutorResult:
        if not self.workdir:
            raise ValueError("LocalDuckDBExecutor needs a workdir to stage the query")

        # Both files are staged on the host and read inside the container, so
        # nothing has to survive shell quoting.
        stage = Path(self.workdir)
        stage.mkdir(parents=True, exist_ok=True)
        (stage / "query.sql").write_text(sql)
        (stage / "run.py").write_text(self._build_local_script(sql))

        cmd = [
            self.cli,
            "run",
            "--rm",
            "--network",
            "host",
            "-e",
            "HOME=/tmp",
            # Keep the pip install and DuckDB extensions between queries;
            # reinstalling per query would dominate every measurement.
            *(["-v", f"{self.workdir}:/duckdb:z"] if self.workdir else []),
            "-e",
            "PYTHONUSERBASE=/duckdb/py",
            "-e",
            "DUCKDB_HOME=/duckdb",
            self.image,
            "sh",
            "-c",
            f"pip install --quiet --user duckdb=={self.duckdb_version} 2>/dev/null; "
            "python /duckdb/run.py",
        ]

        start = time.monotonic()
        try:
            proc = subprocess.run(  # noqa: S603
                cmd, capture_output=True, text=True, timeout=timeout, check=False
            )
        except subprocess.TimeoutExpired:
            return QueryExecutorResult(
                sql=sql,
                engine="duckdb",
                duration_seconds=time.monotonic() - start,
                rows_returned=0,
                raw_output="",
                error=f"Query exceeded {timeout}s",
            )

        elapsed = time.monotonic() - start
        output = (proc.stdout or "").strip()

        if proc.returncode != 0:
            return QueryExecutorResult(
                sql=sql,
                engine="duckdb",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output=output,
                error=_summarise_duckdb_error(proc.stderr or output),
            )

        payload = None
        for line in reversed(output.splitlines()):
            if line.startswith("{"):
                try:
                    payload = json.loads(line)
                    break
                except json.JSONDecodeError:
                    continue

        # A zero exit with no result payload means the query never ran. Treating
        # that as success produces a timing that measures container startup and
        # nothing else.
        if payload is None:
            return QueryExecutorResult(
                sql=sql,
                engine="duckdb",
                duration_seconds=elapsed,
                rows_returned=0,
                raw_output=output,
                error="Query produced no result (it did not run)",
            )

        self._reported_version = str(payload.get("version", "")).lstrip("v")

        return QueryExecutorResult(
            sql=sql,
            engine="duckdb",
            duration_seconds=elapsed,
            rows_returned=payload.get("rows", 0),
            raw_output=output,
        )


def _summarise_duckdb_error(text: str) -> str:
    """Return the meaningful line from a DuckDB or Python traceback.

    Scanning backwards finds the caret line a parser error ends with, which
    carries no information at all. Prefer the named exception, then any line
    with substance.
    """
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]

    for line in lines:
        if "Error:" in line or "Exception:" in line:
            return line[:300]

    for line in reversed(lines):
        # A caret-only line points at a column in the line above it.
        if set(line) <= {"^", " "}:
            continue
        if line.startswith(("File ", "Traceback")):
            continue
        return line[:300]

    return text.strip()[-300:] if text.strip() else "Query failed with no output"
