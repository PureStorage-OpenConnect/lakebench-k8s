"""Iceberg table maintenance helpers.

Provides engine-aware ``expire_snapshots`` and ``remove_orphan_files``
operations that work with Trino or Spark Thrift Server.  DuckDB is
read-only and cannot run Iceberg maintenance.

Used by the continuous monitoring loop and the pre-benchmark batch
maintenance (cli/_sustained.py). Destroy only drops the tables: its buckets
are emptied and deleted right after, so snapshot and orphan maintenance
there would only cost time.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig
    from lakebench.k8s import K8sClient

logger = logging.getLogger(__name__)

# Label selectors for discovering engine pods
_TRINO_SELECTOR = "app=lakebench-trino,component=coordinator"
_SPARK_THRIFT_SELECTOR = "app.kubernetes.io/component=spark-thrift-server"


def find_maintenance_engine(
    cfg: LakebenchConfig,
    namespace: str,
) -> tuple[str | None, str | None, str | None]:
    """Discover which query engine pod can run Iceberg maintenance.

    Returns ``(engine, pod_name, catalog)`` or ``(None, None, None)``.
    Priority: Trino > Spark Thrift.  DuckDB cannot run maintenance.
    """
    from kubernetes import client as k8s_client

    engine_type = cfg.architecture.query_engine.type.value
    core_v1 = k8s_client.CoreV1Api()

    # Try configured engine first, then fall back to the other.
    # DuckDB cannot run maintenance.
    engines_to_try: list[str] = []
    if engine_type in ("trino", "spark-thrift"):
        engines_to_try.append(engine_type)
    # Add fallback engine
    if engine_type != "trino":
        engines_to_try.append("trino")
    if engine_type != "spark-thrift":
        engines_to_try.append("spark-thrift")

    for engine in engines_to_try:
        if engine == "trino":
            try:
                pods = core_v1.list_namespaced_pod(
                    namespace,
                    label_selector=_TRINO_SELECTOR,
                )
                if pods.items:
                    catalog = cfg.architecture.query_engine.trino.catalog_name
                    if engine != engine_type:
                        logger.info(
                            "Maintenance: falling back to Trino (configured engine %s unavailable)",
                            engine_type,
                        )
                    return "trino", pods.items[0].metadata.name, catalog
            except Exception as e:
                logger.warning("Iceberg maintenance: Trino pod lookup failed: %s", e)

        elif engine == "spark-thrift":
            try:
                pods = core_v1.list_namespaced_pod(
                    namespace,
                    label_selector=_SPARK_THRIFT_SELECTOR,
                )
                if pods.items:
                    table_format = cfg.architecture.table_format.type.value
                    if table_format == "delta":
                        catalog = "spark_catalog"
                    else:
                        catalog = cfg.architecture.query_engine.spark_thrift.catalog_name
                    if engine != engine_type:
                        logger.info(
                            "Maintenance: falling back to Spark Thrift (configured engine %s unavailable)",
                            engine_type,
                        )
                    return "spark-thrift", pods.items[0].metadata.name, catalog
            except Exception as e:
                logger.warning(
                    "Iceberg maintenance: Spark Thrift pod lookup failed: %s",
                    e,
                )

    return None, None, None


def _parse_threshold_seconds(retention_threshold: str) -> int:
    """Parse a Trino-style duration string to seconds.

    Supports ``s`` (seconds), ``m`` (minutes), ``h`` (hours), ``d`` (days).
    """
    m = re.fullmatch(r"\s*(\d+)\s*([smhdSMHD])\s*", retention_threshold or "")
    if not m:
        # Never guess: an unknown unit used to read as minutes ("7D" -> 7 min).
        raise ValueError(
            f"retention threshold {retention_threshold!r} is not a whole number and one "
            "unit (s, m, h, d)"
        )
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return int(m.group(1)) * multipliers[m.group(2).lower()]


# Orphan removal never runs below 24 h plus a 10 min margin, on any engine or
# path: racing a writer it deletes files a commit is about to use (Iceberg's
# Spark procedure refuses under 24 h for that reason), and stream apps with
# restartPolicy Always can be writing even when a run believes none are.
ORPHAN_MIN_RETENTION_SECONDS = 24 * 3600 + 600
# Floor for expire_snapshots while streams are live, so a stream's reader is
# never left without the snapshot it is positioned on.
LIVE_EXPIRE_MIN_RETENTION_SECONDS = 3600


def _format_duration(seconds: int) -> str:
    """Seconds as a whole Trino duration in hours, minutes or seconds ("0s", "30m", "24h")."""
    for unit, size in (("h", 3600), ("m", 60)):
        if seconds and seconds % size == 0:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


def _spark_timestamp(seconds_ago: int, now: datetime | None = None) -> str:
    """A Spark TIMESTAMP literal *seconds_ago* before now, with an explicit
    +00:00 offset so it does not depend on the Thrift session time zone."""
    now = now or datetime.now(timezone.utc)
    ts = (now - timedelta(seconds=seconds_ago)).astimezone(timezone.utc)
    return f"TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S')}+00:00'"


def build_maintenance_sql(
    engine: str,
    catalog: str,
    table: str,
    retention_threshold: str,
    orphan_retention: str | None = None,
    now: datetime | None = None,
) -> list[str]:
    """Build expire_snapshots + remove_orphan_files SQL for the given engine.

    ``orphan_retention`` defaults to ``retention_threshold`` and is never
    below ``ORPHAN_MIN_RETENTION_SECONDS`` (24 h + 10 min).

    Trino: ``ALTER TABLE ... EXECUTE`` with a duration, prefixed in the same
    submission by ``SET SESSION <catalog>.<proc>_min_retention`` equal to the
    requested threshold. Without it Trino refuses anything under its 7-day
    system minimum ("Retention specified (30.00m) is shorter than the
    minimum retention configured in the system (7.00d)"), verified live
    2026-09-26; the SET only lasts for its own CLI process, hence one
    submission (a single ``trino --execute "SET SESSION ...; ALTER TABLE
    ..."`` succeeded live for both procedures, 2026-09-26). Spark:
    ``CALL <catalog>.system.<proc>`` with a TIMESTAMP literal computed here
    in UTC with an explicit ``+00:00`` offset; the old ``CAST((UNIX_TIMESTAMP() - N) *
    1000 AS BIGINT)`` always failed "number of args and params must match
    after binding" (live, 2026-09-26).
    """
    expire_s = _parse_threshold_seconds(retention_threshold)
    # The orphan floor is enforced here, whatever the caller passes.
    orphan_s = max(
        _parse_threshold_seconds(orphan_retention or retention_threshold),
        ORPHAN_MIN_RETENTION_SECONDS,
    )
    if engine == "trino":
        expire_d = _format_duration(expire_s)
        orphan_d = _format_duration(orphan_s)
        return [
            (
                f"SET SESSION {catalog}.expire_snapshots_min_retention = '{expire_d}'; "
                f"ALTER TABLE {table} EXECUTE "
                f"expire_snapshots(retention_threshold => '{expire_d}')"
            ),
            (
                f"SET SESSION {catalog}.remove_orphan_files_min_retention = '{orphan_d}'; "
                f"ALTER TABLE {table} EXECUTE "
                f"remove_orphan_files(retention_threshold => '{orphan_d}')"
            ),
        ]
    if engine == "spark-thrift":
        return [
            (
                f"CALL {catalog}.system.expire_snapshots"
                f"(table => '{table}', older_than => {_spark_timestamp(expire_s, now)})"
            ),
            (
                f"CALL {catalog}.system.remove_orphan_files"
                f"(table => '{table}', older_than => {_spark_timestamp(orphan_s, now)})"
            ),
        ]
    return []


#: Trino's optimize threshold when the caller names none.
DEFAULT_FILE_SIZE_THRESHOLD = "128MB"


def compaction_operation(
    engine: str, file_size_threshold: str = DEFAULT_FILE_SIZE_THRESHOLD
) -> dict[str, Any] | None:
    """What ``build_compaction_sql`` runs for *engine*, as the experiment
    records it: ``{"operation", "params"}`` (Trino ``optimize`` with its
    file size threshold; Spark Thrift ``rewrite_data_files`` with Iceberg's
    defaults), or None for an engine that runs none. Kept next to the
    builder so the record names what the statement does."""
    if engine == "trino":
        return {
            "operation": "trino_optimize",
            "params": {"file_size_threshold": file_size_threshold},
        }
    if engine == "spark-thrift":
        return {"operation": "iceberg_rewrite_data_files", "params": {}}
    return None


def build_compaction_sql(
    engine: str,
    catalog: str,
    table: str,
    file_size_threshold: str = DEFAULT_FILE_SIZE_THRESHOLD,
) -> list[str]:
    """Build Iceberg compaction SQL (rewrite_data_files / optimize).

    Compaction merges small files produced by streaming micro-batches or
    repeated incremental writes into larger, better-sized files.  This is
    heavier than expire_snapshots and should run less frequently.
    """
    if engine == "trino":
        return [
            (
                f"ALTER TABLE {table} EXECUTE "
                f"optimize(file_size_threshold => '{file_size_threshold}')"
            ),
        ]
    if engine == "spark-thrift":
        return [
            (f"CALL {catalog}.system.rewrite_data_files(table => '{table}')"),
        ]
    return []


@dataclass(frozen=True)
class CompactionPartitioning:
    """How a Trino compaction chunks one Lakebench table.

    ``column`` is the source column the ``optimize ... WHERE`` ranges on;
    ``transform`` the table's partition transform on it (``identity`` or
    ``month``); ``chunk`` the most partitions one statement rewrites.
    """

    column: str
    transform: str
    chunk: int

    @property
    def partition_field(self) -> str:
        """The field name in Trino's ``$partitions.partition`` row: the
        column itself for identity, Iceberg's default ``<column>_month``
        for months()."""
        return self.column if self.transform == "identity" else f"{self.column}_{self.transform}"


# Partitions per Trino optimize statement on an identity-partitioned table.
# Trino's Iceberg connector refuses a write that opens more than
# max_partitions_per_writer (default 100) writers: "Exceeded limit of 100
# open writers for partitions: 101" (seen in run-20260929-204941-1d17f4).
# What trips it is the number of partitions one optimize rewrites, not the
# number the table holds: batch C360 s1 silver (366 interaction_date
# partitions, a few large files each) compacted in one statement
# (run-20260929-212900-5105a0), while continuous silver, with small
# micro-batch files in every partition, did not. The plan chunks every table
# above 90 partitions anyway, so a run never depends on how many of them hold
# small files; 90 leaves 10 writers of margin below the default. (Both runs
# had 366 distinct interaction dates, c360_correctness distinct_dates.)
COMPACTION_CHUNK_PARTITIONS = 90

# Months per Trino optimize statement on a months()-partitioned AML table.
# Each partition a statement rewrites keeps an open Parquet writer
# that buffers up to a row group (parquet_writer_block_size, 128 MB) before
# it flushes, so writer memory grows with the partitions written at once,
# not with the table: continuous AML s1 silver.transactions at about 4,320 s
# (12 to 13 months of small micro-batch files) failed one unchunked optimize
# on "Query exceeded per-node memory limit of 2.24GB [TableWriterOperator=
# 2.06GB ...]" (lb17-qr32-cont, run-20261003-175243-5496fc), about 160 MB a
# month. One month per statement keeps a statement to one partition's
# writers; Trino stops scaling writers past 70% of the per-node limit
# (task.scale-writers.max-writer-memory-percentage), so this does not grow
# with scale either. Each statement is the same per-partition rewrite as an
# unchunked optimize (optimize compacts each partition on its own; same
# threshold, no writer setting changed), committed as one snapshot per month.
COMPACTION_CHUNK_MONTHS = 1

# Lakebench-created tables compaction chunks, from Lakebench's own DDL:
# silver_build.py and silver_stream.py create customer_interactions_enriched
# partitioned by interaction_date; financial_ddl.py and
# silver_build_financial.py create silver.transactions by
# months(txn_timestamp) and silver.account_statements by months(book_ts).
# Keyed by "schema.table" first, then the bare table name; a renamed table
# falls back to one statement.
_COMPACTION_PARTITIONING = {
    "customer_interactions_enriched": CompactionPartitioning(
        "interaction_date", "identity", COMPACTION_CHUNK_PARTITIONS
    ),
    "silver.transactions": CompactionPartitioning(
        "txn_timestamp", "month", COMPACTION_CHUNK_MONTHS
    ),
    "silver.account_statements": CompactionPartitioning(
        "book_ts", "month", COMPACTION_CHUNK_MONTHS
    ),
}

_DATE_VALUE = re.compile(r"\d{4}-\d{2}-\d{2}")
_INT_VALUE = re.compile(r"-?\d+")


def compaction_partitioning(table: str) -> CompactionPartitioning | None:
    """How compaction chunks *table* (``catalog.schema.table``), or None."""
    parts = [p.strip('"') for p in table.split(".")]
    for key in (".".join(parts[-2:]), parts[-1]):
        if key in _COMPACTION_PARTITIONING:
            return _COMPACTION_PARTITIONING[key]
    return None


def _system_table_ref(table: str, suffix: str) -> str:
    """'catalog.schema.table' -> 'catalog.schema."table$<suffix>"'."""
    parts = table.rsplit(".", 1)
    if len(parts) == 2:
        return f'{parts[0]}."{parts[1]}${suffix}"'
    return f'"{table}${suffix}"'


def build_partition_values_sql(table: str, column: str) -> str:
    """Trino query listing *table*'s values of partition field *column*
    (``CompactionPartitioning.partition_field``)."""
    ref = _system_table_ref(table, "partitions")
    return f"SELECT DISTINCT partition.{column} FROM {ref} ORDER BY 1"


def _month_start(epoch_month: int) -> str:
    """Iceberg's month transform value (months since 1970-01) as the first
    day of that month, ``YYYY-MM-01``."""
    year, month = divmod(epoch_month, 12)
    return f"{1970 + year:04d}-{month + 1:02d}-01"


def parse_partition_values(output: str, transform: str = "identity") -> list[str | None]:
    """Partition values from the Trino CLI's CSV output of
    :func:`build_partition_values_sql`, sorted, with None last for a NULL
    partition (the CLI prints NULL as an empty field).

    ``identity`` values are ``YYYY-MM-DD`` dates. ``month`` values are
    Iceberg's month transform (an integer, months since 1970-01) and come
    back as the month's first day, ``YYYY-MM-01``. Raises ``ValueError`` on
    any line that is neither, so an unexpected format never turns into
    statements that miss partitions.
    """
    if transform not in ("identity", "month"):
        raise ValueError(f"unsupported partition transform {transform!r}")
    values: list[str | None] = []
    for raw in (output or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        value = line.strip('"')
        if value == "":
            values.append(None)
        elif transform == "identity" and _DATE_VALUE.fullmatch(value):
            values.append(value)
        elif transform == "month" and _INT_VALUE.fullmatch(value):
            values.append(_month_start(int(value)))
        else:
            raise ValueError(f"unexpected partition value {line!r}")
    dated = sorted({v for v in values if v is not None})
    return [*dated, *([None] if None in values else [])]


def _utc_month_start(day: str) -> str:
    """A ``timestamp(6) with time zone`` literal for 00:00 UTC on *day*.

    Iceberg's month transform on a timestamptz column is computed in UTC, and
    Trino enforces (pushes into the optimize) a range on the source column
    only when both of its bounds sit on a partition boundary
    (``IcebergUtil.canEnforceRangeWithPartitioningField``); an explicit UTC
    literal keeps that independent of the session time zone. The precision
    matches the column, as in Trino's own
    ``BaseIcebergConnectorTest.testSelectWithDisjunctTimestampFilter``.
    """
    return f"TIMESTAMP '{day} 00:00:00.000000 UTC'"


def build_compaction_plan(
    engine: str,
    catalog: str,
    table: str,
    file_size_threshold: str = DEFAULT_FILE_SIZE_THRESHOLD,
    partitions: list[str | None] | None = None,
) -> list[str]:
    """Compaction statements for *table*.

    On Trino, a table on ``_COMPACTION_PARTITIONING`` whose *partitions*
    (from :func:`parse_partition_values`) number more than its ``chunk`` is
    compacted in runs of at most ``chunk`` sorted partition values, one
    ``optimize ... WHERE`` per run, plus ``WHERE col IS NULL`` when a NULL
    partition exists. Identity tables: run n covers ``col > <last value of
    run n-1> AND col <= <its last value>``. Month tables: run n covers
    ``col >= <its first month, 00:00 UTC> AND col < <the next run's first
    month>``. In both the first run is open below and the last open above,
    so every non-NULL value is in exactly one statement, including one a
    live stream adds after the read. Trino writer settings are not changed,
    so each partition gets the rewrite one unchunked optimize gives it (raising
    max_partitions_per_writer instead grows writer memory with the
    partition count). Every other case, including ``partitions`` None (not
    read, or the read failed), is :func:`build_compaction_sql`.
    """
    spec = compaction_partitioning(table)
    if engine != "trino" or spec is None or partitions is None or len(partitions) <= spec.chunk:
        return build_compaction_sql(engine, catalog, table, file_size_threshold)
    column = spec.column
    dated = [p for p in partitions if p is not None]
    head = f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '{file_size_threshold}')"
    runs = [dated[i : i + spec.chunk] for i in range(0, len(dated), spec.chunk)]
    plan: list[str] = []
    for n, run in enumerate(runs):
        if len(runs) == 1:
            where = f"{column} IS NOT NULL"
        elif spec.transform == "month":
            # Half-open months [first month of run n, first month of run n+1).
            low = f"{column} >= {_utc_month_start(run[0])}"
            high = f"{column} < {_utc_month_start(runs[n + 1][0])}" if n + 1 < len(runs) else ""
            where = " AND ".join(c for c in (low if n else "", high) if c)
        elif n == 0:
            where = f"{column} <= DATE '{run[-1]}'"
        elif n == len(runs) - 1:
            where = f"{column} > DATE '{runs[n - 1][-1]}'"
        else:
            where = f"{column} > DATE '{runs[n - 1][-1]}' AND {column} <= DATE '{run[-1]}'"
        plan.append(f"{head} WHERE {where}")
    if len(dated) < len(partitions):
        plan.append(f"{head} WHERE {column} IS NULL")
    return plan


def build_table_health_sql(
    engine: str,
    table: str,
) -> dict[str, str]:
    """Build SQL queries to probe Iceberg table health metrics.

    Returns a dict mapping metric name to SQL string.  The queries return
    a single integer count each.
    """
    if engine == "trino":
        # Trino system tables use "$files" / "$snapshots" suffix on the table
        # name.  Only the table-name segment needs quoting (because of the "$"),
        # not the catalog.schema prefix.
        # Input: "catalog.schema.table" -> 'catalog.schema."table$files"'
        parts = table.rsplit(".", 1)
        if len(parts) == 2:
            prefix, tbl = parts
            files_ref = f'{prefix}."{tbl}$files"'
            snaps_ref = f'{prefix}."{tbl}$snapshots"'
        else:
            files_ref = f'"{table}$files"'
            snaps_ref = f'"{table}$snapshots"'
        return {
            "data_file_count": f"SELECT count(*) FROM {files_ref}",
            "snapshot_count": f"SELECT count(*) FROM {snaps_ref}",
        }
    if engine == "spark-thrift":
        return {
            "data_file_count": f"SELECT count(*) FROM {table}.files",
            "snapshot_count": f"SELECT count(*) FROM {table}.snapshots",
        }
    return {}


def build_drop_table_sql(engine: str, table: str) -> str:
    """Build DROP TABLE SQL for the given engine."""
    if engine == "trino":
        return f"DROP TABLE IF EXISTS {table}"
    if engine == "spark-thrift":
        return f"DROP TABLE IF EXISTS {table}"
    return ""


class ExecSqlTimeout(RuntimeError):
    """The local kubectl exec timed out. The statement may still be running
    on the engine: a timeout is not proof that it failed or stopped."""


# K8sClient.exec_in_pod's result for an expired timeout.
_EXEC_TIMEOUT_SENTINEL = "Command timed out"


def exec_sql(
    engine: str,
    k8s: K8sClient,
    pod_name: str,
    namespace: str,
    sql: str,
    timeout: int = 30,
) -> None:
    """Execute a single SQL statement on the given engine pod.

    Raises ``RuntimeError`` when the statement fails (non-zero exit from the
    Trino CLI or beeline, a kubectl exec error, or ``timeout`` expiring), with
    the engine's stdout and stderr in the message. ``K8sClient.exec_in_pod``
    never raises, so ignoring its result reported every failure as success.
    """
    if engine == "trino":
        result = k8s.exec_in_pod(pod_name, ["trino", "--execute", sql], namespace, timeout=timeout)
    elif engine == "spark-thrift":
        result = k8s.exec_in_pod(
            pod_name,
            [
                # Options before -e: after it, beeline reads them as more
                # -e statements and drops them.
                "/opt/spark/bin/beeline",
                "-u",
                "jdbc:hive2://localhost:10000",
                "--silent=true",
                "-e",
                sql,
            ],
            namespace,
            container="spark-thrift",
            timeout=timeout,
        )
    else:
        raise ValueError(f"Unsupported engine for exec_sql: {engine}")
    rc, stdout, stderr = result
    if rc != 0 and (stderr or "").strip() == _EXEC_TIMEOUT_SENTINEL and not (stdout or "").strip():
        raise ExecSqlTimeout(
            f"exec_sql timed out after {timeout}s (the statement may still be running)"
        )
    if rc != 0:
        detail = " | ".join(x.strip() for x in (stdout or "", stderr or "") if x and x.strip())
        raise RuntimeError(f"exec_sql failed (rc={rc}): {detail or 'no output'}")


def query_sql(
    engine: str,
    k8s: K8sClient,
    pod_name: str,
    namespace: str,
    sql: str,
    timeout: int = 30,
) -> str:
    """Execute a SQL query and return stdout.

    Like :func:`exec_sql` but returns the raw stdout for result parsing.
    Raises ``RuntimeError`` on non-zero exit code.
    """
    if engine == "trino":
        rc, stdout, stderr = k8s.exec_in_pod(
            pod_name,
            ["trino", "--execute", sql],
            namespace,
            timeout=timeout,
        )
    elif engine == "spark-thrift":
        rc, stdout, stderr = k8s.exec_in_pod(
            pod_name,
            [
                # Options before -e: after it, beeline reads them as more
                # -e statements and drops them.
                "/opt/spark/bin/beeline",
                "-u",
                "jdbc:hive2://localhost:10000",
                "--silent=true",
                "-e",
                sql,
            ],
            namespace,
            container="spark-thrift",
            timeout=timeout,
        )
    else:
        raise ValueError(f"Unsupported engine for query_sql: {engine}")

    if rc != 0:
        raise RuntimeError(f"query_sql failed (rc={rc}): {stderr}")
    return stdout
