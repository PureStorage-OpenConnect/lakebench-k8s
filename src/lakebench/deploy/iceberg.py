"""Iceberg table maintenance helpers.

Provides engine-aware ``expire_snapshots`` and ``remove_orphan_files``
operations that work with Trino or Spark Thrift Server.  DuckDB is
read-only and cannot run Iceberg maintenance.

Used by both the sustained-mode monitoring loop (cli.py) and the
destroy path (engine.py).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

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
    threshold = retention_threshold.strip()
    unit = threshold[-1]
    value = int(threshold[:-1])
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return value * multipliers.get(unit, 60)


def build_maintenance_sql(
    engine: str,
    catalog: str,
    table: str,
    retention_threshold: str,
) -> list[str]:
    """Build expire_snapshots + remove_orphan_files SQL for the given engine.

    Trino uses ``ALTER TABLE ... EXECUTE`` with a duration string.
    Spark uses ``CALL catalog.system.procedure()`` with a timestamp.
    """
    if engine == "trino":
        return [
            (
                f"ALTER TABLE {table} EXECUTE "
                f"expire_snapshots(retention_threshold => '{retention_threshold}')"
            ),
            (
                f"ALTER TABLE {table} EXECUTE "
                f"remove_orphan_files(retention_threshold => '{retention_threshold}')"
            ),
        ]
    if engine == "spark-thrift":
        seconds = _parse_threshold_seconds(retention_threshold)
        ts_expr = f"CAST((UNIX_TIMESTAMP() - {seconds}) * 1000 AS BIGINT)"
        return [
            (
                f"CALL {catalog}.system.expire_snapshots"
                f"(table => '{table}', older_than => {ts_expr})"
            ),
            (
                f"CALL {catalog}.system.remove_orphan_files"
                f"(table => '{table}', older_than => {ts_expr})"
            ),
        ]
    return []


def build_compaction_sql(
    engine: str,
    catalog: str,
    table: str,
    file_size_threshold: str = "128MB",
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


def exec_sql(
    engine: str,
    k8s: K8sClient,
    pod_name: str,
    namespace: str,
    sql: str,
) -> None:
    """Execute a single SQL statement on the given engine pod."""
    if engine == "trino":
        k8s.exec_in_pod(pod_name, ["trino", "--execute", sql], namespace)
    elif engine == "spark-thrift":
        k8s.exec_in_pod(
            pod_name,
            [
                "/opt/spark/bin/beeline",
                "-u",
                "jdbc:hive2://localhost:10000",
                "-e",
                sql,
                "--silent=true",
            ],
            namespace,
            container="spark-thrift",
        )


def query_sql(
    engine: str,
    k8s: K8sClient,
    pod_name: str,
    namespace: str,
    sql: str,
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
        )
    elif engine == "spark-thrift":
        rc, stdout, stderr = k8s.exec_in_pod(
            pod_name,
            [
                "/opt/spark/bin/beeline",
                "-u",
                "jdbc:hive2://localhost:10000",
                "-e",
                sql,
                "--silent=true",
            ],
            namespace,
            container="spark-thrift",
        )
    else:
        raise ValueError(f"Unsupported engine for query_sql: {engine}")

    if rc != 0:
        raise RuntimeError(f"query_sql failed (rc={rc}): {stderr}")
    return stdout
