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

    if engine_type == "trino":
        try:
            pods = core_v1.list_namespaced_pod(
                namespace,
                label_selector=_TRINO_SELECTOR,
            )
            if pods.items:
                catalog = cfg.architecture.query_engine.trino.catalog_name
                return "trino", pods.items[0].metadata.name, catalog
        except Exception as e:
            logger.warning("Iceberg maintenance: Trino pod lookup failed: %s", e)

    if engine_type == "spark-thrift":
        try:
            pods = core_v1.list_namespaced_pod(
                namespace,
                label_selector=_SPARK_THRIFT_SELECTOR,
            )
            if pods.items:
                catalog = cfg.architecture.query_engine.spark_thrift.catalog_name
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
