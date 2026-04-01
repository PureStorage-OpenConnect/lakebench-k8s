"""Iceberg table maintenance helpers.

Backward-compat re-export. Implementation moved to
``lakebench.modules.table_formats.iceberg.maintenance``.
"""

from lakebench.modules.table_formats.iceberg.maintenance import (
    build_compaction_sql,
    build_drop_table_sql,
    build_maintenance_sql,
    build_table_health_sql,
    exec_sql,
    find_maintenance_engine,
    query_sql,
)

__all__ = [
    "build_compaction_sql",
    "build_drop_table_sql",
    "build_maintenance_sql",
    "build_table_health_sql",
    "exec_sql",
    "find_maintenance_engine",
    "query_sql",
]
