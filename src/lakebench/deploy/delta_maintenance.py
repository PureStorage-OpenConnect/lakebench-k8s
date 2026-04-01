"""Delta Lake table maintenance helpers.

Backward-compat re-export. Implementation moved to
``lakebench.modules.table_formats.delta.maintenance``.
"""

from lakebench.modules.table_formats.delta.maintenance import (
    build_delta_compaction_sql,
    build_delta_drop_table_sql,
    build_delta_maintenance_sql,
    build_delta_table_health_sql,
    parse_retention_to_hours,
)

__all__ = [
    "build_delta_compaction_sql",
    "build_delta_drop_table_sql",
    "build_delta_maintenance_sql",
    "build_delta_table_health_sql",
    "parse_retention_to_hours",
]
