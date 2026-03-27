"""Delta Lake table maintenance helpers.

Parallel to ``deploy/iceberg.py`` for Iceberg tables.  Provides SQL
generation for Delta-specific maintenance operations:

- VACUUM (analog of expire_snapshots + remove_orphan_files)
- OPTIMIZE (analog of rewrite_data_files)
- DESCRIBE DETAIL / $properties (analog of $files/$snapshots metadata queries)

Engine discovery (``find_maintenance_engine``) and SQL execution
(``exec_sql``, ``query_sql``) are format-agnostic and reused from
``deploy/iceberg.py``.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)


def parse_retention_to_hours(retention_threshold: str) -> float:
    """Parse a Trino-style duration string to hours.

    Supports ``s`` (seconds), ``m`` (minutes), ``h`` (hours), ``d`` (days).

    Examples::

        >>> parse_retention_to_hours("30m")
        0.5
        >>> parse_retention_to_hours("1h")
        1.0
        >>> parse_retention_to_hours("7d")
        168.0
        >>> parse_retention_to_hours("0s")
        0.0
    """
    threshold = retention_threshold.strip()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([smhd])", threshold)
    if not match:
        raise ValueError(
            f"Invalid retention_threshold format: {retention_threshold!r}. "
            "Expected a number followed by s, m, h, or d (e.g. '30m', '7d')."
        )
    value = float(match.group(1))
    unit = match.group(2)
    multipliers = {"s": 1 / 3600, "m": 1 / 60, "h": 1.0, "d": 24.0}
    return value * multipliers[unit]


def build_delta_maintenance_sql(
    engine: str,
    catalog: str,
    table: str,
    retention_hours: float = 168.0,
) -> list[str]:
    """Build Delta VACUUM SQL for the given engine.

    VACUUM removes data files no longer referenced by the Delta log,
    combining the roles of Iceberg's ``expire_snapshots`` and
    ``remove_orphan_files``.

    Parameters
    ----------
    engine:
        ``"trino"`` or ``"spark-thrift"``.
    catalog:
        Catalog name (e.g. ``"lakehouse"``).
    table:
        Fully-qualified table name (e.g. ``"lakehouse.bronze.events"``).
    retention_hours:
        Number of hours of history to retain.  Files older than this
        threshold are eligible for removal.  Default is 168 (7 days).
    """
    if engine == "trino":
        # Trino Delta connector: VACUUM via catalog-qualified procedure call.
        # Parse schema and table name from the fully-qualified table ref.
        parts = table.rsplit(".", 2)
        if len(parts) == 3:
            _, schema, tbl = parts
        elif len(parts) == 2:
            schema, tbl = parts
        else:
            logger.warning(
                "Table name %r has no schema qualifier -- defaulting to 'default'",
                table,
            )
            schema, tbl = "default", table
        stmts: list[str] = []
        # Trino enforces a 7-day (168h) minimum retention by default.
        # Override the connector session property when a shorter threshold
        # is requested (e.g. retention_hours=0 on destroy path).
        if retention_hours < 168.0:
            stmts.append(
                f"SET SESSION {catalog}.vacuum_min_retention = '0s'"
            )
        stmts.append(
            f"CALL {catalog}.system.vacuum("
            f"schema_name => '{schema}', "
            f"table_name => '{tbl}', "
            f"retention => '{retention_hours}h')"
        )
        return stmts
    if engine == "spark-thrift":
        return [
            f"VACUUM {table} RETAIN {retention_hours} HOURS",
        ]
    return []


def build_delta_compaction_sql(
    engine: str,
    catalog: str,
    table: str,
) -> list[str]:
    """Build Delta OPTIMIZE SQL for the given engine.

    OPTIMIZE compacts small files into larger ones, improving read
    performance.  Analog of Iceberg's ``rewrite_data_files``.
    """
    if engine == "trino":
        # Trino 470+ supports ALTER TABLE ... EXECUTE optimize for Delta.
        return [
            f"ALTER TABLE {table} EXECUTE optimize",
        ]
    if engine == "spark-thrift":
        return [
            f"OPTIMIZE {table}",
        ]
    return []


def build_delta_table_health_sql(
    engine: str,
    catalog: str,
    table: str,
) -> dict[str, str]:
    """Build SQL queries to probe Delta table health metrics.

    Returns a dict mapping metric name to SQL string.

    - Trino exposes a ``$properties`` system table for Delta tables.
    - Spark provides ``DESCRIBE DETAIL`` which returns table metadata
      including ``numFiles``, ``sizeInBytes``, and ``properties``.
    """
    if engine == "trino":
        # Trino Delta connector: $properties system table.
        # Input: "catalog.schema.table" -> 'catalog.schema."table$properties"'
        parts = table.rsplit(".", 1)
        if len(parts) == 2:
            prefix, tbl = parts
            props_ref = f'{prefix}."{tbl}$properties"'
        else:
            props_ref = f'"{table}$properties"'
        return {
            "table_properties": f"SELECT * FROM {props_ref}",
        }
    if engine == "spark-thrift":
        return {
            "table_detail": f"DESCRIBE DETAIL {table}",
        }
    return {}


def build_delta_drop_table_sql(catalog: str, table: str) -> str:
    """Build DROP TABLE SQL for a Delta table.

    DROP TABLE is format-agnostic -- the same SQL works for both Iceberg
    and Delta tables.
    """
    return f"DROP TABLE IF EXISTS {table}"
