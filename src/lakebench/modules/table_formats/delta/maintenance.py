"""Delta Lake table maintenance helpers.

Parallel to ``modules/table_formats/iceberg/maintenance.py`` for Iceberg
tables. Provides SQL generation for Delta VACUUM, OPTIMIZE and the
``DESCRIBE DETAIL`` health probe. Engine discovery and SQL execution are
format-agnostic and reused from the Iceberg module.
"""

from __future__ import annotations

import logging

from lakebench.modules.table_formats.iceberg.maintenance import _parse_threshold_seconds

logger = logging.getLogger(__name__)


def parse_retention_to_hours(retention_threshold: str) -> float:
    """A Trino-style duration string (``s``/``m``/``h``/``d``) as hours.

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
    return _parse_threshold_seconds(retention_threshold) / 3600


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
        # Trino enforces a 7-day (168h) minimum retention by default.
        # Override the connector session property when a shorter threshold
        # is requested (e.g. retention_hours=0 on destroy path). exec_sql
        # runs each list element as its own `trino --execute` process, so a
        # separate SET SESSION would be lost before the CALL: send both in
        # one submission, which the CLI runs in order in one session.
        call = (
            f"CALL {catalog}.system.vacuum("
            f"schema_name => '{schema}', "
            f"table_name => '{tbl}', "
            f"retention => '{retention_hours}h')"
        )
        if retention_hours < 168.0:
            return [f"SET SESSION {catalog}.vacuum_min_retention = '0s'; {call}"]
        return [call]
    if engine == "spark-thrift":
        # Spark refuses VACUUM below the 7-day default retention unless
        # retentionDurationCheck is off for the session. exec_sql runs each
        # list element as its own beeline connection, so the SET must travel
        # in the same `-e` submission as the VACUUM to apply to it.
        vacuum = f"VACUUM {table} RETAIN {retention_hours} HOURS"
        if retention_hours < 168.0:
            return [f"SET spark.databricks.delta.retentionDurationCheck.enabled=false; {vacuum}"]
        return [vacuum]
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


#: Engines that cannot report a Delta table's data file count, and why. The
#: health probe says so instead of recording nothing silently.
DELTA_HEALTH_UNAVAILABLE = {
    "trino": (
        "Trino's Delta connector has no metadata table with a data file count "
        "($properties holds table properties, not counts), and counting distinct "
        '"$path" values would scan the table inside the measured window'
    ),
}


def build_delta_table_health_sql(
    engine: str,
    catalog: str,
    table: str,
) -> dict[str, str]:
    """Build SQL queries to probe Delta table health metrics.

    Returns a dict mapping metric name to SQL string, with the metric names
    the Iceberg probe uses (``data_file_count``).

    - Spark: ``DESCRIBE DETAIL`` returns one row whose ``numFiles`` column
      is the data file count; parse it with :func:`parse_describe_detail`.
    - Trino: nothing (see ``DELTA_HEALTH_UNAVAILABLE``); the connector has no
      metadata table that reports a data file count.
    """
    if engine == "spark-thrift":
        return {
            "data_file_count": f"DESCRIBE DETAIL {table}",
        }
    return {}


def parse_describe_detail(stdout: str, column: str = "numFiles") -> int | None:
    """The integer *column* of beeline's ``DESCRIBE DETAIL`` table output,
    or None when the output has no such column or value.

    Beeline prints a header row of column names and one data row, both
    ``|``-separated, between ``+---+`` rules.
    """
    header: list[str] | None = None
    for line in stdout.splitlines():
        text = line.strip()
        if not text.startswith("|"):
            continue
        cells = [c.strip() for c in text.strip("|").split("|")]
        if header is None:
            if column in cells:
                header = cells
            continue
        if len(cells) != len(header):
            continue
        value = cells[header.index(column)]
        return int(value) if value.isdigit() else None
    return None
