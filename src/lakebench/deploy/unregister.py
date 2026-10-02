"""Unregister a medallion layer's tables before `clean` empties its bucket.

`lakebench clean silver|gold|bronze|data` empties buckets and used to leave
the catalog alone, so the next run met catalog entries whose files were gone:
a Delta table fails every read (DELTA_TABLE_NOT_FOUND), an Iceberg table on a
Hive catalog fails to load its missing metadata file. Removing the entries
before the bucket is emptied lets the next run create the tables afresh.

The statements are destroy's (deploy/destroy.py, table step), on the
deployment's own Trino or Spark Thrift pod:

- Trino: ``CALL <catalog>.system.unregister_table(...)``, the catalog entry
  only, for Iceberg and Delta on every catalog type.
- Spark Thrift, Iceberg: ``DROP TABLE`` without PURGE, the catalog entry only.
- Spark Thrift, Delta: ``DROP TABLE`` also deletes a managed table's
  directory, so it runs only when ``DESCRIBE DETAIL`` puts the table in the
  bucket that is about to be emptied (or its files are already gone).

It runs only after `clean` has proved the bucket is this deployment's.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class LayerUnregister:
    """What happened to one layer's catalog entries."""

    unregistered: list[str] = field(default_factory=list)
    #: (table, reason): left registered on purpose (another bucket).
    kept: list[tuple[str, str]] = field(default_factory=list)
    #: (table, error): the statement failed; the entry is still there.
    failed: list[tuple[str, str]] = field(default_factory=list)
    #: Set when no statement could run at all (no engine pod).
    skipped: str = ""


def unregister_layer_tables(cfg: Any, layer: str, bucket: str, k8s: Any) -> LayerUnregister:
    """Unregister the workload tables of ``layer``, whose bucket is ``bucket``.

    ``k8s`` is the deployment's ``K8sClient`` (for ``exec_in_pod``). Never
    raises for a table: each outcome is in the result.
    """
    from lakebench.deploy.destroy import (
        _is_schema_missing,
        _is_table_missing,
        _split_table,
        _table_location_bucket,
        _trino_unregister_sql,
    )
    from lakebench.modules.table_formats.iceberg.maintenance import (
        build_drop_table_sql,
        exec_sql,
        find_maintenance_engine,
        query_sql,
    )

    out = LayerUnregister()
    namespace = cfg.get_namespace()
    maint_engine, pod_name, catalog = find_maintenance_engine(cfg, namespace)
    if not (maint_engine and pod_name and catalog):
        out.skipped = "no Trino or Spark Thrift pod is running"
        return out
    schema = cfg.architecture.workload.schema_type.value
    table_format = cfg.architecture.table_format.type.value
    tables = [
        f"{catalog}.{t}" for t in cfg.architecture.tables.workload_tables(schema, layers=(layer,))
    ]
    for table in tables:
        if _split_table(table) is None:
            out.failed.append((table, "not a <catalog>.<schema>.<table> name"))
            continue
        if maint_engine == "trino":
            sql = _trino_unregister_sql(table)
        else:
            sql = build_drop_table_sql(maint_engine, table)
            if table_format == "delta":
                where, why = _table_location_bucket(
                    query_sql, maint_engine, k8s, pod_name, namespace, table
                )
                if why == "missing":
                    continue
                if why != "gone" and where != bucket:
                    # DROP would delete a managed table's directory: only in
                    # the bucket being emptied.
                    reason = (
                        f"its data is in {where}, not {bucket}"
                        if where
                        else f"its location could not be read ({why})"
                    )
                    out.kept.append((table, reason))
                    continue
        try:
            exec_sql(maint_engine, k8s, pod_name, namespace, sql, timeout=120)
        except Exception as e:  # noqa: BLE001
            if _is_table_missing(e) or _is_schema_missing(e):
                continue
            out.failed.append((table, " ".join(str(e).split())[:200]))
            continue
        out.unregistered.append(table)
        logger.info("unregistered %s via %s before emptying %s", table, maint_engine, bucket)
    return out
