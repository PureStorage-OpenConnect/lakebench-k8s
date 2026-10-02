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
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# An entry whose files are already gone. Delta: the log is missing. Iceberg:
# the metadata file is missing, which Spark's DROP cannot get past (it loads
# the table first), so the entry is stuck until a Trino unregister.
_FILES_GONE_RE = re.compile(
    r"\[DELTA_TABLE_NOT_FOUND\]|\[DELTA_PATH_DOES_NOT_EXIST\]"
    r"|org\.apache\.iceberg\.exceptions\.NotFoundException"
)


@dataclass
class LayerUnregister:
    """What happened to one layer's catalog entries."""

    unregistered: list[str] = field(default_factory=list)
    #: (table, reason): left registered on purpose, its data in another bucket.
    kept: list[tuple[str, str]] = field(default_factory=list)
    #: (table, error): still registered with its files in place; the bucket
    #: must not be emptied, so a re-run can finish.
    failed: list[tuple[str, str]] = field(default_factory=list)
    #: (table, error): still registered, its files already gone; emptying
    #: the bucket changes nothing for it.
    stuck: list[tuple[str, str]] = field(default_factory=list)
    #: Set when no statement could run at all (no engine pod).
    skipped: str = ""

    @property
    def may_empty(self) -> bool:
        """Whether the bucket may be emptied: no entry still has its files."""
        return not self.failed


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
        ExecSqlTimeout,
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
                    _query_marking_gone(query_sql), maint_engine, k8s, pod_name, namespace, table
                )
                if why == "missing":
                    continue
                gone = why == "gone"
                if not gone and where is None:
                    # Cannot tell where its files are: neither drop it (DROP
                    # deletes a managed table's directory) nor empty the bucket.
                    out.failed.append((table, f"its location could not be read ({why})"))
                    continue
                if not gone and where != bucket:
                    out.kept.append((table, f"its data is in {where}, not {bucket}"))
                    continue
        try:
            exec_sql(maint_engine, k8s, pod_name, namespace, sql, timeout=120)
        except ExecSqlTimeout as e:
            # The engine is stuck; the rest would wait as long. Stop here.
            out.failed.append((table, str(e)))
            break
        except Exception as e:  # noqa: BLE001
            if _is_table_missing(e) or _is_schema_missing(e):
                continue
            text = " ".join(str(e).split())[:200]
            (out.stuck if _FILES_GONE_RE.search(str(e)) else out.failed).append((table, text))
            continue
        out.unregistered.append(table)
        logger.info("unregistered %s via %s before emptying %s", table, maint_engine, bucket)
    return out


def _query_marking_gone(query_sql: Any) -> Any:
    """``query_sql``, with a log-less Delta table's error reported in the
    form destroy's location reader treats as "files gone". It matches on the
    whole error, not the shortened text the reader keeps."""

    def run(*args: Any, **kwargs: Any) -> str:
        try:
            return str(query_sql(*args, **kwargs))
        except Exception as e:
            text = str(e)
            if re.search(r"\[DELTA_TABLE_NOT_FOUND\]|\[DELTA_PATH_DOES_NOT_EXIST\]", text):
                raise RuntimeError("[DELTA_PATH_DOES_NOT_EXIST] " + text) from e
            raise

    return run
