"""Shared harness for the Trino Iceberg compaction run path: a coordinator
fake that answers the partition read and fails chosen statements, and the
driver that runs `_run_iceberg_compaction` against it."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock, patch

from rich.console import Console


def trino_cfg(schema_type: str, tables=None, mode: str | None = None) -> MagicMock:
    """A config for a Trino + Iceberg deployment (tables default to the
    customer360 names)."""
    from lakebench.config.schema import TableNamesConfig

    cfg = MagicMock()
    cfg.get_namespace.return_value = "lakebench-test"
    cfg.architecture.query_engine.type.value = "trino"
    cfg.architecture.query_engine.trino.catalog_name = "lakehouse"
    cfg.architecture.table_format.type.value = "iceberg"
    cfg.architecture.tables = tables if tables is not None else TableNamesConfig()
    cfg.architecture.workload.schema_type.value = schema_type
    if mode is not None:
        cfg.architecture.pipeline.mode.value = mode
    return cfg


class Trino:
    """exec_in_pod for a Trino coordinator.

    ``read(sql)`` answers a partition read as ``(rc, stdout, stderr)``;
    ``fail(sql)`` returns the stderr a statement fails with, or None when it
    succeeds.
    """

    def __init__(
        self,
        read: Callable[[str], tuple[int, str, str]],
        fail: Callable[[str], str | None] = lambda sql: None,
    ):
        self.read = read
        self.fail = fail
        self.reads: list[str] = []
        self.statements: list[str] = []

    def __call__(self, pod, argv, namespace, container=None, timeout=30):
        sql = argv[2]
        if "$files" in sql or "$partitions" in sql:
            self.reads.append(sql)
            return self.read(sql)
        self.statements.append(sql)
        error = self.fail(sql)
        return (1, "", error) if error else (0, "", "")


def compact(trino: Trino, cfg, **kw) -> list[dict]:
    from lakebench.cli._sustained import _run_iceberg_compaction

    k8s = MagicMock()
    k8s.exec_in_pod.side_effect = trino
    outcomes: list[dict] = []
    with patch(
        "lakebench.deploy.iceberg.find_maintenance_engine",
        return_value=("trino", "trino-coordinator-0", "lakehouse"),
    ):
        _run_iceberg_compaction(cfg, k8s, Console(quiet=True), MagicMock(), outcomes=outcomes, **kw)
    return outcomes
