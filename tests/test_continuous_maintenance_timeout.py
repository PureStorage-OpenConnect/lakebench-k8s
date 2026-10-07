"""Continuous maintenance and compaction use a budgeted statement timeout.

The loop in _run_sustained used the helpers' 30 s default, so expire and
rewrite statements on real tables were reported as timed out while the
engine kept running them, and the next statement overlapped.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from rich.console import Console

from tests.fixtures.maintenance_timeout_helpers import loop_call_keywords as loop_call_keywords


def test_maintenance_journals_the_budgeted_timeout():
    """The chosen timeout reaches exec_sql and the journal; a timeout is its own count."""
    from lakebench.cli._sustained import (
        MaintenanceBudget,
        _run_iceberg_maintenance,
        continuous_round_bounds,
    )
    from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout
    from tests.conftest import make_config

    cfg = make_config()
    j = MagicMock()
    seen: list[int] = []

    def fake_exec(engine, k8s, pod, ns, sql, timeout):
        seen.append(timeout)
        if len(seen) == 1:
            raise ExecSqlTimeout("timed out")

    per_stmt, round_s = continuous_round_bounds(1800, 10_000)
    with (
        patch(
            "lakebench.deploy.iceberg.find_maintenance_engine",
            return_value=("trino", "trino-0", "lakehouse"),
        ),
        patch("lakebench.deploy.iceberg.exec_sql", side_effect=fake_exec),
    ):
        _run_iceberg_maintenance(
            cfg,
            MagicMock(),
            Console(quiet=True),
            j,
            "30m",
            timeout=per_stmt,
            live_streams=True,
            budget=MaintenanceBudget(round_s, label="continuous maintenance round"),
        )
    assert seen == [600]  # first statement timed out; the rest not attempted
    details = next(
        c.kwargs["details"]
        for c in j.record.call_args_list
        if c.kwargs.get("message") == "Iceberg maintenance"
    )
    assert details["statement_timeout_seconds"] == 600
    assert details["operations_timed_out"] == 1
    assert details["operations_failed"] == 0
    assert details["operations_not_attempted"] >= 1
    assert "timed out after 600s" in details["stopped"]


def test_maintenance_and_compaction_record_what_ran():
    """The experiment block's effective maintenance reads these outcomes."""
    from lakebench.cli._sustained import _run_iceberg_compaction, _run_iceberg_maintenance
    from tests.conftest import make_config

    cfg = make_config()
    outcomes: list = []

    def fake_exec(engine, k8s, pod, ns, sql, timeout):
        if "rewrite_data_files" in sql or "optimize" in sql.lower():
            raise RuntimeError("compaction failed")

    with (
        patch(
            "lakebench.deploy.iceberg.find_maintenance_engine",
            return_value=("trino", "trino-0", "lakehouse"),
        ),
        patch("lakebench.deploy.iceberg.exec_sql", side_effect=fake_exec),
    ):
        _run_iceberg_maintenance(
            cfg, MagicMock(), Console(quiet=True), MagicMock(), "30m", outcomes=outcomes
        )
        _run_iceberg_compaction(
            cfg, MagicMock(), Console(quiet=True), MagicMock(), outcomes=outcomes
        )
    expire, compaction = outcomes
    assert expire["kind"] == "expire" and expire["succeeded"] == expire["total"] > 0
    assert compaction["kind"] == "compaction" and compaction["succeeded"] == 0
    assert compaction["failed"] == compaction["total"] > 0

    duck = make_config(architecture={"query_engine": {"type": "duckdb"}})
    skipped: list = []
    _run_iceberg_maintenance(
        duck, MagicMock(), Console(quiet=True), MagicMock(), "30m", outcomes=skipped
    )
    assert skipped == [{"kind": "expire", "skipped": "DuckDB cannot run maintenance"}]
