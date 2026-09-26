"""Continuous maintenance and compaction use a budgeted statement timeout.

The loop in _run_sustained used the helpers' 30 s default, so expire and
rewrite statements on real tables were reported as timed out while the
engine kept running them, and the next statement overlapped.
"""

from __future__ import annotations

import ast
import inspect
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console


def loop_call_keywords(func_name: str) -> dict[str, str]:
    """Keyword arguments (as source) of the one call to func_name in _run_sustained."""
    import lakebench.cli._sustained as sus

    tree = ast.parse(inspect.getsource(sus._run_sustained))
    calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == func_name
    ]
    assert len(calls) == 1, f"expected one {func_name} call in the loop, found {len(calls)}"
    return {k.arg: ast.unparse(k.value) for k in calls[0].keywords if k.arg}


def _loop_if(test_src: str) -> ast.If:
    """The `if` in _run_sustained whose condition is exactly test_src."""
    import lakebench.cli._sustained as sus

    tree = ast.parse(inspect.getsource(sus._run_sustained))
    found = [n for n in ast.walk(tree) if isinstance(n, ast.If) and ast.unparse(n.test) == test_src]
    assert len(found) == 1, test_src
    return found[0]


@pytest.mark.parametrize(
    ("func", "interval", "condition"),
    [
        ("_run_iceberg_maintenance", "retention_interval", "elapsed >= next_maintenance_at"),
        (
            "_run_iceberg_compaction",
            "compaction_interval",
            "compaction_enabled and elapsed >= next_compaction_at",
        ),
    ],
)
def test_loop_passes_budgeted_timeout(func, interval, condition):
    kw = loop_call_keywords(func)
    assert kw["timeout"] == "bounds[0]"
    assert kw["budget"] in (
        "maint_budget",
        "MaintenanceBudget(bounds[1], label='continuous compaction round')",
    )
    assert kw["start_at"] in ("maintenance_start", "compaction_start")
    # The round is scheduled by its own interval, and the remaining time is
    # read when the round starts (not the loop-top elapsed).
    block = _loop_if(condition)
    bounds = [
        n
        for n in ast.walk(block)
        if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "continuous_round_bounds"
    ]
    assert len(bounds) == 1
    assert any(
        isinstance(n, ast.Call) and getattr(n.func, "id", "") == func for n in ast.walk(block)
    )
    assert ast.unparse(bounds[0].args[0]) == interval
    remaining = ast.unparse(bounds[0].args[1])
    if remaining == "run_duration - now":
        assert "now = time.time() - start" in ast.unparse(block)
    else:
        assert remaining == "run_duration - (time.time() - start)"
    if func == "_run_iceberg_maintenance":
        body = ast.unparse(block)
        assert (
            "maint_budget = MaintenanceBudget(bounds[1], label='continuous maintenance round')"
            in body
        )
        assert "compaction_hold_until = time.time() - start + bounds[0]" in body
        # Only a timeout holds compaction; a deadline stop leaves nothing running.
        assert "if 'timed out' in maint_budget.stopped:" in body


def test_compaction_waits_for_a_timed_out_maintenance_statement():
    block = ast.unparse(_loop_if("compaction_enabled and elapsed >= next_compaction_at"))
    assert "if now < compaction_hold_until:" in block
    assert "next_compaction_at = compaction_hold_until" in block


@pytest.mark.parametrize(
    ("interval", "remaining", "expected"),
    [
        (1800, 10_000, (600, 900.0)),  # default: statement capped at 600 s
        (600, 10_000, (300, 300.0)),  # half the interval
        (300, 10_000, (150, 150.0)),  # schema minimum retention_interval
        (3600, 230.0, (600, 200.0)),  # round ends by run end (budget + 30 s grace)
        (20, 10_000, (30, 30.0)),  # tiny compaction_interval: 30 s floor
        (3600, 59.0, None),  # too little run left: skip the round
    ],
)
def test_round_bounds(interval, remaining, expected):
    from lakebench.cli._sustained import continuous_round_bounds

    assert continuous_round_bounds(interval, remaining) == expected


def test_round_cannot_outlive_the_run_window():
    """Budget plus the statement grace never exceeds the time left."""
    from lakebench.cli._sustained import _BUDGET_GRACE_SECONDS, continuous_round_bounds

    for remaining in (60.0, 61.0, 100.0, 500.0, 5000.0):
        b = continuous_round_bounds(3600, remaining)
        assert b is not None and b[1] + _BUDGET_GRACE_SECONDS <= remaining


def test_statement_timeout_is_never_the_old_30s_default_for_the_minimum_interval():
    from lakebench.cli._sustained import continuous_round_bounds

    b = continuous_round_bounds(300, 1e9)
    assert b is not None and b[0] > 30


def test_rotated_and_next_start():
    from lakebench.cli._sustained import _next_start, _rotated

    items = ["a", "b", "c"]
    assert _rotated(items, 0) == items
    assert _rotated(items, 1) == ["b", "c", "a"]
    assert _rotated(items, 4) == ["b", "c", "a"]
    assert _rotated([], 3) == []
    assert _next_start(items, {"timed_out_table": "b"}) == 2
    assert _next_start(items, {"timed_out_table": "c"}) == 0
    assert _next_start(items, {"resume_table": "b"}) == 1  # deadline: redo b
    assert _next_start(items, {}) == 0
    assert _next_start([], {"timed_out_table": "b"}) == 0


def _run_maint_with_timeout_on(cfg, bad_table_index: int, start_at: int):
    """Run a continuous maintenance round where one table's first statement times out."""
    from lakebench.cli._sustained import MaintenanceBudget, _run_iceberg_maintenance
    from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout

    tables = [
        f"lakehouse.{t}"
        for t in cfg.architecture.tables.workload_tables(
            cfg.architecture.workload.schema_type.value
        )
    ]
    sent: list[str] = []

    def fake_exec(engine, k8s, pod, ns, sql, timeout):
        sent.append(sql)
        if tables[bad_table_index] in sql:
            raise ExecSqlTimeout("timed out")

    with (
        patch(
            "lakebench.deploy.iceberg.find_maintenance_engine",
            return_value=("trino", "trino-0", "lakehouse"),
        ),
        patch("lakebench.deploy.iceberg.exec_sql", side_effect=fake_exec),
    ):
        nxt = _run_iceberg_maintenance(
            cfg,
            MagicMock(),
            Console(quiet=True),
            MagicMock(),
            "30m",
            timeout=600,
            live_streams=True,
            budget=MaintenanceBudget(900, label="continuous maintenance round"),
            start_at=start_at,
        )
    return tables, sent, nxt


def test_a_stuck_table_costs_one_statement_per_round():
    """Round 1 stalls on the stuck table; round 2 starts after it and finishes."""
    from tests.conftest import make_config

    cfg = make_config()
    tables, sent, nxt = _run_maint_with_timeout_on(cfg, bad_table_index=1, start_at=0)
    assert len(tables) >= 3
    assert tables[0] in sent[0]
    assert nxt == 2
    tables, sent2, nxt2 = _run_maint_with_timeout_on(cfg, bad_table_index=1, start_at=nxt)
    assert tables[2] in sent2[0]  # the round began after the stuck table
    touched = {t for t in tables if any(t in q for q in sent + sent2)}
    assert touched == set(tables)  # every table reached within two rounds
    assert nxt2 == 2  # stalled on it again at the end; resumes after it


def test_budget_label_names_the_continuous_round():
    from lakebench.cli._sustained import MaintenanceBudget

    b = MaintenanceBudget(10, label="continuous maintenance round")
    b._clock = lambda: b.deadline + 1
    assert b.exhausted()
    assert b.stopped == "continuous maintenance round exceeded its 10s cap"
    assert MaintenanceBudget(10).label == "pre-benchmark maintenance"


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
