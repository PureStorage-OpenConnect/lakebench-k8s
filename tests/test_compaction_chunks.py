"""Trino compaction of the partitioned C360 silver table (V16-4, LB-210).

Trino's Iceberg connector refuses an optimize that opens more than 100
partition writers ("Exceeded limit of 100 open writers for partitions:
101"), which is what failed on silver.customer_interactions_enriched in
run-20260929-204941-1d17f4. The plan now chunks the table by its identity
partition column at 90 partitions per statement, a table counts as compacted
only when every chunk succeeds, and a failed table is named in the effective
maintenance record.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance
from lakebench.modules.table_formats.iceberg.maintenance import (
    COMPACTION_CHUNK_PARTITIONS,
    build_compaction_plan,
    build_partition_values_sql,
    parse_partition_values,
)

SILVER = "lakehouse.silver.customer_interactions_enriched"
GOLD = "lakehouse.gold.customer_executive_dashboard"
# The stderr shape of the live failure (journal of run-20260929-204941-1d17f4):
# kubectl's container notice first, then Trino's error.
WRITER_LIMIT = (
    'Defaulted container "trino" out of: trino, wait-for-hive (init)\n'
    "Query 20260929_210101_00042_abcde failed: Exceeded limit of 100 open writers "
    "for partitions: 101\n\tat io.trino.plugin.iceberg.IcebergPageSink.getWriterIndexes"
)


def _days(n: int, start: date = date(2024, 1, 1)) -> list[str]:
    return [(start + timedelta(days=i)).isoformat() for i in range(n)]


def _covered(stmt: str, days: list[str]) -> list[str]:
    """The partition values a chunk's WHERE selects out of *days*."""
    where = stmt.split(" WHERE ", 1)[1]
    keep = list(days)
    for clause in where.split(" AND "):
        value = clause.split("DATE '")[1].split("'")[0]
        if " <= " in clause:
            keep = [d for d in keep if d <= value]
        else:
            assert " > " in clause, clause
            keep = [d for d in keep if d > value]
    return keep


# -- the plan -----------------------------------------------------------------


def test_compaction_plan_chunks_at_90():
    days = _days(366)
    plan = build_compaction_plan("trino", "lakehouse", SILVER, "128MB", days)
    assert COMPACTION_CHUNK_PARTITIONS == 90
    assert len(plan) == 5  # 90 + 90 + 90 + 90 + 6
    covered: list[str] = []
    for stmt in plan:
        assert stmt.startswith(
            f"ALTER TABLE {SILVER} EXECUTE optimize(file_size_threshold => '128MB') "
            "WHERE interaction_date "
        )
        run = _covered(stmt, days)
        assert 0 < len(run) <= 90
        covered.extend(run)
    # Every partition in exactly one chunk, in order.
    assert covered == days
    # The ends are open, so a partition a live stream adds outside the
    # values read is still compacted.
    assert plan[0].endswith("WHERE interaction_date <= DATE '2024-03-30'")
    assert plan[1].endswith(
        "WHERE interaction_date > DATE '2024-03-30' AND interaction_date <= DATE '2024-06-28'"
    )
    assert plan[-1].endswith("WHERE interaction_date > DATE '2024-12-25'")
    # A date between two chunks, below the first or above the last falls in
    # exactly one chunk.
    for late in ("2023-06-01", "2024-03-30", "2025-02-01"):
        assert sum(late in _covered(stmt, [late]) for stmt in plan) == 1, late


def test_ninety_partitions_or_fewer_keep_one_unchunked_statement():
    for n in (0, 1, 90):
        plan = build_compaction_plan("trino", "lakehouse", SILVER, "128MB", _days(n))
        assert plan == [f"ALTER TABLE {SILVER} EXECUTE optimize(file_size_threshold => '128MB')"], n
    assert len(build_compaction_plan("trino", "lakehouse", SILVER, "128MB", _days(91))) == 2


def test_null_partition_gets_its_own_statement():
    plan = build_compaction_plan("trino", "lakehouse", SILVER, "128MB", [*_days(100), None])
    assert len(plan) == 3
    assert plan[-1].endswith("WHERE interaction_date IS NULL")
    # 90 dates and a NULL partition: one dated chunk with no date bound.
    plan = build_compaction_plan("trino", "lakehouse", SILVER, "128MB", [*_days(90), None])
    assert [p.split(" WHERE ")[1] for p in plan] == [
        "interaction_date IS NOT NULL",
        "interaction_date IS NULL",
    ]


def test_unchunked_cases():
    days = _days(366)
    single = [f"ALTER TABLE {GOLD} EXECUTE optimize(file_size_threshold => '128MB')"]
    # A table not on the partition map.
    assert build_compaction_plan("trino", "lakehouse", GOLD, "128MB", days) == single
    # Partitions not read (or the read failed).
    assert len(build_compaction_plan("trino", "lakehouse", SILVER, "128MB", None)) == 1
    # Spark Thrift's rewrite_data_files has no writer limit.
    assert build_compaction_plan("spark-thrift", "lakehouse", SILVER, "128MB", days) == [
        f"CALL lakehouse.system.rewrite_data_files(table => '{SILVER}')"
    ]


def test_partition_read_sql_quotes_the_system_table():
    assert build_partition_values_sql(SILVER, "interaction_date") == (
        "SELECT DISTINCT partition.interaction_date FROM "
        'lakehouse.silver."customer_interactions_enriched$partitions" ORDER BY 1'
    )


def test_parse_partition_values():
    out = '"2024-01-03"\n"2024-01-01"\n""\n\n"2024-01-02"\n'
    assert parse_partition_values(out) == ["2024-01-01", "2024-01-02", "2024-01-03", None]
    assert parse_partition_values("") == []
    with pytest.raises(ValueError, match="unexpected partition value"):
        parse_partition_values('"2024-01-01"\n"Query failed"\n')


# -- the run path and the record -------------------------------------------------


def _cfg():
    from lakebench.config.schema import TableNamesConfig

    cfg = MagicMock()
    cfg.get_namespace.return_value = "lakebench-test"
    cfg.architecture.query_engine.type.value = "trino"
    cfg.architecture.query_engine.trino.catalog_name = "lakehouse"
    cfg.architecture.table_format.type.value = "iceberg"
    cfg.architecture.tables = TableNamesConfig()
    cfg.architecture.workload.schema_type.value = "customer360"
    return cfg


class _Trino:
    """exec_in_pod for a Trino coordinator: answers the partition read and
    fails the statements *fail* matches."""

    def __init__(self, partitions: list[str], fail=lambda sql: False, read_rc: int = 0):
        self.partitions = partitions
        self.fail = fail
        self.read_rc = read_rc
        self.statements: list[str] = []

    def __call__(self, pod, argv, namespace, container=None, timeout=30):
        sql = argv[2]
        if "$partitions" in sql:
            if self.read_rc:
                return self.read_rc, "", "Query failed: Table not found"
            return 0, "".join(f'"{p}"\n' for p in self.partitions), ""
        self.statements.append(sql)
        if self.fail(sql):
            return 1, "", WRITER_LIMIT
        return 0, "", ""


def _compact(trino: _Trino, **kw) -> list[dict]:
    from lakebench.cli._sustained import _run_iceberg_compaction

    k8s = MagicMock()
    k8s.exec_in_pod.side_effect = trino
    outcomes: list[dict] = []
    with patch(
        "lakebench.deploy.iceberg.find_maintenance_engine",
        return_value=("trino", "trino-coordinator-0", "lakehouse"),
    ):
        _run_iceberg_compaction(
            _cfg(), k8s, Console(quiet=True), MagicMock(), outcomes=outcomes, **kw
        )
    return outcomes


def _effective(outcomes: list[dict]) -> dict:
    return effective_maintenance(
        MAINTENANCE_POLICY_ID,
        table_format="iceberg",
        query_engine="trino",
        mode="continuous",
        outcomes=outcomes,
    )


def test_continuous_silver_compacts_two_of_two():
    trino = _Trino(_days(366))
    outcomes = _compact(trino, live_streams=True)
    (rec,) = outcomes
    assert (rec["total"], rec["succeeded"], rec["failed"]) == (2, 2, 0)
    assert rec["unit"] == "tables"
    assert len(trino.statements) == 6  # five silver chunks, one gold
    eff = _effective(outcomes)
    assert eff["compaction"] == "ran"
    assert eff["detail_id"].endswith("compaction=on")
    assert eff["detail"]["compaction_failures"] == []
    assert eff["detail"]["compaction_statements"] == trino.statements
    assert not [r for r in eff["reasons"] if "compaction" in r]


def test_failed_compaction_names_table_in_effective_maintenance():
    # One silver chunk fails: silver counts as failed, gold as compacted.
    trino = _Trino(_days(366), fail=lambda sql: "<= DATE '2024-06-28'" in sql)
    outcomes = _compact(trino, live_streams=True)
    (rec,) = outcomes
    assert (rec["total"], rec["succeeded"], rec["failed"]) == (2, 1, 1)
    (failure,) = rec["failures"]
    assert failure["table"] == SILVER
    assert "<= DATE '2024-06-28'" in failure["statement"]
    # The cause, not kubectl's container notice.
    assert failure["error"].startswith("Query 20260929_210101_00042_abcde failed: Exceeded limit")
    assert "Defaulted container" not in failure["error"] and "\n" not in failure["error"]

    eff = _effective(outcomes)
    # Partial stays out of the identity (RR-2, C27) and is named in detail.
    assert eff["id"].endswith("compaction=ran")
    assert eff["detail_id"].endswith("compaction=partial")
    assert eff["detail"]["compaction_failures"] == [SILVER]
    assert eff["detail"]["compaction_statements"] == trino.statements
    assert "compaction: 1 of 2 table compactions succeeded" in eff["reasons"]
    named = [r for r in eff["reasons"] if r.startswith(f"compaction failed on {SILVER}: ")]
    assert len(named) == 1 and "Exceeded limit of 100 open writers" in named[0]


def test_partly_compacted_tables_are_partial_not_failed():
    """Every table has a failed chunk, but statements succeeded: the id rule
    (failed means no statement succeeded) is unchanged by per-table counts."""
    trino = _Trino(_days(366), fail=lambda sql: "<= DATE '2024-06-28'" in sql or GOLD in sql)
    outcomes = _compact(trino, live_streams=True)
    (rec,) = outcomes
    assert (rec["total"], rec["succeeded"], rec["failed"]) == (2, 0, 2)
    assert (rec["statements_total"], rec["statements_succeeded"]) == (6, 4)
    eff = _effective(outcomes)
    assert eff["id"].endswith("compaction=ran")
    assert eff["detail_id"].endswith("compaction=partial")
    assert eff["detail"]["compaction_failures"] == [SILVER, GOLD]


def test_every_statement_failed_is_still_failed():
    trino = _Trino(_days(366), fail=lambda sql: True)
    eff = _effective(_compact(trino))
    assert eff["id"].endswith("compaction=failed")
    assert "compaction: 0 of 6 statements succeeded" in eff["reasons"]


def test_repeated_failure_across_rounds_is_one_reason():
    """The Trino query id differs per statement; the reason must not."""
    outcomes: list[dict] = []
    for _round in range(3):
        trino = _Trino(_days(366), fail=lambda sql: GOLD in sql)
        outcomes += _compact(trino, live_streams=True)
    for n, rec in enumerate(outcomes):
        rec["failures"][0]["error"] = rec["failures"][0]["error"].replace("_00042_", f"_0004{n}_")
    eff = _effective(outcomes)
    named = [r for r in eff["reasons"] if r.startswith(f"compaction failed on {GOLD}: ")]
    assert len(named) == 1, eff["reasons"]
    assert named[0].endswith("(3 times)")
    assert "Query 2026" not in named[0]
    assert "compaction: 3 of 6 table compactions succeeded" in eff["reasons"]


def test_partition_read_has_its_own_bounded_timeout():
    from lakebench.cli._sustained import _PARTITION_READ_TIMEOUT, MaintenanceBudget

    timeouts: dict[str, list[int]] = {"read": [], "optimize": []}

    class _Timed(_Trino):
        def __call__(self, pod, argv, namespace, container=None, timeout=30):
            timeouts["read" if "$partitions" in argv[2] else "optimize"].append(timeout)
            return super().__call__(pod, argv, namespace, container, timeout)

    _compact(_Timed(_days(10)), timeout=1800)
    assert timeouts["read"] == [_PARTITION_READ_TIMEOUT] and _PARTITION_READ_TIMEOUT <= 120
    assert set(timeouts["optimize"]) == {1800}
    # Under a budget the read gets no more than what is left of it.
    timeouts["read"].clear()
    _compact(_Timed(_days(10)), budget=MaintenanceBudget(40))
    assert timeouts["read"] and timeouts["read"][0] < _PARTITION_READ_TIMEOUT


def test_settle_trigger_counts_compaction_statements_not_tables():
    from lakebench.cli._run import _maintenance_statements_attempted

    trino = _Trino(_days(366))
    outcomes = _compact(trino)
    # Five silver chunks and one gold statement ran, though two tables did.
    assert _maintenance_statements_attempted(outcomes) == 6


def test_failed_partition_read_falls_back_and_says_so():
    trino = _Trino(_days(366), read_rc=1)
    outcomes = _compact(trino)
    assert len(trino.statements) == 2  # one unchunked statement per table
    eff = _effective(outcomes)
    assert any(
        r.startswith(f"compaction: partition read failed on {SILVER}") for r in eff["reasons"]
    )


def test_spent_budget_skips_the_partition_read():
    from lakebench.cli._sustained import MaintenanceBudget

    budget = MaintenanceBudget(60)
    budget.stopped = "pre-benchmark maintenance exceeded its 60s cap"
    trino = _Trino(_days(366))
    k8s_calls = _compact(trino, budget=budget)
    (rec,) = k8s_calls
    assert trino.statements == []
    assert (rec["total"], rec["not_attempted"]) == (2, 2)


def test_record_without_new_fields_keeps_old_detail():
    """A v1.6 record (statement counts, no failures or statements) loads as
    before: no compaction_failures key, the statement wording."""
    old = [{"kind": "compaction", "engine": "trino", "total": 2, "succeeded": 1, "failed": 1}]
    eff = _effective(old)
    assert "compaction_failures" not in eff["detail"]
    assert "compaction: 1 of 2 statements succeeded" in eff["reasons"]


@pytest.mark.parametrize(
    ("message", "cause"),
    [
        # Trino behind kubectl's container notice (the live LB-210 shape).
        (f"exec_sql failed (rc=1): {WRITER_LIMIT}", "Query 20260929_210101_00042_abcde failed:"),
        # Beeline: log lines before the error must not win.
        (
            "exec_sql failed (rc=2): SLF4J: Class path contains multiple SLF4J bindings. | "
            "Error: Error while compiling statement: FAILED: AnalysisException no table",
            "Error: Error while compiling statement: FAILED: AnalysisException",
        ),
        ("query_sql failed (rc=1): Query 2026 failed: Table not found", "Query 2026 failed:"),
    ],
)
def test_error_line_names_the_cause(message, cause):
    from lakebench.cli._sustained import _error_line

    line = _error_line(message)
    assert line.startswith(cause), line
    assert "Defaulted container" not in line and "SLF4J" not in line and "\n" not in line
