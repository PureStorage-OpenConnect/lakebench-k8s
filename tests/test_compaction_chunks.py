"""Trino compaction of the partitioned C360 silver table.

Trino's Iceberg connector refuses an optimize that opens more than 100
partition writers, so the plan chunks the table by its identity partition
column at 90 partitions per statement. A table counts as compacted only when
every chunk succeeds, a failed table is named in the effective maintenance
record, and with live streams the silver tables a MERGE targets are not
compacted.
"""

from __future__ import annotations

from datetime import date, timedelta

from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance
from lakebench.modules.table_formats.iceberg.maintenance import (
    COMPACTION_CHUNK_PARTITIONS,
    build_compaction_plan,
)
from tests.fixtures.compaction_helpers import Trino, compact, trino_cfg

SILVER = "lakehouse.silver.customer_interactions_enriched"
GOLD = "lakehouse.gold.customer_executive_dashboard"
# The stderr shape of the failure: kubectl's container notice first, then
# Trino's error.
WRITER_LIMIT = (
    'Defaulted container "trino" out of: trino, wait-for-hive (init)\n'
    "Query 20240101_000000_00001_abcde failed: Exceeded limit of 100 open writers "
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


# -- the run path and the record -------------------------------------------------


def _trino(days: list[str], fail=lambda sql: False) -> Trino:
    """A coordinator whose silver table holds *days* as partitions; it fails
    the statements *fail* matches."""
    return Trino(
        read=lambda sql: (0, "".join(f'"{d}"\n' for d in days), ""),
        fail=lambda sql: WRITER_LIMIT if fail(sql) else None,
    )


def _compact(trino: Trino, **kw) -> list[dict]:
    return compact(trino, trino_cfg("customer360"), **kw)


def _effective(outcomes: list[dict]) -> dict:
    return effective_maintenance(
        MAINTENANCE_POLICY_ID,
        table_format="iceberg",
        query_engine="trino",
        mode="continuous",
        outcomes=outcomes,
    )


def test_continuous_silver_compacts_two_of_two():
    trino = _trino(_days(366))
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
    trino = _trino(_days(366), fail=lambda sql: "<= DATE '2024-06-28'" in sql)
    outcomes = _compact(trino, live_streams=True)
    (rec,) = outcomes
    assert (rec["total"], rec["succeeded"], rec["failed"]) == (2, 1, 1)
    (failure,) = rec["failures"]
    assert failure["table"] == SILVER
    assert "<= DATE '2024-06-28'" in failure["statement"]
    # The cause, not kubectl's container notice.
    assert failure["error"].startswith("Query 20240101_000000_00001_abcde failed: Exceeded limit")
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
    trino = _trino(_days(366), fail=lambda sql: "<= DATE '2024-06-28'" in sql or GOLD in sql)
    outcomes = _compact(trino, live_streams=True)
    (rec,) = outcomes
    assert (rec["total"], rec["succeeded"], rec["failed"]) == (2, 0, 2)
    assert (rec["statements_total"], rec["statements_succeeded"]) == (6, 4)
    eff = _effective(outcomes)
    assert eff["id"].endswith("compaction=ran")
    assert eff["detail_id"].endswith("compaction=partial")
    assert eff["detail"]["compaction_failures"] == [SILVER, GOLD]


def test_every_statement_failed_is_still_failed():
    trino = _trino(_days(366), fail=lambda sql: True)
    eff = _effective(_compact(trino))
    assert eff["id"].endswith("compaction=failed")
    assert "compaction: 0 of 6 statements succeeded" in eff["reasons"]


def test_spent_budget_skips_the_partition_read():
    from lakebench.cli._sustained import MaintenanceBudget

    budget = MaintenanceBudget(60)
    budget.stopped = "pre-benchmark maintenance exceeded its 60s cap"
    trino = _trino(_days(366))
    k8s_calls = _compact(trino, budget=budget)
    (rec,) = k8s_calls
    assert trino.statements == []
    assert (rec["total"], rec["not_attempted"]) == (2, 2)


def test_live_aml_compaction_leaves_the_tables_silver_merges_into():
    """A rewrite committed while silver-stream's MERGE is planned fails the
    MERGE ("Missing required files to delete") and ends the stream, so with
    live streams only the silver tables it appends to are compacted."""
    cfg = trino_cfg("financial")
    trino = _trino(_days(30))
    compact(trino, cfg, live_streams=True)
    touched = {
        t
        for t in cfg.architecture.tables.workload_tables("financial")
        if any(f"lakehouse.{t}" in s for s in trino.statements)
    }
    merged = {
        "silver.entities",
        "silver.accounts",
        "silver.entity_profiles",
        "silver.silver_batch_versions",
    }
    assert touched and not touched & merged
    assert not any(t.startswith("gold.") for t in touched)
