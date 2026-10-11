"""Trino compaction of the months()-partitioned AML silver tables.

One unchunked ``optimize`` over a table whose months hold many small
micro-batch files exceeds Trino's per-node memory limit. The plan compacts
those tables one month per statement, with a range on the source column that
Trino's Iceberg connector enforces on a months() partition (both bounds on a
month start, 00:00 UTC), and the record names the operation as for any
other table.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import pytest

from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance
from lakebench.modules.table_formats.iceberg.maintenance import (
    build_compaction_plan,
    build_partition_values_sql,
    compaction_partitioning,
    data_size_bytes,
    parse_partition_values,
)
from tests.fixtures.compaction_helpers import Trino, compact, trino_cfg

TXNS = "lakehouse.silver.transactions"
STMTS = "lakehouse.silver.account_statements"
HEAD = "EXECUTE optimize(file_size_threshold => '128MB')"
# A memory-limit failure's stderr, kubectl's notice first.
MEMORY_LIMIT = (
    'Defaulted container "trino" out of: trino, wait-for-hive (init)\n'
    "Query 20240101_000000_00001_abcde failed: Query exceeded per-node memory limit of "
    "2.24GB [Allocated: 2.23GB, Delta: 6.94MB, Top Consumers: {TableWriterOperator=2.06GB, "
    "TableScanOperator-ConnectorPageSource=161MB, LazyOutputBuffer=21.74MB}]"
)

# Iceberg's month transform: months since 1970-01. 648 is 2024-01.
JAN_2024 = 648


def _months(n: int, first: int = JAN_2024, files: int = 5) -> list[str]:
    """The Trino CLI's quoted CSV of *n* month-transform values, each with
    *files* data files."""
    return [f'"{first + i}","{files}"' for i in range(n)]


def _parsed(n: int, first: int = JAN_2024) -> list[str | None]:
    return parse_partition_values("\n".join(_months(n, first)) + "\n", "month")


_CLAUSE = re.compile(r"^(\w+) (<|>=) TIMESTAMP '(\d{4}-\d{2}-\d{2}) 00:00:00\.000000 UTC'$")


def _selects(stmt: str, column: str, ts: datetime) -> bool:
    """Whether a chunk's WHERE selects a row with *column* = *ts* (UTC)."""
    where = stmt.split(" WHERE ", 1)[1]
    if where == f"{column} IS NOT NULL":
        return True
    if where == f"{column} IS NULL":
        return False
    for clause in where.split(" AND "):
        m = _CLAUSE.match(clause)
        assert m and m.group(1) == column, clause
        bound = datetime.fromisoformat(m.group(3)).replace(tzinfo=timezone.utc)
        if (m.group(2) == "<" and not ts < bound) or (m.group(2) == ">=" and not ts >= bound):
            return False
    return True


# -- the plan -----------------------------------------------------------------


def test_partition_map_resolves_quoted_identifiers_and_only_aml_silver():
    txns = compaction_partitioning(TXNS)
    assert txns is not None
    assert compaction_partitioning('lakehouse."silver"."transactions"') == txns
    # The bare name "transactions" in another schema is not an AML silver table.
    assert compaction_partitioning("lakehouse.gold.transactions") is None
    assert compaction_partitioning("lakehouse.silver.accounts") is None


def test_thirteen_months_compact_one_month_per_statement():
    plan = build_compaction_plan("trino", "lakehouse", TXNS, "128MB", _parsed(13))
    assert len(plan) == 13
    for stmt in plan:
        assert stmt.startswith(f"ALTER TABLE {TXNS} {HEAD} WHERE txn_timestamp ")
    # Open below and above, half-open month ranges between.
    assert plan[0].endswith("WHERE txn_timestamp < TIMESTAMP '2024-02-01 00:00:00.000000 UTC'")
    assert plan[1].endswith(
        "WHERE txn_timestamp >= TIMESTAMP '2024-02-01 00:00:00.000000 UTC' "
        "AND txn_timestamp < TIMESTAMP '2024-03-01 00:00:00.000000 UTC'"
    )
    assert plan[-1].endswith("WHERE txn_timestamp >= TIMESTAMP '2025-01-01 00:00:00.000000 UTC'")
    # Every instant falls in exactly one statement: month edges, the
    # microsecond before them, and instants before and after the months read
    # (a stream may add one after the read).
    edges = [datetime(2024, m, 1, tzinfo=timezone.utc) for m in range(1, 13)]
    edges.append(datetime(2025, 1, 1, tzinfo=timezone.utc))
    probes = [e + d for e in edges for d in (timedelta(0), timedelta(microseconds=-1))]
    probes += [
        datetime(2019, 6, 1, tzinfo=timezone.utc),
        datetime(2024, 7, 15, 12, 30, tzinfo=timezone.utc),
        datetime(2031, 1, 1, tzinfo=timezone.utc),
    ]
    for ts in probes:
        hits = [n for n, stmt in enumerate(plan) if _selects(stmt, "txn_timestamp", ts)]
        assert len(hits) == 1, (ts, hits)
    # Each listed month is alone in its statement.
    for n, month in enumerate(edges[:-1]):
        mid = month + timedelta(days=10)
        assert [i for i, s in enumerate(plan) if _selects(s, "txn_timestamp", mid)] == [n]


def test_account_statements_chunk_on_book_ts():
    plan = build_compaction_plan("trino", "lakehouse", STMTS, "128MB", _parsed(3))
    assert [p.split(" WHERE ")[1] for p in plan] == [
        "book_ts < TIMESTAMP '2024-02-01 00:00:00.000000 UTC'",
        "book_ts >= TIMESTAMP '2024-02-01 00:00:00.000000 UTC' "
        "AND book_ts < TIMESTAMP '2024-03-01 00:00:00.000000 UTC'",
        "book_ts >= TIMESTAMP '2024-03-01 00:00:00.000000 UTC'",
    ]


def test_gaps_between_months_read_fold_into_a_neighbour():
    """A month missing from the read (no files yet, or a day-spec partition
    on an evolved table) is still in exactly one statement."""
    values = parse_partition_values('"648","4"\n"651","4"\n', "month")
    assert values == ["2024-01-01", "2024-04-01"]
    plan = build_compaction_plan("trino", "lakehouse", TXNS, "128MB", values)
    assert len(plan) == 2
    feb = datetime(2024, 2, 15, tzinfo=timezone.utc)
    assert [_selects(s, "txn_timestamp", feb) for s in plan] == [True, False]


def test_one_month_or_none_keeps_one_unchunked_statement():
    single = [f"ALTER TABLE {TXNS} {HEAD}"]
    assert build_compaction_plan("trino", "lakehouse", TXNS, "128MB", _parsed(1)) == single
    assert build_compaction_plan("trino", "lakehouse", TXNS, "128MB", []) == single
    assert build_compaction_plan("trino", "lakehouse", TXNS, "128MB", None) == single
    # Spark Thrift's rewrite_data_files is not chunked.
    assert build_compaction_plan("spark-thrift", "lakehouse", TXNS, "128MB", _parsed(13)) == [
        f"CALL lakehouse.system.rewrite_data_files(table => '{TXNS}')"
    ]


def test_null_month_partition_gets_its_own_statement():
    values = parse_partition_values('"648","4"\n"","2"\n', "month")
    assert values == ["2024-01-01", None]
    plan = build_compaction_plan("trino", "lakehouse", TXNS, "128MB", values)
    assert [p.split(" WHERE ")[1] for p in plan] == [
        "txn_timestamp IS NOT NULL",
        "txn_timestamp IS NULL",
    ]


@pytest.mark.parametrize(("threshold", "nbytes"), [("128MB", 134217728), ("256MB", 268435456)])
def test_month_read_counts_files_at_or_under_the_threshold(threshold, nbytes):
    """Optimize may rewrite a file at or under the threshold (Trino drops a
    larger one before it groups by partition)."""
    sql = build_partition_values_sql(TXNS, "txn_timestamp_month", "month", threshold)
    assert sql.endswith(f"file_size_in_bytes <= {nbytes} GROUP BY 1 ORDER BY 1")


def test_data_size_bytes_uses_trino_binary_units():
    assert data_size_bytes("128MB") == 128 * 1024 * 1024
    assert data_size_bytes("1GB") == 1 << 30
    assert data_size_bytes("512kB") == 512 * 1024
    assert data_size_bytes("1.5MB") == 1572864
    for bad in ("128mb", "128", "MB", "", "-1MB"):
        with pytest.raises(ValueError, match="data size"):
            data_size_bytes(bad)


def test_parse_month_values():
    out = '"659","2"\n"648","3"\n"0","9"\n"-1","2"\n'
    assert parse_partition_values(out, "month") == [
        "1969-12-01",
        "1970-01-01",
        "2024-01-01",
        "2024-12-01",
    ]
    # A date where a month number belongs, a row without its file count, or
    # a month number in an identity read is refused.
    for bad in ('"2024-01-01","2"\n', '"648"\n', '"648","x"\n', '"648","2","1"\n'):
        with pytest.raises(ValueError, match="unexpected partition value"):
            parse_partition_values(bad, "month")
    with pytest.raises(ValueError, match="unexpected partition value"):
        parse_partition_values('"648"\n', "identity")
    with pytest.raises(ValueError, match="unsupported partition transform"):
        parse_partition_values('"648"\n', "day")


def test_months_with_one_file_are_not_counted():
    """Trino does not rewrite a partition's only data file (no deletes),
    so a month of one file adds no writer: it is not counted, and it falls in
    the statement of the month before it (or the first, open below)."""
    out = '"648","1"\n"649","7"\n"650","1"\n"651","1"\n"652","9"\n"653","1"\n'
    values = parse_partition_values(out, "month")
    assert values == ["2024-02-01", "2024-05-01"]
    plan = build_compaction_plan("trino", "lakehouse", TXNS, "128MB", values)
    assert [p.split(" WHERE ")[1] for p in plan] == [
        "txn_timestamp < TIMESTAMP '2024-05-01 00:00:00.000000 UTC'",
        "txn_timestamp >= TIMESTAMP '2024-05-01 00:00:00.000000 UTC'",
    ]
    # The row counts of one month (several rows: one per spec's partition
    # struct) are summed before the threshold.
    assert parse_partition_values('"648","1"\n"648","1"\n', "month") == ["2024-01-01"]


def test_batch_layout_keeps_one_statement():
    """Batch AML silver holds one or two large files a month over a 60-month
    corpus: at most one month counts, so the table keeps one statement."""
    out = "\n".join(_months(60, first=JAN_2024 - 48, files=1)) + '\n"650","2"\n'
    values = parse_partition_values(out, "month")
    assert values == ["2024-03-01"]
    assert build_compaction_plan("trino", "lakehouse", TXNS, "128MB", values) == [
        f"ALTER TABLE {TXNS} {HEAD}"
    ]


def test_null_month_counts_every_month():
    """A NULL month (a NULL timestamp, or a file under an older days() spec)
    makes Trino rewrite every file in range, so every month counts."""
    out = '"648","1"\n"649","1"\n"","1"\n'
    assert parse_partition_values(out, "month") == ["2024-01-01", "2024-02-01", None]


# -- the run path -----------------------------------------------------------------


def _cfg():
    from lakebench.config.schema import TableNamesConfig

    # What ArchitectureConfig.financial_table_defaults resolves on AML; in
    # continuous mode silver.counterparty_pairs exists too.
    return trino_cfg(
        "financial", tables=TableNamesConfig(silver="silver.transactions"), mode="continuous"
    )


def _months_in(sql: str) -> int:
    """How many of the 13 small-file months the statement's WHERE selects."""
    column = "txn_timestamp" if TXNS in sql else "book_ts"
    mids = [datetime(2024 + (m // 12), m % 12 + 1, 15, tzinfo=timezone.utc) for m in range(13)]
    return sum(_selects(sql, column, ts) for ts in mids)


def _trino(read_rc: int = 0) -> Trino:
    """A coordinator whose AML month tables hold the 60-month corpus, 47
    months of one large file each and 13 months (2024-01 to 2025-01) of small
    micro-batch files: an optimize that rewrites more than one of the 13
    exceeds the per-node memory limit."""

    def read(sql):
        if read_rc:
            return read_rc, "", "Query failed"
        rows = _months(47, first=JAN_2024 - 47, files=1) + _months(13, files=40)
        return 0, "\n".join(rows) + "\n", ""

    def fail(sql):
        month_table = TXNS in sql or STMTS in sql
        if month_table and (" WHERE " not in sql or _months_in(sql) > 1):
            return MEMORY_LIMIT
        return None

    return Trino(read=read, fail=fail)


def _compact(trino: Trino, **kw) -> list[dict]:
    return compact(trino, _cfg(), **kw)


def test_continuous_aml_silver_compacts_every_table():
    """Continuous AML compacts the 4 silver tables
    silver-stream only appends to (the 4 it MERGEs into are left, see
    _run_iceberg_compaction), and the two month tables no longer fail on the
    per-node memory limit."""
    trino = _trino()
    outcomes = _compact(trino, live_streams=True)
    (rec,) = outcomes
    assert (rec["total"], rec["succeeded"], rec["failed"]) == (4, 4, 0), rec.get("failures")
    assert rec["failures"] == []
    assert sorted(trino.reads) == [
        'SELECT partition.book_ts_month, count(*) FROM lakehouse.silver."account_statements'
        '$files" WHERE content = 0 AND file_size_in_bytes <= 134217728 GROUP BY 1 ORDER BY 1',
        'SELECT partition.txn_timestamp_month, count(*) FROM lakehouse.silver."transactions'
        '$files" WHERE content = 0 AND file_size_in_bytes <= 134217728 GROUP BY 1 ORDER BY 1',
    ]
    # 13 + 13 month statements and one each for counterparty_edges and
    # counterparty_pairs.
    assert rec["statements_total"] == len(trino.statements) == 28
    assert sum(TXNS in s for s in trino.statements) == 13
    assert sum(STMTS in s for s in trino.statements) == 13
    # The record names the operation as before.
    assert (rec["operation"], rec["params"]) == ("trino_optimize", {"file_size_threshold": "128MB"})
    eff = effective_maintenance(
        MAINTENANCE_POLICY_ID,
        table_format="iceberg",
        query_engine="trino",
        mode="continuous",
        outcomes=outcomes,
    )
    assert eff["detail_id"].endswith("compaction=on")
    assert eff["detail"]["compaction_failures"] == []


@pytest.mark.parametrize(
    ("read_rc", "kw", "reads_issued"),
    [
        (1, {}, True),  # the month read fails
        (0, {"file_size_threshold": "128mb"}, False),  # a threshold it cannot turn into bytes
    ],
)
def test_an_unusable_month_read_falls_back_to_one_statement_per_table(read_rc, kw, reads_issued):
    trino = _trino(read_rc=read_rc)
    (rec,) = _compact(trino, live_streams=True, **kw)
    assert bool(trino.reads) is reads_issued
    # every table is still attempted, unchunked
    assert (rec["total"], rec["statements_total"]) == (4, 4)
    assert (rec["succeeded"], rec["failed"]) == (2, 2)
    assert "partition read failed on lakehouse.silver.transactions" in rec["note"]
