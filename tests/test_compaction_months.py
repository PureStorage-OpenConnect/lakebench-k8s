"""Trino compaction of the months()-partitioned AML silver tables (LB-273).

Continuous AML s1 (lb17-qr32-cont, run-20261003-175243-5496fc) ran one
unchunked ``optimize`` on silver.transactions and silver.account_statements
at about 4,320 s, with 12 to 13 monthly partitions of small micro-batch
files, and both failed: "Query exceeded per-node memory limit of 2.24GB
[... TableWriterOperator=2.06GB ...]". The plan now compacts those tables
one month per statement, with a range on the source column that Trino's
Iceberg connector enforces on a months() partition (both bounds on a month
start, 00:00 UTC), and the record names the operation as before.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance
from lakebench.modules.table_formats.iceberg.maintenance import (
    COMPACTION_CHUNK_MONTHS,
    build_compaction_plan,
    build_partition_values_sql,
    compaction_operation,
    compaction_partitioning,
    data_size_bytes,
    parse_partition_values,
)

TXNS = "lakehouse.silver.transactions"
STMTS = "lakehouse.silver.account_statements"
HEAD = "EXECUTE optimize(file_size_threshold => '128MB')"
# The live failure's stderr (lb17-qr32-cont log), kubectl's notice first.
MEMORY_LIMIT = (
    'Defaulted container "trino" out of: trino, wait-for-hive (init)\n'
    "Query 20261004_010656_00257_4jms4 failed: Query exceeded per-node memory limit of "
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


def test_aml_silver_tables_are_on_the_partition_map():
    txns = compaction_partitioning(TXNS)
    stmts = compaction_partitioning(STMTS)
    assert txns is not None and stmts is not None
    assert (txns.column, txns.transform, txns.partition_field) == (
        "txn_timestamp",
        "month",
        "txn_timestamp_month",
    )
    assert (stmts.column, stmts.transform, stmts.partition_field) == (
        "book_ts",
        "month",
        "book_ts_month",
    )
    assert txns.chunk == stmts.chunk == COMPACTION_CHUNK_MONTHS == 1
    # Quoted identifiers resolve the same; the bare name "transactions" in
    # another schema is not an AML silver table.
    assert compaction_partitioning('lakehouse."silver"."transactions"') == txns
    assert compaction_partitioning("lakehouse.gold.transactions") is None
    assert compaction_partitioning("lakehouse.silver.accounts") is None
    # C360 keeps its identity chunking.
    c360 = compaction_partitioning("lakehouse.silver.customer_interactions_enriched")
    assert c360 is not None and (c360.transform, c360.chunk) == ("identity", 90)


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


def test_partition_read_sql_names_the_month_field():
    spec = compaction_partitioning(TXNS)
    assert spec is not None
    assert build_partition_values_sql(TXNS, spec.partition_field) == (
        "SELECT DISTINCT partition.txn_timestamp_month FROM "
        'lakehouse.silver."transactions$partitions" ORDER BY 1'
    )
    # Months: data files at or under the threshold, which optimize may
    # rewrite (Trino drops a larger one before it groups by partition).
    assert build_partition_values_sql(TXNS, spec.partition_field, spec.transform) == (
        "SELECT partition.txn_timestamp_month, count(*) FROM "
        'lakehouse.silver."transactions$files" '
        "WHERE content = 0 AND file_size_in_bytes <= 134217728 GROUP BY 1 ORDER BY 1"
    )
    assert build_partition_values_sql(TXNS, "txn_timestamp_month", "month", "256MB").endswith(
        "file_size_in_bytes <= 268435456 GROUP BY 1 ORDER BY 1"
    )


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
    """Trino 483 does not rewrite a partition's only data file (no deletes),
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


def test_compaction_operation_is_unchanged():
    """Chunking is not a different operation: the record and the
    comparability key stay trino_optimize with its threshold."""
    assert compaction_operation("trino", "128MB") == {
        "operation": "trino_optimize",
        "params": {"file_size_threshold": "128MB"},
    }


# -- the run path -----------------------------------------------------------------


def _cfg():
    from lakebench.config.schema import TableNamesConfig

    cfg = MagicMock()
    cfg.get_namespace.return_value = "lakebench-test"
    cfg.architecture.query_engine.type.value = "trino"
    cfg.architecture.query_engine.trino.catalog_name = "lakehouse"
    cfg.architecture.table_format.type.value = "iceberg"
    # What ArchitectureConfig.financial_table_defaults resolves on AML.
    cfg.architecture.tables = TableNamesConfig(silver="silver.transactions")
    cfg.architecture.workload.schema_type.value = "financial"
    return cfg


class _Trino:
    """exec_in_pod for a Trino coordinator whose AML month tables hold the
    60-month corpus, 47 months of one large file each and 13 months
    (2024-01 to 2025-01) of small micro-batch files: an optimize that
    rewrites more than one of the 13 exceeds the per-node memory limit, as
    live."""

    def __init__(self, read_rc: int = 0):
        self.read_rc = read_rc
        self.reads: list[str] = []
        self.statements: list[str] = []

    def __call__(self, pod, argv, namespace, container=None, timeout=30):
        sql = argv[2]
        if "$files" in sql or "$partitions" in sql:
            self.reads.append(sql)
            if self.read_rc:
                return self.read_rc, "", "Query failed"
            rows = _months(47, first=JAN_2024 - 47, files=1) + _months(13, files=40)
            return 0, "\n".join(rows) + "\n", ""
        self.statements.append(sql)
        month_table = TXNS in sql or STMTS in sql
        if month_table and (" WHERE " not in sql or self._months_in(sql) > 1):
            return 1, "", MEMORY_LIMIT
        return 0, "", ""

    @staticmethod
    def _months_in(sql: str) -> int:
        """How many of the 13 small-file months the statement's WHERE selects."""
        column = "txn_timestamp" if TXNS in sql else "book_ts"
        mids = [datetime(2024 + (m // 12), m % 12 + 1, 15, tzinfo=timezone.utc) for m in range(13)]
        return sum(_selects(sql, column, ts) for ts in mids)


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


def test_continuous_aml_silver_compacts_every_table():
    """The live case: continuous AML compacts the 7 silver tables, and the
    two month tables no longer fail on the per-node memory limit."""
    trino = _Trino()
    outcomes = _compact(trino, live_streams=True)
    (rec,) = outcomes
    assert (rec["total"], rec["succeeded"], rec["failed"]) == (7, 7, 0), rec.get("failures")
    assert rec["failures"] == []
    assert sorted(trino.reads) == [
        'SELECT partition.book_ts_month, count(*) FROM lakehouse.silver."account_statements'
        '$files" WHERE content = 0 AND file_size_in_bytes <= 134217728 GROUP BY 1 ORDER BY 1',
        'SELECT partition.txn_timestamp_month, count(*) FROM lakehouse.silver."transactions'
        '$files" WHERE content = 0 AND file_size_in_bytes <= 134217728 GROUP BY 1 ORDER BY 1',
    ]
    # 13 + 13 month statements and one each for the other five tables.
    assert rec["statements_total"] == len(trino.statements) == 31
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


def test_failed_month_read_falls_back_to_one_statement():
    trino = _Trino(read_rc=1)
    (rec,) = _compact(trino, live_streams=True)
    assert rec["statements_total"] == 7
    assert (rec["succeeded"], rec["failed"]) == (5, 2)
    assert "partition read failed on lakehouse.silver.transactions" in rec["note"]


def test_unreadable_threshold_falls_back_and_says_so():
    """A threshold the month read cannot turn into bytes is a failed read:
    one unchunked statement per table, named in the note."""
    trino = _Trino()
    (rec,) = _compact(trino, live_streams=True, file_size_threshold="128mb")
    assert trino.reads == []
    assert rec["statements_total"] == 7
    assert "partition read failed on lakehouse.silver.transactions" in rec["note"]
    assert "data size '128mb'" in rec["note"]
