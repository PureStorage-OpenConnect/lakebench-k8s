"""FQ4 and IQ3 answer the same on batch and continuous silver, on DuckDB.

The Spark tier (tests/spark/test_aml_queries_mode_invariant_spark.py) runs
the product's batch and micro-batch write paths. This test runs the same
two queries on DuckDB, one of the engines the benchmark runs them on,
against tables laid out the two ways by hand:

* batch: one edge row per (source, target) pair; statements numbered and
  balanced in ledger order (book_ts, txn_id, debit first) from the opening
  balance;
* continuous: the same payments in three micro-batches, the second earlier
  in time than the first; one edge row per pair per micro-batch; statements
  numbered and balanced in arrival order, each micro-batch continuing from
  the account's last stored balance (silver_stream_financial's rule).

Batch answers are unchanged from the previous SQL, continuous answers equal
the batch ones, and the previous SQL reads the two layouts differently.
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from lakebench.benchmark.queries import INVESTIGATOR_QUERIES, get_benchmark_queries
from lakebench.config.schema import WorkloadSchema

duckdb = pytest.importorskip("duckdb")

BASE = datetime(2024, 6, 1)
OPENING = {"IS": Decimal("1000.00"), "IA": Decimal("2000.00"), "IB": Decimal("50.00")}
ENTITY = {"S": 1, "A": 2, "B": 3, "C": 4}
IBAN = {"S": "IS", "A": "IA", "B": "IB", "C": "IB"}  # C banks with B: one shared account

# (txn_id, hour, debtor, creditor, amount) per micro-batch; batch 1 is late.
MICRO_BATCHES = [
    [
        ("T01", 10, "S", "A", "100.00"),
        ("T02", 11, "A", "B", "40.00"),
        ("T03", 12, "A", "C", "30.00"),
    ],
    [("T11", 1, "S", "A", "50.00"), ("T12", 2, "A", "B", "20.00")],
    [("T21", 20, "A", "B", "10.00"), ("T22", 21, "A", "C", "7.00"), ("T23", 21, "B", "C", "1.00")],
]

OLD_FQ4 = """\
WITH top_accts AS (
  SELECT account_id
  FROM {catalog}.{silver_account_statements}
  GROUP BY account_id
  ORDER BY COUNT(*) DESC, account_id
  LIMIT 50
)
SELECT
  s.account_id,
  s.book_ts,
  s.cdt_dbt_ind,
  s.amt,
  s.bal_after,
  ROW_NUMBER() OVER (PARTITION BY s.account_id ORDER BY s.book_ts, s.entry_seq) AS entry_ord
FROM {catalog}.{silver_account_statements} s
JOIN top_accts t ON t.account_id = s.account_id
ORDER BY s.account_id, entry_ord"""

OLD_IQ3_HOP2 = """hop2 AS (
  SELECT h.cp AS via_entity_id, e.target_entity_id AS hop2_entity_id,
         e.cumulative_amount_usd AS amount_usd
  FROM {catalog}.{silver_counterparty_edges} e
  JOIN hop1 h ON e.source_entity_id = h.cp
),"""


def _entries(batch):
    out = []
    for txn, hour, dbtr, cdtr, amt in batch:
        ts = BASE + timedelta(hours=hour)
        out.append((IBAN[dbtr], ts, txn, 0, "DBIT", Decimal(amt)))
        out.append((IBAN[cdtr], ts, txn, 1, "CRDT", Decimal(amt)))
    return out


def _signed(e):
    return e[5] if e[4] == "CRDT" else -e[5]


def _statements_batch():
    rows = []
    by_acct = defaultdict(list)
    for e in _entries([p for b in MICRO_BATCHES for p in b]):
        by_acct[e[0]].append(e)
    for acct, es in by_acct.items():
        bal = OPENING[acct]
        for seq, e in enumerate(sorted(es, key=lambda e: (e[1], e[2], e[3])), 1):
            bal += _signed(e)
            rows.append((acct, seq, e[1], e[4], e[5], bal, e[2]))
    return rows


def _statements_stream():
    rows = []
    last = dict(OPENING)
    seq = defaultdict(int)
    for batch in MICRO_BATCHES:
        for e in sorted(_entries(batch), key=lambda e: (e[0], e[1], e[2], e[3])):
            acct = e[0]
            last[acct] += _signed(e)
            seq[acct] += 1
            rows.append((acct, seq[acct], e[1], e[4], e[5], last[acct], e[2]))
    return rows


def _edges(batches):
    rows = []
    for batch in batches:
        pair = defaultdict(lambda: [Decimal(0), 0])
        for _txn, _h, dbtr, cdtr, amt in batch:
            pair[(ENTITY[dbtr], ENTITY[cdtr])][0] += Decimal(amt)
            pair[(ENTITY[dbtr], ENTITY[cdtr])][1] += 1
        rows += [(s, t, a, n) for (s, t), (a, n) in pair.items()]
    return rows


@pytest.fixture
def conn():
    c = duckdb.connect()
    acct_id = {"IS": 11, "IA": 12, "IB": 13}
    for name, rows in (
        ("stmts_batch", _statements_batch()),
        ("stmts_stream", _statements_stream()),
    ):
        c.execute(
            f"CREATE TABLE {name}(account_id BIGINT, entry_seq BIGINT, book_ts TIMESTAMP, "
            "cdt_dbt_ind VARCHAR, amt DECIMAL(18,2), bal_after DECIMAL(38,2), txn_id VARCHAR)"
        )
        c.executemany(
            f"INSERT INTO {name} VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(acct_id[r[0]], *r[1:]) for r in rows],
        )
    for name, rows in (
        ("edges_batch", _edges([[p for b in MICRO_BATCHES for p in b]])),
        ("edges_stream", _edges(MICRO_BATCHES)),
    ):
        c.execute(
            f"CREATE TABLE {name}(source_entity_id BIGINT, target_entity_id BIGINT, "
            "cumulative_amount_usd DECIMAL(38,2), txn_count BIGINT)"
        )
        c.executemany(f"INSERT INTO {name} VALUES (?, ?, ?, ?)", rows)
    c.execute(
        "CREATE TABLE entities(entity_id BIGINT, name VARCHAR, is_customer BOOLEAN, "
        "country VARCHAR, crr_tier VARCHAR)"
    )
    c.executemany(
        "INSERT INTO entities VALUES (?, ?, true, 'GB', 'low')",
        [(i, n) for n, i in ENTITY.items()],
    )
    c.execute(
        "CREATE TABLE cases(case_id VARCHAR, customer_id BIGINT, base_run_id VARCHAR, "
        "case_status VARCHAR, opened_date DATE)"
    )
    c.execute("INSERT INTO cases VALUES ('c1', 1, 'run-1', 'open', DATE '2024-06-02')")
    c.execute("CREATE TABLE dispositions(entity_id BIGINT, base_run_id VARCHAR)")
    c.execute("INSERT INTO dispositions VALUES (3, 'run-1')")
    return c


def _sql():
    fq4 = next(
        q.sql
        for q in get_benchmark_queries(WorkloadSchema.FINANCIAL)
        if q.name == "FQ4_running_balance_window"
    )
    iq3 = next(q.sql for q in INVESTIGATOR_QUERIES if q.name == "IQ3_counterparty_two_hop")
    old_iq3, n = re.subn(r"hop2 AS \(.*?\n\),", OLD_IQ3_HOP2, iq3, count=1, flags=re.S)
    assert n == 1
    return {"FQ4": {"new": fq4, "old": OLD_FQ4}, "IQ3": {"new": iq3, "old": old_iq3}}


def _answer(conn, text, mode):
    q = text.format(
        catalog="memory",
        silver_account_statements=f"main.stmts_{mode}",
        silver_counterparty_edges=f"main.edges_{mode}",
        silver_entities="main.entities",
        gold_cases="main.cases",
        gold_alert_dispositions="main.dispositions",
        tm_run_id="run-1",
    )
    return [tuple(str(v) for v in r) for r in conn.execute(q).fetchall()]


@pytest.mark.parametrize("query", ["FQ4", "IQ3"])
def test_batch_answer_unchanged_and_continuous_matches_it(conn, query):
    sql = _sql()[query]
    got = {(v, m): _answer(conn, sql[v], m) for v in ("new", "old") for m in ("batch", "stream")}
    assert got[("new", "batch")], got
    assert got[("new", "batch")] == got[("old", "batch")]
    assert got[("new", "stream")] == got[("new", "batch")]
    # The fixture has the layout difference the previous SQL read.
    assert got[("old", "stream")] != got[("old", "batch")]


def test_fq4_balances_are_the_ledger_order_ones(conn):
    rows = _answer(conn, _sql()["FQ4"]["new"], "stream")
    ia = [r for r in rows if r[0] == "12"]
    # IA: opening 2000, +50 (T11), -20 (T12), +100 (T01), -40 (T02), -30 (T03),
    # -10 (T21), -7 (T22), in book_ts order.
    assert [r[4] for r in ia] == [
        "2050.00",
        "2030.00",
        "2130.00",
        "2090.00",
        "2060.00",
        "2050.00",
        "2043.00",
    ]
    assert [r[5] for r in ia] == [str(i) for i in range(1, 8)]
