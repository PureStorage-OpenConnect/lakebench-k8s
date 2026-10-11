"""DuckDB runs every financial benchmark query, and failures say why.

The executed tests build the exact script the pod runs and execute it with
pytz blocked, against in-memory tables of the same shape: auxiliary tables
resolve to ``iceberg_scan``, TIMESTAMPTZ values reach Python without pytz, and
``cardinality`` is rewritten for lists.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys

import pytest

from lakebench.benchmark.queries import _FINANCIAL_QUERIES
from lakebench.benchmark.result import summarise_engine_error
from lakebench.config.schema import TableNamesConfig
from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor
from lakebench.modules.query_engines.duckdb.local_executor import LocalDuckDBExecutor

# ---------------------------------------------------------------------------
# Table resolution and dialect
# ---------------------------------------------------------------------------


def _financial_table_names() -> dict[str, str]:
    t = TableNamesConfig()
    names = {"silver": "silver.transactions", "gold": t.gold_daily_dashboards}
    for field in (
        "silver_entities",
        "silver_accounts",
        "silver_account_statements",
        "silver_counterparty_edges",
        "gold_alerts",
        "gold_risk_scores",
        "gold_entity_clusters",
        "gold_daily_dashboards",
        "gold_alert_dispositions",
        "gold_cases",
    ):
        names[field] = getattr(t, field)
    return names


def _executor(catalog_type: str = "hive") -> DuckDBExecutor:
    return DuckDBExecutor(
        namespace="t",
        catalog_name="lakehouse",
        s3_endpoint="http://10.0.1.50:80",
        s3_buckets={"silver": "sb", "gold": "gb"},
        table_names=_financial_table_names(),
        catalog_type=catalog_type,
    )


def _render(query) -> str:
    names = _financial_table_names()
    extra = {k: v for k, v in names.items() if k not in ("silver", "gold")}
    return query.sql.format(
        catalog="lakehouse",
        silver_table=names["silver"],
        gold_table=names["gold"],
        tm_run_id="run-test",
        **extra,
    )


class TestAdaptQuery:
    @pytest.mark.parametrize(
        ("catalog", "query", "scans"),
        [
            (
                None,
                "SELECT * FROM lakehouse.silver.entities JOIN lakehouse.gold.alerts ON 1=1",
                ["s3://sb/warehouse/silver.db/entities", "s3://gb/warehouse/gold.db/alerts"],
            ),
            (
                "polaris",
                "SELECT * FROM lakehouse.silver.counterparty_edges",
                ["s3://sb/silver/counterparty_edges"],
            ),
            (
                None,
                "SELECT * FROM lakehouse.gold.cases c JOIN lakehouse.gold.alert_dispositions d ON 1=1",
                ["s3://gb/warehouse/gold.db/cases", "s3://gb/warehouse/gold.db/alert_dispositions"],
            ),
        ],
    )
    def test_tables_resolve_to_their_layer_bucket(self, catalog, query, scans):
        """The executed per-query test ignores the bucket, so the bucket choice is checked here."""
        sql = (_executor(catalog) if catalog else _executor()).adapt_query(query)
        for path in scans:
            assert f"iceberg_scan('{path}'" in sql

    def test_local_executor_rewrites_cardinality(self):
        local = LocalDuckDBExecutor(
            endpoint="http://127.0.0.1:1",
            access_key="x",
            secret_key="y",
            warehouse_bucket="wh",
            table_names={"gold_alerts": "gold.alerts"},
        )
        sql = local.adapt_query("SELECT CARDINALITY (related_txn_ids) FROM lb.gold.alerts")
        assert "len(related_txn_ids)" in sql
        assert "iceberg_scan('s3://wh/warehouse/gold/alerts'" in sql


# ---------------------------------------------------------------------------
# Executed: the exact pod script, pytz blocked, in-memory tables
# ---------------------------------------------------------------------------

duckdb = pytest.importorskip("duckdb")


def _duckdb_ddl(table_key: str, name: str) -> str:
    """The real Spark DDL for ``table_key``, translated to a DuckDB table.

    TIMESTAMP becomes TIMESTAMPTZ because that is what Spark writes to Iceberg
    for a TIMESTAMP column. NOT NULL is dropped so fixtures can insert only the
    columns a query reads.
    """
    from lakebench.deploy.financial_ddl import FINANCIAL_TABLE_DDLS

    ddl = FINANCIAL_TABLE_DDLS[table_key].split("USING iceberg")[0]
    body = ddl[ddl.index("(") + 1 : ddl.rindex(")")]
    body = re.sub(r"--[^\n]*", "", body)
    body = re.sub(r"\bNOT NULL\b", "", body)
    body = re.sub(r"\bSTRING\b", "VARCHAR", body)
    body = re.sub(r"\bTIMESTAMP\b", "TIMESTAMPTZ", body)
    body = re.sub(r"ARRAY<(\w+)>", r"\1[]", body)
    body = re.sub(
        r"STRUCT<([^>]*)>",
        lambda m: "STRUCT(" + re.sub(r"(\w+)\s*:\s*", r"\1 ", m.group(1)) + ")",
        body,
    )
    return f"CREATE TABLE s.{name}({body});"


# Timestamp columns are TIMESTAMPTZ because that is what Spark writes to
# Iceberg for a TIMESTAMP column. The TM and entity tables use the real DDL.
_SETUP_TEMPLATE = """
CREATE SCHEMA s;
CREATE TABLE s.transactions(txn_id VARCHAR, originator_id BIGINT, beneficiary_id BIGINT,
  originator_bank_bic VARCHAR, beneficiary_bank_bic VARCHAR, txn_amount DECIMAL(18,2),
  txn_currency VARCHAR, txn_amount_usd DECIMAL(18,2), txn_timestamp TIMESTAMPTZ, cross_border BOOLEAN);
INSERT INTO s.transactions VALUES
  ('a',1,2,'X','Y',9500,'USD',9500,TIMESTAMPTZ '2026-01-01 00:00:00+00',true),
  ('b',1,2,'X','Y',9600,'USD',9600,TIMESTAMPTZ '2026-01-02 00:00:00+00',true),
  ('c',1,2,'X','Y',9700,'USD',9700,TIMESTAMPTZ '2026-01-03 00:00:00+00',false);
{entities}
INSERT INTO s.entities BY NAME
  SELECT * FROM (VALUES (1,'Person','n1',true,'person','GB','low'),
                        (2,'Company','n2',false,NULL,'DE',NULL))
  t(entity_id, entity_type, name, is_customer, customer_type, country, crr_tier);
{accounts}
INSERT INTO s.accounts BY NAME SELECT 10 AS account_id, 1 AS holder_entity_id, 250.00 AS current_balance;
{alert_dispositions}
INSERT INTO s.alert_dispositions BY NAME
  SELECT * FROM (VALUES ('d1',1,'W2','open',DATE '2026-02-20','run-test'),
                        ('d2',2,'W3','closed',DATE '2026-02-21','run-test'),
                        ('d3',1,'W2','open',DATE '2025-02-20','run-old'))
  t(alert_id, entity_id, rule_id, queue_status, generated_date, base_run_id);
{cases}
INSERT INTO s.cases BY NAME
  SELECT * FROM (VALUES
    ('c1',1,'alert_escalation','high','open',DATE '2026-03-01',DATE '2026-06-01',2,'run-test'),
    ('c0',2,'alert_escalation','critical','open',DATE '2025-01-01',DATE '2026-06-01',1,'run-old'))
  t(case_id, customer_id, case_type, priority, case_status, opened_date, as_of_date,
    alert_count, base_run_id);
CREATE TABLE s.counterparty_edges(source_entity_id BIGINT, target_entity_id BIGINT,
  txn_count BIGINT, cumulative_amount_usd DECIMAL(18,2));
INSERT INTO s.counterparty_edges VALUES (1,2,3,100.5),(2,3,1,50);
CREATE TABLE s.account_statements(account_id BIGINT, entry_seq BIGINT, book_ts TIMESTAMPTZ,
  cdt_dbt_ind VARCHAR, amt DECIMAL(18,2), bal_after DECIMAL(38,2), txn_id VARCHAR);
INSERT INTO s.account_statements VALUES
  (7,1,TIMESTAMPTZ '2026-01-01 00:00:00+00','CRDT',10,10,'t1'),
  (7,2,TIMESTAMPTZ '2026-01-02 00:00:00+00','DBIT',5,5,'t2');
CREATE TABLE s.alerts(alert_id VARCHAR, entity_id BIGINT, rule_id VARCHAR, priority VARCHAR,
  status VARCHAR, alert_score DOUBLE, alert_ts TIMESTAMPTZ, related_txn_ids VARCHAR[]);
INSERT INTO s.alerts VALUES
  ('al1',1,'W2','high','open',0.9,TIMESTAMPTZ '2026-01-05 00:00:00+00',['a','b']),
  ('al2',2,'W3','low','open',0.2,TIMESTAMPTZ '2026-01-04 00:00:00+00',NULL);
"""
_SETUP = _SETUP_TEMPLATE.format(
    entities=_duckdb_ddl("silver_entities", "entities"),
    accounts=_duckdb_ddl("silver_accounts", "accounts"),
    alert_dispositions=_duckdb_ddl("gold_alert_dispositions", "alert_dispositions"),
    cases=_duckdb_ddl("gold_cases", "cases"),
)

_EXPECTED_ROWS = {
    "FQ1_txn_full_scan": 1,
    "FQ2_top_corridors_window": 1,
    "FQ6_structuring_scan": 1,
    "FQ3_entity_edge_risk": 2,
    "FQ7_cross_border_concentration": 1,
    "FQ4_running_balance_window": 2,
    "FQ5_alert_triage": 2,
    "FQ8_alert_to_entity_join": 2,
    # Subject is case c1 (customer 1, run-test); the run-old rows must not leak in.
    "IQ1_customer_360": 1,
    "IQ2_case_activity_12m": 1,  # January 2026, inside the year before 2026-03-01
    "IQ3_counterparty_two_hop": 1,  # 1 -> 2 -> 3
    "IQ4_open_cases_over_60_days": 1,  # c1 only; c0 belongs to run-old
}


def _run_pod_script(
    executor: DuckDBExecutor,
    sql: str,
    *,
    tz: str = "UTC",
    setup: str | None = None,
    pre: str = "",
) -> subprocess.CompletedProcess:
    """Run the script the pod would run, with S3 setup swapped for local tables.

    *tz* is the process time zone, *setup* replaces the default tables and *pre*
    is Python run before the script's own session settings.
    """
    script = executor._build_python_script(sql)
    # iceberg_scan(\'s3://bucket/warehouse/ns.db/tbl\', ...) -> s.tbl
    script = re.sub(
        r"iceberg_scan\(\\'s3://[^/]+/warehouse/\w+\.db/(\w+)\\', allow_moved_paths := true\)",
        r"s.\1",
        script,
    )
    marker = "conn.execute('SET unsafe_enable_version_guessing = true'); "
    assert marker in script
    body = script.split(marker, 1)[1]
    setup = " ".join((_SETUP if setup is None else setup).split()).replace("'", "\\'")
    # Block pytz the way a python:3.11-slim pod with only duckdb installed does.
    prelude = (
        "import sys; sys.modules['pytz'] = None; "
        f"import duckdb, json; conn = duckdb.connect(); conn.execute('{setup}'); {pre}"
    )
    env = {**os.environ, "TZ": tz}
    return subprocess.run(
        [sys.executable, "-c", prelude + body],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )


@pytest.mark.parametrize("query", _FINANCIAL_QUERIES, ids=lambda q: q.name)
def test_every_financial_query_runs_on_duckdb(query):
    executor = _executor()
    proc = _run_pod_script(executor, executor.adapt_query(_render(query)))
    assert proc.returncode == 0, summarise_engine_error(proc.stderr)
    assert json.loads(proc.stdout)["rows"] == _EXPECTED_ROWS[query.name]


def test_cast_projection_keeps_order_and_duplicate_names():
    executor = _executor()
    sql = (
        "SELECT x AS a, x AS a, TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_days(x::INT) AS ts "
        "FROM range(5) t(x) ORDER BY x DESC"
    )
    proc = _run_pod_script(executor, sql)
    assert proc.returncode == 0, summarise_engine_error(proc.stderr)
    data = json.loads(proc.stdout)["data"]
    assert [row.split(",")[0] for row in data] == ["(4", "(3", "(2", "(1", "(0"]
    assert "2026-01-05 00:00:00+00" in data[0]


def test_script_pins_utc_whatever_the_process_zone():
    """A pod in another zone must still render and bucket in UTC: only the
    script's own SET TimeZone can produce these values under Pacific/Auckland."""
    sql = (
        "SELECT TIMESTAMPTZ '2026-01-05 00:00:00+00' AS ts, "
        "date_trunc('month', TIMESTAMPTZ '2026-01-31 23:00:00+00') AS month"
    )
    proc = _run_pod_script(_executor(), sql, tz="Pacific/Auckland")
    assert proc.returncode == 0, summarise_engine_error(proc.stderr)
    row = json.loads(proc.stdout)["data"][0]
    assert "2026-01-05 00:00:00+00" in row
    assert "2026-01-01 00:00:00+00" in row


def test_script_output_is_one_json_payload_when_a_query_is_slow():
    """DuckDB prints a progress bar to stdout once a query passes its threshold
    (2 s by default, 50 ms here); the script must turn it off."""
    sql = "SELECT sum(x * x) AS s FROM range(40000000) t(x)"
    proc = _run_pod_script(_executor(), sql, pre="conn.execute('SET progress_bar_time = 50'); ")
    assert proc.returncode == 0, summarise_engine_error(proc.stderr)
    assert json.loads(proc.stdout)["rows"] == 1


def _fq8_setup(alert_ids: list[str]) -> str:
    inserts = ", ".join(
        f"('{a}', 1, 'W2', 'high', 'open', 0.9, TIMESTAMPTZ '2026-01-05 00:00:00+00', NULL)"
        for a in alert_ids
    )
    return f"""
CREATE SCHEMA s;
{_duckdb_ddl("silver_entities", "entities")}
INSERT INTO s.entities BY NAME SELECT 1 AS entity_id, 'Person' AS entity_type, 'n1' AS name;
CREATE TABLE s.alerts(alert_id VARCHAR, entity_id BIGINT, rule_id VARCHAR, priority VARCHAR,
  status VARCHAR, alert_score DOUBLE, alert_ts TIMESTAMPTZ, related_txn_ids VARCHAR[]);
INSERT INTO s.alerts VALUES {inserts};
"""


def test_fq8_pick_among_tied_alerts_does_not_depend_on_row_order():
    """FQ8 takes 100 of 102 alerts that share alert_ts, entity and rule; the
    pick must not follow insertion order or two runs would disagree."""
    fq8 = next(q for q in _FINANCIAL_QUERIES if q.name.startswith("FQ8"))
    executor = _executor()
    sql = executor.adapt_query(_render(fq8))
    ids = [f"a{i:03d}" for i in range(102)]
    results = []
    for order in (ids, ids[::-1]):
        proc = _run_pod_script(executor, sql, setup=_fq8_setup(order))
        assert proc.returncode == 0, summarise_engine_error(proc.stderr)
        payload = json.loads(proc.stdout)
        assert payload["rows"] == 100
        results.append(payload["data"])
    assert results[0] == results[1]
    assert "a099" in results[0][-1] and "a100" not in "".join(results[0])
