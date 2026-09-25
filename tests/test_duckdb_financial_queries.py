"""DuckDB runs every financial benchmark query, and failures say why.

A live AML run on hive-iceberg-spark-duckdb failed FQ3, FQ4, FQ5, FQ6 and
FQ8, and the runner showed only ``Traceback (most recent call last): File
"<string>", line 1`` for each. Three separate causes:

- the auxiliary tables (silver_entities, gold_alerts, ...) were never
  rewritten to ``iceberg_scan``, so DuckDB saw ``lakehouse.silver.entities``
  and failed with ``Catalog "lakehouse" does not exist`` (FQ3, FQ4, FQ5, FQ8);
- Spark writes TIMESTAMP as Iceberg timestamptz, and handing a TIMESTAMP WITH
  TIME ZONE to Python needs pytz, which the pod lacks (FQ4, FQ6, FQ8);
- DuckDB's ``cardinality`` only takes a MAP (FQ8).

The executed tests below build the exact script the pod runs and execute it
with pytz blocked, against in-memory tables of the same shape.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from lakebench.benchmark.queries import _FINANCIAL_QUERIES
from lakebench.benchmark.result import summarise_engine_error
from lakebench.config.schema import TableNamesConfig
from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor
from lakebench.modules.query_engines.duckdb.local_executor import LocalDuckDBExecutor

# ---------------------------------------------------------------------------
# Error surfacing
# ---------------------------------------------------------------------------

_BINDER_TRACEBACK = """\
Traceback (most recent call last):
  File "<string>", line 1, in <module>
_duckdb.BinderException: Binder Error: Catalog "lakehouse" does not exist!
"""

_PYTZ_TRACEBACK = """\
Traceback (most recent call last):
  File "<string>", line 1, in <module>
_duckdb.InvalidInputException: Invalid Input Error: Required module 'pytz' failed \
to import, due to the following Python exception:
ModuleNotFoundError: No module named 'pytz'
"""

_PARSER_TRACEBACK = """\
Traceback (most recent call last):
  File "<string>", line 1, in <module>
_duckdb.ParserException: Parser Error: syntax error at or near "FROMM"

LINE 1: SELECT 1 FROMM t
                 ^
"""

_CHAINED_TRACEBACK = """\
Traceback (most recent call last):
  File "<string>", line 1, in <module>
KeyError: 'AWS_ACCESS_KEY_ID'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "<string>", line 1, in <module>
    conn.execute(sql)
    ~~~~~~~~~~~~^^^^^
RuntimeError: the one that was raised
"""


class TestSummariseEngineError:
    def test_reports_the_exception_not_the_traceback_header(self):
        out = summarise_engine_error(_BINDER_TRACEBACK)
        assert out == 'BinderException: Binder Error: Catalog "lakehouse" does not exist!'

    def test_keeps_the_continuation_that_names_the_missing_module(self):
        out = summarise_engine_error(_PYTZ_TRACEBACK)
        assert out.startswith("InvalidInputException")
        assert "No module named 'pytz'" in out

    def test_drops_the_caret_line(self):
        out = summarise_engine_error(_PARSER_TRACEBACK)
        assert out.startswith("ParserException: Parser Error: syntax error")
        assert "^" not in out

    def test_chained_traceback_reports_the_final_exception(self):
        out = summarise_engine_error(_CHAINED_TRACEBACK)
        assert out == "RuntimeError: the one that was raised"

    def test_non_traceback_picks_the_error_line(self):
        text = "WARNING: jline terminal fallback\nQuery 2026_0001 failed: line 1:8: Column 'x' cannot be resolved\n"
        assert summarise_engine_error(text).startswith("Query 2026_0001 failed:")

    def test_prefers_the_stated_error_over_warn_noise(self):
        text = "WARN util.NativeCodeLoader: Exception: none\nError: real problem\n"
        assert summarise_engine_error(text) == "Error: real problem"

    def test_last_marked_line_when_none_leads(self):
        text = "WARN a: Exception: noise\njava.lang.IllegalStateException: real\n"
        assert "java.lang.IllegalStateException: real" in summarise_engine_error(text)

    def test_hive_failed_line_and_caused_by_are_kept(self):
        text = (
            "WARN HiveConf: Exception: retrying metastore connect\n"
            "FAILED: SemanticException [Error 10001]: Table not found 'x'\n"
            "Caused by: NoSuchObjectException: x\n"
        )
        out = summarise_engine_error(text)
        assert out.startswith("FAILED: SemanticException")
        assert "Caused by: NoSuchObjectException" in out

    def test_shutdown_noise_traceback_is_ignored(self):
        text = (
            _BINDER_TRACEBACK
            + "Exception ignored in: <function X.__del__>\n"
            + "Traceback (most recent call last):\n"
            + '  File "<string>", line 1, in __del__\n'
            + "RuntimeError: cleanup failed\n"
        )
        assert "does not exist" in summarise_engine_error(text)

    def test_drops_kubectl_exit_line(self):
        out = summarise_engine_error(_BINDER_TRACEBACK + "command terminated with exit code 1\n")
        assert out.endswith("does not exist!")

    def test_empty_and_limit(self):
        assert summarise_engine_error("") == "Unknown error"
        assert len(summarise_engine_error("E" * 1000)) == 300
        assert len(summarise_engine_error(_PYTZ_TRACEBACK, limit=40)) == 40


@pytest.mark.parametrize(
    "executor_path,make",
    [
        (
            "duckdb",
            lambda: DuckDBExecutor(namespace="t", catalog_name="lakehouse"),
        ),
        (
            "trino",
            lambda: __import__(
                "lakebench.modules.query_engines.trino.executor", fromlist=["TrinoExecutor"]
            ).TrinoExecutor(namespace="t", catalog_name="lakehouse"),
        ),
        (
            "spark-thrift",
            lambda: __import__(
                "lakebench.modules.query_engines.spark_thrift.executor",
                fromlist=["SparkThriftExecutor"],
            ).SparkThriftExecutor(namespace="t", catalog_name="lakehouse"),
        ),
    ],
)
def test_every_executor_surfaces_the_final_exception(executor_path, make):
    executor = make()
    executor._pod = "pod-0"
    with patch("subprocess.run") as run:
        run.return_value = MagicMock(returncode=1, stderr=_BINDER_TRACEBACK, stdout="")
        result = executor.execute_query("SELECT 1")
    assert not result.success
    assert 'Catalog "lakehouse" does not exist' in result.error
    assert "Traceback" not in result.error


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
        s3_endpoint="http://10.0.0.1:80",
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
    def test_auxiliary_tables_resolve_to_their_layer_bucket(self):
        sql = _executor().adapt_query(
            "SELECT * FROM lakehouse.silver.entities JOIN lakehouse.gold.alerts ON 1=1"
        )
        assert "iceberg_scan('s3://sb/warehouse/silver.db/entities'" in sql
        assert "iceberg_scan('s3://gb/warehouse/gold.db/alerts'" in sql

    def test_polaris_layout_for_auxiliary_tables(self):
        sql = _executor("polaris").adapt_query("SELECT * FROM lakehouse.silver.counterparty_edges")
        assert "iceberg_scan('s3://sb/silver/counterparty_edges'" in sql

    @pytest.mark.parametrize("query", _FINANCIAL_QUERIES, ids=lambda q: q.name)
    def test_no_catalog_qualified_name_survives(self, query):
        assert "lakehouse." not in _executor().adapt_query(_render(query))

    def test_cardinality_becomes_len(self):
        sql = _executor().adapt_query("SELECT cardinality(a.related_txn_ids) FROM t a")
        assert "len(a.related_txn_ids)" in sql
        assert "cardinality" not in sql

    def test_cardinality_inside_a_string_literal_is_kept(self):
        sql = _executor().adapt_query("SELECT 'cardinality(x)' AS s, cardinality(y) FROM t")
        assert "'cardinality(x)'" in sql
        assert "len(y)" in sql

    def test_cardinality_in_identifiers_and_comments_is_kept(self):
        sql = DuckDBExecutor._rewrite_cardinality(
            "/* user's note */ SELECT cardinality(a) AS \"cardinality(a)\", 'it''s', "
            "cardinality(b) -- don't\nFROM t"
        )
        assert sql == (
            "/* user's note */ SELECT len(a) AS \"cardinality(a)\", 'it''s', "
            "len(b) -- don't\nFROM t"
        )

    def test_tm_tables_resolve_to_the_gold_bucket(self):
        sql = _executor().adapt_query(
            "SELECT * FROM lakehouse.gold.cases c JOIN lakehouse.gold.alert_dispositions d ON 1=1"
        )
        assert "iceberg_scan('s3://gb/warehouse/gold.db/cases'" in sql
        assert "iceberg_scan('s3://gb/warehouse/gold.db/alert_dispositions'" in sql

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
CREATE TABLE s.account_statements(account_id BIGINT, book_ts TIMESTAMPTZ, cdt_dbt_ind VARCHAR,
  amt DECIMAL(18,2), bal_after DECIMAL(18,2));
INSERT INTO s.account_statements VALUES
  (7,TIMESTAMPTZ '2026-01-01 00:00:00+00','CRDT',10,10),
  (7,TIMESTAMPTZ '2026-01-02 00:00:00+00','DBIT',5,5);
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


def _run_pod_script(executor: DuckDBExecutor, sql: str) -> subprocess.CompletedProcess:
    """Run the script the pod would run, with S3 setup swapped for local tables."""
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
    setup = " ".join(_SETUP.split()).replace("'", "\\'")
    # Block pytz the way a python:3.11-slim pod with only duckdb installed does.
    prelude = (
        "import sys; sys.modules['pytz'] = None; "
        f"import duckdb, json; conn = duckdb.connect(); conn.execute('{setup}'); "
    )
    # The pod image runs in UTC. Pin it so date_trunc over a TIMESTAMPTZ
    # buckets months the same way on a developer machine in another zone.
    env = {**os.environ, "TZ": "UTC"}
    return subprocess.run(
        [sys.executable, "-c", prelude + body],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )


@pytest.mark.parametrize("query", _FINANCIAL_QUERIES, ids=lambda q: q.name)
def test_every_financial_query_runs_on_duckdb(query):
    import json

    executor = _executor()
    proc = _run_pod_script(executor, executor.adapt_query(_render(query)))
    assert proc.returncode == 0, summarise_engine_error(proc.stderr)
    assert json.loads(proc.stdout)["rows"] == _EXPECTED_ROWS[query.name]


def test_timestamptz_would_fail_without_the_cast():
    """Guards the premise: a bare fetchall of TIMESTAMPTZ needs pytz."""
    code = (
        "import sys; sys.modules['pytz'] = None; import duckdb; "
        "duckdb.connect().execute(\"SELECT TIMESTAMPTZ '2026-01-01 00:00:00+00'\").fetchall()"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode != 0
    assert "pytz" in summarise_engine_error(proc.stderr)


def test_cast_projection_keeps_order_and_duplicate_names():
    executor = _executor()
    sql = (
        "SELECT x AS a, x AS a, TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_days(x::INT) AS ts "
        "FROM range(5) t(x) ORDER BY x DESC"
    )
    proc = _run_pod_script(executor, sql)
    assert proc.returncode == 0, summarise_engine_error(proc.stderr)
    import json

    data = json.loads(proc.stdout)["data"]
    assert [row.split(",")[0] for row in data] == ["(4", "(3", "(2", "(1", "(0"]
    # Rendered in the session time zone, so match the date loosely.
    assert "'2026-01-0" in data[0]
