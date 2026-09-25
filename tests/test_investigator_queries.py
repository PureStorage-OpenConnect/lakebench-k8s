"""P10 stage 9 investigator queries parse and run in the DuckDB dialect.

The Spark dialect is executed in tests/spark/test_tm_operations_spark.py. Here
the DuckDB adapter's rewrites (DATE_DIFF, date_add) are applied and each query
runs against small in-memory tables laid out as catalog.namespace.table, so a
Trino-only construct fails the test instead of the benchmark.
"""

from __future__ import annotations

import pytest

from lakebench.benchmark.queries import INVESTIGATOR_QUERIES, get_benchmark_queries
from lakebench.config.schema import WorkloadSchema

duckdb = pytest.importorskip("duckdb")

TABLES = {
    "silver_table": "silver.transactions",
    "gold_table": "gold.daily_dashboards",
    "silver_entities": "silver.entities",
    "silver_accounts": "silver.accounts",
    "silver_counterparty_edges": "silver.counterparty_edges",
    "gold_alert_dispositions": "gold.alert_dispositions",
    "gold_cases": "gold.cases",
}


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS lakehouse")
    for ns in ("silver", "gold"):
        c.execute(f"CREATE SCHEMA lakehouse.{ns}")
    c.execute(
        "CREATE TABLE lakehouse.silver.entities AS SELECT * FROM (VALUES "
        "(1::BIGINT, 'a', 'person', 'US', DATE '2020-01-01', true, 'high', 70, 'x', false, 1.0),"
        "(2::BIGINT, 'b', NULL, 'GB', NULL, false, NULL, NULL, NULL, false, NULL),"
        "(3::BIGINT, 'c', NULL, 'FR', NULL, false, NULL, NULL, NULL, false, NULL)"
        ") t(entity_id, name, customer_type, country, customer_since, is_customer, crr_tier, "
        "crr_score, crr_factors, pep_status, expected_monthly_volume_usd)"
    )
    c.execute(
        "CREATE TABLE lakehouse.silver.accounts AS SELECT 10::BIGINT AS account_id, "
        "1::BIGINT AS holder_entity_id, 5.0 AS current_balance"
    )
    c.execute(
        "CREATE TABLE lakehouse.silver.counterparty_edges AS SELECT * FROM (VALUES "
        "(1::BIGINT, 2::BIGINT, 100.0, 3::BIGINT), (2::BIGINT, 3::BIGINT, 50.0, 1::BIGINT)"
        ") t(source_entity_id, target_entity_id, cumulative_amount_usd, txn_count)"
    )
    c.execute(
        "CREATE TABLE lakehouse.silver.transactions AS SELECT * FROM (VALUES "
        "(1::BIGINT, 2::BIGINT, 10.0, TIMESTAMP '2024-01-05 10:00:00', false),"
        "(2::BIGINT, 1::BIGINT, 20.0, TIMESTAMP '2024-02-05 10:00:00', true)"
        ") t(originator_id, beneficiary_id, txn_amount_usd, txn_timestamp, cross_border)"
    )
    c.execute(
        "CREATE TABLE lakehouse.gold.alert_dispositions AS SELECT 1::BIGINT AS entity_id, "
        "'W2_structuring' AS rule_id, 'open' AS queue_status, DATE '2024-02-06' AS generated_date"
    )
    c.execute(
        "CREATE TABLE lakehouse.gold.cases AS SELECT * FROM (VALUES "
        "('k1', 1::BIGINT, 'alert_escalation', 'open', 'critical', 'high', DATE '2024-03-01', "
        "DATE '2024-06-01', NULL::VARCHAR, 2)"
        ") t(case_id, customer_id, case_type, case_status, priority, crr_tier, opened_date, "
        "as_of_date, sar_decision, alert_count)"
    )
    yield c
    c.close()


def test_investigator_queries_are_in_the_financial_set():
    names = {q.name for q in get_benchmark_queries(WorkloadSchema.FINANCIAL)}
    assert {q.name for q in INVESTIGATOR_QUERIES} <= names
    assert {q.query_class for q in INVESTIGATOR_QUERIES} == {"investigator"}


@pytest.mark.parametrize("q", INVESTIGATOR_QUERIES, ids=lambda q: q.name)
def test_query_runs_in_duckdb_dialect(con, q):
    from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor

    sql = q.sql.format(catalog="lakehouse", **TABLES)
    # Empty table map: keep catalog.namespace.table (the in-memory layout)
    # and exercise only the dialect rewrites.
    sql = DuckDBExecutor(namespace="x", catalog_name="lakehouse", table_names={}).adapt_query(sql)
    rows = con.execute(sql).fetchall()
    if q.name == "IQ1_customer_360":
        assert len(rows) == 1 and rows[0][0] == 1
    if q.name == "IQ2_case_activity_12m":
        assert sum(r[1] for r in rows) == 2
    if q.name == "IQ3_counterparty_two_hop":
        assert [r[1] for r in rows] == [3]
    if q.name == "IQ4_open_cases_over_60_days":
        assert [r[0] for r in rows] == ["k1"] and rows[0][7] == 92


def test_duckdb_adapter_rewrites_financial_extra_tables():
    """Keys like gold_cases carry their bucket in the layer prefix."""
    from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor

    ex = DuckDBExecutor(
        namespace="x",
        catalog_name="lakehouse",
        s3_buckets={"silver": "sb", "gold": "gb"},
        table_names={"gold_cases": "gold.cases", "silver_entities": "silver.entities"},
    )
    out = ex.adapt_query("SELECT * FROM lakehouse.gold.cases JOIN lakehouse.silver.entities")
    assert "s3://gb/warehouse/gold.db/cases" in out
    assert "s3://sb/warehouse/silver.db/entities" in out


def test_duckdb_hive_path_matches_where_the_gold_job_creates_namespaces():
    """On Hive, tm_operations creates the gold namespace with no LOCATION, so
    Iceberg's HiveCatalog puts it at <catalog warehouse>/<ns>.db and each table
    under it. The gold jobs' catalog warehouse is the gold bucket and the
    silver jobs' the silver bucket, which is the path the DuckDB adapter
    rewrites to."""
    from unittest.mock import MagicMock

    from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor
    from lakebench.spark.job import JobType, SparkJobManager
    from tests.conftest import make_config

    cfg = make_config(architecture={"workload": {"schema": "financial", "datagen": {"scale": 1}}})
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    mgr = SparkJobManager(cfg, k8s)
    b = cfg.platform.storage.s3.buckets

    def warehouse(job):
        conf = mgr._build_manifest(job)["spec"]["sparkConf"]
        return next(v for k, v in conf.items() if k.endswith(".warehouse") and "catalog" in k)

    assert warehouse(JobType.GOLD_FINALIZE) == f"s3a://{b.gold}/warehouse/"
    assert warehouse(JobType.GOLD_REFRESH) == f"s3a://{b.gold}/warehouse/"
    assert warehouse(JobType.SILVER_BUILD) == f"s3a://{b.silver}/warehouse/"
    ex = DuckDBExecutor(
        namespace="x",
        catalog_name="lakehouse",
        s3_buckets={"silver": b.silver, "gold": b.gold},
        table_names={
            "gold_cases": "gold.cases",
            "gold_alert_dispositions": "gold.alert_dispositions",
        },
        catalog_type="hive",
    )
    out = ex.adapt_query("SELECT * FROM lakehouse.gold.cases, lakehouse.gold.alert_dispositions")
    wh = warehouse(JobType.GOLD_FINALIZE).replace("s3a://", "s3://")
    assert f"{wh}gold.db/cases" in out and f"{wh}gold.db/alert_dispositions" in out


def test_tm_tables_are_in_iceberg_maintenance():
    """Continuous appends a snapshot per operations pass to each TM table;
    the continuous maintenance round expires snapshots on all four."""
    from unittest.mock import MagicMock, patch

    from lakebench.cli._sustained import _run_iceberg_maintenance
    from tests.conftest import make_config

    cfg = make_config(architecture={"workload": {"schema": "financial", "datagen": {"scale": 1}}})
    sent = []
    with (
        patch(
            "lakebench.deploy.iceberg.find_maintenance_engine",
            return_value=("trino", "pod", "lakehouse"),
        ),
        patch(
            "lakebench.deploy.iceberg.exec_sql",
            side_effect=lambda e, k, p, n, sql: sent.append(sql),
        ),
    ):
        _run_iceberg_maintenance(cfg, MagicMock(), MagicMock(), MagicMock(), "30m")
    for t in ("tm_reconciliation", "scenario_coverage", "alert_dispositions", "cases"):
        assert any(f"lakehouse.gold.{t} " in q and "expire_snapshots" in q for q in sent), t
