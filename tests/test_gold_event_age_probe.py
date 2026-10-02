"""The in-stream gold event-age probe speaks each engine's dialect.

lb16-cs (2026-09-27, run 20260927-073533-9de9c9): the probe sent Trino's
``date_diff('second', ...)`` to Spark Thrift on every round and every round
warned ``INVALID_PARAMETER_VALUE.DATETIME_UNIT``; adapt_query only rewrites
the 'day' form. Spark SQL has no string-unit date_diff (verified on pyspark
4.0.1: the old form raises that error, the unix_timestamp form returns the
seconds).
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lakebench.benchmark.result import QueryExecutorResult
from lakebench.cli._sustained import (
    _run_benchmark_round,
    gold_event_age_sql,
    gold_event_age_target,
)
from lakebench.modules.query_engines.spark_thrift.executor import SparkThriftExecutor


def test_target_is_alert_ts_on_alerts_for_financial():
    # Financial reads gold.alerts.alert_ts, not daily_dashboards, so an
    # empty-alert window records nothing instead of a baseline age (LB-185 sibling).
    assert gold_event_age_target("financial", "gold.dash", "gold.alerts") == (
        "gold.alerts",
        "alert_ts",
    )


def test_target_is_interaction_date_for_c360():
    assert gold_event_age_target("customer360", "gold.dash", "gold.alerts") == (
        "gold.dash",
        "interaction_date",
    )


def test_alert_ts_column_flows_into_the_sql():
    sql = gold_event_age_sql("trino", "lakehouse.gold.alerts", "alert_ts")
    assert "date_diff('second', CAST(MAX(alert_ts) AS TIMESTAMP), current_timestamp)" in sql
    assert sql.endswith("FROM lakehouse.gold.alerts")


def test_spark_thrift_gets_spark_sql_after_adapt_query():
    sql = SparkThriftExecutor("ns", "spark_catalog").adapt_query(
        gold_event_age_sql("spark-thrift", "spark_catalog.gold.t")
    )
    assert "date_diff" not in sql.lower()
    assert "unix_timestamp(current_timestamp())" in sql
    assert sql.endswith("FROM spark_catalog.gold.t")


@pytest.mark.parametrize("engine", ["trino", "duckdb"])
def test_trino_and_duckdb_keep_date_diff_seconds(engine):
    sql = gold_event_age_sql(engine, "lakehouse.gold.t")
    assert "date_diff('second', CAST(MAX(interaction_date) AS TIMESTAMP), current_timestamp)" in sql


def test_duckdb_form_executes():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT DATE '2025-01-01' AS interaction_date")
    (age,) = con.execute(gold_event_age_sql("duckdb", "t")).fetchone()
    assert age > 0


def test_round_sends_the_spark_form_to_thrift():
    executor = SparkThriftExecutor("ns", "spark_catalog")
    sent: list[str] = []

    def execute(sql, timeout=300):
        sent.append(sql)
        return QueryExecutorResult(
            sql=sql,
            engine="spark-thrift",
            duration_seconds=0.1,
            rows_returned=1,
            raw_output="age\n54805675",
        )

    executor.execute_query = execute  # type: ignore[method-assign]
    runner = MagicMock(executor=executor, catalog="spark_catalog", gold_table="gold.dash")
    runner.run_power.return_value = SimpleNamespace(
        queries=[],
        mode="power",
        cache="hot",
        scale=1,
        qph=100.0,
        total_seconds=1.0,
        iterations=1,
        engine="spark-thrift",
    )
    collector = MagicMock()
    _run_benchmark_round(
        cfg=MagicMock(),
        bench_runner=runner,
        collector=collector,
        console=MagicMock(),
        round_index=1,
        j=MagicMock(),
        k8s=None,
    )
    assert len(sent) == 1 and "date_diff" not in sent[0].lower()
    call = collector.record_round.call_args
    recorded = call.args[0]
    # The round is recorded through record_round with its start and end.
    assert isinstance(call.kwargs["started_at"], datetime)
    assert isinstance(call.kwargs["ended_at"], datetime)
    assert recorded.round_meta.gold_event_age_seconds == 54805675


def test_financial_round_probes_alert_ts_on_gold_alerts():
    sent: list[str] = []

    class _FakeExec:
        def engine_name(self):
            return "trino"

        def flush_cache(self):
            pass

        def adapt_query(self, sql):
            return sql

        def execute_query(self, sql, timeout=300):
            sent.append(sql)
            return QueryExecutorResult(
                sql=sql,
                engine="trino",
                duration_seconds=0.1,
                rows_returned=1,
                raw_output="age\n42",
            )

    runner = MagicMock(executor=_FakeExec(), catalog="lakehouse", gold_table="gold.dash")
    runner._extra_tables = {"gold_alerts": "gold.alerts"}
    runner.config.architecture.workload.schema_type.value = "financial"
    runner.run_power.return_value = SimpleNamespace(
        queries=[],
        mode="power",
        cache="hot",
        scale=1,
        qph=100.0,
        total_seconds=1.0,
        iterations=1,
        engine="trino",
    )
    _run_benchmark_round(
        cfg=MagicMock(),
        bench_runner=runner,
        collector=MagicMock(),
        console=MagicMock(),
        round_index=1,
        j=MagicMock(),
        k8s=None,
    )
    probe = sent[0]
    assert "MAX(alert_ts)" in probe
    assert probe.endswith("FROM lakehouse.gold.alerts")
