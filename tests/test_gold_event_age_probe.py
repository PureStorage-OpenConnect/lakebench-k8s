"""The in-stream gold event-age probe speaks each engine's dialect.

lb16-cs (2026-09-27, run 20260927-073533-9de9c9): the probe sent Trino's
``date_diff('second', ...)`` to Spark Thrift on every round and every round
warned ``INVALID_PARAMETER_VALUE.DATETIME_UNIT``; adapt_query only rewrites
the 'day' form. Spark SQL has no string-unit date_diff (verified on pyspark
4.0.1: the old form raises that error, the unix_timestamp form returns the
seconds).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lakebench.benchmark.result import QueryExecutorResult
from lakebench.cli._sustained import _run_benchmark_round, gold_event_age_sql
from lakebench.modules.query_engines.spark_thrift.executor import SparkThriftExecutor


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
    recorded = collector.record_benchmark_round.call_args.args[0]
    assert recorded.round_meta.gold_event_age_seconds == 54805675
