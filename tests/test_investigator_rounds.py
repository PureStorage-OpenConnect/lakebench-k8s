"""AML continuous in-stream rounds run IQ1 to IQ4 once the run has a case.

Each round first probes the cases table for the run's ``base_run_id``. With a
case it runs the 12-query set and records ``investigator_queries =
"included"``; without one (before the first TM pass) the 8-query set,
labelled ``absent_no_cases``; a probe that errors runs the 8 too, labelled
``probe_failed``. The round record carries the executed set's id.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lakebench.benchmark import BenchmarkRunner
from lakebench.benchmark.queries import query_set_id
from lakebench.benchmark.result import QueryExecutorResult
from lakebench.cli._sustained import _investigator_state, _run_benchmark_round
from lakebench.metrics import MetricsCollector
from lakebench.metrics.collector import round_label
from tests.conftest import make_config

RUN = "20261002-120000-abc123"


def _financial(tm=True):
    return make_config(
        workload={"schema": "financial", "tm_operations": {"enabled": tm}},
    )


class _Exec:
    """Answers every query; the cases probe answers ``case_rows`` rows, or
    fails when ``probe_error`` is set."""

    catalog_name = "lakehouse"

    def __init__(self, case_rows=1, probe_error=None):
        self.case_rows = case_rows
        self.probe_error = probe_error
        self.sql: list[str] = []

    def engine_name(self):
        return "trino"

    def flush_cache(self):
        pass

    def adapt_query(self, sql):
        return sql

    def execute_query(self, sql, timeout=300):
        self.sql.append(sql)
        probe = sql.startswith("SELECT 1 FROM") and "base_run_id" in sql
        error = self.probe_error if probe else None
        rows = self.case_rows if probe else 3
        return QueryExecutorResult(
            sql=sql,
            engine="trino",
            duration_seconds=0.1,
            rows_returned=0 if error else rows,
            raw_output="1\n" * rows,
            error=error,
        )


def _round(executor, cfg=None, tm_run_id=RUN):
    cfg = cfg or _financial()
    with patch("lakebench.benchmark.executor.get_executor", return_value=executor):
        runner = BenchmarkRunner(cfg, tm_run_id=tm_run_id)
    collector = MetricsCollector()
    collector.start_run(RUN, cfg.name, {})
    _run_benchmark_round(
        cfg=cfg,
        bench_runner=runner,
        collector=collector,
        console=MagicMock(),
        round_index=1,
        j=MagicMock(),
        k8s=None,
    )
    (bench,) = collector.current_run.benchmark_rounds
    return runner, bench


def test_a_round_with_a_case_runs_the_twelve_query_set():
    executor = _Exec(case_rows=1)
    runner, bench = _round(executor)
    rec = bench.round_record
    assert len(rec["executed_queries"]) == 12
    assert rec["executed_query_set_id"] == query_set_id(rec["executed_queries"])
    assert rec["investigator_queries"] == "included"
    assert sum(1 for q in rec["executed_queries"] if q.startswith("IQ")) == 4
    assert runner.tm_run_id == RUN  # restored after the round
    probe = [s for s in executor.sql if "base_run_id" in s and s.startswith("SELECT 1")]
    assert probe and f"base_run_id = '{RUN}'" in probe[0]


def test_a_round_before_any_case_runs_the_eight_and_is_labelled():
    runner, bench = _round(_Exec(case_rows=0))
    rec = bench.round_record
    assert len(rec["executed_queries"]) == 8
    assert not any(q.startswith("IQ") for q in rec["executed_queries"])
    assert rec["investigator_queries"] == "absent_no_cases"
    assert round_label(bench) == "8-query set (before cases exist)"
    assert runner.tm_run_id == RUN  # the next round probes again


def test_a_failed_probe_runs_the_eight_and_says_so():
    _, bench = _round(_Exec(probe_error="Table 'gold.cases' does not exist"))
    assert len(bench.round_record["executed_queries"]) == 8
    assert bench.round_record["investigator_queries"] == "probe_failed"


def test_the_two_sets_have_different_ids():
    _, twelve = _round(_Exec(case_rows=1))
    _, eight = _round(_Exec(case_rows=0))
    assert (
        twelve.round_record["executed_query_set_id"] != eight.round_record["executed_query_set_id"]
    )


def test_without_tm_operations_no_probe_and_no_label():
    executor = _Exec(case_rows=1)
    _, bench = _round(executor, cfg=_financial(tm=False), tm_run_id=None)
    assert len(bench.round_record["executed_queries"]) == 8
    assert bench.round_record["investigator_queries"] is None
    assert not any("base_run_id" in s for s in executor.sql)


def test_c360_rounds_are_unchanged():
    executor = _Exec(case_rows=1)
    _, bench = _round(executor, cfg=make_config(), tm_run_id=None)
    assert bench.round_record["investigator_queries"] is None
    assert not any("base_run_id" in s for s in executor.sql)


def test_probe_never_raises():
    runner = MagicMock()
    runner._extra_tables = {"gold_cases": "gold.cases"}
    runner.catalog = "lakehouse"
    runner.executor.adapt_query.side_effect = RuntimeError("no pod")
    assert _investigator_state(runner, RUN) == "probe_failed"


def test_continuous_runner_gets_the_run_id_for_aml_with_tm_operations():
    """The continuous command builds the runner with the run id (static:
    the call is in a long command body)."""
    import ast
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "src/lakebench/cli/_sustained.py").read_text()
    calls = [
        n
        for n in ast.walk(ast.parse(src))
        if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "BenchmarkRunner"
    ]
    assert calls and all(any(k.arg == "tm_run_id" for k in c.keywords) for c in calls)


def test_workload_version_is_aml_2():
    from lakebench.metrics.experiment import WORKLOAD_VERSIONS

    assert WORKLOAD_VERSIONS["financial"] == "aml-2"


@pytest.mark.parametrize("state", ["included", "absent_no_cases", "probe_failed"])
def test_every_state_is_one_the_collector_accepts(state):
    from lakebench.metrics.collector import INVESTIGATOR_QUERY_STATES

    assert state in INVESTIGATOR_QUERY_STATES
