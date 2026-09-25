"""A client timeout must not leave the query running in the engine pod.

The executors run each query through ``kubectl exec``; on a timeout
``subprocess.run`` kills only the local kubectl. Before this, the trino CLI
(or the DuckDB Python process) and its query kept running in the pod,
holding worker memory and slowing every later benchmark query.
"""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import patch

from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor
from lakebench.modules.query_engines.trino.executor import (
    TrinoExecutor,
    server_run_time_limit,
)

_QID = "20260925_101112_00042_abcde"


def _trino() -> TrinoExecutor:
    ex = TrinoExecutor(namespace="ns", catalog_name="lakehouse")
    ex._pod = "lakebench-trino-coordinator-0"
    return ex


def _arg_after(cmd: list[str], flag: str) -> str:
    return cmd[cmd.index(flag) + 1]


def test_trino_query_carries_server_run_time_limit_below_client_timeout():
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 0, "1\n", "")
        _trino().execute_query("SELECT 1", timeout=300)
    cmd = run.call_args_list[0].args[0]
    limit = _arg_after(cmd, "--session")
    assert limit == "query_max_run_time=295s"
    assert int(limit.split("=")[1].rstrip("s")) < 300
    assert _arg_after(cmd, "--source").startswith("lakebench-")
    assert cmd[-2:] == ["--execute", "SELECT 1"]


def test_server_limit_never_zero_or_above_client_timeout():
    for timeout in (1, 5, 6, 15, 30, 300, 900):
        assert 1 <= server_run_time_limit(timeout) <= max(1, timeout - 1)


def test_each_query_gets_a_distinct_source():
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        ex = _trino()
        ex.execute_query("SELECT 1")
        ex.execute_query("SELECT 2")
    sources = {_arg_after(c.args[0], "--source") for c in run.call_args_list}
    assert len(sources) == 2


def test_trino_timeout_cancels_the_query_server_side():
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if len(calls) == 1:
            raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])
        if len(calls) == 2:  # the lookup by source
            return subprocess.CompletedProcess(cmd, 0, f"{_QID}\nnot-a-query-id\n", "")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    with patch("subprocess.run", side_effect=fake_run):
        result = _trino().execute_query("SELECT slow()", timeout=60)

    assert not result.success and "timed out" in result.error
    source = _arg_after(calls[0], "--source")
    lookup = _arg_after(calls[1], "--execute")
    assert "system.runtime.queries" in lookup and f"source = '{source}'" in lookup
    assert len(calls) == 3  # the junk line is not treated as a query id
    kill = _arg_after(calls[2], "--execute")
    assert kill.startswith("CALL system.runtime.kill_query(") and f"'{_QID}'" in kill


def test_trino_cancel_failure_does_not_mask_the_timeout():
    def fake_run(cmd, **kwargs):
        if "--source" in cmd:
            raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])
        raise subprocess.TimeoutExpired(cmd, 30)

    with patch("subprocess.run", side_effect=fake_run):
        result = _trino().execute_query("SELECT slow()", timeout=60)
    assert result.error == "Query timed out (60s)"


def test_no_cancel_when_the_query_finishes():
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 0, "1\n", "")
        _trino().execute_query("SELECT 1")
    assert run.call_count == 1


def _duckdb() -> DuckDBExecutor:
    ex = DuckDBExecutor(namespace="ns", catalog_name="lakehouse", s3_endpoint="http://s3:80")
    ex._pod = "lakebench-duckdb-0"
    return ex


def test_duckdb_script_ends_itself_before_the_client_timeout():
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 0, '{"rows": 1}', "")
        _duckdb().execute_query("SELECT 1", timeout=300)
    script = run.call_args.args[0][-1]
    assert script.startswith("import signal; signal.alarm(295); ")


def test_duckdb_alarm_prefix_terminates_a_blocked_process():
    """Executed: the prefix the executor emits kills a process stuck in a
    call that never returns to the interpreter's signal handling."""
    script = _duckdb()._build_python_script("SELECT 1", timeout=6)
    prefix = script.split("import duckdb", 1)[0]
    assert prefix == "import signal; signal.alarm(1); "
    proc = subprocess.run(
        [sys.executable, "-c", prefix + "import time; time.sleep(30)"],
        capture_output=True,
        timeout=20,
    )
    assert proc.returncode == -14


def test_duckdb_alarm_exit_reads_as_timeout():
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 142, "", "")
        result = _duckdb().execute_query("SELECT 1", timeout=300)
    assert result.error == "Query timed out (300s, ended in the pod)"


def test_duckdb_script_without_timeout_is_unchanged():
    assert _duckdb()._build_python_script("SELECT 1").startswith("import duckdb")
