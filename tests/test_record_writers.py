"""A run record is written once, by the run that owns it (CLI-5).

``query`` prints and never writes into a record; ``benchmark`` writes a
record of its own (``record_kind: "benchmark"``, ``parent_run_id``) and
never opens the run it measured for writing; ``MetricsStorage.save_run``
refuses to replace a record unless the owner says ``seal_update``.
``report`` absorbs ``results`` (``--format``) and reads ./lakebench.yaml
when no argument is given.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.metrics.storage import MetricsStorage, RecordExistsError
from tests.conftest import make_config

FIXTURES = Path(__file__).parent / "fixtures" / "records"
PARENT = "20260926-231711-6dd3bc"  # lb16-val-trino, batch, with an experiment block
OTHER = "20260927-073818-7934eb"  # lb16-cs-pol-trino, a newer run of another deployment
NAME = "lb16-val-trino"


def _runs(tmp_path: Path, *run_ids: str) -> Path:
    """The fixture records, byte for byte, under ./lakebench-output/runs."""
    runs = tmp_path / "lakebench-output" / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    for rid in run_ids:
        shutil.copytree(FIXTURES / f"run-{rid}", runs / f"run-{rid}")
    return runs


def _shas(runs: Path) -> dict[str, str]:
    return {
        str(p.relative_to(runs)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(runs.rglob("*"))
        if p.is_file()
    }


def _stderr(res) -> str:
    return " ".join((res.stderr if hasattr(res, "stderr") else res.output).split())


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------


class _Executor:
    def execute_query(self, sql, timeout=None):
        return SimpleNamespace(
            success=True,
            raw_output="n\n42",
            rows_returned=1,
            duration_seconds=0.25,
            error=None,
        )


def test_query_leaves_records_byte_identical(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runs = _runs(tmp_path, PARENT, OTHER)
    before = _shas(runs)
    (tmp_path / "c.yaml").write_text(f"name: {NAME}\n")
    with (
        mock.patch("lakebench.cli._query.load_config", return_value=make_config(name=NAME)),
        mock.patch("lakebench.benchmark.executor.get_executor", lambda cfg, ns: _Executor()),
        mock.patch("lakebench.cli._query.journal_open"),
    ):
        res = CliRunner().invoke(app, ["query", "c.yaml", "--sql", "SELECT 42"])
    assert res.exit_code == 0, res.output
    assert "42" in res.output
    assert _shas(runs) == before
    assert "appended" not in res.output


# ---------------------------------------------------------------------------
# benchmark
# ---------------------------------------------------------------------------


def _bench_result():
    from lakebench.benchmark.queries import BenchmarkQuery
    from lakebench.benchmark.runner import BenchmarkResult, QueryResult

    q = BenchmarkQuery(
        name="Q1_full_aggregation_scan", display_name="Q1", query_class="scan", sql="SELECT 1"
    )
    return BenchmarkResult(
        mode="power",
        cache="hot",
        scale=1.0,
        queries=[QueryResult(query=q, elapsed_seconds=1.5, rows_returned=3, success=True)],
        total_seconds=1.5,
        qph=2400.0,
        iterations=1,
        streams=1,
        stream_results=[],
        engine="trino",
    )


def _benchmark(tmp_path, refusal=None):
    (tmp_path / "c.yaml").write_text(f"name: {NAME}\n")
    fake_runner = mock.Mock()
    fake_runner.tm_run_id = None
    fake_runner.run.return_value = _bench_result()
    with (
        mock.patch("lakebench.cli._query.load_config", return_value=make_config(name=NAME)),
        mock.patch("lakebench.benchmark.BenchmarkRunner", return_value=fake_runner),
        mock.patch("lakebench.cli._query._latest_tm_run_id", return_value=None),
        mock.patch("lakebench.cli._query.journal_open"),
        mock.patch("lakebench.cli._run._benchmark_gate_problems", return_value=[]),
        mock.patch("lakebench.deps.runtime.attach_refusal", return_value=refusal),
    ):
        return CliRunner().invoke(app, ["benchmark", "c.yaml"])


def test_benchmark_writes_own_record(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runs = _runs(tmp_path, PARENT, OTHER)
    before = _shas(runs)

    res = _benchmark(tmp_path)

    assert res.exit_code == 0, res.output
    after = _shas(runs)
    # Every file that was there is byte-identical: the parent (and the other
    # deployment's run) were never written.
    assert {k: after[k] for k in before} == before
    new = sorted({k.split("/")[0] for k in after} - {k.split("/")[0] for k in before})
    assert len(new) == 1 and new[0].startswith("run-")
    data = json.loads((runs / new[0] / "metrics.json").read_text())
    assert data["record_kind"] == "benchmark"
    assert data["parent_run_id"] == PARENT
    assert data["run_id"] == new[0].removeprefix("run-") != PARENT
    assert data["deployment_name"] == NAME
    assert data["benchmark"]["qph"] == 2400.0
    assert "series" not in data
    # Every place a reader takes a QpH from is this benchmark's, not the parent's.
    pb = data["pipeline_benchmark"]
    assert pb["run_id"] == data["run_id"]
    assert pb["query_benchmark"]["qph"] == 2400.0
    assert pb["scores"]["composite_qph"] == 2400.0
    assert [st["queries_per_hour"] for st in pb["stages"] if st["stage_type"] == "query"] == [
        2400.0
    ]
    # The record names the code that ran the benchmark and when.
    bp = data["provenance"]["benchmark"]
    assert bp["started_at"] and bp["ended_at"] and "lakebench_version" in bp
    # The copied experiment block's benchmark half follows the new benchmark.
    assert data["experiment"]["benchmark_source"].startswith("lakebench benchmark")
    out = _stderr(res)
    assert f"benchmark record of run {PARENT}, which is unchanged" in out
    # The benchmark record is never the deployment's latest run.
    latest = MetricsStorage(runs).get_latest_run_for_deployment(NAME)
    assert latest is not None and latest.run_id == PARENT


def test_benchmark_on_another_dependency_set_records_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runs = _runs(tmp_path, PARENT)
    before = _shas(runs)
    res = _benchmark(tmp_path, refusal="the query engine runs another dependency set")
    assert res.exit_code == 0, res.output
    assert _shas(runs) == before
    assert "Benchmark not recorded: the query engine runs another" in _stderr(res)


def test_benchmark_with_no_run_records_nothing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runs = _runs(tmp_path, OTHER)
    before = _shas(runs)
    res = _benchmark(tmp_path)
    assert res.exit_code == 0, res.output
    assert _shas(runs) == before
    assert f"deployment {NAME} has no run record" in _stderr(res)


# ---------------------------------------------------------------------------
# save_run
# ---------------------------------------------------------------------------


def test_save_run_refuses_an_existing_record(tmp_path):
    runs = _runs(tmp_path, PARENT)
    storage = MetricsStorage(runs)
    path = runs / f"run-{PARENT}" / "metrics.json"
    raw = path.read_bytes()
    rec = storage.load_run(PARENT)
    rec.queries = []
    with pytest.raises(RecordExistsError):
        storage.save_run(rec)
    assert path.read_bytes() == raw
    assert not list(path.parent.glob("*.tmp"))
    # The owning run may replace its own record.
    storage.save_run(rec, seal_update=True)
    assert json.loads(path.read_text())["run_id"] == PARENT


def test_save_run_creates_a_new_record(tmp_path):
    storage = MetricsStorage(tmp_path / "runs")
    rec = MetricsStorage(FIXTURES).load_run(PARENT)
    rec.run_id = "fresh"
    path = storage.save_run(rec)
    assert json.loads(path.read_text())["run_id"] == "fresh"
    assert "record_kind" not in json.loads(path.read_text())  # a run's record is unchanged


def test_record_kind_round_trips():
    rec = MetricsStorage(FIXTURES).load_run(PARENT)
    assert (rec.record_kind, rec.parent_run_id) == ("run", None)
    rec.record_kind, rec.parent_run_id = "benchmark", PARENT
    d = rec.to_dict()
    back = MetricsStorage(FIXTURES)._dict_to_metrics(d)
    assert (back.record_kind, back.parent_run_id) == ("benchmark", PARENT)


# ---------------------------------------------------------------------------
# report and results
# ---------------------------------------------------------------------------


def _invoke(tmp_path, monkeypatch, *argv):
    monkeypatch.chdir(tmp_path)
    return CliRunner().invoke(app, list(argv))


def test_results_aliases_report(tmp_path, monkeypatch):
    _runs(tmp_path, PARENT)
    for fmt in ("table", "json", "csv"):
        a = _invoke(tmp_path, monkeypatch, "results", PARENT, "--format", fmt)
        b = _invoke(tmp_path, monkeypatch, "report", PARENT, "--format", fmt)
        assert a.exit_code == b.exit_code == 0, (a.output, b.output)
        assert a.output == b.output
    # results' default is the table, as before.
    a = _invoke(tmp_path, monkeypatch, "results", PARENT)
    b = _invoke(tmp_path, monkeypatch, "report", PARENT, "--format", "table")
    assert a.output == b.output and "Pipeline Benchmark:" in a.output
    j = _invoke(tmp_path, monkeypatch, "report", "--run", PARENT, "--format", "json")
    assert json.loads(j.stdout)["run_id"] == PARENT


def test_report_honours_default_config(tmp_path, monkeypatch):
    _runs(tmp_path, PARENT, OTHER)  # OTHER is newer and another deployment's
    (tmp_path / "lakebench.yaml").write_text(f"name: {NAME}\nrecipe: hive-iceberg-spark-trino\n")
    res = _invoke(tmp_path, monkeypatch, "report")
    assert res.exit_code == 0, res.output
    assert f"run {PARENT}" in res.output
    assert OTHER not in res.output
    assert f"Showing the latest record of deployment {NAME} (./lakebench.yaml)" in _stderr(res)


def test_report_without_default_config_reads_the_latest_run(tmp_path, monkeypatch):
    _runs(tmp_path, PARENT, OTHER)
    res = _invoke(tmp_path, monkeypatch, "report")
    assert res.exit_code == 0, res.output
    assert f"run {OTHER}" in res.output
    assert "Showing the latest record" not in _stderr(res)


def test_report_positional_run_and_config(tmp_path, monkeypatch):
    runs = _runs(tmp_path, PARENT, OTHER)
    assert f"run {PARENT}" in _invoke(tmp_path, monkeypatch, "report", f"run-{PARENT}").output
    by_dir = _invoke(tmp_path, monkeypatch, "report", str(runs / f"run-{PARENT}") + "/")
    assert f"run {PARENT}" in by_dir.output, by_dir.output
    missing = _invoke(tmp_path, monkeypatch, "report", "myconfig")
    assert missing.exit_code == 2 and "and no file myconfig" in _stderr(missing)
    (tmp_path / "other.yaml").write_text(f"name: {NAME}\nrecipe: hive-iceberg-spark-trino\n")
    assert f"run {PARENT}" in _invoke(tmp_path, monkeypatch, "report", "other.yaml").output


@pytest.mark.parametrize(
    "argv",
    [
        ["report", "missing.yaml"],
        ["report", PARENT, "--run", OTHER],
        ["report", "--format", "xml"],
        ["report", "--format", "json", "--render"],
        ["report", "--format", "json", "--list"],
    ],
    ids=["missing-config", "two-runs", "bad-format", "format-render", "format-list"],
)
def test_report_usage_errors_exit_2(argv, tmp_path, monkeypatch):
    _runs(tmp_path, PARENT, OTHER)
    res = _invoke(tmp_path, monkeypatch, *argv)
    assert res.exit_code == 2, res.output


def test_report_list_names_benchmark_records(tmp_path, monkeypatch):
    runs = _runs(tmp_path, PARENT)
    storage = MetricsStorage(runs)
    rec = storage.load_run(PARENT)
    rec.run_id, rec.record_kind, rec.parent_run_id = "20261002-000000-abcdef", "benchmark", PARENT
    storage.save_run(rec)
    monkeypatch.setenv("COLUMNS", "200")  # one table row per record
    res = _invoke(tmp_path, monkeypatch, "report", "--list")
    assert res.exit_code == 0, res.output
    assert f"benchmark of {PARENT}" in " ".join(res.output.split())
    # The newest record is a benchmark record; the latest run is still the run.
    res = _invoke(tmp_path, monkeypatch, "report")
    assert f"run {PARENT}" in res.output
    assert "is a benchmark record" not in _stderr(res)  # the summary is the run's own
    res = _invoke(tmp_path, monkeypatch, "report", "20261002-000000-abcdef")
    assert f"is a benchmark record of run {PARENT}" in _stderr(res)


def test_benchmark_record_of_a_continuous_run_drops_its_rounds(tmp_path):
    from lakebench.cli._query import _save_benchmark_record

    cont = "20260926-215221-65567b"  # lb16-cont-c360, continuous, in-stream rounds
    runs = _runs(tmp_path, cont)
    storage = MetricsStorage(runs)
    parent = storage.load_run(cont)
    assert parent.benchmark_rounds, "fixture premise: the parent has in-stream rounds"
    path = _save_benchmark_record(storage, parent, _bench_result())
    data = json.loads(path.read_text())
    assert data.get("benchmark_rounds", []) == []
    assert data["pipeline_benchmark"]["scores"]["composite_qph"] == 2400.0
    assert "in_stream_composite_qph" not in data["pipeline_benchmark"]["scores"]


def test_perf_gate_and_export_skip_benchmark_records(tmp_path):
    from lakebench.cli._query import _save_benchmark_record
    from lakebench.metrics import perf_gate as pg

    runs = _runs(tmp_path, PARENT)
    storage = MetricsStorage(runs)
    path = _save_benchmark_record(storage, storage.load_run(PARENT), _bench_result())
    bench_id = path.parent.name.removeprefix("run-")
    assert [r.run_id for r in pg.iter_runs(runs)] == [PARENT]
    run = pg.load_run(path.parent)
    reasons = pg.run_refusals(run, SimpleNamespace(mode=run.mode, fingerprint={}))
    assert any(f"a benchmark record (of run {PARENT}), not a run" in r for r in reasons)
    csv_text = storage.export_csv(tmp_path / "out.csv").read_text()
    assert PARENT in csv_text and bench_id not in csv_text
