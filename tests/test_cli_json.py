"""``--json``: one lb-cli/1 document on stdout per command run (CLI-5b).

Goldens in ``tests/fixtures/cli-json/`` are written by hand from the
TypedDicts in ``cli/_json.py`` and, for ``report``, from a stored record's
own fields; ``compare``'s data is checked to be exactly its ``--format
json`` document. Every verb's data has its TypedDict's keys, stdout holds
the document only, and the exit code and ``exit_code`` agree on every way
out, errors included.
"""

from __future__ import annotations

import json
import shutil
import typing
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from typer.testing import CliRunner

from lakebench.cli import _json, app
from tests import test_cli_cluster_ops as co
from tests.test_cli_cluster_ops import cluster  # noqa: F401 -- the fixture

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests" / "fixtures" / "cli-json"
RECORDS = ROOT / "tests" / "fixtures" / "records"
RUN = "20260929-212900-5105a0"


def _doc(res) -> dict:
    """The one document on stdout (json.loads fails on anything else)."""
    return json.loads(res.stdout)


def _golden(name: str) -> dict:
    return json.loads((GOLDEN / f"{name}.json").read_text())


def _keys(td: type) -> set[str]:
    return set(typing.get_type_hints(td))


@pytest.fixture(autouse=True)
def _no_leftover_mode():
    yield
    assert not _json.active(), "a command left JSON mode on"


# -- status ------------------------------------------------------------------------


def test_status_json_golden(cluster):  # noqa: F811
    cluster.apps.objects = dict(co._TRINO_HIVE)
    res = CliRunner().invoke(app, ["status", str(cluster.config), "--json"])
    assert res.exit_code == 0, res.output
    assert _doc(res) == _golden("status")
    assert "Every listed component is ready" in res.stderr  # human text on stderr


def test_status_drift_json_golden(cluster):  # noqa: F811
    cluster.apps.objects = dict(co._TRINO_HIVE, **{"lakebench-trino-worker": (1, 2)})
    res = CliRunner().invoke(app, ["status", str(cluster.config), "--json"])
    assert res.exit_code == 1, res.output
    assert _doc(res) == _golden("status-drift")


def test_json_error_envelope(cluster):  # noqa: F811
    """A command that raises: data null, the error's path and code, and the
    process exit code equal to the document's."""
    cluster.core.ns_exists = False
    res = CliRunner().invoke(app, ["status", str(cluster.config), "--json"])
    doc = _doc(res)
    assert res.exit_code == doc["exit_code"] == 1
    assert doc["data"] is None
    (err,) = doc["errors"]
    assert (err["code"], err["path"]) == (1, "status.namespace_missing")
    assert err["what"] == "namespace ops does not exist"
    assert err["next"].startswith("lakebench deploy")


# -- report ------------------------------------------------------------------------


def _runs(tmp_path: Path) -> Path:
    runs = tmp_path / "lakebench-output" / "runs"
    runs.mkdir(parents=True)
    shutil.copytree(RECORDS / f"run-{RUN}", runs / f"run-{RUN}")
    return runs


def test_report_json_golden(monkeypatch, tmp_path):
    _runs(tmp_path)
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, ["report", RUN, "--json"])
    assert res.exit_code == 0, res.output
    doc = _doc(res)
    assert doc == _golden("report")
    assert set(doc["data"]) == _keys(_json.ReportRun)


def test_report_list_json(monkeypatch, tmp_path):
    _runs(tmp_path)
    monkeypatch.chdir(tmp_path)
    doc = _doc(CliRunner().invoke(app, ["report", "--list", "--json"]))
    (row,) = doc["data"]["runs"]
    assert set(row) == _keys(_json.ReportListRow)
    assert (row["run_id"], row["record_kind"], row["verdict"]) == (RUN, "run", "PASSED")
    assert (row["verdict_stored"], row["verdict_recomputed"]) == ("PASSED", "PASSED")


def test_report_unknown_run_is_an_error_document(monkeypatch, tmp_path):
    _runs(tmp_path)
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, ["report", "no-such-run", "--json"])
    doc = _doc(res)
    assert res.exit_code == doc["exit_code"] == 2
    assert doc["data"] is None
    assert doc["errors"][0]["code"] == 2 and "no-such-run" in doc["errors"][0]["what"]


def test_report_json_and_format_are_refused(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, ["report", "--json", "--format", "csv"])
    doc = _doc(res)
    assert res.exit_code == doc["exit_code"] == 2
    assert "two outputs" in doc["errors"][0]["what"]


# -- query -------------------------------------------------------------------------


class _Executor:
    def execute_query(self, sql, timeout=None):
        return SimpleNamespace(
            success=True,
            raw_output='"web","40"\n"store","2"',  # Trino's CLI: CSV, no header
            rows_returned=2,
            duration_seconds=0.25,
            error=None,
        )


def test_query_json_golden(monkeypatch, tmp_path):
    from tests.conftest import make_config

    monkeypatch.chdir(tmp_path)
    (tmp_path / "c.yaml").write_text("name: q\n")
    with (
        mock.patch("lakebench.cli._query.load_config", return_value=make_config(name="q")),
        mock.patch("lakebench.benchmark.executor.get_executor", lambda cfg, ns: _Executor()),
        mock.patch("lakebench.cli._query.journal_open"),
    ):
        res = CliRunner().invoke(app, ["query", "c.yaml", "--sql", "SELECT 1", "--json"])
    assert res.exit_code == 0, res.output
    assert _doc(res) == _golden("query")


# -- config recipes ----------------------------------------------------------------


def test_config_recipes_json_shape():
    res = CliRunner().invoke(app, ["config", "recipes", "--json"])
    assert res.exit_code == 0, res.output
    doc = _doc(res)
    assert (doc["schema"], doc["command"], doc["errors"]) == ("lb-cli/1", "config recipes", [])
    rows = doc["data"]["recipes"]
    assert rows and all(set(r) == _keys(_json.RecipeRow) for r in rows)
    from lakebench.config.recipes import RECIPES

    assert [r["recipe"] for r in rows] == sorted(n for n in RECIPES if n != "default")
    assert {s for r in rows for s in r["support"].values()} <= {
        "supported",
        "unverified",
        "unsupported",
    }


def test_config_recipe_detail_json_shape():
    res = CliRunner().invoke(app, ["config", "recipes", "hive-iceberg-spark-trino", "--json"])
    doc = _doc(res)
    assert set(doc["data"]) == _keys(_json.RecipeDetailData)
    assert (doc["data"]["catalog"], doc["data"]["table_format"], doc["data"]["query_engine"]) == (
        "hive",
        "iceberg",
        "trino",
    )
    assert all(set(s) == _keys(_json.RecipeSupport) for s in doc["data"]["support"])


def test_unknown_recipe_is_an_error_document():
    res = CliRunner().invoke(app, ["config", "recipes", "no-such", "--json"])
    doc = _doc(res)
    assert res.exit_code == doc["exit_code"] == 2 and doc["data"] is None


# -- compare -----------------------------------------------------------------------


def test_compare_json_is_the_cmp2_document(monkeypatch, tmp_path):
    runs = _runs(tmp_path)
    other = "20260929-214442-825153"
    shutil.copytree(RECORDS / f"run-{other}", runs / f"run-{other}")
    monkeypatch.chdir(tmp_path)
    plain = CliRunner().invoke(app, ["compare", RUN, other, "--format", "json"])
    wrapped = CliRunner().invoke(app, ["compare", RUN, other, "--json"])
    doc = _doc(wrapped)
    assert wrapped.exit_code == plain.exit_code == doc["exit_code"]
    assert doc["data"] == json.loads(plain.stdout)
    assert doc["command"] == "compare"


# -- plan --------------------------------------------------------------------------


def test_plan_json_document(tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text("name: p\nrecipe: hive-iceberg-spark-trino\n")
    res = CliRunner().invoke(app, ["plan", str(cfg), "--json"])
    doc = _doc(res)
    assert res.exit_code == doc["exit_code"] == 0
    assert set(doc["data"]) == _keys(_json.PlanData)
    assert doc["data"]["plans"][0]["name"] == "p"


# -- the envelope ------------------------------------------------------------------


def test_document_shape():
    doc = _json.document("x", {"a": 1}, 3, [])
    assert set(doc) == _keys(_json.Envelope)
    assert doc["schema"] == "lb-cli/1"


def test_without_json_nothing_changes(cluster):  # noqa: F811
    cluster.apps.objects = dict(co._TRINO_HIVE)
    res = CliRunner().invoke(app, ["status", str(cluster.config)])
    assert res.exit_code == 0
    assert "lb-cli/1" not in res.output
    assert "Components" in res.stdout  # the table stays on stdout


@pytest.mark.parametrize(
    ("engine", "raw", "expected"),
    [
        ("trino", '"web","40"\n"a, b","2"', (None, [["web", "40"], ["a, b", "2"]], "csv")),
        ("spark-thrift", "channel\tn\nweb\t40", (["channel", "n"], [["web", "40"]], "tsv2")),
        ("spark-thrift", "channel\tn", (["channel", "n"], [], "tsv2")),
        (
            "duckdb",
            'progress\n{"rows": 2, "data": ["(\'web\', 40)", "(\'store\', 2)"]}',
            (None, [["('web', 40)"], ["('store', 2)"]], "python-repr"),
        ),
        ("trino", "", (None, [], "csv")),
        # beeline's own output for an empty last cell and for SELECT '',
        # trimmed by the executor as production trims it: both are data.
        ("spark-thrift", "a\tb\n1\t\n", (["a", "b"], [["1", ""]], "tsv2")),
        ("spark-thrift", "_c0\n\n", (["_c0"], [[""]], "tsv2")),
        ("spark-thrift", "_c0\na\n\n", (["_c0"], [["a"], [""]], "tsv2")),
    ],
)
def test_query_rows_per_engine(engine, raw, expected):
    from lakebench.cli._query import _query_json_rows
    from lakebench.modules.query_engines.spark_thrift.executor import _drop_terminal_newline

    if engine == "spark-thrift":
        raw = _drop_terminal_newline(raw)  # what raw_output holds
    assert _query_json_rows(engine, raw) == expected


def test_report_legacy_record_keeps_its_stored_verdict(monkeypatch, tmp_path):
    """A flat run-<id>.json record that stored FAILED: the stored verdict is
    reported and heads the document, though the record recomputes PASSED (a
    reader never promotes)."""
    runs = tmp_path / "lakebench-output" / "runs"
    runs.mkdir(parents=True)
    rec = json.loads((RECORDS / f"run-{RUN}" / "metrics.json").read_text())
    rec["verdict"]["status"] = "FAILED"
    (runs / f"run-{RUN}.json").write_text(json.dumps(rec))
    monkeypatch.chdir(tmp_path)
    doc = _doc(CliRunner().invoke(app, ["report", RUN, "--json"]))
    data = doc["data"]
    assert (data["verdict"], data["verdict_stored"], data["verdict_recomputed"]) == (
        "FAILED",
        "FAILED",
        "PASSED",
    )


def test_report_json_heads_with_a_recomputed_failure(monkeypatch, tmp_path):
    """A record that stored PASSED and that the record gates now fail (silver
    wrote no rows): both halves are shown and the headline is the failure,
    in the document and in the --list row, as compare and the gates read it."""
    runs = tmp_path / "lakebench-output" / "runs" / f"run-{RUN}"
    runs.mkdir(parents=True)
    rec = json.loads((RECORDS / f"run-{RUN}" / "metrics.json").read_text())
    next(j for j in rec["jobs"] if j["job_type"] == "silver-build")["output_rows"] = 0
    (runs / "metrics.json").write_text(json.dumps(rec))
    monkeypatch.chdir(tmp_path)
    data = _doc(CliRunner().invoke(app, ["report", RUN, "--json"]))["data"]
    assert (data["verdict"], data["verdict_stored"], data["verdict_recomputed"]) == (
        "FAILED",
        "PASSED",
        "FAILED",
    )
    (row,) = _doc(CliRunner().invoke(app, ["report", "--list", "--json"]))["data"]["runs"]
    assert (row["verdict"], row["verdict_stored"], row["verdict_recomputed"]) == (
        "FAILED",
        "PASSED",
        "FAILED",
    )


def test_unknown_option_still_gets_a_document(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for argv in (["status", "--bogus", "--json"], ["config", "recipes", "--json", "--bogus"]):
        res = CliRunner().invoke(app, argv)
        doc = _doc(res)
        assert res.exit_code == doc["exit_code"] == 2, argv
        assert doc["data"] is None and "--bogus" in doc["errors"][0]["what"]


def test_help_with_json_prints_help_only():
    res = CliRunner().invoke(app, ["status", "--json", "--help"])
    assert res.exit_code == 0
    assert '"schema"' not in res.stdout and "Usage" in res.stdout


def test_a_sub_app_invoked_directly_does_not_leak_json_mode():
    from lakebench.cli._config import config_app

    CliRunner().invoke(config_app, ["recipes", "--json"])
    res = CliRunner().invoke(app, ["config", "recipes", "--local"])
    assert res.exit_code == 0 and "lb-cli/1" not in res.stdout


def test_every_stdout_console_is_redirected():
    """A new module-level Console() in cli/ would print human text into
    the document's stdout; each one must be in _json's list."""
    import re

    cli_dir = ROOT / "src" / "lakebench" / "cli"
    declared = {
        p.stem
        for p in cli_dir.glob("*.py")
        if re.search(r"^console = Console\(\)", p.read_text(), re.M)
    }
    import lakebench.cli as cli_pkg

    redirected = {
        name
        for name in declared
        if any(
            getattr(__import__(f"lakebench.cli.{name}", fromlist=["x"]), "console", None) is c
            for c in _json._stdout_consoles()
        )
    }
    assert declared and redirected == declared
    assert cli_pkg


@pytest.mark.parametrize(
    "argv",
    [
        ["journal", "--session", "--json"],  # a value, not the flag
        ["query", "--example", "--json"],
        ["deploy", "--json"],  # a command with no --json
        ["query", "c.yaml", "--", "--json"],  # an argument after --
    ],
)
def test_json_as_a_value_or_undeclared_starts_no_document(argv, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, argv)
    assert '"schema": "lb-cli/1"' not in res.stdout, argv


def test_a_real_json_after_a_value_that_reads_json(monkeypatch, tmp_path):
    from lakebench.cli import _json as j

    group = __import__("typer").main.get_command(app)
    j.start_from_args(group, ["query", "--sql", "--json", "--json"])
    try:
        assert j.active()
    finally:
        j.abandon()
        j.root_done()


def test_missing_default_config_is_an_error_in_the_document(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    res = CliRunner().invoke(app, ["query", "--sql", "SELECT 1", "--json"])
    doc = _doc(res)
    assert res.exit_code == doc["exit_code"] == 2
    assert "No config file specified" in doc["errors"][0]["what"]


def test_report_reads_the_record_by_the_requested_id(monkeypatch, tmp_path):
    """A copied run directory whose record carries another run_id: the
    stored verdict still comes from the file load_run read."""
    runs = tmp_path / "lakebench-output" / "runs"
    runs.mkdir(parents=True)
    shutil.copytree(RECORDS / f"run-{RUN}", runs / "run-copied")
    monkeypatch.chdir(tmp_path)
    doc = _doc(CliRunner().invoke(app, ["report", "copied", "--json"]))
    assert doc["data"]["verdict"] == "PASSED" and doc["data"]["scores"]
