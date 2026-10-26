"""``query --format json|csv`` parses each engine's output the way its
executor prints it: Trino prints quoted CSV with no header line."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest import mock

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from tests.conftest import make_config


class _Trino:
    def execute_query(self, sql, timeout=None):
        return SimpleNamespace(
            success=True,
            raw_output='"1","a,b"\n"2","c"',
            rows_returned=2,
            duration_seconds=0.1,
            error=None,
        )


@pytest.mark.parametrize(
    ("fmt", "expected"),
    [
        ("json", lambda out: json.loads(out) == {"rows": [["1", "a,b"], ["2", "c"]], "count": 2}),
        ("csv", lambda out: out.splitlines() == ['1,"a,b"', "2,c"]),
    ],
)
def test_trino_rows_are_csv_without_a_header(tmp_path, monkeypatch, fmt, expected):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "c.yaml").write_text("name: q\n")
    cfg = make_config(name="q")
    assert cfg.architecture.query_engine.type.value == "trino"
    with (
        mock.patch("lakebench.cli._query.load_config", return_value=cfg),
        mock.patch("lakebench.benchmark.executor.get_executor", lambda c, ns: _Trino()),
        mock.patch("lakebench.cli._query.journal_open"),
    ):
        res = CliRunner().invoke(app, ["query", "c.yaml", "--sql", "SELECT 1", "--format", fmt])
    assert res.exit_code == 0, res.output
    assert expected(res.stdout), res.stdout
