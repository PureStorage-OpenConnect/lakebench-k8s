"""CLI-2 (CC-10): one-line errors, markup-safe text, plain machine output."""

from __future__ import annotations

import ast
import json
import logging
import re
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from lakebench.cli import _helpers as helpers
from lakebench.cli import app

SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench"
BRACKETS = "s3a://b/[x]/y under [/tmp] on [main]"


# init writes the S3 keys as ${VAR} references; a test that loads its output
# sets them, as a user who followed init's "next" line would.
_INIT_KEYS = {"LAKEBENCH_S3_ACCESS_KEY": "placeholder", "LAKEBENCH_S3_SECRET_KEY": "placeholder"}


def _stderr(result) -> str:
    try:
        return result.stderr
    except ValueError:  # Click < 8.2 without mix_stderr=False
        return result.output


def _stdout(result) -> str:
    try:
        return result.stdout
    except ValueError:  # pragma: no cover -- Click < 8.2
        return result.output


# -- helpers -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("fn", "prefix"),
    [
        (helpers.print_error, "ERROR"),
        (helpers.print_warning, "WARN"),
        (helpers.print_info, "..."),
        (helpers.print_success, "OK"),
    ],
)
def test_print_helpers_are_verbatim_and_on_stderr(fn, prefix, capsys):
    fn(BRACKETS)
    out, err = capsys.readouterr()
    assert out == ""
    assert err == f"{prefix} {BRACKETS}\n"


def test_print_error_keeps_a_long_message_on_one_line(capsys):
    long = "x" * 300 + " [/tmp] " + "y" * 300
    helpers.print_error(long)
    err = capsys.readouterr().err
    assert err == f"ERROR {long}\n"


def test_emit_error_shape(capsys):
    from lakebench.exit_codes import SafetyRefusal

    helpers.emit_error(SafetyRefusal(BRACKETS, next="run [bold]this[/bold]"))
    out, err = capsys.readouterr()
    assert out == ""
    assert err.splitlines() == [f"ERROR  {BRACKETS}", "Next   run [bold]this[/bold]"]


def test_emit_data_is_plain_stdout(capsys):
    value = "[bold]" + "v" * 500
    helpers.emit_data(json.dumps({"k": value}))
    out, err = capsys.readouterr()
    assert err == ""
    assert json.loads(out) == {"k": value}


def test_esc_neutralises_markup():
    from rich.console import Console

    con = Console(width=200, record=True, file=open("/dev/null", "w"))  # noqa: SIM115
    con.print(f"path {helpers.esc(BRACKETS)}")
    assert con.export_text().strip() == f"path {BRACKETS}"


# -- CLI paths ---------------------------------------------------------------


def test_error_bracket_path(monkeypatch, tmp_path):
    """An exception text with brackets survives verbatim (was MarkupError)."""
    import lakebench.cli as cli
    from lakebench.config import ConfigError

    def bad(*_a, **_k):
        raise ConfigError(f"cannot read {BRACKETS}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "load_config", bad)
    (tmp_path / "c.yaml").write_text("name: x\n")
    result = CliRunner().invoke(app, ["status", str(tmp_path / "c.yaml")])
    assert result.exit_code != 0
    assert "Traceback" not in result.output
    assert f"ERROR Config error: cannot read {BRACKETS}" in _stderr(result)


class _FakeExecutor:
    def __init__(self, raw: str):
        self.raw = raw

    def execute_query(self, sql, timeout=None):
        return SimpleNamespace(
            success=True,
            raw_output=self.raw,
            rows_returned=len(self.raw.splitlines()) - 1,
            duration_seconds=0.5,
            error=None,
        )


def _query(monkeypatch, tmp_path, raw: str, fmt: str):
    import lakebench.benchmark.executor as executor_mod
    import lakebench.cli._query as query_mod
    import lakebench.metrics as metrics_mod

    monkeypatch.chdir(tmp_path)
    runner = CliRunner(env=_INIT_KEYS)
    init = runner.invoke(app, ["init", "--output", str(tmp_path / "c.yaml")])
    assert init.exit_code == 0, init.output
    monkeypatch.setattr(executor_mod, "get_executor", lambda cfg, ns: _FakeExecutor(raw))

    class _NoRuns:
        def get_latest_run_for_deployment(self, *_a, **_k):
            return None

    monkeypatch.setattr(metrics_mod, "MetricsStorage", _NoRuns)
    assert query_mod.load_config  # the command loads the real config
    return runner.invoke(
        app, ["query", str(tmp_path / "c.yaml"), "--sql", "SELECT 1", "--format", fmt]
    )


def test_long_value_json_valid(monkeypatch, tmp_path):
    value = "[bold]" + "v" * 500 + "[/tmp]"
    result = _query(monkeypatch, tmp_path, f"name\tvalue\nrow1\t{value}", "json")
    assert result.exit_code == 0, result.output
    parsed = json.loads(_stdout(result))
    assert parsed == {"rows": [{"name": "row1", "value": value}], "count": 1}
    assert "rows in" in _stderr(result)


def test_long_value_csv_valid(monkeypatch, tmp_path):
    import csv
    import io

    value = "[main]" + "c" * 500
    result = _query(monkeypatch, tmp_path, f"name\tvalue\nrow1\t{value}", "csv")
    assert result.exit_code == 0, result.output
    rows = list(csv.reader(io.StringIO(_stdout(result))))
    assert rows == [["name", "value"], ["row1", value]]


def test_no_traceback_on_list_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg = tmp_path / "list.yaml"
    cfg.write_text("- a\n- b\n")
    result = CliRunner().invoke(app, ["status", str(cfg)])
    # 1 until CC-9 converts the load-error sites; the path is config.validation (2).
    assert result.exit_code == 2, result.output  # config.validation
    assert "Traceback" not in result.output
    errors = [ln for ln in _stderr(result).splitlines() if ln.startswith("ERROR")]
    assert len(errors) == 1
    assert "top level is a YAML list" in errors[0]


def test_group_silences_urllib3(monkeypatch, _restore_urllib3_logger):
    from lakebench.cli import _exit

    monkeypatch.delenv(_exit.DEBUG_ENV, raising=False)

    logging.getLogger("urllib3").setLevel(logging.NOTSET)
    with warnings.catch_warnings():
        _exit.quiet_urllib3()
        assert logging.getLogger("urllib3").level == logging.ERROR
        import urllib3

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("default")
            _exit.quiet_urllib3()
            warnings.warn("x", urllib3.exceptions.InsecureRequestWarning, stacklevel=1)
        assert not caught


def test_root_invoke_calls_quiet_urllib3(monkeypatch):
    from lakebench.cli import _exit

    calls = []
    monkeypatch.setattr(_exit, "quiet_urllib3", lambda: calls.append(1))
    CliRunner().invoke(app, ["version"])
    assert calls


# -- lint --------------------------------------------------------------------

_NUMERIC_SPEC = re.compile(r"(?:[,_]|[bcdeEfFgGnoxX%])$")
_SAFE_CALLS = {"escape", "esc", "markup", "len", "int", "float", "round", "sum", "abs"}
_RICH_TAG = re.compile(
    r"\[/?(?:bold|dim|italic|underline|reverse|strike|blink|link|"
    r"red|green|yellow|blue|magenta|cyan|white|black|bright_\w+|grey\d*|gray\d*)\b[^\]]*\]"
)
_PRINT_HELPERS = {"print_error", "print_warning", "print_info", "print_success"}

# Rich-parsed f-strings with an unescaped value, per file, in modules other
# lanes own (SEQUENCING 2.3). A ratchet: a file may not go above its number,
# so a new unescaped site fails; fixing sites lowers the count (then lower the
# number). Owners convert their files on touch with esc() from
# lakebench.cli._helpers; init_wizard.py is deleted by CC-16.
FOREIGN_BASELINE = {
    "cli/_admin.py": 5,
    "cli/_deploy.py": 8,
    "cli/_destroy.py": 13,
    "cli/_financial.py": 11,
    "cli/_local.py": 8,
    "cli/_run.py": 34,
    "cli/_sustained.py": 30,
    "config/loader.py": 1,
    "init_wizard.py": 14,
}


def _spec_text(v: ast.FormattedValue) -> str:
    if v.format_spec is None:
        return ""
    return "".join(x.value for x in v.format_spec.values if isinstance(x, ast.Constant))


def _value_is_safe(v: ast.FormattedValue) -> bool:
    spec = _spec_text(v)
    if spec and _NUMERIC_SPEC.search(spec):
        return True
    e = v.value
    if isinstance(e, ast.Call):
        name = e.func.attr if isinstance(e.func, ast.Attribute) else getattr(e.func, "id", "")
        if name in _SAFE_CALLS or name.endswith("_markup"):
            return True
    return isinstance(e, ast.Constant)


def _literal_text(node: ast.AST) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value for v in node.values if isinstance(v, ast.Constant))
    return ""


def _rich_text_args(node: ast.Call) -> list[ast.AST]:
    """Arguments of *node* that Rich parses as markup, or [] for other calls."""
    func = node.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
    receiver = (
        func.value.id
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)
        else ""
    )
    if name == "print" and receiver in ("console", "err_console"):
        return list(node.args)
    if name == "Panel":
        return node.args[:1] + [k.value for k in node.keywords if k.arg in ("title", "subtitle")]
    if name == "add_row":
        return list(node.args)
    return []


def _scan(tree: ast.AST) -> tuple[int, list[int]]:
    """(f-strings Rich parses with an unescaped value, lines of print_* with markup)."""
    seen: set[int] = set()
    unescaped = 0
    markup_in_helper: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for arg in _rich_text_args(node):
            if isinstance(arg, ast.Call):
                continue  # a nested Panel(...) is visited as its own call
            for js in (x for x in ast.walk(arg) if isinstance(x, ast.JoinedStr)):
                if id(js) in seen:
                    continue
                seen.add(id(js))
                if any(
                    isinstance(v, ast.FormattedValue) and not _value_is_safe(v) for v in js.values
                ):
                    unescaped += 1
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
        if name in _PRINT_HELPERS and any(_RICH_TAG.search(_literal_text(a)) for a in node.args):
            markup_in_helper.append(node.lineno)
    return unescaped, markup_in_helper


def test_markup_safety_lint():
    counts: dict[str, int] = {}
    helper_markup: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        unescaped, lines = _scan(ast.parse(path.read_text(encoding="utf-8")))
        if unescaped:
            counts[rel] = unescaped
        helper_markup += [f"{rel}:{n}" for n in lines]
    assert not helper_markup, f"print_* prints text verbatim; drop the markup: {helper_markup}"
    owned_or_new = {f: n for f, n in counts.items() if f not in FOREIGN_BASELINE}
    assert not owned_or_new, (
        "a console.print, Panel or add_row f-string interpolates a value without esc(): "
        f"{owned_or_new}. Wrap it in esc(), or markup() if it is markup on purpose."
    )
    over = {f: (counts.get(f, 0), n) for f, n in FOREIGN_BASELINE.items() if counts.get(f, 0) > n}
    assert not over, f"(found, allowed) per file: wrap the new value in esc(): {over}"


def test_lint_catches_an_unescaped_value():
    bad = ast.parse(
        'console.print(f"[red]{e}[/red]")\nprint_error("[bold]x[/bold]")\n'
        'console.print(Panel(f"a {x}" + "\\n".join(f"{r}" for r in rs)))\n'
        't.add_row(f"Error: {e.reason}")\n'
    )
    assert _scan(bad) == (4, [2])
    good = ast.parse(
        'console.print(f"{esc(e)} {n:,} {x:.2f} {len(y)} {markup(s)}")\nprint_error("[x] ok")\n'
    )
    assert _scan(good) == (0, [])


# -- review follow-ups ---------------------------------------------------------


@pytest.fixture
def _restore_urllib3_logger():
    logger = logging.getLogger("urllib3")
    level = logger.level
    yield
    logger.setLevel(level)


def test_debug_keeps_urllib3_output(monkeypatch, _restore_urllib3_logger):
    from lakebench.cli import _exit

    monkeypatch.setenv(_exit.DEBUG_ENV, "1")
    logging.getLogger("urllib3").setLevel(logging.NOTSET)
    _exit.quiet_urllib3()
    assert logging.getLogger("urllib3").level == logging.NOTSET


def test_example_query_json_stdout_is_only_json(monkeypatch, tmp_path):
    """`query --example count --format json` (the docstring's own example)."""
    import lakebench.benchmark.executor as executor_mod
    import lakebench.metrics as metrics_mod

    monkeypatch.chdir(tmp_path)
    runner = CliRunner(env=_INIT_KEYS)
    assert runner.invoke(app, ["init", "--output", "c.yaml"]).exit_code == 0
    raw = "table_name\trow_count\nsilver.x\t10"
    monkeypatch.setattr(executor_mod, "get_executor", lambda cfg, ns: _FakeExecutor(raw))

    class _NoRuns:
        def get_latest_run_for_deployment(self, *_a, **_k):
            return None

    monkeypatch.setattr(metrics_mod, "MetricsStorage", _NoRuns)
    result = runner.invoke(app, ["query", "c.yaml", "--example", "count", "--format", "json"])
    assert result.exit_code == 0, result.output
    assert json.loads(_stdout(result))["count"] == 1
    assert "Query (count)" in _stderr(result)


def test_failed_query_detail_is_on_the_error_line(monkeypatch, tmp_path):
    import lakebench.benchmark.executor as executor_mod

    class _Failing:
        def execute_query(self, sql, timeout=None):
            return SimpleNamespace(
                success=False,
                raw_output="",
                rows_returned=0,
                duration_seconds=0.1,
                error="line 1:8: Table [main].x does not exist",
            )

    monkeypatch.chdir(tmp_path)
    runner = CliRunner(env=_INIT_KEYS)
    assert runner.invoke(app, ["init", "--output", "c.yaml"]).exit_code == 0
    monkeypatch.setattr(executor_mod, "get_executor", lambda cfg, ns: _Failing())
    result = runner.invoke(app, ["query", "c.yaml", "--sql", "SELECT 1", "--format", "json"])
    assert result.exit_code == 1
    assert _stdout(result) == ""
    assert "ERROR Query failed (0.10s): line 1:8: Table [main].x does not exist" in _stderr(result)


@pytest.mark.parametrize("fmt", ["json", "csv"])
def test_compare_machine_output_is_only_data(fmt, monkeypatch, tmp_path):
    """The resolution and every notice go to stderr; stdout is the data."""
    from tests.fixtures import stored_records as sr

    monkeypatch.chdir(tmp_path)
    # Pinned pair P3 (like-for-like); P1's records read failed since the
    # verdict is recomputed from the record (W5 and W6 did not run).
    ids = ("20260927-011043-e338c5", "20260927-073818-7934eb")
    for rid in ids:
        d = tmp_path / "runs" / f"run-{rid}"
        d.mkdir(parents=True)
        (d / "metrics.json").write_text(json.dumps(sr.load_record(rid)))
    result = CliRunner().invoke(
        app, ["compare", *ids, "--runs-dir", str(tmp_path / "runs"), "--format", fmt]
    )
    out = _stdout(result)
    if fmt == "json":
        assert json.loads(out)["verdict"] == "LIKE-FOR-LIKE"
    else:
        assert out.splitlines()[0] == "# schema: cmp2"
    assert f"A: {ids[0]} -> deployment" in _stderr(result)
    assert "A: " not in out
