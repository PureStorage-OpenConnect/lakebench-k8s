"""scripts/release_gate.py: the report names every failure and the exit code
is non-zero whenever any check fails (GOALS P9.6)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("release_gate", ROOT / "scripts" / "release_gate.py")
rg = importlib.util.module_from_spec(_spec)
sys.modules["release_gate"] = rg  # dataclasses look the module up by name
_spec.loader.exec_module(rg)

EM = chr(0x2014)


def _check(name, status, detail=""):
    return rg.Check(name, lambda: rg.Result(name, status, detail), name)


def _boom():
    raise RuntimeError("kaput")


def test_all_pass_reports_passed():
    results = rg.run_checks([_check("a", rg.PASS, "fine"), _check("b", rg.PASS)])
    report = rg.format_report(results)
    assert rg.failures(results) == []
    assert "PASSED: 2 checks" in report


def test_failures_are_listed_with_detail():
    results = rg.run_checks(
        [_check("a", rg.PASS), _check("b", rg.FAIL, "line one\nline two"), _check("c", rg.FAIL)]
    )
    report = rg.format_report(results)
    assert [r.name for r in rg.failures(results)] == ["b", "c"]
    assert "FAILED: 2 of 3 checks" in report
    assert "-- b (failed)" in report and "line two" in report
    assert "-- c (failed)" in report


def test_skip_passes_unless_require_all():
    results = rg.run_checks([_check("a", rg.PASS), _check("leaks", rg.SKIP, "not installed")])
    assert rg.failures(results) == []
    assert "1 skipped: leaks" in rg.format_report(results)
    assert [r.name for r in rg.failures(results, require_all=True)] == ["leaks"]
    assert "skipped, and --require-all is set" in rg.format_report(results, require_all=True)


def test_crashing_check_is_a_failure_not_a_crash():
    results = rg.run_checks([rg.Check("x", _boom, "x")])
    assert results[0].status == rg.FAIL
    assert "RuntimeError: kaput" in results[0].detail


def test_command_check_exit_codes(tmp_path):
    ok = rg.command_check("ok", [sys.executable, "-c", "print('hi')"])
    bad = rg.command_check("bad", [sys.executable, "-c", "import sys; print('boom'); sys.exit(3)"])
    missing = rg.command_check("missing", ["definitely-not-a-binary-xyz"])
    assert ok.status == rg.PASS and ok.detail == "hi"
    assert bad.status == rg.FAIL and "exit 3" in bad.detail and "boom" in bad.detail
    assert missing.status == rg.FAIL and "not found" in missing.detail


def test_find_em_dashes(tmp_path, monkeypatch):
    monkeypatch.setattr(rg, "ROOT", tmp_path)
    clean = tmp_path / "a.md"
    clean.write_text("fine -- text\n")
    dirty = tmp_path / "b.md"
    dirty.write_text(f"ok\nbad {EM} here\n")
    assert rg.find_em_dashes([clean, dirty]) == ["b.md:2"]


def test_main_exit_code_follows_failures(monkeypatch, capsys):
    monkeypatch.setattr(
        rg, "build_checks", lambda tag=None, perf_runs=None: [_check("a", rg.FAIL, "why")]
    )
    assert rg.main([]) == 1
    assert "-- a (failed)" in capsys.readouterr().out
    monkeypatch.setattr(rg, "build_checks", lambda tag=None, perf_runs=None: [_check("a", rg.SKIP)])
    assert rg.main([]) == 0
    assert rg.main(["--require-all"]) == 1


def test_only_rejects_unknown_names():
    with pytest.raises(SystemExit):
        rg.main(["--only", "nope"])


def test_gate_covers_the_required_checks():
    names = {c.name for c in rg.build_checks()}
    assert {
        "pytest",
        "ruff-check",
        "ruff-format",
        "mypy",
        "cargo-fmt",
        "cargo-clippy",
        "cargo-test",
        "gitleaks",
        "examples",
        "version",
        "changelog",
        "em-dashes",
        "uat-results",
        "perf-baselines",
    } <= names


def test_fast_checks_pass_on_this_tree():
    # Version and examples are cheap and must hold on every commit, not only
    # at release time.
    results = rg.run_checks([c for c in rg.build_checks() if c.name in {"version", "examples"}])
    assert rg.failures(results) == [], rg.format_report(results)


def test_uat_results_check(tmp_path, monkeypatch):
    monkeypatch.setattr(rg, "ROOT", tmp_path)
    version = "9.9.9"
    fake = type("M", (), {"package_version": staticmethod(lambda: version)})
    monkeypatch.setattr(rg, "_load_script", lambda name: fake)
    assert rg.check_uat_results().status == rg.FAIL  # missing
    path = tmp_path / "uat" / f"results-{version}.md"
    path.parent.mkdir()
    table = "| recipe | mode | result | run id |\n|---|---|---|---|\n"
    row = "| hive-iceberg-spark-trino | batch | PASS | run-1 |\n"
    path.write_text(f"Mentions {version} but no heading\n{table}{row}")
    assert rg.check_uat_results().status == rg.FAIL
    path.write_text(f"# UAT results {version}.1\n{table}{row}")
    assert rg.check_uat_results().status == rg.FAIL  # heading must match exactly
    path.write_text(f"# UAT results {version}\n{table}")
    res = rg.check_uat_results()
    assert res.status == rg.FAIL and "no results table rows" in res.detail
    path.write_text(f"# UAT results {version}\n\n{table}{row}")
    res = rg.check_uat_results()
    assert res.status == rg.PASS and "1 result rows" in res.detail


def test_em_dash_scope_covers_changelog_github_examples_and_cli():
    scope = rg.EM_DASH_SCOPE
    assert "*.md" in scope and ".github/**" in scope and "examples/**" in scope
    assert any(s.startswith("src/lakebench/cli") for s in scope)


def test_pythonpath_is_appended_not_replaced(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/elsewhere")
    parts = rg._pythonpath_with_src().split(":")
    assert parts[0].endswith("/src") and "/elsewhere" in parts


def test_check_examples_restores_sys_path():
    before = list(sys.path)
    rg.check_examples()
    assert sys.path == before
