"""scripts/release_gate.py: the report names every failure and the exit code
is non-zero whenever any check fails (GOALS P9.6)."""

from __future__ import annotations

import importlib.util
import json
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
    assert res.status == rg.FAIL and "cites no run ids" in res.detail
    rid = "20260926-101500-abc123"
    good = f"| hive-iceberg-spark-trino | batch | PASS | run-{rid} |\n"
    path.write_text(f"# UAT results {version}\n\n{table}{good}")
    res = rg.check_uat_results()
    assert res.status == rg.FAIL and rid in res.detail  # no metrics.json yet
    run_dir = tmp_path / "lakebench-output" / "runs" / f"run-{rid}"
    run_dir.mkdir(parents=True)
    (run_dir / "metrics.json").write_text(json.dumps({"run_id": "20260926-101500-ffffff"}))
    assert rg.check_uat_results().status == rg.FAIL  # a metrics.json for another run
    (run_dir / "metrics.json").write_text(json.dumps({"run_id": rid}))
    res = rg.check_uat_results()
    assert res.status == rg.PASS and "1 result rows, 1 run ids resolved" in res.detail
    assert "1 resolved only outside uat/" in res.detail  # CI cannot see it


def test_uat_results_run_ids_resolve_in_checked_in_or_named_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(rg, "ROOT", tmp_path)
    monkeypatch.delenv(rg.PERF_RUNS_ENV, raising=False)
    version = "9.9.9"
    fake = type("M", (), {"package_version": staticmethod(lambda: version)})
    monkeypatch.setattr(rg, "_load_script", lambda name: fake)
    a, b, c = "20260926-101500-aaaaaa", "20260926-101500-bbbbbb", "20260926-101500-cccccc"
    for d, rid in (("uat/runs", a), ("uat/perf", b)):
        run_dir = tmp_path / d / f"run-{rid}"
        run_dir.mkdir(parents=True)
        (run_dir / "metrics.json").write_text(json.dumps({"run_id": rid}))
    named = tmp_path / "evidence" / "r9.json.d" / "metrics.json"
    named.parent.mkdir(parents=True)
    named.write_text(json.dumps({"run_id": c}))
    path = tmp_path / "uat" / f"results-{version}.md"
    rows = (
        "| recipe | result | run |\n|---|---|---|\n"
        f"| x | PASS | {a} |\n| y | PASS | {b} |\n"
        f"| z | PASS | {c} (evidence/r9.json.d/metrics.json) |\n"
    )
    path.write_text(f"# UAT results {version}\n\n{rows}")
    res = rg.check_uat_results()
    assert res.status == rg.PASS, res.detail
    assert "outside uat/" not in res.detail  # all checked in or named in-repo
    path.write_text(f"# UAT results {version}\n\n{rows}| w | PASS | 20260926-101500-dddddd |\n")
    res = rg.check_uat_results()
    assert res.status == rg.FAIL and "1 of 4" in res.detail and "dddddd" in res.detail
    # An id in prose outside the table needs no evidence.
    path.write_text(f"# UAT results {version}\n\nSuperseded 20260926-101500-dddddd.\n\n{rows}")
    assert rg.check_uat_results().status == rg.PASS
    # Typos are not skipped.
    for bad in (
        "20260926-101500-ABC999",
        "20260926_101500_def456",
        "20260926-101500-abc1234",
        "120260926-101500-abc123",
    ):
        path.write_text(f"# UAT results {version}\n\n{rows}| v | PASS | {bad} |\n")
        res = rg.check_uat_results()
        assert res.status == rg.FAIL and "malformed" in res.detail, bad
    # Evidence outside the repository does not count.
    outside = tmp_path.parent / f"outside-{tmp_path.name}" / "metrics.json"
    outside.parent.mkdir(parents=True, exist_ok=True)
    d = "20260926-101500-eeeeee"
    outside.write_text(json.dumps({"run_id": d}))
    rel_out = f"../{outside.parent.name}/metrics.json"
    for ref in (str(outside), rel_out):
        path.write_text(f"# UAT results {version}\n\n{rows}| u | PASS | {d} ({ref}) |\n")
        assert rg.check_uat_results().status == rg.FAIL, ref


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


def test_releasing_doc_matches_release_workflow_only_list():
    """docs/releasing.md must say what release.yml's gate --only runs."""
    import re

    wf = (ROOT / ".github" / "workflows" / "release.yml").read_text()
    m = re.search(r"release_gate\.py[^\n]*\n?[^\n]*--only ([\w,-]+)", wf)
    assert m, "release.yml no longer passes --only to release_gate.py"
    only = set(m.group(1).split(","))
    doc = (ROOT / "docs" / "releasing.md").read_text()
    in_wf = "perf-baselines" in only
    says_not_in = "not in the release workflow's `--only` list" in doc
    assert in_wf != says_not_in, (sorted(only), says_not_in)
    listed = re.search(r"\(`release\.yml` runs ([^)]*)\)", doc)
    assert listed, "docs/releasing.md no longer lists what release.yml runs"
    doc_names = set(re.split(r",\s*|\s+and\s+", " ".join(listed.group(1).split())))
    assert doc_names == only, (sorted(doc_names), sorted(only))
