"""scripts/release_gate.py: the report names every failure and the exit code
is non-zero whenever any check fails."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("release_gate", ROOT / "scripts" / "release_gate.py")
rg = importlib.util.module_from_spec(_spec)
sys.modules["release_gate"] = rg  # dataclasses look the module up by name
_spec.loader.exec_module(rg)


def _check(name, status, detail=""):
    return rg.Check(name, lambda: rg.Result(name, status, detail), name)


def _boom():
    raise RuntimeError("kaput")


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


def test_main_exit_code_follows_failures(monkeypatch, capsys):
    monkeypatch.setattr(
        rg, "build_checks", lambda tag=None, perf_runs=None: [_check("a", rg.FAIL, "why")]
    )
    assert rg.main([]) == 1
    assert "-- a (failed)" in capsys.readouterr().out
    monkeypatch.setattr(rg, "build_checks", lambda tag=None, perf_runs=None: [_check("a", rg.SKIP)])
    assert rg.main([]) == 0
    assert rg.main(["--require-all"]) == 1


def test_gitleaks_history_check_refuses_a_shallow_clone(monkeypatch, tmp_path):
    monkeypatch.setattr(rg.shutil, "which", lambda name: "/bin/true")
    monkeypatch.setattr(rg, "_is_shallow", lambda root: True)
    res = rg.check_gitleaks_history()
    assert res.status == rg.FAIL and "shallow" in res.detail
    monkeypatch.setattr(rg, "_is_shallow", lambda root: None)
    assert rg.check_gitleaks_history().status == rg.FAIL


def test_gitleaks_history_check_skips_without_gitleaks(monkeypatch):
    monkeypatch.delenv("GITLEAKS", raising=False)
    monkeypatch.setattr(rg.shutil, "which", lambda name: None)
    assert rg.check_gitleaks_history().status == rg.SKIP


def test_pre_push_hook_check(tmp_path, monkeypatch):
    import subprocess

    monkeypatch.setattr(rg, "ROOT", tmp_path)
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "scripts" / "hooks").mkdir(parents=True)
    (tmp_path / "scripts" / "hooks" / "pre-push").write_text("#!/bin/sh\nexit 0\n")
    assert rg.check_pre_push_hook().status == rg.SKIP  # nothing installed
    installed = tmp_path / ".git" / "hooks" / "pre-push"
    installed.write_text("#!/bin/sh\nexit 0\n")
    assert rg.check_pre_push_hook().status == rg.PASS
    installed.write_text("#!/bin/sh\nexit 1\n")
    res = rg.check_pre_push_hook()
    assert res.status == rg.FAIL and "differs" in res.detail
    # With core.hooksPath set, git runs that directory's hook, so that is the one checked.
    other = tmp_path / "hooks2"
    other.mkdir()
    subprocess.run(["git", "-C", str(tmp_path), "config", "core.hooksPath", str(other)], check=True)
    assert rg.check_pre_push_hook().status == rg.SKIP
    (other / "pre-push").write_text("#!/bin/sh\nexit 0\n")
    assert rg.check_pre_push_hook().status == rg.PASS
