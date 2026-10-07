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


def test_prose_check_runs_the_prose_guard(monkeypatch):
    # The gate's check is the guard's own check(): a hit or a stale
    # allowlist entry fails it, with the guard's lines as the detail.
    assert rg.check_prose().status == rg.PASS
    real = rg._load_script

    def fake(name):
        mod = real(name)
        if name == "prose_guard":
            mod.check = lambda skipped=None: ["docs/a.md:2 em-dash -- use `--` or restructure"]
        return mod

    monkeypatch.setattr(rg, "_load_script", fake)
    res = rg.check_prose()
    assert res.status == rg.FAIL and "docs/a.md:2 em-dash" in res.detail


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
        "gitleaks-history",
        "pre-push-hook",
        "examples",
        "version",
        "changelog",
        "prose",
        "package-guard",
    } <= names


def test_fast_checks_pass_on_this_tree():
    # Version and examples are cheap and must hold on every commit, not only
    # at release time.
    results = rg.run_checks([c for c in rg.build_checks() if c.name in {"version", "examples"}])
    assert rg.failures(results) == [], rg.format_report(results)


def test_pythonpath_is_appended_not_replaced(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/elsewhere")
    parts = rg._pythonpath_with_src().split(":")
    assert parts[0].endswith("/src") and "/elsewhere" in parts


def test_check_examples_restores_sys_path():
    before = list(sys.path)
    rg.check_examples()
    assert sys.path == before


def _gitleaks_or_skip() -> str:
    import os
    import shutil

    exe = shutil.which("gitleaks")
    if exe is None:
        if os.environ.get("LB_REQUIRE_GITLEAKS") == "1":
            pytest.fail("gitleaks is not on PATH and LB_REQUIRE_GITLEAKS=1")
        pytest.skip("requires gitleaks on PATH")
    return exe


def _repo(tmp_path, text: str):
    import subprocess

    def git(*args):
        return subprocess.run(
            ["git", "-C", str(tmp_path), "-c", "user.name=t", "-c", "user.email=t@t", *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "-q", "-b", "main")
    (tmp_path / ".gitleaks.toml").write_text((ROOT / ".gitleaks.toml").read_text())
    (tmp_path / "a.txt").write_text(text)
    git("add", ".")
    git("commit", "-q", "-m", "c")
    return git("rev-parse", "HEAD")


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


def test_gitleaks_history_check_sees_merges_and_inline_allow(tmp_path, monkeypatch):
    import subprocess

    _gitleaks_or_skip()
    monkeypatch.setattr(rg, "ROOT", tmp_path)

    def git(*args):
        subprocess.run(
            ["git", "-C", str(tmp_path), "-c", "user.name=t", "-c", "user.email=t@t", *args],
            check=True,
            capture_output=True,
        )

    _repo(tmp_path, "base\n")
    (tmp_path / ".gitleaksignore").write_text("# none\n")
    git("add", ".gitleaksignore")
    git("commit", "-q", "-m", "baseline")
    assert rg.check_gitleaks_history().status == rg.PASS
    key = "PSFB" + "Q" * 38  # built at run time
    # A key added while resolving a merge conflict, in the merge commit only.
    git("checkout", "-q", "-b", "side")
    (tmp_path / "a.txt").write_text("side\n")
    git("commit", "-q", "-am", "side")
    git("checkout", "-q", "main")
    (tmp_path / "a.txt").write_text("main\n")
    git("commit", "-q", "-am", "main")
    m = subprocess.run(
        ["git", "-C", str(tmp_path), "-c", "user.name=t", "-c", "user.email=t@t", "merge", "side"],
        capture_output=True,
        text=True,
    )
    assert (tmp_path / ".git" / "MERGE_HEAD").exists(), m.stdout + m.stderr  # a conflicted merge
    (tmp_path / "a.txt").write_text(f"key: {key}\n")
    git("add", "a.txt")
    git("commit", "-q", "-m", "merge")
    res = rg.check_gitleaks_history()
    assert res.status == rg.FAIL and "leaks found" in res.detail, res.detail
    # An inline allow comment does not hide one either.
    git("reset", "-q", "--hard", "main~1")
    (tmp_path / "b.txt").write_text(f"key: {key} # gitleaks:allow\n")
    git("add", "b.txt")
    git("commit", "-q", "-m", "allow")
    res = rg.check_gitleaks_history()
    assert res.status == rg.FAIL and "leaks found" in res.detail, res.detail


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
