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
        "gitleaks-history",
        "pre-push-hook",
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


def test_gitleaks_history_check(tmp_path, monkeypatch):
    import subprocess

    _gitleaks_or_skip()
    monkeypatch.setattr(rg, "ROOT", tmp_path)
    # Built at run time so this file never matches the FlashBlade rule itself.
    sha = _repo(tmp_path, "access_key_id: " + "PSFB" + "Q" * 38 + "\n")
    # Without the baseline file the check refuses to run.
    res = rg.check_gitleaks_history()
    assert res.status == rg.FAIL and ".gitleaksignore" in res.detail
    (tmp_path / ".gitleaksignore").write_text("# other\n" + "a" * 40 + ":x:generic-api-key:1\n")
    res = rg.check_gitleaks_history()
    assert res.status == rg.FAIL and "leaks found" in res.detail, res.detail
    (tmp_path / ".gitleaksignore").write_text(
        f"# planted\n{sha}:a.txt:pure-flashblade-s3-access-key:1\n"
    )
    res = rg.check_gitleaks_history()
    assert res.status == rg.PASS, res.detail
    # A key in a commit message, which `gitleaks git` alone does not read.
    subprocess.run(
        [
            "git",
            "-C",
            str(tmp_path),
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "note " + "PSFB" + "Z" * 38,
        ],
        check=True,
    )
    res = rg.check_gitleaks_history()
    assert res.status == rg.FAIL and "leaks found" in res.detail, res.detail


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


# --- release evidence: freeze, expected-results, records, support-record ----


def _git(repo, *args):
    import subprocess as sp

    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
        "PATH": "/usr/bin:/bin",
        "HOME": str(repo),
    }
    out = sp.run(["git", *args], cwd=repo, env=env, capture_output=True, text=True, check=True)
    return out.stdout.strip()


@pytest.fixture
def frozen(tmp_path, monkeypatch):
    """A repository with a freeze commit declared in uat/freeze-9.9.9 and
    the expected-results file committed before it."""
    import shutil as sh

    if sh.which("git") is None:
        pytest.skip("git not installed")
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "README.md").write_text((ROOT / "README.md").read_text())
    (repo / "CHANGELOG.md").write_text("# Changelog\n\n## [Unreleased]\n\n- a change\n")
    (repo / "src.txt").write_text("code\n")
    (repo / "uat").mkdir()
    (repo / "uat" / "expected-results-9.9.9.json").write_text(
        '{"version": "9.9.9", "entries": [], "continuous": []}\n'
    )
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "expected")
    (repo / "src.txt").write_text("code 2\n")
    _git(repo, "commit", "-qam", "freeze")
    sha = _git(repo, "rev-parse", "HEAD")
    (repo / "uat" / "freeze-9.9.9").write_text(sha + "\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "declare freeze")
    monkeypatch.setattr(rg, "ROOT", repo)
    fake = type("M", (), {"package_version": staticmethod(lambda: "9.9.9")})
    monkeypatch.setattr(rg, "_load_script", lambda name: fake)
    return repo, sha


def test_release_checks_skip_before_the_freeze(tmp_path, monkeypatch):
    monkeypatch.setattr(rg, "ROOT", tmp_path)
    fake = type("M", (), {"package_version": staticmethod(lambda: "9.9.9")})
    monkeypatch.setattr(rg, "_load_script", lambda name: fake)
    for check in (rg.check_freeze, rg.check_expected_results, rg.check_records):
        assert check().status == rg.SKIP
    assert rg.make_support_record_check(None)().status == rg.SKIP
    # --require-all at the tag makes each a failure.
    results = [rg.check_freeze(), rg.check_records()]
    assert len(rg.failures(results, require_all=True)) == 2


def test_freeze_clean_passes(frozen):
    assert rg.check_freeze().status == rg.PASS, rg.check_freeze().detail


def test_freeze_file_not_a_sha(frozen):
    repo, _sha = frozen
    (repo / "uat" / "freeze-9.9.9").write_text("main\n")
    assert rg.check_freeze().status == rg.FAIL


def test_freeze_not_an_ancestor(frozen):
    repo, _sha = frozen
    (repo / "uat" / "freeze-9.9.9").write_text("e" * 40 + "\n")
    _git(repo, "commit", "-qam", "bad freeze")
    assert "not an ancestor" in rg.check_freeze().detail


def test_freeze_dirty_tree(frozen):
    repo, _sha = frozen
    (repo / "src.txt").write_text("uncommitted\n")
    assert "not clean" in rg.check_freeze().detail


def test_post_freeze_allowed_paths_pass(frozen):
    repo, _sha = frozen
    for rel in ("benchmarks/perf/baselines.yaml", "docs/benchmarks/examples/pair/README.md"):
        p = repo / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("evidence\n")
    (repo / "uat" / "results-9.9.9.md").write_text("# UAT results 9.9.9\n")
    text = (repo / "CHANGELOG.md").read_text().replace("## [Unreleased]", "## [9.9.9] - 2026-11-11")
    (repo / "CHANGELOG.md").write_text(text)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "evidence")
    assert rg.check_freeze().status == rg.PASS, rg.check_freeze().detail


def test_post_freeze_readme_edit_outside_a_block_fails(frozen):
    repo, _sha = frozen
    (repo / "README.md").write_text((repo / "README.md").read_text() + "\nA stray line.\n")
    _git(repo, "commit", "-qam", "stray")
    assert "README.md changed outside its generated blocks" in rg.check_freeze().detail


def test_post_freeze_changelog_body_edit_fails(frozen):
    repo, _sha = frozen
    (repo / "CHANGELOG.md").write_text((repo / "CHANGELOG.md").read_text() + "- another\n")
    _git(repo, "commit", "-qam", "changelog")
    assert "beyond the release heading" in rg.check_freeze().detail


def test_post_freeze_code_change_fails(frozen):
    repo, _sha = frozen
    (repo / "src.txt").write_text("hotfix\n")
    _git(repo, "commit", "-qam", "hotfix")
    assert "src.txt changed after the freeze" in rg.check_freeze().detail


def test_expected_results_before_the_freeze_pass(frozen):
    assert rg.check_expected_results().status == rg.PASS, rg.check_expected_results().detail


def test_expected_results_newer_than_the_freeze_fail(frozen):
    repo, _sha = frozen
    (repo / "uat" / "expected-results-9.9.9.json").write_text(
        '{"version": "9.9.9", "entries": [{"workload": "x"}], "continuous": []}\n'
    )
    _git(repo, "commit", "-qam", "late fingerprints")
    assert "newer than the freeze is refused" in rg.check_expected_results().detail


def test_records_check_reads_each_cited_record(frozen, monkeypatch):
    import json

    from tests.fixtures import stored_records as sr

    repo, sha = frozen
    rid = "20260928-102711-8387da"
    (repo / "uat" / "runs" / f"run-{rid}").mkdir(parents=True)
    (repo / "uat" / "runs" / f"run-{rid}" / "metrics.json").write_text(
        json.dumps(sr.load_record("102711-8387da"))
    )
    (repo / "uat" / "results-9.9.9.md").write_text(
        "# UAT results 9.9.9\n\n| recipe | run |\n|---|---|\n| c360 | " + rid + " |\n"
    )
    res = rg.check_records()
    assert res.status == rg.FAIL
    assert rid in res.detail and "not from the freeze commit" in res.detail


def test_support_record_empty_on_tag_fails(frozen):
    res = rg.make_support_record_check("v9.9.9")()
    assert res.status == rg.FAIL and "lists nothing" in res.detail


def test_post_freeze_rename_out_of_src_fails(frozen):
    repo, _sha = frozen
    _git(repo, "mv", "src.txt", "uat/src.txt")
    _git(repo, "commit", "-qm", "move")
    assert "src.txt changed after the freeze" in rg.check_freeze().detail


def test_support_record_needs_every_row_at_its_scale_on_the_freeze_tree(frozen, monkeypatch):
    import json

    import lakebench.config.support as support
    from lakebench.metrics import release_record as rr
    from tests.fixtures import stored_records as sr

    repo, sha = frozen
    rid = "20260928-130953-f8a2cf"  # AML batch hive Trino at scale 1
    d = repo / "uat" / "runs" / f"run-{rid}"
    d.mkdir(parents=True)
    d.joinpath("metrics.json").write_text(json.dumps(sr.load_record("130953-f8a2cf")))
    record = {
        (w, r, support.canonical_mode(m)): support.Validation(
            w, r, support.canonical_mode(m), "0" * 12, (rid,)
        )
        for w, m, r, _s in rr.RELEASE_MATRIX
    }
    monkeypatch.setattr(support, "load_validation_record", lambda path=None: record)
    detail = rg.make_support_record_check("v9.9.9")().detail
    assert "no validated run for financial hive-iceberg-spark-trino batch at scale 10" in detail
    assert f"not the freeze {sha[:12]}" in detail


def test_post_freeze_version_bump_allowed_other_edits_not(frozen):
    repo, _sha = frozen
    init = repo / "src" / "lakebench" / "__init__.py"
    init.parent.mkdir(parents=True)
    init.write_text('"""Lakebench."""\n\n__version__ = "9.9.9.dev0"\n')
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "version file before the freeze")
    sha = _git(repo, "rev-parse", "HEAD")
    (repo / "uat" / "freeze-9.9.9").write_text(sha + "\n")
    _git(repo, "commit", "-qam", "move the freeze")
    init.write_text('"""Lakebench."""\n\n__version__ = "9.9.9"\n')
    _git(repo, "commit", "-qam", "bump")
    assert rg.check_freeze().status == rg.PASS, rg.check_freeze().detail
    init.write_text('"""Lakebench, edited."""\n\n__version__ = "9.9.9"\n')
    _git(repo, "commit", "-qam", "edit")
    assert "beyond __version__" in rg.check_freeze().detail


def test_release_workflow_runs_the_evidence_checks_on_full_history():
    import yaml

    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "release.yml").read_text())
    gate = wf["jobs"]["gate"]
    assert gate["steps"][0]["with"]["fetch-depth"] == 0
    run = next(s["run"] for s in gate["steps"] if "release_gate.py" in str(s.get("run")))
    only = set(run.split("--only", 1)[1].split()[0].split(","))
    assert {"records", "support-record", "freeze", "expected-results"} <= only
    assert "--require-all" in run
