"""scripts/gitleaks_history.py, the history scanner the secrets-history CI job
and the release gate share (OSS-2).

The tests that run the real gitleaks skip without it, unless
``LB_REQUIRE_GITLEAKS=1`` (the secrets-history CI job sets it).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "gitleaks_history.py"
CONFIG = ROOT / ".gitleaks.toml"


def _gitleaks() -> str:
    exe = shutil.which("gitleaks")
    if exe is None:
        if os.environ.get("LB_REQUIRE_GITLEAKS") == "1":
            pytest.fail("gitleaks is not on PATH and LB_REQUIRE_GITLEAKS=1")
        pytest.skip("requires gitleaks on PATH")
    return exe


def _env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        check=True,
        capture_output=True,
        text=True,
        env=_env(),
    ).stdout.strip()


def _key(fill: str) -> str:
    # Built at run time so this file never matches the FlashBlade rule itself.
    return "PSFB" + fill * 38


def _run(repo: Path, ignore: Path, *extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--repo",
            str(repo),
            "--config",
            str(CONFIG),
            "--ignore",
            str(ignore),
            *extra,
        ],
        capture_output=True,
        text=True,
        env=_env(),
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    r = tmp_path / "repo"
    r.mkdir()
    _git(r, "init", "-q", "-b", "main")
    (r / "a.txt").write_text("clean\n")
    _git(r, "add", "a.txt")
    _git(r, "commit", "-q", "-m", "clean")
    return r


@pytest.fixture
def ignore(tmp_path: Path) -> Path:
    p = tmp_path / "gitleaksignore"
    p.write_text("# none\n")
    return p


def test_clean_history_passes(repo, ignore):
    _gitleaks()
    res = _run(repo, ignore)
    assert res.returncode == 0, res.stdout + res.stderr
    assert "1 commits and their messages scanned" in res.stdout


def test_key_in_a_commit_message_fails(repo, ignore):
    _gitleaks()
    _git(repo, "commit", "-q", "--allow-empty", "-m", f"rotate\n\nnew key {_key('Q')}")
    res = _run(repo, ignore)
    assert res.returncode == 1, res.stdout + res.stderr
    assert _key("Q") not in res.stdout + res.stderr  # redacted


def test_key_in_a_tag_message_fails(repo, ignore):
    _gitleaks()
    _git(repo, "tag", "-a", "v0", "-m", f"release {_key('Z')}")
    res = _run(repo, ignore)
    assert res.returncode == 1, res.stdout + res.stderr


def test_key_in_a_merge_commit_only_fails(repo, ignore):
    _gitleaks()
    _git(repo, "checkout", "-q", "-b", "side")
    (repo / "a.txt").write_text("side\n")
    _git(repo, "commit", "-q", "-am", "side")
    _git(repo, "checkout", "-q", "main")
    (repo / "a.txt").write_text("main\n")
    _git(repo, "commit", "-q", "-am", "main")
    subprocess.run(["git", "-C", str(repo), "merge", "-q", "side"], capture_output=True, env=_env())
    (repo / "a.txt").write_text(f"key: {_key('Q')}\n")
    _git(repo, "add", "a.txt")
    _git(repo, "commit", "-q", "-m", "merge")
    assert _run(repo, ignore).returncode == 1


def test_scanned_trees_own_baseline_is_not_read(repo, ignore):
    _gitleaks()
    (repo / "b.txt").write_text(f"key: {_key('Q')}\n")
    _git(repo, "add", "b.txt")
    _git(repo, "commit", "-q", "-m", "leak")
    sha = _git(repo, "rev-parse", "HEAD")
    (repo / ".gitleaksignore").write_text(f"{sha}:b.txt:pure-flashblade-s3-access-key:1\n")
    assert _run(repo, ignore).returncode == 1
    ignore.write_text(f"# planted\n{sha}:b.txt:pure-flashblade-s3-access-key:1\n")
    res = _run(repo, ignore)
    assert res.returncode == 0, res.stdout + res.stderr


def test_unknown_rev_fails_before_scanning(repo, ignore):
    res = _run(repo, ignore, "--rev", "no-such-ref", "--gitleaks", sys.executable)
    assert res.returncode == 2 and "--remerge-diff" in res.stdout


def _fake_gitleaks(tmp_path: Path, output: str) -> Path:
    fake = tmp_path / "fake-gitleaks"
    fake.write_text(f"#!/bin/sh\nprintf '%s\\n' '{output}' >&2\nexit 0\n")
    fake.chmod(0o755)
    return fake


@pytest.mark.parametrize(
    "output",
    [
        "INF 0 commits scanned.",  # gitleaks' report when its git log failed
        "ERR [git] fatal: bad revision",
        "INF no leaks found",  # no count at all
    ],
)
def test_an_empty_or_failed_scan_fails(repo, ignore, tmp_path, output):
    fake = _fake_gitleaks(tmp_path, output)
    res = _run(repo, ignore, "--gitleaks", str(fake))
    assert res.returncode == 2, res.stdout + res.stderr
    assert "unscanned" in res.stdout


def test_missing_inputs_fail(repo, tmp_path):
    res = _run(repo, tmp_path / "absent", "--gitleaks", sys.executable)
    assert res.returncode == 2
