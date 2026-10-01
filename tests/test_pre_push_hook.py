"""scripts/hooks/pre-push: the shared pre-push hook (OSS-2).

The hook refuses a push whose ref reaches a pre-rewrite commit that carried
leaked keys, and scans the pushed range with gitleaks using the config from
``refs/remotes/origin/integrate/v1.5.0``, never the pushing branch's copy,
and the baseline from the same ref; merge commits' changes are scanned and an
inline allow comment hides nothing.
Each test runs the tracked hook against a throwaway repository. Tests of the
hook's own logic put a stub ``gitleaks`` that finds nothing on PATH; the two
tests marked "requires gitleaks" run the real binary; they skip without it unless
``LB_REQUIRE_GITLEAKS=1`` (set by the secrets-history CI job, which installs
it).
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HOOK = ROOT / "scripts" / "hooks" / "pre-push"
ZERO = "0" * 40
BUILT_IN = (
    "36ca9b27662e0376668735ded650f3c61c5c7128",
    "3619708eccec6566419a4071d46ddf0f9eefe190",
    "476deeaec40a1c2e5adf2c550fd81ea7dd63e6e1",
    "68b750c3e6479998269baab38c0fb76bf5a5d3d2",
)
CONFIG_REF = "refs/remotes/origin/integrate/v1.5.0"


def _require_gitleaks() -> None:
    if shutil.which("gitleaks") is None:
        if os.environ.get("LB_REQUIRE_GITLEAKS") == "1":
            pytest.fail("gitleaks is not on PATH and LB_REQUIRE_GITLEAKS=1")
        pytest.skip("requires gitleaks on PATH")


def _clean_env() -> dict[str, str]:
    """os.environ without GIT_* (set when pytest runs inside a git hook) and
    without the hook's own LB_PREPUSH_* overrides."""
    return {k: v for k, v in os.environ.items() if not k.startswith(("GIT_", "LB_PREPUSH_"))}


def _git(repo: Path, *args: str) -> str:
    out = subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        check=True,
        capture_output=True,
        text=True,
        env=_clean_env(),
    )
    return out.stdout.strip()


def _commit(repo: Path, rel: str, text: str, msg: str) -> str:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    _git(repo, "add", rel)
    _git(repo, "commit", "-q", "-m", msg)
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path):
    """A repo whose origin/integrate/v1.5.0 carries the real .gitleaks.toml."""
    r = tmp_path / "repo"
    r.mkdir()
    _git(r, "init", "-q", "-b", "main")
    base = _commit(r, ".gitleaks.toml", (ROOT / ".gitleaks.toml").read_text(), "base")
    _git(r, "update-ref", CONFIG_REF, base)
    return r


@pytest.fixture
def stub_bin(tmp_path):
    """A PATH directory whose gitleaks finds nothing (exit 0)."""
    d = tmp_path / "bin"
    d.mkdir()
    stub = d / "gitleaks"
    stub.write_text("#!/bin/sh\ncat >/dev/null 2>&1 || true\nexit 0\n")
    stub.chmod(0o755)
    return d


def _path_without_gitleaks() -> str:
    keep = [
        p
        for p in os.environ.get("PATH", "").split(os.pathsep)
        if p and not (Path(p) / "gitleaks").exists()
    ]
    return os.pathsep.join(keep)


def _run(repo: Path, lines: list[str], path: str, **env: str) -> subprocess.CompletedProcess:
    full = _clean_env()
    full.update(env, PATH=path)
    return subprocess.run(
        ["bash", str(HOOK), "origin", "https://example.invalid/repo.git"],
        cwd=repo,
        input="".join(f"{ln}\n" for ln in lines),
        capture_output=True,
        text=True,
        env=full,
    )


def _stub_path(stub_bin: Path) -> str:
    return os.pathsep.join([str(stub_bin), _path_without_gitleaks()])


def _new_branch(lsha: str, name: str = "lane/x") -> str:
    return f"refs/heads/{name} {lsha} refs/heads/{name} {ZERO}"


# -- the tracked file --------------------------------------------------------


def test_hook_is_tracked_executable():
    mode = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "-s", "scripts/hooks/pre-push"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    assert mode and mode[0] == "100755", mode
    assert HOOK.stat().st_mode & stat.S_IXUSR


def test_hook_blocks_the_four_pre_rewrite_commits():
    text = HOOK.read_text()
    for sha in BUILT_IN:
        assert f"\n  {sha}\n" in text, sha
    # LB_PREPUSH_EXTRA_BLOCK can only append to the list.
    assert 'blocked+=("$extra")' in text


# -- the hook's logic, with a stub gitleaks ------------------------------------


def test_refuses_a_ref_that_reaches_a_blocked_commit(repo, stub_bin):
    key = _commit(repo, "a.txt", "planted\n", "the key commit")
    tip = _commit(repo, "b.txt", "later\n", "on top")
    res = _run(repo, [_new_branch(tip)], _stub_path(stub_bin), LB_PREPUSH_EXTRA_BLOCK=key)
    assert res.returncode == 1
    assert f"reaches pre-rewrite commit {key[:12]}" in res.stderr


def test_passes_a_clean_new_branch(repo, stub_bin):
    tip = _commit(repo, "a.txt", "clean\n", "clean work")
    res = _run(repo, [_new_branch(tip)], _stub_path(stub_bin))
    assert res.returncode == 0, res.stderr


def test_passes_an_update_of_a_known_remote_tip(repo, stub_bin):
    old = _commit(repo, "a.txt", "one\n", "one")
    new = _commit(repo, "a.txt", "two\n", "two")
    line = f"refs/heads/lane/x {new} refs/heads/lane/x {old}"
    assert _run(repo, [line], _stub_path(stub_bin)).returncode == 0


def test_skips_a_delete(repo, stub_bin):
    key = _commit(repo, "a.txt", "planted\n", "the key commit")
    line = f"(delete) {ZERO} refs/heads/lane/old {key}"
    res = _run(repo, [line], _stub_path(stub_bin), LB_PREPUSH_EXTRA_BLOCK=key)
    assert res.returncode == 0, res.stderr


def test_refuses_without_the_integrate_config(repo, stub_bin):
    tip = _commit(repo, "a.txt", "clean\n", "clean work")
    _git(repo, "update-ref", "-d", CONFIG_REF)
    res = _run(repo, [_new_branch(tip)], _stub_path(stub_bin))
    assert res.returncode == 1
    assert "cannot read .gitleaks.toml" in res.stderr


def test_a_local_branch_cannot_supply_the_config(repo, stub_bin):
    tip = _commit(repo, "a.txt", "clean\n", "clean work")
    res = _run(
        repo,
        [_new_branch(tip)],
        _stub_path(stub_bin),
        LB_PREPUSH_CONFIG_REF="refs/heads/main",
    )
    assert res.returncode == 1
    assert "must be under refs/remotes/" in res.stderr


def test_refuses_without_gitleaks(repo):
    tip = _commit(repo, "a.txt", "clean\n", "clean work")
    path = _path_without_gitleaks()
    if shutil.which("git", path=path) is None:
        pytest.skip("git and gitleaks share a PATH directory here")
    res = _run(repo, [_new_branch(tip)], path)
    assert res.returncode == 1
    assert "gitleaks is not installed" in res.stderr


def test_extra_block_entry_must_be_a_commit(repo, stub_bin):
    tip = _commit(repo, "a.txt", "clean\n", "clean work")
    res = _run(repo, [_new_branch(tip)], _stub_path(stub_bin), LB_PREPUSH_EXTRA_BLOCK="f" * 40)
    assert res.returncode == 1
    assert "is not a commit here" in res.stderr


# -- with the real gitleaks ------------------------------------------------------


def _planted_key() -> str:
    # Built at run time so this file never matches the FlashBlade rule itself.
    return "PSFB" + "Q" * 38


def test_real_gitleaks_refuses_a_planted_key(repo):
    _require_gitleaks()
    tip = _commit(repo, "conf.txt", f"access_key_id: {_planted_key()}\n", "add config")
    res = _run(repo, [_new_branch(tip)], os.environ["PATH"])
    assert res.returncode == 1
    assert "gitleaks findings" in res.stderr
    assert "rule=pure-flashblade-s3-access-key" in res.stderr
    assert _planted_key() not in res.stderr + res.stdout


def test_real_gitleaks_passes_a_clean_branch(repo):
    _require_gitleaks()
    tip = _commit(repo, "notes.txt", "nothing secret here\n", "clean work")
    res = _run(repo, [_new_branch(tip)], os.environ["PATH"])
    assert res.returncode == 0, res.stderr


def test_real_gitleaks_refuses_an_inline_allow(repo):
    _require_gitleaks()
    tip = _commit(repo, "conf.txt", f"k: {_planted_key()}  # gitleaks:allow\n", "allow it")
    res = _run(repo, [_new_branch(tip)], os.environ["PATH"])
    assert res.returncode == 1, res.stderr
    assert "gitleaks findings" in res.stderr, res.stderr  # a finding, not a failure


def test_real_gitleaks_ignores_the_pushing_trees_baseline(repo):
    """A branch cannot allowlist its own finding in its .gitleaksignore."""
    _require_gitleaks()
    leak = _commit(repo, "conf.txt", f"k: {_planted_key()}\n", "leak")
    tip = _commit(
        repo,
        ".gitleaksignore",
        f"# mine\n{leak}:conf.txt:pure-flashblade-s3-access-key:1\n",
        "hide it",
    )
    res = _run(repo, [_new_branch(tip)], os.environ["PATH"])
    assert res.returncode == 1, res.stderr
    assert "gitleaks findings" in res.stderr, res.stderr  # a finding, not a failure


def test_real_gitleaks_refuses_a_key_added_in_a_merge(repo):
    _require_gitleaks()
    _git(repo, "checkout", "-q", "-b", "side")
    _commit(repo, "a.txt", "side\n", "side")
    _git(repo, "checkout", "-q", "main")
    _commit(repo, "a.txt", "main\n", "main")
    subprocess.run(
        ["git", "-C", str(repo), "merge", "-q", "side"], capture_output=True, env=_clean_env()
    )
    tip = _commit(repo, "a.txt", f"k: {_planted_key()}\n", "merge")
    assert _git(repo, "rev-list", "--parents", "-n", "1", tip).count(" ") == 2
    res = _run(repo, [_new_branch(tip)], os.environ["PATH"])
    assert res.returncode == 1, res.stderr
    assert "gitleaks findings" in res.stderr, res.stderr  # a finding, not a failure
