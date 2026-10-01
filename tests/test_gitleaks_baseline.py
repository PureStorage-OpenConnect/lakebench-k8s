"""The gitleaks history baseline (OSS-2).

``.gitleaksignore`` allowlists the two findings left in the published history,
the old default Polaris client secret that PyPI 1.0.0 to 1.4.0 shipped.
``gitleaks git`` must report nothing beyond them. The tests marked "requires
gitleaks" skip without the binary, unless ``LB_REQUIRE_GITLEAKS=1`` (the
secrets-history CI job sets it), which turns the skip into a failure.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
IGNORE = ROOT / ".gitleaksignore"

#: The baseline, pinned. A new entry is a decision that a finding in published
#: history is not a live credential; it needs this list changed in review.
BASELINE = {
    "6cc98444611411953c72fb394941a11ab98043c3:src/lakebench/_constants.py:generic-api-key:8",
    "e51bd922f17484aead6318536f8c1929b94be8fd:src/lakebench/deploy/engine.py:generic-api-key:260",
}

_FINGERPRINT = re.compile(r"^[0-9a-f]{40}:[^:\s]+:[a-z0-9-]+:[0-9]+$")


def _gitleaks() -> str:
    exe = shutil.which("gitleaks")
    if exe is None:
        if os.environ.get("LB_REQUIRE_GITLEAKS") == "1":
            pytest.fail("gitleaks is not on PATH and LB_REQUIRE_GITLEAKS=1")
        pytest.skip("requires gitleaks on PATH")
    return exe


def _entries(text: str) -> list[tuple[str, list[str]]]:
    """(entry, the comment lines directly above it) per non-comment line."""
    out = []
    comments: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            comments = []
        elif s.startswith("#"):
            comments.append(s)
        else:
            out.append((s, comments))
            comments = []
    return out


def test_ignore_entries_are_well_formed():
    entries = _entries(IGNORE.read_text())
    assert entries, ".gitleaksignore has no entries"
    for entry, comments in entries:
        assert _FINGERPRINT.match(entry), (
            f"not a <commit>:<path>:<rule>:<line> fingerprint: {entry}"
        )
        assert comments, f"{entry} has no reason comment directly above it"


def test_baseline_is_the_published_polaris_default():
    assert {e for e, _ in _entries(IGNORE.read_text())} == BASELINE


def test_entries_parser():
    text = "# head\n\n# why\nabc\n\nlonely\n"
    assert _entries(text) == [("abc", ["# why"]), ("lonely", [])]


def _git(repo: Path, *args: str) -> str:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout.strip()


def _scan(
    exe: str, repo: Path, log_opts: str = "--remerge-diff HEAD"
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            exe,
            "git",
            ".",
            "--config",
            str(ROOT / ".gitleaks.toml"),
            "--redact",
            "--no-banner",
            "--exit-code",
            "1",
            "--ignore-gitleaks-allow",
            f"--log-opts={log_opts}",
        ],
        cwd=repo,
        capture_output=True,
        text=True,
    )


def _planted_key(fill: str) -> str:
    # Built at run time so this file never matches the FlashBlade rule itself.
    return "PSFB" + fill * 38


def test_baseline_suppresses_only_its_own_fingerprint(tmp_path):
    exe = _gitleaks()
    _git(tmp_path, "init", "-q", "-b", "main")
    (tmp_path / "a.txt").write_text(f"access_key_id: {_planted_key('Q')}\n")
    _git(tmp_path, "add", "a.txt")
    _git(tmp_path, "commit", "-q", "-m", "planted")
    sha = _git(tmp_path, "rev-parse", "HEAD")
    assert _scan(exe, tmp_path).returncode == 1

    rule = "pure-flashblade-s3-access-key"
    (tmp_path / ".gitleaksignore").write_text(f"# planted fixture\n{sha}:a.txt:{rule}:1\n")
    res = _scan(exe, tmp_path)
    assert res.returncode == 0, res.stdout + res.stderr

    # A second key, in a later commit, is not covered by the first fingerprint.
    (tmp_path / "b.txt").write_text(f"access_key_id: {_planted_key('Z')}\n")
    _git(tmp_path, "add", "b.txt")
    _git(tmp_path, "commit", "-q", "-m", "second")
    assert _scan(exe, tmp_path).returncode == 1


def test_history_has_nothing_beyond_the_baseline():
    exe = _gitleaks()
    shallow = _git(ROOT, "rev-parse", "--is-shallow-repository")
    if shallow == "true":
        pytest.skip("shallow clone; the secrets-history CI job scans the full history")
    res = _scan(exe, ROOT)
    assert res.returncode == 0, (
        "gitleaks findings beyond .gitleaksignore:\n" + res.stdout + res.stderr
    )


# -- the secrets-history CI job ---------------------------------------------------


def _ci_scan_script() -> str:
    import yaml

    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    job = ci["jobs"]["secrets-history"]
    assert job["steps"][0]["with"]["fetch-depth"] == 0
    return next(s["run"] for s in job["steps"] if s.get("name") == "Scan history")


RULE = "pure-flashblade-s3-access-key"


def test_ci_history_scan_uses_the_trusted_baseline(tmp_path):
    _gitleaks()
    repo = tmp_path / "repo"
    repo.mkdir()
    runner = tmp_path / "runner"
    runner.mkdir()

    # The step runs the tree's scripts/gitleaks_history.py with `python`.
    (repo / "scripts").mkdir()
    shutil.copy(ROOT / "scripts" / "gitleaks_history.py", repo / "scripts")
    pybin = tmp_path / "bin"
    pybin.mkdir()
    (pybin / "python").symlink_to(sys.executable)

    def ci(**env: str) -> subprocess.CompletedProcess:
        full = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        full.update({"EVENT": "push", "BASE_REF": "", "RUNNER_TEMP": str(runner)}, **env)
        full["PATH"] = f"{pybin}{os.pathsep}{full.get('PATH', '')}"
        return subprocess.run(
            ["bash", "-c", _ci_scan_script()], cwd=repo, capture_output=True, text=True, env=full
        )

    def commit(rel: str, text: str, msg: str) -> str:
        (repo / rel).write_text(text)
        _git(repo, "add", rel)
        _git(repo, "commit", "-q", "-m", msg)
        return _git(repo, "rev-parse", "HEAD")

    _git(repo, "init", "-q", "-b", "main")
    commit(".gitleaks.toml", (ROOT / ".gitleaks.toml").read_text(), "config")
    published = commit("a.txt", f"k: {_planted_key('Q')}\n", "the published finding")
    main = commit(".gitleaksignore", f"# published\n{published}:a.txt:{RULE}:1\n", "baseline")

    # Bootstrap: no trusted ref has a baseline yet, so the tree's own is used.
    res = ci()
    assert res.returncode == 0 and "no trusted ref" in res.stdout, res.stdout + res.stderr
    _git(repo, "update-ref", "refs/remotes/origin/main", main)
    res = ci()
    assert res.returncode == 0 and "from origin/main" in res.stdout, res.stdout + res.stderr

    # A branch that adds a key and then allowlists it in its own baseline.
    leak = commit("b.txt", f"k: {_planted_key('Z')}\n", "leak")
    commit(
        ".gitleaksignore",
        f"# published\n{published}:a.txt:{RULE}:1\n# mine\n{leak}:b.txt:{RULE}:1\n",
        "hide it",
    )
    res = ci()
    assert res.returncode == 1 and "leaks found" in res.stdout + res.stderr, res.stderr
    # The same change as a pull request to main uses its own baseline: the owner reviews it.
    res = ci(EVENT="pull_request", BASE_REF="main")
    assert res.returncode == 0 and "::warning::" in res.stdout, res.stdout + res.stderr

    # A branch that allowlists the key in its own config.
    _git(repo, "reset", "-q", "--hard", main)
    commit("c.txt", f"k: {_planted_key('Z')}\n", "leak")
    toml = (repo / ".gitleaks.toml").read_text() + "\n[[allowlists]]\nregexes = ['''^PSFB''']\n"
    commit(".gitleaks.toml", toml, "allowlist it")
    res = ci()
    assert res.returncode == 1 and "leaks found" in res.stdout + res.stderr, res.stderr
    # A pull request to main brings its own baseline, never its own config.
    res = ci(EVENT="pull_request", BASE_REF="main")
    assert res.returncode == 1 and "config from origin/main" in res.stdout, res.stdout

    # A key in a commit message only.
    _git(repo, "reset", "-q", "--hard", main)
    _git(repo, "commit", "-q", "--allow-empty", "-m", f"note {_planted_key('Z')}")
    res = ci()
    assert res.returncode == 1 and "leaks found" in res.stdout + res.stderr, res.stdout

    # With no baseline on main, origin/integrate/v1.5.0 is the trusted ref.
    _git(repo, "reset", "-q", "--hard", main)
    _git(repo, "update-ref", "refs/remotes/origin/integrate/v1.5.0", main)
    _git(repo, "update-ref", "refs/remotes/origin/main", published)
    res = ci()
    assert res.returncode == 0 and "from origin/integrate/v1.5.0" in res.stdout, res.stdout
