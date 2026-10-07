"""The gitleaks history baseline (OSS-2).

``.gitleaksignore`` allowlists the two findings left in the published history,
the old default Polaris client secret that PyPI 1.0.0 to 1.4.0 shipped.
``gitleaks git`` must report nothing beyond them. The tests marked "requires
gitleaks" skip without the binary, unless ``LB_REQUIRE_GITLEAKS=1`` (the
secrets-history CI job sets it), which turns the skip into a failure.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
IGNORE = ROOT / ".gitleaksignore"

#: The baseline, pinned. A new entry is a decision that a finding in published
#: history is not a live credential; it needs this list changed in review.
BASELINE = {
    "6cc98444611411953c72fb394941a11ab98043c3:src/lakebench/_constants.py:generic-api-key:8",
    "e51bd922f17484aead6318536f8c1929b94be8fd:src/lakebench/deploy/engine.py:generic-api-key:260",
    "58d5e35c5bedfd4956bcae58902428e8eb12f4b2:src/lakebench/deploy/deployment_secrets.py:generic-api-key:48",
    "58d5e35c5bedfd4956bcae58902428e8eb12f4b2:src/lakebench/deploy/deployment_secrets.py:generic-api-key:49",
}


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


def test_baseline_is_the_published_polaris_default():
    assert {e for e, _ in _entries(IGNORE.read_text())} == BASELINE


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
