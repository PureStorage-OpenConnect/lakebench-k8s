#!/usr/bin/env python3
"""Scan a repository's history and messages with gitleaks, beyond a baseline.

Used by the ``secrets-history`` CI job and the release gate's
``gitleaks-history`` check, so both apply the same rules:

1. every commit reachable from ``--rev`` (default ``HEAD``), with
   ``--remerge-diff`` so a change made in a merge commit itself is seen, and
   ``--ignore-gitleaks-allow`` so an inline allow comment hides nothing;
2. every commit message reachable from ``--rev`` and every tag's message,
   which ``gitleaks git`` does not read.

gitleaks is run from a temporary directory against the git directory, so it
reads only the baseline passed with ``--ignore`` and never a
``.gitleaksignore`` in the scanned tree.

``gitleaks git`` exits 0 when its ``git log`` fails (it reports "0 commits
scanned"), so this script also fails when git cannot run ``--remerge-diff``
(git older than 2.36), when gitleaks logs an error, or when it scanned no
commit.

Usage:
    python scripts/gitleaks_history.py --repo . --config .gitleaks.toml \\
        --ignore .gitleaksignore [--rev HEAD] [--gitleaks PATH]

Exit 0 when nothing is found beyond the baseline, 1 on a finding, 2 when the
scan could not run or scanned nothing.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_SCANNED = re.compile(r"\b(\d+) commits scanned")
_ERR = re.compile(r"(^|\s)ERR(\s|$)")


def _clean_env() -> dict[str, str]:
    # A GIT_DIR or GIT_WORK_TREE inherited from a hook would point git at
    # another repository than --repo.
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        env=_clean_env(),
    )


def _messages(repo: Path, rev: str) -> str | None:
    """Every commit message reachable from *rev*, then every tag message."""
    log = _git(repo, "log", "--format=commit %H%n%B", rev)
    tags = _git(repo, "for-each-ref", "refs/tags", "--format=tag %(refname)%0a%(contents)")
    if log.returncode or tags.returncode:
        return None
    return log.stdout + tags.stdout


def _report(out: str) -> str:
    text = _ANSI.sub("", out)
    sys.stdout.write(text if text.endswith("\n") or not text else text + "\n")
    return text


def scan(
    repo: Path, config: Path, ignore: Path, rev: str = "HEAD", gitleaks: str = "gitleaks"
) -> int:
    probe = _git(repo, "log", "--remerge-diff", "-1", "--format=%H", rev)
    if probe.returncode != 0:
        print(
            f"::error::git log --remerge-diff {rev} failed (git 2.36 or later is needed, "
            f"and {rev} must exist): {probe.stderr.strip()}"
        )
        return 2
    gitdir = _git(repo, "rev-parse", "--path-format=absolute", "--git-dir").stdout.strip()
    common = [
        "--config",
        str(config.resolve()),
        "--gitleaks-ignore-path",
        str(ignore.resolve()),
        "--redact",
        "--no-banner",
        "--no-color",
        "--exit-code",
        "1",
        "--ignore-gitleaks-allow",
    ]
    with tempfile.TemporaryDirectory(prefix="lb-gitleaks-") as cwd:
        hist = subprocess.run(
            [gitleaks, "git", gitdir, *common, f"--log-opts=--remerge-diff {rev}"],
            cwd=cwd,
            capture_output=True,
            text=True,
            env=_clean_env(),
        )
        text = _report(hist.stdout + hist.stderr)
        if hist.returncode != 0:
            return hist.returncode
        m = _SCANNED.search(text)
        if _ERR.search(text) or m is None or int(m.group(1)) == 0:
            print(
                "::error::gitleaks scanned no commit or logged an error; the history is unscanned"
            )
            return 2
        msgs = _messages(repo, rev)
        if msgs is None:
            print("::error::could not read the commit and tag messages")
            return 2
        res = subprocess.run(
            [gitleaks, "stdin", *common],
            cwd=cwd,
            input=msgs,
            capture_output=True,
            text=True,
            env=_clean_env(),
        )
        text = _report(res.stdout + res.stderr)
        if res.returncode != 0:
            return res.returncode
        if _ERR.search(text):
            print("::error::gitleaks logged an error scanning the commit and tag messages")
            return 2
    print(
        f"gitleaks-history: {m.group(1)} commits and their messages scanned, "
        "nothing beyond the baseline"
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--repo", type=Path, default=Path("."))
    ap.add_argument("--config", type=Path, required=True)
    ap.add_argument("--ignore", type=Path, required=True)
    ap.add_argument("--rev", default="HEAD")
    ap.add_argument("--gitleaks", default=os.environ.get("GITLEAKS") or "gitleaks")
    args = ap.parse_args(argv)
    exe = shutil.which(args.gitleaks) or args.gitleaks
    if not Path(exe).exists():
        print(f"::error::{args.gitleaks} not found")
        return 2
    for p in (args.config, args.ignore):
        if not p.is_file():
            print(f"::error::{p} not found")
            return 2
    return scan(args.repo, args.config, args.ignore, args.rev, exe)


if __name__ == "__main__":
    sys.exit(main())
