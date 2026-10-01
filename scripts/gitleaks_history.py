#!/usr/bin/env python3
"""Scan a repository's history and messages with gitleaks, beyond a baseline.

Used by the ``secrets-history`` CI job and the release gate's
``gitleaks-history`` check, so both apply the same rules:

1. every commit reachable from ``--rev`` (default ``HEAD``), with
   ``--remerge-diff`` so a change made in a two-parent merge commit itself is
   seen, and ``--ignore-gitleaks-allow`` so an inline allow comment hides
   nothing;
2. every commit message reachable from ``--rev`` and every tag's message,
   which ``gitleaks git`` does not read. Each is written to its own file,
   ``msgs/commits/<sha>.txt`` or ``msgs/tags/<object id>.txt`` (holding the
   tag's name and message), and scanned with
   ``gitleaks dir``, so a finding names its commit or tag and has a stable
   fingerprint (``msgs/commits/<sha>.txt:<rule>:<line>``) for the baseline.

gitleaks is run from a temporary directory against the git directory, so it
reads only the baseline passed with ``--ignore`` and never a
``.gitleaksignore`` in the scanned tree.

It fails closed (exit 2) where a scan would silently miss something:

- ``gitleaks git`` exits 0 when its ``git log`` fails (it reports "0 commits
  scanned"), so the script fails when git cannot run ``--remerge-diff`` (git
  older than 2.36) or the rev is missing, when gitleaks logs an error, and
  when it scanned no commit;
- ``--remerge-diff`` shows nothing for a merge with three or more parents, so
  an octopus merge reachable from the rev fails the scan;
- a shallow clone holds only part of the history.

Usage:
    python scripts/gitleaks_history.py --repo . --config .gitleaks.toml \\
        --ignore .gitleaksignore [--rev HEAD] [--gitleaks PATH]

Exit 0 when nothing is found beyond the baseline, 1 on a finding, 2 when the
scan could not run, scanned nothing, or would miss part of the history.
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
    # errors="replace": a message that is not UTF-8 must not crash the scan.
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=_clean_env(),
    )


def _safe_name(ref: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", ref)


def write_messages(repo: Path, rev: str, dest: Path) -> int | None:
    """Write every commit message reachable from *rev* to
    ``dest/commits/<sha>.txt`` and every tag's name and message to
    ``dest/tags/<object id>.txt``. Returns the number of files, or None if git
    failed or a message holds a NUL."""
    log = _git(repo, "log", "--format=%x00%H%x00%B", rev)
    # Tag files are named by object id, never by the tag's name: a name such
    # as "x-gitleaks.toml" would match gitleaks' default path allowlist. The
    # name goes in the file, so it is scanned too.
    tags = _git(
        repo,
        "for-each-ref",
        "refs/tags",
        "--format=%00%(objectname)%00tag %(refname:strip=2)%0a%(contents)",
    )
    if log.returncode or tags.returncode:
        return None
    n = 0
    for sub, raw in (("commits", log.stdout), ("tags", tags.stdout)):
        (dest / sub).mkdir(parents=True, exist_ok=True)
        fields = raw.split("\0")[1:]
        if len(fields) % 2:
            return None  # a NUL inside a message; the pairing cannot be trusted
        for name, body in zip(fields[0::2], fields[1::2], strict=True):
            out = dest / sub / f"{_safe_name(name)}.txt"
            k = 1
            while out.exists():  # two lightweight tags on one commit
                out = dest / sub / f"{_safe_name(name)}.{k}.txt"
                k += 1
            out.write_text(body, encoding="utf-8")
            n += 1
    return n


def _report(out: str) -> str:
    text = _ANSI.sub("", out)
    sys.stdout.write(text if text.endswith("\n") or not text else text + "\n")
    return text


def _fail(msg: str) -> int:
    print(f"::error::{msg}")
    return 2


def scan(
    repo: Path, config: Path, ignore: Path, rev: str = "HEAD", gitleaks: str = "gitleaks"
) -> int:
    if _git(repo, "rev-parse", "--is-shallow-repository").stdout.strip() == "true":
        return _fail("shallow clone: only part of the history would be scanned")
    probe = _git(repo, "log", "--remerge-diff", "-1", "--format=%H", rev)
    if probe.returncode != 0:
        return _fail(
            f"git log --remerge-diff {rev} failed (git 2.36 or later is needed, "
            f"and {rev} must exist): {probe.stderr.strip()}"
        )
    octopus = _git(repo, "rev-list", "--min-parents=3", rev).stdout.split()
    if octopus:
        return _fail(
            f"{len(octopus)} merge(s) with three or more parents (first {octopus[0]}); "
            "--remerge-diff cannot show what they change, so the history is not fully scanned"
        )
    gitdir = _git(repo, "rev-parse", "--path-format=absolute", "--git-dir").stdout.strip()
    common = [
        "--config",
        str(config.resolve()),
        "--gitleaks-ignore-path",
        str(ignore.resolve()),
        "--redact",
        "--no-banner",
        "--no-color",
        "--verbose",
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
            errors="replace",
            env=_clean_env(),
        )
        text = _report(hist.stdout + hist.stderr)
        if hist.returncode != 0:
            return hist.returncode
        m = _SCANNED.search(text)
        if _ERR.search(text) or m is None or int(m.group(1)) == 0:
            return _fail("gitleaks scanned no commit or logged an error; the history is unscanned")
        commits = int(m.group(1))
        count = write_messages(repo, rev, Path(cwd) / "msgs")
        if count is None:
            return _fail(
                "could not read the commit and tag messages (git failed, or one holds a NUL)"
            )
        res = subprocess.run(
            [gitleaks, "dir", "msgs", *common],
            cwd=cwd,
            capture_output=True,
            text=True,
            errors="replace",
            env=_clean_env(),
        )
        text = _report(res.stdout + res.stderr)
        if res.returncode != 0:
            return res.returncode
        if _ERR.search(text):
            return _fail("gitleaks logged an error scanning the commit and tag messages")
    print(
        f"gitleaks-history: {commits} commits and {count} commit and tag messages "
        "scanned, nothing beyond the baseline"
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
