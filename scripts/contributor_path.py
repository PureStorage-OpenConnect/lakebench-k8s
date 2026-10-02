#!/usr/bin/env python3
"""The quick path of CONTRIBUTING.md as a script CI runs verbatim.

The block between ``<!-- contributor-path:begin -->`` and
``<!-- contributor-path:end -->`` in ``CONTRIBUTING.md`` is a fenced bash
block a new contributor copies. CI's "Contributor path" job runs it in a
clean container. Only its one ``git clone`` line changes: it clones the
repository under test (``$REPO_URL``) and checks out the commit under test
(``$SHA``) into the directory the original clone would have made, so every
other command runs as written.

Usage:
    python scripts/contributor_path.py              # print the script, or the problems (exit 1)
    python scripts/contributor_path.py --should-run # print true or false for this CI event

``--should-run`` reads ``EVENT``, ``REF`` and ``BASE_REF`` (the workflow's
``github.event_name``, ``github.ref`` and ``github.base_ref``): a push to
``main``, ``integrate/**`` or ``train/*`` and a tag always run (a release
calls CI with the tag's ref); any other push or pull request runs when it
changes one of ``INPUTS``, or when its change set cannot be read. Stdlib only:
it runs on the container's Python before anything is installed.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BEGIN = "<!-- contributor-path:begin -->"
END = "<!-- contributor-path:end -->"
#: Files whose change can break the quick path; a change to one runs the job.
INPUTS = (
    "CONTRIBUTING.md",
    "Makefile",
    "pyproject.toml",
    ".pre-commit-config.yaml",
    ".github/workflows/",
    "scripts/contributor_path.py",
    "scripts/fetch_test_jars.py",
    "tests/spark/jars.lock.json",
)
#: Refs the job always runs on: main exactly, and these prefixes.
ALWAYS_PREFIXES = ("refs/heads/integrate/", "refs/heads/train/", "refs/tags/")
INTEGRATE = "origin/integrate/v1.5.0"

_CLONE = re.compile(r"^git clone (?P<url>\S+)(?: (?P<dir>\S+))?\s*$")
# make options that take the next word as their argument, not a target.
_MAKE_ARG_OPTS = frozenset({"-C", "-f", "-I", "-j", "-l", "-o", "-W", "--directory", "--file"})


def always_runs(ref: str) -> bool:
    return ref == "refs/heads/main" or ref.startswith(ALWAYS_PREFIXES)


def extract(text: str) -> tuple[list[str], list[str]]:
    """The command lines of the marked block, and the problems found."""
    if text.count(BEGIN) != 1 or text.count(END) != 1:
        return [], [f"CONTRIBUTING.md needs exactly one {BEGIN} and one {END}"]
    body = text.split(BEGIN, 1)[1].split(END, 1)[0].strip().splitlines()
    if len(body) < 2 or not body[0].startswith("```") or body[-1].strip() != "```":
        return [], ["the contributor-path block must be one fenced code block"]
    lines = [ln for ln in body[1:-1] if ln.strip() and not ln.lstrip().startswith("#")]
    return lines, []


def _clone_dir(m: re.Match[str]) -> str:
    if m.group("dir"):
        return m.group("dir")
    return m.group("url").rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")


def problems(lines: Sequence[str], makefile: str) -> list[str]:
    out = []
    clones = [ln for ln in lines if ln.startswith("git clone")]
    if len(clones) != 1 or not _CLONE.match(clones[0]):
        out.append("the block needs exactly one plain `git clone <url> [dir]` line")
    targets = set(re.findall(r"^([A-Za-z0-9_.\-]+):", makefile, re.M))
    for ln in _joined(lines):
        if not re.search(r"(^|[\s;&|(])make(\s|$)", ln):
            continue
        try:
            words = shlex.split(ln, comments=True)
        except ValueError:
            out.append(f"cannot parse the make line {ln!r}")
            continue
        for cmd in _commands(words):
            if cmd[:1] != ["make"]:
                continue
            skip = False
            for w in cmd[1:]:
                if skip:
                    skip = False
                elif w in _MAKE_ARG_OPTS:
                    skip = True
                elif w.startswith("-") or "=" in w:
                    continue
                elif w not in targets:
                    out.append(f"`make {w}` has no Makefile target")
    return out


def _joined(lines: Sequence[str]) -> list[str]:
    """Lines with backslash continuations joined, for parsing only."""
    out: list[str] = []
    cur = ""
    for ln in lines:
        if ln.rstrip().endswith("\\"):
            cur += ln.rstrip()[:-1] + " "
            continue
        out.append(cur + ln)
        cur = ""
    if cur:
        out.append(cur)
    return out


def _commands(words: list[str]) -> list[list[str]]:
    """Split shell words into simple commands at ``&&``, ``||``, ``;`` and ``|``."""
    cmds: list[list[str]] = [[]]
    for w in words:
        if w in ("&&", "||", ";", "|"):
            cmds.append([])
        else:
            cmds[-1].append(w)
    return [c for c in cmds if c]


def runnable(lines: Sequence[str]) -> str:
    """The block with its clone line pointed at $REPO_URL and $SHA."""
    out = ["# CONTRIBUTING.md quick path; the clone is the commit under test"]
    for ln in lines:
        m = _CLONE.match(ln)
        if m:
            d = shlex.quote(_clone_dir(m))
            ln = (
                f'git clone -q "$REPO_URL" {d} && git -C {d} fetch -q origin "$SHA"'
                f' && git -C {d} checkout -q "$SHA"'
            )
        out.append(ln)
    return "\n".join(out) + "\n"


def should_run(event: str, ref: str, changed: Iterable[str] | None) -> bool:
    """``changed`` None means the change set is unknown, which runs the job."""
    if event != "pull_request" and always_runs(ref):
        return True
    if changed is None:
        return True
    return any(p == i or (i.endswith("/") and p.startswith(i)) for p in changed for i in INPUTS)


def changed_files(event: str, base_ref: str, repo: Path = ROOT) -> list[str] | None:
    """The files a pull request or push changes, from git; None if unknown."""

    def git(*args: str) -> str | None:
        r = subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True)
        return r.stdout.strip() if r.returncode == 0 else None

    if event == "pull_request":
        out = git("diff", "--name-only", f"origin/{base_ref}...HEAD") if base_ref else None
    else:
        base = git("merge-base", INTEGRATE, "HEAD")
        out = git("diff", "--name-only", base, "HEAD") if base else None
    return None if out is None else [p for p in out.splitlines() if p]


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--should-run", action="store_true")
    ap.add_argument("--contributing", type=Path, default=ROOT / "CONTRIBUTING.md")
    ap.add_argument("--makefile", type=Path, default=ROOT / "Makefile")
    args = ap.parse_args(argv)
    if args.should_run:
        event, ref = os.environ.get("EVENT", ""), os.environ.get("REF", "")
        changed = None if always_runs(ref) else changed_files(event, os.environ.get("BASE_REF", ""))
        if changed is not None:
            print(f"changed files: {len(changed)}", file=sys.stderr)
        print("true" if should_run(event, ref, changed) else "false")
        return 0
    lines, found = extract(args.contributing.read_text(encoding="utf-8"))
    found += problems(lines, args.makefile.read_text(encoding="utf-8")) if lines else []
    if found:
        for p in found:
            print(p, file=sys.stderr)
        return 1
    sys.stdout.write(runnable(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
