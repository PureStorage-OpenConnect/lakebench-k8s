#!/usr/bin/env python3
"""Fail on em dashes, emoji and AI attribution in tracked files.

Scope: every file ``git ls-files`` lists, except binary files (a NUL byte
in the first 8 KB) and files over 2 MB. Each hit is ``path:line kind --
fix``. Kinds:

- ``em-dash``: U+2014;
- ``emoji``: a code point in U+1F300 to U+1FAFF or U+2600 to U+27BF, or
  U+2B50, U+2705, U+274C, U+FE0F;
- ``ai-attribution``: a co-author trailer naming an AI assistant or its
  vendor, or a "generated with" credit line for one (case-insensitive);
- ``undecodable``: the file is not UTF-8, so it could not be checked.

Filler words are not checked here; reviewers catch those.

A hit that has to stay (a golden file for a rendered UTF-8 case, say) is
listed in ``scripts/prose_allowlist.txt`` as ``path:line:kind  # reason``.
An entry that no longer matches a hit is itself a failure, so the list
cannot outlive what it excuses.

Usage:
    python scripts/prose_guard.py          # exit 1 on any hit or stale entry
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import NamedTuple

ROOT = Path(__file__).resolve().parents[1]
ALLOWLIST = ROOT / "scripts" / "prose_allowlist.txt"
MAX_BYTES = 2 * 1024 * 1024
SNIFF_BYTES = 8192

# Written as escapes so this file passes its own scan.
_EM_DASH = "\u2014"
_EMOJI = re.compile("[\U0001f300-\U0001faff\u2600-\u27bf\u2b50\u2705\u274c\ufe0f]")
# The hyphens are in brackets so that no line here spells a trailer.
_AI = [
    re.compile(r"co[-]authored[-]by:\s*(claude|.*anthropic|.*openai|.*copilot)", re.I),
    re.compile(r"generated (with|by) (\[)?claude", re.I),
    re.compile("\U0001f916 generated", re.I),
]

FIXES = {
    "em-dash": "use `--` or restructure",
    "emoji": "remove",
    "ai-attribution": "remove",
    "undecodable": "save as UTF-8, or allowlist with a reason",
}


class Hit(NamedTuple):
    path: str
    line: int
    kind: str
    fix: str

    def render(self) -> str:
        return f"{self.path}:{self.line} {self.kind} -- {self.fix}"


def tracked_files(root: Path = ROOT) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, capture_output=True, text=True, check=True
    ).stdout
    return sorted(p for p in out.split("\0") if p)


def _line_kinds(line: str) -> list[str]:
    kinds = []
    if _EM_DASH in line:
        kinds.append("em-dash")
    if _EMOJI.search(line):
        kinds.append("emoji")
    if any(p.search(line) for p in _AI):
        kinds.append("ai-attribution")
    return kinds


def scan(paths: Iterable[str], root: Path = ROOT) -> list[Hit]:
    """Hits in *paths* (relative to *root*), one per line and kind."""
    hits = []
    for rel in paths:
        p = root / rel
        if not p.is_file() or p.is_symlink():
            continue
        if p.stat().st_size > MAX_BYTES:
            continue
        data = p.read_bytes()
        if b"\0" in data[:SNIFF_BYTES]:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            line = data[: exc.start].count(b"\n") + 1
            hits.append(Hit(rel, line, "undecodable", FIXES["undecodable"]))
            continue
        for n, line in enumerate(text.splitlines(), 1):
            for kind in _line_kinds(line):
                hits.append(Hit(rel, n, kind, FIXES[kind]))
    return hits


def load_allowlist(path: Path = ALLOWLIST) -> tuple[set[tuple[str, int, str]], list[str]]:
    """Entries ``(path, line, kind)`` and the malformed lines."""
    entries: set[tuple[str, int, str]] = set()
    bad = []
    if not path.is_file():
        return entries, bad
    for n, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        body, _, reason = raw.partition("#")
        body = body.strip()
        if not body:
            continue
        m = re.fullmatch(r"(.+):(\d+):([a-z-]+)", body)
        if not m or m.group(3) not in FIXES or not reason.strip():
            bad.append(f"{path.name}:{n}: expected 'path:line:kind  # reason', got {raw!r}")
            continue
        entries.add((m.group(1), int(m.group(2)), m.group(3)))
    return entries, bad


def check(
    root: Path = ROOT, allowlist: Path = ALLOWLIST, paths: Sequence[str] | None = None
) -> list[str]:
    """Every problem as a line: hits not allowlisted, stale and malformed entries."""
    hits = scan(tracked_files(root) if paths is None else paths, root)
    allowed, bad = load_allowlist(allowlist)
    found = {(h.path, h.line, h.kind) for h in hits}
    out = [h.render() for h in hits if (h.path, h.line, h.kind) not in allowed]
    out += [
        f"{allowlist.name}: stale entry {p}:{n}:{k} (no such hit any more; remove it)"
        for p, n, k in sorted(allowed - found)
    ]
    return out + bad


def main(argv: Sequence[str] | None = None) -> int:
    problems = check()
    for p in problems:
        print(p)
    if problems:
        print(f"prose guard: {len(problems)} problem(s)", file=sys.stderr)
        return 1
    print("prose guard: clean", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
