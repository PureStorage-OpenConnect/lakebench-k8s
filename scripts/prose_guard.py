#!/usr/bin/env python3
"""Fail on em dashes, emoji and AI attribution in tracked files.

Scope: every file ``git ls-files`` lists, except binary files (a NUL byte
in the first 8 KB, unless the file starts with a UTF-16 byte-order mark)
and files over 2 MB. Each hit is ``path:line kind -- fix``. Kinds:

- ``em-dash``: U+2014, or an HTML entity for it (the named one, or the
  numeric 8212 or x2014), which renders as one in a report;
- ``emoji``: a code point in U+1F000 to U+1FAFF, U+2600 to U+27BF, or one
  of the emoji outside those blocks (U+231A-231B, U+23E9-23F3, U+23F8-23FA,
  U+2B1B-2B1C, U+2B50, U+2B55, U+FE0F). U+2600 to U+27BF also holds
  dingbats such as check marks; use ASCII for those too;
- ``ai-attribution``: a co-author, generated-by or assisted-by trailer
  that names an AI assistant or its vendor (Claude, Anthropic, OpenAI,
  ChatGPT, Codex, Copilot, Gemini, Cursor, Aider, Devin), or a credit line
  saying text was generated, created or written with or by Claude
  (case-insensitive);
- ``undecodable``: the file is not UTF-8, so it could not be checked.

Filler words are not checked here; reviewers catch those.

A hit that has to stay (a golden file for a rendered UTF-8 case, say) is
listed in ``scripts/prose_allowlist.txt`` as ``path:kind:key  # reason``,
where key is the one the hit prints: the first 8 hex digits of the SHA-1 of
the line with surrounding whitespace removed. Keyed by content, an entry
survives lines added above it. An entry that no longer matches a hit is
itself a failure, so the list cannot outlive what it excuses.

Usage:
    python scripts/prose_guard.py          # exit 1 on any hit or stale entry
"""

from __future__ import annotations

import hashlib
import os
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
_EM_DASH = re.compile("|".join(["\u2014", "&" + "mdash;", "&#" + "8212;", "&#x" + "2014;"]), re.I)
_EMOJI = re.compile(
    "[\U0001f000-\U0001faff\u2600-\u27bf\u231a\u231b\u23e9-\u23f3\u23f8-\u23fa"
    "\u2b1b\u2b1c\u2b50\u2b55\ufe0f]"
)
_AI_NAMES = r"(\bclaude\b|anthropic|openai|chatgpt|\bcodex\b|copilot|\bgemini\b|\bcursor\b|\baider\b|\bdevin\b)"
# The hyphens are in brackets so that no line here spells a trailer.
_AI = [
    re.compile(rf"(co[-]authored|generated|assisted)[-]by:.*{_AI_NAMES}", re.I),
    re.compile(r"(generated|created|written) (with|by|using) \[?claude\b", re.I),
    re.compile("\U0001f916 generated", re.I),
]
_UTF16_BOMS = (b"\xff\xfe", b"\xfe\xff")

FIXES = {
    "em-dash": "use `--` or restructure",
    "emoji": "use plain ASCII (`ok`, `x`, `->`) or remove",
    "ai-attribution": "remove",
    "undecodable": "save as UTF-8, or allowlist with a reason",
}


class Hit(NamedTuple):
    path: str
    line: int
    kind: str
    fix: str
    key: str = ""

    def render(self) -> str:
        return f"{self.path}:{self.line} {self.kind} -- {self.fix} (allowlist key {self.path}:{self.kind}:{self.key})"


def line_key(line: str) -> str:
    return hashlib.sha1(line.strip().encode("utf-8", "surrogatepass")).hexdigest()[:8]


def _git_env() -> dict[str, str]:
    # Under a git hook GIT_DIR and GIT_INDEX_FILE name the hook's repository;
    # inherited, `git -C <root>` would list that index instead of root's.
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def tracked_files(root: Path = ROOT) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"],
        capture_output=True,
        text=True,
        check=True,
        env=_git_env(),
    ).stdout
    return sorted(p for p in out.split("\0") if p)


def _line_kinds(line: str) -> list[str]:
    kinds = []
    if _EM_DASH.search(line):
        kinds.append("em-dash")
    if _EMOJI.search(line):
        kinds.append("emoji")
    if any(p.search(line) for p in _AI):
        kinds.append("ai-attribution")
    return kinds


def scan(paths: Iterable[str], root: Path = ROOT, skipped: list[str] | None = None) -> list[Hit]:
    """Hits in *paths* (relative to *root*), one per line and kind. Paths
    not scanned (missing, a symlink, binary, over 2 MB) go to *skipped*."""
    hits = []
    for rel in paths:
        p = root / rel
        if not p.is_file() or p.is_symlink() or p.stat().st_size > MAX_BYTES:
            if skipped is not None:
                skipped.append(rel)
            continue
        data = p.read_bytes()
        if data.startswith(_UTF16_BOMS):
            hits.append(Hit(rel, 1, "undecodable", FIXES["undecodable"], "utf16"))
            continue
        if b"\0" in data[:SNIFF_BYTES]:
            if skipped is not None:
                skipped.append(rel)
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            line = data[: exc.start].count(b"\n") + 1
            hits.append(Hit(rel, line, "undecodable", FIXES["undecodable"], "utf8"))
            continue
        # Lines as editors and grep count them: on \n only.
        for n, line in enumerate(text.split("\n"), 1):
            line = line.removesuffix("\r")
            for kind in _line_kinds(line):
                hits.append(Hit(rel, n, kind, FIXES[kind], line_key(line)))
    return hits


def load_allowlist(path: Path = ALLOWLIST) -> tuple[set[tuple[str, str, str]], list[str]]:
    """Entries ``(path, kind, key)`` and the malformed lines."""
    entries: set[tuple[str, str, str]] = set()
    bad = []
    if not path.is_file():
        return entries, bad
    for n, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        body, _, reason = raw.partition("#")
        body = body.strip()
        if not body:
            continue
        m = re.fullmatch(r"(.+):([a-z-]+):([0-9a-f]{8}|utf8|utf16)", body)
        if not m or m.group(2) not in FIXES or not reason.strip():
            bad.append(f"{path.name}:{n}: expected 'path:kind:key  # reason', got {raw!r}")
            continue
        entries.add((m.group(1), m.group(2), m.group(3)))
    return entries, bad


def check(
    root: Path = ROOT,
    allowlist: Path = ALLOWLIST,
    paths: Sequence[str] | None = None,
    skipped: list[str] | None = None,
) -> list[str]:
    """Every problem as a line: hits not allowlisted, stale and malformed entries."""
    hits = scan(tracked_files(root) if paths is None else paths, root, skipped)
    allowed, bad = load_allowlist(allowlist)
    found = {(h.path, h.kind, h.key) for h in hits}
    out = [h.render() for h in hits if (h.path, h.kind, h.key) not in allowed]
    out += [
        f"{allowlist.name}: stale entry {p}:{k}:{key} (no such hit any more; remove it)"
        for p, k, key in sorted(allowed - found)
    ]
    return out + bad


def main(argv: Sequence[str] | None = None) -> int:
    skipped: list[str] = []
    problems = check(skipped=skipped)
    if skipped:
        print(
            f"prose guard: {len(skipped)} tracked file(s) not scanned (missing, symlink,"
            f" binary or over 2 MB): {', '.join(skipped[:10])}"
            + (" ..." if len(skipped) > 10 else ""),
            file=sys.stderr,
        )
    for p in problems:
        print(p)
    if problems:
        print(f"prose guard: {len(problems)} problem(s)", file=sys.stderr)
        return 1
    print("prose guard: clean", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
