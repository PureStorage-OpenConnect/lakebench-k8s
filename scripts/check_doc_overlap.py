#!/usr/bin/env python3
"""Report text in one file that is also stated in the tracked docs.

An agent-instructions file that is not tracked should hold operating rules
and point at the tracked docs for facts; a fact kept in two places drifts.
This check reads FILE and a corpus (every ``git ls-files '*.md'`` file, or
the files ``--corpus GLOB`` matches under ``--repo``) and reports:

- **shared text**: a paragraph of FILE sharing a run of 10 normalised
  tokens with a corpus file (lowercase, markdown link targets and markup
  dropped, every non-alphanumeric character a space), as
  ``FILE:line -> corpus:line``;
- **shared fact**: a line of FILE holding a fact token that the corpus also
  holds, unless the line is a pointer (it links or backticks a corpus path).
  A fact token is a number with a unit (``300Gi``, ``36 cores``, ``1200 s``)
  or a backticked identifier containing a digit (``lb-datagen:1.6.0``).

There is no allowlist: a line that has to repeat a fact is rewritten as a
pointer to the doc that owns it. Exit 1 on any report, 0 when there is none,
2 on a usage error.

Usage:
    python scripts/check_doc_overlap.py FILE [--repo DIR] [--corpus GLOB ...]
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import NamedTuple

ROOT = Path(__file__).resolve().parents[1]
SHINGLE = 10

_FENCE = re.compile(r"^\s*(```|~~~)")
_LINK_TARGET = re.compile(r"\]\([^)]*\)")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_UNITS = (
    "gib|mib|kib|tib|gi|mi|ki|ti|gb|mb|kb|tb|cores|core|vcpus|vcpu|"
    "ms|min|mins|minutes|hours|hour|h|s|sec|seconds|days|day|"
    "pods|executors|rows|%"
)
# A number (thousands commas and a decimal part allowed) and a unit, with at
# most one space between them; the unit must not run on into a word.
_UNIT_FACT = re.compile(
    rf"(?<![\w.])(\d{{1,3}}(?:,\d{{3}})+|\d+)(\.\d+)?\s?({_UNITS})(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_BACKTICK = re.compile(r"`([^`\n]+)`")
_LINK = re.compile(r"\]\(([^)\s]+)")


class Report(NamedTuple):
    line: int
    corpus: str
    corpus_line: int
    kind: str  # "text" or "fact"
    detail: str

    def render(self, name: str) -> str:
        return (
            f"{name}:{self.line} -> {self.corpus}:{self.corpus_line} ({self.kind}: {self.detail})"
        )


def _strip_markdown(line: str) -> str:
    return _LINK_TARGET.sub("]", line)


def _tokens(line: str) -> list[str]:
    return _NON_ALNUM.sub(" ", _strip_markdown(line).lower()).split()


def _paragraphs(lines: Sequence[str]) -> list[list[tuple[int, str]]]:
    """Blocks of (line number, token): blank lines and code fences end one."""
    out: list[list[tuple[int, str]]] = []
    cur: list[tuple[int, str]] = []
    for n, raw in enumerate(lines, 1):
        if not raw.strip() or _FENCE.match(raw):
            if cur:
                out.append(cur)
            cur = []
            continue
        cur.extend((n, t) for t in _tokens(raw))
    if cur:
        out.append(cur)
    return out


def _shingles(par: list[tuple[int, str]]) -> Iterable[tuple[tuple[str, ...], int]]:
    for i in range(len(par) - SHINGLE + 1):
        yield tuple(t for _, t in par[i : i + SHINGLE]), par[i][0]


def _unit_facts(line: str) -> list[str]:
    """Each number-with-unit, normalised: ``4,349 GB`` -> ``4349gb``."""
    return [
        (m.group(1).replace(",", "") + (m.group(2) or "") + m.group(3)).lower()
        for m in _UNIT_FACT.finditer(line)
    ]


def _ident_facts(line: str) -> list[str]:
    return [m.group(1).strip() for m in _BACKTICK.finditer(line) if re.search(r"\d", m.group(1))]


def _clean_ref(ref: str) -> str:
    ref = ref.split("#", 1)[0]
    ref = re.sub(r":\d+(?:[-,]\d+)*$", "", ref)  # path:line or path:12-30
    ref = ref.strip().rstrip("/")
    while ref.startswith(("./", "../")):
        ref = ref.split("/", 1)[1]
    return ref


def _is_pointer(line: str, corpus_paths: frozenset[str]) -> bool:
    """The line links or backticks a corpus path (absolute paths count by suffix)."""
    refs = [m.group(1) for m in _LINK.finditer(line)] + [
        m.group(1) for m in _BACKTICK.finditer(line)
    ]
    for ref in refs:
        ref = _clean_ref(ref)
        if not ref:
            continue
        for path in corpus_paths:
            if ref == path or ref.endswith("/" + path):
                return True
    return False


def corpus_files(repo: Path, globs: Sequence[str] | None) -> list[str]:
    """Repo-relative corpus paths: the ``--corpus`` globs, or the tracked ``*.md``."""
    if globs:
        found: set[str] = set()
        for g in globs:
            found.update(p.relative_to(repo).as_posix() for p in repo.glob(g) if p.is_file())
        return sorted(found)
    res = subprocess.run(
        ["git", "ls-files", "-z", "*.md"], cwd=repo, capture_output=True, text=True, check=True
    )
    return sorted(p for p in res.stdout.split("\0") if p)


def check(file: Path, repo: Path, corpus: Sequence[str]) -> list[Report]:
    try:
        self_rel = file.resolve().relative_to(repo.resolve()).as_posix()
    except ValueError:
        self_rel = None
    corpus = [c for c in corpus if c != self_rel]
    corpus_paths = frozenset(corpus)

    shingle_at: dict[tuple[str, ...], tuple[str, int]] = {}
    unit_at: dict[str, tuple[str, int]] = {}
    corpus_lines: list[tuple[str, int, str]] = []
    for rel in corpus:
        lines = (repo / rel).read_text(encoding="utf-8", errors="replace").splitlines()
        for par in _paragraphs(lines):
            for sh, n in _shingles(par):
                shingle_at.setdefault(sh, (rel, n))
        for n, raw in enumerate(lines, 1):
            corpus_lines.append((rel, n, raw))
            for f in _unit_facts(raw):
                unit_at.setdefault(f, (rel, n))

    lines = file.read_text(encoding="utf-8", errors="replace").splitlines()
    reports: list[Report] = []
    for par in _paragraphs(lines):
        for sh, n in _shingles(par):
            if sh in shingle_at:
                rel, cn = shingle_at[sh]
                reports.append(Report(n, rel, cn, "text", " ".join(sh)))
                break  # one report per paragraph

    ident_cache: dict[str, tuple[str, int] | None] = {}
    for n, raw in enumerate(lines, 1):
        facts = [(f, unit_at.get(f)) for f in _unit_facts(raw)]
        for ident in _ident_facts(raw):
            if ident not in ident_cache:
                pat = re.compile(
                    rf"(?<![A-Za-z0-9]){re.escape(ident)}(?![A-Za-z0-9])", re.IGNORECASE
                )
                ident_cache[ident] = next(
                    ((rel, cn) for rel, cn, text in corpus_lines if pat.search(text)), None
                )
            facts.append((ident, ident_cache[ident]))
        hits = [(f, at) for f, at in facts if at is not None]
        if hits and not _is_pointer(raw, corpus_paths):
            f, (rel, cn) = hits[0]
            reports.append(Report(n, rel, cn, "fact", f))
    return sorted(reports, key=lambda r: (r.line, r.kind))


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("file", type=Path)
    ap.add_argument(
        "--repo", type=Path, default=ROOT, help="corpus root (default: this repository)"
    )
    ap.add_argument(
        "--corpus", action="append", metavar="GLOB", help="corpus glob under --repo (repeatable)"
    )
    args = ap.parse_args(argv)
    if not args.file.is_file():
        print(f"{args.file}: no such file", file=sys.stderr)
        return 2
    corpus = corpus_files(args.repo, args.corpus)
    if not corpus:
        print("empty corpus: nothing to compare against", file=sys.stderr)
        return 2
    reports = check(args.file, args.repo, corpus)
    for r in reports:
        print(r.render(str(args.file)))
    print(f"{len(reports)} overlap(s) against {len(corpus)} corpus file(s)", file=sys.stderr)
    return 1 if reports else 0


if __name__ == "__main__":
    sys.exit(main())
