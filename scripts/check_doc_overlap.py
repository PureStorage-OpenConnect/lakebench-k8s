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
  holds, unless the line links or backticks a corpus file that holds that
  fact (absolute paths into another checkout count by their repo-relative
  suffix). A fact token is a number with a unit (``300Gi``, ``36 cores``,
  ``1200 s``; unit spellings are folded, so ``2 hours`` is ``2 h``) or a
  backticked identifier containing a digit (``lb-datagen:1.6.0``).

``CHANGELOG.md`` is left out of the default corpus: it records past values
and owns none.

There is no allowlist: a line that has to repeat a fact is rewritten as a
pointer to the doc that owns it. Exit 1 on any report, 0 when there is none,
2 when the check cannot run (no file, an empty corpus, a corpus that is not
a git checkout).

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
#: History, not a home for facts: a past value there owns nothing.
DEFAULT_EXCLUDE = ("CHANGELOG.md",)

_FENCE = re.compile(r"^\s*(```|~~~)")
_LINK_TARGET = re.compile(r"\]\([^)]*\)")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
# Each spelling of a unit, mapped to one canonical form, so "300 GiB" and
# "300Gi", or "2 h" and "2 hours", are the same fact.
_UNIT_CANON = {
    "gib": "gi", "gi": "gi", "mib": "mi", "mi": "mi", "kib": "ki", "ki": "ki",
    "tib": "ti", "ti": "ti", "gb": "gb", "mb": "mb", "kb": "kb", "tb": "tb",
    "cores": "cores", "core": "cores", "vcpus": "vcpu", "vcpu": "vcpu",
    "ms": "ms", "min": "min", "mins": "min", "minute": "min", "minutes": "min",
    "h": "h", "hour": "h", "hours": "h", "s": "s", "sec": "s", "secs": "s",
    "second": "s", "seconds": "s", "d": "d", "day": "d", "days": "d",
    "pods": "pods", "pod": "pods", "executors": "executors", "executor": "executors",
    "rows": "rows", "row": "rows", "%": "%",
}  # fmt: skip
_UNITS = "|".join(sorted((re.escape(u) for u in _UNIT_CANON), key=len, reverse=True))
# A number (thousands commas and a decimal part allowed) and a unit, joined
# by nothing, one space or a hyphen; the unit must not run on into a word.
_UNIT_FACT = re.compile(
    rf"(?<![\w.])(\d{{1,3}}(?:,\d{{3}})+|\d+)(\.\d+)?[ -]?({_UNITS})(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_SPAN = re.compile(r"`([^`]+)`")
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


def _tokens(line: str) -> list[str]:
    return _NON_ALNUM.sub(" ", _LINK_TARGET.sub("]", line).lower()).split()


def _blocks(lines: Sequence[str]) -> list[list[int]]:
    """Paragraphs as lists of 1-based line numbers: a blank line or a code
    fence ends one, and a fence line belongs to none."""
    out: list[list[int]] = []
    cur: list[int] = []
    for n, raw in enumerate(lines, 1):
        if not raw.strip() or _FENCE.match(raw):
            if cur:
                out.append(cur)
            cur = []
            continue
        cur.append(n)
    if cur:
        out.append(cur)
    return out


def _shingles(lines: Sequence[str], block: list[int]) -> Iterable[tuple[tuple[str, ...], int]]:
    toks = [(n, t) for n in block for t in _tokens(lines[n - 1])]
    for i in range(len(toks) - SHINGLE + 1):
        yield tuple(t for _, t in toks[i : i + SHINGLE]), toks[i][0]


def _spans(lines: Sequence[str], block: list[int]) -> dict[int, list[str]]:
    """Backtick spans of a paragraph by the line they start on; a span may
    wrap onto the next line."""
    starts, text = [], ""
    for n in block:
        starts.append((len(text), n))
        text += lines[n - 1] + "\n"
    out: dict[int, list[str]] = {}
    for m in _SPAN.finditer(text):
        line = next(n for off, n in reversed(starts) if off <= m.start())
        out.setdefault(line, []).append(" ".join(m.group(1).split()))
    return out


def _unit_facts(line: str) -> list[str]:
    """Each number-with-unit, canonical: ``4,349 GB`` -> ``4349gb``, ``2 hours`` -> ``2h``."""
    return [
        m.group(1).replace(",", "") + (m.group(2) or "") + _UNIT_CANON[m.group(3).lower()]
        for m in _UNIT_FACT.finditer(line)
    ]


def _ident_pattern(ident: str) -> re.Pattern[str]:
    # Whole token: lb-datagen:1.6.0 is not found inside lb-datagen:1.6.0.1,
    # but a sentence's closing full stop is allowed after it.
    return re.compile(rf"(?<![\w.:/-]){re.escape(ident)}(?![\w:/-]|\.\w)", re.IGNORECASE)


def _clean_ref(ref: str) -> str:
    ref = ref.split("#", 1)[0]
    ref = re.sub(r":\d+(?:[-,]\d+)*$", "", ref)  # path:line or path:12-30
    ref = ref.strip().rstrip("/")
    while ref.startswith(("./", "../")):
        ref = ref.split("/", 1)[1]
    return ref


def _resolve(ref: str, corpus_paths: frozenset[str]) -> set[str]:
    """Corpus files a ref names: the path itself, or any path ending in it
    (an absolute path into another checkout of the same repository)."""
    ref = _clean_ref(ref)
    if not ref:
        return set()
    return {p for p in corpus_paths if ref == p or ref.endswith("/" + p)}


class _Corpus:
    def __init__(self, repo: Path, corpus: Sequence[str]):
        self.paths = frozenset(corpus)
        self.shingle_at: dict[tuple[str, ...], tuple[str, int]] = {}
        self.unit_at: dict[str, list[tuple[str, int]]] = {}
        self.lines: list[tuple[str, int, str]] = []
        for rel in corpus:
            path = repo / rel
            if not path.is_file():  # tracked but deleted in this worktree
                continue
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            for block in _blocks(lines):
                for sh, n in _shingles(lines, block):
                    self.shingle_at.setdefault(sh, (rel, n))
            for n, raw in enumerate(lines, 1):
                self.lines.append((rel, n, raw))
                for f in _unit_facts(raw):
                    self.unit_at.setdefault(f, []).append((rel, n))
        self._ident: dict[str, list[tuple[str, int]]] = {}

    def ident_at(self, ident: str) -> list[tuple[str, int]]:
        if ident not in self._ident:
            pat = _ident_pattern(ident)
            self._ident[ident] = [(rel, n) for rel, n, text in self.lines if pat.search(text)]
        return self._ident[ident]


def check(file: Path, repo: Path, corpus: Sequence[str]) -> list[Report]:
    """Reports for FILE against the corpus (repo-relative paths, FILE excluded)."""
    c = _Corpus(repo, corpus)
    lines = file.read_text(encoding="utf-8", errors="replace").splitlines()
    reports: list[Report] = []
    spans: dict[int, list[str]] = {}
    for block in _blocks(lines):
        for sh, n in _shingles(lines, block):
            if sh in c.shingle_at:
                rel, cn = c.shingle_at[sh]
                reports.append(Report(n, rel, cn, "text", " ".join(sh)))
                break  # one report per paragraph
        spans.update(_spans(lines, block))

    for n, raw in enumerate(lines, 1):
        line_spans = spans.get(n, [])
        facts = [(f, c.unit_at.get(f, [])) for f in _unit_facts(raw)]
        facts += [(s, c.ident_at(s)) for s in line_spans if re.search(r"\d", s)]
        refs: set[str] = set()
        for ref in [m.group(1) for m in _LINK.finditer(raw)] + line_spans:
            refs |= _resolve(ref, c.paths)
        for fact, where in facts:
            # A pointer excuses a fact only when it names a file that holds it.
            if where and not refs & {rel for rel, _ in where}:
                rel, cn = where[0]
                reports.append(Report(n, rel, cn, "fact", fact))
                break  # one fact report per line
    return sorted(reports, key=lambda r: (r.line, r.kind))


def corpus_files(repo: Path, globs: Sequence[str] | None) -> list[str]:
    """Repo-relative corpus paths: the ``--corpus`` globs, or the tracked
    ``*.md`` except ``DEFAULT_EXCLUDE``."""
    if globs:
        found: set[str] = set()
        for g in globs:
            found.update(p.relative_to(repo).as_posix() for p in repo.glob(g) if p.is_file())
        return sorted(found)
    res = subprocess.run(
        ["git", "ls-files", "-z", "*.md"], cwd=repo, capture_output=True, text=True, check=True
    )
    return sorted(p for p in res.stdout.split("\0") if p and p not in DEFAULT_EXCLUDE)


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
    try:
        corpus = corpus_files(args.repo, args.corpus)
        try:
            self_rel = args.file.resolve().relative_to(args.repo.resolve()).as_posix()
        except ValueError:
            self_rel = None
        corpus = [p for p in corpus if p != self_rel]
        if not corpus:
            print("empty corpus: nothing to compare against", file=sys.stderr)
            return 2
        reports = check(args.file, args.repo, corpus)
    except (OSError, ValueError, NotImplementedError, subprocess.CalledProcessError) as exc:
        # Exit 1 means "overlap found"; a check that could not run is 2.
        print(f"cannot run the check: {exc}", file=sys.stderr)
        return 2
    for r in reports:
        print(r.render(str(args.file)))
    print(f"{len(reports)} overlap(s) against {len(corpus)} corpus file(s)", file=sys.stderr)
    return 1 if reports else 0


if __name__ == "__main__":
    sys.exit(main())
