#!/usr/bin/env python3
"""Fail when ``reports/generator.py`` prints a bare headline number.

A capped run's throughput or QpH is not what the system can do; the report
labels it ``BOUNDED BY: <cap>`` via ``reports/formatter.format_measurement``.
A raw f-string like ``{qph:,.1f}`` or ``{throughput:,.0f} rows/s`` embedded
into a card value bypasses that formatter and paints a capped figure as
headline evidence (invariant 6).

The lint scans ``card-value`` lines in ``src/lakebench/reports/generator.py``
and fails if it finds a bare number that a formatter should have rendered.
An allowlist covers headline lines that are not run-derived scores
(``Scale Ratio``, ``Time to Value``, plain durations, in-memory CPU-hours).

Usage:
    python3 scripts/lint_capped_bare_numbers.py [<path-to-generator.py>]
Returns 1 on any offending line, 0 when clean.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_DEFAULT_TARGET = Path("src/lakebench/reports/generator.py")

# Substrings that flag a bare run-derived headline the formatter should own.
# Every one of these appeared bare on a card-value line and is what the
# formatter now renders. If a new expression is added, the lint catches it.
_BARE_PATTERNS = (
    re.compile(r"\{[^}]*qph[^}]*:[^}]*\}"),
    re.compile(r"\{[^}]*throughput_rps[^}]*:[^}]*\}"),
    re.compile(r"\{[^}]*rows_per_s[^}]*:[^}]*\}"),
    re.compile(r"\{[^}]*throughput_rows_per_second[^}]*:[^}]*\}"),
    re.compile(r"\{[^}]*pipeline_throughput_gb_per_second[^}]*:[^}]*\}"),
    re.compile(r"\{[^}]*sustained_throughput_rps[^}]*:[^}]*\}"),
)

# Lines that legitimately hold one of the patterns above but do not render
# a headline card value: the formatter builder itself, an assignment feeding
# it, a hint2 caption, a table cell whose column already labels it (compute
# efficiency, executor count, etc.).
_ALLOW_CONTEXT = (
    "format_measurement(",
    "throughput_display",
    "qph_display",
    "throughput_raw",
    "qph_raw",
    "card-hint",
    "card-hint2",
    "queries/min",
    "rows/hr",
    "GB/core-hr",
    "extrapolates to",
    "> higher is better",
    "compute_efficiency_gb_per_core_hour",
    "compute_summary",
    "compute_efficiency",
    "core-hour",
)


def _line_is_card_value(line: str) -> bool:
    """A line embedding a card-value f-string carries the class marker."""
    return "card-value" in line


def scan(path: Path) -> list[str]:
    """Return one message per offending line in *path*."""
    lines = path.read_text().splitlines()
    offences: list[str] = []
    for i, line in enumerate(lines, start=1):
        if not _line_is_card_value(line):
            continue
        if any(ctx in line for ctx in _ALLOW_CONTEXT):
            continue
        for pat in _BARE_PATTERNS:
            if pat.search(line):
                offences.append(
                    f"{path}:{i}: bare headline number in card-value; "
                    "wrap in reports.formatter.format_measurement so a capped "
                    "run carries its BOUNDED BY tag."
                )
                break
    return offences


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    target = Path(argv[0]) if argv else _DEFAULT_TARGET
    if not target.exists():
        print(f"lint: target not found: {target}", file=sys.stderr)
        return 1
    offences = scan(target)
    for msg in offences:
        print(msg, file=sys.stderr)
    if offences:
        print(f"lint: {len(offences)} bare headline number(s) found", file=sys.stderr)
        return 1
    print(f"lint: {target} clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
