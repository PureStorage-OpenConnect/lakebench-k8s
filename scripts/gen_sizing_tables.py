#!/usr/bin/env python3
"""Regenerate the sizing tables in README.md and docs/getting-started.md.

The tables are the minimum cluster per workload x mode x scale, computed
by ``lakebench.config.sizing.plan_requirements`` (the one sizing source
``plan``, ``info``, ``config show``, ``recommend`` and the capacity
preflight use). Each sits between ``<!-- BEGIN GENERATED: sizing-... -->``
and ``<!-- END GENERATED: sizing-... -->`` markers.

Usage:
    python3.11 scripts/gen_sizing_tables.py           # rewrite the blocks
    python3.11 scripts/gen_sizing_tables.py --check   # exit 1 on drift

``--check`` writes nothing; ``tests/test_sizing_tables_drift.py`` runs the
same comparison in the unit suite.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from lakebench.config.sizing import DOCS_WITH_BLOCKS, block_in, expected_block  # noqa: E402


def drift(root: Path = REPO_ROOT) -> list[str]:
    """One line per block that is missing or differs from the code."""
    problems = []
    for rel, names in DOCS_WITH_BLOCKS.items():
        text = (root / rel).read_text()
        for name in names:
            found = block_in(text, name)
            if found is None:
                problems.append(f"{rel}: block {name!r} not found (markers missing)")
            elif found != expected_block(name):
                problems.append(f"{rel}: block {name!r} is stale")
    return problems


def regenerate(root: Path = REPO_ROOT) -> list[str]:
    """Rewrite every block in place; returns the files changed.

    A file whose markers are missing is an error, not an insertion: where
    a table goes is a docs decision, made once by hand.
    """
    changed = []
    for rel, names in DOCS_WITH_BLOCKS.items():
        path = root / rel
        text = path.read_text()
        new = text
        for name in names:
            found = block_in(new, name)
            if found is None:
                raise SystemExit(f"{rel}: block {name!r} not found; add its markers first")
            new = new.replace(found, expected_block(name))
        if new != text:
            path.write_text(new)
            changed.append(rel)
    return changed


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if any block is stale")
    args = ap.parse_args(argv)
    if args.check:
        problems = drift()
        for p in problems:
            print(p, file=sys.stderr)
        if problems:
            print("Run: python3.11 scripts/gen_sizing_tables.py", file=sys.stderr)
            return 1
        return 0
    for rel in regenerate():
        print(f"updated {rel}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
