#!/usr/bin/env python3
"""Generate ``docs/exit-codes.md`` from ``ExitCode`` and ``PATHS``.

The table is rendered by ``lakebench.exit_codes.render_markdown``; this script
only writes or checks the file. ``tests/test_exit_codes.py`` fails when the
checked-in file differs from the render, so a hand edit is caught.

Usage:
    python scripts/gen_exit_codes.py           # rewrite docs/exit-codes.md
    python scripts/gen_exit_codes.py --check   # exit 1 if it is stale
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "exit-codes.md"
sys.path.insert(0, str(ROOT / "src"))

from lakebench.exit_codes import render_markdown  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="fail if the file is stale")
    args = parser.parse_args(argv)
    text = render_markdown()
    if args.check:
        current = DOC.read_text() if DOC.exists() else ""
        if current != text:
            print(f"{DOC.relative_to(ROOT)} is stale: run python scripts/gen_exit_codes.py")
            return 1
        return 0
    DOC.write_text(text)
    print(f"wrote {DOC.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
