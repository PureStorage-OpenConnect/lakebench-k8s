#!/usr/bin/env python3
"""Render docs/prerequisites.md from src/lakebench/deploy/prereqs.py, so the page and the checks cannot drift.

python scripts/gen_prereq_docs.py          # write the page
python scripts/gen_prereq_docs.py --check  # exit 1 if the page is stale
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakebench.deploy.prereqs import render_markdown  # noqa: E402

PAGE = ROOT / "docs" / "prerequisites.md"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="fail if the page is out of date")
    args = ap.parse_args(argv)
    text = render_markdown()
    if args.check:
        current = PAGE.read_text(encoding="utf-8") if PAGE.exists() else ""
        if current != text:
            print(f"{PAGE.relative_to(ROOT)} is stale; run scripts/gen_prereq_docs.py")
            return 1
        return 0
    PAGE.write_text(text, encoding="utf-8")
    print(f"wrote {PAGE.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
