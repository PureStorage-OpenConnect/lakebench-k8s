#!/usr/bin/env python3
"""Regenerate, or check, every generated block and page in docs/.

One command for the release step ``generated-docs`` (RELEASING.md). It
generates nothing itself; it runs, in order:

- the support-state blocks (``lakebench.config.support.regenerate_docs``);
- the configuration reference (``scripts/gen_config_reference.py``);
- the CLI reference (``scripts/gen_cli_reference.py``);
- the sizing tables (``scripts/gen_sizing_tables.py``);
- the exit-code page (``scripts/gen_exit_codes.py``);
- the prerequisites page (``scripts/gen_prereq_docs.py``).

Usage:
    python3.11 scripts/gen_docs.py           # rewrite every stale block
    python3.11 scripts/gen_docs.py --check   # write nothing; exit 1 if any is stale

``--check`` runs every generator even after one fails, so one run lists all
the stale pages.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: The script generators, in the order they run. Each takes ``--check``.
SCRIPTS = (
    "gen_config_reference.py",
    "gen_cli_reference.py",
    "gen_sizing_tables.py",
    "gen_exit_codes.py",
    "gen_prereq_docs.py",
)


def _support(check: bool) -> bool:
    """The support-state blocks; True when they are current (or were rewritten)."""
    sys.path.insert(0, str(ROOT / "src"))
    from lakebench.config import support

    if not Path(support.__file__).resolve().is_relative_to(ROOT / "src"):
        print(
            f"gen_docs: lakebench is imported from {support.__file__}, not {ROOT / 'src'}",
            file=sys.stderr,
        )
        return False
    if not check:
        for rel in support.regenerate_docs(ROOT):
            print(f"updated {rel}")
        return True
    stale = []
    for rel, names in support.DOCS_WITH_BLOCKS.items():
        text = (ROOT / rel).read_text()
        for name in names:
            if support.block_in(text, name) != support.expected_block(name):
                stale.append(f"{rel}: generated block {name!r} is stale")
    for line in stale:
        print(line, file=sys.stderr)
    if stale:
        print(f"Run: PYTHONPATH=src python3.11 -m lakebench.config.support {ROOT}", file=sys.stderr)
    return not stale


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if any block is stale")
    args = ap.parse_args(argv)
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    try:
        support_ok = _support(args.check)
    except Exception as e:  # noqa: BLE001 -- report it and run the other generators
        print(f"gen_docs: support blocks: {e}", file=sys.stderr)
        support_ok = False
    failed = [] if support_ok else ["support"]
    for script in SCRIPTS:
        cmd = [sys.executable, str(ROOT / "scripts" / script)] + (["--check"] if args.check else [])
        if subprocess.run(cmd, cwd=ROOT, env=env, check=False).returncode != 0:
            failed.append(script)
    if failed:
        print(f"gen_docs: stale or failed: {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
