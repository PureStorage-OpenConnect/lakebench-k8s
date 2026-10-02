#!/usr/bin/env python3
"""The AML parity job ran exactly the registered guards, and each passed.

``pytest tests/spark -m aml_parity`` with ``LB_TEST_OUTCOMES=<file>`` writes
every test's outcome by node id (tests/spark/conftest.py). This compares
that with ``tests/spark/aml_parity_guards.txt``: a guard that is missing,
skipped, xfailed, failed or errored, or an extra test, fails the job, and
so does a non-zero pytest exit code. A parity "success" therefore never
means that nothing ran.

Usage:
    python scripts/check_parity_job.py OUTCOMES.json --pytest-rc N
    python scripts/check_parity_job.py --junit-no-skips REPORT.xml

The second form fails a JUnit report with any skipped, failed or errored
test, or with no tests at all (the frozen guard's own tests must run on the
guard's Python, 3.11, and must not skip there).
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

GUARDS = Path(__file__).resolve().parents[1] / "tests" / "spark" / "aml_parity_guards.txt"


def expected(path: Path = GUARDS) -> list[str]:
    lines = [ln.strip() for ln in path.read_text().splitlines()]
    return [ln for ln in lines if ln and not ln.startswith("#")]


def problems(outcomes: dict[str, str], guards: list[str], pytest_rc: int) -> list[str]:
    out = []
    if pytest_rc != 0:
        out.append(f"pytest exited {pytest_rc}")
    if not guards:
        out.append("no guards registered")
    for g in guards:
        got = outcomes.get(g)
        if got is None:
            out.append(f"{g}: did not run")
        elif got != "passed":
            out.append(f"{g}: {got}")
    for nodeid in sorted(set(outcomes) - set(guards)):
        out.append(f"{nodeid}: ran but is not a registered guard (add it to {GUARDS.name})")
    return out


def junit_problems(report: Path) -> list[str]:
    try:
        root = ET.parse(report).getroot()
    except (OSError, ET.ParseError) as e:
        return [f"cannot read {report}: {e}"]
    cases = list(root.iter("testcase"))
    out = [] if cases else [f"{report}: no tests ran"]
    for case in cases:
        for child in case:
            if child.tag in ("skipped", "failure", "error"):
                out.append(f"{case.get('classname')}::{case.get('name')}: {child.tag}")
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("outcomes", type=Path, nargs="?")
    parser.add_argument("--pytest-rc", type=int)
    parser.add_argument("--junit-no-skips", type=Path)
    args = parser.parse_args(argv)
    if args.junit_no_skips is not None:
        found = junit_problems(args.junit_no_skips)
        for p in found:
            print(f"FAIL {p}", file=sys.stderr)
        if not found:
            print("OK no skipped, failed or errored tests")
        return 1 if found else 0
    if args.outcomes is None or args.pytest_rc is None:
        parser.error("OUTCOMES and --pytest-rc are required")
    try:
        outcomes = json.loads(args.outcomes.read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"FAIL cannot read {args.outcomes}: {e}", file=sys.stderr)
        return 1
    found = problems(outcomes, expected(), args.pytest_rc)
    for p in found:
        print(f"FAIL {p}", file=sys.stderr)
    if found:
        return 1
    print(f"OK all {len(expected())} AML parity guards passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
