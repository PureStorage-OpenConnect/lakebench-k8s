#!/usr/bin/env python3
"""Per-file line-coverage floors for the code that decides benchmark results.

``pytest --cov-fail-under`` only checks the total, which lets a scoring file
lose its tests while unrelated code gains some. This reads a coverage JSON
report (``pytest --cov-report=json:<path>``) and fails if any listed file is
below its floor.

Floors are the measured values rounded down: raise them when coverage
improves, never lower them; a change that lowers a number adds tests. Each suite has its own floors because
the files are exercised by different jobs: detection_rules.py needs PySpark
and runs only under tests/spark; the rest run in the plain unit suite.

Measured 2026-09-24 on lane/c-hardening:
    unit   aml/reference_score.py   96.50%  (138/143)
    unit   metrics/collector.py     91.70%  (641/699)
    unit   reports/scorecard.py     90.52%  (105/116)
    spark  spark/scripts/detection_rules.py  44.30%  (105/237)

Measured 2026-09-25 on lane/q-release-hygiene (floors raised; detection_rules
floored a point lower because CI skips the Iceberg-jar Spark tests):
    unit   metrics/collector.py     93.56%
    unit   reports/scorecard.py     93.68%
    spark  spark/scripts/detection_rules.py  91.33%

Measured 2026-10-01 on CI run 36901023453 (Python 3.13, coverage 7.13.2,
the coverage-unit artifact; src/ identical to the v1.7 kickoff commit
46cc3f4). Existing floors re-read and raised where higher; five new ones:
    unit   aml/reference_score.py        96.50%  (138/143)
    unit   metrics/collector.py          94.41%  (1132/1199)  raised to 94
    unit   reports/scorecard.py          96.43%  (216/224)    raised to 96
    unit   deploy/destroy.py             79.94%  (1004/1256)  new
    unit   deploy/ownership.py           85.53%  (337/394)    new
    unit   s3/client.py                  69.86%  (204/292)    new
    unit   metrics/experiment.py         93.89%  (430/458)    new
    unit   metrics/c360_correctness.py   62.46%  (213/341)    new
The tests marked slow (the AML statistics job) import none of these modules,
directly or through what they import, so deselecting them from the unit legs
cannot lower any of these numbers and no separate slow-suite floors exist.

Usage:
    python scripts/check_coverage.py --suite unit coverage-unit.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

FLOORS: dict[str, dict[str, float]] = {
    "unit": {
        "lakebench/aml/reference_score.py": 96.0,
        "lakebench/deploy/destroy.py": 79.0,
        "lakebench/deploy/ownership.py": 85.0,
        "lakebench/metrics/c360_correctness.py": 62.0,
        "lakebench/metrics/collector.py": 94.0,
        "lakebench/metrics/experiment.py": 93.0,
        "lakebench/reports/scorecard.py": 96.0,
        "lakebench/s3/client.py": 69.0,
    },
    "spark": {
        "lakebench/spark/scripts/detection_rules.py": 90.0,
    },
}


def _find(files: dict, suffix: str) -> dict | None:
    for path, data in files.items():
        if path.replace("\\", "/").endswith(suffix):
            return data
    return None


def check(report: dict, floors: dict[str, float]) -> tuple[list[str], list[str]]:
    """Return (failures, lines) for a coverage JSON report against floors."""
    files = report.get("files", {})
    failures: list[str] = []
    lines: list[str] = []
    for suffix, floor in sorted(floors.items()):
        data = _find(files, suffix)
        if data is None:
            failures.append(f"{suffix}: not in the coverage report (not imported by the suite?)")
            continue
        pct = float(data["summary"]["percent_covered"])
        status = "ok" if pct >= floor else "BELOW FLOOR"
        lines.append(f"{status:12} {suffix}  {pct:.2f}% (floor {floor:.0f}%)")
        if pct < floor:
            failures.append(f"{suffix}: {pct:.2f}% is below the {floor:.0f}% floor")
    return failures, lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check per-file coverage floors.")
    parser.add_argument("--suite", choices=sorted(FLOORS), required=True)
    parser.add_argument("report", type=Path, help="coverage JSON report")
    args = parser.parse_args(argv)
    failures, lines = check(json.loads(args.report.read_text()), FLOORS[args.suite])
    for line in lines:
        print(line)
    for f in failures:
        print(f"FAIL: {f}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
