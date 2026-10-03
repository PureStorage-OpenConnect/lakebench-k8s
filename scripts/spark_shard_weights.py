#!/usr/bin/env python3
"""Write tests/spark/shard_weights.json, the recorded seconds per Spark test
file that ``--lb-shard`` balances on (tests/spark/conftest.py shard_files).

Reads JUnit XML reports of Spark-tier runs (each CI Spark job keeps its
report as the ``spark-junit-*`` artifact), sums each file's test times per
report (setup, call and teardown, so the module's Spark session start is
counted) and records the largest over the reports that ran the file: the
slowest job sets the tier's wall time, so a file weighs what it costs on the
Spark line where it is slowest. On the eight reports of run 37153895161 the
split from the largest put 2067 s in the slowest shard and the split from
the mean 2190 s (a check made offline, not by a CI run). One weight per file
serves both Spark lines, so a line whose files are faster stays uneven.

The weights only balance the shards: a stale or missing entry makes one
shard slower, never drops a test, because every file is in exactly one
shard whatever its weight. Refresh them when the shard times in a CI run
drift apart (the step summary records each job's time).

Usage:
    python scripts/spark_shard_weights.py --source "<run id, date>" junit/*.xml
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "tests" / "spark" / "shard_weights.json"


def file_of(classname: str, root: Path = ROOT) -> str | None:
    """The repository path of the file a JUnit ``classname`` names
    (``tests.spark.test_x`` or ``tests.spark.test_x.TestClass``), or None."""
    parts = classname.split(".")
    for end in range(len(parts), 0, -1):
        rel = "/".join(parts[:end]) + ".py"
        if (root / rel).is_file():
            return rel
    return None


def file_seconds(report: Path, root: Path = ROOT) -> dict[str, float]:
    """Seconds per test file in one JUnit report."""
    out: dict[str, float] = defaultdict(float)
    for case in ET.parse(report).getroot().iter("testcase"):
        rel = file_of(case.get("classname", ""), root)
        if rel is not None:
            out[rel] += float(case.get("time") or 0.0)
    return dict(out)


def weights(reports: list[Path], root: Path = ROOT) -> dict[str, float]:
    """Largest seconds per file over the reports that ran it."""
    runs: dict[str, list[float]] = defaultdict(list)
    for report in reports:
        for rel, secs in file_seconds(report, root).items():
            runs[rel].append(secs)
    return {rel: round(max(v), 1) for rel, v in sorted(runs.items())}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("reports", nargs="+", type=Path, help="JUnit XML reports")
    parser.add_argument("--source", required=True, help="where the reports came from")
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args(argv)
    seconds = weights(args.reports)
    if not seconds:
        print("no Spark test files in the reports", file=sys.stderr)
        return 1
    doc = {
        "_comment": "Seconds per Spark test file; balances --lb-shard only. "
        "Refresh with scripts/spark_shard_weights.py.",
        "source": args.source,
        "seconds": seconds,
    }
    args.out.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"{args.out}: {len(seconds)} files, {sum(seconds.values()):.0f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
