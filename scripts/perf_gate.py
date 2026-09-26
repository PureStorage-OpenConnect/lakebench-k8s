#!/usr/bin/env python3
"""Performance-regression gate CLI (GOALS P3.1, P4.2, P9.6).

Usage:
    python scripts/perf_gate.py status
    python scripts/perf_gate.py compare c360-batch-s10 --run 20260930-101500-abc123
    python scripts/perf_gate.py record  c360-batch-s10 --run <run id or metrics.json> --git-sha <sha>
    python scripts/perf_gate.py seed    [--write]
    python scripts/perf_gate.py gate    [--run NAME=RUN ...]  (also searches uat/perf/)

Runs are looked up by id under --runs-dir (default lakebench-output/runs) and
then uat/perf/, or given as a path to metrics.json. Nothing here touches Kubernetes or S3.

Exit codes: 0 pass; 1 regression; 2 refused, no baseline, or bad input.
See docs/perf-regression-gate.md.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lakebench.metrics import perf_gate as pg  # noqa: E402

DEFAULT_STORE = ROOT / "benchmarks" / "perf" / "baselines.yaml"
DEFAULT_RUNS = ROOT / "lakebench-output" / "runs"


def _head_sha() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def cmd_status(store: pg.BaselineStore, args: argparse.Namespace) -> int:
    for name, b in store.baselines.items():
        req = "required" if b.required else "optional"
        if b.accepted:
            print(
                f"{name:22} {req:8} accepted  run {b.run_id}  sha {b.git_sha}  {len(b.metrics)} metrics"
            )
        else:
            print(f"{name:22} {req:8} {b.status}")
    return 0


def cmd_compare(store: pg.BaselineStore, args: argparse.Namespace) -> int:
    run = pg.load_run(args.run, [args.runs_dir, ROOT / "uat" / "perf"])
    c = pg.compare_run(store, args.name, run)
    print(pg.format_comparison(c))
    return {pg.PASS: 0, pg.REGRESSION: 1}.get(c.verdict, 2)


def cmd_record(store: pg.BaselineStore, args: argparse.Namespace) -> int:
    run = pg.load_run(args.run, [args.runs_dir, ROOT / "uat" / "perf"])
    sha = args.git_sha
    if not sha:
        print(
            "error: --git-sha is required: metrics.json does not record the commit a run "
            f"came from (HEAD here is {_head_sha()})",
            file=sys.stderr,
        )
        return 2
    b = pg.record_baseline(store, args.name, run, sha, replace=args.replace)
    store.save()
    print(f"recorded {args.name}: run {b.run_id}, sha {b.git_sha}, {len(b.metrics)} metrics")
    return 0


def cmd_seed(store: pg.BaselineStore, args: argparse.Namespace) -> int:
    changed = False
    for name, b in store.baselines.items():
        if b.accepted:
            print(f"{name}: already accepted (run {b.run_id})")
            continue
        pinned = store.pinned(name)
        good, near = pg.find_runs_for(pinned, args.runs_dir)
        # A run whose pre-benchmark maintenance stopped early has no clean
        # post-maintenance QpH; it can never be a baseline.
        stopped = [r for r in good if pg.post_qph_unmeasured(r.scores)]
        good = [r for r in good if not pg.post_qph_unmeasured(r.scores)]
        for r in stopped:
            print(f"{name}: skipping {r.run_id} ({pg.post_qph_unmeasured(r.scores)})")
        if good:
            print(f"{name}: {len(good)} matching run(s), newest {good[0].run_id}")
            if args.write:
                for cand in good:
                    try:
                        pg.record_baseline(store, name, cand, "unrecorded")
                    except pg.PerfGateError as e:
                        # One config's bad candidate must not stop the seed.
                        print(f"  {cand.run_id} not usable: {e}")
                        continue
                    changed = True
                    print(f"  recorded {cand.run_id} (git sha unrecorded: metrics.json has none)")
                    break
            continue
        print(f"{name}: pending first run ({len(near)} run(s) of the same workload/scale/mode)")
        for run, reasons in near[: args.show]:
            print(f"  {run.run_id}: {reasons[0][:300]}")
            for r in reasons[1:3]:
                print(f"      also: {r[:200]}")
    if changed:
        store.save()
    return 0


def cmd_gate(store: pg.BaselineStore, args: argparse.Namespace) -> int:
    runs = {}
    for item in args.run or []:
        if "=" not in item:
            print(f"error: --run expects NAME=RUN, got {item!r}", file=sys.stderr)
            return 2
        name, ref = item.split("=", 1)
        runs[name] = ref
    passed, lines = pg.release_check(store, [args.runs_dir, ROOT / "uat" / "perf"], runs)
    print("\n".join(lines))
    return 0 if passed else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--store", type=Path, default=DEFAULT_STORE)
    parser.add_argument("--runs-dir", type=Path, default=DEFAULT_RUNS)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    p = sub.add_parser("compare")
    p.add_argument("name")
    p.add_argument("--run", required=True)
    p = sub.add_parser("record")
    p.add_argument("name")
    p.add_argument("--run", required=True)
    p.add_argument("--git-sha", help="commit the run was produced from")
    p.add_argument("--replace", action="store_true")
    p = sub.add_parser("seed")
    p.add_argument("--write", action="store_true", help="record matching runs")
    p.add_argument("--show", type=int, default=3, help="near misses to show per config")
    p = sub.add_parser("gate")
    p.add_argument("--run", action="append", metavar="NAME=RUN")
    args = parser.parse_args(argv)

    try:
        store = pg.load_store(args.store)
        return {
            "status": cmd_status,
            "compare": cmd_compare,
            "record": cmd_record,
            "seed": cmd_seed,
            "gate": cmd_gate,
        }[args.cmd](store, args)
    except pg.PerfGateError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
