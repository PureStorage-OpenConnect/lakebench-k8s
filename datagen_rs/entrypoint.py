#!/usr/bin/env python3
"""Entrypoint for the Rust financial datagen image.

Thin launcher: the Rust binary now writes bronze + party + account + manifest
directly to S3 via rust-s3, so this shell holds no state and never touches the
local filesystem for parquet output. It only:
  1. Detects the pod's CPU quota from cgroups so the rayon pool sizes correctly.
  2. Resolves --node-id from JOB_COMPLETION_INDEX for K8s Indexed Jobs.
  3. Exec's the Rust binary with the passed-through args and env.

S3 credentials + endpoint are read by the Rust binary directly from env vars:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT, AWS_REGION.
"""

from __future__ import annotations

import argparse
import os
import sys


def detect_cpu_quota() -> int:
    """Cores available to this container from the cgroup CFS quota, so the Rust
    pool matches the allotment instead of the host core count. Falls back to
    the logical CPU count. Honors an explicit RAYON_NUM_THREADS / CPU_LIMIT
    override."""
    for env in ("CPU_LIMIT", "RAYON_NUM_THREADS"):
        v = os.environ.get(env)
        if v:
            try:
                return max(1, int(float(v)))
            except ValueError:
                pass
    # cgroup v2
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota, period = f.read().split()
            if quota != "max":
                return max(1, round(int(quota) / int(period)))
    except (OSError, ValueError):
        pass
    # cgroup v1
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota = int(f.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period = int(f.read())
        if quota > 0:
            return max(1, round(quota / period))
    except (OSError, ValueError):
        pass
    return os.cpu_count() or 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", default="financial")
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--corpus-months", type=int, default=60)
    ap.add_argument("--file-size-mb", type=int, default=32)
    ap.add_argument("--bucket", default=os.environ.get("BRONZE_BUCKET", "fraud-aml-uat-bronze"))
    ap.add_argument("--prefix", default=os.environ.get("PREFIX", "datagen-v2-rs"))
    ap.add_argument("--node-id", type=int, default=None)
    ap.add_argument("--total-nodes", type=int, default=1)
    ap.add_argument("--mode", default="all", choices=["all", "bronze", "reference"])
    args = ap.parse_args()

    if args.schema != "financial":
        print(f"schema {args.schema!r} is served by the Python image, not this one", file=sys.stderr)
        return 2

    node_id = args.node_id
    if node_id is None:
        node_id = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))

    threads = detect_cpu_quota()
    cmd = [
        "/app/datagen_rs",
        "--bucket", args.bucket,
        "--prefix", args.prefix,
        "--scale", str(args.scale),
        "--seed", str(args.seed),
        "--corpus-months", str(args.corpus_months),
        "--file-size-mb", str(args.file_size_mb),
        "--node-id", str(node_id),
        "--total-nodes", str(args.total_nodes),
        "--threads", str(threads),
        "--mode", args.mode,
    ]
    print(
        f"[entrypoint] node {node_id}/{args.total_nodes} mode={args.mode} "
        f"scale={args.scale} threads={threads} -> s3://{args.bucket}/{args.prefix}",
        flush=True,
    )
    # execvp replaces this process, so the Rust binary is PID 1 of the pod and
    # signals reach it directly. No shell layer, no upload state to drain.
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    sys.exit(main())
