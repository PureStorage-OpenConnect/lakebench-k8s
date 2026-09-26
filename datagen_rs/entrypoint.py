#!/usr/bin/env python3
"""Entrypoint for the Rust datagen image.

Thin launcher: the Rust binary writes directly to S3 (rust-s3), so this shell
holds no state and never touches the local filesystem for parquet output. It
only:
  1. Detects the pod's CPU quota from cgroups so the rayon pool sizes correctly.
  2. Resolves --node-id from JOB_COMPLETION_INDEX for K8s Indexed Jobs.
  3. Builds a schema-conditional argv and execs the Rust binary.

Two schemas are supported today:
  - financial   -> pacs.008 + party + account + manifest
  - customer360 -> single-table customer interaction event stream

S3 credentials + endpoint are read by the Rust binary directly from env vars:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_ENDPOINT, AWS_REGION.
"""

from __future__ import annotations

import argparse
import os
import sys


SUPPORTED_SCHEMAS = ("financial", "customer360")


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


# Peak-RSS model, mirrored from lakebench.config.autosizer (a unit test keeps
# the two copies equal): per worker thread about MULT x the output file size;
# node 0 on the financial path also builds the full world at about 575 B per
# entity (111,111 entities per scale unit).
PER_THREAD_FILE_MULTIPLIER = {"financial": 4.8, "customer360": 3.0}
WORLD_BYTES_PER_ENTITY_NODE0 = 650
ENTITIES_PER_SCALE = 111_111
BASE_GIB = {"financial": 1.7, "customer360": 0.3}
HEADROOM = 1.25


def detect_memory_limit_bytes() -> int | None:
    """The container's memory limit from cgroup v2 or v1, or None."""
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path) as f:
                raw = f.read().strip()
            if raw and raw != "max":
                v = int(raw)
                if v < 1 << 60:  # v1 reports "unlimited" as a huge number
                    return v
        except (OSError, ValueError):
            pass
    return None


def max_threads_for_memory(
    schema: str, scale: float, file_size_mb: int, is_node0: bool, limit_bytes: int
) -> int:
    """Largest thread count whose estimated peak RSS fits the memory limit."""
    gib = limit_bytes / 2**30
    world = 0.0
    if schema == "financial" and is_node0:
        world = ENTITIES_PER_SCALE * scale * WORLD_BYTES_PER_ENTITY_NODE0 / 2**30
    per_thread = (file_size_mb / 1024.0) * PER_THREAD_FILE_MULTIPLIER.get(schema, 3.0)
    budget = gib / HEADROOM - world - BASE_GIB.get(schema, 0.3)
    return max(1, int(budget // per_thread)) if per_thread > 0 else 1


def main() -> int:
    # No prefix matching: an abbreviation such as --rob must not turn on
    # --robustness-perturbation (or any other flag).
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--schema", default="financial", choices=SUPPORTED_SCHEMAS)
    # Shared args -- both schemas consume these.
    # No default for financial: AML seeds are pre-registered and 42 is spent
    # (the Rust driver refuses spent seeds too). c360 keeps 42.
    ap.add_argument("--seed", type=int, default=None)
    # Robustness corpus (financial only): forwarded to the Rust driver, which
    # applies corpora.robustness_perturbation (datagen_rs/src/robustness.rs).
    ap.add_argument("--robustness-perturbation", action="store_true")
    # Multi-cycle runs (datagen_rs/src/cycle.rs). AML: cycle n of --cycles N
    # emits the one-shot corpus rows in calendar-mass slice [n/N, (n+1)/N),
    # so the union of all cycles is the one-shot corpus. c360: cycle n > 0
    # shifts the per-file streams. Keys are cycle-suffixed for n > 0, so
    # bronze accumulates. The defaults (0 of 1) are a single run.
    ap.add_argument("--cycle", type=int, default=0)
    ap.add_argument("--cycles", type=int, default=1)
    # NOTE: default is 32 to match the pre-M6 entrypoint (existing financial
    # K8s Job YAMLs assume 32). c360 K8s Job templates that want a different
    # file size pass --file-size-mb explicitly.
    ap.add_argument("--file-size-mb", type=int, default=32)
    ap.add_argument("--bucket", default=os.environ.get("BRONZE_BUCKET", ""))
    ap.add_argument("--prefix", default=os.environ.get("PREFIX", ""))
    ap.add_argument("--node-id", type=int, default=None)
    ap.add_argument("--total-nodes", type=int, default=1)
    # Financial-only args -- ignored on the customer360 path.
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--corpus-months", type=int, default=60)
    # `batch` accepted as a synonym for `all` because the K8s Job template
    # in `src/lakebench/templates/datagen/job.yaml.j2` unconditionally
    # passes `--mode batch` for both schemas; the Rust financial driver
    # only accepts all/bronze/reference. Mapping keeps existing K8s
    # templates working without a template + entrypoint rev at the same
    # time. On the customer360 path --mode is dropped entirely.
    # `continuous` is what lakebench passes for continuous pipelines (and the
    # autosizer's `auto` mode resolves to above scale 10). The generator
    # always pre-writes the corpus, so it maps to `all` like `batch`.
    # Rejecting it crash-looped every scale > 10 generate.
    ap.add_argument(
        "--mode",
        default="all",
        choices=["all", "bronze", "reference", "batch", "continuous"],
    )
    # customer360-only args -- ignored on the financial path.
    ap.add_argument("--target-tb", type=float, default=0.1)
    # None: the Rust binary derives the id space from --scale (100K
    # customers per scale unit). It used to default to 500K at every scale.
    ap.add_argument("--customer-id-max", type=int, default=None)
    ap.add_argument("--payload-kb", type=int, default=2)
    ap.add_argument("--dirty-ratio", type=float, default=0.08)
    ap.add_argument("--duplicate-email-pct", type=float, default=0.10)
    ap.add_argument("--timestamp-start", default="2024-01-01")
    ap.add_argument("--timestamp-end", default="2025-01-01")
    # `--workers` is what the lakebench K8s Job template passes today. Accept
    # it as an alias for `--threads` so the Rust rayon pool sizes correctly
    # even if the template hasn't been updated to pass --threads explicitly.
    ap.add_argument("--workers", type=int, default=None)
    # Ignore any other args silently (e.g. --payload-kb=0 which some templates
    # pass): argparse handles unknown args by erroring, so we let it.
    args, unknown = ap.parse_known_args()
    if unknown:
        print(f"[entrypoint] ignoring unknown args: {unknown}", file=sys.stderr)

    if args.schema not in SUPPORTED_SCHEMAS:
        print(
            f"[entrypoint] --schema must be one of {SUPPORTED_SCHEMAS}; got {args.schema!r}",
            file=sys.stderr,
        )
        return 2
    if args.seed is None:
        if args.schema == "financial":
            print(
                "[entrypoint] --seed is required for the financial schema (AML seeds are "
                "pre-registered; see corpora in aml_preregistration.json)",
                file=sys.stderr,
            )
            return 2
        args.seed = 42

    if not args.bucket:
        print("[entrypoint] --bucket is required (or set BRONZE_BUCKET env)", file=sys.stderr)
        return 2

    node_id = args.node_id
    if node_id is None:
        node_id = int(os.environ.get("JOB_COMPLETION_INDEX", "0"))

    threads = detect_cpu_quota()
    if args.workers and args.workers > 0:
        # An explicit, user-chosen --workers overrides the detected CPU count.
        # lakebench passes 0 ("auto") unless the config sets
        # datagen.generators, so by default threads follow the pod CPU.
        threads = args.workers

    # Never start more threads than the memory limit holds: each thread holds
    # a full output file several times over, and an OOMKill restarts the pod
    # from scratch. Fewer threads is slower but finishes.
    limit = detect_memory_limit_bytes()
    if limit:
        is_node0 = node_id == 0 and args.mode in ("all", "batch", "continuous", "reference")
        cap = max_threads_for_memory(args.schema, args.scale, args.file_size_mb, is_node0, limit)
        if threads > cap:
            print(
                f"[entrypoint] capping threads {threads} -> {cap} to fit memory limit "
                f"{limit / 2**30:.1f} GiB (file_size_mb={args.file_size_mb})",
                file=sys.stderr,
            )
            threads = cap

    # Schema-conditional argv. --schema first so the Rust binary can dispatch
    # before parsing the shared args.
    common = [
        "/app/datagen_rs",
        "--schema",
        args.schema,
        "--bucket",
        args.bucket,
        "--seed",
        str(args.seed),
        "--file-size-mb",
        str(args.file_size_mb),
        "--node-id",
        str(node_id),
        "--total-nodes",
        str(args.total_nodes),
        "--threads",
        str(threads),
    ]
    # --prefix: only forward when explicitly set. Forwarding empty overrides
    # the Rust binary's schema-appropriate default (e.g. c360's
    # "customer/interactions/" default), and would silently write files at
    # bucket root, breaking Silver's read path.
    if args.prefix:
        common += ["--prefix", args.prefix]
    if args.cycles < 1 or not 0 <= args.cycle < args.cycles:
        print(
            f"[entrypoint] need 0 <= --cycle < --cycles; got {args.cycle} of {args.cycles}",
            file=sys.stderr,
        )
        return 2
    # Forwarded only when not the defaults, so a single-run argv is unchanged.
    if args.cycle:
        common += ["--cycle", str(args.cycle)]
    if args.cycles != 1:
        common += ["--cycles", str(args.cycles)]

    if args.robustness_perturbation and args.schema != "financial":
        print(
            "[entrypoint] --robustness-perturbation applies to the financial schema only",
            file=sys.stderr,
        )
        return 2

    if args.schema == "financial":
        # Rust driver only knows all/bronze/reference. Map the K8s
        # template's `batch` alias to `all`.
        rust_mode = "all" if args.mode in ("batch", "continuous") else args.mode
        cmd = common + [
            "--scale",
            str(args.scale),
            "--corpus-months",
            str(args.corpus_months),
            "--mode",
            rust_mode,
        ]
        # Forwarded only when set, so a default argv is unchanged.
        if args.robustness_perturbation:
            cmd.append("--robustness-perturbation")
        summary = f"scale={args.scale} corpus_months={args.corpus_months} mode={rust_mode}"
        if args.robustness_perturbation:
            summary += " robustness_perturbation=on"
    else:  # customer360
        cmd = common + [
            "--target-tb",
            str(args.target_tb),
            *(
                ["--customer-id-max", str(args.customer_id_max)]
                if args.customer_id_max
                else ["--scale", str(args.scale)]
            ),
            "--payload-kb",
            str(args.payload_kb),
            "--dirty-ratio",
            str(args.dirty_ratio),
            "--duplicate-email-pct",
            str(args.duplicate_email_pct),
            "--timestamp-start",
            args.timestamp_start,
            "--timestamp-end",
            args.timestamp_end,
        ]
        summary = (
            f"target_tb={args.target_tb} "
            f"customer_id_max={args.customer_id_max or f'scale({args.scale})'} "
            f"payload_kb={args.payload_kb} dirty_ratio={args.dirty_ratio} "
            f"ts=[{args.timestamp_start},{args.timestamp_end})"
        )

    print(
        f"[entrypoint] schema={args.schema} node {node_id}/{args.total_nodes} "
        f"threads={threads} cycle={args.cycle} -> s3://{args.bucket}/{args.prefix} :: {summary}",
        flush=True,
    )
    # execvp replaces this process, so the Rust binary is PID 1 of the pod and
    # signals reach it directly. No shell layer, no upload state to drain.
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    sys.exit(main())
