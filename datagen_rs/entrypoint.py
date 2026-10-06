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
import math
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


# Peak-memory model, mirrored from lakebench.config.autosizer (a unit test keeps
# the two copies equal; see autosizer.py for the measurement basis):
#   peak = BASE + GIB_PER_SCALE * scale + max(0, threads - 8) * GIB_PER_EXTRA_THREAD
# at the fixed 64 MB file size, for the busiest pod. A limit below the 8-thread
# peak drops one thread per GIB_PER_EXTRA_THREAD short (user override only).
BASE_GIB = {"financial": 5.35, "customer360": 2.2}
GIB_PER_SCALE = {"financial": 0.0087, "customer360": 0.0007}
GIB_PER_EXTRA_THREAD = {"financial": 0.5, "customer360": 0.1875}
BASE_THREADS = 8
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


def max_threads_for_memory(schema: str, scale: float, limit_bytes: int) -> int:
    """Largest thread count whose modelled peak fits the memory limit."""
    base = BASE_GIB.get(schema, BASE_GIB["financial"])
    per_scale = GIB_PER_SCALE.get(schema, GIB_PER_SCALE["financial"])
    extra = GIB_PER_EXTRA_THREAD.get(schema, GIB_PER_EXTRA_THREAD["financial"])
    spare = limit_bytes / 2**30 / HEADROOM - base - per_scale * scale
    if spare >= 0:
        return BASE_THREADS + int(spare // extra)
    # Below the 8-thread peak (only a user memory override gets here: the
    # autosized request always fits 8 threads). Drop one thread per per-thread
    # cost short; the pod logs the cut.
    return max(1, BASE_THREADS - math.ceil(-spare / extra))


BINARY = "/app/datagen_rs"


def main() -> int:
    # `<image> --version` is passed straight to the generator before any
    # parsing (only as the first argument, so a value is never read as it).
    if sys.argv[1:2] == ["--version"]:
        os.execvp(BINARY, [BINARY, "--version"])
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
    # Default 64 matches DatagenConfig.file_size ("64mb"), the template's
    # default(64), and the Rust binary's default (aligned 2026-09-28), so
    # raw-CLI reproducers and pod runs pick the same file size when the
    # flag is omitted. The historical pre-M6 default was 32 for the
    # financial K8s YAMLs, before deploy/datagen.py started rendering
    # --file-size-mb from config unconditionally.
    ap.add_argument("--file-size-mb", type=int, default=64)
    ap.add_argument("--bucket", default=os.environ.get("BRONZE_BUCKET", ""))
    ap.add_argument("--prefix", default=os.environ.get("PREFIX", ""))
    ap.add_argument("--node-id", type=int, default=None)
    ap.add_argument("--total-nodes", type=int, default=1)
    # --scale is required on the financial path (drives typology counts + world
    # size). For c360 it is only accepted for back-compat with old templates
    # that render it unconditionally; --customer-id-max wins when both are set
    # and a stderr note names the drop. Default is None so we can tell "user
    # supplied 1.0" from "not set".
    ap.add_argument("--scale", type=float, default=None)
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
    # Delivery mode (Wave 2 D3, 2026-09-28): how the datagen writes bronze
    # files to S3. `batch` (default) buffers the whole file then does one
    # PUT; `continuous` streams parquet row-groups through S3 multipart as
    # they close. Corpus content is byte-for-byte identical at a fixed seed;
    # only the write pipeline differs. Forwarded to the Rust binary as
    # --delivery-mode. `auto` resolves to `continuous` (the K8s template
    # default), matching PipelineMode.CONTINUOUS naming (owner D18).
    ap.add_argument(
        "--delivery-mode",
        default="auto",
        choices=["auto", "batch", "continuous"],
    )
    # customer360-only args -- ignored on the financial path.
    ap.add_argument("--target-tb", type=float, default=0.1)
    # None: the Rust binary derives the id space from --scale (100K
    # customers per scale unit). It used to default to 500K at every scale.
    ap.add_argument("--customer-id-max", type=int, default=None)
    # --payload-kb was a CLI knob that had only ever been calibrated at 2 KiB;
    # the Rust binary refused any other value, and no shipped template passed
    # anything else. Dropped 2026-09-28. Accepted here as
    # a silently-ignored back-compat arg so older K8s Job templates still parse.
    ap.add_argument("--payload-kb", type=int, default=None)
    ap.add_argument("--dirty-ratio", type=float, default=0.08)
    ap.add_argument("--duplicate-email-pct", type=float, default=0.10)
    ap.add_argument("--timestamp-start", default="2024-01-01")
    ap.add_argument("--timestamp-end", default="2025-01-01")
    # `--workers` is what the lakebench K8s Job template passes today. Accept
    # it as an alias for `--threads` so the Rust rayon pool sizes correctly
    # even if the template hasn't been updated to pass --threads explicitly.
    ap.add_argument("--workers", type=int, default=None)
    # Parse exactly as for a Job, then let the generator print the resolved
    # corpus arguments (canonical JSON on stdout) instead of generating.
    ap.add_argument("--print-resolved-args", action="store_true")
    # Strict: an unknown flag exits 2 (argparse), so a typo or a flag from a
    # newer Lakebench never runs as a silent default. --payload-kb stays
    # declared above because a v1.6 template may still pass it.
    args = ap.parse_args()

    if args.schema not in SUPPORTED_SCHEMAS:
        print(
            f"[entrypoint] --schema must be one of {SUPPORTED_SCHEMAS}; got {args.schema!r}",
            file=sys.stderr,
        )
        return 2
    # A registered (held-out) corpus's seed arrives in LB_DATAGEN_SEED from
    # a Secret, never in the Job's args. It is passed on in the environment
    # (the Rust generator reads it), so it is in no command line either; it
    # is never printed. Given twice, or not an integer, is refused.
    env_seed = os.environ.get("LB_DATAGEN_SEED")
    seed_from_env = False
    if env_seed is not None:
        if args.seed is not None:
            print(
                "[entrypoint] the seed is given both as --seed and in LB_DATAGEN_SEED; "
                "pass it once",
                file=sys.stderr,
            )
            return 2
        if args.schema != "financial":
            print(
                "[entrypoint] LB_DATAGEN_SEED applies to the financial schema only", file=sys.stderr
            )
            return 2
        try:
            if int(env_seed.strip()) < 0:
                raise ValueError
        except ValueError:
            print(
                "[entrypoint] LB_DATAGEN_SEED is not a non-negative integer (value not shown)",
                file=sys.stderr,
            )
            return 2
        seed_from_env = True
    if args.seed is None and not seed_from_env:
        if args.schema == "financial":
            print(
                "[entrypoint] --seed is required for the financial schema (AML seeds are "
                "pre-registered; see corpora in aml_preregistration.json)",
                file=sys.stderr,
            )
            return 2
        args.seed = 42

    # --scale default. `None` from argparse means "not user-set". Financial
    # needs a scale (drives typology counts, world size); c360 uses it only if
    # --customer-id-max is also absent, and if both are set we warn and use
    # --customer-id-max. Fill in 1.0 as the historical default so downstream
    # (memory cap, forwarding) has a concrete number, and record whether the
    # user supplied it explicitly for the c360 "both set" note.
    scale_user_set = args.scale is not None
    if args.scale is None:
        args.scale = 1.0
    if args.schema == "customer360" and scale_user_set and args.customer_id_max is not None:
        print(
            f"[entrypoint] c360: both --scale ({args.scale}) and --customer-id-max "
            f"({args.customer_id_max}) were set; --customer-id-max wins, --scale is "
            "recorded in provenance only. (Template renders one; a raw-CLI caller "
            "hit both.)",
            file=sys.stderr,
        )

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
        # c360 is sized by its customer count (100,000 per scale unit); the
        # template passes --customer-id-max, not --scale, for c360.
        cap_scale = args.scale
        if args.schema == "customer360" and args.customer_id_max:
            cap_scale = args.customer_id_max / 100_000
        cap = max_threads_for_memory(args.schema, cap_scale, limit)
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
        BINARY,
        "--schema",
        args.schema,
        "--bucket",
        args.bucket,
        *([] if seed_from_env else ["--seed", str(args.seed)]),
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
    # Delivery-mode resolution and forwarding (Wave 2 D3, 2026-09-28). `auto`
    # picks continuous unconditionally; there is no scale threshold today
    # because the choice affects per-worker RSS, not correctness. If a future
    # scale-based split is needed, it goes here.
    delivery = args.delivery_mode
    if delivery == "auto":
        delivery = "continuous"
    # Always forwarded: the Rust default is continuous, so dropping
    # "batch" here silently ran every batch request as continuous.
    common += ["--delivery-mode", delivery]

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
        # --payload-kb dropped 2026-09-28; Rust hardcodes 2.
        # Any --payload-kb from an older template arrives on args.payload_kb
        # (default None here) and is silently discarded when we do not forward it.
        cmd = common + [
            "--target-tb",
            str(args.target_tb),
            *(
                ["--customer-id-max", str(args.customer_id_max)]
                if args.customer_id_max
                else ["--scale", str(args.scale)]
            ),
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
            f"payload_kb=2(fixed) dirty_ratio={args.dirty_ratio} "
            f"ts=[{args.timestamp_start},{args.timestamp_end})"
        )

    if args.print_resolved_args:
        cmd.append("--print-resolved-args")
    print(
        f"[entrypoint] schema={args.schema} node {node_id}/{args.total_nodes} "
        f"threads={threads} cycle={args.cycle} -> s3://{args.bucket}/{args.prefix} :: {summary}",
        # stdout carries only the generator's JSON under --print-resolved-args.
        file=sys.stderr if args.print_resolved_args else sys.stdout,
        flush=True,
    )
    # execvp replaces this process, so the Rust binary is PID 1 of the pod and
    # signals reach it directly. No shell layer, no upload state to drain.
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    sys.exit(main())
