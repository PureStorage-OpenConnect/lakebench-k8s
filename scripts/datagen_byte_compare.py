#!/usr/bin/env python3
"""Byte-compare datagen images on five fixed cases.

``capture --image <ref> --case <case>`` runs one case on a datagen image
against a local MinIO in a scratch directory, hashes every object it wrote,
and writes ``tests/fixtures/datagen_reference/<case>.json``. Before the first
case it proves the image: the image's generate binary is run with the argv of
the five in-tree cargo pins (``datagen_rs/tests/cycles.rs``), writing to a
local directory, and every digest must equal the pinned one. A mismatch
stops the capture: the image is not the generator the pins froze.

``compare --image <ref>`` runs all five cases on another image, compares
each object's key, size and sha256 with the reference manifests, and writes
``tests/fixtures/datagen_reference/compare-<digest12>.json``. It also
re-renders each case's argv from the current tree; a case whose rendered argv
differs from the recorded one fails, because production would then run
different arguments from the ones compared.

The cases (design ch05 section 4.1):

    F0  financial, seed 43, scale 1, 4 nodes
    F1  F0 plus --robustness-perturbation (seed 43 is a development seed)
    C0  customer 360, seed 42, scale 1, 4 nodes
    F2  F0 as 2 cycles (cycles 0 and 1)
    C2  C0 as 2 cycles, with the two windows the deployer gives

Argv is the Job argv Lakebench renders for a scale-1 config of that schema
(``DatagenDeployer._build_datagen_context`` through
``templates/datagen/job.yaml.j2``), with the per-cycle changes
``deploy_cycle`` makes. It is rendered once at capture and recorded in the
manifest; ``compare`` replays the recorded argv, so a later change to
Lakebench's rendering cannot move the reference (it is reported instead, see
above). Each node runs as one container with JOB_COMPLETION_INDEX set, as the
Indexed Job sets it, but the nodes run one after another, so cross-node
concurrency is not exercised. CPU_LIMIT is fixed, and the thread count each
node reports is recorded and compared.

Objects under ``_corpus/`` are excluded by path (OA10); the number excluded
is recorded per side, and the reference side must exclude none. The manifests
carry no endpoint, credential or bucket name (keys are relative to the
bucket): MinIO runs with random throwaway keys that are never written
anywhere. Peak disk is one case (about 10 GiB); the script checks for that
plus 20% before each case, and removes the MinIO container and data
directory on normal exit, an exception, Ctrl-C, SIGTERM or SIGHUP (not on
SIGKILL). Every container has a timeout.

Requires podman and boto3 on the host. Never run it against a real bucket:
it starts and talks to its own MinIO only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REF_DIR = ROOT / "tests" / "fixtures" / "datagen_reference"
CYCLES_RS = ROOT / "datagen_rs" / "tests" / "cycles.rs"
# The tracked held-out hash file, mounted into every generator container as a
# pod gets it from its ConfigMap. A generator from v1.7 on refuses the
# financial schema without it; an older image ignores it. It is a check input,
# not a corpus input, so it is not part of a case's recorded env.
HELDOUT_FILE = ROOT / "src" / "lakebench" / "spark" / "data" / "aml" / "heldout_hashes.json"
HELDOUT_MOUNT = "/etc/lakebench/heldout/heldout_hashes.json"
HELDOUT_ARGS = [
    "-v",
    f"{HELDOUT_FILE}:{HELDOUT_MOUNT}:ro,z",
    "-e",
    f"LB_HELDOUT_HASHES={HELDOUT_MOUNT}",
]
MINIO_IMAGE = (
    "docker.io/bitnamilegacy/minio@sha256:"
    "8935e75fa5d11295c17171e4aa49efe390a1193cd7f12e4d21b92af9ffef09d7"
)
BUCKET = "lbref"  # MinIO refuses bucket names under 3 characters
EXCLUDED = ("_corpus/",)
CASES = ("F0", "F1", "C0", "F2", "C2")
CPU_LIMIT = "8"
BINARY = "/app/datagen_rs"
GEN_TIMEOUT = 1800  # seconds per generator container
# Largest case at scale 1 (C0, 10.1 GiB in the v1.6 smoke ledger) plus 20%.
NEED_BYTES = int(10.1 * 1.2 * 2**30)
FORMAT = 1


# ---------------------------------------------------------------------------
# Manifests and comparison (pure; unit tested)
# ---------------------------------------------------------------------------


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def objects_sha256(objects: list[dict]) -> str:
    """sha256 over the canonical JSON of the sorted (key, size, sha256) list."""
    rows = sorted([o["key"], int(o["size"]), o["sha256"]] for o in objects)
    return hashlib.sha256(canonical(rows).encode()).hexdigest()


def excluded(key: str) -> bool:
    return any(key.startswith(p) or f"/{p}" in key for p in EXCLUDED)


def compare_objects(ref: list[dict], new: list[dict]) -> list[str]:
    """Every difference between two object lists, by key; [] when equal."""
    a = {o["key"]: (int(o["size"]), o["sha256"]) for o in ref}
    b = {o["key"]: (int(o["size"]), o["sha256"]) for o in new}
    out = [f"missing from the new image: {k}" for k in sorted(a.keys() - b.keys())]
    out += [f"only in the new image: {k}" for k in sorted(b.keys() - a.keys())]
    for k in sorted(a.keys() & b.keys()):
        if a[k][0] != b[k][0]:
            out.append(f"size differs: {k} ({a[k][0]} vs {b[k][0]})")
        elif a[k][1] != b[k][1]:
            out.append(f"sha256 differs: {k}")
    return out


def load_reference(case: str, ref_dir: Path = REF_DIR) -> dict:
    doc = json.loads((ref_dir / f"{case}.json").read_text())
    if doc.get("format") != FORMAT or doc.get("case") != case:
        raise ValueError(f"{case}.json: not a format-{FORMAT} manifest for case {case}")
    for k in ("image", "runs", "env", "seed_ref", "objects"):
        if k not in doc:
            raise ValueError(f"{case}.json: missing {k}")
    return doc


def compare_result(image_a: str, image_b: str, cases: dict[str, tuple[dict, dict]]) -> dict:
    """The compare-<digest12>.json document for the five cases.

    ``cases`` maps case -> (reference manifest, new run), where the new run
    holds ``objects``, ``threads``, ``excluded_objects`` and ``rendered_runs``
    (the argv the current tree renders for the case)."""
    out = []
    for case in CASES:
        ref, run = cases[case]
        new = run["objects"]
        diffs = compare_objects(ref["objects"], new)
        argv_ok = run["rendered_runs"] == ref["runs"]
        out.append(
            {
                "case": case,
                "seed_ref": ref["seed_ref"],
                "argv_canonical": ref["runs"],
                "digest_a": image_a,
                "digest_b": image_b,
                "objects": len(ref["objects"]),
                "sha256_a": objects_sha256(ref["objects"]),
                "sha256_b": objects_sha256(new),
                "equal": not diffs and argv_ok,
                "bytes_equal": not diffs,
                "argv_rendered_equal": argv_ok,
                "rendered_runs": None if argv_ok else run["rendered_runs"],
                "threads_a": ref.get("threads"),
                "threads_b": run["threads"],
                "threads_equal": ref.get("threads") == run["threads"],
                "differences": diffs[:50],
                "excluded": list(EXCLUDED),
                "excluded_objects_a": ref.get("excluded_objects", 0),
                "excluded_objects_b": run["excluded_objects"],
            }
        )
    return {"format": FORMAT, "image_a": image_a, "image_b": image_b, "cases": out}


# ---------------------------------------------------------------------------
# The cargo-pin digests (FNV-1a, as datagen_rs/tests/cycles.rs computes them)
# ---------------------------------------------------------------------------

_FNV_OFFSET, _FNV_PRIME, _M64 = 0xCBF29CE484222325, 0x100000001B3, (1 << 64) - 1


def _fnv(paths: list[str], root: Path) -> tuple[int, int]:
    h = _FNV_OFFSET
    for rel in paths:
        data = rel.encode() + (root / rel).read_bytes()
        for x in data:
            h = ((h ^ x) * _FNV_PRIME) & _M64
    return h, len(paths)


def _files(root: Path) -> list[str]:
    # The per-node marker (any ``_corpus`` directory) records build and time;
    # it is excluded by path, as cycles.rs's digests exclude it.
    return [
        str(p.relative_to(root))
        for p in root.rglob("*")
        if p.is_file() and "_corpus" not in p.relative_to(root).parts[:-1]
    ]


def tree_digest(root: Path) -> tuple[int, int]:
    """cycles.rs ``tree_digest``: relative paths sorted as strings."""
    return _fnv(sorted(_files(root)), root)


def c360_driver_digest(root: Path) -> tuple[int, int]:
    """cycles.rs ``c360_driver_digest``: paths sorted as PathBufs, which
    compares component by component, not as one string."""
    return _fnv(sorted(_files(root), key=lambda r: Path(r).parts), root)


# The five cargo pins: test function in cycles.rs, digest kind, and the trees
# it builds (each a list of argv run into one fresh directory).
def _fin(extra: list[str]) -> list[list[str]]:
    return [
        ["--bucket", "b", "--seed", "7777", "--scale", "0.02", "--threads", "2", "--mode", "all"]
        + ["--total-nodes", "2", "--node-id", n, "--file-size-mb", "1", "--delivery-mode", "batch"]
        + extra
        for n in ("0", "1")
    ]


def _c360(threads: str, extra: list[str]) -> list[str]:
    return [
        "--schema", "customer360", "--bucket", "b", "--seed", "43", "--target-tb", "0.00002",
        "--file-size-mb", "4", "--threads", threads, *extra,
    ]  # fmt: skip


PINS: dict[str, tuple[str, str, list[list[list[str]]]]] = {
    # c360_driver_digest(threads), run for threads 2 and 3: one tree each.
    "c360": ("fn c360_driver_output_is_pinned", "c360", [[_c360(t, [])] for t in ("2", "3")]),
    "financial": (
        "fn financial_output_is_pinned_to_the_frozen_generator",
        "tree",
        [_fin([])],
    ),
    "financial_perturbed": (
        "fn financial_perturbed_output_is_pinned",
        "tree",
        [_fin(["--robustness-perturbation"])],
    ),
    "financial_two_cycle": (
        "fn financial_two_cycle_output_is_pinned",
        "tree",
        [_fin(["--cycle", "0", "--cycles", "2"]) + _fin(["--cycle", "1", "--cycles", "2"])],
    ),
    "c360_two_cycle": (
        "fn c360_two_cycle_output_is_pinned",
        "tree",
        [
            [
                _c360(
                    "2",
                    ["--timestamp-start", a, "--timestamp-end", b, "--cycle", n, "--cycles", "2"],
                )
                for n, a, b in (
                    ("0", "2024-01-01", "2024-12-31"),
                    ("1", "2024-12-31", "2025-12-31"),
                )
            ]
        ],
    ),
}


def pinned_digests(src: str | None = None) -> dict[str, tuple[int, int]]:
    """The pinned (digest, files) pair of each cargo pin, read from cycles.rs."""
    text = src if src is not None else CYCLES_RS.read_text()
    out = {}
    for name, (needle, _kind, _trees) in PINS.items():
        body = text[text.index(needle) :]
        body = body[: body.find("\n#[test]") if "\n#[test]" in body else len(body)]
        # The assert_eq! whose expected value is a (digest, files) literal;
        # comments may quote older digests.
        m = re.search(r"assert_eq!\(\s*\w+\s*,\s*\(\s*([0-9_]+)\s*,\s*([0-9]+)\s*\)", body)
        if m is None:
            raise ValueError(f"cannot read the pinned digest of {needle}")
        out[name] = (int(m.group(1).replace("_", "")), int(m.group(2)))
    return out


# ---------------------------------------------------------------------------
# Rendering the case argv (once, at capture)
# ---------------------------------------------------------------------------


def _deployer(schema: str, seed: int):
    from types import SimpleNamespace

    from lakebench.config.schema import LakebenchConfig
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import TemplateRenderer

    cfg = LakebenchConfig(
        name="byte-compare",
        platform={"storage": {"s3": {"endpoint": "http://127.0.0.1:9000"}}},
        workload={"schema": schema, "datagen": {"scale": 1, "seed": seed}},
    )
    context = {
        "name": "byte-compare",
        "namespace": "byte-compare",
        "bucket_bronze": BUCKET,
        "s3_endpoint": "http://127.0.0.1:9000",
        "s3_region": "us-east-1",
        "s3_path_style": True,
        "s3_verify_ssl": False,
        "s3_ca_cert_pem": "",
    }
    eng = SimpleNamespace(config=cfg, k8s=None, renderer=TemplateRenderer(), context=context)
    return cfg, DatagenDeployer(eng), eng.renderer


def _rendered_args(renderer, context: dict) -> list[str]:
    import yaml

    from lakebench.deploy.datagen import DatagenDeployer

    for name in DatagenDeployer.TEMPLATES:
        doc = yaml.safe_load(renderer.render(name, context))
        if isinstance(doc, dict) and doc.get("kind") == "Job":
            return [str(a) for a in doc["spec"]["template"]["spec"]["containers"][0]["args"]]
    raise ValueError("the datagen templates render no Job")


def render_runs(case: str) -> list[list[str]]:
    """The argv of each generator invocation of ``case`` (one per cycle),
    without --node-id: each is run once per node."""
    schema, seed = ("financial", 43) if case.startswith("F") else ("customer360", 42)
    cfg, dep, renderer = _deployer(schema, seed)
    base = dep._build_datagen_context()
    if case in ("F0", "C0"):
        return [_rendered_args(renderer, base)]
    if case == "F1":
        return [_rendered_args(renderer, base) + ["--robustness-perturbation"]]
    # F2 and C2: the context changes deploy_cycle makes for cycle n of 2.
    dg = cfg.architecture.workload.datagen
    dims = cfg.get_scale_dimensions()
    runs = []
    for n in range(2):
        ctx = dict(base)
        ts_start, ts_end = dep._cycle_timestamp_range(n, 2, dg.timestamp_start, dg.timestamp_end)
        ctx.update(
            datagen_timestamp_start=ts_start,
            datagen_timestamp_end=ts_end,
            datagen_cycle=n,
            datagen_cycles=2,
            datagen_target_tb=f"{(dims.approx_bronze_gb / 2) / 1024.0:.6f}",
        )
        runs.append(_rendered_args(renderer, ctx))
    return runs


def nodes_of(argv: list[str]) -> int:
    return int(argv[argv.index("--total-nodes") + 1])


# ---------------------------------------------------------------------------
# podman, MinIO, hashing
# ---------------------------------------------------------------------------


def _podman(*args: str, check: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["podman", *args], check=check, capture_output=capture, text=True)


def image_digest(ref: str) -> str:
    """The digest the image is named by: the @sha256 in ``ref``. It is pulled
    by that digest (which fails unless the registry serves it), and it must be
    one of the local image's RepoDigests."""
    m = re.search(r"@(sha256:[0-9a-f]{64})$", ref)
    if m is None:
        raise SystemExit(f"--image must name a digest (<repo>@sha256:...); got {ref}")
    r = _podman("pull", "-q", ref, check=False)
    if r.returncode != 0:
        raise SystemExit(f"{ref}: the registry does not serve that digest: {r.stderr[-500:]}")
    digests = json.loads(
        _podman("image", "inspect", ref, "--format", "{{json .RepoDigests}}").stdout
    )
    if not any(d.endswith(m.group(1)) for d in digests):
        raise SystemExit(f"{ref}: the local image does not carry that digest")
    return m.group(1)


def _run_container(name: str, args: list[str], what: str) -> str:
    """``podman run --rm --name <name> <args>`` with a timeout; the container
    is force-removed whatever happens. Returns stdout."""
    try:
        r = subprocess.run(
            ["podman", "run", "--rm", "--name", name, *args],
            capture_output=True,
            text=True,
            timeout=GEN_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        raise SystemExit(f"{what}: no exit within {GEN_TIMEOUT} s") from None
    finally:
        _podman("rm", "-f", "--ignore", name, check=False)
    if r.returncode != 0:
        tail = "\n".join(r.stderr.splitlines()[-20:])
        raise SystemExit(f"{what} exited {r.returncode}:\n{tail}")
    return r.stdout


def _rm_tree(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    if path.exists():  # files owned by a container's uid
        _podman("unshare", "rm", "-rf", str(path), check=False)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Minio:
    """A throwaway MinIO on 127.0.0.1 with random keys and its own data dir."""

    def __init__(self, workdir: Path):
        self.data = Path(tempfile.mkdtemp(prefix="minio-", dir=workdir))
        os.chmod(self.data, 0o777)  # the bitnami image runs as uid 1001
        self.port = _free_port()
        self.name = f"lb-bytecmp-minio-{os.getpid()}-{self.port}"
        self.access = secrets.token_hex(12)
        self.secret = secrets.token_hex(24)

    def __enter__(self) -> Minio:
        try:
            _podman(
                "run", "-d", "--rm", "--name", self.name,
                "-p", f"127.0.0.1:{self.port}:9000",
                "-v", f"{self.data}:/bitnami/minio/data:Z",
                "-e", f"MINIO_ROOT_USER={self.access}",
                "-e", f"MINIO_ROOT_PASSWORD={self.secret}",
                "-e", f"MINIO_DEFAULT_BUCKETS={BUCKET}",
                MINIO_IMAGE,
            )  # fmt: skip
            deadline = time.time() + 120
            while time.time() < deadline:
                try:
                    self.client().head_bucket(Bucket=BUCKET)
                    return self
                except Exception:  # noqa: BLE001 -- not up yet
                    time.sleep(1)
            raise SystemExit("MinIO did not come up with the bucket within 120 s")
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *exc) -> None:
        _podman("rm", "-f", "--ignore", self.name, check=False)
        _rm_tree(self.data)

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def client(self):
        import boto3
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access,
            aws_secret_access_key=self.secret,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )

    def objects(self) -> tuple[list[dict], int]:
        """key, size and sha256 of every object, ``_corpus/`` excluded, and
        how many were excluded."""
        s3 = self.client()
        out = []
        skipped = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
            for o in page.get("Contents", []):
                if excluded(o["Key"]):
                    skipped += 1
                    continue
                h = hashlib.sha256()
                body = s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"]
                while chunk := body.read(8 << 20):
                    h.update(chunk)
                out.append({"key": o["Key"], "size": int(o["Size"]), "sha256": h.hexdigest()})
        return sorted(out, key=lambda o: o["key"]), skipped


_THREADS = re.compile(r"\[entrypoint\] .* threads=([0-9]+) ")


def run_generator(image: str, argv: list[str], node: int, minio: Minio, env: dict) -> int:
    """One node of one run; returns the thread count the entrypoint reports."""
    envs = {
        "S3_ENDPOINT": minio.endpoint,
        "AWS_REGION": "us-east-1",
        "S3_REGION": "us-east-1",
        "S3_PATH_STYLE": "true",
        "AWS_ACCESS_KEY_ID": minio.access,
        "AWS_SECRET_ACCESS_KEY": minio.secret,
        "JOB_COMPLETION_INDEX": str(node),
        **env,
    }
    flags = [x for k, v in envs.items() for x in ("-e", f"{k}={v}")]
    name = f"lb-bytecmp-gen-{os.getpid()}-{minio.port}-{node}"
    stdout = _run_container(
        name,
        ["--network", "host", *flags, *HELDOUT_ARGS, image, *argv],
        f"generator node {node} ({argv})",
    )
    m = _THREADS.search(stdout)
    if m is None:
        raise SystemExit(f"generator node {node}: the entrypoint reported no thread count")
    return int(m.group(1))


def run_case(image: str, runs: list[list[str]], workdir: Path, env: dict) -> dict:
    """Run every node of every run into a fresh MinIO; return the objects,
    the per-node thread counts and the number of excluded objects."""
    free = shutil.disk_usage(workdir).free
    if free < NEED_BYTES:
        raise SystemExit(
            f"{workdir}: {free / 2**30:.1f} GiB free, one case needs {NEED_BYTES / 2**30:.1f}"
        )
    threads = []
    with Minio(workdir) as minio:
        for argv in runs:
            threads.append(
                [run_generator(image, argv, n, minio, env) for n in range(nodes_of(argv))]
            )
        objects, skipped = minio.objects()
    if not objects:
        raise SystemExit("the case wrote no objects")
    return {"objects": objects, "threads": threads, "excluded_objects": skipped}


def prove_image(image: str, workdir: Path) -> dict:
    """Run every cargo pin's argv in the image (local sink, the binary
    directly) and require the pinned digests. Returns what was checked."""
    want = pinned_digests()
    got = {}
    for name, (_needle, kind, trees) in PINS.items():
        seen = []
        for i, runs in enumerate(trees):
            out = Path(tempfile.mkdtemp(prefix=f"pin-{name}-", dir=workdir))
            os.chmod(out, 0o777)
            try:
                for j, a in enumerate(runs):
                    _run_container(
                        f"lb-bytecmp-pin-{os.getpid()}-{name}-{i}-{j}".replace("_", "-"),
                        ["--entrypoint", BINARY, "-e", "DG_LOCAL_DIR=/out",
                         "-v", f"{out}:/out:Z", *HELDOUT_ARGS, image, *a],
                        f"pin {name}",
                    )  # fmt: skip
                seen.append(c360_driver_digest(out) if kind == "c360" else tree_digest(out))
            finally:
                _rm_tree(out)
        if any(d != want[name] for d in seen):
            raise SystemExit(
                f"the image is not the frozen generator: pin {name} gives {seen}, "
                f"cycles.rs pins {want[name]}. Stop and tell the owner."
            )
        got[name] = {"digest": want[name][0], "files": want[name][1], "trees": len(seen)}
    return got


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_capture(args) -> int:
    digest = image_digest(args.image)
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    proof = prove_image(args.image, workdir)
    print(f"image proven against the cargo pins: {proof}")
    cases = CASES if args.case == ["all"] else args.case
    REF_DIR.mkdir(parents=True, exist_ok=True)
    for case in cases:
        runs = render_runs(case)
        t0 = time.time()
        env = {"CPU_LIMIT": CPU_LIMIT}
        run = run_case(args.image, runs, workdir, env)
        if run["excluded_objects"]:
            raise SystemExit(f"{case}: the reference image wrote objects under {EXCLUDED}")
        objects = run["objects"]
        doc = {
            "format": FORMAT,
            "case": case,
            "image": digest,
            "seed_ref": "43" if case.startswith("F") else "42",
            "runs": runs,
            "nodes": nodes_of(runs[0]),
            "env": env,
            "threads": run["threads"],
            "excluded": list(EXCLUDED),
            "excluded_objects": 0,
            "pin_proof": proof,
            "objects": objects,
        }
        (REF_DIR / f"{case}.json").write_text(json.dumps(doc, indent=1) + "\n")
        size = sum(o["size"] for o in objects) / 2**30
        print(f"{case}: {len(objects)} objects, {size:.2f} GiB, {time.time() - t0:.0f} s")
    return 0


def cmd_compare(args) -> int:
    digest = image_digest(args.image)
    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    refs = {c: load_reference(c) for c in CASES}
    images = {r["image"] for r in refs.values()}
    if len(images) != 1:
        raise SystemExit(f"the reference manifests name more than one image: {sorted(images)}")
    results = {}
    for case in CASES:
        ref = refs[case]
        # Replay exactly what the reference ran (its argv and env; the
        # held-out hash file is mounted as on a pod and is no corpus input),
        # and record
        # what the current tree would render for the case.
        run = run_case(args.image, ref["runs"], workdir, ref["env"])
        run["rendered_runs"] = render_runs(case)
        results[case] = (ref, run)
        print(f"{case}: done")
    doc = compare_result(images.pop(), digest, results)
    out = REF_DIR / f"compare-{digest.split(':')[1][:12]}.json"
    out.write_text(json.dumps(doc, indent=1) + "\n")
    bad = [c["case"] for c in doc["cases"] if not c["equal"]]
    drift = [c["case"] for c in doc["cases"] if not c["threads_equal"]]
    if drift:
        print(f"note: thread counts differ from the reference in {drift} (see threads_a/b)")
    print(f"wrote {out}; " + (f"MISMATCH in {bad}" if bad else "all five cases equal"))
    return 1 if bad else 0


def _on_signal(signum, _frame) -> None:
    # SystemExit unwinds through the context managers, so MinIO and its data
    # directory are removed on SIGTERM and SIGHUP as on Ctrl-C.
    raise SystemExit(f"stopped by signal {signum}")


def main(argv: list[str] | None = None) -> int:
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, _on_signal)
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("capture", "compare"):
        p = sub.add_parser(name)
        p.add_argument("--image", required=True, help="<repo>@sha256:<digest>")
        p.add_argument(
            "--workdir",
            default=os.environ.get("TMPDIR", tempfile.gettempdir()),
            help="scratch directory for the MinIO data (needs about 12 GiB free)",
        )
        if name == "capture":
            p.add_argument("--case", action="append", required=True, choices=[*CASES, "all"])
    args = ap.parse_args(argv)
    return cmd_capture(args) if args.cmd == "capture" else cmd_compare(args)


if __name__ == "__main__":
    sys.exit(main())
