#!/usr/bin/env python3
"""Fail when a built wheel, an sdist or the rendered script ConfigMaps ship
what a published package must not.

It reads the built artifacts, not the build configuration, so an exclude
that silently stops excluding is caught. Checks, over every wheel and
sdist in ``--dist`` (or built fresh with ``--build``) and over the
ConfigMaps rendered offline from the wheel's own files:

- **names**: no member under ``docs/internal/``, the maintainers' local
  artifacts directory, an agent-instructions file at any depth, anything
  named like a credentials file, or a ``.claude/`` directory;
- **content**, on every text member: no FlashBlade or AWS access key id, no
  PEM private key, nothing the custom rules of ``.gitleaks.toml`` match
  (with its entropy limits and allowlists), and then ``gitleaks dir`` over
  the extracted members when gitleaks is on ``PATH`` (SKIP when it is not).
  A hit prints the member and line, never the value;
- **held-out seeds**, when ``spark/data/aml/heldout_hashes.json`` exists: the
  absence check of ``lakebench.config.datagen_seed`` over every text member.
  The hash file's own ``absence_check`` sets what a hit is: ``report`` prints
  it as PENDING-OA5, ``enforce`` makes it a FAIL. Without the file the check
  is SKIP.

Exit 1 on any FAIL; with ``--require-all`` also on any SKIP or PENDING-OA5.

Usage:
    python scripts/package_guard.py --dist dist
    python scripts/package_guard.py --build [--require-all]
"""

from __future__ import annotations

import argparse
import math
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, NamedTuple

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10; tomli is in the dev extra there
    import tomli as tomllib  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
GITLEAKS_CONFIG = ROOT / ".gitleaks.toml"
HELDOUT_HASHES = SRC / "lakebench" / "spark" / "data" / "aml" / "heldout_hashes.json"
SNIFF_BYTES = 8192

PASS, FAIL, SKIP, PENDING = "PASS", "FAIL", "SKIP", "PENDING-OA5"

# Spelled so that no tracked line names the local-only paths themselves.
NAME_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("docs/internal", re.compile(r"(^|/)docs/internal/")),
    ("maintainer artifacts", re.compile(r"(^|/)dev[-]artifacts(/|$)")),
    ("agent instructions", re.compile(r"(^|/)CLAUDE\.md$", re.I)),
    ("credentials file", re.compile(r"CREDENTIALS", re.I)),
    ("agent settings", re.compile(r"(^|/)\.claude(/|$)")),
)
KEY_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("flashblade-access-key", re.compile(r"\bPSFB[A-Z]{38}\b")),
    ("aws-access-key", re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("private-key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
)


class Finding(NamedTuple):
    status: str
    check: str
    detail: str

    def render(self) -> str:
        return f"{self.status:11} {self.check:9} {self.detail}"


# -- members -------------------------------------------------------------------


def _safe(name: str) -> str | None:
    p = PurePosixPath(name)
    if p.is_absolute() or ".." in p.parts:
        return None
    return p.as_posix()


def wheel_members(path: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(path) as z:
        return {n: z.read(n) for n in z.namelist() if not n.endswith("/")}


def sdist_members(path: Path) -> dict[str, bytes]:
    """Members with the ``<name>-<version>/`` prefix stripped."""
    out: dict[str, bytes] = {}
    with tarfile.open(path, "r:gz") as t:
        for m in t.getmembers():
            if not m.isfile():
                continue
            parts = m.name.split("/", 1)
            rel = parts[1] if len(parts) == 2 else parts[0]
            f = t.extractfile(m)
            out[rel] = f.read() if f is not None else b""
    return out


def rendered_configmaps(wheel: dict[str, bytes], workdir: Path) -> dict[str, bytes]:
    """The script ConfigMaps rendered offline from the wheel's own files, as
    members ``configmap/<map>/<key>``. Both workloads; no credentials."""
    pkg = workdir / "wheel-package"
    for name, data in wheel.items():
        rel = _safe(name)
        if rel is None or not rel.startswith("lakebench/"):
            continue
        dest = pkg / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from lakebench.config.schema import LakebenchConfig
    from lakebench.modules.pipeline_engines.spark.scripts_maps import build_script_configmaps

    out: dict[str, bytes] = {}
    for schema in ("customer360", "financial"):
        cfg = LakebenchConfig(
            name="package-guard",
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://s3.example.invalid:80",
                        "access_key": "placeholder",
                        "secret_key": "placeholder",
                    }
                }
            },
            workload={"schema": schema},
        )
        for m in build_script_configmaps(cfg, "package-guard", package_dir=pkg / "lakebench"):
            for key, value in m["data"].items():
                out[f"configmap/{m['metadata']['name']}/{key}"] = value.encode("utf-8")
    return out


def _is_text(data: bytes) -> bool:
    return b"\0" not in data[:SNIFF_BYTES]


# -- checks --------------------------------------------------------------------


def check_names(members: Iterable[str]) -> list[Finding]:
    out = []
    for name in sorted(members):
        if _safe(name) is None:
            out.append(Finding(FAIL, "names", f"{name}: unsafe member path"))
            continue
        for label, rule in NAME_RULES:
            if rule.search(name):
                out.append(Finding(FAIL, "names", f"{name}: {label} must not ship"))
    return out


def _entropy(s: str) -> float:
    if not s:
        return 0.0
    counts = {c: s.count(c) for c in set(s)}
    return -sum(n / len(s) * math.log2(n / len(s)) for n in counts.values())


def gitleaks_rules(
    config: Path = GITLEAKS_CONFIG,
) -> tuple[list[dict[str, Any]], list[re.Pattern[str]]]:
    """The custom ``[[rules]]`` of the repository's gitleaks config, and its
    global allowlist regexes."""
    doc = tomllib.loads(config.read_text(encoding="utf-8"))
    rules = []
    for r in doc.get("rules", []):
        try:
            rules.append({**r, "_re": re.compile(r["regex"])})
        except (KeyError, re.error):
            continue
    allow = [re.compile(x) for block in doc.get("allowlists", []) for x in block.get("regexes", [])]
    return rules, allow


def check_content(members: Mapping[str, bytes], config: Path = GITLEAKS_CONFIG) -> list[Finding]:
    rules, allow = gitleaks_rules(config)
    out = []
    for name in sorted(members):
        data = members[name]
        if not _is_text(data):
            continue
        for n, line in enumerate(data.decode("utf-8", errors="replace").split("\n"), 1):
            hit = next((rid for rid, rule in KEY_RULES if rule.search(line)), None)
            for r in rules if hit is None else ():
                for m in r["_re"].finditer(line):
                    group = int(r.get("secretGroup", 0) or 0)
                    secret = m.group(group) if group <= (m.re.groups or 0) else m.group(0)
                    if r.get("entropy") and _entropy(secret) < float(r["entropy"]):
                        continue
                    if any(a.search(secret) for a in allow):
                        continue
                    hit = f"gitleaks rule {r.get('id', '?')}"
                    break
                if hit:
                    break
            if hit:  # one finding per line, never the value
                out.append(Finding(FAIL, "content", f"{name}:{n}: {hit}"))
    return out


def extract(members: Mapping[str, bytes], dest: Path) -> None:
    for name, data in members.items():
        rel = _safe(name)
        if rel is None:
            continue
        p = dest / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)


def check_gitleaks(tree: Path, config: Path = GITLEAKS_CONFIG) -> list[Finding]:
    exe = shutil.which("gitleaks")
    if exe is None:
        return [Finding(SKIP, "gitleaks", "gitleaks is not on PATH")]
    # A file named .gitleaksignore inside the scanned tree would be honoured;
    # the members are scanned with none.
    for ignore in tree.rglob(".gitleaksignore"):
        ignore.unlink()
    r = subprocess.run(
        [
            exe,
            "dir",
            str(tree),
            "--config",
            str(config),
            "--redact",
            "--no-banner",
            "--exit-code",
            "1",
        ],
        capture_output=True,
        text=True,
    )
    if r.returncode == 0:
        return [Finding(PASS, "gitleaks", f"no leaks in {sum(1 for _ in tree.rglob('*'))} paths")]
    tail = [ln for ln in (r.stdout + r.stderr).splitlines() if ln.strip()][-8:]
    return [Finding(FAIL, "gitleaks", f"gitleaks dir exit {r.returncode}: " + " | ".join(tail))]


AbsenceFn = Callable[[Mapping[str, str]], tuple[str, list[str]]]


def _datagen_seed_absence(texts: Mapping[str, str]) -> tuple[str, list[str]]:
    """(mode, problems) from lakebench.config.datagen_seed."""
    if str(SRC) not in sys.path:
        sys.path.insert(0, str(SRC))
    from lakebench.config import datagen_seed as ds

    absence = getattr(ds, "absence_problems", None)
    load = getattr(ds, "load_heldout", None)
    if absence is None or load is None:
        raise RuntimeError(
            "heldout_hashes.json exists but lakebench.config.datagen_seed has no absence check"
        )
    held = load(HELDOUT_HASHES)
    return held.absence_check, absence(texts, held)


def check_heldout(
    members: Mapping[str, bytes],
    absence: AbsenceFn | None = None,
    hashes: Path = HELDOUT_HASHES,
) -> list[Finding]:
    if not hashes.is_file():
        return [Finding(SKIP, "heldout", f"no {hashes.name}: the held-out check is off")]
    texts = {n: d.decode("utf-8", errors="replace") for n, d in members.items() if _is_text(d)}
    try:
        mode, problems = (absence or _datagen_seed_absence)(texts)
    except Exception as exc:  # noqa: BLE001 -- a check that cannot run fails
        return [Finding(FAIL, "heldout", f"cannot run: {exc}")]
    if not problems:
        return [Finding(PASS, "heldout", f"no held-out seed in {len(texts)} text members")]
    status = PENDING if mode == "report" else FAIL
    return [Finding(status, "heldout", p) for p in problems]


# -- runner --------------------------------------------------------------------


def build(outdir: Path) -> None:
    """``python -m build`` as CI and the release run it: the sdist, then the
    wheel from the unpacked sdist."""
    subprocess.run(
        [sys.executable, "-m", "build", "--no-isolation", "--outdir", str(outdir), str(ROOT)],
        check=True,
        capture_output=True,
    )


def guard(dist: Path, workdir: Path, absence: AbsenceFn | None = None) -> list[Finding]:
    """Every finding over the artifacts in *dist*."""
    wheels = sorted(dist.glob("*.whl"))
    sdists = sorted(dist.glob("*.tar.gz"))
    if not wheels or not sdists:
        return [
            Finding(
                FAIL,
                "dist",
                f"{dist}: needs a wheel and an sdist, found {len(wheels)} and {len(sdists)}",
            )
        ]
    members: dict[str, bytes] = {}
    for w in wheels:
        wm = wheel_members(w)
        members.update({f"{w.name}/{k}": v for k, v in wm.items()})
        try:
            members.update(rendered_configmaps(wm, workdir / w.stem))
        except Exception as exc:  # noqa: BLE001 -- maps that cannot render fail
            return [Finding(FAIL, "configmap", f"{w.name}: cannot render the script maps: {exc}")]
    for s in sdists:
        members.update({f"{s.name}/{k}": v for k, v in sdist_members(s).items()})
    names = [n.split("/", 1)[1] if not n.startswith("configmap/") else n for n in members]
    findings = check_names(names) or [Finding(PASS, "names", f"{len(members)} members")]
    findings += check_content(members) or [Finding(PASS, "content", "no key pattern")]
    tree = workdir / "members"
    extract(members, tree)
    findings += check_gitleaks(tree)
    findings += check_heldout(members, absence)
    return findings


def exit_code(findings: Sequence[Finding], require_all: bool = False) -> int:
    bad = {FAIL, SKIP, PENDING} if require_all else {FAIL}
    return 1 if any(f.status in bad for f in findings) else 0


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--dist", type=Path, help="directory holding the built wheel and sdist")
    src.add_argument("--build", action="store_true", help="build them first, in a temp dir")
    ap.add_argument("--require-all", action="store_true", help="SKIP and PENDING-OA5 also fail")
    args = ap.parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="package-guard-") as tmp:
        work = Path(tmp)
        dist = args.dist
        if args.build:
            dist = work / "dist"
            try:
                build(dist)
            except subprocess.CalledProcessError as exc:
                err = (exc.stderr or b"").decode("utf-8", "replace").strip().splitlines()[-5:]
                print(f"{FAIL:11} build     python -m build failed: {' | '.join(err)}")
                return 1
        findings = guard(dist, work)
    for f in findings:
        print(f.render())
    return exit_code(findings, args.require_all)


if __name__ == "__main__":
    sys.exit(main())
