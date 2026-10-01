#!/usr/bin/env python3
"""Resolve, verify and serve a deployment's dependency set (DEP-2, ch01 s2.3-2.6).

Shipped in the ``lb-deps-tools`` ConfigMap with ``request.json`` and run by
the ``lb-deps`` pod and its consumers. Standard library only. It runs on the
stock Spark image's Python 3.10 and on ``images.duckdb`` (3.11 by default);
the source keeps to Python 3.8 grammar and stdlib. It must not import
``lakebench``; ``tests/test_lb_deps.py`` pins every value copied from
``lakebench.deps.request``.

Subcommands:

``resolve duckdb``  first init container, only when the request selects the
                    ``duckdb`` group: the DuckDB wheel and extension files.
``resolve spark``   always, last: the jars and the reference wheels; then it
                    finalises the set.
``serve``           re-hash the set, then serve it read-only over HTTP.
``show``            print the request's manifest (pointer record plus set).
``fetch``           consumer init containers: download one group and check
                    each file's sha256 against the manifest ConfigMap.

On-disk layout of the PVC at ``LB_DEPS_ROOT`` (default ``/deps``):

``sets/<pinset>/``            the set; ``manifest.json`` holds only content:
                              the pinset, its file entries and the jar order
                              (both enter the pinset), so a set that a later
                              request reuses carries nothing from the request
                              that built it
``requests/<request>.json``   the pointer: the request-scoped record
                              (repositories, Python versions, coordinates,
                              overlaps), written last
``staging/<request>/``        a resolve in progress; never served
``.resolve.lock``             held for a whole resolve container

Every failure prints one line starting ``LB_DEPS_ERROR`` and exits with a
distinct code (``EXIT_*`` below). A line about a repository or index that
could not be reached carries ``egress:``.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import gzip
import hashlib
import http.server
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import zlib
from collections.abc import Callable
from typing import Any, NoReturn
from xml.sax.saxutils import quoteattr

EXIT_MISSING = 3  # an artifact, wheel, extension or host could not be had
EXIT_HASH = 4  # a file does not match its sha256, is corrupt, or is missing
EXIT_UX_D2 = 5  # the set breaks the one-runtime rule
EXIT_SPACE = 6  # no space left on the PVC or the destination
EXIT_INTERNAL = 7  # anything else, including a killed child or SIGTERM
EXIT_REQUEST = 8  # request.json is not the request the pod was rendered for

# Copied from lakebench.deps.request; tests/test_lb_deps.py pins the copies.
GROUP_JARS = "jars"
GROUP_PY_REFERENCE = "py-reference"
GROUP_DUCKDB = "duckdb"
MANIFEST_GROUPS = {
    GROUP_JARS: ("jars",),
    GROUP_PY_REFERENCE: ("py-reference",),
    GROUP_DUCKDB: ("duckdb-wheels", "duckdb-ext"),
}
# Manifest group -> directory under the set, which is also its URL path.
MANIFEST_DIRS = {
    "jars": "jars",
    "py-reference": "py-reference",
    "duckdb-wheels": "duckdb/wheels",
    "duckdb-ext": "duckdb-ext",
}

MIN_FREE_MIB = 2048
# extensions.duckdb.org answers 403 to urllib's default "Python-urllib/3.x"
# User-Agent (SD-1, 2026-10-01).
USER_AGENT = "lakebench-lb-deps/1"
HTTP_TIMEOUT = 120
FETCH_ATTEMPTS = 3
CHUNK = 1 << 20
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
# Tool output that means a repository or index could not be reached, as
# opposed to reached and missing the artifact.
_EGRESS = re.compile(
    r"(UnknownHostException|ConnectException|Connection refused|Connection timed out|"
    r"connect timed out|Network is unreachable|No route to host|Name or service not known|"
    r"Temporary failure in name resolution|NewConnectionError|ConnectTimeoutError|"
    r"Failed to establish a new connection|SSLError|certificate verify failed|"
    r"SSLHandshakeException|PKIX path building failed|unable to find valid certification path|"
    r"SocketTimeoutException|Read timed out|Connection reset)"
)


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def root() -> str:
    return _env("LB_DEPS_ROOT", "/deps")


def work() -> str:
    return _env("LB_DEPS_WORK", "/work")


def tools_dir() -> str:
    return _env("LB_DEPS_TOOLS", os.path.dirname(os.path.abspath(__file__)))


def spark_home() -> str:
    return _env("SPARK_HOME", "/opt/spark")


def pip_cmd() -> list[str]:
    override = os.environ.get("LB_DEPS_PIP")
    return override.split() if override else [sys.executable, "-m", "pip"]


class Fail(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class Terminated(Fail):
    """SIGTERM. Never caught as a verification result."""


class SetDamaged(Fail):
    """A set or staging tree holds something it must not (a symlink)."""


def fail(code: int, message: str) -> NoReturn:
    raise Fail(code, message)


def info(line: str) -> None:
    print(line, flush=True)


def check_child_space(output: str, what: str) -> None:
    if "No space left on device" in output or "Disk quota exceeded" in output:
        fail(EXIT_SPACE, f"{what}: no space left on the PVC or work volume")


def egress_note(output: str) -> str:
    m = _EGRESS.search(output)
    return " egress: " + m.group(1) if m else ""


# --- children and signals --------------------------------------------------------

_CHILD: list[subprocess.Popen] = []


def run_child(cmd: list[str], what: str) -> tuple[int, str]:
    """Run a tool, keep its output (stdout and stderr) for classification,
    echo it to the log, and make a killed child an internal error."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    _CHILD.append(proc)
    try:
        out_b, _ = proc.communicate()
    finally:
        _CHILD.remove(proc)
    out = out_b.decode("utf-8", "replace")
    if proc.returncode < 0:
        info("\n".join(out.splitlines()[-30:]))
        fail(EXIT_INTERNAL, f"{what} killed by signal {-proc.returncode} (memory limit?)")
    return proc.returncode, out


def _terminate(signum: int, frame: Any) -> None:
    # As PID 1 a container gets no default SIGTERM action. Stop the child and
    # leave with one error line; the next start sees no pointer and resumes.
    for p in list(_CHILD):
        p.terminate()
    raise Terminated(EXIT_INTERNAL, f"terminated by signal {signum}")


# --- hashing and the manifest -------------------------------------------------


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def pinset_sha256(groups: dict[str, list[dict[str, Any]]], jar_order: list[str]) -> str:
    """Same bytes as ``lakebench.deps.request.pinset_sha256``: the sorted
    file triples and the jar order. Raises ValueError when ``jar_order`` is
    not an ordering of the jars group."""
    triples = sorted([g, e["file"], e["sha256"]] for g, entries in groups.items() for e in entries)
    order = list(jar_order)
    jars = sorted(e["file"] for e in groups.get("jars", ()))
    if sorted(order) != jars or len(set(order)) != len(order):
        raise ValueError(f"jar_order {order} is not an ordering of the jars group {jars}")
    return hashlib.sha256(
        json.dumps(
            {"files": triples, "jar_order": order}, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


def manifest_pinset(man: dict[str, Any], where: str) -> str:
    """The pinset a manifest's own entries and jar order hash to."""
    try:
        return pinset_sha256(man["groups"], man["jar_order"])
    except (ValueError, KeyError, TypeError) as e:
        fail(EXIT_HASH, f"manifest {where} is malformed: {e}")


def write_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_json(path: str) -> Any:
    with open(path) as f:
        return json.load(f)


def set_dir(pinset: str) -> str:
    if not _HEX64.match(pinset or ""):
        fail(EXIT_HASH, f"not a pinset: {pinset!r}")
    return os.path.join(root(), "sets", pinset)


def pointer_path(request_sha: str) -> str:
    if not _HEX64.match(request_sha or ""):
        fail(EXIT_REQUEST, f"not a request sha256: {request_sha!r}")
    return os.path.join(root(), "requests", request_sha + ".json")


def _walk_files(top: str, base: str) -> list[str]:
    """Every regular file under ``top``, relative to ``base``. Any symlink,
    to a file or a directory, fails: a set holds only its own bytes."""
    out = []
    for dirpath, dirnames, files in os.walk(top):
        for d in dirnames:
            if os.path.islink(os.path.join(dirpath, d)):
                raise SetDamaged(
                    EXIT_HASH, "symlink in set: " + os.path.relpath(os.path.join(dirpath, d), base)
                )
        for fn in files:
            p = os.path.join(dirpath, fn)
            if os.path.islink(p):
                raise SetDamaged(EXIT_HASH, "symlink in set: " + os.path.relpath(p, base))
            out.append(os.path.relpath(p, base).replace(os.sep, "/"))
    return sorted(out)


def scan_groups(base: str) -> dict[str, list[dict[str, Any]]]:
    """Hash every file under the group directories of ``base``."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for group, sub in sorted(MANIFEST_DIRS.items()):
        top = os.path.join(base, sub)
        if not os.path.isdir(top):
            continue
        entries = []
        for rel in _walk_files(top, top):
            p = os.path.join(top, rel)
            entries.append({"file": rel, "sha256": sha256_file(p), "size": os.path.getsize(p)})
        groups[group] = entries
    return groups


def verify_set(pinset: str) -> str | None:
    """None when ``sets/<pinset>`` holds exactly its manifest's files with
    their sha256s and the manifest hashes to ``pinset``; else the reason."""
    if not _HEX64.match(pinset or ""):
        return f"not a pinset: {pinset!r}"
    sd = set_dir(pinset)
    mpath = os.path.join(sd, "manifest.json")
    if not os.path.isfile(mpath):
        return "missing " + mpath
    try:
        man = read_json(mpath)
        groups = man["groups"]
        if man.get("pinset_sha256") != pinset or pinset_sha256(groups, man["jar_order"]) != pinset:
            return "manifest " + mpath + " does not hash to " + pinset
    except (ValueError, KeyError, TypeError) as exc:
        return "unreadable " + mpath + ": " + str(exc)
    expected = {"manifest.json"}
    for group, entries in groups.items():
        if group not in MANIFEST_DIRS:
            return "unknown group " + group
        for e in entries:
            rel = MANIFEST_DIRS[group] + "/" + e["file"]
            if not _safe_rel(e["file"]):
                return "unsafe path " + rel
            expected.add(rel)
            p = os.path.join(sd, rel)
            if not os.path.isfile(p) or os.path.islink(p):
                return "hash mismatch " + rel + " expected=" + e["sha256"] + " got=missing"
            got = sha256_file(p)
            if got != e["sha256"]:
                return "hash mismatch " + rel + " expected=" + e["sha256"] + " got=" + got
    try:
        present = _walk_files(sd, sd)
    except SetDamaged as e:
        return e.message
    for rel in present:
        if rel not in expected:
            return "file not in manifest: " + rel
    return None


def _safe_rel(rel: str) -> bool:
    parts = rel.split("/")
    return bool(rel) and not rel.startswith("/") and ".." not in parts and "" not in parts


def zip_check(path: str) -> None:
    """Every jar and wheel must open and pass its CRC check, so a file a
    killed copy truncated never enters a self-consistent manifest."""
    try:
        with zipfile.ZipFile(path) as z:
            bad = z.testzip()
    except (zipfile.BadZipFile, OSError) as e:
        fail(EXIT_HASH, "corrupt archive " + os.path.basename(path) + ": " + str(e))
    if bad is not None:
        fail(EXIT_HASH, "corrupt archive " + os.path.basename(path) + ": bad entry " + bad)


# --- the request ----------------------------------------------------------------


def load_request(path: str) -> tuple[str, dict[str, Any]]:
    """Read request.json and check it is the request this pod was rendered
    for (``LB_DEPS_REQUEST_SHA256``) and that it names this resolver."""
    want = os.environ.get("LB_DEPS_REQUEST_SHA256", "")
    if not _HEX64.match(want):
        fail(EXIT_REQUEST, "LB_DEPS_REQUEST_SHA256 is not a sha256")
    with open(path, "rb") as f:
        raw = f.read()
    got = hashlib.sha256(raw).hexdigest()
    if got != want:
        fail(EXIT_REQUEST, "request.json hashes to " + got + ", the pod expects " + want)
    req = json.loads(raw.decode())
    own = sha256_file(os.path.abspath(__file__))
    if req.get("tools_sha256") != own:
        fail(
            EXIT_REQUEST,
            "request names resolver " + str(req.get("tools_sha256")) + ", this one is " + own,
        )
    return want, req


def selected_manifest_groups(req: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for g in req.get("groups", []):
        if g not in MANIFEST_GROUPS:
            fail(EXIT_REQUEST, "unknown request group " + str(g))
        out.extend(MANIFEST_GROUPS[g])
    return out


def pip_index_args(index: str) -> list[str]:
    """One builder for every pip download, so the configured index applies
    to the reference wheels and the DuckDB wheel alike. ``--isolated`` keeps
    the image's pip.conf and PIP_* variables from choosing another index."""
    if not index:
        fail(EXIT_REQUEST, "request selects wheels but names no PyPI index")
    args = ["--isolated", "--index-url", index]
    parsed = urllib.parse.urlparse(index)
    if parsed.scheme == "http" and parsed.hostname:
        # pip ignores a plain-HTTP index that is not trusted.
        args += ["--trusted-host", parsed.hostname]
    return args


def wheel_name_version(fn: str) -> tuple[str, str]:
    parts = fn[: -len(".whl")].split("-") if fn.endswith(".whl") else []
    if len(parts) < 5:
        fail(EXIT_MISSING, "not a wheel file name: " + fn)
    return norm_dist(parts[0]), parts[1]


def norm_dist(name: str) -> str:
    return re.sub(r"[-_.]+", "_", name).lower()


def check_wheels(files: list[str], pins: list[str], what: str) -> None:
    """Each ``name==version`` pin yields exactly one wheel of that version,
    and nothing else was downloaded."""
    found = [wheel_name_version(f) for f in files]
    for pin in pins:
        name, _, ver = pin.partition("==")
        if not ver:
            fail(EXIT_REQUEST, "pin is not name==version: " + pin)
        hits = [files[i] for i, nv in enumerate(found) if nv == (norm_dist(name), ver)]
        if len(hits) != 1:
            fail(
                EXIT_MISSING,
                what + " pin " + pin + " gave " + str(len(hits)) + " wheels " + str(hits),
            )
    if len(files) != len(pins):
        fail(EXIT_MISSING, f"{what}: {len(files)} files for {len(pins)} pins")


def run_pip_download(dest: str, index: str, pins: list[str], what: str) -> list[str]:
    os.makedirs(dest, exist_ok=True)
    cmd = pip_cmd() + ["download", "--no-deps", "--only-binary=:all:", "--no-cache-dir", "-d", dest]
    cmd += pip_index_args(index) + pins
    rc, out = run_child(cmd, "pip download")
    info(out.rstrip())
    check_child_space(out, what)
    if rc != 0:
        fail(EXIT_MISSING, f"{what}: pip download exited {rc} from {index}{egress_note(out)}")
    files = sorted(os.listdir(dest))
    check_wheels(files, pins, what)
    return files


# --- resolve ----------------------------------------------------------------------


def staging_paths(request_sha: str) -> tuple[str, str, str]:
    top = os.path.join(root(), "staging", request_sha)
    return top, os.path.join(top, "set"), os.path.join(top, "meta")


def skip_if_done(request_sha: str) -> tuple[bool, str]:
    """(True, "") when the pointer names a set that verifies. A pointer whose
    set does not verify is removed, with its set, and the resolve runs again;
    the reason goes into the new pointer record."""
    ptr = pointer_path(request_sha)
    if not os.path.isfile(ptr):
        return False, ""
    try:
        pinset = read_json(ptr)["pinset_sha256"]
    except (ValueError, KeyError, TypeError):
        pinset = ""
    reason = verify_set(pinset)
    if reason is None:
        info("LB_DEPS_SKIP request=" + request_sha + " pinset=" + pinset)
        return True, ""
    info("LB_DEPS_RESOLVE_AGAIN " + reason)
    os.remove(ptr)
    if _HEX64.match(pinset or ""):
        shutil.rmtree(set_dir(pinset), ignore_errors=True)
    return False, reason


def complete_pending(request_sha: str) -> bool:
    """Finish a resolve killed between moving its set into place and writing
    the pointer: the staged record names the set; if it verifies, write the
    pointer. Otherwise the resolve runs from the start."""
    _, _, meta = staging_paths(request_sha)
    pending = os.path.join(meta, "record.json")
    if not os.path.isfile(pending):
        return False
    try:
        record = read_json(pending)
    except ValueError:
        return False
    if (
        record.get("request_sha256") != request_sha
        or verify_set(record.get("pinset_sha256", "")) is not None
    ):
        return False
    publish(request_sha, record)
    info(f"LB_DEPS_RESOLVED request={request_sha} pinset={record['pinset_sha256']} (completed)")
    return True


def check_space() -> None:
    os.makedirs(root(), exist_ok=True)
    free = shutil.disk_usage(root()).free >> 20
    if free < MIN_FREE_MIB:
        fail(
            EXIT_SPACE,
            f"pvc lb-deps-data has {free} MiB free, needs {MIN_FREE_MIB};"
            " delete PVC lb-deps-data or set platform.deps.storage_class",
        )


def begin(request_sha: str, first: bool) -> tuple[str, str]:
    """Prepare staging; the first container of the pod clears it."""
    _, st_set, meta = staging_paths(request_sha)
    if first:
        shutil.rmtree(os.path.join(root(), "staging"), ignore_errors=True)
        check_space()
    os.makedirs(st_set, exist_ok=True)
    os.makedirs(meta, exist_ok=True)
    return st_set, meta


def clean_work(name: str) -> str:
    """This container's work directory, always cleared: Ivy keeps a retrieved
    jar that is newer than its cache copy, truncated or not."""
    wd = os.path.join(work(), name)
    shutil.rmtree(wd, ignore_errors=True)
    os.makedirs(wd)
    return wd


def ivysettings_xml(repos: list[str]) -> str:
    """One ibiblio resolver per repository, in order, in one chain. With
    ``spark.jars.ivySettings`` Spark uses only these resolvers (R9)."""
    resolvers = "\n".join(
        f'      <ibiblio name="repo-{i}" m2compatible="true" usepoms="true" root={quoteattr(r)}/>'
        for i, r in enumerate(repos)
    )
    return (
        "<ivysettings>\n"
        '  <property name="ivy.checksums" value="sha1,md5" override="true"/>\n'
        '  <property name="ivy.maven.lookup.sources" value="false" override="true"/>\n'
        '  <property name="ivy.maven.lookup.javadoc" value="false" override="true"/>\n'
        '  <settings defaultResolver="lb-deps-chain"/>\n'
        "  <resolvers>\n"
        '    <chain name="lb-deps-chain" returnFirst="true">\n'
        + resolvers
        + "\n    </chain>\n  </resolvers>\n</ivysettings>\n"
    )


def coordinate_jar(coord: str) -> str:
    """Ivy's retrieve name for ``group:artifact:version`` under Spark's
    pattern ``[organization]_[artifact]-[revision](-[classifier]).[ext]``."""
    parts = coord.split(":")
    if len(parts) != 3 or not all(parts):
        fail(EXIT_REQUEST, "not a group:artifact:version coordinate: " + coord)
    return f"{parts[0]}_{parts[1]}-{parts[2]}.jar"


_SPARK_JARS_LINE = re.compile(r"^\(spark\.jars,(.*)\)\s*$")
_IVY_FOUND = re.compile(r"^\s*found (\S+)#(\S+);(\S+) in ")


def spark_jar_order(output: str, jar_dir: str) -> list[str]:
    """The jar order Spark itself uses for these packages: ``--verbose``
    prints the resolved ``spark.jars``, which is today's child-loader order
    (SD-1: it matches a 1.6 driver's "Added JAR" order)."""
    for line in output.splitlines():
        m = _SPARK_JARS_LINE.match(line.strip())
        if m:
            order = []
            for uri in m.group(1).split(","):
                path = urllib.parse.urlparse(uri).path if uri.startswith("file:") else uri
                if os.path.dirname(os.path.normpath(path)) == os.path.normpath(jar_dir):
                    order.append(os.path.basename(path))
            return order
    return []


def ivy_coordinates(output: str) -> dict[str, str]:
    """Retrieved file name -> ``group:artifact:version``, from Ivy's "found"
    lines: the exact coordinate, with no parsing of the file name."""
    out = {}
    for line in output.splitlines():
        m = _IVY_FOUND.match(line)
        if m:
            coord = ":".join(m.groups())
            out[coordinate_jar(coord)] = coord
    return out


def resolve_jars(req: dict[str, Any], st_set: str, meta: str) -> None:
    coords = list(req.get("jar_coordinates") or [])
    repos = list(req.get("repositories") or [])
    if not coords or not repos:
        fail(EXIT_REQUEST, "request has no jar coordinates or repositories")
    wd = clean_work("spark")
    settings = os.path.join(wd, "ivysettings.xml")
    with open(settings, "w") as f:
        f.write(ivysettings_xml(repos))
    ivy = os.path.join(wd, "ivy")
    cmd = [
        os.path.join(spark_home(), "bin", "spark-submit"),
        "--verbose",
        "--packages",
        ",".join(coords),
        "--conf",
        "spark.jars.ivySettings=" + settings,
        "--conf",
        "spark.jars.ivy=" + ivy,
        "--class",
        "org.apache.spark.deploy.DummyNonExistent",
        "local:///dev/null",
    ]
    t0 = time.time()
    # A non-zero exit is expected (the dummy class always fails); the files
    # decide. A child killed by a signal (the Ivy JVM at its memory limit)
    # fails in run_child.
    _, out = run_child(cmd, "spark-submit")
    with open(os.path.join(wd, "spark-submit.log"), "w") as f:
        f.write(out)
    check_child_space(out, "spark-submit")
    jar_dir = os.path.join(ivy, "jars")
    have = set(os.listdir(jar_dir)) if os.path.isdir(jar_dir) else set()
    for c in coords:
        if coordinate_jar(c) not in have:
            info("\n".join(out.splitlines()[-30:]))
            fail(EXIT_MISSING, "missing " + c + " from " + ",".join(repos) + egress_note(out))
    jars = sorted(f for f in have if f.endswith(".jar"))
    order = spark_jar_order(out, jar_dir)
    if sorted(order) != jars:
        fail(EXIT_MISSING, "spark-submit --verbose did not list the resolved jars in spark.jars")
    found = ivy_coordinates(out)
    dest = os.path.join(st_set, MANIFEST_DIRS["jars"])
    os.makedirs(dest, exist_ok=True)
    for fn in jars:
        shutil.copyfile(os.path.join(jar_dir, fn), os.path.join(dest, fn))
        # Before anything opens the jars to read them.
        zip_check(os.path.join(dest, fn))
    check_one_runtime(coords, jars)
    write_json(
        os.path.join(meta, "jars.json"),
        {
            "order": order,
            "coordinates": {fn: found.get(fn, "") for fn in jars},
            "overlaps": overlaps(dest, jars, found),
            "duplicate_classes": duplicate_classes(dest, order),
        },
    )
    info(f"LB_DEPS_JARS files={len(jars)} secs={time.time() - t0:.1f}")


def check_one_runtime(coords: list[str], jars: list[str]) -> None:
    """UX D2 in the set (s2.3 step 6): exactly one table-format runtime jar,
    the one the request asked for, and none of the other format."""
    families = ("iceberg-spark-runtime-", "delta-spark_")
    asked = {m: [c for c in coords if c.split(":")[1].startswith(m)] for m in families}
    held = {m: [j for j in jars if j.split("_", 1)[-1].startswith(m)] for m in families}
    wanted = [m for m in families if asked[m]]
    if len(wanted) != 1 or len(asked[wanted[0]]) != 1:
        fail(EXIT_UX_D2, "request must name one Iceberg or Delta runtime: " + ",".join(coords))
    m = wanted[0]
    other = [x for x in families if x != m][0]
    if held[m] != [coordinate_jar(asked[m][0])] or held[other]:
        fail(
            EXIT_UX_D2,
            f"want only {coordinate_jar(asked[m][0])}, the set holds {held[m] + held[other]}",
        )


_TRAILING_VERSION = re.compile(r"^(?P<a>.+?)-(?P<v>\d[\w.+-]*)$")


def jar_artifact(path: str, stem: str) -> tuple[str, str]:
    """(artifactId, version) of an image jar: from its own ``pom.properties``
    when one matches the file name, else the name split at the first
    ``-<digit>`` (versions may contain dashes, artifactIds rarely start a
    segment with a digit)."""
    try:
        with zipfile.ZipFile(path) as z:
            props = [
                n
                for n in z.namelist()
                if n.startswith("META-INF/maven/") and n.endswith("/pom.properties")
            ]
            for n in props:
                kv = {}
                for line in z.read(n).decode("utf-8", "replace").splitlines():
                    k, sep, v = line.partition("=")
                    if sep:
                        kv[k.strip()] = v.strip()
                a, v = kv.get("artifactId", ""), kv.get("version", "")
                if a and v and (stem == a + "-" + v or stem.startswith(a + "-" + v + "-")):
                    return a, v
    except (zipfile.BadZipFile, OSError):
        pass
    m = _TRAILING_VERSION.match(stem)
    return (m.group("a"), m.group("v")) if m else (stem, "")


def overlaps(staged_dir: str, jars: list[str], found: dict[str, str]) -> list[dict[str, str]]:
    """Every staged jar whose artifactId the image also ships in
    ``$SPARK_HOME/jars`` (s2.3 step 7). Thrift puts the set on the system
    classpath, so these can shadow the image's classes. The staged side is
    the exact Ivy coordinate; an image jar matches by artifactId or, when
    its name could not be split, by ``<artifactId>-<digit>`` prefix."""
    image_dir = os.path.join(spark_home(), "jars")
    image = []
    if os.path.isdir(image_dir):
        for fn in sorted(os.listdir(image_dir)):
            if fn.endswith(".jar"):
                a, v = jar_artifact(os.path.join(image_dir, fn), fn[:-4])
                image.append((fn, fn[:-4], a, v))
    if not image:
        fail(EXIT_INTERNAL, f"no jars in {image_dir}: cannot check overlaps")
    out = []
    for fn in jars:
        coord = found.get(fn, "")
        if coord:
            _, a, v = coord.split(":")
        else:
            stem = fn[:-4].split("_", 1)[1] if "_" in fn else fn[:-4]
            a, v = jar_artifact(os.path.join(staged_dir, fn), stem)
        for img_fn, img_stem, ia, iv in image:
            rest = img_stem[len(a) + 1 :]
            if ia == a or (img_stem.startswith(a + "-") and rest[:1].isdigit()):
                out.append(
                    {
                        "artifact": a,
                        "jar": fn,
                        "version": v,
                        "image_jar": img_fn,
                        "image_version": iv if ia == a else rest,
                    }
                )
                break
    return out


def duplicate_classes(staged_dir: str, order: list[str]) -> list[dict[str, Any]]:
    """Pairs of set jars that hold the same class, with the jar that wins
    under the recorded order. Recorded, not judged (the Iceberg sets carry
    two AWS SDK v2 copies)."""
    first: dict[str, str] = {}
    pairs: dict[tuple[str, str], int] = {}
    for fn in order:
        with zipfile.ZipFile(os.path.join(staged_dir, fn)) as z:
            for n in z.namelist():
                if (
                    not n.endswith(".class")
                    or n.startswith("META-INF/")
                    or n.endswith("module-info.class")
                ):
                    continue
                if n in first:
                    key = (first[n], fn)
                    pairs[key] = pairs.get(key, 0) + 1
                else:
                    first[n] = fn
    return [{"wins": a, "shadowed": b, "classes": n} for (a, b), n in sorted(pairs.items())]


def resolve_py_reference(req: dict[str, Any], st_set: str) -> None:
    pins = list(req.get("py_reference") or [])
    if not pins:
        fail(EXIT_REQUEST, "py-reference selected with no pins")
    dest = os.path.join(st_set, MANIFEST_DIRS["py-reference"])
    files = run_pip_download(dest, req.get("pypi_index", ""), pins, "py-reference")
    info(f"LB_DEPS_PY_REFERENCE files={len(files)}")


_PLATFORM_PROBE = (
    "import sys; sys.path.insert(0, sys.argv[1]); import duckdb; "
    "print(duckdb.sql('PRAGMA platform').fetchone()[0])"
)


def resolve_duckdb(req: dict[str, Any], st_set: str) -> None:
    version = req.get("duckdb_version", "")
    exts = list(req.get("duckdb_extensions") or [])
    repo = (req.get("duckdb_extension_repository") or "").rstrip("/")
    if not version or not exts or not repo:
        fail(EXIT_REQUEST, "duckdb selected without a version, extensions or repository")
    for g in ("duckdb-wheels", "duckdb-ext"):
        shutil.rmtree(os.path.join(st_set, MANIFEST_DIRS[g]), ignore_errors=True)
    wheels = os.path.join(st_set, MANIFEST_DIRS["duckdb-wheels"])
    files = run_pip_download(wheels, req.get("pypi_index", ""), ["duckdb==" + version], "duckdb")
    site = clean_work("duckdb")
    rc, out = run_child(
        pip_cmd()
        + [
            "install",
            "--isolated",
            "--no-deps",
            "--no-index",
            "--target",
            site,
            os.path.join(wheels, files[0]),
        ],
        "pip install",
    )
    if rc != 0:
        info(out.rstrip())
        fail(EXIT_MISSING, f"pip install of {files[0]} exited {rc}")
    rc, platform = run_child([sys.executable, "-c", _PLATFORM_PROBE, site], "duckdb platform probe")
    platform = platform.strip()
    if rc != 0 or not re.match(r"^[A-Za-z0-9_]+$", platform):
        fail(EXIT_MISSING, "could not read PRAGMA platform from duckdb " + version)
    rel_dir = f"v{version}/{platform}"
    out_dir = os.path.join(st_set, MANIFEST_DIRS["duckdb-ext"], rel_dir)
    os.makedirs(out_dir, exist_ok=True)
    for name in exts:
        url = f"{repo}/{rel_dir}/{name}.duckdb_extension.gz"
        dest = os.path.join(out_dir, name + ".duckdb_extension")
        part = dest + ".part"
        try:
            req_ = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            # Nested, not one multi-item with: the formatter would turn that
            # into a parenthesised form Python 3.8 cannot parse.
            with urllib.request.urlopen(req_, timeout=HTTP_TIMEOUT) as resp:
                with gzip.GzipFile(fileobj=resp) as gz:
                    with open(part, "wb") as f:
                        shutil.copyfileobj(gz, f, CHUNK)
            os.replace(part, dest)
        except urllib.error.HTTPError as e:
            fail(EXIT_MISSING, f"missing duckdb extension {name} from {url}: {e}")
        except urllib.error.URLError as e:
            fail(
                EXIT_MISSING, f"missing duckdb extension {name} from {url}: {e} egress: {e.reason}"
            )
        except OSError as e:
            if e.errno in (errno.ENOSPC, errno.EDQUOT):
                raise
            fail(EXIT_MISSING, f"missing duckdb extension {name} from {url}: {e}")
        except (EOFError, zlib.error) as e:
            fail(EXIT_MISSING, f"missing duckdb extension {name} from {url}: {e}")
        finally:
            if os.path.exists(part):
                os.remove(part)
        if os.path.getsize(dest) == 0:
            fail(EXIT_MISSING, "empty duckdb extension " + name + " from " + url)
    info(f"LB_DEPS_DUCKDB platform={platform} extensions={','.join(exts)}")


def expected_counts(req: dict[str, Any]) -> dict[str, int]:
    """Files each selected manifest group must hold; 0 means at least one."""
    want: dict[str, int] = {}
    for g in selected_manifest_groups(req):
        want[g] = 0
    if "py-reference" in want:
        want["py-reference"] = len(req.get("py_reference") or [])
    if "duckdb-wheels" in want:
        want["duckdb-wheels"] = 1
        want["duckdb-ext"] = len(req.get("duckdb_extensions") or [])
    return want


def publish(request_sha: str, record: dict[str, Any]) -> None:
    """Write the pointer, then leave one set on the PVC: other pointers go
    before their sets, so no pointer ever names a set that is half deleted."""
    os.makedirs(os.path.join(root(), "requests"), exist_ok=True)
    write_json(pointer_path(request_sha), record)
    for fn in os.listdir(os.path.join(root(), "requests")):
        if fn != request_sha + ".json":
            os.remove(os.path.join(root(), "requests", fn))
    for fn in os.listdir(os.path.join(root(), "sets")):
        if fn != record["pinset_sha256"]:
            shutil.rmtree(os.path.join(root(), "sets", fn), ignore_errors=True)
    # Only this request's staging: another pod's resolve may be between its
    # containers. The first container of a pod clears the rest (begin).
    shutil.rmtree(staging_paths(request_sha)[0], ignore_errors=True)


def finalise(request_sha: str, req: dict[str, Any], st_set: str, meta: str) -> None:
    want = expected_counts(req)
    extra = sorted(set(os.listdir(st_set)) - {MANIFEST_DIRS[g].split("/")[0] for g in want})
    if extra:
        fail(EXIT_INTERNAL, "staged entries the request did not select: " + ",".join(extra))
    for g in ("jars", "py-reference", "duckdb-wheels"):
        top = os.path.join(st_set, MANIFEST_DIRS[g])
        if g in want and os.path.isdir(top):
            for rel in _walk_files(top, top):
                zip_check(os.path.join(top, rel))
    groups = scan_groups(st_set)
    for g, n in want.items():
        have = len(groups.get(g, []))
        if have == 0 or (n and have != n):
            fail(EXIT_MISSING, f"group {g} holds {have} files, needs {n if n else 'at least 1'}")
    jars_meta = read_json(os.path.join(meta, "jars.json"))
    # The jar order is content: the same files in another order load other
    # classes first, so they are another set.
    order = jars_meta["order"]
    pinset = pinset_sha256(groups, order)
    write_json(
        os.path.join(st_set, "manifest.json"),
        {"pinset_sha256": pinset, "groups": groups, "jar_order": order},
    )
    python = {}
    for part in ("spark", "duckdb"):
        p = os.path.join(meta, "python-" + part + ".txt")
        if os.path.isfile(p):
            with open(p) as f:
                python[part] = f.read().strip()
    again = os.path.join(meta, "resolved_again.txt")
    record = {
        "request_sha256": request_sha,
        "pinset_sha256": pinset,
        "tools_sha256": req.get("tools_sha256"),
        "repositories": req.get("repositories"),
        "pypi_index": req.get("pypi_index", ""),
        "duckdb_extension_repository": req.get("duckdb_extension_repository", ""),
        "python": python,
        "coordinates": jars_meta["coordinates"],
        "overlaps": jars_meta["overlaps"],
        "duplicate_classes": jars_meta["duplicate_classes"],
        "resolved_again": open(again).read() if os.path.isfile(again) else "",
        "resolved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    # The record is staged before the set moves, so a kill between the move
    # and the pointer is finished by the next start (complete_pending).
    write_json(os.path.join(meta, "record.json"), record)
    dest = set_dir(pinset)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    if os.path.isdir(dest) and verify_set(pinset) is None:
        shutil.rmtree(st_set)  # same bytes already there and verified
    else:
        shutil.rmtree(dest, ignore_errors=True)
        os.rename(st_set, dest)
    publish(request_sha, record)
    n = sum(len(v) for v in groups.values())
    size = sum(e["size"] for v in groups.values() for e in v)
    info(f"LB_DEPS_RESOLVED request={request_sha} pinset={pinset} files={n} bytes={size}")


def resolve_lock() -> Any:
    """One resolve at a time on this PVC. A replacement pod can start while
    the old one is still in its grace period on the same node; it waits."""
    os.makedirs(root(), exist_ok=True)
    f = open(os.path.join(root(), ".resolve.lock"), "w")  # noqa: SIM115
    deadline = time.time() + float(_env("LB_DEPS_LOCK_WAIT", "600"))
    while True:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return f
        except BlockingIOError:
            if time.time() > deadline:
                fail(EXIT_INTERNAL, "another resolve holds " + f.name)
            time.sleep(1)


def cmd_resolve(a: argparse.Namespace) -> None:
    request_sha, req = load_request(a.request)
    groups = list(req.get("groups") or [])
    if GROUP_JARS not in groups:
        fail(EXIT_REQUEST, "request does not select the jars group")
    has_duckdb = GROUP_DUCKDB in groups
    if a.part == "duckdb" and not has_duckdb:
        fail(EXIT_REQUEST, "resolve duckdb on a request without the duckdb group")
    signal.signal(signal.SIGTERM, _terminate)
    lock = resolve_lock()
    try:
        done, again = skip_if_done(request_sha)
        if done or complete_pending(request_sha):
            return
        first = a.part == "duckdb" or not has_duckdb
        st_set, meta = begin(request_sha, first)
        if again:
            with open(os.path.join(meta, "resolved_again.txt"), "w") as f:
                f.write(again)
        with open(os.path.join(meta, "python-" + a.part + ".txt"), "w") as f:
            f.write(sys.version.split()[0])
        if a.part == "duckdb":
            resolve_duckdb(req, st_set)
            return
        if has_duckdb and not os.path.isdir(os.path.join(st_set, MANIFEST_DIRS["duckdb-ext"])):
            fail(
                EXIT_INTERNAL,
                "resolve spark started without the duckdb files; delete the lb-deps pod",
            )
        # A retried container starts its own groups from empty, so a file a
        # killed attempt left behind cannot enter the set.
        keep = {"duckdb", "duckdb-ext"} if has_duckdb else set()
        for name in os.listdir(st_set):
            if name not in keep:
                path = os.path.join(st_set, name)
                if os.path.isdir(path) and not os.path.islink(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
        resolve_jars(req, st_set, meta)
        if GROUP_PY_REFERENCE in groups:
            resolve_py_reference(req, st_set)
        finalise(request_sha, req, st_set, meta)
    finally:
        lock.close()


# --- show -----------------------------------------------------------------------


def manifest_for(request_sha: str) -> dict[str, Any]:
    """The pointer record plus its set's entries: what ``lb-deps-manifest``
    holds (s2.3 step 8 shape)."""
    ptr = pointer_path(request_sha)
    if not os.path.isfile(ptr):
        fail(EXIT_HASH, "no set for request " + request_sha)
    record = read_json(ptr)
    pinset = record.get("pinset_sha256", "")
    man = read_json(os.path.join(set_dir(pinset), "manifest.json"))
    if man.get("pinset_sha256") != pinset or manifest_pinset(man, pinset) != pinset:
        fail(EXIT_HASH, "set " + pinset + " does not hash to its pointer")
    out = dict(record)
    out["groups"] = man["groups"]
    out["jar_order"] = man["jar_order"]
    return out


def cmd_show(a: argparse.Namespace) -> None:
    request_sha = a.request or os.environ.get("LB_DEPS_REQUEST_SHA256", "")
    sys.stdout.write(json.dumps(manifest_for(request_sha), indent=1, sort_keys=True) + "\n")


# --- serve ----------------------------------------------------------------------


class SetHandler(http.server.SimpleHTTPRequestHandler):
    """GET and HEAD under ``/sets/<pinset>/`` only; ``/ready``; 405 else."""

    pinset = ""
    base = ""
    # A connection that sends nothing must not hold shutdown for the whole
    # grace period (server_close joins handler threads).
    timeout = 60

    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, directory=self.base, **kw)

    def _rest(self, path: str) -> str | None:
        """The part of a URL path below ``/sets/<pinset>``, or None."""
        path = urllib.parse.urlsplit(path).path
        prefix = "/sets/" + self.pinset
        if path != prefix and not path.startswith(prefix + "/"):
            return None
        return path[len(prefix) :] or "/"

    def translate_path(self, path: str) -> str:
        # send_head() and list_directory() call this with the request path;
        # map it below the set directory. _target() has already refused
        # anything outside the prefix.
        rest = self._rest(path)
        return super().translate_path(rest if rest is not None else "/")

    def _target(self) -> str | None:
        if self._rest(self.path) is None or "\x00" in urllib.parse.unquote(self.path):
            return None
        target = self.translate_path(self.path)
        real, base = os.path.realpath(target), os.path.realpath(self.base)
        if real != base and not real.startswith(base + os.sep):
            return None
        return target

    def do_GET(self) -> None:  # noqa: N802
        if urllib.parse.urlsplit(self.path).path == "/ready":
            body = self.pinset.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self._target() is None:
            self.send_error(404)
            return
        super().do_GET()

    def do_HEAD(self) -> None:  # noqa: N802
        if self._target() is None:
            self.send_error(404)
            return
        super().do_HEAD()

    def _not_allowed(self) -> None:
        self.send_response(405)
        self.send_header("Allow", "GET, HEAD")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def __getattr__(self, name: str) -> Callable[[], None]:
        # BaseHTTPRequestHandler looks up do_<METHOD>; every method other
        # than GET and HEAD lands here.
        if name.startswith("do_"):
            return self._not_allowed
        raise AttributeError(name)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass  # no per-request lines

    def log_error(self, format: str, *args: Any) -> None:  # noqa: A002
        sys.stderr.write(self.address_string() + " " + (format % args) + "\n")


class QuietServer(http.server.ThreadingHTTPServer):
    # Not daemon threads: server_close() waits for downloads in flight, so a
    # SIGTERM during a rollout finishes them (kubelet's grace period bounds
    # the wait) instead of cutting a consumer's file short.
    daemon_threads = False
    block_on_close = True

    def handle_error(self, request: Any, client_address: Any) -> None:
        # A client that hangs up mid-download is not a server error.
        if isinstance(sys.exc_info()[1], (ConnectionError, TimeoutError)):
            return
        super().handle_error(request, client_address)


def cmd_serve(a: argparse.Namespace) -> None:
    request_sha = os.environ.get("LB_DEPS_REQUEST_SHA256", "")
    ptr = pointer_path(request_sha)
    if not os.path.isfile(ptr):
        fail(EXIT_HASH, "no set for request " + request_sha)
    pinset = read_json(ptr).get("pinset_sha256", "")
    reason = verify_set(pinset)
    if reason is not None:
        fail(EXIT_HASH, reason)
    groups = read_json(os.path.join(set_dir(pinset), "manifest.json"))["groups"]
    n = sum(len(v) for v in groups.values())
    size = sum(e["size"] for v in groups.values() for e in v)
    SetHandler.pinset = pinset
    SetHandler.base = set_dir(pinset)
    srv = QuietServer(("", a.port), SetHandler)

    def stop(signum: int, frame: Any) -> None:
        # As PID 1 the process gets no default SIGTERM action; shut down from
        # another thread, because shutdown() waits for serve_forever().
        threading.Thread(target=srv.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    info(f"LB_DEPS_READY request={request_sha} pinset={pinset} files={n} bytes={size}")
    try:
        srv.serve_forever()
    finally:
        srv.server_close()


# --- fetch ------------------------------------------------------------------------


def _file_ok(path: str, sha: str) -> bool:
    return os.path.isfile(path) and sha256_file(path) == sha


def fetch_one(url: str, dest: str, sha: str, size: int) -> int:
    """Download to ``<dest>.part`` hashing as it streams; rename on a match.
    A short or failed transfer is retried; a full-length mismatch is not."""
    part = dest + ".part"
    last = ""
    try:
        for attempt in range(FETCH_ATTEMPTS):
            if attempt:
                time.sleep(2 * attempt)
            h = hashlib.sha256()
            n = 0
            try:
                req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
                with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                    with open(part, "wb") as f:
                        for chunk in iter(lambda: r.read(CHUNK), b""):
                            h.update(chunk)
                            f.write(chunk)
                            n += len(chunk)
            except OSError as e:
                if e.errno in (errno.ENOSPC, errno.EDQUOT):
                    raise
                last = str(e)
                continue
            if n != size:
                last = f"short transfer: {n} of {size} bytes"
                continue
            if h.hexdigest() != sha:
                fail(
                    EXIT_HASH,
                    "hash mismatch "
                    + os.path.basename(dest)
                    + " expected="
                    + sha
                    + " got="
                    + h.hexdigest(),
                )
            os.replace(part, dest)
            return n
    finally:
        if os.path.exists(part):
            os.remove(part)
    fail(EXIT_MISSING, f"cannot fetch {url} after {FETCH_ATTEMPTS} attempts: {last}")


def cmd_fetch(a: argparse.Namespace) -> None:
    man = read_json(a.manifest)
    groups = man.get("groups") or {}
    pinset = man.get("pinset_sha256", "")
    if manifest_pinset(man, a.manifest) != pinset:
        fail(EXIT_HASH, "manifest " + a.manifest + " does not hash to its pinset " + pinset)
    url = (a.url or os.environ.get("LB_DEPS_URL", "")).rstrip("/")
    if not url.endswith("/sets/" + pinset):
        fail(EXIT_HASH, "url " + url + " does not name the manifest's pinset " + pinset)
    entries = groups.get(a.group) or []
    if a.group not in MANIFEST_DIRS or not entries:
        fail(EXIT_MISSING, "manifest has no files in group " + a.group)
    base = url + "/" + MANIFEST_DIRS[a.group] + "/"
    t0 = time.time()
    total = 0
    for e in entries:
        rel = e["file"]
        if not _safe_rel(rel):
            fail(EXIT_HASH, "unsafe path in manifest: " + rel)
        dest = os.path.join(a.dest, *rel.split("/"))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if _file_ok(dest, e["sha256"]):
            continue  # a restarted init container keeps verified files
        total += fetch_one(base + urllib.parse.quote(rel), dest, e["sha256"], int(e["size"]))
    secs = time.time() - t0
    info(f"LB_DEPS_FETCHED group={a.group} files={len(entries)} bytes={total} secs={secs:.1f}")


# --- main -----------------------------------------------------------------------


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lb_deps.py", description="Resolve, verify and serve a dependency set."
    )
    sub = p.add_subparsers(dest="cmd")
    r = sub.add_parser("resolve")
    r.add_argument("part", choices=("duckdb", "spark"))
    r.add_argument("--request", default=os.path.join(tools_dir(), "request.json"))
    s = sub.add_parser("serve")
    s.add_argument("--port", type=int, default=8080)
    sh = sub.add_parser("show")
    sh.add_argument("--request", default="")
    f = sub.add_parser("fetch")
    f.add_argument("--group", required=True)
    f.add_argument("--dest", required=True)
    f.add_argument("--manifest", default="/opt/lb-deps-manifest/manifest.json")
    f.add_argument("--url", default="")
    return p


def main(argv: list[str] | None = None) -> int:
    a = parser().parse_args(argv)
    commands = {"resolve": cmd_resolve, "serve": cmd_serve, "show": cmd_show, "fetch": cmd_fetch}
    if a.cmd not in commands:
        parser().print_usage(sys.stderr)
        return 2
    try:
        commands[a.cmd](a)
    except Fail as e:
        info("LB_DEPS_ERROR " + e.message)
        return e.code
    except OSError as e:
        if e.errno in (errno.ENOSPC, errno.EDQUOT):
            info(f"LB_DEPS_ERROR no space left writing {e.filename or ''}: {e.strerror}")
            return EXIT_SPACE
        info(f"LB_DEPS_ERROR internal {type(e).__name__}: {e}")
        return EXIT_INTERNAL
    except Exception as e:  # noqa: BLE001 -- one LB_DEPS_ERROR line for anything else
        info(f"LB_DEPS_ERROR internal {type(e).__name__}: {e}")
        return EXIT_INTERNAL
    return 0


if __name__ == "__main__":
    sys.exit(main())
