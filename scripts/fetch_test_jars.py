#!/usr/bin/env python3
"""Fetch the Spark-tier test jars pinned in tests/spark/jars.lock.json.

Each Spark line ("leg") pins the Iceberg runtime, delta-spark and
delta-storage jars the product requests for that line (the lock is checked
against job.py by tests/test_spark_jars_lock.py). This script downloads them
from Maven Central, falls back to the Google mirror on HTTP 429, a 5xx or a
network error, checks every file against its pinned sha256, and caches them
under their Maven file names, which the harness (tests/spark/conftest.py)
classifies by name.

    python scripts/fetch_test_jars.py --leg 4.0 --print-env >> "$GITHUB_ENV"
    jars=$(python scripts/fetch_test_jars.py --leg auto --print-env) && export "$jars"

A cached file is hashed again on every run, never trusted. A download whose
sha256 differs from the lock is deleted, and the script exits 1 naming the
coordinate and both hashes.

``--update-lock`` (maintainers, needs network) rebuilds the lock from the
product defaults in job.py: it confirms each coordinate by fetching its POM,
reads the delta-spark POM's compile dependencies to find delta-storage, checks
the downloaded bytes against Central's ``.sha1``, and writes the sha256 values.

Usage:
    python scripts/fetch_test_jars.py [--leg {4.0,4.1,auto}] [--cache DIR]
        [--lock PATH] [--print-env]
    python scripts/fetch_test_jars.py --update-lock [--cache DIR] [--lock PATH]
"""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import re
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCK = ROOT / "tests" / "spark" / "jars.lock.json"
DEFAULT_CACHE = Path.home() / ".cache" / "lakebench-test-jars"
CENTRAL = "https://repo1.maven.org/maven2/"
# The same mirror job.py hands Spark (_MAVEN_MIRROR_REPOS).
MIRROR = "https://maven-central.storage-download.googleapis.com/maven2/"
# The pyspark release each leg runs in CI and in /home/lb-toolchain.
LEG_PYSPARK = {"4.0": "4.0.1", "4.1": "4.1.1"}
_TIMEOUT = 120

Fetch = Callable[[str], bytes]


class FetchError(Exception):
    """A coordinate that could not be downloaded or did not match its pin."""


def jar_name(coord: str) -> str:
    """``group:artifact:version`` to the Maven file name ``artifact-version.jar``."""
    _group, artifact, version = coord.split(":")
    return f"{artifact}-{version}.jar"


def artifact_path(coord: str, ext: str = "jar") -> str:
    group, artifact, version = coord.split(":")
    return f"{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.{ext}"


def _http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "lakebench-fetch-test-jars"})
    with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:  # noqa: S310 (fixed https hosts)
        data: bytes = resp.read()
        return data


def _retryable(err: Exception) -> bool:
    """429 and 5xx, and a transport failure (refused, reset, timed out, TLS,
    a body cut short); never another HTTP status."""
    if isinstance(err, urllib.error.HTTPError):
        return err.code == 429 or err.code >= 500
    return isinstance(err, (OSError, http.client.HTTPException))


def fetch_path(path: str, get: Fetch | None = None) -> bytes:
    """GET *path* from Central, or from the mirror when Central answers 429,
    a 5xx or does not answer. A 404 is not retried: the coordinate is wrong."""
    get = get or _http_get
    try:
        return get(CENTRAL + path)
    except Exception as err:  # noqa: BLE001 (classified below)
        if not _retryable(err):
            raise FetchError(f"{CENTRAL}{path}: {err}") from err
        print(f"Maven Central failed ({err}); trying the mirror", file=sys.stderr)
        try:
            return get(MIRROR + path)
        except Exception as err2:  # noqa: BLE001
            raise FetchError(f"{path}: Central failed ({err}), mirror failed ({err2})") from err2


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_lock(path: Path = LOCK) -> dict:
    lock = json.loads(path.read_text())
    if lock.get("schema") != 1:
        raise FetchError(f"{path}: unknown lock schema {lock.get('schema')!r}")
    return lock


def ensure_jar(entry: dict, cache: Path, get: Fetch | None = None) -> Path:
    """The cached jar for one lock entry, downloaded if missing or if its
    bytes no longer match the pin."""
    coord, want = entry["coord"], entry["sha256"]
    dest = cache / jar_name(coord)
    if dest.is_file():
        have = sha256(dest)
        if have == want:
            return dest
        print(
            f"{dest}: cached sha256 {have} is not the pinned {want}; fetching again",
            file=sys.stderr,
        )
        dest.unlink()
    cache.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    part.write_bytes(fetch_path(artifact_path(coord), get))
    have = sha256(part)
    if have != want:
        part.unlink()
        raise FetchError(f"{coord}: downloaded sha256 {have}, lock pins {want}")
    os.replace(part, dest)
    return dest


def auto_leg() -> str:
    try:
        import pyspark
    except ImportError as err:
        raise FetchError("--leg auto needs pyspark installed") from err
    m = re.match(r"(\d+\.\d+)", pyspark.__version__)
    if not m:
        raise FetchError(f"cannot read the Spark line from pyspark {pyspark.__version__}")
    return m.group(1)


def fetch_leg(lock: dict, leg: str, cache: Path, get: Fetch | None = None) -> list[Path]:
    legs = lock["legs"]
    if leg not in legs:
        raise FetchError(f"no leg {leg!r} in the lock (legs: {', '.join(sorted(legs))})")
    return [ensure_jar(entry, cache, get) for entry in legs[leg]["jars"]]


# --- --update-lock ---------------------------------------------------------


def product_coordinates(leg: str) -> dict[str, str]:
    """The Iceberg runtime and delta-spark coordinates job.py requests for
    the Spark line *leg* with its default format versions."""
    try:
        import lakebench  # noqa: F401
    except ImportError:
        sys.path.insert(0, str(ROOT / "src"))
    from lakebench.modules.pipeline_engines.spark.job import (
        _FORMAT_VERSION_DEFAULTS,
        _delta_spark_artifact,
        iceberg_runtime_suffix_for,
    )

    key = tuple(int(x) for x in leg.split("."))
    spark_key = (key[0], key[1])
    defaults = _FORMAT_VERSION_DEFAULTS[spark_key]
    scala = "_2.13"
    ice = defaults["iceberg"]
    suffix = iceberg_runtime_suffix_for(spark_key, ice)
    return {
        "iceberg": f"org.apache.iceberg:iceberg-spark-runtime-{suffix}{scala}:{ice}",
        "delta": _delta_spark_artifact(scala, defaults["delta"]),
    }


_POM_NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def compile_dependencies(pom: bytes, group: str) -> list[str]:
    """``group:artifact:version`` of the compile-scope dependencies in *pom*
    whose groupId is *group* (versions given literally in the POM)."""
    root = ET.fromstring(pom)
    out = []
    for dep in root.findall("m:dependencies/m:dependency", _POM_NS):
        g = dep.findtext("m:groupId", default="", namespaces=_POM_NS)
        a = dep.findtext("m:artifactId", default="", namespaces=_POM_NS)
        v = dep.findtext("m:version", default="", namespaces=_POM_NS)
        scope = dep.findtext("m:scope", default="compile", namespaces=_POM_NS)
        if g == group and scope == "compile":
            if not v or "$" in v:
                raise FetchError(f"{g}:{a}: version {v!r} is not literal in the POM")
            out.append(f"{g}:{a}:{v}")
    return out


def _pinned(coord: str, kind: str, cache: Path, get: Fetch, **extra: str) -> dict:
    # The POM proves the coordinate exists; a search API can list one that does not.
    fetch_path(artifact_path(coord, "pom"), get)
    data = fetch_path(artifact_path(coord), get)
    sha1 = fetch_path(artifact_path(coord, "jar.sha1"), get).decode().split()[0].strip().lower()
    if hashlib.sha1(data).hexdigest() != sha1:  # noqa: S324 (Central's published checksum)
        raise FetchError(f"{coord}: downloaded bytes do not match Central's .sha1 {sha1}")
    cache.mkdir(parents=True, exist_ok=True)
    (cache / jar_name(coord)).write_bytes(data)
    return {"coord": coord, "sha256": hashlib.sha256(data).hexdigest(), "kind": kind, **extra}


def build_lock(cache: Path, get: Fetch | None = None) -> dict:
    legs = {}
    for leg, pyspark in LEG_PYSPARK.items():
        coords = product_coordinates(leg)
        delta_pom = fetch_path(artifact_path(coords["delta"], "pom"), get)
        storage = [c for c in compile_dependencies(delta_pom, "io.delta") if ":delta-storage:" in c]
        if len(storage) != 1:
            raise FetchError(
                f"{coords['delta']}: expected one delta-storage dependency, got {storage}"
            )
        jars = [
            _pinned(coords["iceberg"], "iceberg", cache, get),
            _pinned(coords["delta"], "delta", cache, get),
            _pinned(storage[0], "delta", cache, get, transitive_of=coords["delta"]),
        ]
        legs[leg] = {"pyspark": pyspark, "jars": jars}
    return {"schema": 1, "legs": legs}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--leg", default="auto", help="4.0, 4.1 or auto (from pyspark)")
    p.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    p.add_argument("--lock", type=Path, default=LOCK)
    p.add_argument("--print-env", action="store_true", help="print LB_SPARK_TEST_JARS=...")
    p.add_argument("--update-lock", action="store_true", help="rebuild the lock (network)")
    args = p.parse_args(argv)
    try:
        if args.update_lock:
            lock = build_lock(args.cache)
            args.lock.write_text(json.dumps(lock, indent=2) + "\n")
            print(f"wrote {args.lock}", file=sys.stderr)
            return 0
        leg = auto_leg() if args.leg == "auto" else args.leg
        paths = fetch_leg(load_lock(args.lock), leg, args.cache)
    except FetchError as err:
        print(f"fetch_test_jars: {err}", file=sys.stderr)
        return 1
    if args.print_env:
        print("LB_SPARK_TEST_JARS=" + ",".join(str(p) for p in paths))
    else:
        for path in paths:
            print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
