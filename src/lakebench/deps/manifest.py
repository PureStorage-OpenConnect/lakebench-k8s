"""The served dependency set as the CLI sees it.

``deploy`` reads the set's manifest from the ``lb-deps`` server
(``lb_deps.py show``), checks it here against the request it built, and
writes the checked manifest into the ``lb-deps-manifest`` ConfigMap that the
consumers and ``run`` read. Nothing here talks to the cluster:
``deploy/deps.py`` does that, and ``run``'s start checks call
:func:`check_manifest` on the ConfigMap again.

Names, labels and annotations of the server's objects live here too, so the
deployer, the consumers' templates and destroy share one spelling.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from lakebench.deps.request import (
    GROUP_DUCKDB,
    GROUP_PY_REFERENCE,
    MANIFEST_GROUPS,
    DepsRequest,
    pinset_sha256,
    unknown_overlaps,
)

# --- names --------------------------------------------------------------------

SERVER_NAME = "lb-deps"  # Deployment and Service
PVC_NAME = "lb-deps-data"
PVC_SIZE = "5Gi"
# The tools ConfigMap is named by its request: its two values are hashed by
# the pod (request.json against LB_DEPS_REQUEST_SHA256, lb_deps.py against
# the request's tools_sha256), so it is immutable and a new request gets a
# new map. A pod never mounts a map whose content it does not expect.
TOOLS_CONFIGMAP_PREFIX = "lb-deps-tools-"
MANIFEST_CONFIGMAP = "lb-deps-manifest"
PORT = 8080
TOOLS_MOUNT = "/opt/lb-deps-tools"
MANIFEST_MOUNT = "/opt/lb-deps-manifest"
SERVE_CONTAINER = "serve"

# Namespace annotation written last by a deploy whose server verified and
# served this pinset; absent while the server changes or after a failure.
ANNOTATION_DEPS_SET = "lakebench.deployment/deps-set"
# Pod annotations: the request the server pod was rendered for, and (on the
# consumers) the pinset a pod runs.
POD_ANNOTATION_REQUEST = "lakebench.io/deps-request"
POD_ANNOTATION_SET = "lakebench.io/deps-set"
LABEL_ROLE = "lakebench.io/deps-role"

# The server's Deployment and Service select on all three, so a later
# object labelled component=deps (a helper, a consumer) is never adopted.
SELECTOR_LABELS: dict[str, str] = {
    "app.kubernetes.io/name": "lakebench",
    "app.kubernetes.io/component": "deps",
    "lakebench.io/deps-role": "server",
}

# --- resources ----------------------------------------------------------------

# The resolve init containers (the Ivy JVM) and the serving container
# (a live cluster run used these).
RESOLVE_REQUESTS = {"cpu": "1", "memory": "2Gi"}
RESOLVE_LIMITS = {"cpu": "2", "memory": "3Gi"}
SERVE_REQUESTS = {"cpu": "250m", "memory": "512Mi"}
SERVE_LIMITS = {"cpu": "1", "memory": "1Gi"}


def _cpu_m(q: str) -> int:
    return int(q[:-1]) if q.endswith("m") else int(float(q) * 1000)


def _mem_mi(q: str) -> int:
    units = {"Mi": 1, "Gi": 1024}
    return int(q[:-2]) * units[q[-2:]]


# What the scheduler reserves for the lb-deps pod for its whole life: the
# larger of the largest init container request and the sum of the regular
# containers' requests. Design s2.5 counted the serving container alone on
# the premise that the init request is transient; Kubernetes keeps it
# reserved, so the co-resident sum counts this figure (1 CPU, 2 GiB).
POD_REQUEST_CPU_M = max(_cpu_m(RESOLVE_REQUESTS["cpu"]), _cpu_m(SERVE_REQUESTS["cpu"]))
POD_REQUEST_MEMORY_MI = max(_mem_mi(RESOLVE_REQUESTS["memory"]), _mem_mi(SERVE_REQUESTS["memory"]))

# A ConfigMap holds at most 1 MiB; leave room for metadata.
MANIFEST_CONFIGMAP_BUDGET = 900 * 1024


def tools_configmap_name(request_sha256: str) -> str:
    return TOOLS_CONFIGMAP_PREFIX + request_sha256[:16]


def server_host(namespace: str) -> str:
    return f"{SERVER_NAME}.{namespace}.svc.cluster.local"


def base_url(namespace: str, pinset: str) -> str:
    """Where the set is served, from the config's own namespace only."""
    return f"http://{server_host(namespace)}:{PORT}/sets/{pinset}"


@dataclass(frozen=True)
class DepsHandle:
    """A verified, served set: what deploy exposes as ``engine.deps`` and
    ``run`` sets as ``SparkJobManager.deps``."""

    pinset_sha256: str
    request_sha256: str
    base_url: str
    server_pod_uid: str
    manifest: Mapping[str, Any] = field(default_factory=dict, compare=False)


# --- checking what the server serves ------------------------------------------

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_PLAIN_FILE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")
_EXT_FILE = re.compile(r"^v([0-9A-Za-z.+-]+)/([A-Za-z0-9_]+)/([a-z0-9_]+)\.duckdb_extension$")
_RUNTIME_PREFIXES = ("iceberg-spark-runtime-", "delta-spark_")


def ivy_jar_name(coordinate: str) -> str:
    """Ivy's retrieve name for ``group:artifact:version`` (lb_deps.py
    ``coordinate_jar``; the parity test pins the two)."""
    g, a, v = coordinate.split(":")
    return f"{g}_{a}-{v}.jar"


def _norm_dist(name: str) -> str:
    return re.sub(r"[-_.]+", "_", name).lower()


def _wheel_name_version(fn: str) -> tuple[str, str] | None:
    parts = fn[: -len(".whl")].split("-") if fn.endswith(".whl") else []
    return (_norm_dist(parts[0]), parts[1]) if len(parts) >= 5 else None


def expected_manifest_groups(request: DepsRequest) -> set[str]:
    return {g for rg in request.groups for g in MANIFEST_GROUPS[rg]}


def _pin_wheels(pins: tuple[str, ...], entries: list[Mapping[str, Any]], group: str) -> list[str]:
    """Problems matching ``name==version`` pins one to one with wheels."""
    problems = []
    found = [(_wheel_name_version(str(e["file"])), e) for e in entries]
    for pin in pins:
        name, _, ver = pin.partition("==")
        hits = [e for nv, e in found if nv == (_norm_dist(name), ver)]
        if len(hits) != 1:
            problems.append(f"{group}: pin {pin} matches {len(hits)} wheels")
    if len(entries) != len(pins):
        problems.append(f"{group}: {len(entries)} files for {len(pins)} pins")
    return problems


def check_manifest(request: DepsRequest, shown: Mapping[str, Any]) -> list[str]:
    """Every reason the manifest a server printed is not the set this
    request needs; empty when it is. The pinset is recomputed from the
    entries and the jar order, never taken from the printed field."""
    problems: list[str] = []
    want = request.request_sha256
    if shown.get("request_sha256") != want:
        problems.append(
            f"request_sha256 {shown.get('request_sha256')!r} is not this deploy's {want}"
        )
    if shown.get("tools_sha256") != request.tools_sha256:
        problems.append(f"resolver {shown.get('tools_sha256')!r} is not {request.tools_sha256}")
    groups = shown.get("groups")
    order = shown.get("jar_order")
    if not isinstance(groups, Mapping) or not isinstance(order, list):
        return [*problems, "manifest has no groups or no jar_order"]
    try:
        recomputed = pinset_sha256(groups, order)
    except (ValueError, KeyError, TypeError) as e:
        return [*problems, f"manifest entries do not hash: {e}"]
    printed = shown.get("pinset_sha256")
    if printed != recomputed:
        problems.append(f"printed pinset {printed!r} is not the entries' {recomputed}")

    expected = expected_manifest_groups(request)
    if set(groups) != expected:
        problems.append(f"groups {sorted(groups)} are not the request's {sorted(expected)}")
    for g, entries in groups.items():
        if not isinstance(entries, list) or not entries:
            problems.append(f"group {g} is empty")
            continue
        for entry in entries:
            if not isinstance(entry, Mapping):
                problems.append(f"{g}: entry {entry!r} is not an object")
                continue
            name, sha, size = entry.get("file"), entry.get("sha256"), entry.get("size")
            ok_name = isinstance(name, str) and (
                _EXT_FILE.match(name) if g == "duckdb-ext" else _PLAIN_FILE.match(name)
            )
            if not ok_name:
                problems.append(f"{g}: unsafe file name {name!r}")
            if not (isinstance(sha, str) and _HEX64.match(sha)):
                problems.append(f"{g}: {name!r} has no sha256")
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                problems.append(f"{g}: {name!r} has no size")
    if problems:
        return problems

    jars = [str(e["file"]) for e in groups.get("jars", [])]
    for c in request.jar_coordinates:
        if ivy_jar_name(c) not in jars:
            problems.append(f"jars: {c} is not in the set")
    # UX D2: the one table-format runtime the request names, and no other.
    asked = [c for c in request.jar_coordinates if c.split(":")[1].startswith(_RUNTIME_PREFIXES)]
    held = sorted(j for j in jars if j.split("_", 1)[-1].startswith(_RUNTIME_PREFIXES))
    if len(asked) != 1:
        problems.append(f"the request names {len(asked)} table-format runtimes")
    elif held != [ivy_jar_name(asked[0])]:
        problems.append(f"runtime jars {held} are not exactly {ivy_jar_name(asked[0])}")

    if GROUP_PY_REFERENCE in request.groups:
        problems += _pin_wheels(request.py_reference, groups["py-reference"], "py-reference")
    if GROUP_DUCKDB in request.groups:
        problems += _pin_wheels(
            (f"duckdb=={request.duckdb_version}",), groups["duckdb-wheels"], "duckdb-wheels"
        )
        exts = [_EXT_FILE.match(str(e["file"])) for e in groups["duckdb-ext"]]
        versions = {m.group(1) for m in exts if m}
        platforms = {m.group(2) for m in exts if m}
        names = sorted(m.group(3) for m in exts if m)
        if versions != {request.duckdb_version} or len(platforms) != 1:
            problems.append(
                f"duckdb-ext: versions {sorted(versions)} and platforms {sorted(platforms)}"
                f" are not one platform of v{request.duckdb_version}"
            )
        if names != sorted(request.duckdb_extensions):
            problems.append(f"duckdb-ext: {names} are not {sorted(request.duckdb_extensions)}")

    overlaps = shown.get("overlaps")
    if not isinstance(overlaps, list):
        problems.append("manifest has no overlaps list")
        return problems
    try:
        unknown = unknown_overlaps(overlaps)
    except (KeyError, TypeError) as e:
        problems.append(f"overlaps are malformed: {e}")
    else:
        if unknown:
            problems.append(
                "jars that shadow another version of an image jar and are not in "
                "KNOWN_OVERLAPS: " + ", ".join(f"{o['jar']} over {o['image_jar']}" for o in unknown)
            )
    return problems


# --- the lb-deps-manifest ConfigMap ---------------------------------------------


def manifest_configmap_data(
    request: DepsRequest, shown: Mapping[str, Any], server_pod: str, server_pod_uid: str
) -> dict[str, str]:
    """The ``lb-deps-manifest`` data for a manifest :func:`check_manifest`
    accepted.

    ``manifest.json`` is the server's record plus ``groups`` and
    ``jar_order``: ``lb_deps.py fetch`` reads ``pinset_sha256``, ``groups``
    and ``jar_order`` from it, and ``run`` reads ``request_sha256`` and the
    provenance fields. The pip requirement files carry one ``name==version
    --hash=sha256:<h>`` line per pin (the ``--require-hashes`` form a live
    run installed from)."""
    groups = shown["groups"]
    data = {
        "manifest.json": json.dumps(dict(shown), indent=1, sort_keys=True) + "\n",
        "server-pod": server_pod,
        "server-pod-uid": server_pod_uid,
    }
    by_name = {str(e["file"]): str(e["sha256"]) for e in groups["jars"]}
    data["jars.sha256"] = "".join(f"{by_name[f]}  {f}\n" for f in shown["jar_order"])
    if GROUP_PY_REFERENCE in request.groups:
        data["requirements-py-reference.txt"] = _requirements(
            request.py_reference, groups["py-reference"]
        )
    if GROUP_DUCKDB in request.groups:
        data["requirements-duckdb.txt"] = _requirements(
            (f"duckdb=={request.duckdb_version}",), groups["duckdb-wheels"]
        )
        data["duckdb-ext.sha256"] = "".join(
            f"{e['sha256']}  {e['file']}\n" for e in sorted(groups["duckdb-ext"], key=_file)
        )
    size = sum(len(k) + len(v.encode()) for k, v in data.items())
    if size > MANIFEST_CONFIGMAP_BUDGET:
        raise ValueError(
            f"{MANIFEST_CONFIGMAP} would hold {size} bytes, over its "
            f"{MANIFEST_CONFIGMAP_BUDGET} byte budget"
        )
    return data


def _file(e: Mapping[str, Any]) -> str:
    return str(e["file"])


def _requirements(pins: tuple[str, ...], entries: list[Mapping[str, Any]]) -> str:
    lines = []
    for pin in pins:
        name, _, ver = pin.partition("==")
        (entry,) = [
            e for e in entries if _wheel_name_version(str(e["file"])) == (_norm_dist(name), ver)
        ]
        lines.append(f"{pin} --hash=sha256:{entry['sha256']}\n")
    return "".join(lines)


# --- what consumers read from a handle -----------------------------------------------


class DepsSetMissing(RuntimeError):
    """A Spark job, Thrift or DuckDB pod was about to be built with no
    verified dependency set. ``run`` loads the handle before any submit."""

    def __init__(self, what: str) -> None:
        super().__init__(
            f"{what}: no verified dependency set for this deployment; "
            "run `lakebench deploy <config>` (it starts the dependency server)"
        )


PLACEHOLDER_HOST = "lb-deps.placeholder.invalid"


def placeholder_handle(cfg: Any) -> DepsHandle:
    """A handle for offline manifest builds (the perf fingerprint, dry run):
    the request's jar names, zero hashes and a host that resolves nowhere.
    Never given to anything that submits or applies."""
    from lakebench.deps.request import select_request

    request = select_request(cfg, tools_digest="0" * 64)
    jars = [ivy_jar_name(c) for c in request.jar_coordinates]
    groups: dict[str, list[dict[str, Any]]] = {
        "jars": [{"file": f, "sha256": "0" * 64, "size": 0} for f in jars]
    }
    if GROUP_PY_REFERENCE in request.groups:
        groups["py-reference"] = [
            {
                "file": f"{p.split('==')[0]}-{p.split('==')[1]}-py3-none-any.whl",
                "sha256": "0" * 64,
                "size": 0,
            }
            for p in request.py_reference
        ]
    pinset = pinset_sha256(groups, jars)
    return DepsHandle(
        pinset_sha256=pinset,
        request_sha256=request.request_sha256,
        base_url=f"http://{PLACEHOLDER_HOST}:{PORT}/sets/{pinset}",
        server_pod_uid="",
        manifest={"pinset_sha256": pinset, "groups": groups, "jar_order": jars},
    )


def jar_urls(handle: DepsHandle) -> list[str]:
    """``spark.jars`` for a handle: the set's jars in the manifest's jar
    order, which is the classpath order Spark used for ``--packages``."""
    from urllib.parse import quote

    return [f"{handle.base_url}/jars/{quote(f)}" for f in handle.manifest["jar_order"]]


def delta_jar(handle: DepsHandle) -> str | None:
    """The set's one ``delta-spark_*`` jar, or None for an Iceberg set."""
    hits = [
        f for f in handle.manifest["jar_order"] if f.split("_", 1)[-1].startswith("delta-spark_")
    ]
    return hits[0] if len(hits) == 1 else None


def has_group(handle: DepsHandle, group: str) -> bool:
    return bool((handle.manifest.get("groups") or {}).get(group))


def consumer_context(handle: DepsHandle) -> dict[str, Any]:
    """Template context for the long-lived consumers (Spark Thrift, DuckDB):
    where the set is served, which ConfigMaps to mount, and the Thrift
    classpath in the set's jar order. Built from the handle only, so every
    consumer of one deploy names one pinset."""
    from urllib.parse import urlsplit

    host = urlsplit(handle.base_url).hostname
    return {
        "deps_pinset": handle.pinset_sha256,
        "deps_base_url": handle.base_url,
        "deps_host": host,
        "deps_tools_configmap": tools_configmap_name(handle.request_sha256),
        "deps_manifest_configmap": MANIFEST_CONFIGMAP,
        "deps_tools_mount": TOOLS_MOUNT,
        "deps_manifest_mount": MANIFEST_MOUNT,
        # The conf dir and the image's jars first, as the jobs' parent-first
        # loader sees them, then the set in its jar order (an overlapping image
        # jar wins, as in the jobs; the set's own duplicates resolve in jar
        # order). The launcher drops its own later conf and jars entries as
        # duplicates.
        "deps_thrift_classpath": ":".join(
            [
                "/opt/spark/conf",
                "/opt/spark/jars/*",
                *(f"/extra-jars/{f}" for f in handle.manifest["jar_order"]),
            ]
        ),
    }


def provenance_block(handle: DepsHandle) -> dict[str, Any]:
    """``provenance.deps`` of a run (design s2.8): what set its jobs ran."""
    man = handle.manifest
    return {
        "pinset_sha256": handle.pinset_sha256,
        "request_sha256": handle.request_sha256,
        "repositories": list(man.get("repositories") or []),
        "pypi_index": man.get("pypi_index", ""),
        "groups": {
            g: [{"file": e["file"], "sha256": e["sha256"], "size": e["size"]} for e in entries]
            for g, entries in (man.get("groups") or {}).items()
        },
        "python": dict(man.get("python") or {}),
        "overlaps": list(man.get("overlaps") or []),
        "resolved_at": man.get("resolved_at"),
        "server_pod": handle.server_pod_uid,
        "pods_checked": None,
        "pod_mismatches": [],
    }
