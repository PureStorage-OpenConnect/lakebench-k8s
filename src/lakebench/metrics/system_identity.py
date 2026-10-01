"""The system a run ran on, as a fingerprint.

``observe_system`` samples what the system is, never how loaded it is. Each
part is one flat entry, so a gap is always a whole part:

* ``api_server_ca``: sha256 (12 hex) of the CA bundle the Kubernetes client
  verifies the API server with (the same bytes
  ``deploy/ownership.api_server_fingerprint`` hashes);
* ``kubernetes``: the API server's ``git_version``;
* ``openshift``: ``{version, state}`` of the newest ClusterVersion history
  entry, or None on vanilla Kubernetes (a 404 is an observation, not a gap);
* ``nodes``: the node inventory by hardware class (role, CPU architecture,
  CPU capacity, memory capacity rounded to GiB, and the instance-type or
  NFD CPU-model label when present), with a count per class. Allocatable,
  conditions, cordons and node names never enter: they move with load and
  maintenance, not with the hardware;
* ``storage_endpoint``: the S3 endpoint's lowercased ``host:port``;
* ``storage_backend``: ``s3/conformance.detect_backend`` of the endpoint
  (``aws`` or ``unknown``), the read-only half of the conformance summary;
* ``storage_server``: the ``Server`` header the endpoint answers a HEAD on
  the deployment's bronze bucket with (None when it sends none);
* ``scratch``: whether scratch PVCs are on and their StorageClass name.

The result is ``{type, version, fingerprint, partial, parts}``. A part that
cannot be read (RBAC, an unreachable endpoint, an unparseable node) is
``{"not_observed": reason}``, is left out of the hash, and sets ``partial``.
``type`` enters the hash. ``common_fingerprints(a, b)`` hashes two
observations over the parts both observed, which is how two partial
observations are compared, and ``same_system(a, b)`` is the one test a
repeat check uses for "one system": equal over the common parts, and ``api_server_ca``
among them, since it is the only part that names one cluster rather than
its shape.

Known limits, by design or not yet measured:

* The CA hash is of the bundle the client holds. Two kubeconfigs for one
  cluster that embed different CA bundles (an installer kubeconfig against
  an ``oc login --certificate-authority`` one, or workstation against
  in-cluster) can hash differently; a client that skips TLS verification has
  no CA, so the part is not observed.
* A node added or removed, or a capacity change that crosses a GiB rounding
  boundary, changes the fingerprint (the safe direction). A node replaced by
  one of the same class, or a VM moved to a host with a newer CPU, does not:
  the instance-type label on vSphere encodes CPU count and memory only, and
  the host CPU is visible only through the NFD cpu-model labels. Such a
  hardware change reads as the same system.
* A node that has not yet reported its capacity (still registering) makes
  the node part a gap rather than a zero-sized class.
* One storage system reached by IP and by DNS name reads as two systems.
* Without ``api_server_ca`` on both sides (a local run, a client that skips
  TLS verification, a refused read), the remaining parts describe a
  cluster's shape and version, not which cluster: two sibling clusters on
  one VM template and one storage system hash equal. ``same_system`` is
  False for such a pair.
* ``same_system`` trusts the CA to name one cluster. Clusters whose API
  certificates chain to one shared organisation CA, reached with that CA as
  the client bundle, share the part, so siblings of one shape read as one
  system (the same limit as ``api_server_fingerprint``'s F-3). Not checked
  on the reference cluster, which presents its own installer CA.
* ``storage_backend`` hashes ``detect_backend``'s answer; a change to that
  heuristic changes the part for an unchanged system, so it needs a bump of
  ``SYSTEM_IDENTITY_VERSION`` (pinned by a test).
* Each cluster read has a request timeout (``_REQUEST_TIMEOUT``, at most
  20 s each); the S3 HEAD is bounded by the S3 client's own settings (3
  attempts of 10 s connect and 30 s read, about 120 s at worst).

Nothing here writes to the cluster or to S3, and ``observe_system`` never
raises.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

#: Bump when what a part holds changes; it enters the hash, so fingerprints
#: of different versions never compare equal by accident.
SYSTEM_IDENTITY_VERSION = 2

PARTS = (
    "api_server_ca",
    "kubernetes",
    "openshift",
    "nodes",
    "storage_endpoint",
    "storage_backend",
    "storage_server",
    "scratch",
)

#: The parts read from the cluster itself rather than from the config.
CLUSTER_PARTS = ("api_server_ca", "kubernetes", "openshift", "nodes")

_CONTROL_PLANE_LABELS = (
    "node-role.kubernetes.io/control-plane",
    "node-role.kubernetes.io/master",
)
_INSTANCE_TYPE_LABELS = (
    "node.kubernetes.io/instance-type",
    "beta.kubernetes.io/instance-type",
)
_NFD_CPU_MODEL = (
    "feature.node.kubernetes.io/cpu-model.vendor_id",
    "feature.node.kubernetes.io/cpu-model.family",
    "feature.node.kubernetes.io/cpu-model.id",
)

_GIB = 1024**3

#: (connect, read) seconds for each Kubernetes read: an API server that
#: stops answering must not hang the run (the client default waits forever).
_REQUEST_TIMEOUT = (5, 15)


def not_observed(reason: str) -> dict[str, str]:
    return {"not_observed": reason}


def is_observed(part: Any) -> bool:
    return not (isinstance(part, Mapping) and "not_observed" in part)


def observed_parts(parts: Mapping[str, Any]) -> set[str]:
    return {k for k in PARTS if k in parts and is_observed(parts[k])}


def fingerprint_of(
    parts: Mapping[str, Any],
    keys: Iterable[str] | None = None,
    system_type: str = "cluster",
    version: int = SYSTEM_IDENTITY_VERSION,
) -> str:
    """sha256 over the observed parts named by *keys* (all when None), the
    system type and the identity version, as sorted JSON, first 16 hex
    digits."""
    chosen = PARTS if keys is None else tuple(keys)
    body: dict[str, Any] = {k: parts[k] for k in chosen if k in parts and is_observed(parts[k])}
    body["_type"] = system_type
    body["_version"] = version
    blob = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def common_fingerprints(a: Mapping[str, Any], b: Mapping[str, Any]) -> tuple[str, str, list[str]]:
    """Fingerprints of two observations over the parts both observed, and
    those part names. Each side hashes with its own type and version, so
    observations of different versions never compare equal. Equal
    fingerprints over no part of ``CLUSTER_PARTS`` are not evidence of one
    system; a caller must not read such a pair as a repeat."""
    pa, pb = a.get("parts") or {}, b.get("parts") or {}
    keys = sorted(observed_parts(pa) & observed_parts(pb))
    return (
        fingerprint_of(pa, keys, str(a.get("type") or "cluster"), _version_of(a)),
        fingerprint_of(pb, keys, str(b.get("type") or "cluster"), _version_of(b)),
        keys,
    )


def same_system(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    """True when *a* and *b* are evidence of one system: equal over the
    parts both observed, and those include ``api_server_ca``. Without the
    CA on both sides the common parts describe a cluster's shape, which two
    clusters can share, so the answer is False (not known to be one)."""
    fa, fb, keys = common_fingerprints(a, b)
    return "api_server_ca" in keys and fa == fb


def _version_of(obs: Mapping[str, Any]) -> int:
    """The observation's identity version; -1 when missing or malformed, so
    it never matches a current observation."""
    v = obs.get("version")
    return v if isinstance(v, int) and not isinstance(v, bool) else -1


def _reason(exc: BaseException) -> str:
    status = getattr(exc, "status", None)
    if status in (401, 403):
        return f"forbidden ({status})"
    first = str(exc).splitlines()[0][:120] if str(exc) else ""
    return f"{type(exc).__name__}: {first}" if first else type(exc).__name__


# ---------------------------------------------------------------------------
# Parts
# ---------------------------------------------------------------------------


def _api_server_ca(k8s: Any) -> Any:
    """The CA the client itself verifies with (its ``ssl_ca_cert``), so the
    hash follows the context the client is pinned to, not whatever the
    kubeconfig's current-context is when this runs."""
    configuration = getattr(getattr(k8s._core_v1, "api_client", None), "configuration", None)
    path = getattr(configuration, "ssl_ca_cert", None)
    if path:
        try:
            with open(path, "rb") as fh:
                data = fh.read()
        except OSError as exc:
            return not_observed(f"CA bundle: {_reason(exc)}")
        if data:
            return hashlib.sha256(data).hexdigest()[:12]
    if getattr(configuration, "verify_ssl", True) is False:
        return not_observed("client skips TLS verification")
    return not_observed("client holds no CA bundle")


def _kubernetes(k8s: Any) -> Any:
    from kubernetes import client

    try:
        version = client.VersionApi(k8s._core_v1.api_client).get_code(
            _request_timeout=_REQUEST_TIMEOUT
        )
    except Exception as exc:  # noqa: BLE001 -- recorded as not observed
        return not_observed(f"kubernetes version: {_reason(exc)}")
    return getattr(version, "git_version", None)


def _openshift(k8s: Any) -> Any:
    try:
        cv = k8s._custom.get_cluster_custom_object(
            group="config.openshift.io",
            version="v1",
            plural="clusterversions",
            name="version",
            _request_timeout=_REQUEST_TIMEOUT,
        )
    except Exception as exc:  # noqa: BLE001
        if getattr(exc, "status", None) == 404:
            return None  # not OpenShift: an observed fact, not a gap
        return not_observed(f"openshift clusterversion: {_reason(exc)}")
    history = ((cv or {}).get("status") or {}).get("history") or []
    if not history:
        return {"version": None, "state": None}
    # history[0] is the newest entry; during an upgrade it is the target with
    # state Partial, so the state is part of what identifies the cluster.
    newest = history[0] or {}
    return {"version": newest.get("version"), "state": newest.get("state")}


def _memory_gib(quantity: str) -> int:
    from lakebench.k8s.client import K8sClient

    return round(K8sClient._parse_memory_to_bytes(quantity) / _GIB)


def _cpu_cores(quantity: str) -> float:
    from lakebench.k8s.client import K8sClient

    millis = K8sClient._parse_cpu_to_millicores(quantity)
    return millis / 1000 if millis % 1000 else millis // 1000


def _hardware_class(node: Any) -> dict[str, Any]:
    labels = (node.metadata.labels or {}) if node.metadata else {}
    status = node.status
    capacity = (status.capacity or {}) if status else {}
    if not capacity.get("cpu") or not capacity.get("memory"):
        raise ValueError("capacity not reported yet")
    info = getattr(status, "node_info", None) if status else None
    model = next((labels[k] for k in _INSTANCE_TYPE_LABELS if labels.get(k)), None)
    if model is None and any(labels.get(k) for k in _NFD_CPU_MODEL):
        model = "/".join(str(labels.get(k, "")) for k in _NFD_CPU_MODEL)
    return {
        "role": "control-plane" if any(k in labels for k in _CONTROL_PLANE_LABELS) else "worker",
        "architecture": getattr(info, "architecture", None),
        "cpu": _cpu_cores(str(capacity["cpu"])),
        "memory_gib": _memory_gib(str(capacity["memory"])),
        "model": model,
    }


def _nodes(k8s: Any) -> Any:
    try:
        nodes = k8s._core_v1.list_node(_request_timeout=_REQUEST_TIMEOUT)
    except Exception as exc:  # noqa: BLE001
        return not_observed(f"node list: {_reason(exc)}")
    counts: dict[str, dict[str, Any]] = {}
    for node in nodes.items or []:
        try:
            hw = _hardware_class(node)
        except Exception as exc:  # noqa: BLE001
            # A partial inventory would read as a smaller fleet: the whole
            # part is a gap instead.
            name = getattr(getattr(node, "metadata", None), "name", "?")
            return not_observed(f"node {name}: {_reason(exc)}")
        key = json.dumps(hw, sort_keys=True, default=str)
        counts.setdefault(key, {**hw, "count": 0})["count"] += 1
    if not counts:
        return not_observed("node list is empty")
    return [counts[k] for k in sorted(counts)]


def _storage_endpoint(cfg: Any) -> Any:
    endpoint = str(cfg.platform.storage.s3.endpoint or "").strip()
    parsed = urlparse(endpoint if "://" in endpoint else f"//{endpoint}")
    host = (parsed.hostname or "").lower()
    if not host:
        return not_observed("no S3 endpoint configured")
    try:
        port = parsed.port
    except ValueError:
        port = None
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    return f"{host}:{port}"


def _storage_backend(cfg: Any) -> Any:
    """The conformance module's backend guess. The conformance runner itself
    writes a temporary bucket and objects, so it never runs here; this and
    the ``Server`` header stand in for its summary (ch03 section 6, main-lane
    decision 2026-10-01)."""
    from lakebench.s3.conformance import detect_backend

    endpoint = str(cfg.platform.storage.s3.endpoint or "").strip()
    if not endpoint:
        return not_observed("no S3 endpoint configured")
    return detect_backend(endpoint)


def _storage_server(cfg: Any, s3_client: Any) -> Any:
    if s3_client is None:
        return not_observed("no S3 client")
    try:
        resp = s3_client.raw_client.head_bucket(Bucket=cfg.platform.storage.s3.buckets.bronze)
        headers = (resp.get("ResponseMetadata") or {}).get("HTTPHeaders") or {}
    except Exception as exc:  # noqa: BLE001
        # A refused HEAD still carries the server's headers.
        headers = ((getattr(exc, "response", None) or {}).get("ResponseMetadata") or {}).get(
            "HTTPHeaders"
        ) or {}
        if not headers:
            return not_observed(f"head bucket: {_reason(exc)}")
    # An absent header was observed: the endpoint answered without one.
    return {k.lower(): v for k, v in headers.items()}.get("server") or None


def _scratch(cfg: Any) -> dict[str, Any]:
    scratch = cfg.platform.storage.scratch
    return {
        "enabled": bool(scratch.enabled),
        "storage_class": scratch.storage_class if scratch.enabled else None,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def observe_system(
    k8s: Any, cfg: Any, *, s3_client: Any = None, local: bool = False
) -> dict[str, Any]:
    """``{type, version, fingerprint, partial, parts}`` for the system *cfg*
    runs on.

    *k8s* is a ``K8sClient`` (None or *local* for a ``--local`` run, whose
    cluster parts are then not observed); *s3_client* an ``S3Client`` used
    for one HEAD on the bronze bucket (None leaves the Server header not
    observed). Never raises.
    """
    system_type = "local" if local else "cluster"
    readers: list[tuple[str, Any]] = [
        ("api_server_ca", _api_server_ca),
        ("kubernetes", _kubernetes),
        ("openshift", _openshift),
        ("nodes", _nodes),
    ]
    parts: dict[str, Any] = {}
    for name, read in readers:
        if local or k8s is None:
            parts[name] = not_observed("local run" if local else "no Kubernetes client")
            continue
        parts[name] = _safely(name, lambda read=read: read(k8s))
    parts["storage_endpoint"] = _safely("storage_endpoint", lambda: _storage_endpoint(cfg))
    parts["storage_backend"] = _safely("storage_backend", lambda: _storage_backend(cfg))
    parts["storage_server"] = _safely("storage_server", lambda: _storage_server(cfg, s3_client))
    parts["scratch"] = _safely("scratch", lambda: _scratch(cfg))
    return {
        "type": system_type,
        "version": SYSTEM_IDENTITY_VERSION,
        "fingerprint": fingerprint_of(parts, system_type=system_type),
        "partial": len(observed_parts(parts)) < len(PARTS),
        "parts": parts,
    }


def _safely(name: str, read: Any) -> Any:
    try:
        return read()
    except Exception as exc:  # noqa: BLE001 -- a reader bug is a gap, not a crash
        logger.debug("system identity part %s: %s", name, exc)
        return not_observed(_reason(exc))
