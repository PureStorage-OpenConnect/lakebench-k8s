"""The system a run ran on, as a fingerprint (SPEC v1.7 EVD-7, OD-2; SP-1).

``observe_system`` samples what the system is, never how loaded it is:

* ``api_server_ca``: the cluster CA hash (``deploy/ownership.api_server_fingerprint``);
* ``platform``: Kubernetes version and, on OpenShift, the ClusterVersion;
* ``nodes``: the node inventory by hardware class (role, CPU architecture,
  CPU capacity, memory capacity rounded to GiB, and the instance-type or
  NFD CPU-model label when present). Allocatable, conditions, cordons and
  node names never enter: they move with load and maintenance, not with
  the hardware;
* ``storage``: the S3 endpoint host, the ``Server`` header the endpoint
  answers with, and the backend lakebench recognises from the endpoint;
* ``scratch``: whether scratch PVCs are on and their StorageClass name.

The result is ``{type, fingerprint, partial, parts}``. A part that cannot be
read (RBAC, an unreachable endpoint) is ``{"not_observed": reason}``, is left
out of the hash, and sets ``partial``. ``fingerprint_of(parts, keys)`` hashes
a chosen subset, so two partial observations can be compared on the parts
both observed. Values the system decides for a run (the endpoint written
into Spark conf, autosized resources) are recorded elsewhere and are not
architecture.

Nothing here writes to the cluster or to S3.
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
SYSTEM_IDENTITY_VERSION = 1

#: The parts, in the order they are documented above.
PARTS = ("api_server_ca", "platform", "nodes", "storage", "scratch")

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


def not_observed(reason: str) -> dict[str, str]:
    return {"not_observed": reason}


def is_observed(part: Any) -> bool:
    return not (isinstance(part, Mapping) and "not_observed" in part)


def _observed_only(value: Any) -> Any:
    """*value* with every not-observed entry removed, at any depth, so the
    reason text never enters a hash."""
    if isinstance(value, Mapping):
        return {k: _observed_only(v) for k, v in value.items() if is_observed(v)}
    if isinstance(value, list):
        return [_observed_only(v) for v in value if is_observed(v)]
    return value


def _has_gap(value: Any) -> bool:
    if not is_observed(value):
        return True
    if isinstance(value, Mapping):
        return any(_has_gap(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_gap(v) for v in value)
    return False


def fingerprint_of(parts: Mapping[str, Any], keys: Iterable[str] | None = None) -> str:
    """sha256 over the observed parts named by *keys* (all when None), as
    sorted JSON, first 16 hex digits. Not-observed entries are left out at
    any depth."""
    chosen = PARTS if keys is None else tuple(keys)
    body = {k: _observed_only(parts[k]) for k in chosen if k in parts and is_observed(parts[k])}
    body["version"] = SYSTEM_IDENTITY_VERSION
    blob = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


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
    from lakebench.deploy.ownership import api_server_fingerprint

    ca = api_server_fingerprint(getattr(k8s, "context_name", "") or None)
    return {"sha256_12": ca} if ca else not_observed("no CA material in the kubeconfig")


def _platform(k8s: Any) -> Any:
    from kubernetes import client

    core = k8s._core_v1
    try:
        version = client.VersionApi(core.api_client).get_code()
    except Exception as exc:  # noqa: BLE001 -- recorded as not observed
        return not_observed(f"kubernetes version: {_reason(exc)}")
    out: dict[str, Any] = {
        "kubernetes": getattr(version, "git_version", None),
        "openshift": None,
    }
    try:
        cv = k8s._custom.get_cluster_custom_object(
            group="config.openshift.io", version="v1", plural="clusterversions", name="version"
        )
    except Exception as exc:  # noqa: BLE001
        if getattr(exc, "status", None) == 404:
            return out  # not OpenShift: an observed fact, not a gap
        return not_observed(f"openshift clusterversion: {_reason(exc)}")
    history = ((cv or {}).get("status") or {}).get("history") or []
    out["openshift"] = (history[0] or {}).get("version") if history else None
    return out


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
    info = getattr(status, "node_info", None) if status else None
    model = next((labels[k] for k in _INSTANCE_TYPE_LABELS if labels.get(k)), None)
    if model is None and any(labels.get(k) for k in _NFD_CPU_MODEL):
        model = "/".join(str(labels.get(k, "")) for k in _NFD_CPU_MODEL)
    return {
        "role": "control-plane" if any(k in labels for k in _CONTROL_PLANE_LABELS) else "worker",
        "architecture": getattr(info, "architecture", None),
        "cpu": _cpu_cores(str(capacity.get("cpu", "0"))),
        "memory_gib": _memory_gib(str(capacity.get("memory", "0"))),
        "model": model,
    }


def _nodes(k8s: Any) -> Any:
    try:
        nodes = k8s._core_v1.list_node()
    except Exception as exc:  # noqa: BLE001
        return not_observed(f"node list: {_reason(exc)}")
    counts: dict[str, dict[str, Any]] = {}
    for node in nodes.items or []:
        hw = _hardware_class(node)
        key = json.dumps(hw, sort_keys=True, default=str)
        counts.setdefault(key, {**hw, "count": 0})["count"] += 1
    if not counts:
        return not_observed("node list is empty")
    return [counts[k] for k in sorted(counts)]


def _storage(cfg: Any, s3_client: Any) -> Any:
    from lakebench.s3.conformance import detect_backend

    s3 = cfg.platform.storage.s3
    endpoint = str(s3.endpoint or "")
    host = urlparse(endpoint).hostname if "://" in endpoint else endpoint.partition(":")[0]
    if not host:
        return not_observed("no S3 endpoint configured")
    out: dict[str, Any] = {"endpoint_host": host, "backend": detect_backend(endpoint)}
    if s3_client is None:
        out["server"] = not_observed("no S3 client")
        return out
    try:
        raw = s3_client.raw_client
        resp = raw.head_bucket(Bucket=s3.buckets.bronze)
        headers = (resp.get("ResponseMetadata") or {}).get("HTTPHeaders") or {}
    except Exception as exc:  # noqa: BLE001
        # A refused HEAD still carries the server's headers.
        headers = ((getattr(exc, "response", None) or {}).get("ResponseMetadata") or {}).get(
            "HTTPHeaders"
        ) or {}
        if not headers:
            out["server"] = not_observed(f"head bucket: {_reason(exc)}")
            return out
    server = {k.lower(): v for k, v in headers.items()}.get("server")
    out["server"] = server if server else not_observed("endpoint sent no Server header")
    return out


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
    """``{type, fingerprint, partial, parts}`` for the system *cfg* runs on.

    *k8s* is a ``K8sClient`` (None or *local* for a ``--local`` run, whose
    cluster parts are then not observed); *s3_client* an ``S3Client`` used
    for one HEAD on the bronze bucket (None leaves the Server header not
    observed). Never raises for an unreadable part.
    """
    parts: dict[str, Any] = {}
    if local or k8s is None:
        reason = "local run" if local else "no Kubernetes client"
        for name in ("api_server_ca", "platform", "nodes"):
            parts[name] = not_observed(reason)
    else:
        for name, read in (
            ("api_server_ca", _api_server_ca),
            ("platform", _platform),
            ("nodes", _nodes),
        ):
            try:
                parts[name] = read(k8s)
            except Exception as exc:  # noqa: BLE001 -- a reader bug is a gap, not a crash
                logger.debug("system identity part %s: %s", name, exc)
                parts[name] = not_observed(_reason(exc))
    try:
        parts["storage"] = _storage(cfg, s3_client)
    except Exception as exc:  # noqa: BLE001
        parts["storage"] = not_observed(_reason(exc))
    parts["scratch"] = _scratch(cfg)
    return {
        "type": "local" if local else "cluster",
        "version": SYSTEM_IDENTITY_VERSION,
        "fingerprint": fingerprint_of(parts),
        "partial": _has_gap(parts),
        "parts": parts,
    }
