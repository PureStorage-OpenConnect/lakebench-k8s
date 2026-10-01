"""The system a run ran on, as a fingerprint (SPEC v1.7 EVD-7, OD-2; SP-1).

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
observations are compared, and ``same_system(a, b)`` is the one test ER-10b
uses for "one system": equal over the common parts, and ``api_server_ca``
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

``observe_load`` samples how loaded the cluster is (K31): allocatable CPU
and memory over schedulable worker nodes, and the CPU and memory requested
by every other namespace's scheduled, non-terminal pods on those nodes.
That is Observational evidence, never part of the fingerprint.
``sample_run_start`` and ``sample_run_end`` are the run's two calls: they
write ``experiment_inputs.system_identity`` (start only) and
``experiment_inputs.observed`` into the run's config snapshot, from which
``build_experiment`` copies them into the experiment block.

Nothing here writes to the cluster or to S3, and no function here raises.
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
    system; the caller (ER-10b) must not read such a pair as a repeat."""
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


# ---------------------------------------------------------------------------
# Load (K31): allocatable and co-tenant requests, sampled at start and end
# ---------------------------------------------------------------------------

#: Pod lists can be large on a shared cluster; give the read longer.
_POD_LIST_TIMEOUT = (5, 60)


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _quantities(requests: Any) -> tuple[int, int]:
    """(millicores, bytes) of a ``{cpu, memory}`` request mapping."""
    from lakebench.k8s.client import K8sClient

    requests = requests or {}
    cpu, mem = requests.get("cpu"), requests.get("memory")
    return (
        K8sClient._parse_cpu_to_millicores(str(cpu)) if cpu else 0,
        K8sClient._parse_memory_to_bytes(str(mem)) if mem else 0,
    )


def _pod_request(pod: Any) -> tuple[int, int]:
    """(millicores, bytes) a pod requests as the scheduler counts it: the
    larger of the sum over its containers and the largest init container,
    per resource, plus the pod overhead."""
    spec = pod.spec

    def of(containers: Any) -> list[tuple[int, int]]:
        return [
            _quantities(getattr(getattr(c, "resources", None), "requests", None))
            for c in containers or []
        ]

    main, init = of(spec.containers), of(getattr(spec, "init_containers", None))
    o_cpu, o_mem = _quantities(getattr(spec, "overhead", None))
    cpu = max(sum(c for c, _ in main), max((c for c, _ in init), default=0)) + o_cpu
    mem = max(sum(m for _, m in main), max((m for _, m in init), default=0)) + o_mem
    return cpu, mem


def observe_load(k8s: Any, namespace: str, *, local: bool = False) -> dict[str, Any]:
    """One load sample: ``{at, allocatable: {cpu, memory_gib, nodes},
    cotenant_requested: {cpu, memory_gib, pods}}``.

    Allocatable is summed over worker nodes (no control-plane role) that
    are schedulable (not cordoned). Co-tenant requests are those of pods in
    every namespace except *namespace* that are scheduled to one of those
    nodes and not Succeeded or Failed. A refused or failed read makes that
    half ``{"not_observed": reason}``. Never raises."""
    from lakebench.k8s.client import K8sClient

    at = _now()
    if local or k8s is None:
        reason = "local run" if local else "no Kubernetes client"
        return {
            "at": at,
            "allocatable": not_observed(reason),
            "cotenant_requested": not_observed(reason),
        }
    out: dict[str, Any] = {"at": at}
    workers: set[str] = set()
    try:
        nodes = k8s._core_v1.list_node(_request_timeout=_REQUEST_TIMEOUT)
        cpu = mem = 0
        for node in nodes.items or []:
            labels = (node.metadata.labels or {}) if node.metadata else {}
            if any(k in labels for k in _CONTROL_PLANE_LABELS):
                continue
            if getattr(node.spec, "unschedulable", False):
                continue
            alloc = (node.status.allocatable or {}) if node.status else {}
            cpu += K8sClient._parse_cpu_to_millicores(str(alloc.get("cpu", "0")))
            mem += K8sClient._parse_memory_to_bytes(str(alloc.get("memory", "0")))
            workers.add(node.metadata.name)
        out["allocatable"] = {
            "cpu": round(cpu / 1000, 3),
            "memory_gib": round(mem / _GIB, 1),
            "nodes": len(workers),
        }
    except Exception as exc:  # noqa: BLE001
        out["allocatable"] = not_observed(f"node list: {_reason(exc)}")
    if not workers:
        out["cotenant_requested"] = not_observed(
            "no schedulable worker node observed"
            if is_observed(out["allocatable"])
            else "allocatable not observed"
        )
        return out
    try:
        pods = k8s._core_v1.list_pod_for_all_namespaces(
            field_selector="status.phase!=Succeeded,status.phase!=Failed",
            _request_timeout=_POD_LIST_TIMEOUT,
        )
        cpu = mem = count = 0
        for pod in pods.items or []:
            if getattr(pod.metadata, "namespace", None) == namespace:
                continue
            if getattr(pod.spec, "node_name", None) not in workers:
                continue
            c, m = _pod_request(pod)
            cpu, mem, count = cpu + c, mem + m, count + 1
        out["cotenant_requested"] = {
            "cpu": round(cpu / 1000, 3),
            "memory_gib": round(mem / _GIB, 1),
            "pods": count,
        }
    except Exception as exc:  # noqa: BLE001
        out["cotenant_requested"] = not_observed(f"cluster-wide pod list: {_reason(exc)}")
    return out


def _inputs(run: Any) -> dict[str, Any] | None:
    snapshot = getattr(run, "config_snapshot", None)
    inputs = snapshot.get("experiment_inputs") if isinstance(snapshot, dict) else None
    return inputs if isinstance(inputs, dict) else None


def _clients(cfg: Any, local: bool, *, s3_too: bool = True) -> tuple[Any, Any]:
    if local:
        return None, None
    k8s = s3 = None
    try:
        from lakebench.k8s import get_k8s_client

        k8s = get_k8s_client(context=cfg.platform.kubernetes.context, namespace=cfg.get_namespace())
    except Exception as exc:  # noqa: BLE001
        logger.debug("system identity: no Kubernetes client: %s", exc)
    if not s3_too or k8s is None:
        # Without a cluster client this is not a cluster run; skip the HEAD.
        return k8s, None
    try:
        from lakebench.s3 import S3Client

        s3c = cfg.platform.storage.s3
        s3 = S3Client(
            endpoint=s3c.endpoint,
            access_key=s3c.access_key,
            secret_key=s3c.secret_key,
            region=s3c.region,
            path_style=s3c.path_style,
            ca_cert=s3c.ca_cert,
            verify_ssl=s3c.verify_ssl,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("system identity: no S3 client: %s", exc)
    return k8s, s3


def sample_run_start(run: Any, cfg: Any, *, local: bool = False, k8s: Any = None) -> None:
    """At run start: write the system identity and the first load sample
    into *run*'s ``experiment_inputs`` (``system_identity``, ``observed``).
    *k8s* overrides the client built from *cfg* (tests). Never raises."""
    try:
        inputs = _inputs(run)
        if inputs is None:
            return
        built_k8s, s3 = _clients(cfg, local) if k8s is None else (k8s, None)
        inputs["system_identity"] = observe_system(built_k8s, cfg, s3_client=s3, local=local)
        _record_load(inputs, "start", observe_load(built_k8s, cfg.get_namespace(), local=local))
    except Exception as exc:  # noqa: BLE001 -- evidence, never a run failure
        logger.warning("system identity not sampled at run start: %s", exc)


def sample_run_end(run: Any, cfg: Any, *, local: bool = False, k8s: Any = None) -> None:
    """Before the record is saved: the second load sample. The system
    identity is not re-read (a changed system mid-run reads in the load
    sample's allocatable, not in the fingerprint). Never raises."""
    try:
        inputs = _inputs(run)
        if inputs is None:
            return
        if k8s is None and not local:
            k8s, _ = _clients(cfg, local, s3_too=False)
        _record_load(inputs, "end", observe_load(k8s, cfg.get_namespace(), local=local))
    except Exception as exc:  # noqa: BLE001
        logger.warning("load not sampled at run end: %s", exc)


def _record_load(inputs: dict[str, Any], when: str, sample: Mapping[str, Any]) -> None:
    """``observed = {allocatable: {start, end}, cotenant_requested: {start,
    end}}``, each leaf the half of one sample with its ``at``."""
    observed = inputs.setdefault("observed", {})
    for half in ("allocatable", "cotenant_requested"):
        leaf = dict(sample.get(half) or not_observed("not sampled"))
        leaf["at"] = sample.get("at")
        observed.setdefault(half, {})[when] = leaf
