"""The run-start checks on the deployment's dependency set.

``load_handle`` is called by every command that submits a Spark job
(``run``, continuous runs, ``financial``) before anything is recorded or
submitted. It returns the verified handle the jobs name their jars from, or
raises a typed CLI error that says what to do:

1. the namespace annotation ``lakebench.deployment/deps-set`` is absent: a
   v1.6 deployment (no ``lb-deps-manifest``), or a deploy that did not
   finish (the ConfigMap is there, the annotation is not);
2. the request this config builds now differs from the one deployed (the
   fields that differ are named);
3. the ConfigMap's manifest does not check against the request, or names
   another pinset than the annotation;
4. the server has no Ready pod of this request, or its pod was replaced and
   now serves another set;
5. a Spark Thrift or DuckDB pod runs another set.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from lakebench.deps import manifest as m
from lakebench.deps.request import DepsRequest, pinset_sha256, select_request
from lakebench.exit_codes import PrerequisiteError, SafetyRefusal

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig

logger = logging.getLogger(__name__)

REDEPLOY = "run `lakebench deploy {config}` (it resolves and serves the set), then run again"
QUERY_ENGINE_DEPLOYMENTS = {"spark-thrift": "lakebench-spark-thrift", "duckdb": "lakebench-duckdb"}


_CONFIG_PATH: list[str] = []


def _fix(cfg: Any) -> str:
    return REDEPLOY.format(config=_CONFIG_PATH[-1] if _CONFIG_PATH else "<config>")


def _live(pod: Any) -> bool:
    phase = pod.status.phase if pod.status is not None else None
    return not pod.metadata.deletion_timestamp and phase not in ("Failed", "Succeeded")


def _request_diff(deployed: dict[str, Any], current: DepsRequest) -> list[str]:
    """The request fields that differ (``tools_sha256`` is the resolver
    shipped with Lakebench)."""
    now = json.loads(current.canonical_json())
    return [k for k in sorted(set(deployed) | set(now)) if deployed.get(k) != now.get(k)]


def load_handle(cfg: LakebenchConfig, k8s: Any, config_path: Any = None) -> m.DepsHandle:
    """The deployment's verified dependency set, or a typed refusal. A
    cluster read that fails is a refusal too (exit 4, named), never an
    unclassified error."""
    from lakebench.exit_codes import LakebenchError

    _CONFIG_PATH[:] = [str(config_path)] if config_path else []
    try:
        return _load_handle(cfg, k8s)
    except LakebenchError:
        raise
    except Exception as e:
        if not _is_cluster_error(e):
            raise  # a config or programming error is not a cluster read
        status = getattr(e, "status", None)
        reason = getattr(e, "reason", None) or type(e).__name__
        raise PrerequisiteError(
            "cannot read the deployment's dependency set",
            why=f"{status} {reason}".strip() if status else f"{reason}: {str(e)[:200]}",
            next="check the kube-context and that you may read pods, ConfigMaps, "
            "Deployments and the namespace in it",
            where=f"namespace {cfg.get_namespace()}",
            path="run.deps_stale",
        ) from e


def _is_cluster_error(e: BaseException) -> bool:
    from kubernetes.client.rest import ApiException

    from lakebench.k8s import K8sConnectionError

    name = type(e).__name__
    return isinstance(e, (ApiException, OSError, K8sConnectionError)) or name in (
        "MaxRetryError",
        "NewConnectionError",
        "ProtocolError",
        "ReadTimeoutError",
    )


def _load_handle(cfg: LakebenchConfig, k8s: Any) -> m.DepsHandle:
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    ns = cfg.get_namespace()
    if k8s is None:
        from lakebench.k8s import get_k8s_client

        # Also loads the config's kube-context for the API classes below.
        k8s = get_k8s_client(context=cfg.platform.kubernetes.context, namespace=ns)
    core = k8s_client.CoreV1Api()
    where = f"namespace {ns}"
    try:
        anns = core.read_namespace(ns).metadata.annotations or {}
    except ApiException as e:
        if e.status == 404:
            raise PrerequisiteError(
                f"namespace {ns} does not exist",
                next=_fix(cfg),
                where=where,
                path="run.deps_missing",
            ) from e
        raise
    pinset = anns.get(m.ANNOTATION_DEPS_SET)
    try:
        cm = core.read_namespaced_config_map(m.MANIFEST_CONFIGMAP, ns)
    except ApiException as e:
        if e.status != 404:
            raise
        cm = None
    if cm is None:
        raise PrerequisiteError(
            "this deployment has no dependency server",
            why="it was deployed by Lakebench 1.6 or earlier, or never finished a deploy",
            next=_fix(cfg),
            where=where,
            path="run.deps_missing",
        )
    if not pinset:
        raise PrerequisiteError(
            "this deployment's dependency set is not verified",
            why="the last deploy did not finish its dependency server step, or a deploy "
            "or destroy is running",
            next=_fix(cfg),
            where=where,
            path="run.deps_stale",
        )
    data = cm.data or {}
    try:
        shown = json.loads(data["manifest.json"])
    except (KeyError, TypeError, ValueError) as e:
        raise PrerequisiteError(
            f"ConfigMap {m.MANIFEST_CONFIGMAP} holds no manifest",
            next=_fix(cfg),
            where=where,
            path="run.deps_stale",
        ) from e

    request = select_request(cfg)
    if shown.get("request_sha256") != request.request_sha256:
        deployed = _deployed_request(core, ns, shown.get("request_sha256"))
        changed = _request_diff(deployed, request) if deployed is not None else []
        what = ", ".join(changed) if changed else "the request"
        raise PrerequisiteError(
            "the dependency set was resolved for another request than this config's",
            why=f"changed since deploy: {what} (a config edit, or another Lakebench "
            "version than the one that deployed)",
            next=_fix(cfg),
            where=where,
            path="run.deps_stale",
        )
    problems = m.check_manifest(request, shown)
    if problems or shown.get("pinset_sha256") != pinset:
        raise SafetyRefusal(
            "the recorded dependency set does not check",
            why="; ".join(problems)
            or f"ConfigMap names {shown.get('pinset_sha256')}, the namespace {pinset}",
            next=_fix(cfg),
            where=where,
            path="run.deps_mismatch",
        )

    server = _ready_server_pod(core, ns, request.request_sha256)
    if server is None:
        raise PrerequisiteError(
            f"the dependency server {m.SERVER_NAME} has no Ready pod",
            why="its pod is restarting or was lost with its node",
            next=f"wait for pod {m.SERVER_NAME} in {ns} to be Ready, or {_fix(cfg)}",
            where=where,
            path="run.deps_stale",
        )
    if server.metadata.uid != data.get("server-pod-uid"):
        served, err = _served_pinset(k8s, ns, server, request.request_sha256)
        if served is None:
            raise PrerequisiteError(
                f"cannot read the set the restarted dependency server {server.metadata.name} serves",
                why=err,
                next=f"check that you may exec into pods in {ns}, or {_fix(cfg)}",
                where=where,
                path="run.deps_stale",
            )
        if served != pinset:
            raise SafetyRefusal(
                "the dependency server was replaced and serves another set",
                why=f"pod {server.metadata.name} serves {served}, the deployment recorded {pinset}",
                next=_fix(cfg),
                where=where,
                path="run.deps_mismatch",
            )
    stale = _consumer_mismatches(core, ns, cfg, pinset)
    if stale:
        raise SafetyRefusal(
            "a query engine pod runs another dependency set",
            why=", ".join(f"{p['pod']} on {p['pinset']}" for p in stale),
            next=_fix(cfg),
            where=where,
            path="run.deps_mismatch",
        )
    return m.DepsHandle(
        pinset_sha256=pinset,
        request_sha256=request.request_sha256,
        base_url=m.base_url(ns, pinset),
        server_pod_uid=str(server.metadata.uid),
        manifest=shown,
    )


def _deployed_request(core: Any, ns: str, request_sha: str | None) -> dict[str, Any] | None:
    """The deployed request, from its immutable tools ConfigMap."""
    if not request_sha:
        return None
    try:
        cm = core.read_namespaced_config_map(m.tools_configmap_name(request_sha), ns)
        return dict(json.loads((cm.data or {})["request.json"]))
    except Exception:  # noqa: BLE001 -- only names what changed
        return None


def _ready_server_pod(core: Any, ns: str, request_sha: str) -> Any | None:
    selector = ",".join(f"{k}={v}" for k, v in m.SELECTOR_LABELS.items())
    for pod in core.list_namespaced_pod(ns, label_selector=selector).items:
        if pod.metadata.deletion_timestamp:
            continue
        if (pod.metadata.annotations or {}).get(m.POD_ANNOTATION_REQUEST) != request_sha:
            continue
        st = pod.status
        if st is not None and st.phase == "Running":
            if any(c.type == "Ready" and c.status == "True" for c in (st.conditions or [])):
                return pod
    return None


def _served_pinset(k8s: Any, ns: str, pod: Any, request_sha: str) -> tuple[str | None, str]:
    """The pinset a replaced server pod verified, recomputed from its
    entries, or None with why it could not be read."""
    cmd = ["python3", f"{m.TOOLS_MOUNT}/lb_deps.py", "show", "--request", request_sha]
    rc, out, err = k8s.exec_in_pod(
        pod.metadata.name, cmd, namespace=ns, container=m.SERVE_CONTAINER, timeout=120
    )
    if rc != 0:
        return None, f"lb_deps.py show exited {rc}: {(err or out).strip()[:300]}"
    try:
        shown = json.loads(out)
        return pinset_sha256(shown["groups"], shown["jar_order"]), ""
    except (KeyError, TypeError, ValueError) as e:
        return None, f"lb_deps.py show printed no manifest: {e}"


def _consumer_mismatches(core: Any, ns: str, cfg: Any, pinset: str) -> list[dict[str, str]]:
    """Thrift or DuckDB pods whose ``lakebench.io/deps-set`` is not ``pinset``."""
    name = QUERY_ENGINE_DEPLOYMENTS.get(cfg.architecture.query_engine.type.value)
    if name is None:
        return []
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    try:
        dep = k8s_client.AppsV1Api().read_namespaced_deployment(name, ns)
    except ApiException as e:
        if e.status == 404:
            raise PrerequisiteError(
                f"the deployment's query engine ({name}) is missing",
                why="the last deploy stopped after its dependency server step",
                next=_fix(cfg),
                where=f"namespace {ns}",
                path="run.deps_stale",
            ) from e
        raise
    labels = dep.spec.selector.match_labels or {}
    selector = ",".join(f"{k}={v}" for k, v in labels.items())
    out = []
    for pod in core.list_namespaced_pod(ns, label_selector=selector).items:
        if not _live(pod):
            continue
        got = (pod.metadata.annotations or {}).get(m.POD_ANNOTATION_SET) or "none"
        if got != pinset:
            out.append({"pod": pod.metadata.name, "role": name, "pinset": got})
    return out


def pods_on_sets(core: Any, ns: str, cfg: Any, since: Any = None) -> list[dict[str, str]]:
    """The query engine pods (Spark Thrift, DuckDB) with the set each names,
    for the run-end check. The Spark drivers are not listed: every job of a
    run is built from the run's one handle, so their annotation cannot
    differ; the long-lived query engine pods can (a redeploy mid-run)."""
    name = QUERY_ENGINE_DEPLOYMENTS.get(cfg.architecture.query_engine.type.value)
    if name is None:
        return []
    from kubernetes import client as k8s_client

    dep = k8s_client.AppsV1Api().read_namespaced_deployment(name, ns)
    labels = dep.spec.selector.match_labels or {}
    selector = ",".join(f"{k}={v}" for k, v in labels.items())
    return [
        {
            "pod": pod.metadata.name,
            "role": name,
            "pinset": (pod.metadata.annotations or {}).get(m.POD_ANNOTATION_SET) or "none",
        }
        for pod in core.list_namespaced_pod(ns, label_selector=selector).items
        if _live(pod)
    ]


def check_pods(cfg: Any, handle: m.DepsHandle, since: Any) -> dict[str, Any]:
    """The run-end pod check: ``{"pods_checked", "pod_mismatches"}``. A
    failed listing records ``pods_checked: None`` and the error, never a
    silent pass."""
    import time

    from kubernetes import client as k8s_client

    last = ""
    for attempt in range(3):
        if attempt:
            time.sleep(2 * attempt)  # an API blip at run end is retried
        try:
            pods = pods_on_sets(k8s_client.CoreV1Api(), cfg.get_namespace(), cfg, since=since)
            break
        except Exception as e:  # noqa: BLE001 -- recorded, not swallowed
            last = (str(e) or type(e).__name__)[:300]
    else:
        return {"pods_checked": None, "pods_check_error": last, "pod_mismatches": []}
    return {
        "pods_checked": len(pods),
        "pod_mismatches": [p for p in pods if p["pinset"] != handle.pinset_sha256],
    }


def attach_refusal(cfg: Any, run: Any) -> str | None:
    """Why ``benchmark`` or ``query`` must not add results to ``run``: the
    query engine's pods now run another dependency set than the run
    recorded (a redeploy since), or cannot be read. None when they match or
    the run records no set."""
    deps = ((getattr(run, "provenance", None) or {}).get("deps")) or {}
    recorded = deps.get("pinset_sha256") if isinstance(deps, dict) else None
    if not recorded or cfg.architecture.query_engine.type.value not in QUERY_ENGINE_DEPLOYMENTS:
        return None
    from kubernetes import client as k8s_client

    from lakebench.k8s import get_k8s_client

    try:
        # Loads the config's kube-context for the API class below.
        get_k8s_client(context=cfg.platform.kubernetes.context, namespace=cfg.get_namespace())
        stale = _consumer_mismatches(k8s_client.CoreV1Api(), cfg.get_namespace(), cfg, recorded)
    except Exception as e:  # noqa: BLE001 -- an unread set is not a matching one
        return f"the query engine's dependency set could not be read ({str(e)[:200]})"
    if not stale:
        return None
    return (
        f"the query engine runs another dependency set than run {run.run_id} recorded "
        f"({', '.join(p['pod'] + ' on ' + p['pinset'][:12] for p in stale)}); "
        "start a new run to record these results"
    )
