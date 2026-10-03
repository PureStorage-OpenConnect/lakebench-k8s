"""The ``deps`` deploy step: the dependency server in the namespace.

One ``lb-deps`` Deployment per deployment resolves the jars, wheels and
DuckDB files the request names onto its own PVC and serves them read-only.
This step applies the server's objects, waits until a pod rendered for this
request is Ready, reads the set's manifest from that pod (``lb_deps.py
show``), checks it against the request (``deps.manifest.check_manifest``),
writes the ``lb-deps-manifest`` ConfigMap, and writes the namespace
annotation ``lakebench.deployment/deps-set`` last.

The step removes the annotation before anything else it does and writes it
only after every check passed and the ConfigMap is written, so when this
step fails or is interrupted the annotation is absent and ``run`` refuses
until a deploy succeeds. (A deploy that fails in an earlier step does not
reach this one and leaves the annotation as it was; ``run`` also compares
the request it would build with the ConfigMap's.)

Every object is in the config's namespace (Category 1). Nothing here is
cluster-scoped or shared, so the cluster lease is not involved.
"""

from __future__ import annotations

import json
import logging
import math
import time
from typing import TYPE_CHECKING, Any

import yaml

from lakebench.deploy import deadline as deploy_deadline
from lakebench.deploy.engine import DeploymentResult, DeploymentStatus
from lakebench.deps import manifest as m
from lakebench.deps.request import (
    GROUP_DUCKDB,
    LB_DEPS_EXIT,
    TOOLS_PATH,
    DepsRequest,
    select_request,
)

if TYPE_CHECKING:
    from lakebench.deploy.engine import DeploymentEngine

logger = logging.getLogger(__name__)

TEMPLATES = ("deps/pvc.yaml.j2", "deps/service.yaml.j2", "deps/deployment.yaml.j2")
READY_TIMEOUT_S = 900  # one cold resolve took about 1 min in a live run
PVC_GONE_TIMEOUT_S = 120
NO_CLASS_GRACE_S = 30
STALE_REPLICA_FAILURE_GRACE_S = 120
SHOW_TIMEOUT_S = 120
MIRROR_HINT = (
    "set platform.deps.maven_repository and platform.deps.pypi_index "
    "(and platform.deps.duckdb_extension_repository for DuckDB) to reach a mirror"
)
# SparkApplication states that hold no driver.
_SPARK_APP_DONE = frozenset({"COMPLETED", "FAILED", "SUBMISSION_FAILED"})


class DepsStepFailed(Exception):
    """The step cannot complete; the message says why and what to do."""


def _api_exception() -> type[Exception]:
    from kubernetes.client.rest import ApiException

    return ApiException


def _status(e: Exception) -> int | None:
    return getattr(e, "status", None)


class DependencyServerDeployer:
    """Deploys and verifies the ``lb-deps`` server."""

    def __init__(self, engine: DeploymentEngine):
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.namespace = self.config.get_namespace()
        self.warnings: list[str] = []
        # Set per wait: when a slow-to-clear state was first seen, and
        # whether a ReplicaFailure predates this attempt.
        self._first_seen: dict[str, float] = {}
        self._stale_replica_failure = False

    # -- entry -------------------------------------------------------------

    def deploy(self) -> DeploymentResult:
        start = time.time()
        try:
            request = select_request(self.config)
        except Exception as e:  # noqa: BLE001 -- a config the resolve cannot serve
            return self._failed(start, f"cannot build the dependency request: {e}")
        if self.engine.dry_run:
            return DeploymentResult(
                component="deps",
                status=DeploymentStatus.SUCCESS,
                message=(
                    f"Would deploy the dependency server (request "
                    f"{request.request_sha256[:12]}, groups {', '.join(request.groups)})"
                ),
                label="Dependency server",
                detail=f"request {request.request_sha256[:12]}",
            )
        try:
            handle = self._deploy(request)
        except deploy_deadline.DeployTimeout:
            self._clear_annotation_quietly()
            raise
        except DepsStepFailed as e:
            self._clear_annotation_quietly()
            return self._failed(start, str(e))
        except Exception as e:  # noqa: BLE001 -- one FAILED result, logged
            from lakebench.deploy.engine import DeploymentEngine

            self._clear_annotation_quietly()
            # apply_manifest wraps the API error; _run_steps retries the step
            # once on the transient one underneath.
            inner: BaseException | None = e
            while inner is not None:
                if isinstance(inner, Exception) and DeploymentEngine._is_transient_error(inner):
                    raise inner from e
                inner = inner.__cause__ or inner.__context__
            logger.exception("dependency server deploy failed")
            return self._failed(start, f"dependency server deploy failed: {e}")
        self.engine.deps = handle
        files = sum(len(v) for v in handle.manifest["groups"].values())
        size = sum(e["size"] for v in handle.manifest["groups"].values() for e in v)
        note = "".join(f"; warning: {w}" for w in self.warnings)
        return DeploymentResult(
            component="deps",
            status=DeploymentStatus.SUCCESS,
            message=(
                f"Dependency server serving set {handle.pinset_sha256[:12]} "
                f"({files} files, {size / 1e6:.0f} MB){note}"
            ),
            elapsed_seconds=time.time() - start,
            details={
                "pinset_sha256": handle.pinset_sha256,
                "request_sha256": handle.request_sha256,
                "warnings": list(self.warnings),
            },
            label="Dependency server",
            detail=f"set {handle.pinset_sha256[:12]} ({files} files)",
        )

    def _failed(self, start: float, message: str) -> DeploymentResult:
        return DeploymentResult(
            component="deps",
            status=DeploymentStatus.FAILED,
            message=message,
            elapsed_seconds=time.time() - start,
            label="Dependency server",
        )

    def _warn(self, text: str) -> None:
        logger.warning(text)
        self.warnings.append(text)

    # -- the step ----------------------------------------------------------

    def _deploy(self, request: DepsRequest) -> m.DepsHandle:
        from kubernetes import client as k8s_client

        core = k8s_client.CoreV1Api()
        sha = request.request_sha256
        self.warnings = []  # the engine may retry the step on this deployer

        # First: the annotation must never name a set this deploy has not
        # verified, whatever fails below.
        self._remove_annotation(core)
        self._check_storage_class()
        previous = self._previous_request(core)
        if previous is not None and previous != sha:
            self._warn_active_applications()

        self._ensure_tools_configmap(core, request)
        stale_replica_failure = self._replica_failure_now()
        self._apply_objects(request)
        stale = self._delete_stale_failed_pods(core, sha)
        pod = self._wait_for_server_ready(
            sha,
            stale,
            timeout_seconds=deploy_deadline.clamp(READY_TIMEOUT_S),
            stale_replica_failure=stale_replica_failure,
        )
        shown = self._show(pod, sha)

        problems = m.check_manifest(request, shown)
        if problems:
            raise DepsStepFailed(
                "the dependency server's set is not the set this deploy needs: "
                + "; ".join(problems)
            )
        pod_name, pod_uid = pod.metadata.name, pod.metadata.uid
        data = m.manifest_configmap_data(request, shown, pod_name, pod_uid)
        data["server-images.json"] = self._pod_images(pod)
        pinset = str(shown["pinset_sha256"])
        # Another deploy of this namespace may have replaced the server since
        # the wait; never record a pod or request that is no longer serving.
        self._recheck_server(core, sha, pod_uid)
        self._write_manifest_configmap(data, sha, pinset)
        self._write_annotation(core, pinset)
        self._remove_old_tools_configmaps(core, sha, previous)
        return m.DepsHandle(
            pinset_sha256=pinset,
            request_sha256=sha,
            base_url=m.base_url(self.namespace, pinset),
            server_pod_uid=pod_uid,
            manifest=dict(shown),
        )

    # -- reads before the change -------------------------------------------

    def _check_storage_class(self) -> None:
        name = self.config.platform.deps.storage_class
        if not name:
            return
        from kubernetes import client as k8s_client

        try:
            pvc = k8s_client.CoreV1Api().read_namespaced_persistent_volume_claim(
                m.PVC_NAME, self.namespace
            )
        except _api_exception() as e:
            if _status(e) not in (403, 404):
                raise
            pvc = None
        if pvc is not None and not pvc.metadata.deletion_timestamp:
            # The class is read only when the PVC is created; the PVC step
            # warns when it differs from the configured one.
            return
        try:
            k8s_client.StorageV1Api().read_storage_class(name)
        except _api_exception() as e:
            if _status(e) == 404:
                raise DepsStepFailed(
                    f"StorageClass {name!r} (platform.deps.storage_class) does not exist"
                ) from e
            if _status(e) == 403:
                self._warn(f"cannot read StorageClass {name!r} to check it exists (403)")
                return
            raise

    def _previous_request(self, core: Any) -> str | None:
        """The request the last successful deps step recorded, if any."""
        try:
            cm = core.read_namespaced_config_map(m.MANIFEST_CONFIGMAP, self.namespace)
        except _api_exception() as e:
            if _status(e) == 404:
                return None
            raise
        try:
            return str(json.loads((cm.data or {})["manifest.json"])["request_sha256"])
        except (KeyError, TypeError, ValueError):
            return ""

    def _clear_annotation_quietly(self) -> None:
        """After a failure: another deploy of this namespace may have written
        the annotation meanwhile for a server this one then replaced."""
        try:
            from kubernetes import client as k8s_client

            self._remove_annotation(k8s_client.CoreV1Api())
        except Exception as e:  # noqa: BLE001 -- the step has already failed
            logger.warning("could not clear %s: %s", m.ANNOTATION_DEPS_SET, e)

    def _remove_annotation(self, core: Any) -> None:
        ns = core.read_namespace(self.namespace)
        if m.ANNOTATION_DEPS_SET not in (ns.metadata.annotations or {}):
            return
        core.patch_namespace(
            self.namespace, {"metadata": {"annotations": {m.ANNOTATION_DEPS_SET: None}}}
        )

    def _warn_active_applications(self) -> None:
        """s2.11: the set changes under running SparkApplications."""
        from kubernetes import client as k8s_client

        try:
            items = (
                k8s_client.CustomObjectsApi()
                .list_namespaced_custom_object(
                    "sparkoperator.k8s.io", "v1beta2", self.namespace, "sparkapplications"
                )
                .get("items", [])
            )
        except Exception as e:  # noqa: BLE001 -- a warning only
            logger.debug("cannot list SparkApplications: %s", e)
            return
        active = sorted(
            str((a.get("metadata") or {}).get("name"))
            for a in items
            if ((a.get("status") or {}).get("applicationState") or {}).get("state", "")
            not in _SPARK_APP_DONE
        )
        if active:
            self._warn(
                f"the dependency set changes while {len(active)} SparkApplication(s) are "
                f"active in {self.namespace} ({', '.join(active)}): a driver that starts "
                "during the switch fails to fetch and fails its stage, and a continuous "
                "driver restarted later asks for the old set and gets 404"
            )

    # -- objects ------------------------------------------------------------

    def _ensure_tools_configmap(self, core: Any, request: DepsRequest) -> None:
        """Create the immutable tools map for this request, or check that
        the one already there holds the same bytes."""
        import hashlib

        name = m.tools_configmap_name(request.request_sha256)
        raw = TOOLS_PATH.read_bytes()
        if hashlib.sha256(raw).hexdigest() != request.tools_sha256:
            raise DepsStepFailed(
                f"{TOOLS_PATH} changed while deploy ran (its sha256 is not the request's); "
                "re-run deploy"
            )
        data = {"lb_deps.py": raw.decode("utf-8"), "request.json": request.canonical_json()}
        try:
            existing = core.read_namespaced_config_map(name, self.namespace)
        except _api_exception() as e:
            if _status(e) != 404:
                raise
            existing = None
        if existing is not None:
            if dict(existing.data or {}) != data:
                raise DepsStepFailed(
                    f"ConfigMap {name} in {self.namespace} does not hold this request's "
                    f"resolver and request; delete it and re-run deploy"
                )
            return
        body = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "labels": {**self._labels(), m.LABEL_ROLE: "tools"},
                "annotations": {m.POD_ANNOTATION_REQUEST: request.request_sha256},
            },
            "immutable": True,
            "data": data,
        }
        core.create_namespaced_config_map(self.namespace, body)

    def _labels(self) -> dict[str, str]:
        return {
            **m.SELECTOR_LABELS,
            "app.kubernetes.io/managed-by": "lakebench",
            "app.kubernetes.io/instance": self.config.name,
            "lakebench.io/deployment": self.config.name,
        }

    def render(self, request: DepsRequest) -> list[dict[str, Any]]:
        """The PVC, Service and Deployment for this request, as dicts."""
        cfg = self.config
        parts: list[tuple[str, str]] = []
        if GROUP_DUCKDB in request.groups:
            parts.append(("duckdb", cfg.images.duckdb))
        parts.append(("spark", cfg.images.spark))
        context = {
            "namespace": self.namespace,
            "deployment_name": cfg.name,
            "server_name": m.SERVER_NAME,
            "pvc_name": m.PVC_NAME,
            "pvc_size": m.PVC_SIZE,
            "storage_class": cfg.platform.deps.storage_class,
            "port": m.PORT,
            "request_sha256": request.request_sha256,
            "tools_configmap": m.tools_configmap_name(request.request_sha256),
            "tools_mount": m.TOOLS_MOUNT,
            "serve_container": m.SERVE_CONTAINER,
            "spark_image": cfg.images.spark,
            "image_pull_policy": cfg.images.pull_policy.value,
            "resolve_parts": parts,
            "resolve_requests": m.RESOLVE_REQUESTS,
            "resolve_limits": m.RESOLVE_LIMITS,
            "serve_requests": m.SERVE_REQUESTS,
            "serve_limits": m.SERVE_LIMITS,
        }
        docs: list[dict[str, Any]] = []
        for template in TEMPLATES:
            for doc in yaml.safe_load_all(self.renderer.render(template, context)):
                if doc:
                    docs.append(doc)
        return docs

    def _apply_objects(self, request: DepsRequest) -> None:
        """PVC (create-only), Service, Deployment (replace; an unchanged
        template does not roll the pod)."""
        docs = self.render(request)
        pvc = next(d for d in docs if d["kind"] == "PersistentVolumeClaim")
        self._ensure_pvc(pvc)
        for doc in docs:
            if doc["kind"] != "PersistentVolumeClaim":
                self.k8s.apply_manifest(doc, namespace=self.namespace)

    def _ensure_pvc(self, body: dict[str, Any]) -> None:
        from kubernetes import client as k8s_client

        from lakebench.k8s.wait import WaitStatus, wait_for_condition

        core = k8s_client.CoreV1Api()

        def read() -> Any:
            try:
                return core.read_namespaced_persistent_volume_claim(m.PVC_NAME, self.namespace)
            except _api_exception() as e:
                if _status(e) == 404:
                    return None
                raise

        existing = read()
        if existing is not None and existing.metadata.deletion_timestamp:
            # "Delete PVC lb-deps-data and re-run deploy" is the documented fix
            # for a full, unwritable or lost volume. The claim stays
            # Terminating while a scheduled pod mounts it, so the server is
            # scaled to zero first; the Deployment apply below restores it.
            self._release_pvc(core)
            result = wait_for_condition(
                lambda: (read() is None, "PVC lb-deps-data is terminating"),
                timeout_seconds=PVC_GONE_TIMEOUT_S,
                poll_interval=2,
                description=f"PVC {m.PVC_NAME} to finish deleting",
            )
            if result.status != WaitStatus.READY:
                raise DepsStepFailed(
                    f"PVC {m.PVC_NAME} in {self.namespace} is still terminating after the "
                    f"{m.SERVER_NAME} pods were stopped; a pod that mounts it may be stuck "
                    "Terminating on a lost node. Re-run deploy when the PVC is gone"
                )
            existing = None
        if existing is None:
            core.create_namespaced_persistent_volume_claim(self.namespace, body)
            return
        want = self.config.platform.deps.storage_class
        have = existing.spec.storage_class_name or ""
        if want and have != want:
            self._warn(
                f"PVC {m.PVC_NAME} uses StorageClass {have or '(none)'!r}; "
                f"platform.deps.storage_class {want!r} applies only to a new PVC: "
                f"delete PVC {m.PVC_NAME} in {self.namespace} and re-run deploy to move it"
            )

    def _release_pvc(self, core: Any) -> None:
        from kubernetes import client as k8s_client

        try:
            k8s_client.AppsV1Api().patch_namespaced_deployment(
                m.SERVER_NAME, self.namespace, {"spec": {"replicas": 0}}
            )
        except _api_exception() as e:
            if _status(e) != 404:
                raise
        selector = ",".join(f"{k}={v}" for k, v in m.SELECTOR_LABELS.items())
        for pod in core.list_namespaced_pod(self.namespace, label_selector=selector).items:
            if pod.metadata.deletion_timestamp:
                continue
            try:
                core.delete_namespaced_pod(pod.metadata.name, self.namespace)
            except _api_exception() as e:
                if _status(e) != 404:
                    raise

    # -- pods -----------------------------------------------------------------

    def _server_pods(self, core: Any, sha: str) -> list[Any]:
        selector = ",".join(f"{k}={v}" for k, v in m.SELECTOR_LABELS.items())
        pods = core.list_namespaced_pod(self.namespace, label_selector=selector).items
        return [
            p
            for p in pods
            if (p.metadata.annotations or {}).get(m.POD_ANNOTATION_REQUEST) == sha
            and not p.metadata.deletion_timestamp
        ]

    @staticmethod
    def _container_statuses(pod: Any) -> list[Any]:
        st = pod.status
        if st is None:
            return []
        return list(st.init_container_statuses or []) + list(st.container_statuses or [])

    @staticmethod
    def _failed_termination(cs: Any) -> Any | None:
        """The failed run behind a container's state: terminated non-zero
        now, or waiting in CrashLoopBackOff after a non-zero exit."""
        state = cs.state
        if state is None:
            return None
        term = state.terminated
        if term is not None and term.exit_code:
            return term
        waiting = state.waiting
        last = cs.last_state.terminated if cs.last_state is not None else None
        if waiting is not None and waiting.reason == "CrashLoopBackOff" and last is not None:
            return last if last.exit_code else None
        return None

    def _delete_stale_failed_pods(self, core: Any, sha: str) -> set[str]:
        """Delete this request's pods that are failing from an earlier
        attempt, so a re-run after a transient failure gets a fresh resolve
        instead of failing on the old pod's last state. A pod whose serving
        container is running is never deleted. Returns their UIDs."""
        stale: set[str] = set()
        for pod in self._server_pods(core, sha):
            if self._is_ready(pod):
                continue
            if self._serve_running(pod) and not self._serve_failed_hash(pod):
                continue
            evicted = pod.status is not None and pod.status.phase == "Failed"
            failing = self._serve_failed_hash(pod) or any(
                self._failed_termination(cs) for cs in self._container_statuses(pod)
            )
            if not (evicted or failing):
                continue
            logger.info("deleting failed lb-deps pod %s for a fresh attempt", pod.metadata.name)
            try:
                core.delete_namespaced_pod(pod.metadata.name, self.namespace)
            except _api_exception() as e:
                if _status(e) != 404:
                    raise
            stale.add(pod.metadata.uid)
        return stale

    @staticmethod
    def _is_ready(pod: Any) -> bool:
        st = pod.status
        if st is None or st.phase != "Running":
            return False
        ready = any(c.type == "Ready" and c.status == "True" for c in (st.conditions or []))
        serve = [c for c in (st.container_statuses or []) if c.name == m.SERVE_CONTAINER]
        return ready and len(serve) == 1 and bool(serve[0].ready)

    @staticmethod
    def _serve_failed_hash(pod: Any) -> bool:
        """The serving container's last run failed its start re-hash (exit 4):
        the set on the PVC is damaged, and only a new pod (whose init
        containers resolve it again) recovers it."""
        st = pod.status
        for c in (st.container_statuses or []) if st is not None else []:
            last = c.last_state.terminated if c.last_state is not None else None
            if c.name == m.SERVE_CONTAINER and last is not None:
                return bool(last.exit_code == LB_DEPS_EXIT["hash"])
        return False

    @staticmethod
    def _serve_running(pod: Any) -> bool:
        st = pod.status
        for c in (st.container_statuses or []) if st is not None else []:
            if c.name == m.SERVE_CONTAINER and c.state is not None and c.state.running:
                return True
        return False

    def _wait_for_server_ready(
        self,
        sha: str,
        stale: set[str],
        timeout_seconds: float,
        stale_replica_failure: bool = False,
    ) -> Any:
        """The one Ready server pod of this request, or DepsStepFailed with
        the cause. Fails at once on a state that does not recover by
        waiting; the wait itself is bounded by the deploy deadline."""
        from kubernetes import client as k8s_client

        from lakebench.deploy.engine import DeploymentEngine
        from lakebench.k8s.wait import WaitStatus, WaitTerminal, wait_for_condition

        core = k8s_client.CoreV1Api()
        apps = k8s_client.AppsV1Api()
        found: list[Any] = []
        terminal: list[str] = []
        self._first_seen = {}
        self._stale_replica_failure = stale_replica_failure

        def check() -> tuple[bool, str]:
            try:
                return self._check_server(core, apps, sha, stale, found)
            except WaitTerminal as e:
                terminal.append(str(e))
                raise
            except _api_exception() as e:
                if _status(e) in (401, 403):
                    terminal.append(f"cannot read the lb-deps server: {e}")
                    raise WaitTerminal(terminal[-1]) from e
                return False, f"API error {_status(e)}"
            except Exception as e:  # noqa: BLE001 -- never let a bug read as "still waiting"
                if DeploymentEngine._is_transient_error(e):
                    return False, f"API connection error: {type(e).__name__}"
                terminal.append(f"cannot check the lb-deps server: {e!r}")
                raise WaitTerminal(terminal[-1]) from e

        # Rounded up and at least 1 s: wait_for_condition clamps it to the
        # deploy deadline again and raises DeployTimeout when that cuts it.
        result = wait_for_condition(
            check,
            timeout_seconds=max(1, math.ceil(timeout_seconds)),
            poll_interval=5,
            description=f"the dependency server {m.SERVER_NAME} in {self.namespace}",
        )
        if result.status == WaitStatus.READY and found:
            return found[-1]
        if result.status == WaitStatus.FAILED:
            raise DepsStepFailed(terminal[-1] if terminal else result.message)
        # A timeout clamped to the deploy deadline that ran out with it.
        deploy_deadline.check(f"dependency server {m.SERVER_NAME}", result.message)
        diagnosis = self._diagnose(core, sha)
        raise DepsStepFailed(result.message + (f" | {diagnosis}" if diagnosis else ""))

    def _check_server(
        self, core: Any, apps: Any, sha: str, stale: set[str], found: list[Any]
    ) -> tuple[bool, str]:
        from lakebench.k8s.wait import WaitTerminal

        dep = apps.read_namespaced_deployment(m.SERVER_NAME, self.namespace)
        failure = self._replica_failure(dep)
        if failure is not None:
            # Only once the controller has seen this generation. A condition
            # that predates this attempt may clear when the ReplicaSet retries
            # (an admin fixed the SCC or quota); it fails after a grace.
            first = self._first_seen.setdefault("replica-failure", time.monotonic())
            waited = time.monotonic() - first
            if not self._stale_replica_failure or waited >= STALE_REPLICA_FAILURE_GRACE_S:
                raise WaitTerminal(self._replica_failure_message(failure))
        pods = self._server_pods(core, sha)
        for pod in pods:
            if pod.metadata.uid not in stale:
                self._raise_on_pod_failure(core, pod)
        self._raise_on_unbindable_pvc(core)

        st = dep.status
        generation = dep.metadata.generation or 0
        rolled = (
            st is not None
            and (st.observed_generation or 0) >= generation
            and (st.replicas or 0) == 1
            and (st.updated_replicas or 0) == 1
            and (st.ready_replicas or 0) == 1
        )
        ready = [p for p in pods if self._is_ready(p)]
        if rolled and len(ready) == 1:
            found.append(ready[0])
            return True, f"pod {ready[0].metadata.name} Ready"
        if not pods:
            return False, "no pod for this request yet"
        return False, "; ".join(self._pod_state(p) for p in pods)

    @staticmethod
    def _replica_failure(dep: Any) -> str | None:
        st = dep.status
        if st is None or (st.observed_generation or 0) < (dep.metadata.generation or 0):
            return None
        for cond in st.conditions or []:
            if cond.type == "ReplicaFailure" and cond.status == "True":
                return str(cond.message or "")
        return None

    def _replica_failure_now(self) -> bool:
        """Whether the Deployment already reports ReplicaFailure before this
        step applies anything. Such a condition is from an earlier attempt
        (the ReplicaSet may be in its retry backoff after an admin fixed the
        cause), so it does not fail this attempt at once; a timeout still
        reports it."""
        from kubernetes import client as k8s_client

        try:
            dep = k8s_client.AppsV1Api().read_namespaced_deployment(m.SERVER_NAME, self.namespace)
        except _api_exception() as e:
            if _status(e) == 404:
                return False
            raise
        return self._replica_failure(dep) is not None

    def _replica_failure_message(self, message: str) -> str:
        text = f"the lb-deps ReplicaSet cannot create its pod: {message}"
        if "security context constraint" in message.lower() or "scc" in message.lower():
            text += (
                ". The pod runs as UID 185 through the lakebench-spark-runner "
                "ServiceAccount, which needs the anyuid SCC on OpenShift; check the "
                "rbac step's output"
            )
        text += (
            ". Once the cause is fixed the ReplicaSet retries on its own backoff; "
            "re-run deploy then"
        )
        return text

    def _raise_on_pod_failure(self, core: Any, pod: Any) -> None:
        from lakebench.k8s.wait import _TERMINAL_WAITING_REASONS, WaitTerminal

        name = pod.metadata.name
        st = pod.status
        if st is not None and st.phase == "Failed":
            raise WaitTerminal(
                f"lb-deps pod {name} failed ({st.reason or 'no reason'}: "
                f"{st.message or 'no detail'}); the resolve needs up to 4Gi in its work "
                "volume and 2Gi free on the PVC. Re-run deploy for a fresh pod"
            )
        for cs in self._container_statuses(pod):
            waiting = cs.state.waiting if cs.state is not None else None
            if waiting is not None and waiting.reason in _TERMINAL_WAITING_REASONS:
                raise WaitTerminal(
                    f"lb-deps container {cs.name} cannot start: {waiting.reason} "
                    f"({waiting.message or 'no detail'}), image {cs.image}"
                )
            term = self._failed_termination(cs)
            if term is not None:
                previous = term is not (cs.state.terminated if cs.state is not None else None)
                raise WaitTerminal(
                    self._exit_message(
                        cs.name, cs.image, term, self._log_tail(core, name, cs.name, previous)
                    )
                )
        for cond in (st.conditions or []) if st is not None else []:
            if (
                cond.type == "PodScheduled"
                and cond.status == "False"
                and "volume node affinity conflict" in (cond.message or "")
            ):
                raise WaitTerminal(
                    f"lb-deps pod {name} cannot be scheduled: its PVC {m.PVC_NAME} is bound to "
                    f"a node it cannot reach ({cond.message}). Delete PVC {m.PVC_NAME} in "
                    f"{self.namespace} and re-run deploy; the set is re-resolved"
                )

    def _raise_on_unbindable_pvc(self, core: Any) -> None:
        from lakebench.k8s.wait import WaitTerminal

        try:
            pvc = core.read_namespaced_persistent_volume_claim(m.PVC_NAME, self.namespace)
        except _api_exception() as e:
            if _status(e) == 404:
                return
            raise
        phase = pvc.status.phase if pvc.status is not None else None
        if phase == "Pending" and not pvc.spec.storage_class_name:
            # A static PV without a class can still bind; give it 30 s.
            first = self._first_seen.setdefault("pvc-no-class", time.monotonic())
            if time.monotonic() - first < NO_CLASS_GRACE_S:
                return
            raise WaitTerminal(
                f"PVC {m.PVC_NAME} has no StorageClass and the cluster has no default one; "
                "set platform.deps.storage_class"
            )

    def _log_tail(self, core: Any, pod: str, container: str, previous: bool) -> str:
        try:
            return str(
                core.read_namespaced_pod_log(
                    pod, self.namespace, container=container, tail_lines=100, previous=previous
                )
                or ""
            )
        except Exception as e:  # noqa: BLE001 -- the exit code still fails the step
            return f"(log unavailable: {e})"

    def _exit_message(self, container: str, image: str, term: Any, log: str) -> str:
        code = term.exit_code
        errors = [ln.strip() for ln in log.splitlines() if ln.strip().startswith("LB_DEPS_ERROR")]
        head = f"lb-deps container {container} exited {code}"
        if errors:
            line = errors[-1]
            text = f"{head}: {line}"
            if "egress:" in line:
                text += f". {MIRROR_HINT}"
            if "PermissionError" in line or "Permission denied" in line:
                text += (
                    f". The volume of PVC {m.PVC_NAME} is not writable by UID 185; set "
                    "platform.deps.storage_class to a StorageClass that honours fsGroup, "
                    f"delete PVC {m.PVC_NAME} and re-run deploy"
                )
            if container == m.SERVE_CONTAINER and code == LB_DEPS_EXIT["hash"]:
                text += (
                    ". Re-run deploy: the pod is replaced and its init containers resolve "
                    "the damaged set again"
                )
            return text
        tail = " | ".join(ln for ln in log.splitlines()[-5:] if ln.strip()) or "no log"
        text = (
            f"{head} ({term.reason or 'no reason'}) without an LB_DEPS_ERROR line; last log: {tail}"
        )
        if code == 127:
            text += f". python3 is not on {image}; images.spark and images.duckdb must ship python3"
        if term.reason == "OOMKilled":
            text += ". The container hit its memory limit"
        return text

    @staticmethod
    def _pod_state(pod: Any) -> str:
        st = pod.status
        if st is None:
            return f"{pod.metadata.name}: no status"
        parts = [f"{pod.metadata.name}: {st.phase}"]
        for cs in DependencyServerDeployer._container_statuses(pod):
            s = cs.state
            if s is None:
                continue
            if s.running is not None:
                parts.append(f"{cs.name} running{'' if cs.ready else ' (not ready)'}")
            elif s.waiting is not None:
                parts.append(f"{cs.name} waiting ({s.waiting.reason})")
            elif s.terminated is not None:
                parts.append(f"{cs.name} done ({s.terminated.exit_code})")
        return ", ".join(parts)

    def _diagnose(self, core: Any, sha: str) -> str:
        """Events and scheduling detail for a wait that ran out of time."""
        notes = []
        try:
            from kubernetes import client as k8s_client

            dep = k8s_client.AppsV1Api().read_namespaced_deployment(m.SERVER_NAME, self.namespace)
            failure = self._replica_failure(dep)
            if failure is not None:
                notes.append(self._replica_failure_message(failure))
            names = {p.metadata.name for p in self._server_pods(core, sha)} | {m.PVC_NAME}
            for pod in self._server_pods(core, sha):
                for cond in (pod.status.conditions or []) if pod.status is not None else []:
                    if cond.type == "PodScheduled" and cond.status == "False":
                        notes.append(f"{pod.metadata.name} unschedulable: {cond.message}")
            events = core.list_namespaced_event(self.namespace).items
            for ev in events:
                obj = ev.involved_object
                ours = obj is not None and (
                    obj.name in names
                    or (obj.kind == "ReplicaSet" and str(obj.name).startswith(m.SERVER_NAME + "-"))
                )
                if ours and ev.type == "Warning":
                    notes.append(f"{obj.kind} {obj.name}: {ev.reason}: {ev.message}")
        except Exception as e:  # noqa: BLE001 -- diagnostics only
            notes.append(f"events unavailable: {e}")
        return " | ".join(notes[-8:])

    # -- the manifest -----------------------------------------------------------

    def _show(self, pod: Any, sha: str) -> dict[str, Any]:
        cmd = ["python3", f"{m.TOOLS_MOUNT}/lb_deps.py", "show", "--request", sha]
        timeout = deploy_deadline.clamp_whole_seconds(SHOW_TIMEOUT_S, "lb_deps.py show")
        rc, out, err = self.k8s.exec_in_pod(
            pod.metadata.name,
            cmd,
            namespace=self.namespace,
            container=m.SERVE_CONTAINER,
            timeout=timeout,
        )
        if rc != 0:
            if err.strip() == "Command timed out":
                deploy_deadline.check(f"lb_deps.py show in {pod.metadata.name}")
            lines = [ln for ln in (out + "\n" + err).splitlines() if ln.startswith("LB_DEPS_ERROR")]
            detail = lines[-1] if lines else (err.strip() or out.strip() or "no output")
            raise DepsStepFailed(f"lb_deps.py show in {pod.metadata.name} exited {rc}: {detail}")
        try:
            shown = json.loads(out)
        except ValueError as e:
            raise DepsStepFailed(f"lb_deps.py show printed no manifest: {e}") from e
        if not isinstance(shown, dict):
            raise DepsStepFailed("lb_deps.py show printed no manifest object")
        return shown

    def _recheck_server(self, core: Any, sha: str, pod_uid: str) -> None:
        """Just before the writes: the Deployment still renders this request
        and the pod ``show`` ran in is still this request's live server."""
        from kubernetes import client as k8s_client

        dep = k8s_client.AppsV1Api().read_namespaced_deployment(m.SERVER_NAME, self.namespace)
        want = (dep.spec.template.metadata.annotations or {}).get(m.POD_ANNOTATION_REQUEST)
        live = [p for p in self._server_pods(core, sha) if self._is_ready(p)]
        if want != sha or [p.metadata.uid for p in live] != [pod_uid]:
            raise DepsStepFailed(
                f"the {m.SERVER_NAME} server changed while this deploy verified it "
                "(another deploy of this namespace?); re-run deploy"
            )

    @staticmethod
    def _pod_images(pod: Any) -> str:
        """Each container's image and the digest it actually ran (imageID):
        the request names images by tag, so provenance records the digest."""
        out = {
            cs.name: {"image": cs.image, "image_id": cs.image_id}
            for cs in DependencyServerDeployer._container_statuses(pod)
        }
        return json.dumps(out, indent=1, sort_keys=True) + "\n"

    def _write_manifest_configmap(self, data: dict[str, str], sha: str, pinset: str) -> None:
        body = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": m.MANIFEST_CONFIGMAP,
                "namespace": self.namespace,
                "labels": {**self._labels(), m.LABEL_ROLE: "manifest"},
                "annotations": {
                    m.POD_ANNOTATION_REQUEST: sha,
                    m.POD_ANNOTATION_SET: pinset,
                },
            },
            "data": data,
        }
        self.k8s.apply_manifest(body, namespace=self.namespace)

    def _write_annotation(self, core: Any, pinset: str) -> None:
        core.patch_namespace(
            self.namespace, {"metadata": {"annotations": {m.ANNOTATION_DEPS_SET: pinset}}}
        )

    def _remove_old_tools_configmaps(self, core: Any, sha: str, previous: str | None) -> None:
        """Tools maps older than the previous request's. The current map, the
        previous request's (consumers that have not rolled yet may still
        mount it) and any map the live Deployment names are kept."""
        from kubernetes import client as k8s_client

        keep = {m.tools_configmap_name(sha)}
        if previous:
            keep.add(m.tools_configmap_name(previous))
        selector = f"app.kubernetes.io/component=deps,{m.LABEL_ROLE}=tools"
        try:
            dep = k8s_client.AppsV1Api().read_namespaced_deployment(m.SERVER_NAME, self.namespace)
            for vol in dep.spec.template.spec.volumes or []:
                if vol.config_map is not None:
                    keep.add(vol.config_map.name)
            for cm in core.list_namespaced_config_map(
                self.namespace, label_selector=selector
            ).items:
                if cm.metadata.name not in keep and cm.metadata.name.startswith(
                    m.TOOLS_CONFIGMAP_PREFIX
                ):
                    core.delete_namespaced_config_map(cm.metadata.name, self.namespace)
        except Exception as e:  # noqa: BLE001 -- leftovers are harmless; destroy removes them
            logger.warning("could not remove old lb-deps tools ConfigMaps: %s", e)


# --- the long-lived consumers (Spark Thrift, DuckDB) -----------------------------


def consumer_context(engine: DeploymentEngine, what: str) -> dict[str, Any]:
    """The template context a consumer renders with: the engine's context
    plus where this deploy's verified set is served. Never a placeholder for
    a real deploy: without engine.deps the consumer step fails."""
    handle = engine.deps
    if handle is None:
        if not engine.dry_run:
            raise m.DepsSetMissing(what)
        handle = m.placeholder_handle(engine.config)
    return {**engine.context, **m.consumer_context(handle)}


def live_pod(pod: Any) -> bool:
    """Not terminating and not finished (an evicted pod stays listed in
    phase Failed until it is garbage collected)."""
    phase = pod.status.phase if pod.status is not None else None
    return not pod.metadata.deletion_timestamp and phase not in ("Failed", "Succeeded")


# A consumer fetch failure kubelet's own retry can fix: the server was
# unreachable or restarting (exit 3), or the pod mounted the manifest
# ConfigMap a moment before the deploy rewrote it.
_RETRYABLE_FETCH = ("does not name the manifest's pinset", "does not hash to its pinset")


def _consumer_init_failure(
    core: Any,
    namespace: str,
    pod: Any,
    *,
    terminal_only: bool = False,
    retried: list[str] | None = None,
) -> str | None:
    """A consumer pod's failed lb-deps-fetch, with its LB_DEPS_ERROR line.
    With ``terminal_only``, a failure kubelet's retry may fix is None (and
    noted in ``retried``): the server unreachable or a stale manifest mount,
    for its first three restarts; a 404 (the server serves another set) is
    never retried."""
    for cs in (pod.status.init_container_statuses or []) if pod.status is not None else []:
        term = DependencyServerDeployer._failed_termination(cs)
        if term is None:
            continue
        previous = term is not (cs.state.terminated if cs.state is not None else None)
        try:
            log = str(
                core.read_namespaced_pod_log(
                    pod.metadata.name,
                    namespace,
                    container=cs.name,
                    tail_lines=50,
                    previous=previous,
                )
                or ""
            )
        except Exception as e:  # noqa: BLE001 -- the exit code still fails the step
            log = f"(log unavailable: {e})"
        lines = [ln.strip() for ln in log.splitlines() if ln.strip().startswith("LB_DEPS_ERROR")]
        detail = lines[-1] if lines else (log.strip().splitlines() or ["no log"])[-1]
        transient = term.exit_code == LB_DEPS_EXIT["missing"] or any(
            r in detail for r in _RETRYABLE_FETCH
        )
        retryable = transient and "HTTP Error 404" not in detail and (cs.restart_count or 0) < 3
        message = f"{pod.metadata.name} init container {cs.name} exited {term.exit_code}: {detail}"
        if terminal_only and retryable:
            if retried is not None:
                retried.append(message)
            continue
        return message
    return None


def _delete_stale_consumer_pods(core: Any, namespace: str, selector: str, pinset: str) -> None:
    """A re-run after a failed fetch gets a fresh pod at once, instead of
    waiting out kubelet's back-off on the old one (same pinset, so the
    unchanged template would not roll it)."""
    for pod in core.list_namespaced_pod(namespace, label_selector=selector).items:
        if pod.metadata.deletion_timestamp:
            continue
        if (pod.metadata.annotations or {}).get(m.POD_ANNOTATION_SET) != pinset:
            continue
        st = pod.status
        evicted = st is not None and st.phase == "Failed"
        inits = list(st.init_container_statuses or []) if st is not None else []
        failing = any(DependencyServerDeployer._failed_termination(cs) for cs in inits)
        if not (evicted or failing):
            continue
        try:
            core.delete_namespaced_pod(pod.metadata.name, namespace)
        except _api_exception() as e:
            if _status(e) != 404:
                raise


def wait_consumer_rolled(
    namespace: str,
    deployment: str,
    pinset: str | None,
    *,
    timeout_seconds: float,
    what: str,
) -> None:
    """Wait until ``deployment`` has rolled out completely and its one pod
    runs ``pinset`` (``lakebench.io/deps-set``) and is Ready. Ready replicas
    alone would count the old pod during a rollout. A failed set fetch fails
    at once. ``pinset`` None (no set check) keeps the plain rollout wait."""
    from kubernetes import client as k8s_client

    from lakebench.k8s.wait import WaitStatus, WaitTerminal, wait_for_condition

    apps = k8s_client.AppsV1Api()
    core = k8s_client.CoreV1Api()
    terminal: list[str] = []
    retried: list[str] = []
    if pinset is not None:
        try:
            dep0 = apps.read_namespaced_deployment(deployment, namespace)
            labels0 = dep0.spec.selector.match_labels or {}
            if labels0:  # never a namespace-wide list
                _delete_stale_consumer_pods(
                    core, namespace, ",".join(f"{k}={v}" for k, v in labels0.items()), pinset
                )
        except _api_exception() as e:
            if _status(e) != 404:
                raise

    def check() -> tuple[bool, str]:
        try:
            dep = apps.read_namespaced_deployment(deployment, namespace)
        except _api_exception() as e:
            if _status(e) == 404:
                return False, "not created"
            raise
        st = dep.status
        want = dep.spec.replicas or 1
        rolled = (
            st is not None
            and (st.observed_generation or 0) >= (dep.metadata.generation or 0)
            and (st.replicas or 0) == want
            and (st.updated_replicas or 0) == want
            and (st.ready_replicas or 0) == want
        )
        labels = dep.spec.selector.match_labels or {}
        selector = ",".join(f"{k}={v}" for k, v in labels.items())
        pods = [
            p
            for p in core.list_namespaced_pod(namespace, label_selector=selector).items
            if live_pod(p)
        ]
        for pod in pods:
            ann = (pod.metadata.annotations or {}).get(m.POD_ANNOTATION_SET)
            if pinset is not None and ann == pinset:
                failed = _consumer_init_failure(
                    core, namespace, pod, terminal_only=True, retried=retried
                )
                if failed:
                    terminal.append(failed)
                    raise WaitTerminal(failed)
        on_set = [
            p
            for p in pods
            if pinset is None or (p.metadata.annotations or {}).get(m.POD_ANNOTATION_SET) == pinset
        ]
        if rolled and len(pods) == want and len(on_set) == want:
            return True, "rolled out"
        state = (
            f"{st.ready_replicas or 0}/{want} ready, {len(on_set)}/{len(pods)} on the set"
            if st is not None
            else "no status"
        )
        return False, state

    result = wait_for_condition(
        check,
        timeout_seconds=max(1, math.ceil(timeout_seconds)),
        poll_interval=5,
        description=f"{what} ({deployment}) to roll out",
    )
    if result.status == WaitStatus.READY:
        return
    if result.status == WaitStatus.FAILED and terminal:
        raise RuntimeError(f"{what} cannot fetch the dependency set: {terminal[-1]}")
    # The caller clamped the timeout to the deploy deadline; a wait that ran
    # out with it is a deploy timeout, not a slow rollout.
    last = f"; last fetch failure: {retried[-1]}" if retried else ""
    deploy_deadline.check(f"{what} ({deployment}) to roll out", result.message + last)
    raise RuntimeError(result.message + last)
