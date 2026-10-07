"""The ``deps`` deploy step (DEP-2, ch01 s2.5) under the recording fixture.

The fake cluster has no controllers: ``Controller`` stands in for the
Deployment controller and kubelet, creating the server pod after the step's
stale-pod scan, so the readiness wait sees what a real rollout shows. The
fake also has no generation or defaulting, so "an unchanged config does not
roll the pod" is checked on the rendered Deployment body (byte-identical),
and the same-UID check of a real redeploy is SD-7's live proof.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml

from lakebench.deploy import deps as deps_mod
from lakebench.deploy.deps import DependencyServerDeployer
from lakebench.deploy.engine import DeploymentEngine, DeploymentStatus, TemplateRenderer
from lakebench.deps import manifest as m
from lakebench.deps import request as req
from tests.conftest import make_config
from tests.fixtures.deps_manifest_helpers import fake_shown
from tests.fixtures.recording_k8s import OWN

NS = "dp1"


def _cfg(name: str = NS, **over) -> Any:
    s3 = {"endpoint": "http://minio:9000", "access_key": "k", "secret_key": "s"}
    platform = {"storage": {"s3": s3}, **over.pop("platform", {})}
    return make_config(name=name, platform=platform, **over)


def _pod(name: str, sha: str, *, uid: str | None = None, **status: Any) -> dict:
    serve = {
        "name": "serve",
        "ready": True,
        "restartCount": 0,
        "image": "apache/spark:4.0.2-python3",
        "imageID": "",
        "state": {"running": {}},
    }
    init = {
        "name": "resolve-spark",
        "ready": False,
        "restartCount": 0,
        "image": "apache/spark:4.0.2-python3",
        "imageID": "",
        "state": {"terminated": {"exitCode": 0, "reason": "Completed"}},
    }
    st = {
        "phase": "Running",
        "conditions": [{"type": "Ready", "status": "True"}],
        "containerStatuses": [serve],
        "initContainerStatuses": [init],
    }
    st.update(status)
    meta = {
        "name": name,
        "namespace": NS,
        "labels": dict(m.SELECTOR_LABELS),
        "annotations": {m.POD_ANNOTATION_REQUEST: sha},
    }
    if uid:
        meta["uid"] = uid
    return {"metadata": meta, "status": st}


class Controller:
    """Stands in for the Deployment controller and kubelet: after the step's
    stale-pod scan, sets the rollout status and creates this request's pod
    (healthy unless ``status`` says otherwise)."""

    def __init__(self, rec, monkeypatch, *, status: dict | None = None, log: str = ""):
        self.rec = rec
        self.status = status
        self.log = log
        self.created: list[str] = []
        real = DependencyServerDeployer._delete_stale_failed_pods

        def scan_then_roll(deployer, core, sha):
            stale = real(deployer, core, sha)
            self.roll(sha)
            return stale

        monkeypatch.setattr(DependencyServerDeployer, "_delete_stale_failed_pods", scan_then_roll)

    def roll(self, sha: str) -> None:
        from kubernetes.client.models import V1DeploymentStatus

        dep = self.rec.store[("deployments", NS, m.SERVER_NAME)]
        dep.metadata.generation = 1
        ok = self.status is None
        dep.status = V1DeploymentStatus(
            observed_generation=1,
            replicas=1,
            updated_replicas=1,
            ready_replicas=1 if ok else 0,
        )
        name = f"lb-deps-{sha[:8]}-{len(self.created)}"
        if not any(
            k[0] == "pods"
            and (getattr(v.metadata, "annotations", None) or {}).get(m.POD_ANNOTATION_REQUEST)
            == sha
            for k, v in self.rec.store.items()
        ):
            self.rec.add("pods", _pod(name, sha, **(self.status or {})))
            self.created.append(name)
            if self.log:
                self.rec.pod_logs[(NS, name)] = self.log


def _setup(rec, cfg, *, annotations: dict | None = None):
    from lakebench.k8s.client import K8sClient

    rec.for_config(cfg)
    rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS, **(annotations or {})})
    engine = MagicMock()
    engine.config = cfg
    engine.dry_run = False
    engine.k8s = K8sClient(namespace=NS)
    engine.renderer = TemplateRenderer()
    engine.deps = None
    return engine


def _show(rec, shown_for: Any) -> list[list[str]]:
    """Script ``kubectl exec ... lb_deps.py show``; ``shown_for(sha)`` gives
    the manifest. Returns the argv of each call."""
    seen: list[list[str]] = []

    def handler(argv, kwargs):
        seen.append(list(argv))
        sha = argv[argv.index("--request") + 1]
        return (0, json.dumps(shown_for(sha)), "")

    rec.on_command("kubectl", "exec", handler=handler)
    return seen


def _ns_annotations(rec) -> dict:
    return rec.store[("namespaces", None, NS)].metadata.annotations or {}


def _idx(rec, **match) -> list[int]:
    return [i for i, c in enumerate(rec.calls) if all(getattr(c, k) == v for k, v in match.items())]


# --- the success path ----------------------------------------------------------


def test_deploy_serves_verifies_and_writes_the_annotation_last(recording_k8s, monkeypatch):
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    request = req.select_request(cfg)
    shown = fake_shown(request)
    Controller(rec, monkeypatch)
    execs = _show(rec, lambda sha: shown)

    result = DependencyServerDeployer(engine).deploy()

    assert result.status == DeploymentStatus.SUCCESS, result.message
    h = engine.deps
    assert (h.pinset_sha256, h.request_sha256) == (shown["pinset_sha256"], request.request_sha256)
    assert h.base_url == f"http://lb-deps.{NS}.svc.cluster.local:8080/sets/{h.pinset_sha256}"
    assert _ns_annotations(rec)[m.ANNOTATION_DEPS_SET] == h.pinset_sha256
    cm = rec.store[("configmaps", NS, m.MANIFEST_CONFIGMAP)]
    assert json.loads(cm.data["manifest.json"])["pinset_sha256"] == h.pinset_sha256
    assert cm.data["server-pod-uid"] == h.server_pod_uid
    # show ran in the serving container of the Ready pod, for this request.
    (argv,) = execs
    assert argv[argv.index("-c") + 1] == "serve" and request.request_sha256 in argv
    # The annotation is written after the manifest ConfigMap.
    (cm_write,) = [
        i for i in _idx(rec, kind="configmaps", name=m.MANIFEST_CONFIGMAP) if rec.calls[i].mutating
    ]
    (ann,) = _idx(rec, kind="namespaces", verb="patch")
    assert cm_write < ann
    # Every mutation is in the config's namespace.
    assert all(c.scope == OWN for c in rec.mutations()), [c.describe() for c in rec.mutations()]
    assert {c.namespace for c in rec.mutations()} <= {NS, None}


def test_tools_configmap_holds_exactly_the_hashed_bytes(recording_k8s, monkeypatch):
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    request = req.select_request(cfg)
    Controller(rec, monkeypatch)
    _show(rec, lambda sha: fake_shown(request))
    assert DependencyServerDeployer(engine).deploy().status == DeploymentStatus.SUCCESS

    tools = rec.store[("configmaps", NS, m.tools_configmap_name(request.request_sha256))]
    assert tools.immutable is True
    import hashlib

    dep = rec.store[("deployments", NS, m.SERVER_NAME)]
    env = {e.name: e.value for e in dep.spec.template.spec.containers[0].env}
    raw = tools.data["request.json"].encode()
    assert hashlib.sha256(raw).hexdigest() == env["LB_DEPS_REQUEST_SHA256"]
    assert hashlib.sha256(tools.data["lb_deps.py"].encode()).hexdigest() == request.tools_sha256
    assert tools.data["lb_deps.py"].encode() == req.TOOLS_PATH.read_bytes()


def test_same_config_redeploy_renders_an_identical_deployment(recording_k8s, monkeypatch):
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    request = req.select_request(cfg)
    Controller(rec, monkeypatch)
    _show(rec, lambda sha: fake_shown(request))
    d = DependencyServerDeployer(engine)
    first = yaml.safe_dump(d.render(request))
    assert d.deploy().status == DeploymentStatus.SUCCESS
    stored = rec.store[("deployments", NS, m.SERVER_NAME)].spec.template
    n_calls = len(rec.calls)

    again = DependencyServerDeployer(engine)
    assert yaml.safe_dump(again.render(req.select_request(cfg))) == first
    assert again.deploy().status == DeploymentStatus.SUCCESS
    later = rec.calls[n_calls:]
    assert rec.store[("deployments", NS, m.SERVER_NAME)].spec.template == stored
    # No pod deleted, no new tools ConfigMap, the same set announced again.
    assert not [c for c in later if c.kind == "pods" and c.deleting]
    assert not [c for c in later if c.kind == "configmaps" and c.verb == "create"]
    assert _ns_annotations(rec)[m.ANNOTATION_DEPS_SET] == fake_shown(request)["pinset_sha256"]


def test_a_changed_image_renders_a_new_pod_template():
    a = _cfg()
    b = _cfg(images={"spark": "apache/spark:4.0.2-python3"})
    eng = MagicMock(renderer=TemplateRenderer(), dry_run=False)

    def template(cfg):
        eng.config = cfg
        docs = DependencyServerDeployer(eng).render(req.select_request(cfg))
        return next(d for d in docs if d["kind"] == "Deployment")["spec"]["template"]

    ta, tb = template(a), template(b)
    ra, rb = req.select_request(a).request_sha256, req.select_request(b).request_sha256
    assert ra != rb
    assert ta["metadata"]["annotations"][m.POD_ANNOTATION_REQUEST] == ra
    assert tb["metadata"]["annotations"][m.POD_ANNOTATION_REQUEST] == rb
    assert tb["spec"]["volumes"][0]["configMap"]["name"] == m.tools_configmap_name(rb)
    assert ta != tb


def test_dry_run_makes_no_cluster_call(recording_k8s):
    rec = recording_k8s
    cfg = _cfg()
    rec.for_config(cfg)
    engine = MagicMock(config=cfg, dry_run=True, renderer=TemplateRenderer())
    result = DependencyServerDeployer(engine).deploy()
    assert result.status == DeploymentStatus.SUCCESS
    assert req.select_request(cfg).request_sha256[:12] in result.message
    rec.assert_no_calls()


# --- failures leave no annotation ---------------------------------------------------


def test_a_set_whose_entries_do_not_hash_fails_and_writes_nothing(recording_k8s, monkeypatch):
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    request = req.select_request(cfg)
    Controller(rec, monkeypatch)
    _show(rec, lambda sha: fake_shown(request, pinset_sha256="e" * 64))

    result = DependencyServerDeployer(engine).deploy()

    assert result.status == DeploymentStatus.FAILED
    assert "printed pinset" in result.message
    assert ("configmaps", NS, m.MANIFEST_CONFIGMAP) not in rec.store
    assert m.ANNOTATION_DEPS_SET not in _ns_annotations(rec)
    assert engine.deps is None


def test_the_old_annotation_goes_before_the_server_changes(recording_k8s, monkeypatch):
    """A failed redeploy must never leave an annotation naming a set the
    current deploy did not verify (an unchanged request included)."""
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg, annotations={m.ANNOTATION_DEPS_SET: "a" * 64})
    request = req.select_request(cfg)
    rec.add(
        "configmaps",
        {
            "metadata": {"name": m.MANIFEST_CONFIGMAP},
            "data": {"manifest.json": json.dumps({"request_sha256": request.request_sha256})},
        },
        namespace=NS,
    )
    Controller(rec, monkeypatch)
    _show(rec, lambda sha: fake_shown(request, overlaps=[{"bad": 1}]))

    result = DependencyServerDeployer(engine).deploy()

    assert result.status == DeploymentStatus.FAILED
    assert m.ANNOTATION_DEPS_SET not in _ns_annotations(rec)
    (removal,) = _idx(rec, kind="namespaces", verb="patch")
    first_server_write = min(i for i in _idx(rec, kind="deployments") if rec.calls[i].mutating)
    assert removal < first_server_write


# --- fail fast with the cause ------------------------------------------------------


def test_a_deleted_class_does_not_fail_a_deploy_whose_pvc_exists(recording_k8s, monkeypatch):
    """The class is read only when the PVC is created: a bound PVC on a class
    deleted since keeps the set, and the redeploy succeeds."""
    rec = recording_k8s
    cfg = _cfg(platform={"deps": {"storage_class": "nope"}})
    engine = _setup(rec, cfg)
    rec.add(
        "persistentvolumeclaims",
        {"metadata": {"name": m.PVC_NAME}, "spec": {"storageClassName": "nope"}},
        namespace=NS,
    )
    request = req.select_request(cfg)
    Controller(rec, monkeypatch)
    _show(rec, lambda sha: fake_shown(request))
    result = DependencyServerDeployer(engine).deploy()
    assert result.status == DeploymentStatus.SUCCESS, result.message


# --- stale pods ---------------------------------------------------------------------


def test_a_pod_whose_server_runs_is_never_deleted(recording_k8s, monkeypatch):
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    request = req.select_request(cfg)
    sha = request.request_sha256
    blip = _pod("lb-deps-live", sha, uid="uid-live")
    blip["status"]["conditions"] = [{"type": "Ready", "status": "False"}]
    blip["status"]["containerStatuses"][0]["ready"] = False
    blip["status"]["containerStatuses"][0]["lastState"] = {
        "terminated": {"exitCode": 137, "reason": "OOMKilled"}
    }
    # A stale init record next to a running server must not get it deleted.
    blip["status"]["initContainerStatuses"][0]["state"] = {
        "terminated": {"exitCode": 3, "reason": "Error"}
    }
    rec.add("pods", blip)
    d = DependencyServerDeployer(engine)
    from kubernetes import client as k8s_client

    assert d._delete_stale_failed_pods(k8s_client.CoreV1Api(), sha) == set()
    assert not [c for c in rec.calls if c.kind == "pods" and c.deleting]


# --- the rendered pod ----------------------------------------------------------------


@pytest.mark.parametrize(
    "over,parts",
    [
        ({}, ["spark"]),
        ({"recipe": "polaris-iceberg-spark-duckdb"}, ["duckdb", "spark"]),
    ],
)
def test_resolve_commands_are_pinned(over, parts):
    """Every flag that decides what is resolved lives in lb_deps.py, which
    is in the request hash; a flag added to the template instead would
    change the set without changing the request (SD-1 note)."""
    cfg = _cfg(**over)
    eng = MagicMock(config=cfg, renderer=TemplateRenderer(), dry_run=False)
    docs = DependencyServerDeployer(eng).render(req.select_request(cfg))
    spec = next(d for d in docs if d["kind"] == "Deployment")["spec"]["template"]["spec"]
    tool = f"{m.TOOLS_MOUNT}/lb_deps.py"
    assert [c["command"] for c in spec["initContainers"]] == [
        ["python3", tool, "resolve", p] for p in parts
    ]
    assert [c["image"] for c in spec["initContainers"]] == [
        cfg.images.duckdb if p == "duckdb" else cfg.images.spark for p in parts
    ]
    (serve,) = spec["containers"]
    assert serve["command"] == ["python3", tool, "serve", "--port", str(m.PORT)]
    for c in [*spec["initContainers"], serve]:
        assert {e["name"] for e in c["env"]} == {"HOME", "LB_DEPS_REQUEST_SHA256"}
        assert "args" not in c
    assert spec["automountServiceAccountToken"] is False
    assert {v["name"] for v in spec["volumes"]} == {"tools", "data", "work"}
    data_mount = next(v for v in serve["volumeMounts"] if v["name"] == "data")
    assert data_mount["readOnly"] is True


def test_deploy_all_runs_deps_after_the_operator_and_before_the_engines(monkeypatch):
    cfg = _cfg()
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    engine = DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=True)
    captured: list[str] = []
    monkeypatch.setattr(
        engine, "_run_steps", lambda steps, cb: captured.extend(s[0] for s in steps) or []
    )
    engine.deploy_all()
    i = captured.index("deps")
    assert captured.index("spark-operator") < i < captured.index("trino")
    assert i < captured.index("spark-thrift") and i < captured.index("duckdb")
    assert captured.index("rbac") < i  # the pod runs as the spark-runner ServiceAccount


# --- isolation --------------------------------------------------------------------


def test_two_deployments_isolated(recording_k8s, monkeypatch):
    """Deploying A touches nothing of B, whose namespace holds its own
    lb-deps objects and annotation; every object, read and URL of A is built
    from A's namespace, which differs from A's name here (a server URL or
    label built from the name would fail)."""
    rec = recording_k8s
    cfg = _cfg(name="dep-a", platform={"kubernetes": {"namespace": NS}})
    engine = _setup(rec, cfg)
    rec.add_namespace("dp2", annotations={m.ANNOTATION_DEPS_SET: "b" * 64})
    for kind, name in (
        ("configmaps", m.MANIFEST_CONFIGMAP),
        ("configmaps", m.tools_configmap_name("b" * 64)),
        ("services", m.SERVER_NAME),
    ):
        rec.add(
            kind, {"metadata": {"name": name, "labels": dict(m.SELECTOR_LABELS)}}, namespace="dp2"
        )
    rec.add("pods", {**_pod("lb-deps-b", "b" * 64), "metadata": {
        **_pod("lb-deps-b", "b" * 64)["metadata"], "namespace": "dp2"}}, namespace="dp2")  # fmt: skip
    before = {
        k: v for k, v in rec.store.items() if k[1] == "dp2" or k == ("namespaces", None, "dp2")
    }
    request = req.select_request(cfg)
    Controller(rec, monkeypatch)
    _show(rec, lambda sha: fake_shown(request))

    assert DependencyServerDeployer(engine).deploy().status == DeploymentStatus.SUCCESS

    after = {
        k: v for k, v in rec.store.items() if k[1] == "dp2" or k == ("namespaces", None, "dp2")
    }
    assert after == before
    assert engine.deps.base_url.startswith(f"http://lb-deps.{NS}.svc.cluster.local:8080/")
    assert all(c.namespace in (NS, None) for c in rec.calls if c.api != "kubectl")
    assert not [c for c in rec.calls if c.namespace == "*"]  # no all-namespace call
    cluster = [c for c in rec.mutations() if c.namespace is None]
    assert {(c.kind, c.name) for c in cluster} <= {("namespaces", NS)}
    labels = rec.store[("deployments", NS, m.SERVER_NAME)].metadata.labels
    assert labels["lakebench.io/deployment"] == "dep-a"


# --- review fixes (SD-4a Full review) ---------------------------------------------


def test_a_terminating_pvc_is_released_then_recreated(recording_k8s, monkeypatch):
    """ "Delete PVC lb-deps-data and re-run deploy" works: the server is
    scaled to zero so pvc-protection lets the claim go, then it is created
    again and the Deployment apply restores one replica."""
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    request = req.select_request(cfg)
    rec.add(
        "persistentvolumeclaims",
        {
            "metadata": {"name": m.PVC_NAME, "deletionTimestamp": "2026-10-01T00:00:00Z"},
            "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "x"},
        },
        namespace=NS,
    )
    rec.add("deployments", {"metadata": {"name": m.SERVER_NAME}, "spec": {
        "selector": {"matchLabels": dict(m.SELECTOR_LABELS)}, "template": {}}}, namespace=NS)  # fmt: skip
    rec.add("pods", _pod("lb-deps-old", "f" * 64, uid="uid-old"))
    real = DependencyServerDeployer._release_pvc

    def release(deployer, core):
        real(deployer, core)
        # pvc-protection lets go once no scheduled pod mounts the claim.
        rec.store.pop(("persistentvolumeclaims", NS, m.PVC_NAME))

    monkeypatch.setattr(DependencyServerDeployer, "_release_pvc", release)
    Controller(rec, monkeypatch)
    _show(rec, lambda sha: fake_shown(request))

    result = DependencyServerDeployer(engine).deploy()

    assert result.status == DeploymentStatus.SUCCESS, result.message
    scale = _idx(rec, kind="deployments", verb="patch")
    create = _idx(rec, kind="persistentvolumeclaims", verb="create")
    assert scale and create and scale[0] < create[0]
    rec.assert_recorded(verb="delete", kind="pods", name="lb-deps-old", scope=OWN)
    assert rec.store[("deployments", NS, m.SERVER_NAME)].spec.replicas == 1


def test_the_rollout_must_finish_before_a_ready_pod_counts(recording_k8s, monkeypatch):
    """A Ready pod of this request while the controller has not observed the
    spec (or an old pod still counts) is not the server yet."""
    from kubernetes.client.models import V1DeploymentStatus

    rec = recording_k8s
    monkeypatch.setattr(deps_mod, "READY_TIMEOUT_S", 1)
    cfg = _cfg()
    engine = _setup(rec, cfg)
    ctl = Controller(rec, monkeypatch)
    real_roll = ctl.roll

    def half_rolled(sha):
        real_roll(sha)
        rec.store[("deployments", NS, m.SERVER_NAME)].status = V1DeploymentStatus(
            observed_generation=1, replicas=2, updated_replicas=1, ready_replicas=1
        )

    ctl.roll = half_rolled
    _show(rec, lambda sha: pytest.fail("show must not run"))
    result = DependencyServerDeployer(engine).deploy()
    assert result.status == DeploymentStatus.FAILED and "Timeout" in result.message


def test_a_server_replaced_after_the_wait_is_not_recorded(recording_k8s, monkeypatch):
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    request = req.select_request(cfg)
    Controller(rec, monkeypatch)

    def show_then_replace(sha):
        dep = rec.store[("deployments", NS, m.SERVER_NAME)]
        dep.spec.template.metadata.annotations[m.POD_ANNOTATION_REQUEST] = "c" * 64
        return fake_shown(request)

    _show(rec, show_then_replace)
    result = DependencyServerDeployer(engine).deploy()
    assert result.status == DeploymentStatus.FAILED and "changed while" in result.message
    assert ("configmaps", NS, m.MANIFEST_CONFIGMAP) not in rec.store
    assert m.ANNOTATION_DEPS_SET not in _ns_annotations(rec)


def test_a_tools_map_with_other_bytes_is_refused(recording_k8s):
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    sha = req.select_request(cfg).request_sha256
    rec.add(
        "configmaps",
        {"metadata": {"name": m.tools_configmap_name(sha)}, "data": {"lb_deps.py": "x"}},
        namespace=NS,
    )
    result = DependencyServerDeployer(engine).deploy()
    assert result.status == DeploymentStatus.FAILED and "delete it and re-run" in result.message


def test_the_manifest_records_the_images_that_ran(recording_k8s, monkeypatch):
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    request = req.select_request(cfg)
    Controller(rec, monkeypatch)
    _show(rec, lambda sha: fake_shown(request))
    assert DependencyServerDeployer(engine).deploy().status == DeploymentStatus.SUCCESS
    images = json.loads(
        rec.store[("configmaps", NS, m.MANIFEST_CONFIGMAP)].data["server-images.json"]
    )
    assert set(images) == {"serve", "resolve-spark"}
    assert "image_id" in images["serve"]


# --- brief-pass fixes ----------------------------------------------------------------


def test_a_failed_step_clears_an_annotation_written_meanwhile(recording_k8s, monkeypatch):
    """Another deploy of the namespace may write the annotation while this one
    runs and then lose its server to this one's rollout."""
    rec = recording_k8s
    cfg = _cfg()
    engine = _setup(rec, cfg)
    request = req.select_request(cfg)
    Controller(rec, monkeypatch)

    def concurrent_write(sha):
        ns = rec.store[("namespaces", None, NS)]
        ns.metadata.annotations = {**ns.metadata.annotations, m.ANNOTATION_DEPS_SET: "d" * 64}
        return fake_shown(request, pinset_sha256="e" * 64)  # this deploy's check fails

    _show(rec, concurrent_write)
    result = DependencyServerDeployer(engine).deploy()
    assert result.status == DeploymentStatus.FAILED
    assert m.ANNOTATION_DEPS_SET not in _ns_annotations(rec)
