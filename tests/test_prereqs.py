"""DEP-4: the prerequisite registry and its read-only checks.

Each check runs against a fake ``ClusterReader``; nothing reaches a cluster.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from lakebench.config import LakebenchConfig
from lakebench.deploy import prereqs as pr
from lakebench.deploy.prereqs import DeploymentView, PrereqStatus


@dataclass
class FakeReader:
    crds: set[str] = field(
        default_factory=lambda: {
            pr.SPARK_APP_CRD,
            "hiveclusters.hive.stackable.tech",
            "secretclasses.secrets.stackable.tech",
        }
    )
    scs: set[str] = field(default_factory=lambda: {"px-csi-scratch"})
    deps: dict[str, list[DeploymentView]] = field(default_factory=dict)
    running: set[str] = field(
        default_factory=lambda: {
            f"app.kubernetes.io/name={op}"
            for op in ("hive-operator", "secret-operator", "listener-operator", "commons-operator")
        }
    )
    role: bool | None = True
    openshift: bool = True
    s3: tuple[bool, str] = (True, "S3 ok")
    calls: list[str] = field(default_factory=list)

    def crd_names(self):
        return self.crds

    def storage_class_names(self):
        return self.scs

    def deployments(self, label_selector, namespace=None):
        self.calls.append(f"{namespace}:{label_selector}")
        return [
            d
            for d in self.deps.get(label_selector, [])
            if namespace is None or d.namespace == namespace
        ]

    def running_pod_exists(self, label_selector):
        return label_selector in self.running

    def cluster_role_exists(self, name):
        assert name == "system:openshift:scc:anyuid"
        return self.role

    def is_openshift(self):
        return self.openshift

    def s3_probe(self, cfg):
        return self.s3


def _controller(ready=1, chart="spark-operator-2.5.1", component="controller", ns="spark-operator"):
    labels = {"app.kubernetes.io/name": "spark-operator", "helm.sh/chart": chart}
    if component:
        labels["app.kubernetes.io/component"] = component
    return DeploymentView(ns, f"spark-operator-{component or 'x'}", ready, labels)


def _reader(**kw) -> FakeReader:
    r = FakeReader(**kw)
    r.deps.setdefault("app.kubernetes.io/name=spark-operator", [_controller()])
    return r


def _cfg(**over) -> LakebenchConfig:
    base: dict = {
        "name": "t",
        "platform": {
            "storage": {
                "s3": {"endpoint": "http://10.0.1.50:80", "access_key": "a", "secret_key": "b"},
                "scratch": {"enabled": True},
            }
        },
    }
    for k, v in over.items():
        base[k] = v
    return LakebenchConfig(**base)


def _result(outcomes, pid):
    return next(o.result for o in outcomes if o.prereq.id == pid)


def test_registry_ids_unique_and_documented():
    ids = [p.id for p in pr.PREREQS]
    assert len(ids) == len(set(ids))
    for p in pr.PREREQS:
        assert p.title and p.doc and p.fix and p.when, p.id
        assert "\u2014" not in p.doc + p.fix + p.title


def test_all_ok_on_a_complete_cluster():
    out = pr.run_prereqs(_cfg(), _reader())
    by = {o.prereq.id: o.result.status for o in out}
    assert by == {
        "scratch-storage-class": PrereqStatus.OK,
        "spark-operator": PrereqStatus.OK,
        "stackable": PrereqStatus.OK,
        "observability-stack": PrereqStatus.SKIPPED,
        "openshift-scc-clusterrole": PrereqStatus.OK,
        "s3-reachable-and-credentials": PrereqStatus.OK,
    }
    assert _result(out, "spark-operator").message.startswith("Spark Operator 2.5.1 ready")


def test_missing_scratch_storage_class_fails():
    res = _result(pr.run_prereqs(_cfg(), _reader(scs=set())), "scratch-storage-class")
    assert res.status is PrereqStatus.FAIL
    assert "px-csi-scratch" in res.message


def test_scratch_disabled_is_skipped():
    cfg = _cfg(
        platform={"storage": {"s3": {"endpoint": "http://x"}, "scratch": {"enabled": False}}}
    )
    res = _result(pr.run_prereqs(cfg, _reader(scs=set())), "scratch-storage-class")
    assert res.status is PrereqStatus.SKIPPED


@pytest.mark.parametrize(
    "deps,status,text",
    [
        (
            [],
            PrereqStatus.FAIL,
            "no Spark Operator controller Deployment in namespace spark-operator",
        ),
        ([_controller(ready=0)], PrereqStatus.FAIL, "not ready"),
        ([_controller(chart="spark-operator-1.1.27")], PrereqStatus.FAIL, "not a supported 2.x"),
        ([_controller(chart="")], PrereqStatus.OK, "chart version unknown"),
        ([_controller(component="webhook")], PrereqStatus.FAIL, "no Spark Operator controller"),
        ([_controller(component="webhook"), _controller()], PrereqStatus.OK, "2.5.1"),
        ([_controller(ns="tenant-b")], PrereqStatus.FAIL, "no Spark Operator controller"),
    ],
)
def test_spark_operator_states(deps, status, text):
    r = _reader()
    r.deps["app.kubernetes.io/name=spark-operator"] = deps
    res = _result(pr.run_prereqs(_cfg(), r), "spark-operator")
    assert res.status is status and text in res.message


def test_spark_operator_is_read_in_the_configured_namespace():
    """A ready operator in another tenant's namespace is not ours."""
    r = _reader()
    r.deps["app.kubernetes.io/name=spark-operator"] = [
        _controller(ready=0),
        _controller(ns="tenant-b"),
    ]
    res = _result(pr.run_prereqs(_cfg(), r), "spark-operator")
    assert res.status is PrereqStatus.FAIL
    assert "spark-operator:app.kubernetes.io/name=spark-operator" in r.calls


def test_operator_missing_under_install_true_is_a_warning():
    r = _reader()
    r.deps["app.kubernetes.io/name=spark-operator"] = []
    cfg = _cfg(
        platform={
            "storage": {"s3": {"endpoint": "http://x", "access_key": "a", "secret_key": "b"}},
            "compute": {"spark": {"operator": {"install": True}}},
        }
    )
    res = _result(pr.run_prereqs(cfg, r), "spark-operator")
    assert res.status is PrereqStatus.WARN


def test_spark_operator_crd_missing():
    res = _result(pr.run_prereqs(_cfg(), _reader(crds=set())), "spark-operator")
    assert res.status is PrereqStatus.FAIL and "CRD not found" in res.message


def test_stackable_only_for_hive():
    cfg = _cfg(recipe="polaris-iceberg-spark-trino")
    res = _result(pr.run_prereqs(cfg, _reader(crds={pr.SPARK_APP_CRD})), "stackable")
    assert res.status is PrereqStatus.SKIPPED


def test_stackable_crds_without_running_operator_fail():
    res = _result(pr.run_prereqs(_cfg(), _reader(running=set())), "stackable")
    assert res.status is PrereqStatus.FAIL
    assert "hive-operator, secret-operator" in res.message


def test_observability_missing_is_a_warning():
    cfg = _cfg(observability={"enabled": True})
    res = _result(pr.run_prereqs(cfg, _reader()), "observability-stack")
    assert res.status is PrereqStatus.WARN


@pytest.mark.parametrize("ready,status", [(1, PrereqStatus.OK), (0, PrereqStatus.WARN)])
def test_observability_ready(ready, status):
    r = _reader()
    r.deps["release=lakebench-observability"] = [
        DeploymentView("lakebench-observability", "prom-operator", ready)
    ]
    cfg = _cfg(observability={"enabled": True})
    assert _result(pr.run_prereqs(cfg, r), "observability-stack").status is status


def test_stackable_support_operator_missing_is_a_warning():
    r = _reader()
    r.running.discard("app.kubernetes.io/name=listener-operator")
    res = _result(pr.run_prereqs(_cfg(), r), "stackable")
    assert res.status is PrereqStatus.WARN and "listener-operator" in res.message


@pytest.mark.parametrize(
    "openshift,role,status",
    [
        (False, True, PrereqStatus.SKIPPED),
        (True, True, PrereqStatus.OK),
        (True, False, PrereqStatus.FAIL),
        (True, None, PrereqStatus.WARN),
    ],
)
def test_scc_clusterrole(openshift, role, status):
    res = _result(
        pr.run_prereqs(_cfg(), _reader(openshift=openshift, role=role)), "openshift-scc-clusterrole"
    )
    assert res.status is status


def test_s3_failure():
    res = _result(
        pr.run_prereqs(_cfg(), _reader(s3=(False, "S3 check failed: 403"))),
        "s3-reachable-and-credentials",
    )
    assert res.status is PrereqStatus.FAIL and "403" in res.message


def test_s3_without_inline_keys_fails_and_is_not_probed():
    """secret_ref never supplies credentials (the loader refuses it alone), so
    a config without inline keys fails the check without a probe, and the
    fix text does not offer secret_ref."""
    cfg = _cfg(platform={"storage": {"s3": {"endpoint": "http://10.0.1.50:80"}}})
    reader = _reader(s3=(True, "probe must not run"))
    res = _result(pr.run_prereqs(cfg, reader), "s3-reachable-and-credentials")
    assert res.status is PrereqStatus.FAIL and "no S3 credentials" in res.message
    assert "probe must not run" not in res.message
    fix = next(p.fix for p in pr.PREREQS if p.id == "s3-reachable-and-credentials")
    assert "secret_ref" not in fix


def test_components_name_the_admin_install_targets():
    comps = {p.id: p.component for p in pr.PREREQS}
    assert comps["scratch-storage-class"] == "scratch-storage-class"
    assert comps["spark-operator"] == "spark-operator"
    assert comps["stackable"] == "stackable"
    assert comps["observability-stack"] == "observability"
    assert comps["s3-reachable-and-credentials"] is None


def test_offline_listing_needs_no_reader():
    """plan --offline: applies() alone says what a config needs."""
    needed = [p.id for p in pr.PREREQS if p.applies(_cfg())]
    assert "spark-operator" in needed and "observability-stack" not in needed


def test_reader_without_config_is_cluster_unreachable(monkeypatch):
    from kubernetes import config

    def no_config(*a, **k):
        raise config.ConfigException("no kubeconfig")

    monkeypatch.setattr(config, "load_kube_config", no_config)
    monkeypatch.setattr(config, "load_incluster_config", no_config)
    with pytest.raises(pr.ClusterUnreachable, match="no usable Kubernetes config"):
        pr.KubeClusterReader(_cfg())


def test_run_preflight_uses_the_registry(monkeypatch):
    """`lakebench run`'s preflight reports the registry's checks by id."""
    from unittest.mock import MagicMock

    from lakebench.cli import _prerequisites as cp

    monkeypatch.setattr("lakebench.k8s.get_k8s_client", lambda **k: MagicMock())
    r = _reader(scs=set())
    monkeypatch.setattr(pr, "KubeClusterReader", lambda cfg, load_config=True: r)
    results = {c.name: c for c in cp._registry_checks(_cfg())}
    applicable = {p.id for p in pr.PREREQS if p.applies(_cfg())} - {"openshift-scc-clusterrole"}
    assert applicable <= set(results)
    scratch = results["scratch-storage-class"]
    assert scratch.passed is False and "install-scratch-storage-class" in scratch.hint


def test_run_preflight_without_cluster_does_not_reach_one(monkeypatch):
    from lakebench.cli import _prerequisites as cp

    def boom(**k):
        raise RuntimeError("no cluster")

    monkeypatch.setattr("lakebench.k8s.get_k8s_client", boom)
    [res] = cp._registry_checks(_cfg())
    assert res.passed is False and "no cluster" in res.message


def test_a_check_that_raises_is_unknown_not_a_crash():
    class Broken(FakeReader):
        def crd_names(self):
            raise RuntimeError("apiserver 503")

    r = Broken()
    r.deps["app.kubernetes.io/name=spark-operator"] = [_controller()]
    res = _result(pr.run_prereqs(_cfg(), r), "spark-operator")
    assert res.status is PrereqStatus.UNKNOWN
    assert res.message == "could not check: apiserver 503"


def test_kube_reader_makes_only_reads():
    """Every API method KubeClusterReader calls is a read (list/read)."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(pr.KubeClusterReader))
    called = {
        n.func.attr
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    }
    writes = sorted(c for c in called if c.startswith(("create_", "delete_", "patch_", "replace_")))
    assert writes == []
    assert {"list_custom_resource_definition", "read_cluster_role"} <= called
