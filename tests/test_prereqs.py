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
    scs: set[str] = field(default_factory=lambda: {"px-csi-scratch", "px-csi-db"})
    default_scs: set[str] = field(default_factory=lambda: {"px-csi-db"})
    pvcs: dict[tuple[str, str], str] = field(default_factory=dict)
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

    def default_storage_class_names(self):
        return self.default_scs

    def pvc_storage_class(self, namespace, name):
        return self.pvcs.get((namespace, name))

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


def test_spark_operator_crd_missing():
    res = _result(pr.run_prereqs(_cfg(), _reader(crds=set())), "spark-operator")
    assert res.status is PrereqStatus.FAIL and "CRD not found" in res.message


def test_stackable_crds_without_running_operator_fail():
    res = _result(pr.run_prereqs(_cfg(), _reader(running=set())), "stackable")
    assert res.status is PrereqStatus.FAIL
    assert "hive-operator, secret-operator" in res.message


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
