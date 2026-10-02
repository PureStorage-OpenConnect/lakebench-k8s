"""DEP-3 (SD-10): shared components install through ``admin install
--component`` under the lease, and deploy only verifies them.

Cluster calls run against the recording fixture (tests/fixtures/recording_k8s.py,
SD-9), so a shared mutation without the lease, or any mutation where the
test expects none, fails the test.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.cli._admin import admin_app
from lakebench.deploy import shared_components as sc
from lakebench.deploy.cluster_lock import LOCK_CONFIGMAP_NAME
from tests.conftest import make_config
from tests.fixtures.recording_k8s import K8sRecorder, recording

runner = CliRunner()
NS = "sd10-t"
OBS_NS = "lakebench-observability"
SCRATCH_PARAMS = {"repl": "1", "io_profile": "auto", "priority_io": "high"}
STACKABLE_OPS = ("commons-operator", "listener-operator", "secret-operator", "hive-operator")


def _config(tmp_path: Path, **over) -> Path:
    data: dict = {
        "name": NS,
        "platform": {
            "storage": {
                "s3": {"endpoint": "http://10.0.1.50:80", "access_key": "a", "secret_key": "b"},
                "scratch": {"enabled": True},
            }
        },
        "observability": {"enabled": True},
    }
    for k, v in over.items():
        data[k] = v
    path = tmp_path / "lakebench.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def _running_pod(rec: K8sRecorder, op: str, ns: str = "stackable") -> None:
    from kubernetes.client.models import (
        V1Container,
        V1ObjectMeta,
        V1Pod,
        V1PodSpec,
        V1PodStatus,
    )

    rec.add(
        "pods",
        V1Pod(
            metadata=V1ObjectMeta(name=f"{op}-0", labels={"app.kubernetes.io/name": op}),
            spec=V1PodSpec(containers=[V1Container(name=op)]),
            status=V1PodStatus(phase="Running"),
        ),
        namespace=ns,
    )


def _seed_scratch(rec: K8sRecorder, params: dict | None = None) -> None:
    rec.add(
        "storageclasses",
        {
            "metadata": {"name": "px-csi-scratch"},
            "provisioner": "pxd.portworx.com",
            "parameters": dict(SCRATCH_PARAMS if params is None else params),
        },
    )


def _seed_stackable(rec: K8sRecorder) -> None:
    rec.add_namespace("stackable")
    rec.add_stackable()
    for op in STACKABLE_OPS:
        _running_pod(rec, op)


def _prometheus_ready(rec: K8sRecorder, ready: bool = True) -> None:
    """Answer the one-shot Prometheus readiness read (a jsonpath range the
    fake cannot evaluate)."""
    rec.on_command("kubectl", "get", "pods", "-n", OBS_NS, stdout="True\n" if ready else "")


def _seed_observability(rec: K8sRecorder, *, dashboard: bool = True, ready: bool = True) -> None:
    rec.add_namespace(OBS_NS)
    rec.add_helm_release(OBS_NS, OBS_NS, chart="kube-prometheus-stack", version="87.19.2")
    _prometheus_ready(rec, ready)
    if dashboard:
        for doc in sc.Observability.dashboard_manifests():
            rec.add("configmaps", doc, namespace=OBS_NS)


def _seed_all(rec: K8sRecorder, *, operator_version: str = "2.5.1", watched=("default",)) -> None:
    _seed_scratch(rec)
    rec.add_spark_operator(watched=list(watched), version=operator_version)
    _seed_stackable(rec)
    _seed_observability(rec)


def _lease_writes(rec: K8sRecorder) -> list:
    return [c for c in rec.mutations() if c.name == LOCK_CONFIGMAP_NAME]


def _invoke(*args: str):
    return runner.invoke(admin_app, list(args), env={"COLUMNS": "200"})


# ---------------------------------------------------------------------------
# admin install on an installed cluster is a no-op
# ---------------------------------------------------------------------------


def test_admin_install_noop_when_installed(tmp_path):
    """DEP-3 acceptance: every component installed -> exit 0, zero mutations
    and no lease taken. The reads are asserted so a short-circuit cannot
    pass as a no-op."""
    cfg = _config(tmp_path)
    with recording(NS) as rec:
        _seed_all(rec)
        r = _invoke("install", str(cfg), "--component", "all", "-y")
        assert r.exit_code == 0, r.output
        assert rec.mutations() == [], [c.describe() for c in rec.mutations()]
        assert _lease_writes(rec) == []
        rec.assert_recorded(api="helm", verb="list")
        rec.assert_recorded(kind="storageclasses", verb="read")
        rec.assert_recorded(kind="configmaps", verb="read", namespace=OBS_NS)
        rec.assert_clean()
    out = " ".join(r.output.split())
    assert "--component all: scratch-storage-class, spark-operator, stackable, observability" in out
    assert "nothing to change" in out
    for name, version in (
        ("spark-operator", "2.5.1"),
        ("stackable", "25.7.0"),
        ("observability", "87.19.2"),
    ):
        row = rf"{name}\s*│\s*yes\s*│\s*{re.escape(version)}\s*│\s*yes"
        assert re.search(row, r.output), r.output


def test_config_pin_does_not_move_installed_version(tmp_path):
    """cluster-safety 3: a config pinning 2.4.0 against an installed 2.5.1
    issues no helm write. Reverted, the old verb upgraded to the config pin."""
    cfg = _config(
        tmp_path,
        platform={
            "storage": {
                "s3": {"endpoint": "http://10.0.1.50:80", "access_key": "a", "secret_key": "b"}
            },
            "compute": {"spark": {"operator": {"version": "2.4.0"}}},
        },
    )
    with recording(NS) as rec:
        rec.add_spark_operator(watched=["default", "a"], version="2.5.1")
        r = _invoke("install-spark-operator", str(cfg))
        assert r.exit_code == 0, r.output
        assert not [c for c in rec.calls if c.api == "helm" and c.mutating]
        assert rec.releases[("spark-operator", "spark-operator")].version == "2.5.1"
        rec.assert_clean()


def test_version_change_refused_without_flag(tmp_path):
    with recording(NS) as rec:
        _seed_all(rec)
        r = _invoke(
            "install", "--component", "spark-operator", "--version", "spark-operator=2.4.0", "-y"
        )
        assert r.exit_code == 2, r.output
        assert rec.mutations() == []
    out = " ".join(r.output.split())
    assert "installed at 2.5.1; the target is 2.4.0" in out
    assert "--allow-version-change" in out


def test_version_change_refused_while_watched(tmp_path):
    """Exit 3 listing both users, zero shared mutations."""
    with recording(NS) as rec:
        _seed_all(rec, watched=("a", "b"))
        for ns in ("a", "b"):
            rec.add_namespace(ns, annotations={"lakebench.deployment/name": ns})
        r = _invoke(
            "install",
            "--component",
            "spark-operator",
            "--version",
            "spark-operator=2.4.0",
            "--allow-version-change",
            "-y",
        )
        assert r.exit_code == 3, r.output
        assert rec.shared_mutations() == []
        assert rec.mutations() == []
    out = " ".join(r.output.split())
    assert "2 deployment(s) use spark-operator: ['a', 'b']" in out


def test_version_change_ignores_default_placeholder(tmp_path):
    """`default` is the chart's placeholder, not a user: no users are listed,
    and the change is still refused (helm leaves the chart's CRDs behind)."""
    with recording(NS) as rec:
        _seed_all(rec, watched=("default",))
        r = _invoke(
            "install",
            "--component",
            "spark-operator",
            "--version",
            "spark-operator=2.4.0",
            "--allow-version-change",
            "-y",
        )
        assert r.exit_code == 3, r.output
        assert rec.mutations() == []
    out = " ".join(r.output.split())
    assert "deployment(s) use" not in out
    assert "does not automate the change" in out and "crds/" in out


@pytest.mark.parametrize("component,pair", [("stackable", "24.11.0"), ("observability", "80.0.0")])
def test_stackable_and_observability_version_changes_are_refused(component, pair):
    with recording(NS) as rec:
        _seed_all(rec)
        r = _invoke(
            "install",
            "--component",
            component,
            "--version",
            f"{component}={pair}",
            "--allow-version-change",
            "-y",
        )
        assert r.exit_code == 3, r.output
        assert rec.mutations() == []


# ---------------------------------------------------------------------------
# Fresh installs: every shared write under the lease
# ---------------------------------------------------------------------------


def test_fresh_spark_operator_install_is_leased_and_uses_helm_install():
    with recording(NS) as rec:

        def installed(argv, kwargs):
            rec.add_spark_operator(watched=["default"], version="2.5.1")
            return 0, "installed", ""

        rec.on_command("helm", "install", "spark-operator", handler=installed)
        with patch(
            "lakebench.modules.pipeline_engines.spark.operator.SparkOperatorManager._verify_tmp_size",
            return_value=True,
        ):
            r = _invoke("install", "--component", "spark-operator", "-y")
        assert r.exit_code == 0, r.output
        writes = [c for c in rec.calls if c.api == "helm" and c.mutating]
        assert [c.verb for c in writes] == ["install"], [c.describe() for c in writes]
        assert all(c.lease_held for c in writes)
        assert _lease_writes(rec)  # the lease was taken and released
        rec.assert_clean()
    argv = next(c.argv for c in rec.calls if c.api == "helm" and c.verb == "install")
    assert "--reuse-values" not in argv
    assert argv[argv.index("--version") + 1] == "2.5.1"
    assert not any(a.startswith("spark.jobNamespaces") for a in argv)


def test_fresh_stackable_install_installs_four_charts_under_the_lease():
    with recording(NS) as rec:

        def installed(argv, kwargs):
            op = argv[argv.index("install") + 1]
            if op == "secret-operator":
                rec.add_crd(
                    "secretclasses", "secrets.stackable.tech", "SecretClass", scope="Cluster"
                )
            if op == "hive-operator":
                rec.add_crd("hiveclusters", "hive.stackable.tech", "HiveCluster")
            rec.add_helm_release(op, "stackable", chart=op, version="25.7.0")
            _running_pod(rec, op)
            return 0, "", ""

        rec.on_command("helm", "install", handler=installed)
        r = _invoke("install", "--component", "stackable", "-y")
        assert r.exit_code == 0, r.output
        writes = [c for c in rec.calls if c.api == "helm" and c.mutating]
        assert [c.name for c in writes] == list(STACKABLE_OPS)
        assert all(c.lease_held and c.verb == "install" for c in writes)
        rec.assert_clean()
    first = next(c.argv for c in rec.calls if c.api == "helm" and c.verb == "install")
    assert "--create-namespace" in first and "--wait" not in first


def test_fresh_observability_install_applies_the_dashboard_under_the_lease(tmp_path):
    cfg = _config(tmp_path)
    with recording(NS) as rec:

        def installed(argv, kwargs):
            rec.add_namespace(OBS_NS)
            rec.add_helm_release(OBS_NS, OBS_NS, chart="kube-prometheus-stack", version="87.19.2")
            return 0, "", ""

        rec.on_command("helm", "install", OBS_NS, handler=installed)
        with patch("lakebench.deploy.observability._wait_for_prometheus", return_value=None) as w:
            r = _invoke("install", str(cfg), "--component", "observability", "-y")
        assert r.exit_code == 0, r.output
        shared = rec.shared_mutations()
        assert any(c.api == "helm" and c.verb == "install" for c in shared)
        assert any(c.kind == "configmaps" and c.namespace == OBS_NS for c in shared)
        assert all(c.lease_held for c in shared)
        rec.assert_clean()
        # readiness is awaited (the full wait, not only the one-shot status read)
        assert any("timeout_s" not in c.kwargs for c in w.call_args_list)
    argv = next(c.argv for c in rec.calls if c.api == "helm" and c.verb == "install")
    assert argv[argv.index("--version") + 1] == "87.19.2"
    assert not any("adminPassword" in a for a in argv)


def test_stale_dashboard_is_updated_under_the_lease(tmp_path):
    cfg = _config(tmp_path)
    with recording(NS) as rec:
        _seed_observability(rec, dashboard=False)
        r = _invoke("install", str(cfg), "--component", "observability", "-y")
        assert r.exit_code == 0, r.output
        shared = rec.shared_mutations()
        assert shared and all(c.kind == "configmaps" and c.lease_held for c in shared)
        assert not [c for c in rec.calls if c.api == "helm" and c.mutating]
        rec.assert_clean()


# ---------------------------------------------------------------------------
# States admin install must not touch
# ---------------------------------------------------------------------------


def test_second_spark_operator_is_never_installed():
    with recording(NS) as rec:
        rec.add_spark_operator(watched=["default"], namespace="other-ops")
        r = _invoke("install", "--component", "spark-operator", "-y")
        assert r.exit_code == 3, r.output
        assert rec.mutations() == []
    assert "already runs in namespace(s) other-ops" in " ".join(r.output.split())


def test_leftover_spark_crd_refuses():
    with recording(NS) as rec:
        rec.add_crd("sparkapplications", "sparkoperator.k8s.io", "SparkApplication")
        r = _invoke("install", "--component", "spark-operator", "-y")
        assert r.exit_code == 3, r.output
        assert rec.mutations() == []


@pytest.mark.parametrize("status", ["pending-install", "pending-upgrade", "failed", "uninstalling"])
def test_release_not_deployed_refuses(status):
    with recording(NS) as rec:
        rec.add_spark_operator(watched=["default"])
        rec.releases[("spark-operator", "spark-operator")].status = status
        r = _invoke("install", "--component", "spark-operator", "-y")
        assert r.exit_code == 1, r.output
        assert rec.mutations() == []
    assert f"is {status!r}, not 'deployed'" in " ".join(r.output.split())


def test_unreadable_status_refuses_and_changes_nothing():
    with recording(NS) as rec:
        rec.on_command("helm", "list", returncode=1, stderr="Error: Unauthorized")
        r = _invoke("install", "--component", "spark-operator", "-y")
        assert r.exit_code == 1, r.output
        assert rec.mutations() == []
    assert "cannot tell whether spark-operator is installed" in " ".join(r.output.split())


def test_stackable_installed_in_another_namespace_is_installed():
    """Releases in another namespace count: no second operator set."""
    with recording(NS) as rec:
        rec.add_namespace("sdp")
        rec.add_crd("hiveclusters", "hive.stackable.tech", "HiveCluster")
        rec.add_crd("secretclasses", "secrets.stackable.tech", "SecretClass", scope="Cluster")
        for op in STACKABLE_OPS:
            rec.add_helm_release(op, "sdp", chart=op, version="25.7.0")
            _running_pod(rec, op, ns="sdp")
        r = _invoke("install", "--component", "stackable", "-y")
        assert r.exit_code == 0, r.output
        assert rec.mutations() == []


def test_stackable_leftover_crds_refuse():
    with recording(NS) as rec:
        rec.add_crd("hiveclusters", "hive.stackable.tech", "HiveCluster")
        r = _invoke("install", "--component", "stackable", "-y")
        assert r.exit_code == 3, r.output
        assert rec.mutations() == []


def test_partial_stackable_is_completed_at_its_version():
    with recording(NS) as rec:
        rec.add_namespace("stackable")
        for op in STACKABLE_OPS[:2]:
            rec.add_helm_release(op, "stackable", chart=op, version="25.3.0")
            _running_pod(rec, op)

        def installed(argv, kwargs):
            op = argv[argv.index("install") + 1]
            if op == "secret-operator":
                rec.add_crd(
                    "secretclasses", "secrets.stackable.tech", "SecretClass", scope="Cluster"
                )
            else:
                rec.add_crd("hiveclusters", "hive.stackable.tech", "HiveCluster")
            rec.add_helm_release(op, "stackable", chart=op, version="25.3.0")
            _running_pod(rec, op)
            return 0, "", ""

        rec.on_command("helm", "install", handler=installed)
        r = _invoke("install", "--component", "stackable", "-y")
        assert r.exit_code == 0, r.output
        writes = [c for c in rec.calls if c.api == "helm" and c.mutating]
        assert [c.name for c in writes] == ["secret-operator", "hive-operator"]
        assert all(c.lease_held for c in writes)
    for c in writes:
        assert c.argv[c.argv.index("--version") + 1] == "25.3.0"
        assert "--create-namespace" not in c.argv


def test_scratch_parameter_diff_always_refuses(tmp_path):
    """A StorageClass is immutable, so a parameter difference refuses
    (exit 3), named or under all, and changes nothing."""
    cfg = _config(tmp_path)
    with recording(NS) as rec:
        _seed_all(rec)
        rec.store.pop(("storageclasses", None, "px-csi-scratch"))
        _seed_scratch(rec, {"repl": "3"})
        named = _invoke("install", str(cfg), "--component", "scratch-storage-class", "-y")
        under_all = _invoke("install", str(cfg), "--component", "all", "-y")
        alias = _invoke("install-scratch-storage-class", str(cfg))
        assert rec.mutations() == []
    for r in (named, under_all, alias):
        assert r.exit_code == 3, r.output
        assert "repl='3' (config '1')" in " ".join(r.output.split())


def test_version_change_names_stale_entries_and_repair():
    with recording(NS) as rec:
        _seed_all(rec, watched=("default", "gone"))
        r = _invoke(
            "install",
            "--component",
            "spark-operator",
            "--version",
            "spark-operator=2.4.0",
            "--allow-version-change",
            "-y",
        )
        assert r.exit_code == 3, r.output
        assert rec.mutations() == []
    out = " ".join(r.output.split())
    assert "deleted namespaces ['gone']" in out and "repair-operator" in out
    assert "No deployment uses it" in out


def test_version_change_with_an_unreadable_watch_list_says_users_are_unknown():
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    with recording(NS) as rec:
        _seed_all(rec)
        with patch.object(
            SparkOperatorManager, "_get_active_namespaces", side_effect=RuntimeError("timeout")
        ):
            r = _invoke(
                "install",
                "--component",
                "spark-operator",
                "--version",
                "spark-operator=2.4.0",
                "--allow-version-change",
                "-y",
            )
        assert r.exit_code == 3, r.output
    out = " ".join(r.output.split())
    assert "deployments using it are unknown" in out
    assert "No deployment uses it" not in out


def test_config_pin_differing_from_the_installed_version_warns(tmp_path):
    cfg = _config(
        tmp_path,
        platform={
            "storage": {
                "s3": {"endpoint": "http://10.0.1.50:80", "access_key": "a", "secret_key": "b"}
            },
            "compute": {"spark": {"operator": {"version": "2.4.0"}}},
        },
    )
    with recording(NS) as rec:
        rec.add_spark_operator(watched=["default"], version="2.5.1")
        r = _invoke("install", str(cfg), "--component", "spark-operator", "-y")
        assert r.exit_code == 0, r.output
        assert rec.mutations() == []
    out = " ".join(r.output.split())
    assert "installed at 2.5.1, the config (or Lakebench default) names 2.4.0" in out


def test_another_kube_prometheus_stack_is_never_doubled(tmp_path):
    cfg = _config(tmp_path)
    with recording(NS) as rec:
        rec.add_namespace("monitoring")
        rec.add_helm_release("prom", "monitoring", chart="kube-prometheus-stack", version="80.0.0")
        r = _invoke("install", str(cfg), "--component", "observability", "-y")
        assert r.exit_code == 3, r.output
        assert rec.mutations() == []
    assert "prom in monitoring" in " ".join(r.output.split())


def test_observability_not_ready_exits_one(tmp_path):
    cfg = _config(tmp_path)
    with recording(NS) as rec:
        _seed_observability(rec, ready=False)
        r = _invoke("install", str(cfg), "--component", "observability", "-y")
        assert r.exit_code == 1, r.output
        assert rec.mutations() == []
    assert "not ready: observability" in " ".join(r.output.split())


def test_partial_stackable_with_a_leftover_crd_refuses():
    with recording(NS) as rec:
        rec.add_namespace("stackable")
        for op in STACKABLE_OPS[:3]:
            rec.add_helm_release(op, "stackable", chart=op, version="25.7.0")
            _running_pod(rec, op)
        rec.add_crd("hiveclusters", "hive.stackable.tech", "HiveCluster")  # hive-operator's
        r = _invoke("install", "--component", "stackable", "-y")
        assert r.exit_code == 3, r.output
        assert rec.mutations() == []
    assert "CRD hiveclusters.hive.stackable.tech" in " ".join(r.output.split())


def test_installed_but_not_ready_exits_one():
    with recording(NS) as rec:
        rec.add_spark_operator(watched=["default"])
        dep = rec.store[("deployments", "spark-operator", "spark-operator-controller")]
        dep.status.ready_replicas = 0
        r = _invoke("install", "--component", "spark-operator", "-y")
        assert r.exit_code == 1, r.output
        assert rec.mutations() == []
    assert "not ready: spark-operator" in " ".join(r.output.split())


def test_controller_tmp_size_on_an_installed_operator_names_repair():
    with recording(NS) as rec:
        rec.add_spark_operator(watched=["default"])
        r = _invoke(
            "install", "--component", "spark-operator", "--controller-tmp-size", "16Gi", "-y"
        )
        assert r.exit_code == 2, r.output
        assert rec.mutations() == []
    assert "admin repair-operator --controller-tmp-size 16Gi" in " ".join(r.output.split())


# ---------------------------------------------------------------------------
# Usage errors (nothing reaches the cluster)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "args,needle",
    [
        (["--component", "all"], "--component all needs a config"),
        (["--component", "nope"], "unknown component"),
        ([], "name at least one --component"),
        (["-c", "spark-operator", "--version", "stackable=25.7.0"], "not a requested"),
        (["-c", "spark-operator", "--version", "spark-operator=^2.0.0"], "exact chart version"),
        (["-c", "scratch-storage-class", "--version", "scratch-storage-class=1.0.0"], "no version"),
        (["-c", "stackable", "--controller-tmp-size", "16Gi"], "spark-operator only"),
    ],
)
def test_usage_errors_exit_two_without_cluster_calls(args, needle):
    with recording(NS) as rec:
        r = _invoke("install", *args, "-y")
        assert r.exit_code == 2, r.output
        rec.assert_no_calls()
    assert needle in " ".join(r.output.split())


def test_old_verbs_alias(tmp_path):
    """CLI-7: one line on stderr naming the new command, then the same run."""
    cfg = _config(tmp_path)
    with recording(NS) as rec:
        _seed_all(rec)
        rs = _invoke("install-scratch-storage-class", str(cfg))
        ro = _invoke("install-spark-operator", str(cfg))
        assert rec.mutations() == []
    assert rs.exit_code == 0 and ro.exit_code == 0, rs.output + ro.output
    assert "is now 'lakebench admin install --component scratch-storage-class'" in rs.stderr
    assert "is now 'lakebench admin install --component spark-operator'" in ro.stderr


def test_dry_run_plans_without_a_lease_or_mutation():
    with recording(NS) as rec:
        r = _invoke("install", "--component", "spark-operator", "--dry-run")
        assert r.exit_code == 0, r.output
        assert rec.mutations() == []
        rec.assert_recorded(api="helm", verb="list")
    assert "would install spark-operator 2.5.1" in " ".join(r.output.split())


def test_without_yes_a_declined_prompt_changes_nothing():
    with recording(NS) as rec:
        r = runner.invoke(admin_app, ["install", "--component", "spark-operator"], input="n\n")
        assert r.exit_code == 1, r.output
        assert rec.mutations() == []
    assert "deploys and destroys on this cluster wait" in " ".join(r.output.split())


# ---------------------------------------------------------------------------
# run_install: inside the lease
# ---------------------------------------------------------------------------


class _Fake:
    versioned = True

    def __init__(self, name, statuses, *, ok=True, after=None):
        self.name = name
        self._statuses = list(statuses)
        self.installs: list = []
        self.ok = ok
        self.after = after

    def config_version(self, s):
        return "1.0.0"

    def status(self, s):
        return self._statuses.pop(0) if len(self._statuses) > 1 else self._statuses[0]

    def install(self, s, version):
        self.installs.append(version)
        return sc.InstallResult(self.ok, "done", after_lease=self.after)


ABSENT = sc.ComponentStatus(installed=False)
READY = sc.ComponentStatus(installed=True, version="1.0.0", ready=True)


def _run(fakes, names, order=None, **kw):
    lock = MagicMock()
    order = [] if order is None else order
    lock.return_value.__enter__.side_effect = lambda *a: order.append("lease taken")
    lock.return_value.__exit__.side_effect = lambda *a: order.append("lease released") or False
    with (
        patch.dict(sc.REGISTRY, fakes),
        patch("lakebench.deploy.cluster_lock.cluster_lock", lock),
    ):
        report = sc.run_install(
            sc.Settings.from_config(None), names, emit=lambda lvl, m: order.append(m), **kw
        )
    return report, order


def test_inside_the_lease_an_install_that_appeared_meanwhile_is_skipped():
    fake = _Fake("spark-operator", [ABSENT, READY, READY])
    report, _ = _run({"spark-operator": fake}, ["spark-operator"])
    assert report.code == 0 and fake.installs == []


def test_inside_the_lease_a_new_refusal_stops_everything():
    blocked = sc.ComponentStatus(installed=True, blocked=(3, "a second operator appeared"))
    first = _Fake("spark-operator", [ABSENT, blocked])
    second = _Fake("stackable", [ABSENT, ABSENT])
    report, _ = _run(
        {"spark-operator": first, "stackable": second}, ["spark-operator", "stackable"]
    )
    assert report.code == 3
    assert first.installs == [] and second.installs == []


def test_readiness_waits_run_after_the_lease_is_released():
    order: list[str] = []
    fake = _Fake(
        "observability",
        [ABSENT, ABSENT, READY],
        after=lambda: order.append("wait") or None,
    )
    report, _ = _run({"observability": fake}, ["observability"], order=order)
    assert report.code == 0
    assert order.index("lease taken") < order.index("lease released") < order.index("wait")


def test_prepare_runs_before_the_lease_and_a_failure_changes_nothing():
    order: list[str] = []

    class _Prepared(_Fake):
        def prepare(self, s):
            order.append("prepare")
            return self.problem

    ok = _Prepared("spark-operator", [ABSENT, ABSENT, READY])
    ok.problem = None
    report, _ = _run({"spark-operator": ok}, ["spark-operator"], order=order)
    assert report.code == 0 and order.index("prepare") < order.index("lease taken")

    order.clear()
    bad = _Prepared("spark-operator", [ABSENT, ABSENT])
    bad.problem = "helm repo update failed"
    report, _ = _run({"spark-operator": bad}, ["spark-operator"], order=order)
    assert report.code == 1 and bad.installs == [] and "lease taken" not in order


def test_a_failed_install_stops_the_rest_and_exits_one():
    first = _Fake("spark-operator", [ABSENT, ABSENT, ABSENT], ok=False)
    second = _Fake("stackable", [ABSENT, ABSENT, ABSENT])
    report, _ = _run(
        {"spark-operator": first, "stackable": second}, ["spark-operator", "stackable"]
    )
    assert report.code == 1 and first.installs == ["1.0.0"] and second.installs == []


def test_refusals_report_the_highest_code_and_take_no_lease():
    a = _Fake("spark-operator", [sc.ComponentStatus(installed=None, detail="x")])
    b = _Fake("stackable", [sc.ComponentStatus(installed=True, blocked=(3, "no"))])
    report, order = _run({"spark-operator": a, "stackable": b}, ["spark-operator", "stackable"])
    assert report.code == 3 and not report.lease_taken
    assert "lease taken" not in order


# ---------------------------------------------------------------------------
# Deploy only verifies (the fixture sees no shared mutation from deploy)
# ---------------------------------------------------------------------------


def _engine(cfg):
    from lakebench.k8s.client import K8sClient

    engine = MagicMock()
    engine.config = cfg
    engine.dry_run = False
    engine.k8s = K8sClient(namespace=cfg.get_namespace())
    engine.renderer = MagicMock()
    engine.renderer.render.return_value = ""
    engine.context = {"namespace": cfg.get_namespace()}
    return engine


def test_deploy_never_installs_under_fixture():
    """Nothing installed: each deploy step fails naming its admin component
    and makes no shared mutation. Reverted (v1.6 with install: true), the
    steps helm-installed the operator, Stackable and the observability stack."""
    from lakebench.deploy.engine import DeploymentEngine, DeploymentStatus
    from lakebench.deploy.hive import HiveDeployer
    from lakebench.deploy.observability import ObservabilityDeployer

    cfg = make_config(name=NS, observability={"enabled": True})
    with recording(NS) as rec:
        rec.for_config(cfg)
        engine = _engine(cfg)
        results = {
            "spark-operator": DeploymentEngine._deploy_spark_operator(engine),
            "hive": HiveDeployer(engine).deploy(),
            "observability": ObservabilityDeployer(engine).deploy(),
        }
        assert rec.shared_mutations() == [], [c.describe() for c in rec.shared_mutations()]
        assert not [c for c in rec.calls if c.api == "helm" and c.mutating]
        rec.assert_clean()
    for name, res in results.items():
        assert res.status == DeploymentStatus.FAILED, (name, res.message)
    assert "admin install --component spark-operator" in results["spark-operator"].message
    assert "admin install --component stackable" in results["hive"].message
    assert "admin install --component observability" in results["observability"].message


def test_deploy_with_everything_installed_makes_only_the_leased_watch_list_add():
    from lakebench.deploy.engine import DeploymentEngine, DeploymentStatus
    from lakebench.deploy.observability import ObservabilityDeployer

    cfg = make_config(name=NS, observability={"enabled": True})
    with recording(NS) as rec:
        rec.for_config(cfg)
        rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS})
        _seed_all(rec)
        engine = _engine(cfg)
        with (
            patch.object(DeploymentEngine, "_operator_rbac_exists", return_value=True),
            patch("lakebench.deploy.observability._wait_for_prometheus", return_value=None),
        ):
            op = DeploymentEngine._deploy_spark_operator(engine)
            obs = ObservabilityDeployer(engine).deploy()
        assert op.status == DeploymentStatus.SUCCESS, op.message
        assert obs.status == DeploymentStatus.SUCCESS, obs.message
        shared = rec.shared_mutations()
        assert shared, "the watch-list add must reach the fake"
        assert all(c.lease_held for c in shared)
        assert {(c.api, c.verb) for c in shared if c.api == "helm"} == {("helm", "upgrade")}
        assert not [c for c in shared if c.namespace == OBS_NS]
        rec.assert_clean()
    upgrade = next(c.argv for c in rec.calls if c.api == "helm" and c.verb == "upgrade")
    assert upgrade[upgrade.index("--version") + 1] == "2.5.1"


# ---------------------------------------------------------------------------
# Config: install: true is refused
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "key,component",
    [
        ("platform.compute.spark.operator.install", "spark-operator"),
        ("architecture.catalog.hive.operator.install", "stackable"),
    ],
)
def test_deploy_refuses_install_true(tmp_path, key, component):
    from lakebench.config import ConfigValidationError, LoadPurpose, load_config

    data: dict = {"name": "t"}
    node = data
    parts = key.split(".")
    for p in parts[:-1]:
        node = node.setdefault(p, {})
    node[parts[-1]] = True
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(data))
    for purpose in (LoadPurpose.MUTATE, LoadPurpose.RUN):
        with pytest.raises(ConfigValidationError) as exc:
            load_config(path, purpose=purpose, print_notes=False)
        assert f"lakebench admin install --component {component}" in str(exc.value)
    # Teardown and read commands still load it (a v1.6 deployment stays
    # destroyable), as false.
    for purpose in (LoadPurpose.TEARDOWN, LoadPurpose.READ):
        cfg = load_config(path, purpose=purpose, print_notes=False)
        node2 = cfg
        for p in parts:
            node2 = getattr(node2, p)
        assert node2 is False
    # false and absent load as before.
    node[parts[-1]] = False
    path.write_text(yaml.safe_dump(data))
    load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)


def test_operator_install_keys_registry_names_both_keys():
    from lakebench.config.schema import OPERATOR_INSTALL_KEYS, operator_install_fix

    assert set(OPERATOR_INSTALL_KEYS) == {
        "platform.compute.spark.operator.install",
        "architecture.catalog.hive.operator.install",
    }
    for key in OPERATOR_INSTALL_KEYS:
        assert "admin install --component" in operator_install_fix(key)


# ---------------------------------------------------------------------------
# admin doctor runs the prerequisite registry
# ---------------------------------------------------------------------------


def test_doctor_runs_the_registry_and_exits_one_on_a_failure():
    from lakebench.deploy import prereqs

    def outcomes(cfg, reader):
        out = []
        for p in prereqs.PREREQS:
            status = (
                prereqs.PrereqStatus.FAIL if p.id == "spark-operator" else prereqs.PrereqStatus.OK
            )
            out.append(prereqs.PrereqOutcome(p, prereqs.PrereqResult(status, "x")))
        return out

    with (
        patch("lakebench.cli._admin._get_core_v1", return_value=MagicMock()),
        patch("lakebench.deploy.prereqs.run_prereqs", side_effect=outcomes) as run,
        patch("lakebench.deploy.cluster_lock.read_cluster_lock", return_value=None),
        patch("lakebench.cli._admin._print_operator_scratch", return_value=True),
    ):
        r = _invoke("doctor")
    assert r.exit_code == 1, r.output
    checked = run.call_args.args[0]
    assert checked.platform.storage.scratch.enabled and checked.observability.enabled
    out = " ".join(r.output.split())
    assert "lakebench admin install --component spark-operator" in out
    assert "S3 endpoint" not in out  # the deployment's own checks are not doctor's


def test_admin_install_report_lists_components_as_json_safe_rows():
    """The status table renders every requested component (vacuity guard for
    the no-op test: a resolved set that dropped one would show here)."""
    report = sc.Report(0, [], {"spark-operator": READY, "stackable": ABSENT}, False)
    from lakebench.cli._admin import _print_component_table

    with patch("lakebench.cli._admin.console") as console:
        _print_component_table(report)
    table = console.print.call_args.args[0]
    assert [c._cells for c in table.columns][0] == ["spark-operator", "stackable"]
    json.dumps([str(x) for x in table.columns[0]._cells])


def test_install_true_is_refused_after_coercion(tmp_path):
    """``"true"``, ``yes`` and ``1`` are true too: the refusal runs after
    Pydantic coercion, so a quoted value cannot slip past it."""
    from lakebench.config import ConfigValidationError, LoadPurpose, load_config

    for raw in ("true", "yes", 1):
        path = tmp_path / "c.yaml"
        path.write_text(
            yaml.safe_dump(
                {"name": "t", "platform": {"compute": {"spark": {"operator": {"install": raw}}}}}
            )
        )
        with pytest.raises(ConfigValidationError):
            load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)


def test_install_false_loads_quietly(tmp_path, recwarn):
    """Every v1.6 example and saved config carries install: false."""
    from lakebench.config import LoadPurpose, load_config

    path = tmp_path / "c.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": "t",
                "platform": {"compute": {"spark": {"operator": {"install": False}}}},
                "architecture": {"catalog": {"hive": {"operator": {"install": False}}}},
            }
        )
    )
    load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)
    assert not [w for w in recwarn if "admin install" in str(w.message)]


def test_nothing_outside_the_schema_reads_operator_install():
    """Static guard for 'deploy only verifies': no code path decides anything
    on operator.install (v1.6 deploy, validate, run and the preflight did)."""
    import re as _re

    src = Path(__file__).resolve().parents[1] / "src" / "lakebench"
    hits = []
    for path in src.rglob("*.py"):
        if path.name == "schema.py":
            continue
        for i, line in enumerate(path.read_text().splitlines(), 1):
            if _re.search(r"operator\.install\b|op_cfg\.install\b|\"install\",\s*False", line):
                if line.lstrip().startswith("#") or "``" in line or "'" in line or '"' in line:
                    continue
                hits.append(f"{path.relative_to(src)}:{i}: {line.strip()}")
    assert hits == []


def test_doctor_fails_when_a_check_cannot_run():
    from lakebench.deploy import prereqs

    def outcomes(cfg, reader):
        return [
            prereqs.PrereqOutcome(
                p,
                prereqs.PrereqResult(
                    prereqs.PrereqStatus.UNKNOWN
                    if p.id == "spark-operator"
                    else prereqs.PrereqStatus.OK,
                    "could not check: 403",
                ),
            )
            for p in prereqs.PREREQS
        ]

    with (
        patch("lakebench.cli._admin._get_core_v1", return_value=MagicMock()),
        patch("lakebench.deploy.prereqs.run_prereqs", side_effect=outcomes),
        patch("lakebench.deploy.cluster_lock.read_cluster_lock", return_value=None),
    ):
        r = _invoke("doctor")
    assert r.exit_code == 1, r.output


def test_doctor_without_a_config_does_not_gate_on_optional_components():
    from lakebench.deploy import prereqs

    def outcomes(cfg, reader):
        bad = {"stackable", "observability-stack"}
        return [
            prereqs.PrereqOutcome(
                p,
                prereqs.PrereqResult(
                    prereqs.PrereqStatus.FAIL if p.id in bad else prereqs.PrereqStatus.OK, "x"
                ),
            )
            for p in prereqs.PREREQS
        ]

    with (
        patch("lakebench.cli._admin._get_core_v1", return_value=MagicMock()),
        patch("lakebench.deploy.prereqs.run_prereqs", side_effect=outcomes),
        patch("lakebench.deploy.cluster_lock.read_cluster_lock", return_value=None),
        patch("lakebench.cli._admin._print_operator_scratch", return_value=True),
    ):
        r = _invoke("doctor")
    assert r.exit_code == 0, r.output
    assert "pass a config that uses it" in " ".join(r.output.split())


def test_deploy_step_says_could_not_check_rather_than_not_installed():
    """check_status maps a read error to installed=None (unknown); the step
    must not then tell the user to install an operator that may be running."""
    from lakebench.deploy.engine import DeploymentEngine, DeploymentStatus
    from lakebench.modules.pipeline_engines.spark.operator import (
        OperatorStatus,
        SparkOperatorManager,
    )

    engine = MagicMock()
    engine.config = make_config(name=NS)
    engine.dry_run = False
    broken = OperatorStatus(
        installed=None,
        version=None,
        namespace=None,
        ready=False,
        message="Error checking operator status: timeout",
    )
    with patch.object(SparkOperatorManager, "ensure_namespace_watched", return_value=broken):
        res = DeploymentEngine._deploy_spark_operator(engine)
    assert res.status == DeploymentStatus.FAILED
    assert "not ready: Error checking" in res.message
    assert "admin install" not in res.message


def test_spark_prepare_refuses_a_repo_name_bound_to_another_url():
    """helm returns 0 for the same repo already added; non-zero "already
    exists" means the name points at another URL, which must not supply the
    shared operator's chart."""
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    taken = MagicMock(
        returncode=1,
        stdout="",
        stderr="Error: repository name (spark-operator) already exists, please specify a "
        "different name",
    )
    with patch.object(SparkOperatorManager, "_run", return_value=taken):
        problem = sc.SparkOperator().prepare(sc.Settings.from_config(None))
    assert problem and "already exists" in problem


@pytest.mark.parametrize(
    "recipe", ["hive-iceberg-spark-trino+observability", "polaris-iceberg-spark-duckdb"]
)
def test_full_deploy_makes_no_shared_mutation_but_the_leased_watch_list_add(
    recipe, monkeypatch, tmp_path
):
    """DEP-3 acceptance on the whole deploy: under the recording fixture,
    with every shared component installed, deploy_all's only shared
    mutations are the leased watch-list add (helm upgrade of spark-operator
    and the operator restart), and it installs nothing."""
    from lakebench.deploy.engine import DeploymentEngine
    from lakebench.k8s.client import K8sClient
    from tests.test_category1_teardown import _config as cat1_config
    from tests.test_category1_teardown import _instant_waits, _seed_cluster

    _instant_waits(monkeypatch)
    cfg = cat1_config(recipe, "c360-batch", tmp_path)
    with recording() as rec:
        rec.for_config(cfg)
        _seed_cluster(rec)
        results = DeploymentEngine(
            cfg, k8s_client=K8sClient(namespace=cfg.get_namespace())
        ).deploy_all()
        assert [(r.component, r.message) for r in results if r.status.value == "failed"] == []
        shared = rec.shared_mutations()
        assert shared, "the watch-list add must reach the fake"
        assert all(c.lease_held for c in shared), [c.describe() for c in shared]
        helm = {(c.verb, c.name) for c in rec.calls if c.api == "helm" and c.mutating}
        assert helm == {("upgrade", "spark-operator")}, helm
        assert not [c for c in shared if c.kind in ("storageclasses", "customresourcedefinitions")]
        # Nothing in the shared observability namespace (v1.6 deploy applied
        # the dashboard there).
        assert not [c for c in shared if c.namespace == OBS_NS], [c.describe() for c in shared]
        rec.assert_clean()
