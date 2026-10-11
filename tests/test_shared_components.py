"""DEP-3 (SD-10): shared components install through ``admin install
--component`` under the lease, and deploy only verifies them.

Cluster calls run against the recording fixture (tests/fixtures/recording_k8s.py,
SD-9), so a shared mutation without the lease, or any mutation where the
test expects none, fails the test.
"""

from __future__ import annotations

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


@pytest.mark.parametrize(
    "argv",
    [["install-spark-operator"], ["install", "--component", "spark-operator", "-y"]],
    ids=["alias", "component"],
)
def test_config_pin_does_not_move_installed_version(tmp_path, argv):
    """A config pinning 2.4.0 against an installed 2.5.1 issues no helm
    write."""
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
        r = _invoke(argv[0], str(cfg), *argv[1:])
        assert r.exit_code == 0, r.output
        assert rec.mutations() == []
        assert rec.releases[("spark-operator", "spark-operator")].version == "2.5.1"
        rec.assert_clean()


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
        assert {c.name for c in writes} == set(STACKABLE_OPS) and len(writes) == 4
        assert all(c.lease_held and c.verb == "install" for c in writes)
        rec.assert_clean()


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


# ---------------------------------------------------------------------------
# Usage errors (nothing reaches the cluster)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "args",
    [
        ["--component", "all"],
        ["--component", "nope"],
        [],
        ["-c", "spark-operator", "--version", "stackable=25.7.0"],
        ["-c", "spark-operator", "--version", "spark-operator=^2.0.0"],
        ["-c", "scratch-storage-class", "--version", "scratch-storage-class=1.0.0"],
        ["-c", "stackable", "--controller-tmp-size", "16Gi"],
    ],
)
def test_usage_errors_exit_two_without_cluster_calls(args):
    with recording(NS) as rec:
        r = _invoke("install", *args, "-y")
        assert r.exit_code == 2, r.output
        rec.assert_no_calls()


def test_dry_run_plans_without_a_lease_or_mutation():
    with recording(NS) as rec:
        r = _invoke("install", "--component", "spark-operator", "--dry-run")
        assert r.exit_code == 0, r.output
        assert rec.mutations() == []
        rec.assert_recorded(api="helm", verb="list")


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
    """Nothing installed: each deploy step fails and makes no shared mutation."""
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
    "key,component,raw",
    [
        ("platform.compute.spark.operator.install", "spark-operator", True),
        ("architecture.catalog.hive.operator.install", "stackable", True),
        # The refusal runs after Pydantic coercion, so a quoted or numeric
        # true cannot slip past it.
        ("platform.compute.spark.operator.install", "spark-operator", "true"),
        ("platform.compute.spark.operator.install", "spark-operator", "yes"),
        ("platform.compute.spark.operator.install", "spark-operator", 1),
    ],
)
def test_deploy_refuses_install_true(tmp_path, key, component, raw):
    from lakebench.config import ConfigValidationError, LoadPurpose, load_config

    data: dict = {"name": "t"}
    node = data
    parts = key.split(".")
    for p in parts[:-1]:
        node = node.setdefault(p, {})
    node[parts[-1]] = raw
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


# ---------------------------------------------------------------------------
# admin doctor runs the prerequisite registry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad,code",
    [
        ({"spark-operator": "FAIL"}, 1),
        ({"spark-operator": "UNKNOWN"}, 1),
        # Without a config, optional components do not gate.
        ({"stackable": "FAIL", "observability-stack": "FAIL"}, 0),
        ({}, 0),
    ],
    ids=["failed-check", "check-cannot-run", "optional-not-gated", "all-ok"],
)
def test_doctor_exit_code_follows_the_registry(bad, code):
    from lakebench.deploy import prereqs

    def outcomes(cfg, reader):
        return [
            prereqs.PrereqOutcome(
                p,
                prereqs.PrereqResult(
                    prereqs.PrereqStatus[bad[p.id]] if p.id in bad else prereqs.PrereqStatus.OK,
                    "x",
                ),
            )
            for p in prereqs.PREREQS
        ]

    with (
        patch("lakebench.cli._admin._get_core_v1", return_value=MagicMock()),
        patch("lakebench.deploy.prereqs.run_prereqs", side_effect=outcomes) as run,
        patch("lakebench.deploy.cluster_lock.read_cluster_lock", return_value=None),
        patch("lakebench.cli._admin._print_operator_scratch", return_value=True),
    ):
        r = _invoke("doctor")
    assert r.exit_code == code, r.output
    checked = run.call_args.args[0]
    assert checked.platform.storage.scratch.enabled and checked.observability.enabled


def test_spark_prepare_refuses_a_repo_name_bound_to_another_url():
    """A repo name bound to another URL (helm exits non-zero) must not supply
    the shared operator's chart."""
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    taken = MagicMock(
        returncode=1,
        stdout="",
        stderr="Error: repository name (spark-operator) already exists, please specify a "
        "different name",
    )
    with patch.object(SparkOperatorManager, "_run", return_value=taken):
        problem = sc.SparkOperator().prepare(sc.Settings.from_config(None))
    assert problem


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


# --- admin install refusals: each state answers with its exit code and
# changes nothing on the shared cluster.


def _seed_prom_elsewhere(rec):
    rec.add_namespace("monitoring")
    rec.add_helm_release("prom", "monitoring", chart="kube-prometheus-stack", version="80.0.0")


def _seed_operator(rec):
    rec.add_spark_operator(watched=["default"])


def _seed_operator_not_ready(rec):
    rec.add_spark_operator(watched=["default"])
    dep = rec.store[("deployments", "spark-operator", "spark-operator-controller")]
    dep.status.ready_replicas = 0


def _seed_leftover_spark_crd(rec):
    rec.add_crd("sparkapplications", "sparkoperator.k8s.io", "SparkApplication")


def _seed_observability_not_ready(rec):
    _seed_observability(rec, ready=False)


def _seed_partial_stackable(rec):
    rec.add_namespace("stackable")
    for op in STACKABLE_OPS[:3]:
        rec.add_helm_release(op, "stackable", chart=op, version="25.7.0")
        _running_pod(rec, op)
    rec.add_crd("hiveclusters", "hive.stackable.tech", "HiveCluster")  # hive-operator's


def _seed_release_status(status):
    def seed(rec):
        rec.add_spark_operator(watched=["default"])
        rec.releases[("spark-operator", "spark-operator")].status = status

    return seed


def _seed_scratch_diff(rec):
    _seed_all(rec)
    rec.store.pop(("storageclasses", None, "px-csi-scratch"))
    _seed_scratch(rec, {"repl": "3"})


def _seed_operator_elsewhere(rec):
    rec.add_spark_operator(watched=["default"], namespace="other-ops")


def _seed_stackable_elsewhere(rec):
    rec.add_namespace("sdp")
    rec.add_crd("hiveclusters", "hive.stackable.tech", "HiveCluster")
    rec.add_crd("secretclasses", "secrets.stackable.tech", "SecretClass", scope="Cluster")
    for op in STACKABLE_OPS:
        rec.add_helm_release(op, "sdp", chart=op, version="25.7.0")
        _running_pod(rec, op, ns="sdp")


def _seed_stackable_crd(rec):
    rec.add_crd("hiveclusters", "hive.stackable.tech", "HiveCluster")


def _seed_helm_unauthorized(rec):
    rec.on_command("helm", "list", returncode=1, stderr="Error: Unauthorized")


def _seed_watched(*watched):
    def seed(rec):
        _seed_all(rec, watched=watched)
        for ns in watched:
            if ns not in ("default", "gone"):
                rec.add_namespace(ns, annotations={"lakebench.deployment/name": ns})

    return seed


_OP_DOWNGRADE = ("--component", "spark-operator", "--version", "spark-operator=2.4.0")
_INSTALL_REFUSALS = [
    pytest.param(_seed_prom_elsewhere, ("<cfg>", "--component", "observability"), 3, None, id="second-prometheus-stack"),
    pytest.param(_seed_operator, ("--component", "spark-operator", "--controller-tmp-size", "16Gi"), 2, None, id="tmp-size-on-installed-operator"),
    pytest.param(_seed_operator_not_ready, ("--component", "spark-operator"), 1, None, id="operator-not-ready"),
    pytest.param(_seed_leftover_spark_crd, ("--component", "spark-operator"), 3, None, id="leftover-spark-crd"),
    pytest.param(_seed_observability_not_ready, ("<cfg>", "--component", "observability"), 1, None, id="observability-not-ready"),
    pytest.param(_seed_partial_stackable, ("--component", "stackable"), 3, None, id="partial-stackable-leftover-crd"),
    *[
        pytest.param(_seed_release_status(s), ("--component", "spark-operator"), 1, None, id=f"release-{s}")
        for s in ("pending-install", "pending-upgrade", "failed", "uninstalling")
    ],
    pytest.param(_seed_scratch_diff, ("<cfg>", "--component", "scratch-storage-class"), 3, None, id="scratch-diff-named"),
    pytest.param(_seed_scratch_diff, ("<cfg>", "--component", "all"), 3, None, id="scratch-diff-all"),
    pytest.param(_seed_operator_elsewhere, ("--component", "spark-operator"), 3, None, id="second-spark-operator"),
    pytest.param(_seed_all, ("--component", "stackable", "--version", "stackable=24.11.0", "--allow-version-change"), 3, None, id="stackable-version-change"),
    pytest.param(_seed_all, ("--component", "observability", "--version", "observability=80.0.0", "--allow-version-change"), 3, None, id="observability-version-change"),
    pytest.param(_seed_stackable_elsewhere, ("--component", "stackable"), 0, None, id="stackable-in-another-namespace"),
    pytest.param(_seed_stackable_crd, ("--component", "stackable"), 3, None, id="leftover-stackable-crd"),
    pytest.param(_seed_helm_unauthorized, ("--component", "spark-operator"), 1, None, id="release-state-unreadable"),
    pytest.param(_seed_watched("default"), (*_OP_DOWNGRADE, "--allow-version-change"), 3, None, id="version-change-default-placeholder"),
    pytest.param(_seed_watched("default", "gone"), (*_OP_DOWNGRADE, "--allow-version-change"), 3, None, id="version-change-stale-entries"),
    pytest.param(_seed_watched("a", "b"), (*_OP_DOWNGRADE, "--allow-version-change"), 3, None, id="version-change-while-watched"),
    pytest.param(_seed_all, _OP_DOWNGRADE, 2, None, id="version-change-without-flag"),
    pytest.param(_seed_all, (*_OP_DOWNGRADE, "--allow-version-change"), 3, "watch-list-unreadable", id="version-change-users-unknown"),
]  # fmt: skip


@pytest.mark.parametrize(("seed", "argv", "code", "fault"), _INSTALL_REFUSALS)
def test_admin_install_answers_and_changes_nothing(tmp_path, seed, argv, code, fault):
    """Each cluster state answers with its exit code and no mutation at all:
    a second operator, a leftover CRD, a release not deployed, an unreadable
    release state, a version change (with or without users), a StorageClass
    parameter difference."""
    from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

    cfg = _config(tmp_path)
    argv = [str(cfg) if a == "<cfg>" else a for a in argv]
    with recording(NS) as rec:
        seed(rec)
        if fault == "watch-list-unreadable":
            with patch.object(
                SparkOperatorManager, "_get_active_namespaces", side_effect=RuntimeError("timeout")
            ):
                r = _invoke("install", *argv, "-y")
        else:
            r = _invoke("install", *argv, "-y")
        assert r.exit_code == code, r.output
        assert rec.mutations() == []


def test_scratch_parameter_diff_refuses_through_the_alias(tmp_path):
    cfg = _config(tmp_path)
    with recording(NS) as rec:
        _seed_scratch_diff(rec)
        r = _invoke("install-scratch-storage-class", str(cfg))
        assert r.exit_code == 3, r.output
        assert rec.mutations() == []


def test_without_yes_a_declined_prompt_changes_nothing():
    with recording(NS) as rec:
        r = runner.invoke(admin_app, ["install", "--component", "spark-operator"], input="n\n")
        assert r.exit_code == 1, r.output
        assert rec.mutations() == []
