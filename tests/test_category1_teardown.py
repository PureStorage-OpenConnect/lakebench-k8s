"""The Category-1 teardown registry covers what deploy and run create (SD-21).

With ``create_namespace: false`` the namespace survives destroy, so nothing
removes an object destroy does not delete by name or selector. This test runs
the real deploy, then the objects ``run`` creates (scripts maps, datagen Jobs,
every SparkApplication of the workload), under the recording fake, for every
query engine and for C360 batch, AML batch and C360 continuous; then the real
destroy with ``create_namespace: false``. Every namespaced object created in
the config's namespace must be gone afterwards, or be listed in
``KEPT_ON_DESTROY`` with its reason, and every ``CATEGORY1_ANNOTATIONS`` key
must be off the namespace.

Objects Spark creates at run time (driver Services, ``spark-drv-*``
ConfigMaps, OnDemand scratch PVCs) carry ownerReferences to the driver pod
and are garbage collected after the SparkApplication delete; the fake has no
operator, so it never creates them, and ``test_sparkapplications_go_before_category1``
checks the order that relies on.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.fixtures.recording_k8s import K8sRecorder, recording

NS = "u01"
SRC = Path(__file__).resolve().parent.parent / "src" / "lakebench"

RECIPES = (
    "hive-iceberg-spark-trino",
    "polaris-iceberg-spark-duckdb",
    "hive-delta-spark-thrift",
    "polaris-iceberg-spark-none",
    "hive-iceberg-spark-trino+observability",
)
WORKLOADS = ("c360-batch", "aml-batch", "c360-continuous")
_CREATE_VERBS = frozenset({"create", "replace", "patch", "apply"})


def _instant_waits(mp: pytest.MonkeyPatch) -> None:
    """Every readiness wait answers ready: the fake runs no pods."""
    import lakebench.deploy.engine  # noqa: F401 -- load every deployer module
    import lakebench.k8s.wait as w
    from lakebench.k8s.wait import WaitResult, WaitStatus
    from lakebench.modules.catalogs.hive.deployer import HiveDeployer
    from lakebench.modules.catalogs.polaris.deployer import PolarisDeployer
    from lakebench.modules.query_engines.duckdb.deployer import DuckDBDeployer
    from lakebench.modules.query_engines.spark_thrift.deployer import SparkThriftDeployer

    ok = WaitResult(WaitStatus.READY, "ready", 0.0, 1)
    names = [n for n in dir(w) if n.startswith("wait_for_")]
    for modname, mod in list(sys.modules.items()):
        if mod is None or not modname.startswith("lakebench"):
            continue
        for n in names:
            if callable(getattr(mod, n, None)):
                mp.setattr(mod, n, lambda *a, **k: ok)
    mp.setattr(HiveDeployer, "_wait_for_hivecluster", lambda *a, **k: ok)
    mp.setattr(PolarisDeployer, "_wait_for_bootstrap_job", lambda *a, **k: True)
    mp.setattr(SparkThriftDeployer, "_wait_for_ready", lambda *a, **k: None)
    mp.setattr(DuckDBDeployer, "_wait_for_ready", lambda *a, **k: None)
    mp.setattr("lakebench.deploy.observability._wait_for_prometheus", lambda *a, **k: "")


def _config(recipe: str, workload: str):
    from tests.conftest import make_config

    recipe, _, extra = recipe.partition("+")
    if workload.startswith("aml"):
        # AML runs on Iceberg only; the Delta recipe's engine is covered on Iceberg.
        recipe = recipe.replace("-delta-", "-iceberg-")
    overrides: dict = {"name": NS, "recipe": recipe}
    if extra == "observability":
        overrides["observability"] = {"enabled": True}
    if workload.startswith("aml"):
        overrides["workload"] = {"schema": "financial"}
    cfg = make_config(**overrides)
    if workload.endswith("continuous"):
        from lakebench.config.schema import PipelineMode

        cfg.architecture.pipeline.mode = PipelineMode.CONTINUOUS
    cfg.platform.kubernetes.create_namespace = True
    return cfg


def _seed_cluster(rec: K8sRecorder) -> None:
    rec.add_spark_operator(watched=["default"])
    rec.add_stackable()
    rec.add_crd("podmonitors", "monitoring.coreos.com", "PodMonitor")
    rec.add_namespace("stackable")
    for op in ("hive-operator", "secret-operator"):
        rec.add(
            "pods",
            {
                "metadata": {"name": f"{op}-0", "labels": {"app.kubernetes.io/name": op}},
                "spec": {"containers": [{"name": op}]},
                "status": {"phase": "Running"},
            },
            namespace="stackable",
        )
    # Deploy looks for the Trino coordinator pod; the fake starts none.
    rec.add(
        "pods",
        {
            "metadata": {
                "name": "lakebench-trino-coordinator-0",
                "labels": {"app": "lakebench-trino", "component": "coordinator"},
            },
            "spec": {"containers": [{"name": "trino"}]},
            "status": {"phase": "Running"},
        },
        namespace=NS,
    )


def _job_types(workload: str) -> list:
    from lakebench.modules.pipeline_engines.spark.job import JobType

    if workload.endswith("continuous"):
        return [JobType.BRONZE_INGEST, JobType.SILVER_STREAM, JobType.GOLD_REFRESH]
    jobs = [JobType.BRONZE_VERIFY, JobType.SILVER_BUILD, JobType.GOLD_FINALIZE]
    if workload.startswith("aml"):
        jobs += [
            JobType.SCORE_FINANCIAL,
            JobType.SCORE_FINANCIAL_REFERENCE,
            JobType.REPLAY_FINANCIAL,
            JobType.REPRODUCE_FINANCIAL,
        ]
    return jobs


def _run_creators(cfg, k8s) -> None:
    """The namespaced objects ``run`` creates, through the code that creates them."""
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine
    from lakebench.modules.pipeline_engines.spark.job import SparkJobManager

    engine = DeploymentEngine(cfg, k8s_client=k8s)
    DatagenDeployer(engine).deploy()
    mgr = SparkJobManager(cfg, k8s)
    assert mgr.deploy_scripts_configmap()
    for jt in _job_types(cfg_workload(cfg)):
        status = mgr.submit_job(jt)
        assert status.state.value != "failed", status.message


def cfg_workload(cfg) -> str:
    schema = cfg.architecture.workload.schema_type.value
    mode = getattr(cfg.architecture.pipeline.mode, "value", cfg.architecture.pipeline.mode)
    if schema == "financial":
        return "aml-batch"
    return "c360-continuous" if mode in ("continuous", "sustained") else "c360-batch"


def _created_objects(rec: K8sRecorder) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for c in rec.calls:
        if not c.mutating or c.verb not in _CREATE_VERBS:
            continue
        if c.namespace != NS or not c.name or c.kind == "namespaces":
            continue
        out.add((c.kind, c.name))
    return out


def _destroy(rec: K8sRecorder, cfg) -> list:
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict
    from lakebench.k8s.client import K8sClient

    cfg.platform.kubernetes.create_namespace = False
    engine = MagicMock()
    engine.config = cfg
    engine.k8s = K8sClient(namespace=NS)
    match = IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name=NS,
        expected_deployment=NS,
        found_deployment=NS,
    )
    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=match),
        patch("lakebench.deploy.ownership.verify_bucket_ownership", return_value=match),
    ):
        return destroy_all(engine, clean_buckets=False)


@pytest.mark.parametrize("workload", WORKLOADS)
@pytest.mark.parametrize("recipe", RECIPES)
def test_teardown_covers_every_created_object(recipe, workload, monkeypatch):
    from lakebench.deploy.category1 import CATEGORY1_ANNOTATIONS, KEPT_ON_DESTROY
    from lakebench.deploy.engine import DeploymentEngine
    from lakebench.k8s.client import K8sClient

    _instant_waits(monkeypatch)
    cfg = _config(recipe, workload)
    with recording() as rec:
        rec.for_config(cfg)
        _seed_cluster(rec)
        k8s = K8sClient(namespace=NS)
        results = DeploymentEngine(cfg, k8s_client=k8s).deploy_all()
        failed = [(r.component, r.message) for r in results if r.status.value == "failed"]
        assert failed == []
        _run_creators(cfg, k8s)
        created = _created_objects(rec)
        # The check below reads the store, so every create must have landed.
        absent = sorted(o for o in created if (o[0], NS, o[1]) not in rec.store)
        assert absent == [], f"creates the fake did not apply: {absent}"
        # The deploy really ran: a sample of what every recipe creates.
        assert ("statefulsets", "lakebench-postgres") in created
        assert ("sparkapplications", f"lakebench-{_job_types(workload)[0].value}") in created

        # CC-2 writes this annotation beside the deploy nonce; seed it so the
        # removal is exercised before CC-2 lands.
        ns = rec.store[("namespaces", None, NS)]
        ns.metadata.annotations = {
            **(ns.metadata.annotations or {}),
            "lakebench.deployment/state-schema": "lb-state/1",
        }
        results = _destroy(rec, cfg)
        kept = {(k.kind, k.name) for k in KEPT_ON_DESTROY}
        left = sorted(
            (kind, name)
            for kind, name in created
            if (kind, NS, name) in rec.store and (kind, name) not in kept
        )
        assert left == [], f"{recipe} {workload}: destroy left {left}"
        anns = rec.store[("namespaces", None, NS)].metadata.annotations or {}
        assert not [a for a in CATEGORY1_ANNOTATIONS if a in anns], anns
        # The identity annotations stay: a re-run of a half-done destroy needs them.
        assert anns.get("lakebench.deployment/name") == NS
        assert anns.get("lakebench.deployment/deploy-nonce")
        statuses = {r.component: r.status.value for r in results}
        assert statuses.get("category1") == "success", statuses
        # Deploy, the run-time creators and destroy stayed in their lane (SAF-4).
        rec.assert_clean()


def test_registry_entries_have_owner():
    from lakebench.deploy.category1 import CATEGORY1_OBJECTS, KEPT_ON_DESTROY

    for e in CATEGORY1_OBJECTS:
        assert e.owner_wi and e.step, e
        assert bool(e.name) != bool(e.label_selector), e
    for k in KEPT_ON_DESTROY:
        assert k.owner_wi and k.reason, k


def test_category1_selectors_are_deployment_scoped():
    """A selector delete in the category1 step can only match this deployment."""
    from lakebench.deploy.category1 import CATEGORY1_OBJECTS

    for e in CATEGORY1_OBJECTS:
        if e.step == "category1" and e.label_selector:
            assert "app.kubernetes.io/instance={name}" in e.label_selector, e


def _step_order(tree: ast.Module) -> list[str]:
    """The ``report("<component>", DeploymentStatus.IN_PROGRESS, ...)`` order in destroy_all."""
    fn = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "destroy_all"
    )
    order: list[str] = []
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "report"
            and len(node.args) >= 2
            and isinstance(node.args[0], ast.Constant)
            and ast.unparse(node.args[1]) == "DeploymentStatus.IN_PROGRESS"
        ):
            order.append((node.lineno, node.args[0].value))
    return [name for _, name in sorted(order)]


def test_sparkapplications_go_before_category1():
    """Spark's runtime objects are garbage collected after the SparkApplication delete."""
    order = _step_order(ast.parse((SRC / "deploy" / "destroy.py").read_text()))
    assert "spark-jobs" in order and "category1" in order
    assert order.index("spark-jobs") < order.index("category1")


@pytest.mark.parametrize(("ns_goes", "status"), [(False, "failed"), (True, "skipped")])
def test_category1_failure_is_reported(ns_goes, status):
    """A failed delete is FAILED when the namespace survives, a skip when it goes."""
    from lakebench.deploy.destroy import _category1_step

    with recording(NS) as rec:
        rec.add_namespace(NS, annotations={"lakebench.deployment/state-schema": "lb-state/1"})
        rec.add("serviceaccounts", {"metadata": {"name": "lakebench-postgres"}}, namespace=NS)
        rec.fail(verb="delete", kind="serviceaccounts", name="lakebench-postgres", status=500)
        result = _category1_step(NS, NS, ns_goes=ns_goes)
        assert result.status.value == status
        assert "serviceaccounts/lakebench-postgres" in result.message
        # The other entries and the annotation patch still ran.
        rec.assert_recorded(
            verb="delete", kind="persistentvolumeclaims", name="lakebench-pushgateway"
        )
        anns = rec.store[("namespaces", None, NS)].metadata.annotations or {}
        assert "lakebench.deployment/state-schema" not in anns
