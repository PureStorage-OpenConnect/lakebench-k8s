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
    "hive-iceberg-spark-trino+ca",
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


def _config(recipe: str, workload: str, tmp_path: Path | None = None):
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
    if extra == "ca":
        # An HTTPS endpoint with a custom CA: deploy adds lakebench-ca-certificate.
        assert tmp_path is not None
        pem = tmp_path / "ca.pem"
        pem.write_text("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
        cfg.platform.storage.s3.ca_cert = str(pem)
    cfg.platform.kubernetes.create_namespace = True
    return cfg


def _seed_cluster(rec: K8sRecorder) -> None:
    # Deploy's psql execs into lakebench-postgres-0 (SAF-8): the polaris role
    # probe (none yet, a fresh Polaris) and the role password sync.
    rec.exec_output = "lbrole:0\nALTER ROLE\nCREATE ROLE\nCREATE DATABASE\nGRANT\n"
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


def _labels(obj) -> dict:
    md = obj.get("metadata") if isinstance(obj, dict) else getattr(obj, "metadata", None)
    labels = md.get("labels") if isinstance(md, dict) else getattr(md, "labels", None)
    return dict(labels or {})


def _registered(kind: str, name: str, labels: dict | None) -> bool:
    from lakebench.deploy.category1 import CATEGORY1_OBJECTS, KEPT_ON_DESTROY

    if any(k.kind == kind and k.name == name for k in KEPT_ON_DESTROY):
        return True
    return any(e.matches(kind, name, labels, NS) for e in CATEGORY1_OBJECTS)


@pytest.mark.parametrize("workload", WORKLOADS)
@pytest.mark.parametrize("recipe", RECIPES)
def test_teardown_covers_every_created_object(recipe, workload, monkeypatch, tmp_path):
    from lakebench.deploy.category1 import CATEGORY1_ANNOTATIONS, KEPT_ON_DESTROY
    from lakebench.deploy.engine import DeploymentEngine
    from lakebench.k8s.client import K8sClient

    _instant_waits(monkeypatch)
    cfg = _config(recipe, workload, tmp_path)
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
        # The registry is the authority: every created object has its entry.
        unlisted = sorted(
            (kind, name)
            for kind, name in created
            if not _registered(kind, name, _labels(rec.store[(kind, NS, name)]))
        )
        assert unlisted == [], f"objects with no CATEGORY1_OBJECTS entry: {unlisted}"
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

    steps = set(_step_order(ast.parse((SRC / "deploy" / "destroy.py").read_text())))
    for e in CATEGORY1_OBJECTS:
        assert e.owner_wi and e.step in steps, e
        assert bool(e.name) != bool(e.label_selector), e
        assert e.when in ("", "observability"), e
    for k in KEPT_ON_DESTROY:
        assert k.owner_wi and k.reason, k


# Selector entries that are not scoped to the deployment, and why that is
# acceptable: fixed object names (lakebench-postgres and the rest) already
# make two deployments in one namespace impossible.
_NAMESPACE_WIDE_SELECTORS = {
    "app.kubernetes.io/managed-by=lakebench",  # datagen-jobs, pre-1.7
    "app.kubernetes.io/component=trino-worker",  # the worker claims, pre-1.7
}


def test_selectors_are_deployment_scoped():
    """A selector delete can only match this deployment, or is a listed pre-1.7 one."""
    from lakebench.deploy.category1 import CATEGORY1_OBJECTS

    selectors = [e for e in CATEGORY1_OBJECTS if e.label_selector]
    assert selectors
    for e in selectors:
        assert (
            "app.kubernetes.io/instance={name}" in e.label_selector
            or e.label_selector in _NAMESPACE_WIDE_SELECTORS
        ), e
        if e.step == "category1":
            assert "app.kubernetes.io/instance={name}" in e.label_selector, e


def test_scripts_entry_is_the_spark_scripts_step_selector():
    from lakebench.deploy.category1 import CATEGORY1_OBJECTS
    from lakebench.modules.pipeline_engines.spark.scripts_maps import scripts_label_selector

    (entry,) = [e for e in CATEGORY1_OBJECTS if e.step == "spark-scripts"]
    assert entry.label_selector == scripts_label_selector("{name}")


def test_entry_matching():
    from lakebench.deploy.category1 import Cat1Entry

    pvc = Cat1Entry("core_v1", "persistentvolumeclaims", name="data-lakebench-postgres-<n>")
    assert pvc.matches("persistentvolumeclaims", "data-lakebench-postgres-12", None, NS)
    assert not pvc.matches("persistentvolumeclaims", "data-lakebench-postgres-0-x", None, NS)
    assert not pvc.matches("configmaps", "data-lakebench-postgres-0", None, NS)
    sel = Cat1Entry("core_v1", "configmaps", label_selector="a=1,app.kubernetes.io/instance={name}")
    assert sel.matches("configmaps", "x", {"a": "1", "app.kubernetes.io/instance": NS}, NS)
    assert not sel.matches("configmaps", "x", {"a": "1", "app.kubernetes.io/instance": "u02"}, NS)
    assert Cat1Entry("custom", "sparkapplications", name="*").matches(
        "sparkapplications", "y", None, NS
    )


# Templates rendered by nothing: HiveDeployer.LEGACY_TEMPLATES (deprecated).
_UNUSED_TEMPLATES = {"hive/configmap.yaml.j2", "hive/deployment.yaml.j2"}
_NOT_NAMESPACED = {"Namespace", "SecretClass", "StorageClass"}
_PLURAL = {
    "ConfigMap": "configmaps",
    "Secret": "secrets",
    "Service": "services",
    "ServiceAccount": "serviceaccounts",
    "PersistentVolumeClaim": "persistentvolumeclaims",
    "Deployment": "deployments",
    "StatefulSet": "statefulsets",
    "Job": "jobs",
    "Role": "roles",
    "RoleBinding": "rolebindings",
    "HiveCluster": "hiveclusters",
    "PodMonitor": "podmonitors",
}


def _template_objects():
    """``(template, kind, name, namespace, labels)`` for every document in templates/."""
    import re

    root = SRC / "templates"
    for path in sorted(root.rglob("*.j2")):
        for doc in re.split(r"(?m)^---\s*$", path.read_text()):
            kind = re.search(r"(?m)^kind:\s*(\S+)", doc)
            meta = re.search(r"(?m)^metadata:\s*\n((?:[ \t]+.*\n?)*)", doc)
            if not kind or not meta:
                continue
            block = meta.group(1)
            name = re.search(r"(?m)^  name:\s*(.+)$", block)
            ns = re.search(r"(?m)^  namespace:\s*(.+)$", block)
            labels_block = re.search(r"(?m)^  labels:\s*\n((?:    .*\n?)*)", block)
            labels = {}
            if labels_block:
                for line in labels_block.group(1).splitlines():
                    k, _, v = line.strip().partition(":")
                    if k and not k.startswith("#"):
                        labels[k.strip()] = v.strip().replace("{{ name }}", NS)
            yield (
                path.relative_to(root).as_posix(),
                kind.group(1),
                name.group(1).strip() if name else "",
                ns.group(1).strip() if ns else "",
                labels,
            )


def test_every_template_object_is_registered():
    """[static] a template that makes a namespaced object needs its registry entry."""
    from lakebench.modules.catalogs.hive.deployer import HiveDeployer

    assert set(HiveDeployer.LEGACY_TEMPLATES) - {"hive/service.yaml.j2"} == _UNUSED_TEMPLATES
    hive_src = (SRC / "modules/catalogs/hive/deployer.py").read_text()
    assert hive_src.count("LEGACY_TEMPLATES") == 1, "LEGACY_TEMPLATES is rendered now"
    missing = []
    seen = 0
    for tpl, kind, name, ns, labels in _template_objects():
        if tpl in _UNUSED_TEMPLATES or kind in _NOT_NAMESPACED or ns != "{{ namespace }}":
            continue
        seen += 1
        plural = _PLURAL.get(kind)
        if plural is None or not _registered(plural, name, labels):
            missing.append(f"{tpl}: {kind}/{name}")
    assert seen > 30
    assert missing == [], missing


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
        result = _category1_step(NS, NS, ns_goes=ns_goes, conditions=frozenset({"observability"}))
        assert result.status.value == status
        assert "serviceaccounts/lakebench-postgres" in result.message
        # The other entries still ran.
        rec.assert_recorded(
            verb="delete", kind="persistentvolumeclaims", name="lakebench-pushgateway"
        )
        anns = rec.store[("namespaces", None, NS)].metadata.annotations or {}
        # Removed only from a surviving namespace; when the namespace step is
        # meant to delete it, a namespace that step keeps stays whole.
        assert ("lakebench.deployment/state-schema" in anns) is ns_goes


def test_category1_observability_entries_tolerate_403_when_off():
    """Observability off now: still tried (it may have been on at deploy), a 403 is no failure."""
    from lakebench.deploy.destroy import _category1_step

    with recording(NS) as rec:
        rec.add_namespace(NS)
        rec.add("podmonitors", {"metadata": {"name": "lakebench-spark-driver"}}, namespace=NS)
        rec.fail(verb="delete", kind="podmonitors", status=403)
        result = _category1_step(NS, NS, ns_goes=False)
        assert result.status.value == "success", result.message
        rec.assert_recorded(
            verb="delete", kind="persistentvolumeclaims", name="lakebench-pushgateway"
        )
        # With observability on, the same 403 is a real failure.
        result = _category1_step(NS, NS, ns_goes=False, conditions=frozenset({"observability"}))
        assert result.status.value == "failed"
        assert "podmonitors/lakebench-spark-driver" in result.message


def test_category1_annotation_patch_is_conditional():
    """A namespace that changed since the read (a redeploy) keeps its annotations."""
    from lakebench.deploy.destroy import _category1_step

    with recording(NS) as rec:
        rec.add_namespace(NS, annotations={"lakebench.deployment/state-schema": "lb-state/1"})
        rec.fail(verb="patch", kind="namespaces", name=NS, status=409)
        result = _category1_step(NS, NS, ns_goes=False)
        assert result.status.value == "failed"
        assert "changed during destroy" in result.message
    core = MagicMock()
    core.read_namespace.return_value.metadata.annotations = {
        "lakebench.deployment/state-schema": "lb-state/1",
        "lakebench.deployment/name": NS,
    }
    core.read_namespace.return_value.metadata.resource_version = "42"
    with patch("lakebench.deploy.destroy._category1_api", return_value=core):
        _category1_step(NS, NS, ns_goes=False)
    body = core.patch_namespace.call_args.args[1]
    assert body == {
        "metadata": {
            "annotations": {"lakebench.deployment/state-schema": None},
            "resourceVersion": "42",
        }
    }


def test_trino_worker_claims_are_deleted(monkeypatch):
    """The worker StatefulSet's claims (its controller makes them) go with the trino step."""
    from lakebench.deploy.engine import DeploymentEngine
    from lakebench.k8s.client import K8sClient

    _instant_waits(monkeypatch)
    cfg = _config("hive-iceberg-spark-trino", "c360-batch")
    cfg.architecture.query_engine.trino.worker.storage_class = "px-csi-db"
    with recording() as rec:
        rec.for_config(cfg)
        _seed_cluster(rec)
        assert not [
            r
            for r in DeploymentEngine(cfg, k8s_client=K8sClient(namespace=NS)).deploy_all()
            if r.status.value == "failed"
        ]
        sts = rec.store[("statefulsets", NS, "lakebench-trino-worker")]
        (template,) = sts.spec.volume_claim_templates
        for i in range(2):
            rec.add(
                "persistentvolumeclaims",
                {
                    "metadata": {
                        "name": f"data-lakebench-trino-worker-{i}",
                        "labels": dict(template.metadata.labels),
                    },
                    "spec": {"accessModes": ["ReadWriteOnce"]},
                },
                namespace=NS,
            )
            assert _registered(
                "persistentvolumeclaims",
                f"data-lakebench-trino-worker-{i}",
                template.metadata.labels,
            )
        _destroy(rec, cfg)
        assert not [k for k in rec.store if k[0] == "persistentvolumeclaims" and k[1] == NS]
