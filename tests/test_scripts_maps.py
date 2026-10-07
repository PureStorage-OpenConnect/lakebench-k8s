"""Scripts ConfigMaps by role (DEP-1, LB-207, SD-8).

The scripts ship in one ConfigMap per role, each guarded at 80% of the 1 MiB
ConfigMap limit by measuring exactly the data that is applied. A listed file
missing from the package raises instead of being skipped (v1.6 skipped it and
the driver failed three attempts later with an ImportError).
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lakebench import _resources
from lakebench.modules.pipeline_engines.spark import scripts_maps as sm
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.fixtures.scripts_maps_helpers import LEGACY as LEGACY
from tests.fixtures.scripts_maps_helpers import FakeK8s as FakeK8s
from tests.fixtures.scripts_maps_helpers import _cfg as _cfg

PKG = Path(_resources.__file__).parent


@pytest.fixture(autouse=True)
def _no_live_apps(monkeypatch):
    """No SparkApplication mounts the legacy map unless a test says so."""
    monkeypatch.setattr(SparkJobManager, "_live_apps_mounting", lambda self, cm: [], raising=False)


def _copy_package(tmp_path: Path) -> Path:
    """A package tree holding exactly the listed files."""
    for sources in sm.SCRIPT_MAPS.values():
        for src in sources:
            dst = tmp_path / src.path
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(PKG / src.path, dst)
    return tmp_path


# -- budget ------------------------------------------------------------------


# -- manifest ------------------------------------------------------------------


def test_deploy_refuses_when_listed_file_missing(tmp_path, monkeypatch):
    """Fix-reverted: v1.6 skipped a missing listed script and returned True."""
    pkg = _copy_package(tmp_path)
    (pkg / "spark/scripts/tm_operations.py").unlink()
    monkeypatch.setattr(_resources, "_package_dir", lambda: pkg)
    k8s = FakeK8s()
    mgr = SparkJobManager(_cfg("financial"), k8s)
    with pytest.raises(sm.ScriptsMapError, match="tm_operations.py is listed for map aml-rules"):
        mgr.deploy_scripts_configmap()
    assert k8s.applied == [], "nothing may be applied when the manifest is incomplete"


def test_duplicate_key_across_roles_raises(monkeypatch):
    dup = dict(sm.SCRIPT_MAPS)
    dup["aml-rules"] = (*dup["aml-rules"], sm.ScriptSource("spark/scripts/common.py", "common.py"))
    monkeypatch.setattr(sm, "SCRIPT_MAPS", dup)
    with pytest.raises(sm.ScriptsManifestError, match="common.py"):
        sm.build_script_configmaps(_cfg(), "ns")


def test_every_aml_json_listed_or_excluded():
    listed = {s.path for s in sm.SCRIPT_MAPS["aml-data"]} | set(sm.NOT_SHIPPED)
    on_disk = {f"spark/data/aml/{p.name}" for p in (PKG / "spark/data/aml").glob("*.json")}
    assert on_disk, "the AML data directory is empty"
    assert on_disk <= listed, f"unlisted AML JSON: {sorted(on_disk - listed)}"


def test_ships_the_same_bytes_as_v16():
    """The role maps together ship exactly the v1.6 single map's files and
    bytes, plus the files v1.7 adds."""
    v16_scripts = [
        "common.py",
        "bronze_verify.py",
        "silver_build.py",
        "gold_finalize.py",
        "bronze_ingest.py",
        "silver_stream.py",
        "gold_refresh.py",
        "silver_build_delta.py",
        "gold_finalize_delta.py",
        "gold_refresh_delta.py",
        "bronze_ingest_delta.py",
        "silver_stream_delta.py",
        "bronze_verify_financial.py",
        "silver_build_financial.py",
        "gold_finalize_financial.py",
        "bronze_ingest_financial.py",
        "silver_stream_financial.py",
        "gold_refresh_financial.py",
        "replay_financial.py",
        "reproduce_financial.py",
        "score_financial.py",
        "score_financial_reference.py",
        "detection_rules.py",
        "aml_features.py",
        "tm_operations.py",
    ]
    # Files v1.7 adds to the maps: the AML reason-code vocabulary
    # and the continuous time-travel reads.
    v16_scripts += ["aml_reason_codes.py", "time_travel_financial.py"]
    want = {n: (PKG / "spark/scripts" / n).read_bytes() for n in v16_scripts}
    want["reference_score.py"] = (PKG / "aml/reference_score.py").read_bytes()
    want["fidelity_gate.py"] = (PKG / "aml/fidelity_gate.py").read_bytes()
    want["datagen_seed.py"] = (PKG / "config/datagen_seed.py").read_bytes()
    for p in (PKG / "spark/data/aml").glob("*.json"):
        want[p.name] = p.read_bytes()
    got = {
        k: v.encode("utf-8")
        for cm in sm.build_script_configmaps(_cfg(), "ns")
        for k, v in cm["data"].items()
    }
    assert got.keys() == want.keys()
    assert all(got[k] == want[k] for k in want), [k for k in want if got[k] != want[k]]


def test_map_metadata_labels_and_hash():
    cm = sm.build_script_configmaps(_cfg(), "ns-x")[0]
    md = cm["metadata"]
    assert md["namespace"] == "ns-x"
    assert md["labels"]["app.kubernetes.io/component"] == "spark-scripts"
    assert md["labels"]["app.kubernetes.io/instance"] == "sd8"
    assert md["labels"]["lakebench.io/deployment"] == "sd8"
    assert md["labels"]["lakebench.io/scripts-role"] == "common"
    assert md["annotations"]["lakebench.io/scripts-sha256"] == sm.data_sha256(cm["data"])


def test_hashes_move_with_one_byte(tmp_path):
    pkg = _copy_package(tmp_path)
    before = sm.build_script_configmaps(_cfg(), "ns", package_dir=pkg)
    path = pkg / "spark/data/aml/synthetic_corridors.json"
    path.write_bytes(path.read_bytes() + b" ")
    after = sm.build_script_configmaps(_cfg(), "ns", package_dir=pkg)
    ann = sm.SCRIPTS_SHA256_ANNOTATION
    changed = [
        a["metadata"]["name"]
        for a, b in zip(before, after, strict=True)
        if a["metadata"]["annotations"][ann] != b["metadata"]["annotations"][ann]
    ]
    assert changed == ["lakebench-scripts-aml-data"]
    assert sm.scripts_sha256(before) != sm.scripts_sha256(after)
    assert sm.scripts_sha256(before) == sm.scripts_sha256(list(reversed(before)))


# -- mounts ------------------------------------------------------------------


def _scripts_volume(template: dict) -> dict:
    vols = [v for v in template["spec"]["volumes"] if v["name"] == "spark-scripts"]
    assert len(vols) == 1
    return vols[0]


@pytest.mark.parametrize("schema,fmt", [("customer360", "iceberg"), ("financial", "iceberg")])
def test_pod_template_projects_every_role(schema, fmt):
    mgr = SparkJobManager(_cfg(schema, fmt), FakeK8s())
    want = [{"configMap": {"name": sm.map_name(r), "optional": False}} for r in sm.ROLES]
    for jt in JobType:
        if sm.MOUNTS_BY_JOB_TYPE[jt] != sm.ROLES:
            continue
        if jt is JobType.SCORE_FINANCIAL_REFERENCE and schema != "financial":
            continue  # refused without the AML set's reference wheels
        manifest = mgr._build_manifest(jt)
        for side in ("driver", "executor"):
            tpl = manifest["spec"][side]["template"]
            vol = _scripts_volume(tpl)
            assert "configMap" not in vol, "the v1.6 single-map volume is gone"
            assert vol["projected"]["sources"] == want, (jt, side)
            mounts = [
                m
                for c in tpl["spec"]["containers"]
                for m in c["volumeMounts"]
                if m["name"] == "spark-scripts"
            ]
            assert [m["mountPath"] for m in mounts] == ["/opt/spark/scripts"], (jt, side)


@pytest.mark.parametrize(
    "schema,fmt",
    [("customer360", "iceberg"), ("customer360", "delta"), ("financial", "iceberg")],
)
def test_every_main_file_ships_in_a_mounted_role(schema, fmt):
    cfg = _cfg(schema, fmt)
    by_role = {
        cm["metadata"]["labels"][sm.ROLE_LABEL]: set(cm["data"])
        for cm in sm.build_script_configmaps(cfg, "ns")
    }
    mgr = SparkJobManager(cfg, FakeK8s())
    for jt in JobType:
        if jt is JobType.SCORE_FINANCIAL_REFERENCE and schema != "financial":
            continue  # refused without the AML set's reference wheels
        main = mgr._build_manifest(jt)["spec"]["mainApplicationFile"]
        key = main.removeprefix("local:///opt/spark/scripts/")
        mounted = set().union(*(by_role[r] for r in sm.MOUNTS_BY_JOB_TYPE[jt]))
        assert key in mounted, f"{jt.value} runs {key}, which no mounted map ships"


# -- apply ------------------------------------------------------------------


def test_deploy_applies_every_role_reads_back_and_drops_legacy():
    k8s = FakeK8s()
    mgr = SparkJobManager(_cfg(), k8s)
    assert mgr.scripts_provenance is None
    assert mgr.deploy_scripts_configmap() is True
    assert [cm["metadata"]["name"] for cm in k8s.applied] == [sm.map_name(r) for r in sm.ROLES]
    assert k8s.deleted == [LEGACY]
    prov = mgr.scripts_provenance
    assert prov is not None
    assert set(prov["scripts_maps"]) == set(sm.ROLES)
    assert prov["scripts_sha256"] == sm.scripts_sha256(k8s.applied)
    prereg = (PKG / "spark/data/aml/aml_preregistration.json").read_bytes()
    assert prov["files_sha256"]["aml_preregistration.json"] == hashlib.sha256(prereg).hexdigest()


def test_deploy_fails_on_read_back_mismatch():
    bad = {
        "labels": {"app.kubernetes.io/instance": "sd8"},
        "annotations": {sm.SCRIPTS_SHA256_ANNOTATION: "0" * 64},
        "data": {},
    }
    k8s = FakeK8s(overrides={"lakebench-scripts-aml-gate": bad})
    mgr = SparkJobManager(_cfg(), k8s)
    with pytest.raises(sm.ScriptsApplyError, match=r"reads back changed"):
        mgr.deploy_scripts_configmap()
    assert k8s.deleted == [], "the legacy map stays until the new maps read back"
    assert mgr.scripts_provenance is None


def test_deploy_fails_when_data_does_not_match_its_annotation():
    """The read-back hashes the returned data, not only the annotation."""

    class Tamper(FakeK8s):
        def get_configmap(self, name, namespace=None):
            got = super().get_configmap(name, namespace)
            if name == "lakebench-scripts-aml-data" and got:
                got = {**got, "data": {**got["data"], "synthetic_corridors.json": "{}"}}
            return got

    with pytest.raises(
        sm.ScriptsApplyError, match=r"lakebench-scripts-aml-data reads back changed"
    ):
        SparkJobManager(_cfg(), Tamper()).deploy_scripts_configmap()


def test_deploy_fails_when_a_map_is_missing_on_read_back():
    k8s = FakeK8s(overrides={"lakebench-scripts-common": None})
    with pytest.raises(sm.ScriptsApplyError, match=r"lakebench-scripts-common reads back changed"):
        SparkJobManager(_cfg(), k8s).deploy_scripts_configmap()


def test_foreign_legacy_map_is_kept():
    k8s = FakeK8s(legacy_owner="someone-else")
    assert SparkJobManager(_cfg(), k8s).deploy_scripts_configmap() is True
    assert k8s.deleted == []
    assert LEGACY in k8s.store


def test_legacy_map_kept_while_a_live_app_mounts_it(monkeypatch):
    monkeypatch.setattr(
        SparkJobManager, "_live_apps_mounting", lambda self, cm: ["lakebench-silver-stream"]
    )
    k8s = FakeK8s()
    assert SparkJobManager(_cfg(), k8s).deploy_scripts_configmap() is True
    assert k8s.deleted == []


def test_legacy_map_kept_when_apps_cannot_be_listed(monkeypatch):
    def boom(self, cm):
        raise RuntimeError("403 forbidden")

    monkeypatch.setattr(SparkJobManager, "_live_apps_mounting", boom)
    k8s = FakeK8s()
    assert SparkJobManager(_cfg(), k8s).deploy_scripts_configmap() is True
    assert k8s.deleted == []


def test_live_apps_mounting_reads_templates(monkeypatch):
    def app(name, state, vol, restart="OnFailure"):
        tpl = {"spec": {"volumes": [vol]}}
        return {
            "metadata": {"name": name},
            "spec": {"driver": {"template": tpl}, "restartPolicy": {"type": restart}},
            "status": {"applicationState": {"state": state}},
        }

    def cm(name):
        return {"name": "spark-scripts", "configMap": {"name": name}}

    def projected(*names):
        return {
            "name": "spark-scripts",
            "projected": {"sources": [{"configMap": {"name": n}} for n in names]},
        }

    common = "lakebench-scripts-common"
    listing = {
        "items": [
            app("a-running", "RUNNING", cm(LEGACY)),
            app("b-done", "COMPLETED", cm(LEGACY)),
            app("c-other-map", "RUNNING", cm(common)),
            app("d-submitted", "", cm(LEGACY)),
            app("e-failed-onfailure", "FAILED", cm(LEGACY)),
            app("f-failed-final", "FAILED", cm(LEGACY), restart="Never"),
            app("g-projected", "RUNNING", projected("lakebench-scripts-c360", common)),
        ]
    }
    api = MagicMock()
    api.list_namespaced_custom_object.return_value = listing
    monkeypatch.undo()  # drop the autouse stub for this test
    mgr = SparkJobManager(_cfg(), FakeK8s())
    with patch("kubernetes.client.CustomObjectsApi", return_value=api):
        assert mgr._live_apps_mounting(LEGACY) == ["a-running", "d-submitted"]
        assert mgr._live_apps_mounting(common) == ["c-other-map", "g-projected"]


def _prior_apply(k8s: FakeK8s, owner: str = "sd8", tweak: str | None = None) -> None:
    """Put role maps in the fake as an earlier run left them."""
    for cm in sm.build_script_configmaps(_cfg(), "ns"):
        name = cm["metadata"]["name"]
        ann = dict(cm["metadata"]["annotations"])
        if name == tweak:
            ann[sm.SCRIPTS_SHA256_ANNOTATION] = "e" * 64
        k8s.store[name] = {
            "labels": {"app.kubernetes.io/instance": owner},
            "annotations": ann,
            "data": dict(cm["data"]),
        }


def test_role_map_of_another_deployment_is_never_replaced():
    k8s = FakeK8s()
    _prior_apply(k8s, owner="other")
    with pytest.raises(sm.ScriptsApplyError, match=r"belongs to deployment 'other'"):
        SparkJobManager(_cfg(), k8s).deploy_scripts_configmap()
    assert k8s.applied == []


def test_changed_map_not_replaced_under_a_live_app(monkeypatch):
    k8s = FakeK8s()
    _prior_apply(k8s, tweak="lakebench-scripts-aml-rules")
    seen: list[str] = []

    def live(self, name):
        seen.append(name)
        return ["lakebench-gold-refresh"] if name == "lakebench-scripts-aml-rules" else []

    monkeypatch.setattr(SparkJobManager, "_live_apps_mounting", live)
    with pytest.raises(
        sm.ScriptsApplyError,
        match=r"lakebench-scripts-aml-rules would change under running SparkApplication.s. lakebench-gold-refresh",
    ):
        SparkJobManager(_cfg(), k8s).deploy_scripts_configmap()
    assert k8s.applied == []
    assert seen == ["lakebench-scripts-aml-rules"], "unchanged maps need no live check"


def test_unchanged_maps_reapply_while_apps_run(monkeypatch):
    k8s = FakeK8s()
    _prior_apply(k8s)
    monkeypatch.setattr(SparkJobManager, "_live_apps_mounting", lambda self, n: ["x"])
    assert SparkJobManager(_cfg(), k8s).deploy_scripts_configmap() is True


def test_changed_map_refused_when_apps_cannot_be_listed(monkeypatch):
    k8s = FakeK8s()
    _prior_apply(k8s, tweak="lakebench-scripts-common")

    def boom(self, name):
        raise RuntimeError("503")

    monkeypatch.setattr(SparkJobManager, "_live_apps_mounting", boom)
    with pytest.raises(sm.ScriptsApplyError, match=r"could not list SparkApplications"):
        SparkJobManager(_cfg(), k8s).deploy_scripts_configmap()
    assert k8s.applied == []


def test_submit_refuses_when_scripts_changed_after_apply():
    k8s = FakeK8s()
    mgr = SparkJobManager(_cfg(), k8s)
    assert mgr.deploy_scripts_configmap() is True
    assert mgr.scripts_changed_since_apply(JobType.SILVER_BUILD) is None
    k8s.store["lakebench-scripts-common"]["annotations"][sm.SCRIPTS_SHA256_ANNOTATION] = "f" * 64
    created = MagicMock()
    with patch("kubernetes.client.CustomObjectsApi", return_value=created):
        status = mgr.submit_job(JobType.SILVER_BUILD)
    assert status.state.name == "FAILED"
    assert "lakebench-scripts-common changed since this run applied it" in status.message
    created.create_namespaced_custom_object.assert_not_called()


def test_financial_command_exits_when_not_submitted(capsys):
    """A refused submit must not fall through to waiting on a previous
    application of the same name (a false pass) or on nothing (a hang)."""
    import typer

    from lakebench.cli import _financial
    from lakebench.modules.pipeline_engines.spark.job import JobState, JobStatus

    status = JobStatus(name="lakebench-score-financial", state=JobState.FAILED, message="why")
    with pytest.raises(typer.Exit) as ei:
        _financial._require_submitted(status)
    assert ei.value.exit_code == 1
    assert "Not submitted: why" in capsys.readouterr().out
    _financial._require_submitted(
        JobStatus(name="x", state=JobState.SUBMITTED, message="ok")
    )  # no exit


def test_client_configmap_helpers_treat_404_as_absent():
    from kubernetes.client.rest import ApiException

    from lakebench.k8s.client import K8sClient

    c = K8sClient.__new__(K8sClient)
    c._namespace = "ns"
    c._core_v1 = MagicMock()
    c._core_v1.read_namespaced_config_map.side_effect = ApiException(status=404)
    c._core_v1.delete_namespaced_config_map.side_effect = ApiException(status=404)
    assert c.get_configmap("x") is None
    assert c.delete_configmap("x") is False
    c._core_v1.read_namespaced_config_map.side_effect = ApiException(status=403)
    with pytest.raises(ApiException):
        c.get_configmap("x")


# -- destroy ------------------------------------------------------------------


def _destroy(core, create_namespace: bool = False):
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    engine = MagicMock()
    engine.config.name = "u02"
    engine.config.get_namespace.return_value = "u02"
    engine.config.platform.kubernetes.create_namespace = create_namespace
    match = IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name="u02",
        expected_deployment="u02",
        found_deployment="u02",
    )
    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=match),
        patch("lakebench.deploy.ownership.verify_bucket_ownership", return_value=match),
        patch("kubernetes.client.CoreV1Api", return_value=core),
        patch("lakebench.spark.SparkOperatorManager", MagicMock()),
        patch("lakebench.deploy.destroy.logger"),
    ):
        return destroy_all(engine, clean_buckets=False)


def _scripts_list_calls(core):
    return [
        c
        for c in core.list_namespaced_config_map.call_args_list
        if "spark-scripts" in str(c.kwargs.get("label_selector", ""))
    ]


def test_destroy_deletes_this_deployments_scripts_maps():
    """Fix-reverted: with create_namespace=false the namespace survives
    destroy, and v1.6 destroy never deleted the scripts ConfigMap."""
    core = MagicMock()
    names = ["lakebench-scripts-common", LEGACY]
    core.list_namespaced_config_map.return_value = SimpleNamespace(
        items=[SimpleNamespace(metadata=SimpleNamespace(name=n)) for n in names]
    )
    results = _destroy(core)

    calls = _scripts_list_calls(core)
    assert calls, "destroy never listed the scripts ConfigMaps"
    sel = calls[0].kwargs["label_selector"]
    assert calls[0].args[0] == "u02"
    assert "app.kubernetes.io/component=spark-scripts" in sel
    assert "app.kubernetes.io/instance=u02" in sel, "the selector must name this deployment"
    deleted = [c.args for c in core.delete_namespaced_config_map.call_args_list]
    assert ("lakebench-scripts-common", "u02") in deleted
    assert (LEGACY, "u02") in deleted
    step = [r for r in results if r.component == "spark-scripts"]
    assert step and step[-1].message == "Deleted 2 scripts ConfigMaps"


@pytest.mark.parametrize("create_namespace,status", [(False, "FAILED"), (True, "SKIPPED")])
def test_destroy_scripts_step_list_failure(create_namespace, status):
    """A surviving namespace keeps the maps, so a failure is FAILED; when the
    namespace delete follows, it removes them and the step only skips."""
    core = MagicMock()

    def listing(ns, label_selector=""):
        if "spark-scripts" in label_selector:
            raise RuntimeError("403 forbidden: configmaps is forbidden\nmore detail")
        return SimpleNamespace(items=[])

    core.list_namespaced_config_map.side_effect = listing
    results = _destroy(core, create_namespace=create_namespace)
    step = [r for r in results if r.component == "spark-scripts"]
    assert step and step[-1].status.name == status
    assert "\n" not in step[-1].message and "403 forbidden" in step[-1].message
