"""Scripts ConfigMaps by role (DEP-1, LB-207, SD-8).

The scripts ship in one ConfigMap per role, each guarded at 80% of the 1 MiB
ConfigMap limit by measuring exactly the data that is applied. A listed file
missing from the package raises instead of being skipped (v1.6 skipped it and
the driver failed three attempts later with an ImportError).
"""

from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from lakebench import _resources
from lakebench.config import LakebenchConfig
from lakebench.modules.pipeline_engines.spark import scripts_maps as sm
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

PKG = Path(_resources.__file__).parent


class FakeK8s:
    """Records applies; reads back what was applied (or ``overrides``)."""

    def __init__(self, fail_apply_of: str | None = None, overrides: dict | None = None):
        self.applied: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.store: dict[str, dict[str, str]] = {}
        self.fail_apply_of = fail_apply_of
        self.overrides = overrides or {}
        self.legacy_present = True

    def get_cluster_capacity(self):
        return None

    def apply_manifest(self, manifest, namespace=None):
        name = manifest["metadata"]["name"]
        if name == self.fail_apply_of:
            return False
        self.applied.append(manifest)
        self.store[name] = dict(manifest["metadata"].get("annotations", {}))
        return True

    def get_configmap_annotations(self, name, namespace=None):
        if name in self.overrides:
            return self.overrides[name]
        return self.store.get(name)

    def delete_configmap(self, name, namespace=None):
        self.deleted.append(name)
        present = self.legacy_present
        self.legacy_present = False
        return present


def _cfg(schema: str = "customer360", fmt: str = "iceberg") -> LakebenchConfig:
    return LakebenchConfig(
        name="sd8",
        workload={"schema": schema},
        architecture={"table_format": {"type": fmt}},
    )


def _copy_package(tmp_path: Path) -> Path:
    """A package tree holding exactly the listed files."""
    for sources in sm.SCRIPT_MAPS.values():
        for src in sources:
            dst = tmp_path / src.path
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(PKG / src.path, dst)
    return tmp_path


def _size(data: dict[str, str]) -> int:
    # Independent of sm.map_data_size: the API server's rule, key + value bytes.
    total = 0
    for k, v in data.items():
        total += len(bytes(k, "utf-8")) + len(bytes(v, "utf-8"))
    return total


# -- budget ------------------------------------------------------------------


def test_budget_is_80_percent_of_one_mib():
    assert sm.MAP_BUDGET_BYTES == 838_860


def test_every_map_under_budget_at_tip():
    maps = sm.build_script_configmaps(_cfg(), "ns")
    assert [cm["metadata"]["name"] for cm in maps] == [sm.map_name(r) for r in sm.ROLES]
    for cm in maps:
        size = _size(cm["data"])
        assert 0 < size < 838_860, f"{cm['metadata']['name']} is {size} bytes"


def _pad_common_to(pkg: Path, target: int) -> None:
    """Grow common.py so the common map's measured size is ``target``."""
    path = pkg / "spark/scripts/common.py"
    body = path.read_bytes()
    now = len(b"common.py") + len(body)
    assert target > now
    path.write_bytes(body + b"#" * (target - now))


def test_map_pushed_to_81_percent_raises(tmp_path):
    pkg = _copy_package(tmp_path)
    _pad_common_to(pkg, int(0.81 * 1_048_576))
    with pytest.raises(sm.ScriptsBudgetError) as ei:
        sm.build_script_configmaps(_cfg(), "ns", package_dir=pkg)
    assert ei.value.role == "common"
    assert ei.value.size == int(0.81 * 1_048_576)
    assert "lakebench-scripts-common" in str(ei.value)
    assert "\n" not in str(ei.value)


def test_budget_boundary_is_inclusive(tmp_path):
    pkg = _copy_package(tmp_path)
    _pad_common_to(pkg, sm.MAP_BUDGET_BYTES)
    sm.build_script_configmaps(_cfg(), "ns", package_dir=pkg)  # exactly at budget: ok
    _pad_common_to(pkg, sm.MAP_BUDGET_BYTES + 1)
    with pytest.raises(sm.ScriptsBudgetError):
        sm.build_script_configmaps(_cfg(), "ns", package_dir=pkg)


def test_budget_counts_utf8_bytes_not_characters(tmp_path):
    pkg = _copy_package(tmp_path)
    path = pkg / "spark/scripts/common.py"
    body = path.read_bytes()
    room = sm.MAP_BUDGET_BYTES - len(b"common.py") - len(body)
    # Two-byte characters: as many characters as there are bytes of room fit
    # by character count but are twice the budget by bytes.
    path.write_bytes(body + ("é" * room).encode("utf-8"))
    with pytest.raises(sm.ScriptsBudgetError):
        sm.build_script_configmaps(_cfg(), "ns", package_dir=pkg)


# -- manifest ------------------------------------------------------------------


def test_listed_missing_file_raises(tmp_path):
    pkg = _copy_package(tmp_path)
    (pkg / "spark/scripts/tm_operations.py").unlink()
    with pytest.raises(sm.ScriptsManifestError) as ei:
        sm.build_script_configmaps(_cfg(), "ns", package_dir=pkg)
    assert str(ei.value) == (
        "spark/scripts/tm_operations.py is listed for map aml-rules but is not in the package"
    )


def test_deploy_refuses_when_listed_file_missing(tmp_path, monkeypatch):
    """Fix-reverted: v1.6 skipped a missing listed script and returned True."""
    pkg = _copy_package(tmp_path)
    (pkg / "spark/scripts/tm_operations.py").unlink()
    monkeypatch.setattr(_resources, "_package_dir", lambda: pkg)
    k8s = FakeK8s()
    mgr = SparkJobManager(_cfg("financial"), k8s)
    assert mgr.deploy_scripts_configmap() is False
    assert k8s.applied == [], "nothing may be applied when the manifest is incomplete"


def test_no_duplicate_keys():
    keys = [s.key for sources in sm.SCRIPT_MAPS.values() for s in sources]
    assert len(keys) == len(set(keys))


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
    bytes (freeze cost None: frozen scripts and the prereg ship unchanged)."""
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


def test_mounts_table_covers_every_job_type():
    assert set(sm.MOUNTS_BY_JOB_TYPE) == set(JobType)
    for jt, roles in sm.MOUNTS_BY_JOB_TYPE.items():
        assert roles, f"{jt} mounts nothing"
        assert set(roles) <= set(sm.ROLES), f"{jt} names an unknown role"


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
    assert k8s.deleted == ["lakebench-spark-scripts"]
    prov = mgr.scripts_provenance
    assert prov is not None
    assert set(prov["scripts_maps"]) == set(sm.ROLES)
    assert prov["scripts_sha256"] == sm.scripts_sha256(k8s.applied)


def test_deploy_fails_on_read_back_mismatch():
    k8s = FakeK8s(
        overrides={"lakebench-scripts-aml-gate": {sm.SCRIPTS_SHA256_ANNOTATION: "0" * 64}}
    )
    mgr = SparkJobManager(_cfg(), k8s)
    assert mgr.deploy_scripts_configmap() is False
    assert k8s.deleted == [], "the legacy map stays until the new maps read back"
    assert mgr.scripts_provenance is None


def test_deploy_fails_when_a_map_is_missing_on_read_back():
    k8s = FakeK8s(overrides={"lakebench-scripts-common": None})
    assert SparkJobManager(_cfg(), k8s).deploy_scripts_configmap() is False


def test_partial_apply_stops_before_the_rest():
    k8s = FakeK8s(fail_apply_of="lakebench-scripts-aml-rules")
    assert SparkJobManager(_cfg(), k8s).deploy_scripts_configmap() is False
    assert [cm["metadata"]["name"] for cm in k8s.applied] == [
        "lakebench-scripts-common",
        "lakebench-scripts-c360",
    ]


def test_budget_error_returns_false_with_one_line(tmp_path, monkeypatch, caplog):
    pkg = _copy_package(tmp_path)
    _pad_common_to(pkg, sm.MAP_BUDGET_BYTES + 1)
    monkeypatch.setattr(_resources, "_package_dir", lambda: pkg)
    k8s = FakeK8s()
    assert SparkJobManager(_cfg(), k8s).deploy_scripts_configmap() is False
    assert k8s.applied == []
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == 1 and "\n" not in errors[0]
    assert "lakebench-scripts-common" in errors[0] and "budget" in errors[0]


# -- destroy ------------------------------------------------------------------


def test_destroy_deletes_this_deployments_scripts_maps():
    """Fix-reverted: with create_namespace=false the namespace survives
    destroy, and v1.6 destroy never deleted the scripts ConfigMap."""
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    engine = MagicMock()
    engine.config.name = "u02"
    engine.config.get_namespace.return_value = "u02"
    engine.config.platform.kubernetes.create_namespace = False

    core = MagicMock()
    names = ["lakebench-scripts-common", "lakebench-spark-scripts"]
    core.list_namespaced_config_map.return_value = SimpleNamespace(
        items=[SimpleNamespace(metadata=SimpleNamespace(name=n)) for n in names]
    )
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
        patch("lakebench.deploy.destroy.logger"),
    ):
        results = destroy_all(engine, clean_buckets=False)

    selector_calls = [
        c
        for c in core.list_namespaced_config_map.call_args_list
        if "spark-scripts" in str(c.kwargs.get("label_selector", ""))
    ]
    assert selector_calls, "destroy never listed the scripts ConfigMaps"
    sel = selector_calls[0].kwargs["label_selector"]
    assert selector_calls[0].args[0] == "u02"
    assert "app.kubernetes.io/component=spark-scripts" in sel
    assert "app.kubernetes.io/instance=u02" in sel, "the selector must name this deployment"
    deleted = [c.args for c in core.delete_namespaced_config_map.call_args_list]
    assert ("lakebench-scripts-common", "u02") in deleted
    assert ("lakebench-spark-scripts", "u02") in deleted
    step = [r for r in results if r.component == "spark-scripts"]
    assert step and step[-1].message == "Deleted 2 scripts ConfigMaps"
