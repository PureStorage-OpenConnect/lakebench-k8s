"""The datagen pod gets heldout_hashes.json from a ConfigMap (DAT-1 Rust
side): the generator refuses the financial schema without it, so a financial
generate applies the map before its Job and mounts it; Customer 360 never
does."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from lakebench.config import datagen_seed as ds
from tests.conftest import make_config

ROOT = Path(__file__).resolve().parents[1]


def _cfg(schema="financial", **dg):
    return make_config(architecture={"workload": {"schema": schema, "datagen": dg}})


def _container(cfg):
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    engine = DeploymentEngine(cfg, dry_run=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    job = yaml.safe_load(engine.renderer.render("datagen/job.yaml.j2", ctx))
    return job["spec"]["template"]["spec"]


def test_financial_job_mounts_the_hash_file():
    from lakebench.deploy.datagen import HELDOUT_MAP_NAME, HELDOUT_MOUNT_DIR

    spec = _container(_cfg(seed=43))
    vols = {v["name"]: v for v in spec["volumes"]}
    assert vols["heldout"]["configMap"] == {"name": HELDOUT_MAP_NAME, "optional": False}
    c = spec["containers"][0]
    mounts = {m["name"]: m for m in c["volumeMounts"]}
    assert mounts["heldout"] == {
        "name": "heldout",
        "mountPath": HELDOUT_MOUNT_DIR,
        "readOnly": True,
    }
    env = {e["name"]: e.get("value") for e in c["env"]}
    assert env["LB_HELDOUT_HASHES"] == f"{HELDOUT_MOUNT_DIR}/{ds.HELDOUT_FILENAME}"


def test_customer360_job_is_unchanged():
    spec = _container(_cfg("customer360", seed=42))
    assert "volumes" not in spec
    c = spec["containers"][0]
    assert "volumeMounts" not in c
    assert "LB_HELDOUT_HASHES" not in {e["name"] for e in c["env"]}


def test_ca_and_heldout_volumes_together(tmp_path):
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine

    cfg = _cfg(seed=43)
    engine = DeploymentEngine(cfg, dry_run=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    ctx["s3_ca_cert_pem"] = "-----BEGIN CERTIFICATE-----"
    spec = yaml.safe_load(engine.renderer.render("datagen/job.yaml.j2", ctx))["spec"]["template"][
        "spec"
    ]
    assert {v["name"] for v in spec["volumes"]} == {"ca-cert", "heldout"}
    assert {m["name"] for m in spec["containers"][0]["volumeMounts"]} == {"ca-cert", "heldout"}


def test_map_carries_the_packaged_file():
    from lakebench.deploy.datagen import HELDOUT_MAP_NAME, ensure_heldout_map

    k8s = MagicMock()
    k8s.apply_manifest.return_value = True
    cfg = _cfg(seed=43)
    ensure_heldout_map(cfg, k8s)
    m = k8s.apply_manifest.call_args.args[0]
    assert m["kind"] == "ConfigMap" and m["metadata"]["name"] == HELDOUT_MAP_NAME
    assert m["metadata"]["labels"]["app.kubernetes.io/instance"] == cfg.name
    shipped = json.loads(m["data"][ds.HELDOUT_FILENAME])
    assert shipped == json.loads(ds.heldout_path().read_text())


@pytest.mark.parametrize("failure", ["raise", "false", "unreadable"])
def test_map_failure_submits_no_job(monkeypatch, failure):
    import lakebench.deploy.datagen as dg
    from lakebench.deploy.engine import DeploymentEngine

    cfg = _cfg(seed=43)
    applied = []

    def apply(m, namespace=None):
        applied.append(m["kind"])
        if m["kind"] == "ConfigMap":
            if failure == "raise":
                raise RuntimeError("HTTP 403")
            if failure == "false":
                return False
        return True

    if failure == "unreadable":

        def gone():
            raise FileNotFoundError("heldout_hashes.json")

        monkeypatch.setattr(ds, "heldout_path", gone)
    engine = DeploymentEngine(cfg, dry_run=True)
    engine.dry_run = False
    engine.k8s = MagicMock()
    engine.k8s.apply_manifest.side_effect = apply
    d = dg.DatagenDeployer(engine)
    d.stop_previous_job = lambda: None
    d._clear_bronze_prefix_if_fresh = lambda *a, **k: None
    d._begin_series = lambda *a, **k: None  # the series marker (CD-18): no S3 here
    r = d.deploy()
    assert r.status.value == "failed" and "Job" not in applied, r.message
    assert "heldout" in r.message.lower() or "held-out" in r.message.lower(), r.message


def test_customer360_generate_applies_no_map():
    import lakebench.deploy.datagen as dg
    from lakebench.deploy.engine import DeploymentEngine

    cfg = _cfg("customer360", seed=42)
    engine = DeploymentEngine(cfg, dry_run=True)
    engine.dry_run = False
    engine.k8s = MagicMock()
    engine.k8s.apply_manifest.return_value = True
    d = dg.DatagenDeployer(engine)
    d.stop_previous_job = lambda: None
    d._clear_bronze_prefix_if_fresh = lambda *a, **k: None
    d._begin_series = lambda *a, **k: None  # the series marker (CD-18): no S3 here
    assert d.deploy().status.value == "success"
    kinds = [c.args[0]["kind"] for c in engine.k8s.apply_manifest.call_args_list]
    assert kinds == ["Job"]


def test_byte_compare_mounts_the_hash_file():
    from tests.conftest import exec_repo_script

    bc = exec_repo_script(ROOT / "scripts/datagen_byte_compare.py", "datagen_byte_compare_hm")
    assert bc.HELDOUT_FILE.is_file()
    assert f"LB_HELDOUT_HASHES={bc.HELDOUT_MOUNT}" in bc.HELDOUT_ARGS


def test_byte_compare_runs_every_generator_with_the_hash_file(monkeypatch):
    from types import SimpleNamespace

    from tests.conftest import exec_repo_script

    bc = exec_repo_script(ROOT / "scripts/datagen_byte_compare.py", "datagen_byte_compare_hm2")
    seen = []

    def run(name, args, what):
        seen.append(args)
        return "[entrypoint] schema=financial node 0/1 threads=2 cycle=0 -> s3://b/ :: x"

    monkeypatch.setattr(bc, "_run_container", run)
    minio = SimpleNamespace(endpoint="http://127.0.0.1:1", access="a", secret="s", port=1)
    assert bc.run_generator("img", ["--seed", "43"], 0, minio, {}) == 2
    args = seen[0]
    i = args.index("img")
    assert all(a in args[:i] for a in bc.HELDOUT_ARGS)
    # A shared SELinux label: a private one (Z) would deny the next container.
    assert bc.HELDOUT_ARGS[1].endswith(":ro,z")


def test_python_and_rust_refuse_a_non_integer_format(tmp_path):
    doc = json.loads(ds.heldout_path().read_text())
    for bad in (True, 1.0):
        p = tmp_path / "h.json"
        p.write_text(json.dumps({**doc, "format": bad}))
        with pytest.raises(ValueError, match="format"):
            ds.load_heldout(p)
