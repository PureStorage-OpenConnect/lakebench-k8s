"""Evidence and config honesty fixes for v1.6 (LB-189, LB-190, LB-191).

Each test fails if its fix is reverted: the recorded Hive version is the one
the template renders, a config field nothing reads says so at load, and the
runtime strings describe what the code does.
"""

from __future__ import annotations

import time
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config import LakebenchConfig
from lakebench.deploy.engine import DeploymentEngine, DeploymentStatus
from lakebench.k8s import WaitResult, WaitStatus
from tests.conftest import make_config

ROOT = Path(__file__).resolve().parents[1]

# The Hive the HiveCluster deploys (deliberate; Hive 4 breaks Iceberg).
STACKABLE_HIVE_VERSION = "3.1.3"


def _engine(cfg: LakebenchConfig) -> DeploymentEngine:
    k8s = MagicMock()
    k8s.namespace_exists.return_value = True
    k8s.apply_manifest.return_value = True
    k8s.get_cluster_capacity.return_value = None
    with patch(
        "lakebench.deploy.engine.DeploymentEngine._detect_openshift",
        return_value=False,
    ):
        return DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=True)


def _hive_warnings(rec) -> list[str]:
    return [str(w.message) for w in rec if "images.hive" in str(w.message)]


# -- LB-189: the recorded Hive is the Hive the template deploys ---------------


def test_stackable_hive_version_is_the_deliberate_3_1_3():
    from lakebench.config.schema import STACKABLE_HIVE_VERSION as pinned

    assert pinned == STACKABLE_HIVE_VERSION


def test_hive_deploy_records_the_rendered_product_version():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        cfg = make_config(images={"hive": "apache/hive:4.0.1"})
    engine = _engine(cfg)

    from lakebench.modules.catalogs.hive.deployer import HiveDeployer

    deployer = HiveDeployer(engine)
    with (
        patch.object(
            deployer,
            "_wait_for_hivecluster",
            return_value=WaitResult(
                status=WaitStatus.READY, message="ok", elapsed_seconds=0, attempts=1
            ),
        ),
        patch.object(deployer, "_get_hive_pod_name", return_value="hive-0"),
    ):
        result = deployer._deploy_stackable(cfg.get_namespace(), time.time())

    assert result.status == DeploymentStatus.SUCCESS
    applied = [c.args[0] for c in engine.k8s.apply_manifest.call_args_list]
    clusters = [d for d in applied if d.get("kind") == "HiveCluster"]
    assert len(clusters) == 1
    rendered = clusters[0]["spec"]["image"]["productVersion"]
    assert rendered == STACKABLE_HIVE_VERSION
    # The deploy result names what was applied, not images.hive.
    assert result.detail == rendered
    assert "4.0.1" not in result.message


def test_run_provenance_records_the_rendered_hive_not_images_hive():
    from lakebench.metrics.experiment import _stackable_hive

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        cfg = make_config(images={"hive": "apache/hive:4.0.1"})
    recorded = _stackable_hive(cfg)
    assert f"hive:{STACKABLE_HIVE_VERSION}-stackable" in recorded
    assert "4.0.1" not in recorded


@pytest.mark.parametrize("value", ["apache/hive:4.0.1", "4.0.1", "apache/hive@sha256:abc"])
def test_images_hive_naming_another_version_warns_it_has_no_effect(value):
    with pytest.warns(DeprecationWarning) as rec:
        make_config(images={"hive": value})
    hits = _hive_warnings(rec)
    assert hits, [str(w.message) for w in rec]
    assert "has no effect" in hits[0] and STACKABLE_HIVE_VERSION in hits[0]


@pytest.mark.parametrize(
    "value",
    [
        "apache/hive:3.1.3",
        "3.1.3",
        "my.registry:5000/hive:3.1.3",
        "oci.stackable.tech/sdp/hive:3.1.3-stackable25.7.0",
    ],
)
def test_images_hive_naming_3_1_3_does_not_warn(value):
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        make_config(images={"hive": value})
    assert not _hive_warnings(rec)


# -- LB-190: secret_ref and fields read by nothing ----------------------------


def _s3(**extra):
    return {"storage": {"s3": {"endpoint": "http://minio:9000", **extra}}}


def test_secret_ref_without_inline_keys_is_refused_at_load():
    with pytest.raises(ValidationError, match="secret_ref .* is not supported"):
        LakebenchConfig(name="t", platform=_s3(secret_ref="my-secret"))


def test_secret_ref_only_config_still_loads_for_teardown():
    # destroy/status/clean load with allow_long_names; an old secret_ref-only
    # deployment must stay destroyable through lakebench destroy.
    data = {"name": "t", "platform": _s3(secret_ref="my-secret")}
    with pytest.raises(ValidationError):
        LakebenchConfig.model_validate(data)
    with pytest.warns(DeprecationWarning, match="secret_ref"):
        cfg = LakebenchConfig.model_validate(data, context={"allow_long_names": True})
    assert cfg.has_s3_secret_ref()


def test_secret_ref_with_inline_keys_loads_and_warns_no_effect():
    with pytest.warns(DeprecationWarning) as rec:
        cfg = LakebenchConfig(
            name="t", platform=_s3(access_key="a", secret_key="b", secret_ref="my-secret")
        )
    hits = [str(w.message) for w in rec if "'secret_ref' (S3Config)" in str(w.message)]
    assert hits and "has no effect" in hits[0]
    assert cfg.has_inline_s3_credentials()


def test_config_without_any_credentials_still_loads():
    # Missing keys stay a deploy-preflight failure so info/validate can load.
    cfg = LakebenchConfig(name="t", platform=_s3())
    assert not cfg.has_inline_s3_credentials()


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        (
            {"architecture": {"catalog": {"polaris": {"version": "1.5.0"}}}},
            "'version' (PolarisConfig)",
        ),
        ({"observability": {"storage_class": "fast"}}, "'storage_class' (ObservabilityConfig)"),
        (
            {"architecture": {"catalog": {"hive": {"thrift": {"min_threads": 20}}}}},
            "'min_threads' (HiveThriftConfig)",
        ),
        (
            {"architecture": {"catalog": {"hive": {"thrift": {"max_threads": 99}}}}},
            "'max_threads' (HiveThriftConfig)",
        ),
        (
            {"architecture": {"catalog": {"hive": {"thrift": {"client_timeout": "60s"}}}}},
            "'client_timeout' (HiveThriftConfig)",
        ),
    ],
)
def test_unread_field_set_warns_it_has_no_effect(overrides, field):
    with pytest.warns(DeprecationWarning) as rec:
        make_config(**overrides)
    hits = [str(w.message) for w in rec if field in str(w.message)]
    assert hits, [str(w.message) for w in rec]
    assert "has no effect" in hits[0]


@pytest.mark.parametrize(
    ("datagen", "key"),
    [({"uploaders": 2}, "uploaders"), ({"checkpoint": {"enabled": False}}, "checkpoint")],
)
def test_removed_datagen_keys_warn_they_are_ignored(datagen, key):
    # Superseded on integrate (2026-09-28): uploaders and checkpoint were
    # removed from DatagenConfig, so they load, are dropped and warn that
    # they are ignored, instead of warning as dead fields.
    with pytest.warns(DeprecationWarning) as rec:
        cfg = make_config(workload={"datagen": datagen})
    hits = [str(w.message) for w in rec if f"'{key}' (DatagenConfig)" in str(w.message)]
    assert hits and "is ignored" in hits[0]
    assert not hasattr(cfg.workload.datagen, key)


def test_unread_fields_at_their_defaults_do_not_warn(tmp_path):
    # A saved config carries every field at its default and must reload quietly.
    from lakebench.config.loader import load_config, save_config

    cfg = make_config()
    path = tmp_path / "saved.yaml"
    save_config(cfg, path)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        load_config(path)
    msgs = [str(w.message) for w in rec if "has no effect" in str(w.message)]
    assert not msgs, msgs


def test_generated_config_template_carries_no_unread_keys():
    from lakebench.config.loader import generate_example_config_yaml

    text = generate_example_config_yaml()
    for key in ("secret_ref", "uploaders:", "checkpoint:", "min_threads", "max_threads"):
        assert key not in text, key
    # The dead polaris.version and observability.storage_class lines.
    assert "Min 1.3.0 for FlashBlade/MinIO" not in text
    assert "PVC storage class (empty = default)" not in text


# -- LB-191: runtime strings say what the code does ---------------------------


def test_deploy_step_labels_say_verify_and_check():
    engine = _engine(make_config())
    labels: dict[str, str] = {}

    def _cb(component, status, message):
        if status == DeploymentStatus.IN_PROGRESS:
            labels[component] = message

    with patch.object(DeploymentEngine, "_deploy_namespace", return_value=MagicMock()):
        engine.deploy_all(progress_callback=_cb, timeout=0)
    assert labels["scratch-sc"] == "Verifying scratch StorageClass"
    assert labels["spark-operator"] == "Checking Spark Operator and watch list"
    assert not any(v.startswith(("Creating scratch", "Installing Spark")) for v in labels.values())


@pytest.mark.parametrize(
    ("install", "expected"),
    [
        (False, "Spark Operator watch list"),
        (True, "Spark Operator (installed if missing, namespace watched)"),
    ],
)
def test_deploy_summary_lists_the_operator_step_either_way(install, expected):
    from lakebench.cli._deploy import _build_component_list

    cfg = make_config()
    cfg.platform.compute.spark.operator.install = install
    assert expected in _build_component_list(cfg)


def test_generate_has_no_resume_flag(tmp_path):
    # Superseded on integrate: generate --resume was removed (the Rust
    # generator has no checkpoint resume), so the flag is refused outright
    # instead of warning that it has no effect.
    from typer.testing import CliRunner

    from lakebench.cli import app
    from lakebench.config.loader import save_config

    cfg_path = tmp_path / "c.yaml"
    save_config(make_config(name="gen-resume"), cfg_path)
    with patch("lakebench.deploy.DatagenDeployer") as deployer:
        res = CliRunner().invoke(app, ["generate", str(cfg_path), "--resume", "--yes"])
    assert res.exit_code == 2
    assert "No such option" in res.output
    deployer.assert_not_called()


def test_datagen_mode_docstring_does_not_claim_fixed_sizing():
    from lakebench.config.schema import DatagenMode

    doc = DatagenMode.__doc__ or ""
    assert "hard-locked" not in doc
    assert "24Gi" not in doc


def test_autosizer_and_job_comments_do_not_cite_150gi_silver_scratch():
    for rel in (
        "src/lakebench/config/autosizer.py",
        "src/lakebench/modules/pipeline_engines/spark/job.py",
    ):
        text = (ROOT / rel).read_text()
        for line in text.splitlines():
            if "150Gi" in line:
                # Only the history note on the 300Gi profile may name 150Gi.
                assert "was 150Gi" in line or "exceeds 150Gi" in line, (rel, line)


def test_legacy_reproduction_package_is_marked_and_refused():
    from lakebench.cli._reproduce import _load_package, _policy_refusal
    from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID

    path = ROOT / "docs/reproductions/c360-scale-0-1.yaml"
    text = path.read_text()
    assert text.startswith("# LEGACY PACKAGE")
    meta = _load_package(path)["reproduction_metadata"]
    # verify refuses it on both grounds the marker names.
    assert not meta.get("experiment_identity")
    assert _policy_refusal(meta, MAINTENANCE_POLICY_ID) is not None
    readme = (ROOT / "docs/reproductions/README.md").read_text()
    assert "Legacy, verify refuses it" in readme
    assert yaml.safe_load(text)["schema_version"] == 1
