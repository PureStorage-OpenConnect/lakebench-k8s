"""Evidence and config honesty fixes for v1.6 (LB-189, LB-190, LB-191).

Each test fails if its fix is reverted: the recorded Hive version is the one
the template renders, a config field nothing reads says so at load, and the
runtime strings describe what the code does.
"""

from __future__ import annotations

import copy
import time
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

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


# -- LB-189: the recorded Hive is the Hive the template deploys ---------------


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


def _load(tmp_path, data: dict, purpose):
    from lakebench.config import load_config

    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(data))
    return load_config(path, purpose=purpose, print_notes=False)


# Superseded by CFG-9 (CC-17): images.hive never selected the Hive that runs.
# A value naming another Hive is refused by the commands that change data
# with fix text; one naming 3.1.3 changed nothing and loads with a note.
def test_images_hive_naming_another_version_is_refused(tmp_path):
    for value in ["apache/hive:4.0.1", "4.0.1", "apache/hive@sha256:abc"]:
        from lakebench.config import LoadPurpose
        from lakebench.config.loader import ConfigValidationError, load_notes

        data = {
            "name": "t",
            "images": {"hive": value},
            "platform": _s3(access_key="a", secret_key="b"),
        }
        with pytest.raises(ConfigValidationError) as e:
            _load(tmp_path, data, LoadPurpose.MUTATE)
        assert "'hive' was removed" in str(e.value) and STACKABLE_HIVE_VERSION in str(e.value)
        cfg = _load(tmp_path, data, LoadPurpose.TEARDOWN)
        assert any("'hive' (ImagesConfig)" in t for t in load_notes(cfg).texts())


# -- LB-190: secret_ref and fields read by nothing ----------------------------


def _s3(**extra):
    return {"storage": {"s3": {"endpoint": "http://minio:9000", **extra}}}


# Superseded by CFG-9 (CC-17): secret_ref is removed. Refused with fix text by
# the commands that change data, with or without inline keys; destroy still
# loads an old secret_ref-only deployment.
@pytest.mark.parametrize("keys", [{}, {"access_key": "a", "secret_key": "b"}])
def test_secret_ref_is_refused_with_fix_text(tmp_path, keys):
    from lakebench.config import LoadPurpose
    from lakebench.config.loader import ConfigValidationError

    data = {"name": "t", "platform": _s3(secret_ref="my-secret", **keys)}
    with pytest.raises(ConfigValidationError) as e:
        _load(tmp_path, data, LoadPurpose.MUTATE)
    assert "'secret_ref' was removed" in str(e.value)
    assert "access_key and secret_key" in str(e.value)


def test_secret_ref_only_config_still_loads_for_teardown(tmp_path):
    from lakebench.config import LoadPurpose
    from lakebench.config.loader import load_notes

    data = {"name": "t", "platform": _s3(secret_ref="my-secret")}
    cfg = _load(tmp_path, data, LoadPurpose.TEARDOWN)
    assert not hasattr(cfg.platform.storage.s3, "secret_ref")
    assert any("'secret_ref' (S3Config)" in t for t in load_notes(cfg).texts())


# Superseded by CFG-9 (CC-17): the fields nothing read are removed. A
# non-default value is refused with fix text by the commands that change
# data and dropped with a note by teardown.
def test_unread_field_set_is_refused_with_fix_text(tmp_path):
    for overrides, key, fix in [
        (
            {"architecture": {"catalog": {"polaris": {"version": "1.5.0"}}}},
            "'version' was removed",
            "tag of images.polaris",
        ),
        (
            {"observability": {"storage_class": "fast"}},
            "'storage_class' was removed",
            "cluster default StorageClass",
        ),
        (
            {"architecture": {"catalog": {"hive": {"thrift": {"min_threads": 20}}}}},
            "'thrift' was removed",
            "min.threads 10",
        ),
        (
            {"architecture": {"catalog": {"hive": {"thrift": {"max_threads": 99}}}}},
            "'thrift' was removed",
            "max.threads 50",
        ),
        (
            {"architecture": {"catalog": {"hive": {"thrift": {"client_timeout": "60s"}}}}},
            "'thrift' was removed",
            "socket.timeout 300s",
        ),
    ]:
        from lakebench.config import LoadPurpose
        from lakebench.config.loader import ConfigValidationError

        data = {"name": "t", "platform": _s3(access_key="a", secret_key="b"), **overrides}
        with pytest.raises(ConfigValidationError) as e:
            _load(tmp_path, data, LoadPurpose.MUTATE)
        assert key in str(e.value) and fix in str(e.value)
        assert _load(tmp_path, data, LoadPurpose.TEARDOWN).name == "t"


# The unread Spark sizing blocks and scratch.size were superseded by CFG-1
# (CC-11): a command that changes data refuses them with fix text, and the
# read and teardown commands drop them with a note.
def test_unread_spark_sizing_set_is_refused_with_fix_text(tmp_path):
    for overrides, key, fix in [
        (
            {"platform": {"compute": {"spark": {"executor": {"instances": 16}}}}},
            "'executor' was removed",
            "per-executor sizing is fixed in the job profiles",
        ),
        (
            {"platform": {"compute": {"spark": {"driver": {"memory": "16g"}}}}},
            "'driver' was removed",
            "driver_memory/driver_cores for the driver",
        ),
        (
            {"platform": {"storage": {"scratch": {"size": "200Gi"}}}},
            "'size' was removed",
            "per-job scratch comes from the job profiles",
        ),
    ]:
        from lakebench.config import LoadPurpose, load_config
        from lakebench.config.loader import ConfigValidationError

        platform = copy.deepcopy(overrides["platform"])
        platform.setdefault("storage", {})["s3"] = _s3(access_key="a", secret_key="b")["storage"][
            "s3"
        ]
        path = tmp_path / "c.yaml"
        path.write_text(yaml.safe_dump({"name": "t", "platform": platform}))
        with pytest.raises(ConfigValidationError) as e:
            load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)
        assert key in str(e.value) and fix in str(e.value)
        # destroy still loads it.
        assert load_config(path, purpose=LoadPurpose.TEARDOWN, print_notes=False).name == "t"


# -- LB-191: runtime strings say what the code does ---------------------------


def test_legacy_reproduction_package_is_marked_and_refused():
    from lakebench.cli._reproduce import _load_package, _policy_refusal
    from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID

    path = ROOT / "docs/reproductions/c360-scale-0-1.yaml"
    meta = _load_package(path)["reproduction_metadata"]
    # verify refuses it on both grounds the marker names.
    assert not meta.get("experiment_identity")
    assert _policy_refusal(meta, MAINTENANCE_POLICY_ID) is not None
