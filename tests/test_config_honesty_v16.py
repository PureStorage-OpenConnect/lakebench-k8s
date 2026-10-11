"""Evidence and config honesty fixes for v1.6.

Each test fails if its fix is reverted: the recorded Hive version is the one
the template renders, a config field nothing reads says so at load, and the
runtime strings describe what the code does.
"""

from __future__ import annotations

import copy
import time
import warnings
from unittest.mock import MagicMock, patch

import pytest
import yaml

from lakebench.config import LakebenchConfig
from lakebench.deploy.engine import DeploymentEngine, DeploymentStatus
from lakebench.k8s import WaitResult, WaitStatus
from tests.conftest import make_config


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


# -- The recorded Hive is the Hive the template deploys ---------------


def test_recorded_hive_is_the_rendered_hive_not_images_hive():
    from lakebench.metrics.experiment import _stackable_hive
    from lakebench.modules.catalogs.hive.deployer import HiveDeployer

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        cfg = make_config(images={"hive": "apache/hive:4.0.1"})
    engine = _engine(cfg)

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
    assert rendered != "4.0.1"
    # The deploy result and the run provenance name what was applied.
    assert result.detail == rendered
    assert "4.0.1" not in result.message
    recorded = _stackable_hive(cfg)
    assert f"hive:{rendered}-stackable" in recorded
    assert "4.0.1" not in recorded


def _load(tmp_path, data: dict, purpose):
    from lakebench.config import load_config

    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(data))
    return load_config(path, purpose=purpose, print_notes=False)


# -- secret_ref and fields read by nothing ----------------------------


def _s3(**extra):
    return {"storage": {"s3": {"endpoint": "http://minio:9000", **extra}}}


def _merge(base: dict, extra: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in extra.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


_KEYS = {"access_key": "a", "secret_key": "b"}
_BASE = {"name": "t", "platform": _s3(**_KEYS)}


def _with(overrides: dict) -> dict:
    return _merge(_BASE, overrides)


# A field nothing read is removed: the commands that change data refuse it and
# name the key; destroy still loads the same config.
_REMOVED_FIELDS = [
    pytest.param(_with({"images": {"hive": "apache/hive:4.0.1"}}), "hive", id="images-hive-tag"),
    pytest.param(_with({"images": {"hive": "4.0.1"}}), "hive", id="images-hive-bare-version"),
    pytest.param(
        _with({"images": {"hive": "apache/hive@sha256:abc"}}), "hive", id="images-hive-digest"
    ),
    pytest.param(
        _with({"architecture": {"catalog": {"polaris": {"version": "1.5.0"}}}}),
        "version",
        id="polaris",
    ),
    pytest.param(
        _with({"observability": {"storage_class": "fast"}}), "storage_class", id="obs-storage-class"
    ),
    pytest.param(
        _with({"architecture": {"catalog": {"hive": {"thrift": {"min_threads": 20}}}}}),
        "thrift",
        id="thrift-min",
    ),
    pytest.param(
        _with({"architecture": {"catalog": {"hive": {"thrift": {"max_threads": 99}}}}}),
        "thrift",
        id="thrift-max",
    ),
    pytest.param(
        _with({"architecture": {"catalog": {"hive": {"thrift": {"client_timeout": "60s"}}}}}),
        "thrift",
        id="thrift-timeout",
    ),
    pytest.param(
        _with({"platform": {"compute": {"spark": {"executor": {"instances": 16}}}}}),
        "executor",
        id="spark-executor",
    ),
    pytest.param(
        _with({"platform": {"compute": {"spark": {"driver": {"memory": "16g"}}}}}),
        "driver",
        id="spark-driver",
    ),
    pytest.param(
        _with({"platform": {"storage": {"scratch": {"size": "200Gi"}}}}), "size", id="scratch-size"
    ),
    pytest.param(
        _with({"platform": _s3(secret_ref="my-secret")}), "secret_ref", id="secret-ref-with-keys"
    ),
    pytest.param(
        {"name": "t", "platform": _s3(secret_ref="my-secret")},
        "secret_ref",
        id="secret-ref-no-keys",
    ),
]


@pytest.mark.parametrize("data,key", _REMOVED_FIELDS)
def test_removed_field_is_refused_for_mutation_and_loads_for_teardown(tmp_path, data, key):
    from lakebench.config import LoadPurpose
    from lakebench.config.loader import ConfigValidationError

    with pytest.raises(ConfigValidationError) as e:
        _load(tmp_path, data, LoadPurpose.MUTATE)
    assert f"'{key}'" in str(e.value)
    assert _load(tmp_path, data, LoadPurpose.TEARDOWN).name == "t"


def test_secret_ref_only_config_still_loads_for_teardown(tmp_path):
    from lakebench.config import LoadPurpose
    from lakebench.config.loader import load_notes

    data = {"name": "t", "platform": _s3(secret_ref="my-secret")}
    cfg = _load(tmp_path, data, LoadPurpose.TEARDOWN)
    assert not hasattr(cfg.platform.storage.s3, "secret_ref")
    assert any("'secret_ref' (S3Config)" in t for t in load_notes(cfg).texts())
