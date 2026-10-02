"""The capacity check and the plan count the driver the manifest builds
(LB-244): a platform.compute.spark driver override and the Spark 3 driver
size reach compute_peak_requirements(config=...)."""

from __future__ import annotations

import itertools
from unittest import mock

import pytest

from lakebench.config import LakebenchConfig
from lakebench.k8s.client import ClusterCapacity
from lakebench.modules.pipeline_engines.spark.job import (
    BATCH_JOB_TYPES,
    STREAMING_JOB_TYPES,
    _driver_pod_bytes,
    compute_peak_requirements,
    effective_driver,
    get_job_profile,
)
from lakebench.spark.job import JobType, SparkJobManager

GIB = 1024**3


def _config(schema="customer360", mode="batch", spark_image=None, **spark):
    return LakebenchConfig(
        name="t",
        **({"images": {"spark": spark_image}} if spark_image else {}),
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "a",
                    "secret_key": "b",
                    "buckets": {"bronze": "b", "silver": "s", "gold": "g"},
                }
            },
            **({"compute": {"spark": spark}} if spark else {}),
        },
        architecture={
            "workload": {"schema": schema, "datagen": {"scale": 1}},
            "pipeline": {"mode": mode},
        },
    )


def _capacity_k8s():
    k8s = mock.MagicMock()
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        434_000, 4349 * GIB, 8, 40_000, 402 * GIB
    )
    return k8s


_IMAGES = ("apache/spark:4.0.2-python3", "apache/spark:3.5.4-python3")
_OVERRIDES = ({}, {"driver_memory": "64g"}, {"driver_cores": 8, "driver_memory": "16g"})


@pytest.mark.parametrize(
    ("image", "overrides", "schema"),
    list(itertools.product(_IMAGES, _OVERRIDES, ("customer360", "financial"))),
)
def test_peak_drivers_are_the_manifest_drivers(image, overrides, schema):
    """For every batch and continuous job, the driver the peak counts is the
    one the SparkApplication requests."""
    for mode, job_types in (("batch", BATCH_JOB_TYPES), ("sustained", STREAMING_JOB_TYPES)):
        cfg = _config(schema, mode, image, **overrides)
        peak = compute_peak_requirements(1, mode, schema, config=cfg)
        mgr = SparkJobManager(cfg, _capacity_k8s())
        for req in peak.per_job:
            assert req.job_type in job_types
            drv = mgr._build_manifest(JobType(req.job_type))["spec"]["driver"]
            profile = get_job_profile(req.job_type, schema)
            assert effective_driver(req.job_type, profile, cfg) == (drv["cores"], drv["memory"])
            exec_bytes = (
                int(profile["executor_memory"].rstrip("g"))
                + int(profile["executor_memory_overhead"].rstrip("g"))
            ) * GIB
            want = req.executors * exec_bytes + _driver_pod_bytes(drv["memory"])
            assert req.memory_gb == -(-want // GIB)
            assert req.cpu_cores == req.executors * profile["executor_cores"] + drv["cores"]


def test_default_config_changes_nothing():
    for mode in ("batch", "sustained"):
        for schema in ("customer360", "financial"):
            cfg = _config(schema, mode)
            assert compute_peak_requirements(1, mode, schema, config=cfg) == (
                compute_peak_requirements(1, mode, schema)
            )


def test_driver_override_raises_the_batch_peak():
    # silver-build: 8 x 60 GiB executors + a 64g driver (89.6 GiB pod).
    peak = compute_peak_requirements(1, "batch", config=_config(driver_memory="64g"))
    assert peak.memory_gb == 570
    assert compute_peak_requirements(1, "batch").memory_gb == 525


def test_spark3_driver_is_24g():
    cfg = _config(spark_image="apache/spark:3.5.4-python3")
    sb = next(
        r
        for r in compute_peak_requirements(1, "batch", config=cfg).per_job
        if r.job_type == "silver-build"
    )
    # 8 x 60 GiB + 24 GiB heap + 9.6 GiB overhead
    assert sb.memory_gb == 514


def test_preflight_counts_the_driver_override():
    from lakebench.cli._prerequisites import _check_cluster_capacity

    cfg = _config(driver_memory="64g")
    k8s = mock.MagicMock()
    # Enough for the default peak (525 + co-resident), not for 570.
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        200_000, 560 * GIB, 8, 64_000, 256 * GIB
    )
    with mock.patch("lakebench.k8s.get_k8s_client", return_value=k8s):
        result = _check_cluster_capacity(cfg)
    assert not result.passed, result.message
    assert "570 GB pipeline" in (result.hint or "") + result.message


def test_plan_counts_the_driver_override():
    # config show, info and recommend read the one sizing plan.
    from lakebench.config.sizing import plan_requirements

    plan = plan_requirements(_config(driver_memory="64g"))
    assert plan.spark.memory_gb == 570


@pytest.mark.parametrize("value", ["16Gi", "1.5g", "16 g", "lots", "16384", "0g", "\u0661\u0666g"])
def test_unreadable_driver_memory_is_refused_for_run(value):
    # A model built without a load purpose refuses, as run and deploy do.
    with pytest.raises(ValueError, match="not a Spark memory size"):
        _config(driver_memory=value)


@pytest.mark.parametrize("value", ["16g", "16G", "16384m", "16gb", "16GB", "1t"])
def test_spark_sizes_load_and_count(value):
    cfg = _config(driver_memory=value)
    peak = compute_peak_requirements(1, "batch", config=cfg)
    assert peak.memory_gb >= 480 + 16


def test_unreadable_driver_memory_drops_with_a_note_for_teardown(tmp_path):
    import yaml

    from lakebench.config._load_context import LoadPurpose
    from lakebench.config.loader import load_config

    raw = _config().model_dump(mode="json", by_alias=True, exclude_none=True)
    raw["platform"]["compute"]["spark"]["driver_memory"] = "16Gi"
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(raw))
    cfg = load_config(path, purpose=LoadPurpose.TEARDOWN, print_notes=False)
    assert cfg.platform.compute.spark.driver_memory is None
    with pytest.raises(Exception, match="not a Spark memory size"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
