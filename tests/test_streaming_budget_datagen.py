"""LB-158: the continuous streaming budget reserved the datagen fleet's
cores (352 at c360 s100) for the whole run, although the finite corpus is
written in about two minutes before the streams start. That capped c360
s100 streams from 5/11/5 executors to 2/6/2. Once the datagen Job has
finished its cores are not reserved.
"""

from __future__ import annotations

import pytest

from lakebench.modules.pipeline_engines.spark.job import (
    _streaming_concurrent_budget,
    get_executor_count,
)
from lakebench.spark.job import JobType, SparkJobManager
from tests.test_aml_continuous_sizing import _capacity_k8s, _config

_STAGES = (JobType.BRONZE_INGEST, JobType.SILVER_STREAM, JobType.GOLD_REFRESH)


def _c360_s100():
    cfg = _config("customer360", 100)
    cfg.architecture.workload.datagen.parallelism = 44
    cfg.architecture.workload.datagen.cpu = "8"
    return cfg


def test_finished_datagen_releases_its_cores_to_the_streams():
    cfg = _c360_s100()
    held = _streaming_concurrent_budget(cfg, 434_000)
    freed = _streaming_concurrent_budget(cfg, 434_000, datagen_running=False)
    for jt in _STAGES:
        assert freed[jt] == get_executor_count(jt.value, 100, "customer360")
    assert sum(freed.values()) > sum(held.values())


def test_default_keeps_reserving_datagen():
    """Callers that cannot know (the pre-run preflight) stay conservative."""
    cfg = _c360_s100()
    assert _streaming_concurrent_budget(cfg, 434_000) == _streaming_concurrent_budget(
        cfg, 434_000, datagen_running=True
    )


@pytest.mark.parametrize("schema", ["customer360", "financial"])
def test_financial_override_path_also_releases(schema):
    cfg = _config(schema, 100)
    cfg.architecture.workload.datagen.parallelism = 44
    cfg.architecture.workload.datagen.cpu = "8"
    held = _streaming_concurrent_budget(cfg, 434_000)
    freed = _streaming_concurrent_budget(cfg, 434_000, datagen_running=False)
    assert all(freed[jt] >= held[jt] for jt in _STAGES)
    assert sum(freed.values()) > sum(held.values())


def test_manager_uses_the_datagen_flag():
    mgr = SparkJobManager(_c360_s100(), _capacity_k8s(434))
    assert mgr.datagen_running is True
    held = mgr._build_manifest(JobType.SILVER_STREAM)["spec"]["executor"]["instances"]
    mgr.budget_warnings.clear()
    mgr.datagen_running = False
    freed = mgr._build_manifest(JobType.SILVER_STREAM)["spec"]["executor"]["instances"]
    assert freed == get_executor_count("silver-stream", 100, "customer360") > held
    assert mgr.budget_warnings == []
