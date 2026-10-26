"""The executor scratch keys size the PVC each executor gets, and the run
records the size it requested."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from lakebench.config import LakebenchConfig
from lakebench.config._load_context import LoadPurpose
from lakebench.config.autosizer import resolve_auto_sizing, scratch_will_be_enabled
from lakebench.config.loader import load_config
from lakebench.metrics.fingerprint_inputs import scratch_size_per_job
from lakebench.modules.pipeline_engines.spark.job import (
    compute_peak_requirements,
    scratch_override,
)
from lakebench.spark.job import JobType, SparkJobManager

_SIZE_KEY = (
    "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit"
)


def _config(spark: dict, *, scratch: dict | None = None, mode: str = "batch") -> LakebenchConfig:
    return LakebenchConfig(
        name="t",
        platform={
            "storage": {
                "s3": {"endpoint": "http://minio:9000", "access_key": "a", "secret_key": "b"},
                "scratch": {"enabled": True} if scratch is None else scratch,
            },
            "compute": {"spark": spark},
        },
        architecture={
            "workload": {"schema": "financial", "datagen": {"scale": 20}},
            "pipeline": {"mode": mode},
            "catalog": {"type": "polaris", "polaris": {"client_secret": "test-only-secret"}},
        },
    )


def _pvc(cfg: LakebenchConfig, job: JobType) -> str:
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    return SparkJobManager(cfg, k8s)._build_manifest(job)["spec"]["sparkConf"][_SIZE_KEY]


def test_executor_scratch_sizes_the_time_travel_job():
    """The time-travel job's profile gives 50Gi; executor_scratch is the one
    key that reaches a job without a stage key of its own."""
    assert _pvc(_config({}), JobType.TIME_TRAVEL_FINANCIAL) == "50Gi"
    assert _pvc(_config({"executor_scratch": "200Gi"}), JobType.TIME_TRAVEL_FINANCIAL) == "200Gi"


def test_a_stage_key_wins_for_its_stage_only():
    cfg = _config({"executor_scratch": "200Gi", "silver_executor_scratch": "400Gi"})
    assert _pvc(cfg, JobType.SILVER_BUILD) == "400Gi"
    assert _pvc(cfg, JobType.GOLD_FINALIZE) == "200Gi"
    assert _pvc(cfg, JobType.TIME_TRAVEL_FINANCIAL) == "200Gi"


def test_the_size_is_exact_not_scaled_by_cores():
    cfg = _config(
        {"silver_executor_scratch": "120Gi", "silver_executor_cores": 16}, mode="continuous"
    )
    assert _pvc(cfg, JobType.SILVER_STREAM) == "120Gi"


def test_a_continuous_runs_bronze_verify_preflight_takes_only_the_global_key():
    cfg = _config(
        {"executor_scratch": "200Gi", "bronze_executor_scratch": "80Gi"}, mode="continuous"
    )
    assert scratch_override("bronze-ingest", cfg) == "80Gi"
    assert scratch_override("bronze-verify", cfg) == "200Gi"


def test_the_capacity_plan_counts_the_set_size():
    cfg = _config({"silver_executor_scratch": "400Gi"})
    peak = compute_peak_requirements(20, "batch", schema_type="financial", config=cfg)
    sb = next(r for r in peak.per_job if r.job_type == "silver-build")
    assert sb.scratch_gb == sb.executors * 400


def test_the_run_record_holds_the_set_size():
    cfg = _config({"executor_scratch": "200Gi", "silver_executor_scratch": "400Gi"})
    sizes = scratch_size_per_job(cfg, continuous=False)
    assert sizes["silver-build"] == "400Gi"
    assert sizes["bronze-verify"] == sizes["gold-finalize"] == "200Gi"
    assert scratch_size_per_job(_config({}), continuous=False)["silver-build"] != "400Gi"


def _write(tmp_path: Path, spark: dict, scratch: dict | None = None) -> Path:
    raw = _config({}).model_dump(mode="json", by_alias=True, exclude_none=True)
    raw["platform"]["compute"]["spark"].update(spark)
    if scratch is not None:
        raw["platform"]["storage"]["scratch"] = scratch
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


@pytest.mark.parametrize("value", ["200G", "200g", "0Gi", "1.5Gi", "200Mi"])
def test_a_size_not_in_whole_gi_is_refused_and_dropped_for_teardown(tmp_path, value):
    path = _write(tmp_path, {"executor_scratch": value})
    with pytest.raises(Exception, match="not a scratch size"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    cfg = load_config(path, purpose=LoadPurpose.TEARDOWN, print_notes=False)
    assert cfg.platform.compute.spark.executor_scratch is None


def test_a_size_with_scratch_turned_off_is_refused(tmp_path):
    path = _write(tmp_path, {"gold_executor_scratch": "200Gi"}, scratch={"enabled": False})
    with pytest.raises(Exception, match="scratch.enabled is false"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    assert load_config(path, purpose=LoadPurpose.TEARDOWN, print_notes=False)


def test_a_size_turns_scratch_on_when_it_is_unset():
    cfg = _config({"executor_scratch": "200Gi"}, scratch={}, mode="continuous")
    assert scratch_will_be_enabled(cfg)
    resolve_auto_sizing(cfg)
    assert cfg.platform.storage.scratch.enabled
    plain = _config({}, scratch={}, mode="continuous")
    assert not scratch_will_be_enabled(plain)
