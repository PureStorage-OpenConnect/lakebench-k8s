"""CFG-2 (CC-12): executor overrides are bounded, counted, labelled and kept
out of evidence. Design 02 section 2.2."""

from __future__ import annotations

import copy
import warnings
from pathlib import Path

import pytest
import yaml

from lakebench.config._load_context import LoadPurpose
from lakebench.config.loader import load_config
from lakebench.config.schema import MAX_DRIVER_CORES, MAX_EXECUTOR_OVERRIDE
from lakebench.metrics import comparability as cmp
from lakebench.metrics.collector import JobMetrics
from lakebench.modules.pipeline_engines.spark import job as job_mod
from lakebench.modules.pipeline_engines.spark.job import (
    EXECUTOR_OVERRIDE_FIELDS,
    MissingSizingProfile,
    compute_peak_requirements,
    executor_count,
    executor_override,
)
from lakebench.spark.job import JobType
from tests.conftest import make_config
from tests.fixtures import stored_records as sr
from tests.test_experiment import _cfg, _metrics

# -- 1. bounds -----------------------------------------------------------------


def test_the_bound_is_the_proven_ceiling():
    assert MAX_EXECUTOR_OVERRIDE == job_mod._MAX_EXECUTORS_SAFE == 28
    assert MAX_DRIVER_CORES == 16


@pytest.mark.parametrize("value", [29, 200])
@pytest.mark.parametrize("field", [f for f, _ in EXECUTOR_OVERRIDE_FIELDS.values()])
def test_override_above_28_refused(field, value):
    with pytest.raises(ValueError, match="proven ceiling of 28"):
        make_config(platform={"compute": {"spark": {field: value}}})


def test_override_28_loads_and_17_driver_cores_refused():
    assert make_config(platform={"compute": {"spark": {"silver_executors": 28}}})
    with pytest.raises(ValueError, match="proven ceiling of 16"):
        make_config(platform={"compute": {"spark": {"driver_cores": 17}}})


@pytest.mark.parametrize("value", [0, -3])
def test_override_below_1_refused(value):
    with pytest.raises(ValueError):
        make_config(platform={"compute": {"spark": {"silver_executors": value}}})


def _write(tmp_path: Path, spark: dict) -> Path:
    raw = make_config().model_dump(mode="json", by_alias=True, exclude_none=True)
    raw["platform"]["compute"]["spark"].update(spark)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


@pytest.mark.parametrize("purpose", [LoadPurpose.TEARDOWN, LoadPurpose.READ])
def test_a_v16_config_with_40_can_still_be_destroyed(tmp_path, purpose):
    path = _write(tmp_path, {"silver_executors": 40})
    cfg = load_config(path, purpose=purpose, print_notes=False)
    assert cfg.platform.compute.spark.silver_executors is None
    with pytest.raises(Exception, match="proven ceiling of 28"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)


# -- 2. the peak counts overrides ------------------------------------------------


def test_override_20_raises_peak():
    cfg = make_config(platform={"compute": {"spark": {"silver_executors": 20}}})
    peak = compute_peak_requirements(1, "batch", config=cfg)
    sb = next(r for r in peak.per_job if r.job_type == "silver-build")
    assert sb.executors == 20
    assert sb.cpu_cores == 20 * 4 + 4 == 84
    # 20 x 60 GiB executors + the 32g driver pod (32 + 0.4 x 32 GiB), rounded up
    assert sb.memory_gb == 1245
    assert sb.scratch_gb == 20 * 300
    assert compute_peak_requirements(1, "batch").memory_gb == 525


def test_a_continuous_override_is_counted_in_the_capacity_plan():
    from lakebench.config.sizing import plan_requirements

    cfg = make_config(
        architecture={"pipeline": {"mode": "continuous"}},
        platform={"compute": {"spark": {"gold_refresh_executors": 10}}},
    )
    base = plan_requirements(make_config(architecture={"pipeline": {"mode": "continuous"}}))
    plan = plan_requirements(cfg)
    gr = next(r for r in plan.spark.per_job if r.job_type == "gold-refresh")
    assert gr.executors == 10
    assert plan.spark.cpu_cores > base.spark.cpu_cores


def test_one_override_rule():
    """The manifest, the peak and the run record read one table."""
    cfg = make_config(platform={"compute": {"spark": {"gold_executors": 6}}})
    assert executor_override("gold-finalize", cfg) == 6
    assert executor_override("silver-build", cfg) is None
    assert executor_override("score-financial", cfg) is None
    spark = cfg.platform.compute.spark
    for job_type, (field, _) in EXECUTOR_OVERRIDE_FIELDS.items():
        setattr(spark, field, 3)
        assert executor_override(job_type, cfg) == 3, field
        setattr(spark, field, None)


def test_executor_count_is_the_one_count():
    p = job_mod._JOB_PROFILES["gold-finalize"]
    assert executor_count(p, 1) == p["base_executors"]
    assert executor_count(p, 100_000) == p["max_executors"]
    assert executor_count(p, 100_000, capped=False) > p["max_executors"]
    assert job_mod._scale_executor_count(p, 500) == executor_count(p, 500)


# -- 3. a missing profile raises ---------------------------------------------------


def test_every_job_type_has_a_profile():
    assert {jt.value for jt in JobType} <= set(job_mod._JOB_PROFILES)


@pytest.mark.parametrize("schema", ["customer360", "financial"])
@pytest.mark.parametrize(
    "job_type", ["replay-financial", "reproduce-financial", "score-financial-reference"]
)
def test_financial_ops_profiles_equal_silver_build(job_type, schema):
    """The fallback these took is gone; their profiles are literal copies,
    so their manifests are unchanged (the CC-12 golden over 340 manifests
    was byte-identical before and after)."""
    assert job_mod._resolve_job_profile(job_type, schema) == job_mod._resolve_job_profile(
        "silver-build", schema
    )


def test_missing_profile_raises(monkeypatch):
    from lakebench.spark.job import SparkJobManager
    from tests.test_spark import _mock_k8s

    profiles = dict(job_mod._JOB_PROFILES)
    del profiles["replay-financial"]
    monkeypatch.setattr(job_mod, "_JOB_PROFILES", profiles)
    mgr = SparkJobManager(make_config(), _mock_k8s())
    with pytest.raises(MissingSizingProfile, match="replay-financial"):
        mgr._build_manifest(JobType.REPLAY_FINANCIAL)


# -- 4. labelled when it binds -------------------------------------------------------


def _record_with(cfg, job_type="silver-build"):
    run = _metrics(cfg)
    run.jobs.append(JobMetrics(job_name="j", job_type=job_type, success=True))
    return run.to_dict()


def test_binding_override_labelled():
    cfg = _cfg()
    cfg.platform.compute.spark.silver_executors = 4  # the profile asks 8 at scale 1
    e = _record_with(cfg)["experiment"]
    sb = next(x for x in e["limits"]["executors"] if x["job_type"] == "silver-build")
    assert sb["override_bound"] is True
    assert "silver-build: executor override" in e["limits"]["bound_kinds"]
    assert "silver-build: executor override 4 (profile asks 8)" in e["limits"]["bound"]


def test_an_override_above_the_profile_is_not_bound():
    cfg = _cfg()
    cfg.platform.compute.spark.silver_executors = 12
    e = _record_with(cfg)["experiment"]
    sb = next(x for x in e["limits"]["executors"] if x["job_type"] == "silver-build")
    assert "override_bound" not in sb
    assert not any("executor override" in k for k in e["limits"]["bound_kinds"])
    assert e["architecture"]["spark_executor_overrides"] == {"silver": 12}


def test_default_record_unchanged():
    e = _record_with(_cfg())["experiment"]
    assert "spark_executor_overrides" not in e["architecture"]
    assert "spark_driver_overrides" not in e["architecture"]
    assert not any("override_bound" in x for x in e["limits"]["executors"])


# -- 5. identity -----------------------------------------------------------------------


def test_overrides_recorded_for_the_run_mode_only():
    from lakebench.metrics.experiment import experiment_inputs

    cfg = make_config(
        platform={
            "compute": {
                "spark": {"silver_executors": 12, "gold_refresh_executors": 3, "driver_cores": 8}
            }
        }
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        batch = experiment_inputs(cfg, run_mode="batch")["architecture"]
        cont = experiment_inputs(cfg, run_mode="continuous")["architecture"]
        local = experiment_inputs(cfg, run_mode="batch", system="local")["architecture"]
    assert batch["spark_executor_overrides"] == {"silver": 12}
    assert cont["spark_executor_overrides"] == {"gold_refresh": 3}
    assert batch["spark_driver_overrides"] == {"driver_cores": 8}
    assert "spark_executor_overrides" not in local and "spark_driver_overrides" not in local


def test_v16_and_v17_records_read_the_same_overrides():
    """One key space: a stored v1.6 record's snapshot overrides and a v1.7
    block's recorded overrides of the same config are equal; the snapshot's
    overrides for the other mode do not enter."""
    a = sr.load_record("5105a0")
    a["config_snapshot"]["spark"]["executor_overrides"]["silver"] = 12
    a["config_snapshot"]["spark"]["executor_overrides"]["gold_refresh"] = 3
    b = copy.deepcopy(sr.load_record("5105a0"))
    b["experiment"]["architecture"]["spark_executor_overrides"] = {"silver": 12}
    assert cmp.optional_keys(a["experiment"], a)["spark executor overrides"] == {"silver": 12}
    assert cmp.optional_keys(b["experiment"], b)["spark executor overrides"] == {"silver": 12}


def test_override_pair_architecture_difference():
    a = sr.load_record("5105a0")
    b = sr.load_record("5105a0")
    a["config_snapshot"]["spark"]["executor_overrides"]["silver"] = 12
    b["config_snapshot"]["spark"]["executor_overrides"]["silver"] = 16
    ca, cb = cmp.classify(a["experiment"], a), cmp.classify(b["experiment"], b)
    assert [d.key for d in cmp.diff_group(ca, cb, cmp.ARCHITECTURE)] == ["spark executor overrides"]
    assert cmp.diff_group(ca, cb, cmp.CONDITIONS) == []


def test_driver_overrides_are_their_own_architecture_key():
    a = sr.load_record("5105a0")
    b = copy.deepcopy(a)
    b["experiment"]["architecture"]["spark_driver_overrides"] = {"driver_memory": "16g"}
    ca, cb = cmp.classify(a["experiment"], a), cmp.classify(b["experiment"], b)
    assert [d.key for d in cmp.diff_group(ca, cb, cmp.ARCHITECTURE)] == ["spark driver overrides"]


# -- 6. not a baseline, not evidence -------------------------------------------------------


def test_a_binding_override_labels_the_metrics_it_caps():
    from lakebench.metrics.metric_registry import capped_by

    kinds = ["silver-build: executor override"]
    assert capped_by("total_core_hours", kinds, "batch") == kinds


def test_a_count_pinned_at_the_cap_keeps_the_cap_label():
    cfg = _cfg(scale=1000)
    cap = job_mod._JOB_PROFILES["gold-finalize"]["max_executors"]
    cfg.platform.compute.spark.gold_executors = cap
    e = _record_with(cfg, "gold-finalize")["experiment"]
    gold = next(x for x in e["limits"]["executors"] if x["job_type"] == "gold-finalize")
    assert gold["cap_hit"] and "override_bound" not in gold
    assert "gold-finalize: executor cap" in e["limits"]["bound_kinds"]


def test_a_local_run_applies_no_override():
    cfg = _cfg()
    cfg.platform.compute.spark.silver_executors = 4
    run = _metrics(cfg)
    run.config_snapshot["local"] = True
    run.jobs.append(JobMetrics(job_name="j", job_type="silver-build", success=True))
    rec = run.to_dict()
    sb = next(
        x for x in rec["experiment"]["limits"]["executors"] if x["job_type"] == "silver-build"
    )
    assert sb["override"] is None and "override_bound" not in sb
    # A local run's block records no override (experiment_inputs with
    # system="local"); the identity must not take one back from the snapshot.
    rec["experiment"]["architecture"].pop("spark_executor_overrides", None)
    assert "spark executor overrides" not in cmp.optional_keys(rec["experiment"], rec)


def test_the_identity_fallback_reads_the_run_mode():
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["spark"]["executor_overrides"]["silver"] = 12
    rec["config_snapshot"]["spark"]["executor_overrides"]["gold_refresh"] = 3
    rec["experiment"]["mode"] = "continuous"
    assert cmp.optional_keys(rec["experiment"], rec)["spark executor overrides"] == {
        "gold_refresh": 3
    }


def test_the_comparability_key_table_is_the_job_table():
    for mode, job_types in (
        ("batch", job_mod.BATCH_JOB_TYPES),
        ("continuous", job_mod.STREAMING_JOB_TYPES),
    ):
        assert cmp._OVERRIDE_KEYS_BY_MODE[mode] == {
            jt: EXECUTOR_OVERRIDE_FIELDS[jt][1] for jt in job_types
        }


def test_a_refusal_names_the_overrides():
    from unittest import mock

    from lakebench.cli._prerequisites import _check_cluster_capacity
    from lakebench.k8s.client import ClusterCapacity, FreeCapacity

    g = 1024**3
    cfg = make_config(platform={"compute": {"spark": {"silver_executors": 28}}})
    cap = ClusterCapacity(100_000, 800 * g, 4, 40_000, 402 * g)
    k8s = mock.MagicMock()
    k8s.get_cluster_capacity.return_value = cap
    k8s.get_free_capacity.return_value = FreeCapacity(
        free=cap, allocatable=cap, free_by_node=((40_000, 402 * g),)
    )
    with mock.patch("lakebench.k8s.get_k8s_client", return_value=k8s):
        result = _check_cluster_capacity(cfg)
    assert not result.passed
    assert "with executor overrides silver-build 28" in result.message
    assert "Lower or unset the executor overrides" in result.hint
