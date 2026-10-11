"""CFG-2 (CC-12): executor overrides are bounded, counted, labelled and kept
out of evidence."""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest
import yaml

from lakebench.config._load_context import LoadPurpose
from lakebench.config.loader import load_config
from lakebench.metrics import comparability as cmp
from lakebench.metrics.collector import JobMetrics
from lakebench.modules.pipeline_engines.spark import job as job_mod
from lakebench.modules.pipeline_engines.spark.job import (
    EXECUTOR_OVERRIDE_FIELDS,
    MissingSizingProfile,
    compute_peak_requirements,
    executor_override,
)
from lakebench.spark.job import JobType
from tests.conftest import make_config
from tests.fixtures import stored_records as sr
from tests.fixtures.experiment_helpers import _cfg, _metrics

# -- 1. bounds -----------------------------------------------------------------


def test_override_above_28_refused():
    for field in [f for f, _ in EXECUTOR_OVERRIDE_FIELDS.values()]:
        for value in [29, 200]:
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
    base = next(
        r for r in compute_peak_requirements(1, "batch").per_job if r.job_type == "silver-build"
    )
    profile = job_mod._JOB_PROFILES["silver-build"]
    assert base.executors < 20 == sb.executors
    # Each added executor adds its profile's cores and scratch; the driver is unchanged.
    added = 20 - base.executors
    assert sb.cpu_cores == base.cpu_cores + added * profile["executor_cores"]
    assert sb.scratch_gb == base.scratch_gb + added * (base.scratch_gb // base.executors)
    assert sb.memory_gb > base.memory_gb
    assert peak.memory_gb > compute_peak_requirements(1, "batch").memory_gb


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


# -- 3. a missing profile raises ---------------------------------------------------


def test_missing_profile_raises(monkeypatch):
    from lakebench.spark.job import SparkJobManager
    from tests.fixtures.spark_helpers import _mock_k8s

    profiles = dict(job_mod._JOB_PROFILES)
    del profiles["time-travel-financial"]
    monkeypatch.setattr(job_mod, "_JOB_PROFILES", profiles)
    mgr = SparkJobManager(make_config(), _mock_k8s())
    with pytest.raises(MissingSizingProfile, match="time-travel-financial"):
        mgr._build_manifest(JobType.TIME_TRAVEL_FINANCIAL)


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


def _snapshot_overrides(**overrides):
    def mutate(rec):
        rec["config_snapshot"]["spark"]["executor_overrides"].update(overrides)

    return mutate


def _recorded_block(key, value):
    def mutate(rec):
        rec["experiment"]["architecture"][key] = value

    return mutate


def _run_mode(mode):
    def mutate(rec):
        rec["experiment"]["mode"] = mode

    return mutate


@pytest.mark.parametrize(
    ("mutations", "key", "expected"),
    [
        # A stored v1.6 snapshot and a v1.7 block read the same overrides; the
        # snapshot's overrides for the other mode do not enter.
        (
            [_snapshot_overrides(silver=12, gold_refresh=3)],
            "spark executor overrides",
            {"silver": 12},
        ),
        (
            [_recorded_block("spark_executor_overrides", {"silver": 12})],
            "spark executor overrides",
            {"silver": 12},
        ),
        (
            [_snapshot_overrides(silver=12, gold_refresh=3), _run_mode("continuous")],
            "spark executor overrides",
            {"gold_refresh": 3},
        ),
        (
            [_recorded_block("spark_driver_overrides", {"driver_memory": "16g"})],
            "spark driver overrides",
            {"driver_memory": "16g"},
        ),
    ],
)
def test_identity_reads_the_run_modes_overrides(mutations, key, expected):
    rec = sr.load_record("5105a0")
    for mutate in mutations:
        mutate(rec)
    assert cmp.optional_keys(rec["experiment"], rec)[key] == expected


@pytest.mark.parametrize(
    ("mutate_a", "mutate_b", "key"),
    [
        (
            _snapshot_overrides(silver=12),
            _snapshot_overrides(silver=16),
            "spark executor overrides",
        ),
        (
            lambda rec: None,
            _recorded_block("spark_driver_overrides", {"driver_memory": "16g"}),
            "spark driver overrides",
        ),
    ],
)
def test_override_pair_architecture_difference(mutate_a, mutate_b, key):
    a = sr.load_record("5105a0")
    b = sr.load_record("5105a0")
    mutate_a(a)
    mutate_b(b)
    ca, cb = cmp.classify(a["experiment"], a), cmp.classify(b["experiment"], b)
    assert [d.key for d in cmp.diff_group(ca, cb, cmp.ARCHITECTURE)] == [key]
    assert cmp.diff_group(ca, cb, cmp.CONDITIONS) == []


# -- 6. not a baseline, not evidence -------------------------------------------------------


@pytest.mark.parametrize(
    "metric",
    ["total_core_hours", "pipeline_throughput_gb_per_second", "total_elapsed_seconds"],
)
def test_a_binding_override_labels_the_metrics_it_caps(metric):
    from lakebench.metrics.metric_registry import capped_by

    cfg = _cfg()
    cfg.platform.compute.spark.silver_executors = 4  # the profile asks 8 at scale 1
    kinds = _record_with(cfg)["experiment"]["limits"]["bound_kinds"]
    assert "silver-build: executor override" in capped_by(metric, kinds, "batch")


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


def test_the_identity_fallback_reads_the_run_mode():
    rec = sr.load_record("5105a0")
    rec["config_snapshot"]["spark"]["executor_overrides"]["silver"] = 12
    rec["config_snapshot"]["spark"]["executor_overrides"]["gold_refresh"] = 3
    rec["experiment"]["mode"] = "continuous"
    assert cmp.optional_keys(rec["experiment"], rec)["spark executor overrides"] == {
        "gold_refresh": 3
    }
