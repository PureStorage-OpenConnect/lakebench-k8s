"""Executor overrides are bounded, counted, labelled and kept out of
evidence."""

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


def test_override_ceiling_is_the_job_modules_executor_cap():
    from lakebench.config.schema import MAX_EXECUTOR_OVERRIDE

    assert MAX_EXECUTOR_OVERRIDE == job_mod._MAX_EXECUTORS_SAFE


def _write(tmp_path: Path, spark: dict) -> Path:
    raw = make_config().model_dump(mode="json", by_alias=True, exclude_none=True)
    raw["platform"]["compute"]["spark"].update(spark)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_counts_above_the_auto_ceiling_are_kept_for_a_deploy(tmp_path):
    """Lakebench advises above 28 executors and 16 driver cores; it does not
    refuse or drop what the config sets."""
    path = _write(tmp_path, {"silver_executors": 40, "driver_cores": 17})
    spark = load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False).platform.compute.spark
    assert (spark.silver_executors, spark.driver_cores) == (40, 17)


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
        platform={"compute": {"spark": {"gold_executors": 10}}},
    )
    base = plan_requirements(make_config(architecture={"pipeline": {"mode": "continuous"}}))
    plan = plan_requirements(cfg)
    gr = next(r for r in plan.spark.per_job if r.job_type == "gold-refresh")
    assert gr.executors == 10
    assert plan.spark.cpu_cores > base.spark.cpu_cores


@pytest.mark.parametrize(
    ("mode", "job", "cores"),
    [
        # batch takes the stage's size keys, as continuous does
        ("batch", JobType.SILVER_BUILD, "8"),
        # a continuous run's bronze-verify preflight keeps its profile
        ("continuous", JobType.BRONZE_VERIFY, None),
    ],
)
def test_stage_size_keys_size_the_job_of_the_runs_mode(mode, job, cores):
    from lakebench.spark.job import SparkJobManager
    from tests.fixtures.spark_helpers import _mock_k8s

    spark = {"silver_executor_cores": 8, "silver_executor_memory": "100g"}
    spark |= {"bronze_executor_cores": 8}
    cfg = make_config(
        workload={"schema": "financial"},
        architecture={"pipeline": {"mode": mode}},
        platform={"compute": {"spark": spark}},
    )
    executor = SparkJobManager(cfg, _mock_k8s())._build_manifest(job)["spec"]["executor"]
    profile = job_mod._resolve_job_profile(job.value, "financial")
    assert str(executor["cores"]) == (cores or str(profile["executor_cores"]))
    if cores:
        assert executor["memory"] == "100g"


def test_a_stage_driver_key_wins_over_the_global_key_for_its_stage():
    from lakebench.spark.job import SparkJobManager
    from tests.fixtures.spark_helpers import _mock_k8s

    spark = {"driver_memory": "40g", "bronze_driver_memory": "48g", "gold_driver_cores": 6}
    cfg = make_config(workload={"schema": "financial"}, platform={"compute": {"spark": spark}})
    manager = SparkJobManager(cfg, _mock_k8s())
    drivers = {
        job: manager._build_manifest(job)["spec"]["driver"]
        for job in (JobType.BRONZE_VERIFY, JobType.SILVER_BUILD, JobType.GOLD_FINALIZE)
    }
    assert drivers[JobType.BRONZE_VERIFY]["memory"] == "48g"
    assert drivers[JobType.SILVER_BUILD]["memory"] == "40g"
    assert drivers[JobType.GOLD_FINALIZE]["cores"] == 6


def test_one_override_rule():
    """The manifest, the peak and the run record read one table."""
    cfg = make_config(platform={"compute": {"spark": {"gold_executors": 6}}})
    assert executor_override("gold-finalize", cfg) == 6
    assert executor_override("silver-build", cfg) is None
    assert executor_override("score-financial-reference", cfg) is None
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
    del profiles["gold-finalize"]
    monkeypatch.setattr(job_mod, "_JOB_PROFILES", profiles)
    mgr = SparkJobManager(make_config(), _mock_k8s())
    with pytest.raises(MissingSizingProfile, match="gold-finalize"):
        mgr._build_manifest(JobType.GOLD_FINALIZE)


# -- 4. labelled when it binds -------------------------------------------------------


def _record_with(cfg, job_type="silver-build"):
    run = _metrics(cfg)
    run.jobs.append(JobMetrics(job_name="j", job_type=job_type, success=True))
    return run.to_dict()


def test_binding_override_labelled():
    from lakebench.metrics.metric_registry import capped_by

    cfg = _cfg()
    cfg.platform.compute.spark.silver_executors = 4  # the profile asks 8 at scale 1
    e = _record_with(cfg)["experiment"]
    sb = next(x for x in e["limits"]["executors"] if x["job_type"] == "silver-build")
    assert sb["override_bound"] is True
    kinds = e["limits"]["bound_kinds"]
    assert "silver-build: executor override" in kinds
    assert "silver-build: executor override 4 (profile asks 8)" in e["limits"]["bound"]
    # The bound kind caps the metrics it should.
    assert "silver-build: executor override" in capped_by("total_core_hours", kinds, "batch")


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
            "compute": {"spark": {"silver_executors": 12, "gold_executors": 3, "driver_cores": 8}}
        }
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        batch = experiment_inputs(cfg, run_mode="batch")["architecture"]
        cont = experiment_inputs(cfg, run_mode="continuous")["architecture"]
        local = experiment_inputs(cfg, run_mode="batch", system="local")["architecture"]
    assert batch["spark_executor_overrides"] == {"silver": 12, "gold": 3}
    assert cont["spark_executor_overrides"] == {"silver_stream": 12, "gold_refresh": 3}
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


# -- 7. one key per stage -----------------------------------------------------------


def test_the_bronze_verify_preflight_of_a_continuous_run_keeps_its_profile():
    from lakebench.modules.pipeline_engines.spark.job import stage_key_applies

    cont = make_config(
        architecture={"pipeline": {"mode": "continuous"}},
        platform={"compute": {"spark": {"bronze_executors": 24}}},
    )
    batch = make_config(platform={"compute": {"spark": {"bronze_executors": 24}}})
    assert stage_key_applies("bronze-ingest", cont)
    assert not stage_key_applies("bronze-verify", cont)
    assert stage_key_applies("bronze-verify", batch)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("bronze_ingest_executors", "bronze_executors"),
        ("gold_refresh_executor_cores", "gold_executor_cores"),
    ],
)
def test_the_old_streaming_keys_are_refused_and_name_their_replacement(tmp_path, old, new):
    path = _write(tmp_path, {old: 8})
    with pytest.raises(Exception, match=new):
        load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)
    # A null left in a saved config sizes nothing, so it still loads.
    path = _write(tmp_path, {old: None})
    assert load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)
