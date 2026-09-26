"""maintenance_policy_id: stamped in metrics.json, gated like query_set_id."""

from __future__ import annotations

from datetime import datetime

import pytest
import yaml

from lakebench.metrics import perf_gate as pg
from lakebench.metrics.collector import PipelineBenchmark, PipelineMetrics
from lakebench.metrics.maintenance_policy import (
    LEGACY_MAINTENANCE_POLICY_ID,
    MAINTENANCE_POLICY_ID,
    policy_mismatch,
    recorded_policy,
    skipped_policy_id,
)
from tests.test_perf_gate import _batch_run, _compare, _record, env  # noqa: F401

NAME = "c360-batch-s10"


def test_ids_and_helpers():
    assert MAINTENANCE_POLICY_ID == "m2-2026-09-26"
    assert recorded_policy({}) == LEGACY_MAINTENANCE_POLICY_ID
    assert recorded_policy(None) == LEGACY_MAINTENANCE_POLICY_ID
    assert recorded_policy({"maintenance_policy_id": "x"}) == "x"
    assert policy_mismatch(None, LEGACY_MAINTENANCE_POLICY_ID) is None
    assert policy_mismatch(MAINTENANCE_POLICY_ID, MAINTENANCE_POLICY_ID) is None
    assert "maintenance policy differs" in (policy_mismatch(None, MAINTENANCE_POLICY_ID) or "")


def test_a_new_run_stamps_the_current_policy():
    m = PipelineMetrics(run_id="r", deployment_name="d", start_time=datetime(2026, 9, 26))
    assert m.to_dict()["maintenance_policy_id"] == MAINTENANCE_POLICY_ID


def test_storage_reads_an_unstamped_record_as_legacy(tmp_path):
    from lakebench.metrics.storage import MetricsStorage

    storage = MetricsStorage(tmp_path)
    m = PipelineMetrics(
        run_id="20260926-000000-aaaaaa", deployment_name="d", start_time=datetime(2026, 9, 26)
    )
    path = storage.save_run(m)
    assert storage.load_run(m.run_id).maintenance_policy_id == MAINTENANCE_POLICY_ID
    raw = path.read_text().replace(f'"maintenance_policy_id": "{MAINTENANCE_POLICY_ID}", ', "")
    raw = raw.replace(f', "maintenance_policy_id": "{MAINTENANCE_POLICY_ID}"', "")
    raw = raw.replace(f'"maintenance_policy_id": "{MAINTENANCE_POLICY_ID}"', '"x_removed": 1')
    path.write_text(raw)
    assert storage.load_run(m.run_id).maintenance_policy_id == LEGACY_MAINTENANCE_POLICY_ID


def test_perf_gate_refuses_runs_not_under_the_current_policy(env):  # noqa: F811
    """Legacy lumps two real policies, and +skipped ran no maintenance."""
    snap = env.snaps[NAME]
    _record(env, NAME, _batch_run(snap, "20260924-100000-aaaaaa"))
    assert _compare(env, NAME, _batch_run(snap, "20260924-110000-bbbbbb")).verdict == pg.PASS
    for i, policy in enumerate((None, skipped_policy_id(), "m9-future")):
        run = _batch_run(snap, f"20260924-12000{i}-cccccc")
        if policy is None:
            del run["maintenance_policy_id"]
        else:
            run["maintenance_policy_id"] = policy
        c = _compare(env, NAME, run)
        assert c.verdict == pg.REFUSED, policy
        assert any("not the current" in r for r in c.reasons)


def test_legacy_run_cannot_become_a_baseline(env):  # noqa: F811
    snap = env.snaps[NAME]
    legacy = _batch_run(snap, "20260924-100000-aaaaaa")
    del legacy["maintenance_policy_id"]
    with pytest.raises(pg.PerfGateError, match="not the current"):
        _record(env, NAME, legacy)


def test_baseline_under_another_policy_refuses_current_runs(env):  # noqa: F811
    snap = env.snaps[NAME]
    store = _record(env, NAME, _batch_run(snap, "20260924-100000-aaaaaa"))
    assert store.baselines[NAME].maintenance_policy_id == MAINTENANCE_POLICY_ID
    raw = yaml.safe_load(env.store_path.read_text())
    assert raw["baselines"][NAME]["maintenance_policy_id"] == MAINTENANCE_POLICY_ID
    del raw["baselines"][NAME]["maintenance_policy_id"]  # a pre-id baseline
    env.store_path.write_text(yaml.safe_dump(raw, sort_keys=False))
    c = _compare(env, NAME, _batch_run(snap, "20260924-110000-bbbbbb"))
    assert c.verdict == pg.REFUSED
    assert any("maintenance policy differs" in r for r in c.reasons)
    assert c.rows == []


def test_skip_maintenance_stamps_a_distinct_id():
    import inspect

    import lakebench.cli._run as run_mod
    import lakebench.cli._sustained as sus

    for src in (inspect.getsource(run_mod.run), inspect.getsource(sus._run_sustained)):
        assert "if skip_maintenance and collector.current_run is not None:" in src
        assert "collector.current_run.maintenance_policy_id = skipped_policy_id()" in src
    assert skipped_policy_id() == MAINTENANCE_POLICY_ID + "+skipped"


def test_reproduce_package_carries_the_policy():
    from lakebench.cli._reproduce import _build_package
    from tests.test_reproduce import _metrics

    pkg = _build_package(_metrics(), config_reference=None, commit_sha="abc1234")
    assert pkg["reproduction_metadata"]["maintenance_policy_id"] == MAINTENANCE_POLICY_ID


@pytest.mark.parametrize(
    ("meta", "actual", "refused"),
    [
        ({}, MAINTENANCE_POLICY_ID, True),  # legacy package, current code
        ({"maintenance_policy_id": MAINTENANCE_POLICY_ID}, MAINTENANCE_POLICY_ID, False),
        ({"maintenance_policy_id": "m9-future"}, MAINTENANCE_POLICY_ID, True),
        ({}, LEGACY_MAINTENANCE_POLICY_ID, False),
    ],
)
def test_reproduce_policy_refusal(meta, actual, refused):
    from lakebench.cli._reproduce import _policy_refusal

    assert (_policy_refusal(meta, actual) is not None) is refused


def test_reproduce_verify_refuses_a_legacy_package_before_running(tmp_path):
    """Exit 2 before the multi-hour run, even on --dry-run."""
    import typer

    from lakebench.cli._reproduce import _build_package, reproduce
    from tests.test_reproduce import _ONE_SAMPLE_CFG, _metrics

    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(_ONE_SAMPLE_CFG)
    pkg = _build_package(_metrics(), config_reference="cfg.yaml", commit_sha="unknown")
    del pkg["reproduction_metadata"]["maintenance_policy_id"]
    pkg_path = tmp_path / "pkg.yaml"
    pkg_path.write_text(yaml.safe_dump(pkg))
    with pytest.raises(typer.Exit) as exc:
        reproduce(package=pkg_path, dry_run=True)
    assert exc.value.exit_code == 2


def _report_html(tmp_path, *, mode: str, fmt: str, policy: str = MAINTENANCE_POLICY_ID) -> str:
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.reports.generator import ReportGenerator

    storage = MetricsStorage(tmp_path)
    m = PipelineMetrics(
        run_id="20260926-000000-bbbbbb",
        deployment_name="d",
        start_time=datetime(2026, 9, 26),
        success=True,
        config_snapshot={"table_format": fmt},
        maintenance_policy_id=policy,
    )
    m.pipeline_benchmark = PipelineBenchmark(
        run_id=m.run_id, deployment_name="d", start_time=m.start_time, pipeline_mode=mode
    )
    storage.save_run(m)
    out = ReportGenerator(metrics_dir=tmp_path, output_dir=tmp_path).generate_report(m.run_id)
    return out.read_text()


def test_report_shows_the_policy(tmp_path):
    html = _report_html(tmp_path, mode="batch", fmt="iceberg")
    assert "Maintenance policy" in html and MAINTENANCE_POLICY_ID in html
    assert "no effective table maintenance" not in html


def test_delta_continuous_report_states_no_effective_maintenance(tmp_path):
    html = _report_html(tmp_path, mode="sustained", fmt="delta")
    assert "no effective table maintenance in continuous mode (v1.6)" in html
    assert MAINTENANCE_POLICY_ID in html


def test_legacy_delta_continuous_report_does_not_claim_the_new_policy(tmp_path):
    html = _report_html(
        tmp_path, mode="sustained", fmt="delta", policy=LEGACY_MAINTENANCE_POLICY_ID
    )
    assert "no effective table maintenance" not in html
    assert LEGACY_MAINTENANCE_POLICY_ID in html


def test_lakebench_compare_warns_across_policies():
    from lakebench.cli._compare import _build_comparison

    a = {"run_id": "a", "pipeline_benchmark": {"scores": {}}}
    b = {
        "run_id": "b",
        "pipeline_benchmark": {"scores": {}},
        "maintenance_policy_id": MAINTENANCE_POLICY_ID,
    }
    assert any(
        "maintenance policy differs" in w for w in _build_comparison("A", a, "B", b)["warnings"]
    )
    assert not _build_comparison("A", a, "B", dict(a))["warnings"]


def test_turned_down_maintenance_is_a_fingerprint_difference(env):  # noqa: F811
    """pre_benchmark_maintenance: false is not the policy's maintenance."""
    import copy

    snap = env.snaps[NAME]
    assert snap["maintenance"]["pre_benchmark_maintenance"] is True
    _record(env, NAME, _batch_run(snap, "20260924-100000-aaaaaa"))
    off = copy.deepcopy(snap)
    off["maintenance"]["pre_benchmark_maintenance"] = False
    c = _compare(env, NAME, _batch_run(off, "20260924-110000-bbbbbb"))
    assert c.verdict == pg.REFUSED
    assert any("maintenance.pre_benchmark_maintenance" in r for r in c.reasons)


def test_latest_candidate_skips_runs_not_under_the_current_policy(env):  # noqa: F811
    snap = env.snaps[NAME]
    good = _batch_run(snap, "20260924-100000-aaaaaa")
    env.write_run(good)
    later = _batch_run(snap, "20260924-110000-bbbbbb")
    later["maintenance_policy_id"] = skipped_policy_id()
    env.write_run(later)
    store = env.store()
    cand = pg.latest_candidate(store.pinned(NAME), env.runs)
    assert cand is not None and cand.run_id == good["run_id"]
