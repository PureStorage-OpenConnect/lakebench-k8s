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


# -- per-operation identity (lb16 sweep: Polaris + Thrift orphan removal) ---


def _eff_from_run(cfg, fail_orphan_on):
    from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance
    from tests.test_evidence_polish import _trino_maintenance

    outcomes = _trino_maintenance(cfg, fail_orphan_on=fail_orphan_on)
    return effective_maintenance(
        MAINTENANCE_POLICY_ID,
        table_format="iceberg",
        query_engine="trino",
        mode="batch",
        outcomes=[*outcomes, {"kind": "compaction", "total": 2, "succeeded": 2}],
    )


def test_failed_orphan_removal_is_failed_in_the_identity():
    """Orphan removal failing on every table while expiry succeeds must not
    be stamped as maintenance that ran (the sweep recorded expire=ran)."""
    from tests.conftest import make_config

    cfg = make_config(architecture={"workload": {"schema": "customer360"}})
    good = _eff_from_run(cfg, None)
    bad = _eff_from_run(cfg, "lakehouse")
    assert bad["operations"] == {
        "expire_snapshots": "ran",
        "remove_orphan_files": "failed",
        "compaction": "ran",
    }
    assert "remove_orphan_files=failed" in bad["id"]
    assert bad["expire"] == "failed"
    assert any("remove_orphan_files: 0 of 2" in r for r in bad["reasons"])
    assert good["id"] != bad["id"]


def test_one_side_failed_orphan_removal_is_not_like_for_like():
    from lakebench.metrics.experiment import condition_differences
    from tests.conftest import make_config

    cfg = make_config(architecture={"workload": {"schema": "customer360"}})
    good = _eff_from_run(cfg, None)
    bad = _eff_from_run(cfg, "lakehouse")
    a = {"effective_maintenance": good}
    b = {"effective_maintenance": bad}
    diffs = condition_differences(a, b)
    assert any("effective maintenance" in d for d in diffs)
    assert condition_differences(a, {"effective_maintenance": dict(good)}) == []


def test_delta_identity_names_vacuum_and_compaction():
    from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance

    e = effective_maintenance(
        MAINTENANCE_POLICY_ID,
        table_format="delta",
        query_engine="trino",
        mode="batch",
        outcomes=[
            {
                "kind": "expire",
                "total": 2,
                "succeeded": 0,
                "operations": [{"operation": "vacuum", "total": 2, "succeeded": 0}],
            },
            {"kind": "compaction", "skipped": "Delta OPTIMIZE is never run"},
        ],
    )
    assert e["id"] == f"{MAINTENANCE_POLICY_ID}:vacuum=failed,compaction=not_supported"


def test_legacy_coarse_id_still_loads_and_does_not_match_a_current_one():
    """A record written before per-operation ids keeps its coarse id and
    loads; it is not like-for-like with a current record, since its
    expire=ran could hide a failed orphan removal."""
    from lakebench.metrics.experiment import condition_differences, identity
    from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance

    legacy = {"effective_maintenance": {"id": f"{MAINTENANCE_POLICY_ID}:expire=ran,compaction=ran"}}
    assert identity(legacy)["effective maintenance"].endswith("expire=ran,compaction=ran")
    current = {
        "effective_maintenance": effective_maintenance(
            MAINTENANCE_POLICY_ID, table_format="iceberg", query_engine="trino", mode="batch"
        )
    }
    assert condition_differences(legacy, current)


# ---------------------------------------------------------------------------
# lb16-cf defect 2: an operation that executed is never labelled not_supported
# ---------------------------------------------------------------------------


def _delta_continuous(engine: str, outcomes):
    from lakebench.metrics.maintenance_policy import effective_maintenance

    return effective_maintenance(
        MAINTENANCE_POLICY_ID,
        table_format="delta",
        query_engine=engine,
        mode="continuous",
        outcomes=outcomes,
    )


def _vacuum_round(succeeded: int = 3) -> dict:
    return {
        "kind": "expire",
        "retention": "168h",
        "total": 3,
        "succeeded": succeeded,
        "operations": [
            {"operation": "vacuum", "retention": "168h", "total": 3, "succeeded": succeeded}
        ],
    }


_OPTIMIZE_SKIP = {"kind": "compaction", "skipped": "Delta OPTIMIZE is never run"}


def test_delta_trino_continuous_vacuum_that_ran_is_not_not_supported():
    """lb16-cf: VACUUM ran 3/3 twice at 168h and the identity read
    vacuum=not_supported. It ran, with no effect in the window."""
    e = _delta_continuous("trino", [_vacuum_round(), _vacuum_round(), _OPTIMIZE_SKIP])
    assert e["operations"] == {"vacuum": "ran_no_effect", "compaction": "not_supported"}
    assert e["id"] == f"{MAINTENANCE_POLICY_ID}:vacuum=ran_no_effect,compaction=not_supported"
    assert "vacuum=no_effect" in e["detail_id"]
    why = " ".join(e["reasons"])
    assert "vacuum ran at 168h retention" in why and "removed nothing" in why
    assert e["detail"]["operations"]["vacuum"]["succeeded"] == 6


def test_delta_continuous_vacuum_that_failed_stays_failed():
    """ran_no_effect never hides a failure: 0 of 6 is failed."""
    e = _delta_continuous("trino", [_vacuum_round(0), _vacuum_round(0), _OPTIMIZE_SKIP])
    assert e["operations"]["vacuum"] == "failed"
    partial = _delta_continuous("trino", [_vacuum_round(0), _vacuum_round(3), _OPTIMIZE_SKIP])
    assert partial["operations"]["vacuum"] == "ran_no_effect"
    assert "vacuum=partial" in partial["detail_id"]
    assert "vacuum: 3 of 6 statements succeeded" in partial["reasons"]


def test_delta_continuous_vacuum_that_never_ran_is_not_run():
    e = _delta_continuous("trino", [_OPTIMIZE_SKIP])
    assert e["operations"]["vacuum"] == "not_run"


def test_delta_thrift_continuous_vacuum_is_not_supported_with_its_reason():
    """Thrift skips VACUUM (it OOMs): not_supported, for the reason that applies."""
    skip = {"kind": "expire", "skipped": "Delta VACUUM is skipped on Spark Thrift"}
    e = _delta_continuous("spark-thrift", [skip, skip, _OPTIMIZE_SKIP])
    assert e["operations"] == {"vacuum": "not_supported", "compaction": "not_supported"}
    assert any("OOMs" in r for r in e["reasons"])
    assert not any("vacuum ran" in r for r in e["reasons"])


def test_delta_continuous_names_the_known_limitation():
    from lakebench.metrics.maintenance_policy import (
        DELTA_CONTINUOUS_LIMITATION,
        effective_maintenance,
    )

    for engine in ("trino", "spark-thrift"):
        e = _delta_continuous(engine, [_OPTIMIZE_SKIP])
        assert e["known_limitations"] == [DELTA_CONTINUOUS_LIMITATION]
    assert "owner decision #46" in DELTA_CONTINUOUS_LIMITATION
    batch = effective_maintenance(
        MAINTENANCE_POLICY_ID, table_format="delta", query_engine="trino", mode="batch"
    )
    iceberg = effective_maintenance(
        MAINTENANCE_POLICY_ID, table_format="iceberg", query_engine="trino", mode="continuous"
    )
    assert batch["known_limitations"] == [] and iceberg["known_limitations"] == []


# ---------------------------------------------------------------------------
# lb16-cf defect 1: the in-window QpH trend beside the composite median
# ---------------------------------------------------------------------------


def _continuous_pb(qphs, files, *, fmt="delta", engine="spark-thrift"):
    from lakebench.metrics.collector import BenchmarkMetrics, BenchmarkRoundMeta

    pb = PipelineBenchmark(
        run_id="r",
        deployment_name="d",
        start_time=datetime(2026, 9, 27),
        pipeline_mode="sustained",
        config_snapshot={"table_format": fmt, "query_engine": engine},
    )
    for i, (q, f) in enumerate(zip(qphs, files, strict=True)):
        pb.benchmark_rounds.append(
            BenchmarkMetrics(
                mode="power",
                cache="hot",
                scale=1,
                qph=q,
                total_seconds=1.0,
                round_meta=BenchmarkRoundMeta(round_index=i, silver_data_file_count=f),
            )
        )
    return pb


def test_qph_trend_shows_a_declining_series():
    """lb16-cf Delta + Thrift: 390 -> 203 QpH, silver files 252 -> 1372; the
    median alone read like a steady state."""
    pb = _continuous_pb([390.0, 362.0, 267.0, 203.0], [252, 644, 980, 1372])
    t = pb.to_dict()["qph_trend"]
    assert t == {
        "rounds": 4,
        "first_round_qph": 390.0,
        "last_round_qph": 203.0,
        "change_pct": -47.9,
        "silver_data_files_start": 252,
        "silver_data_files_end": 1372,
        "silver_data_files_rounds": 4,
    }
    assert "qph_trend" not in pb.to_dict()["scores"]


def test_qph_trend_says_why_file_counts_are_missing():
    trino = _continuous_pb([899.0, 1056.0], [None, None], engine="trino").qph_trend()
    assert trino is not None
    assert trino["silver_data_files_unavailable"].startswith("unavailable on trino")
    assert "silver_data_files_start" not in trino
    duck = _continuous_pb([1.0], [None], fmt="iceberg", engine="duckdb").qph_trend()
    assert duck is not None and "duckdb" in duck["silver_data_files_unavailable"]
    probe = _continuous_pb([1.0], [None], fmt="iceberg", engine="trino").qph_trend()
    assert probe is not None and "no count" in probe["silver_data_files_unavailable"]
    # Skips failed rounds and rounds whose probe failed.
    mixed = _continuous_pb([0.0, 500.0, 400.0], [10, None, 30]).qph_trend()
    assert mixed is not None
    assert (mixed["rounds"], mixed["first_round_qph"], mixed["silver_data_files_start"]) == (
        2,
        500.0,
        10,
    )
    assert _continuous_pb([0.0], [None]).qph_trend() is None


def _continuous_report(tmp_path, pb, fmt: str) -> str:
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.reports.generator import ReportGenerator

    storage = MetricsStorage(tmp_path)
    m = PipelineMetrics(
        run_id="20260927-000000-cccccc",
        deployment_name="d",
        start_time=datetime(2026, 9, 27),
        success=True,
        config_snapshot={"table_format": fmt},
        maintenance_policy_id=MAINTENANCE_POLICY_ID,
    )
    pb.run_id = m.run_id
    m.pipeline_benchmark = pb
    storage.save_run(m)
    out = ReportGenerator(metrics_dir=tmp_path, output_dir=tmp_path).generate_report(m.run_id)
    return out.read_text()


def test_report_shows_the_limitation_and_the_trend(tmp_path):
    pb = _continuous_pb([390.0, 203.0], [252, 1372])
    html = _continuous_report(tmp_path, pb, "delta")
    assert "Known limitation" in html and "owner decision #46" in html
    assert "first round 390.0, last round 203.0 (-47.9%) over 2 rounds" in html
    assert "252 at the first probed round, 1,372 at the last" in html


def test_report_trend_without_file_counts_and_no_limitation_on_iceberg(tmp_path):
    pb = _continuous_pb([1000.0, 1100.0], [None, None], fmt="iceberg", engine="trino")
    html = _continuous_report(tmp_path, pb, "iceberg")
    assert "Known limitation" not in html
    assert "(+10.0%) over 2 rounds" in html
    assert "the table health probe returned no count in any round" in html
