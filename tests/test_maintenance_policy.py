"""maintenance_policy_id: stamped in metrics.json, read back as legacy when absent."""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from typer.testing import CliRunner

from lakebench.metrics.collector import PipelineBenchmark, PipelineMetrics
from lakebench.metrics.maintenance_policy import (
    LEGACY_MAINTENANCE_POLICY_ID,
    MAINTENANCE_POLICY_ID,
    effective_maintenance,
    recorded_policy,
    skipped_policy_id,
)

NAME = "c360-batch-s10"


def test_ids_and_helpers():
    assert recorded_policy({}) == LEGACY_MAINTENANCE_POLICY_ID
    assert recorded_policy(None) == LEGACY_MAINTENANCE_POLICY_ID
    assert recorded_policy({"maintenance_policy_id": "x"}) == "x"


def test_storage_reads_an_unstamped_record_as_legacy(tmp_path):
    from lakebench.metrics.storage import MetricsStorage

    storage = MetricsStorage(tmp_path)
    m = PipelineMetrics(
        run_id="20260926-000000-aaaaaa", deployment_name="d", start_time=datetime(2026, 9, 26)
    )
    path = storage.save_run(m)
    assert storage.load_run(m.run_id).maintenance_policy_id == MAINTENANCE_POLICY_ID
    raw = json.loads(path.read_text())
    assert raw.pop("maintenance_policy_id") == MAINTENANCE_POLICY_ID
    path.write_text(json.dumps(raw))
    assert storage.load_run(m.run_id).maintenance_policy_id == LEGACY_MAINTENANCE_POLICY_ID


@pytest.mark.parametrize("mode", ["batch", "continuous"])
@pytest.mark.parametrize("skip", [True, False])
def test_skip_maintenance_stamps_a_distinct_id(monkeypatch, tmp_path, mode, skip):
    """A run without maintenance is not labelled with the full policy id."""
    from unittest.mock import MagicMock

    import lakebench.cli._sustained as sustained_mod
    from lakebench.cli import app
    from lakebench.metrics.collector import MetricsCollector
    from tests.fixtures import datagen_timeout_helpers as dg

    monkeypatch.chdir(tmp_path)
    stubs = dg._stub_full_run(monkeypatch)
    stubs["op"].check_status.return_value = MagicMock(ready=False, installed=True, message="down")
    seen = []
    real_start = MetricsCollector.start_run

    def start_run(self, *a, **k):
        out = real_start(self, *a, **k)
        seen.append(self.current_run)
        return out

    monkeypatch.setattr(MetricsCollector, "start_run", start_run)
    sustained_calls = []
    real_sustained = sustained_mod._run_sustained

    def counting_sustained(*a, **k):
        sustained_calls.append(1)
        return real_sustained(*a, **k)

    monkeypatch.setattr(sustained_mod, "_run_sustained", counting_sustained)
    extras = {"architecture": "{pipeline: {mode: continuous}}"} if mode == "continuous" else {}
    cfg = dg._write_cfg(tmp_path, **extras)
    argv = ["run", str(cfg), "--skip-preflight", "--skip-benchmark", "--yes"]
    if skip:
        argv.append("--skip-maintenance")
    res = CliRunner().invoke(app, argv)
    assert len(seen) == 1, res.output[-2000:]
    assert bool(sustained_calls) is (mode == "continuous")
    want = skipped_policy_id() if skip else MAINTENANCE_POLICY_ID
    assert seen[0].maintenance_policy_id == want
    assert skipped_policy_id() != MAINTENANCE_POLICY_ID


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


# -- per-operation identity ---


def test_legacy_coarse_id_still_loads_and_does_not_match_a_current_one():
    """A record written before per-operation ids keeps its coarse id and
    loads; it is not like-for-like with a current record, since its
    expire=ran could hide a failed orphan removal."""
    from lakebench.metrics.experiment import condition_differences, identity

    legacy = {"effective_maintenance": {"id": f"{MAINTENANCE_POLICY_ID}:expire=ran,compaction=ran"}}
    assert identity(legacy)["effective maintenance"].endswith("expire=ran,compaction=ran")
    current = {
        "effective_maintenance": effective_maintenance(
            MAINTENANCE_POLICY_ID, table_format="iceberg", query_engine="trino", mode="batch"
        )
    }
    assert condition_differences(legacy, current)


# ---------------------------------------------------------------------------
# an operation that executed is never labelled not_supported
# ---------------------------------------------------------------------------


def _delta_continuous(engine: str, outcomes):
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


_FAILED_EXPIRE = {
    "kind": "expire",
    "total": 2,
    "succeeded": 0,
    "operations": [{"operation": "vacuum", "total": 2, "succeeded": 0}],
}
_THRIFT_SKIP = {"kind": "expire", "skipped": "Delta VACUUM is skipped on Spark Thrift"}


@pytest.mark.parametrize(
    ("engine", "mode", "outcomes", "operations"),
    [
        # a failed VACUUM is named failed, whatever the mode
        (
            "trino",
            "batch",
            [_FAILED_EXPIRE, {"kind": "compaction", "skipped": "Delta OPTIMIZE is never run"}],
            {"vacuum": "failed", "compaction": "not_supported"},
        ),
        (
            "trino",
            "continuous",
            [_vacuum_round(0), _vacuum_round(0), _OPTIMIZE_SKIP],
            {"vacuum": "failed", "compaction": "not_supported"},
        ),
        # an operation that executed is never labelled not_supported
        (
            "trino",
            "continuous",
            [_vacuum_round(), _vacuum_round(), _OPTIMIZE_SKIP],
            {"vacuum": "ran_no_effect", "compaction": "not_supported"},
        ),
        (
            "trino",
            "continuous",
            [_vacuum_round(0), _vacuum_round(3), _OPTIMIZE_SKIP],
            {"vacuum": "ran_no_effect", "compaction": "not_supported"},
        ),
        (
            "trino",
            "continuous",
            [_OPTIMIZE_SKIP],
            {"vacuum": "not_run", "compaction": "not_supported"},
        ),
        (
            "spark-thrift",
            "continuous",
            [_THRIFT_SKIP, _THRIFT_SKIP, _OPTIMIZE_SKIP],
            {"vacuum": "not_supported", "compaction": "not_supported"},
        ),
    ],
)
def test_delta_identity_names_each_operation_by_what_happened(engine, mode, outcomes, operations):
    e = effective_maintenance(
        MAINTENANCE_POLICY_ID,
        table_format="delta",
        query_engine=engine,
        mode=mode,
        outcomes=outcomes,
    )
    assert e["operations"] == operations
    assert e["id"] == f"{MAINTENANCE_POLICY_ID}:" + ",".join(
        f"{op}={state}" for op, state in operations.items()
    )


def test_delta_continuous_vacuum_detail_separates_partial_from_none():
    ran = _delta_continuous("trino", [_vacuum_round(), _vacuum_round(), _OPTIMIZE_SKIP])
    assert "vacuum=no_effect" in ran["detail_id"]
    assert ran["detail"]["operations"]["vacuum"]["succeeded"] == 6
    partial = _delta_continuous("trino", [_vacuum_round(0), _vacuum_round(3), _OPTIMIZE_SKIP])
    assert "vacuum=partial" in partial["detail_id"]
    assert "vacuum: 3 of 6 statements succeeded" in partial["reasons"]


def test_delta_thrift_continuous_vacuum_gives_the_reason_that_applies():
    e = _delta_continuous("spark-thrift", [_THRIFT_SKIP, _THRIFT_SKIP, _OPTIMIZE_SKIP])
    assert any("OOMs" in r for r in e["reasons"])
    assert not any("vacuum ran" in r for r in e["reasons"])


def test_delta_continuous_names_a_known_limitation_batch_and_iceberg_do_not():
    for engine in ("trino", "spark-thrift"):
        assert _delta_continuous(engine, [_OPTIMIZE_SKIP])["known_limitations"]
    batch = effective_maintenance(
        MAINTENANCE_POLICY_ID, table_format="delta", query_engine="trino", mode="batch"
    )
    iceberg = effective_maintenance(
        MAINTENANCE_POLICY_ID, table_format="iceberg", query_engine="trino", mode="continuous"
    )
    assert batch["known_limitations"] == [] and iceberg["known_limitations"] == []


# ---------------------------------------------------------------------------
# the in-window QpH trend beside the composite median
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
    """A declining series (390 -> 203 QpH, silver files 252 -> 1372): the
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
    from tests.fixtures.report_goldens import page_text

    return page_text(out.read_text())


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
