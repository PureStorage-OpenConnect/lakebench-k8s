"""Tests for the Verdict value object and its collector wiring (A2a)."""

from __future__ import annotations

import itertools
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from lakebench.metrics.collector import (
    BenchmarkMetrics,
    JobMetrics,
    PipelineMetrics,
)
from lakebench.metrics.storage import MetricsStorage
from lakebench.metrics.verdict import Verdict, compute_verdict

FIXTURES = Path(__file__).parent / "fixtures" / "verdict"


# ---------------------------------------------------------------------------
# Verdict.strictest -- unit tests for every flag combination.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exit_ok,badge_ok,success_flag",
    list(itertools.product([True, False], repeat=3)),
)
def test_strictest_any_false_flag_is_failed(
    exit_ok: bool, badge_ok: bool, success_flag: bool
) -> None:
    """If any of exit_ok / badge_ok / success_flag is False, the verdict is
    FAILED. Otherwise (all three True) PASSED, absent other gate signals."""
    v = Verdict.strictest(
        exit_ok=exit_ok,
        badge_ok=badge_ok,
        success_flag=success_flag,
        gate_outcomes={},
        reasons=[],
        qualifiers={},
    )
    if exit_ok and badge_ok and success_flag:
        assert v.status == "PASSED"
    else:
        assert v.status == "FAILED", (
            f"exit_ok={exit_ok} badge_ok={badge_ok} success_flag={success_flag} "
            f"expected FAILED, got {v.status}"
        )


def test_strictest_priority_order() -> None:
    """FAILED > INTERRUPTED > REFUSED > PASSED."""
    # INTERRUPTED beats REFUSED beats PASSED
    v_int = Verdict.strictest(
        exit_ok=True,
        badge_ok=True,
        success_flag=True,
        gate_outcomes={"a": "INTERRUPTED", "b": "REFUSED", "c": "PASS"},
        reasons=[],
        qualifiers={},
    )
    assert v_int.status == "INTERRUPTED"

    v_refused = Verdict.strictest(
        exit_ok=True,
        badge_ok=True,
        success_flag=True,
        gate_outcomes={"a": "REFUSED", "b": "PASS"},
        reasons=[],
        qualifiers={},
    )
    assert v_refused.status == "REFUSED"

    # FAILED beats INTERRUPTED
    v_failed = Verdict.strictest(
        exit_ok=True,
        badge_ok=True,
        success_flag=True,
        gate_outcomes={"pipeline": "PASS", "a": "FAIL", "b": "INTERRUPTED"},
        reasons=["explicit gate failure"],
        qualifiers={"n_runs": 1},
    )
    assert v_failed.status == "FAILED"
    assert "explicit gate failure" in v_failed.reasons


def test_strictest_passed_when_everything_ok() -> None:
    v = Verdict.strictest(
        exit_ok=True,
        badge_ok=True,
        success_flag=True,
        gate_outcomes={"pipeline": "PASS", "benchmark": "PASS"},
        reasons=[],
        qualifiers={"n_runs": 1},
    )
    assert v.status == "PASSED"
    # A PASSED verdict discards reasons the caller may have supplied.
    assert v.reasons == []


# ---------------------------------------------------------------------------
# Round-trip through the collector: verdict is present in metrics dict.
# ---------------------------------------------------------------------------


def _make_metrics(
    *,
    success: bool = True,
    with_jobs: bool = True,
    job_success: bool = True,
    benchmark_error: str | None = None,
    with_benchmark: bool = False,
    benchmark_query_success: bool = True,
) -> PipelineMetrics:
    m = PipelineMetrics(
        run_id="test-run",
        deployment_name="test-deploy",
        start_time=datetime(2026, 9, 27, tzinfo=timezone.utc),
        end_time=datetime(2026, 9, 27, 0, 1, tzinfo=timezone.utc),
        total_elapsed_seconds=60.0,
        success=success,
        benchmark_error=benchmark_error,
    )
    if with_jobs:
        # Every layer has rows, so the EVD-1 layer_rows gate passes.
        for stage in ("bronze-verify", "silver-build", "gold-finalize"):
            m.jobs.append(
                JobMetrics(
                    job_name=f"lakebench-{stage}",
                    job_type=stage,
                    success=job_success,
                    output_rows=100,
                )
            )
    if with_benchmark:
        m.benchmark = BenchmarkMetrics(
            mode="power",
            cache="hot",
            scale=1.0,
            qph=100.0,
            total_seconds=10.0,
            queries=[{"name": "q1", "success": benchmark_query_success, "elapsed_seconds": 1.0}],
        )
    return m


@pytest.mark.parametrize(
    ("kw", "status", "gate", "gate_value", "success"),
    [
        ({}, "PASSED", "pipeline", "PASS", True),
        # a failed job fails the verdict and leaves the legacy success field alone
        ({"job_success": False}, "FAILED", "pipeline", "FAIL", True),
        ({"success": True, "job_success": False}, "FAILED", "pipeline", "FAIL", True),
        ({"success": False}, "FAILED", None, None, False),
        ({"benchmark_error": "Trino timeout"}, "FAILED", "benchmark", "FAIL", None),
        (
            {"with_benchmark": True, "benchmark_query_success": False},
            "FAILED",
            "benchmark",
            "FAIL",
            None,
        ),
    ],
)
def test_collector_writes_the_verdict_block(kw, status, gate, gate_value, success) -> None:
    d = _make_metrics(**kw).to_dict()
    assert d["verdict"]["status"] == status
    if gate:
        assert d["verdict"]["gates"][gate] == gate_value
    if success is not None:
        assert d["success"] is success
    if not kw:
        # no benchmark recorded: nothing was scoped in, so no benchmark gate
        assert "benchmark" not in d["verdict"]["gates"]


# ---------------------------------------------------------------------------
# Regression fixtures: real runs the HTML badge marked FAILED even though
# their raw ``success`` flag was True. The verdict must catch these.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fixture_name",
    [
        "run-20260925-104452-21bf3a.metrics.json",
        "run-20260925-135005-4b7a97.metrics.json",
    ],
)
def test_regression_badge_failed_becomes_verdict_failed(fixture_name: str) -> None:
    """Both fixtures had success=True but the HTML badge said FAILED.

    The verdict must report FAILED so the two agree.
    """
    fixture = FIXTURES / fixture_name
    assert fixture.exists(), (
        f"Fixture missing: {fixture}. Copy it from the integrate tree "
        f"(lakebench-output/runs/<run>/metrics.json)."
    )
    raw = json.loads(fixture.read_text())
    assert raw["success"] is True, "fixture invariant: raw success is True"

    # Load through MetricsStorage._dict_to_metrics so pipeline_benchmark is
    # rehydrated as a PipelineBenchmark object.
    storage = MetricsStorage(FIXTURES)
    metrics: PipelineMetrics | None = storage._dict_to_metrics(raw)
    assert metrics is not None

    v = compute_verdict(metrics)
    assert v.status == "FAILED", f"{fixture_name}: expected FAILED, got {v}"
    assert v.reasons, "FAILED verdict must carry at least one reason"


# ---------------------------------------------------------------------------
# Strict gate-outcome vocabulary. A past-tense value like "FAILED" is a
# common miswrite; it must raise, not silently PASS.
# ---------------------------------------------------------------------------


def test_strictest_rejects_unknown_gate_vocabulary() -> None:
    # Past-tense misfire from a hypothetical future caller.
    with pytest.raises(ValueError, match="Unknown gate outcome"):
        Verdict.strictest(
            exit_ok=True,
            badge_ok=True,
            success_flag=True,
            gate_outcomes={"pipeline": "FAILED"},
            reasons=[],
            qualifiers={},
        )
    # Other typos should also raise.
    with pytest.raises(ValueError, match="Unknown gate outcome"):
        Verdict.strictest(
            exit_ok=True,
            badge_ok=True,
            success_flag=True,
            gate_outcomes={"pipeline": "ok"},
            reasons=[],
            qualifiers={},
        )
    # PASS/FAIL/REFUSED/INTERRUPTED all remain accepted.
    for value in ("PASS", "FAIL", "REFUSED", "INTERRUPTED"):
        Verdict.strictest(
            exit_ok=True,
            badge_ok=True,
            success_flag=True,
            gate_outcomes={"pipeline": value},
            reasons=[],
            qualifiers={},
        )
