"""Tests for the Verdict value object and its collector wiring (A2a)."""

from __future__ import annotations

import itertools
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def test_strictest_gate_fail_wins_over_true_flags() -> None:
    """Any gate outcome of FAIL forces FAILED, even when every flag is True."""
    v = Verdict.strictest(
        exit_ok=True,
        badge_ok=True,
        success_flag=True,
        gate_outcomes={"pipeline": "PASS", "benchmark": "FAIL"},
        reasons=["explicit gate failure"],
        qualifiers={"n_runs": 1},
    )
    assert v.status == "FAILED"
    assert "explicit gate failure" in v.reasons


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
        gate_outcomes={"a": "FAIL", "b": "INTERRUPTED"},
        reasons=[],
        qualifiers={},
    )
    assert v_failed.status == "FAILED"


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


def test_verdict_is_frozen() -> None:
    """Verdict is an immutable value object."""
    from dataclasses import FrozenInstanceError

    v = Verdict(status="PASSED", reasons=[], gates={}, qualifiers={})
    with pytest.raises(FrozenInstanceError):
        v.status = "FAILED"  # type: ignore[misc]


def test_verdict_to_dict_shape() -> None:
    v = Verdict(
        status="FAILED",
        reasons=["ingest saturated"],
        gates={"pipeline": "FAIL"},
        qualifiers={"pipeline_mode": "sustained", "n_runs": 1},
    )
    d = v.to_dict()
    assert d == {
        "status": "FAILED",
        "reasons": ["ingest saturated"],
        "gates": {"pipeline": "FAIL"},
        "qualifiers": {"pipeline_mode": "sustained", "n_runs": 1},
    }
    # Round-trip JSON-safe.
    assert json.loads(json.dumps(d)) == d


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
        m.jobs.append(
            JobMetrics(
                job_name="lakebench-bronze-verify",
                job_type="bronze-verify",
                success=job_success,
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


def test_collector_writes_verdict_block_passed() -> None:
    m = _make_metrics()
    d = m.to_dict()
    assert "verdict" in d
    assert d["success"] is True
    # Passed on happy path: exit True, no failing job, no benchmark error.
    assert d["verdict"]["status"] == "PASSED"
    assert d["verdict"]["gates"]["pipeline"] == "PASS"
    # A run that recorded no benchmark simply omits the benchmark gate:
    # nothing was scoped in, so nothing can pass, fail, refuse or interrupt.
    assert "benchmark" not in d["verdict"]["gates"]


def test_collector_writes_verdict_block_failed_on_job_failure() -> None:
    m = _make_metrics(job_success=False)
    d = m.to_dict()
    assert d["success"] is True  # not mutated
    v = d["verdict"]
    assert v["status"] == "FAILED"
    assert v["gates"]["pipeline"] == "FAIL"
    # Reasons carry the badge's phrasing.
    assert any("Batch jobs failed" in r for r in v["reasons"])


def test_collector_writes_verdict_block_failed_when_success_false() -> None:
    m = _make_metrics(success=False)
    d = m.to_dict()
    assert d["success"] is False
    assert d["verdict"]["status"] == "FAILED"


def test_collector_writes_verdict_block_failed_on_benchmark_error() -> None:
    m = _make_metrics(benchmark_error="Trino timeout")
    d = m.to_dict()
    v = d["verdict"]
    assert v["status"] == "FAILED"
    assert v["gates"]["benchmark"] == "FAIL"
    assert any("Trino timeout" in r for r in v["reasons"])


def test_collector_writes_verdict_block_failed_on_query_failure() -> None:
    m = _make_metrics(with_benchmark=True, benchmark_query_success=False)
    d = m.to_dict()
    v = d["verdict"]
    assert v["status"] == "FAILED"
    assert v["gates"]["benchmark"] == "FAIL"


def test_success_still_present_and_unchanged() -> None:
    """A2a hard requirement: success is not removed or changed."""
    m = _make_metrics(success=True, job_success=False)
    d = m.to_dict()
    assert "success" in d
    assert d["success"] is True
    # Verdict correctly reports FAILED while success stays True.
    assert d["verdict"]["status"] == "FAILED"


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


def test_regression_stub_when_fixtures_absent(tmp_path: Path) -> None:
    """Documentation guard: if the copied fixtures ever go missing this test
    steers the reader to the parametrised regression above."""
    for name in (
        "run-20260925-104452-21bf3a.metrics.json",
        "run-20260925-135005-4b7a97.metrics.json",
    ):
        assert (FIXTURES / name).exists(), (
            f"Fixture missing: {name}. Copy metrics.json from the integrate "
            "tree at /home/repos/lakebench-k8s-integrate/lakebench-output/runs/"
            f"<run-id>/metrics.json into {FIXTURES}/"
        )


# ---------------------------------------------------------------------------
# The verdict block also survives a JSON round-trip via MetricsStorage.
# ---------------------------------------------------------------------------


def test_verdict_survives_json_roundtrip(tmp_path: Path) -> None:
    storage = MetricsStorage(tmp_path)
    m = _make_metrics(job_success=False)
    storage.save_run(m)
    saved = json.loads((tmp_path / f"run-{m.run_id}" / "metrics.json").read_text())
    assert "verdict" in saved
    assert saved["verdict"]["status"] == "FAILED"


# ---------------------------------------------------------------------------
# The strictest classmethod returns immutable data and does not alias input.
# ---------------------------------------------------------------------------


def test_strictest_does_not_alias_input() -> None:
    gate_outcomes: dict[str, str] = {"pipeline": "PASS"}
    reasons: list[str] = ["r1"]
    qualifiers: dict[str, Any] = {"n_runs": 1}
    v = Verdict.strictest(
        exit_ok=False,
        badge_ok=True,
        success_flag=True,
        gate_outcomes=gate_outcomes,
        reasons=reasons,
        qualifiers=qualifiers,
    )
    gate_outcomes["pipeline"] = "FAIL"
    reasons.append("r2")
    qualifiers["extra"] = "x"
    assert v.gates == {"pipeline": "PASS"}
    assert v.reasons == ["r1"]
    assert v.qualifiers == {"n_runs": 1}


# ---------------------------------------------------------------------------
# Strict gate-outcome vocabulary. A past-tense value like "FAILED" is a
# common miswrite; it must raise, not silently PASS.
# ---------------------------------------------------------------------------


def test_strictest_rejects_unknown_gate_vocabulary() -> None:
    import pytest

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


# ---------------------------------------------------------------------------
# Deep copy on ingress and on to_dict: a frozen value object must not be
# mutable-by-reference through nested containers.
# ---------------------------------------------------------------------------


def test_strictest_deep_copies_nested_qualifier_values() -> None:
    inner_list: list[str] = ["a"]
    qualifiers: dict[str, Any] = {"tags": inner_list}
    v = Verdict.strictest(
        exit_ok=True,
        badge_ok=True,
        success_flag=True,
        gate_outcomes={},
        reasons=[],
        qualifiers=qualifiers,
    )
    # Mutating the nested list the caller still holds must not leak through.
    inner_list.append("b")
    qualifiers["tags"].append("c")
    assert v.qualifiers == {"tags": ["a"]}


def test_to_dict_deep_copies_nested_values() -> None:
    v = Verdict.strictest(
        exit_ok=True,
        badge_ok=True,
        success_flag=True,
        gate_outcomes={},
        reasons=[],
        qualifiers={"tags": ["a"]},
    )
    d = v.to_dict()
    # Mutating the dict returned by to_dict must not change the Verdict.
    d["qualifiers"]["tags"].append("b")
    assert v.qualifiers == {"tags": ["a"]}
