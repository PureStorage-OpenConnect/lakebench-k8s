"""The run-end dependency pod check through the real ``run`` (QA-9 harness).

A pod on another set fails a batch and a continuous run (exit 1, success
false, the dependency_set gate FAIL); an interrupt or a namespace-gone abort leaves the
check unread and the record says why; an exception that escapes the pipeline
is recorded as a failed run and reaches the CLI unmasked.
"""

from __future__ import annotations

import dataclasses

import pytest

from tests.fixtures.sigterm_sentinel import sentinel_sigterm
from tests.harness.run_harness import (
    SCENARIOS,
    invoke_scenario,
    run_scenario_full,
    saved_record,
)

__all__ = ["sentinel_sigterm"]


def _pods(monkeypatch, result):
    from lakebench.deps import runtime

    calls = []

    def check(cfg, handle, since):
        calls.append(handle.pinset_sha256)
        return result(handle) if callable(result) else result

    monkeypatch.setattr(runtime, "check_pods", check)
    return calls


def _mismatch(handle):
    return {
        "pods_checked": 1,
        "pod_mismatches": [{"pod": "lakebench-trino-0", "pinset": "f" * 64}],
    }


def _run(base, tmp_path, monkeypatch, **changes):
    scenario = dataclasses.replace(SCENARIOS[base], **changes)
    trace, rec = run_scenario_full(scenario, tmp_path, monkeypatch)
    return trace, saved_record(tmp_path)


@pytest.mark.parametrize("base", ["batch_c360", "continuous_c360"])
def test_a_pod_on_another_set_fails_the_run(tmp_path, monkeypatch, base):
    """With the check's result ignored the run exits 0 and records a pass."""
    calls = _pods(monkeypatch, _mismatch)
    trace, record = _run(base, tmp_path, monkeypatch)
    assert calls, "the run-end pod check did not run"
    assert trace["exit_code"] == 1
    assert record["success"] is False
    assert record["verdict"]["gates"]["dependency_set"] == "FAIL"
    deps = record["provenance"]["deps"]
    assert deps["pods_checked"] == 1 and deps["pod_mismatches"][0]["pod"] == "lakebench-trino-0"


@pytest.mark.parametrize("base", ["batch_c360", "continuous_c360"])
def test_a_clean_pod_check_passes(tmp_path, monkeypatch, base):
    calls = _pods(monkeypatch, {"pods_checked": 2, "pod_mismatches": []})
    trace, record = _run(base, tmp_path, monkeypatch)
    assert calls and record["verdict"]["gates"].get("dependency_set") != "FAIL"
    if base == "batch_c360":
        # The recorded continuous_c360 drain never settles (its logs end
        # first), so only batch is expected to exit 0 here.
        assert trace["exit_code"] == 0 and record["success"] is True
    assert record["provenance"]["deps"]["pods_checked"] == 2
    assert "pods_check_skipped" not in record["provenance"]["deps"]


def test_an_unreadable_pod_check_fails_the_run(tmp_path, monkeypatch):
    _pods(
        monkeypatch,
        {"pods_checked": None, "pods_check_error": "503", "pod_mismatches": []},
    )
    trace, record = _run("batch_c360", tmp_path, monkeypatch)
    assert trace["exit_code"] == 1 and record["success"] is False
    assert record["verdict"]["gates"]["dependency_set"] == "FAIL"


def test_an_interrupted_batch_run_skips_the_check(tmp_path, monkeypatch):
    calls = _pods(monkeypatch, _mismatch)
    trace, record = _run("batch_c360", tmp_path, monkeypatch, interrupt=("silver-build", "SIGINT"))
    assert trace["exit_code"] == 130 and calls == []
    assert record["provenance"]["deps"]["pods_check_skipped"] == "interrupted"
    assert record["verdict"]["status"] == "INTERRUPTED"


@pytest.mark.parametrize(
    "event,code,why", [("SIGINT", 130, "interrupted"), ("namespace_gone", 1, "namespace gone")]
)
def test_a_stopped_continuous_run_skips_the_check(tmp_path, monkeypatch, event, code, why):
    calls = _pods(monkeypatch, _mismatch)
    trace, record = _run("continuous_c360", tmp_path, monkeypatch, events=((700.0, event),))
    assert trace["exit_code"] == code and calls == []
    assert record["provenance"]["deps"]["pods_check_skipped"] == why


def test_an_exception_in_the_pipeline_is_recorded_failed_and_not_masked(tmp_path, monkeypatch):
    """A crash is a failed record and the CLI's own unhandled-error exit,
    never a typer exit that hides it, even with a failing pod check."""
    from lakebench.metrics import collector as collector_mod

    _pods(monkeypatch, _mismatch)

    def boom(self, *a, **k):
        raise RuntimeError("collector broke mid-run")

    monkeypatch.setattr(collector_mod.MetricsCollector, "record_job", boom)
    result, _ = invoke_scenario(SCENARIOS["batch_c360"], tmp_path, monkeypatch)
    # The CLI's unhandled-error path reports it; a typer exit raised by the
    # finally would replace it and the message would be gone.
    assert result.exit_code == 1
    assert "collector broke mid-run" in result.output + repr(result.exception)
    assert saved_record(tmp_path)["success"] is False
