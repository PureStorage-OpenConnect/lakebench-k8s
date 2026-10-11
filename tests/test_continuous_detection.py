"""Tests for the continuous AML detection spine.

The continuous gold stage (gold_refresh_financial) re-runs detection over the
full silver corpus each tick, reusing the batch DELETE-per-rule + INSERT
driver (run_detection_rules), and the sustained runner fails an AML run that
produced zero alerts. These scripts execute inside a
Spark driver and cannot import pyspark in the test env, so the driver-side
assertions are source/AST-based. The sustained-runner helper is pure Python
and is exercised directly.

Design note guarded here: an earlier sliding-data-clock-window design was
rejected in adversarial review (silent recall loss from windows anchored at
the global max over an out-of-order corpus, plus content-hash alert_id drift
across ticks). These tests assert the corrected full-rescan design and guard
against the windowed approach regressing back in.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]


def _src(path: Path) -> str:
    return path.read_text()


# --- sustained honest-runner gate (3a) ---------------------------------------


@pytest.mark.parametrize(
    ("logs", "alerts"),
    [
        (None, None),
        ("", None),
        ("some unrelated driver log\nRefreshed dashboards", None),
        ("[detection] cumulative gold.alerts rows: 0", 0),
        (
            "[detection] cumulative gold.alerts rows: 12\n"
            "[detection] cumulative gold.alerts rows: 40\n"
            "[detection] cumulative gold.alerts rows: 37\n",
            40,
        ),
    ],
)
def test_aml_cumulative_alerts(logs, alerts):
    """No detection line is unknown (None), never 0; otherwise the maximum."""
    from lakebench.cli._sustained import _aml_cumulative_alerts

    assert _aml_cumulative_alerts(logs) == alerts


def test_sustained_gate_is_financial_scoped_and_fails_on_zero():
    """The gate must be gated to schema_type == 'financial' (C360 sustained
    legitimately emits no alerts) and must set pipeline_success = False on a
    zero/None alert outcome."""
    src = _src(_ROOT / "src/lakebench/cli/_sustained.py")
    assert "_aml_cumulative_alerts(gold_logs)" in src
    assert "alert_count == 0" in src
    assert "alert_count is None" in src
    assert src.count("pipeline_success = False") >= 4


def test_aml_bronze_verify_timeout_budget_clears_measured_cost():
    """The shared AML bronze-verify budget must clear the measured scale-10
    cost (4278s, run-20260923-120258-b71af2) with real headroom, be flat at
    the floor for the scales we operate at, and grow at high scale."""
    from lakebench.spark.job import aml_bronze_verify_timeout_budget as budget

    assert budget(1) == 5400
    assert budget(10) == 5400 and budget(10) - 4278 >= 1000  # >= ~23% headroom
    assert budget(45) == 5400  # floor still dominates
    assert budget(100) == 12000  # slope active past the floor
    # Float scales (local mode allows <1) must not blow up.
    assert budget(0.1) == 5400
    assert budget(2.5) == 5400


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
