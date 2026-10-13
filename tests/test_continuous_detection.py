"""The continuous AML run fails on zero alerts, and the AML bronze-verify
budget clears its measured cost.

The sustained runner reads the cumulative alert count from the gold driver
log and fails a financial run that produced none (a log with no detection
line is unknown, not zero). The bronze-verify timeout budget is checked
against the measured scale-10 cost.
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
