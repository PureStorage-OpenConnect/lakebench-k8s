"""The continuous AML run fails on zero alerts, and the AML bronze-verify
budget clears its measured cost.

The sustained runner reads the cumulative alert count from the gold driver
log and fails a financial run that produced none (a log with no detection
line is unknown, not zero). The bronze-verify timeout budget is checked
against the measured scale-10 cost.
"""

from __future__ import annotations

import pytest

# --- sustained alert gate ----------------------------------------------------


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


@pytest.mark.parametrize(
    ("logs", "alerts"),
    [
        ("[detection] cumulative gold.alerts rows: 0", 0),
        ("Refreshed dashboards", None),
        (None, None),
    ],
    ids=["zero_alerts", "detection_did_not_run", "no_gold_log"],
)
def test_alert_gate_fails(logs, alerts):
    from lakebench.cli._sustained import _aml_alert_gate_failure

    assert _aml_alert_gate_failure(logs, alerts) is not None


def test_alert_gate_passes_on_positive_alerts():
    from lakebench.cli._sustained import _aml_alert_gate_failure

    assert _aml_alert_gate_failure("[detection] cumulative gold.alerts rows: 40", 40) is None


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
