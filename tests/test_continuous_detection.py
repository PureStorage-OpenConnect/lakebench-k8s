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
        # no detection line of any shape is unknown (None), never 0
        (None, None),
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
    ("logs", "alerts", "fails"),
    [
        ("[detection] cumulative gold.alerts rows: 0", 0, True),  # zero alerts
        ("Refreshed dashboards", None, True),  # detection did not run
        (None, None, True),  # no gold log
        ("[detection] cumulative gold.alerts rows: 40", 40, False),  # positive alerts pass
    ],
)
def test_alert_gate(logs, alerts, fails):
    from lakebench.cli._sustained import _aml_alert_gate_failure

    failure = _aml_alert_gate_failure(logs, alerts)
    assert (failure is not None) == fails, (logs, failure)


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
