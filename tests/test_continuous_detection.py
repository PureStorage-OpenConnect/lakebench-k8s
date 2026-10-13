"""The continuous AML run fails on zero alerts, and the AML bronze-verify
budget clears its measured cost.

The sustained runner reads the cumulative alert count from the gold driver
log and fails a financial run that produced none (a log with no detection
line is unknown, not zero). The bronze-verify timeout budget is checked
against the measured scale-10 cost.
"""

from __future__ import annotations

import pytest

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


def test_alert_gate_runs_for_financial_only():
    """Customer360 sustained runs emit no alerts by design and are exempt."""
    from lakebench.cli._sustained import _aml_alert_gate_applies
    from lakebench.config.schema import WorkloadSchema

    assert _aml_alert_gate_applies(WorkloadSchema.FINANCIAL.value) is True
    assert _aml_alert_gate_applies(WorkloadSchema.CUSTOMER360.value) is False


def test_alert_gate_fails_on_zero_alerts():
    from lakebench.cli._sustained import _aml_alert_gate_failure

    failure = _aml_alert_gate_failure("[detection] cumulative gold.alerts rows: 0", 0)
    assert failure is not None
    assert failure[1] == "AML continuous gate: detection produced 0 alerts"


def test_alert_gate_fails_when_detection_did_not_run():
    from lakebench.cli._sustained import _aml_alert_gate_failure

    failure = _aml_alert_gate_failure("Refreshed dashboards", None)
    assert failure is not None
    assert failure[1] == "AML continuous gate: detection did not run"


def test_alert_gate_fails_without_gold_logs():
    from lakebench.cli._sustained import _aml_alert_gate_failure

    failure = _aml_alert_gate_failure(None, None)
    assert failure is not None
    assert failure[1] == "AML continuous gate: no gold-refresh driver log"


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
