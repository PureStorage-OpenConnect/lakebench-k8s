"""AML-2 changes W4's alerts (sorted, capped related lists) and adds
truncation evidence to W2, W4 and the W5 rescreen, so AML results change:
the financial workload version moves to aml-2 (SPEC section 9, K17), and a
stored aml-1 record never compares with an aml-2 one."""

from __future__ import annotations

from lakebench.metrics.experiment import WORKLOAD_VERSIONS


def test_financial_workload_is_aml_2():
    assert WORKLOAD_VERSIONS["financial"] == "aml-2"
