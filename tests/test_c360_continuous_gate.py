"""c360 continuous runs that moved no data must fail (LB-044 for c360;
2026-09-24 audit: the honest gate existed only for AML)."""

from lakebench.cli._sustained import _c360_continuous_gate_problems


def test_zero_rows_fails():
    probs = _c360_continuous_gate_problems({"bronze-ingest": 10, "silver-stream": 0})
    assert len(probs) == 1 and "silver-stream processed 0 rows" in probs[0]


def test_missing_logs_fail():
    assert _c360_continuous_gate_problems({"bronze-ingest": None, "silver-stream": 5})


def test_data_moving_passes_and_absent_stage_is_ignored():
    assert _c360_continuous_gate_problems({"bronze-ingest": 10, "silver-stream": 7}) == []
    assert _c360_continuous_gate_problems({"silver-stream": 7}) == []
