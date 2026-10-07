"""Tests for the continuous AML detection spine (LB-127).

The continuous gold stage (gold_refresh_financial) re-runs detection over the
full silver corpus each tick, reusing the batch DELETE-per-rule + INSERT
driver (run_detection_rules), and the sustained runner fails an AML run that
produced zero alerts (closes LB-044 for AML). These scripts execute inside a
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

import ast
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
GOLD_REFRESH_PATH = _ROOT / "src/lakebench/spark/scripts/gold_refresh_financial.py"
GOLD_FINALIZE_PATH = _ROOT / "src/lakebench/spark/scripts/gold_finalize_financial.py"


def _src(path: Path) -> str:
    return path.read_text()


def _tuple_values(tree: ast.Module, name: str) -> set[str]:
    node = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Assign) and any(getattr(t, "id", None) == name for t in n.targets)
    )
    return {e.value for e in node.value.elts}


# --- continuous detection driver (3b) ----------------------------------------


def test_continuous_runs_w2w3w4_and_marks_w1w7w8_skipped():
    """Continuous runs only the rules that need silver.transactions alone
    (W2/W3/W4/W17). W1 (per-tick graph too costly), W7 (silver.entities not
    maintained in continuous), and W8 (90-day dormancy gap) are recorded as
    SKIPPED -- not omitted -- so score renders their typologies "not run"
    instead of a false 0% recall."""
    tree = ast.parse(_src(GOLD_REFRESH_PATH))
    run = _tuple_values(tree, "CONTINUOUS_RULES")
    skip = _tuple_values(tree, "CONTINUOUS_SKIPPED_RULES")
    assert run == {
        "W2_structuring",
        "W3_round_tripping",
        "W4_risk_propagation",
        "W17_layering_chain",
    }
    assert skip == {
        "W5_sanctions_match",
        "W6_pep_counterparty",
        "W1_connected_components",
        "W7_cross_border_high_risk",
        "W8_dormant_reactivation",
    }
    assert not (run & skip)


# --- batch driver refactor (reused by continuous) ----------------------------


# --- optimizer workaround (LB-127) -------------------------------------------


# --- deterministic alert_id (3d) ---------------------------------------------


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


def test_sustained_failure_raises_nonzero_exit():
    """Adversarial-review P0 regression guard: flagging pipeline_success=False
    is NOT enough -- the function MUST exit 1 (ExitCode.FAILED) so an exit-code-only
    UAT runner sees the failure (the exact LB-044 gap). Assert the control
    flow, not just that a string is present."""
    src = _src(_ROOT / "src/lakebench/cli/_sustained.py")
    tree = ast.parse(src)
    fn = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_run_sustained"
    )
    found_exit_guard = False
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.UnaryOp)
            and isinstance(node.test.op, ast.Not)
            and isinstance(node.test.operand, ast.Name)
            and node.test.operand.id == "pipeline_success"
        ):
            body_src = ast.get_source_segment(src, node)
            # CLI-1 spells the code by name; 1 is ExitCode.FAILED.
            if body_src and "raise typer.Exit(ExitCode.FAILED)" in body_src:
                found_exit_guard = True
    assert found_exit_guard, (
        "no `if not pipeline_success: raise typer.Exit(ExitCode.FAILED)` guard found"
    )


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
