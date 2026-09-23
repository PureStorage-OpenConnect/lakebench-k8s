"""Tests for the continuous FAML detection spine (LB-127).

The continuous gold stage (gold_refresh_financial) re-runs detection over the
full silver corpus each tick, reusing the batch DELETE-per-rule + INSERT
driver (run_detection_rules), and the sustained runner fails a FAML run that
produced zero alerts (closes LB-044 for FAML). These scripts execute inside a
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
    (W2/W3/W4). W1 (per-tick graph too costly), W7 (silver.entities not
    maintained in continuous), and W8 (90-day dormancy gap) are recorded as
    SKIPPED -- not omitted -- so score renders their typologies "not run"
    instead of a false 0% recall."""
    tree = ast.parse(_src(GOLD_REFRESH_PATH))
    run = _tuple_values(tree, "CONTINUOUS_RULES")
    skip = _tuple_values(tree, "CONTINUOUS_SKIPPED_RULES")
    assert run == {"W2_structuring", "W3_round_tripping", "W4_risk_propagation"}
    assert skip == {
        "W1_connected_components",
        "W7_cross_border_high_risk",
        "W8_dormant_reactivation",
    }
    assert not (run & skip)


def test_continuous_reuses_batch_detection_driver():
    """Continuous detection must call the batch run_detection_rules, not fork
    the rules or reimplement a windowed detector. One driver, both modes."""
    src = _src(GOLD_REFRESH_PATH)
    assert "from gold_finalize_financial import" in src
    assert "run_detection_rules(" in src
    assert "rules=CONTINUOUS_RULES" in src
    assert "skipped_rules=CONTINUOUS_SKIPPED_RULES" in src
    tree = ast.parse(src)
    local_defs = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert not any(name[0] == "w" and len(name) > 1 and name[1].isdigit() for name in local_defs), (
        local_defs
    )


def test_continuous_is_full_rescan_not_windowed():
    """Regression guard for the rejected sliding-window design: no data-clock
    window, no Python-side timedelta cutoff, no MERGE keyed on the drifting
    content hash -- all three caused silent recall loss / duplicate alerts."""
    src = _src(GOLD_REFRESH_PATH)
    assert "timedelta(" not in src
    assert "data_clock_max" not in src
    assert "MERGE INTO" not in src


def test_bootstraps_all_gold_tables_in_continuous_mode():
    """gold_finalize never runs in continuous mode, so gold_refresh must
    bootstrap ALL gold tables the tick touches, not just alerts+status: the
    baseline refresh DELETEs gold.daily_dashboards and the derived projections
    overwrite gold.risk_scores / gold.entity_clusters. Creating only
    alerts+status (the reviewed-and-fixed HIGH) fails every tick on a fresh
    continuous catalog. Plus the detected_ts upgrade guard."""
    src = _src(GOLD_REFRESH_PATH)
    for ddl in ("DDL_ALERTS", "DDL_RISK", "DDL_CLUSTERS", "DDL_DASH", "DDL_STATUS"):
        assert ddl in src, f"continuous bootstrap missing {ddl}"
    # All five must be imported from the batch module and issued in the bootstrap.
    assert "for ddl in (DDL_ALERTS, DDL_RISK, DDL_CLUSTERS, DDL_DASH, DDL_STATUS)" in src
    assert "ADD COLUMNS (detected_ts TIMESTAMP)" in src
    assert '"detected_ts" not in cols' in src


def test_continuous_logs_cumulative_alert_count_for_gate():
    """The honest-runner gate parses this exact line; it must be emitted every
    tick with a bare integer."""
    src = _src(GOLD_REFRESH_PATH)
    assert 'log(f"[detection] cumulative gold.alerts rows: {total_alerts}")' in src


# --- batch driver refactor (reused by continuous) ----------------------------


def test_run_detection_rules_accepts_rules_and_skipped_lists():
    """run_detection_rules must accept rules + skipped_rules so the continuous
    loop can reuse it; default (None) preserves the full batch set."""
    tree = ast.parse(_src(GOLD_FINALIZE_PATH))
    fn = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "run_detection_rules"
    )
    argnames = [a.arg for a in fn.args.args]
    assert "rules" in argnames
    assert "skipped_rules" in argnames


def test_batch_driver_dedups_alert_id_before_write():
    """Symmetric dedup guard: a rule whose join fans out could emit two rows
    sharing a deterministic alert_id; dropDuplicates prevents a duplicate row
    landing in gold.alerts (adversarial-review Finding 3)."""
    src = _src(GOLD_FINALIZE_PATH)
    assert 'dropDuplicates(["alert_id"])' in src


def test_skipped_rules_recorded_as_skipped_status():
    """Skipped rules must be written to detection_status as status='skipped'
    so score marks their typologies not-run, not 0%."""
    src = _src(GOLD_FINALIZE_PATH)
    assert 'status_rows.append((rule_id, "skipped", "mode-excluded"' in src


# --- deterministic alert_id (3d) ---------------------------------------------


def test_alert_id_helper_null_guarded():
    """concat_ws skips null args, which would collapse distinct alerts onto a
    shorter key; the helper coalesces entity and txn-set to sentinels
    (adversarial-review Finding 4)."""
    src = _src(_ROOT / "src/lakebench/spark/scripts/detection_rules.py")
    assert "__NULL_ENTITY__" in src
    assert "__NULL_TXNS__" in src


# --- sustained honest-runner gate (3a) ---------------------------------------


def test_faml_cumulative_alerts_none_when_no_logs():
    from lakebench.cli._sustained import _faml_cumulative_alerts

    assert _faml_cumulative_alerts(None) is None
    assert _faml_cumulative_alerts("") is None


def test_faml_cumulative_alerts_none_when_no_detection_line():
    from lakebench.cli._sustained import _faml_cumulative_alerts

    assert _faml_cumulative_alerts("some unrelated driver log\nRefreshed dashboards") is None


def test_faml_cumulative_alerts_zero_and_max():
    from lakebench.cli._sustained import _faml_cumulative_alerts

    assert _faml_cumulative_alerts("[detection] cumulative gold.alerts rows: 0") == 0
    logs = (
        "[detection] cumulative gold.alerts rows: 12\n"
        "[detection] cumulative gold.alerts rows: 40\n"
        "[detection] cumulative gold.alerts rows: 37\n"
    )
    assert _faml_cumulative_alerts(logs) == 40


def test_sustained_gate_is_financial_scoped_and_fails_on_zero():
    """The gate must be gated to schema_type == 'financial' (C360 sustained
    legitimately emits no alerts) and must set pipeline_success = False on a
    zero/None alert outcome."""
    src = _src(_ROOT / "src/lakebench/cli/_sustained.py")
    assert "_faml_cumulative_alerts(gold_logs)" in src
    assert "alert_count == 0" in src
    assert "alert_count is None" in src
    assert src.count("pipeline_success = False") >= 4


def test_sustained_failure_raises_nonzero_exit():
    """Adversarial-review P0 regression guard: flagging pipeline_success=False
    is NOT enough -- the function MUST raise typer.Exit(1) so an exit-code-only
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
            if body_src and "raise typer.Exit(1)" in body_src:
                found_exit_guard = True
    assert found_exit_guard, "no `if not pipeline_success: raise typer.Exit(1)` guard found"


def test_sustained_success_panel_is_guarded():
    """Adversarial-review P1: the green 'completed' panel must not print on a
    failed run. It must sit under `if pipeline_success:`."""
    src = _src(_ROOT / "src/lakebench/cli/_sustained.py")
    idx = src.index("Sustained pipeline completed!")
    prefix = src[:idx]
    assert "if pipeline_success:" in prefix


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
