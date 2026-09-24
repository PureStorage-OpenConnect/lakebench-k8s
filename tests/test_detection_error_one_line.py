"""A rule that fails with a multi-line Spark error must still show up in
rule_errors (2026-09-24 audit: the parser reads one line per rule, and the
error text spilled onto the next line, so the rule vanished)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from lakebench.metrics.collector import MetricsCollector

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def _one_line():
    spec = importlib.util.spec_from_file_location("lb_common_one_line", SCRIPTS / "common.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod.one_line


def test_multiline_error_is_parsed_as_rule_error():
    one_line = _one_line()
    raw = (
        "AnalysisException: [UNRESOLVED_COLUMN] cannot resolve 'dst'\nSQLSTATE: 42703\n  at line 1"
    )
    err = one_line(raw)
    assert "\n" not in err
    logs = "\n".join(
        [
            "[detection] W2_structuring: alerts=5 elapsed=1.0s",
            f"[detection] W7_cross_border_high_risk: alerts=0 error={err} elapsed=0.3s",
        ]
    )
    parsed = MetricsCollector().parse_driver_logs(logs, "gold-finalize")
    assert "W7_cross_border_high_risk" in parsed.rule_errors
    assert parsed.alerts_by_rule.get("W2_structuring") == 5


def test_without_one_line_the_rule_is_lost():
    """Documents the defect the helper fixes."""
    raw = "AnalysisException: bad\nSQLSTATE: 42703"
    logs = f"[detection] W7_cross_border_high_risk: alerts=0 error={raw} elapsed=0.3s"
    parsed = MetricsCollector().parse_driver_logs(logs, "gold-finalize")
    assert "W7_cross_border_high_risk" not in parsed.rule_errors
