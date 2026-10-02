"""AML-1: the gold drivers never take meaning from the Spark job group.

run_detection_rules sets a job group per rule (for the stage profile) and
restores the caller's after each rule. The silver stream reads the job group
as its stream run id (common.stream_run_id); a gold driver that did the same
would see the rule's group instead. Both gold drivers stay off it.
"""

from __future__ import annotations

import ast
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"


def _called_names(path: Path) -> set[str]:
    names = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Call):
            f = node.func
            names.add(f.id if isinstance(f, ast.Name) else getattr(f, "attr", ""))
    return names


def test_gold_drivers_do_not_read_the_stream_run_id():
    for name in ("gold_finalize_financial.py", "gold_refresh_financial.py"):
        assert "stream_run_id" not in _called_names(SCRIPTS / name), name


def test_rule_group_is_restored_in_the_rule_loop_finally():
    tree = ast.parse((SCRIPTS / "gold_finalize_financial.py").read_text())
    fn = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_detection_rules"
    )
    finals = [stmt for node in ast.walk(fn) if isinstance(node, ast.Try) for stmt in node.finalbody]
    called = {
        s.value.func.id
        for s in finals
        if isinstance(s, ast.Expr)
        and isinstance(s.value, ast.Call)
        and isinstance(s.value.func, ast.Name)
    }
    assert {"rule_stage_profile", "_restore_job_group"} <= called, called
