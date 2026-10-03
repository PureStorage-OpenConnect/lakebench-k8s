"""AML-1: the AML gold code never takes meaning from the Spark job group.

run_detection_rules (with profile_stages, batch gold-finalize) sets a job
group per rule for the stage profile and restores the caller's after each
rule. The silver stream reads the job group as its stream run id
(common.stream_run_id, common.replay_possible); gold code that did the same
would see the rule's group instead. The gold drivers and the modules they
run (detection rules, TM operations) neither call those helpers nor read the
job-group property, except to save and restore it.
"""

from __future__ import annotations

import ast
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"
GOLD_SIDE = (
    "gold_finalize_financial.py",
    "gold_refresh_financial.py",
    "detection_rules.py",
    "tm_operations.py",
)


def _calls(tree: ast.AST) -> list[tuple[str, ast.Call]]:
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            f = node.func
            out.append((f.id if isinstance(f, ast.Name) else getattr(f, "attr", ""), node))
    return out


def test_gold_side_never_reads_the_job_group():
    for name in GOLD_SIDE:
        tree = ast.parse((SCRIPTS / name).read_text())
        called = {n for n, _ in _calls(tree)}
        assert not {"stream_run_id", "replay_possible"} & called, name
        readers = [
            fn.name
            for fn in ast.walk(tree)
            if isinstance(fn, ast.FunctionDef)
            and any(n == "getLocalProperty" for n, _ in _calls(fn))
        ]
        assert readers in ([], ["_job_group_props"]), (name, readers)


def test_rule_group_is_restored_in_the_rule_loop_finally():
    tree = ast.parse((SCRIPTS / "gold_finalize_financial.py").read_text())
    fn = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_detection_rules"
    )
    in_finally = {
        name
        for node in ast.walk(fn)
        if isinstance(node, ast.Try)
        for stmt in node.finalbody
        for name, _ in _calls(stmt)
    }
    assert {"rule_stage_profile", "_restore_job_group"} <= in_finally, in_finally


def test_only_batch_gold_finalize_profiles_stages():
    """The continuous tick calls run_detection_rules without profile_stages,
    so its per-rule commit times and time to detect are as before."""
    for name, want in (("gold_finalize_financial.py", True), ("gold_refresh_financial.py", False)):
        tree = ast.parse((SCRIPTS / name).read_text())
        calls = [c for n, c in _calls(tree) if n == "run_detection_rules"]
        assert calls, name
        for c in calls:
            kw = {k.arg: k.value for k in c.keywords}
            got = (
                isinstance(kw.get("profile_stages"), ast.Constant)
                and kw["profile_stages"].value is True
            )
            assert got is want, name
