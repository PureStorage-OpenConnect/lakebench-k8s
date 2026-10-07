"""The AML continuous reset and the stream's fresh-checkpoint refusal cover
every silver table the continuous stream writes, and stay in step when one
is added. Executed proof: tests/spark/test_aml_continuous_reset_spark.py."""

from __future__ import annotations

import ast
import re
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def _tuple_names(script, func_or_const):
    tree = ast.parse((_SCRIPTS / script).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == func_or_const for t in node.targets
        ):
            return {e.id for e in node.value.elts}
        if isinstance(node, ast.FunctionDef) and node.name == func_or_const:
            return {
                n.id
                for n in ast.walk(node)
                if isinstance(n, ast.Name) and n.id.startswith("SILVER_")
            }
    raise AssertionError(f"{func_or_const} not found in {script}")


def _func(script, name):
    tree = ast.parse((_SCRIPTS / script).read_text())
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)


def test_the_set_matches_the_tables_the_stream_bootstraps():
    """The stream creates each table it owns at startup; that DDL list is the
    authoritative set, so a new table there must join the reset and refusal."""
    main = _func("silver_stream_financial.py", "main")
    boot = None
    for loop in (n for n in ast.walk(main) if isinstance(n, ast.For)):
        if isinstance(loop.iter, ast.Tuple) and all(
            isinstance(e, ast.Tuple) and len(e.elts) == 2 for e in loop.iter.elts
        ):
            names = [e.elts[0].value for e in loop.iter.elts if isinstance(e.elts[0], ast.Constant)]
            if "transactions" in names:
                boot = names
    assert boot, "the stream's bootstrap DDL loop was not found"
    reset = _tuple_names("bronze_verify_financial.py", "CONTINUOUS_SILVER_TABLES")
    assert (
        len(boot)
        == len(reset)
        == len(_tuple_names("silver_stream_financial.py", "refuse_reused_silver"))
    )


def test_the_reset_drops_the_gold_tables_gold_refresh_writes():
    """gold-refresh bootstraps one table per DDL in _bootstrap_gold_tables;
    the reset's CONTINUOUS_GOLD_TABLES must name each of them, and its loop
    must drop them."""
    reset = _func("bronze_verify_financial.py", "_continuous_reset")
    loops = [
        n
        for n in ast.walk(reset)
        if isinstance(n, ast.For)
        and isinstance(n.iter, ast.Name)
        and n.iter.id == "CONTINUOUS_GOLD_TABLES"
        and any(
            isinstance(c, ast.Call) and getattr(c.func, "id", "") == "_drop_owned_table"
            for c in ast.walk(n)
        )
    ]
    assert loops, "_continuous_reset no longer drops CONTINUOUS_GOLD_TABLES"
    # The DDL names gold-refresh bootstraps, mapped to the env keys of the
    # tables they create (gold_finalize_financial defines both).
    boot = _func("gold_refresh_financial.py", "_bootstrap_gold_tables")
    ddls = {
        e.id
        for loop in ast.walk(boot)
        if isinstance(loop, ast.For) and isinstance(loop.iter, ast.Tuple)
        for e in loop.iter.elts
        if isinstance(e, ast.Name)
    }
    assert ddls, "gold-refresh's bootstrap DDL loop was not found"
    finalize = (_SCRIPTS / "gold_finalize_financial.py").read_text()
    ddl_table = dict(
        re.findall(
            r"^(DDL_[A-Z]+) = f\"\"\"\s*CREATE TABLE IF NOT EXISTS \{CATALOG\}\.\{(GOLD_[A-Z]+)\}",
            finalize,
            re.M,
        )
    )
    gold_env = {
        const: (key, default)
        for const, key, default in re.findall(
            r'^(GOLD_[A-Z]+) = env\("([A-Z_]+)", "([a-z_.]+)"\)', finalize, re.M
        )
    }
    wanted = {gold_env[ddl_table[d]] for d in ddls}
    tree = ast.parse((_SCRIPTS / "bronze_verify_financial.py").read_text())
    tup = next(
        n.value
        for n in ast.walk(tree)
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "CONTINUOUS_GOLD_TABLES" for t in n.targets)
    )
    dropped = {(c.args[0].value, c.args[1].value) for c in tup.elts if isinstance(c, ast.Call)}
    assert wanted == dropped
