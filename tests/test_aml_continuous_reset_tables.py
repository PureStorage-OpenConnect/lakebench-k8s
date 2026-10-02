"""The AML continuous reset and the stream's fresh-checkpoint refusal cover
every silver table the continuous stream writes, and stay in step when one
is added. Executed proof: tests/spark/test_aml_continuous_reset_spark.py."""

from __future__ import annotations

import ast
import re
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"
_ENV = re.compile(r'^(SILVER_[A-Z_]+) = env\("[A-Z_]+", "(silver\.[a-z_]+)"\)$', re.M)


def _silver_constants(script):
    return dict(_ENV.findall((_SCRIPTS / script).read_text()))


def _written(script):
    """SILVER_* constants the stream writes: append, MERGE INTO or a DDL."""
    src = (_SCRIPTS / script).read_text()
    names = set(re.findall(r"\{(SILVER_[A-Z_]+)\}\"?\)?\.append\(\)", src))
    names |= set(re.findall(r"MERGE INTO \{CATALOG\}\.\{(SILVER_[A-Z_]+)\}", src))
    return names


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


def test_the_reset_drops_every_silver_table_the_stream_defines():
    stream = _silver_constants("silver_stream_financial.py")
    reset = _silver_constants("bronze_verify_financial.py")
    dropped = {
        reset[n] for n in _tuple_names("bronze_verify_financial.py", "CONTINUOUS_SILVER_TABLES")
    }
    assert set(stream.values()) <= dropped


def test_the_refusal_checks_every_silver_table_the_stream_defines():
    stream = _silver_constants("silver_stream_financial.py")
    checked = _tuple_names("silver_stream_financial.py", "refuse_reused_silver")
    assert set(stream) <= checked


def test_every_table_the_stream_writes_has_a_constant():
    written = _written("silver_stream_financial.py")
    assert written, "no write found: the pattern no longer matches the script"
    assert written <= set(_silver_constants("silver_stream_financial.py"))
