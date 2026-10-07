"""G1: SALTED strategy is refused at parse time.

The old code accepted `salted`, dispatched to SIMPLE, and logged
`Strategy: salted` -- a metrics-tag lie that violates invariant 5
(published evidence identifies what produced it). The new code raises
`SilverAbort` before any log line names the strategy.

Both silver_build.py and silver_build_delta.py have their own
`get_strategy_override` helper; test both.

The silver mains import pyspark at module top level; pyspark is not
guaranteed in the unit tier, so the resolver and its enum are lifted from
each script and run against a stub conf.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"
pytestmark = pytest.mark.usefixtures("load_script")


# --- Direct behaviour: the common-only path raises SilverAbort ---


def test_silver_abort_symbol_is_public_from_common():
    """SilverAbort is exported from common.py so silver_build can import it.

    ``common`` comes from this test's load_script namespace, the same copy
    silver_build.py would import.
    """
    common = importlib.import_module("common")
    assert hasattr(common, "SilverAbort")
    assert issubclass(common.SilverAbort, RuntimeError)


# --- Each script's resolver refuses SALTED before any log line names it ---


def _strategy_resolver(script: str):
    """get_strategy_override and SilverStrategy lifted from *script* (the
    silver mains import pyspark at module top level)."""
    import ast
    import os
    from enum import Enum

    tree = ast.parse((_SCRIPTS_DIR / script).read_text())
    keep = [
        n
        for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.ClassDef))
        and n.name in ("get_strategy_override", "SilverStrategy")
    ]
    logged: list[str] = []
    ns = {
        "os": os,
        "Enum": Enum,
        "SilverAbort": importlib.import_module("common").SilverAbort,
        "log": logged.append,
    }
    exec(compile(ast.Module(keep, []), script, "exec"), ns)  # noqa: S102
    return ns["get_strategy_override"], ns["SilverAbort"], logged


class _Conf:
    def __init__(self, value):
        self.value = value

    def get(self, key, default=None):
        return self.value if key == "spark.lb.silver.strategy" else default


@pytest.mark.parametrize("script", ["silver_build.py", "silver_build_delta.py"])
def test_override_refuses_salted(script, monkeypatch):
    """A salted override aborts; it never runs as SIMPLE under a salted tag."""
    monkeypatch.delenv("LB_SILVER_STRATEGY", raising=False)
    resolve, abort, logged = _strategy_resolver(script)
    spark = type("S", (), {})()
    for value in ("salted", "SALTED"):
        spark.conf = _Conf(value)
        with pytest.raises(abort):
            resolve(spark)
    assert logged == []
    spark.conf = _Conf(None)
    monkeypatch.setenv("LB_SILVER_STRATEGY", "salted")
    with pytest.raises(abort):
        resolve(spark)
