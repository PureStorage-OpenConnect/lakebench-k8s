"""A test that needs pyspark lives under tests/spark.

CI's unit legs install no pyspark, so a test outside tests/spark that
imports pyspark, or skips without it, never runs in CI: five such tests
went stale and failed for weeks while CI stayed green (LB-235). Under
tests/spark the Spark legs run it, and ``LB_REQUIRE_JARS=1`` there turns
any unlisted skip into a failure.
"""

from __future__ import annotations

import ast
from pathlib import Path

TESTS = Path(__file__).resolve().parent
SPARK_TIER = TESTS / "spark"
# Packages only the Spark legs install.
SPARK_ONLY = ("pyspark", "delta", "py4j")


# Calls that import a module named by their first argument.
_IMPORT_CALLS = {"importorskip", "import_module", "__import__"}


def _called_name(func: ast.expr) -> str | None:
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return None


def _spark_only(name: str) -> bool:
    return any(name == p or name.startswith(p + ".") for p in SPARK_ONLY)


def pyspark_uses(source: str) -> list[str]:
    """Each ``import``, ``from ... import``, ``pytest.importorskip``,
    ``importlib.import_module`` or ``__import__`` of a Spark-only package
    named by a string literal in *source*, at any depth, as ``line: text``.
    A module name built at run time is not seen."""
    found: list[tuple[int, str]] = []
    for node in ast.walk(ast.parse(source)):
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = [node.module]
        elif (
            isinstance(node, ast.Call)
            and _called_name(node.func) in _IMPORT_CALLS
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            names = [node.args[0].value]
        found += [(node.lineno, n) for n in names if _spark_only(n)]
    return [f"{line}: {name}" for line, name in sorted(found)]


def test_no_pyspark_test_outside_spark_tier():
    offenders = {}
    for path in sorted(TESTS.rglob("*.py")):
        if SPARK_TIER in path.parents:
            continue
        if path == Path(__file__).resolve():
            continue
        uses = pyspark_uses(path.read_text(encoding="utf-8"))
        if uses:
            offenders[str(path.relative_to(TESTS))] = uses
    assert not offenders, (
        "these tests need pyspark but are outside tests/spark, so CI's unit legs "
        f"skip them; move them to tests/spark: {offenders}"
    )


def test_guard_sees_each_form():
    src = (
        "import pyspark\n"
        "from pyspark.sql import functions as F\n"
        "import delta.tables\n"
        "def t():\n"
        "    import pytest\n"
        "    pytest.importorskip('pyspark')\n"
        "    from py4j.java_gateway import JavaGateway\n"
        "import pysparkling\n"
        "from . import pyspark\n"
        "importlib.import_module('pyspark.sql')\n"
        "__import__('delta')\n"
        "import_module('py4j')\n"
    )
    assert pyspark_uses(src) == [
        "1: pyspark",
        "2: pyspark.sql",
        "3: delta.tables",
        "6: pyspark",
        "7: py4j.java_gateway",
        "10: pyspark.sql",
        "11: delta",
        "12: py4j",
    ]
