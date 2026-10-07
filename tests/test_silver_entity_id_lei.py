"""LB-101 (PR-L): silver_build_financial must key entity_id on
``dbtr.id.lei`` / ``cdtr.id.lei`` (bronze's role-independent identity)
before falling back to name+country+city, so a single real entity does
not split into two silver entities across dbtr/cdtr sides.

Pyspark is not installed in the test environment so these are AST-based
source-inspection tests. Adversarial review found regex-based
inspection too brittle -- a single ``def`` reorder passed the tests
while reverting the fix. This version parses the module with ``ast``
and asserts on the actual function nodes.
"""

from __future__ import annotations

import ast
from pathlib import Path

_SRC_PATH = Path("src/lakebench/spark/scripts/silver_build_financial.py")
_TREE = ast.parse(_SRC_PATH.read_text())


def _find_function(name: str) -> ast.FunctionDef:
    for node in ast.walk(_TREE):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name!r} not found in {_SRC_PATH}")


def _iter_calls_of(fn: ast.FunctionDef, name: str):
    for node in ast.walk(fn):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name:
            yield node


def _is_lei_column_ref(node: ast.AST) -> bool:
    """True iff ``node`` is a ``col("<prefix>.id.lei")`` call.

    Adversarial-review finding F2: an earlier check accepted any
    Constant containing ``"lei"`` (including ``lit("lei")`` passed as a
    city_col positional arg). Tighten to require a ``col("...id.lei")``
    call specifically -- the only shape the emitted bronze schema
    exposes and the only shape the LB-101 fix uses.
    """
    if not (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "col"
    ):
        return False
    if len(node.args) != 1:
        return False
    arg = node.args[0]
    if not (isinstance(arg, ast.Constant) and isinstance(arg.value, str)):
        return False
    return arg.value.endswith(".id.lei")


def _call_has_lei_arg(call: ast.Call) -> bool:
    """Positional or ``lei_col=...`` kwarg carrying a ``col("...id.lei")``."""
    for a in call.args:
        if _is_lei_column_ref(a):
            return True
    for kw in call.keywords:
        if kw.arg == "lei_col" and _is_lei_column_ref(kw.value):
            return True
    return False


def test_build_transactions_passes_lei_for_both_sides():
    """Both originator (dbtr) and beneficiary (cdtr) sides of
    silver.transactions must derive entity_id from LEI."""
    fn = _find_function("build_transactions")
    calls = list(_iter_calls_of(fn, "_entity_id_from"))
    assert len(calls) == 2, (
        f"expected two _entity_id_from calls in build_transactions (dbtr + cdtr), got {len(calls)}"
    )
    for call in calls:
        assert _call_has_lei_arg(call), (
            "each _entity_id_from call in build_transactions must pass "
            "a col('...id.lei') column; without it the bipartite split "
            "persists"
        )


def test_aml_multicycle_is_full_rebuild_not_append():
    """LB-121: AML silver_build must NOT append on LB_SILVER_INCREMENTAL.
    AML bronze is cumulative across cycles, so appending would double-count
    the prior corpus. The mode contract is a full rebuild (overwrite) every
    cycle; this test locks that in so nobody copies the Customer 360 append
    pattern into the AML silver builder.
    """
    body = _SRC_PATH.read_text()
    # The flag is acknowledged (not silently ignored)...
    assert "LB_SILVER_INCREMENTAL" in body
    # ...but the write path stays full-overwrite: the only write helper is
    # _replace_data using .overwrite(lit(True)), and there is no bare
    # .append() on the silver tables in the main build path.
    assert ".overwrite(lit(True))" in body
    assert ".append()" not in body, (
        "silver_build_financial must not append; AML multi-cycle is a full "
        "rebuild from cumulative bronze (LB-121)"
    )
