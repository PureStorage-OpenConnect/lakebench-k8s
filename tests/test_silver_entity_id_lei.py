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


def test_entity_id_from_accepts_lei_column():
    """Signature must accept ``lei_col`` (any default). Required for
    call sites to pass ``dbtr.id.lei``."""
    fn = _find_function("_entity_id_from")
    param_names = [a.arg for a in fn.args.args]
    assert "lei_col" in param_names, (
        f"_entity_id_from must accept lei_col; got signature: {param_names}"
    )


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


def test_build_accounts_passes_lei_for_both_sides():
    """Same plumbing for build_accounts so silver.accounts.holder_entity_id
    does not coin-flip between the two halves of one entity."""
    fn = _find_function("build_accounts")
    calls = list(_iter_calls_of(fn, "_entity_id_from"))
    assert len(calls) == 2, (
        f"expected two _entity_id_from calls in build_accounts (dbtr + cdtr), got {len(calls)}"
    )
    for call in calls:
        assert _call_has_lei_arg(call), (
            "each _entity_id_from call in build_accounts must pass a col('...id.lei') column"
        )


def _identifier_load_count(fn: ast.FunctionDef, name: str) -> int:
    """Count ``ast.Name(id=name, ctx=Load)`` occurrences in ``fn`` body
    excluding the docstring. Argument bindings (``ast.arg``) do NOT
    count; only real reads."""
    n = 0
    for node in ast.walk(fn):
        if isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Load):
            n += 1
    return n


def test_entity_id_from_branches_on_lei():
    """Body must guard on lei_col in at least two places: a None check
    on the parameter itself, and a non-empty check on the trimmed
    value. Adversarial-review finding F3: an earlier check
    ``body.count("lei_col") >= 2`` counted docstring mentions, so a
    revert that deleted the branch but kept the docstring passed
    green. This version counts actual identifier reads in the AST.
    """
    fn = _find_function("_entity_id_from")
    reads = _identifier_load_count(fn, "lei_col")
    assert reads >= 2, (
        f"_entity_id_from body must read lei_col in >= 2 places "
        f"(None guard + non-empty check on trimmed value); got {reads} reads"
    )


def test_entity_id_from_retains_name_hash_fallback():
    """The name-hash fallback must remain for rows with missing LEI
    (historical bronze, real-world non-financial-institution parties).
    Two markers: ``xxhash64`` for the hash, ``concat_ws`` for the
    composite key."""
    fn = _find_function("_entity_id_from")
    body_src = ast.unparse(fn)
    assert "xxhash64" in body_src, "name-hash fallback (xxhash64) missing"
    assert "concat_ws" in body_src, "composite-key builder (concat_ws) missing"


def test_silver_entities_lei_still_null_pending_enrichment():
    """LB-101 scope note: this PR fixes IDENTITY (entity_id keying),
    not enrichment. silver.entities.lei / .bic / .country still project
    NULL. If a future PR fills these in, delete this test."""
    fn = _find_function("build_entities")
    body_src = ast.unparse(fn)
    assert "alias('lei')" in body_src or 'alias("lei")' in body_src, (
        "silver.entities.lei projection alias missing from build_entities"
    )
    assert "lit(None)" in body_src, "silver.entities.lei still expected to project as NULL"


def test_faml_multicycle_is_full_rebuild_not_append():
    """LB-121: FAML silver_build must NOT append on LB_SILVER_INCREMENTAL.
    FAML bronze is cumulative across cycles, so appending would double-count
    the prior corpus. The mode contract is a full rebuild (overwrite) every
    cycle; this test locks that in so nobody copies the Customer 360 append
    pattern into the FAML silver builder.
    """
    body = _SRC_PATH.read_text()
    # The flag is acknowledged (not silently ignored)...
    assert "LB_SILVER_INCREMENTAL" in body
    # ...but the write path stays full-overwrite: the only write helper is
    # _replace_data using .overwrite(lit(True)), and there is no bare
    # .append() on the silver tables in the main build path.
    assert ".overwrite(lit(True))" in body
    assert ".append()" not in body, (
        "silver_build_financial must not append; FAML multi-cycle is a full "
        "rebuild from cumulative bronze (LB-121)"
    )
