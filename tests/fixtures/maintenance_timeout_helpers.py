"""Shared test helpers moved from tests/test_continuous_maintenance_timeout.py (imported by several test files)."""

from __future__ import annotations

import ast
import inspect


def loop_call_keywords(func_name: str) -> dict[str, str]:
    """Keyword arguments (as source) of the one call to func_name in _run_sustained."""
    import lakebench.cli._sustained as sus

    tree = ast.parse(inspect.getsource(sus._run_sustained))
    calls = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == func_name
    ]
    assert len(calls) == 1, f"expected one {func_name} call in the loop, found {len(calls)}"
    return {k.arg: ast.unparse(k.value) for k in calls[0].keywords if k.arg}
