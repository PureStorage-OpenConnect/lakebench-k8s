"""CI regression net for the AML seed guards.

Held-out AML data must never be used during development. The held-out
invariant is stated in ``docs/DESIGN.md``; the seed protocol lives at
``docs/internal/aml-protocol.md``. Two guards keep the invariant true
and both must exist. This test greps the source tree so an accidental
removal of either guard fails CI with a clear message.

Guard 1 -- load-time refusal. ``check_seed()`` is defined in
``src/lakebench/config/datagen_seed.py`` and is called (via
``resolve_seed()``) from the ``WorkloadConfig`` validator in
``src/lakebench/config/schema.py``. A LakebenchConfig that names a
protected corpus_role, or a spent seed, is refused as soon as it loads.

Guard 2 -- render-time re-check. Every deployment path that materialises
a datagen seed goes through ``config_seed()`` (which wraps
``resolve_seed()``). Two render sites exist today:

* ``src/lakebench/deploy/datagen.py`` (datagen Job env).
* ``src/lakebench/modules/pipeline_engines/spark/job.py`` (Spark job env
  for the silver-build sampler and the AML reference-score provenance).

The render-time backstop stops a spent or role-tagged seed from being
re-used even when the load-time check has been bypassed (mutating the
Pydantic model after construction, or synthesising a raw manifest).

Removing any of these sites voids the held-out invariant. Any change
to this file requires an owner decision and a matching update to
``docs/internal/aml-protocol.md``.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = _REPO_ROOT / "src" / "lakebench"

_INVARIANT_HINT = (
    "AML seed guard removed. The held-out AML invariant (docs/DESIGN.md; "
    "seed protocol in docs/internal/aml-protocol.md) requires both a "
    "load-time refusal in config/datagen_seed.py + config/schema.py and "
    "a render-time re-check at every deploy site (deploy/datagen.py and "
    "modules/pipeline_engines/spark/job.py)."
)

# Sites the CI net guards. Each entry pairs a source file with the substrings
# any one of which proves the guard is still wired in. ``any_of`` matches
# either the direct helper (``resolve_seed``) or the wrapper (``config_seed``)
# so a future rename that keeps the semantics does not fail this net.
_RENDER_SITES = [
    (
        _SRC / "deploy" / "datagen.py",
        ("config_seed(", "resolve_seed("),
        "datagen deploy render (datagen Job env)",
    ),
    (
        _SRC / "modules" / "pipeline_engines" / "spark" / "job.py",
        ("config_seed(", "resolve_seed("),
        "Spark job render (silver-build sampler + AML reference-score env)",
    ),
]

_GUARD_DEF_FILE = _SRC / "config" / "datagen_seed.py"
_LOAD_SITE_FILE = _SRC / "config" / "schema.py"


_EXACT_HELPER_NAMES: frozenset[str] = frozenset(
    {"check_seed", "resolve_seed", "config_seed"}
)


def _read(path: Path) -> str:
    assert path.is_file(), f"{path} missing. {_INVARIANT_HINT}"
    return path.read_text(encoding="utf-8")


def _parse(path: Path) -> ast.Module:
    return ast.parse(_read(path), filename=str(path))


def _has_function_def(module: ast.Module, name: str) -> bool:
    """True when the module defines a top-level or nested `def name(...)`.

    AST-based so a comment `# def name(` does not pass, and a rename to
    `_name` or `name_v2` does not pass either. Enforces the exact name.
    """
    for node in ast.walk(module):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return True
    return False


def _has_call_to(module: ast.Module, names: frozenset[str]) -> bool:
    """True when the module contains a `Call` whose function is one of `names`.

    Matches bare calls (`check_seed(...)`), attribute calls
    (`datagen_seed.check_seed(...)`), and via-alias imports where the
    alias resolves at read-time. Comments and docstrings are ignored
    because AST does not lift them to `Call` nodes; a substring match
    would have missed this.
    """
    for node in ast.walk(module):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id in names:
            return True
        if isinstance(func, ast.Attribute) and func.attr in names:
            return True
    return False


def test_check_seed_is_defined_in_datagen_seed_module():
    """Guard 1 body: `check_seed` and `resolve_seed` are defined."""
    module = _parse(_GUARD_DEF_FILE)
    assert _has_function_def(module, "check_seed"), (
        f"{_GUARD_DEF_FILE} no longer defines check_seed(). {_INVARIANT_HINT}"
    )
    assert _has_function_def(module, "resolve_seed"), (
        f"{_GUARD_DEF_FILE} no longer defines resolve_seed(). {_INVARIANT_HINT}"
    )


def test_load_time_guard_has_a_caller_in_workload_validator():
    """Guard 1 caller: the WorkloadConfig validator invokes the guard.

    A defined guard function with no caller is a dormant guard. The
    WorkloadConfig Pydantic model calls `resolve_seed` (which calls
    `check_seed`) in a `model_validator(mode="after")` so an unsafe
    config is refused before any deploy step runs. AST-based so a
    commented-out or docstring-mentioned call does not satisfy the net.
    """
    module = _parse(_LOAD_SITE_FILE)
    assert _has_call_to(module, _EXACT_HELPER_NAMES), (
        f"{_LOAD_SITE_FILE} no longer calls the AML seed guard at load time. "
        f"{_INVARIANT_HINT}"
    )


@pytest.mark.parametrize(
    ("path", "needles", "label"),
    _RENDER_SITES,
    ids=[label for _, _, label in _RENDER_SITES],
)
def test_render_time_guard_present_at_deploy_site(
    path: Path, needles: tuple[str, ...], label: str
) -> None:
    """Guard 2 sites: each render site materialises the seed via the guard.

    AST-based (`needles` is retained for the human-readable failure
    message only): a comment `# config_seed(...)` no longer satisfies
    the net, and a rename to `_config_seed` or `resolve_seed_v2` does
    not either. Removing the real call always fails.
    """
    module = _parse(path)
    if not _has_call_to(module, _EXACT_HELPER_NAMES):
        pytest.fail(
            f"Render-time AML seed guard missing at {path} ({label}). "
            f"Expected a call to one of {sorted(_EXACT_HELPER_NAMES)}. "
            f"{_INVARIANT_HINT}"
        )
