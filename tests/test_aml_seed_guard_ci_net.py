"""CI regression net for the AML seed guards (CLAUDE.md invariant 1).

Held-out AML data must never be used during development. Two guards keep
this true and both must exist. This test greps the source tree so an
accidental removal of either guard fails CI with a clear message.

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

Removing any of these sites voids invariant 1. Any change to this file
requires an owner decision and a matching update to
``docs/internal/aml-protocol.md``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = _REPO_ROOT / "src" / "lakebench"

_INVARIANT_HINT = (
    "AML seed guard removed. CLAUDE.md section 4 invariant 1 (held-out "
    "AML data is never used during development) requires both a load-time "
    "refusal in config/datagen_seed.py + config/schema.py and a render-time "
    "re-check at every deploy site (deploy/datagen.py and "
    "modules/pipeline_engines/spark/job.py). See docs/internal/aml-protocol.md."
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


def _read(path: Path) -> str:
    assert path.is_file(), f"{path} missing. {_INVARIANT_HINT}"
    return path.read_text(encoding="utf-8")


def test_check_seed_is_defined_in_datagen_seed_module():
    """Guard 1 body: ``check_seed`` (or the current spelling) is defined."""
    text = _read(_GUARD_DEF_FILE)
    # ``def check_seed(`` proves the load-time guard function still exists.
    # If it is renamed, update this expectation AND the load-time caller in
    # config/schema.py; do not delete it.
    assert "def check_seed(" in text, (
        f"{_GUARD_DEF_FILE} no longer defines check_seed(). {_INVARIANT_HINT}"
    )
    # The render-time helper the two deploy sites reach for lives here too.
    assert "def resolve_seed(" in text, (
        f"{_GUARD_DEF_FILE} no longer defines resolve_seed(). {_INVARIANT_HINT}"
    )


def test_load_time_guard_has_a_caller_in_workload_validator():
    """Guard 1 caller: the WorkloadConfig validator invokes the guard.

    A defined guard function with no caller is a dormant guard. The
    WorkloadConfig Pydantic model runs ``resolve_seed`` (which calls
    ``check_seed``) in a ``model_validator(mode="after")`` so an unsafe
    config is refused before any deploy step runs.
    """
    text = _read(_LOAD_SITE_FILE)
    assert "from lakebench.config.datagen_seed import" in text and (
        "resolve_seed" in text or "check_seed" in text
    ), f"{_LOAD_SITE_FILE} no longer imports the AML seed guard. {_INVARIANT_HINT}"
    # The caller pattern the WorkloadConfig validator uses today.
    assert "resolve_seed(" in text or "check_seed(" in text, (
        f"{_LOAD_SITE_FILE} no longer calls the AML seed guard at load time. {_INVARIANT_HINT}"
    )


@pytest.mark.parametrize(
    ("path", "needles", "label"),
    _RENDER_SITES,
    ids=[label for _, _, label in _RENDER_SITES],
)
def test_render_time_guard_present_at_deploy_site(
    path: Path, needles: tuple[str, ...], label: str
) -> None:
    """Guard 2 sites: each render site materialises the seed via the guard."""
    text = _read(path)
    if not any(n in text for n in needles):
        pytest.fail(
            f"Render-time AML seed guard missing at {path} ({label}). "
            f"Expected one of {needles}. {_INVARIANT_HINT}"
        )
