"""Runtime dependencies whose new minors have broken lakebench carry an upper cap."""

from __future__ import annotations

import sys
from pathlib import Path

from packaging.requirements import Requirement

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]

#: name -> the exclusive upper bound the dependency must carry.
CAPPED = {"typer": "0.28"}


def _dependencies() -> dict[str, Requirement]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    reqs = [Requirement(r) for r in data["project"]["dependencies"]]
    return {r.name.lower(): r for r in reqs}


def test_capped_dependencies_keep_their_upper_bound():
    deps = _dependencies()
    for name, cap in CAPPED.items():
        assert name in deps, f"{name} is no longer a dependency; drop it from CAPPED"
        uppers = [s for s in deps[name].specifier if s.operator == "<"]
        assert [s.version for s in uppers] == [cap], (
            f"{name} must be capped at <{cap} in pyproject.toml, got {deps[name].specifier}"
        )
