"""scripts/check_version.py: one version source, and release tags must match it."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "check_version", ROOT / "scripts" / "check_version.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


cv = _load()

_GOOD_PYPROJECT = """
[project]
name = "x"
dynamic = ["version"]

[tool.hatch.version]
path = "src/lakebench/__init__.py"
"""


@pytest.fixture
def tree(tmp_path):
    def make(version="1.6.0", pyproject=_GOOD_PYPROJECT):
        init = tmp_path / "src" / "lakebench" / "__init__.py"
        init.parent.mkdir(parents=True, exist_ok=True)
        init.write_text(f'"""doc"""\n\n__version__ = "{version}"\n')
        pp = tmp_path / "pyproject.toml"
        pp.write_text(pyproject)
        return init, pp

    return make


def test_repo_is_consistent():
    assert cv.check() == []


def test_matching_tag_passes(tree):
    init, pp = tree("1.6.0")
    assert cv.check("v1.6.0", init, pp) == []
    assert cv.check("refs/tags/v1.6.0", init, pp) == []


def test_mismatched_tag_fails(tree):
    init, pp = tree("1.6.0")
    problems = cv.check("v1.6.1", init, pp)
    assert len(problems) == 1 and "does not match" in problems[0]


def test_dev_version_cannot_be_tagged(tree):
    init, pp = tree("1.6.0.dev0")
    problems = cv.check("v1.6.0.dev0", init, pp)
    assert any(".dev build" in p for p in problems)


def test_static_pyproject_version_fails(tree):
    init, pp = tree("1.6.0", '[project]\nname = "x"\nversion = "1.6.0"\n')
    problems = cv.check(None, init, pp)
    assert any("static version" in p for p in problems)
    assert any("dynamic" in p for p in problems)
    assert any("hatch.version" in p for p in problems)


def test_missing_version_literal(tmp_path):
    init = tmp_path / "__init__.py"
    init.write_text("from ._v import __version__\n")
    assert "no __version__ literal" in cv.check(None, init, tmp_path / "p.toml")[0]
