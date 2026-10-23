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


def test_matching_tag_passes(tree):
    init, pp = tree("1.6.0")
    assert cv.check("v1.6.0", init, pp) == []
    assert cv.check("refs/tags/v1.6.0", init, pp) == []


def test_mismatched_tag_fails(tree):
    init, pp = tree("1.6.0")
    problems = cv.check("v1.6.1", init, pp)
    assert len(problems) == 1 and "does not match" in problems[0]


def test_dev_version_cannot_be_tagged(tree):
    for version in ["1.6.0.dev0", "1.6.0dev0", "1.6.0-dev0", "1.6.0.DEV0"]:
        init, pp = tree(version)
        problems = cv.check("v1.6.0.dev0", init, pp)
        assert any("dev release" in p for p in problems), problems
