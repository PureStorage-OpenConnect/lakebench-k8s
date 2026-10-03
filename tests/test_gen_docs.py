"""scripts/gen_docs.py runs every docs generator in one step (RELEASING.md
step ``generated-docs``): ``--check`` exits 1 when any generated block is
stale and writes nothing; without it the blocks are rewritten."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
GEN = "scripts/gen_docs.py"


def _run(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(root / GEN), *args],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture(scope="module")
def tree(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A copy of the files the generators read and write."""
    root = tmp_path_factory.mktemp("gen_docs")
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    for d in ("src", "docs", "scripts"):
        shutil.copytree(REPO / d, root / d, ignore=ignore)
    shutil.copy(REPO / "README.md", root / "README.md")
    return root


def test_wraps_every_generator_script():
    import importlib.util

    spec = importlib.util.spec_from_file_location("gen_docs", REPO / GEN)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    on_disk = {p.name for p in (REPO / "scripts").glob("gen_*.py")} - {"gen_docs.py"}
    assert set(mod.SCRIPTS) == on_disk


def test_check_passes_on_the_tree(tree: Path):
    r = _run(tree, "--check")
    assert r.returncode == 0, r.stdout + r.stderr


def test_a_description_changed_without_regenerating_fails_then_regenerates(tree: Path):
    schema = tree / "src/lakebench/config/schema.py"
    text = schema.read_text()
    assert '"""Table names for each layer."""' in text
    schema.write_text(
        text.replace('"""Table names for each layer."""', '"""Planted description."""')
    )
    # A section field's description is not in the reference tables; plant a
    # leaf change too, so the configuration reference goes stale.
    text = schema.read_text()
    old = "Unique deployment name."
    assert old in text
    schema.write_text(text.replace(old, "Planted name description.", 1))
    doc_before = (tree / "docs/configuration.md").read_text()

    r = _run(tree, "--check")
    assert r.returncode == 1
    assert "gen_config_reference.py" in r.stderr
    assert (tree / "docs/configuration.md").read_text() == doc_before

    r = _run(tree)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "Planted name description." in (tree / "docs/configuration.md").read_text()
    assert _run(tree, "--check").returncode == 0


def test_a_stale_support_block_fails_the_check(tree: Path):
    readme = tree / "README.md"
    text = readme.read_text()
    marker = "<!-- BEGIN GENERATED: support-states"
    assert marker in text
    i = text.index(marker)
    j = text.index("\n", text.index("\n", i) + 1)
    readme.write_text(text[:j] + "\nplanted line" + text[j:])
    try:
        r = _run(tree, "--check")
        assert r.returncode == 1
        assert "README.md: generated block 'support-states' is stale" in r.stderr
    finally:
        readme.write_text(text)
