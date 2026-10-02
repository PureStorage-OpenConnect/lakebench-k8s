"""The sizing tables in README.md and docs/getting-started.md are generated
by scripts/gen_sizing_tables.py from config.sizing (CC-22). A hand edit,
or a profile change without a regenerate, fails here."""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path

import pytest

from lakebench.config import sizing

REPO = Path(__file__).resolve().parents[1]


def _script():
    spec = importlib.util.spec_from_file_location(
        "gen_sizing_tables", REPO / "scripts" / "gen_sizing_tables.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("rel", sorted(sizing.DOCS_WITH_BLOCKS))
def test_docs_blocks_match_the_code(rel):
    text = (REPO / rel).read_text()
    for name in sizing.DOCS_WITH_BLOCKS[rel]:
        assert sizing.block_in(text, name) == sizing.expected_block(name), (
            f"{rel}: the {name} block is stale; run python3.11 scripts/gen_sizing_tables.py"
        )


def test_check_flags_a_hand_edit(tmp_path):
    for rel in sizing.DOCS_WITH_BLOCKS:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO / rel, tmp_path / rel)
    gen = _script()
    assert gen.drift(tmp_path) == []
    p = tmp_path / "README.md"
    p.write_text(p.read_text().replace("| 40 cores |", "| 36 cores |", 1))
    assert gen.drift(tmp_path) == ["README.md: block 'sizing-minimums' is stale"]
    assert gen.regenerate(tmp_path) == ["README.md"]
    assert gen.drift(tmp_path) == []


def test_missing_markers_reported(tmp_path):
    for rel in sizing.DOCS_WITH_BLOCKS:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text("no markers here\n")
    problems = _script().drift(tmp_path)
    assert len(problems) == sum(len(v) for v in sizing.DOCS_WITH_BLOCKS.values())
    assert all("markers missing" in p for p in problems)
