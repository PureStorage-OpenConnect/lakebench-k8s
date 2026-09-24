"""c360 multi-cycle correctness (E2).

- gold INCREMENTAL appended a last_updated column the other strategies do not
  write, failing cycle 2 on a schema mismatch; its strict > watermark dropped
  rows on the last processed date.
- silver SALTED ignored incremental mode (createOrReplace wiped earlier
  cycles) and salted nothing.
- a failed cycle datagen was a warning, so the pipeline rebuilt the cycle
  from the previous cycle's bronze and incremental silver appended it twice.
"""

from __future__ import annotations

import ast
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def _func_src(path: Path, name: str) -> str:
    src = path.read_text()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(src, node) or ""
    raise AssertionError(f"{name} not found")


def test_gold_incremental_matches_other_strategies_and_replaces_boundary():
    body = _func_src(SCRIPTS / "gold_finalize.py", "gold_incremental")
    assert "last_updated" not in body
    assert ">= last_date" in body
    assert "DELETE FROM" in body


def test_silver_salted_is_gone():
    src = (SCRIPTS / "silver_build.py").read_text()
    assert "def silver_salted" not in src
    assert "return SilverStrategy.SALTED" not in src


def test_cycle_datagen_failure_is_fatal():
    src = (Path(__file__).resolve().parents[1] / "src/lakebench/cli/_run.py").read_text()
    i = src.index("datagen_result = _cycle_datagen.deploy_cycle")
    window = src[i : i + 2500]
    assert 'print_warning(f"Datagen cycle' not in window
    assert window.count("pipeline_success = False") >= 3
