"""Gate 3b: c360 gold non-degeneracy gate. Gold must hold one daily-KPI row per
distinct silver interaction_date, or the run fails (invariant 3: exit 0 is not a
pass). The check is a pure function so it is verified without Spark; both c360
gold adapters (Iceberg + Delta) must apply it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"


@pytest.fixture
def common(load_script):
    return load_script("common")


@pytest.mark.parametrize(
    ("gold", "silver", "problem"),
    [(366, 366, False), (300, 366, True), (400, 366, True)],
)
def test_gold_date_coverage(common, gold, silver, problem):
    """Gold must cover every silver date once: short or duplicated is a problem."""
    assert (common.gold_date_coverage_problem(gold, silver) is not None) is problem


@pytest.mark.parametrize("script", ["gold_finalize.py", "gold_finalize_delta.py"])
def test_both_c360_gold_adapters_apply_the_gate(script):
    src = (_SCRIPTS / script).read_text()
    assert "gold_date_coverage_problem(" in src, f"{script} does not apply the non-degeneracy gate"
    assert "non-degeneracy gate FAILED" in src, f"{script} does not fail the run on a gate problem"
