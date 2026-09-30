"""Gate 3b: c360 gold non-degeneracy gate. Gold must hold one daily-KPI row per
distinct silver interaction_date, or the run fails (invariant 3: exit 0 is not a
pass). The check is a pure function so it is verified without Spark; both c360
gold adapters (Iceberg + Delta) must apply it.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"


@pytest.fixture
def common():
    p = str(_SCRIPTS)
    if p not in sys.path:
        sys.path.insert(0, p)
    sys.modules.pop("common", None)
    mod = importlib.import_module("common")
    yield mod
    sys.modules.pop("common", None)


def test_match_returns_none(common):
    assert common.gold_date_coverage_problem(366, 366) is None


def test_gold_short_of_silver_dates_is_a_problem(common):
    msg = common.gold_date_coverage_problem(300, 366)
    assert msg and "300" in msg and "366" in msg


def test_gold_exceeds_silver_dates_is_a_problem(common):
    # Duplicated dates: more gold rows than distinct silver dates.
    assert common.gold_date_coverage_problem(400, 366) is not None


@pytest.mark.parametrize("script", ["gold_finalize.py", "gold_finalize_delta.py"])
def test_both_c360_gold_adapters_apply_the_gate(script):
    src = (_SCRIPTS / script).read_text()
    assert "gold_date_coverage_problem(" in src, f"{script} does not apply the non-degeneracy gate"
    assert "non-degeneracy gate FAILED" in src, f"{script} does not fail the run on a gate problem"
