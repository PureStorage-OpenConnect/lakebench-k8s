"""The c360 gold non-degeneracy gate. Gold must hold one daily-KPI row per
distinct silver interaction_date, or the run fails (exit 0 is not a pass). The
check is a pure function so it is verified without Spark.
"""

from __future__ import annotations

import pytest


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
