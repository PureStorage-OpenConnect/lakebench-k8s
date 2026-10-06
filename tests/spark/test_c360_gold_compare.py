"""What counts as the same Customer 360 gold (LB-267): no Spark needed.

The parity tests (``test_c360_gold_batch_stream_parity``,
``test_c360_multicycle_equivalence_spark``) compare gold with
``c360_gold_compare``. Here it is pinned on hand-made tables: the two
averages of a DOUBLE amount may land one cent apart; every other cell is
exact, so a cent on a revenue, a quantum on an average of an INT column, a
changed count, a missing or doubled day, or a NULL against a value is a
difference, and so is a one-cent bias on every day of an average.
"""

from __future__ import annotations

import copy
import re

import pytest
from c360_gold_compare import (
    DOUBLE_SILVER,
    ORDER_SENSITIVE,
    gold_differences,
    kpi_source,
    product_mismatch,
)

COLUMNS = [
    "avg_engagement_score",
    "avg_transaction_value",
    "interaction_date",
    "total_daily_revenue",
    "total_transactions",
]
TYPES = {
    "avg_engagement_score": "double",
    "avg_transaction_value": "double",
    "interaction_date": "date",
    "total_daily_revenue": "double",
    "total_transactions": "bigint",
}


def _gold(days: int = 200) -> dict:
    rows = [
        [
            1.31,
            round(40 + d * 0.37, 2),
            f"2024-{1 + d // 28:02d}-{1 + d % 28:02d}",
            round(2000 + d * 3.11, 2),
            18 + d % 5,
        ]
        for d in range(days)
    ]
    return {"columns": list(COLUMNS), "types": dict(TYPES), "rows": rows}


def _moved(t: dict, row: int, col: str, by: float) -> dict:
    out = copy.deepcopy(t)
    i = out["columns"].index(col)
    out["rows"][row][i] = round(out["rows"][row][i] + by, 6)
    return out


def test_the_flips_lb267_saw_are_the_same_gold():
    """Five days a cent apart on avg_transaction_value, as two Spark builds
    of one silver table gave (0.009999999999990905 apart as doubles)."""
    a = _gold()
    b = a
    for row in (3, 50, 77, 120, 199):
        b = _moved(b, row, "avg_transaction_value", 0.01)
    b["rows"][3][1] = a["rows"][3][1] + 0.009999999999990905
    assert gold_differences(a, b) == []
    assert product_mismatch(a, b) is None


@pytest.mark.parametrize("by", [0.02, 1.0, -0.03])
def test_more_than_one_cent_on_an_average_is_a_difference(by):
    problems = gold_differences(_gold(), _moved(_gold(), 9, "avg_transaction_value", by))
    assert len(problems) == 1 and "avg_transaction_value" in problems[0], problems


@pytest.mark.parametrize("col", ["total_daily_revenue", "avg_engagement_score"])
def test_one_cent_on_an_order_free_kpi_is_a_difference(col):
    """A cent sum and an average of an INT column do not depend on the
    summation order, so they are compared exactly."""
    problems = gold_differences(_gold(), _moved(_gold(), 9, col, 0.01))
    assert len(problems) == 1 and col in problems[0], problems
    assert product_mismatch(_gold(), _moved(_gold(), 9, col, 0.01))


def test_a_cent_on_every_day_of_an_average_fails_the_product_fingerprint():
    """Each cell is within one cent, but the bias is not summation noise."""
    b = _gold()
    for row in range(200):
        b = _moved(b, row, "avg_transaction_value", 0.01)
    assert gold_differences(_gold(), b) == []
    assert product_mismatch(_gold(), b)


def test_a_count_off_by_one_is_a_difference():
    problems = gold_differences(_gold(), _moved(_gold(), 9, "total_transactions", 1))
    assert len(problems) == 1 and "total_transactions" in problems[0], problems


def test_null_against_a_value_is_a_difference():
    b = _gold()
    b["rows"][4][1] = None
    assert gold_differences(_gold(), b)
    assert gold_differences(copy.deepcopy(b), b) == []


def test_nan_matches_nan_only():
    a, b = _gold(), _gold()
    a["rows"][4][0] = b["rows"][4][0] = float("nan")
    assert gold_differences(a, b) == []
    b["rows"][4][0] = 1.31
    assert gold_differences(a, b)


def test_a_missing_or_repeated_day_is_a_difference():
    a, b = _gold(), _gold()
    b["rows"].pop()
    assert any("days differ" in p for p in gold_differences(a, b))
    b = _gold()
    b["rows"][1] = list(b["rows"][0])
    assert any("appears twice" in p for p in gold_differences(a, b))


def test_column_or_type_drift_is_a_difference():
    b = _gold()
    b["types"]["total_transactions"] = "int"
    assert gold_differences(_gold(), b)
    b = _gold()
    b["types"]["avg_transaction_value"] = "decimal(10,2)"
    assert gold_differences(b, b) == [
        "order-sensitive column avg_transaction_value is decimal(10,2), not DOUBLE"
    ]


def test_order_sensitive_is_every_rounded_average_of_a_double_amount():
    """ORDER_SENSITIVE names each KPI of common.get_daily_kpi_aggregations
    (read as source text) that ROUNDs an average of a DOUBLE silver column,
    at the quantum of its ROUND."""
    body = kpi_source()
    averages = {}
    pattern = r'round_\(\s*avg\((.*?)\),\s*(\d)\s*\)\.alias\(\s*"(\w+)"'
    for m in re.finditer(pattern, body, re.S):
        averaged = re.findall(r'"(\w+)"', m.group(1))[-1]
        averages[m.group(3)] = (averaged, 10.0 ** -int(m.group(2)))
    assert len(averages) == 6, averages
    sensitive = {k: q for k, (c, q) in averages.items() if c in DOUBLE_SILVER}
    assert sensitive == ORDER_SENSITIVE
