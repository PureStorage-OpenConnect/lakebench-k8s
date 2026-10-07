"""Executed: Customer 360 gold is the same from batch and continuous (V16-7).

The continuous ``gold_refresh``, ticking three times while silver grows, must
leave after every tick the same gold rows as the batch ``gold_finalize``
over silver as it stands then, on Iceberg and on Delta: a reader of gold
gets the same answers from either pipeline over the same corpus. "The same"
is ``c360_gold_compare``: every KPI exact except the two averages of a
DOUBLE amount, which may land one cent apart, and the product's result
fingerprint matching; an exact hash was not stable (LB-267). A one-cent
change to one silver purchase must fail both checks, so equality is not an
accident of what they cover.

Runs ``c360_gold_parity_scenarios.py`` in a fresh JVM with the Iceberg and
Delta jars from ``LB_SPARK_TEST_JARS``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from c360_gold_compare import gold_differences, product_mismatch

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg", "delta")

FORMATS = ["iceberg", "delta"]


@pytest.fixture(scope="module")
def result(tmp_path_factory, spark_subprocess, spark_jars):
    work = tmp_path_factory.mktemp("c360-gold-parity")
    script = Path(__file__).with_name("c360_gold_parity_scenarios.py")
    proc = spark_subprocess(script, spark_jars.classpath, work, timeout=1800)
    return json.loads(proc.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("fmt", FORMATS)
def test_stream_gold_equals_batch_gold_after_every_tick(result, fmt):
    case = result[fmt]
    rows = [len(t["stream"]["rows"]) for t in case["ticks"]]
    assert len(rows) == 3 and rows[0] > 0 and rows[0] < rows[1] < rows[2], rows
    for i, t in enumerate(case["ticks"]):
        assert gold_differences(t["stream"], t["batch"]) == [], i
        assert product_mismatch(t["stream"], t["batch"]) is None, i


@pytest.mark.parametrize("fmt", FORMATS)
def test_one_changed_silver_amount_changes_gold(result, fmt):
    case = result[fmt]
    final = case["ticks"][-1]["batch"]
    assert len(case["changed"]["rows"]) == len(final["rows"])
    problems = gold_differences(case["changed"], final)
    # One purchase moved, so one day, and its revenue among what moved.
    assert len({p.split()[0] for p in problems}) == 1, problems
    assert any(" total_daily_revenue:" in p for p in problems), problems
    assert product_mismatch(case["changed"], final) is not None
