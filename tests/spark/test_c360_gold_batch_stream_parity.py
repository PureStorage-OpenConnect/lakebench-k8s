"""Executed: Customer 360 gold is the same from batch and continuous (V16-7).

The continuous ``gold_refresh``, ticking three times while silver grows, must
leave after every tick the same gold rows as the batch ``gold_finalize``
over silver as it stands then, on Iceberg and on Delta: a reader of gold
gets the same answers from either pipeline over the same corpus. A one-cent change to one
silver purchase must change the batch fingerprint, so equality is not an
accident of what the fingerprint covers.

Runs ``c360_gold_parity_scenarios.py`` in a fresh JVM with the Iceberg and
Delta jars from ``LB_SPARK_TEST_JARS``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

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
    rows = [t["stream"]["rows"] for t in case["ticks"]]
    assert len(rows) == 3 and rows[0] > 0 and rows[0] < rows[1] < rows[2], rows
    for i, t in enumerate(case["ticks"]):
        assert t["stream"] == t["batch"], i


@pytest.mark.parametrize("fmt", FORMATS)
def test_one_changed_silver_amount_changes_gold(result, fmt):
    case = result[fmt]
    final = case["ticks"][-1]["batch"]
    assert case["changed"]["rows"] == final["rows"]
    assert case["changed"]["sha256"] != final["sha256"]
