"""Executed: N incremental Customer 360 cycles equal one rebuild (C36-2).

Three cycles run as a multi-cycle ``lakebench run`` runs them (the real
silver and gold scripts, one driver per job, ``LB_BRONZE_CYCLE`` and the
incremental flags from cycle 1, one data clock) must leave the same silver
and gold as one rebuild over the same bronze, on Iceberg and on Delta, every
column but ``silver_processing_timestamp`` compared (``_batch_id`` included:
the rebuild tags rows by file name). The later cycles must have taken the
incremental path, and a one-cent change to one cycle-1 purchase must change
both fingerprints, so equality is not an accident of what is compared. Only
the SIMPLE silver strategy runs (what small scales choose).

Runs ``c360_multicycle_equivalence_scenarios.py`` in fresh JVMs with the
Iceberg and Delta jars from ``LB_SPARK_TEST_JARS``.
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
    work = tmp_path_factory.mktemp("c360-multicycle")
    script = Path(__file__).with_name("c360_multicycle_equivalence_scenarios.py")
    proc = spark_subprocess(script, spark_jars.classpath, work, timeout=2400)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _why(case: dict) -> str:
    return json.dumps(case.get("log"))[-4000:]


@pytest.mark.parametrize("fmt", FORMATS)
def test_every_job_succeeded(result, fmt):
    case = result[fmt]
    assert case["rcs"] == [0] * 10, _why(case)


@pytest.mark.parametrize("fmt", FORMATS)
def test_later_cycles_took_the_incremental_path(result, fmt):
    """Cycles 1 and 2 appended to silver and replaced gold from the
    watermark: a fallback to a full rebuild would make equality trivial."""
    for cycle, seen in result[fmt]["incremental"].items():
        assert seen == {"silver_appended": True, "gold_incremental": True}, (
            cycle,
            _why(result[fmt]),
        )


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("layer", ["silver", "gold"])
def test_incremental_cycles_equal_one_rebuild(result, fmt, layer):
    fp = result[fmt][layer]
    assert fp["incremental"]["rows"] > 0
    assert fp["incremental"] == fp["rebuild"], _why(result[fmt])


@pytest.mark.parametrize("fmt", FORMATS)
@pytest.mark.parametrize("layer", ["silver", "gold"])
def test_one_changed_amount_changes_the_fingerprint(result, fmt, layer):
    fp = result[fmt][layer]
    assert fp["changed"]["rows"] == fp["rebuild"]["rows"]
    assert fp["changed"]["sha256"] != fp["rebuild"]["sha256"]
