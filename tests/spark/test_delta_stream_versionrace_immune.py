"""I8: ``silver_stream_delta.write_silver_batch`` returns the numOutputRows
this write itself committed, read from its own history row. Immune to
interleaved metadata commits (compaction/vacuum stand-ins) that would
have moved the before/after ``delta_table_version`` bracket.

Runs in a fresh JVM with the Delta and Iceberg jars from
``LB_SPARK_TEST_JARS``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg", "delta")


@pytest.fixture(scope="module")
def result(tmp_path_factory, spark_subprocess, spark_jars):
    work = tmp_path_factory.mktemp("delta-version-race")
    script = Path(__file__).with_name("delta_stream_mech_scenarios.py")
    proc = spark_subprocess(script, "version_race", spark_jars.classpath, work, timeout=900)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_ten_batches_written(result):
    assert result["batches"] == 10
    assert len(result["returns"]) == 10


def test_returns_non_zero_and_match_table_delta(result):
    """Every non-empty batch's return value equals the row-count delta the
    table saw. With interleaved metadata commits the old before/after bracket
    read zero (or garbage); the I8 fix ties the count to this writer's own
    commit metrics, so returns match ground truth."""
    returns = result["returns"]
    row_counts = result["row_counts"]
    prev = 0
    for i, (n, tot) in enumerate(zip(returns, row_counts, strict=True)):
        assert n > 0, f"batch {i}: write returned 0; churn thread should not dedupe real writes"
        assert n == tot - prev, f"batch {i}: returned {n} but table grew by {tot - prev}"
        prev = tot
