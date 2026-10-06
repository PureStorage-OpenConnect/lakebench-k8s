"""I8: ``silver_stream_delta.write_silver_batch`` returns the numOutputRows
this write itself committed, read from its own history row. Immune to
interleaved commits (OPTIMIZE and a second writer's appends) that would
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
    assert result["errors"] == [], result
    assert result["batches"] == 10
    assert len(result["returns"]) == 10


def test_returns_non_zero_and_match_table_delta(result):
    """Every non-empty batch's return value equals the growth of this
    writer's own rows. With interleaved commits a before/after-version
    bracket miscounts; the I8 fix ties the count to this writer's own
    commit metrics, so returns match ground truth."""
    assert result["errors"] == [], result
    returns = result["returns"]
    row_counts = result["row_counts"]
    prev = 0
    for i, (n, tot) in enumerate(zip(returns, row_counts, strict=True)):
        assert n > 0, f"batch {i}: write returned 0; churn thread should not dedupe real writes"
        assert n == tot - prev, f"batch {i}: returned {n} but its rows grew by {tot - prev}"
        prev = tot


def test_the_churn_interleaved_and_failed_nothing(result):
    """The other commits really landed between this writer's batches (the
    version moved by more than its own commit at least once), and none of
    them failed: OPTIMIZE and a second writer run beside the stream."""
    assert result["errors"] == [], result
    churn = result["churn"]
    assert churn["errors"] == [], churn
    assert churn["optimize"] > 0 and churn["foreign_appends"] > 0, churn
    steps = [b - a for a, b in zip(result["versions"], result["versions"][1:], strict=False)]
    assert max(steps) > 1, steps
