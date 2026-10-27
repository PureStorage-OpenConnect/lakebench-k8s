"""``silver_stream_delta.write_silver_batch`` under concurrent commits.

Startup race: two concurrent writers with different stream ids racing the
not-exists branch on the same Delta table converge on a metadata-only
``CREATE TABLE IF NOT EXISTS`` commit; both then take the append path
without data loss.

Version race: the write returns the numOutputRows it committed itself, read
from its own history row, so interleaved commits (OPTIMIZE and a second
writer's appends) that would move a before/after ``delta_table_version``
bracket do not change it.

Runs both scenarios in one fresh JVM with the Delta and Iceberg jars from
``LB_SPARK_TEST_JARS`` (Iceberg too: the session registers an Iceberg
catalog).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg", "delta")


@pytest.fixture(scope="module")
def result(tmp_path_factory, spark_subprocess, spark_jars):
    work = tmp_path_factory.mktemp("delta-writer-races")
    script = Path(__file__).with_name("delta_stream_mech_scenarios.py")
    proc = spark_subprocess(script, spark_jars.classpath, work, timeout=1200)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _ran(case):
    """The scenario's own errors, shown before a check that needs its output."""
    assert case["errors"] == [], case


def test_startup_race_both_wrote_without_data_loss(result):
    """Both racers commit non-zero rows (distinct app ids, so neither is
    Delta-deduped), and after both settle the table holds both racers' rows
    with both stream ids: the not-exists branch took the append path rather
    than an overwrite that would have squashed one."""
    case = result["startup_race"]
    _ran(case)
    written = case["written"]
    assert set(written.keys()) == {"A", "B"}, written
    assert written["A"] > 0 and written["B"] > 0, written
    assert case["table_rows"] == written["A"] + written["B"], case
    assert set(case["per_stream_rows"]) == {"qA", "qB"}, case["per_stream_rows"]


def test_version_race_returns_match_this_writers_commits_under_churn(result):
    """Every non-empty batch's return equals the growth of this writer's own
    rows, read from its own commit metrics rather than a before/after version
    bracket, and the churn (OPTIMIZE and a second writer) really interleaved
    without failing."""
    case = result["version_race"]
    _ran(case)
    prev = 0
    for i, (n, tot) in enumerate(zip(case["returns"], case["row_counts"], strict=True)):
        assert n > 0, f"batch {i}: write returned 0; churn thread should not dedupe real writes"
        assert n == tot - prev, f"batch {i}: returned {n} but its rows grew by {tot - prev}"
        prev = tot
    churn = case["churn"]
    assert churn["errors"] == [], churn
    assert churn["optimize"] > 0 and churn["foreign_appends"] > 0, churn
    steps = [b - a for a, b in zip(case["versions"], case["versions"][1:], strict=False)]
    assert max(steps) > 1, steps
