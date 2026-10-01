"""B4: two concurrent silver_stream_delta writers with different stream ids
racing the not-exists branch on the same Delta table converge on a metadata-
only ``CREATE TABLE IF NOT EXISTS`` commit; both then take the append path
without data loss.

Runs the scenario in a fresh JVM with the Delta and Iceberg jars from
``LB_SPARK_TEST_JARS`` (Iceberg too: the shared session registers an
Iceberg catalog).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg", "delta")


@pytest.fixture(scope="module")
def result(tmp_path_factory, spark_subprocess, spark_jars):
    work = tmp_path_factory.mktemp("delta-startup-race")
    script = Path(__file__).with_name("delta_stream_mech_scenarios.py")
    proc = spark_subprocess(script, "startup_race", spark_jars.classpath, work, timeout=900)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _ran(result):
    """The scenario's own errors, shown before a check that needs its output."""
    assert result["errors"] == [], result


@pytest.mark.known_bug(
    "LB-195",
    match="queryId is not set",
    reason="sql.streaming.queryId is not set: the test calls the writer outside foreachBatch",
)
def test_no_errors(result):
    assert result["errors"] == [], result


@pytest.mark.known_bug(
    "LB-195",
    match="queryId is not set",
    reason="sql.streaming.queryId is not set: the test calls the writer outside foreachBatch",
)
def test_both_racers_wrote(result):
    """Both racers commit non-zero rows: neither is Delta-deduped by
    (appId, version) because they used distinct app ids."""
    _ran(result)
    written = result["written"]
    assert set(written.keys()) == {"A", "B"}, written
    assert written["A"] > 0 and written["B"] > 0, written


@pytest.mark.known_bug(
    "LB-195",
    match="queryId is not set",
    reason="sql.streaming.queryId is not set: the test calls the writer outside foreachBatch",
)
def test_no_data_loss(result):
    """After both writes settle, the table holds both racers' rows."""
    _ran(result)
    assert result["table_rows"] == result["written"]["A"] + result["written"]["B"], result


@pytest.mark.known_bug(
    "LB-195",
    match="queryId is not set",
    reason="sql.streaming.queryId is not set: the test calls the writer outside foreachBatch",
)
def test_both_stream_ids_visible(result):
    """The I6 columns let operators see both racers' rows partitioned by
    stream id -- proves the not-exists branch wrote through the append
    path (which carries _stream_id) rather than an overwrite that would
    have squashed one racer's rows."""
    _ran(result)
    per = result["per_stream_rows"]
    assert per.get("qA", 0) > 0 and per.get("qB", 0) > 0, per
