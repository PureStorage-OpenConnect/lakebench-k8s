"""Executed: c360 continuous writers leave exactly one copy of a replayed
micro-batch (E3, GOALS P4.3/P6.1).

Runs ``c360_stream_scenarios.py`` in a fresh JVM with the Iceberg and Delta
jars from ``LB_SPARK_TEST_JARS`` on the classpath.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg", "delta")


@pytest.fixture(scope="module")
def result(tmp_path_factory, spark_subprocess, spark_jars):
    work = tmp_path_factory.mktemp("c360-replay")
    script = Path(__file__).with_name("c360_stream_scenarios.py")
    proc = spark_subprocess(script, spark_jars.classpath, work, timeout=900)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _section_ran(result, section):
    """A Delta section that raised in the child fails its tests with the
    child's error (the child records it so the Iceberg sections still run)."""
    if f"{section}_error" in result:
        pytest.fail(f"{section} failed in the child: {result[section + '_error']}", pytrace=False)


# Each writer: 3 one-file micro-batches of 10 rows; the run crashes right
# after committing batch 1 (an append onto the existing table, not the
# create), and the restart from the same checkpoint replays batch 1.
_REPLAYED = [0, 1, 1, 2]


def test_harness_really_replays(result):
    """Control: a plain append under the same crash writes batch 1 twice."""
    assert result["control_batches"] == _REPLAYED
    assert result["control_rows"] == result["bronze_rows"] + 10


def test_iceberg_silver_replay_leaves_one_copy(result):
    assert result["ice_silver_batches"] == _REPLAYED
    assert result["ice_silver_same_query"] is True
    assert result["ice_silver_rows"] == result["silver_rows"]
    assert result["ice_silver_distinct_ids"] == result["silver_rows"]


def test_iceberg_silver_deletes_only_on_a_real_replay(result):
    """One delete snapshot, the replay's; no empty delete per micro-batch."""
    assert result["ice_silver_delete_snapshots"] == 1
    assert result["ice_silver_delete_snapshots_after_fresh"] == 1


def test_iceberg_silver_fresh_checkpoint_keeps_earlier_stream(result):
    """A new query's batch 0 must not delete the previous query's batch 0
    (the key was the bare batch id: 45 rows plus 3 ended at 39)."""
    assert result["ice_silver_old_batch0_rows"] == 9
    assert result["ice_silver_new_stream_rows"] == 3
    assert result["ice_silver_rows_after_fresh"] == result["silver_rows"] + 3


def test_iceberg_silver_recency_is_data_clocked(result):
    """The newest event scores 30 whatever the date the test runs."""
    assert result["ice_silver_recency_max"] == 30


def test_fresh_checkpoint_over_full_table_is_refused(result):
    assert result["refuse_fresh"] is True
    assert result["refuse_allows_used_or_empty"] is True


def test_reused_silver_table_gains_key_columns(result):
    """A table without the key is upgraded once, then a replay is exact."""
    assert result["legacy_added"] is True
    assert result["legacy_added_again"] is False
    assert result["legacy_rows"] == 5 + 9


def test_iceberg_bronze_replay_is_skipped(result):
    assert result["ice_bronze_log"] == [[0, 10], [1, 10], [1, 0], [2, 10]]
    assert result["ice_bronze_same_query_id"] is True
    assert result["ice_bronze_rows"] == result["bronze_rows"]


def test_iceberg_bronze_fresh_checkpoint_is_not_skipped(result):
    """A new query's batch 0 is not mistaken for the old query's batch 0."""
    assert result["ice_bronze_rows_after_fresh"] == result["bronze_rows"] + 10


def test_delta_silver_replay_is_skipped_and_reported(result):
    """Delta skips the replay; the writer sees it and reports 0 rows."""
    _section_ran(result, "delta_silver")
    assert result["delta_silver_log"] == [[0, 9], [1, 9], [1, 0], [2, 9]]
    assert result["delta_silver_rows"] == result["silver_rows"]


def test_delta_silver_fresh_checkpoint_is_not_skipped(result):
    """txnAppId carries the query id, so a fresh checkpoint still writes."""
    _section_ran(result, "delta_silver")
    assert result["delta_silver_rows_after_fresh"] == result["silver_rows"] + 9


def test_delta_bronze_replay_is_skipped_and_reported(result):
    _section_ran(result, "delta_bronze")
    assert result["delta_bronze_log"] == [[0, 10], [1, 10], [1, 0], [2, 10]]
    assert result["delta_bronze_rows"] == result["bronze_rows"]


_GOLD = [
    ["2024-06-01", 1],
    ["2024-06-02", 1],
    ["2024-06-03", 1],
    ["2024-06-04", 2],
    ["2024-06-05", 2],
    ["2024-06-06", 2],
]


def test_gold_incremental_boundary_replace_is_one_commit(result):
    """Days before the watermark kept, from it on replaced, in one commit."""
    assert result["ice_gold"] == _GOLD
    assert result["ice_gold_commits"] == 1
    assert result["delta_gold"] == _GOLD
    assert result["delta_gold_commits"] == 1
