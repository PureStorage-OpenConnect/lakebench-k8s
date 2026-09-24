"""Executed: c360 continuous writers leave exactly one copy of a replayed
micro-batch (E3, GOALS P4.3/P6.1).

Runs ``c360_stream_scenarios.py`` in a fresh JVM with the Iceberg and Delta
jars on the classpath. Set ``LB_SPARK_TEST_JARS`` to a directory holding
iceberg-spark-runtime-4.0_2.13, delta-spark_2.13 and delta-storage jars;
the test is skipped without it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

_JARS = os.environ.get("LB_SPARK_TEST_JARS", "")
_NEEDED = ("iceberg-spark-runtime", "delta-spark", "delta-storage")


def _have_jars() -> bool:
    if not _JARS or not Path(_JARS).is_dir():
        return False
    names = [p.name for p in Path(_JARS).glob("*.jar")]
    return all(any(n.startswith(k) for n in names) for k in _NEEDED)


pytestmark = pytest.mark.skipif(
    not _have_jars(), reason="LB_SPARK_TEST_JARS with Iceberg and Delta jars not set"
)


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    work = tmp_path_factory.mktemp("c360-replay")
    env = dict(os.environ)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    script = Path(__file__).with_name("c360_stream_scenarios.py")
    proc = subprocess.run(
        [sys.executable, str(script), _JARS, str(work)],
        capture_output=True,
        text=True,
        env=env,
        timeout=900,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_harness_really_replays(result):
    """Control: a plain append under the same crash writes the batch twice."""
    assert result["control_batches"] == [0, 0]
    assert result["control_rows"] == 2 * result["n"]


def test_iceberg_silver_replay_leaves_one_copy(result):
    assert result["ice_silver_batches"] == [0, 0]
    assert result["ice_silver_rows"] == result["expected_silver"]
    assert result["ice_silver_distinct_ids"] == result["expected_silver"]
    assert result["ice_silver_batch_ids"] == [0]


def test_iceberg_silver_recency_is_data_clocked(result):
    """The newest event scores 30 whatever the date the test runs."""
    assert result["ice_silver_recency_max"] == 30


def test_reused_silver_table_gains_batch_id(result):
    """A table without _batch_id is upgraded once, then replays are exact."""
    assert result["legacy_added"] is True
    assert result["legacy_added_again"] is False
    assert result["legacy_rows_batch3"] == 9
    assert result["legacy_rows"] == 5 + 9


def test_iceberg_bronze_replay_leaves_one_copy(result):
    assert result["ice_bronze_batches"] == [0, 0]
    assert result["ice_bronze_same_query_id"] is True
    assert result["ice_bronze_rows"] == result["n"]


def test_iceberg_bronze_fresh_checkpoint_is_not_skipped(result):
    """A new query's batch 0 is not mistaken for the old query's batch 0."""
    assert result["ice_bronze_rows_after_fresh"] == result["n"] + 10


def test_delta_silver_replay_leaves_one_copy(result):
    assert result["delta_silver_batches"] == [0, 0]
    assert result["delta_silver_rows"] == result["expected_silver"]


def test_delta_silver_fresh_checkpoint_is_not_skipped(result):
    """txnAppId carries the query id, so a fresh checkpoint still writes."""
    assert result["delta_silver_rows_after_fresh"] == result["expected_silver"] + 9


def test_delta_bronze_replay_leaves_one_copy(result):
    assert result["delta_bronze_batches"] == [0, 0]
    assert result["delta_bronze_rows"] == result["n"]
