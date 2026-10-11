"""Executed: a reset or stale rebuild epoch cannot make Delta skip a c360
silver cycle as already committed.

Delta skips a write whose (txnAppId, txnVersion) its log already holds. The
batch silver build keys cycles 1..N on the rebuild epoch from the
``lakebench-silver-state`` ConfigMap, whose lifetime is not the table's: when
the counter reads lower than an epoch the table already used, the new run's
cycles are skipped, silver is short and the run still exits 0. The build now
takes the epoch from the table's own transaction log.

Runs ``delta_silver_epoch_scenarios.py`` in fresh JVMs with the Delta jars
from ``LB_SPARK_TEST_JARS`` on the classpath.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("delta")


@pytest.fixture(scope="module")
def result(tmp_path_factory, spark_subprocess, spark_jars):
    work = tmp_path_factory.mktemp("delta-epoch")
    script = Path(__file__).with_name("delta_silver_epoch_scenarios.py")
    proc = spark_subprocess(script, spark_jars.classpath, work, timeout=1800)
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _why(case):
    return json.dumps([{k: e[k] for k in ("cycle", "epoch", "rc")} for e in case["log"]]) + (
        "\n" + case["log"][-1]["tail"]
    )


def test_epoch_reset_keeps_every_cycle_of_the_new_run(result):
    """Run 1 restarts at epoch 0 with --force-rebuild over run 0's epoch-0 log."""
    case = result["epoch_reset"]
    assert case["rcs"] == [[0, 0], [0, 0]], _why(case)
    assert case["held"] == {"1:0": 9, "1:1": 9}, _why(case)


def test_one_stale_cycle_epoch_is_not_skipped(result):
    """Cycle 1 reads epoch 0 while cycles 0 and 2 read 1 (STREAMING strategy)."""
    case = result["stale_cycle"]
    assert case["rcs"] == [[0, 0, 0], [0, 0, 0]], _why(case)
    assert case["held"] == {"1:0": 9, "1:1": 9, "1:2": 9}, _why(case)


def test_operator_retry_of_a_committed_cycle_is_still_a_no_op(result):
    """The idempotence the txn keys exist for survives the change."""
    case = result["stale_cycle"]
    assert case["retry_rc"] == 0, _why(case)
    assert case["held_after_retry"] == case["held"], _why(case)


def test_precondition_deciding_reads_come_from_a_checkpoint(result):
    """Scenario precondition, not product behaviour: the cycle that would
    collide reads its keys with a checkpoint already in the log
    (checkpointInterval 2), as on a long-lived table, so the scenarios above
    exercise the checkpoint read path."""
    reset = result["epoch_reset"]["log"][-1]
    assert reset["cycle"] == 1 and reset["checkpoints_before"] >= 1
    stale = [e for e in result["stale_cycle"]["log"] if e["epoch"] == 0 and e["cycle"] == 1][-1]
    assert stale["checkpoints_before"] > 0


def test_catalog_lost_cycle0_refuses_the_surviving_log(result):
    """With the metastore gone, cycle 0 refuses the old log instead of
    appending to it, so no old rows are adopted and no cycle is skipped."""
    case = result["catalog_lost"]
    assert case["rcs"] == [[0, 0], [1]], _why(case)
    assert case["refused_orphan_log"], _why(case)
    assert case["held"] == {"0:0": 9, "0:1": 9}, _why(case)


def test_later_cycle_rebuild_after_the_remedy_and_its_retry(result):
    """A cycle that finds no table builds it from every cycle's files; the
    create records its key, so an operator retry is skipped, not appended."""
    case = result["catalog_lost"]
    assert case["rebuild_rc"] == 0, _why(case)
    assert case["held_after_rebuild"] == {"1:0": 9, "1:1": 9}, _why(case)
    assert case["retry_rc"] == 0, _why(case)
    assert case["held_after_retry"] == case["held_after_rebuild"], _why(case)
