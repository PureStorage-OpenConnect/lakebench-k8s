"""Executed: a reset or stale rebuild epoch cannot make Delta skip a c360
silver cycle as already committed.

Delta skips a write whose (txnAppId, txnVersion) its log already holds. The
batch silver build keys cycles 1..N on the rebuild epoch from the
``lakebench-silver-state`` ConfigMap, whose lifetime is not the table's: when
the counter reads lower than an epoch the table already used, the new run's
cycles are skipped, silver is short and the run still exits 0. The build now
takes the epoch from the table's own transaction log.

Runs ``delta_silver_epoch_scenarios.py`` in fresh JVMs with the Delta jars on
the classpath. Set ``LB_SPARK_TEST_JARS`` to a directory holding the
delta-spark and delta-storage jars for the installed Spark; the test is
skipped without it, and fails instead when ``LB_REQUIRE_JARS=1``.
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
_NEEDED = ("delta-spark", "delta-storage")


def _have_jars() -> bool:
    if not _JARS or not Path(_JARS).is_dir():
        return False
    names = [p.name for p in Path(_JARS).glob("*.jar")]
    return all(any(n.startswith(k) for n in names) for k in _NEEDED)


if not _have_jars() and os.environ.get("LB_REQUIRE_JARS") == "1":
    raise RuntimeError("LB_REQUIRE_JARS=1 but LB_SPARK_TEST_JARS has no Delta jars")

pytestmark = pytest.mark.skipif(
    not _have_jars(), reason="LB_SPARK_TEST_JARS with Delta jars not set"
)

FULL_RUN = {"1:0": 9, "1:1": 9, "1:2": 9}


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    work = tmp_path_factory.mktemp("delta-epoch")
    env = dict(os.environ)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    script = Path(__file__).with_name("delta_silver_epoch_scenarios.py")
    proc = subprocess.run(
        [sys.executable, str(script), _JARS, str(work)],
        capture_output=True,
        text=True,
        env=env,
        timeout=1800,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _why(case):
    return json.dumps([{k: e[k] for k in ("cycle", "epoch", "rc")} for e in case["log"]]) + (
        "\n" + case["log"][-1]["tail"]
    )


def test_epoch_reset_keeps_every_cycle_of_the_new_run(result):
    """Run 1 restarts at epoch 0 with --force-rebuild over run 0's epoch-0 log."""
    case = result["epoch_reset"]
    assert case["rcs"] == [[0, 0, 0], [0, 0, 0]], _why(case)
    assert case["held"] == FULL_RUN, _why(case)


def test_one_stale_cycle_epoch_is_not_skipped(result):
    """Cycle 1 reads epoch 0 while cycles 0 and 2 read 1 (STREAMING strategy)."""
    case = result["stale_cycle"]
    assert case["rcs"] == [[0, 0, 0], [0, 0, 0]], _why(case)
    assert case["held"] == FULL_RUN, _why(case)


def test_operator_retry_of_a_committed_cycle_is_still_a_no_op(result):
    """The idempotence the txn keys exist for survives the change."""
    case = result["stale_cycle"]
    assert case["retry_rc"] == 0, _why(case)
    assert case["held_after_retry"] == case["held"], _why(case)


def test_catalog_lost_cycle0_refuses_the_surviving_log(result):
    """With the metastore gone, cycle 0 refuses the old log instead of
    appending to it, so no old rows are adopted and no cycle is skipped."""
    case = result["catalog_lost"]
    assert case["rcs"][0] == [0, 0, 0], _why(case)
    assert case["rcs"][1] != [0, 0, 0] and case["rcs"][1][0] != 0, _why(case)
    assert case["refused_orphan_log"], _why(case)
    assert case["held"] == {"0:0": 9, "0:1": 9, "0:2": 9}, _why(case)
