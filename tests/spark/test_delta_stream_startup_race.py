"""B4: two concurrent silver_stream_delta writers with different stream ids
racing the not-exists branch on the same Delta table converge on a metadata-
only ``CREATE TABLE IF NOT EXISTS`` commit; both then take the append path
without data loss.

Runs the scenario in a fresh JVM with the Delta jars on the classpath. Set
``LB_SPARK_TEST_JARS`` to a directory holding delta-spark_2.13 and
delta-storage jars (Iceberg too: the shared session registers an Iceberg
catalog). Without the jars, the local unit lane skips this and the plan's
Wave-4 live gate covers B4 at scale.
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
    not _have_jars(),
    reason="LB_SPARK_TEST_JARS with Iceberg and Delta jars not set; B4 covered by Wave-4 live gate",
)


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    work = tmp_path_factory.mktemp("delta-startup-race")
    env = dict(os.environ)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    script = Path(__file__).with_name("delta_stream_mech_scenarios.py")
    proc = subprocess.run(
        [sys.executable, str(script), "startup_race", _JARS, str(work)],
        capture_output=True,
        text=True,
        env=env,
        timeout=900,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_no_errors(result):
    assert result["errors"] == [], result


def test_both_racers_wrote(result):
    """Both racers commit non-zero rows: neither is Delta-deduped by
    (appId, version) because they used distinct app ids."""
    written = result["written"]
    assert set(written.keys()) == {"A", "B"}, written
    assert written["A"] > 0 and written["B"] > 0, written


def test_no_data_loss(result):
    """After both writes settle, the table holds both racers' rows."""
    assert result["table_rows"] == result["written"]["A"] + result["written"]["B"], result


def test_both_stream_ids_visible(result):
    """The I6 columns let operators see both racers' rows partitioned by
    stream id -- proves the not-exists branch wrote through the append
    path (which carries _stream_id) rather than an overwrite that would
    have squashed one racer's rows."""
    per = result["per_stream_rows"]
    assert per.get("qA", 0) > 0 and per.get("qB", 0) > 0, per
