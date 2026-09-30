"""I8: ``silver_stream_delta.write_silver_batch`` returns the numOutputRows
this write itself committed, read from its own history row. Immune to
interleaved metadata commits (compaction/vacuum stand-ins) that would
have moved the before/after ``delta_table_version`` bracket.

Runs in a fresh JVM with Delta jars via ``LB_SPARK_TEST_JARS``. Without the
jars the local unit lane skips; the plan's Wave-3 live gate covers I8 at
scale.
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
    reason="LB_SPARK_TEST_JARS with Iceberg and Delta jars not set; I8 covered by Wave-3 live gate",
)


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    work = tmp_path_factory.mktemp("delta-version-race")
    env = dict(os.environ)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    script = Path(__file__).with_name("delta_stream_mech_scenarios.py")
    proc = subprocess.run(
        [sys.executable, str(script), "version_race", _JARS, str(work)],
        capture_output=True,
        text=True,
        env=env,
        timeout=900,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-4000:]
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
