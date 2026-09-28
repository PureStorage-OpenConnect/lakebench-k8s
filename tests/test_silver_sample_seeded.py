"""G3: silver_build profile sampling is seeded from LB_SEED.

Without a seed, `DataFrame.sample(fraction)` picks a different subset on
every run and the profile numbers (customer count, skew, date range)
drift. The fix passes `seed=int(os.getenv("LB_SEED", "0"))` at both
call sites; LB_SEED is exported by job.py:_build_env_vars alongside
LB_DATA_CLOCK so the driver sees a resolved value.

The silver_build modules import pyspark at top level; the unit tier
has no pyspark, so this test proves the invariant via source
inspection. A live-Spark exercise of the seeded profile lives in
tests/spark under the local-Spark tier.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


@pytest.mark.parametrize(
    "script",
    ["silver_build.py", "silver_build_delta.py"],
)
def test_sample_call_seeded_from_lb_seed(script):
    """The `.sample(...)` call site takes `seed=int(os.getenv("LB_SEED", "0"))`.

    Anchored on the profile-sampling call, not any other .sample() the
    file might grow later.
    """
    src = (_SCRIPTS_DIR / script).read_text()
    # LB_SEED-derived seed variable is defined next to the sample call.
    assert 'sample_seed = int(os.getenv("LB_SEED", "0"))' in src, (
        f"{script}: seed missing; profile sampling would drift on every run"
    )
    # The seed variable is passed into .sample(...).
    sample_call = re.search(
        r"transactions\.sample\(\s*sample_fraction\s*,\s*seed\s*=\s*sample_seed\s*\)",
        src,
    )
    assert sample_call is not None, (
        f"{script}: transactions.sample() must be called with seed=sample_seed"
    )


def test_job_env_bundle_exports_lb_seed():
    """job.py:_build_env_vars must ship LB_SEED to the driver.

    The pod's silver-build only sees env vars _build_env_vars sets. If
    LB_SEED is not there the driver's `os.getenv("LB_SEED", "0")` falls
    back to 0 for every deployment, so the seed override never takes.
    """
    path = (
        Path(__file__).resolve().parents[1] / "src/lakebench/modules/pipeline_engines/spark/job.py"
    )
    text = path.read_text()
    assert '"name": "LB_SEED"' in text, (
        "LB_SEED not exported from job.py:_build_env_vars; silver-build "
        "profile sampling would fall back to seed=0 for every deployment"
    )
    # And in the same helper as LB_DATA_CLOCK, so the two travel together.
    method = text.split("def _build_env_vars", 1)[1]
    method = method.split("\n    def ", 1)[0]
    assert '"name": "LB_SEED"' in method
    assert '"name": "LB_DATA_CLOCK"' in method
