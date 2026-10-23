"""assert_progress refuses exit-0 on zero silver rows.

The gate lands in common.py and is called at end-of-main by every silver
script (three batches + three streams). This unit test exercises the
helper directly; the end-to-end wiring is covered by tests/spark tests
that invoke the mains against a local Iceberg fixture.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def common(load_script):
    return load_script("common")


def test_assert_progress_raises_on_zero(common):
    """The gate's core contract: zero rows written is not a pass."""
    with pytest.raises(common.SilverAbort, match="zero rows written"):
        common.assert_progress(0, "silver-build")
