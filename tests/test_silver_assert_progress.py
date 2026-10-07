"""A1 (LB-044 gate): assert_progress refuses exit-0 on zero silver rows.

The gate lands in common.py and is called at end-of-main by every silver
script (three batches + three streams). These unit tests exercise the
helper directly; the end-to-end wiring is covered by tests/spark tests
that invoke the mains against a local Iceberg fixture (out of A1's unit
budget -- those are the A1 local-Spark tier).
"""

from __future__ import annotations

import pytest


@pytest.fixture
def common(load_script):
    return load_script("common")


def test_assert_progress_raises_on_zero(monkeypatch, common):
    """The gate's core contract: zero rows written is not a pass."""
    monkeypatch.delenv("LB_SILVER_TEST_ALLOW_EMPTY", raising=False)
    monkeypatch.delenv("LB_TESTING", raising=False)
    with pytest.raises(common.SilverAbort, match="zero rows written"):
        common.assert_progress(0, "silver-build")


def test_bypass_requires_both_env_vars(monkeypatch, common):
    """LB_SILVER_TEST_ALLOW_EMPTY alone must not silence the gate.

    Belt-and-braces against a production caller accidentally setting one
    of the two flags. Both must be present, and the LB_TESTING flag
    signals a test harness rather than a job.py deploy.
    """
    # Only ALLOW_EMPTY set: still raises.
    monkeypatch.setenv("LB_SILVER_TEST_ALLOW_EMPTY", "1")
    monkeypatch.delenv("LB_TESTING", raising=False)
    with pytest.raises(common.SilverAbort):
        common.assert_progress(0, "silver-build")

    # Only LB_TESTING set: still raises.
    monkeypatch.delenv("LB_SILVER_TEST_ALLOW_EMPTY", raising=False)
    monkeypatch.setenv("LB_TESTING", "1")
    with pytest.raises(common.SilverAbort):
        common.assert_progress(0, "silver-build")

    # Both set: bypassed (test-only path).
    monkeypatch.setenv("LB_SILVER_TEST_ALLOW_EMPTY", "1")
    monkeypatch.setenv("LB_TESTING", "1")
    assert common.assert_progress(0, "silver-build") is None


def test_bypass_only_at_exact_value_1(monkeypatch, common):
    """Truthy strings other than "1" must not silence the gate.

    "true", "yes", "0" all raise -- the bypass is deliberate and
    requires the exact opt-in.
    """
    monkeypatch.setenv("LB_SILVER_TEST_ALLOW_EMPTY", "true")
    monkeypatch.setenv("LB_TESTING", "true")
    with pytest.raises(common.SilverAbort):
        common.assert_progress(0, "silver-build")

    monkeypatch.setenv("LB_SILVER_TEST_ALLOW_EMPTY", "1")
    monkeypatch.setenv("LB_TESTING", "0")
    with pytest.raises(common.SilverAbort):
        common.assert_progress(0, "silver-build")
