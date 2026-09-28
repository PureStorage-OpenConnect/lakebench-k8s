"""A1 (LB-044 gate): assert_progress refuses exit-0 on zero silver rows.

The gate lands in common.py and is called at end-of-main by every silver
script (three batches + three streams). These unit tests exercise the
helper directly; the end-to-end wiring is covered by tests/spark tests
that invoke the mains against a local Iceberg fixture (out of A1's unit
budget -- those are the A1 local-Spark tier).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"))

from common import SilverAbort, assert_progress  # noqa: E402


def test_assert_progress_raises_on_zero(monkeypatch):
    """The gate's core contract: zero rows written is not a pass."""
    monkeypatch.delenv("LB_SILVER_TEST_ALLOW_EMPTY", raising=False)
    monkeypatch.delenv("LB_TESTING", raising=False)
    with pytest.raises(SilverAbort, match="zero rows written"):
        assert_progress(0, "silver-build")


def test_assert_progress_accepts_nonzero(monkeypatch):
    """One row committed is enough; the gate's purpose is empty runs."""
    monkeypatch.delenv("LB_SILVER_TEST_ALLOW_EMPTY", raising=False)
    monkeypatch.delenv("LB_TESTING", raising=False)
    # Any of the following raising would fail the test; a return is a pass.
    assert assert_progress(1, "silver-build") is None
    assert assert_progress(1_000_000, "silver-stream") is None


def test_bypass_requires_both_env_vars(monkeypatch):
    """LB_SILVER_TEST_ALLOW_EMPTY alone must not silence the gate.

    Belt-and-braces against a production caller accidentally setting one
    of the two flags. Both must be present, and the LB_TESTING flag
    signals a test harness rather than a job.py deploy.
    """
    # Only ALLOW_EMPTY set: still raises.
    monkeypatch.setenv("LB_SILVER_TEST_ALLOW_EMPTY", "1")
    monkeypatch.delenv("LB_TESTING", raising=False)
    with pytest.raises(SilverAbort):
        assert_progress(0, "silver-build")

    # Only LB_TESTING set: still raises.
    monkeypatch.delenv("LB_SILVER_TEST_ALLOW_EMPTY", raising=False)
    monkeypatch.setenv("LB_TESTING", "1")
    with pytest.raises(SilverAbort):
        assert_progress(0, "silver-build")

    # Both set: bypassed (test-only path).
    monkeypatch.setenv("LB_SILVER_TEST_ALLOW_EMPTY", "1")
    monkeypatch.setenv("LB_TESTING", "1")
    assert assert_progress(0, "silver-build") is None


def test_bypass_only_at_exact_value_1(monkeypatch):
    """Truthy strings other than "1" must not silence the gate.

    "true", "yes", "0" all raise -- the bypass is deliberate and
    requires the exact opt-in.
    """
    monkeypatch.setenv("LB_SILVER_TEST_ALLOW_EMPTY", "true")
    monkeypatch.setenv("LB_TESTING", "true")
    with pytest.raises(SilverAbort):
        assert_progress(0, "silver-build")

    monkeypatch.setenv("LB_SILVER_TEST_ALLOW_EMPTY", "1")
    monkeypatch.setenv("LB_TESTING", "0")
    with pytest.raises(SilverAbort):
        assert_progress(0, "silver-build")


def test_message_names_job_type_and_gate(monkeypatch):
    """Failure diagnostic names the job that failed and the gate id.

    The K8s Job's exit message ends up in the operator log; naming the
    invariant here saves a manual grep.
    """
    monkeypatch.delenv("LB_SILVER_TEST_ALLOW_EMPTY", raising=False)
    monkeypatch.delenv("LB_TESTING", raising=False)
    with pytest.raises(SilverAbort) as exc:
        assert_progress(0, "silver-stream-financial")
    msg = str(exc.value)
    assert "silver-stream-financial" in msg
    assert "LB-044" in msg


def test_accepts_string_int_from_iceberg_stats(monkeypatch):
    """iceberg_table_stats returns int; callers may pass float-ish too."""
    monkeypatch.delenv("LB_SILVER_TEST_ALLOW_EMPTY", raising=False)
    monkeypatch.delenv("LB_TESTING", raising=False)
    assert assert_progress(1.0, "silver-build") is None
    with pytest.raises(SilverAbort):
        assert_progress(0.0, "silver-build")
