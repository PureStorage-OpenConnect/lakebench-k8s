"""C1: resolve_data_clock(strict=True) raises SilverAbort when LB_DATA_CLOCK
is unset. Default (non-strict) fallback keeps the current behaviour.

Two cases:
  - strict + LB_DATA_CLOCK missing -> SilverAbort
  - default (non-strict) fallback path unchanged: returns the measured
    date from ``df_fallback`` when the env var is unset.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"))


def test_resolve_data_clock_strict_missing_env_raises(monkeypatch):
    """C1: silver C360 mains use strict=True; a missing LB_DATA_CLOCK
    stops the run rather than silently falling back to a measured date
    or NULL. C2 always sets LB_DATA_CLOCK in the silver env bundle, so
    reaching this raise means the env plumbing broke.
    """
    from common import SilverAbort, resolve_data_clock

    monkeypatch.delenv("LB_DATA_CLOCK", raising=False)

    with pytest.raises(SilverAbort, match="LB_DATA_CLOCK"):
        resolve_data_clock(df_fallback=None, strict=True)


def test_resolve_data_clock_strict_missing_env_raises_even_with_fallback_df(monkeypatch):
    """strict=True refuses to silently measure a fallback anchor even when
    a bronze DataFrame is available. The whole point of strict is to trust
    C2's env resolution or fail loud."""
    from common import SilverAbort, resolve_data_clock

    monkeypatch.delenv("LB_DATA_CLOCK", raising=False)

    class FakeDf:
        def agg(self, *_a, **_k):  # pragma: no cover -- not reached
            raise AssertionError("strict path must not touch df_fallback")

    with pytest.raises(SilverAbort, match="LB_DATA_CLOCK"):
        resolve_data_clock(df_fallback=FakeDf(), strict=True)


def test_resolve_data_clock_default_fallback_unchanged(monkeypatch):
    """Non-strict callers (AML, bronze, gold) keep the existing behaviour:
    when LB_DATA_CLOCK is unset and a fallback DF is given, the anchor is
    measured from it; when no DF is given, the anchor is None."""
    import common

    monkeypatch.delenv("LB_DATA_CLOCK", raising=False)

    # Patch data_clock_date to avoid needing pyspark in this test.
    monkeypatch.setattr(common, "data_clock_date", lambda _df: date(2025, 6, 15))

    assert common.resolve_data_clock(df_fallback=object()) == date(2025, 6, 15)
    assert common.resolve_data_clock(df_fallback=None) is None


def test_resolve_data_clock_strict_from_env(monkeypatch):
    """A set LB_DATA_CLOCK returns its parsed anchor whether strict or not."""
    from common import resolve_data_clock

    monkeypatch.setenv("LB_DATA_CLOCK", "2025-07-01")

    # timestamp_end is exclusive, so the anchor is 2025-06-30.
    assert resolve_data_clock(df_fallback=None, strict=True) == date(2025, 6, 30)
    assert resolve_data_clock(df_fallback=None) == date(2025, 6, 30)
