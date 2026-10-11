"""resolve_data_clock(strict=True) raises SilverAbort when LB_DATA_CLOCK is
unset. The default (non-strict) fallback returns the measured date from
``df_fallback`` when the env var is unset.
"""

from __future__ import annotations

from datetime import date

import pytest

pytestmark = pytest.mark.usefixtures("load_script")


class _FakeDf:
    def agg(self, *_a, **_k):  # pragma: no cover -- not reached
        raise AssertionError("strict path must not touch df_fallback")


@pytest.mark.parametrize("df_fallback", [None, _FakeDf()], ids=["no-fallback", "fallback-df"])
def test_resolve_data_clock_strict_missing_env_raises(monkeypatch, df_fallback):
    """Silver C360 mains use strict=True: a missing LB_DATA_CLOCK stops the
    run rather than silently falling back to a measured date or NULL, even
    when a bronze DataFrame is available. Reaching this raise means the env
    plumbing broke."""
    from common import SilverAbort, resolve_data_clock

    monkeypatch.delenv("LB_DATA_CLOCK", raising=False)

    with pytest.raises(SilverAbort, match="LB_DATA_CLOCK"):
        resolve_data_clock(df_fallback=df_fallback, strict=True)


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
