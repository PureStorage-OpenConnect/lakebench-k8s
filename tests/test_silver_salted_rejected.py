"""The silver build refuses a ``salted`` strategy override before any log
line names a strategy.

``salted`` once ran as SIMPLE while the metrics reported
``Strategy: salted``. Both silver builds resolve the override through
``common.silver_strategy_override``.
"""

from __future__ import annotations

import pytest


class _Conf:
    def __init__(self, value):
        self.value = value

    def get(self, key, default=None):
        return self.value if key == "spark.lb.silver.strategy" else default


def test_override_refuses_salted(load_script, monkeypatch):
    """A salted override aborts; it never runs as SIMPLE under a salted tag."""
    common = load_script("common")
    logged: list[str] = []
    monkeypatch.setattr(common, "log", logged.append)
    monkeypatch.delenv("LB_SILVER_STRATEGY", raising=False)
    spark = type("S", (), {})()
    for value in ("salted", "SALTED"):
        spark.conf = _Conf(value)
        with pytest.raises(common.SilverAbort):
            common.silver_strategy_override(spark)
    assert logged == []
    spark.conf = _Conf(None)
    monkeypatch.setenv("LB_SILVER_STRATEGY", "salted")
    with pytest.raises(common.SilverAbort):
        common.silver_strategy_override(spark)
