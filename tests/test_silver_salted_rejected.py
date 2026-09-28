"""G1: SALTED strategy is refused at parse time.

The old code accepted `salted`, dispatched to SIMPLE, and logged
`Strategy: salted` -- a metrics-tag lie that violates invariant 5
(published evidence identifies what produced it). The new code raises
`SilverAbort` before any log line names the strategy.

Both silver_build.py and silver_build_delta.py have their own
`get_strategy_override` helper; test both.

The silver mains import pyspark at module top level; pyspark is not
guaranteed in the unit tier. Use source inspection to prove the
resolver refuses salted before any log line runs; a live-Spark version
of the assertion is exercised by tests/spark under the local-Spark tier.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

from common import SilverAbort  # noqa: E402, I001


# --- Direct behaviour: the common-only path raises SilverAbort ---


def test_silver_abort_symbol_is_public_from_common():
    """SilverAbort is exported from common.py so silver_build can import it."""
    from common import SilverAbort as Imported

    assert Imported is SilverAbort
    assert issubclass(SilverAbort, RuntimeError)


# --- Source inspection: each dispatch site refuses SALTED before it runs ---


@pytest.fixture
def iceberg_source() -> str:
    return (_SCRIPTS_DIR / "silver_build.py").read_text()


@pytest.fixture
def delta_source() -> str:
    return (_SCRIPTS_DIR / "silver_build_delta.py").read_text()


def _get_strategy_override_body(text: str) -> str:
    """Return the body of get_strategy_override (up to the next def)."""
    marker = "def get_strategy_override"
    idx = text.index(marker)
    tail = text[idx:]
    # cut at the next top-level def
    end = tail.index("\ndef ", 5)
    return tail[:end]


def test_iceberg_override_refuses_salted_in_source(iceberg_source):
    """silver_build.get_strategy_override raises SilverAbort for salted."""
    body = _get_strategy_override_body(iceberg_source)
    assert "SALTED.value" in body
    assert "raise SilverAbort" in body
    # The refuse-path sits BEFORE the `SilverStrategy(override.lower())`
    # coercion, so a salted override never reaches the log-and-run path.
    refuse_idx = body.index("SALTED strategy is deferred to v1.7")
    coerce_idx = body.index("SilverStrategy(override.lower())")
    assert refuse_idx < coerce_idx


def test_delta_override_refuses_salted_in_source(delta_source):
    """silver_build_delta.get_strategy_override raises SilverAbort for salted."""
    body = _get_strategy_override_body(delta_source)
    assert "SALTED.value" in body
    assert "raise SilverAbort" in body
    refuse_idx = body.index("SALTED strategy is deferred to v1.7")
    coerce_idx = body.index("SilverStrategy(override.lower())")
    assert refuse_idx < coerce_idx


def test_iceberg_dispatch_branch_no_longer_logs_and_runs_simple(iceberg_source):
    """The old dispatch was `log('...running SIMPLE'); silver_simple(...)`.

    Ensure that safety-net still raises rather than dispatching, so
    even a resolver bypass cannot mis-label metrics.
    """
    # Locate the SALTED branch in the dispatch.
    marker = "elif strategy == SilverStrategy.SALTED:"
    idx = iceberg_source.index(marker)
    tail = iceberg_source[idx : idx + 800]
    assert "raise SilverAbort" in tail
    assert "silver_simple(spark" not in tail.split("\nelse:")[0]


def test_delta_dispatch_branch_no_longer_logs_and_runs_simple(delta_source):
    marker = "elif strategy == SilverStrategy.SALTED:"
    idx = delta_source.index(marker)
    tail = delta_source[idx : idx + 800]
    assert "raise SilverAbort" in tail
    assert "silver_simple(spark" not in tail.split("\nelse:")[0]
