"""F2: pre-commit row-count assertion (assert_preflight_rows) refuses
to let ``silver_build_financial.main()`` write a silver table when a
bronze-derived DataFrame carries zero rows.

The helper lives in ``common.py`` so it can be unit-tested without
pyspark on the local host -- the assertion contract is ``df.count() >=
minimum``, and any Python object exposing ``.count()`` is enough to
exercise every branch of the gate. The A1-atomic pyspark test at
``tests/spark/test_a1_atomic_preflight_stages_before_write.py`` covers
the wiring inside ``main()``.

The F2 contract:

* ``rows >= minimum`` -> return the row count silently.
* ``rows <  minimum`` -> raise ``SilverAbort`` naming the target table
  and the observed row count; the caller (main()) must NOT have written
  any silver table yet -- see the A1-atomic test for the wiring proof.
* The abort message must name the ``A1-atomic + F2 gate`` marker so a
  grep across silver-build logs can distinguish an F2 refusal from the
  post-write LB-044 gate refusal.
* Default ``minimum=1`` enforces "no empty silver frames"; a caller may
  pass a tighter ``minimum`` (e.g. ``entities >= unique parties count``
  from bronze) as a future refinement -- F3/F4 are v1.7 candidates.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.usefixtures("load_script")


class _FakeDF:
    """Minimal stand-in for a Spark DataFrame -- only ``.count()`` matters."""

    def __init__(self, rows: int):
        self._rows = rows
        self.count_calls = 0

    def count(self) -> int:
        self.count_calls += 1
        return self._rows


def test_zero_rows_raises_with_named_frame():
    """Zero rows -> SilverAbort, message names the target silver table.

    Fails against the pre-fix tree (there is no ``assert_preflight_rows``
    helper), so this test also acts as the fix-reverted gate for the
    helper's introduction. Post-fix it passes.
    """
    from common import SilverAbort, assert_preflight_rows

    df = _FakeDF(rows=0)
    with pytest.raises(SilverAbort) as excinfo:
        assert_preflight_rows(df, "silver.entities")
    msg = str(excinfo.value)
    # The abort must name the failing frame so an operator reading the
    # log sees exactly which silver table's frame was empty.
    assert "silver.entities" in msg
    # And it must expose the observed row count and the expected minimum
    # so the log line documents the gate directly.
    assert "0 rows" in msg
    assert "expected >= 1" in msg
    # Distinct grep-marker so it is not confused with the LB-044 post-write
    # gate emitted by common.assert_progress.
    assert "A1-atomic" in msg or "F2" in msg
    # The helper must actually have called .count() (it did not short-circuit
    # on some stale attribute), so the check is real, not cached.
    assert df.count_calls == 1


def test_one_row_passes_silently():
    """>= minimum rows -> returns row count, no raise."""
    from common import assert_preflight_rows

    df = _FakeDF(rows=1)
    rows = assert_preflight_rows(df, "silver.transactions")
    assert rows == 1
    assert df.count_calls == 1


def test_returns_actual_row_count_for_logging():
    """The helper returns the observed count so the caller can log it."""
    from common import assert_preflight_rows

    df = _FakeDF(rows=17)
    rows = assert_preflight_rows(df, "silver.counterparty_edges")
    assert rows == 17


def test_below_custom_minimum_raises():
    """A tighter ``minimum`` catches an under-count that ``>= 1`` misses.

    F2 allows a caller to pass a known lower bound (e.g. entities >=
    unique parties count from bronze). The helper must honour it.
    """
    from common import SilverAbort, assert_preflight_rows

    df = _FakeDF(rows=3)
    with pytest.raises(SilverAbort) as excinfo:
        assert_preflight_rows(df, "silver.entities", minimum=5)
    msg = str(excinfo.value)
    assert "silver.entities" in msg
    assert "3 rows" in msg
    assert "expected >= 5" in msg


def test_at_custom_minimum_passes():
    """``rows == minimum`` is a pass, not a fence-post fail."""
    from common import assert_preflight_rows

    df = _FakeDF(rows=5)
    assert assert_preflight_rows(df, "silver.entities", minimum=5) == 5


def test_helper_is_exposed_from_common():
    """The helper is importable from ``common`` -- ``main()`` imports it
    by name and a rename would silently drop the pre-flight gate.
    """
    import common

    assert hasattr(common, "assert_preflight_rows"), (
        "assert_preflight_rows must live in common.py so silver_build_financial "
        "imports the same helper as the F2 tests"
    )
