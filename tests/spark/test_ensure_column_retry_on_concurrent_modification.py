"""I9: startup ``ensure_column`` race.

Multiple silver stream/batch drivers race on the same Iceberg (or Delta)
catalog table during startup. Their ``ensure_column`` calls -- ``ALTER TABLE
... ADD COLUMNS`` -- collide: the loser sees the other's commit, its own
commit fails with a Java ``ConcurrentModificationException`` (Iceberg) or an
equivalent Delta metadata-conflict exception, and the whole silver run crashes
before it processes a batch.

The fix wraps ``ensure_column`` with bounded retries: on
``ConcurrentModificationException`` we re-check the schema (the concurrent
writer may already have added the column, in which case we return cleanly
without a second ``ALTER``), then retry the ``ALTER`` at most ``max_attempts``
times, propagating after that.

This test mocks the ALTER call to raise once, then succeed; the retry wrapper
must eat the first raise and complete the second attempt cleanly.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# The retry wrapper is a pure-Python control-flow helper; the tests mock out
# Spark entirely with a fake session, so pyspark itself is not needed here.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


class _FakeSpark:
    """Minimal Spark stand-in: one table with a fixed column set and one ALTER
    handler under test control."""

    def __init__(self, initial_columns, alter_side_effect):
        self._columns = list(initial_columns)
        self._alter_side_effect = alter_side_effect
        self.alter_calls = 0
        self.column_reads = 0

    def table(self, _fq_table):
        outer = self

        class _T:
            @property
            def columns(self_inner):
                outer.column_reads += 1
                return list(outer._columns)

        return _T()

    def sql(self, stmt):
        assert stmt.strip().upper().startswith("ALTER TABLE")
        self.alter_calls += 1
        # Extract the added column name (very rough parse) so the second
        # attempt sees a schema that already contains it, matching real
        # ALTER semantics.
        result = self._alter_side_effect(self.alter_calls)
        if isinstance(result, Exception):
            raise result
        # side-effect returned None -> success; add the column to state.
        # Column name is the token after "ADD COLUMNS (".
        import re

        m = re.search(r"ADD\s+COLUMNS\s*\(\s*(\S+)", stmt, re.IGNORECASE)
        if m and m.group(1) not in self._columns:
            self._columns.append(m.group(1))
        return None


def test_ensure_column_retry_eats_first_conflict_and_succeeds_second():
    from common import ensure_column_with_retry

    def side_effect(call_num):
        if call_num == 1:
            # Simulate the Iceberg-side error surface: a Py4J-wrapped Java
            # exception whose class name contains ``ConcurrentModificationException``.
            return RuntimeError(
                "org.apache.iceberg.exceptions.CommitFailedException: "
                "Cannot commit, concurrent modification: ConcurrentModificationException"
            )
        return None

    fake = _FakeSpark(initial_columns=["a", "b"], alter_side_effect=side_effect)
    added = ensure_column_with_retry(
        fake, "cat.silver.t", "c", "BIGINT", max_attempts=3, backoff_seconds=0
    )
    # The column was in fact added on the second attempt.
    assert added is True
    assert fake.alter_calls == 2, f"expected exactly 2 ALTER attempts, saw {fake.alter_calls}"
    assert "c" in fake._columns


def test_ensure_column_retry_clean_when_concurrent_writer_added_it():
    """If the losing writer's ALTER fails and by then the other writer has
    already added the column, we must recognise the column-already-present
    branch and return False without a second ALTER."""
    from common import ensure_column_with_retry

    fake = _FakeSpark(initial_columns=["a", "b"], alter_side_effect=lambda n: None)

    def side_effect(call_num):
        if call_num == 1:
            # Simulate the concurrent writer beating us to it: add ``c`` to
            # the fake state before the exception surfaces.
            fake._columns.append("c")
            return RuntimeError("Iceberg CommitFailedException: ConcurrentModificationException")
        return None

    fake._alter_side_effect = side_effect
    added = ensure_column_with_retry(
        fake, "cat.silver.t", "c", "BIGINT", max_attempts=3, backoff_seconds=0
    )
    # ALTER ran once, saw the conflict, and the re-check saw the column
    # already present -> the wrapper returns cleanly without a second ALTER.
    assert added is False
    assert fake.alter_calls == 1


def test_ensure_column_retry_propagates_after_bounded_attempts():
    """After ``max_attempts`` conflicts, the last exception is re-raised so
    the caller does not spin silently."""
    from common import ensure_column_with_retry

    def side_effect(_call_num):
        return RuntimeError("Iceberg ConcurrentModificationException: still racing")

    fake = _FakeSpark(initial_columns=["a", "b"], alter_side_effect=side_effect)
    with pytest.raises(RuntimeError, match="ConcurrentModificationException"):
        ensure_column_with_retry(
            fake, "cat.silver.t", "c", "BIGINT", max_attempts=3, backoff_seconds=0
        )
    assert fake.alter_calls == 3


def test_ensure_column_retry_does_not_swallow_unrelated_errors():
    """A non-conflict error must propagate on the first attempt: a wrong-type
    ALTER should not be retried into oblivion."""
    from common import ensure_column_with_retry

    def side_effect(_call_num):
        return RuntimeError("AnalysisException: cannot ALTER TABLE, syntax error at 'FOO'")

    fake = _FakeSpark(initial_columns=["a", "b"], alter_side_effect=side_effect)
    with pytest.raises(RuntimeError, match="AnalysisException"):
        ensure_column_with_retry(
            fake, "cat.silver.t", "c", "BIGINT", max_attempts=3, backoff_seconds=0
        )
    assert fake.alter_calls == 1


def test_ensure_column_is_a_no_op_when_column_already_present():
    from common import ensure_column_with_retry

    fake = _FakeSpark(initial_columns=["a", "b", "c"], alter_side_effect=lambda n: None)
    added = ensure_column_with_retry(
        fake, "cat.silver.t", "c", "BIGINT", max_attempts=3, backoff_seconds=0
    )
    assert added is False
    assert fake.alter_calls == 0
