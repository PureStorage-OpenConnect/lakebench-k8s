"""Drive a stream script's ``main()`` through its startup steps on a stub
Spark session, so a test can check which startup calls it makes.

The stub session records every ``spark.sql`` statement, hands back inert
read and write chains, and starts a query that is already inactive, so the
script's wait loop exits at once. The one startup step that needs a real
JVM and does not catch its own failure, the fresh-checkpoint refusal
(``checkpoint_is_fresh`` reads ``spark._jvm``), is replaced with a no-op;
the stream-marker helpers log and carry on, so they run as they are.

These tests import the scripts, which import pyspark, so they live in the
Spark tier: CI's unit legs have no pyspark and would skip them.
"""

from __future__ import annotations

import types
from typing import Any

# Startup helpers that need the JVM and raise without it.
JVM_STEPS = ("refuse_fresh_checkpoint_over_data",)


class _Query:
    isActive = False
    lastProgress = None

    def exception(self) -> None:
        return None

    def stop(self) -> None:
        pass

    def awaitTermination(self, *_a: Any) -> bool:  # noqa: N802 (pyspark name)
        return True


class _WriteStream:
    def foreachBatch(self, _fn: Any) -> _WriteStream:  # noqa: N802
        return self

    def option(self, *_a: Any, **_k: Any) -> _WriteStream:
        return self

    def trigger(self, *_a: Any, **_k: Any) -> _WriteStream:
        return self

    def start(self) -> _Query:
        return _Query()


class _ReadStream:
    def format(self, *_a: Any) -> _ReadStream:
        return self

    def option(self, *_a: Any, **_k: Any) -> _ReadStream:
        return self

    def _frame(self) -> types.SimpleNamespace:
        return types.SimpleNamespace(writeStream=_WriteStream())

    def load(self, *_a: Any, **_k: Any) -> types.SimpleNamespace:
        return self._frame()

    def table(self, *_a: Any, **_k: Any) -> types.SimpleNamespace:
        return self._frame()


class _Conf:
    def set(self, *_a: Any, **_k: Any) -> None:
        pass

    def get(self, *_a: Any, **_k: Any) -> str:
        return "UTC"


class StubSession:
    """The stub ``SparkSession``. ``executed_sql`` lists every statement;
    ``skipped_calls`` names each replaced step main() called, in order, so a
    test can still check that main() reaches it."""

    def __init__(self) -> None:
        self.conf = _Conf()
        self.readStream = _ReadStream()
        self.executed_sql: list[str] = []
        self.skipped_calls: list[str] = []

    def sql(self, statement: str, *_a: Any, **_k: Any) -> None:
        self.executed_sql.append(statement)

    def table(self, *_a: Any, **_k: Any) -> None:
        return None

    def stop(self) -> None:
        pass


class _Builder:
    def __init__(self, spark: StubSession) -> None:
        self._spark = spark

    def appName(self, *_a: Any) -> _Builder:  # noqa: N802
        return self

    def config(self, *_a: Any, **_k: Any) -> _Builder:
        return self

    def getOrCreate(self) -> StubSession:  # noqa: N802
        return self._spark


def install(monkeypatch: Any, script: Any) -> StubSession:
    """Make ``script.main()`` build a ``StubSession`` and skip the JVM-only
    startup steps and the zero-row progress gate. Returns the session."""
    spark = StubSession()

    def _skipped(step: str) -> Any:
        def call(*_a: Any, **_k: Any) -> None:
            spark.skipped_calls.append(step)

        return call

    for step in JVM_STEPS:
        # raising=True: a renamed or removed step fails here, not silently.
        monkeypatch.setattr(script, step, _skipped(step))
    # The stub stream writes no rows, which the zero-row gate would refuse.
    monkeypatch.setattr(script, "assert_progress", _skipped("assert_progress"))
    monkeypatch.setattr(script.SparkSession, "builder", _Builder(spark))
    return spark
