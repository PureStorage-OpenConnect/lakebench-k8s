"""H2: `silver_stream_financial.main()` calls `ensure_partition_transform`
for `silver.transactions` (days -> months) and `silver.account_statements`
(days -> months) at startup, matching `silver_build_financial`. A
continuous-only deployment on a reused catalog that still holds the daily
spec would otherwise never migrate to months, leaving the stream running
against a partition layout the batch code long since evolved away from.

This is an import- and call-shape test rather than an end-to-end Iceberg
run: full Iceberg is out of scope for a unit lane, and the partition-
transform helper itself is exercised by
`tests/spark/test_partition_evolution_spark.py`.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "src/lakebench/spark/scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))


def test_stream_imports_ensure_partition_transform():
    """Regression guard on the import block: the H2 fix wired the helper into
    the stream's `from common import ...` line."""
    src = (
        Path(__file__).resolve().parent.parent
        / "src/lakebench/spark/scripts/silver_stream_financial.py"
    ).read_text()
    # The name must appear inside the `from common import (` block, not merely
    # somewhere in the file (a comment or unused symbol would not count).
    assert "ensure_partition_transform," in src, (
        "silver_stream_financial.py does not import ensure_partition_transform "
        "in its `from common import (...)` block"
    )


def test_stream_main_invokes_ensure_partition_transform(monkeypatch):
    """Drive `silver_stream_financial.main()` far enough to run the DDL
    bootstrap and the partition-transform migration, capturing which
    (table, old, new) triples are requested. `.start()` is stubbed to a
    non-active query so the wait loop exits and `assert_progress` does not
    swallow SilverAbort. All Spark interactions are stubs -- this test does
    not launch a real SparkSession.
    """
    pytest.importorskip("pyspark")
    import common  # noqa: F401
    import silver_stream_financial as ss

    # D-safe residual (entity_profiles not maintained in continuous mode).
    monkeypatch.setenv("LB_ALLOW_PARTIAL_SILVER", "1")

    # Capture every call to ensure_partition_transform (imported into
    # silver_stream_financial via `from common import ensure_partition_transform`,
    # so patch the local binding).
    calls: list[tuple[str, str, str]] = []

    def _capture(spark, table, old, new):
        calls.append((table, old, new))
        return True

    monkeypatch.setattr(ss, "ensure_partition_transform", _capture)
    # ensure_column and ensure_namespaces_for_ddl should be no-ops.
    monkeypatch.setattr(ss, "ensure_column", lambda *a, **k: False)
    monkeypatch.setattr(ss, "ensure_namespaces_for_ddl", lambda *a, **k: None)
    # assert_progress would raise on our stubbed 0-row stream; bypass it.
    monkeypatch.setattr(ss, "assert_progress", lambda *a, **k: None)

    class _Query:
        isActive = False
        lastProgress = None

        def exception(self):
            return None

        def stop(self):
            pass

    class _WriteStream:
        def foreachBatch(self, fn):
            return self

        def option(self, *a, **k):
            return self

        def trigger(self, *a, **k):
            return self

        def start(self):
            return _Query()

    class _ReadStream:
        def format(self, *_):
            return self

        def option(self, *_a, **_k):
            return self

        def load(self, *_a, **_k):
            df = types.SimpleNamespace()
            df.writeStream = _WriteStream()
            return df

    class _Conf:
        def set(self, *a, **k):
            pass

        def get(self, *a, **k):
            return "UTC"

    class _SparkStub:
        conf = _Conf()
        readStream = _ReadStream()

        def sql(self, *_a, **_k):
            return None

        def table(self, *_a, **_k):
            return None

        def stop(self):
            pass

    class _Builder:
        def appName(self, *_):
            return self

        def getOrCreate(self):
            return _SparkStub()

    # Patch the whole SparkSession.builder chain.
    monkeypatch.setattr(ss.SparkSession, "builder", _Builder())

    ss.main()

    assert (f"lakehouse.{ss.SILVER_TXNS}", "days(txn_timestamp)", "months(txn_timestamp)") in calls
    assert (
        f"lakehouse.{ss.SILVER_STATEMENTS}",
        "days(book_ts)",
        "months(book_ts)",
    ) in calls
