"""H3: `silver_stream_financial.main()` bootstraps `silver.entity_profiles`
at startup. The previous 5-table bootstrap loop skipped it, so a
continuous-only deployment ended up with a missing table that gold-side
consumers hit as "table not found". This test asserts the DDL constant is
imported and its SQL is executed at startup.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "src/lakebench/spark/scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))


def test_stream_imports_ddl_profiles():
    src = (
        Path(__file__).resolve().parent.parent
        / "src/lakebench/spark/scripts/silver_stream_financial.py"
    ).read_text()
    assert "DDL_PROFILES," in src, (
        "silver_stream_financial.py does not import DDL_PROFILES from silver_build_financial"
    )


def test_stream_main_executes_ddl_profiles(monkeypatch):
    """Capture every `spark.sql(...)` call during startup and assert
    DDL_PROFILES appears in the sequence.
    """
    pytest.importorskip("pyspark")
    import silver_stream_financial as ss

    # D-safe residual: allow the partial-silver path (entity_profiles is
    # not maintained by the continuous stream).

    executed_sql: list[str] = []

    monkeypatch.setattr(ss, "ensure_partition_transform", lambda *a, **k: True)
    monkeypatch.setattr(ss, "ensure_column", lambda *a, **k: False)
    monkeypatch.setattr(ss, "ensure_namespaces_for_ddl", lambda *a, **k: None)
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

        def sql(self, s):
            executed_sql.append(s)
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

    monkeypatch.setattr(ss.SparkSession, "builder", _Builder())

    ss.main()

    assert ss.DDL_PROFILES in executed_sql, (
        "silver_stream_financial.main() did not execute DDL_PROFILES; "
        "silver.entity_profiles will be missing on continuous-only deployments"
    )


def test_stream_ensure_namespaces_includes_profiles_ddl(monkeypatch):
    """`ensure_namespaces_for_ddl` receives the tuple of DDLs it must scan
    for namespaces to create. DDL_PROFILES must be in it, otherwise a
    fresh catalog would fail on the `silver` namespace CREATE for that
    table.
    """
    pytest.importorskip("pyspark")
    import silver_stream_financial as ss

    seen_ddls: list[tuple] = []

    def _ensure_ns(spark, catalog, ddls):
        seen_ddls.append(tuple(ddls))

    monkeypatch.setattr(ss, "ensure_partition_transform", lambda *a, **k: True)
    monkeypatch.setattr(ss, "ensure_column", lambda *a, **k: False)
    monkeypatch.setattr(ss, "ensure_namespaces_for_ddl", _ensure_ns)
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

        def sql(self, s):
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

    monkeypatch.setattr(ss.SparkSession, "builder", _Builder())

    ss.main()

    assert seen_ddls, "ensure_namespaces_for_ddl was never called"
    ddls = seen_ddls[0]
    assert ss.DDL_PROFILES in ddls, (
        "DDL_PROFILES is missing from ensure_namespaces_for_ddl's DDL tuple"
    )
