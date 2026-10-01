"""H3: `silver_stream_financial.main()` bootstraps `silver.entity_profiles`
at startup. The previous 5-table bootstrap loop skipped it, so a
continuous-only deployment ended up with a missing table that gold-side
consumers hit as "table not found". This test asserts the DDL constant is
imported and its SQL is executed at startup.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"
pytestmark = pytest.mark.usefixtures("load_script")


def test_stream_imports_ddl_profiles():
    src = (_SCRIPTS_DIR / "silver_stream_financial.py").read_text()
    assert "DDL_PROFILES," in src, (
        "silver_stream_financial.py does not import DDL_PROFILES from silver_build_financial"
    )


def test_stream_main_executes_ddl_profiles(monkeypatch):
    """Capture every `spark.sql(...)` call during startup and assert
    DDL_PROFILES appears in the sequence.
    """
    pytest.importorskip("pyspark")
    import _stream_main_stub
    import silver_stream_financial as ss

    monkeypatch.setattr(ss, "ensure_partition_transform", lambda *a, **k: True)
    monkeypatch.setattr(ss, "ensure_column", lambda *a, **k: False)
    monkeypatch.setattr(ss, "ensure_namespaces_for_ddl", lambda *a, **k: None)
    spark = _stream_main_stub.install(monkeypatch, ss)

    ss.main()

    assert ss.DDL_PROFILES in spark.executed_sql, (
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
    import _stream_main_stub
    import silver_stream_financial as ss

    seen_ddls: list[tuple] = []

    def _ensure_ns(spark, catalog, ddls):
        seen_ddls.append(tuple(ddls))

    monkeypatch.setattr(ss, "ensure_partition_transform", lambda *a, **k: True)
    monkeypatch.setattr(ss, "ensure_column", lambda *a, **k: False)
    monkeypatch.setattr(ss, "ensure_namespaces_for_ddl", _ensure_ns)
    _stream_main_stub.install(monkeypatch, ss)

    ss.main()

    assert seen_ddls, "ensure_namespaces_for_ddl was never called"
    ddls = seen_ddls[0]
    assert ss.DDL_PROFILES in ddls, (
        "DDL_PROFILES is missing from ensure_namespaces_for_ddl's DDL tuple"
    )
