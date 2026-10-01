"""I6: `silver_stream_delta.main()` adds `_stream_id STRING` and
`_batch_id BIGINT` to the reused silver Delta table at stream startup, via
``ensure_column``. Delta's native ``txnAppId`` / ``txnVersion`` protocol
gives replay-idempotency; these columns give operators the same debugging
signal the Iceberg path (silver_stream.py) already emits, so the two
formats are symmetric under log inspection.

Import- and call-shape test only. Delta jar path is exercised by
``tests/spark/`` scenarios.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"
pytestmark = pytest.mark.usefixtures("load_script")


_SRC = (_SCRIPTS_DIR / "silver_stream_delta.py").read_text()


def test_stream_delta_imports_ensure_column():
    """The I6 fix imports ``ensure_column`` in the ``from common import (`` block.

    Merely referencing the name from a comment or unused symbol does not count.
    """
    assert "ensure_column," in _SRC, (
        "silver_stream_delta.py does not import ensure_column from common"
    )


def test_stream_delta_imports_streaming_query_id():
    """I6 write path stamps ``_stream_id`` from ``streaming_query_id(spark)``,
    matching silver_stream.py -- so the import must appear in the from-block."""
    assert "streaming_query_id," in _SRC, (
        "silver_stream_delta.py does not import streaming_query_id from common"
    )


def test_stream_delta_write_projects_stream_and_batch_id():
    """I6: the enriched DataFrame must carry `_stream_id` and `_batch_id`
    columns before the append, so operators see them in silver rows."""
    assert '"_stream_id"' in _SRC, (
        "silver_stream_delta.py does not project `_stream_id` on the enriched DataFrame"
    )
    assert '"_batch_id"' in _SRC, (
        "silver_stream_delta.py does not project `_batch_id` on the enriched DataFrame"
    )


def test_stream_delta_history_read_replaces_version_bracket():
    """I8: read the write's own commit from ``DeltaTable.forName(...).history``,
    not the before/after ``delta_table_version`` bracket the old code used.
    Delta history is immune to interleaved compaction/vacuum commits."""
    assert "DeltaTable.forName" in _SRC, (
        "silver_stream_delta.py does not use DeltaTable.forName(...).history() to "
        "attribute numOutputRows to this write"
    )
    assert "history(" in _SRC, (
        "silver_stream_delta.py does not call history() to look up this write's commit"
    )
    assert "operationMetrics" in _SRC, (
        "silver_stream_delta.py does not read operationMetrics from Delta history"
    )
    assert "numOutputRows" in _SRC, (
        "silver_stream_delta.py does not read numOutputRows from Delta history"
    )
    # The old `delta_table_version` bracket must be gone: if the code still
    # brackets version numbers to detect skips, this whole fix is a no-op.
    assert "delta_table_version" not in _SRC, (
        "silver_stream_delta.py still uses delta_table_version -- the before/after "
        "bracket must be replaced with a history read (I8)"
    )


def test_stream_delta_create_if_not_exists_replaces_overwrite():
    """B4: the not-exists branch must use ``CREATE TABLE IF NOT EXISTS`` (via
    ``DeltaTable.createIfNotExists``) followed by a plain append -- not the old
    ``mode='overwrite'`` write that would delete a racer's data files."""
    assert "createIfNotExists" in _SRC, (
        "silver_stream_delta.py does not call DeltaTable.createIfNotExists() -- the "
        "not-exists branch is still using an overwrite that races with a second writer"
    )
    # The old overwrite branch had ``mode="overwrite"`` and
    # ``partition_cols=["interaction_date"]`` on the same call. Both must go.
    assert 'mode="overwrite"' not in _SRC, (
        'silver_stream_delta.py still has a mode="overwrite" write -- the B4 split '
        "must replace it with CREATE-IF-NOT-EXISTS + append"
    )


def test_stream_delta_main_calls_ensure_column_for_stream_and_batch_id(monkeypatch, tmp_path):
    """Drive ``silver_stream_delta.main()`` far enough to hit the startup
    ``ensure_column`` calls with a table_exists stub returning True. Capture
    every call and assert both columns land on the silver table.
    """
    pytest.importorskip("pyspark")
    import _stream_main_stub
    import silver_stream_delta as ss

    calls: list[tuple[str, str, str]] = []

    def _capture(spark, tbl, name, sql_type):
        calls.append((tbl, name, sql_type))
        return False

    monkeypatch.setattr(ss, "ensure_column", _capture)
    monkeypatch.setattr(ss, "table_exists", lambda *a, **k: True)
    monkeypatch.setattr(ss, "emit_stream_scale_admission", lambda *a, **k: None)
    monkeypatch.setattr(ss, "set_utc_session", lambda *a, **k: None)
    _stream_main_stub.install(monkeypatch, ss)

    monkeypatch.setenv("CHECKPOINT_LOCATION", str(tmp_path / "ckpt-noop-i6"))
    monkeypatch.setenv("LB_ICEBERG_CATALOG", "ice")
    monkeypatch.setenv("LB_BRONZE_TABLE", "default.bronze_raw")
    monkeypatch.setenv("LB_SILVER_TABLE", "silver.customer_interactions_enriched")
    # job.py always exports the data clock for silver jobs; main() refuses
    # to start without it.
    monkeypatch.setenv("LB_DATA_CLOCK", "2025-06-15")

    ss.main()

    silver_tbl = "ice.silver.customer_interactions_enriched"
    assert (silver_tbl, "_stream_id", "STRING") in calls, (
        f"silver_stream_delta.main() did not call ensure_column for _stream_id STRING; "
        f"observed calls: {calls}"
    )
    assert (silver_tbl, "_batch_id", "BIGINT") in calls, (
        f"silver_stream_delta.main() did not call ensure_column for _batch_id BIGINT; "
        f"observed calls: {calls}"
    )
