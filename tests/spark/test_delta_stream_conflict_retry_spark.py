"""Delta silver stream: an append that loses a real optimistic-concurrency
race is retried with the same transaction id and counted once; other errors
are not retried; the builder gets a name Delta parses (LB-223).

The conflict is made deterministic: the first append runs through
``mapInArrow`` whose first batch writes a marker and sleeps, and a driver
thread that sees the marker commits ``ALTER TABLE ... SET TBLPROPERTIES`` to
the same table while that append's tasks run. The append's transaction read
the table before the ALTER and commits after it, so Delta refuses it with
MetadataChangedException.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.usefixtures("load_script"), pytest.mark.requires_jars("delta")]

TBL = "spark_catalog.silver.conflict_retry"


@pytest.fixture
def ss(spark_session, load_script, monkeypatch, tmp_path):
    monkeypatch.setenv("LB_CATALOG_TYPE", "hive")
    spark_session.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")
    spark_session.sql(f"DROP TABLE IF EXISTS {TBL}")
    mod = load_script("silver_stream_delta")
    yield mod
    spark_session.sql(f"DROP TABLE IF EXISTS {TBL}")


def _write(ss, spark, bid, start, qid="qretry"):
    from _foreach_batch import inside_foreach_batch
    from c360_stream_scenarios import bronze_df

    df = bronze_df(spark, 5, start=start)
    with inside_foreach_batch(spark, bid, qid):
        return ss.write_silver_batch(df, bid, TBL, "file:///unused/")


def _own_rows(spark, qid="qretry"):
    return spark.table(TBL).where(f"_stream_id = '{qid}'").count()


def test_builder_name_drops_only_the_session_catalog(ss):
    assert ss._builder_name("spark_catalog.silver.t") == "silver.t"
    assert ss._builder_name("SPARK_CATALOG.silver.t") == "silver.t"
    assert ss._builder_name("unity.silver.t") == "unity.silver.t"
    assert ss._builder_name("silver.t") == "silver.t"


def test_three_part_session_catalog_name_creates_the_table(spark_session, ss, capsys):
    """LB-223: the first micro-batch creates the table under the
    three-part name the product passes."""
    assert _write(ss, spark_session, 0, 0) > 0
    assert spark_session.table(TBL).count() > 0


def test_append_retries_a_real_metadata_conflict_exactly_once(
    spark_session, ss, monkeypatch, tmp_path, capsys
):
    first = _write(ss, spark_session, 0, 0)
    assert first > 0
    marker = tmp_path / "write-running"
    real = ss.write_delta_table
    calls = {"n": 0}

    def _stall(batches, marker=str(marker)):
        import os
        import time as _t

        for i, b in enumerate(batches):
            if i == 0 and not os.path.exists(marker):
                Path(marker).write_text("1")
                _t.sleep(6)
            yield b

    altered = threading.Event()
    alter_error: list[str] = []

    def _alter_when_running():
        try:
            for _ in range(400):
                if marker.exists():
                    spark_session.sql(
                        f"ALTER TABLE {TBL} SET TBLPROPERTIES ('lb.test.conflict' = '1')"
                    )
                    altered.set()
                    return
                time.sleep(0.05)
            alter_error.append("no marker in 20 s")
        except Exception as e:  # noqa: BLE001 -- reported by the assertion below
            alter_error.append(f"{type(e).__name__}: {str(e)[:500]}")

    def wrapper(spark, df, *a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            threading.Thread(target=_alter_when_running, daemon=True).start()
            # One task, so the other local core is free for the ALTER.
            df = df.coalesce(1).mapInArrow(_stall, df.schema)
        return real(spark, df, *a, **k)

    monkeypatch.setattr(ss, "write_delta_table", wrapper)
    capsys.readouterr()
    second = _write(ss, spark_session, 1, 100)
    out = capsys.readouterr().out
    assert altered.is_set(), alter_error
    assert calls["n"] == 2, out
    assert "append lost a concurrent commit (MetadataChangedException), attempt 1 of 5" in out
    # Counted once: the return equals this stream's growth, and one commit
    # carries batch 1's transaction.
    assert second > 0 and _own_rows(spark_session) == first + second
    hist = spark_session.sql(f"DESCRIBE HISTORY {TBL}").collect()
    appends = [h for h in hist if h["operation"] == "WRITE"]
    assert len(appends) == 2, [(h["version"], h["operation"]) for h in hist]
    assert spark_session.table(TBL).count() == first + second


def test_a_non_conflict_error_is_not_retried(spark_session, ss, monkeypatch):
    calls = {"n": 0}

    def wrapper(*a, **k):
        calls["n"] += 1
        raise ValueError("disk full")

    _write(ss, spark_session, 0, 0)
    monkeypatch.setattr(ss, "write_delta_table", wrapper)
    with pytest.raises(ValueError, match="disk full"):
        _write(ss, spark_session, 1, 100)
    assert calls["n"] == 1


def test_a_conflict_on_every_attempt_gives_up(spark_session, ss, monkeypatch):
    class MetadataChangedException(Exception):
        pass

    calls = {"n": 0}

    def wrapper(*a, **k):
        calls["n"] += 1
        raise MetadataChangedException("concurrent update")

    _write(ss, spark_session, 0, 0)
    monkeypatch.setattr(ss, "write_delta_table", wrapper)
    # The script's own time module only: the retry waits nothing here.
    monkeypatch.setattr(ss, "time", SimpleNamespace(sleep=lambda s: None, time=time.time))
    with pytest.raises(MetadataChangedException):
        _write(ss, spark_session, 1, 100)
    assert calls["n"] == ss._APPEND_ATTEMPTS


def test_shared_checkpoint_conflict_is_not_retried(ss):
    """ConcurrentTransactionException means two writers share one txnAppId
    (one checkpoint): never retried."""

    class ConcurrentTransactionException(Exception):
        pass

    assert ss._delta_conflict(ConcurrentTransactionException("x")) is None
    assert ss._delta_conflict(ValueError("MetadataChangedException in a cause text")) is None
