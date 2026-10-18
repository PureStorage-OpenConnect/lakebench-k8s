"""Maintenance holds bronze's expire_snapshots back from the snapshot a live
silver stream still references, so a maintenance round cannot kill the stream
by expiring the offset it resumes from.

Covers the pure-Python pieces (the Spark tier covers the end-to-end expiry):
the ``protect_from`` floor in ``build_maintenance_sql`` for both engines, the
checkpoint-offset snapshot-id read, and that the continuous loop protects only
the bronze streaming source.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from lakebench.modules.table_formats.iceberg.maintenance import (
    _parse_threshold_seconds,
    _spark_timestamp,
    build_maintenance_sql,
)


def _expire_stmt(sqls):
    return next(s for s in sqls if "expire_snapshots" in s)


def test_protect_from_floors_spark_cutoff_before_referenced_snapshot():
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    # The stream's referenced snapshot committed 45 minutes ago; a 30m
    # retention would expire everything older than 30m ago (11:30), which
    # includes the referenced snapshot: the crash. Protection moves the cutoff
    # to one second before the referenced commit so it and newer survive.
    protect = now - timedelta(minutes=45)
    unprotected_cutoff = _spark_timestamp(_parse_threshold_seconds("30m"), now)
    unprotected = build_maintenance_sql(
        "spark-thrift", "lh", "db.bronze", "30m", orphan_retention="48h", now=now
    )
    assert unprotected_cutoff in _expire_stmt(unprotected)

    protected = build_maintenance_sql(
        "spark-thrift",
        "lh",
        "db.bronze",
        "30m",
        orphan_retention="48h",
        now=now,
        protect_from=protect,
    )
    expected = _spark_timestamp(int((now - (protect - timedelta(seconds=1))).total_seconds()), now)
    assert expected in _expire_stmt(protected)
    # The protected cutoff (45m+1s ago) is strictly older than the unprotected
    # 30m cutoff, so expiry reaches less, never more.
    assert unprotected_cutoff not in _expire_stmt(protected)


def test_protect_from_is_a_noop_when_retention_already_keeps_the_snapshot():
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    # The referenced snapshot is only 5 minutes old; a 30m retention already
    # keeps everything newer than 30m, so it is safe without protection and
    # the cutoff stays at the 30m mark (protection never expires less than the
    # configured retention asks).
    protect = now - timedelta(minutes=5)
    protected = build_maintenance_sql(
        "spark-thrift", "lh", "db.bronze", "30m", now=now, protect_from=protect
    )
    assert _spark_timestamp(_parse_threshold_seconds("30m"), now) in _expire_stmt(protected)


def test_protect_from_floors_trino_duration():
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    protect = now - timedelta(minutes=5)
    protected = build_maintenance_sql(
        "trino", "lh", "db.bronze", "30m", orphan_retention="48h", now=now, protect_from=protect
    )
    # A snapshot referenced 5m ago is already kept by the 30m retention
    # (max(1800s, 301s) = 1800s), so the duration stays 30m.
    assert "expire_snapshots(retention_threshold => '30m')" in _expire_stmt(protected)
    # A referenced snapshot OLDER than retention widens the window.
    protect_old = now - timedelta(hours=2)
    widened = build_maintenance_sql(
        "trino", "lh", "db.bronze", "30m", now=now, protect_from=protect_old
    )
    # 2h + 1s -> 7201s; _format_duration renders seconds when not a whole hour.
    assert "7201s" in _expire_stmt(widened)


def test_offset_snapshot_id_reads_newest_committed_offset():
    from lakebench.cli._sustained import _silver_stream_offset_snapshot_id

    cfg = MagicMock()
    cfg.architecture.pipeline.sustained.checkpoint_base = "/_checkpoints"
    cfg.platform.storage.s3.buckets.silver = "silver-bkt"

    raw = MagicMock()
    raw.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "_checkpoints/silver-stream/offsets/0"},
            {"Key": "_checkpoints/silver-stream/offsets/1"},
            {"Key": "_checkpoints/silver-stream/offsets/2"},
        ]
    }
    # A Spark structured-streaming offset file: a metadata line, a blank line,
    # then the Iceberg source offset JSON.
    body = b'{"batchWatermarkMs":0,"batchTimestampMs":1}\n\n{"version":1,"snapshot_id":777,"position":3,"scan_all_files":false}\n'
    raw.get_object.return_value = {"Body": MagicMock(read=lambda: body)}
    client = MagicMock()
    client.raw_client = raw

    with patch("lakebench.deploy.datagen._s3_client_for", return_value=client):
        sid = _silver_stream_offset_snapshot_id(cfg)
    assert sid == 777
    # The newest batch (2) was read.
    _, kwargs = raw.get_object.call_args
    assert kwargs["Key"].endswith("offsets/2")


def test_offset_snapshot_id_none_when_no_offsets():
    from lakebench.cli._sustained import _silver_stream_offset_snapshot_id

    cfg = MagicMock()
    cfg.architecture.pipeline.sustained.checkpoint_base = "/_checkpoints"
    cfg.platform.storage.s3.buckets.silver = "silver-bkt"
    raw = MagicMock()
    raw.list_objects_v2.return_value = {"Contents": []}
    client = MagicMock()
    client.raw_client = raw
    with patch("lakebench.deploy.datagen._s3_client_for", return_value=client):
        assert _silver_stream_offset_snapshot_id(cfg) is None
