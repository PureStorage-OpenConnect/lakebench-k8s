"""Maintenance holds bronze's expire_snapshots back from the snapshot a live
silver stream still references, so a maintenance round cannot kill the stream
by expiring the offset it resumes from.

Covers the pure-Python pieces (the Spark tier covers the end-to-end expiry):
the ``protect_from`` floor in ``build_maintenance_sql`` for both engines, the
checkpoint-offset snapshot-id read, that the floor is passed only for bronze,
and that the continuous loop skips bronze expiry when it cannot establish the
floor and flags a protection miss after the fact.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from lakebench.modules.table_formats.iceberg.maintenance import (
    STREAM_OFFSET_PROTECT_MARGIN_SECONDS,
    _parse_threshold_seconds,
    _spark_timestamp,
    build_maintenance_sql,
)


def _expire_stmt(sqls):
    return next(s for s in sqls if "expire_snapshots" in s)


def test_protect_margin_is_minutes_not_seconds():
    # The margin must be large enough to absorb the host-to-engine-pod clock
    # skew the cutoff is evaluated against; a one-second margin let a pod 8 s
    # ahead expire the protected snapshot. Pin the value so a silent change to
    # a too-small margin fails here.
    assert STREAM_OFFSET_PROTECT_MARGIN_SECONDS == 600


def test_protect_from_floors_spark_cutoff_before_referenced_snapshot():
    # now is chosen so the protected cutoff is a round, literal timestamp: the
    # referenced snapshot committed at 11:15:00 (45m ago), the margin pushes the
    # cutoff to 11:05:00, i.e. 55m before now. Asserting the literal rather than
    # re-deriving the production formula keeps the test from moving with it.
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
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
    # 45m back + 10m margin = 55m back = 11:05:00 UTC.
    assert "TIMESTAMP '2026-01-01 11:05:00+00:00'" in _expire_stmt(protected)
    # Strictly older than the unprotected 30m cutoff: expiry reaches less.
    assert unprotected_cutoff not in _expire_stmt(protected)


def test_protect_from_is_a_noop_when_retention_already_keeps_the_snapshot():
    # A snapshot only 5 minutes old is inside the 30m retention, so protection
    # must not widen (or narrow) the window: the invariant that protection only
    # ever expires less than retention, never more recent than it asks.
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    protect = now - timedelta(minutes=5)
    protected = build_maintenance_sql(
        "spark-thrift", "lh", "db.bronze", "30m", now=now, protect_from=protect
    )
    assert _spark_timestamp(_parse_threshold_seconds("30m"), now) in _expire_stmt(protected)


def test_protect_from_widens_trino_duration_by_the_margin():
    # A referenced snapshot OLDER than retention widens the window to its commit
    # time plus the margin. 2h back + 10m margin -> 130m. On the 1.7.8 code
    # (1 s margin) this was 121m; the literal pins the margin through the Trino
    # duration path, where the clock-skew bug lived.
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    widened = build_maintenance_sql(
        "trino", "lh", "db.bronze", "30m", now=now, protect_from=now - timedelta(hours=2)
    )
    assert "expire_snapshots(retention_threshold => '130m')" in _expire_stmt(widened)


def test_protect_margin_survives_engine_pod_clock_skew_ahead_of_host():
    # The exact 24h failure mechanism, as executable reasoning: the cutoff is a
    # relative duration built on the host clock but evaluated on the engine pod,
    # so a pod running ahead shifts the effective cutoff forward by the skew.
    # Silver lagging past retention (snapshot older than the 1h floor) is the
    # case where only the margin keeps the snapshot. This proves margin > skew
    # for a skew worse than the field's 8 s; it does not pin the constant (that
    # is test_protect_margin_is_minutes_not_seconds) or run against an engine
    # (the Spark tier does that).
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    skew = timedelta(seconds=30)
    protect = now - timedelta(minutes=90)
    stmt = _expire_stmt(
        build_maintenance_sql("trino", "lh", "db.bronze", "1h", now=now, protect_from=protect)
    )
    secs = _parse_threshold_seconds(re.search(r"=> '(\w+)'", stmt).group(1))
    # Effective cutoff the pod computes: (host now + skew) - duration. It must
    # land strictly before the protected commit, or expire removes it.
    effective_cutoff = (now + skew) - timedelta(seconds=secs)
    assert effective_cutoff < protect, (
        f"cutoff {effective_cutoff.isoformat()} did not clear protected commit "
        f"{protect.isoformat()} under {skew} skew"
    )


def _offset_reader_cfg():
    cfg = MagicMock()
    cfg.architecture.pipeline.sustained.checkpoint_base = "/_checkpoints"
    cfg.platform.storage.s3.buckets.silver = "silver-bkt"
    return cfg


def test_offset_snapshot_id_reads_numerically_newest_offset():
    # The fail-closed path depends on reading the stream's NEWEST committed
    # offset. Batch ids are listed out of order and cross the 9->10 boundary
    # where lexical and numeric ordering diverge: a str-max implementation would
    # pick offsets/9 and read the wrong (older) snapshot.
    from lakebench.cli._sustained import _silver_stream_offset_snapshot_id

    cfg = _offset_reader_cfg()
    raw = MagicMock()
    raw.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "_checkpoints/silver-stream/offsets/9"},
            {"Key": "_checkpoints/silver-stream/offsets/10"},
            {"Key": "_checkpoints/silver-stream/offsets/8"},
        ]
    }
    bodies = {
        "_checkpoints/silver-stream/offsets/10": b'{"batchWatermarkMs":0}\n\n{"snapshot_id":1010}\n',
        "_checkpoints/silver-stream/offsets/9": b'{"batchWatermarkMs":0}\n\n{"snapshot_id":909}\n',
    }
    raw.get_object.side_effect = lambda Bucket, Key: {"Body": MagicMock(read=lambda: bodies[Key])}
    client = MagicMock()
    client.raw_client = raw

    with patch("lakebench.deploy.datagen._s3_client_for", return_value=client):
        sid = _silver_stream_offset_snapshot_id(cfg)
    assert sid == 1010
    _, kwargs = raw.get_object.call_args
    assert kwargs["Key"].endswith("offsets/10")


def test_offset_snapshot_id_none_when_no_offsets():
    # The empty-listing -> None contract is what the fail-closed branch keys on.
    from lakebench.cli._sustained import _silver_stream_offset_snapshot_id

    cfg = _offset_reader_cfg()
    raw = MagicMock()
    raw.list_objects_v2.return_value = {"Contents": []}
    client = MagicMock()
    client.raw_client = raw
    with patch("lakebench.deploy.datagen._s3_client_for", return_value=client):
        assert _silver_stream_offset_snapshot_id(cfg) is None


BRONZE = "lakehouse.bronze.bronze_raw"
SILVER = "lakehouse.silver.transactions"


def _run_live_iceberg_maintenance(offset_sid, committed_at_returns, live_streams=True):
    """Drive ``_run_iceberg_maintenance`` on a Trino Iceberg continuous run,
    mocking the engine and S3 reads. ``offset_sid`` is what the checkpoint
    offset read returns; ``committed_at_returns`` is what ``_snapshot_committed_at``
    yields in order (floor read, then post-expire verify). The real
    ``build_maintenance_sql`` is replaced by a fake that RECORDS its
    ``protect_from`` kwarg per table, so the test can check the floor is wired
    to bronze only. Returns ``(plan, outcomes, protect_by_table, verify_calls)``
    where ``verify_calls`` is the ``_snapshot_committed_at`` call count."""
    from lakebench.cli import _sustained

    cfg = MagicMock()
    cfg.architecture.query_engine.type.value = "trino"
    cfg.architecture.table_format.type.value = "iceberg"
    cfg.architecture.pipeline.mode.value = "continuous"
    cfg.architecture.workload.schema_type.value = "financial"
    cfg.architecture.tables.bronze = "bronze.bronze_raw"
    cfg.get_namespace.return_value = "ns"

    outcomes: list = []
    captured: dict = {}
    protect_by_table: dict = {}

    def fake_run_statements(plan, **kwargs):
        captured["plan"] = list(plan)
        n = len(plan)
        return {
            "succeeded": n,
            "failures": [],
            "timed_out": [],
            "not_attempted": [],
            "status": ["ok"] * n,
        }

    def fake_build_sql(engine, catalog, table, retention, orphan_retention=None, protect_from=None):
        protect_by_table[table] = protect_from
        return [
            f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '1h')",
            f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '1450m')",
        ]

    committed = MagicMock(side_effect=committed_at_returns)

    with (
        patch(
            "lakebench.modules.table_formats.iceberg.maintenance.find_maintenance_engine",
            return_value=("trino", "trino-pod", "lakehouse"),
        ),
        patch(
            "lakebench.modules.table_formats.iceberg.maintenance.build_maintenance_sql",
            side_effect=fake_build_sql,
        ),
        patch.object(
            _sustained,
            "maintained_tables",
            return_value=["bronze.bronze_raw", "silver.transactions"],
        ),
        patch.object(_sustained, "_run_statements", side_effect=fake_run_statements),
        patch.object(_sustained, "_silver_stream_offset_snapshot_id", return_value=offset_sid),
        patch.object(_sustained, "_snapshot_committed_at", committed),
    ):
        _sustained._run_iceberg_maintenance(
            cfg,
            MagicMock(),
            MagicMock(),  # console
            MagicMock(),  # journal
            "1h",
            live_streams=live_streams,
            outcomes=outcomes,
        )
    return captured["plan"], outcomes, protect_by_table, committed.call_count


def _expire_tables(plan):
    return {t for t, s in plan if "expire_snapshots" in s}


def _orphan_tables(plan):
    return {t for t, s in plan if "remove_orphan_files" in s}


@pytest.mark.parametrize(
    "offset_sid, committed_at_returns",
    [
        (None, []),  # no readable committed offset
        (4242, [None]),  # offset readable, its snapshot's commit time is not
    ],
)
def test_fail_closed_skips_only_bronze_expire_when_floor_unavailable(
    offset_sid, committed_at_returns
):
    # When the floor can't be established while a stream is live, bronze's
    # expire_snapshots is dropped (running it at the retention floor is what
    # kills a lagging stream), but bronze orphan removal and every other table's
    # expire still run. 1.7.8 fails this: it runs bronze expire regardless.
    plan, outcomes, _, _ = _run_live_iceberg_maintenance(offset_sid, committed_at_returns)

    assert BRONZE not in _expire_tables(plan), "bronze expire should be skipped"
    assert SILVER in _expire_tables(plan), "silver expire must still run"
    assert BRONZE in _orphan_tables(plan), "bronze orphan removal must still run"
    assert any(o.get("bronze_expire_skipped") for o in outcomes)


def test_healthy_round_wires_floor_to_bronze_only_and_verifies_after():
    # Floor established, snapshot still present after expire. Bronze expire runs;
    # the floor is passed to the builder for bronze and NOT for silver (the
    # protection is bronze-only); and the post-expire verify actually ran (two
    # _snapshot_committed_at calls: floor read + verify), so dropping the verify
    # would fail this test.
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    commit = now - timedelta(minutes=30)
    plan, outcomes, protect_by_table, verify_calls = _run_live_iceberg_maintenance(
        offset_sid=4242, committed_at_returns=[commit, commit]
    )
    assert BRONZE in _expire_tables(plan)
    assert protect_by_table[BRONZE] == commit, "bronze must get the floor"
    assert protect_by_table[SILVER] is None, "only bronze is a streaming source"
    assert verify_calls == 2, "floor read + post-expire verify"
    assert not any(o.get("bronze_expire_skipped") for o in outcomes)
    assert not any(o.get("bronze_protection_failed") for o in outcomes)


def test_post_verify_flags_when_protected_snapshot_was_expired():
    # Floor established and bronze expire ran, but the post-expire re-read finds
    # the snapshot gone: the protection miss is recorded in its own field (not
    # the skip field, since the expire did run). 1.7.8 has no post-verify at all.
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    plan, outcomes, _, verify_calls = _run_live_iceberg_maintenance(
        offset_sid=4242, committed_at_returns=[now - timedelta(hours=2), None]
    )
    assert BRONZE in _expire_tables(plan), "bronze expire did run this round"
    assert verify_calls == 2
    assert any(o.get("bronze_protection_failed") for o in outcomes)
    # The expire was not skipped, so the skip field stays clear.
    assert not any(o.get("bronze_expire_skipped") for o in outcomes)


def test_no_protection_or_verify_when_streams_not_live():
    # With no live stream there is nothing to protect: bronze expire runs, the
    # floor is never read, and no verify happens.
    plan, outcomes, protect_by_table, verify_calls = _run_live_iceberg_maintenance(
        offset_sid=4242, committed_at_returns=[], live_streams=False
    )
    assert BRONZE in _expire_tables(plan)
    assert protect_by_table[BRONZE] is None
    assert verify_calls == 0
    assert not any(o.get("bronze_expire_skipped") for o in outcomes)
    assert not any(o.get("bronze_protection_failed") for o in outcomes)
