"""B5: bronze-checkpoint-reset guard for the C360 silver stream.

Scenario the guard defends: an operator wipes bronze (DROP TABLE or
truncate-and-rebuild), bronze-ingest re-populates it from scratch under a
brand-new snapshot lineage, but the silver stream restarts against the OLD
checkpoint which still references the wiped table's snapshot ids. Structured
Streaming then either fails silently on the missing snapshot or -- worse in
the Delta case -- silently starts from ``version 0`` and re-writes every row
it already wrote, so silver ends up with two copies of the data.

The guard writes the bronze snapshot fingerprint (Iceberg snapshot_id or
Delta commit version) to a JSON sidecar next to the streaming checkpoint on
first start, then reads and compares on every subsequent start:

* sidecar present and fingerprint MATCHES -> resume the stream unchanged.
* sidecar present and fingerprint DIFFERS -> raise ``SilverAbort`` telling
  the operator to also reset silver (the stream cannot resume safely from a
  checkpoint whose bronze source no longer exists).
* sidecar MISSING -> fail-open: write it, log a warning, continue. This is
  the ``upgrade path from an older release`` case; refusing here would break
  every running deployment on first restart.

The helper lives in ``common.py`` and both C360 silver streams
(``silver_stream.py`` for Iceberg, ``silver_stream_delta.py`` for Delta) call
it before the stream starts.

The tests use a small ``_FakeSpark`` that stands in for the catalog reads and
Hadoop-FS sidecar writes -- the guard is pure control flow around IO, so the
tests exercise the branches without a real Spark session.
"""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.usefixtures("load_script")


class _FakeFS:
    """In-memory Hadoop FileSystem for the sidecar path."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.writes = 0

    def exists(self, path):
        return str(path) in self.files

    def read(self, path):
        return self.files[str(path)]

    def write(self, path, data):
        self.files[str(path)] = data
        self.writes += 1


class _FakeSpark:
    """Minimal Spark stand-in for the fingerprint guard.

    ``snapshot_by_table`` maps a bronze table name to its current snapshot id
    string (Iceberg snapshot id or Delta version). Absent -> "no snapshot yet".
    ``fs`` is the sidecar filesystem the helper reads and writes through.
    """

    def __init__(self, snapshot_by_table, fs=None):
        self._snapshots = dict(snapshot_by_table)
        self.fs = fs or _FakeFS()
        self.sql_calls: list[str] = []

    def sql(self, stmt):
        self.sql_calls.append(stmt)

        class _R:
            def __init__(self, rows):
                self._rows = rows

            def collect(self_inner):
                return self_inner._rows

        # Iceberg: SELECT snapshot_id FROM <t>.history WHERE is_current_ancestor ...
        # Delta:   DESCRIBE HISTORY <t> LIMIT 1
        import re

        m = re.search(
            r"FROM\s+([^\s]+)\.history\s+WHERE\s+is_current_ancestor",
            stmt,
            re.IGNORECASE,
        )
        if m:
            tbl = m.group(1)
            sid = self._snapshots.get(tbl)
            if sid is None:
                return _R([])

            class _Row:
                def __init__(self, v):
                    self[0] = v  # noqa

                def __getitem__(self, i):
                    return self._v if i == 0 else None

            row = type("R", (), {"__getitem__": lambda s, i: sid if i == 0 else None})()
            return _R([row])
        m = re.search(r"DESCRIBE\s+HISTORY\s+(\S+)", stmt, re.IGNORECASE)
        if m:
            tbl = m.group(1)
            sid = self._snapshots.get(tbl)
            if sid is None:
                return _R([])
            row = {"version": sid}
            return _R([row])
        return _R([])


def _guard(spark, source_format, bronze_tbl, checkpoint_location):
    from common import check_bronze_fingerprint

    return check_bronze_fingerprint(
        spark,
        bronze_tbl,
        checkpoint_location,
        source_format=source_format,
        fs=spark.fs,
    )


# ---------------------------------------------------------------------------
# Iceberg source
# ---------------------------------------------------------------------------


def test_iceberg_sidecar_missing_fails_open_and_writes_it(caplog):
    fs = _FakeFS()
    spark = _FakeSpark({"cat.default.bronze_raw": 111111}, fs=fs)

    with caplog.at_level("WARNING"):
        _guard(spark, "iceberg", "cat.default.bronze_raw", "s3a://silver/ckpt/stream")

    # Fail-open: sidecar was written on the fly with the current snapshot id.
    sidecar_path = "s3a://silver/ckpt/stream/bronze_fingerprint.json"
    assert fs.files, "guard did not write the sidecar on fail-open"
    written = json.loads(next(iter(fs.files.values())).decode("utf-8"))
    assert str(written.get("bronze_snapshot_fingerprint")) == "111111"
    assert written.get("source_format") == "iceberg"
    assert written.get("bronze_table") == "cat.default.bronze_raw"
    # Path check.
    assert sidecar_path in fs.files


def test_iceberg_sidecar_matches_current_snapshot_returns_cleanly():
    fs = _FakeFS()
    payload = {
        "bronze_table": "cat.default.bronze_raw",
        "source_format": "iceberg",
        "bronze_snapshot_fingerprint": "222222",
    }
    fs.write(
        "s3a://silver/ckpt/stream/bronze_fingerprint.json",
        json.dumps(payload).encode("utf-8"),
    )
    spark = _FakeSpark({"cat.default.bronze_raw": 222222}, fs=fs)

    # No raise; sidecar not overwritten.
    _guard(spark, "iceberg", "cat.default.bronze_raw", "s3a://silver/ckpt/stream")
    assert fs.writes == 1  # only the seeded write


def test_iceberg_sidecar_mismatch_raises_silver_abort_with_reset_hint():
    from common import SilverAbort

    fs = _FakeFS()
    fs.write(
        "s3a://silver/ckpt/stream/bronze_fingerprint.json",
        json.dumps(
            {
                "bronze_table": "cat.default.bronze_raw",
                "source_format": "iceberg",
                "bronze_snapshot_fingerprint": "222222",
            }
        ).encode("utf-8"),
    )
    # Bronze snapshot id changed -- a fresh table lineage under the old
    # checkpoint. This is the corruption case.
    spark = _FakeSpark({"cat.default.bronze_raw": 999999}, fs=fs)

    with pytest.raises(SilverAbort) as excinfo:
        _guard(spark, "iceberg", "cat.default.bronze_raw", "s3a://silver/ckpt/stream")
    msg = str(excinfo.value).lower()
    assert "fingerprint" in msg
    assert "222222" in str(excinfo.value)
    assert "999999" in str(excinfo.value)
    # Operator hint present.
    assert "reset silver" in msg or "reset the silver" in msg


# ---------------------------------------------------------------------------
# Delta source
# ---------------------------------------------------------------------------


def test_delta_sidecar_missing_fails_open_and_writes_it():
    fs = _FakeFS()
    spark = _FakeSpark({"cat.default.bronze_raw": 7}, fs=fs)

    _guard(spark, "delta", "cat.default.bronze_raw", "s3a://silver/ckpt/stream-delta")

    written = json.loads(next(iter(fs.files.values())).decode("utf-8"))
    assert str(written["bronze_snapshot_fingerprint"]) == "7"
    assert written["source_format"] == "delta"


def test_delta_sidecar_mismatch_raises_silver_abort():
    from common import SilverAbort

    fs = _FakeFS()
    fs.write(
        "s3a://silver/ckpt/stream-delta/bronze_fingerprint.json",
        json.dumps(
            {
                "bronze_table": "cat.default.bronze_raw",
                "source_format": "delta",
                "bronze_snapshot_fingerprint": "42",
            }
        ).encode("utf-8"),
    )
    spark = _FakeSpark({"cat.default.bronze_raw": 3}, fs=fs)

    with pytest.raises(SilverAbort):
        _guard(spark, "delta", "cat.default.bronze_raw", "s3a://silver/ckpt/stream-delta")


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_no_snapshot_available_fails_open_no_sidecar_written():
    """Greenfield / bronze-not-created-yet: no snapshot id to fingerprint yet.
    The guard must not raise and must not write a sidecar with an unknown
    value (that would poison the next start)."""
    fs = _FakeFS()
    spark = _FakeSpark({}, fs=fs)  # no snapshot known

    _guard(spark, "iceberg", "cat.default.bronze_raw", "s3a://silver/ckpt/stream")
    assert fs.writes == 0, "sidecar written with no fingerprint would poison the next run"


def test_snapshot_lookup_error_does_not_kill_the_stream():
    """The catalog probe fails (transient network). This must not raise --
    only a real fingerprint mismatch should. See fail-open policy."""
    fs = _FakeFS()

    class _Boom(_FakeSpark):
        def sql(self, stmt):
            raise RuntimeError("catalog unreachable: connection reset")

    spark = _Boom({}, fs=fs)
    _guard(spark, "iceberg", "cat.default.bronze_raw", "s3a://silver/ckpt/stream")
    assert fs.writes == 0
