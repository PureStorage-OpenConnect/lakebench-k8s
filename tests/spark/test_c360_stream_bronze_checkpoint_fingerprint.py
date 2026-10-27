"""A bronze reset under an old silver checkpoint aborts the stream; a
resume on the same lineage proceeds; every case the guard cannot judge
(missing sidecar, unavailable snapshot, expired history) fails open.
"""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.usefixtures("load_script")

_TBL = "cat.default.bronze_raw"
_CKPT = "s3a://silver/ckpt/stream"
_SIDECAR = f"{_CKPT}/bronze_fingerprint.json"
_FORMATS = ["iceberg", "delta"]


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


def _seed_sidecar(fs, source_format, fingerprint, table_id=None):
    payload = {
        "bronze_table": _TBL,
        "source_format": source_format,
        "bronze_snapshot_fingerprint": fingerprint,
    }
    if table_id is not None:
        payload["bronze_table_id"] = table_id
    fs.write(_SIDECAR, json.dumps(payload).encode("utf-8"))


def _saved(fs):
    return json.loads(fs.files[_SIDECAR].decode("utf-8"))


def _iceberg(current, ancestors, truncated=False):
    return {"fingerprint": current, "ancestors": set(ancestors), "truncated": truncated}


def _delta(version, table_id):
    return {"fingerprint": version, "table_id": table_id}


def _guard(monkeypatch, fs, source_format, lineage, spark=None):
    """Run the guard with bronze's lineage fixed to *lineage* (None =
    unavailable)."""
    import common

    if spark is None:
        monkeypatch.setattr(common, "_read_bronze_lineage", lambda *a, **k: lineage)
    common.check_bronze_fingerprint(spark, _TBL, _CKPT, source_format=source_format, fs=fs)


def test_sidecar_missing_fails_open_and_writes_it(monkeypatch):
    fs = _FakeFS()

    _guard(monkeypatch, fs, "delta", _delta("111111", "id-a"))

    written = _saved(fs)
    assert str(written["bronze_snapshot_fingerprint"]) == "111111"
    assert written["bronze_table_id"] == "id-a"
    assert written["source_format"] == "delta"
    assert written["bronze_table"] == _TBL


def test_sidecar_matches_current_snapshot_returns_cleanly(monkeypatch):
    fs = _FakeFS()
    _seed_sidecar(fs, "iceberg", "222222")

    _guard(monkeypatch, fs, "iceberg", _iceberg("222222", ["222222"]))

    assert fs.writes == 1  # only the seeded write


def test_iceberg_resume_after_bronze_moved_on_proceeds_and_saves_current(monkeypatch):
    """Bronze commits on every ingest batch: a saved snapshot that is an
    ancestor of the current one is the normal restart, not a reset."""
    fs = _FakeFS()
    _seed_sidecar(fs, "iceberg", "222222")

    _guard(monkeypatch, fs, "iceberg", _iceberg("333333", ["222222", "333333"]))

    assert _saved(fs)["bronze_snapshot_fingerprint"] == "333333"


def test_iceberg_saved_snapshot_off_the_lineage_raises_silver_abort(monkeypatch):
    from common import SilverAbort

    fs = _FakeFS()
    _seed_sidecar(fs, "iceberg", "222222")

    with pytest.raises(SilverAbort) as excinfo:
        _guard(monkeypatch, fs, "iceberg", _iceberg("999999", ["999999"]))

    assert "222222" in str(excinfo.value)
    assert "999999" in str(excinfo.value)


def test_iceberg_saved_snapshot_expired_fails_open(monkeypatch):
    """Snapshot expiry removed the start of the chain: the saved snapshot's
    absence proves nothing."""
    fs = _FakeFS()
    _seed_sidecar(fs, "iceberg", "222222")

    _guard(monkeypatch, fs, "iceberg", _iceberg("999999", ["999999"], truncated=True))

    assert _saved(fs)["bronze_snapshot_fingerprint"] == "999999"


def test_delta_resume_after_bronze_moved_on_proceeds_and_saves_current(monkeypatch):
    fs = _FakeFS()
    _seed_sidecar(fs, "delta", "5", table_id="id-a")

    _guard(monkeypatch, fs, "delta", _delta("9", "id-a"))

    assert _saved(fs)["bronze_snapshot_fingerprint"] == "9"


def test_delta_sidecar_without_table_id_resumes_and_records_it(monkeypatch):
    fs = _FakeFS()
    _seed_sidecar(fs, "delta", "5")

    _guard(monkeypatch, fs, "delta", _delta("9", "id-a"))

    assert _saved(fs)["bronze_table_id"] == "id-a"


@pytest.mark.parametrize(
    ("saved_version", "lineage"),
    [("5", _delta("9", "id-b")), ("9", _delta("5", "id-a"))],
    ids=["table-id-changed", "saved-version-ahead"],
)
def test_delta_reset_raises_silver_abort(monkeypatch, saved_version, lineage):
    from common import SilverAbort

    fs = _FakeFS()
    _seed_sidecar(fs, "delta", saved_version, table_id="id-a")

    with pytest.raises(SilverAbort, match="bronze snapshot fingerprint mismatch"):
        _guard(monkeypatch, fs, "delta", lineage)


def test_no_snapshot_available_fails_open_no_sidecar_written(monkeypatch):
    """Bronze not created yet: a sidecar with an unknown value would poison
    the next start."""
    fs = _FakeFS()

    _guard(monkeypatch, fs, "iceberg", None)

    assert fs.writes == 0


def test_snapshot_lookup_error_does_not_kill_the_stream(monkeypatch):
    """A transient catalog failure must not raise; only a reset does."""
    fs = _FakeFS()

    class _Boom:
        def sql(self, stmt):
            raise RuntimeError("catalog unreachable: connection reset")

    _guard(monkeypatch, fs, "iceberg", None, spark=_Boom())

    assert fs.writes == 0


@pytest.mark.requires_jars("iceberg", "delta")
@pytest.mark.parametrize("source_format", _FORMATS)
def test_real_bronze_resume_passes_and_recreate_aborts(
    spark_session, iceberg_catalog, tmp_path, source_format
):
    """The lookup SQL is real: a restart after bronze committed more batches
    resumes, and a dropped and re-created bronze aborts."""
    import common

    spark = spark_session
    fs = _FakeFS()
    if source_format == "iceberg":
        cat = iceberg_catalog(spark, "fp", tmp_path / "wh")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.default")
        tbl = f"{cat}.default.bronze_fp"
    else:
        tbl = "default.bronze_fp_delta"

    def create(generation):
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
        if source_format == "iceberg":
            spark.sql(f"CREATE TABLE {tbl} (id BIGINT) USING iceberg")
        else:
            location = tmp_path / f"delta{generation}"
            spark.sql(f"CREATE TABLE {tbl} (id BIGINT) USING delta LOCATION '{location}'")
        spark.sql(f"INSERT INTO {tbl} VALUES (1)")

    def guard():
        common.check_bronze_fingerprint(spark, tbl, _CKPT, source_format=source_format, fs=fs)

    create(0)
    guard()
    first = _saved(fs)["bronze_snapshot_fingerprint"]
    assert isinstance(first, str) and first

    spark.sql(f"INSERT INTO {tbl} VALUES (2)")
    guard()
    assert _saved(fs)["bronze_snapshot_fingerprint"] != first

    create(1)
    with pytest.raises(common.SilverAbort):
        guard()
