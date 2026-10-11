"""A bronze reset under an old silver checkpoint aborts the stream; every
other case (match, missing sidecar, unavailable snapshot) fails open.
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


def _seed_sidecar(fs, source_format, fingerprint):
    fs.write(
        _SIDECAR,
        json.dumps(
            {
                "bronze_table": _TBL,
                "source_format": source_format,
                "bronze_snapshot_fingerprint": fingerprint,
            }
        ).encode("utf-8"),
    )


def _guard(monkeypatch, fs, source_format, current, spark=None):
    """Run the guard with the bronze snapshot fingerprint fixed to *current*
    (None = unavailable)."""
    import common

    if spark is None:
        monkeypatch.setattr(common, "_read_bronze_snapshot_fingerprint", lambda *a, **k: current)
    common.check_bronze_fingerprint(spark, _TBL, _CKPT, source_format=source_format, fs=fs)


@pytest.mark.parametrize("source_format", _FORMATS)
def test_sidecar_missing_fails_open_and_writes_it(monkeypatch, source_format):
    fs = _FakeFS()

    _guard(monkeypatch, fs, source_format, "111111")

    written = json.loads(fs.files[_SIDECAR].decode("utf-8"))
    assert str(written["bronze_snapshot_fingerprint"]) == "111111"
    assert written["source_format"] == source_format
    assert written["bronze_table"] == _TBL


@pytest.mark.parametrize("source_format", _FORMATS)
def test_sidecar_matches_current_snapshot_returns_cleanly(monkeypatch, source_format):
    fs = _FakeFS()
    _seed_sidecar(fs, source_format, "222222")

    _guard(monkeypatch, fs, source_format, "222222")

    assert fs.writes == 1  # only the seeded write


@pytest.mark.parametrize("source_format", _FORMATS)
def test_sidecar_mismatch_raises_silver_abort_carrying_both_fingerprints(
    monkeypatch, source_format
):
    from common import SilverAbort

    fs = _FakeFS()
    _seed_sidecar(fs, source_format, "222222")

    with pytest.raises(SilverAbort) as excinfo:
        _guard(monkeypatch, fs, source_format, "999999")

    assert "222222" in str(excinfo.value)
    assert "999999" in str(excinfo.value)


@pytest.mark.parametrize("source_format", _FORMATS)
def test_no_snapshot_available_fails_open_no_sidecar_written(monkeypatch, source_format):
    """Bronze not created yet: a sidecar with an unknown value would poison
    the next start."""
    fs = _FakeFS()

    _guard(monkeypatch, fs, source_format, None)

    assert fs.writes == 0


def test_snapshot_lookup_error_does_not_kill_the_stream(monkeypatch):
    """A transient catalog failure must not raise; only a mismatch does."""
    fs = _FakeFS()

    class _Boom:
        def sql(self, stmt):
            raise RuntimeError("catalog unreachable: connection reset")

    _guard(monkeypatch, fs, "iceberg", None, spark=_Boom())

    assert fs.writes == 0


@pytest.mark.requires_jars("iceberg", "delta")
@pytest.mark.parametrize("source_format", _FORMATS)
def test_real_bronze_fingerprint_is_a_string_that_changes_on_write(
    spark_session, iceberg_catalog, tmp_path, source_format
):
    """The lookup SQL and row read are real: a wrong query returns None and
    the guard would silently fail open."""
    import common

    spark = spark_session
    if source_format == "iceberg":
        cat = iceberg_catalog(spark, "fp", tmp_path / "wh")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.default")
        tbl = f"{cat}.default.bronze_fp"
        spark.sql(f"CREATE TABLE {tbl} (id BIGINT) USING iceberg")
    else:
        tbl = "default.bronze_fp_delta"
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
        spark.sql(f"CREATE TABLE {tbl} (id BIGINT) USING delta LOCATION '{tmp_path / 'delta'}'")
    spark.sql(f"INSERT INTO {tbl} VALUES (1)")

    first = common._read_bronze_snapshot_fingerprint(spark, tbl, source_format)
    assert isinstance(first, str) and first

    spark.sql(f"INSERT INTO {tbl} VALUES (2)")

    assert common._read_bronze_snapshot_fingerprint(spark, tbl, source_format) != first
