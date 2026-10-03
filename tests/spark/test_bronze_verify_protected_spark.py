"""bronze_verify_financial refuses a corpus from a held-out or spent AML seed
before it reads or writes anything (owner, 10-03).

A development config pointed at a bucket that holds a registered corpus
would otherwise run bronze-verify, silver, gold and the TM operations on it
before the scorer refused. TEST VALUES ONLY: the held-out record is the test
fixture (tests/fixtures/heldout_test.json).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from tests.fixtures import heldout_test_seeds as ts  # noqa: E402
from tests.fixtures import protected_corpus as pc  # noqa: E402


class _Session:
    """The module's Spark session with ``stop`` a no-op, so a refusal does
    not end the session the other tests use."""

    def __init__(self, spark):
        self._spark = spark
        self.stopped = 0

    def stop(self):
        self.stopped += 1

    def __getattr__(self, name):
        return getattr(self._spark, name)


@pytest.fixture(scope="module")
def spark(spark_session):
    return spark_session


@pytest.fixture
def bvf(load_script, monkeypatch, tmp_path):
    pc.use_heldout(monkeypatch)
    mod = load_script("bronze_verify_financial")
    monkeypatch.setattr(mod, "BRONZE_URI", f"file://{tmp_path}/")
    monkeypatch.setattr(mod, "MANIFEST_PATH", "pacs008/manifest/manifest*.parquet")
    return mod


def _write_manifest(spark, tmp_path, rows, name="manifest.parquet"):
    out = tmp_path / "pacs008" / "manifest" / name
    spark.createDataFrame(rows, "typology_id string, seed bigint").coalesce(1).write.mode(
        "overwrite"
    ).parquet(str(out))


def test_held_out_rows_past_row_200_are_refused(spark, bvf, tmp_path):
    rows = ts.manifest_rows(pc.CALIBRATION, 1000) + ts.manifest_rows(pc.EV, 5, start=1000)
    _write_manifest(spark, tmp_path, rows)
    reason = bvf.protected_manifest_reason(spark, required=True)
    assert reason == "the corpus manifest comes from the registered evaluation seed"
    assert pc.seed_tokens(reason) == []


def test_every_cycle_manifest_is_read(spark, bvf, tmp_path):
    _write_manifest(spark, tmp_path, ts.manifest_rows(pc.CALIBRATION, 50))
    _write_manifest(spark, tmp_path, ts.manifest_rows(pc.RB, 3), name="manifest-c001.parquet")
    assert "robustness" in bvf.protected_manifest_reason(spark, required=True)


def test_a_spent_corpus_is_refused_and_calibration_passes(spark, bvf, tmp_path):
    _write_manifest(spark, tmp_path, ts.manifest_rows(42, 20))
    assert "spent" in bvf.protected_manifest_reason(spark, required=True)
    _write_manifest(spark, tmp_path, ts.manifest_rows(pc.CALIBRATION, 300))
    assert bvf.protected_manifest_reason(spark, required=True) is None


def test_a_missing_manifest_refuses_only_when_required(spark, bvf):
    assert "no manifest" in bvf.protected_manifest_reason(spark, required=True)
    assert bvf.protected_manifest_reason(spark, required=False) is None


def test_refusal_stops_with_the_marker_and_no_seed(spark, bvf, tmp_path):
    _write_manifest(spark, tmp_path, ts.manifest_rows(pc.EV, 10))
    session = _Session(spark)
    with pytest.raises(SystemExit) as info:
        bvf.refuse_protected_corpus(session)
    text = str(info.value)
    assert text.startswith(bvf.PROTECTED_REFUSAL) and "evaluation" in text
    assert pc.seed_tokens(text) == [] and session.stopped == 1


def test_an_unreadable_manifest_is_refused(spark, bvf, tmp_path):
    bad = tmp_path / "pacs008" / "manifest"
    bad.mkdir(parents=True)
    (bad / "manifest.parquet").write_bytes(b"not parquet")
    with pytest.raises(SystemExit, match="could not be checked"):
        bvf.refuse_protected_corpus(_Session(spark))


def test_an_unreadable_held_out_record_is_refused(spark, bvf, tmp_path, monkeypatch):
    from lakebench.config import datagen_seed as ds

    def gone():
        raise FileNotFoundError("heldout_hashes.json")

    monkeypatch.setattr(ds, "_heldout", gone)
    _write_manifest(spark, tmp_path, ts.manifest_rows(pc.CALIBRATION, 10))
    with pytest.raises(SystemExit, match="cannot be read"):
        bvf.refuse_protected_corpus(_Session(spark))


@pytest.mark.parametrize("held_out", [True, False], ids=["held-out", "development"])
def test_main_refuses_before_any_namespace_read_or_write(
    spark, bvf, tmp_path, monkeypatch, held_out
):
    """main() runs the check first: a refused corpus never reaches the
    namespace step (or any read, ConfigMap write or registration after it);
    a development corpus does."""
    seed = pc.EV if held_out else pc.CALIBRATION
    _write_manifest(spark, tmp_path, ts.manifest_rows(seed, 30))
    reached: list[str] = []

    class _Reached(Exception):
        pass

    def first_storage_step(*a, **k):
        reached.append("ensure_namespaces")
        raise _Reached

    monkeypatch.setattr(bvf, "ensure_namespaces", first_storage_step)
    monkeypatch.setattr(bvf, "write_bronze_data_clock", lambda *a, **k: reached.append("clock"))
    builder = type(
        "B", (), {"appName": lambda self, n: self, "getOrCreate": lambda self: _Session(spark)}
    )
    monkeypatch.setattr("pyspark.sql.SparkSession.builder", builder())
    if held_out:
        with pytest.raises(SystemExit, match=bvf.PROTECTED_REFUSAL):
            bvf.main()
        assert reached == []
    else:
        with pytest.raises(_Reached):
            bvf.main()
        assert reached == ["ensure_namespaces"]


def test_check_only_mode_stops_after_the_check(spark, bvf, tmp_path, monkeypatch):
    _write_manifest(spark, tmp_path, ts.manifest_rows(pc.CALIBRATION, 30))
    monkeypatch.setattr(bvf, "CHECK_ONLY", True)
    reached: list[str] = []
    monkeypatch.setattr(bvf, "ensure_namespaces", lambda *a, **k: reached.append("ns"))
    session = _Session(spark)
    builder = type("B", (), {"appName": lambda self, n: self, "getOrCreate": lambda self: session})
    monkeypatch.setattr("pyspark.sql.SparkSession.builder", builder())
    bvf.main()
    assert reached == [] and session.stopped == 1


def test_an_unreadable_manifest_is_unchecked_not_refused(spark, bvf, tmp_path):
    bad = tmp_path / "pacs008" / "manifest"
    bad.mkdir(parents=True)
    (bad / "manifest.parquet").write_bytes(b"not parquet")
    with pytest.raises(SystemExit) as info:
        bvf.refuse_protected_corpus(_Session(spark))
    assert str(info.value).startswith(bvf.PROTECTED_UNCHECKED)
