"""F1 (silver): `_read_reference` fails loud on KYC schema drift.

A reference file that exists but silently drops a KYC column (a datagen
bump, a migration that never replayed the schema) used to slip through:
`build_kyc` returned None, silver wrote NULL KYC for every entity, and
every customer-scoped rule turned into "ran, 0 alerts". This test proves
`_read_reference` now raises `SilverAbort` naming the missing columns
before any silver DataFrame is built.

The check lives in `_read_reference`, not in `build_kyc`, to preserve the
contract that `build_kyc(party.drop("is_customer"), account) is None` (see
tests/spark/test_silver_kyc_spark.py::test_missing_or_old_reference_files_give_null_kyc).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

# Silver scripts import `common` and `silver_build_financial` as top-level
# modules (job.py adds the scripts dir to sys.path at submit time).
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "src/lakebench/spark/scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


def _write_parquet(df, path):
    df.write.mode("overwrite").parquet(str(path))


def _write_full_refs(spark, tmp_path):
    """Write a party + account pair with the full expected KYC schema."""
    import datetime as dt

    party = spark.createDataFrame(
        [
            (
                1,
                True,
                "MERIUS2L",
                dt.date(2015, 5, 1),
                "person",
                1234.5,
                2,
                "medium",
                "country=0;type=0;volume=0;pep=1",
            ),
        ],
        "entity_id bigint, is_customer boolean, home_fi string, "
        "customer_since date, customer_type string, "
        "expected_monthly_volume_usd double, crr_score int, "
        "crr_tier string, crr_factors string",
    )
    account = spark.createDataFrame(
        [(11, "US01", 1, "MERIUS2L")],
        "account_id bigint, iban string, holder_entity_id bigint, home_fi string",
    )
    party_path = tmp_path / "party.parquet"
    acct_path = tmp_path / "account.parquet"
    _write_parquet(party, party_path)
    _write_parquet(account, acct_path)
    return party_path, acct_path


def _read_reference_with_paths(spark, monkeypatch, party_path, acct_path):
    import silver_build_financial as sb

    monkeypatch.setattr(sb, "PARTY_PATH", str(party_path))
    monkeypatch.setattr(sb, "ACCOUNT_PATH", str(acct_path))
    return sb._read_reference(spark)


def test_read_reference_ok_when_schema_complete(spark, tmp_path, monkeypatch):
    party_path, acct_path = _write_full_refs(spark, tmp_path)
    party, account = _read_reference_with_paths(spark, monkeypatch, party_path, acct_path)
    assert party is not None and account is not None
    assert "is_customer" in party.columns
    assert set(account.columns) == {"account_id", "iban", "holder_entity_id", "home_fi"}


def _write_party_missing(spark, tmp_path, dropped):
    """Write a full account and a party missing ``dropped`` columns, to a
    fresh subdirectory so read-and-overwrite races cannot fire."""
    party_path, acct_path = _write_full_refs(spark, tmp_path)
    full = spark.read.parquet(str(party_path))
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(exist_ok=True)
    drift_path = drift_dir / "party.parquet"
    full.drop(*dropped).write.mode("overwrite").parquet(str(drift_path))
    return drift_path, acct_path


def _write_account_missing(spark, tmp_path, dropped):
    party_path, acct_path = _write_full_refs(spark, tmp_path)
    full = spark.read.parquet(str(acct_path))
    drift_dir = tmp_path / "drift"
    drift_dir.mkdir(exist_ok=True)
    drift_path = drift_dir / "account.parquet"
    full.drop(*dropped).write.mode("overwrite").parquet(str(drift_path))
    return party_path, drift_path


def test_read_reference_raises_when_party_missing_is_customer(spark, tmp_path, monkeypatch):
    from common import SilverAbort

    party_path, acct_path = _write_party_missing(spark, tmp_path, ["is_customer"])
    with pytest.raises(SilverAbort) as exc:
        _read_reference_with_paths(spark, monkeypatch, party_path, acct_path)
    msg = str(exc.value)
    assert "KYC schema drift" in msg
    assert "is_customer" in msg
    assert str(party_path) in msg


def test_read_reference_raises_when_party_missing_crr_tier(spark, tmp_path, monkeypatch):
    from common import SilverAbort

    party_path, acct_path = _write_party_missing(spark, tmp_path, ["crr_tier"])
    with pytest.raises(SilverAbort) as exc:
        _read_reference_with_paths(spark, monkeypatch, party_path, acct_path)
    assert "crr_tier" in str(exc.value)


def test_read_reference_raises_when_account_missing_holder_entity_id(spark, tmp_path, monkeypatch):
    from common import SilverAbort

    party_path, acct_path = _write_account_missing(spark, tmp_path, ["holder_entity_id"])
    with pytest.raises(SilverAbort) as exc:
        _read_reference_with_paths(spark, monkeypatch, party_path, acct_path)
    msg = str(exc.value)
    assert "holder_entity_id" in msg
    assert str(acct_path) in msg


def test_read_reference_still_returns_none_for_pre_kyc_manifest(spark, tmp_path, monkeypatch):
    """A corpus without party/account and with a pre-KYC manifest still
    yields (None, None). The schema check must not intercept that path."""
    import silver_build_financial as sb

    # Write a pre-KYC manifest so `_corpus_predates_kyc` returns True.
    manifest_dir = tmp_path / "manifest"
    manifest_dir.mkdir()
    version = next(iter(sb.PRE_KYC_MODEL_VERSIONS))
    m = spark.createDataFrame([(version,)], "model_version string")
    _write_parquet(m, manifest_dir / "manifest0.parquet")

    monkeypatch.setattr(sb, "PARTY_PATH", str(tmp_path / "nope/party.parquet"))
    monkeypatch.setattr(sb, "ACCOUNT_PATH", str(tmp_path / "nope/account.parquet"))
    monkeypatch.setattr(sb, "MANIFEST_GLOB", str(manifest_dir / "manifest*.parquet"))

    assert sb._read_reference(spark) == (None, None)
