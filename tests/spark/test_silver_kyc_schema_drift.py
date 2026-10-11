"""`_read_reference` raises `SilverAbort` naming the missing column when a
KYC reference file drops one, instead of silently writing NULL KYC for every
entity. The check lives in `_read_reference`, not `build_kyc`, so
`build_kyc(party.drop("is_customer"), account)` still returns None.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")


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


@pytest.mark.parametrize(
    ("side", "dropped"),
    [
        ("party", "is_customer"),
        ("party", "crr_tier"),
        ("account", "holder_entity_id"),
    ],
)
def test_read_reference_raises_naming_the_dropped_column(
    spark_session, tmp_path, monkeypatch, side, dropped
):
    from common import SilverAbort

    write = _write_party_missing if side == "party" else _write_account_missing
    party_path, acct_path = write(spark_session, tmp_path, [dropped])
    with pytest.raises(SilverAbort) as exc:
        _read_reference_with_paths(spark_session, monkeypatch, party_path, acct_path)
    assert dropped in str(exc.value)


def test_read_reference_ok_when_schema_complete(spark_session, tmp_path, monkeypatch):
    party_path, acct_path = _write_full_refs(spark_session, tmp_path)
    party, account = _read_reference_with_paths(spark_session, monkeypatch, party_path, acct_path)
    assert party is not None and account is not None
