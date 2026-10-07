"""Executed test: silver.entities and silver.accounts carry the monitored
population and KYC from the datagen party/account masters (GOALS P10 stages 0
and 2). A payment reaches its party's KYC through the account IBAN."""

from __future__ import annotations

import datetime as dt

import pytest

from tests.fixtures import silver_kyc_helpers as _kyc
from tests.fixtures.silver_kyc_helpers import ACCT as ACCT
from tests.fixtures.silver_kyc_helpers import AGT as AGT
from tests.fixtures.silver_kyc_helpers import PARTY as PARTY
from tests.fixtures.silver_kyc_helpers import TS as TS
from tests.fixtures.silver_kyc_helpers import _bronze as _bronze
from tests.fixtures.silver_kyc_helpers import _refs as _refs
from tests.fixtures.silver_kyc_helpers import _txns as _txns

spark = _kyc.spark  # the module-scoped session fixture

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")


def test_entities_carry_kyc_for_customers_only(spark):
    from silver_build_financial import DDL_ENTITIES, build_entities, build_kyc

    bronze = _bronze(spark)
    kyc = build_kyc(*_refs(spark))
    df = build_entities(_txns(bronze), bronze, kyc)
    rows = {r["name"]: r for r in df.collect()}
    a, b = rows["ALICE"], rows["BOB"]
    assert a["is_customer"] is True and b["is_customer"] is False
    assert (a["home_fi"], b["home_fi"]) == ("MERIUS2L", "NRTHGB3X")
    assert a["crr_tier"] == "medium" and a["customer_type"] == "person"
    assert a["customer_since"] == dt.date(2015, 5, 1)
    assert float(a["expected_monthly_volume_usd"]) == 1234.5
    assert b["crr_tier"] is None and b["customer_since"] is None
    # Answer keys never reach silver (AML-GOALS #50): even a party master
    # that still carries sanctions/PEP flags (a pre-0.3 corpus, as here) is
    # not copied; screening outcomes are W5/W6 alerts.
    for r in (a, b):
        assert (r["pep_status"], r["sanctions_status"], r["initial_risk_score"]) == (
            None,
            None,
            None,
        )
    # Column order is the DDL's (the inline DDL is what silver bootstraps).
    ddl_cols = [
        ln.split()[0] for ln in DDL_ENTITIES.split("(", 1)[1].splitlines() if ln.startswith("    ")
    ]
    assert df.columns == ddl_cols


def test_accounts_carry_home_fi_and_customer_flag(spark):
    from silver_build_financial import (
        DDL_ACCOUNTS,
        build_accounts,
        build_kyc,
        update_accounts_balance,
    )

    bronze = _bronze(spark)
    kyc = build_kyc(*_refs(spark))
    accts = build_accounts(bronze, kyc)
    rows = {r["iban"]: r for r in accts.collect()}
    assert set(rows) == {"US01", "GB02"}
    assert (rows["US01"]["home_fi"], rows["US01"]["is_customer"]) == ("MERIUS2L", True)
    assert (rows["GB02"]["home_fi"], rows["GB02"]["is_customer"]) == ("NRTHGB3X", False)
    ddl_cols = [
        ln.split()[0] for ln in DDL_ACCOUNTS.split("(", 1)[1].splitlines() if ln.startswith("    ")
    ]
    assert accts.columns == ddl_cols
    stmts = spark.createDataFrame(
        [], "account_id bigint, bal_after decimal(38,2), entry_seq bigint"
    )
    assert update_accounts_balance(accts, stmts).columns == ddl_cols


def test_missing_or_old_reference_files_give_null_kyc(spark):
    from silver_build_financial import build_accounts, build_entities, build_kyc

    party, account = _refs(spark)
    assert build_kyc(None, None) is None
    assert build_kyc(party.drop("is_customer"), account) is None
    bronze = _bronze(spark)
    ents = build_entities(_txns(bronze), bronze, None).collect()
    assert all(r["is_customer"] is None and r["pep_status"] is None for r in ents)
    assert all(r["home_fi"] is None for r in build_accounts(bronze, None).collect())


def test_reference_read_tolerates_only_a_missing_path(spark, tmp_path, monkeypatch):
    import silver_build_financial as sb

    monkeypatch.setattr(sb, "PARTY_PATH", str(tmp_path / "nope/party.parquet"))
    monkeypatch.setattr(sb, "ACCOUNT_PATH", str(tmp_path / "nope/account.parquet"))
    assert sb.reference_frames(spark) == (None, None)

    class Boom:
        class read:  # noqa: N801 -- mimics spark.read
            @staticmethod
            def parquet(_path):
                raise RuntimeError("403 Forbidden: InvalidAccessKeyId")

    with pytest.raises(RuntimeError, match="Forbidden"):
        sb._read_reference(Boom())

    # One file present, the other missing: a broken reference write.
    party, _ = _refs(spark)
    party.write.parquet(str(tmp_path / "p.parquet"))
    monkeypatch.setattr(sb, "PARTY_PATH", str(tmp_path / "p.parquet"))
    with pytest.raises(RuntimeError, match="only one KYC reference file"):
        sb._read_reference(spark)


def test_missing_kyc_raises_unless_the_manifest_proves_pre_kyc(spark, tmp_path, monkeypatch):
    import silver_build_financial as sb

    monkeypatch.setattr(sb, "PARTY_PATH", str(tmp_path / "nope/party.parquet"))
    monkeypatch.setattr(sb, "ACCOUNT_PATH", str(tmp_path / "nope/account.parquet"))
    man = tmp_path / "manifest"
    monkeypatch.setattr(sb, "MANIFEST_GLOB", str(man / "manifest*.parquet"))
    # No manifest either (reference pod never ran, or a wrong prefix): raise.
    with pytest.raises(RuntimeError, match="does not show a pre-KYC"):
        sb._read_reference(spark)
    # A pre-KYC manifest: tolerated, NULL KYC.
    spark.createDataFrame([("datagen-v2-rs-0.1",)], "model_version string").write.parquet(
        str(man / "manifest.parquet")
    )
    assert sb._read_reference(spark) == (None, None)
    # A KYC-era cycle manifest next to it: missing KYC raises again.
    spark.createDataFrame([("datagen-v2-rs-0.2",)], "model_version string").write.parquet(
        str(man / "manifest-c001.parquet")
    )
    with pytest.raises(RuntimeError, match="does not show a pre-KYC"):
        sb._read_reference(spark)
