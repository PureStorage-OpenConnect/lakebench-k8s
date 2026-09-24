"""Executed test: silver.entities and silver.accounts carry the monitored
population and KYC from the datagen party/account masters (GOALS P10 stages 0
and 2). A payment reaches its party's KYC through the account IBAN."""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


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


PARTY = (
    "struct<nm:string, ctry_of_res:string, pstl_adr:struct<twn_nm:string>, id:struct<lei:string>>"
)
ACCT = "struct<iban:string, ccy:string>"
AGT = "struct<bicfi:string>"
TS = dt.datetime(2024, 3, 1, 12, 0)


def _bronze(spark):
    alice = ("ALICE", "US", ("BOSTON",), ("LEIALICE",))
    bob = ("BOB", "GB", ("LONDON",), ("LEIBOB",))
    return spark.createDataFrame(
        [
            (alice, bob, ("US01", "USD"), ("GB02", "GBP"), ("MERIUS2LXXX",), ("NRTHGB3XXXX",), TS),
            (bob, alice, ("GB02", "GBP"), ("US01", "USD"), ("NRTHGB3XXXX",), ("MERIUS2LXXX",), TS),
        ],
        f"dbtr {PARTY}, cdtr {PARTY}, dbtr_acct {ACCT}, cdtr_acct {ACCT}, "
        f"dbtr_agt {AGT}, cdtr_agt {AGT}, cre_dt_tm timestamp",
    )


def _refs(spark):
    party = spark.createDataFrame(
        [
            (
                1,
                "clear",
                True,
                True,
                "MERIUS2L",
                dt.date(2015, 5, 1),
                "person",
                1234.5,
                2,
                "medium",
                "country=0;type=0;volume=0;pep=1",
            ),
            (2, "SDN", False, False, "NRTHGB3X", None, None, None, None, None, None),
        ],
        "entity_id bigint, sanctions_status string, pep_status boolean, is_customer boolean, "
        "home_fi string, customer_since date, customer_type string, "
        "expected_monthly_volume_usd double, crr_score int, crr_tier string, crr_factors string",
    )
    account = spark.createDataFrame(
        [
            (11, "US01", 1, "MERIUS2L"),
            (12, "US99", 1, "MERIUS2L"),  # a second account, never used in payments
            (21, "GB02", 2, "NRTHGB3X"),
        ],
        "account_id bigint, iban string, holder_entity_id bigint, home_fi string",
    )
    return party, account


def _txns(bronze):
    from silver_build_financial import _entity_id_from

    return bronze.select(
        _entity_id_from(
            bronze.dbtr.nm, bronze.dbtr.ctry_of_res, bronze.dbtr.pstl_adr.twn_nm, bronze.dbtr.id.lei
        ).alias("originator_id"),
        _entity_id_from(
            bronze.cdtr.nm, bronze.cdtr.ctry_of_res, bronze.cdtr.pstl_adr.twn_nm, bronze.cdtr.id.lei
        ).alias("beneficiary_id"),
        bronze.dbtr.nm.alias("rptd_originator_name"),
        bronze.cdtr.nm.alias("rptd_beneficiary_name"),
    )


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
    # PEP and sanctions now come from the party master, not constants.
    assert a["pep_status"] is True and b["pep_status"] is False
    assert (a["sanctions_status"], b["sanctions_status"]) == ("clear", "sdn")
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
    assert all(r["is_customer"] is None and r["pep_status"] is False for r in ents)
    assert all(r["home_fi"] is None for r in build_accounts(bronze, None).collect())
