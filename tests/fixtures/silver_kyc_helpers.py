"""Shared test helpers moved from tests/spark/test_silver_kyc_spark.py (imported by several test files)."""

from __future__ import annotations

import datetime as dt
import sys

import pytest


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
