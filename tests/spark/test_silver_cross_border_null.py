"""I2: `build_transactions` preserves NULL on `cross_border` when either
party's country of residence is unknown. Coalescing to False previously
claimed "same country" for missing-country corpora and silently dropped
rows W7 (high-risk corridor) should have flagged.

Three bronze rows are synthesised: both countries set (expect False when
they match, True when they differ), one country NULL, and both NULL.
The last two must emit NULL on `cross_border`.
"""

from __future__ import annotations

import datetime as dt
import decimal

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")


PARTY_T = (
    "struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<strt_nm:string, twn_nm:string, ctry:string>, "
    "id:struct<any_bic:string, lei:string>>"
)
ACCT_T = "struct<iban:string, othr:string, ccy:string>"
AGT_T = "struct<bicfi:string, lei:string, nm:string>"


def _bronze_row(dbtr_ctry, cdtr_ctry):
    """Minimal-fields bronze row for build_transactions."""
    dbtr = ("ALICE", dbtr_ctry, ("MAIN ST", "BOSTON", "US"), ("BIC1", "LEIA"))
    cdtr = ("BOB", cdtr_ctry, ("HIGH ST", "LONDON", "GB"), ("BIC2", "LEIB"))
    agt_none = (None, None, None)
    return (
        "MSG",
        dt.datetime(2026, 1, 1),
        dbtr,
        cdtr,
        ("US01", None, "USD"),
        ("GB02", None, "GBP"),
        ("MERIUS2LXXX", None, None),
        ("NRTHGB3XXXX", None, None),
        "TXN",
        "UETR",
        decimal.Decimal("1.00000"),
        "USD",
        dt.datetime(2026, 1, 1),
        "SALA",
        None,
        agt_none,
        agt_none,
        agt_none,
    )


def _bronze(spark, rows):
    return spark.createDataFrame(
        rows,
        f"msg_id string, cre_dt_tm timestamp, dbtr {PARTY_T}, cdtr {PARTY_T}, "
        f"dbtr_acct {ACCT_T}, cdtr_acct {ACCT_T}, dbtr_agt {AGT_T}, cdtr_agt {AGT_T}, "
        f"txn_id string, uetr string, intr_bk_sttlm_amt decimal(18,5), "
        f"intr_bk_sttlm_ccy string, ingest_ts timestamp, purp_cd string, "
        f"rgltry_rptg array<struct<a:string>>, "
        f"intrmy_agt_1 {AGT_T}, intrmy_agt_2 {AGT_T}, intrmy_agt_3 {AGT_T}",
    )


@pytest.mark.parametrize(
    ("dbtr_ctry", "cdtr_ctry", "expected"),
    [
        ("US", "GB", True),
        ("US", "US", False),
        ("US", None, None),
        (None, None, None),
    ],
    ids=["differ", "match", "one_missing", "both_missing"],
)
def test_cross_border(spark_session, dbtr_ctry, cdtr_ctry, expected):
    from silver_build_financial import build_transactions

    bronze = _bronze(spark_session, [_bronze_row(dbtr_ctry, cdtr_ctry)])
    row = build_transactions(bronze).select("cross_border").collect()[0]
    assert row["cross_border"] is expected
