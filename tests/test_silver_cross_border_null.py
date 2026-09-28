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
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
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


def test_cross_border_true_when_countries_differ(spark):
    from silver_build_financial import build_transactions

    bronze = _bronze(spark, [_bronze_row("US", "GB")])
    row = build_transactions(bronze).select("cross_border").collect()[0]
    assert row["cross_border"] is True


def test_cross_border_false_when_countries_match(spark):
    from silver_build_financial import build_transactions

    bronze = _bronze(spark, [_bronze_row("US", "US")])
    row = build_transactions(bronze).select("cross_border").collect()[0]
    assert row["cross_border"] is False


def test_cross_border_null_when_one_country_missing(spark):
    from silver_build_financial import build_transactions

    bronze = _bronze(spark, [_bronze_row("US", None)])
    row = build_transactions(bronze).select("cross_border").collect()[0]
    assert row["cross_border"] is None


def test_cross_border_null_when_both_countries_missing(spark):
    from silver_build_financial import build_transactions

    bronze = _bronze(spark, [_bronze_row(None, None)])
    row = build_transactions(bronze).select("cross_border").collect()[0]
    assert row["cross_border"] is None


def test_deploy_and_script_ddl_cross_border_nullable():
    """Belt-and-braces: both DDL locations must have removed NOT NULL from
    cross_border so an emitted NULL does not fail the write."""
    import re

    def _cross_border_line(text: str) -> str:
        for line in text.splitlines():
            if "cross_border" in line:
                return line.strip()
        raise AssertionError("cross_border column not found")

    deploy = (
        Path(__file__).resolve().parent.parent / "src/lakebench/deploy/financial_ddl.py"
    ).read_text()
    script = (
        Path(__file__).resolve().parent.parent
        / "src/lakebench/spark/scripts/silver_build_financial.py"
    ).read_text()

    for tag, text in ("deploy", deploy), ("script", script):
        line = _cross_border_line(text)
        # Only match a standalone NOT NULL adjacent to cross_border, not the
        # word "null" in a comment further along the file.
        assert not re.search(r"\bNOT\s+NULL\b", line, re.IGNORECASE), (
            f"{tag} DDL still declares cross_border NOT NULL: {line}"
        )
