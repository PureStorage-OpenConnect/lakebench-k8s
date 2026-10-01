"""I3: `build_accounts` picks one deterministic winning row per iban.

Prior code used a per-column min over the debtor+creditor union, which
mixed fields across the two sides for the same iban (holder_entity_id
from the debtor row, bank_bic from the creditor row). Now: a row_number
over (holder_entity_id, bank_bic, opened_date) filter selects one whole
row per iban.

The test synthesises a bronze corpus with the same iban on both sides
with different holder_entity_id / bank_bic combinations, and asserts
one row per iban whose fields all come from a single side (the
deterministic minimum by the ordering keys).
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "src/lakebench/spark/scripts"
pytestmark = pytest.mark.usefixtures("load_script")


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
    "struct<nm:string, ctry_of_res:string, pstl_adr:struct<twn_nm:string>, id:struct<lei:string>>"
)
ACCT_T = "struct<iban:string, ccy:string>"
AGT_T = "struct<bicfi:string>"


def _bronze(spark):
    """Two rows that share IBAN US01 on both sides with different holders
    and different bank_bics; one row with an unshared iban US02.
    """
    alice = ("ALICE", "US", ("BOSTON",), ("LEIALICE",))
    bob = ("BOB", "GB", ("LONDON",), ("LEIBOB",))
    return spark.createDataFrame(
        [
            # US01 as debtor, holder derived from alice, bic Z_BANK.
            (
                alice,
                bob,
                ("US01", "USD"),
                ("GB02", "GBP"),
                ("Z_BANK1",),
                ("BANKG1",),
                dt.datetime(2024, 3, 1),
            ),
            # US01 as creditor, holder derived from bob, bic A_BANK
            # (should NOT be picked -- Alice's holder_entity_id is smaller
            # deterministically; regardless of which is smaller the fields
            # must all come from ONE side, not mixed).
            (
                bob,
                alice,
                ("GB03", "GBP"),
                ("US01", "USD"),
                ("BANKG2",),
                ("A_BANK1",),
                dt.datetime(2024, 4, 1),
            ),
            # US02 appears once only.
            (
                alice,
                bob,
                ("US02", "USD"),
                ("GB02", "GBP"),
                ("M_BANK",),
                ("BANKG1",),
                dt.datetime(2024, 5, 1),
            ),
        ],
        f"dbtr {PARTY_T}, cdtr {PARTY_T}, dbtr_acct {ACCT_T}, cdtr_acct {ACCT_T}, "
        f"dbtr_agt {AGT_T}, cdtr_agt {AGT_T}, cre_dt_tm timestamp",
    )


def test_build_accounts_one_row_per_iban(spark):
    from silver_build_financial import build_accounts

    accts = build_accounts(_bronze(spark), kyc=None).collect()
    ibans = [r["iban"] for r in accts]
    # Every iban present exactly once.
    assert sorted(ibans) == sorted(set(ibans))
    assert set(ibans) == {"US01", "US02", "GB02", "GB03"}


def test_build_accounts_row_fields_from_single_side(spark):
    """For the shared iban US01, the winning row's (holder_entity_id,
    bank_bic, opened_date) must all be an actual observed triple, not a
    per-column min blend across sides.
    """
    from silver_build_financial import build_accounts

    accts = {r["iban"]: r for r in build_accounts(_bronze(spark), kyc=None).collect()}
    us01 = accts["US01"]

    # The two observed (holder, bic, opened_date) triples for US01:
    #   side A (dbtr): holder from ALICE, bic Z_BANK1, opened 2024-03-01
    #   side B (cdtr): holder from BOB,   bic A_BANK1, opened 2024-04-01
    # The winning row must be one of these two, not a Frankenstein mix.
    observed_triples = {
        # holder_entity_id we cannot know without recomputing _entity_id_from,
        # but the (bic, opened_date) pair uniquely identifies the side.
        ("Z_BANK1", dt.date(2024, 3, 1)),
        ("A_BANK1", dt.date(2024, 4, 1)),
    }
    assert (us01["bank_bic"], us01["opened_date"]) in observed_triples

    # And -- because the row_number ordering starts with holder_entity_id
    # ascending -- the choice is deterministic across runs. Assert a repeat.
    accts_second = {r["iban"]: r for r in build_accounts(_bronze(spark), kyc=None).collect()}
    assert accts_second["US01"]["holder_entity_id"] == us01["holder_entity_id"]
    assert accts_second["US01"]["bank_bic"] == us01["bank_bic"]
    assert accts_second["US01"]["opened_date"] == us01["opened_date"]


def test_build_accounts_deterministic_across_calls(spark):
    """Repeat build_accounts twice on the same bronze; result byte-identical."""
    from silver_build_financial import build_accounts

    a = sorted(build_accounts(_bronze(spark), kyc=None).collect(), key=lambda r: r["iban"])
    b = sorted(build_accounts(_bronze(spark), kyc=None).collect(), key=lambda r: r["iban"])
    assert [(r["iban"], r["holder_entity_id"], r["bank_bic"], r["opened_date"]) for r in a] == [
        (r["iban"], r["holder_entity_id"], r["bank_bic"], r["opened_date"]) for r in b
    ]


def test_build_accounts_prefers_non_null_bank_bic(spark):
    """Regression on the adversarial finding: an iban that has a NULL
    bank_bic on one side and a non-NULL bank_bic on the other must win the
    non-NULL row. The prior per-column min behaved this way (min skips
    NULLs); the row_number ordering must use NULLS LAST to match, otherwise
    the write fails against silver.accounts's `bank_bic NOT NULL`
    constraint."""
    from silver_build_financial import build_accounts

    alice = ("ALICE", "US", ("BOSTON",), ("LEIA",))
    bob = ("BOB", "GB", ("LONDON",), ("LEIB",))
    bronze = spark.createDataFrame(
        [
            # US10 as debtor with a NULL bank_bic on that side.
            (
                alice,
                bob,
                ("US10", "USD"),
                ("GB02", "GBP"),
                (None,),
                ("BANKG1",),
                dt.datetime(2024, 3, 1),
            ),
            # US10 as creditor with a real bank_bic on that side.
            (
                bob,
                alice,
                ("GB03", "GBP"),
                ("US10", "USD"),
                ("BANKG2",),
                ("Z_BANK1",),
                dt.datetime(2024, 4, 1),
            ),
        ],
        f"dbtr {PARTY_T}, cdtr {PARTY_T}, dbtr_acct {ACCT_T}, cdtr_acct {ACCT_T}, "
        f"dbtr_agt {AGT_T}, cdtr_agt {AGT_T}, cre_dt_tm timestamp",
    )
    row = {r["iban"]: r for r in build_accounts(bronze, kyc=None).collect()}["US10"]
    assert row["bank_bic"] == "Z_BANK1"
