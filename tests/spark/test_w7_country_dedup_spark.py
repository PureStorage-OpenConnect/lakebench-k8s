"""Executed: W7 collapses silver.entities to one country per entity the same
way whatever order the rows arrive in.

An entity can carry two country values (an SCD1 update in flight, an upstream
dedup miss). Keeping whichever row comes first makes W7's alert count depend
on row order, so two runs over the same silver can disagree. The rule takes
the smallest country per entity instead.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")

CUSTOMER = 5
BENE = 60  # held as both US and AE (a synthetic high-risk corridor)


def _txns(spark):
    return spark.createDataFrame(
        [
            (
                "u1",
                "u1",
                CUSTOMER,
                BENE,
                datetime(2024, 5, 1, tzinfo=timezone.utc),
                Decimal("100.00"),
                "USD",
                Decimal("100.00"),
                True,
                "PARTY 60",
            )
        ],
        "txn_id string, uetr string, originator_id long, beneficiary_id long, "
        "txn_timestamp timestamp, txn_amount decimal(18,2), txn_currency string, "
        "txn_amount_usd decimal(18,2), cross_border boolean, rptd_beneficiary_name string",
    )


def _entities(spark, bene_countries):
    rows = [(CUSTOMER, True, "US", "low")]
    rows += [(BENE, False, c, "low") for c in bene_countries]
    # One partition, so a first-row-wins dedup would see the rows in this order.
    return spark.createDataFrame(
        rows, "entity_id long, is_customer boolean, country string, crr_tier string"
    ).coalesce(1)


@pytest.mark.parametrize("order", [("US", "AE"), ("AE", "US")])
def test_w7_picks_the_same_country_in_either_row_order(spark, order):
    from detection_rules import w7_cross_border_high_risk

    alerts = w7_cross_border_high_risk(_txns(spark), silver_entities=_entities(spark, order))
    got = [(r["entity_id"], r["evidence"]["country"]) for r in alerts.collect()]
    assert got == [(CUSTOMER, "AE")], (order, got)
