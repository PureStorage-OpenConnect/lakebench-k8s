"""Executed: the W5/W6 watchlist screen (AML-GOALS #50).

The generator plants payments to listed parties under name variants and pays
namesake decoys; the screen must catch the variants, reject the decoys a
country check or a different middle name separates, and find payments made
before a list-version-2 entry was listed only through the rescreen.
"""

from __future__ import annotations

import datetime as dt
import sys
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.usefixtures("load_script")

WL_SCHEMA = (
    "list_id string, list_type string, list_version int, version_published_date date, "
    "listed_date date, entity_type string, name string, aliases array<string>, "
    "country string, town string, program string, position string, model_version string"
)
V1 = dt.date(2021, 1, 1)
V2 = dt.date(2024, 10, 1)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", str(tmp_path_factory.mktemp("sparklocal")))
        .getOrCreate()
    )
    yield s
    s.stop()


def _watchlist(spark):
    rows = [
        (
            "LBS-1",
            "sanctions",
            1,
            V1,
            V1,
            "person",
            "Mohammed Albert Hassan",
            ["Muhammad Albert Hassan"],
            "US",
            "Austin",
            "SDGT",
            None,
        ),
        (
            "LBS-2",
            "sanctions",
            1,
            V1,
            V1,
            "company",
            "Meridian Kestrel Trading LLC",
            [],
            "GB",
            "Leeds",
            "IRAN",
            None,
        ),
        (
            "LBS-3",
            "sanctions",
            2,
            V2,
            V2,
            "person",
            "Priya Linda Sharma",
            [],
            "IN",
            "Pune",
            "GLOMAG",
            None,
        ),
        (
            "LBP-1",
            "pep",
            1,
            V1,
            V1,
            "person",
            "Elena Karen Rodriguez",
            [],
            "MX",
            "Puebla",
            None,
            "minister",
        ),
    ]
    return spark.createDataFrame([(*r, "datagen-v2-rs-0.3") for r in rows], WL_SCHEMA)


def test_screen_catches_variants_and_rejects_decoys(spark):
    from detection_rules import screen_counterparties

    names = spark.createDataFrame(
        [
            ("Mohammed Albert Hassan", "US"),  # exact
            ("Hassan Mohammed Albert", "US"),  # token order
            ("Mohammed Albret Hassan", "US"),  # typo (transposition)
            ("Muhammad Albert Hassan", "US"),  # the list's alias
            ("Muhamad Albert Hasan", "US"),  # a romanisation not on the list
            ("Mohammed Robert Hassan", "US"),  # decoy: another middle name
            ("Mohammed Albert Hassan", "CA"),  # decoy: same name, another country
            ("Meridian Kestrel Trading", "GB"),  # suffix dropped
            ("Kestrel Meridian Trading Ltd", "GB"),  # heads swapped
            ("Meridian Kestrel Logistics LLC", "GB"),  # decoy: other business
            ("Meridian Trading LLC", "GB"),  # an ordinary world company name
            ("James A. Smith", "US"),
        ],
        "rptd_beneficiary_name string, bene_country string",
    )
    got = {
        (r["rptd_beneficiary_name"], r["bene_country"]): (r["list_id"], r["match_mode"])
        for r in screen_counterparties(names, _watchlist(spark)).collect()
    }
    assert got[("Mohammed Albert Hassan", "US")] == ("LBS-1", "exact")
    assert got[("Hassan Mohammed Albert", "US")] == ("LBS-1", "exact")
    assert got[("Muhammad Albert Hassan", "US")] == ("LBS-1", "exact")
    for fuzzy in ("Mohammed Albret Hassan", "Muhamad Albert Hasan"):
        assert got[(fuzzy, "US")] == ("LBS-1", "fuzzy")
    assert got[("Meridian Kestrel Trading", "GB")][0] == "LBS-2"
    assert got[("Kestrel Meridian Trading Ltd", "GB")][0] == "LBS-2"
    for miss in (
        ("Mohammed Robert Hassan", "US"),
        ("Mohammed Albert Hassan", "CA"),
        ("Meridian Kestrel Logistics LLC", "GB"),
        ("Meridian Trading LLC", "GB"),
        ("James A. Smith", "US"),
    ):
        assert miss not in got, miss


def _txns_entities(spark, rows):
    """rows: (uetr, originator, beneficiary, datetime, usd, beneficiary name)."""
    txns = spark.createDataFrame(
        [(u, o, b, t, Decimal(f"{a:.2f}"), Decimal(f"{a:.2f}"), n) for u, o, b, t, a, n in rows],
        "uetr string, originator_id long, beneficiary_id long, txn_timestamp timestamp, "
        "txn_amount decimal(18,2), txn_amount_usd decimal(18,2), rptd_beneficiary_name string",
    )
    country = {100: "US", 101: "IN", 102: "MX", 103: "MX"}
    ids = sorted({o for _, o, *_ in rows} | {b for _, _, b, *_ in rows})
    entities = spark.createDataFrame(
        [(e, e < 10, country.get(e, "US")) for e in ids],
        "entity_id long, is_customer boolean, country string",
    )
    return txns, entities


def test_w5_transaction_screen_and_rescreen(spark, tmp_path, monkeypatch):
    import detection_rules as dr

    path = str(tmp_path / "watchlist.parquet")
    _watchlist(spark).write.parquet(path)
    monkeypatch.setenv("LB_FINANCIAL_WATCHLIST_PATH", path)
    t = dt.datetime
    txns, entities = _txns_entities(
        spark,
        [
            ("a1", 1, 100, t(2022, 3, 1, 10), 500.0, "Hassan Mohammed Albert"),
            # Paid before LBS-3 was listed (version 2): only the rescreen.
            ("b1", 2, 101, t(2023, 5, 2, 11), 800.0, "Priya Linda Sharma"),
            ("b2", 2, 101, t(2024, 2, 9, 11), 900.0, "Priya Lynda Sharma"),
            # A non-customer paying a listed party: out of the bank's scope.
            ("c1", 50, 100, t(2022, 4, 1, 10), 500.0, "Mohammed Albert Hassan"),
            ("d1", 3, 60, t(2022, 4, 1, 10), 500.0, "Mohammed Robert Hassan"),
        ],
    )
    rows = dr.w5_sanctions_match(txns, silver_entities=entities, run_id="r").collect()
    by_type = {}
    for r in rows:
        by_type.setdefault(r["alert_type"], []).append(r)
    assert [(r["entity_id"], r["related_txn_ids"]) for r in by_type["sanctions_match"]] == [
        (1, ["a1"])
    ]
    (re,) = by_type["sanctions_rescreen"]
    assert (re["entity_id"], sorted(re["related_txn_ids"])) == (2, ["b1", "b2"])
    # Dated at the version-2 publication (compared loosely: collect() turns
    # the UTC instant into a local wall-clock time).
    assert abs(re["alert_ts"] - dt.datetime(2024, 10, 1)) <= dt.timedelta(days=1)
    assert re["evidence"]["list_id"] == "LBS-3"
    assert re["evidence"]["txn_total"] == "2" and re["evidence"]["txns_truncated"] == "false"
    assert re["alert_ts"] < re["detected_ts"]
    assert list(rows[0].asDict()) == [f.name for f in dr._empty_alerts_df(spark, "r").schema.fields]


def test_w6_priority_split_and_missing_watchlist(spark, tmp_path, monkeypatch):
    import detection_rules as dr

    path = str(tmp_path / "watchlist.parquet")
    _watchlist(spark).write.parquet(path)
    monkeypatch.setenv("LB_FINANCIAL_WATCHLIST_PATH", path)
    t = dt.datetime
    txns, entities = _txns_entities(
        spark,
        [
            ("p1", 4, 102, t(2023, 1, 5, 9), 25_000.0, "Rodriguez Elena Karen"),
            ("p2", 4, 102, t(2023, 2, 5, 9), 900.0, "Elena Karen Rodriguez"),
            # A namesake with a distant middle name. A close one (Maria for
            # Karen) passes 0.85: that is a namesake false positive, which
            # the decoys exist to charge to precision.
            ("p3", 5, 103, t(2023, 2, 5, 9), 30_000.0, "Elena Fatima Rodriguez"),
        ],
    )
    got = sorted(
        (r["entity_id"], r["related_txn_ids"], r["priority"])
        for r in dr.w6_pep_counterparty(txns, silver_entities=entities, run_id="r").collect()
    )
    # Every PEP payment alerts; the $10,000 line only sets priority.
    assert got == [(4, ["p1"], "MED"), (4, ["p2"], "LOW")]
    monkeypatch.setenv("LB_FINANCIAL_WATCHLIST_PATH", str(tmp_path / "missing.parquet"))
    with pytest.raises(dr.RuleSkipped) as e:
        dr.w6_pep_counterparty(txns, silver_entities=entities, run_id="r")
    assert e.value.reason == "no-watchlist"


def test_tm_truth_is_rule_aware_for_screening_exposures(spark):
    """A planted PEP payment is a true hit for W6 only: a W1 alert citing it
    is not, and a W6 alert on it is not simulated as a false positive."""
    import datetime as dt

    import tm_operations as tm

    t = dt.datetime(2023, 3, 1, 9)
    alerts = spark.createDataFrame(
        [
            ("a6", "W6_pep_counterparty", 4, ["p1"], t),
            ("a1", "W1_connected_components", 4, ["p1", "x9"], t),
            ("a2", "W2_structuring", 4, ["c1"], t),
        ],
        "alert_id string, rule_id string, entity_id long, related_txn_ids array<string>, "
        "alert_ts timestamp",
    )
    entities = spark.createDataFrame(
        [(4, True, "US", "low")],
        "entity_id long, is_customer boolean, country string, crr_tier string",
    )
    manifest = spark.createDataFrame(
        [("PEP_MATCH_1", "pep_match", ["p1"]), ("M1", "micro_structuring", ["c1"])],
        "typology_id string, typology_type string, participant_uetrs array<string>",
    )
    params = dict(tm.params_from_env(), counterparty_scenarios=tm.DEFAULT_COUNTERPARTY_SCENARIOS)
    truth = {
        r["alert_id"]: r["simulated_truth"]
        for r in tm.build_alert_inputs(spark, alerts, entities, manifest, params).collect()
    }
    assert truth == {"a6": True, "a1": False, "a2": True}
