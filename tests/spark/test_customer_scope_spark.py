"""Executed: which entities each detection rule can alert on (GOALS P10
stage 0). The reporting FI monitors its own customers, so a customer-only
scenario (W2, W5, W6, W7, W8) alerts only on an entity that silver.entities
marks is_customer; non-customers appear as related parties. The graph
scenarios (W1, W3, W4, W17) are declared to alert on counterparties too, and
the TM layer's noncustomer_alerts_declared invariant holds on alerts from all
nine rules over a mixed customer / counterparty silver.

One fixture plants, for every customer-only rule, the same pattern twice: once
with a customer as the subject and once with a non-customer.
"""

from __future__ import annotations

import random
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))

T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
CUSTOMERS = set(range(1, 10))
AE = 60  # the only entity in a FATF-listed country
BACKGROUND = range(200, 260)  # even ids are customers

# rule -> (customer subject, non-customer subject) planted in _txn_rows.
PLANTED = {
    "W2_structuring": ((1, 2), (101, 102)),  # originator kind, beneficiary kind
    "W5_sanctions_match": ((3,), (103,)),
    "W6_pep_counterparty": ((4,), (104,)),
    "W7_cross_border_high_risk": ((5,), (105,)),
    "W8_dormant_reactivation": ((6,), (106,)),
}
CUSTOMER_ONLY = tuple(PLANTED)
GRAPH = (
    "W1_connected_components",
    "W3_round_tripping",
    "W4_risk_propagation",
    "W17_layering_chain",
)


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


def _is_customer(e: int) -> bool:
    return e in CUSTOMERS or (e in BACKGROUND and e % 2 == 0)


def _txn_rows():
    """(uetr, orig, bene, day, hour, amount_usd, cross_border, bene_name)."""
    r = []

    def add(o, b, day, hour, amt, xb=False, name=None):
        r.append((f"u{len(r)}", o, b, day, hour, amt, xb, name))

    # W2 originator kind: three in-band USD payments in a day.
    for sender, bene in ((1, 50), (101, 51)):
        for h in (1, 2, 3):
            add(sender, bene, 120, h, 9500.0)
    # W2 beneficiary kind: three senders structuring into one account.
    for bene, senders in ((2, (131, 132, 133)), (102, (134, 135, 136))):
        for i, s in enumerate(senders):
            add(s, bene, 121, i + 1, 9600.0)
    # W5: a payment to an SDN-listed name.
    for o in (3, 103):
        add(o, 140, 122, 1, 100.0, name="GLOBAL COMMODITY TRADING LLC")
    # W6: a payment over $10,000 to a PEP-listed name.
    for o in (4, 104):
        add(o, 141, 123, 1, 20_000.0, name="MINISTRY OF FINANCE ARCADIA")
    # W7: a cross-border payment into a FATF-listed country.
    for o in (5, 105):
        add(o, AE, 124, 1, 100.0, xb=True)
    # W8: dormant 130 days, then $6,000.
    for o, b in ((6, 142), (106, 143)):
        add(o, b, 0, 1, 100.0)
        add(o, b, 130, 1, 6000.0)
    # W3: a cycle started by a customer and one entirely among non-customers.
    for a, b, c, day in ((7, 150, 151, 125), (152, 153, 154, 126)):
        add(a, b, day, 1, 1000.0)
        add(b, c, day, 2, 990.0)
        add(c, a, day, 3, 980.0)
    # W4: pass-through at a non-customer.
    add(156, 157, 127, 1, 1000.0)
    add(157, 158, 127, 2, 950.0)
    # W17: a layering chain whose first intermediary is a non-customer.
    add(160, 161, 128, 1, 1000.0)
    add(161, 162, 128, 5, 950.0)
    add(162, 163, 128, 9, 900.0)
    # Background payments among customers and non-customers, 30 days.
    rng = random.Random(20260924)
    ids = list(BACKGROUND)
    for _ in range(300):
        o, b = rng.sample(ids, 2)
        add(o, b, 120 + rng.randrange(30), rng.randrange(24), round(rng.uniform(10, 8000), 2))
    return r


def _silver(spark):
    rows = _txn_rows()
    txns = spark.createDataFrame(
        [
            (
                u,
                u,
                o,
                b,
                T0 + timedelta(days=d, hours=h),
                Decimal(f"{amt:.2f}"),
                "USD",
                Decimal(f"{amt:.2f}"),
                xb,
                name or f"PARTY {b}",
            )
            for u, o, b, d, h, amt, xb, name in rows
        ],
        "txn_id string, uetr string, originator_id long, beneficiary_id long, "
        "txn_timestamp timestamp, txn_amount decimal(18,2), txn_currency string, "
        "txn_amount_usd decimal(18,2), cross_border boolean, rptd_beneficiary_name string",
    )
    ids = sorted({o for _, o, *_ in rows} | {b for _, _, b, *_ in rows})
    entities = spark.createDataFrame(
        [(e, _is_customer(e), "AE" if e == AE else "US", "low") for e in ids],
        "entity_id long, is_customer boolean, country string, crr_tier string",
    )
    return txns, entities


def _run(spark, rule_id, txns, entities):
    import inspect

    from detection_rules import get_rule

    fn = get_rule(rule_id)
    kw = {"run_id": "r"}
    if "silver_entities" in inspect.signature(fn).parameters:
        kw["silver_entities"] = entities
    return fn(txns, **kw)


@pytest.fixture(scope="module")
def alerts(spark):
    txns, entities = _silver(spark)
    out = {}
    for rule in CUSTOMER_ONLY + GRAPH:
        out[rule] = [
            (r["entity_id"], r["alert_type"]) for r in _run(spark, rule, txns, entities).collect()
        ]
    return out


@pytest.mark.parametrize("rule", CUSTOMER_ONLY)
def test_customer_only_rule_alerts_on_customers_only(alerts, rule):
    got = {e for e, _ in alerts[rule]}
    cust, noncust = PLANTED[rule]
    assert set(cust) <= got, f"{rule} lost its customer subject: {sorted(got)}"
    assert not (set(noncust) & got), f"{rule} alerted on a non-customer: {sorted(got)}"
    assert all(_is_customer(e) for e in got), f"{rule}: {sorted(got)}"


def test_w2_keeps_both_aggregations(alerts):
    kinds = {e: t for e, t in alerts["W2_structuring"] if e in (1, 2)}
    assert kinds == {1: "structuring", 2: "structuring_beneficiary"}


@pytest.mark.parametrize("rule", GRAPH)
def test_graph_rule_can_alert_on_a_counterparty(alerts, rule):
    """Why the graph scenarios are declared: their subject is where the
    pattern sits in the network, which can be an account at another bank."""
    got = {e for e, _ in alerts[rule]}
    assert any(not _is_customer(e) for e in got), f"{rule}: {sorted(got)}"


def test_declared_lists_partition_the_rules():
    import detection_rules as dr
    import tm_operations as tm

    assert set(dr.COUNTERPARTY_SCENARIOS) == set(GRAPH)
    assert set(dr.CUSTOMER_SCOPED_RULES) == set(CUSTOMER_ONLY)
    assert set(dr.COUNTERPARTY_SCENARIOS) | set(dr.CUSTOMER_SCOPED_RULES) == set(dr.known_rules())
    assert tuple(tm.DEFAULT_COUNTERPARTY_SCENARIOS) == tuple(dr.COUNTERPARTY_SCENARIOS)


def test_noncustomer_alerts_declared_passes_on_all_rules(spark, alerts):
    """The TM layer's own classification (build_alert_inputs) over the union
    of all nine rules' alerts: every non-customer alert comes from a declared
    scenario, and the graph rules do contribute non-customer alerts."""
    import tm_operations as tm

    txns, entities = _silver(spark)
    frames = [_run(spark, r, txns, entities) for r in CUSTOMER_ONLY + GRAPH]
    union = frames[0]
    for f in frames[1:]:
        union = union.unionByName(f)
    manifest = spark.createDataFrame(
        [("t1", "cycle", ["u0"])],
        "typology_id string, typology_type string, participant_uetrs array<string>",
    )
    params = dict(tm.params_from_env(), counterparty_scenarios=tm.DEFAULT_COUNTERPARTY_SCENARIOS)
    rows = tm.build_alert_inputs(spark, union, entities, manifest, params).collect()
    assert rows
    nonc = [r for r in rows if not r["is_customer"]]
    undeclared = [r for r in nonc if not r["declared_counterparty"]]
    assert nonc, "the fixture should produce counterparty alerts from the graph rules"
    assert not undeclared, sorted({(r["rule_id"], r["entity_id"]) for r in undeclared})
    counts = defaultdict(int)
    counts.update(
        {
            "customers": len(CUSTOMERS),
            "monitored": 1,
            "alerts": len(rows),
            "dispositions": len(rows),
            "noncustomer_alerts": len(nonc),
            "noncustomer_undeclared": len(undeclared),
            "customer_alerts": len(rows) - len(nonc),
        }
    )
    inv = {n: s for n, s, _ in tm.evaluate_invariants(counts)}
    assert inv["noncustomer_alerts_declared"] == "pass"


@pytest.mark.parametrize(
    ("entities_sql", "reason"),
    [
        ("SELECT 1L AS entity_id, 'US' AS country", "no-kyc"),
        (
            "SELECT 1L AS entity_id, CAST(NULL AS BOOLEAN) AS is_customer, 'US' AS country",
            "no-customers",
        ),
        ("SELECT 1L AS entity_id, false AS is_customer, 'US' AS country", "no-customers"),
    ],
)
@pytest.mark.parametrize("rule", CUSTOMER_ONLY)
def test_no_customer_population_is_a_skip_not_zero(spark, rule, entities_sql, reason):
    """A silver with no customer flag (a pre-KYC corpus) or no customer must
    read "not run": "ran, 0 alerts" scores as 0% recall."""
    from detection_rules import RuleSkipped

    txns, _ = _silver(spark)
    with pytest.raises(RuleSkipped) as exc:
        _run(spark, rule, txns, spark.sql(entities_sql)).collect()
    assert exc.value.reason == reason


def test_missing_customer_master_is_a_skip(spark):
    """No silver_entities passed and no catalog table: skipped, as W7 was
    for a missing entity master, now for every customer-scoped rule."""
    from detection_rules import RuleSkipped, get_rule

    txns, _ = _silver(spark)
    for rule in CUSTOMER_ONLY:
        with pytest.raises(RuleSkipped) as exc:
            get_rule(rule)(txns, run_id="r")
        assert exc.value.reason == "no-customer-master", rule
