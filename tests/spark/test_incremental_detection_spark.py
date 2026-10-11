"""Continuous gold's incremental re-detection gives, after every tick, the
alerts a full recompute gives on the same silver (a wrong merge would change
continuous recall with nothing failing).

A synthetic corpus with every continuous rule's pattern (structuring by
originator and by beneficiary, pass-throughs, round trips, layering chains,
and a hub that is busy in one week only) arrives over eight silver batches in
event-time order with a few days of overlap; one batch also carries rows two
months old, one tick brings nothing new, W3 and W17 miss every other tick
and W4 one tick (as skips would), and an entity becomes a customer part way
through. W5 and W6 screen payee names against a small watchlist (exact,
typo and wrong-country entries, one listed part way through), and one
originator's entity row lands a tick after its first payments.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

pytestmark = [pytest.mark.slow, pytest.mark.usefixtures("load_script_module")]

T0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
HUB = 3
LATE_CUSTOMER = 7
# Lower than the default (200 a week) so the small corpus has a hub week.
OUT_DEGREE = 8
NETWORK = ("W3_round_tripping", "W17_layering_chain")
# An originator whose entity row (and customer flag) lands one tick after
# the tick its first payments do, as silver_stream commits it.
LATE_ORIGINATOR = 41
# Payee names: listed parties, a one-letter typo of one, and everyone else.
# The listed parties sit outside the random traffic (entities 1-40), and
# their payers never include LATE_CUSTOMER: silver fixes the customer flag
# when an entity is first written, so a flag that flips later (which W2's
# case exercises) never happens to a screened payer.
NAMES = {
    43: "IVAN PETROV SMIRNOV",
    44: "MARIA LOPEZ GARCIA",
    45: "IVAN PETROV SMIRNOW",
    46: "OLEG IVANOV KUZNETSOV",
    47: "CHEN WEI ZHANG",
}


def _name(entity):
    return NAMES.get(entity, f"PARTY {entity:02d} TRADING")


def _watchlist(spark, path):
    """Sanctions and PEP entries: 43 (exact) and 45 (typo of 43) match the
    first sanctions entry, 44 the PEP one, 46 a version-2 entry listed on
    day 60 (payments before it are a rescreen's, after it the transaction
    screen's), and 47's namesake is listed in another country."""
    d = datetime(2023, 12, 1).date()
    late = (T0 + timedelta(days=60)).date()
    rows = [
        ("sanctions", "S-1", 1, d, d, None, "IVAN PETROV SMIRNOV", []),
        ("pep", "P-1", 1, d, d, None, "MARIA LOPEZ GARCIA", ["MARIA L GARCIA"]),
        ("sanctions", "S-2", 2, late, late, None, "OLEG IVANOV KUZNETSOV", []),
        ("sanctions", "S-3", 1, d, d, "CN", "CHEN WEI ZHANG", []),
    ]
    spark.createDataFrame(
        rows,
        "list_type string, list_id string, list_version int, version_published_date date, "
        "listed_date date, country string, name string, aliases array<string>",
    ).write.mode("overwrite").parquet(path)


def _corpus():
    """[(uetr, src, dst, hours after T0, usd)], deterministic."""
    rng = random.Random(43)
    rows = []

    def add(src, dst, h, usd):
        rows.append((f"u{len(rows):05d}", src, dst, h, usd))

    span = 150 * 24
    for _ in range(1200):
        src, dst = rng.sample(range(1, 41), 2)
        add(src, dst, rng.uniform(0, span), rng.uniform(100, 5000))
    # The hub's busy week (hours 1680-1848) and ordinary traffic elsewhere.
    for _ in range(12):
        add(HUB, rng.choice([d for d in range(1, 41) if d != HUB]), rng.uniform(1680, 1840), 700)
    for _ in range(14):
        h = rng.uniform(0, span)
        a, b, c = rng.sample(range(1, 41), 3)
        add(a, b, h, 4000)  # W4: in, then 80-100% out within 6 h
        add(b, c, h + rng.uniform(0.5, 5.5), 4000 * rng.uniform(0.82, 0.99))
    for _ in range(10):
        h = rng.uniform(0, span)
        hops = rng.choice([3, 4])
        nodes = rng.sample(range(1, 41), hops)
        for i in range(hops):  # W3: a cycle within days
            add(nodes[i], nodes[(i + 1) % hops], h + 30 * i, 1500)
    for _ in range(12):
        h = rng.uniform(0, span)
        hops = rng.choice([3, 4, 5, 6])
        nodes = rng.sample(range(1, 41), hops + 1)
        amt = 3000.0
        for i in range(hops):  # W17: a chain passing on 85-99% each hop
            add(nodes[i], nodes[i + 1], h + 20 * i, amt)
            amt *= rng.uniform(0.85, 0.99)
    for _ in range(10):
        h = rng.uniform(0, span)
        a, b = rng.sample(range(1, 41), 2)
        for i in range(rng.choice([3, 4])):  # W2 originator
            add(a, b, h + 3 * i, rng.uniform(9100, 9900))
    for _ in range(6):
        h = rng.uniform(0, span)
        senders = rng.sample([e for e in range(1, 41) if e != LATE_CUSTOMER], 4)
        for i, s in enumerate(senders):  # W2 beneficiary (smurfing)
            add(s, LATE_CUSTOMER if _ % 2 else rng.randint(1, 40), h + 4 * i, 9500)
    # A beneficiary stream that runs for days, so its burst crosses batches.
    for i in range(16):
        add(20 + i % 8, 30, 1000 + 7 * i, 9400)
    # Payments to the listed parties and their namesakes across the period.
    payers = [e for e in range(1, 31) if e != LATE_CUSTOMER]
    for _ in range(40):
        add(rng.choice(payers), rng.choice(list(NAMES)), rng.uniform(0, span), 2500)
    # The late originator pays a listed party in one stretch of days.
    for i in range(5):
        add(LATE_ORIGINATOR, 43, 2600 + 9 * i, 3100)
    return [r for r in rows if r[1] != r[2]]


def _batches(rows, layout):
    """{batch: [(stream_id, batch_id, rows)]}: eight consecutive event-time
    ranges with a few days of overlap at each boundary and two-month-old rows
    in batch 6, delivered as ``layout``:

    - ``ordered``: one silver stream, batch k is range k.
    - ``drift``: two datagen pods in one silver stream (silver's stream id is
      its query's, not a pod's): half the rows run ahead, so batch k brings
      range k of the slow half and range k + 3 of the fast half.
    - ``restart``: as ordered, but silver restarts at batch 5 under a new
      stream id with batch ids from 1 again.
    """
    ordered = sorted(rows, key=lambda r: r[3])
    n = len(ordered)
    ranges = {k: [] for k in range(1, 9)}
    for i, r in enumerate(ordered):
        k = 1 + i * 8 // n
        # The last 2% of a range lands with the next batch, at most a few days
        # behind that batch's newest row.
        if k < 8 and (i * 8) % n > n - n // 50:
            k += 1
        ranges[k].append(r)
    old = ranges[3][:6]
    ranges[3] = ranges[3][6:]
    ranges[6].extend(old)
    if layout == "drift":
        slow = {k: [r for i, r in enumerate(v) if i % 2 == 0] for k, v in ranges.items()}
        fast = {k: [r for i, r in enumerate(v) if i % 2 == 1] for k, v in ranges.items()}
        out = {}
        for k in range(1, 9):
            ahead = [r for j in range(1 if k == 1 else k + 3, k + 4) if j <= 8 for r in fast[j]]
            if k == 8:
                ahead = []
            out[k] = [("s", k, slow[k] + ahead)]
        return out
    if layout == "restart":
        return {k: [("s", k, v)] if k < 5 else [("s2", k - 4, v)] for k, v in ranges.items()}
    return {k: [("s", k, v)] for k, v in ranges.items()}


_COLS = (
    "uetr string, originator_id bigint, beneficiary_id bigint, txn_timestamp timestamp, "
    "txn_amount decimal(18,2), txn_currency string, txn_amount_usd decimal(18,2), "
    "rptd_beneficiary_name string, _stream_id string, _batch_id bigint"
)


def _frame(spark, batches, upto):
    data = [
        (
            u,
            a,
            b,
            T0 + timedelta(hours=h),
            Decimal(f"{usd:.2f}"),
            "USD",
            Decimal(f"{usd:.2f}"),
            _name(b),
            stream,
            batch,
        )
        for k in range(1, upto + 1)
        for stream, batch, rows in batches[k]
        for u, a, b, h, usd in rows
    ]
    return spark.createDataFrame(data, _COLS)


def _position(batches, upto):
    out = {}
    for k in range(1, upto + 1):
        for stream, batch, _ in batches[k]:
            out[stream] = batch
    return out


def _new_hours(batches, upto, done):
    """Event times (hours after T0) of the rows batches ``done``+1..``upto``
    bring."""
    return [r[3] for k in range(done + 1, upto + 1) for _, _, rows in batches[k] for r in rows]


def _entities(spark, tick, late_from):
    """Entities 1-40, the listed payees (not customers), and the late
    originator from tick ``late_from`` on, all in one country."""
    ids = list(range(1, 41)) + ([LATE_ORIGINATOR] if tick >= late_from else [])
    rows = [(e, e != LATE_CUSTOMER or tick >= 5, "US") for e in ids]
    rows += [(e, False, "US") for e in NAMES]
    return spark.createDataFrame(rows, "entity_id bigint, is_customer boolean, country string")


def _first_tick_with(batches, plan, entity):
    """The tick whose silver first holds a payment from ``entity``."""
    for tick, upto in enumerate(plan, start=1):
        if any(
            r[1] == entity for k in range(1, upto + 1) for _, _, rows in batches[k] for r in rows
        ):
            return tick
    raise AssertionError(f"no payment from {entity}")


def _content(df):
    return _content_rows(df.collect())


def _content_rows(rows):
    return sorted(
        (
            r["rule_id"],
            r["entity_id"],
            tuple(r["related_txn_ids"] or ()),
            tuple(r["related_entity_ids"] or ()),
            r["alert_ts"],
            r["alert_score"],
            r["priority"],
            r["alert_type"],
            r["narrative"],
            tuple(sorted((r["evidence"] or {}).items())),
            tuple(r["reason_codes"] or ()),
        )
        for r in rows
    )


def _ts_us(ts):
    # PySpark collects a timestamp as a naive datetime in this process's
    # local time zone, whatever the session time zone: timestamp() reads it
    # as local time.
    return int(ts.timestamp() * 1_000_000)


def _outside(rows, spans):
    """The rows of ``rows`` (collected alerts) whose alert_ts is outside
    ``spans``."""
    return [r for r in rows if not any(lo <= _ts_us(r["alert_ts"]) < hi for lo, hi in spans)]


@pytest.mark.parametrize("layout", ["ordered", "drift", "restart"])
def test_incremental_alerts_equal_a_full_recompute_every_tick(
    spark_session, tmp_path, layout, monkeypatch
):
    """Every tick: the incremental alerts equal a full recompute; the rows
    outside a pass's write spans are the ones the last pass wrote (the
    driver rewrites only the spans, so gold.alerts then equals the full
    recompute); and with drifting pods the silver a pass reads leaves out
    the event time between the slow pod's and the fast pod's new rows."""
    import detection_rules as rules
    import gold_refresh_financial as g
    from incremental_detection import FULL, INCREMENTAL_RULES, IncrementalDetection

    spark = spark_session
    batches = _batches(_corpus(), layout)
    watchlist = f"file://{tmp_path}/watchlist.parquet"
    _watchlist(spark, watchlist)
    monkeypatch.setenv("LB_FINANCIAL_WATCHLIST_PATH", watchlist)
    inc = IncrementalDetection(spark, f"file://{tmp_path}/state")
    seen = None
    # Tick 4 repeats tick 3's silver (nothing new); W4 misses tick 6.
    # The last tick repeats batch 8's silver: only the entity row of a late
    # originator whose payments came last can be new.
    plan = [1, 2, 3, 3, 4, 5, 6, 7, 8, 8]
    late_from = _first_tick_with(batches, plan, LATE_ORIGINATOR) + 1
    final = {}
    table = {}
    rule_done = {}
    for tick, upto in enumerate(plan, start=1):
        txns = _frame(spark, batches, upto)
        ents = _entities(spark, tick, late_from)
        cut, position = g.tick_position(txns, seen)
        assert (cut == FULL) == (seen is None)
        assert position == _position(batches, upto), (tick, position)
        if tick in (4, len(plan)):
            assert cut is None, cut
        inc.begin_tick(cut, INCREMENTAL_RULES)
        seen = position
        for rule_id in INCREMENTAL_RULES:
            if rule_id in NETWORK and tick % 2 and tick != len(plan):
                continue
            if rule_id == "W4_risk_propagation" and tick == 6:
                continue
            fn = rules.get_rule(rule_id)
            params = rules.rule_params(fn, "run-inc", ents)
            params.update(g.CONTINUOUS_RULE_OVERRIDES.get(rule_id, {}))
            if rule_id in NETWORK:
                params["max_out_degree"] = OUT_DEGREE
            if rule_id == "W2_structuring" and tick == 3:
                # A skip: the driver drops the rule's alerts, so the next
                # pass with nothing new must write them again.
                inc.rule(rule_id, fn)(txns, **params)
                inc.failed(rule_id)
                table[rule_id] = []
                continue
            if tick == 4 and rule_id in ("W2_structuring", "W4_risk_propagation"):
                # W4 committed tick 3 and nothing is new; W2 skipped tick 3.
                assert inc.unchanged(rule_id) is (rule_id == "W4_risk_propagation"), rule_id
            got = inc.rule(rule_id, fn)(txns, **params).persist()
            want = fn(txns, **params)
            # gold.alerts is written positionally: the column order counts.
            assert got.columns == want.columns, (rule_id, tick, got.columns)
            rows = got.collect()
            assert _content_rows(rows) == _content(want), (rule_id, tick, inc.last.get(rule_id))
            spans = inc.write_spans(rule_id)
            if spans is None:
                table[rule_id] = rows
            else:
                kept = _outside(table[rule_id], spans)
                assert _content_rows(kept) == _content_rows(_outside(rows, spans)), (rule_id, tick)
                inside = [
                    r for r in rows if any(lo <= _ts_us(r["alert_ts"]) < hi for lo, hi in spans)
                ]
                table[rule_id] = kept + inside
            assert _content_rows(table[rule_id]) == _content(want), (rule_id, tick)
            last = inc.last[rule_id]
            if last["mode"] == "region" and rule_id == "W4_risk_propagation":
                # W4 reads within a day, a week and velocity_hours of a new row,
                # never the event time between pods' new rows.
                near = [
                    (T0 + timedelta(hours=h - 10 * 24), T0 + timedelta(hours=h + 10 * 24))
                    for h in _new_hours(batches, upto, rule_done.get(rule_id, 0))
                ]
                for lo, hi in last["read"]:
                    for us in range(lo, hi, 86_400_000_000):
                        at = datetime.fromtimestamp(us / 1e6, timezone.utc)
                        assert any(a <= at <= b for a, b in near), (tick, at)
            inc.committed(rule_id)
            rule_done[rule_id] = upto
            got.unpersist()
            final[rule_id] = len(rows)
            if tick > 2:
                assert last["mode"] in ("region", "unchanged"), last
    # Every rule has alerts to compare.
    assert all(final[r] > 0 for r in INCREMENTAL_RULES), final
