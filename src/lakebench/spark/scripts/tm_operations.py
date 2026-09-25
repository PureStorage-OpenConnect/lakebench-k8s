"""Transaction-monitoring operations layer on top of gold.alerts (GOALS P10).

Runs after detection in the AML gold stage (gold_finalize_financial in batch,
gold_refresh_financial in continuous) and turns an alert table into the shape
of a bank's TM operation:

- Stage 1, monitoring completeness: ``gold.tm_reconciliation`` accounts for
  every source payment as monitored or excluded for a stated reason, lists
  data-quality rule failures, and carries the cycle funnel (payments ->
  monitored -> alerts -> escalated -> cases -> SARs).
  ``gold.scenario_coverage`` is the scenario-to-typology matrix.
- Stage 6, L1 triage: ``gold.alert_dispositions``. Priority is scenario
  weight x CRR tier. Each alert is closed (``closed_nfa``) or ``escalated``
  by a simulated analyst, or ``attached`` to the customer's open case
  (suppression). A QA sample is re-reviewed and the disagreement rate kept.
- Stage 7, L2 cases: ``gold.cases``, keyed on the customer, at most one open
  case per customer, pulling every open alert plus the lookback window of
  activity.
- Stage 8, SAR decision: ``sar_filed`` / ``no_sar`` with the two clocks
  (alert-to-decision aging against a policy SLA; determination-to-filing
  against 31 CFR 1020.320, 30 days, 60 when no suspect is identified) and
  the continuing-activity review 90 days after each SAR.

Dispositions are SIMULATED from ground truth. An alert is truly suspicious
when it touches a planted (non-control) typology transaction in the datagen
manifest; the L1 analyst decides correctly with probability
``analyst_accuracy`` and the L2 investigator with ``investigator_accuracy``.
Every random draw is a hash of (seed, draw name, a content key), not of
arrival order, so a rerun, and the next cycle over a longer corpus, reproduce
the same decisions for the same alert.

Time. The unit of work is the business day. An alert is generated the day
after its last contributing payment (the overnight scenario run), and the
workflow is replayed day by day up to the cycle's as-of date (the day after
the newest payment). Decisions dated after the as-of date have not happened
yet: those alerts and cases are the open backlog.

Continuous behaviour. Detection re-runs over the full corpus each tick, and
this layer is a deterministic projection of the tick's alerts, so each tick
rebuilds alert_dispositions, cases and scenario_coverage in full and appends
one reconciliation set (cycle = tick). Payments datagen has written but the
stream has not yet carried into silver are excluded as ``in_flight``.

Workflow invariants (P10.2) are checked against the tables as written, read
back, and logged as ``[tm-invariant]`` lines; the CLI fails the run when any
is violated. A ``[tm-ops]`` JSON line carries the operations summary for the
scorecard.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import re
from datetime import date, timedelta

from common import env, iceberg_table_stats, log, one_line

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
GOLD_ALERTS = env("LB_FINANCIAL_GOLD_ALERTS", "gold.alerts")
GOLD_STATUS = env("LB_FINANCIAL_GOLD_DETECTION_STATUS", "gold.detection_status")
GOLD_RECON = env("LB_FINANCIAL_GOLD_TM_RECONCILIATION", "gold.tm_reconciliation")
GOLD_COVERAGE = env("LB_FINANCIAL_GOLD_SCENARIO_COVERAGE", "gold.scenario_coverage")
GOLD_DISPOSITIONS = env("LB_FINANCIAL_GOLD_ALERT_DISPOSITIONS", "gold.alert_dispositions")
GOLD_CASES = env("LB_FINANCIAL_GOLD_CASES", "gold.cases")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
MANIFEST_TABLE = env("LB_FINANCIAL_MANIFEST_TABLE", "bronze.manifest")


def _float_env(name, default):
    try:
        return float(env(name, str(default)))
    except ValueError:
        return float(default)


def _int_env(name, default):
    try:
        return int(env(name, str(default)))
    except ValueError:
        return int(default)


def params_from_env() -> dict:
    """Workflow parameters (``workload.tm_operations`` in the config)."""
    return {
        "seed": _int_env("LB_TM_SEED", 20260924),
        "analyst_accuracy": _float_env("LB_TM_ANALYST_ACCURACY", 0.90),
        "investigator_accuracy": _float_env("LB_TM_INVESTIGATOR_ACCURACY", 0.95),
        "qa_sample_rate": _float_env("LB_TM_QA_SAMPLE_RATE", 0.05),
        "alert_sla_days": _int_env("LB_TM_ALERT_SLA_DAYS", 60),
        "lookback_months": _int_env("LB_TM_CASE_LOOKBACK_MONTHS", 12),
        "late_filing_rate": _float_env("LB_TM_LATE_FILING_RATE", 0.03),
    }


# ---------------------------------------------------------------------------
# DDL. Mirrored in src/lakebench/deploy/financial_ddl.py; a test holds the two
# column lists in lock-step.
# ---------------------------------------------------------------------------

DDL_RECON = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_RECON} (
    run_id         STRING NOT NULL,
    cycle          INT NOT NULL,
    cycle_run_id   STRING NOT NULL,
    as_of_date     DATE,
    section        STRING NOT NULL,
    item           STRING NOT NULL,
    unit           STRING NOT NULL,
    item_count     BIGINT,
    amount_usd     DECIMAL(38, 2),
    computed_ts    TIMESTAMP NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""

DDL_COVERAGE = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_COVERAGE} (
    typology             STRING,
    rule_id              STRING,
    coverage             STRING NOT NULL,
    rule_status          STRING,
    alert_count          BIGINT,
    customer_alert_count BIGINT,
    planted_instances    BIGINT,
    run_id               STRING NOT NULL,
    computed_ts          TIMESTAMP NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""

DDL_DISPOSITIONS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_DISPOSITIONS} (
    alert_id           STRING NOT NULL,
    alert_key          STRING NOT NULL,
    rule_id            STRING NOT NULL,
    entity_id          BIGINT NOT NULL,
    is_customer        BOOLEAN NOT NULL,
    crr_tier           STRING,
    scenario_weight    DOUBLE NOT NULL,
    priority_score     DOUBLE NOT NULL,
    triage_priority    STRING NOT NULL,
    generated_date     DATE NOT NULL,
    l1_decision_date   DATE,
    disposition        STRING,
    queue_status       STRING NOT NULL,
    case_id            STRING,
    decision_date      DATE,
    aging_days         INT NOT NULL,
    sla_breached       BOOLEAN NOT NULL,
    simulated_truth    BOOLEAN NOT NULL,
    analyst_correct    BOOLEAN,
    qa_sampled         BOOLEAN NOT NULL,
    qa_disposition     STRING,
    qa_disagrees       BOOLEAN,
    as_of_date         DATE NOT NULL,
    run_id             STRING NOT NULL,
    computed_ts        TIMESTAMP NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""

DDL_CASES = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_CASES} (
    case_id                        STRING NOT NULL,
    customer_id                    BIGINT NOT NULL,
    case_type                      STRING NOT NULL,
    parent_case_id                 STRING,
    opened_date                    DATE NOT NULL,
    crr_tier                       STRING,
    priority                       STRING NOT NULL,
    alert_count                    INT NOT NULL,
    escalated_alert_count          INT NOT NULL,
    rule_ids                       ARRAY<STRING>,
    first_alert_date               DATE,
    activity_window_start          DATE NOT NULL,
    activity_window_end            DATE NOT NULL,
    activity_txn_count             BIGINT,
    activity_amount_usd            DECIMAL(38, 2),
    case_status                    STRING NOT NULL,
    determination                  STRING,
    determination_date             DATE,
    sar_decision                   STRING,
    suspect_identified             BOOLEAN NOT NULL,
    filing_deadline_date           DATE,
    filing_date                    DATE,
    determination_to_filing_days   INT,
    filed_late                     BOOLEAN,
    alert_to_decision_days         INT,
    sla_breached                   BOOLEAN NOT NULL,
    continuing_review_due_date     DATE,
    continuing_review_case_id      STRING,
    simulated_truth                BOOLEAN NOT NULL,
    as_of_date                     DATE NOT NULL,
    run_id                         STRING NOT NULL,
    computed_ts                    TIMESTAMP NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""

TM_DDLS = (
    ("tm_reconciliation", DDL_RECON),
    ("scenario_coverage", DDL_COVERAGE),
    ("alert_dispositions", DDL_DISPOSITIONS),
    ("cases", DDL_CASES),
)


def bootstrap_tm_tables(spark) -> None:
    for name, ddl in TM_DDLS:
        spark.sql(ddl)
        log(f"Bootstrapped gold.{name}")


# ---------------------------------------------------------------------------
# Triage priority (stage 6)
# ---------------------------------------------------------------------------

# Scenario weight: how much an alert of this scenario is worth an analyst's
# time before anything is known about the customer. Network and round-trip
# scenarios score highest; W4 rapid pass-through is the noisiest scenario and
# scores lowest.
SCENARIO_WEIGHT = {
    "W1_connected_components": 3.0,
    "W3_round_tripping": 3.0,
    "W17_layering_chain": 3.0,
    "W5_sanctions_match": 3.0,
    "W2_structuring": 2.0,
    "W6_pep_counterparty": 2.0,
    "W7_cross_border_high_risk": 2.0,
    "W8_dormant_reactivation": 2.0,
    "W4_risk_propagation": 1.0,
}
DEFAULT_SCENARIO_WEIGHT = 1.0
# Customer risk rating tier multiplier. A non-customer or unrated party
# counts as low.
CRR_MULTIPLIER = {"low": 1.0, "medium": 2.0, "high": 3.0}
# priority_score = weight x CRR multiplier, in [1, 9]. Thresholds below.
PRIORITY_BANDS = ((6.0, "critical"), (4.0, "high"), (2.0, "medium"), (0.0, "low"))
PRIORITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}


def triage_priority(score: float) -> str:
    for floor, name in PRIORITY_BANDS:
        if score >= floor:
            return name
    return "low"


# ---------------------------------------------------------------------------
# Workflow clocks and simulated turnaround
# ---------------------------------------------------------------------------

# L1 turnaround by priority: the queue is worked highest priority first, so a
# critical alert is decided in days and a low one waits weeks.
L1_TURNAROUND_DAYS = {
    "critical": (1, 3),
    "high": (1, 7),
    "medium": (2, 14),
    "low": (3, 21),
}
INVESTIGATION_DAYS = (10, 60)
FILING_DAYS = (2, 28)
LATE_FILING_DAYS = (31, 45)
# 31 CFR 1020.320(d): file within 30 calendar days of the initial
# determination of facts; a further 30 (60 in total) when no suspect is
# identified.
FILING_DEADLINE_DAYS = 30
FILING_DEADLINE_NO_SUSPECT_DAYS = 60
# FinCEN SAR FAQ: review continuing activity 90 days after the SAR is filed.
CONTINUING_REVIEW_DAYS = 90

# Alert queue aging buckets (P10.3 queue health).
AGING_BUCKETS = ((0, 30, "0-30"), (31, 60, "31-60"), (61, 90, "61-90"), (91, None, "90+"))


def aging_bucket(days: int) -> str:
    for lo, hi, name in AGING_BUCKETS:
        if days >= lo and (hi is None or days <= hi):
            return name
    return "0-30"


def draw(seed: int, name: str, key: str) -> float:
    """Uniform [0, 1) from a hash of (seed, draw name, content key)."""
    h = hashlib.sha256(f"{seed}|{name}|{key}".encode()).digest()
    return int.from_bytes(h[:8], "big") / 2.0**64


def draw_int(seed: int, name: str, key: str, lo: int, hi: int) -> int:
    return lo + min(hi - lo, int(draw(seed, name, key) * (hi - lo + 1)))


def add_months(d: date, months: int) -> date:
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    days_in = [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28]
    days_in += [31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return date(y, m, min(d.day, days_in[m - 1]))


def filing_deadline(determination: date, suspect_identified: bool) -> date:
    days = FILING_DEADLINE_DAYS if suspect_identified else FILING_DEADLINE_NO_SUSPECT_DAYS
    return determination + timedelta(days=days)


def case_id_for(customer_id: int, opened: date, case_type: str) -> str:
    raw = f"{customer_id}|{opened.isoformat()}|{case_type}"
    return "case-" + hashlib.sha256(raw.encode()).hexdigest()[:24]


# ---------------------------------------------------------------------------
# Per-customer workflow replay (pure Python; runs on executors)
# ---------------------------------------------------------------------------

# Event order within one day: a case closing frees the customer before a
# review opens, reviews and L1 decisions open cases before the day's new
# alerts arrive, so a same-day arrival attaches to a case opened that day.
_EV_CLOSE, _EV_DETERMINE, _EV_REVIEW, _EV_L1, _EV_ARRIVE = range(5)


def simulate_customer(customer_id, crr_tier, alerts, params, as_of):
    """Replay one customer's alerts through L1, L2 and SAR filing.

    ``alerts``: dicts with alert_id, alert_key, rule_id, generated_date,
    truth, triage_priority, priority_score, scenario_weight. ``as_of``: the
    cycle's business date; nothing dated later has happened.

    Returns ``(dispositions, cases)``: lists of dicts, one per alert and one
    per case.
    """
    seed = int(params["seed"])
    l1_acc = float(params["analyst_accuracy"])
    l2_acc = float(params["investigator_accuracy"])
    qa_rate = float(params["qa_sample_rate"])
    sla = int(params["alert_sla_days"])
    lookback = int(params["lookback_months"])
    late_rate = float(params["late_filing_rate"])

    st = {}  # alert_key -> mutable state
    pending = {}  # alert keys awaiting L1, in arrival order (dict as ordered set)
    cases = []
    open_case = [None]
    events = []
    seq = [0]

    def push(day, kind, ref):
        seq[0] += 1
        heapq.heappush(events, (day, kind, seq[0], ref))

    for a in sorted(alerts, key=lambda x: (x["generated_date"], x["alert_key"])):
        st[a["alert_key"]] = {
            "a": a,
            "disposition": None,
            "l1_date": None,
            "case": None,
            "decision_date": None,
            "correct": None,
        }
        push(a["generated_date"], _EV_ARRIVE, a["alert_key"])

    def link(s, case, disposition, day):
        s["disposition"] = disposition
        s["case"] = case
        case["alert_keys"].append(s["a"]["alert_key"])
        if case["determination_date"] is not None:
            # Determination already made (SAR being filed): the alert is
            # reviewed as continuing activity on attachment.
            s["decision_date"] = max(day, case["determination_date"])

    def open_new_case(day, case_type, parent=None):
        case = {
            "case_id": case_id_for(customer_id, day, case_type),
            "case_type": case_type,
            "parent_case_id": parent["case_id"] if parent else None,
            "opened_date": day,
            "alert_keys": [],
            "escalated": 0,
            "determination": None,
            "determination_date": None,
            "sar_decision": None,
            "filing_date": None,
            "filing_deadline": None,
            "close_date": None,
            "review_due": None,
            "review_case_id": None,
            "truth": False,
            "review_since": parent["filing_date"] if parent else None,
        }
        cases.append(case)
        open_case[0] = case
        # Suppression and case scope: every alert still waiting for L1 on
        # this customer joins the case.
        for k in list(pending):
            link(st[k], case, "attached", day)
        pending.clear()
        inv = draw_int(seed, "investigation", case["case_id"], *INVESTIGATION_DAYS)
        push(day + timedelta(days=inv), _EV_DETERMINE, case["case_id"])
        return case

    by_id = {}
    while events:
        day, kind, _, ref = heapq.heappop(events)
        if day > as_of:
            break
        if kind == _EV_ARRIVE:
            s = st[ref]
            if open_case[0] is not None:
                link(s, open_case[0], "attached", day)
            else:
                lo, hi = L1_TURNAROUND_DAYS[s["a"]["triage_priority"]]
                pending[ref] = True
                push(
                    day + timedelta(days=draw_int(seed, "l1_turnaround", ref, lo, hi)), _EV_L1, ref
                )
        elif kind == _EV_L1:
            s = st[ref]
            if ref not in pending:
                continue  # attached to a case before its turn came
            del pending[ref]
            s["l1_date"] = day
            correct = draw(seed, "l1", ref) < l1_acc
            truth = bool(s["a"]["truth"])
            s["correct"] = correct
            if truth == correct:  # escalate: right on a true alert, wrong on a false one
                case = open_case[0]
                if case is None:
                    case = open_new_case(day, "alert_escalation")
                    by_id[case["case_id"]] = case
                link(s, case, "escalated", day)
                case["escalated"] += 1
            else:
                s["disposition"] = "closed_nfa"
                s["decision_date"] = day
        elif kind == _EV_DETERMINE:
            case = by_id[ref]
            members = [st[k] for k in case["alert_keys"]]
            if case["case_type"] == "continuing_activity":
                since = case["review_since"]
                truth = any(
                    x["a"]["truth"] and since < x["a"]["generated_date"] <= day for x in st.values()
                )
            else:
                truth = any(m["a"]["truth"] for m in members)
            case["truth"] = truth
            correct = draw(seed, "l2", case["case_id"]) < l2_acc
            suspicious = truth == correct
            case["determination_date"] = day
            for m in members:
                m["decision_date"] = day
            if suspicious:
                case["determination"] = "suspicious"
                case["filing_deadline"] = filing_deadline(day, True)
                if draw(seed, "late_filing", case["case_id"]) < late_rate:
                    lag = draw_int(seed, "filing_days", case["case_id"], *LATE_FILING_DAYS)
                else:
                    lag = draw_int(seed, "filing_days", case["case_id"], *FILING_DAYS)
                push(day + timedelta(days=lag), _EV_CLOSE, case["case_id"])
            else:
                case["determination"] = "not_suspicious"
                case["sar_decision"] = "no_sar"
                case["close_date"] = day
                open_case[0] = None
        elif kind == _EV_CLOSE:
            case = by_id[ref]
            case["sar_decision"] = "sar_filed"
            case["filing_date"] = day
            case["close_date"] = day
            case["review_due"] = day + timedelta(days=CONTINUING_REVIEW_DAYS)
            open_case[0] = None
            push(case["review_due"], _EV_REVIEW, case["case_id"])
        elif kind == _EV_REVIEW:
            parent = by_id[ref]
            if open_case[0] is not None:
                # The customer is already under investigation: the review is
                # folded into the open case.
                parent["review_case_id"] = open_case[0]["case_id"]
            else:
                rc = open_new_case(day, "continuing_activity", parent=parent)
                by_id[rc["case_id"]] = rc
                parent["review_case_id"] = rc["case_id"]

    dispositions = []
    for k, s in st.items():
        a = s["a"]
        decided = s["decision_date"]
        aging = max(0, ((decided or as_of) - a["generated_date"]).days)
        qa_sampled = s["l1_date"] is not None and draw(seed, "qa_pick", k) < qa_rate
        qa_disp = None
        qa_dis = None
        if qa_sampled:
            qa_correct = draw(seed, "qa", k) < l2_acc
            qa_disp = "escalated" if bool(a["truth"]) == qa_correct else "closed_nfa"
            l1_disp = "escalated" if s["disposition"] in ("escalated",) else "closed_nfa"
            qa_dis = qa_disp != l1_disp
        dispositions.append(
            {
                "alert_id": a["alert_id"],
                "alert_key": k,
                "l1_decision_date": s["l1_date"],
                "disposition": s["disposition"],
                "queue_status": "closed" if decided is not None else "open",
                "case_id": s["case"]["case_id"] if s["case"] else None,
                "decision_date": decided,
                "aging_days": aging,
                "sla_breached": aging > sla,
                "analyst_correct": s["correct"],
                "qa_sampled": qa_sampled,
                "qa_disposition": qa_disp,
                "qa_disagrees": qa_dis,
            }
        )

    out_cases = []
    for c in cases:
        members = [st[k]["a"] for k in c["alert_keys"]]
        first = min((m["generated_date"] for m in members), default=None)
        rank = max((PRIORITY_RANK[m["triage_priority"]] for m in members), default=1)
        priority = [p for p, r in PRIORITY_RANK.items() if r == rank][0]
        det = c["determination_date"]
        start_clock = first or c["opened_date"]
        a2d = max(0, ((det or as_of) - start_clock).days)
        filing_days = (c["filing_date"] - det).days if c["filing_date"] and det else None
        if c["close_date"] is not None:
            status = "closed"
        elif det is not None:
            status = "pending_filing"
        else:
            status = "open"
        out_cases.append(
            {
                "case_id": c["case_id"],
                "customer_id": customer_id,
                "case_type": c["case_type"],
                "parent_case_id": c["parent_case_id"],
                "opened_date": c["opened_date"],
                "crr_tier": crr_tier,
                "priority": priority,
                "alert_count": len(members),
                "escalated_alert_count": c["escalated"],
                "rule_ids": sorted({m["rule_id"] for m in members}),
                "first_alert_date": first,
                "activity_window_start": add_months(c["opened_date"], -lookback),
                "activity_window_end": c["opened_date"],
                "case_status": status,
                "determination": c["determination"],
                "determination_date": det,
                "sar_decision": c["sar_decision"],
                "suspect_identified": True,
                "filing_deadline_date": c["filing_deadline"],
                "filing_date": c["filing_date"],
                "determination_to_filing_days": filing_days,
                "filed_late": (
                    c["filing_date"] > c["filing_deadline"]
                    if c["filing_date"] and c["filing_deadline"]
                    else None
                ),
                "alert_to_decision_days": a2d,
                "sla_breached": a2d > sla,
                "continuing_review_due_date": c["review_due"],
                "continuing_review_case_id": c["review_case_id"],
                "simulated_truth": bool(c["truth"]),
            }
        )
    return dispositions, out_cases


def _simulate_group(item, params, as_of):
    """RDD adapter: (customer_id, iterable of alert tuples) -> tagged rows."""
    customer_id, rows = item
    rows = list(rows)
    crr_tier = rows[0][7] if rows else None
    alerts = [
        {
            "alert_id": r[0],
            "alert_key": r[1],
            "rule_id": r[2],
            "generated_date": r[3],
            "truth": r[4],
            "triage_priority": r[5],
            "priority_score": r[6],
        }
        for r in rows
    ]
    disp, cases = simulate_customer(customer_id, crr_tier, alerts, params, as_of)
    out = [("d", json.dumps(_jsonable(d))) for d in disp]
    out += [("c", json.dumps(_jsonable(c))) for c in cases]
    return out


def _jsonable(d):
    return {k: (v.isoformat() if isinstance(v, date) else v) for k, v in d.items()}


# ---------------------------------------------------------------------------
# Run identity
# ---------------------------------------------------------------------------

_CYCLE_SUFFIX = re.compile(r"^(?P<base>.+)-c(?P<n>\d+)$")


def split_run_id(run_id: str, cycle: int | None = None) -> tuple[str, int]:
    """(base run id, cycle number). Multi-cycle batch runs pass
    ``<run>-c<N>``; continuous passes the tick as ``cycle``."""
    m = _CYCLE_SUFFIX.match(run_id)
    if cycle is not None:
        return run_id, int(cycle)
    if m:
        return m.group("base"), int(m.group("n"))
    return run_id, 1


# ---------------------------------------------------------------------------
# Spark stages
# ---------------------------------------------------------------------------


def count_source_rows(spark):
    """Payments the datagen wrote (the raw pacs.008 Parquet), or None."""
    from bronze_verify_financial import BRONZE_URI, PACS_PREFIX

    try:
        return int(spark.read.parquet(BRONZE_URI + PACS_PREFIX).count())
    except Exception as e:  # noqa: BLE001
        log(f"[tm] source payment count unavailable: {one_line(e)}")
        return None


def classify_payments(txns, entities):
    """Monitored or excluded, per silver payment, plus DQ rule flags.

    A payment is monitored when either party is a customer of the reporting
    FI, unless its amount cannot be put in USD (no amount-based scenario can
    evaluate it). Order of the exclusion reasons: not our payment first.
    """
    from pyspark.sql.functions import coalesce, col, lit, when

    cust = entities.where(col("is_customer") == lit(True)).select("entity_id").distinct()
    o = cust.select(col("entity_id").alias("originator_id"), lit(True).alias("_o_cust"))
    b = cust.select(col("entity_id").alias("beneficiary_id"), lit(True).alias("_b_cust"))
    t = txns.select(
        "originator_id",
        "beneficiary_id",
        "txn_amount",
        "txn_amount_usd",
        "originator_bank_bic",
        "beneficiary_bank_bic",
    )
    t = t.join(o, "originator_id", "left").join(b, "beneficiary_id", "left")
    customer_party = coalesce(col("_o_cust"), lit(False)) | coalesce(col("_b_cust"), lit(False))
    return t.select(
        when(~customer_party, lit("no_customer_party"))
        .when(col("txn_amount_usd").isNull(), lit("dq_unconvertible_currency"))
        .otherwise(lit("monitored"))
        .alias("classification"),
        col("txn_amount_usd"),
        col("txn_amount_usd").isNull().alias("dq_unconvertible_currency"),
        (col("txn_amount") <= 0).alias("dq_nonpositive_amount"),
        (col("originator_id") == col("beneficiary_id")).alias("dq_self_transfer"),
        (col("originator_bank_bic").isNull() | col("beneficiary_bank_bic").isNull()).alias(
            "dq_missing_agent_bic"
        ),
    )


DQ_RULES = (
    "dq_unconvertible_currency",
    "dq_nonpositive_amount",
    "dq_self_transfer",
    "dq_missing_agent_bic",
)


def reconcile(spark, txns, entities, source_rows, bronze_rows, continuous):
    """Completeness rows (section, item, unit, count, amount) for one cycle."""
    from pyspark.sql.functions import col, count, lit, when
    from pyspark.sql.functions import sum as sum_

    cls = classify_payments(txns, entities)
    aggs = [count(lit(1)).alias("n"), sum_(col("txn_amount_usd")).alias("usd")]
    aggs += [sum_(when(col(r), lit(1)).otherwise(lit(0))).alias(r) for r in DQ_RULES]
    grouped = {r["classification"]: r for r in cls.groupBy("classification").agg(*aggs).collect()}

    def n(k):
        return int(grouped[k]["n"]) if k in grouped else 0

    def usd(k):
        return grouped[k]["usd"] if k in grouped else None

    silver_rows = sum(int(r["n"]) for r in grouped.values())
    rows = [
        ("completeness", "source", "payments", source_rows, None),
        ("completeness", "bronze", "payments", bronze_rows, None),
        ("completeness", "silver", "payments", silver_rows, None),
        ("completeness", "monitored", "payments", n("monitored"), usd("monitored")),
    ]
    excluded = 0
    for reason in ("no_customer_party", "dq_unconvertible_currency"):
        rows.append(("exclusion", reason, "payments", n(reason), usd(reason)))
        excluded += n(reason)
    if continuous and source_rows is not None:
        # Written by datagen, not yet carried into silver by the stream.
        in_flight = source_rows - silver_rows
        rows.append(("exclusion", "in_flight", "payments", in_flight, None))
        excluded += in_flight
    rows.append(("completeness", "excluded", "payments", excluded, None))
    for rule in DQ_RULES:
        rows.append(
            ("dq", rule, "payments", sum(int(r[rule] or 0) for r in grouped.values()), None)
        )
    return rows, {"monitored": n("monitored"), "excluded": excluded, "silver": silver_rows}


def build_alert_inputs(spark, alerts, entities, manifest):
    """One row per alert: stable key, customer flags, truth and priority."""
    from pyspark.sql.functions import (
        array,
        array_sort,
        coalesce,
        col,
        concat_ws,
        create_map,
        date_add,
        explode,
        lit,
        row_number,
        sha2,
        to_date,
        when,
    )
    from pyspark.sql.window import Window

    weight_map = create_map(*[x for k, v in SCENARIO_WEIGHT.items() for x in (lit(k), lit(v))])
    crr_map = create_map(*[x for k, v in CRR_MULTIPLIER.items() for x in (lit(k), lit(v))])
    ents = entities.select(
        col("entity_id"),
        coalesce(col("is_customer"), lit(False)).alias("is_customer"),
        col("crr_tier"),
    ).dropDuplicates(["entity_id"])
    content = sha2(
        concat_ws(
            "|",
            col("rule_id"),
            col("entity_id").cast("string"),
            col("alert_ts").cast("string"),
            concat_ws(
                ",", array_sort(coalesce(col("related_txn_ids"), array().cast("array<string>")))
            ),
        ),
        256,
    )
    base = alerts.select(
        "alert_id", "rule_id", "entity_id", "alert_ts", "related_txn_ids"
    ).withColumn("_content", content)
    # Identical alerts (same rule, entity, time and txns) share a content
    # hash; the occurrence number keeps their keys distinct.
    w = Window.partitionBy("_content").orderBy("alert_id")
    base = base.withColumn(
        "alert_key",
        concat_ws("-", col("_content"), (row_number().over(w) - 1).cast("string")),
    )
    planted = (
        manifest.where(col("typology_type") != lit("random"))
        .select(explode(col("participant_uetrs")).alias("uetr"))
        .distinct()
    )
    hits = (
        base.select("alert_id", explode(col("related_txn_ids")).alias("uetr"))
        .join(planted, "uetr", "left_semi")
        .select("alert_id")
        .distinct()
        .withColumn("_hit", lit(True))
    )
    out = (
        base.join(hits, "alert_id", "left")
        .join(ents, "entity_id", "left")
        .select(
            "alert_id",
            "alert_key",
            "rule_id",
            "entity_id",
            coalesce(col("is_customer"), lit(False)).alias("is_customer"),
            col("crr_tier"),
            coalesce(weight_map[col("rule_id")], lit(DEFAULT_SCENARIO_WEIGHT)).alias(
                "scenario_weight"
            ),
            date_add(to_date(col("alert_ts")), 1).alias("generated_date"),
            coalesce(col("_hit"), lit(False)).alias("simulated_truth"),
        )
        .withColumn(
            "priority_score",
            col("scenario_weight") * coalesce(crr_map[col("crr_tier")], lit(1.0)),
        )
    )
    band = None
    for floor, name in PRIORITY_BANDS:
        cond = col("priority_score") >= lit(floor)
        band = when(cond, lit(name)) if band is None else band.when(cond, lit(name))
    return out.withColumn("triage_priority", band.otherwise(lit("low")))


_DISP_SCHEMA = (
    "alert_id STRING, alert_key STRING, l1_decision_date DATE, disposition STRING, "
    "queue_status STRING, case_id STRING, decision_date DATE, aging_days INT, "
    "sla_breached BOOLEAN, analyst_correct BOOLEAN, qa_sampled BOOLEAN, "
    "qa_disposition STRING, qa_disagrees BOOLEAN"
)
_CASE_SCHEMA = (
    "case_id STRING, customer_id BIGINT, case_type STRING, parent_case_id STRING, "
    "opened_date DATE, crr_tier STRING, priority STRING, alert_count INT, "
    "escalated_alert_count INT, rule_ids ARRAY<STRING>, first_alert_date DATE, "
    "activity_window_start DATE, activity_window_end DATE, case_status STRING, "
    "determination STRING, determination_date DATE, sar_decision STRING, "
    "suspect_identified BOOLEAN, filing_deadline_date DATE, filing_date DATE, "
    "determination_to_filing_days INT, filed_late BOOLEAN, alert_to_decision_days INT, "
    "sla_breached BOOLEAN, continuing_review_due_date DATE, "
    "continuing_review_case_id STRING, simulated_truth BOOLEAN"
)


def simulate(spark, inputs, params, as_of):
    """Replay every customer's alerts. Returns (dispositions, cases) frames."""
    import functools

    from pyspark.sql.functions import col, from_json, lit

    cust = inputs.where(col("is_customer")).select(
        "entity_id",
        "alert_id",
        "alert_key",
        "rule_id",
        "generated_date",
        "simulated_truth",
        "triage_priority",
        "priority_score",
        "crr_tier",
    )
    fn = functools.partial(_simulate_group, params=dict(params), as_of=as_of)
    from pyspark import StorageLevel

    rdd = cust.rdd.map(lambda r: (r[0], tuple(r[1:]))).groupByKey().flatMap(fn)
    tagged = spark.createDataFrame(rdd, "tag STRING, payload STRING")
    tagged = tagged.persist(StorageLevel.MEMORY_AND_DISK)
    disp = tagged.where(col("tag") == lit("d")).select(
        from_json(col("payload"), _DISP_SCHEMA).alias("r")
    )
    cases = tagged.where(col("tag") == lit("c")).select(
        from_json(col("payload"), _CASE_SCHEMA).alias("r")
    )
    return disp.select("r.*"), cases.select("r.*")


def case_activity(txns, cases):
    """Payments in each case's lookback window, both sides of the customer."""
    from pyspark.sql.functions import broadcast, col, count, lit, to_date
    from pyspark.sql.functions import sum as sum_

    win = cases.select("case_id", "customer_id", "activity_window_start", "activity_window_end")
    t = txns.select(
        "originator_id",
        "beneficiary_id",
        to_date(col("txn_timestamp")).alias("_d"),
        "txn_amount_usd",
        "uetr",
    )
    sides = []
    for side in ("originator_id", "beneficiary_id"):
        j = t.join(broadcast(win), t[side] == win["customer_id"]).where(
            (col("_d") >= col("activity_window_start")) & (col("_d") < col("activity_window_end"))
        )
        sides.append(j.select("case_id", "uetr", "txn_amount_usd"))
    both = sides[0].unionByName(sides[1]).dropDuplicates(["case_id", "uetr"])
    return both.groupBy("case_id").agg(
        count(lit(1)).alias("activity_txn_count"),
        sum_(col("txn_amount_usd")).cast("decimal(38,2)").alias("activity_amount_usd"),
    )


def coverage_rows(spark, inputs, status_rows, manifest, rule_targets):
    """Scenario-to-typology matrix: designed coverage, this cycle's rule
    status and alert volume, and planted typologies no scenario covers."""
    from pyspark.sql.functions import col, count, lit, when
    from pyspark.sql.functions import sum as sum_

    counts = {
        r["rule_id"]: (int(r["n"]), int(r["c"]))
        for r in inputs.groupBy("rule_id")
        .agg(
            count(lit(1)).alias("n"),
            sum_(when(col("is_customer"), lit(1)).otherwise(lit(0))).alias("c"),
        )
        .collect()
    }
    planted = {}
    if manifest is not None:
        planted = {
            r["typology_type"]: int(r["n"])
            for r in manifest.groupBy("typology_type").agg(count(lit(1)).alias("n")).collect()
        }
    status = {r["rule_id"]: r["status"] for r in status_rows}
    rows = []
    rules = list(rule_targets) + sorted(set(status) - set(rule_targets))
    for rid in rules:
        typ = rule_targets.get(rid)
        n, c = counts.get(rid, (0, 0))
        rows.append(
            (
                typ,
                rid,
                "designated" if typ else "attribute",
                status.get(rid, "not_configured"),
                n,
                c,
                planted.get(typ) if typ else None,
            )
        )
    covered = {t for t in rule_targets.values() if t}
    for typ in sorted(set(planted) - covered - {"random"}):
        rows.append((typ, None, "gap", None, None, None, planted[typ]))
    return rows


# ---------------------------------------------------------------------------
# Invariants (P10.2), evaluated on the written tables
# ---------------------------------------------------------------------------


def evaluate_invariants(counts: dict) -> list[tuple[str, str, str]]:
    """``counts`` from :func:`read_back_counts`. Returns (name, status, detail)."""
    out = []

    def check(name, ok, detail):
        out.append((name, "pass" if ok else "fail", detail))

    src = counts.get("source")
    mon, exc = counts["monitored"], counts["excluded"]
    if src is None:
        out.append(
            ("reconciliation", "fail", "source payment count unavailable; completeness unproven")
        )
    else:
        # A negative item (silver holding more than the source, so in_flight
        # below zero) is a duplication, not a balanced ledger.
        neg = counts.get("negative_items") or []
        check(
            "reconciliation",
            mon + exc == src and not neg,
            f"monitored {mon} + excluded {exc} = {mon + exc} vs source {src}"
            + (f"; negative: {', '.join(neg)}" if neg else ""),
        )
    alerts, disp = counts["alerts"], counts["dispositions"]
    check("every_alert_dispositioned", alerts == disp, f"alerts {alerts}, disposition rows {disp}")
    ca, esc = counts["customer_alerts"], counts["escalated"]
    check("escalated_le_alerts", esc <= ca, f"escalated {esc} <= customer alerts {ca}")
    ac = counts["alert_cases"]
    check("cases_le_escalated", ac <= esc, f"alert-driven cases {ac} <= escalated {esc}")
    sars, cases = counts["sars"], counts["cases"]
    check("sars_le_cases", sars <= cases, f"SARs {sars} <= cases {cases}")
    mo = counts["max_open_per_customer"]
    check("one_open_case_per_customer", mo <= 1, f"most open cases on one customer: {mo}")
    funnel = [
        ("payments", src if src is not None else counts["silver"]),
        ("monitored", mon),
        ("alerts", ca),
        ("escalated", esc),
        ("cases", ac),
        ("sars", counts["alert_sars"]),
    ]
    # No zip(strict=...): the Spark image runs Python 3.9.
    bad = [
        f"{funnel[i][0]}={funnel[i][1]} < {funnel[i + 1][0]}={funnel[i + 1][1]}"
        for i in range(len(funnel) - 1)
        if funnel[i + 1][1] > funnel[i][1]
    ]
    check(
        "funnel_monotone",
        not bad,
        "; ".join(bad) or " >= ".join(f"{k} {v}" for k, v in funnel),
    )
    miss = counts["reviews_missing"]
    check(
        "continuing_review_fires",
        miss == 0,
        f"{miss} SARs past their 90-day review date with no review ({counts['reviews_due']} due)",
    )
    return out


def read_back_counts(spark, cycle_run_id, base_run_id, cycle, as_of):
    """Counts the invariants need, from the tables as written."""
    from pyspark.sql.functions import col, count, lit, when
    from pyspark.sql.functions import max as max_
    from pyspark.sql.functions import sum as sum_

    def one(v):
        return int(v or 0)

    rec = {
        (r["section"], r["item"]): r["item_count"]
        for r in spark.table(f"{CATALOG}.{GOLD_RECON}")
        .where((col("run_id") == lit(base_run_id)) & (col("cycle") == lit(cycle)))
        .collect()
    }
    alerts = (
        spark.table(f"{CATALOG}.{GOLD_ALERTS}").where(col("run_id") == lit(cycle_run_id)).count()
    )
    d = (
        spark.table(f"{CATALOG}.{GOLD_DISPOSITIONS}")
        .where(col("run_id") == lit(cycle_run_id))
        .agg(
            count(lit(1)).alias("n"),
            sum_(when(col("is_customer"), lit(1)).otherwise(lit(0))).alias("cust"),
            sum_(when(col("disposition") == lit("escalated"), lit(1)).otherwise(lit(0))).alias(
                "esc"
            ),
        )
        .collect()[0]
    )
    cases = spark.table(f"{CATALOG}.{GOLD_CASES}").where(col("run_id") == lit(cycle_run_id))
    sar = col("sar_decision") == lit("sar_filed")
    alert_case = col("case_type") == lit("alert_escalation")
    c = cases.agg(
        count(lit(1)).alias("n"),
        sum_(when(alert_case, lit(1)).otherwise(lit(0))).alias("alert_cases"),
        sum_(when(sar, lit(1)).otherwise(lit(0))).alias("sars"),
        sum_(when(sar & alert_case, lit(1)).otherwise(lit(0))).alias("alert_sars"),
        sum_(
            when(sar & (col("continuing_review_due_date") <= lit(as_of)), lit(1)).otherwise(0)
        ).alias("due"),
        sum_(
            when(
                sar
                & (col("continuing_review_due_date") <= lit(as_of))
                & col("continuing_review_case_id").isNull(),
                lit(1),
            ).otherwise(lit(0))
        ).alias("missing"),
    ).collect()[0]
    mo = (
        cases.where(col("case_status") != lit("closed"))
        .groupBy("customer_id")
        .count()
        .agg(max_("count").alias("m"))
        .collect()[0]["m"]
    )
    src = rec.get(("completeness", "source"))
    negative = sorted(f"{s}.{i}={v}" for (s, i), v in rec.items() if v is not None and v < 0)
    return {
        "negative_items": negative,
        "source": None if src is None else int(src),
        "silver": one(rec.get(("completeness", "silver"))),
        "monitored": one(rec.get(("completeness", "monitored"))),
        "excluded": one(rec.get(("completeness", "excluded"))),
        "alerts": int(alerts),
        "dispositions": one(d["n"]),
        "customer_alerts": one(d["cust"]),
        "escalated": one(d["esc"]),
        "cases": one(c["n"]),
        "alert_cases": one(c["alert_cases"]),
        "sars": one(c["sars"]),
        "alert_sars": one(c["alert_sars"]),
        "max_open_per_customer": one(mo),
        "reviews_due": one(c["due"]),
        "reviews_missing": one(c["missing"]),
    }


# ---------------------------------------------------------------------------
# Operations summary (P10.3 / P10.4 vocabulary)
# ---------------------------------------------------------------------------


def ops_summary(spark, cycle_run_id, as_of, params, recon_rows, counts):
    from pyspark.sql.functions import col, count, lit, when
    from pyspark.sql.functions import sum as sum_

    d = spark.table(f"{CATALOG}.{GOLD_DISPOSITIONS}").where(col("run_id") == lit(cycle_run_id))
    cases = spark.table(f"{CATALOG}.{GOLD_CASES}").where(col("run_id") == lit(cycle_run_id))
    sar_cases = cases.where(col("sar_decision") == lit("sar_filed")).select(
        "case_id", lit(True).alias("_sar")
    )
    cust = d.where(col("is_customer")).join(sar_cases, "case_id", "left")
    decided = (
        col("disposition").isin("escalated", "closed_nfa", "attached")
        & col("decision_date").isNotNull()
    )
    productive = (col("disposition") == lit("escalated")) | col("_sar").isNotNull()
    per_rule = {}
    for r in (
        cust.groupBy("rule_id")
        .agg(
            count(lit(1)).alias("alerts"),
            sum_(when(decided, lit(1)).otherwise(lit(0))).alias("decided"),
            sum_(when(decided & productive, lit(1)).otherwise(lit(0))).alias("productive"),
            sum_(when(col("_sar").isNotNull(), lit(1)).otherwise(lit(0))).alias("sar"),
            sum_(when(col("disposition") == lit("escalated"), lit(1)).otherwise(lit(0))).alias(
                "escalated"
            ),
        )
        .collect()
    ):
        dec = int(r["decided"] or 0)
        per_rule[r["rule_id"]] = {
            "alerts": int(r["alerts"]),
            "escalated": int(r["escalated"] or 0),
            "productive_rate": (int(r["productive"] or 0) / dec) if dec else None,
            "sar_conversion": (int(r["sar"] or 0) / int(r["alerts"])) if r["alerts"] else None,
        }
    bucket = None
    for lo, hi, name in AGING_BUCKETS:
        cond = col("aging_days") >= lit(lo)
        if hi is not None:
            cond = cond & (col("aging_days") <= lit(hi))
        bucket = when(cond, lit(name)) if bucket is None else bucket.when(cond, lit(name))
    aging = {name: 0 for _, _, name in AGING_BUCKETS}
    for r in (
        d.where(col("queue_status") == lit("open"))
        .groupBy(bucket.otherwise(lit("0-30")).alias("b"))
        .count()
        .collect()
    ):
        aging[r["b"]] = int(r["count"])
    l1 = (
        cust.where(col("l1_decision_date").isNotNull())
        .agg(
            count(lit(1)).alias("n"),
            sum_(when(col("disposition") == lit("escalated"), lit(1)).otherwise(lit(0))).alias(
                "esc"
            ),
            sum_(when(col("qa_sampled"), lit(1)).otherwise(lit(0))).alias("qa"),
            sum_(when(col("qa_disagrees"), lit(1)).otherwise(lit(0))).alias("qa_dis"),
            sum_(when(col("sla_breached"), lit(1)).otherwise(lit(0))).alias("sla"),
        )
        .collect()[0]
    )
    by_priority = {
        r["triage_priority"]: int(r["count"])
        for r in cust.groupBy("triage_priority").count().collect()
    }
    filing = [
        int(r["determination_to_filing_days"])
        for r in cases.where(col("filing_date").isNotNull())
        .select("determination_to_filing_days")
        .collect()
    ]
    filing.sort()
    open_cases = cases.where(col("case_status") != lit("closed"))
    old_open = open_cases.where(col("opened_date") <= lit(as_of - timedelta(days=60))).count()
    by_status = {
        r["case_status"]: int(r["count"]) for r in cases.groupBy("case_status").count().collect()
    }
    by_type = {
        r["case_type"]: int(r["count"]) for r in cases.groupBy("case_type").count().collect()
    }
    n_l1 = int(l1["n"] or 0)
    n_qa = int(l1["qa"] or 0)

    def pct(xs, q):
        if not xs:
            return None
        return xs[min(len(xs) - 1, int(q * (len(xs) - 1) + 0.5))]

    return {
        "as_of_date": as_of.isoformat(),
        "simulation": {
            "note": "dispositions simulated from datagen ground truth",
            **{k: params[k] for k in sorted(params)},
        },
        "reconciliation": {f"{s}.{i}": v for s, i, _, v, _ in recon_rows},
        "funnel": {
            "payments": counts["source"],
            "monitored": counts["monitored"],
            "alerts": counts["customer_alerts"],
            "escalated": counts["escalated"],
            "cases": counts["alert_cases"],
            "sars": counts["alert_sars"],
        },
        "alerts_total": counts["alerts"],
        "alerts_out_of_scope": counts["alerts"] - counts["customer_alerts"],
        "l1_decided": n_l1,
        "l1_escalation_rate": (int(l1["esc"] or 0) / n_l1) if n_l1 else None,
        "alert_sla_breaches": int(l1["sla"] or 0),
        "qa_sample": n_qa,
        "qa_disagreement_rate": (int(l1["qa_dis"] or 0) / n_qa) if n_qa else None,
        "alerts_by_priority": by_priority,
        "alert_aging_open": aging,
        "cases_by_status": by_status,
        "cases_by_type": by_type,
        "open_cases": counts["cases"] - by_status.get("closed", 0),
        "open_cases_over_60_days": old_open,
        "sars_filed": counts["sars"],
        "filing_days_median": pct(filing, 0.5),
        "filing_days_p95": pct(filing, 0.95),
        "filed_over_30_days_pct": (
            (sum(1 for x in filing if x > FILING_DEADLINE_DAYS) / len(filing)) if filing else None
        ),
        "continuing_reviews_due": counts["reviews_due"],
        "scenarios": per_rule,
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _read_manifest(spark):
    try:
        return spark.table(f"{CATALOG}.{MANIFEST_TABLE}")
    except Exception as e:  # noqa: BLE001
        log(f"[tm] manifest {MANIFEST_TABLE} not readable: {one_line(e)}")
        return None


def _write_recon(spark, rows, base_run_id, cycle, cycle_run_id, as_of):
    from decimal import Decimal

    from pyspark.sql.functions import current_timestamp, lit

    schema = "section STRING, item STRING, unit STRING, item_count BIGINT, amount_usd DECIMAL(38,2)"
    data = [
        (
            s,
            i,
            u,
            int(c) if c is not None else None,
            Decimal(str(a)).quantize(Decimal("0.01")) if a is not None else None,
        )
        for s, i, u, c, a in rows
    ]
    df = spark.createDataFrame(data, schema).select(
        lit(base_run_id).alias("run_id"),
        lit(cycle).cast("int").alias("cycle"),
        lit(cycle_run_id).alias("cycle_run_id"),
        lit(as_of).cast("date").alias("as_of_date"),
        "section",
        "item",
        "unit",
        "item_count",
        "amount_usd",
        current_timestamp().alias("computed_ts"),
    )
    t = f"{CATALOG}.{GOLD_RECON}"
    # This run's history only; a rerun of the same cycle replaces its rows.
    spark.sql(f"DELETE FROM {t} WHERE run_id <> '{base_run_id}' OR cycle_run_id = '{cycle_run_id}'")
    df.writeTo(t).append()


def run_tm_operations(
    spark,
    txns,
    run_id,
    *,
    cycle=None,
    continuous=False,
    params=None,
    source_rows_fn=None,
    rule_targets=None,
):
    """Stages 1, 6, 7 and 8 for one cycle, then the invariant check.

    Never raises: a failure is logged as the ``workflow`` invariant with
    status ``error``, which the CLI gate treats as a failed run. Returns the
    list of (name, status, detail) invariants.
    """
    from pyspark.sql.functions import col, current_timestamp, lit
    from pyspark.sql.functions import max as max_

    params = dict(params or params_from_env())
    base_run_id, cycle_no = split_run_id(run_id, cycle)
    try:
        if rule_targets is None:
            from detection_rules import RULE_TARGET_TYPOLOGY

            rule_targets = dict(RULE_TARGET_TYPOLOGY)
        bootstrap_tm_tables(spark)
        newest = txns.agg(max_(col("txn_timestamp")).alias("m")).collect()[0]["m"]
        if newest is None:
            log("[tm] silver.transactions is empty; nothing to monitor this cycle")
            as_of = date.today()
        else:
            as_of = newest.date() + timedelta(days=1)
        entities = spark.table(f"{CATALOG}.{SILVER_ENTITIES}")
        manifest = _read_manifest(spark)
        if manifest is None:
            raise RuntimeError(
                f"no ground-truth manifest ({MANIFEST_TABLE}); dispositions cannot be simulated"
            )

        # Stage 1: completeness.
        source_rows = (source_rows_fn or count_source_rows)(spark)
        bronze_rows, _ = iceberg_table_stats(spark, f"{CATALOG}.{BRONZE_TABLE}")
        recon_rows, recon = reconcile(spark, txns, entities, source_rows, bronze_rows, continuous)

        # Stage 6-8: triage, cases, SAR decisions.
        alerts = spark.table(f"{CATALOG}.{GOLD_ALERTS}").where(col("run_id") == lit(run_id))
        inputs = build_alert_inputs(spark, alerts, entities, manifest).cache()
        disp_sim, cases_sim = simulate(spark, inputs, params, as_of)
        common = [lit(as_of).cast("date").alias("as_of_date"), lit(run_id).alias("run_id")]
        disp = inputs.join(disp_sim, ["alert_id", "alert_key"], "left").select(
            "alert_id",
            "alert_key",
            "rule_id",
            "entity_id",
            "is_customer",
            "crr_tier",
            "scenario_weight",
            "priority_score",
            "triage_priority",
            "generated_date",
            "l1_decision_date",
            # Alerts on a non-customer are outside the monitored
            # population: no customer, no case, not worked.
            _for_customer("disposition", "out_of_scope"),
            _for_customer("queue_status", "closed"),
            "case_id",
            _default_decision(),
            _default_int("aging_days"),
            _default_bool("sla_breached"),
            "simulated_truth",
            "analyst_correct",
            _default_bool("qa_sampled"),
            "qa_disposition",
            "qa_disagrees",
            *common,
            current_timestamp().alias("computed_ts"),
        )
        disp.writeTo(f"{CATALOG}.{GOLD_DISPOSITIONS}").overwrite(lit(True))
        cases_sim = cases_sim.cache()
        activity = case_activity(txns, cases_sim)
        cases = cases_sim.join(activity, "case_id", "left").select(
            *[
                col(c)
                for c in (
                    "case_id",
                    "customer_id",
                    "case_type",
                    "parent_case_id",
                    "opened_date",
                    "crr_tier",
                    "priority",
                    "alert_count",
                    "escalated_alert_count",
                    "rule_ids",
                    "first_alert_date",
                    "activity_window_start",
                    "activity_window_end",
                )
            ],
            _default_long("activity_txn_count"),
            col("activity_amount_usd"),
            *[
                col(c)
                for c in (
                    "case_status",
                    "determination",
                    "determination_date",
                    "sar_decision",
                    "suspect_identified",
                    "filing_deadline_date",
                    "filing_date",
                    "determination_to_filing_days",
                    "filed_late",
                    "alert_to_decision_days",
                    "sla_breached",
                    "continuing_review_due_date",
                    "continuing_review_case_id",
                    "simulated_truth",
                )
            ],
            *common,
            current_timestamp().alias("computed_ts"),
        )
        cases.writeTo(f"{CATALOG}.{GOLD_CASES}").overwrite(lit(True))

        # Scenario coverage matrix.
        try:
            status_rows = [
                r.asDict()
                for r in spark.table(f"{CATALOG}.{GOLD_STATUS}")
                .where(col("run_id") == lit(run_id))
                .collect()
            ]
        except Exception as e:  # noqa: BLE001
            log(f"[tm] detection_status not readable: {one_line(e)}")
            status_rows = []
        cov = coverage_rows(spark, inputs, status_rows, manifest, rule_targets)
        spark.createDataFrame(
            cov,
            "typology STRING, rule_id STRING, coverage STRING, rule_status STRING, "
            "alert_count BIGINT, customer_alert_count BIGINT, planted_instances BIGINT",
        ).select(
            "*", lit(run_id).alias("run_id"), current_timestamp().alias("computed_ts")
        ).writeTo(f"{CATALOG}.{GOLD_COVERAGE}").overwrite(lit(True))

        # Funnel rows complete the cycle ledger once cases exist.
        funnel_counts = read_back_counts_partial(spark, run_id, as_of)
        recon_rows = recon_rows + [
            ("funnel", "alerts", "alerts", funnel_counts["customer_alerts"], None),
            ("funnel", "alerts_out_of_scope", "alerts", funnel_counts["out_of_scope"], None),
            ("funnel", "escalated", "alerts", funnel_counts["escalated"], None),
            ("funnel", "cases", "cases", funnel_counts["alert_cases"], None),
            ("funnel", "continuing_review_cases", "cases", funnel_counts["review_cases"], None),
            ("funnel", "sars", "sars", funnel_counts["alert_sars"], None),
            ("funnel", "continuing_sars", "sars", funnel_counts["review_sars"], None),
        ]
        _write_recon(spark, recon_rows, base_run_id, cycle_no, run_id, as_of)
        inputs.unpersist()
        cases_sim.unpersist()

        counts = read_back_counts(spark, run_id, base_run_id, cycle_no, as_of)
        invariants = evaluate_invariants(counts)
        summary = ops_summary(spark, run_id, as_of, params, recon_rows, counts)
    except Exception as e:  # noqa: BLE001 -- reported through the gate
        err = one_line(f"{type(e).__name__}: {e}", limit=400)
        log(f"[tm-invariant] workflow: status=error cycle={cycle_no} detail={err}")
        return [("workflow", "error", err)]
    finally:
        spark.catalog.clearCache()

    for name, status, detail in invariants:
        log(f"[tm-invariant] {name}: status={status} cycle={cycle_no} detail={detail}")
    summary["cycle"] = cycle_no
    summary["invariants"] = {n: {"status": s, "detail": d} for n, s, d in invariants}
    log("[tm-ops] " + json.dumps(summary, sort_keys=True, default=str))
    return invariants


def read_back_counts_partial(spark, cycle_run_id, as_of):
    """Funnel counts for the reconciliation rows, from the written tables."""
    from pyspark.sql.functions import col, count, lit, when
    from pyspark.sql.functions import sum as sum_

    def flag(c):
        return sum_(when(c, lit(1)).otherwise(lit(0)))

    d = (
        spark.table(f"{CATALOG}.{GOLD_DISPOSITIONS}")
        .where(col("run_id") == lit(cycle_run_id))
        .agg(
            flag(col("is_customer")).alias("cust"),
            flag(~col("is_customer")).alias("oos"),
            flag(col("disposition") == lit("escalated")).alias("esc"),
        )
        .collect()[0]
    )
    sar = col("sar_decision") == lit("sar_filed")
    alert_case = col("case_type") == lit("alert_escalation")
    c = (
        spark.table(f"{CATALOG}.{GOLD_CASES}")
        .where(col("run_id") == lit(cycle_run_id))
        .agg(
            count(lit(1)).alias("n"),
            flag(alert_case).alias("ac"),
            flag(sar & alert_case).alias("asar"),
            flag(sar & ~alert_case).alias("rsar"),
        )
        .collect()[0]
    )
    n = int(c["n"] or 0)
    ac = int(c["ac"] or 0)
    return {
        "customer_alerts": int(d["cust"] or 0),
        "out_of_scope": int(d["oos"] or 0),
        "escalated": int(d["esc"] or 0),
        "alert_cases": ac,
        "review_cases": n - ac,
        "alert_sars": int(c["asar"] or 0),
        "review_sars": int(c["rsar"] or 0),
    }


def _for_customer(name, fallback):
    from pyspark.sql.functions import col, lit, when

    return when(col("is_customer"), col(name)).otherwise(lit(fallback)).alias(name)


def _default_decision():
    from pyspark.sql.functions import col, when

    return (
        when(col("is_customer"), col("decision_date"))
        .otherwise(col("generated_date"))
        .alias("decision_date")
    )


def _default_int(name):
    from pyspark.sql.functions import coalesce, col, lit

    return coalesce(col(name), lit(0)).cast("int").alias(name)


def _default_long(name):
    from pyspark.sql.functions import coalesce, col, lit

    return coalesce(col(name), lit(0)).cast("bigint").alias(name)


def _default_bool(name):
    from pyspark.sql.functions import coalesce, col, lit

    return coalesce(col(name), lit(False)).alias(name)
