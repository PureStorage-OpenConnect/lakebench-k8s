"""Customer 360 expected results: what a correct batch run must produce.

The Spark side (``spark/scripts/common.py``: ``log_c360_check``) logs one
``[c360-check] {json}`` line of silver and gold facts after gold-finalize;
bronze-verify logs ``[c360-bronze] rows=N silver_filter_rows=M``. This
module compares those facts, and the benchmark row counts, with what the
generator (``datagen_rs/src/customer360.rs`` and ``customer360_realism.rs``)
defines.

Three kinds of check:

- ``invariant`` and ``reconcile``: exact. A correct run cannot fail them.
- ``statistical``: a value drawn from the generator's distributions, bounded
  at 6 standard errors (8 on the heavy right tail of transaction value), so
  a correct run fails one by chance with probability well under 1e-6.
- ``shape``: the row count a benchmark query must return on a correct corpus.

REPORTING ONLY (owner decision D6): the verdict is recorded in the run's
metrics and printed, and never changes the run's success until the owner
approves what each check means. The definitions for that approval are in
the lane's expected-results table. ``GATING_CHECKS`` is the switch: the ids
it names fail the run (``gating_problems``, read by ``cli/_run.py``); it is
empty until the owner approves.
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, timedelta
from typing import Any

# D6: the owner approves the meaning before any check gates a run. Add a
# check id here to make its failure fail the run; empty means reporting only.
GATING_CHECKS: frozenset[str] = frozenset()
GATING = bool(GATING_CHECKS)

# A verdict is "pass" only when these ran and passed; without them nothing
# says the corpus reached gold intact.
CORE_CHECKS = (
    "bronze_rows_match_datagen",
    "bronze_to_silver_rows",
    "silver_to_gold_days",
    "silver_to_gold_counts",
    "gold_daily_identities",
)

CHECK_TAG = "[c360-check]"
_CHECK_RE = re.compile(r"\[c360-check\]\s+(?P<json>\{.*\})\s*$", re.MULTILINE)
_BRONZE_RE = re.compile(
    r"\[c360-bronze\]\s+rows=(?P<rows>\d+)\s+silver_filter_rows=(?P<kept>\d+)\s*$",
    re.MULTILINE,
)

# ---------------------------------------------------------------------------
# Generator semantics (datagen_rs). Change these only with the generator.
# ---------------------------------------------------------------------------

# customer360_realism.rs INTERACTION_TYPES / INTERACTION_WEIGHTS.
INTERACTION_WEIGHTS = {
    "purchase": 0.18,
    "browse": 0.35,
    "support": 0.12,
    "login": 0.20,
    "abandoned_cart": 0.15,
}
# DATA_QUALITY_WEIGHTS: "duplicate_suspected" is the row silver drops.
DUPLICATE_FLAG_SHARE = 0.02
# customer360.rs: purchase amount = clamp(exp(N(4.3, 1.2)), 1, 9999.99),
# rounded to cents; 0.0 on every other row.
AMOUNT_LOG_MU = 4.3
AMOUNT_LOG_SIGMA = 1.2
AMOUNT_MIN = 1.0
AMOUNT_MAX = 9999.99
# page_views = 1 + below(20) on purchase/browse, else 0.
PAGE_VIEWS_MEAN = 10.5
PAGE_VIEWS_VAR = (20**2 - 1) / 12.0
# time_on_site = 30 + below(3600) when page_views > 0, else 0.
TIME_ON_SITE_MEAN = 30 + 3599 / 2.0
TIME_ON_SITE_VAR = (3600**2 - 1) / 12.0
# satisfaction = 1 + below(5) on support rows, else NULL.
SATISFACTION_MEAN = 3.0
SATISFACTION_VAR = 2.0
# churn_risk_indicator: high_risk when satisfaction <= 2, medium_risk when 3.
HIGH_CHURN_SHARE = 0.4
MEDIUM_CHURN_SHARE = 0.2
# Sessions: 5 + below(16) rows, one customer each (CustomerIdSampler::new:
# 500 hot ids get 40% of sessions with Zipf(1.2) weights, the rest uniform).
SESSION_MEAN_ROWS = 12.5
HOT_CUSTOMERS = 500
HOT_SHARE = 0.40
HOT_ZIPF = 1.2
# datagen_rs bin/generate.rs default window when the config sets none; a
# multi-cycle run splits deploy/datagen.py's window, whose default end is
# 2025-12-31.
DEFAULT_WINDOW_START = "2024-01-01"
DEFAULT_WINDOW_END = "2025-01-01"
DEFAULT_MULTI_CYCLE_WINDOW_END = "2025-12-31"
# writer.rs customer360_bytes_per_row_default per DG_COMPRESSION codec; the
# lakebench datagen Job sets none, so snappy.
BYTES_PER_ROW = {"snappy": 4332.0, "zstd": 2233.0, "lz4": 4356.0, "none": 4399.0}

Z = 6.0  # standard errors for a statistical bound
Z_RIGHT_TAIL = 8.0  # right tail of a mean of lognormal amounts (skewed)
# Per-day upper bound: one test per day, n as low as MIN_DAY_TRANSACTIONS,
# where P(z > 8) is about 1e-6 a day; 10 keeps a 730-day run under 1e-6.
Z_RIGHT_TAIL_DAILY = 10.0
MIN_DAY_TRANSACTIONS = 200  # per-day transaction-value bound below this is noise
DENSE_SESSIONS_PER_DAY = 30.0  # every day has data with P(miss) < 1e-13 per day


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_c360_check(logs: str | None) -> dict[str, Any] | None:
    """The last ``[c360-check]`` facts in a gold-finalize driver log."""
    last = None
    for m in _CHECK_RE.finditer(logs or ""):
        try:
            last = json.loads(m.group("json"))
        except ValueError:
            continue
    return last


def parse_c360_bronze(logs: str | None) -> dict[str, int] | None:
    """``{"rows", "silver_filter_rows"}`` from a bronze-verify driver log."""
    last = None
    for m in _BRONZE_RE.finditer(logs or ""):
        last = {"rows": int(m.group("rows")), "silver_filter_rows": int(m.group("kept"))}
    return last


# ---------------------------------------------------------------------------
# Expected context from the config
# ---------------------------------------------------------------------------


def expected_context(cfg) -> dict[str, Any]:
    """What the checks need from the config: window, customers, scale."""
    workload = cfg.architecture.workload
    dg = workload.datagen
    cycles = int(getattr(cfg.architecture.pipeline, "cycles", 1) or 1)
    start = dg.timestamp_start or DEFAULT_WINDOW_START
    end = dg.timestamp_end or (DEFAULT_MULTI_CYCLE_WINDOW_END if cycles > 1 else DEFAULT_WINDOW_END)
    dims = cfg.get_scale_dimensions()
    file_size_mb = _size_bytes(dg.file_size) // (1024 * 1024)
    return {
        "window_start": str(start)[:10],
        "window_end": str(end)[:10],
        "customers": int(dims.customers),
        "scale": float(dg.get_effective_scale()),
        "cycles": cycles,
        "bronze_rows_expected": {
            codec: cycles * datagen_rows(dims.approx_bronze_gb / cycles, file_size_mb, bpr)
            for codec, bpr in BYTES_PER_ROW.items()
        },
    }


def _size_bytes(size: str) -> int:
    """deploy/datagen.py ``_parse_size_to_bytes``: "64mb" -> bytes."""
    t = str(size).strip().upper()
    for suffix, mult in (("TB", 1024**4), ("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if t.endswith(suffix):
            return int(float(t[: -len(suffix)]) * mult)
    return int(t.rstrip("B") or 0)


def datagen_rows(bronze_gb: float, file_size_mb: int, bytes_per_row: float) -> int:
    """Rows one c360 datagen Job writes (datagen_rs bin/generate.rs sizing).

    ``--target-tb`` is rendered with six decimals (deploy/datagen.py);
    ``total_files = max(1, target_bytes // file_size)`` and
    ``rows_per_file = max(1000, file_size / bytes_per_row)``, both truncated.
    """
    target_tb = float(f"{bronze_gb / 1024.0:.6f}")
    file_size = int(file_size_mb) * 1024 * 1024
    if file_size <= 0:
        return 0
    target_bytes = int(target_tb * 1024.0 * 1024.0 * 1024.0 * 1024.0)
    total_files = max(1, target_bytes // file_size)
    rows_per_file = int(max(file_size / bytes_per_row, 1000.0))
    return total_files * rows_per_file


# ---------------------------------------------------------------------------
# Distribution helpers
# ---------------------------------------------------------------------------


def _phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def transaction_value_moments() -> tuple[float, float]:
    """Mean and standard deviation of one purchase amount (clamped lognormal)."""
    mu, s = AMOUNT_LOG_MU, AMOUNT_LOG_SIGMA
    la, lb = math.log(AMOUNT_MIN), math.log(AMOUNT_MAX)

    def partial(k: int) -> float:  # E[X^k; a <= X <= b]
        return math.exp(k * mu + k * k * s * s / 2.0) * (
            _phi((lb - mu - k * s * s) / s) - _phi((la - mu - k * s * s) / s)
        )

    p_lo = _phi((la - mu) / s)
    p_hi = 1.0 - _phi((lb - mu) / s)
    m1 = p_lo * AMOUNT_MIN + partial(1) + p_hi * AMOUNT_MAX
    m2 = p_lo * AMOUNT_MIN**2 + partial(2) + p_hi * AMOUNT_MAX**2
    # Rounding to cents adds a uniform +-0.005 error: variance 1/120000.
    return m1, math.sqrt(max(m2 - m1 * m1, 0.0) + 1.0 / 120000.0)


def hot_weights(customers: int) -> list[float]:
    """CustomerIdSampler hot-id weights (normalised Zipf over the hot ids)."""
    hot = min(HOT_CUSTOMERS, max(customers, 1))
    w = [1.0 / (k**HOT_ZIPF) for k in range(1, hot + 1)]
    t = sum(w)
    return [x / t for x in w]


def expected_distinct_customers(customers: int, sessions: float) -> float:
    """Expected distinct customer ids across ``sessions`` sessions."""
    w = hot_weights(customers)
    hot = len(w)
    retail = customers - hot
    if retail <= 0:
        # CustomerIdSampler: uniform over the whole id space off the hot path.
        per_id = [HOT_SHARE * wk + (1.0 - HOT_SHARE) / customers for wk in w]
        return sum(1.0 - (1.0 - p) ** sessions for p in per_id)
    hot_part = sum(1.0 - (1.0 - HOT_SHARE * wk) ** sessions for wk in w)
    retail_part = retail * (1.0 - (1.0 - (1.0 - HOT_SHARE) / retail) ** sessions)
    return hot_part + retail_part


# ---------------------------------------------------------------------------
# Check records
# ---------------------------------------------------------------------------


def _check(
    cid: str,
    kind: str,
    passed: bool | None,
    observed: Any,
    expected: Any,
    tolerance: Any = 0,
    detail: str = "",
) -> dict[str, Any]:
    status = "unchecked" if passed is None else ("pass" if passed else "fail")
    return {
        "id": cid,
        "kind": kind,
        "status": status,
        "observed": observed,
        "expected": expected,
        "tolerance": tolerance,
        "detail": detail,
    }


def _mean_bound(
    cid: str,
    observed: float | None,
    n: int,
    mean: float,
    var: float,
    rounding: float = 0.0,
    z_hi: float = Z,
    detail: str = "",
) -> dict[str, Any]:
    if observed is None or n <= 0:
        return _check(cid, "statistical", None, observed, mean, None, "no rows to average")
    se = math.sqrt(var / n)
    lo = mean - Z * se - rounding
    hi = mean + z_hi * se + rounding
    return _check(
        cid,
        "statistical",
        lo <= observed <= hi,
        round(observed, 4),
        round(mean, 4),
        [round(lo, 4), round(hi, 4)],
        detail or f"n={n}",
    )


def _share_bound(cid: str, k: int, n: int, p: float, detail: str = "") -> dict[str, Any]:
    if n <= 0:
        return _check(cid, "statistical", None, None, p, None, "no rows")
    se = math.sqrt(p * (1.0 - p) / n)
    obs = k / n
    return _check(
        cid,
        "statistical",
        abs(obs - p) <= Z * se,
        round(obs, 6),
        p,
        round(Z * se, 6),
        detail or f"{k} of {n}",
    )


def _weighted(days: list[list[Any]], value_i: int, weight_i: int) -> tuple[float | None, int]:
    num = 0.0
    n = 0
    for d in days:
        v, w = d[value_i], d[weight_i]
        if v is None or not w:
            continue
        num += float(v) * int(w)
        n += int(w)
    return (num / n if n else None), n


def _add_months(d: date, months: int) -> date:
    """Trino date_add('month', n, d): same day, clamped to the month's end."""
    y, m = divmod(d.month - 1 + months, 12)
    y, m = d.year + y, m + 1
    nxt = date(y + (m == 12), m % 12 + 1, 1)
    return date(y, m, min(d.day, (nxt - timedelta(days=1)).day))


# ---------------------------------------------------------------------------
# Pipeline checks
# ---------------------------------------------------------------------------

# gold sum column -> silver fact it must equal exactly.
_COUNT_RECONCILE = {
    "total_transactions": "transaction_rows",
    "conversions": "purchase_rows",
    "awareness_interactions": "browse_rows",
    "consideration_interactions": "abandoned_cart_rows",
    "retention_interactions": "support_rows",
    "support_tickets_created": "ticket_rows",
    "high_churn_risk_count": "high_churn_rows",
    "medium_churn_risk_count": "medium_churn_rows",
    "total_points_earned": "points_earned",
}


def pipeline_checks(
    facts: dict[str, Any], bronze: dict[str, int] | None, ctx: dict[str, Any]
) -> list[dict[str, Any]]:
    """Every pipeline check for one run's facts."""
    s = facts.get("silver") or {}
    g = facts.get("gold") or {}
    out: list[dict[str, Any]] = []
    rows = int(s.get("rows") or 0)

    # -- silver invariants (exact) ----------------------------------------
    out.append(
        _check(
            "silver_duplicate_filter_applied",
            "invariant",
            s.get("duplicate_flag_rows") == 0,
            s.get("duplicate_flag_rows"),
            0,
        )
    )
    out.append(
        _check(
            "amount_only_on_purchases",
            "invariant",
            s.get("non_purchase_amount_rows") == 0
            and s.get("transaction_rows") == s.get("purchase_rows"),
            {
                "non_purchase_amount_rows": s.get("non_purchase_amount_rows"),
                "transaction_rows": s.get("transaction_rows"),
                "purchase_rows": s.get("purchase_rows"),
            },
            "0 non-purchase amounts; transactions == purchases",
        )
    )
    amin, amax = s.get("purchase_amount_min"), s.get("purchase_amount_max")
    out.append(
        _check(
            "purchase_amount_range",
            "invariant",
            None
            if amin is None
            else (amin >= AMOUNT_MIN - 1e-9 and amax is not None and amax <= AMOUNT_MAX + 1e-9),
            [amin, amax],
            [AMOUNT_MIN, AMOUNT_MAX],
        )
    )
    nulls = {k: s.get(k) for k in ("null_customer_rows", "null_date_rows", "null_amount_rows")}
    out.append(
        _check("silver_no_null_keys", "invariant", all(v == 0 for v in nulls.values()), nulls, 0)
    )
    customers = int(ctx.get("customers") or 0)
    cmin, cmax = s.get("customer_id_min"), s.get("customer_id_max")
    out.append(
        _check(
            "customer_ids_in_id_space",
            "invariant",
            None
            if cmin is None or cmax is None or not customers
            else (int(cmin) >= 1 and int(cmax) <= customers),
            [cmin, cmax],
            [1, customers],
        )
    )
    ws = date.fromisoformat(ctx["window_start"])
    we = date.fromisoformat(ctx["window_end"])
    last_day = we - timedelta(days=1)
    dmin, dmax = s.get("date_min"), s.get("date_max")
    out.append(
        _check(
            "dates_in_window",
            "invariant",
            None
            if dmin is None or dmax is None
            else (
                date.fromisoformat(str(dmin)) >= ws and date.fromisoformat(str(dmax)) <= last_day
            ),
            [dmin, dmax],
            [ws.isoformat(), last_day.isoformat()],
        )
    )
    out.append(
        _check(
            "one_ticket_and_score_per_support",
            "invariant",
            s.get("ticket_rows") == s.get("support_rows") == s.get("satisfaction_rows"),
            {
                "ticket_rows": s.get("ticket_rows"),
                "satisfaction_rows": s.get("satisfaction_rows"),
                "support_rows": s.get("support_rows"),
            },
            "all equal",
        )
    )

    # -- corpus size: bronze holds exactly what datagen was asked to write ---
    expected_rows = ctx.get("bronze_rows_expected") or {}
    if bronze and expected_rows:
        match = [c for c, n in expected_rows.items() if n == bronze["rows"]]
        out.append(
            _check(
                "bronze_rows_match_datagen",
                "reconcile",
                bool(match),
                bronze["rows"],
                expected_rows.get("snappy"),
                0,
                f"codec {match[0]}" if match else "no datagen codec gives this row count",
            )
        )
    else:
        out.append(
            _check(
                "bronze_rows_match_datagen",
                "reconcile",
                None,
                bronze["rows"] if bronze else None,
                expected_rows.get("snappy"),
                0,
                "no [c360-bronze] line or no datagen sizing",
            )
        )

    # -- bronze -> silver -> gold reconciliation (exact) ---------------------
    if bronze:
        out.append(
            _check(
                "bronze_to_silver_rows",
                "reconcile",
                rows == bronze["silver_filter_rows"],
                rows,
                bronze["silver_filter_rows"],
                0,
                "silver rows == bronze rows passing the quality filter",
            )
        )
    else:
        out.append(
            _check(
                "bronze_to_silver_rows",
                "reconcile",
                None,
                rows,
                None,
                0,
                "no [c360-bronze] line from bronze-verify",
            )
        )
    out.append(
        _check(
            "silver_to_gold_days",
            "reconcile",
            g.get("rows") == g.get("distinct_dates") == s.get("distinct_dates")
            and g.get("null_dates") == 0,
            {"gold_rows": g.get("rows"), "gold_distinct_dates": g.get("distinct_dates")},
            s.get("distinct_dates"),
            0,
            "one gold row per silver date, no duplicate or NULL dates",
        )
    )
    sums = g.get("sums") or {}
    mism = {}
    for gc, sc in _COUNT_RECONCILE.items():
        gv, sv = sums.get(gc), s.get(sc)
        if gv is None or sv is None or int(round(gv)) != int(sv):
            mism[gc] = [gv, sv]
    out.append(
        _check(
            "silver_to_gold_counts",
            "reconcile",
            not mism,
            mism or "all equal",
            "sum over gold days == silver count",
        )
    )
    grev, srev = sums.get("total_daily_revenue"), s.get("revenue")
    if grev is None or srev is None:
        out.append(_check("silver_to_gold_revenue", "reconcile", None, grev, srev))
    else:
        tol = 0.005 * int(g.get("rows") or 0) + 1e-9 * abs(srev) + 0.01
        out.append(
            _check(
                "silver_to_gold_revenue",
                "reconcile",
                abs(grev - srev) <= tol,
                round(grev, 2),
                round(srev, 2),
                round(tol, 2),
                "per-day revenue is rounded to cents",
            )
        )

    # -- gold invariants (exact) ---------------------------------------------
    bad = {"negative": g.get("negative_values") or {}, "null": g.get("null_values") or {}}
    out.append(
        _check(
            "gold_counts_non_negative",
            "invariant",
            not bad["negative"] and not bad["null"],
            bad,
            "no negative or NULL count/amount",
        )
    )
    viol = {k: v for k, v in (g.get("violations") or {}).items() if v}
    out.append(
        _check(
            "gold_daily_identities",
            "invariant",
            not viol,
            {"violations": viol, "first_day": g.get("violation_examples") or {}},
            "every day consistent",
        )
    )
    out.append(
        _check(
            "daily_active_within_customers",
            "invariant",
            None if not customers else int(g.get("max_daily_active_customers") or 0) <= customers,
            g.get("max_daily_active_customers"),
            customers,
        )
    )

    # -- statistical (generator distributions) -------------------------------
    if bronze and bronze["rows"] > 0:
        out.append(
            _share_bound(
                "duplicate_filter_share",
                bronze["rows"] - bronze["silver_filter_rows"],
                bronze["rows"],
                DUPLICATE_FLAG_SHARE,
            )
        )
    mix = [
        _share_bound(it, int(s.get(f"{it}_rows") or 0), rows, p)
        for it, p in INTERACTION_WEIGHTS.items()
    ]
    out.append(
        _check(
            "interaction_mix",
            "statistical",
            None if rows <= 0 else all(c["status"] == "pass" for c in mix),
            {c["id"]: c["observed"] for c in mix},
            dict(INTERACTION_WEIGHTS),
            {c["id"]: c["tolerance"] for c in mix},
            "share of silver rows per interaction type",
        )
    )

    mean_tv, sd_tv = transaction_value_moments()
    tx = int(sums.get("total_transactions") or 0)
    out.append(
        _mean_bound(
            "avg_transaction_value_overall",
            (grev / tx) if (grev is not None and tx) else None,
            tx,
            mean_tv,
            sd_tv**2,
            z_hi=Z_RIGHT_TAIL,
            detail=f"sum(total_daily_revenue) / sum(total_transactions), n={tx}",
        )
    )
    days = g.get("days") or []
    checked = 0
    bad_days = []
    for d in days:
        n_d, atv = int(d[1] or 0), d[2]
        if n_d < MIN_DAY_TRANSACTIONS or atv is None:
            continue
        checked += 1
        se = sd_tv / math.sqrt(n_d)
        if not (mean_tv - Z * se - 0.005 <= atv <= mean_tv + Z_RIGHT_TAIL_DAILY * se + 0.005):
            bad_days.append([d[0], atv, n_d])
    out.append(
        _check(
            "avg_transaction_value_daily",
            "statistical",
            None if checked == 0 else not bad_days,
            {"days_checked": checked, "days_out_of_range": len(bad_days), "first": bad_days[:3]},
            round(mean_tv, 2),
            f"-{Z:g}/+{Z_RIGHT_TAIL_DAILY:g} standard errors per day",
            f"days with >= {MIN_DAY_TRANSACTIONS} transactions",
        )
    )
    pv, n_v = _weighted(days, 4, 3)
    out.append(
        _mean_bound("avg_page_views_per_visit", pv, n_v, PAGE_VIEWS_MEAN, PAGE_VIEWS_VAR, 0.05)
    )
    tos, n_v2 = _weighted(days, 5, 3)
    out.append(
        _mean_bound(
            "avg_time_on_site_per_visit", tos, n_v2, TIME_ON_SITE_MEAN, TIME_ON_SITE_VAR, 0.5
        )
    )
    sat, n_s = _weighted(days, 7, 6)
    out.append(
        _mean_bound("avg_satisfaction_score", sat, n_s, SATISFACTION_MEAN, SATISFACTION_VAR, 0.005)
    )
    sup = int(sums.get("retention_interactions") or 0)
    out.append(
        _share_bound(
            "high_churn_share", int(sums.get("high_churn_risk_count") or 0), sup, HIGH_CHURN_SHARE
        )
    )
    out.append(
        _share_bound(
            "medium_churn_share",
            int(sums.get("medium_churn_risk_count") or 0),
            sup,
            MEDIUM_CHURN_SHARE,
        )
    )

    # Distinct customers from the session count the rows imply.
    bronze_rows = bronze["rows"] if bronze else None
    if customers and (bronze_rows or rows):
        n_rows = bronze_rows if bronze_rows else rows / (1.0 - DUPLICATE_FLAG_SHARE)
        sessions = n_rows / SESSION_MEAN_ROWS
        exp_c = expected_distinct_customers(customers, sessions)
        obs_c = s.get("distinct_customers")
        tol = 0.03 * exp_c + Z * math.sqrt(max(exp_c, 1.0))
        out.append(
            _check(
                "distinct_customers",
                "statistical",
                None if obs_c is None else abs(obs_c - exp_c) <= tol,
                obs_c,
                round(exp_c),
                round(tol),
                f"{customers} ids, about {sessions:,.0f} sessions",
            )
        )

    # Every calendar day in the window has data once sessions are dense.
    window_days = (we - ws).days
    sessions_per_day = (
        ((bronze_rows or rows) / SESSION_MEAN_ROWS) / window_days if window_days > 0 else 0.0
    )
    if sessions_per_day >= DENSE_SESSIONS_PER_DAY:
        out.append(
            _check(
                "gold_days_cover_window",
                "statistical",
                g.get("rows") == window_days,
                g.get("rows"),
                window_days,
                0,
                f"about {sessions_per_day:,.0f} sessions a day; a missing day is a lost day",
            )
        )
    else:
        out.append(
            _check(
                "gold_days_cover_window",
                "statistical",
                None if g.get("rows") is None else int(g["rows"]) <= window_days,
                g.get("rows"),
                f"<= {window_days}",
                0,
                "too few sessions a day to require every day",
            )
        )
    return out


# ---------------------------------------------------------------------------
# Benchmark shape checks
# ---------------------------------------------------------------------------


def benchmark_checks(
    queries: list[Any], facts: dict[str, Any] | None, ctx: dict[str, Any]
) -> list[dict[str, Any]]:
    """Row counts the c360 queries must return on a correct corpus.

    ``queries`` are QueryResult objects or their ``to_dict()`` form. Only
    successful queries are checked; a failed query is the benchmark gate's.
    """
    s = (facts or {}).get("silver") or {}
    g = (facts or {}).get("gold") or {}
    gold_days = g.get("rows")
    days = [d[0] for d in (g.get("days") or []) if d[0]]
    tx = int(s.get("transaction_rows") or 0)
    support = int(s.get("support_rows") or 0)
    rows = int(s.get("rows") or 0)
    per_day_rows = rows / len(days) if days else 0.0

    def expected_for(name: str) -> tuple[Any, str]:
        if name.startswith("Q1_"):
            return 1, "one aggregate row"
        if name.startswith("Q7_"):
            return 5, "one row per channel (5 channels)"
        if name.startswith("Q3_"):
            if tx >= 5000:
                return 12, "3 value tiers x 4 channel preferences"
            return None, "too few transactions to require all 12 cells"
        if name.startswith("Q4_"):
            if support >= 5000:
                return 6, "2 churn risks x retention stage x 3 device categories"
            return None, "too few support rows to require all 6 cells"
        if name.startswith("Q5_"):
            return (None, "no gold days") if gold_days is None else (min(90, gold_days), "")
        if name.startswith("Q9_"):
            return (None, "no gold days") if gold_days is None else (min(30, gold_days), "")
        if name.startswith("Q2_"):
            # Every type on every day needs ~30 of the rarest (support, 12%).
            if not days or per_day_rows * min(INTERACTION_WEIGHTS.values()) < 30:
                return None, "too few rows a day to require every type"
            d0 = date.fromisoformat(min(days))
            d3 = _add_months(d0, 3)
            n = sum(1 for d in days if d0 <= date.fromisoformat(d) < d3)
            return 5 * n, f"{n} days x 5 interaction types"
        if name.startswith("Q6_"):
            return "1..6", "RFM segments present"
        return None, "no expected row count"

    out = []
    for q in queries or []:
        name = q["name"] if isinstance(q, dict) else q.query.name
        ok = q["success"] if isinstance(q, dict) else q.success
        got = q["rows_returned"] if isinstance(q, dict) else q.rows_returned
        exp, why = expected_for(name)
        cid = f"benchmark_rows_{name.split('_')[0]}"
        if not ok:
            out.append(_check(cid, "shape", None, got, exp, 0, "query failed"))
        elif exp == "1..6":
            out.append(_check(cid, "shape", 1 <= int(got) <= 6, got, exp, 0, why))
        elif exp is None:
            out.append(_check(cid, "shape", None, got, None, 0, why))
        else:
            out.append(_check(cid, "shape", int(got) == int(exp), got, exp, 0, why))
    return out


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------


def verdict(checks: list[dict[str, Any]], reason: str = "") -> dict[str, Any]:
    """Summarise checks.

    ``fail`` when any check failed. ``pass`` only when every core check
    (``CORE_CHECKS``) ran and passed; otherwise ``unknown``, so a run whose
    facts are missing cannot pass on the few benchmark shapes that need none.
    """
    by_id = {c["id"]: c["status"] for c in checks}
    failed = [c["id"] for c in checks if c["status"] == "fail"]
    passed = [c["id"] for c in checks if c["status"] == "pass"]
    missing_core = [cid for cid in CORE_CHECKS if by_id.get(cid) != "pass"]
    if failed:
        status = "fail"
    elif not missing_core:
        status = "pass"
    else:
        status = "unknown"
        if not reason:
            reason = "core checks not run: " + ", ".join(missing_core)
    return {
        "status": status,
        "gating": GATING,
        "gating_checks": sorted(GATING_CHECKS),
        "note": "" if GATING else "reporting only: owner approval of meaning pending (D6)",
        "reason": reason,
        "failed": failed,
        "passed": len(passed),
        "unchecked": sum(1 for c in checks if c["status"] == "unchecked"),
        "checks": checks,
    }


def gating_problems(
    record: dict[str, Any] | None, only: tuple[str, ...] | None = None
) -> list[str]:
    """Reasons the run must fail: a failed check named in ``GATING_CHECKS``,
    or, when any check gates, no record or no facts to judge them by.
    ``only`` limits the answer to check ids with those prefixes (the
    benchmark shapes, judged after the benchmark). Empty while
    ``GATING_CHECKS`` is empty (D6)."""
    gating = {g for g in GATING_CHECKS if only is None or g.startswith(only)}
    if not gating:
        return []
    if record is None or not record.get("facts_present"):
        if only is not None:
            return []  # already reported when the pipeline was judged
        why = (record or {}).get("reason") or "the check did not run"
        return [f"Customer 360 correctness gate: no expected-result facts ({why})."]
    return [
        f"Customer 360 correctness gate: {c['id']} failed (observed {c['observed']}, "
        f"expected {c['expected']}, tolerance {c['tolerance']})."
        for c in record.get("checks") or []
        if c["id"] in gating and c["status"] == "fail"
    ]


def evaluate_run(
    gold_jobs: list[Any], bronze_jobs: list[Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    """The c360 verdict from the last gold-finalize and bronze-verify jobs.

    Only the last job of each: in a multi-cycle run it is the one that saw
    the whole corpus. An earlier cycle's facts judged against the full
    window and the last bronze count would report misleading failures.
    """
    facts = getattr(gold_jobs[-1], "c360_check", None) if gold_jobs else None
    bronze = getattr(bronze_jobs[-1], "c360_bronze", None) if bronze_jobs else None
    ok = False
    if not facts:
        v = verdict([], "no [c360-check] line in the last gold-finalize driver log")
    elif facts.get("error"):
        v = verdict([], f"fact collection failed: {facts['error']}")
    elif facts.get("skipped"):
        v = verdict([], f"skipped: {facts['skipped']}")
    else:
        ok = True
        v = verdict(pipeline_checks(facts, bronze, ctx))
    v["facts_present"] = ok
    v["context"] = ctx
    v["facts"] = facts
    v["bronze"] = bronze
    return v


def add_benchmark_checks(record: dict[str, Any], queries: list[Any]) -> dict[str, Any]:
    """Fold the benchmark shape checks into a run's verdict record."""
    checks = list(record.get("checks") or []) + benchmark_checks(
        queries, record.get("facts"), record.get("context") or {}
    )
    reason = record.get("reason", "") if not record.get("facts_present") else ""
    v = verdict(checks, reason)
    for k in ("facts_present", "context", "facts", "bronze"):
        v[k] = record.get(k)
    return v


def summary_lines(record: dict[str, Any]) -> list[str]:
    """Console lines for a verdict: status, then each failed check."""
    status = record.get("status")
    head = (
        f"Customer 360 expected results: {status} "
        f"({record.get('passed', 0)} passed, {len(record.get('failed') or [])} failed, "
        f"{record.get('unchecked', 0)} unchecked)"
    )
    if not record.get("gating"):
        head += "; reporting only, not gating (D6)"
    if record.get("reason"):
        head += f"; {record['reason']}"
    lines = [head]
    for c in record.get("checks") or []:
        if c["status"] == "fail":
            lines.append(
                f"  {c['id']}: observed {c['observed']} expected {c['expected']} "
                f"(tolerance {c['tolerance']})"
            )
    return lines
