"""A small Python model of the c360 generator for local Spark tests.

Not collected by pytest. Follows ``datagen_rs/src/customer360.rs`` row by row
for every column silver and gold read: sessions of 5-20 rows for one
customer inside a 30 minute window, the hot/retail customer sampler, the
interaction weights, purchase-only amounts, visit-only page views and time
on site, support-only tickets and scores, offline channels without a device.
Bytes differ from the Rust (different RNG); the distributions are the same,
which is what the expected-result checks are defined on.
"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone

INTERACTION_TYPES = ["purchase", "browse", "support", "login", "abandoned_cart"]
INTERACTION_WEIGHTS = [0.18, 0.35, 0.12, 0.20, 0.15]
CHANNELS = ["web", "mobile_app", "store", "call_center", "social_media"]
DQ_FLAGS = ["clean", "duplicate_suspected", "incomplete_data", "format_inconsistent"]
DQ_WEIGHTS = [0.92, 0.02, 0.03, 0.03]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["chrome", "safari", "firefox", "edge"]
TIERS = ["bronze", "silver", "gold"]


def _hot_cdf(customers):
    hot = min(500, customers)
    w = [1.0 / (k**1.2) for k in range(1, hot + 1)]
    t = sum(w)
    out, run = [], 0.0
    for x in w:
        run += x / t
        out.append(run)
    out[-1] = 1.0
    return out


def generate(rows, customers, start, days, seed=7, ticket_space=90_000):
    """``rows`` bronze rows over ``days`` days from ``start`` (UTC)."""
    rng = random.Random(seed)
    hot = _hot_cdf(customers)
    member = [rng.random() < 0.6 for _ in range(customers + 1)]
    tier = [
        TIERS[0 if r < 0.7 else (1 if r < 0.9 else 2)]
        for r in (rng.random() for _ in range(customers + 1))
    ]
    t0 = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    span_us = days * 86_400 * 1_000_000
    sess_us = 30 * 60 * 1_000_000
    end_us = span_us - 1
    out = []
    i = 0
    sid = 0
    while i < rows:
        length = min(5 + rng.randrange(16), rows - i)
        if rng.random() < 0.4:
            u = rng.random()
            lo, hi = 0, len(hot) - 1
            while lo < hi:
                mid = (lo + hi) // 2
                if hot[mid] <= u:
                    lo = mid + 1
                else:
                    hi = mid
            cid = lo + 1
        elif len(hot) >= customers:
            cid = 1 + rng.randrange(customers)
        else:
            cid = len(hot) + 1 + rng.randrange(customers - len(hot))
        anchor = rng.randrange(max(span_us - sess_us, 1))
        sid += 1
        for _ in range(length):
            ts_us = min(anchor + rng.randrange(sess_us), end_us)
            it = rng.choices(INTERACTION_TYPES, INTERACTION_WEIGHTS)[0]
            purchase = it == "purchase"
            visit = it in ("purchase", "browse")
            amt = (
                round(min(max(math.exp(rng.gauss(4.3, 1.2)), 1.0), 9999.99), 2) if purchase else 0.0
            )
            pv = 1 + rng.randrange(20) if visit else 0
            ch = rng.choice(CHANNELS)
            offline = ch in ("store", "call_center")
            support = it == "support"
            no_product = it in ("login", "support")
            out.append(
                {
                    "id": i,
                    "row_id": i,
                    "event_timestamp": t0 + timedelta(microseconds=ts_us),
                    "event_id": f"e{i}",
                    "session_id": f"s{sid}",
                    "customer_id": cid,
                    "email_raw": f"user{1000 + rng.randrange(998_999)}@gmail.com",
                    "phone_raw": "5551234567",
                    "interaction_type": it,
                    "product_id": None if no_product else f"PRD{10000 + rng.randrange(89_999)}",
                    "product_category": None if no_product else "books",
                    "transaction_amount": amt,
                    "currency": rng.choice(["USD", "EUR", "GBP", "CAD"]),
                    "channel": ch,
                    "device_type": None if offline else rng.choice(DEVICES),
                    "browser": None if offline else rng.choice(BROWSERS),
                    "ip_address": "10.0.1.50",
                    "city_raw": "Austin",
                    "state_raw": "TX",
                    "zip_code": "73301",
                    "page_views": pv,
                    "time_on_site_seconds": 30 + rng.randrange(3600) if pv > 0 else 0,
                    "support_ticket_id": (
                        f"TKT{10000 + rng.randrange(ticket_space)}" if support else None
                    ),
                    "satisfaction_score": 1 + rng.randrange(5) if support else None,
                    "utm_source": "google" if rng.random() < 0.4 else None,
                    "utm_medium": "cpc",
                    "loyalty_member": member[cid],
                    "loyalty_tier": tier[cid] if member[cid] else None,
                    "points_earned": int(amt * 10) if (member[cid] and purchase) else 0,
                    "points_redeemed": 100 + rng.randrange(900)
                    if (member[cid] and rng.random() < 0.1)
                    else 0,
                    "data_quality_flag": rng.choices(DQ_FLAGS, DQ_WEIGHTS)[0],
                    "interaction_payload": "ab",
                }
            )
            i += 1
    return out
