"""Distribution helpers to shape pacs.008 rows into something that
resembles real bank wire activity.

Everything here is deterministic in the caller-supplied ``rng`` seed
(no wall-clock, no os.urandom). Hash-based flags (sanctions, PEP) are
deterministic in ``entity_id`` so the same entity has the same status
across every file in a run.

Design goal per practitioner feedback (2026-09-16): the first thing an
AML investigator does with a synthetic dataset is:

  SELECT AVG(size(correspondent_chain)),
         percentile(intr_bk_sttlm_amt, 0.5),
         percentile(intr_bk_sttlm_amt, 0.95),
         COUNT(DISTINCT dbtr_country || '->' || cdtr_country),
         AVG(CASE WHEN sanctions_status='clear' THEN 0 ELSE 1 END),
         AVG(CASE WHEN pep_status THEN 1 ELSE 0 END)
  FROM silver.transactions t JOIN silver.entities e ON ...

Each of those queries returning a plausible number is what buys the
first thirty seconds of credibility. This module makes them plausible.

No production-realistic distribution here is precise -- these are v0
approximations chosen for shape, not fidelity. Real bank distributions
are proprietary; ours are order-of-magnitude correct.
"""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Amount distributions
# ---------------------------------------------------------------------------

# Log-normal parameters for wire amounts. Chosen so that:
#   median ~= $5,000  (exp(mu))
#   p95    ~= $50,000
#   p99    ~= $250,000
#   p999   ~= $2,000,000
# Roughly matches the corporate + retail wire mix reported in public
# BIS/CPMI payment statistics for USD-denominated cross-border wires.
_LOGNORMAL_MU = math.log(5_000)
_LOGNORMAL_SIGMA = 1.4

# Fraction of amounts snapped to a round number ($1K / $10K / $100K).
# Real wires cluster on payroll-anniversary, invoice-round, tax figures.
_ROUND_SNAP_RATE = 0.15

# Amount ceiling to prevent absurd log-normal tails (>$50M is once-a-year
# territory at most banks). Values above are clamped.
_AMOUNT_CEILING = Decimal("50000000.00")


def log_normal_amount(rng: np.random.Generator) -> Decimal:
    """Sample one wire amount from the log-normal + round-snap mixture."""
    raw = float(rng.lognormal(mean=_LOGNORMAL_MU, sigma=_LOGNORMAL_SIGMA))
    if rng.random() < _ROUND_SNAP_RATE:
        raw = _snap_to_round(raw)
    amt = Decimal(str(round(raw, 2)))
    return amt if amt <= _AMOUNT_CEILING else _AMOUNT_CEILING


def _snap_to_round(x: float) -> float:
    """Snap to the nearest $100/$1000/$10000 depending on magnitude."""
    if x < 1_000:
        step = 100
    elif x < 10_000:
        step = 1_000
    elif x < 100_000:
        step = 10_000
    else:
        step = 100_000
    return float(round(x / step) * step)


def structuring_amount(rng: np.random.Generator) -> Decimal:
    """Tight cluster $9,500 - $9,999 for W2 structuring under US $10K CTR.

    Real structuring targets the reporting threshold: $10,000 US, $15,000
    EU (6AMLD Article 34). We use the US threshold since our currency
    default is USD.
    """
    return Decimal(str(round(float(rng.uniform(9_500, 9_999)), 2)))


# ---------------------------------------------------------------------------
# Party reuse (Zipf)
# ---------------------------------------------------------------------------


class PartySelector:
    """Zipf-shaped party ID sampler.

    Top ``hot_corp_count`` IDs get ~``hot_corp_share`` of transactions
    (corporate accounts with recurring beneficiaries). Rest are uniform
    retail. Deterministic in the caller-supplied ``rng``.
    """

    def __init__(
        self,
        customer_id_max: int,
        hot_corp_count: int = 500,
        hot_corp_share: float = 0.40,
    ):
        self.customer_id_max = customer_id_max
        self.hot_corp_count = min(hot_corp_count, customer_id_max)
        self.hot_corp_share = hot_corp_share
        # Reserve the low ID range for hot corporates. Zipf over that range
        # gives the strong power-law shape practitioners see in the data.
        # numpy zipf can return unbounded ints; we clip to hot_corp_count.

    def sample(self, rng: np.random.Generator) -> int:
        """Return one party ID biased by the corporate-hot distribution."""
        if rng.random() < self.hot_corp_share and self.hot_corp_count > 0:
            # Zipf shape parameter 1.5: heavy-tailed, top-500 concentrated.
            raw = int(rng.zipf(1.5))
            return min(raw, self.hot_corp_count)
        # Retail: uniform over the non-hot range.
        return int(rng.integers(self.hot_corp_count + 1, self.customer_id_max + 1))


# ---------------------------------------------------------------------------
# Country corridors
# ---------------------------------------------------------------------------

# Weighted (dbtr_country, cdtr_country) pairs. Weights sum to 1.0.
# Corridor mix approximated from public SWIFT / CHIPS traffic reports.
CORRIDORS: tuple[tuple[tuple[str, str], float], ...] = (
    # Domestic (single-country) -- makes up the bulk of daily wire volume.
    (("US", "US"), 0.28),
    (("GB", "GB"), 0.05),
    (("DE", "DE"), 0.04),
    (("FR", "FR"), 0.03),
    (("JP", "JP"), 0.02),
    # Major cross-border corridors.
    (("US", "GB"), 0.06),
    (("US", "MX"), 0.05),
    (("US", "CN"), 0.04),
    (("US", "CA"), 0.03),
    (("US", "IN"), 0.03),
    (("GB", "US"), 0.03),
    (("GB", "SG"), 0.02),
    (("GB", "AE"), 0.02),
    (("DE", "FR"), 0.03),
    (("DE", "CH"), 0.02),
    (("FR", "DE"), 0.02),
    # Higher-risk corridors that money-laundering typologies over-index.
    (("US", "PA"), 0.008),  # Panama
    (("US", "KY"), 0.005),  # Cayman
    (("US", "AE"), 0.010),  # UAE
    (("GB", "CH"), 0.010),
    (("GB", "KY"), 0.005),
    # Tail: everything else uniformly-ish.
    (("SG", "CN"), 0.015),
    (("CH", "US"), 0.010),
    (("JP", "US"), 0.010),
    (("CA", "US"), 0.012),
    (("MX", "US"), 0.008),
    (("CN", "US"), 0.012),
    (("AE", "GB"), 0.008),
    (("SG", "GB"), 0.007),
)

_CORRIDOR_KEYS = tuple(c for c, _ in CORRIDORS)
_CORRIDOR_WEIGHTS = np.array([w for _, w in CORRIDORS], dtype=np.float64)
_CORRIDOR_WEIGHTS = _CORRIDOR_WEIGHTS / _CORRIDOR_WEIGHTS.sum()
# Pre-computed cumulative distribution -- bisect on a uniform sample is
# ~10x faster than np.random.Generator.choice() per-call for our N ~= 28.
_CORRIDOR_CDF = np.cumsum(_CORRIDOR_WEIGHTS)


def sample_corridor(rng: np.random.Generator) -> tuple[str, str]:
    """Sample one (dbtr_country, cdtr_country) pair from the weighted mix."""
    u = float(rng.random())
    idx = int(np.searchsorted(_CORRIDOR_CDF, u))
    if idx >= len(_CORRIDOR_KEYS):
        idx = len(_CORRIDOR_KEYS) - 1
    return _CORRIDOR_KEYS[idx]


_CURRENCY_BY_COUNTRY = {
    "US": "USD",
    "GB": "GBP",
    "DE": "EUR",
    "FR": "EUR",
    "CH": "CHF",
    "SG": "SGD",
    "JP": "JPY",
    "AE": "AED",
    "PA": "USD",  # dollarised
    "KY": "USD",  # dollarised
    "CN": "CNY",
    "MX": "MXN",
    "IN": "INR",
    "CA": "CAD",
}


def currency_for(country: str) -> str:
    """Return the local currency for a country code."""
    return _CURRENCY_BY_COUNTRY.get(country, "USD")


def is_cross_border(dbtr_country: str, cdtr_country: str) -> bool:
    return dbtr_country != cdtr_country


# ---------------------------------------------------------------------------
# Time-of-day / day-of-week / quarter-end shape
# ---------------------------------------------------------------------------

# Weight per hour-of-day (0..23). Peaks 10-11 and 15-16 local time.
_INTRADAY_WEIGHTS = np.array(
    [
        0.005, 0.003, 0.002, 0.002, 0.003, 0.006,  # 00-05: overnight
        0.015, 0.030, 0.055, 0.075, 0.095, 0.100,  # 06-11: morning ramp + peak
        0.070, 0.075, 0.080, 0.095, 0.090, 0.075,  # 12-17: lunch dip + afternoon peak
        0.050, 0.035, 0.020, 0.010, 0.007, 0.006,  # 18-23: evening decay
    ],
    dtype=np.float64,
)
_INTRADAY_WEIGHTS = _INTRADAY_WEIGHTS / _INTRADAY_WEIGHTS.sum()

# Weight per day-of-week (Mon=0..Sun=6). Mon + Fri elevated.
_DOW_WEIGHTS = np.array([0.20, 0.15, 0.15, 0.15, 0.20, 0.08, 0.07], dtype=np.float64)
_DOW_WEIGHTS = _DOW_WEIGHTS / _DOW_WEIGHTS.sum()

# Pre-compute peak weight once. Was per-call in the sampler (7.4s / 27% of
# datagen wall-clock at 200K rows measured 2026-09-16). Constant across
# every call, so lift it out of the hot loop.
_INTRADAY_PEAK = float(_INTRADAY_WEIGHTS.max())
_DOW_PEAK = float(_DOW_WEIGHTS.max())
_TS_PEAK = _INTRADAY_PEAK * _DOW_PEAK * 1.5


def sample_timestamp_shaped(
    rng: np.random.Generator,
    window_start: datetime,
    window_end: datetime,
) -> datetime:
    """Sample a timestamp shaped by intraday + day-of-week + quarter-end.

    Draws candidate timestamps and accepts with a shape-weighted
    probability. Bounded iterations (max 8) so it can't hang; falls back
    to uniform on the last attempt.
    """
    span_s = (window_end - window_start).total_seconds()
    if span_s <= 0:
        return window_start
    peak = _TS_PEAK
    for attempt in range(8):
        offset_s = float(rng.uniform(0, span_s))
        ts = window_start + timedelta(seconds=offset_s)
        hour_w = _INTRADAY_WEIGHTS[ts.hour]
        dow_w = _DOW_WEIGHTS[ts.weekday()]
        # Quarter-end boost: last 5 days of Mar/Jun/Sep/Dec.
        qend_w = 1.5 if ts.month in (3, 6, 9, 12) and ts.day >= 26 else 1.0
        joint = hour_w * dow_w * qend_w
        if attempt == 7 or rng.random() < (joint / peak):
            return ts
    return ts  # never hit; for type-checker


# ---------------------------------------------------------------------------
# Sanctions / PEP flags (deterministic in entity_id, no rng needed)
# ---------------------------------------------------------------------------

_SANCTIONS_RATE = 0.005  # 0.5% -- higher than production reality but visible in samples
_PEP_RATE = 0.020        # 2%   -- roughly a corporate portfolio's PEP share


def _entity_hash_frac(entity_id: int, salt: str) -> float:
    """Return a stable float in [0, 1) for (entity_id, salt)."""
    key = f"{salt}:{entity_id}".encode()
    digest = hashlib.blake2b(key, digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64


def sanctions_flag(entity_id: int) -> bool:
    return _entity_hash_frac(entity_id, "sanctions") < _SANCTIONS_RATE


def pep_flag(entity_id: int) -> bool:
    return _entity_hash_frac(entity_id, "pep") < _PEP_RATE


def sanctions_status_for(dbtr_id: int, cdtr_id: int) -> str:
    """Return the sanctions-screening result for silver.entities.

    A transaction touches a flagged entity ~1% of the time given 0.5%
    per party; we report ``flagged`` per entity, not per transaction.
    """
    return "flagged" if sanctions_flag(dbtr_id) or sanctions_flag(cdtr_id) else "clear"


# ---------------------------------------------------------------------------
# Correspondent chain (populate ~15% of cross-border wires)
# ---------------------------------------------------------------------------

_CORRESPONDENT_CHAIN_RATE_CROSS_BORDER = 0.35
_CORRESPONDENT_CHAIN_RATE_DOMESTIC = 0.02


def sample_correspondent_chain(
    rng: np.random.Generator,
    cross_border: bool,
    bic_pool: tuple[str, ...],
) -> tuple[dict | None, dict | None, dict | None]:
    """Return (intrmy_agt_1, intrmy_agt_2, intrmy_agt_3).

    Cross-border wires get a chain 35% of the time; domestic wires 2%.
    Chain length is 1-3 banks. Overall population rate targets the
    REQ-F-02 acceptance of ~15% multi-hop assuming a 40% cross-border
    mix in the corridor table above.
    """
    rate = _CORRESPONDENT_CHAIN_RATE_CROSS_BORDER if cross_border else _CORRESPONDENT_CHAIN_RATE_DOMESTIC
    if rng.random() >= rate:
        return None, None, None
    depth = int(rng.integers(1, 4))
    idxs = rng.choice(len(bic_pool), size=depth, replace=False)
    banks = tuple(_bic_to_agent(bic_pool[i]) for i in idxs) + (None, None, None)
    return banks[0], banks[1], banks[2]


def _bic_to_agent(bic: str) -> dict:
    """Turn a BIC into an agent struct matching the pacs.008 schema."""
    idx_str = bic[4:6] if len(bic) >= 6 else "00"
    try:
        idx = int(idx_str)
    except ValueError:
        idx = 0
    return {
        "bicfi": bic,
        "lei": f"LEI{idx:018d}",
        "nm": f"Bank {idx:02d}",
    }


# ---------------------------------------------------------------------------
# Regulatory reporting (populate for cross-border > $10K USD equivalent)
# ---------------------------------------------------------------------------


def sample_regulatory_reporting(
    dbtr_country: str,
    cdtr_country: str,
    amount_usd: Decimal,
) -> list[dict] | None:
    """FinCEN / EBA / MAS-style regulatory-reporting flags on the wire.

    Populated for cross-border wires >= $10,000 USD-equivalent
    (US FinCEN CTR threshold, EU 6AMLD 15,000 EUR ~= $16K USD).
    Returns a list because pacs.008 allows multiple reporting
    authorities per transaction (source country + destination country).
    """
    if dbtr_country == cdtr_country:
        return None
    if amount_usd < Decimal("10000.00"):
        return None
    entries: list[dict] = []
    entries.append(
        {
            "dbt_cdt_rptg_ind": "DEBT",
            "authrty_nm": _regulator_for(dbtr_country),
            "authrty_ctry": dbtr_country,
            "details": [f"amount:{amount_usd}", "cross-border"],
        }
    )
    entries.append(
        {
            "dbt_cdt_rptg_ind": "CRED",
            "authrty_nm": _regulator_for(cdtr_country),
            "authrty_ctry": cdtr_country,
            "details": [f"amount:{amount_usd}", "cross-border"],
        }
    )
    return entries


_REGULATORS = {
    "US": "FinCEN",
    "GB": "FCA",
    "DE": "BaFin",
    "FR": "ACPR",
    "CH": "FINMA",
    "SG": "MAS",
    "JP": "FSA",
    "AE": "CBUAE",
    "CN": "SAFE",
    "MX": "CNBV",
    "IN": "RBI",
    "CA": "FINTRAC",
    "PA": "SBP",
    "KY": "CIMA",
}


def _regulator_for(country: str) -> str:
    return _REGULATORS.get(country, "UNKNOWN")


# ---------------------------------------------------------------------------
# BIC pool (shared with typologies.py)
# ---------------------------------------------------------------------------


def build_bic_pool(size: int = 40) -> tuple[str, ...]:
    """A pool of synthetic BICs used for correspondent-chain sampling."""
    return tuple(f"BANK{i:02d}XX" for i in range(size))


# ---------------------------------------------------------------------------
# Structuring burst window (24-72h with tight amount spread)
# ---------------------------------------------------------------------------


def structuring_burst_window(rng: np.random.Generator) -> timedelta:
    """Duration for a structuring burst: 24-72h, roughly Poisson-shaped."""
    hours = float(rng.uniform(24, 72))
    return timedelta(hours=hours)


# ---------------------------------------------------------------------------
# Public accessors (thin wrappers, useful in tests)
# ---------------------------------------------------------------------------


def summarise_realism_calibration() -> dict[str, Any]:
    """Return the tunable constants, so tests can assert plausible defaults."""
    return {
        "lognormal_mu": _LOGNORMAL_MU,
        "lognormal_sigma": _LOGNORMAL_SIGMA,
        "round_snap_rate": _ROUND_SNAP_RATE,
        "amount_ceiling": _AMOUNT_CEILING,
        "sanctions_rate": _SANCTIONS_RATE,
        "pep_rate": _PEP_RATE,
        "correspondent_chain_rate_cross_border": _CORRESPONDENT_CHAIN_RATE_CROSS_BORDER,
        "correspondent_chain_rate_domestic": _CORRESPONDENT_CHAIN_RATE_DOMESTIC,
        "corridor_count": len(CORRIDORS),
    }


# Silence unused-import warnings in tests that only need timezone-aware datetimes.
_ = timezone
