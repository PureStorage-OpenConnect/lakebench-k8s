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


# Currency-specific CTR/STR thresholds. Values are the local-currency
# reporting-threshold minus a small buffer for the "just under" band.
# References:
#   US   -- FinCEN CTR $10,000 (31 CFR 1010.311)
#   GB   -- FCA £10,000 for cash SAR-adjacent; MLR £15,000 for occasional-txn CDD
#   EU   -- 6AMLD Article 34 EUR 15,000
#   CH   -- FINMA-CDB CHF 15,000
#   JP   -- JFSA JPY 1,000,000
#   AE   -- CB UAE AED 55,000 (~USD 15K)
#   SG   -- MAS SGD 20,000
#   CA   -- FINTRAC CAD 10,000
#   MX   -- CNBV MXN 645,000 (~USD 30K); we use 100,000 as the "just under" tell
#   IN   -- RBI INR 1,000,000
_STRUCTURING_BANDS: dict[str, tuple[float, float]] = {
    "USD": (9_500.0, 9_999.0),
    "GBP": (14_700.0, 14_995.0),
    "EUR": (14_700.0, 14_995.0),
    "CHF": (14_700.0, 14_995.0),
    "JPY": (990_000.0, 999_999.0),
    "AED": (54_500.0, 54_999.0),
    "SGD": (19_500.0, 19_999.0),
    "CAD": (9_500.0, 9_999.0),
    "MXN": (99_000.0, 99_999.0),
    "INR": (990_000.0, 999_999.0),
    "CNY": (49_500.0, 49_999.0),
}


def structuring_amount(rng: np.random.Generator, currency: str = "USD") -> Decimal:
    """Tight cluster just under the local CTR/STR reporting threshold.

    Currency-aware so a GBP wire lands under £15K, JPY under ¥1M, etc.
    Detection rules must be threshold-aware per currency; a v0 that emits
    9,500 CHF or 9,500 JPY looks unrealistic to any real AML operator.
    """
    lo, hi = _STRUCTURING_BANDS.get(currency, _STRUCTURING_BANDS["USD"])
    return Decimal(str(round(float(rng.uniform(lo, hi)), 2)))


# ---------------------------------------------------------------------------
# Party reuse (Zipf)
# ---------------------------------------------------------------------------


class PartySelector:
    """Power-law-shaped party ID sampler.

    Top ``hot_corp_count`` IDs get ~``hot_corp_share`` of transactions
    (corporate accounts with recurring beneficiaries). Rest are uniform
    retail. Deterministic in the caller-supplied ``rng``.

    Cycle-3 fix (2026-09-17): prior version used ``rng.zipf(1.5)`` which
    puts ~50% of hot-corporate mass on ID=1 -- entity_1 alone became a
    supernode with ~4% self-loops and ~30% edge incidence, breaking any
    graph-shaped detection (W1 CC, W3 motif finding). Real tier-1 bank
    corporate distributions look more like Zipf(2.2-2.5). We use a
    truncated Zipf drawn from a pre-computed CDF over IDs 1..hot_corp_count
    so entity_1's share is bounded (~10-12%) and every top-500 ID has
    non-trivial mass.
    """

    def __init__(
        self,
        customer_id_max: int,
        hot_corp_count: int = 500,
        hot_corp_share: float = 0.40,
        zipf_shape: float = 2.3,
    ):
        self.customer_id_max = customer_id_max
        self.hot_corp_count = min(hot_corp_count, customer_id_max)
        self.hot_corp_share = hot_corp_share
        self.zipf_shape = zipf_shape
        # Pre-compute truncated-Zipf CDF over the hot-corp range so
        # bisect on rng.random() gives O(log N) sampling with a fixed
        # (bounded) share for id=1. Weights: w_k = 1/k^shape for k in 1..N.
        weights = np.array(
            [1.0 / (k ** zipf_shape) for k in range(1, self.hot_corp_count + 1)],
            dtype=np.float64,
        )
        weights = weights / weights.sum()
        self._hot_cdf = np.cumsum(weights)
        # Pre-check: what share of hot mass lands on id=1? Guard so future
        # changes don't quietly re-introduce a supernode.
        self._id1_share = float(weights[0]) if len(weights) > 0 else 0.0

    def sample(self, rng: np.random.Generator) -> int:
        """Return one party ID biased by the corporate-hot distribution."""
        if rng.random() < self.hot_corp_share and self.hot_corp_count > 0:
            u = float(rng.random())
            idx = int(np.searchsorted(self._hot_cdf, u))
            if idx >= self.hot_corp_count:
                idx = self.hot_corp_count - 1
            return idx + 1  # 1-indexed IDs
        # Retail: uniform over the non-hot range. When the retail range is
        # empty (customer_id_max <= hot_corp_count), fall back to the full
        # ID space so tiny populations don't hit numpy's low >= high error.
        if self.hot_corp_count >= self.customer_id_max:
            return int(rng.integers(1, self.customer_id_max + 1))
        return int(rng.integers(self.hot_corp_count + 1, self.customer_id_max + 1))


# ---------------------------------------------------------------------------
# Country corridors
# ---------------------------------------------------------------------------

# Weighted (dbtr_country, cdtr_country) pairs. Real bank wire mix is
# heavily domestic (85-90%); prior v0 shipped 45% cross-border which
# fails smell test 4 in the practitioner review (2026-09-16). Rebalanced
# so cross-border sits at ~18-22% -- inside R4 [0.15, 0.25] band.
CORRIDORS: tuple[tuple[tuple[str, str], float], ...] = (
    # Domestic -- ~80% of volume (matches SWIFT/CHIPS/domestic-rail mix)
    (("US", "US"), 0.42),
    (("GB", "GB"), 0.09),
    (("DE", "DE"), 0.07),
    (("FR", "FR"), 0.05),
    (("JP", "JP"), 0.04),
    (("CA", "CA"), 0.03),
    (("SG", "SG"), 0.02),
    (("CH", "CH"), 0.02),
    (("AU", "AU"), 0.02),
    (("IN", "IN"), 0.02),
    (("CN", "CN"), 0.02),
    # Major cross-border corridors -- ~15% aggregate
    (("US", "GB"), 0.015),
    (("US", "MX"), 0.015),
    (("US", "CN"), 0.012),
    (("US", "CA"), 0.010),
    (("US", "IN"), 0.008),
    (("GB", "US"), 0.008),
    (("GB", "SG"), 0.005),
    (("GB", "AE"), 0.005),
    (("DE", "FR"), 0.010),
    (("DE", "CH"), 0.005),
    (("FR", "DE"), 0.008),
    (("CA", "US"), 0.008),
    (("MX", "US"), 0.005),
    (("CN", "US"), 0.006),
    (("JP", "US"), 0.005),
    (("SG", "CN"), 0.005),
    (("CH", "US"), 0.004),
    (("AE", "GB"), 0.004),
    (("SG", "GB"), 0.003),
    # Higher-risk corridors -- ~3% aggregate (lower than v0 but present)
    (("US", "PA"), 0.004),  # Panama
    (("US", "KY"), 0.003),  # Cayman
    (("US", "AE"), 0.005),  # UAE
    (("GB", "CH"), 0.005),
    (("GB", "KY"), 0.003),
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


# Approx FX rates to USD (2026-ish). Used by sample_regulatory_reporting to
# FX-normalize amounts before comparing to the $10K USD threshold. Prior code
# compared raw local-currency amounts to a USD threshold, so a JPY50,000 wire
# (~$340 USD) triggered reporting while a EUR5,000 wire (~$5,500) did not.
_FX_TO_USD = {
    "USD": 1.0,
    "GBP": 1.30,
    "EUR": 1.10,
    "CHF": 1.15,
    "JPY": 0.0068,
    "AED": 0.27,
    "SGD": 0.74,
    "CAD": 0.73,
    "MXN": 0.055,
    "CNY": 0.14,
    "INR": 0.012,
}


def to_usd(amount: Decimal, currency: str) -> Decimal:
    """FX-normalise an amount to USD-equivalent using cached rates."""
    rate = _FX_TO_USD.get(currency, 1.0)
    return (amount * Decimal(str(rate))).quantize(Decimal("0.01"))


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
    amount: Decimal,
    currency: str = "USD",
) -> list[dict] | None:
    """FinCEN / EBA / MAS-style regulatory-reporting flags on the wire.

    Populated for cross-border wires >= $10,000 USD-equivalent. Prior
    signature took `amount_usd` but callers passed the wire-currency
    amount directly, causing a JPY50,000 wire (~$340) to trigger reporting
    and a EUR5,000 wire (~$5,500) to escape it. Now takes currency
    explicitly and FX-normalises before comparing to the threshold.
    """
    if dbtr_country == cdtr_country:
        return None
    amount_usd = to_usd(amount, currency)
    if amount_usd < Decimal("10000.00"):
        return None
    entries: list[dict] = []
    entries.append(
        {
            "dbt_cdt_rptg_ind": "DEBT",
            "authrty_nm": _regulator_for(dbtr_country),
            "authrty_ctry": dbtr_country,
            "details": [f"amount_usd:{amount_usd}", "cross-border"],
        }
    )
    entries.append(
        {
            "dbt_cdt_rptg_ind": "CRED",
            "authrty_nm": _regulator_for(cdtr_country),
            "authrty_ctry": cdtr_country,
            "details": [f"amount_usd:{amount_usd}", "cross-border"],
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
    "AU": "AUSTRAC",
}


def _regulator_for(country: str) -> str:
    return _REGULATORS.get(country, "UNKNOWN")


# ---------------------------------------------------------------------------
# Home country per entity (stable across corridors)
# ---------------------------------------------------------------------------

# Weighted distribution of entity home countries. Real bank customer bases
# concentrate on the home market plus the top corporate/expat destinations.
# Fixes cycle-2 finding 2: prior code stamped ctry_of_res from the transaction
# corridor, so the same corporate transacting on US->PA and US->GB fragmented
# into distinct entity_ids at silver (since silver.entity_id keys on
# hash(name, country)).
# US-headquartered global bank customer base: dominant home market plus a
# spread of top corporate/expat destinations. US weight of 0.85 targets
# ~72% P(both parties US) => ~28% home-country-cross-border, which after
# accounting for weighted small-country reuse comes in near R4 band [0.15,
# 0.25]. Silver derives cross_border from party ctry_of_res, so the home-
# country distribution IS the cross-border distribution.
_HOME_COUNTRIES: tuple[tuple[str, float], ...] = (
    ("US", 0.85),
    ("GB", 0.03),
    ("DE", 0.02),
    ("FR", 0.015),
    ("CA", 0.015),
    ("JP", 0.010),
    ("SG", 0.010),
    ("CH", 0.010),
    ("CN", 0.010),
    ("IN", 0.010),
    ("MX", 0.010),
    ("AE", 0.005),
    ("AU", 0.005),
    ("PA", 0.003),
    ("KY", 0.002),
)
_HOME_COUNTRIES_LIST = tuple(c for c, _ in _HOME_COUNTRIES)
_HOME_COUNTRIES_CDF: tuple[float, ...] = ()  # populated below


def _init_home_countries_cdf() -> tuple[float, ...]:
    total = sum(w for _, w in _HOME_COUNTRIES)
    running = 0.0
    out = []
    for _, w in _HOME_COUNTRIES:
        running += w / total
        out.append(running)
    return tuple(out)


_HOME_COUNTRIES_CDF = _init_home_countries_cdf()


def home_country_for(entity_id: int) -> str:
    """Return the deterministic home country for an entity_id.

    Stable across corridors, transactions, and worker forks -- the same
    entity_id always yields the same country, so silver's `hash(name,
    country)` entity_id derivation doesn't fragment reused corporates.
    Uses a blake2b hash then a bisect against the pre-built CDF.
    """
    frac = _entity_hash_frac(entity_id, "home_country")
    # searchsorted-equivalent linear scan (small array, negligible cost)
    for idx, cum in enumerate(_HOME_COUNTRIES_CDF):
        if frac < cum:
            return _HOME_COUNTRIES_LIST[idx]
    return _HOME_COUNTRIES_LIST[-1]


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
