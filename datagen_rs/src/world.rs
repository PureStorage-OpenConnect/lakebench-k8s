//! The static world: per-entity attributes as pure functions of (seed, id),
//! bit-faithful to datagen_v2/world.py.

use crate::hash::{hash_frac, splitmix64};

pub const TYPE_PERSON: i8 = 0;
pub const TYPE_COMPANY: i8 = 1;
pub const TYPE_FI: i8 = 2;
pub const TYPE_LABELS: [&str; 3] = ["Person", "Company", "FI"];

// Entity type shares: Person 0.55, Company 0.40, FI 0.05 (cumulative 0.55, 0.95).
pub const HOME_CODES: [&str; 18] = [
    "US", "GB", "DE", "FR", "CA", "MX", "CN", "IN", "JP", "SG", "CH", "BR", "AE", "AU", "HK", "KR",
    "NL", "ES",
];
const HOME_WEIGHTS: [f64; 18] = [
    0.88, 0.015, 0.010, 0.010, 0.015, 0.010, 0.008, 0.008, 0.005, 0.005, 0.005, 0.005, 0.005,
    0.003, 0.002, 0.002, 0.001, 0.001,
];

// Ring size range by type (lo, hi).
const RING_RANGE: [(f64, f64); 3] = [(3.0, 15.0), (50.0, 500.0), (500.0, 5000.0)];
// Baseline activity weight by type.
pub const BASELINE_ACTIVITY: [f64; 3] = [60.0, 240.0, 1500.0];
// Share of transactions whose beneficiary is drawn from the originator's ring.
pub const RING_HIT_RATE: [f64; 3] = [0.95, 0.80, 0.60];

pub const SANCTIONS_RATE: f64 = 0.0005;
pub const PEP_RATE: f64 = 0.0020;

// --- Persona (P2, LB-130 datagen fidelity) --------------------------------
// Before P2 every account of a given type shared one activity rate and one
// amount distribution, so entity_profiles features (avg_gap_days,
// avg_amount_usd) were near-constant across accounts and no typology could be
// separated from an average account: the injected rows were swamped by an
// identical baseline. The persona gives each account an INDIVIDUAL, consistent
// cadence and amount scale, derived as a pure function of (id, seed) so
// generation stays reproducible and shardable by time window. This is the
// foundation the trajectory typologies (P3) deviate against.

/// Log-sd of the per-account activity-rate multiplier. The multiplier is
/// exp(N(0, sd)), so a meaningful minority of accounts are genuinely quiet
/// (rate well below 1x) and some are hyperactive, spreading realised
/// transaction counts -- and therefore avg_gap_days -- across the population.
/// At 1.0, ~16% of accounts sit below 0.37x and ~16% above 2.7x their per-type
/// base rate.
pub const RATE_LOG_SD: f64 = 1.0;

/// Log-sd of the per-account amount scale, applied as an additive shift to the
/// log-normal mean and recentred so the population-level mean amount is
/// preserved (see `amount_log_shift`).
pub const AMOUNT_LOG_SD: f64 = 0.6;

/// Deterministic standard normal from (id, salt) via Box-Muller on two
/// independent hash_frac draws. Pure function of the inputs, so each entity's
/// persona regenerates bit-identically. `salt` and `salt + 1` map through the
/// golden-ratio multiply in hash_frac to well-separated streams, so u1 and u2
/// are effectively independent.
#[inline]
pub fn hash_normal(id: u64, salt: i64) -> f64 {
    let u1 = hash_frac(id, salt).max(1e-300);
    let u2 = hash_frac(id, salt + 1);
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

/// Per-account activity-rate multiplier (log-normal, median 1x). Multiplies the
/// per-type BASELINE_ACTIVITY so originator sampling reflects each account's
/// own rate, giving a per-account (inhomogeneous Poisson) point process rather
/// than a per-type constant.
#[inline]
pub fn rate_mult(id: u64, seed: i64) -> f64 {
    rate_mult_sd(id, seed, 1.0)
}

/// `rate_mult` with the log-sd multiplied by `sd_mult` (the robustness
/// perturbation, crate::robustness). `sd_mult` = 1.0 is bit-identical to
/// `rate_mult`: `RATE_LOG_SD * 1.0` is exact.
#[inline]
pub fn rate_mult_sd(id: u64, seed: i64, sd_mult: f64) -> f64 {
    ((RATE_LOG_SD * sd_mult) * hash_normal(id, seed + 909)).exp()
}

/// Per-account additive shift to the log-normal amount mean, recentred by
/// -sd^2/2 so E[exp(shift)] = 1: the population MEAN amount (total money moved)
/// is preserved. Only the mean is held -- the population is now a scale mixture
/// of lognormals, so its median drops (~18% at sd=0.6) and its spread widens
/// (effective log-sd sqrt(1.4^2 + 0.6^2)). That is by design: it gives each
/// account a consistent typical amount instead of every account drawing from
/// one shared distribution. It is NOT band-preserving -- absolute-threshold
/// rules (W8 $5000, W2 structuring bands) are recalibrated at P5, and the
/// baseline density inside the structuring band is guarded by a regression test
/// so a larger sd cannot silently starve the band past the leakage gate.
///
/// Under the robustness perturbation (`amount_log_shift_p`) the recentring
/// stays at the base sd, so the mean is NOT preserved there: the median moves
/// only with the median multiplier (x1.2 exactly) and the population mean
/// rises with the wider sd (about 8% at sd x1.2, on top of the median's 1.2).
/// tests/robustness.rs checks the structuring-band densities on that world.
#[inline]
pub fn amount_log_shift(id: u64, seed: i64) -> f64 {
    amount_log_shift_p(id, seed, 1.0, 0.0)
}

/// `amount_log_shift` under the robustness perturbation (crate::robustness).
/// The log-sd is multiplied by `sd_mult` around an unchanged centre (the
/// recentring stays at the base `AMOUNT_LOG_SD`), so the population median of
/// per-account typical amounts does not move with the sd; then `log_mu_shift`
/// = ln(median multiplier) is added, which multiplies every account's median
/// amount, and the population median, by that multiplier. (1.0, 0.0) is
/// bit-identical to the unperturbed shift: `AMOUNT_LOG_SD * 1.0` is exact and
/// adding 0.0 changes at most the sign of a zero, which `exp` and addition
/// ignore.
#[inline]
pub fn amount_log_shift_p(id: u64, seed: i64, sd_mult: f64, log_mu_shift: f64) -> f64 {
    (AMOUNT_LOG_SD * sd_mult) * hash_normal(id, seed + 1010) - 0.5 * AMOUNT_LOG_SD * AMOUNT_LOG_SD
        + log_mu_shift
}

// Accounts-per-entity CDF: 70/22/6/2 -> 1..4 accounts.
const ACCT_CDF: [f64; 4] = [0.70, 0.92, 0.98, 1.00];

pub struct Dimensions {
    pub scale: f64,
    pub population: usize,
    pub corpus_months: i64,
    pub txn_per_entity_per_month: i64,
}

impl Dimensions {
    pub fn total_txns(&self) -> i64 {
        self.population as i64 * self.txn_per_entity_per_month * self.corpus_months
    }
}

pub fn dimensions(scale: f64, corpus_months: i64) -> Dimensions {
    Dimensions {
        scale,
        population: ((111_111.0 * scale).round() as i64).max(100) as usize,
        corpus_months,
        txn_per_entity_per_month: 4,
    }
}

/// searchsorted(cdf, f, side='left'): smallest i with cdf[i] >= f.
#[inline]
fn searchsorted_left(cdf: &[f64], f: f64) -> usize {
    let mut i = 0;
    while i < cdf.len() && cdf[i] < f {
        i += 1;
    }
    i.min(cdf.len() - 1)
}

#[inline]
pub fn entity_type(id: u64, seed: i64) -> i8 {
    let f = hash_frac(id, seed + 101);
    if f < 0.55 {
        TYPE_PERSON
    } else if f < 0.95 {
        TYPE_COMPANY
    } else {
        TYPE_FI
    }
}

#[inline]
pub fn home_country_idx(id: u64, seed: i64) -> usize {
    // Cumulative distribution of normalised home weights.
    let total: f64 = HOME_WEIGHTS.iter().sum();
    let mut cdf = [0.0f64; 18];
    let mut acc = 0.0;
    for i in 0..18 {
        acc += HOME_WEIGHTS[i] / total;
        cdf[i] = acc;
    }
    let f = hash_frac(id, seed + 202);
    searchsorted_left(&cdf, f)
}

#[inline]
pub fn ring_size(id: u64, ty: i8, seed: i64) -> i64 {
    let f = hash_frac(id, seed + 303);
    let (lo, hi) = RING_RANGE[ty as usize];
    (lo + f * (hi - lo)) as i64
}

/// ring(orig)[slot] as a pure function of (seed, orig, slot). == world.ring_member.
#[inline]
pub fn ring_member(orig: u64, slot: u64, population: usize, seed: i64) -> u64 {
    let key = splitmix64(orig ^ (seed as u64).wrapping_add(404))
        ^ splitmix64(slot ^ (seed as u64).wrapping_add(505));
    let mut out = (splitmix64(key) % population as u64) + 1;
    if out == orig {
        out = (out % population as u64) + 1;
    }
    out
}

#[inline]
pub fn accounts_for(id: u64, seed: i64) -> i32 {
    let f = hash_frac(id, seed + 808);
    (searchsorted_left(&ACCT_CDF, f) + 1) as i32
}

/// Exact-quota flagged ids (1-based), matching world.flagged_ids: hash every id,
/// take the n with the smallest hash. n = round(rate * population).
fn flagged_ids(population: usize, rate: f64, salt: i64) -> Vec<u64> {
    let n = (rate * population as f64).round() as usize;
    if n == 0 {
        return Vec::new();
    }
    let saltv = (salt as u64).wrapping_mul(0xDEAD_BEEF);
    let mut h: Vec<(u64, u64)> = (1..=population as u64)
        .map(|id| (splitmix64(id ^ saltv), id))
        .collect();
    h.sort_unstable();
    h.into_iter().take(n).map(|(_, id)| id).collect()
}

pub fn sanctioned_set(population: usize, seed: i64) -> Vec<bool> {
    let mut m = vec![false; population + 1];
    for id in flagged_ids(population, SANCTIONS_RATE, seed + 606) {
        m[id as usize] = true;
    }
    m
}

pub fn pep_set(population: usize, seed: i64) -> Vec<bool> {
    let mut m = vec![false; population + 1];
    for id in flagged_ids(population, PEP_RATE, seed + 707) {
        m[id as usize] = true;
    }
    m
}
