//! The static world: per-entity attributes as pure functions of (seed, id),
//! bit-faithful to datagen_v2/world.py.

use crate::hash::{hash_frac, splitmix64};

pub const TYPE_PERSON: i8 = 0;
pub const TYPE_COMPANY: i8 = 1;
pub const TYPE_FI: i8 = 2;
pub const TYPE_LABELS: [&str; 3] = ["Person", "Company", "FI"];

// Entity type shares: Person 0.55, Company 0.40, FI 0.05 (cumulative 0.55, 0.95).
pub const HOME_CODES: [&str; 18] = [
    "US", "GB", "DE", "FR", "CA", "MX", "CN", "IN", "JP", "SG", "CH", "BR", "AE", "AU",
    "HK", "KR", "NL", "ES",
];
const HOME_WEIGHTS: [f64; 18] = [
    0.88, 0.015, 0.010, 0.010, 0.015, 0.010, 0.008, 0.008, 0.005, 0.005, 0.005, 0.005,
    0.005, 0.003, 0.002, 0.002, 0.001, 0.001,
];

// Ring size range by type (lo, hi).
const RING_RANGE: [(f64, f64); 3] = [(3.0, 15.0), (50.0, 500.0), (500.0, 5000.0)];
// Baseline activity weight by type.
pub const BASELINE_ACTIVITY: [f64; 3] = [60.0, 240.0, 1500.0];
// Share of transactions whose beneficiary is drawn from the originator's ring.
pub const RING_HIT_RATE: [f64; 3] = [0.95, 0.80, 0.60];

pub const SANCTIONS_RATE: f64 = 0.0005;
pub const PEP_RATE: f64 = 0.0020;

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
