//! The reporting FI, its monitored population, and minimal KYC (GOALS P10
//! stages 0 and 2).
//!
//! A bank's transaction monitoring runs on its own customers; everyone else in
//! its payment data is a counterparty at another bank. Before this module the
//! corpus had no institution concept, so every entity was implicitly monitored.
//! Now one US bank group is the reporting FI, half the population are its
//! customers, and each customer carries the KYC attributes a customer risk
//! rating (CRR) is computed from.
//!
//! Every attribute here is a pure function of (id, seed) plus, for the declared
//! volume, the world's activity total. None depends on file layout, codec,
//! thread or node count (GOALS P4.1).
//!
//! Leakage (AML-GOALS R1, D5). Customer status is drawn independently of every
//! other attribute. Typology instances only force their subject role to be a
//! customer (typology::enforce_subject); nothing selects on PEP, tenure,
//! declared volume or tier. The CRR is one deterministic method applied to
//! everyone, never drawn per label, so a typology subject's tier has the same
//! distribution as a baseline customer's from the same source pool.

use crate::amounts::{LN_MU, LN_SIGMA};
use crate::hash::{hash_frac, splitmix64};
use crate::world::{hash_normal, TYPE_COMPANY, TYPE_FI, TYPE_PERSON};

/// BIC8 of the reporting FI's bank group. Its two BIC11s sit at these indices of
/// `ids::bic_pool()` (`MERIUS2LXXX` head office, `MERIUS2LNYC` branch); a test
/// pins both.
pub const REPORTING_FI: &str = "MERIUS2L";
pub const REPORTING_FI_POOL_IDX: [usize; 2] = [0, 320];

/// Share of the population that are the reporting FI's customers. At 0.50,
/// 75% of payments have a customer on at least one side (1 - 0.5^2); a small
/// share would leave most of the corpus invisible to the bank. It also bounds
/// how much a depth-2 feature pair can gain from is_customer: at most 1 / 0.5.
pub const CUSTOMER_RATE: f64 = 0.50;

/// Countries on the FATF grey or black list among the generator's home codes.
/// Mirrors `spark/data/aml/high_risk_jurisdictions.json` (a drift test checks
/// every code here is in that file, and that no other home code is).
pub const FATF_LISTED: [&str; 1] = ["AE"];

const DOMESTIC: &str = "US";

/// Mean customer tenure before the corpus starts, in years (exponential), and
/// its cap. Self-chosen: a book of long-standing customers with a tail of
/// recent ones.
const TENURE_MEAN_YEARS: f64 = 8.0;
const TENURE_CAP_YEARS: f64 = 30.0;

/// Log-sd of the error between a customer's declared expected monthly volume
/// and its actual persona. Onboarding declarations are rough estimates, so the
/// declared figure is not a clean copy of behaviour.
const DECLARATION_LOG_SD: f64 = 0.5;

#[inline]
pub fn is_customer(id: u64, seed: i64) -> bool {
    hash_frac(id, seed + 1212) < CUSTOMER_RATE
}

/// Pool index of the bank that holds `id`'s accounts. Customers bank with the
/// reporting FI (head office or branch, by an id hash). A non-customer whose
/// hashed index lands on a reporting-FI entry moves to the next entry, so the
/// reporting FI's BIC appears as a debtor or creditor agent exactly when that
/// party is a customer.
#[inline]
pub fn entity_bic_idx(id: u64, seed: i64, pool_len: usize) -> usize {
    bic_idx_for(id, is_customer(id, seed), pool_len)
}

/// `entity_bic_idx` with the customer flag already known (the emit hot path
/// computes it once per party per row).
#[inline]
pub fn bic_idx_for(id: u64, customer: bool, pool_len: usize) -> usize {
    if customer {
        return REPORTING_FI_POOL_IDX[(splitmix64(id ^ 0xB1C1) & 1) as usize];
    }
    own_bic_idx(id, pool_len)
}

/// Pool index of an FI entity's own BIC (its identity as an institution),
/// never a reporting-FI entry.
#[inline]
pub fn own_bic_idx(id: u64, pool_len: usize) -> usize {
    let i = crate::ids::bic_idx(id, pool_len);
    if REPORTING_FI_POOL_IDX.contains(&i) {
        (i + 1) % pool_len
    } else {
        i
    }
}

/// Country of the entity's accounts, and so of its IBANs. A customer's
/// accounts are held at the reporting FI, a US bank, so they are US accounts
/// whatever the holder's residence (a foreign resident banking in the US);
/// everyone else's accounts are in their home country.
#[inline]
pub fn account_country(customer: bool, home: &'static str) -> &'static str {
    if customer {
        DOMESTIC
    } else {
        home
    }
}

/// BIC8 (bank group) of a BIC11.
#[inline]
pub fn home_fi(bic: &str) -> &str {
    &bic[..8.min(bic.len())]
}

/// Days since 1970-01-01 for a civil date.
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// First day of the corpus: it ends 2026-01-01 and spans `corpus_months`
/// (same arithmetic as bin/generate.rs).
pub fn corpus_start_day(corpus_months: i64) -> i64 {
    let start_year = 2026 - (corpus_months + 11) / 12;
    let start_month = ((12 - (corpus_months % 12)) % 12) + 1;
    days_from_civil(start_year, start_month, 1)
}

/// Days of an exponential tenure (mean TENURE_MEAN_YEARS, capped) from `u`.
fn tenure_days(u: f64) -> i64 {
    ((-(1.0 - u).ln() * TENURE_MEAN_YEARS).min(TENURE_CAP_YEARS) * 365.25) as i64
}

/// Onboarding date: an exponential tenure before the corpus starts. Every
/// customer is on the book when the corpus begins, so no entity changes bank
/// mid-corpus.
pub fn customer_since_day(id: u64, seed: i64, corpus_months: i64) -> i32 {
    let d = corpus_start_day(corpus_months) - 1 - tenure_days(hash_frac(id, seed + 1414));
    d as i32
}

/// Opening date of the entity's payment account (its first account, the one
/// the pacs.008 rows use). It is open before the corpus starts, so before the
/// first payment. For a customer it is on or after customer_since (the
/// account is opened at onboarding or later); for anyone else it is an
/// exponential tenure before the corpus start.
pub fn primary_opened_day(id: u64, seed: i64, corpus_months: i64) -> i32 {
    let start = corpus_start_day(corpus_months);
    let d = if is_customer(id, seed) {
        let since = customer_since_day(id, seed, corpus_months) as i64;
        since + (hash_frac(id, seed + 1515) * (start - since) as f64) as i64
    } else {
        start - 1 - tenure_days(hash_frac(id, seed + 1616))
    };
    (d.min(start - 1)) as i32
}

/// Opening date of a further account (seq > 0): 2020-01-01 plus up to six
/// years, but never before the payment account.
#[inline]
pub fn account_opened_day(id: u64, primary_opened: i32) -> i32 {
    (18262 + (splitmix64(id ^ 0x0DA7E) % 2191) as i32).max(primary_opened)
}

pub fn customer_type(ty: i8) -> &'static str {
    if ty == TYPE_PERSON {
        "person"
    } else {
        "business"
    }
}

/// Declared expected monthly outgoing volume in USD: the persona's expected
/// sends per month times its mean amount, times a declaration error. Uses the
/// persona only, never realised rows, so planted typology rows cannot move it.
/// `activity_share` is activity[id] / sum(activity).
pub fn expected_monthly_volume_usd(
    id: u64,
    seed: i64,
    amount_logshift: f64,
    activity_share: f64,
    population: usize,
    txn_per_entity_per_month: i64,
) -> f64 {
    let sends = txn_per_entity_per_month as f64 * population as f64 * activity_share;
    let mean_amt = (LN_MU + amount_logshift + 0.5 * LN_SIGMA * LN_SIGMA).exp();
    let err = (DECLARATION_LOG_SD * hash_normal(id, seed + 1313)).exp();
    let v = sends * mean_amt * err;
    (v * 100.0).round() / 100.0
}

/// Customer risk rating: a documented points model (self-chosen weights, no
/// external anchor).
///
/// - country: 0 domestic (US), 1 other, 2 FATF-listed
/// - customer type: 0 person, 1 company, 2 financial institution
/// - declared expected monthly volume: 0 below $10K, 1 below $100K, 2 above
///
/// Score is the sum (0..6); tier low for 0-1, medium for 2-3, high for 4+.
/// PEP overrides the tier to high: PEPs require enhanced due diligence (FATF
/// Recommendation 12). Returns (score, tier, factors string).
pub fn crr(
    ty: i8,
    country: &str,
    pep: bool,
    expected_volume_usd: f64,
) -> (i32, &'static str, String) {
    let c = if country == DOMESTIC {
        0
    } else if FATF_LISTED.contains(&country) {
        2
    } else {
        1
    };
    let t = match ty {
        TYPE_FI => 2,
        TYPE_COMPANY => 1,
        _ => 0,
    };
    let v = if expected_volume_usd < 10_000.0 {
        0
    } else if expected_volume_usd < 100_000.0 {
        1
    } else {
        2
    };
    let score = c + t + v;
    let tier = if pep || score >= 4 {
        "high"
    } else if score >= 2 {
        "medium"
    } else {
        "low"
    };
    let factors = format!("country={};type={};volume={};pep={}", c, t, v, pep as i32);
    (score, tier, factors)
}
