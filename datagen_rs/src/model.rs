//! Precomputed per-entity world, the Rust analogue of emit.World. Every vector
//! is indexed by entity_id (index 0 is an unused sentinel).

use rayon::prelude::*;

use crate::arena::ArenaCol;
use crate::ids::{bic_pool, iban_for, lei_for};
use crate::kyc::entity_bic_idx;
use crate::realism as R;
use crate::world as W;

/// Stamped on every party, account and manifest row. 0.2 is the first
/// version with the monitored population and KYC (party.is_customer etc.):
/// silver_build_financial refuses to write NULL KYC for a 0.2+ corpus whose
/// party/account files are missing. Bump on any change readers must detect.
pub const MODEL_VERSION: &str = "datagen-v2-rs-0.2";

pub struct World {
    pub seed: i64,
    pub population: usize,
    pub dims: W::Dimensions,
    pub ty: Vec<i8>,
    pub country: Vec<&'static str>,
    // Hot in the emit gather (per row: name, street, town for both parties).
    // Stored as contiguous arenas so a lookup is two adjacent u32 reads plus
    // a slice into a flat byte buffer, with no per-entry pointer chase into
    // the heap. See crate::arena for the layout rationale.
    pub name: ArenaCol,
    pub street: ArenaCol,
    pub town: ArenaCol,
    // Reference-only columns: still Vec<String> because the party.rs builders
    // consume them once each and .clone() them into the Arrow batch; there is
    // no random gather over an 11M-entry column here.
    pub region: Vec<String>,
    pub postcode: Vec<String>,
    pub email: Vec<String>,
    pub iban: Vec<String>,
    pub lei: Vec<String>,
    pub bic: Vec<String>,
    pub ccy: Vec<&'static str>,
    pub sanctioned: Vec<bool>,
    pub pep: Vec<bool>,
    pub n_accounts: Vec<i32>,
    pub ring_sz: Vec<i64>,
    /// Per-entity activity rate: BASELINE_ACTIVITY[type] * persona rate_mult(id).
    /// Drives activity-weighted originator sampling, so each account has its own
    /// consistent cadence (a per-account Poisson process) rather than a per-type
    /// constant. Always built (the bronze emit path samples originators).
    pub activity: Vec<f64>,
    /// Sum of `activity` over the population, so a declared expected volume can
    /// use each entity's share of the send volume.
    pub total_activity: f64,
    /// Per-entity additive shift to the log-normal amount mean (persona). Always
    /// built: the bronze base-amount draw reads it per row.
    pub amount_logshift: Vec<f64>,
    pub ring_hit: Vec<f64>,
    pub bic_pool: Vec<String>,
}

pub fn build_world(scale: f64, seed: i64, corpus_months: i64) -> World {
    build_world_ex(scale, seed, corpus_months, false)
}

/// `bronze_only` skips the columns the pacs.008 emit path never reads
/// (region, postcode, email, sanctioned, pep, n_accounts). Those exist only for
/// the party/account reference tables, so a dedicated-bronze pod that does not
/// write the reference zones pays nothing for them -- at scale 100 that removes
/// roughly six full-population passes (email being the costly one) from every
/// bronze pod, which is the dominant fixed per-pod cost.
pub fn build_world_ex(scale: f64, seed: i64, corpus_months: i64, bronze_only: bool) -> World {
    let dims = W::dimensions(scale, corpus_months);
    let n = dims.population;
    let pool = bic_pool();

    // Per-entity attributes are independent, so build every column in parallel.
    // Vec<String> columns allocate a struct-triple per entry plus one heap
    // buffer each; the arena columns transiently hold their producer's
    // Vec<String> before compacting (see crate::arena module docs). Peak
    // memory during build is therefore roughly 2-3x the final world for the
    // arena columns, not 1x. Pod memory limits must budget for this.
    let sentinel = |first: &str, f: &(dyn Fn(usize) -> String + Sync)| -> Vec<String> {
        (0..=n)
            .into_par_iter()
            .map(|i| if i == 0 { first.to_string() } else { f(i) })
            .collect()
    };

    let ty: Vec<i8> = (0..=n)
        .into_par_iter()
        .map(|i| {
            if i == 0 {
                0
            } else {
                W::entity_type(i as u64, seed)
            }
        })
        .collect();
    let country: Vec<&'static str> = (0..=n)
        .into_par_iter()
        .map(|i| {
            if i == 0 {
                "US"
            } else {
                W::HOME_CODES[W::home_country_idx(i as u64, seed)]
            }
        })
        .collect();
    let ccy: Vec<&'static str> = country
        .par_iter()
        .map(|c| R::currency_for_country(c))
        .collect();
    // Build the hot per-entity strings into an arena. The parallel producer
    // stays as-is (build cost is ~1% of total), the from_vec is one sequential
    // pass; the arena is what emit reads at scale.
    let names_vec: Vec<String> = (0..=n)
        .into_par_iter()
        .map(|i| {
            if i == 0 {
                return "_".to_string();
            }
            let id = i as u64;
            match ty[i] {
                W::TYPE_PERSON => R::person_name(id, seed),
                W::TYPE_COMPANY => R::company_name(id, country[i], seed),
                _ => R::fi_name(id, seed),
            }
        })
        .collect();
    let name = ArenaCol::from_vec(names_vec);
    let street = ArenaCol::build_par(n, |i| {
        if i == 0 {
            "_".into()
        } else {
            R::street(i as u64, country[i], seed)
        }
    });
    let town = ArenaCol::build_par(n, |i| {
        if i == 0 {
            "_".into()
        } else {
            R::city(i as u64, country[i], seed)
        }
    });
    // Reference-only columns: skipped for a dedicated-bronze pod.
    let region = if bronze_only {
        Vec::new()
    } else {
        sentinel("_", &|i| R::region(i as u64, country[i], seed))
    };
    let postcode = if bronze_only {
        Vec::new()
    } else {
        sentinel("_", &|i| R::postcode(i as u64, country[i], seed))
    };
    let email: Vec<String> = if bronze_only {
        Vec::new()
    } else {
        (0..=n)
            .into_par_iter()
            .map(|i| {
                if i == 0 {
                    "_".to_string()
                } else {
                    R::email(name.get(i), i as u64, country[i], seed)
                }
            })
            .collect()
    };
    // The bronze emit path recomputes IBAN/LEI/BIC per row (ids::iban_into etc.),
    // so a dedicated-bronze pod skips materialising these three 11M-entry
    // columns too. The reference pod still needs them for party/account.
    let iban = if bronze_only {
        Vec::new()
    } else {
        sentinel("_", &|i| {
            let cc =
                crate::kyc::account_country(crate::kyc::is_customer(i as u64, seed), country[i]);
            iban_for(&[cc.as_bytes()[0], cc.as_bytes()[1]], i as u64)
        })
    };
    let lei = if bronze_only {
        Vec::new()
    } else {
        sentinel("_", &|i| lei_for(i as u64))
    };
    let bic: Vec<String> = if bronze_only {
        Vec::new()
    } else {
        (0..=n)
            .into_par_iter()
            .map(|i| pool[entity_bic_idx(i as u64, seed, pool.len())].clone())
            .collect()
    };
    let n_accounts: Vec<i32> = if bronze_only {
        Vec::new()
    } else {
        (0..=n)
            .into_par_iter()
            .map(|i| {
                if i == 0 {
                    0
                } else {
                    W::accounts_for(i as u64, seed)
                }
            })
            .collect()
    };
    let ring_sz: Vec<i64> = (0..=n)
        .into_par_iter()
        .map(|i| {
            if i == 0 {
                0
            } else {
                W::ring_size(i as u64, ty[i], seed)
            }
        })
        .collect();
    // Per-entity activity rate = per-type base * persona multiplier, so accounts
    // have individual cadences. Indexed (not par_iter over ty) because rate_mult
    // needs the entity id. Index 0 is the unused sentinel.
    let activity: Vec<f64> = (0..=n)
        .into_par_iter()
        .map(|i| {
            let t = ty[i];
            if i == 0 || t < 0 {
                0.0
            } else {
                W::BASELINE_ACTIVITY[t as usize] * W::rate_mult(i as u64, seed)
            }
        })
        .collect();
    // Per-entity persona amount shift (recentred, mean-preserving).
    let amount_logshift: Vec<f64> = (0..=n)
        .into_par_iter()
        .map(|i| {
            if i == 0 {
                0.0
            } else {
                W::amount_log_shift(i as u64, seed)
            }
        })
        .collect();
    let total_activity: f64 = activity.iter().sum();
    let ring_hit: Vec<f64> = ty
        .par_iter()
        .map(|&t| {
            if t < 0 {
                0.0
            } else {
                W::RING_HIT_RATE[t as usize]
            }
        })
        .collect();

    World {
        seed,
        population: n,
        dims,
        ty,
        country,
        name,
        street,
        town,
        region,
        postcode,
        email,
        iban,
        lei,
        bic,
        ccy,
        sanctioned: if bronze_only {
            Vec::new()
        } else {
            W::sanctioned_set(n, seed)
        },
        pep: if bronze_only {
            Vec::new()
        } else {
            W::pep_set(n, seed)
        },
        n_accounts,
        ring_sz,
        activity,
        total_activity,
        amount_logshift,
        ring_hit,
        bic_pool: pool,
    }
}
