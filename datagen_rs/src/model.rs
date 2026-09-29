//! Precomputed per-entity world, the Rust analogue of emit.World.
//!
//! LB-204 (datagen per-pod memory redesign, Tier-1): the world no longer
//! materialises the full-population string/attribute columns. Every one of
//! those columns is a pure function of `(id, seed)` (and, for the amount
//! shift, the persona perturbation), so it is recomputed on demand through the
//! methods below instead of being held resident. Only the genuinely-global
//! sampler inputs stay materialised: `activity` and its `total_activity` sum
//! (the Tier-2 targets). This keeps per-pod memory from scaling with the
//! name/street/town/region/postcode/email/iban/lei/bic/ccy/country/ty/
//! ring_sz/ring_hit/amount_logshift columns, while the corpus stays
//! byte-identical (the recompute uses the same functions the columns were
//! built from).

use crate::ids::{bic_pool, iban_for, lei_for};
use crate::kyc::entity_bic_idx;
use crate::realism as R;
use crate::robustness::Perturbation;
use crate::world as W;

/// Stamped on every party, account and manifest row. 0.2 is the first
/// version with the monitored population and KYC (party.is_customer etc.):
/// silver_build_financial refuses to write NULL KYC for a 0.2+ corpus whose
/// party/account files are missing. Bump on any change readers must detect.
///
/// 0.3 (AML generator freeze, AML-GOALS #50 and #44): the sanctions and PEP
/// screening track (bronze/watchlist.parquet, planted sanctions_match and
/// pep_match instances) and the answer keys moved out of the party zone
/// (no sanctions_status, pep_status or initial_risk_score; crr ignores PEP).
/// A pre-freeze corpus carries 0.2 and must not pass as current.
pub const MODEL_VERSION: &str = "datagen-v2-rs-0.3";

/// The static per-entity world. After LB-204 this holds only the sampler
/// inputs; every attribute column is recomputed on demand (see the methods
/// below), so its size no longer scales with the population's string columns.
pub struct World {
    pub seed: i64,
    pub population: usize,
    pub dims: W::Dimensions,
    /// Persona perturbation inputs needed to recompute `amount_logshift(id)`
    /// on demand. `sd_mult` (== `perturb.persona_sd`) and the log-mean shift
    /// (== `perturb.amount_log_mu_shift()`), captured at build time so the
    /// recompute reproduces the exact value the materialised column held.
    persona_sd: f64,
    amount_log_mu_shift: f64,
    /// Per-entity activity rate: BASELINE_ACTIVITY[type] * persona rate_mult(id).
    /// Drives activity-weighted originator sampling. STAYS materialised
    /// (Tier-2 target): the sampler prefix sum consumes it and `total_activity`
    /// is a strictly ordered ascending-id sum of it (freeze-void landmine).
    pub activity: Vec<f64>,
    /// Sum of `activity` over the population, so a declared expected volume can
    /// use each entity's share of the send volume.
    pub total_activity: f64,
    pub bic_pool: Vec<String>,
    /// External counterparties above the population (crate::screening):
    /// listed parties and their namesake decoys. Never customers, never in
    /// the party or account zones; only the pacs.008 emit reads them, as a
    /// beneficiary. Empty until `attach_external`. Small (bounded by the
    /// watchlist size), so these stay materialised.
    pub ext_first: usize,
    pub ext_name: Vec<String>,
    pub ext_street: Vec<String>,
    pub ext_town: Vec<String>,
    pub ext_country: Vec<&'static str>,
}

impl World {
    /// Register external counterparties with ids `population + 1 ..`, in
    /// order. Leaves every population attribute untouched.
    pub fn attach_external(&mut self, ents: Vec<(String, String, String, &'static str)>) {
        self.ext_first = self.population + 1;
        for (n, s, t, c) in ents {
            self.ext_name.push(n);
            self.ext_street.push(s);
            self.ext_town.push(t);
            self.ext_country.push(c);
        }
    }

    #[inline]
    pub fn is_external(&self, id: usize) -> bool {
        id > self.population
    }

    // --- Recomputed per-entity attributes (pure functions of (id, seed)) ----
    // Each reproduces, bit for bit, the value the former materialised column
    // held at index `id`. Index 0 keeps its old sentinel value so a debug dump
    // over 0..=n is unchanged; real entities are 1..=population.

    #[inline]
    pub fn ty(&self, id: usize) -> i8 {
        if id == 0 {
            0
        } else {
            W::entity_type(id as u64, self.seed)
        }
    }

    #[inline]
    pub fn country(&self, id: usize) -> &'static str {
        if id == 0 {
            "US"
        } else {
            W::HOME_CODES[W::home_country_idx(id as u64, self.seed)]
        }
    }

    #[inline]
    pub fn ccy(&self, id: usize) -> &'static str {
        R::currency_for_country(self.country(id))
    }

    #[inline]
    pub fn ring_sz(&self, id: usize) -> i64 {
        if id == 0 {
            0
        } else {
            W::ring_size(id as u64, self.ty(id), self.seed)
        }
    }

    #[inline]
    pub fn ring_hit(&self, id: usize) -> f64 {
        let t = self.ty(id);
        if t < 0 {
            0.0
        } else {
            W::RING_HIT_RATE[t as usize]
        }
    }

    #[inline]
    pub fn amount_logshift(&self, id: usize) -> f64 {
        if id == 0 {
            0.0
        } else {
            W::amount_log_shift_p(
                id as u64,
                self.seed,
                self.persona_sd,
                self.amount_log_mu_shift,
            )
        }
    }

    #[inline]
    pub fn n_accounts(&self, id: usize) -> i32 {
        if id == 0 {
            0
        } else {
            W::accounts_for(id as u64, self.seed)
        }
    }

    pub fn name(&self, id: usize) -> String {
        if id == 0 {
            return "_".to_string();
        }
        let iid = id as u64;
        match self.ty(id) {
            W::TYPE_PERSON => R::person_name(iid, self.seed),
            W::TYPE_COMPANY => R::company_name(iid, self.country(id), self.seed),
            _ => R::fi_name(iid, self.seed),
        }
    }

    pub fn street(&self, id: usize) -> String {
        if id == 0 {
            "_".to_string()
        } else {
            R::street(id as u64, self.country(id), self.seed)
        }
    }

    pub fn town(&self, id: usize) -> String {
        if id == 0 {
            "_".to_string()
        } else {
            R::city(id as u64, self.country(id), self.seed)
        }
    }

    pub fn region(&self, id: usize) -> String {
        if id == 0 {
            "_".to_string()
        } else {
            R::region(id as u64, self.country(id), self.seed)
        }
    }

    pub fn postcode(&self, id: usize) -> String {
        if id == 0 {
            "_".to_string()
        } else {
            R::postcode(id as u64, self.country(id), self.seed)
        }
    }

    pub fn email(&self, id: usize) -> String {
        if id == 0 {
            "_".to_string()
        } else {
            R::email(&self.name(id), id as u64, self.country(id), self.seed)
        }
    }

    pub fn iban(&self, id: usize) -> String {
        if id == 0 {
            return "_".to_string();
        }
        let cc = crate::kyc::account_country(
            crate::kyc::is_customer(id as u64, self.seed),
            self.country(id),
        );
        iban_for(&[cc.as_bytes()[0], cc.as_bytes()[1]], id as u64)
    }

    pub fn lei(&self, id: usize) -> String {
        if id == 0 {
            "_".to_string()
        } else {
            lei_for(id as u64)
        }
    }

    /// Pool BIC for entity `id`. Matches the former `bic` column, which had no
    /// index-0 sentinel (`bic[0] == pool[entity_bic_idx(0, seed, len)]`).
    pub fn bic(&self, id: usize) -> String {
        self.bic_pool[entity_bic_idx(id as u64, self.seed, self.bic_pool.len())].clone()
    }

    // --- Counterparty helpers (population OR external) ----------------------

    /// Counterparty name for any entity id, population (recomputed) or
    /// external (from the small attached table).
    #[inline]
    pub fn cp_name(&self, id: usize) -> String {
        if id > self.population {
            self.ext_name[id - self.ext_first].clone()
        } else {
            self.name(id)
        }
    }
    #[inline]
    pub fn cp_street(&self, id: usize) -> String {
        if id > self.population {
            self.ext_street[id - self.ext_first].clone()
        } else {
            self.street(id)
        }
    }
    #[inline]
    pub fn cp_town(&self, id: usize) -> String {
        if id > self.population {
            self.ext_town[id - self.ext_first].clone()
        } else {
            self.town(id)
        }
    }
    #[inline]
    pub fn cp_country(&self, id: usize) -> &'static str {
        if id > self.population {
            self.ext_country[id - self.ext_first]
        } else {
            self.country(id)
        }
    }
}

pub fn build_world(scale: f64, seed: i64, corpus_months: i64) -> World {
    build_world_ex(scale, seed, corpus_months, false)
}

/// `bronze_only` is retained for API compatibility but no longer changes the
/// built world: after LB-204 every attribute column is recomputed on demand,
/// so there is nothing for a dedicated-bronze pod to skip materialising.
pub fn build_world_ex(scale: f64, seed: i64, corpus_months: i64, bronze_only: bool) -> World {
    build_world_p(scale, seed, corpus_months, bronze_only, &Perturbation::NONE)
}

/// `build_world_ex` with the robustness perturbation applied to the persona
/// (crate::robustness): activity-rate and amount log-sds, and the median
/// amount. `Perturbation::NONE` builds the unperturbed world bit for bit.
pub fn build_world_p(
    scale: f64,
    seed: i64,
    corpus_months: i64,
    bronze_only: bool,
    perturb: &Perturbation,
) -> World {
    // See build_world_ex: bronze_only is now inert (all columns recomputed).
    let _ = bronze_only;
    let sd_mult = perturb.persona_sd;
    let log_mu_shift = perturb.amount_log_mu_shift();
    let dims = W::dimensions(scale, corpus_months);
    let n = dims.population;
    let pool = bic_pool();

    // The only materialised population column: the activity rate. It feeds the
    // activity-weighted originator sampler (Tier-2 target) and `total_activity`.
    // Built sequentially so `total_activity` below is a strictly ordered
    // ascending-id sum (see the freeze-void guard). The per-entity work is a
    // couple of hashes, so a sequential build is cheap even at scale.
    let mut activity: Vec<f64> = Vec::with_capacity(n + 1);
    for i in 0..=n {
        if i == 0 {
            activity.push(0.0);
            continue;
        }
        let t = W::entity_type(i as u64, seed);
        activity.push(if t < 0 {
            0.0
        } else {
            W::BASELINE_ACTIVITY[t as usize] * W::rate_mult_sd(i as u64, seed, sd_mult)
        });
    }

    // FREEZE-VOID GUARD (DATAGEN-SHARDING-POC REVISION 2): total_activity feeds
    // crr_score/crr_tier in party.parquet (frozen). It MUST be a strictly
    // ordered, ascending-id, sequential f64 sum -- a reordered or parallel
    // reduction can flip a low bit and change party.parquet bytes -> freeze
    // void. `Iterator::sum` folds left-to-right in index order; the explicit
    // loop below re-derives the same ordered sum and the assert pins that the
    // two agree bit for bit, so any future switch to a reordered/parallel sum
    // trips here instead of silently voiding the freeze.
    let total_activity: f64 = activity.iter().sum();
    {
        let mut ordered = 0.0f64;
        for i in 0..=n {
            ordered += activity[i];
        }
        assert_eq!(
            total_activity.to_bits(),
            ordered.to_bits(),
            "total_activity is not a strictly ordered ascending-id sum (freeze-void)"
        );
    }

    World {
        seed,
        population: n,
        dims,
        persona_sd: sd_mult,
        amount_log_mu_shift: log_mu_shift,
        activity,
        total_activity,
        bic_pool: pool,
        ext_first: n + 1,
        ext_name: Vec::new(),
        ext_street: Vec::new(),
        ext_town: Vec::new(),
        ext_country: Vec::new(),
    }
}
