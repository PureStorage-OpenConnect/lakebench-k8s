//! Regular (scheduled) baseline sends, AML-GOALS D2.
//!
//! Every baseline send used to be an independent draw of an originator by
//! activity weight, so each account's sends were a memoryless process on the
//! calendar: gap CV about 1 for everyone (0.004% of the D2 cohort below 0.5).
//! Real payment behaviour is a mixture: some accounts pay mostly on a steady
//! cadence (standing orders, bills, payroll and supplier runs) and others are
//! bursty. Here a share of accounts is "scheduled": most of its expected send
//! volume becomes a steady cadence (one payment every 1/K of calendar mass,
//! rolled to the next business day), and only the rest is drawn at random. The account's
//! expected total sends, amounts and counterparty draws do not change; only
//! when it sends.
//!
//! Scheduled events are not stored: an account's k-th event time is a pure
//! function of (seed, account, k), and accounts with the same event count K
//! share a class whose events are enumerated per file by time range, so a pod
//! holds O(classes + scheduled accounts) state at any scale and every file's
//! content still depends only on (seed, file).
//!
//! Parameters are self-chosen (AML-GOALS D2 anchor pending a citation).

use crate::hash::splitmix64;

/// Share of accounts whose sends are mostly scheduled.
pub const SCHEDULED_ACCOUNT_SHARE: f64 = 0.30;
/// Range of a scheduled account's expected sends that follow its cadence.
pub const SCHEDULED_FRACTION_LO: f64 = 0.75;
pub const SCHEDULED_FRACTION_HI: f64 = 0.95;
/// Fewest scheduled events that make a cadence.
pub const MIN_EVENTS: u64 = 4;

#[inline]
fn frac(id: u64, seed: i64, salt: u64) -> f64 {
    (splitmix64(id ^ splitmix64((seed as u64) ^ salt)) >> 11) as f64 / (1u64 << 53) as f64
}

/// Accounts sharing one scheduled event count.
pub struct Class {
    /// Scheduled events per account over the corpus.
    pub k: u64,
    /// Members in phase order.
    pub members: Vec<u64>,
    /// Phase offset of the class, in [0, 1).
    pub phase: f64,
    /// First uid of the class's events (uids follow the random base rows).
    pub uid_base: u64,
}

pub struct Regular {
    pub classes: Vec<Class>,
    /// Per-account sampling weight for the random (unscheduled) base rows.
    pub residual_weight: Vec<f64>,
    /// Total scheduled events.
    pub n_sched: u64,
    /// Random base rows (n_base - n_sched); scheduled uids start here.
    pub n_rand: u64,
}

/// Expected sends of account `a` over the corpus if every base row is an
/// activity-weighted draw.
#[inline]
fn expected(activity: f64, total_w: f64, n_base: u64) -> f64 {
    n_base as f64 * activity / total_w
}

/// Scheduled event count for account `a` (0 = not scheduled).
pub fn scheduled_events(a: u64, activity: f64, total_w: f64, n_base: u64, seed: i64) -> u64 {
    if frac(a, seed, 0xD2_0001) >= SCHEDULED_ACCOUNT_SHARE {
        return 0;
    }
    let s = SCHEDULED_FRACTION_LO
        + (SCHEDULED_FRACTION_HI - SCHEDULED_FRACTION_LO) * frac(a, seed, 0xD2_0002);
    let k = (s * expected(activity, total_w, n_base)).round() as u64;
    if k < MIN_EVENTS {
        0
    } else {
        k
    }
}

impl Regular {
    /// `activity[0]` is unused (ids start at 1). `n_base` is the number of
    /// baseline rows the corpus holds.
    pub fn build(activity: &[f64], n_base: u64, seed: i64) -> Regular {
        let pop = activity.len().saturating_sub(1);
        let total_w: f64 = activity[1..].iter().sum();
        let mut residual_weight = activity.to_vec();
        let mut by_k: std::collections::BTreeMap<u64, Vec<(u64, u64)>> = Default::default();
        let mut n_sched = 0u64;
        if total_w > 0.0 && n_base > 0 {
            for a in 1..=pop as u64 {
                let act = activity[a as usize];
                let k = scheduled_events(a, act, total_w, n_base, seed);
                if k == 0 {
                    continue;
                }
                let e = expected(act, total_w, n_base);
                // The random draws make up the rest of the account's expected
                // sends, so its total is unchanged.
                residual_weight[a as usize] = act * ((e - k as f64).max(0.0) / e);
                n_sched += k;
                by_k.entry(k)
                    .or_default()
                    .push((splitmix64(a ^ splitmix64((seed as u64) ^ 0xD2_0003)), a));
            }
        }
        // Never more scheduled events than baseline rows (only possible at
        // degenerate scales); drop scheduling entirely then.
        if n_sched > n_base {
            return Regular {
                classes: Vec::new(),
                residual_weight: activity.to_vec(),
                n_sched: 0,
                n_rand: n_base,
            };
        }
        let n_rand = n_base - n_sched;
        let mut uid_base = n_rand;
        let classes = by_k
            .into_iter()
            .map(|(k, mut m)| {
                // Phase order is a hash of the id, so phase does not follow id.
                m.sort_unstable();
                let members: Vec<u64> = m.into_iter().map(|(_, a)| a).collect();
                let c = Class {
                    k,
                    phase: frac(k, seed, 0xD2_0004),
                    uid_base,
                    members,
                };
                uid_base += c.k * c.members.len() as u64;
                c
            })
            .collect();
        Regular {
            classes,
            residual_weight,
            n_sched,
            n_rand,
        }
    }
}

impl Class {
    /// Events in the class.
    pub fn len(&self) -> u64 {
        self.k * self.members.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Nominal calendar mass of event `j`: account `members[j % n]`'s
    /// (j / n)-th payment. Events are evenly spaced in calendar MASS, the
    /// same measure baseline and typology rows are placed by, so scheduled
    /// rows keep the corpus's day-of-week, salary-day and quarter-end shape
    /// (evenly spaced wall-clock times would under-weight salary days and pile
    /// weekend slots onto Mondays, and typology rows would then stand out).
    #[inline]
    pub fn nominal_mass(&self, j: u64) -> f64 {
        (j as f64 + self.phase) / self.len() as f64
    }

    #[inline]
    pub fn account(&self, j: u64) -> u64 {
        self.members[(j % self.members.len() as u64) as usize]
    }

    /// Payment number of event `j` within its account's cadence.
    #[inline]
    pub fn ordinal(&self, j: u64) -> u64 {
        j / self.members.len() as u64
    }

    /// Events whose nominal mass lies in [lo, hi).
    pub fn events_between(&self, lo: f64, hi: f64) -> (u64, u64) {
        let n = self.len();
        if n == 0 || hi <= lo {
            return (0, 0);
        }
        let to_j = |m: f64| -> u64 { ((m * n as f64 - self.phase).ceil().max(0.0) as u64).min(n) };
        let (mut a, mut b) = (to_j(lo), to_j(hi));
        // Float edges: settle on exact nominal masses.
        while a > 0 && self.nominal_mass(a - 1) >= lo {
            a -= 1;
        }
        while a < n && self.nominal_mass(a) < lo {
            a += 1;
        }
        while b > 0 && self.nominal_mass(b - 1) >= hi {
            b -= 1;
        }
        while b < n && self.nominal_mass(b) < hi {
            b += 1;
        }
        (a, b.max(a))
    }
}
