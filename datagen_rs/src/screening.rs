//! Sanctions and PEP screening track (AML-GOALS #50).
//!
//! The generator publishes a synthetic, dated watchlist (a sanctions list in
//! two versions and a PEP list) and plants payments from the reporting FI's
//! customers to listed parties, so W5 (sanctions) and W6 (PEP) are scored for
//! recall and precision against known ground truth like the behavioural rules.
//!
//! Design constraints:
//!
//! - **Byte identity.** Every draw here comes from streams keyed by
//!   `SCREEN_SALT`, never from the typology or base-row streams, and the
//!   planted rows are added on top of the corpus row budget (the base row
//!   count is computed before they exist). Every behavioural row, typology
//!   instance and manifest row is therefore the same at the same seed as a
//!   generator without this module.
//! - **Listed parties are never customers** and are not in the party master.
//!   They are external entities with ids above the population (the
//!   counterparty accounts of another bank). `World::attach_external` gives
//!   the pacs.008 emit their name, address and country.
//! - **No answer key in bronze.** The watchlist is an input a bank has; which
//!   listed party was paid, and by whom, is only in the manifest.
//! - **A real fuzzy screen is needed.** A planted payment names the listed
//!   party as the payer typed it: the list spelling, an alias, a typo, a
//!   token-order swap or a different transliteration. An exact join on the
//!   list name misses the variants.
//! - **Precision means something.** Namesake decoys (a different person or
//!   company whose name is close to a listed one, or the same name in another
//!   country) receive ordinary payments that are not hits. They are not in the
//!   manifest.
//!
//! Listed names use the world's name pools (no vocabulary a detector could key
//! on), but with a full middle name for persons and two heads for companies,
//! so a listed name is not a world entity's name. That structure is shared by
//! the positives and the decoys, so it does not separate them.

use crate::hash::{splitmix64, Rng};
use crate::realism as R;
use crate::timing::{sample_ts_on_day, DayCal};
use crate::typology::Instance;
use crate::world as W;

/// Salt of every screening stream. Distinct from the typology (0xF100...),
/// base (0xBA5E...), scheduled (0xD2D2...) and shaping (0x5A4E...) salts.
pub const SCREEN_SALT: u64 = 0x5C4E_E400_0000_0001;

/// Typology types the screening track writes to the manifest.
pub const SANCTIONS_TYPOLOGY: &str = "sanctions_match";
pub const PEP_TYPOLOGY: &str = "pep_match";

/// Listed parties per entity of population (self-chosen; enough instances for
/// a recall estimate at scale 1, rare next to the corpus: at scale 2 about 90
/// sanctions and 130 PEP entries against 222,222 entities).
const SANCTIONS_V1_RATE: f64 = 4.0e-4;
const SANCTIONS_V1_MIN: usize = 24;
const SANCTIONS_V2_MIN: usize = 8;
const PEP_RATE: f64 = 6.0e-4;
const PEP_MIN: usize = 24;
/// Share of listed parties a customer pays, and of listed parties with a
/// namesake decoy.
const PAID_RATE: f64 = 0.7;
const DECOY_RATE: f64 = 0.5;
/// Where in the corpus list version 2 is published (share of the span).
const V2_AT: f64 = 0.75;

const PROGRAMS: [&str; 6] = ["SDGT", "IRAN", "RUSSIA-EO14024", "DPRK3", "SDNTK", "GLOMAG"];
const POSITIONS: [&str; 7] = [
    "head_of_state",
    "minister",
    "legislator",
    "senior_judge",
    "central_bank_board",
    "soe_executive",
    "ambassador",
];

/// Whole-token romanisation alternatives for names in the world pools.
const TRANSLIT: [(&str, &str); 30] = [
    ("Mohammed", "Muhammad"),
    ("Ahmed", "Ahmad"),
    ("Omar", "Umar"),
    ("Hassan", "Hasan"),
    ("Ibrahim", "Ebrahim"),
    ("Fatima", "Fatimah"),
    ("Aisha", "Ayesha"),
    ("Layla", "Leila"),
    ("Zainab", "Zaynab"),
    ("Al-Saud", "Al Saoud"),
    ("Al-Farsi", "Alfarsi"),
    ("Zhang", "Chang"),
    ("Wang", "Wong"),
    ("Chen", "Chan"),
    ("Li", "Lee"),
    ("Liu", "Lau"),
    ("Zhao", "Chao"),
    ("Wu", "Woo"),
    ("Yang", "Yeung"),
    ("Kim", "Gim"),
    ("Park", "Pak"),
    ("Choi", "Choe"),
    ("Sato", "Satou"),
    ("Yuki", "Yuuki"),
    ("Hiroshi", "Hirosi"),
    ("Diallo", "Jallow"),
    ("Mensah", "Mensa"),
    ("Sharma", "Sarma"),
    ("Khan", "Kahn"),
    ("Jose", "Josef"),
];

/// Generic spelling alternations for a token with no table entry.
const RESPELL: [(&str, &str); 8] = [
    ("ph", "f"),
    ("ck", "k"),
    ("ee", "i"),
    ("oo", "u"),
    ("th", "t"),
    ("ie", "y"),
    ("ou", "u"),
    ("c", "k"),
];

#[derive(Clone, Debug)]
pub struct Party {
    /// Entity id, above the population: an external counterparty.
    pub ext_id: u64,
    pub list_id: String,
    /// "sanctions" or "pep".
    pub list_type: &'static str,
    /// List version in which the entry first appears (1 or 2).
    pub list_version: i32,
    pub listed_us: i64,
    /// "person" or "company".
    pub entity_type: &'static str,
    pub name: String,
    pub aliases: Vec<String>,
    pub street: String,
    pub town: String,
    pub country: &'static str,
    /// Sanctions programme tag (sanctions entries only).
    pub program: Option<&'static str>,
    /// PEP position (PEP entries only).
    pub position: Option<&'static str>,
}

#[derive(Clone, Debug)]
pub struct Decoy {
    pub ext_id: u64,
    pub name: String,
    pub street: String,
    pub town: String,
    pub country: &'static str,
}

pub struct Screening {
    pub parties: Vec<Party>,
    pub decoys: Vec<Decoy>,
    /// List publication instants (version 1 at corpus start).
    pub v1_published_us: i64,
    pub v2_published_us: i64,
    /// First external id (population + 1); ids run contiguously over parties
    /// then decoys.
    pub first_ext_id: u64,
}

impl Screening {
    /// (name, street, town, country) of every external entity, indexed by
    /// `ext_id - first_ext_id`.
    pub fn external_entities(&self) -> Vec<(String, String, String, &'static str)> {
        let mut v: Vec<(String, String, String, &'static str)> = self
            .parties
            .iter()
            .map(|p| (p.name.clone(), p.street.clone(), p.town.clone(), p.country))
            .collect();
        v.extend(
            self.decoys
                .iter()
                .map(|d| (d.name.clone(), d.street.clone(), d.town.clone(), d.country)),
        );
        v
    }
}

#[inline]
fn stream(seed: i64, a: u64, b: u64) -> Rng {
    Rng::new(splitmix64(
        (seed as u64) ^ SCREEN_SALT ^ splitmix64(a.wrapping_mul(0x9E37_79B9) ^ b),
    ))
}

fn pick<'a>(rng: &mut Rng, pool: &'a [&'a str]) -> &'a str {
    pool[rng.below(pool.len() as u64) as usize]
}

fn person_name(rng: &mut Rng) -> (String, String, String) {
    let first = pick(rng, R::FIRST);
    let mut middle = pick(rng, R::FIRST);
    for _ in 0..8 {
        if middle != first {
            break;
        }
        middle = pick(rng, R::FIRST);
    }
    let last = pick(rng, R::LAST);
    (first.to_string(), middle.to_string(), last.to_string())
}

fn company_name(rng: &mut Rng, cc: &str, id: u64) -> (String, String, String, &'static str) {
    let h1 = pick(rng, R::CORP_HEAD);
    let mut h2 = pick(rng, R::CORP_HEAD);
    for _ in 0..8 {
        if h2 != h1 {
            break;
        }
        h2 = pick(rng, R::CORP_HEAD);
    }
    let d = pick(rng, R::CORP_DESC);
    (
        h1.to_string(),
        h2.to_string(),
        d.to_string(),
        R::legal_suffix(cc, id),
    )
}

/// One edit inside a token of four or more letters: substitute, transpose,
/// delete or double a character. Returns None when no token qualifies.
pub fn typo(name: &str, rng: &mut Rng) -> Option<String> {
    let toks: Vec<&str> = name.split(' ').collect();
    let cand: Vec<usize> = (0..toks.len())
        .filter(|&i| toks[i].len() >= 4 && toks[i].bytes().all(|b| b.is_ascii_alphabetic()))
        .collect();
    if cand.is_empty() {
        return None;
    }
    // A swap of two equal letters is no edit; draw again (bounded).
    for _ in 0..8 {
        let ti = cand[rng.below(cand.len() as u64) as usize];
        let mut t: Vec<u8> = toks[ti].as_bytes().to_vec();
        // Interior positions only, so the first letter survives (as in most
        // real typos, and so a token keeps its initial).
        let p = 1 + rng.below((t.len() - 2) as u64) as usize;
        match rng.below(4) {
            0 => {
                let c = t[p].to_ascii_lowercase();
                let r = if c == b'z' { b'a' } else { c + 1 };
                t[p] = if t[p].is_ascii_uppercase() {
                    r.to_ascii_uppercase()
                } else {
                    r
                };
            }
            1 => t.swap(p, p + 1),
            2 => {
                t.remove(p);
            }
            _ => t.insert(p, t[p]),
        }
        let new = String::from_utf8(t).ok()?;
        if new != toks[ti] {
            let mut out: Vec<String> = toks.iter().map(|s| s.to_string()).collect();
            out[ti] = new;
            return Some(out.join(" "));
        }
    }
    None
}

/// A different romanisation of one token: a table entry when a token has one,
/// else a generic spelling alternation. None when nothing applies.
pub fn transliterate(name: &str, rng: &mut Rng) -> Option<String> {
    let toks: Vec<String> = name.split(' ').map(str::to_string).collect();
    let table: Vec<usize> = (0..toks.len())
        .filter(|&i| TRANSLIT.iter().any(|(a, _)| *a == toks[i]))
        .collect();
    if !table.is_empty() {
        let i = table[rng.below(table.len() as u64) as usize];
        let to = TRANSLIT.iter().find(|(a, _)| *a == toks[i]).unwrap().1;
        let mut out = toks.clone();
        out[i] = to.to_string();
        return Some(out.join(" "));
    }
    let start = rng.below(toks.len() as u64) as usize;
    for k in 0..toks.len() {
        let i = (start + k) % toks.len();
        let lower = toks[i].to_ascii_lowercase();
        for (a, b) in RESPELL.iter() {
            // Never at the start of a token: the initial stays.
            if let Some(pos) = lower[1..].find(a) {
                let pos = pos + 1;
                let mut t = toks[i].clone();
                t.replace_range(pos..pos + a.len(), b);
                let mut out = toks.clone();
                out[i] = t;
                return Some(out.join(" "));
            }
        }
    }
    None
}

/// Build the watchlist and its decoys. Pure function of (population, seed,
/// corpus bounds).
pub fn build(population: usize, seed: i64, start_us: i64, end_us: i64) -> Screening {
    let span = (end_us - start_us).max(1);
    let day = 86_400_000_000i64;
    let v2_us = start_us + ((span as f64 * V2_AT) as i64 / day) * day;
    let n_s1 = ((population as f64 * SANCTIONS_V1_RATE).round() as usize).max(SANCTIONS_V1_MIN);
    let n_s2 = (n_s1 / 4).max(SANCTIONS_V2_MIN);
    let n_p = ((population as f64 * PEP_RATE).round() as usize).max(PEP_MIN);
    let first = population as u64 + 1;
    let mut parties = Vec::with_capacity(n_s1 + n_s2 + n_p);
    let specs = std::iter::repeat_n(("sanctions", 1), n_s1)
        .chain(std::iter::repeat_n(("sanctions", 2), n_s2))
        .chain(std::iter::repeat_n(("pep", 1), n_p));
    let (mut s_no, mut p_no) = (0usize, 0usize);
    for (k, (list_type, version)) in specs.enumerate() {
        let ext_id = first + k as u64;
        let mut rng = stream(seed, 1, k as u64);
        let country = W::HOME_CODES[W::home_country_idx(ext_id, seed ^ 0x5C4E)];
        let is_person = list_type == "pep" || rng.unit() < 0.7;
        let (name, aliases) = if is_person {
            let (f, m, l) = person_name(&mut rng);
            let name = format!("{f} {m} {l}");
            let mut aliases = Vec::new();
            if list_type == "sanctions" && rng.unit() < 0.4 {
                if let Some(a) = transliterate(&name, &mut rng) {
                    aliases.push(a);
                }
            }
            (name, aliases)
        } else {
            let (h1, h2, d, sfx) = company_name(&mut rng, country, ext_id);
            let name = format!("{h1} {h2} {d} {sfx}");
            let mut aliases = Vec::new();
            if rng.unit() < 0.3 {
                aliases.push(format!("{h2} {h1} {d} {sfx}"));
            }
            (name, aliases)
        };
        let list_id = if list_type == "sanctions" {
            s_no += 1;
            format!("LBS-{s_no:06}")
        } else {
            p_no += 1;
            format!("LBP-{p_no:06}")
        };
        let salted = seed ^ 0x5C4E;
        parties.push(Party {
            ext_id,
            list_id,
            list_type,
            list_version: version,
            listed_us: if version == 1 { start_us } else { v2_us },
            entity_type: if is_person { "person" } else { "company" },
            name,
            aliases,
            street: R::street(ext_id, country, salted),
            town: R::city(ext_id, country, salted),
            country,
            program: (list_type == "sanctions").then(|| pick(&mut rng, &PROGRAMS)),
            position: (list_type == "pep").then(|| pick(&mut rng, &POSITIONS)),
        });
    }
    // Namesake decoys, after every party so party ids do not depend on them.
    let mut decoys = Vec::new();
    for (k, p) in parties.iter().enumerate() {
        let mut rng = stream(seed, 2, k as u64);
        if rng.unit() >= DECOY_RATE {
            continue;
        }
        let ext_id = first + parties.len() as u64 + decoys.len() as u64;
        let toks: Vec<&str> = p.name.split(' ').collect();
        let (name, country) = if rng.unit() < 0.5 {
            // Same name, another country.
            let mut c = p.country;
            for _ in 0..16 {
                c = W::HOME_CODES[rng.below(W::HOME_CODES.len() as u64) as usize];
                if c != p.country {
                    break;
                }
            }
            (p.name.clone(), c)
        } else if p.entity_type == "person" {
            // A different person: same given and family name, another middle.
            let mut m = pick(&mut rng, R::FIRST);
            for _ in 0..8 {
                if m != toks[1] && m != toks[0] {
                    break;
                }
                m = pick(&mut rng, R::FIRST);
            }
            (format!("{} {} {}", toks[0], m, toks[2]), p.country)
        } else {
            // A different company: same heads, another line of business.
            let mut d = pick(&mut rng, R::CORP_DESC);
            for _ in 0..8 {
                if d != toks[2] {
                    break;
                }
                d = pick(&mut rng, R::CORP_DESC);
            }
            let sfx = toks[3..].join(" ");
            (format!("{} {} {} {}", toks[0], toks[1], d, sfx), p.country)
        };
        if name == p.name && country == p.country {
            continue;
        }
        let salted = seed ^ 0x5C4E;
        decoys.push(Decoy {
            ext_id,
            name,
            street: R::street(ext_id, country, salted),
            town: R::city(ext_id, country, salted),
            country,
        });
    }
    Screening {
        parties,
        decoys,
        v1_published_us: start_us,
        v2_published_us: v2_us,
        first_ext_id: first,
    }
}

/// One planted screening payment. `bene_name` is the creditor name as the
/// payer typed it.
#[derive(Clone, Debug)]
pub struct ScreenRow {
    pub orig: u64,
    pub bene: u64,
    pub ts_us: i64,
    pub bene_name: String,
    pub variant: &'static str,
}

/// A planted instance (one customer's payments to one listed party) with its
/// manifest-only annotations.
pub struct Planted {
    pub inst: Instance,
    pub rows: Vec<ScreenRow>,
    pub extra: Vec<(&'static str, String)>,
}

/// The creditor name on one planted payment: the list spelling, an alias, or
/// a variant of the list spelling.
fn reported_name(p: &Party, rng: &mut Rng) -> (String, &'static str) {
    let u = rng.unit();
    let v = if u < 0.30 {
        None
    } else if u < 0.45 && !p.aliases.is_empty() {
        let a = &p.aliases[rng.below(p.aliases.len() as u64) as usize];
        Some((a.clone(), "alias"))
    } else if u < 0.62 {
        let toks: Vec<&str> = p.name.split(' ').collect();
        let s = if p.entity_type == "person" {
            // Family name first, as many payment systems format it.
            format!("{} {} {}", toks[2], toks[0], toks[1])
        } else {
            format!("{} {} {}", toks[1], toks[0], toks[2..].join(" "))
        };
        Some((s, "token_order"))
    } else if u < 0.82 {
        typo(&p.name, rng).map(|s| (s, "typo"))
    } else if p.entity_type == "person" {
        transliterate(&p.name, rng).map(|s| (s, "transliteration"))
    } else {
        // Companies: the legal suffix spelled out or left off.
        let toks: Vec<&str> = p.name.split(' ').collect();
        Some((toks[..3].join(" "), "suffix"))
    };
    v.unwrap_or_else(|| (p.name.clone(), "exact"))
}

/// Draw a payment day uniformly by calendar mass in [lo_us, hi_us) and a
/// shaped instant on it for country `cc`; None when the business-day roll
/// leaves the window after a few tries.
fn draw_ts(rng: &mut Rng, cal: &DayCal, lo_us: i64, hi_us: i64, cc: &str) -> Option<i64> {
    let (m0, m1) = (cal.mass_at(lo_us), cal.mass_at(hi_us));
    for _ in 0..8 {
        let m = m0 + rng.unit() * (m1 - m0);
        let t = sample_ts_on_day(rng, cal, cal.day_for_mass(m.min(0.999_999_999)), cc);
        if t >= lo_us && t < hi_us {
            return Some(t);
        }
    }
    None
}

/// Plant payments to listed parties and decoys. `draw_customer(rng, ts)`
/// returns a customer originator eligible to pay at `ts` (activity-weighted,
/// outside any dormancy window), or None; `country_of(id)` is the
/// originator's home country, which fixes the business calendar.
///
/// Returns (planted instances, decoy rows). Decoy rows are ordinary
/// negatives and never reach the manifest.
#[allow(clippy::too_many_arguments)]
pub fn plant(
    scr: &Screening,
    seed: i64,
    cal: &DayCal,
    start_us: i64,
    end_us: i64,
    mut draw_customer: impl FnMut(&mut Rng) -> Option<u64>,
    country_of: impl Fn(u64) -> &'static str,
    mut eligible: impl FnMut(u64, i64) -> bool,
) -> (Vec<Planted>, Vec<ScreenRow>) {
    let mut planted = Vec::new();
    let mut j_s = 0usize;
    let mut j_p = 0usize;
    let mut payments = |rng: &mut Rng,
                        lo: i64,
                        hi: i64,
                        bene: u64,
                        name: &dyn Fn(&mut Rng) -> (String, &'static str)|
     -> Vec<ScreenRow> {
        let mut out = Vec::new();
        let Some(mut o) = draw_customer(rng) else {
            return out;
        };
        let n = 1 + rng.below(3) as usize;
        for _ in 0..n {
            let Some(mut t) = draw_ts(rng, cal, lo, hi, country_of(o)) else {
                continue;
            };
            let mut tries = 0;
            while !eligible(o, t) && tries < 8 {
                if let Some(o2) = draw_customer(rng) {
                    o = o2;
                }
                if let Some(t2) = draw_ts(rng, cal, lo, hi, country_of(o)) {
                    t = t2;
                }
                tries += 1;
            }
            if !eligible(o, t) {
                continue;
            }
            let (nm, v) = name(rng);
            out.push(ScreenRow {
                orig: o,
                bene,
                ts_us: t,
                bene_name: nm,
                variant: v,
            });
        }
        out.sort_by_key(|r| r.ts_us);
        out
    };
    for (k, p) in scr.parties.iter().enumerate() {
        let mut rng = stream(seed, 3, k as u64);
        if rng.unit() >= PAID_RATE {
            continue;
        }
        // Version-1 parties are paid after listing (a transaction-time screen
        // sees them); version-2 additions only before their listing date, so
        // only a rescreen of the counterparty base on the list update finds
        // them.
        let (lo, hi, detectable_by) = if p.list_version == 1 {
            (p.listed_us.max(start_us), end_us, "transaction_screen")
        } else {
            (start_us, p.listed_us, "rescreen")
        };
        let n_rel = if rng.unit() < 0.3 { 2 } else { 1 };
        for r in 0..n_rel {
            let iseed = splitmix64(
                (seed as u64) ^ SCREEN_SALT ^ splitmix64(0x51_0000 + (k as u64) * 4 + r),
            ) as i64;
            let mut irng = Rng::new(iseed as u64);
            let rows = payments(&mut irng, lo, hi, p.ext_id, &|g: &mut Rng| {
                reported_name(p, g)
            });
            if rows.is_empty() {
                continue;
            }
            let (typ, id, workload, severity) = if p.list_type == "sanctions" {
                j_s += 1;
                (
                    SANCTIONS_TYPOLOGY,
                    format!("SANCTIONS_MATCH_{j_s:07}"),
                    "W5_sanctions",
                    "strategic",
                )
            } else {
                j_p += 1;
                (
                    PEP_TYPOLOGY,
                    format!("PEP_MATCH_{j_p:07}"),
                    "W6_pep",
                    "operational",
                )
            };
            let variants: Vec<&str> = rows.iter().map(|r| r.variant).collect();
            let mut inst = Instance {
                id,
                typ,
                participants: vec![rows[0].orig, p.ext_id],
                start_us: rows[0].ts_us,
                end_us: rows[rows.len() - 1].ts_us,
                workload,
                severity,
                seed: iseed,
                rows_per_instance: rows.len(),
                corpus_start_us: start_us,
                corpus_end_us: end_us,
                suppress_start_us: 0,
                suppress_end_us: 0,
            };
            // A replacement originator (eligibility retry) can differ from the
            // first row's; participants name every distinct payer, subject
            // first.
            for row in &rows {
                if !inst.participants[..inst.participants.len() - 1].contains(&row.orig) {
                    let at = inst.participants.len() - 1;
                    inst.participants.insert(at, row.orig);
                }
            }
            planted.push(Planted {
                inst,
                rows,
                extra: vec![
                    ("list_id", p.list_id.clone()),
                    ("list_version", p.list_version.to_string()),
                    ("detectable_by", detectable_by.to_string()),
                    ("name_variants", variants.join(",")),
                ],
            });
        }
    }
    let mut decoy_rows = Vec::new();
    for (k, d) in scr.decoys.iter().enumerate() {
        let mut rng = stream(seed, 4, k as u64);
        let rows = payments(&mut rng, start_us, end_us, d.ext_id, &|_g: &mut Rng| {
            (d.name.clone(), "decoy")
        });
        decoy_rows.extend(rows);
    }
    (planted, decoy_rows)
}

/// Stable uid of a planted screening row (same shape as a typology row's uid:
/// top bit set, so it never collides with the base-row namespace). Decoys use
/// the decoy's external id as the instance seed.
#[inline]
pub fn screen_uid(inst_seed: i64, row_idx: usize) -> u64 {
    let mixed = splitmix64((inst_seed as u64 ^ SCREEN_SALT).wrapping_add((row_idx as u64) << 40));
    mixed | 0x8000_0000_0000_0000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_and_disjoint_ids() {
        let a = build(10_000, 43, 0, 1_000 * 86_400_000_000);
        let b = build(10_000, 43, 0, 1_000 * 86_400_000_000);
        assert_eq!(a.parties.len(), b.parties.len());
        for (x, y) in a.parties.iter().zip(&b.parties) {
            assert_eq!(
                (x.ext_id, &x.name, x.country),
                (y.ext_id, &y.name, y.country)
            );
        }
        let ids: Vec<u64> = a
            .parties
            .iter()
            .map(|p| p.ext_id)
            .chain(a.decoys.iter().map(|d| d.ext_id))
            .collect();
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(*id, 10_001 + i as u64);
        }
        assert!(a.parties.iter().any(|p| p.list_version == 2));
        assert!(!a.decoys.is_empty());
    }

    #[test]
    fn variants_differ_from_the_list_name() {
        let mut rng = Rng::new(7);
        for _ in 0..200 {
            let n = "Mohammed Albert Hassan";
            assert_ne!(typo(n, &mut rng).unwrap(), n);
            assert_ne!(transliterate(n, &mut rng).unwrap(), n);
        }
        let mut rng = Rng::new(9);
        assert_ne!(transliterate("James Robert Smith", &mut rng), None);
    }
}
