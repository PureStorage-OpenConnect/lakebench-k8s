//! Fifteen typology primitives, scheduled to REQ-G-03 density (0.1% of rows).
//!
//! The first nine (fan_in, fan_out, cycle, stack, gather_scatter,
//! scatter_gather, bipartite, synthetic_identity, random) mirror
//! typologies.py's structure so the gate's graph-closure and density checks
//! pass. The additional six exercise schema features a flat AML CSV
//! (PaySim, IBM-AML, SAML-D flat variants) cannot express -- pass-through
//! chains via intermediary agents, dormancy gaps against opened_date,
//! cross-border corridors via country tuples, high-variance repeat edges
//! for TBML-style repeated invoicing, and low-amount structuring against
//! the reporting threshold. See MEMORY note project_fraud_aml for context.

use crate::hash::{splitmix64, Rng};
use crate::world::{entity_type, TYPE_PERSON};

pub struct Spec {
    /// Stable per-typology id embedded in typology_id strings and used as
    /// the high bits of the per-instance seed. NEVER reorder or reuse a
    /// tid: the 0..=8 assignments below match the pre-15-typology release
    /// so old goldens stay reproducible, and new typologies start at 9.
    pub tid: i32,
    pub name: &'static str,
    pub participants: usize,
    pub workload: &'static str,
    pub severity: &'static str,
    pub rows_per_instance: usize,
}

// The original nine tids (0..=8) match the pre-15-typology release so
// reruns at the same top-level --seed produce identical participants and
// identical typology_id strings for the original nine. New typologies get
// tids 9..=14 in alphabetical order among themselves.
pub const SPECS: [Spec; 15] = [
    Spec { tid: 0, name: "bipartite", participants: 8, workload: "W1_synthetic_id", severity: "strategic", rows_per_instance: 16 },
    Spec { tid: 1, name: "cycle", participants: 4, workload: "W3_round_tripping", severity: "operational", rows_per_instance: 4 },
    Spec { tid: 2, name: "fan_in", participants: 6, workload: "W2_structuring", severity: "operational", rows_per_instance: 5 },
    Spec { tid: 3, name: "fan_out", participants: 6, workload: "W2_structuring", severity: "operational", rows_per_instance: 5 },
    Spec { tid: 4, name: "gather_scatter", participants: 9, workload: "W2_structuring", severity: "strategic", rows_per_instance: 8 },
    Spec { tid: 5, name: "random", participants: 2, workload: "W2_structuring", severity: "smoke", rows_per_instance: 1 },
    Spec { tid: 6, name: "scatter_gather", participants: 7, workload: "W3_round_tripping", severity: "strategic", rows_per_instance: 10 },
    Spec { tid: 7, name: "stack", participants: 5, workload: "W2_structuring", severity: "strategic", rows_per_instance: 4 },
    Spec { tid: 8, name: "synthetic_identity", participants: 5, workload: "W1_synthetic_id", severity: "strategic", rows_per_instance: 3 },
    Spec { tid: 9, name: "corridor_high_risk", participants: 2, workload: "W3_round_tripping", severity: "strategic", rows_per_instance: 1 },
    Spec { tid: 10, name: "cross_border_cycle", participants: 5, workload: "W3_round_tripping", severity: "strategic", rows_per_instance: 5 },
    // rows_per_instance for dormant_reactivation counts only rows inside
    // the manifest injection window (the burst). The early ping is
    // dormancy evidence outside the window and does NOT count toward
    // this number, so a downstream consumer reconstructing (participants,
    // [s,e], rows_per_instance) sees an accurate ground-truth count.
    Spec { tid: 11, name: "dormant_reactivation", participants: 2, workload: "W2_structuring", severity: "strategic", rows_per_instance: 4 },
    Spec { tid: 12, name: "micro_structuring", participants: 9, workload: "W2_structuring", severity: "operational", rows_per_instance: 8 },
    Spec { tid: 13, name: "rapid_layering", participants: 3, workload: "W3_round_tripping", severity: "strategic", rows_per_instance: 2 },
    Spec { tid: 14, name: "tbml_repeated_invoice", participants: 2, workload: "W2_structuring", severity: "strategic", rows_per_instance: 6 },
];

/// Per-tid seed stride. Must be larger than the maximum n_inst any single
/// typology can produce at supported scales, so `iseed(tid, j)` never
/// collides with `iseed(tid+1, j')`. At scale 100 with rows_per_instance=1
/// typologies (random, corridor_high_risk) n_inst is ~2e5; 1e8 gives four
/// orders of magnitude headroom and stays well within i64 for tid up to 14.
const TID_SEED_STRIDE: i64 = 100_000_000;

/// Country codes classified as elevated-risk corridors for demo purposes only
/// (paired with any non-matching second country). These are not a regulatory
/// list; they are picked to give the corridor_high_risk typology a stable,
/// deterministic pool of participants regardless of the US-heavy home
/// distribution.
const HIGH_RISK_CC: [&str; 6] = ["AE", "CN", "SG", "HK", "MX", "IN"];

pub struct Instance {
    pub id: String,
    pub typ: &'static str,
    pub participants: Vec<u64>,
    pub start_us: i64,
    pub end_us: i64,
    pub workload: &'static str,
    pub severity: &'static str,
    pub seed: i64,
    pub rows_per_instance: usize,
    /// Corpus bounds, kept per-instance so emit_instance can place
    /// out-of-window rows (dormant_reactivation's early ping) at a real
    /// corpus-start position without re-deriving it from the instance
    /// window. Manifest builders should not surface these.
    pub corpus_start_us: i64,
    pub corpus_end_us: i64,
}

#[derive(Clone)]
pub struct TxRow {
    pub orig: u64,
    pub bene: u64,
    pub ts_us: i64,
    pub structuring: bool,
}

fn person_pool(population: usize, seed: i64) -> Vec<u64> {
    let mut v: Vec<u64> = (1..=population as u64)
        .filter(|&id| entity_type(id, seed) == TYPE_PERSON)
        .collect();
    if v.len() < 10 {
        v = (1..=population as u64).collect();
    }
    v
}

/// Subset of `person_pool` where the participant lives in a corridor
/// country. Falls back to the full pool if the corridor pool is too small.
fn corridor_pool(pool: &[u64], country: &[&'static str]) -> Vec<u64> {
    let mut v: Vec<u64> = pool
        .iter()
        .copied()
        .filter(|&id| {
            let idx = id as usize;
            idx < country.len() && HIGH_RISK_CC.iter().any(|&cc| cc == country[idx])
        })
        .collect();
    if v.len() < 4 {
        v = pool.to_vec();
    }
    v
}

fn pick_distinct(rng: &mut Rng, pool: &[u64], k: usize) -> Vec<u64> {
    let mut out: Vec<u64> = Vec::with_capacity(k);
    let mut tries = 0;
    while out.len() < k && tries < k * 50 {
        let cand = pool[rng.below(pool.len() as u64) as usize];
        if !out.contains(&cand) {
            out.push(cand);
        }
        tries += 1;
    }
    while out.len() < k {
        // Fallback: allow repeats at tiny pools.
        out.push(pool[rng.below(pool.len() as u64) as usize]);
    }
    out
}

pub fn schedule(
    seed: i64,
    total_rows: i64,
    population: usize,
    corpus_start_us: i64,
    corpus_end_us: i64,
    country: &[&'static str],
) -> Vec<Instance> {
    let pool = person_pool(population, seed);
    let corridor = corridor_pool(&pool, country);
    let budget = 0.001 * total_rows as f64 / SPECS.len() as f64;
    let span = (corpus_end_us - corpus_start_us).max(1);
    let mut instances = Vec::new();
    let mut warned_corridor_skip = false;
    for spec in SPECS.iter() {
        // corridor_high_risk and cross_border_cycle select from the
        // high-risk country pool; every other typology uses the full
        // person pool.
        let src_pool: &[u64] = match spec.name {
            "corridor_high_risk" | "cross_border_cycle" => &corridor,
            _ => &pool,
        };
        // Skip a typology entirely if its source pool is too small to
        // yield `spec.participants` distinct entities. pick_distinct's
        // "allow repeats at tiny pools" fallback would otherwise produce
        // duplicated participants which the cycle emitter then turns into
        // orig==bene self-loops -- silent corruption. Per-spec check
        // (was a scalar corridor_ok at len>=4, which admitted
        // cross_border_cycle at 4 corridor persons and produced self-loops
        // on 5-participant cycles).
        if src_pool.len() < spec.participants {
            if matches!(spec.name, "corridor_high_risk" | "cross_border_cycle") && !warned_corridor_skip {
                // In practice only cross_border_cycle (needs 5) trips this
                // -- corridor_pool falls back to the full person pool when
                // its natural size is < 4, so corridor.len() >= 4 always,
                // and corridor_high_risk (needs 2) is never skipped here.
                eprintln!(
                    "note: corridor pool has {} entities (< {}); skipping {} -- expected at tiny scales",
                    corridor.len(), spec.participants, spec.name
                );
                warned_corridor_skip = true;
            }
            continue;
        }
        // dormant_reactivation emits one out-of-window ping in addition to
        // its `rows_per_instance` burst rows, so its actual row density is
        // (rows_per_instance + 1) per instance. Scheduling divides budget
        // by that total so this typology's contribution matches the target
        // budget (~0.1% of rows) instead of overshooting by ~25%. Every
        // other typology emits exactly `rows_per_instance` rows per
        // instance and divides budget by that.
        let emitted_per_instance = spec.rows_per_instance
            + if spec.name == "dormant_reactivation" { 1 } else { 0 };
        let n_inst = ((budget / emitted_per_instance as f64).round() as i64).max(1);
        for j in 0..n_inst {
            let iseed = seed + 0xF100 + spec.tid as i64 * TID_SEED_STRIDE + j;
            let mut rng = Rng::new(iseed as u64);
            let participants = pick_distinct(&mut rng, src_pool, spec.participants);
            // Duration model per typology name. Comments explain the intent.
            let (start, end) = match spec.name {
                // rapid_layering: whole chain lands within one civil day.
                // Manifest window is the full day so downstream consumers
                // that filter injected rows by (participants, [s,e]) find
                // both legs -- an earlier version recorded a 6-hour window
                // here but the emit path was pinned to the whole day, so
                // ~50% of legs fell outside the manifest window. Both
                // schedule and emit must agree on the same window.
                "rapid_layering" => {
                    let day_us = 86_400_000_000i64;
                    let offset = (rng.unit() * span as f64 * 0.95) as i64;
                    let raw_s = corpus_start_us + offset;
                    let day_start = raw_s - raw_s.rem_euclid(day_us);
                    let s = day_start.max(corpus_start_us);
                    let e = (day_start + day_us).min(corpus_end_us);
                    (s, e)
                }
                // dormant_reactivation: the manifest injection window
                // covers only the burst band (last ~2 days), not the early
                // ping. A wide manifest window would attribute organic
                // ring-driven activity between the same participants to
                // this typology and inflate recall. The early ping's ts
                // (pinned to corpus start via the seed) is derived in
                // emit_instance and lives OUTSIDE the manifest window on
                // purpose -- the "dormancy" signal is that the edge exists
                // before the manifest window, not that the ping is part of
                // the injection band.
                "dormant_reactivation" => {
                    // Burst window is 7 days, not 2, to absorb up to five
                    // days of weekend/holiday rollover that shape_fixed_day
                    // in timing.rs applies to typology rows. Anchored to the
                    // last ~2% of the corpus so the dormancy gap (ping at
                    // corpus start, burst at corpus end) is 4+ years at a
                    // 60-month corpus. Still ~1000x narrower than the
                    // previous full-corpus window that was over-attributing
                    // organic traffic to the injection.
                    let burst_off = ((0.98 + rng.unit() * 0.02) * span as f64) as i64;
                    let e = (corpus_start_us + burst_off).min(corpus_end_us);
                    let s = e - (7 * 86_400_000_000).min(e - corpus_start_us);
                    (s, e)
                }
                // corridor_high_risk: single-transaction pattern, so the
                // window can be as small as one minute.
                "corridor_high_risk" => {
                    let offset = (rng.unit() * span as f64 * 0.95) as i64;
                    let s = corpus_start_us + offset;
                    let e = (s + 60_000_000).min(corpus_end_us);
                    (s, e)
                }
                // tbml_repeated_invoice: 6 transactions on the same edge
                // over a 1-week window so amount variance is the signal.
                "tbml_repeated_invoice" => {
                    let offset = (rng.unit() * span as f64 * 0.95) as i64;
                    let s = corpus_start_us + offset;
                    let e = (s + 7 * 86_400_000_000).min(corpus_end_us);
                    (s, e)
                }
                // micro_structuring: many small structured transactions
                // over a 3-day window (distinct from fan_in's 1-3 day
                // burst by having more origs and a shorter window).
                "micro_structuring" => {
                    let offset = (rng.unit() * span as f64 * 0.95) as i64;
                    let s = corpus_start_us + offset;
                    let e = (s + 3 * 86_400_000_000).min(corpus_end_us);
                    (s, e)
                }
                // cross_border_cycle: like `cycle` but tighter (2-4 days)
                // so the roundtrip signature is clean. Participants are
                // drawn from the corridor pool above so all hops are
                // non-US, giving the "cross-border" property regardless
                // of the US-heavy home distribution. Cast of
                // rng.unit()*3.0 (range 0..3) truncates to {0,1,2}, so
                // duration is 2..=4 days inclusive.
                "cross_border_cycle" => {
                    let offset = (rng.unit() * span as f64 * 0.95) as i64;
                    let s = corpus_start_us + offset;
                    let dur = ((rng.unit() * 3.0 + 2.0) as i64) * 86_400_000_000;
                    let e = (s + dur).min(corpus_end_us);
                    (s, e)
                }
                // Existing typologies preserve their prior window shape.
                _ => {
                    let offset = (rng.unit() * span as f64 * 0.95) as i64;
                    let s = corpus_start_us + offset;
                    let dur_us: i64 = match spec.name {
                        "fan_in" | "fan_out" => (rng.unit() * 48.0 + 24.0) as i64 * 3_600_000_000,
                        "cycle" | "stack" | "gather_scatter" | "scatter_gather" => {
                            ((rng.unit() * 5.0 + 2.0) as i64) * 86_400_000_000
                        }
                        "bipartite" => ((rng.unit() * 23.0 + 7.0) as i64) * 86_400_000_000,
                        "synthetic_identity" => ((rng.unit() * 150.0 + 30.0) as i64) * 86_400_000_000,
                        _ => ((rng.unit() * 167.0 + 1.0) as i64) * 3_600_000_000,
                    };
                    let e = (s + dur_us).min(corpus_end_us);
                    (s, e)
                }
            };
            instances.push(Instance {
                id: format!("{}_{}_{:07}", spec.name.to_uppercase(), spec.tid, j),
                typ: spec.name,
                participants,
                start_us: start,
                end_us: end,
                workload: spec.workload,
                severity: spec.severity,
                seed: iseed,
                rows_per_instance: spec.rows_per_instance,
                corpus_start_us,
                corpus_end_us,
            });
        }
    }
    instances
}

fn uu(rng: &mut Rng, start: i64, end: i64) -> i64 {
    if end <= start {
        return start;
    }
    start + (rng.unit() * (end - start) as f64) as i64
}

pub fn emit_instance(inst: &Instance) -> Vec<TxRow> {
    let mut rng = Rng::new(inst.seed as u64);
    let p = &inst.participants;
    let (s, e) = (inst.start_us, inst.end_us);
    let mut rows = Vec::new();
    match inst.typ {
        "fan_in" => {
            let bene = *p.last().unwrap();
            for &snd in &p[..p.len() - 1] {
                rows.push(TxRow { orig: snd, bene, ts_us: uu(&mut rng, s, e), structuring: true });
            }
        }
        "fan_out" => {
            let orig = p[0];
            for &b in &p[1..] {
                rows.push(TxRow { orig, bene: b, ts_us: uu(&mut rng, s, e), structuring: true });
            }
        }
        "gather_scatter" => {
            let hub = p[0];
            let half = ((p.len() - 1) / 2).max(1);
            let third = (e - s) / 3;
            for &o in &p[1..1 + half] {
                rows.push(TxRow { orig: o, bene: hub, ts_us: uu(&mut rng, s, s + third), structuring: false });
            }
            for &b in &p[1 + half..] {
                rows.push(TxRow { orig: hub, bene: b, ts_us: uu(&mut rng, s + 2 * third, e), structuring: false });
            }
        }
        "scatter_gather" => {
            let orig = p[0];
            let bene = *p.last().unwrap();
            let half = (e - s) / 2;
            for &m in &p[1..p.len() - 1] {
                rows.push(TxRow { orig, bene: m, ts_us: uu(&mut rng, s, s + half), structuring: false });
                rows.push(TxRow { orig: m, bene, ts_us: uu(&mut rng, s + half, e), structuring: false });
            }
        }
        "cycle" => {
            let n = p.len();
            let step = (e - s) / n as i64;
            for i in 0..n {
                let base = s + step * i as i64;
                let jit = (rng.unit() * step as f64 * 0.5) as i64;
                rows.push(TxRow { orig: p[i], bene: p[(i + 1) % n], ts_us: base + jit, structuring: false });
            }
        }
        "stack" => {
            let step = (e - s) / (p.len().max(2) - 1) as i64;
            for i in 0..p.len() - 1 {
                let base = s + step * i as i64;
                let jit = (rng.unit() * step as f64 * 0.5) as i64;
                rows.push(TxRow { orig: p[i], bene: p[i + 1], ts_us: base + jit, structuring: false });
            }
        }
        "random" => {
            rows.push(TxRow { orig: p[0], bene: p[1], ts_us: uu(&mut rng, s, e), structuring: false });
        }
        "bipartite" => {
            let half = (p.len() / 2).max(1);
            for &src in &p[..half] {
                for &dst in &p[half..] {
                    rows.push(TxRow { orig: src, bene: dst, ts_us: uu(&mut rng, s, e), structuring: false });
                }
            }
        }
        "synthetic_identity" => {
            for _ in 0..inst.rows_per_instance {
                let a = p[rng.below(p.len() as u64) as usize];
                let mut bb = p[rng.below(p.len() as u64) as usize];
                if bb == a {
                    bb = p[(p.iter().position(|&x| x == a).unwrap() + 1) % p.len()];
                }
                rows.push(TxRow { orig: a, bene: bb, ts_us: uu(&mut rng, s, e), structuring: false });
            }
        }
        // A -> mule -> B chain, both legs pinned to the SAME day. The
        // schedule places [s, e] on day boundaries (see schedule case for
        // rapid_layering) so we can uu straight over [s, e]. That
        // guarantees both legs land inside the manifest window even after
        // shape_fixed_day resamples the hour independently and rolls
        // weekends forward. The only intraday invariant that survives
        // shaping is "same civil day"; hour-of-day gaps up to ~14h under
        // peak-hour resampling are possible. Signal: mule receives and
        // forwards on the same business day.
        "rapid_layering" => {
            let (a, m, b) = (p[0], p[1], p[2]);
            rows.push(TxRow { orig: a, bene: m, ts_us: uu(&mut rng, s, e), structuring: false });
            rows.push(TxRow { orig: m, bene: b, ts_us: uu(&mut rng, s, e), structuring: false });
        }
        // dormant_reactivation. The manifest injection window [s, e]
        // covers only the burst band (last ~2 days of corpus). The early
        // ping's ts is pinned to the first 5% of the corpus using the
        // real corpus_start_us carried on the instance -- earlier code
        // tried to derive it from (s, e) and picked the wrong side of a
        // max()/min(), landing the ping only ~50 days before the burst.
        // A gate that reconstructs typology rows as "activity on this
        // edge inside the manifest window" picks up ONLY the burst; the
        // ping is dormancy evidence outside the injection band.
        "dormant_reactivation" => {
            let (a, b) = (p[0], p[1]);
            let cs = inst.corpus_start_us;
            let ce = inst.corpus_end_us;
            let ping_end = cs + ((ce - cs) / 20).max(1); // first 5% of corpus
            let early_ts = uu(&mut rng, cs, ping_end);
            // One early ping (outside manifest window, dormancy evidence)
            // plus `rows_per_instance` burst rows inside [s, e]. The
            // manifest's rows_per_instance for this typology reflects
            // only the in-window count, so downstream recall counters
            // do not undercount.
            rows.push(TxRow { orig: a, bene: b, ts_us: early_ts, structuring: false });
            for _ in 0..inst.rows_per_instance {
                rows.push(TxRow { orig: a, bene: b, ts_us: uu(&mut rng, s, e), structuring: false });
            }
        }
        // Cycle across cross-border-likely participants (spec caller picks
        // from person pool; corridor mix comes from the natural country
        // weighting -- non-US pairs occur naturally at 12% each ring hop).
        "cross_border_cycle" => {
            let n = p.len();
            let step = (e - s) / n as i64;
            for i in 0..n {
                let base = s + step * i as i64;
                let jit = (rng.unit() * step as f64 * 0.5) as i64;
                rows.push(TxRow { orig: p[i], bene: p[(i + 1) % n], ts_us: base + jit, structuring: false });
            }
        }
        // Many origs, one bene, structured amounts, 3-day window. Distinct
        // from fan_in by density (rows_per_instance=8 not 5) and window.
        "micro_structuring" => {
            let bene = *p.last().unwrap();
            let n = (inst.rows_per_instance).min(p.len() - 1);
            for i in 0..n {
                let orig = p[i];
                rows.push(TxRow { orig, bene, ts_us: uu(&mut rng, s, e), structuring: true });
            }
        }
        // Repeated same-edge invoicing over one week. Signal: high amount
        // variance on one edge (invoice != payment). Rows share (orig, bene)
        // so a group-by-edge amount-cv detector fires.
        "tbml_repeated_invoice" => {
            let (a, b) = (p[0], p[1]);
            for _ in 0..inst.rows_per_instance {
                // Not marked as structuring -- amounts come from the normal
                // lognormal draw which spans four orders of magnitude, so
                // the amount CV on this edge is high by construction.
                rows.push(TxRow { orig: a, bene: b, ts_us: uu(&mut rng, s, e), structuring: false });
            }
        }
        // Single transaction between participants both drawn from the
        // corridor pool -> non-US, non-US country tuple. Amount is a
        // normal lognormal draw; the corridor country pair is the signal,
        // not the amount. To emit a genuinely "large" transaction we
        // would need an amount-channel hook in emit.rs, which is a
        // larger change deferred to a follow-up.
        "corridor_high_risk" => {
            // splitmix64 to jitter which participant is orig vs bene per
            // instance without adding an extra rng draw.
            let flip = splitmix64(inst.seed as u64) & 1;
            let (a, b) = if flip == 0 { (p[0], p[1]) } else { (p[1], p[0]) };
            rows.push(TxRow { orig: a, bene: b, ts_us: uu(&mut rng, s, e), structuring: false });
        }
        _ => {}
    }
    rows
}
