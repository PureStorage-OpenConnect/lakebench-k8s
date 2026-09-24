//! Placing typology instances and shaping their rows on the baseline calendar.
//!
//! Baseline rows are spread over calendar MASS (day-of-week, salary-day and
//! quarter-end weights), so planted rows must follow the same calendar or the
//! calendar itself becomes a label. Two rules keep them on it:
//!
//! 1. An instance's window is placed in mass space, not time. Its start is
//!    drawn uniformly over mass and it spans the same fraction of mass as its
//!    time length is of the corpus. A fixed-length window in time that starts
//!    part-way through a heavy day spills into the next light day, which put
//!    planted rows on Fridays and post-salary days at 1.5 to 1.7x the baseline
//!    rate per typology.
//! 2. Rows are shaped with their OWN originator's country and handed their
//!    shaped times by the rank of their original time. Sorting the shaped
//!    times and zipping them back in emit order (the previous approach) broke
//!    the phase structure of scatter_gather and bipartite instances, and could
//!    give a row a time rolled for another country, landing 0.25% of planted
//!    rows on their originator's public holiday, where baseline has none.

use crate::hash::Rng;
use crate::timing::{sample_ts_on_day, DayCal};
use crate::typology::{Instance, TxRow};

const US_PER_DAY: i64 = 86_400_000_000;

/// Fraction of the corpus a window of `len_us` covers, capped at one half.
fn window_fraction(len_us: i64, span_us: i64) -> f64 {
    (len_us.max(1) as f64 / span_us.max(1) as f64).min(0.5)
}

/// Move every instance, as a unit (window, suppression window, anchor), so
/// its window starts at a mass-uniform position. The schedule's own start is
/// uniform in time; its relative position is reused as the mass draw, so no
/// extra randomness is consumed and the schedule stays the only RNG stream.
pub fn place_instances(instances: &mut [Instance], cal: &DayCal, start_us: i64, end_us: i64) {
    let span_us = (end_us - start_us).max(1);
    for inst in instances.iter_mut() {
        let len = window_fraction(inst.end_us - inst.start_us, span_us);
        let p = ((inst.start_us - start_us) as f64 / span_us as f64).clamp(0.0, 0.999_999);
        let ms = p * (1.0 - len);
        let day = cal.day_for_mass(ms);
        let offset = inst.start_us.rem_euclid(US_PER_DAY);
        let mut delta = cal.day_start_us(day) + offset - inst.start_us;
        // Keep the whole instance, including a pre-window anchor, in the corpus.
        let has_suppress = inst.suppress_end_us > inst.suppress_start_us;
        let earliest = if has_suppress {
            inst.start_us.min(inst.suppress_start_us)
        } else {
            inst.start_us
        };
        let lo = earliest - 3 * US_PER_DAY;
        if lo + delta < start_us {
            delta = start_us - lo;
        }
        if inst.end_us + delta > end_us {
            delta = end_us - inst.end_us;
        }
        inst.start_us += delta;
        inst.end_us += delta;
        if has_suppress {
            inst.suppress_start_us += delta;
            inst.suppress_end_us += delta;
        }
    }
}

/// Shape one instance's rows in place. `country[orig]` is each originator's
/// country. Returns the [min, max] of the shaped in-window rows, which is
/// what the manifest should report as the injection window.
pub fn shape_instance_rows(
    rows: &mut [TxRow],
    inst: &Instance,
    cal: &DayCal,
    span_us: i64,
    country: &[&str],
    rng: &mut Rng,
) -> Option<(i64, i64)> {
    let n = rows.len();
    if n == 0 {
        return None;
    }
    let ws = inst.start_us;
    let we = inst.end_us.max(ws + 1);
    let len = window_fraction(we - ws, span_us);
    let ms = cal.mass_at(ws);
    let in_win: Vec<bool> = rows.iter().map(|r| r.ts_us >= ws && r.ts_us < we).collect();

    // 1. A day for every row, then a shaped time on it for the row's country.
    let mut shaped: Vec<i64> = Vec::with_capacity(n);
    for (i, r) in rows.iter().enumerate() {
        let day = if in_win[i] {
            let frac = (r.ts_us - ws) as f64 / (we - ws) as f64;
            cal.day_for_mass((ms + frac * len).min(0.999_999_999))
        } else {
            // Out-of-window rows (the dormancy anchor) get a mass-drawn day in
            // [day - 3, day + 1): keeping the scheduled day and rolling
            // weekends forward put 42% of anchors on Mondays.
            let d0 = r.ts_us - r.ts_us.rem_euclid(US_PER_DAY);
            let lo = cal.mass_at(d0 - 3 * US_PER_DAY);
            let hi = cal.mass_at(d0 + US_PER_DAY);
            if hi > lo {
                cal.day_for_mass(lo + rng.unit() * (hi - lo))
            } else {
                cal.day_of(r.ts_us)
            }
        };
        shaped.push(sample_ts_on_day(rng, cal, day, country[r.orig as usize]));
    }

    // 2. Rank by original time (stable), so shaping never reorders legs.
    let mut rank: Vec<usize> = (0..n).collect();
    rank.sort_by_key(|&i| (rows[i].ts_us, i));

    // 3. Rows that landed on the same day swap times so that, within the
    // day, earlier-ranked rows get earlier times. Days are valid for every
    // row in the group because each row was rolled for its own country.
    let mut by_day: std::collections::BTreeMap<usize, Vec<usize>> = Default::default();
    for &i in &rank {
        by_day.entry(cal.day_of(shaped[i])).or_default().push(i);
    }
    for idxs in by_day.values() {
        let mut ts: Vec<i64> = idxs.iter().map(|&i| shaped[i]).collect();
        ts.sort_unstable();
        for (&i, t) in idxs.iter().zip(ts) {
            shaped[i] = t;
        }
    }

    // 4. A holiday roll can still push an earlier row past a later one on a
    // different day. Move the later row just after it, on a business day for
    // its own country.
    let mut prev: Option<i64> = None;
    for &i in &rank {
        if let Some(p) = prev {
            if shaped[i] <= p {
                let cc = country[rows[i].orig as usize];
                let bump = p + 1_000_000 + (rng.unit() * 1_800_000_000.0) as i64;
                let d = cal.day_of(bump);
                let same_day = bump < cal.day_start_us(d) + US_PER_DAY;
                shaped[i] = if same_day && cal.is_business_day(d, cc) && cal.day_of(p) == d {
                    bump
                } else {
                    sample_ts_on_day(rng, cal, (cal.day_of(p) + 1).min(cal.span() - 1), cc)
                        .max(p + 1_000_000)
                };
            }
        }
        prev = Some(shaped[i]);
    }

    let mut bounds: Option<(i64, i64)> = None;
    for (i, r) in rows.iter_mut().enumerate() {
        r.ts_us = shaped[i];
        if in_win[i] {
            bounds = Some(match bounds {
                None => (r.ts_us, r.ts_us),
                Some((a, b)) => (a.min(r.ts_us), b.max(r.ts_us)),
            });
        }
    }
    bounds
}
