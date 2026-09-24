//! Shaped settlement timestamps: day-of-week suppression, salary and
//! quarter-end spikes, per-country holiday roll-forward, intraday peak.
//! Matches timing.py's shape (gate G4.8-4.10), not its exact RNG stream.

use crate::hash::Rng;

const SECS_PER_DAY: i64 = 86_400;
const US_PER_DAY: i64 = 86_400_000_000;

// Intraday hour weights, peaks at 10-11 and 15-16.
const INTRADAY: [f64; 24] = [
    0.005, 0.003, 0.002, 0.002, 0.003, 0.006, 0.015, 0.030, 0.055, 0.075, 0.095, 0.100, 0.070,
    0.075, 0.080, 0.095, 0.090, 0.075, 0.050, 0.035, 0.020, 0.010, 0.007, 0.006,
];
// Day-of-week weights, Monday-indexed; weekends suppressed.
const DOW_W: [f64; 7] = [0.22, 0.16, 0.16, 0.16, 0.22, 0.005, 0.005];

/// (year, month, day) from days since the Unix epoch (Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (y + if m <= 2 { 1 } else { 0 }, m, d)
}

#[inline]
fn weekday_monday0(epoch_day: i64) -> u32 {
    (epoch_day + 3).rem_euclid(7) as u32
}

/// Compact per-country public-holiday set as (month, day).
fn holidays(cc: &str) -> &'static [(u32, u32)] {
    match cc {
        "US" | "CA" => &[
            (1, 1),
            (7, 4),
            (12, 25),
            (12, 26),
            (11, 24),
            (11, 25),
            (5, 27),
            (9, 2),
            (10, 14),
            (1, 15),
            (2, 19),
            (6, 19),
        ],
        "GB" => &[
            (1, 1),
            (4, 15),
            (5, 6),
            (5, 27),
            (8, 26),
            (12, 25),
            (12, 26),
        ],
        "DE" | "AT" => &[(1, 1), (4, 19), (5, 1), (10, 3), (12, 25), (12, 26)],
        "FR" | "BE" => &[(1, 1), (5, 1), (5, 8), (7, 14), (8, 15), (11, 1), (12, 25)],
        "JP" => &[
            (1, 1),
            (2, 11),
            (4, 29),
            (5, 3),
            (5, 4),
            (5, 5),
            (11, 3),
            (11, 23),
            (12, 23),
        ],
        "CH" => &[(1, 1), (5, 1), (8, 1), (12, 25), (12, 26)],
        "AE" => &[(1, 1), (12, 2), (12, 3)],
        "IN" => &[(1, 1), (1, 26), (8, 15), (10, 2), (12, 25)],
        "AU" => &[(1, 1), (1, 26), (4, 25), (12, 25), (12, 26)],
        "MX" => &[(1, 1), (2, 5), (3, 18), (5, 1), (9, 16), (11, 20), (12, 25)],
        "BR" => &[
            (1, 1),
            (4, 21),
            (5, 1),
            (9, 7),
            (10, 12),
            (11, 15),
            (12, 25),
        ],
        "CN" => &[(1, 1), (2, 8), (2, 9), (2, 10), (5, 1), (10, 1)],
        "HK" => &[(1, 1), (5, 1), (7, 1), (10, 1), (12, 25)],
        "KR" => &[(1, 1), (3, 1), (5, 5), (6, 6), (8, 15), (10, 3), (12, 25)],
        "NL" => &[(1, 1), (5, 1), (5, 5), (12, 25), (12, 26)],
        "ES" => &[(1, 1), (1, 6), (5, 1), (8, 15), (10, 12), (11, 1), (12, 25)],
        "SG" => &[(1, 1), (5, 1), (8, 9), (12, 25)],
        _ => &[(1, 1), (12, 25)],
    }
}

/// Precomputed per-day calendar facts for a corpus window.
pub struct DayCal {
    start_epoch_day: i64,
    span: usize,
    pub weekday: Vec<u32>,
    month: Vec<u32>,
    dom: Vec<u32>,
    pub day_cdf: Vec<f64>,
}

impl DayCal {
    pub fn new(start_epoch_us: i64, span_days: usize) -> Self {
        let start_epoch_day = start_epoch_us / US_PER_DAY;
        let span = span_days.max(1);
        let mut weekday = vec![0u32; span];
        let mut month = vec![0u32; span];
        let mut dom = vec![0u32; span];
        let mut w = vec![0.0f64; span];
        for o in 0..span {
            let ed = start_epoch_day + o as i64;
            let (_, m, d) = civil_from_days(ed);
            let wd = weekday_monday0(ed);
            weekday[o] = wd;
            month[o] = m;
            dom[o] = d;
            let salary = if d == 1 || d == 15 || d == 25 {
                5.0
            } else {
                1.0
            };
            let qe = if matches!(m, 3 | 6 | 9 | 12) && d >= 26 {
                2.0
            } else {
                1.0
            };
            w[o] = DOW_W[wd as usize] * salary * qe;
        }
        let total: f64 = w.iter().sum();
        let mut cdf = vec![0.0f64; span];
        let mut acc = 0.0;
        for o in 0..span {
            acc += w[o] / total;
            cdf[o] = acc;
        }
        DayCal {
            start_epoch_day,
            span,
            weekday,
            month,
            dom,
            day_cdf: cdf,
        }
    }

    #[inline]
    fn is_business(&self, day: usize, hol: &[(u32, u32)]) -> bool {
        if self.weekday[day] >= 5 {
            return false;
        }
        let md = (self.month[day], self.dom[day]);
        !hol.contains(&md)
    }

    /// Roll a sampled day forward to the next business day for the country.
    fn roll(&self, mut day: usize, cc: &str) -> usize {
        let hol = holidays(cc);
        for _ in 0..5 {
            if self.is_business(day, hol) {
                break;
            }
            day = (day + 1).min(self.span - 1);
        }
        day
    }
}

/// Sample one shaped timestamp (microseconds since epoch) for a given country.
pub fn sample_ts(rng: &mut Rng, cal: &DayCal, cc: &str) -> i64 {
    // Inverse-CDF day draw.
    let u = rng.unit();
    let mut day = cal.day_cdf.partition_point(|&c| c < u);
    if day >= cal.span {
        day = cal.span - 1;
    }
    day = cal.roll(day, cc);
    // Intraday hour by inverse-CDF.
    let uh = rng.unit();
    let mut acc = 0.0;
    let mut hour = 23usize;
    for (h, &w) in INTRADAY.iter().enumerate() {
        acc += w;
        if uh <= acc {
            hour = h;
            break;
        }
    }
    let sub = rng.unit();
    let micros_in_day = (hour as i64 * 3600 + (sub * 3600.0) as i64) * 1_000_000;
    (cal.start_epoch_day + day as i64) * US_PER_DAY + micros_in_day
}

/// Convert a specific epoch-microsecond instant to the rolled business day for a
/// country, then to a shaped intraday time -- used for typology rows whose day is
/// fixed by the schedule but which still deserve a business-hours time.
pub fn shape_fixed_day(rng: &mut Rng, ts_us: i64, cc: &str) -> i64 {
    let epoch_day = ts_us / US_PER_DAY;
    // Roll forward locally.
    let mut ed = epoch_day;
    let hol = holidays(cc);
    for _ in 0..5 {
        let (_, m, d) = civil_from_days(ed);
        let wd = weekday_monday0(ed);
        if wd < 5 && !hol.contains(&(m, d)) {
            break;
        }
        ed += 1;
    }
    let uh = rng.unit();
    let mut acc = 0.0;
    let mut hour = 23usize;
    for (h, &w) in INTRADAY.iter().enumerate() {
        acc += w;
        if uh <= acc {
            hour = h;
            break;
        }
    }
    let sub = rng.unit();
    ed * US_PER_DAY + (hour as i64 * 3600 + (sub * 3600.0) as i64) * 1_000_000
}

#[allow(dead_code)]
pub const fn secs_per_day() -> i64 {
    SECS_PER_DAY
}
