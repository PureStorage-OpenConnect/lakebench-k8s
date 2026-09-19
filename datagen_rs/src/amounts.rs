//! Amount distributions and FX, matching amounts.py's shapes (not its exact RNG
//! sequence -- the gate checks distribution bands, not byte-parity).

use crate::hash::Rng;

pub const LN_MU: f64 = 8.517193191416238; // ln(5000)
pub const LN_SIGMA: f64 = 1.4;
pub const AMOUNT_CEILING: f64 = 50_000_000.0;
pub const ROUND_SNAP_RATE: f64 = 0.15;

pub fn fx_to_usd(ccy: &str) -> f64 {
    match ccy {
        "USD" => 1.0,
        "GBP" => 1.30,
        "EUR" => 1.10,
        "CHF" => 1.15,
        "JPY" => 0.0068,
        "AED" => 0.27,
        "SGD" => 0.74,
        "CAD" => 0.73,
        "MXN" => 0.055,
        "CNY" => 0.14,
        "INR" => 0.012,
        "AUD" => 0.66,
        "HKD" => 0.128,
        "KRW" => 0.00075,
        "BRL" => 0.19,
        _ => 1.0,
    }
}

/// (lo, hi) structuring band just under the local reporting threshold.
pub fn structuring_band(ccy: &str) -> (f64, f64) {
    match ccy {
        "USD" | "CAD" | "AUD" => (9_500.0, 9_999.0),
        "GBP" | "EUR" | "CHF" => (14_700.0, 14_995.0),
        "JPY" | "INR" => (990_000.0, 999_999.0),
        "AED" => (54_500.0, 54_999.0),
        "SGD" => (19_500.0, 19_999.0),
        "MXN" => (99_000.0, 99_999.0),
        "CNY" | "BRL" => (49_500.0, 49_999.0),
        "HKD" => (74_500.0, 74_999.0),
        "KRW" => (9_900_000.0, 9_999_999.0),
        _ => (9_500.0, 9_999.0),
    }
}

/// Baseline log-normal amount with round-number snapping. Rounded to cents.
pub fn lognormal_amount(rng: &mut Rng) -> f64 {
    let raw = (LN_MU + LN_SIGMA * rng.normal()).exp().min(AMOUNT_CEILING);
    let amt = if rng.unit() < ROUND_SNAP_RATE {
        let step = if raw < 1_000.0 {
            100.0
        } else if raw < 10_000.0 {
            1_000.0
        } else if raw < 100_000.0 {
            10_000.0
        } else {
            100_000.0
        };
        (raw / step).round() * step
    } else {
        raw
    };
    (amt * 100.0).round() / 100.0
}

/// Amount tight against the local structuring band.
pub fn structuring_amount(rng: &mut Rng, ccy: &str) -> f64 {
    let (lo, hi) = structuring_band(ccy);
    let v = lo + rng.unit() * (hi - lo);
    (v * 100.0).round() / 100.0
}
