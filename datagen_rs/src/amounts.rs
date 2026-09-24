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
    lognormal_amount_shifted(rng, 0.0)
}

/// Log-normal amount with a per-account additive shift to the log-mean, then
/// round-number snapping, in USD. `mu_shift` is the account's persona amount
/// shift (`world::amount_log_shift`); 0.0 reproduces the population-default draw.
pub fn lognormal_amount_shifted(rng: &mut Rng, mu_shift: f64) -> f64 {
    native_amount(rng, mu_shift, "USD")
}

/// Minor units per currency (JPY and KRW have none).
fn minor_units(ccy: &str) -> f64 {
    match ccy {
        "JPY" | "KRW" => 1.0,
        _ => 100.0,
    }
}

/// Amount in the account's own currency (LB-137). The log-normal is defined
/// in USD, where the bank model's median and tail live, and the draw is then
/// expressed in `ccy`. Drawing every currency on the USD scale left JPY, INR
/// and KRW baselines about 150x too small, so almost no baseline payment sat in
/// those currencies' structuring bands and a structuring row there was a label.
/// Snapping to round numbers happens in `ccy`, since people round in the
/// currency they pay in. For USD the result, and the RNG draw order (normal,
/// then unit), are identical to the old USD-only draw.
pub fn native_amount(rng: &mut Rng, mu_shift: f64, ccy: &str) -> f64 {
    let fx = fx_to_usd(ccy);
    let raw_usd = ((LN_MU + mu_shift) + LN_SIGMA * rng.normal())
        .exp()
        .min(AMOUNT_CEILING);
    let raw = raw_usd / fx;
    let amt = if rng.unit() < ROUND_SNAP_RATE {
        let step_usd = if raw_usd < 1_000.0 {
            100.0
        } else if raw_usd < 10_000.0 {
            1_000.0
        } else if raw_usd < 100_000.0 {
            10_000.0
        } else {
            100_000.0
        };
        // The same step in `ccy`, snapped to a power of ten.
        let step = if fx == 1.0 {
            step_usd
        } else {
            10f64.powf((step_usd / fx).log10().round()).max(1.0)
        };
        (raw / step).round() * step
    } else {
        raw
    };
    let m = minor_units(ccy);
    (amt * m).round() / m
}

/// Amount tight against the local structuring band.
pub fn structuring_amount(rng: &mut Rng, ccy: &str) -> f64 {
    let (lo, hi) = structuring_band(ccy);
    let v = lo + rng.unit() * (hi - lo);
    (v * 100.0).round() / 100.0
}
