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
        // A payment smaller than half the step would round to 0.00; nobody
        // rounds a $30 payment to nothing, so small amounts keep their value.
        let snapped = (raw / step).round() * step;
        if snapped > 0.0 {
            snapped
        } else {
            raw
        }
    } else {
        raw
    };
    let m = minor_units(ccy);
    // Never below one minor unit: a zero-value payment is not a payment.
    ((amt * m).round() / m).max(1.0 / m)
}

/// Local cash-reporting threshold (USD 10,000 CTR and local equivalents),
/// the same figures as detection_rules._STRUCTURING_THRESHOLDS: it is the
/// regulation both sides read, not a rule parameter.
pub fn reporting_threshold(ccy: &str) -> f64 {
    match ccy {
        "USD" | "CAD" | "AUD" => 10_000.0,
        "GBP" | "EUR" | "CHF" => 15_000.0,
        "JPY" | "INR" => 1_000_000.0,
        "AED" => 55_000.0,
        "SGD" => 20_000.0,
        "MXN" => 100_000.0,
        "CNY" | "BRL" => 50_000.0,
        "HKD" => 75_000.0,
        "KRW" => 10_000_000.0,
        _ => 10_000.0,
    }
}

/// Deepest a structured payment goes below the threshold, as a fraction of
/// it. Self-chosen (no primary source gives a distribution): structurers stay
/// under the threshold, most of them close to it, and some well below so the
/// deposits do not all look alike (the FFIEC BSA/AML manual's structuring
/// examples are "just under" amounts, and it also describes varying amounts to
/// avoid an obvious pattern).
pub const STRUCTURED_MAX_DEPTH: f64 = 0.4;

/// A structured payment: under the local reporting threshold by a depth of
/// `STRUCTURED_MAX_DEPTH * (1 - sqrt(u))` of it. The depth's density is
/// triangular, highest at the threshold and falling linearly to zero at the
/// maximum depth: dense near the threshold but finite there (no pile-up on the
/// threshold itself), no step at W2's 90% band floor (R2), and it stops at the
/// threshold, which is what structuring is. About 44% of structured payments
/// land in W2's band. One RNG draw, as the old fixed-band draw took.
pub fn structuring_amount(rng: &mut Rng, ccy: &str) -> f64 {
    let t = reporting_threshold(ccy);
    let u = rng.unit();
    let v = t * (1.0 - STRUCTURED_MAX_DEPTH * (1.0 - u.sqrt()));
    // In the currency's minor units: a fractional yen or won would be a label.
    let m = minor_units(ccy);
    // Strictly under the threshold: a payment at it is reported.
    ((v * m).round() / m).min(t - 1.0 / m)
}

/// Typologies whose amounts come from their own instance-keyed stream, with
/// the number of shared-stream draws each of their rows took before they were
/// reworked. The driver replays (discards) that many draws on the shared
/// stream, so every other typology keeps exactly the amounts it had. Each of
/// the three still emits the same total number of rows, so the replay matches
/// the old consumption (3 draws for a persona amount, 1 for a band amount).
pub fn own_amount_stream(typ: &str) -> Option<usize> {
    match typ {
        "corridor_high_risk" | "dormant_reactivation" => Some(3),
        "micro_structuring" => Some(1),
        _ => None,
    }
}

/// Typologies that move one pot of money along a chain: each leg after the
/// first forwards what the previous leg delivered (emit order is chain order
/// for all four; see typology::emit_instance, and placement.rs preserves it).
pub fn is_chained(typ: &str) -> bool {
    matches!(
        typ,
        "rapid_layering" | "stack" | "cycle" | "cross_border_cycle"
    )
}

/// Per-hop skim range. Money mules and layering intermediaries keep a
/// commission on what they pass on. Europol's EMMA (European Money Mule
/// Action) press releases describe mules as paid by commission but give no
/// figure; the commonly quoted recruitment offer is "keep 10% and wire the
/// rest" (Wikipedia "Money mule"; NASAA investor advisory on money mules),
/// hence single-digit percent up to about 10%. Secondary sources only: a
/// primary figure is still to be found. The range is a realism parameter,
/// not tuned to any rule's pass-through threshold.
pub const SKIM_MIN: f64 = 0.01;
pub const SKIM_MAX: f64 = 0.10;

/// Amount of a forwarding leg: the previous leg's USD value less a
/// U(SKIM_MIN, SKIM_MAX) skim, expressed in the forwarding account's currency
/// (legs can change currency) and rounded to its minor units. No round-number
/// snapping: a forwarded remainder is an odd amount.
pub fn forwarded_amount(rng: &mut Rng, prev_usd: f64, ccy: &str) -> f64 {
    let skim = SKIM_MIN + rng.unit() * (SKIM_MAX - SKIM_MIN);
    let usd = prev_usd * (1.0 - skim);
    let m = minor_units(ccy);
    // At least one minor unit, so a long chain never forwards nothing.
    ((usd / fx_to_usd(ccy) * m).round() / m).max(1.0 / m)
}

/// Amounts for one instance's rows, in row order. `ccy_of(orig)` and
/// `shift_of(orig)` give the originator's currency and persona amount shift.
/// Structuring rows draw from the band; the first leg of a chained typology
/// and every other row keep the persona draw; later chained legs forward the
/// previous leg (see `forwarded_amount`).
pub fn instance_amounts<'a>(
    typ: &str,
    rows: &[crate::typology::TxRow],
    ccy_of: impl Fn(u64) -> &'a str,
    shift_of: impl Fn(u64) -> f64,
    rng: &mut Rng,
) -> Vec<f64> {
    let chained = is_chained(typ);
    let mut prev_usd: Option<f64> = None;
    rows.iter()
        .map(|r| {
            let ccy = ccy_of(r.orig);
            let amt = match (chained, prev_usd) {
                (true, Some(p)) => forwarded_amount(rng, p, ccy),
                _ if r.structuring => structuring_amount(rng, ccy),
                _ => native_amount(rng, shift_of(r.orig), ccy),
            };
            prev_usd = Some(amt * fx_to_usd(ccy));
            amt
        })
        .collect()
}
