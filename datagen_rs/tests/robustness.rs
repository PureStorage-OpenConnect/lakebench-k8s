//! Robustness perturbation (AML-GOALS R3(b), Level 2 condition 5; prereg
//! corpora.robustness_perturbation). The default path is pinned so the flag
//! cannot move an unperturbed corpus, and the perturbed path is checked to
//! move exactly the three registered nuisance parameters, in natural units.

use datagen_rs::model::build_world_ex;
use datagen_rs::typology::{schedule, Instance};

const DEV_SEED: i64 = 7777;
const DAY: i64 = 86_400_000_000;

fn fnv(h: &mut u64, bytes: &[u8]) {
    for &x in bytes {
        *h ^= x as u64;
        *h = h.wrapping_mul(0x0100_0000_01b3);
    }
}

// Values are quantised to 1e-9 before hashing. The world is built with
// f64::exp / f64::ln, which come from the platform libm, so raw bits differ in
// the last ulp between hosts (RHEL dev host vs the CI runner). Quantising keeps
// the pin portable while any real data change still moves it. Bit-exact output
// is only guaranteed within one build environment (the pinned datagen image).
fn q(v: f64) -> i64 {
    (v * 1e9).round() as i64
}

fn world_digest(activity: &[f64], logshift: &[f64], total: f64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for v in activity
        .iter()
        .chain(logshift)
        .chain(std::iter::once(&total))
    {
        fnv(&mut h, &q(*v).to_le_bytes());
    }
    h
}

fn schedule_digest(insts: &[Instance]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for i in insts {
        fnv(&mut h, i.typ.as_bytes());
        for p in &i.participants {
            fnv(&mut h, &p.to_le_bytes());
        }
        for v in [
            i.start_us,
            i.end_us,
            i.suppress_start_us,
            i.suppress_end_us,
            i.seed,
            i.rows_per_instance as i64,
        ] {
            fnv(&mut h, &v.to_le_bytes());
        }
    }
    h
}

fn corpus() -> (i64, i64) {
    let start = 1_600_000_000i64 * 1_000_000;
    (start, start + 1826 * DAY)
}

#[test]
fn default_world_and_schedule_are_pinned() {
    // Quantised digest captured on c9d7202 (corpus output byte-identical to c0d658f,
    // the pre-perturbation point, verified by hashing all 67 files) on dev seed 7777.
    // A change here is an AML data change, not a refactor.
    let w = build_world_ex(0.05, DEV_SEED, 60, false);
    let (s, e) = corpus();
    let insts = schedule(
        DEV_SEED,
        w.dims.total_txns(),
        w.population,
        s,
        e,
        &w.country,
    );
    let got = (
        world_digest(&w.activity, &w.amount_logshift, w.total_activity),
        schedule_digest(&insts),
    );
    assert_eq!(
        got,
        (10_581_863_675_670_526_329, 8_773_633_312_168_967_265),
        "default AML world or schedule changed"
    );
}

// ---------------------------------------------------------------------------
// The perturbed path moves the three registered parameters, in natural units,
// and nothing else.
// ---------------------------------------------------------------------------

use datagen_rs::amounts::native_amount;
use datagen_rs::hash::Rng;
use datagen_rs::model::build_world_p;
use datagen_rs::robustness::{
    Perturbation, ROBUSTNESS_DORMANCY_RANGE_MULTIPLIER, ROBUSTNESS_MEDIAN_AMOUNT_MULTIPLIER,
    ROBUSTNESS_PERSONA_SD_MULTIPLIER,
};
use datagen_rs::typology::{schedule_p, DORMANCY_MAX_DAYS, DORMANCY_MIN_DAYS};
use datagen_rs::world::{BASELINE_ACTIVITY, TYPE_PERSON};

const SCALE: f64 = 0.05;

fn only(median_amount: f64, persona_sd: f64, dormancy: f64) -> Perturbation {
    Perturbation {
        median_amount,
        persona_sd,
        dormancy,
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[test]
fn none_is_the_default_path() {
    // Perturbation::NONE through the _p entry points is the default world and
    // schedule, bit for bit (the pinned digests above).
    let w = build_world_p(SCALE, DEV_SEED, 60, false, &Perturbation::NONE);
    let (s, e) = corpus();
    let insts = schedule_p(
        DEV_SEED,
        DEV_SEED,
        w.dims.total_txns(),
        w.population,
        s,
        e,
        &w.country,
        &Perturbation::NONE,
    );
    assert_eq!(
        (
            world_digest(&w.activity, &w.amount_logshift, w.total_activity),
            schedule_digest(&insts),
        ),
        (10_581_863_675_670_526_329, 8_773_633_312_168_967_265),
    );
}

#[test]
fn registered_multipliers_are_1_2() {
    // The prereg's values; tests/test_datagen_seed.py ties these constants to
    // the JSON itself.
    assert_eq!(
        Perturbation::REGISTERED,
        only(
            ROBUSTNESS_MEDIAN_AMOUNT_MULTIPLIER,
            ROBUSTNESS_PERSONA_SD_MULTIPLIER,
            ROBUSTNESS_DORMANCY_RANGE_MULTIPLIER
        )
    );
    assert_eq!(Perturbation::REGISTERED, only(1.2, 1.2, 1.2));
}

#[test]
fn median_amount_moves_the_log_mean_by_ln_m_only() {
    let base = build_world_p(SCALE, DEV_SEED, 60, false, &Perturbation::NONE);
    let p = build_world_p(SCALE, DEV_SEED, 60, false, &only(1.2, 1.0, 1.0));
    let ln_m = 1.2f64.ln();
    for i in 1..=base.population {
        let d = p.amount_logshift[i] - base.amount_logshift[i];
        assert!(
            (d - ln_m).abs() < 1e-12,
            "entity {i}: log shift moved by {d}"
        );
    }
    // Not LN_MU * 1.2 (a 5.5x median shift, AML-GOALS section 9 #25).
    assert!((ln_m - 0.1823).abs() < 1e-4);
    // Activity untouched.
    assert_eq!(p.activity, base.activity);
    // The drawn amounts: same RNG stream, so the population median of persona
    // draws moves by the multiplier up to round-number snapping.
    let draws = |w: &datagen_rs::model::World| -> Vec<f64> {
        let mut rng = Rng::new(99);
        (0..200_000)
            .map(|k| {
                let o = 1 + (k % w.population);
                native_amount(&mut rng, w.amount_logshift[o], "USD")
            })
            .collect()
    };
    let r = median(draws(&p)) / median(draws(&base));
    assert!((r - 1.2).abs() < 0.03, "median amount ratio {r}");
}

#[test]
fn persona_sd_scales_the_log_spread_around_fixed_centres() {
    let base = build_world_p(SCALE, DEV_SEED, 60, false, &Perturbation::NONE);
    let p = build_world_p(SCALE, DEV_SEED, 60, false, &only(1.0, 1.2, 1.0));
    let c = -0.5 * datagen_rs::world::AMOUNT_LOG_SD.powi(2);
    for i in 1..=base.population {
        // Amount: deviation from the unchanged centre scales by exactly 1.2.
        let (b, q) = (base.amount_logshift[i] - c, p.amount_logshift[i] - c);
        assert!((q - 1.2 * b).abs() < 1e-12, "entity {i}: {b} -> {q}");
        // Activity: log of the rate multiplier (median 1x) scales by 1.2.
        let t = base.ty[i];
        if t >= 0 {
            let per_type = BASELINE_ACTIVITY[t as usize];
            let (lb, lq) = (
                (base.activity[i] / per_type).ln(),
                (p.activity[i] / per_type).ln(),
            );
            assert!((lq - 1.2 * lb).abs() < 1e-9, "entity {i}: {lb} -> {lq}");
        }
    }
    // Everything else in the world is the same.
    assert_eq!(p.ty, base.ty);
    assert_eq!(p.country, base.country);
    assert_eq!(p.ccy, base.ccy);
    assert_eq!(p.ring_sz, base.ring_sz);
    assert_eq!(p.n_accounts, base.n_accounts);
    assert_eq!(p.iban, base.iban);
    assert!(p.ty.contains(&TYPE_PERSON));
}

#[test]
fn dormancy_scales_each_episode_and_nothing_else_in_the_schedule() {
    let w = build_world_p(SCALE * 10.0, DEV_SEED, 60, true, &Perturbation::NONE);
    let (s, e) = corpus();
    let run = |p: &Perturbation| {
        schedule_p(
            DEV_SEED,
            DEV_SEED,
            w.dims.total_txns(),
            w.population,
            s,
            e,
            &w.country,
            p,
        )
    };
    let base = run(&Perturbation::NONE);
    let pert = run(&Perturbation::REGISTERED);
    // Same instances: planting rates, participants, seeds and row counts.
    assert_eq!(base.len(), pert.len());
    let mut n_dorm = 0;
    let (mut sum_b, mut sum_p) = (0.0, 0.0);
    for (b, q) in base.iter().zip(&pert) {
        assert_eq!(
            (b.typ, &b.participants, b.seed, b.rows_per_instance),
            (q.typ, &q.participants, q.seed, q.rows_per_instance)
        );
        if b.typ != "dormant_reactivation" {
            // Non-dormant windows do not move.
            assert_eq!((b.start_us, b.end_us), (q.start_us, q.end_us));
            assert_eq!((q.suppress_start_us, q.suppress_end_us), (0, 0));
            continue;
        }
        n_dorm += 1;
        // Burst span unchanged.
        assert_eq!(b.end_us - b.start_us, q.end_us - q.start_us);
        let db = (b.start_us - b.suppress_start_us) / DAY;
        let dq = (q.start_us - q.suppress_start_us) / DAY;
        // d = floor(45 * r^u) and floor(1.2 * 45 * r^u) for the same u.
        assert!(
            dq >= (1.2 * db as f64).floor() as i64 && dq <= (1.2 * (db + 1) as f64).ceil() as i64,
            "dormancy {db} d -> {dq} d"
        );
        let (lo, hi) = (
            (1.2 * DORMANCY_MIN_DAYS) as i64,
            (1.2 * DORMANCY_MAX_DAYS) as i64,
        );
        assert!(
            (lo..=hi).contains(&dq),
            "dormancy {dq} d outside {lo}..={hi}"
        );
        sum_b += db as f64;
        sum_p += dq as f64;
    }
    assert!(n_dorm > 20, "only {n_dorm} dormant instances");
    let r = sum_p / sum_b;
    assert!((r - 1.2).abs() < 0.01, "mean dormancy ratio {r}");
}

#[test]
fn registered_seeds_get_the_right_corpus_only() {
    use datagen_rs::robustness::{perturbation_for_seed, EVALUATION_SEED, ROBUSTNESS_SEED};
    // Pure decision function only: the registered seeds are never generated
    // here.
    assert!(perturbation_for_seed(ROBUSTNESS_SEED, false).is_err());
    assert_eq!(
        perturbation_for_seed(ROBUSTNESS_SEED, true),
        Ok(Perturbation::REGISTERED)
    );
    assert!(perturbation_for_seed(EVALUATION_SEED, true).is_err());
    assert_eq!(
        perturbation_for_seed(EVALUATION_SEED, false),
        Ok(Perturbation::NONE)
    );
    assert_eq!(
        perturbation_for_seed(DEV_SEED, false),
        Ok(Perturbation::NONE)
    );
    assert_eq!(
        perturbation_for_seed(DEV_SEED, true),
        Ok(Perturbation::REGISTERED)
    );
}

#[test]
fn flag_is_parsed_as_a_flag_not_a_value() {
    use datagen_rs::robustness::flag_in_argv;
    let v = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let f = "--robustness-perturbation";
    assert_eq!(flag_in_argv(&v(&["gen"])), Ok(false));
    assert_eq!(flag_in_argv(&v(&["gen", f])), Ok(true));
    assert_eq!(flag_in_argv(&v(&["gen", "--seed", "7777", f])), Ok(true));
    assert_eq!(flag_in_argv(&v(&["gen", f, "--seed", "7777"])), Ok(true));
    assert_eq!(flag_in_argv(&v(&["gen", "--prefix=x", f])), Ok(true));
    // A flag's value is never the flag.
    assert_eq!(flag_in_argv(&v(&["gen", "--prefix", f])), Ok(false));
    assert_eq!(
        flag_in_argv(&v(&["gen", "--bucket", f, "--seed", "7777"])),
        Ok(false)
    );
    assert!(flag_in_argv(&v(&["gen", "--robustness-perturbation=1"])).is_err());
}

fn manifest_params(b: &arrow::record_batch::RecordBatch) -> Vec<Vec<(String, String)>> {
    use arrow::array::{Array, MapArray, StringArray};
    let m = b
        .column_by_name("injection_parameters")
        .unwrap()
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();
    (0..m.len())
        .map(|r| {
            let e = m.value(r);
            let k = e.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let v = e.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            (0..k.len())
                .map(|j| (k.value(j).to_string(), v.value(j).to_string()))
                .collect()
        })
        .collect()
}

#[test]
fn manifest_carries_the_stamp_only_when_perturbed() {
    use datagen_rs::party::{build_manifest, build_manifest_p};
    let w = build_world_ex(SCALE, DEV_SEED, 60, false);
    let (s, e) = corpus();
    let insts = schedule(
        DEV_SEED,
        w.dims.total_txns(),
        w.population,
        s,
        e,
        &w.country,
    );
    let uids = std::collections::HashMap::new();
    let plain = build_manifest(&insts, DEV_SEED, &uids);
    let none = build_manifest_p(&insts, DEV_SEED, &uids, &Perturbation::NONE);
    assert_eq!(plain, none);
    for row in manifest_params(&none) {
        assert_eq!(row.len(), 1);
        assert_eq!(row[0].0, "rows_per_instance");
    }
    let stamped = build_manifest_p(&insts, DEV_SEED, &uids, &Perturbation::REGISTERED);
    let rows = manifest_params(&stamped);
    assert_eq!(rows.len(), insts.len());
    for (row, inst) in rows.iter().zip(&insts) {
        let want: Vec<(String, String)> = [
            ("rows_per_instance", inst.rows_per_instance.to_string()),
            ("robustness_perturbation", "true".to_string()),
            ("robustness_median_amount_multiplier", "1.2".to_string()),
            ("robustness_persona_sd_multiplier", "1.2".to_string()),
            ("robustness_dormancy_range_multiplier", "1.2".to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(row, &want);
    }
}

#[test]
fn perturbed_world_keeps_structuring_band_baseline_density() {
    // The band-density leakage guards in regression.rs
    // (persona_preserves_structuring_band_baseline_density,
    // every_currency_has_baseline_mass_in_its_structuring_band), rerun on the
    // registered perturbation: a starved band would make a structuring row a
    // label on the robustness corpus. Compared against the unperturbed world
    // on the same draws, because the USD guard's absolute 1.0% floor sits
    // inside its own seed-to-seed noise (1.007% on 0xB0BA, 0.967% on 0xB0BB,
    // unperturbed; perturbed 0.994% and 0.995%).
    use datagen_rs::amounts::{native_amount, structuring_band};
    use datagen_rs::world::amount_log_shift_p;
    let n = 400_000u64;
    let density = |p: &Perturbation, ccy: &str, lo: f64, hi: f64| -> f64 {
        let mut rng = Rng::new(0xB0BB);
        let mut k = 0u64;
        for id in 1..=n {
            let shift = amount_log_shift_p(id, 42, p.persona_sd, p.amount_log_mu_shift());
            if (lo..=hi).contains(&native_amount(&mut rng, shift, ccy)) {
                k += 1;
            }
        }
        k as f64 / n as f64
    };
    for ccy in [
        "USD", "GBP", "EUR", "CHF", "JPY", "AED", "SGD", "CAD", "MXN", "CNY", "INR", "AUD", "HKD",
        "KRW", "BRL",
    ] {
        // USD: the regression test's [9500, 9999] band.
        let (lo, hi) = if ccy == "USD" {
            (9500.0, 9999.0)
        } else {
            structuring_band(ccy)
        };
        let base = density(&Perturbation::NONE, ccy, lo, hi);
        let pert = density(&Perturbation::REGISTERED, ccy, lo, hi);
        assert!(
            pert >= 0.001 && pert >= 0.9 * base,
            "{ccy}: structuring-band baseline density {:.4}% perturbed vs {:.4}% unperturbed",
            pert * 100.0,
            base * 100.0
        );
    }
}
