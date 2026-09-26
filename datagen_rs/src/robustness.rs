//! Robustness corpus perturbation (AML-GOALS R3(b), Level 2 condition 5).
//!
//! The robustness corpus shifts three nuisance parameters in natural units so
//! Level 2 is shown not to depend on the exact values the calibration corpus
//! was tuned on. The multipliers are the pre-registration's
//! `corpora.robustness_perturbation` block; tests/test_datagen_seed.py fails if
//! these constants drift from it.
//!
//! What each one does (all RNG draws are unchanged, so the perturbed corpus has
//! the same instances, participants, row counts and files as the unperturbed
//! one; only the values below move):
//!
//! - `median_amount`: every persona amount draw is log-normal with median
//!   `exp(LN_MU + shift)`. The median is multiplied, so the log-mean moves by
//!   `ln(m)` (added to the per-account shift), never `LN_MU * m`.
//! - `persona_sd`: the per-account log-sds (`RATE_LOG_SD`, `AMOUNT_LOG_SD`) are
//!   multiplied around unchanged centres: the activity multiplier keeps median
//!   1x, and the amount shift keeps its base recentring, so the population
//!   median amount does not move with the sd (the population mean rises, by
//!   about 8% at 1.2, as a wider log-normal with a fixed median does). Each
//!   knob therefore moves one statistic: the median, or the log-spread.
//! - `dormancy`: every dormant_reactivation dormancy length is multiplied (both
//!   bounds of the log-uniform range, so 45..365 d becomes 54..438 d at 1.2).
//!   The existing cap to what the corpus can hold still applies. The tail
//!   passes the scoring unit's history window (prereg unit_of_scoring:
//!   lead_in_days 14 + history_days 395): for roughly the top 3.5% of
//!   perturbed episodes (about 407 d and up, depending on where the burst
//!   falls in its month) the pre-gap send lies outside H, so
//!   days_since_prior_send is null and the history ratios see an empty H, as
//!   for a new account. The unperturbed range (max 365 d) never reaches it.

/// Pre-registration `corpora.robustness_perturbation.median_amount_multiplier`.
pub const ROBUSTNESS_MEDIAN_AMOUNT_MULTIPLIER: f64 = 1.2;
/// Pre-registration `corpora.robustness_perturbation.persona_sd_multiplier`.
pub const ROBUSTNESS_PERSONA_SD_MULTIPLIER: f64 = 1.2;
/// Pre-registration `corpora.robustness_perturbation.dormancy_range_multiplier`.
pub const ROBUSTNESS_DORMANCY_RANGE_MULTIPLIER: f64 = 1.2;

/// The pre-registered robustness seed (`corpora.robustness_seed`). The driver
/// refuses it without the perturbation, so a corpus generated from it is the
/// perturbed one by construction.
pub const ROBUSTNESS_SEED: i64 = 90_000_042;
/// The pre-registered evaluation seed (`corpora.evaluation_seed`). The driver
/// refuses it with the perturbation.
pub const EVALUATION_SEED: i64 = 50_000_043;

/// Manifest stamp (injection_parameters map keys). A perturbed corpus carries
/// these on every manifest row; an unperturbed one carries none, so its
/// manifest bytes are unchanged. The scorer and scripts/aml_gate.py require
/// the stamp for a registered robustness look and refuse it on any other
/// role, so a corpus from an image without the perturbation cannot be scored
/// as the robustness corpus. src/lakebench/config/datagen_seed.py mirrors the
/// key names (tests/test_datagen_robustness.py ties them).
pub const MANIFEST_STAMP_KEY: &str = "robustness_perturbation";
pub const MANIFEST_MEDIAN_AMOUNT_KEY: &str = "robustness_median_amount_multiplier";
pub const MANIFEST_PERSONA_SD_KEY: &str = "robustness_persona_sd_multiplier";
pub const MANIFEST_DORMANCY_KEY: &str = "robustness_dormancy_range_multiplier";

/// Natural-unit multipliers on the three nuisance parameters. `NONE` is the
/// identity: every multiplier is exactly 1.0, and each use site is written so
/// that 1.0 reproduces the unperturbed arithmetic bit for bit.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Perturbation {
    pub median_amount: f64,
    pub persona_sd: f64,
    pub dormancy: f64,
}

impl Perturbation {
    pub const NONE: Perturbation = Perturbation {
        median_amount: 1.0,
        persona_sd: 1.0,
        dormancy: 1.0,
    };

    pub const REGISTERED: Perturbation = Perturbation {
        median_amount: ROBUSTNESS_MEDIAN_AMOUNT_MULTIPLIER,
        persona_sd: ROBUSTNESS_PERSONA_SD_MULTIPLIER,
        dormancy: ROBUSTNESS_DORMANCY_RANGE_MULTIPLIER,
    };

    /// Manifest entries for this perturbation: empty for `NONE`, otherwise
    /// the stamp and the three multipliers.
    pub fn manifest_entries(&self) -> Vec<(&'static str, String)> {
        if *self == Perturbation::NONE {
            return Vec::new();
        }
        vec![
            (MANIFEST_STAMP_KEY, "true".to_string()),
            (
                MANIFEST_MEDIAN_AMOUNT_KEY,
                format!("{}", self.median_amount),
            ),
            (MANIFEST_PERSONA_SD_KEY, format!("{}", self.persona_sd)),
            (MANIFEST_DORMANCY_KEY, format!("{}", self.dormancy)),
        ]
    }

    /// Log-mean shift for the median-amount multiplier: `ln(m)`.
    #[inline]
    pub fn amount_log_mu_shift(&self) -> f64 {
        self.median_amount.ln()
    }
}

/// The perturbation for a financial run of `seed`, `on` when the driver was
/// given `--robustness-perturbation`. The robustness seed is refused without
/// it and the evaluation seed with it, so a corpus from either registered seed
/// is the right corpus by construction; the scoring side cannot tell a
/// perturbed corpus from its manifest, so this is what makes the robustness
/// look safe.
pub fn perturbation_for_seed(seed: i64, on: bool) -> Result<Perturbation, String> {
    if seed == ROBUSTNESS_SEED && !on {
        return Err(format!(
            "--seed {seed} is the pre-registered robustness seed: it is generated only \
             with --robustness-perturbation (corpora.robustness_perturbation)"
        ));
    }
    if seed == EVALUATION_SEED && on {
        return Err(format!(
            "--seed {seed} is the pre-registered evaluation seed: it is never perturbed"
        ));
    }
    Ok(if on {
        Perturbation::REGISTERED
    } else {
        Perturbation::NONE
    })
}

/// The driver flag that turns the registered perturbation on.
pub const FLAG: &str = "--robustness-perturbation";

/// Is `FLAG` present as a flag in `argv` (argv[0] is the program)? Every other
/// driver flag takes a value, so a `--name` token without `=` consumes the next
/// token as its value and that token is never read as a flag: `--prefix
/// --robustness-perturbation` does not turn the perturbation on. `FLAG=value`
/// is an error. A misparse can only turn the perturbation off, which the
/// driver refuses for the robustness seed.
pub fn flag_in_argv(argv: &[String]) -> Result<bool, String> {
    let mut on = false;
    let mut i = 1;
    while i < argv.len() {
        let a = argv[i].as_str();
        if a == FLAG {
            on = true;
            i += 1;
        } else if a.starts_with(&format!("{FLAG}=")) {
            return Err(format!("{FLAG} takes no value; got {a:?}"));
        } else if a.starts_with("--") && !a.contains('=') {
            i += 2;
        } else {
            i += 1;
        }
    }
    Ok(on)
}
