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
//!   The existing cap to what the corpus can hold still applies.

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
