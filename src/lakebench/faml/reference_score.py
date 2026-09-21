"""FAML reference-detector metrics + leakage gate.

Two independent checks that answer the standing rule
"distribution checks do not prove semantics -- must run reference
detector + leakage check" (recorded in the maintainer's MEMORY as
`feedback_distribution_checks_dont_prove_semantics`).

**Leakage gate.** The FAML audit found that datagen writes typology
"structuring" transactions into a narrow currency-specific band
(USD ``9500..9999``) and that W2's detector filters on the same
band. Every planted structuring row lands in the exact window the
detector inspects, so W2 recall reduces to "is the schedule of >=3
txns/24h within an entity" -- guaranteed by construction. The
leakage gate compares baseline (log-normal) transaction density
against typology density inside the same band. If baseline density
is < 10 % of typology density, we mark the band as "label leaks
through the amount feature": a downstream reference detector that
sees the raw amount will pass by memorising the band, and the
benchmark's recall number is not informative about detector
skill. Threshold from the audit.

**Reference-detector metric.** A scikit-learn Gradient Boosted
Classifier trained on features that EXPLICITLY EXCLUDE
``amount_in_structuring_band`` and any function of it. If the model
still recovers reasonable per-typology recall, the data has signal
detectable by a canonical detector. If it collapses, the rules'
recall claims are effectively self-scoring against a planted trap.

Both checks return small dataclasses so callers can serialise them
to parquet, YAML, or console without further glue. Callers that
want a pass/fail decision read the ``verdict`` field; callers that
want to explain the number read ``hint``.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover -- only for type hints
    import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------


class LeakageVerdict(str, Enum):
    """Per-band leakage decision."""

    #: baseline_in_band >= 10 % of typology_in_band. Band is not the
    #: sole label; downstream detectors must use additional features.
    PASS = "pass"
    #: baseline_in_band < 10 % of typology_in_band. Any model that
    #: sees raw amount will memorise the band and inflate recall.
    LEAKING = "leaking"
    #: No typology transactions in this band; leakage is undefined
    #: because there is nothing to leak. Reported for completeness.
    NO_TYPOLOGY = "no_typology"


class ReferenceModelVerdict(str, Enum):
    """Overall reference-model outcome."""

    #: Model trained, evaluated, and results are usable.
    OK = "ok"
    #: scikit-learn (or an equally-canonical alternative) not
    #: available on the driver. Model skipped; leakage gate still ran.
    NO_SKLEARN = "no_sklearn"
    #: Model trained but there was not enough labelled data (fewer
    #: than ``min_positive_per_class`` typology instances) to compute
    #: a stable per-typology recall.
    INSUFFICIENT_LABELS = "insufficient_labels"


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BandLeakageRow:
    """One (currency, band) leakage measurement."""

    currency: str
    band_lo: float
    band_hi: float
    baseline_count: int
    typology_count: int
    ratio_baseline_over_typology: float
    verdict: LeakageVerdict
    hint: str


@dataclass(frozen=True)
class LeakageReport:
    """All bands scored under one run."""

    threshold_ratio: float
    rows: tuple[BandLeakageRow, ...]

    @property
    def overall_pass(self) -> bool:
        """True iff at least one band was scored and none is LEAKING.

        Bands with no typology transactions (``NO_TYPOLOGY``) do not
        affect the verdict: they carry no leakage claim. An empty
        report -- or one where every row is NO_TYPOLOGY -- does NOT
        pass. The correct answer to "no data seen" is "nothing
        scored", not "OK". P2 fix from PR-A adversarial review: the
        previous version returned True for an empty rows tuple,
        which would silently hide a datagen regression that stops
        planting structuring transactions in any band.
        """
        scored = [r for r in self.rows if r.verdict is not LeakageVerdict.NO_TYPOLOGY]
        if not scored:
            return False
        return all(r.verdict is not LeakageVerdict.LEAKING for r in scored)

    def as_dicts(self) -> list[dict[str, Any]]:
        """Serialisable rows for parquet / YAML."""
        return [asdict(r) | {"verdict": r.verdict.value} for r in self.rows]


@dataclass(frozen=True)
class TypologyRecallRow:
    """Per-typology precision/recall from the reference model."""

    typology_type: str
    support: int
    tp: int
    fp: int
    fn: int
    precision: float
    recall: float
    f1: float


@dataclass(frozen=True)
class ReferenceModelReport:
    """Reference-detector evaluation output."""

    verdict: ReferenceModelVerdict
    feature_names: tuple[str, ...]
    excluded_features: tuple[str, ...] = ()
    per_typology: tuple[TypologyRecallRow, ...] = field(default_factory=tuple)
    overall_precision: float = 0.0
    overall_recall: float = 0.0
    overall_f1: float = 0.0
    n_train: int = 0
    n_test: int = 0
    note: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict.value,
            "feature_names": list(self.feature_names),
            "excluded_features": list(self.excluded_features),
            "per_typology": [asdict(r) for r in self.per_typology],
            "overall_precision": self.overall_precision,
            "overall_recall": self.overall_recall,
            "overall_f1": self.overall_f1,
            "n_train": self.n_train,
            "n_test": self.n_test,
            "note": self.note,
        }


# ---------------------------------------------------------------------------
# Leakage gate
# ---------------------------------------------------------------------------


#: Default threshold ratio from the FAML audit. A band passes if
#: ``baseline_count / typology_count >= 0.10``. Below that ratio the
#: band is essentially a label indicator and a raw-amount feature
#: alone would perfectly predict typology membership. Not a hard
#: physical constant -- the number is defensible ("at least one
#: baseline row per ten typology rows"); if you have a stronger
#: argument for a tighter or looser bound, override it at the call
#: site and record the reason in the ``hint`` output.
DEFAULT_LEAKAGE_RATIO = 0.10


def compute_leakage_gate(
    bands: list[dict[str, Any]],
    threshold_ratio: float = DEFAULT_LEAKAGE_RATIO,
) -> LeakageReport:
    """Score each (currency, band) row.

    Args:
        bands: list of dicts, one per (currency, band_lo, band_hi).
            Each dict must carry ``currency`` (str), ``band_lo``
            (float), ``band_hi`` (float), ``baseline_count`` (int),
            ``typology_count`` (int). Extra keys are ignored.
        threshold_ratio: minimum ``baseline_count / typology_count``
            for the band to pass. Defaults to
            :data:`DEFAULT_LEAKAGE_RATIO` (0.10).

    Returns:
        :class:`LeakageReport` with a row per input band and a
        derived overall pass flag.
    """
    if threshold_ratio <= 0.0:
        raise ValueError("threshold_ratio must be > 0")

    rows: list[BandLeakageRow] = []
    for entry in bands:
        currency = str(entry["currency"])
        band_lo = float(entry["band_lo"])
        band_hi = float(entry["band_hi"])
        baseline = int(entry["baseline_count"])
        typology = int(entry["typology_count"])

        if typology == 0:
            verdict = LeakageVerdict.NO_TYPOLOGY
            ratio = float("inf") if baseline > 0 else 0.0
            hint = (
                f"No typology-structuring transactions in "
                f"{currency} [{band_lo:.0f}, {band_hi:.0f}]. "
                "Leakage is undefined; band recorded for completeness."
            )
        else:
            ratio = baseline / typology
            if ratio >= threshold_ratio:
                verdict = LeakageVerdict.PASS
                hint = (
                    f"baseline/typology = {ratio:.3f} >= "
                    f"{threshold_ratio:.2f} in {currency} "
                    f"[{band_lo:.0f}, {band_hi:.0f}]. Band is not the "
                    "sole label; downstream detector must use additional "
                    "features."
                )
            else:
                verdict = LeakageVerdict.LEAKING
                hint = (
                    f"baseline/typology = {ratio:.3f} < "
                    f"{threshold_ratio:.2f} in {currency} "
                    f"[{band_lo:.0f}, {band_hi:.0f}]. Every planted "
                    "structuring row falls in a band the detector inspects, "
                    "with too few baseline rows sharing the band to force a "
                    "model to learn additional features. Rule W2 recall on "
                    "this band is a tautology, not a detector skill claim. "
                    "Widen the baseline log-normal spread, narrow the "
                    "structuring band, or plant additional non-structuring "
                    "rows in this band before publishing recall."
                )
        rows.append(
            BandLeakageRow(
                currency=currency,
                band_lo=band_lo,
                band_hi=band_hi,
                baseline_count=baseline,
                typology_count=typology,
                ratio_baseline_over_typology=ratio,
                verdict=verdict,
                hint=hint,
            )
        )
    return LeakageReport(threshold_ratio=threshold_ratio, rows=tuple(rows))


# ---------------------------------------------------------------------------
# Reference detector (sklearn GBT)
# ---------------------------------------------------------------------------


#: Feature columns the reference model must NEVER see -- these are
#: either the label itself or exact functions of it. Adding to this
#: set widens the "signal must come from elsewhere" contract.
LEAKY_FEATURES: frozenset[str] = frozenset(
    {
        "amount_in_structuring_band",  # exact W2 filter output
        "is_structuring",  # any pre-labelled proxy
        "typology_type",  # the label
        "typology_id",  # the label
        "expected_workload",  # workload group the label belongs to
    }
)


def _sklearn_available() -> bool:
    """True iff scikit-learn is importable in this process."""
    try:  # pragma: no cover -- environment probe
        import sklearn  # noqa: F401

        return True
    except ImportError:
        return False


def train_reference_gbt(
    features: pd.DataFrame,
    labels: pd.Series,
    *,
    typology_names: list[str] | None = None,
    test_size: float = 0.25,
    min_positive_per_class: int = 10,
    random_state: int = 0,
    n_estimators: int = 100,
    max_depth: int = 3,
) -> ReferenceModelReport:
    """Train a GBT reference detector and evaluate per typology.

    Args:
        features: pandas DataFrame. Column names are the feature
            names. Rows must align with ``labels``. Any column named
            in :data:`LEAKY_FEATURES` is refused -- the caller must
            drop it before training. A ValueError names the offending
            columns so the failure is obvious.
        labels: pandas Series of typology-type strings, one per row.
            Rows with no typology are the negative class and should
            carry a sentinel string (``"baseline"`` by convention).
        typology_names: optional list of positive-class typology names
            to report on. If None, we take the sorted set of unique
            values in ``labels`` minus ``"baseline"``.
        test_size: fraction of rows held out for evaluation.
        min_positive_per_class: minimum instances of a typology in the
            test split for its recall to be reported. If any typology
            falls short, verdict is ``INSUFFICIENT_LABELS`` and the
            partial per-typology rows are still returned.
        random_state: seed for the train/test split and the model.
        n_estimators, max_depth: GBT hyperparameters. Kept small so
            the model does not memorise; the point is "can a
            canonical detector find signal in the data?", not
            "how well can we fit the data?".

    Returns:
        :class:`ReferenceModelReport` with per-typology precision /
        recall / F1, and an overall aggregate. Verdict is
        ``NO_SKLEARN`` if scikit-learn is not importable in this
        environment -- callers can then choose to skip vs. fail.
    """
    # Refuse leaky features up front. This is the single most
    # important guard the module offers.
    present_leaks = sorted(set(features.columns) & LEAKY_FEATURES)
    if present_leaks:
        raise ValueError(
            "Refusing to train reference model with leaky feature columns "
            f"{present_leaks}. These encode the label directly or are exact "
            "functions of it; a model trained on them would report a recall "
            "number that is a tautology, not a detector-skill claim. Drop "
            "them from `features` before calling."
        )

    if not _sklearn_available():
        return ReferenceModelReport(
            verdict=ReferenceModelVerdict.NO_SKLEARN,
            feature_names=tuple(features.columns),
            excluded_features=tuple(sorted(LEAKY_FEATURES)),
            note=(
                "scikit-learn not importable in this environment. Install "
                "scikit-learn on the Spark driver image (e.g. add "
                "`scikit-learn>=1.3` to the datagen requirements or the "
                "Spark image pip layer) to enable the reference detector. "
                "The leakage gate still ran without it."
            ),
        )

    # Local imports so module import stays cheap and the sklearn
    # dependency is only paid when we actually train.
    import numpy as np
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import train_test_split

    y = labels.to_numpy()
    X = features.to_numpy()

    # Multi-class GBT: baseline is one class; each typology is
    # another. Stratify so rare typologies aren't lost entirely to
    # the test split -- rarest class must have at least 2 rows for
    # stratify to work; fall back to non-stratified if not.
    _, counts = np.unique(y, return_counts=True)
    can_stratify = counts.min() >= 2
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=y if can_stratify else None,
    )

    clf = GradientBoostingClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
    )
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)

    # Positive-class names default to "everything in labels except
    # the baseline sentinel." Callers who plant additional negatives
    # (e.g. dormant baseline, low-risk baseline) should pass the list
    # explicitly.
    if typology_names is None:
        typology_names = sorted(set(y) - {"baseline"})

    per_typology_rows: list[TypologyRecallRow] = []
    total_tp = total_fp = total_fn = 0
    insufficient = False
    for name in typology_names:
        # One-vs-rest metrics for this typology.
        y_test_pos = y_test == name
        y_pred_pos = y_pred == name
        tp = int((y_test_pos & y_pred_pos).sum())
        fp = int((~y_test_pos & y_pred_pos).sum())
        fn = int((y_test_pos & ~y_pred_pos).sum())
        support = int(y_test_pos.sum())
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        per_typology_rows.append(
            TypologyRecallRow(
                typology_type=name,
                support=support,
                tp=tp,
                fp=fp,
                fn=fn,
                precision=precision,
                recall=recall,
                f1=f1,
            )
        )
        total_tp += tp
        total_fp += fp
        total_fn += fn
        if support < min_positive_per_class:
            insufficient = True

    overall_p = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0.0
    overall_r = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0.0
    overall_f1 = (
        2 * overall_p * overall_r / (overall_p + overall_r) if (overall_p + overall_r) > 0 else 0.0
    )

    return ReferenceModelReport(
        verdict=(
            ReferenceModelVerdict.INSUFFICIENT_LABELS if insufficient else ReferenceModelVerdict.OK
        ),
        feature_names=tuple(features.columns),
        excluded_features=tuple(sorted(LEAKY_FEATURES)),
        per_typology=tuple(per_typology_rows),
        overall_precision=overall_p,
        overall_recall=overall_r,
        overall_f1=overall_f1,
        n_train=int(X_train.shape[0]),
        n_test=int(X_test.shape[0]),
        note=(
            "Per-typology support < min_positive_per_class "
            f"({min_positive_per_class}); recall figures are unstable. "
            "Rerun at higher scale or lower min_positive_per_class."
            if insufficient
            else "OK"
        ),
    )
