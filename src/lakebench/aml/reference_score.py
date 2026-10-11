"""AML band leakage gate.

One of two checks that answer the standing rule
"distribution checks do not prove semantics -- must run reference
detector + leakage check" (recorded in the maintainer's MEMORY as
`feedback_distribution_checks_dont_prove_semantics`).

**Leakage gate.** The AML audit found that datagen writes typology
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

The reference detector itself is ``lakebench.aml.fidelity_gate``.

The gate returns small dataclasses so callers can serialise them
to parquet, YAML, or console without further glue. Callers that
want a pass/fail decision read the ``verdict`` field; callers that
want to explain the number read ``hint``.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

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


# ---------------------------------------------------------------------------
# Leakage gate
# ---------------------------------------------------------------------------


#: Default threshold ratio from the AML audit. A band passes if
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


def _sklearn_available() -> bool:
    """True iff scikit-learn is importable in this process."""
    try:  # pragma: no cover -- environment probe
        import sklearn  # noqa: F401

        return True
    except ImportError:
        return False
