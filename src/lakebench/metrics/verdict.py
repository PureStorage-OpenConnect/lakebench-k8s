"""Verdict value object for a Lakebench run.

Historically a run reported a single ``success`` boolean. That was too coarse:
the process exit code, the HTML badge, and the underlying pipeline / benchmark
gate outcomes can disagree. A run whose CLI exited 0 with all jobs succeeded
can still be FAILED by the badge (e.g. LB-044, sustained runs where bronze
never kept pace with the trickle, or gold was stale for most of the window).

This module defines the ``Verdict`` value object, the shared badge helper
(``compute_badge_status``) that both this module and ``reports.generator``
use, and the small readers (``has_verdict``, ``verdict_status``, ``passed``)
that every raw-``success`` consumer goes through so v1.6 records prefer the
verdict and v1.5 records fall back to the legacy flag.

Owner decision OD-6: v1.6 ``success == (verdict.status == "PASSED")``. A
v1.5 record (no verdict block) is legacy: never comparable, never a perf
baseline.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    from lakebench.metrics.collector import PipelineMetrics


VerdictStatus = Literal["PASSED", "FAILED", "REFUSED", "INTERRUPTED"]

# Gate outcomes use present-tense vocab (PASS/FAIL/REFUSED/INTERRUPTED) so
# that only ``Verdict.status`` carries the past tense. A caller that hands us
# a past-tense outcome like ``"FAILED"`` in ``gate_outcomes`` is not silently
# treated as PASS; ``Verdict.strictest`` raises ``ValueError`` instead.
_VALID_GATE_OUTCOMES: frozenset[str] = frozenset({"PASS", "FAIL", "REFUSED", "INTERRUPTED"})


@dataclass(frozen=True)
class Verdict:
    """A structured verdict for a Lakebench run.

    ``status`` is the strictest outcome across every input: FAILED beats
    INTERRUPTED beats REFUSED beats PASSED. ``reasons`` is a short human list
    (one per failing gate). ``gates`` maps a gate name to its outcome literal.
    ``qualifiers`` carries adjacent context (n_runs, capped_by, support_state,
    etc.) that a report can render without re-deriving.
    """

    status: VerdictStatus
    reasons: list[str] = field(default_factory=list)
    gates: dict[str, str] = field(default_factory=dict)
    qualifiers: dict[str, Any] = field(default_factory=dict)

    # Priority order: FAILED > INTERRUPTED > REFUSED > PASSED.
    # Any gate that reports one of these dominates a stricter one below it.
    PRIORITY: ClassVar[tuple[str, ...]] = ("FAILED", "INTERRUPTED", "REFUSED", "PASSED")

    @classmethod
    def strictest(
        cls,
        exit_ok: bool,
        badge_ok: bool,
        success_flag: bool,
        gate_outcomes: dict[str, str],
        reasons: list[str],
        qualifiers: dict[str, Any],
    ) -> Verdict:
        """Compute the strictest ``Verdict`` from a run's inputs.

        A FAIL wins over any true flag: if any of ``exit_ok`` / ``badge_ok`` /
        ``success_flag`` is false, or if any ``gate_outcomes`` value is
        ``"FAIL"``, the verdict is FAILED. Otherwise INTERRUPTED wins over
        REFUSED wins over PASSED. Order: FAILED > INTERRUPTED > REFUSED >
        PASSED.
        """
        # Validate gate vocab strictly: an unknown value (for example the
        # past-tense ``"FAILED"``) must raise, not silently be treated as
        # PASS. Callers today emit PASS or FAIL; new callers see the failure
        # immediately.
        for name, value in gate_outcomes.items():
            if value not in _VALID_GATE_OUTCOMES:
                raise ValueError(
                    f"Unknown gate outcome for {name!r}: {value!r}. "
                    f"Expected one of {sorted(_VALID_GATE_OUTCOMES)}."
                )
        gate_values = list(gate_outcomes.values())

        failed = (
            (not exit_ok)
            or (not badge_ok)
            or (not success_flag)
            or any(v == "FAIL" for v in gate_values)
        )
        interrupted = any(v == "INTERRUPTED" for v in gate_values)
        refused = any(v == "REFUSED" for v in gate_values)

        if failed:
            status: VerdictStatus = "FAILED"
        elif interrupted:
            status = "INTERRUPTED"
        elif refused:
            status = "REFUSED"
        else:
            status = "PASSED"

        # A PASSED verdict carries no reasons: the caller may pass an
        # explanatory list, but reasons only make sense when something failed.
        kept_reasons: list[str] = list(reasons) if status != "PASSED" else []

        # Deep copy on ingress so a caller cannot mutate a nested list inside
        # ``qualifiers`` after this Verdict is constructed (the dataclass is
        # frozen, but ``dict(other)`` would only shallow-copy). Callers today
        # pass scalars, so no behaviour change; the invariant holds for
        # future callers.
        return cls(
            status=status,
            reasons=kept_reasons,
            gates=copy.deepcopy(gate_outcomes),
            qualifiers=copy.deepcopy(qualifiers),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict representation.

        The returned dict is deep-copied so a caller mutating it does not
        change the underlying Verdict (relevant for nested values).
        """
        return {
            "status": self.status,
            "reasons": list(self.reasons),
            "gates": copy.deepcopy(self.gates),
            "qualifiers": copy.deepcopy(self.qualifiers),
        }


# ---------------------------------------------------------------------------
# Legacy-fallback readers. A v1.6 record carries a ``verdict`` block written
# by the collector; a v1.5 record does not. Every raw-``success`` consumer
# now goes through ``passed`` (or ``verdict_status`` + a comparison) so v1.6
# records use the verdict and v1.5 records fall back to the legacy flag.
# ---------------------------------------------------------------------------


def has_verdict(record: Mapping[str, Any] | None) -> bool:
    """True when *record* carries a ``verdict`` block with a string status."""
    if record is None:
        return False
    v = record.get("verdict")
    return isinstance(v, Mapping) and isinstance(v.get("status"), str)


def verdict_status(record: Mapping[str, Any] | None) -> str | None:
    """The persisted ``verdict.status`` for *record*, or ``None`` when the
    record has no verdict block."""
    if record is None:
        return None
    v = record.get("verdict")
    if isinstance(v, Mapping):
        status = v.get("status")
        if isinstance(status, str):
            return status
    return None


def passed(record: Mapping[str, Any] | None) -> bool:
    """Whether *record* is a PASSED run.

    Prefers ``verdict.status == "PASSED"`` when the v1.6 verdict block is
    present. Falls back to ``record.get("success")`` for a legacy v1.5
    record. Records loaded via ``storage._dict_to_metrics`` (PipelineMetrics
    objects) go through the ``compute_verdict`` path in the caller, not
    this helper; this reader is for the raw dict shape.
    """
    status = verdict_status(record)
    if status is not None:
        return status == "PASSED"
    if record is None:
        return False
    return bool(record.get("success", False))


# ---------------------------------------------------------------------------
# Shared badge helper. Both ``compute_verdict`` (below) and the HTML report
# generator (``reports.generator``) compute the same badge over a
# ``PipelineMetrics``: pass/fail bit, fail reasons, and (report only)
# warnings. The generator returns all three; the verdict discards warnings
# because a warning is by construction not a fail.
# ---------------------------------------------------------------------------


def _is_sustained(metrics: PipelineMetrics) -> bool:
    """True when the run used the sustained/continuous pipeline."""
    pb = metrics.pipeline_benchmark
    if pb is not None:
        return pb.pipeline_mode in ("sustained", "continuous")
    return bool(metrics.streaming)


def compute_badge_status(
    metrics: PipelineMetrics,
) -> tuple[bool, list[str], list[str]]:
    """Return ``(passed, fail_reasons, warnings)`` for the HTML badge.

    Warnings are not failures; they surface as an amber badge when
    ``passed`` is True. A run is failed when any reason is recorded.
    """
    reasons: list[str] = []
    warnings: list[str] = []

    if metrics.benchmark_error:
        reasons.append(f"Benchmark did not complete ({metrics.benchmark_error}); no QpH")
    elif not metrics.success:
        reasons.append("Pipeline crashed or was interrupted")

    pb = metrics.pipeline_benchmark
    is_sustained = _is_sustained(metrics)

    # Data completeness
    if pb is not None:
        if is_sustained and pb.ingest_ratio is None:
            warnings.append("Ingest ratio unmeasurable (datagen row count unknown)")
        elif (
            is_sustained
            and pb.ingest_ratio is not None
            and pb.ingest_ratio < 0.95
            and pb.intake_limit == "trickle_rate"
            and pb.pipeline_saturated is False
        ):
            # The configured trickle bounded intake and the pipeline kept
            # pace with it (LB-156): a caveat on the ratio, not a failure.
            warnings.append(pb.trickle_note() or "Intake held to the trickle rate")
        elif (
            is_sustained
            and pb.ingest_ratio is not None
            and pb.ingest_ratio < 0.95
            and pb.intake_limit == "trickle_rate"
            and pb.pipeline_saturated is None
        ):
            warnings.append(
                f"Ingest ratio {pb.ingest_ratio:.2f}: intake held to the trickle rate, "
                "but silver's pace was not measured, so saturation is unknown"
            )
        elif is_sustained and pb.ingest_ratio is not None and pb.ingest_ratio < 0.95:
            cause = (
                "intake held to the trickle rate, silver did not keep pace"
                if pb.intake_limit == "trickle_rate"
                else "pipeline saturated"
            )
            reasons.append(f"Ingest ratio {pb.ingest_ratio:.2f} < 0.95 ({cause})")
        elif is_sustained and pb.ingest_ratio is not None and pb.ingest_ratio > 1.05:
            warnings.append(
                f"Ingest ratio {pb.ingest_ratio:.2f} > 1.05 (gold re-reads exceed input)"
            )
        if not is_sustained and 0 < pb.scale_ratio < 0.95:
            reasons.append(f"Scale ratio {pb.scale_ratio:.1%} < 95% (incomplete data)")

    # Freshness -- sustained mode only
    if is_sustained and pb is not None and pb.data_freshness_seconds is not None:
        run_dur = metrics.total_elapsed_seconds or 1.0
        freshness_pct = pb.data_freshness_seconds / run_dur
        if freshness_pct > 0.5:
            reasons.append(
                f"Gold freshness {pb.data_freshness_seconds:,.0f}s "
                f"({freshness_pct:.0%} of run duration -- gold was stale for most of the run)"
            )

    if is_sustained and pb is not None and pb.corpus_drained:
        warnings.append(
            "Corpus fully ingested before the window ended: freshness covers only gold "
            "cycles that saw new data, and rows/s is a lower bound set by corpus size (LB-145)"
        )

    # Job / streaming success
    if is_sustained and metrics.streaming:
        failed_streams = [s.job_name for s in metrics.streaming if not s.success]
        if failed_streams:
            reasons.append(f"Streaming jobs failed: {', '.join(failed_streams)}")
    elif metrics.jobs:
        failed_jobs = [j.job_name for j in metrics.jobs if not j.success]
        if failed_jobs:
            reasons.append(f"Batch jobs failed: {', '.join(failed_jobs)}")

    # Benchmark query failures
    if metrics.benchmark is not None and metrics.benchmark.queries:
        n_failed = sum(
            1
            for q in metrics.benchmark.queries
            if isinstance(q, dict) and not q.get("success", True)
        )
        if n_failed:
            reasons.append(f"{n_failed} benchmark queries failed")

    return (len(reasons) == 0, reasons, warnings)


# ---------------------------------------------------------------------------
# compute_verdict
# ---------------------------------------------------------------------------


def _pipeline_gate_outcome(metrics: PipelineMetrics) -> str | None:
    """Outcome for the aggregate ``pipeline`` gate, or ``None`` when the run
    recorded no pipeline stages at all (nothing to gate on)."""
    if _is_sustained(metrics) and metrics.streaming:
        if any(not s.success for s in metrics.streaming):
            return "FAIL"
        return "PASS"
    if metrics.jobs:
        if any(not j.success for j in metrics.jobs):
            return "FAIL"
        return "PASS"
    return None


def _benchmark_gate_outcome(metrics: PipelineMetrics) -> str | None:
    """Outcome for the aggregate ``benchmark`` gate.

    "FAIL" when the benchmark raised or any query failed. "PASS" when a
    benchmark ran with no failures. ``None`` when no benchmark was attempted
    (a shape that scopes the benchmark out; not a gate failure).
    """
    if metrics.benchmark_error:
        return "FAIL"
    if metrics.benchmark is None:
        return None
    for q in metrics.benchmark.queries or []:
        if isinstance(q, dict) and not q.get("success", True):
            return "FAIL"
    return "PASS"


def _c360_gate(metrics: PipelineMetrics) -> tuple[str | None, str | None]:
    """Outcome and human reason for the ``c360`` gate.

    A run without a ``c360_correctness`` record (batch or otherwise) has
    nothing to gate on, so this returns ``(None, None)``. A record whose
    verdict is "fail" surfaces here as FAIL with the underlying reason. Any
    other status (pass, unknown, unchecked) is not a fail: c360 gating for
    the run is scoped by the c360_correctness verdict itself (D6 governs
    what "unknown" means and whether it should fail; that decision lives
    with the c360 module, not here).
    """
    rec = metrics.c360_correctness
    if not isinstance(rec, Mapping):
        return None, None
    status = rec.get("status")
    if status == "fail":
        failed = rec.get("failed") or []
        if failed:
            detail = ", ".join(str(f) for f in failed)
            return "FAIL", f"Customer 360 correctness gate failed: {detail}"
        reason = rec.get("reason") or "one or more expected-result checks failed"
        return "FAIL", f"Customer 360 correctness gate failed: {reason}"
    return None, None


def compute_verdict(metrics: PipelineMetrics) -> Verdict:
    """Compute a ``Verdict`` for a completed ``PipelineMetrics`` run.

    ``exit_ok`` is inferred from ``success`` (today the CLI writes
    ``success = True`` when it intended to exit 0). ``badge_ok`` is the HTML
    badge's pass/fail decision, computed via the shared
    ``compute_badge_status`` helper. ``success_flag`` is ``metrics.success``.
    ``gate_outcomes`` covers, at minimum, ``pipeline`` and ``benchmark``,
    and adds ``c360`` when the run recorded a c360 correctness verdict of
    "fail".
    """
    exit_ok = bool(metrics.success)
    badge_ok, badge_reasons, _warnings = compute_badge_status(metrics)
    success_flag = bool(metrics.success)

    gate_outcomes: dict[str, str] = {}
    pipeline_outcome = _pipeline_gate_outcome(metrics)
    if pipeline_outcome is not None:
        gate_outcomes["pipeline"] = pipeline_outcome
    benchmark_outcome = _benchmark_gate_outcome(metrics)
    if benchmark_outcome is not None:
        gate_outcomes["benchmark"] = benchmark_outcome
    c360_outcome, c360_reason = _c360_gate(metrics)
    if c360_outcome is not None:
        gate_outcomes["c360"] = c360_outcome

    reasons: list[str] = []
    if not exit_ok:
        reasons.append("Process exit intent was not OK")
    for r in badge_reasons:
        if r not in reasons:
            reasons.append(r)
    if c360_reason and c360_reason not in reasons:
        reasons.append(c360_reason)
    for name, outcome in gate_outcomes.items():
        if outcome == "FAIL":
            marker = f"Gate '{name}' FAILED"
            if marker not in reasons:
                reasons.append(marker)

    qualifiers: dict[str, Any] = {}
    if metrics.pipeline_benchmark is not None:
        qualifiers["pipeline_mode"] = metrics.pipeline_benchmark.pipeline_mode
    n_streams = len(metrics.streaming or [])
    n_jobs = len(metrics.jobs or [])
    if n_streams:
        qualifiers["n_streaming_stages"] = n_streams
    if n_jobs:
        qualifiers["n_batch_jobs"] = n_jobs
    if metrics.benchmark is not None:
        qualifiers["n_benchmark_queries"] = len(metrics.benchmark.queries or [])

    return Verdict.strictest(
        exit_ok=exit_ok,
        badge_ok=badge_ok,
        success_flag=success_flag,
        gate_outcomes=gate_outcomes,
        reasons=reasons,
        qualifiers=qualifiers,
    )
