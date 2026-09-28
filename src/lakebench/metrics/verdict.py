"""Verdict value object for a Lakebench run (A2a skeleton).

Historically a run reported a single ``success`` boolean. That was too coarse:
the process exit code, the HTML badge, and the underlying pipeline / benchmark
gate outcomes can disagree. A run whose CLI exited 0 with all jobs succeeded
can still be FAILED by the badge (e.g. LB-044, sustained runs where bronze
never kept pace with the trickle, or gold was stale for most of the window).

This module introduces the ``Verdict`` value object. A2a wires it into the
metrics dict alongside ``success`` without changing any consumer of
``success``. A2b (a separate lane) will migrate the readers and unify the
badge rules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    from lakebench.metrics.collector import PipelineMetrics


VerdictStatus = Literal["PASSED", "FAILED", "REFUSED", "INTERRUPTED"]


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
        gate_values = [v.upper() for v in gate_outcomes.values()]

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

        return cls(
            status=status,
            reasons=kept_reasons,
            gates=dict(gate_outcomes),
            qualifiers=dict(qualifiers),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict representation."""
        return {
            "status": self.status,
            "reasons": list(self.reasons),
            "gates": dict(self.gates),
            "qualifiers": dict(self.qualifiers),
        }


# ---------------------------------------------------------------------------
# TODO(A2b: unify with reports.generator._compute_overall_status).
#
# The block below duplicates the badge rules that ``reports.generator`` uses
# so this A2a skeleton can score a run without a circular import (the reports
# module imports from ``lakebench.metrics`` at module top). A2b will lift the
# badge rules into a shared helper and delete this duplication.
# ---------------------------------------------------------------------------


def _is_sustained(metrics: PipelineMetrics) -> bool:
    """True when the run used the sustained/continuous pipeline."""
    pb = metrics.pipeline_benchmark
    if pb is not None:
        return pb.pipeline_mode in ("sustained", "continuous")
    return bool(metrics.streaming)


def _badge_ok(metrics: PipelineMetrics) -> tuple[bool, list[str]]:
    """Recompute the HTML badge pass/fail decision and its reasons.

    Mirrors ``ReportGenerator._compute_overall_status`` (badge_ok is the
    ``passed`` half). Warnings are not treated as failures.

    TODO(A2b: unify): replace with a call to the shared badge helper.
    """
    reasons: list[str] = []

    if metrics.benchmark_error:
        reasons.append(f"Benchmark did not complete ({metrics.benchmark_error}); no QpH")
    elif not metrics.success:
        reasons.append("Pipeline crashed or was interrupted")

    pb = metrics.pipeline_benchmark
    is_sustained = _is_sustained(metrics)

    if pb is not None:
        if (
            is_sustained
            and pb.ingest_ratio is not None
            and pb.ingest_ratio < 0.95
            and not (pb.intake_limit == "trickle_rate" and pb.pipeline_saturated is False)
            and not (pb.intake_limit == "trickle_rate" and pb.pipeline_saturated is None)
        ):
            cause = (
                "intake held to the trickle rate, silver did not keep pace"
                if pb.intake_limit == "trickle_rate"
                else "pipeline saturated"
            )
            reasons.append(f"Ingest ratio {pb.ingest_ratio:.2f} < 0.95 ({cause})")
        if not is_sustained and 0 < pb.scale_ratio < 0.95:
            reasons.append(f"Scale ratio {pb.scale_ratio:.1%} < 95% (incomplete data)")

    if is_sustained and pb is not None and pb.data_freshness_seconds is not None:
        run_dur = metrics.total_elapsed_seconds or 1.0
        freshness_pct = pb.data_freshness_seconds / run_dur
        if freshness_pct > 0.5:
            reasons.append(
                f"Gold freshness {pb.data_freshness_seconds:,.0f}s "
                f"({freshness_pct:.0%} of run duration -- gold was stale for most of the run)"
            )

    # Job / streaming success
    if is_sustained and metrics.streaming:
        failed = [s.job_name for s in metrics.streaming if not s.success]
        if failed:
            reasons.append(f"Streaming jobs failed: {', '.join(failed)}")
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

    return (len(reasons) == 0, reasons)


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


def compute_verdict(metrics: PipelineMetrics) -> Verdict:
    """Compute a ``Verdict`` for a completed ``PipelineMetrics`` run.

    ``exit_ok`` is inferred from ``success`` (today the CLI writes
    ``success = True`` when it intended to exit 0). ``badge_ok`` is the HTML
    badge's pass/fail decision, duplicated here with a TODO(A2b: unify).
    ``success_flag`` is ``metrics.success``. ``gate_outcomes`` covers, at
    minimum, ``pipeline`` and ``benchmark``.
    """
    exit_ok = bool(metrics.success)
    badge_ok, badge_reasons = _badge_ok(metrics)
    success_flag = bool(metrics.success)

    gate_outcomes: dict[str, str] = {}
    pipeline_outcome = _pipeline_gate_outcome(metrics)
    if pipeline_outcome is not None:
        gate_outcomes["pipeline"] = pipeline_outcome
    benchmark_outcome = _benchmark_gate_outcome(metrics)
    if benchmark_outcome is not None:
        gate_outcomes["benchmark"] = benchmark_outcome

    reasons: list[str] = []
    if not exit_ok:
        reasons.append("Process exit intent was not OK")
    for r in badge_reasons:
        if r not in reasons:
            reasons.append(r)
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
