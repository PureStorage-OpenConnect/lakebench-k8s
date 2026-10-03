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
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from lakebench.metrics import c360_correctness

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


def _is_run_record(record: Mapping[str, Any]) -> bool:
    """A whole metrics.json run record (``PipelineMetrics.to_dict`` always
    writes ``run_id``, ``start_time`` and the ``jobs`` list), as opposed to a
    summary row such as ``MetricsStorage.list_runs`` returns."""
    return (
        isinstance(record.get("run_id"), str)
        and isinstance(record.get("start_time"), str)
        and isinstance(record.get("jobs"), list)
    )


def stored_passed(record: Mapping[str, Any] | None) -> bool:
    """Whether *record* stored a pass: ``verdict.status == "PASSED"`` when it
    has a verdict block, else its ``success`` flag (a v1.5 record). Only the
    stored half of ``passed``; a reader that recomputes the verdict itself
    starts here."""
    if record is None:
        return False
    status = verdict_status(record)
    return status == "PASSED" if status is not None else bool(record.get("success", False))


#: Verdict statuses from strictest to least strict (``Verdict.PRIORITY``).
_STRICTNESS = {"FAILED": 0, "INTERRUPTED": 1, "REFUSED": 2, "PASSED": 3}


def verdict_of(record: Mapping[str, Any] | None) -> dict[str, str | None]:
    """The verdict a reader reports for *record*: ``{"stored", "recomputed",
    "status"}``.

    ``stored`` is ``verdict.status`` as the record holds it (None when it
    has no verdict block). ``recomputed`` is ``verdict_from_record``'s
    status for a whole run record (None for a summary row, which cannot be
    recomputed, or a record the loader cannot read). ``status``, the
    headline, is the strictest of the two, where a record with no verdict
    block stands for PASSED or FAILED by its ``success`` flag (a v1.5
    record) and a whole record that cannot be recomputed reads FAILED: a
    reader never promotes. None only for no record at all."""
    if record is None:
        return {"stored": None, "recomputed": None, "status": None}
    return {k: v for k, v in judge(record).items() if k in ("stored", "recomputed", "status")}


def judge(record: Mapping[str, Any]) -> dict[str, Any]:
    """``verdict_of`` plus why: ``reasons`` (the recomputed verdict's
    reasons, without the gate markers) and ``error`` (why the record could
    not be recomputed, else None). Readers that explain a refusal (compare)
    use this; everything else uses ``verdict_of`` or ``passed``."""
    stored = verdict_status(record)
    base = stored if stored is not None else ("PASSED" if record.get("success") else "FAILED")
    if not base:
        base = "FAILED"  # an empty stored status is no pass
    recomputed: str | None = None
    reasons: list[str] = []
    error: str | None = None
    if _is_run_record(record):
        try:
            again = _recompute(record)
            recomputed = again.status
            reasons = [str(r) for r in again.reasons if not str(r).startswith("Gate '")]
        except Exception as e:  # noqa: BLE001 -- an unreadable record never reads passed
            import logging

            error = f"{type(e).__name__}: {e}"
            logging.getLogger(__name__).warning(
                "run %s: verdict not recomputable (%s); read as not passed",
                record.get("run_id"),
                e,
            )
    candidates = [base] + ([recomputed] if recomputed else []) + (["FAILED"] if error else [])
    # An unknown status ranks with FAILED: never read as a pass.
    status = min(candidates, key=lambda x: _STRICTNESS.get(x, 0))
    return {
        "stored": stored,
        "recomputed": recomputed,
        "status": status,
        "reasons": reasons,
        "error": error,
    }


def passed(record: Mapping[str, Any] | None) -> bool:
    """Whether *record* is a PASSED run: ``verdict_of(record)["status"]``,
    the strictest of what it stored and what the record shows today. A
    reader never promotes.

    The stored half prefers ``verdict.status == "PASSED"`` when the v1.6
    verdict block is present and falls back to ``record.get("success")`` for
    a legacy v1.5 record. A whole run record is recomputed with
    ``verdict_from_record``, so a v1.6 record that the record gates fail
    (rows per layer, rules, scale ratio, query answers) reads failed. A
    record the loader cannot read reads failed. A summary row (no ``jobs``
    list) is judged on what it stored.
    """
    return verdict_of(record)["status"] == "PASSED"


# ---------------------------------------------------------------------------
# Shared badge helper. Both ``compute_verdict`` (below) and the HTML report
# generator (``reports.generator``) compute the same badge over a
# ``PipelineMetrics``: pass/fail bit, fail reasons, and (report only)
# warnings. The generator returns all three; the verdict discards warnings
# because a warning is by construction not a fail.
# ---------------------------------------------------------------------------


#: ``JobMetrics.error_message`` of the stage a SIGINT or SIGTERM stopped
#: (cli/_interrupt.py). That job did not fail; it is left out of the
#: ``pipeline`` gate and the badge's failed jobs.
INTERRUPTED_JOB_MESSAGE = "interrupted"


def _interrupt_record(metrics: PipelineMetrics) -> Mapping[str, Any] | None:
    """The run's ``interrupted`` block, or None when it was not interrupted."""
    rec = getattr(metrics, "interrupted", None)
    return rec if isinstance(rec, Mapping) else None


def _stopped_by_interrupt(job: Any, interrupted: Mapping[str, Any] | None) -> bool:
    """The job of the stage the interrupt landed in (not a failed job)."""
    return (
        interrupted is not None
        and getattr(job, "error_message", None) == INTERRUPTED_JOB_MESSAGE
        and getattr(job, "job_type", None) == interrupted.get("at_stage")
    )


def interrupt_reason(interrupted: Mapping[str, Any]) -> str:
    """One line for a verdict or badge: which signal, during which stage."""
    return (
        f"Run interrupted ({interrupted.get('signal') or 'signal'} during "
        f"{interrupted.get('at_stage') or 'an unknown stage'})"
    )


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

    interrupted = _interrupt_record(metrics)
    if metrics.benchmark_error:
        reasons.append(f"Benchmark did not complete ({metrics.benchmark_error}); no QpH")
    elif not metrics.success:
        abort = getattr(metrics, "abort_reason", None)
        if interrupted is not None:
            reasons.append(interrupt_reason(interrupted))
        if isinstance(abort, Mapping) and abort.get("reason"):
            reasons.append(f"Run stopped: {abort['reason']}")
        if (
            interrupted is None
            and not (isinstance(abort, Mapping) and abort.get("reason"))
            and not (getattr(metrics, "failure_reasons", None) or [])
        ):
            # Only when the run recorded no reason of its own (a gate the
            # CLI or the save gate failed names itself in failure_reasons).
            reasons.append("Pipeline crashed or was interrupted")
    # Reasons the run recorded itself (e.g. "datagen timed out"), after the
    # generic one so existing reasons keep their place.
    for recorded in getattr(metrics, "failure_reasons", None) or []:
        if recorded not in reasons:
            reasons.append(recorded)

    pb = metrics.pipeline_benchmark
    is_sustained = _is_sustained(metrics)

    # Datagen wrote over objects it did not clear (--allow-stale-bronze).
    stale = getattr(metrics, "datagen_stale_bronze", None)
    if stale:
        warnings.append(
            f"bronze held {stale.get('objects_before', 0)} objects before generate; "
            "rows may be over-counted"
        )

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
        failed_jobs = [
            j.job_name
            for j in metrics.jobs
            if not j.success and not _stopped_by_interrupt(j, interrupted)
        ]
        if failed_jobs:
            reasons.append(f"Batch jobs failed: {', '.join(failed_jobs)}")

    # Benchmark query failures
    n_failed = len(_failed_queries(metrics))
    if n_failed:
        reasons.append(f"{n_failed} benchmark queries failed")

    # A request the run did not meet (requested gold strategy, executors,
    # mode or trickle): a warning, never a failure.
    from lakebench.metrics.requested_effective import warning_line

    for key, entry in _requested_effective_mismatches(metrics).items():
        line = warning_line(key, entry)
        if line not in warnings:
            warnings.append(line)

    # The record gates (rows per layer, AML rules, scale ratio, query
    # answers): their reasons fail the badge as they fail the verdict.
    for gate in record_gates(metrics).values():
        for r in gate.reasons:
            if r not in reasons:
                reasons.append(r)
        for w in gate.warnings:
            if w not in warnings:
                warnings.append(w)

    return (len(reasons) == 0, reasons, warnings)


# ---------------------------------------------------------------------------
# compute_verdict
# ---------------------------------------------------------------------------


def _requested_effective_mismatches(metrics: PipelineMetrics) -> dict[str, dict[str, Any]]:
    """The requested and effective entries of the record that are
    mismatches, by key (metrics/requested_effective.py)."""
    from lakebench.metrics.requested_effective import stored_or_derived

    entries, keys = stored_or_derived(metrics)
    return {k: entries[k] for k in keys if k in entries}


def _pipeline_gate_outcome(metrics: PipelineMetrics) -> str | None:
    """Outcome for the aggregate ``pipeline`` gate, or ``None`` when the run
    recorded no pipeline stages at all (nothing to gate on)."""
    if _is_sustained(metrics) and metrics.streaming:
        if any(not s.success for s in metrics.streaming):
            return "FAIL"
        return "PASS"
    interrupted = _interrupt_record(metrics)
    jobs = [j for j in metrics.jobs if not _stopped_by_interrupt(j, interrupted)]
    if jobs:
        if any(not j.success for j in jobs):
            return "FAIL"
        return "PASS"
    return None


def _rounds(metrics: PipelineMetrics) -> list[Any]:
    """A continuous run's in-stream benchmark rounds, in order ([] otherwise)."""
    return list(metrics.benchmark_rounds or []) if _is_sustained(metrics) else []


def _failed_queries(metrics: PipelineMetrics) -> list[str]:
    """Names of the benchmark queries that failed. A continuous run with
    in-stream rounds is judged round by round, as the CLI judges it: a Q9
    that failed is left out (gold refresh replaces the table Q9 reads, so a
    failure after its contention retries is expected in any round,
    ``cli._sustained.tolerated_q9_results``). Otherwise the benchmark's own
    queries; the aggregate of continuous rounds keeps only round 1's
    outcome per query."""
    rounds = _rounds(metrics)
    if rounds:
        out: list[str] = []
        for rnd in rounds:
            for q in rnd.queries or []:
                if not isinstance(q, Mapping) or q.get("success", True):
                    continue
                name = str(q.get("name") or q.get("query_name") or "")
                if not name.startswith("Q9") and name not in out:
                    out.append(name)
        return out
    if metrics.benchmark is None:
        return []
    return [
        str(q.get("name") or q.get("query_name") or "")
        for q in metrics.benchmark.queries or []
        if isinstance(q, Mapping) and not q.get("success", True)
    ]


def _benchmark_gate_outcome(metrics: PipelineMetrics) -> str | None:
    """Outcome for the aggregate ``benchmark`` gate.

    "FAIL" when the benchmark raised or any query failed
    (``_failed_queries``). "PASS" when a benchmark ran with no failures.
    ``None`` when no benchmark was attempted (a shape that scopes the
    benchmark out; not a gate failure).
    """
    if metrics.benchmark_error:
        return "FAIL"
    if metrics.benchmark is None and not _rounds(metrics):
        return None
    return "FAIL" if _failed_queries(metrics) else "PASS"


def _c360_gate(metrics: PipelineMetrics) -> tuple[str | None, str | None]:
    """Outcome and human reason for the ``c360`` gate.

    ``c360_correctness.gating_outcome`` decides: FAIL when a check in its
    ``GATING_CHECKS`` failed, did not run or is absent, or when the record
    has no facts. A run without a ``c360_correctness`` record, a record
    whose only failures are checks outside that list, and a continuous
    record marked ``reporting_only`` return ``(None, None)``.
    """
    rec = metrics.c360_correctness
    if not isinstance(rec, Mapping):
        return None, None
    return c360_correctness.gating_outcome(rec)


def _deps_pods_reason(metrics: PipelineMetrics) -> str | None:
    """A FAIL reason when a pod of the run ran another dependency set than
    the run recorded (``provenance.deps.pod_mismatches``)."""
    deps = (getattr(metrics, "provenance", None) or {}).get("deps")
    if not isinstance(deps, dict):
        return None
    if deps.get("job_manager_pinset"):
        return (
            f"the jobs were built on dependency set {str(deps['job_manager_pinset'])[:12]}, "
            f"not the recorded {str(deps.get('pinset_sha256'))[:12]}"
        )
    if "pods_checked" in deps and deps["pods_checked"] is None and deps.get("pods_check_error"):
        return (
            f"query engine pods not checked for their dependency set ({deps['pods_check_error']})"
        )
    mismatches = deps.get("pod_mismatches")
    if not mismatches:
        return None
    pods = ", ".join(f"{p.get('pod')} on {str(p.get('pinset'))[:12]}" for p in mismatches[:5])
    return f"pods ran different dependency sets ({pods})"


# ---------------------------------------------------------------------------
# Record gates: decided from the record alone.
# Each reads only fields a stored metrics.json carries, so a report, compare,
# the perf gate and the release gate recompute the outcome the run saved.
# They apply to a run that was not interrupted: an interrupted run is never
# PASSED, and its partial layers must not turn INTERRUPTED into FAILED.
# ---------------------------------------------------------------------------

#: The rule skips a PASSED run may carry, keyed by (workload, mode): rule ->
#: the skip reasons allowed. Any other skip, or another reason, fails the
#: ``aml_rules`` gate. W1's giant-component is on the stored PASSED records
#: at scale 1 and 10; vertex-cap is W1's scale-100 skip, and path-cap is
#: W3's and W17's (owner, 10-03). A skip whose reason is a Lakebench cap is
#: labelled: ``limits.bound`` and ``limits.bound_kinds`` carry it (``rule <id>
#: cap``), and the verdict lists it in the ``rule_caps`` qualifier. Continuous
#: allows none: its mode-excluded rules
#: (``config.support.AML_CONTINUOUS_SKIPPED_RULES``) are never in the skip
#: list; they are left out of the expected set instead.
EXPECTED_SKIPS: dict[tuple[str, str], dict[str, frozenset[str]]] = {
    ("financial", "batch"): {
        "W1_connected_components": frozenset({"giant-component", "vertex-cap"}),
        "W3_round_tripping": frozenset({"path-cap"}),
        "W17_layering_chain": frozenset({"path-cap"}),
    },
    ("financial", "continuous"): {},
}

#: Verdict qualifier listing the layers whose rows were not measured, where
#: the ``layer_rows`` gate passed on bytes alone. Always set (possibly
#: empty) when the gate is computed; the release gate reads it.
LAYER_ROWS_UNMEASURED = "layer_rows_unmeasured"

#: Verdict qualifier naming each AML rule a PASSED run skipped on a
#: Lakebench cap (rule -> skip reason): the rule set is bounded by Lakebench,
#: not by the system (invariant 6; ``limits.bound`` carries the same).
RULE_CAPS = "rule_caps"

#: The batch stage job that measures each layer.
_BATCH_LAYER_JOBS: tuple[tuple[str, str], ...] = (
    ("bronze", "bronze-verify"),
    ("silver", "silver-build"),
    ("gold", "gold-finalize"),
)


@dataclass
class _GateResult:
    """One record gate: its outcome (PASS, FAIL, or None when it does not
    apply), FAIL reasons, badge warnings and verdict qualifiers."""

    outcome: str | None = None
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    qualifiers: dict[str, Any] = field(default_factory=dict)


def _workload(metrics: PipelineMetrics) -> str | None:
    """The record's workload name: the config snapshot's schema, else the
    experiment inputs or the stored experiment block."""
    snap = getattr(metrics, "config_snapshot", None) or {}
    name = snap.get("workload_schema")
    if not name:
        inputs = snap.get("experiment_inputs") or {}
        name = (inputs.get("workload") or {}).get("name")
    if not name:
        exp = getattr(metrics, "experiment", None) or {}
        name = (exp.get("workload") or {}).get("name")
    return str(name) if name else None


def _last_by_type(items: list[Any]) -> dict[str, Any]:
    """The last job (or stream) of each job type, in record order."""
    out: dict[str, Any] = {}
    for item in items or []:
        out[str(getattr(item, "job_type", ""))] = item
    return out


def _aml_continuous_alerts(gold: Any) -> int | None:
    """The alerts the AML gold refresh's time-to-detect lines counted over
    the run's ticks (matched to an arrival or not). None when not logged, or
    when none were counted while a cycle could not measure its alerts (0
    would then prove nothing)."""
    if gold is None or gold.ttd_alerts is None:
        return None
    total = int(gold.ttd_alerts) + int(gold.ttd_unmatched or 0)
    if total == 0 and (gold.ttd_unmeasured_cycles or 0) > 0:
        return None
    return total


def _layer_rows_gate(metrics: PipelineMetrics) -> _GateResult:
    """Rows per layer > 0.

    Batch reads each layer's last stage job's ``output_rows``; a missing
    stage job fails (the stage did not run). ``JobMetrics.output_rows``
    defaults to 0, so a driver log that was never parsed reads as 0 rows and
    fails: the run cannot show the layer is not empty. Continuous reads
    ``bronze-ingest.output_rows``; ``silver-stream.output_rows`` (rows after
    the transforms), else its ``committed_rows`` (the rows of the batches
    that committed, which is all the AML silver stream logs); and
    ``gold-refresh.output_rows``. AML gold refresh logs no row count: the
    alerts it counted (``ttd_alerts`` plus ``ttd_unmatched``) stand for
    gold's rows, so 0 alerts fails unless a cycle could not count them. A
    continuous layer with no row figure falls back to its bytes: > 0 passes
    with the layer listed in ``LAYER_ROWS_UNMEASURED`` and a warning, 0
    fails. A ``run --stage`` record (``stage_only``) is checked for that
    stage's layer only.
    """
    res = _GateResult()
    unmeasured: list[str] = []
    stage_only = getattr(metrics, "stage_only", None)
    if _is_sustained(metrics):
        streams = _last_by_type(metrics.streaming)
        bronze = streams.get("bronze-ingest")
        silver = streams.get("silver-stream")
        gold = streams.get("gold-refresh")
        rows: dict[str, int | None] = {
            "bronze": getattr(bronze, "output_rows", None),
            "silver": (
                silver.output_rows
                if silver is not None and silver.output_rows is not None
                else getattr(silver, "committed_rows", None)
            ),
            "gold": getattr(gold, "output_rows", None),
        }
        if rows["gold"] is None and _workload(metrics) == "financial":
            rows["gold"] = _aml_continuous_alerts(gold)
        for layer, value in rows.items():
            if value is not None:
                if value <= 0:
                    res.reasons.append(f"{layer} has 0 rows: the layer is empty")
                continue
            size = float(getattr(metrics, f"{layer}_size_gb", 0) or 0)
            if size > 0:
                unmeasured.append(layer)
                res.warnings.append(f"rows not measured for {layer}; bytes > 0")
            else:
                res.reasons.append(f"{layer}: rows not measured and the layer holds 0 bytes")
    else:
        jobs = _last_by_type(metrics.jobs)
        for layer, job_type in _BATCH_LAYER_JOBS:
            if stage_only and stage_only != job_type:
                continue
            job = jobs.get(job_type)
            if job is None:
                res.reasons.append(f"{layer}: no {job_type} job recorded")
            elif int(job.output_rows or 0) <= 0:
                res.reasons.append(
                    f"{layer} has 0 rows ({job_type} recorded 0 output rows, "
                    "or its driver log was not read)"
                )
    res.outcome = "FAIL" if res.reasons else "PASS"
    res.qualifiers[LAYER_ROWS_UNMEASURED] = unmeasured
    return res


def _aml_rules_gate(metrics: PipelineMetrics) -> _GateResult:
    """The expected AML rules ran, none errored, and detection alerted.

    Batch reads the last gold-finalize job (it re-detects over the whole
    corpus, as the CLI's gate does): any ``rule_errors`` entry fails; zero
    alerts fails, judged by ``financial_scoring.total_alerts`` when scoring
    ran and by the sum of ``alerts_by_rule`` otherwise; a skip that is not
    in ``EXPECTED_SKIPS`` with its reason fails; with per-rule counts
    recorded, a rule of ``RULE_TARGETS`` that neither ran nor was an allowed
    skip fails, and so does a rule outside it. With no per-rule counts the
    rule set is not measured: a warning, as the CLI says. A ``run --stage
    gold-finalize`` record is judged the same way; another single stage
    runs no detection. Continuous takes the rules from the gold-refresh
    per-rule time-to-detect lines, which exist only for rules that alerted:
    it fails a mode-excluded rule (or one outside ``RULE_TARGETS``) that
    ran, and zero alerts unless a cycle could not count them. The continuous
    record carries no rule errors or skips, so neither is judged there, and
    a rule with no alerts is not a failure.
    """
    from lakebench.benchmark.aml_queries import RULE_TARGETS
    from lakebench.config.support import AML_CONTINUOUS_SKIPPED_RULES

    res = _GateResult()
    if getattr(metrics, "stage_only", None) not in (None, "gold-finalize"):
        return res
    mode = "continuous" if _is_sustained(metrics) else "batch"
    allowed = EXPECTED_SKIPS.get(("financial", mode), {})
    expected = set(RULE_TARGETS)
    if mode == "continuous":
        expected -= set(AML_CONTINUOUS_SKIPPED_RULES)
    gold_jobs = [j for j in metrics.jobs if j.job_type == "gold-finalize"]
    last = gold_jobs[-1] if gold_jobs else None
    if mode == "batch" and last is None:
        return res  # no gold stage: the layer_rows gate fails the run
    errors = dict(getattr(last, "rule_errors", None) or {})
    skipped = dict(getattr(last, "rules_skipped", None) or {})
    by_rule = dict(getattr(last, "alerts_by_rule", None) or {})
    executed = set(by_rule)
    if mode == "continuous":
        executed |= {r for s in metrics.streaming for r in (s.ttd_by_rule or {})}
    executed -= set(errors)
    for rule, err in sorted(errors.items()):
        res.reasons.append(f"detection rule {rule} failed: {err}")
    for rule, why in sorted(skipped.items()):
        if str(why) not in allowed.get(rule, frozenset()):
            res.reasons.append(f"rule {rule} skipped ({why}), not an allowed skip")
        elif "cap" in str(why):
            # An allowed skip on a Lakebench cap: the run passes, labelled.
            res.qualifiers.setdefault(RULE_CAPS, {})[rule] = str(why)
    outside = sorted(executed - expected)
    if outside:
        res.reasons.append(f"rules outside the expected set ran: {', '.join(outside)}")
    if mode == "continuous":
        streams = _last_by_type(metrics.streaming)
        alerts = _aml_continuous_alerts(streams.get("gold-refresh"))
        if alerts == 0:
            res.reasons.append("AML continuous run produced zero alerts")
    if mode == "batch":
        scoring = getattr(metrics, "financial_scoring", None)
        total = scoring.get("total_alerts") if isinstance(scoring, Mapping) else None
        if total is not None:
            if int(total) == 0:
                res.reasons.append("AML batch run produced zero alerts (scoring total_alerts 0)")
        elif by_rule and sum(int(v or 0) for v in by_rule.values()) == 0:
            res.reasons.append("AML batch run produced zero alerts (every rule 0)")
        if by_rule:
            missing = sorted(expected - executed - set(skipped) - set(errors))
            if missing:
                res.reasons.append(f"expected rules did not run: {', '.join(missing)}")
        elif not errors:
            res.warnings.append(
                "executed rule set not recorded: no per-rule counts in the gold-finalize log"
            )
    res.outcome = "FAIL" if res.reasons else "PASS"
    return res


def _scale_ratio_gate(metrics: PipelineMetrics) -> _GateResult:
    """Batch: the bronze the run read is at least 95% of the scale's
    expected volume. A ratio of 0 means bronze was not measured and fails
    (before v1.7 it passed). Not applied to continuous runs, a record with
    no pipeline benchmark, or a ``run --stage`` record of a stage other than
    bronze-verify (the ratio then divides another layer's input by the
    expected bronze)."""
    res = _GateResult()
    pb = metrics.pipeline_benchmark
    stage_only = getattr(metrics, "stage_only", None)
    if pb is None or _is_sustained(metrics) or stage_only not in (None, "bronze-verify"):
        return res
    ratio = float(pb.scale_ratio or 0.0)
    if ratio <= 0:
        res.reasons.append("Scale ratio 0: bronze input volume was not measured")
    elif ratio < 0.95:
        res.reasons.append(f"Scale ratio {ratio:.1%} < 95% (incomplete data)")
    res.outcome = "FAIL" if res.reasons else "PASS"
    return res


def _query_answers_gate(metrics: PipelineMetrics) -> _GateResult:
    """A successful benchmark query that returned no rows fails unless its
    ``BenchmarkQuery.allow_empty`` declares that it may (design
    contradiction 2). A continuous run with in-stream rounds is judged on
    its last round only, as the CLI judges it: earlier rounds can run before
    gold holds rows. A query absent from ``BENCHMARK_QUERIES_BY_DOMAIN`` is
    left out with a warning, and one with no ``rows_returned`` is not
    judged. Not applied when the record holds no benchmark queries."""
    from lakebench.benchmark.queries import BENCHMARK_QUERIES_BY_DOMAIN

    res = _GateResult()
    rounds = _rounds(metrics)
    bench = rounds[-1] if rounds else metrics.benchmark
    if bench is None and metrics.pipeline_benchmark is not None:
        bench = metrics.pipeline_benchmark.query_benchmark
    queries = [q for q in (getattr(bench, "queries", None) or []) if isinstance(q, Mapping)]
    if not queries:
        return res
    registry = {bq.name: bq.allow_empty for qs in BENCHMARK_QUERIES_BY_DOMAIN.values() for bq in qs}
    empty: list[str] = []
    unknown: list[str] = []
    for q in queries:
        name = str(q.get("name") or q.get("query_name") or "")
        rows = q.get("rows_returned")
        if not q.get("success", True) or rows is None or int(rows) != 0:
            continue
        if name not in registry:
            unknown.append(name)
        elif not registry[name]:
            empty.append(name)
    if empty:
        res.reasons.append(
            f"{len(empty)} benchmark queries returned no rows ({', '.join(empty)}); "
            "an empty result measures nothing"
        )
    if unknown:
        res.warnings.append(
            "queries not in the registry returned no rows and were not judged: "
            + ", ".join(unknown)
        )
    res.outcome = "FAIL" if res.reasons else "PASS"
    return res


def record_gates(metrics: PipelineMetrics) -> dict[str, _GateResult]:
    """The record gates that apply to *metrics*, by gate id:
    ``layer_rows``, ``aml_rules`` (financial runs), ``scale_ratio`` (batch)
    and ``query_answers`` (runs with benchmark queries). A gate that does
    not apply is left out. Empty for an interrupted run, and for a run whose
    ``pipeline`` gate failed: a failed stage already fails the verdict, and
    the empty layers after it are its consequence, not another finding."""
    if _interrupt_record(metrics) is not None or _pipeline_gate_outcome(metrics) == "FAIL":
        return {}
    gates: dict[str, _GateResult] = {"layer_rows": _layer_rows_gate(metrics)}
    if _workload(metrics) == "financial":
        gates["aml_rules"] = _aml_rules_gate(metrics)
    gates["scale_ratio"] = _scale_ratio_gate(metrics)
    gates["query_answers"] = _query_answers_gate(metrics)
    return {k: g for k, g in gates.items() if g.outcome is not None}


def _recompute(record: Mapping[str, Any]) -> Verdict:
    from lakebench.metrics.storage import MetricsStorage

    return compute_verdict(MetricsStorage()._dict_to_metrics(dict(record)))


def verdict_from_record(record: Mapping[str, Any]) -> Verdict:
    """The verdict of a stored metrics.json dict, decided from the record
    alone: the dict is loaded the way ``lakebench`` loads a stored run
    (``MetricsStorage._dict_to_metrics``) and judged by ``compute_verdict``.
    A save computes its verdict with the same function, so a fresh record
    reads back the status it was saved with."""
    return _recompute(record)


def save_gate_problems(metrics: PipelineMetrics) -> list[str]:
    """For the CLI, before ``save_run``: the reasons the record about to be
    saved does not read PASSED, or ``[]`` when it does. The verdict is the
    one ``PipelineMetrics.to_dict`` stores, which is decided from the
    serialised record (``verdict_from_record``), so the exit code, the
    stored verdict and every later reader apply one rule to one record. The
    CLI's own gates print a problem as it happens. A record that cannot be
    judged returns that as its problem."""
    try:
        v = metrics.to_dict()["verdict"]
    except Exception as e:  # noqa: BLE001 -- fail closed, never a silent pass
        return [f"the verdict could not be computed from the record ({type(e).__name__}: {e})"]
    if v.get("status") == "PASSED":
        return []
    reasons = [str(r) for r in v.get("reasons") or []]
    return [r for r in reasons if not r.startswith("Gate '")] or [f"verdict {v.get('status')}"]


def apply_save_gate(metrics: PipelineMetrics, ok: bool, report: Callable[[str], None]) -> bool:
    """The CLI's last gate, immediately before ``save_run``: when the run
    has passed so far (*ok*) but the record about to be saved does not read
    PASSED, each reason goes to *report*, ``success`` turns False and the
    reasons are kept in ``failure_reasons`` (so the saved verdict names
    them). Returns whether the run still passes, which sets the exit code."""
    if not ok:
        return False
    problems = save_gate_problems(metrics)
    for p in problems:
        report(f"Verdict: {p}")
        if p not in metrics.failure_reasons:
            metrics.failure_reasons.append(p)
    if problems:
        metrics.success = False
        return False
    return True


def compute_verdict(metrics: PipelineMetrics) -> Verdict:
    """Compute a ``Verdict`` for a completed ``PipelineMetrics`` run.

    ``exit_ok`` is inferred from ``success`` (today the CLI writes
    ``success = True`` when it intended to exit 0). ``badge_ok`` is the HTML
    badge's pass/fail decision, computed via the shared
    ``compute_badge_status`` helper. ``success_flag`` is ``metrics.success``.
    ``gate_outcomes`` covers, at minimum, ``pipeline`` and ``benchmark``,
    and adds ``c360`` when a check in the c360 gating list failed or did not
    run (``c360_correctness.gating_outcome``), ``dependency_set`` when a pod
    ran another dependency set, and the record gates of
    ``record_gates`` (``layer_rows``, ``aml_rules``, ``scale_ratio``,
    ``query_answers``) that apply.

    An interrupted run (``metrics.interrupted``) adds ``interrupt =
    "INTERRUPTED"``. Its status is INTERRUPTED when ``prior_failure`` is
    false and no other gate failed (the stage the interrupt stopped is not a
    failure), and FAILED otherwise. It is never PASSED, and ``success`` stays
    False.
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
    deps_reason = _deps_pods_reason(metrics)
    if deps_reason is not None:
        gate_outcomes["dependency_set"] = "FAIL"
    gates = record_gates(metrics)
    for name, gate in gates.items():
        if gate.outcome is not None:
            gate_outcomes[name] = gate.outcome

    reasons: list[str] = []
    interrupted = _interrupt_record(metrics)
    if interrupted is not None:
        gate_outcomes["interrupt"] = "INTERRUPTED"
    if (
        interrupted is not None
        and interrupted.get("prior_failure") is False
        and not (getattr(metrics, "failure_reasons", None) or [])
    ):
        # The interrupt is why success is False, and the badge's other
        # reasons (an ingest ratio over a cut window, say) describe partial
        # data. The verdict is INTERRUPTED unless a gate failed; never PASSED.
        exit_ok = badge_ok = success_flag = True
        reasons.append(interrupt_reason(interrupted))
    else:
        # Not interrupted, or something had already failed (or the record
        # does not say): today's inputs stand, so an interrupt after a
        # failure reads FAILED.
        if not exit_ok:
            # The run's own reasons first (a gate the CLI or the save gate
            # failed names itself), so a reader of reasons[0] sees why.
            for r in getattr(metrics, "failure_reasons", None) or []:
                if r not in reasons:
                    reasons.append(r)
            reasons.append("Process exit intent was not OK")
        for r in badge_reasons:
            if r not in reasons:
                reasons.append(r)
    if c360_reason and c360_reason not in reasons:
        reasons.append(c360_reason)
    if deps_reason is not None:
        reasons.append(deps_reason)
    for gate in gates.values():
        for r in gate.reasons:
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
    if isinstance(metrics.c360_correctness, Mapping):
        # Failed checks outside the gating list: shown, never a FAIL.
        not_gating = c360_correctness.reporting_failures(metrics.c360_correctness)
        if not_gating:
            qualifiers["c360_failed_not_gating"] = not_gating
    for gate in gates.values():
        qualifiers.update(copy.deepcopy(gate.qualifiers))
    mismatched = _requested_effective_mismatches(metrics)
    if mismatched:
        # A request the run did not meet: labelled, never a FAIL.
        qualifiers["requested_effective"] = mismatched
    preflight = (getattr(metrics, "provenance", None) or {}).get("preflight") or {}
    if preflight.get("capacity") == "skipped":
        # --skip-preflight: nothing checked that the cluster could hold it.
        qualifiers["capacity"] = "capacity not checked"
    if preflight.get("scratch") == "not_measurable":
        qualifiers["scratch_capacity"] = "scratch capacity not checked"

    return Verdict.strictest(
        exit_ok=exit_ok,
        badge_ok=badge_ok,
        success_flag=success_flag,
        gate_outcomes=gate_outcomes,
        reasons=reasons,
        qualifiers=qualifiers,
    )
