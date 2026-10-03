"""What a reader must know before any metric: the report's front matter.

Both the HTML page and the terminal ``lakebench report`` render
:class:`FrontMatter` first, in this order: verdict and headline, evidence
class, support state with its meaning, binding caps ("none" when empty), n,
provenance, digest. No metric precedes it.

Every field is read from the run's record:

- **Verdict**: the strictest of the verdict the record stored and the one
  recomputed from it (``metrics.verdict.compute_verdict``); a reader never
  promotes a record. When they differ the front matter says so ("stored
  PASSED; recomputed FAILED").
- **Evidence class**: read only from the registered-look record
  (``config.datagen_seed.load_looks``), never from the config, since a
  config can claim anything. A completed look entry whose ``run_ids`` list
  names this run is "registered look: <role>"; an AML calibration corpus
  (role ``calibration``, or the public development seed) is in-sample
  development; everything else, including a missing or unreadable look
  record, is "development".
- **Support state**, **binding caps** (``metrics.bounds.binding_caps``),
  **n**, **provenance** and the identity **digest** from the stored
  experiment block.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from lakebench.reports import copy as words

#: compute_verdict puts this first whenever ``success`` is False; it says
#: nothing a reader can act on, so a headline takes the next reason.
EXIT_INTENT_REASON = "Process exit intent was not OK"

#: The public AML development seed (docs/internal/aml-protocol.md): its
#: corpus is the one the rules were developed on. ``config.datagen_seed.
#: calibration_seed()`` is the source; this is its documented value, used
#: only if that cannot be read.
DEVELOPMENT_SEED = 43


def _calibration_seed() -> int:
    try:
        from lakebench.config.datagen_seed import calibration_seed

        return int(calibration_seed())
    except Exception:  # noqa: BLE001
        return DEVELOPMENT_SEED


_SUPPORT_MEANING = {
    "supported": "a release-matrix run validated this combination at these versions",
    "unverified": "a valid combination that no release-matrix run has validated",
    "unsupported": "known not to work; its numbers are not evidence",
}


@dataclass(frozen=True)
class FrontMatter:
    verdict: str  # PASSED | FAILED | INTERRUPTED | REFUSED
    verdict_note: str  # "stored PASSED; recomputed FAILED", or ""
    headline: str
    evidence_class: str
    evidence_basis: str
    support_state: str
    support_basis: str
    binding_caps: list[str]
    n_runs: int
    samples_per_query: int | None
    digest: str
    provenance: str
    corpus: str = ""
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    qualifiers: list[str] = field(default_factory=list)
    limits: list[str] = field(default_factory=list)

    def lines(self) -> list[tuple[str, str]]:
        """``(label, text)`` in the order every renderer shows them."""
        verdict = self.verdict + (f" ({self.verdict_note})" if self.verdict_note else "")
        n = f"{self.n_runs} run" + ("" if self.n_runs == 1 else "s")
        if self.samples_per_query:
            n += (
                f", {self.samples_per_query} sample"
                + ("" if self.samples_per_query == 1 else "s")
                + " per query"
            )
        return [
            ("Verdict", verdict),
            ("Headline", self.headline),
            ("Evidence class", f"{self.evidence_class} ({self.evidence_basis})"),
            ("Corpus", self.corpus or "not recorded"),
            ("Support state", f"{self.support_state} ({self.support_basis})"),
            ("Binding caps", "; ".join(self.binding_caps) or "none"),
            ("n", n),
            ("Provenance", self.provenance),
            ("Digest", self.digest),
        ]


# ---------------------------------------------------------------------------
# Pieces, usable on their own
# ---------------------------------------------------------------------------


def _metrics_of(record: Any):
    """A ``PipelineMetrics`` for *record*: a loaded one as is, a metrics.json
    dict loaded the way ``lakebench`` loads a stored run."""
    if isinstance(record, Mapping):
        from lakebench.metrics.storage import MetricsStorage

        return MetricsStorage()._dict_to_metrics(dict(record))
    return record


def page_verdict(metrics) -> tuple[str, list[str], str]:
    """``(status, reasons, note)``: the strictest of the stored and the
    recomputed verdict, and a note naming both when they differ."""
    from lakebench.metrics.verdict import Verdict, compute_verdict

    recomputed = compute_verdict(metrics)
    status: str = str(recomputed.status)
    reasons = list(recomputed.reasons)
    stored = getattr(metrics, "stored_verdict", None)
    note = ""
    if isinstance(stored, Mapping) and isinstance(stored.get("status"), str):
        # An empty stored status is no pass (metrics/verdict.judge's rule).
        stored_status = str(stored["status"]) or "FAILED"
        if stored_status != status:
            note = f"stored {stored_status}; recomputed {status}"
            stored_reasons = [str(r) for r in stored.get("reasons") or []] or [
                f"stored verdict {stored_status}"
            ]
            order = list(Verdict.PRIORITY)
            rank = {s: i for i, s in enumerate(order)}
            # The strictest wins (FAILED > INTERRUPTED > REFUSED > PASSED);
            # a reader never promotes. Both sides' reasons are kept,
            # the winner's first.
            if rank.get(stored_status, 0) < rank.get(status, len(order)):
                status, first, second = stored_status, stored_reasons, reasons
            else:
                # A stored PASSED has no reasons to add to a failure.
                first = reasons
                second = [] if stored_status == "PASSED" else stored_reasons
            reasons = first + [r for r in second if r not in first]
    return status, reasons, note


def headline_reason(status: str, reasons: list[str]) -> str:
    """The first reason a reader can act on."""
    useful = [r for r in reasons if r != EXIT_INTENT_REASON]
    if useful:
        return useful[0]
    return reasons[0] if reasons else f"verdict {status}"


def look_for_run(run_id: str | None) -> tuple[str | None, str | None, str | None]:
    """``(role, report_sha256, error)`` of the completed registered look whose
    ``run_ids`` list names *run_id*; all None when none does, and *error*
    set when the look record cannot be read (which names no run)."""
    if not run_id:
        return None, None, None
    try:
        from lakebench.config.datagen_seed import load_looks

        looks = load_looks()
    except Exception as exc:  # noqa: BLE001 -- an unreadable record names no run
        # The class only: load_looks's messages quote the entry, seed
        # included, and a held-out seed must never reach a report.
        return None, None, type(exc).__name__
    for entry in looks:
        run_ids = entry.get("run_ids") if isinstance(entry, Mapping) else None
        # Only a list names runs; anything else names none (fail closed).
        if (
            entry.get("state") == "complete"
            and isinstance(run_ids, list)
            and any(isinstance(r, str) and r == run_id for r in run_ids)
        ):
            return str(entry.get("role")), str(entry.get("report_sha256") or ""), None
    return None, None, None


def registered_look_role(run_id: str | None) -> str | None:
    """The role of the completed registered look that names this run."""
    return look_for_run(run_id)[0]


def _d(value: Any) -> dict[str, Any]:
    """*value* when it is a dict, else {}: a malformed block reads empty."""
    return value if isinstance(value, dict) else {}


def _experiment(metrics) -> dict[str, Any]:
    try:
        exp = metrics.experiment_block() or {}
    except Exception:  # noqa: BLE001 -- a bad block must not break the render
        exp = {}
    return exp if isinstance(exp, dict) else {}


def evidence_class(metrics) -> tuple[str, str]:
    """``(class, basis)`` from the look record and the stored corpus."""
    role, sha, error = look_for_run(getattr(metrics, "run_id", None))
    if role:
        return f"registered look: {role}", f"report sha256 {(sha or 'not recorded')[:12]}"
    corpus = _d(_experiment(metrics).get("corpus"))
    workload = (getattr(metrics, "config_snapshot", None) or {}).get("workload_schema")
    if workload == "financial" and (
        corpus.get("corpus_role") == "calibration" or corpus.get("seed") == _calibration_seed()
    ):
        return (
            "development (calibration corpus: in-sample, the corpus the rules were developed on)",
            "no look record names this run",
        )
    if error:
        return "development", f"look record unreadable: {error}"
    return "development", "no look record names this run"


def provenance_line(metrics) -> str:
    """Which Lakebench produced the record: version, commit and whether the
    tree had uncommitted changes."""
    prov = getattr(metrics, "provenance", None)
    if not isinstance(prov, dict) or not prov:
        return "not recorded (this record predates provenance)"
    version = prov.get("lakebench_version") or "version unknown"
    sha = str(prov.get("git_sha") or "")[:7] or "commit unknown"
    dirty = prov.get("git_dirty")
    state = "dirty" if dirty is True else "clean" if dirty is False else "tree state unknown"
    return f"lakebench {version}, commit {sha}, {state}"


def _corpus_label(exp: dict[str, Any]) -> str:
    """The corpus the run read: role, id (v1, first 12) and scale."""
    corpus = _d(exp.get("corpus"))
    if not corpus:
        return ""
    role = corpus.get("corpus_role") or "none"
    cid = str(corpus.get("id") or "unknown")[:12]
    return f"{role} (id {cid}, scale {corpus.get('scale')})"


def _digest(exp: dict[str, Any]) -> str:
    if not exp:
        return "none (this record predates the experiment block)"
    try:
        from lakebench.metrics.experiment import identity_hash

        return identity_hash(exp)
    except Exception:  # noqa: BLE001
        return "unresolved"


def _qualifier_lines(qualifiers: Mapping[str, Any]) -> list[str]:
    """The verdict qualifiers a reader must see beside the verdict."""
    from lakebench.metrics.verdict import LAYER_ROWS_UNMEASURED, RULE_CAPS

    out = []
    caps = qualifiers.get(RULE_CAPS)
    if isinstance(caps, Mapping) and caps:
        out.append(
            "rules skipped on a Lakebench cap: "
            + ", ".join(f"{r} ({why})" for r, why in sorted(caps.items()))
        )
    unmeasured = qualifiers.get(LAYER_ROWS_UNMEASURED)
    if isinstance(unmeasured, list) and unmeasured:
        out.append(
            "rows not measured (the layer gate passed on bytes): "
            + ", ".join(str(x) for x in unmeasured)
        )
    not_gating = qualifiers.get("c360_failed_not_gating")
    if isinstance(not_gating, list) and not_gating:
        out.append(
            "C360 checks failed outside the gating set (not a verdict FAIL): "
            + ", ".join(str(x) for x in not_gating)
        )
    for key in ("capacity", "scratch_capacity", "investigators"):
        if qualifiers.get(key):
            out.append(str(qualifiers[key]))
    return out


def limits_interpretation(
    metrics,
    exp: dict[str, Any] | None = None,
    *,
    count: Callable[[int, str], str] | None = None,
    esc: Callable[[str], str] | None = None,
) -> list[str]:
    """What limits interpretation: n=1, rules skipped or errored, continuous
    AML rules not run, in-sample AML recall, a dirty tree. Plain text by
    default; the HTML page passes *count* (a derived span for a count at a
    record path) and *esc* (HTML escaping)."""
    count = count or (lambda n, _path: str(n))
    esc = esc or (lambda t: t)
    exp = _experiment(metrics) if exp is None else exp
    items: list[str] = []
    runs = _d(exp.get("repetitions")).get("runs") or 1
    if runs == 1:
        items.append("n=1: one run, so no figure here is a repeatability claim")
    rules = _d(exp.get("rules"))
    skipped = _d(rules.get("skipped"))
    errored = _d(rules.get("errored"))
    skipped_path, errored_path = "experiment.rules.skipped", "experiment.rules.errored"
    if not skipped and not errored:
        # Records before the rules block: the last gold-finalize job's skips.
        gold = [i for i, j in enumerate(metrics.jobs) if j.job_type == "gold-finalize"]
        if gold:
            job = metrics.jobs[gold[-1]]
            skipped = dict(job.rules_skipped or {})
            errored = dict(job.rule_errors or {})
            skipped_path = f"jobs[{gold[-1]}].rules_skipped"
            errored_path = f"jobs[{gold[-1]}].rule_errors"
    if skipped:
        names = ", ".join(f"{k} ({v})" for k, v in sorted(skipped.items()))
        items.append(f"{count(len(skipped), skipped_path)} detection rule(s) skipped: {esc(names)}")
    if errored:
        items.append(
            f"{count(len(errored), errored_path)} detection rule(s) errored: "
            f"{esc(', '.join(sorted(errored)))}"
        )
    if (metrics.config_snapshot or {}).get("workload_schema") == "financial":
        from lakebench.config.support import AML_CONTINUOUS_SKIPPED_RULES

        pb = metrics.pipeline_benchmark
        if pb is not None and pb.pipeline_mode in ("sustained", "continuous"):
            executed = set(rules.get("executed") or [])
            not_run = [r for r in AML_CONTINUOUS_SKIPPED_RULES if r not in executed]
            n_not_run = len(not_run)  # a fixed rule list, not a record count
            if not_run:
                ids = ", ".join(r.split("_", 1)[0] for r in not_run)
                items.append(
                    f"{n_not_run} detection rules not run in continuous mode ({esc(ids)}): "
                    "their typologies have no result in this run"
                )
        if not registered_look_role(metrics.run_id):
            items.append(
                "AML recall is uncalibrated and in-sample: no registered look names this run"
            )
    prov = getattr(metrics, "provenance", None)
    if isinstance(prov, dict) and prov.get("git_dirty") is True:
        items.append("produced from a tree with uncommitted changes (dirty)")
    return items


def _passed_headline(metrics, exp: dict[str, Any], n_runs: int) -> str:
    snap = getattr(metrics, "config_snapshot", None) or {}
    workload = words.workload_label(snap.get("workload_schema"))
    mode = words.mode_label(
        metrics.pipeline_benchmark.pipeline_mode if metrics.pipeline_benchmark else "batch"
    )
    scale = _d(exp.get("corpus")).get("scale") or snap.get("scale")
    parts = [f"{workload} {mode}" + (f", scale {scale}" if scale is not None else "")]
    rules = _d(exp.get("rules"))
    executed = list(rules.get("executed") or [])
    skipped = _d(rules.get("skipped"))
    if executed or skipped:
        part = f"{len(executed)} of {len(executed) + len(skipped)} rules ran"
        if skipped:
            part += (
                " ("
                + ", ".join(
                    f"{r.split('_', 1)[0]} skipped: {why}" for r, why in sorted(skipped.items())
                )
                + ")"
            )
        parts.append(part)
    parts.append(f"n={n_runs}")
    return ", ".join(parts)


def front_matter(record: Any) -> FrontMatter:
    """The front matter of a run: *record* is a metrics.json dict or a loaded
    ``PipelineMetrics``."""
    from lakebench.metrics.bounds import binding_caps
    from lakebench.metrics.verdict import compute_badge_status, compute_verdict

    metrics = _metrics_of(record)
    if isinstance(record, Mapping) and getattr(metrics, "stored_verdict", None) is None:
        metrics.stored_verdict = record.get("verdict")
    exp = _experiment(metrics)
    status, reasons, note = page_verdict(metrics)
    _ok, _fail, warnings = compute_badge_status(metrics)
    rep = _d(exp.get("repetitions"))
    runs = rep.get("runs")
    n_runs = runs if isinstance(runs, int) and not isinstance(runs, bool) and runs > 0 else 1
    samples = rep.get("benchmark_samples_per_query")
    samples = samples if isinstance(samples, int) and not isinstance(samples, bool) else None
    if status != "PASSED":
        headline = headline_reason(status, reasons)
    elif warnings:
        headline = warnings[0]
    else:
        headline = _passed_headline(metrics, exp, n_runs)
    ev_class, ev_basis = evidence_class(metrics)
    sup = _d(exp.get("support"))
    state = str(sup.get("state") or "unknown")
    basis = str(sup.get("basis") or "") or _SUPPORT_MEANING.get(state, "not recorded")
    meaning = _SUPPORT_MEANING.get(state)
    if meaning and meaning not in basis:
        basis = f"{meaning}; {basis}" if basis else meaning
    caps = [str(c) for c in binding_caps(metrics)]
    mismatches = exp.get("requested_effective_mismatches") or []
    for m in mismatches if isinstance(mismatches, list) else []:
        caps.append(f"requested and effective differ: {m}")
    try:
        qualifiers = _qualifier_lines(compute_verdict(metrics).qualifiers)
    except Exception:  # noqa: BLE001 -- the verdict above already decided
        qualifiers = []
    return FrontMatter(
        verdict=status,
        verdict_note=note,
        headline=headline,
        evidence_class=ev_class,
        evidence_basis=ev_basis,
        support_state=state,
        support_basis=basis,
        binding_caps=caps,
        n_runs=n_runs,
        samples_per_query=samples,
        digest=_digest(exp),
        provenance=provenance_line(metrics),
        corpus=_corpus_label(exp),
        reasons=reasons,
        warnings=list(warnings),
        qualifiers=qualifiers,
        limits=limits_interpretation(metrics, exp),
    )


def rich_markup(fm: FrontMatter) -> str:
    """The front matter as Rich markup, for the terminal ``report`` and the
    end-of-run panel."""
    from rich.markup import escape

    colour = (
        "green"
        if fm.verdict == "PASSED" and not fm.warnings
        else "yellow"
        if fm.verdict == "PASSED"
        else "red"
    )
    lines = []
    for label, text in fm.lines():
        value = f"[{colour}]{escape(text)}[/{colour}]" if label == "Verdict" else escape(text)
        lines.append(f"[bold]{label}:[/bold] {value}")
    notes = [*fm.qualifiers, *(fm.warnings[1:] if fm.verdict == "PASSED" else fm.warnings)]
    lines.extend(f"[yellow]- {escape(n)}[/yellow]" for n in notes)
    if fm.limits:
        lines.append("[bold]What limits interpretation:[/bold]")
        lines.extend(f"  - {escape(item)}" for item in fm.limits)
    return "\n".join(lines)


def print_front_matter(metrics, console, *, storage=None, run_id: str | None = None) -> None:
    """Print the front matter panel for *metrics*, or for the record
    *storage* holds for *run_id* when given (read back after a save). Never
    raises: a run that finished is not failed by its summary."""
    import logging

    from rich.markup import escape
    from rich.panel import Panel

    try:
        if storage is not None and run_id:
            metrics = storage.load_run(run_id) or metrics
        fm = front_matter(metrics)
        title = "Read this first: run " + escape(str(metrics.run_id))
        console.print(Panel(rich_markup(fm), title=title, expand=False))
    except Exception:  # noqa: BLE001 -- the summary never fails a finished run
        logging.getLogger(__name__).warning("front matter not printed", exc_info=True)
        # Never scores without a verdict: the bare status at least.
        try:
            console.print("Verdict: " + escape(str(page_verdict(metrics)[0])))
        except Exception:  # noqa: BLE001
            console.print("Verdict: not computable from this record")
