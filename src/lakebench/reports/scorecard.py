"""Per-domain scorecard extension points for the report generator.

ENG-2C.5. A ScorecardBlock owns the domain-specific parts of an HTML
scorecard: the display label that appears in the run-context banner
plus (in later revisions) any workload-specific detail rows. The block
for the run's workload schema is looked up via
:func:`get_scorecard_block`, which the report generator calls once per
render.

Only ``domain_label`` is consumed today, per the ENG-2C.5 rescope in
docs/lakebench.next-spec-addendum.md A.4. The ``render_detail_html``
hook exists so Financial can slot in Investigator-panel rows and
Customer 360 can move its existing panels off the hardcoded generator
path without another interface change.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from lakebench.metrics import PipelineMetrics


@runtime_checkable
class ScorecardBlock(Protocol):
    """Per-schema scorecard extension point.

    Implementations declare a stable ``schema_name`` slug (matching
    :class:`~lakebench.config.schema.WorkloadSchema` values) so the
    registry can be looked up by string, e.g. from a metrics.json
    ``config_snapshot`` field written by an older run.
    """

    schema_name: str
    domain_label: str

    def render_detail_html(self, metrics: PipelineMetrics) -> str:
        """Return workload-specific detail HTML for the scorecard.

        Empty string means "no extra rows"; the generator inserts it
        verbatim after the shared stage table.
        """
        ...


class Customer360ScorecardBlock:
    """Scorecard block for the Customer 360 medallion pipeline."""

    schema_name = "customer360"
    domain_label = "Customer360"

    def render_detail_html(self, metrics: PipelineMetrics) -> str:
        return ""


class FinancialScorecardBlock:
    """Scorecard block for the Financial (FinServ-Crime, AML) workload.

    Renders a per-rule detection table for a batch run: alert count, the
    planted typology each rule targets, recall (from the folded-in
    ``financial score``, LB-123), and a status that reads "not run" for a
    rule the gold-finalize step skipped (e.g. W1 above its vertex cap,
    LB-119) -- never 0%, which would misreport a skip as a miss.
    """

    schema_name = "financial"
    domain_label = "Financial (FinServ-Crime, AML)"

    def render_detail_html(self, metrics: PipelineMetrics) -> str:
        # The report generator must never crash rendering a report. This
        # method reads best-effort, possibly hand-editable data (recall.json,
        # driver-log-parsed dicts), so the whole body degrades to "" on any
        # unexpected shape rather than taking down every other report section.
        try:
            return self._render_detail_html(metrics)
        except Exception:  # noqa: BLE001 -- render must never crash the report
            return ""

    def _render_detail_html(self, metrics: PipelineMetrics) -> str:
        if metrics is None:
            return ""

        # Detection metrics come from the gold-finalize job. gold_finalize
        # re-detects over the WHOLE cumulative silver each cycle and does a
        # per-rule DELETE-then-INSERT, so each cycle's [detection] counts are
        # CUMULATIVE and the final gold.alerts holds the last cycle's totals --
        # NOT the sum across cycles (an earlier "sum" fix was backwards: it
        # would ~triple a 3-cycle run and contradict the score-financial
        # footer, which reads the final gold.alerts once). Use the LAST
        # gold-finalize job's dicts as the authoritative final state
        # (last-cycle-wins), which is also correct for a single-cycle run.
        # Taking one job's pair keeps alerts and skips mutually consistent
        # (a rule skipped in the final cycle is in rules_skipped and absent
        # from alerts_by_rule).
        alerts_by_rule: dict[str, int] = {}
        rules_skipped: dict[str, str] = {}
        jobs = getattr(metrics, "jobs", None) or []
        gold_jobs = [j for j in jobs if getattr(j, "job_type", "") == "gold-finalize"]
        source_jobs = gold_jobs or [
            j
            for j in jobs
            if (getattr(j, "alerts_by_rule", None) or getattr(j, "rules_skipped", None))
        ]
        if source_jobs:
            last = source_jobs[-1]
            alerts_by_rule = dict(getattr(last, "alerts_by_rule", None) or {})
            rules_skipped = dict(getattr(last, "rules_skipped", None) or {})
            rule_errors = dict(getattr(last, "rule_errors", None) or {})

        scoring = getattr(metrics, "financial_scoring", None)
        if not source_jobs:
            rule_errors = {}

        # Nothing AML-specific to show (e.g. a c360 run mislabelled, or a
        # financial run before detection wired) -- stay silent.
        if not alerts_by_rule and not rules_skipped and not scoring:
            return _safe_tm_section(metrics, gold_jobs)

        try:
            from lakebench.benchmark.aml_queries import RULE_TARGETS
        except Exception:  # noqa: BLE001 -- render must never crash the report
            RULE_TARGETS = {}

        recall_by_typology: dict[str, dict] = {}
        total_alerts = None
        fp_rate = None
        fp_by_rule: dict = {}
        chance_by_rule: dict = {}
        txn_prec_by_rule: dict = {}
        if scoring:
            for t in scoring.get("typologies", []) or []:
                tt = t.get("typology_type")
                if tt:
                    recall_by_typology[tt] = t
            total_alerts = scoring.get("total_alerts")
            fp_rate = scoring.get("fp_rate")
            fp_by_rule = dict(scoring.get("fp_rate_by_rule") or {})
            chance_by_rule = dict(scoring.get("chance_by_rule") or {})
            txn_prec_by_rule = dict(scoring.get("txn_precision_by_rule") or {})

        # Known rules first (in RULE_TARGETS order), then any rule that
        # emitted alerts or skipped but is not yet in RULE_TARGETS -- so a
        # newly added detection rule's alerts are never silently dropped from
        # the report (LB-123 review).
        known = list(RULE_TARGETS)
        extra = sorted((set(alerts_by_rule) | set(rules_skipped) | set(rule_errors)) - set(known))
        rules = known + extra

        body_rows: list[str] = []
        for rule in rules:
            typ = RULE_TARGETS.get(rule)
            alerts = alerts_by_rule.get(rule)
            incidental_cell = "-"
            fp_val = fp_by_rule.get(rule)
            fp_cell = f"{float(fp_val) * 100:.1f}%" if fp_val is not None else "-"
            chance_val = chance_by_rule.get(rule)
            chance_cell = f"{float(chance_val) * 100:.1f}%" if chance_val is not None else "-"
            txn_val = txn_prec_by_rule.get(rule)
            txn_cell = f"{float(txn_val) * 100:.2f}%" if txn_val is not None else "-"
            if rule in rule_errors:
                # A crashed rule is not "ran, 0 alerts".
                status = '<span style="color: var(--danger, red);">error</span>'
                recall_cell = "n/a"
                alerts_cell = "-"
            elif rule in rules_skipped:
                status = (
                    f'<span style="color: var(--warning);">not run</span> ({rules_skipped[rule]})'
                )
                recall_cell = "n/a"
                alerts_cell = "-"
            elif typ is None:
                # Attribute rule (W5 sanctions / W6 PEP): matches a party flag,
                # no planted typology to score recall against.
                status = "ran"
                recall_cell = "n/a (attribute)"
                alerts_cell = f"{alerts:,}" if alerts is not None else "0"
            else:
                alerts_cell = f"{alerts:,}" if alerts is not None else "0"
                trow = recall_by_typology.get(typ)
                if trow and trow.get("incidental_recall") is not None:
                    incidental_cell = f"{float(trow['incidental_recall']) * 100:.1f}%"
                if trow and trow.get("detection_status") == "rule_error":
                    status = '<span style="color: var(--danger, red);">error</span>'
                    recall_cell = "n/a"
                elif trow and trow.get("detection_status") == "partial":
                    status = '<span style="color: var(--warning);">partial</span>'
                    recall_cell = (
                        f"{float(trow['recall']) * 100:.1f}%"
                        if trow.get("recall") is not None
                        else "n/a"
                    )
                elif trow and trow.get("detection_status") == "rule_skipped":
                    status = '<span style="color: var(--warning);">not run</span>'
                    recall_cell = "n/a"
                elif trow and trow.get("recall") is not None:
                    status = '<span style="color: var(--success, green);">scored</span>'
                    recall_cell = f"{float(trow['recall']) * 100:.1f}%"
                else:
                    status = "ran" if alerts is not None else "no data"
                    recall_cell = "-"
            typ_cell = typ if typ is not None else "&mdash;"
            body_rows.append(
                f"<tr><td>{rule}</td><td>{typ_cell}</td>"
                f"<td>{alerts_cell}</td><td>{recall_cell}</td>"
                f"<td>{chance_cell}</td><td>{incidental_cell}</td>"
                f"<td>{fp_cell}</td><td>{txn_cell}</td><td>{status}</td></tr>"
            )

        # Recall counts only alerts from each typology's designated rule;
        # "Chance" is that rule's hit rate on the random control, the floor
        # its recall must beat. "Incidental" is recall credited by any rule.
        footer = ""
        if total_alerts is not None:
            fp_str = f"{fp_rate * 100:.1f}%" if fp_rate is not None else "n/a"
            footer = (
                '<div style="margin-top: 0.5rem; color: var(--text-muted); '
                'font-size: 0.8125rem;">'
                f"Total alerts: <strong>{total_alerts:,}</strong> | "
                f"False-positive rate (alerts touching no planted txn): <strong>{fp_str}</strong>"
                + "</div>"
            )

        tm_html = _safe_tm_section(metrics, gold_jobs)
        return (
            tm_html
            + f"""
        <section>
            <h3>Detection Scorecard</h3>
            <table>
                <thead>
                    <tr>
                        <th>Rule</th>
                        <th>Target typology</th>
                        <th title="Alerts emitted by this rule">Alerts</th>
                        <th title="Fraction of planted instances detected by this rule">Recall</th>
                        <th title="Share of random-control instances this rule's alerts touch; recall at or below it is chance">Chance</th>
                        <th title="Fraction detected by any rule (includes chance overlap)">Incidental</th>
                        <th title="Share of this rule's alerts that touch none of its target typology's txns">FP</th>
                        <th title="Share of the txns in this rule's alerts that are planted target txns">Txn precision</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(body_rows)}
                </tbody>
            </table>
            {footer}
        </section>
        """
        )


def _fmt_pct(v) -> str:
    return f"{float(v) * 100:.1f}%" if v is not None else "n/a"


def _fmt_n(v) -> str:
    return f"{int(v):,}" if isinstance(v, (int, float)) else "n/a"


def _safe_tm_section(metrics, gold_jobs: list) -> str:
    """The TM section in its own guard: a malformed ``tm_operations`` or
    ``tm_ops`` blanks this section only, never the detection table."""
    try:
        return _render_tm_operations(gold_jobs, getattr(metrics, "tm_operations", None))
    except Exception:  # noqa: BLE001 -- render must never crash the report
        return (
            "<section><h3>Transaction Monitoring Operations</h3>"
            "<p>The operations summary in this run's metrics could not be rendered.</p></section>"
        )


def _render_tm_operations(gold_jobs: list, verdict: dict | None = None) -> str:
    """TM operations pack (GOALS P10.3, core sections) from the last
    gold-finalize job's ``tm_ops`` summary, plus every cycle's invariants.

    Operations vocabulary (P10.4): "productive rate" (escalated or SAR), not
    precision. Every number here is conditional on the simulated analyst,
    which the section states with the accuracies used.
    """
    from html import escape

    verdict = verdict if isinstance(verdict, dict) else {}
    ops = verdict.get("ops") if isinstance(verdict.get("ops"), dict) else None
    cycles: list[tuple[str, str, dict]] = []
    if not ops:
        for j in reversed(gold_jobs or []):
            if getattr(j, "tm_ops", None):
                ops = j.tm_ops
                break
    if verdict.get("invariants"):
        # Run-level record: batch (merged over cycles) or continuous (one
        # entry per operations pass).
        for c, inv in sorted(verdict["invariants"].items(), key=lambda kv: int(kv[0])):
            cycles.append(("", str(c), inv))
    else:
        for idx, j in enumerate(gold_jobs or [], start=1):
            for c, inv in sorted((getattr(j, "tm_invariants", None) or {}).items()):
                cycles.append((str(idx), c, inv))
    if not ops and not cycles and not verdict:
        return ""
    ops = ops or {}
    parts = ["<section><h3>Transaction Monitoring Operations</h3>"]
    if verdict.get("status"):
        mode = verdict.get("mode") or ""
        unit = "operations pass" if mode == "continuous" else "cycle"
        colour = {"pass": "success", "fail": "danger"}.get(verdict["status"], "warning")
        parts.append(
            f"<p>P10 gate ({escape(mode)}, per {unit}): "
            f'<strong style="color: var(--{colour});">'
            f"{escape(str(verdict['status']).replace('_', ' '))}</strong>"
            + (f" -- {escape(str(verdict.get('reason')))}" if verdict.get("reason") else "")
            + "</p>"
        )
    sim = ops.get("simulation") or {}
    if sim:
        parts.append(
            '<p style="color: var(--text-muted); font-size: 0.8125rem;">'
            "Dispositions are simulated from the datagen ground truth: L1 analyst "
            f"accuracy {sim.get('analyst_accuracy')}, investigator accuracy "
            f"{sim.get('investigator_accuracy')}, QA sample {sim.get('qa_sample_rate')}, "
            f"seed {sim.get('seed')}. As of {escape(str(ops.get('as_of_date')))}.</p>"
        )

    # Invariants: one row per (cycle, invariant) that did not pass, else a
    # one-line all-pass statement per cycle.
    inv_rows = []
    for _job, c, inv in cycles:
        bad = {n: r for n, r in inv.items() if r.get("status") != "pass"}
        if bad:
            for n, r in sorted(bad.items()):
                inv_rows.append(
                    f"<tr><td>{escape(c)}</td><td>{escape(n)}</td>"
                    f'<td><span style="color: var(--danger, red);">{escape(r.get("status", ""))}'
                    f"</span></td><td>{escape(r.get('detail', ''))}</td></tr>"
                )
        else:
            inv_rows.append(
                f"<tr><td>{escape(c)}</td><td>all {len(inv)}</td>"
                '<td><span style="color: var(--success, green);">pass</span></td><td></td></tr>'
            )
    if inv_rows:
        parts.append(
            "<h4>Workflow invariants</h4><table><thead><tr><th>Cycle</th><th>Invariant</th>"
            "<th>Status</th><th>Detail</th></tr></thead><tbody>"
            + "".join(inv_rows)
            + "</tbody></table>"
        )

    rec = ops.get("reconciliation") or {}
    if rec:
        rows = []
        for key in sorted(rec):
            section, _, item = key.partition(".")
            if section in ("completeness", "exclusion", "dq"):
                rows.append(
                    f"<tr><td>{escape(section)}</td><td>{escape(item)}</td>"
                    f"<td>{_fmt_n(rec[key])}</td></tr>"
                )
        parts.append(
            "<h4>Coverage and completeness</h4><table><thead><tr><th>Section</th>"
            "<th>Item</th><th>Payments</th></tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table>"
        )

    funnel = ops.get("funnel") or {}
    if funnel:
        order = ("payments", "monitored", "alerts", "escalated", "cases", "sars")
        parts.append(
            "<h4>Cycle funnel</h4><table><thead><tr>"
            + "".join(f"<th>{k}</th>" for k in order)
            + "</tr></thead><tbody><tr>"
            + "".join(f"<td>{_fmt_n(funnel.get(k))}</td>" for k in order)
            + "</tr></tbody></table>"
            f"<p>L1 escalation rate {_fmt_pct(ops.get('l1_escalation_rate'))}; "
            f"QA disagreement {_fmt_pct(ops.get('qa_disagreement_rate'))} on "
            f"{_fmt_n(ops.get('qa_sample'))} re-reviewed; "
            f"{_fmt_n(ops.get('alerts_out_of_scope'))} alerts on non-customers "
            "(outside the monitored population).</p>"
        )

    aging = ops.get("alert_aging_open") or {}
    if aging or "open_cases" in ops:
        parts.append(
            "<h4>Queue health</h4><table><thead><tr><th>Open alerts 0-30 d</th>"
            "<th>31-60 d</th><th>61-90 d</th><th>90+ d</th><th>Open cases</th>"
            "<th>Open cases &gt; 60 d</th><th>SLA breaches</th></tr></thead><tbody><tr>"
            + "".join(f"<td>{_fmt_n(aging.get(b))}</td>" for b in ("0-30", "31-60", "61-90", "90+"))
            + f"<td>{_fmt_n(ops.get('open_cases'))}</td>"
            f"<td>{_fmt_n(ops.get('open_cases_over_60_days'))}</td>"
            f"<td>{_fmt_n(ops.get('alert_sla_breaches'))}</td></tr></tbody></table>"
        )

    scen = ops.get("scenarios") or {}
    if scen:
        rows = [
            f"<tr><td>{escape(rid)}</td><td>{_fmt_n(v.get('alerts'))}</td>"
            f"<td>{_fmt_n(v.get('escalated'))}</td><td>{_fmt_pct(v.get('productive_rate'))}</td>"
            f"<td>{_fmt_pct(v.get('sar_conversion'))}</td></tr>"
            for rid, v in sorted(scen.items())
        ]
        parts.append(
            "<h4>Scenario performance</h4><table><thead><tr><th>Scenario</th>"
            "<th>Customer alerts</th><th>Escalated</th>"
            '<th title="Share of decided alerts escalated or on a SAR case">Productive rate</th>'
            "<th>SAR conversion</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
        )

    if "sars_filed" in ops:
        parts.append(
            f"<p>SARs filed: <strong>{_fmt_n(ops.get('sars_filed'))}</strong>; "
            f"determination-to-filing median {_fmt_n(ops.get('filing_days_median'))} d, "
            f"p95 {_fmt_n(ops.get('filing_days_p95'))} d, "
            f"{_fmt_pct(ops.get('filed_over_30_days_pct'))} over 30 days; "
            f"continuing-activity reviews due: {_fmt_n(ops.get('continuing_reviews_due'))} "
            f"(opened {_fmt_n(ops.get('continuing_reviews_opened'))}, folded into an open "
            f"investigation {_fmt_n(ops.get('continuing_reviews_folded'))}, waiting on a case "
            f"pending filing {_fmt_n(ops.get('continuing_reviews_deferred'))}, covered by "
            f"the SAR that case filed {_fmt_n(ops.get('continuing_reviews_superseded'))}).</p>"
        )
        limits = ops.get("sars_by_limit") or {}
        if limits:
            rows = "".join(
                f"<tr><td>{escape(str(k))}</td><td>{_fmt_n(v.get('filed'))}</td>"
                f"<td>{_fmt_n(v.get('late'))}</td></tr>"
                for k, v in sorted(limits.items())
            )
            parts.append(
                "<table><thead><tr><th>Filing limit</th><th>SARs filed</th><th>Filed late</th>"
                "</tr></thead><tbody>" + rows + "</tbody></table>"
                f"<p>Continuing-activity SARs filed more than 120 days after the prior SAR: "
                f"{_fmt_n(ops.get('continuing_sars_over_120_days'))}.</p>"
            )
    parts.append("</section>")
    return "\n".join(parts)


_REGISTRY: dict[str, ScorecardBlock] = {
    Customer360ScorecardBlock.schema_name: Customer360ScorecardBlock(),
    FinancialScorecardBlock.schema_name: FinancialScorecardBlock(),
}


def register_scorecard_block(block: ScorecardBlock) -> None:
    """Register a ScorecardBlock, overwriting any existing entry for its schema."""
    _REGISTRY[block.schema_name] = block


def get_scorecard_block(schema_name: str | None) -> ScorecardBlock:
    """Return the ScorecardBlock for a schema.

    Unknown or missing schema names fall back to Customer 360 so older
    metrics.json files (which have no ``workload_schema`` in their
    config_snapshot) still render.
    """
    if not schema_name:
        return _REGISTRY["customer360"]
    return _REGISTRY.get(schema_name, _REGISTRY["customer360"])
