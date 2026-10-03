"""Per-domain scorecard extension points for the report generator.

ENG-2C.5. A ScorecardBlock owns the domain-specific parts of an HTML
scorecard: the display label that appears in the run-context banner
plus (in later revisions) any workload-specific detail rows. The block
for the run's workload schema is looked up via
:func:`get_scorecard_block`, which the report generator calls once per
render.

Only ``domain_label`` is consumed today (the ENG-2C.5 rescope found the
generator already domain-neutral apart from one header). The ``render_detail_html``
hook exists so Financial can slot in Investigator-panel rows and
Customer 360 can move its existing panels off the hardcoded generator
path without another interface change.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from lakebench.metrics import PipelineMetrics

logger = logging.getLogger(__name__)


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

    Renders a per-rule detection table (batch, or continuous from the
    gold-refresh time-to-detect counts): alert count, the
    planted typology each rule targets, recall (from the folded-in
    ``financial score``, LB-123), and a status that reads "not run" for a
    rule the gold-finalize step skipped (e.g. W1 above its vertex cap,
    LB-119) -- never 0%, which would misreport a skip as a miss.
    """

    schema_name = "financial"
    domain_label = "Financial (FinServ-Crime, AML)"

    def render_detail_html(self, metrics: PipelineMetrics) -> str:
        # The report generator must never crash rendering a report, and an
        # AML run whose results cannot be shown must say so: a malformed
        # scoring or detection record renders a visible notice naming the
        # error (the traceback goes to the log), never an empty section.
        try:
            return self._render_detail_html(metrics)
        except Exception as exc:  # noqa: BLE001 -- render must never crash the report
            from html import escape

            logger.exception("AML results could not be rendered")
            first = (str(exc).splitlines() or [""])[0][:200]
            return (
                '<section class="aml-render-error" style="border-left: 3px solid var(--danger);">'
                "<h3>Detection Scorecard</h3>"
                '<p style="color: var(--danger);">AML results could not be rendered: '
                f"{escape(type(exc).__name__)}: {escape(first)}</p></section>"
            )

    def _render_detail_html(self, metrics: PipelineMetrics) -> str:
        if metrics is None:
            return ""
        from html import escape

        from lakebench.reports import derived as dv

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

        # Continuous AML has no gold-finalize job: the per-rule counts come
        # from gold-refresh's time-to-detect histogram (P1.4). They are new
        # alert versions summed over ticks, not gold.alerts rows: an alert
        # whose evidence changes is counted again, and alerts with no
        # silver-matched transaction or in a tick without a TTD line are not
        # counted. The column header and footnote say so.
        continuous_alerts = False
        alerts_paths: dict[str, list[str]] = {}
        if not source_jobs:
            for si, sm in enumerate(getattr(metrics, "streaming", None) or []):
                if getattr(sm, "job_type", "") != "gold-refresh":
                    continue
                for rule, row in (getattr(sm, "ttd_by_rule", None) or {}).items():
                    n = row.get("alerts") if isinstance(row, dict) else None
                    if isinstance(n, (int, float)):
                        alerts_by_rule[rule] = alerts_by_rule.get(rule, 0) + int(n)
                        alerts_paths.setdefault(rule, []).append(
                            dv.path("streaming", si, "ttd_by_rule", rule, "alerts")
                        )
                        continuous_alerts = True

        # Nothing AML-specific to show (e.g. a c360 run mislabelled, or a
        # financial run before detection wired) -- stay silent.
        if not alerts_by_rule and not rules_skipped and not scoring:
            return _safe_tm_section(metrics, gold_jobs)

        try:
            from lakebench.benchmark.aml_queries import RULE_TARGETS
        except Exception:  # noqa: BLE001 -- render must never crash the report
            RULE_TARGETS = {}

        recall_by_typology: dict[str, dict] = {}
        typology_index: dict[str, int] = {}
        total_alerts = None
        fp_rate = None
        fp_by_rule: dict = {}
        chance_by_rule: dict = {}
        txn_prec_by_rule: dict = {}
        if scoring:
            for ti, t in enumerate(scoring.get("typologies", []) or []):
                tt = t.get("typology_type")
                if tt:
                    recall_by_typology[tt] = t
                    typology_index[tt] = ti
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

        from lakebench.config.support import AML_CONTINUOUS_SKIPPED_RULES

        continuous_run = not source_jobs and bool(getattr(metrics, "streaming", None))
        continuous_excluded = frozenset(AML_CONTINUOUS_SKIPPED_RULES)

        # A batch rule with no count ran and emitted nothing; a continuous
        # rule missing from the histogram is unknown, not zero.
        missing_alerts = "-" if continuous_alerts else "0"

        def _rule_pct(table: dict, key: str, rule: str, digits: int = 1) -> str:
            val = table.get(rule)
            if val is None:
                return "-"
            return dv.pct(
                float(val), num_path=dv.path("financial_scoring", key, rule), digits=digits
            )

        def _typ_pct(typ: str, key: str) -> str:
            return dv.pct(
                float(recall_by_typology[typ][key]),
                num_path=dv.path("financial_scoring", "typologies", typology_index[typ], key),
            )

        def _alerts(rule: str, alerts: int | None) -> str:
            if alerts is None:
                return missing_alerts
            if rule in alerts_paths:
                return dv.total(alerts, paths=alerts_paths[rule], fmt=",d")
            return f"{alerts:,}"

        body_rows: list[str] = []
        for rule in rules:
            typ = RULE_TARGETS.get(rule)
            alerts = alerts_by_rule.get(rule)
            incidental_cell = "-"
            fp_cell = _rule_pct(fp_by_rule, "fp_rate_by_rule", rule)
            chance_cell = _rule_pct(chance_by_rule, "chance_by_rule", rule)
            txn_cell = _rule_pct(txn_prec_by_rule, "txn_precision_by_rule", rule, digits=2)
            if continuous_run and rule in continuous_excluded and alerts is None:
                # Continuous mode does not run this rule (config/support.py
                # MODE_NOTES): excluded by design, not missing data.
                status = (
                    '<span style="color: var(--text-muted);">excluded in continuous mode</span>'
                )
                recall_cell = "n/a"
                alerts_cell = "-"
            elif rule in rule_errors:
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
                # A rule with no planted typology to score recall against.
                status = "ran" if alerts is not None or not continuous_alerts else "no data"
                recall_cell = "n/a (attribute)"
                alerts_cell = _alerts(rule, alerts)
            else:
                alerts_cell = _alerts(rule, alerts)
                trow = recall_by_typology.get(typ)
                if trow and trow.get("incidental_recall") is not None:
                    incidental_cell = _typ_pct(typ, "incidental_recall")
                if trow and trow.get("detection_status") == "rule_error":
                    status = '<span style="color: var(--danger, red);">error</span>'
                    recall_cell = "n/a"
                elif trow and trow.get("detection_status") == "partial":
                    status = '<span style="color: var(--warning);">partial</span>'
                    recall_cell = (
                        _typ_pct(typ, "recall") if trow.get("recall") is not None else "n/a"
                    )
                elif trow and trow.get("detection_status") == "rule_skipped":
                    status = '<span style="color: var(--warning);">not run</span>'
                    recall_cell = "n/a"
                elif trow and trow.get("detection_status") == "no_rule":
                    # Scored run, but this rule was never scheduled (no
                    # detection_status row): unknown, not "ran, 0 alerts".
                    status = '<span style="color: var(--warning);">not run</span> (not scheduled)'
                    recall_cell = "n/a"
                    alerts_cell = "-"
                elif trow and trow.get("recall") is not None:
                    status = '<span style="color: var(--success, green);">scored</span>'
                    recall_cell = _typ_pct(typ, "recall")
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
        if continuous_alerts:
            footer += (
                '<div style="margin-top: 0.5rem; color: var(--text-muted); '
                'font-size: 0.8125rem;">'
                "Continuous run: counts are new alert versions measured by time to "
                "detect, summed over gold-refresh ticks (an alert is counted again when "
                "its evidence changes; alerts without a matched silver transaction are "
                "not counted), not gold.alerts rows."
                + (
                    ""
                    if scoring
                    else " Recall is not scored in continuous mode: stopping the streams "
                    "can interrupt a detection pass, and scoring refuses a partial one."
                )
                + "</div>"
            )
        if total_alerts is not None:
            fp_str = (
                dv.pct(float(fp_rate), num_path="financial_scoring.fp_rate")
                if fp_rate is not None
                else "n/a"
            )
            footer += (
                '<div style="margin-top: 0.5rem; color: var(--text-muted); '
                'font-size: 0.8125rem;">'
                f"Total alerts: <strong>{total_alerts:,}</strong> | "
                f"Overall off-target rate (alerts touching no planted txn): <strong>{fp_str}</strong>"
                + "</div>"
            )

        alerts_th = (
            '<th title="New alert versions measured by time to detect, summed over '
            'gold-refresh ticks">New alert versions</th>'
            if continuous_alerts
            else '<th title="Alerts emitted by this rule">Alerts</th>'
        )
        look_role = registered_look_role(getattr(metrics, "run_id", None))
        if look_role:
            recall_label = f"Recall (registered look: {escape(look_role)})"
            recall_title = (
                "Fraction of planted instances detected by this rule, on the "
                f"registered {escape(look_role)} look that names this run."
            )
        else:
            recall_label = "Recall (uncalibrated, in-sample)"
            recall_title = (
                "Fraction of planted instances detected by this rule. Uncalibrated and "
                "in-sample: measured on this run's own corpus, which no registered look "
                "names (docs/aml-scoring.md)."
            )
        footer += _subject_check_html(scoring)
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
                        {alerts_th}
                        <th title="{recall_title}">{recall_label}</th>
                        <th title="Share of random-control instances this rule's alerts touch; recall at or below it is chance">Chance</th>
                        <th title="Fraction detected by any rule (includes chance overlap)">Incidental</th>
                        <th title="Share of this rule's alerts that touch none of its target typology's txns. NOT a production ops-queue false-positive rate; see docs/aml-scoring.md.">Off-target</th>
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


def registered_look_role(run_id: str | None) -> str | None:
    """The role of a completed registered look whose ``run_ids`` names this
    run, read from the look record (never the config); None for every other
    run, and when the record is missing or unreadable."""
    if not run_id:
        return None
    try:
        from lakebench.config.datagen_seed import load_looks

        looks = load_looks()
    except Exception:  # noqa: BLE001 -- no readable look record means no look
        return None
    for entry in looks:
        run_ids = entry.get("run_ids") if isinstance(entry, dict) else None
        # Only a list names runs; anything else names none (fail closed).
        if (
            entry.get("state") == "complete"
            and isinstance(run_ids, list)
            and any(isinstance(r, str) and r == run_id for r in run_ids)
        ):
            return str(entry.get("role"))
    return None


def _subject_check_html(scoring) -> str:
    """The planted-subject customer check (score_financial
    ``subject_customer_check``): whether every planted subject is a customer
    in silver, which customer-scoped recall depends on."""
    from html import escape

    check = (scoring or {}).get("subject_customer_check") if isinstance(scoring, dict) else None
    if not isinstance(check, dict):
        return ""
    status = str(check.get("status") or "unknown")
    colour = "success" if status == "ok" else "danger" if status == "fail" else "warning"
    parts = [
        f"{_fmt_n(check.get('subjects'))} planted subjects",
        f"{_fmt_n(check.get('unmapped'))} with no silver entity",
        f"{_fmt_n(check.get('not_customer'))} not customers",
    ]
    if check.get("reason"):
        parts.append(f"reason: {check.get('reason')}")
    failing = check.get("failing_typologies") or []
    unresolved = check.get("unresolved_typologies") or []
    if failing:
        parts.append("failing: " + ", ".join(str(t) for t in failing))
    if unresolved:
        parts.append("unresolved: " + ", ".join(str(t) for t in unresolved))
    return (
        '<div style="margin-top: 0.5rem; color: var(--text-muted); font-size: 0.8125rem;">'
        f'Subject customer check: <strong style="color: var(--{colour});">'
        f"{escape(status)}</strong> ({escape('; '.join(parts))})</div>"
    )


def continuous_trend_rows(pb, *, delta_limitation: bool) -> list[str]:
    """Table Maintenance rows for a continuous run: the documented Delta
    limitation (owner decision #46) and the in-window QpH trend, so a
    composite median over a declining series is not read as steady state.

    *pb* is the run's PipelineBenchmark (or None). Returns HTML ``<tr>`` rows.
    """
    from html import escape

    from lakebench.metrics.maintenance_policy import DELTA_CONTINUOUS_LIMITATION

    rows: list[str] = []
    if delta_limitation:
        rows.append(
            "<tr><td>Known limitation</td>"
            f'<td style="color: var(--danger)">{escape(DELTA_CONTINUOUS_LIMITATION)}</td></tr>'
        )
    trend = pb.qph_trend() if pb is not None else None
    if not trend:
        return rows
    from lakebench.reports import derived as dv

    # The change between the first and last rounds with a QpH, derived from
    # those two rounds of the record.
    positive = [i for i, r in enumerate(pb.benchmark_rounds) if r.qph > 0]
    change_txt = ""
    if trend.get("change_pct") is not None and positive:
        first_i, last_i = positive[0], positive[-1]
        first_p = dv.path("pipeline_benchmark", "benchmark_rounds", first_i, "qph")
        last_p = dv.path("pipeline_benchmark", "benchmark_rounds", last_i, "qph")
        first = pb.benchmark_rounds[first_i].qph
        last = pb.benchmark_rounds[last_i].qph
        change_html = dv.pct(
            last - first,
            first,
            num_path=[last_p, dv.product(-1, first_p)],
            den_path=first_p,
            signed=True,
        )
        change_txt = f" ({change_html})"
    rows.append(
        "<tr><td>In-window QpH trend</td>"
        f"<td>first round {trend['first_round_qph']:.1f}, last round "
        f"{trend['last_round_qph']:.1f}{change_txt} over "
        + dv.count(
            trend["rounds"], path="pipeline_benchmark.benchmark_rounds[*].qph", where="positive"
        )
        + " rounds</td></tr>"
    )
    if "silver_data_files_start" in trend:
        rows.append(
            "<tr><td>Silver data files</td>"
            f"<td>{trend['silver_data_files_start']:,} at the first probed round, "
            f"{trend['silver_data_files_end']:,} at the last</td></tr>"
        )
    else:
        rows.append(
            "<tr><td>Silver data files</td>"
            f"<td>{escape(str(trend.get('silver_data_files_unavailable')))}</td></tr>"
        )
    return rows


def _fmt_pct(v, p: str | None = None) -> str:
    """A stored fraction as a percentage; a derived span when its record
    path *p* is known."""
    if v is None:
        return "n/a"
    if p is None:
        # Called without the record (a direct unit call): no path to name.
        return f"{float(v) * 100:.1f}%"
    from lakebench.reports import derived as dv

    return dv.pct(float(v), num_path=p)


def _p(base: str | None, *parts: str) -> str | None:
    """*parts* under the ops block's record path, or None when unknown."""
    if base is None:
        return None
    from lakebench.reports import derived as dv

    tail = dv.path(*parts)
    return f"{base}{tail}" if tail.startswith("[") else f"{base}.{tail}"


def _fmt_n(v) -> str:
    return f"{int(v):,}" if isinstance(v, (int, float)) else "n/a"


def _safe_tm_section(metrics, gold_jobs: list) -> str:
    """The TM section in its own guard: a malformed ``tm_operations`` or
    ``tm_ops`` blanks this section only, never the detection table."""
    try:
        all_jobs = list(getattr(metrics, "jobs", None) or [])
        job_index = {id(j): i for i, j in enumerate(all_jobs)}
        job_paths = [f"jobs[{job_index[id(j)]}]" for j in gold_jobs or []]
        return _render_tm_operations(
            gold_jobs, getattr(metrics, "tm_operations", None), job_paths=job_paths
        )
    except Exception:  # noqa: BLE001 -- render must never crash the report
        return (
            "<section><h3>Transaction Monitoring Operations</h3>"
            "<p>The operations summary in this run's metrics could not be rendered.</p></section>"
        )


def _render_tm_operations(
    gold_jobs: list, verdict: dict | None = None, *, job_paths: list[str] | None = None
) -> str:
    """TM operations pack (GOALS P10.3, core sections) from the last
    gold-finalize job's ``tm_ops`` summary, plus every cycle's invariants.

    Operations vocabulary (P10.4): "productive rate" (escalated or SAR), not
    precision. Every number here is conditional on the simulated analyst,
    which the section states with the accuracies used.
    """
    from html import escape

    verdict = verdict if isinstance(verdict, dict) else {}
    ops = verdict.get("ops") if isinstance(verdict.get("ops"), dict) else None
    ops_path: str | None = "tm_operations.ops" if ops else None
    cycles: list[tuple[str, str, dict, str | None]] = []
    if not ops:
        paths = job_paths or []
        for k in range(len(gold_jobs or []) - 1, -1, -1):
            j = gold_jobs[k]
            if getattr(j, "tm_ops", None):
                ops = j.tm_ops
                ops_path = f"{paths[k]}.tm_ops" if k < len(paths) else None
                break
    if verdict.get("invariants"):
        # Run-level record: batch (merged over cycles) or continuous (one
        # entry per operations pass).
        for c, inv in sorted(verdict["invariants"].items(), key=lambda kv: int(kv[0])):
            cycles.append(("", str(c), inv, _p("tm_operations", "invariants", str(c))))
    else:
        paths = job_paths or []
        for idx, j in enumerate(gold_jobs or [], start=1):
            base = f"{paths[idx - 1]}.tm_invariants" if idx - 1 < len(paths) else None
            for c, inv in sorted((getattr(j, "tm_invariants", None) or {}).items()):
                cycles.append((str(idx), c, inv, _p(base, str(c))))
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
    from lakebench.reports import derived as dv

    for _job, c, inv, inv_path in cycles:
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
                f"<tr><td>{escape(c)}</td><td>all "
                + (dv.count(len(inv), path=inv_path) if inv_path else str(len(inv)))
                + "</td>"
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
            f"<p>L1 escalation rate "
            f"{_fmt_pct(ops.get('l1_escalation_rate'), _p(ops_path, 'l1_escalation_rate'))}; "
            f"QA disagreement "
            f"{_fmt_pct(ops.get('qa_disagreement_rate'), _p(ops_path, 'qa_disagreement_rate'))} on "
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
            f"<td>{_fmt_n(v.get('escalated'))}</td>"
            f"<td>{_fmt_pct(v.get('productive_rate'), _p(ops_path, 'scenarios', rid, 'productive_rate'))}</td>"
            f"<td>{_fmt_pct(v.get('sar_conversion'), _p(ops_path, 'scenarios', rid, 'sar_conversion'))}</td></tr>"
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
            f"{_fmt_pct(ops.get('filed_over_30_days_pct'), _p(ops_path, 'filed_over_30_days_pct'))} over 30 days; "
            f"continuing-activity reviews due: {_fmt_n(ops.get('continuing_reviews_due'))} "
            f"(opened {_fmt_n(ops.get('continuing_reviews_opened'))}, folded into an open "
            f"investigation {_fmt_n(ops.get('continuing_reviews_folded'))}, waiting on a case "
            f"pending filing {_fmt_n(ops.get('continuing_reviews_deferred'))}, covered by "
            f"the SAR that case filed {_fmt_n(ops.get('continuing_reviews_superseded'))}).</p>"
        )
        limits = ops.get("sars_by_limit") or {}
        if limits:
            limit_rows = "".join(
                f"<tr><td>{escape(str(k))}</td><td>{_fmt_n(v.get('filed'))}</td>"
                f"<td>{_fmt_n(v.get('late'))}</td></tr>"
                for k, v in sorted(limits.items())
            )
            parts.append(
                "<table><thead><tr><th>Filing limit</th><th>SARs filed</th><th>Filed late</th>"
                "</tr></thead><tbody>" + limit_rows + "</tbody></table>"
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
