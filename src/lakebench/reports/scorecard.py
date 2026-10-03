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
    """Scorecard block for the Customer 360 medallion pipeline: the
    expected-results checks (``record.c360_correctness``), failures first."""

    schema_name = "customer360"
    domain_label = "Customer360"

    # metrics/c360_correctness.py check kinds, grouped as the report shows them.
    FAMILIES: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("pipeline", ("invariant", "reconcile")),
        ("benchmark shapes", ("shape",)),
        ("statistical", ("statistical",)),
    )

    def render_detail_html(self, metrics: PipelineMetrics) -> str:
        from collections.abc import Mapping

        record = getattr(metrics, "c360_correctness", None) if metrics is not None else None
        if not isinstance(record, Mapping):
            return ""
        record = dict(record)
        try:
            return self._render(record)
        except Exception as exc:  # noqa: BLE001 -- render must never crash the report
            from html import escape

            logger.exception("C360 results could not be rendered")
            first = (str(exc).splitlines() or [""])[0][:200]
            return (
                '<section class="c360-render-error" style="border-left: 3px solid var(--danger);">'
                "<h3>Expected results (Customer 360)</h3>"
                '<p style="color: var(--danger);">C360 results could not be rendered: '
                f"{escape(type(exc).__name__)}: {escape(first)}</p></section>"
            )

    @staticmethod
    def judged_gating_ids(record: dict) -> set[str]:
        """The GATING_CHECKS ids the verdict's c360 gate judges on *record*:
        the rule of ``c360_correctness.gating_outcome`` (continuous
        reporting-only records judge none; benchmark shapes are judged only
        when the record holds shape checks). A test drops each id from every
        stored record and asserts gating_outcome fails exactly for these, so
        a change to that rule fails here rather than drifting."""
        from lakebench.metrics import c360_correctness as cc

        if not cc.GATING_CHECKS or (
            record.get("reporting_only") is True and record.get("mode") == "continuous"
        ):
            return set()
        gated = cc._gated_ids(None)
        checks = [c for c in record.get("checks") or [] if isinstance(c, dict)]
        if any(str(c.get("id", "")).startswith("benchmark_rows_") for c in checks):
            gated |= cc._gated_ids(("benchmark_rows_",))
        return gated

    @classmethod
    def _family(cls, kind: str) -> str:
        for name, kinds in cls.FAMILIES:
            if kind in kinds:
                return name
        return "other"

    def _render(self, record: dict) -> str:
        import json
        from html import escape

        from lakebench.metrics import c360_correctness as cc
        from lakebench.reports import derived as dv

        checks = [c for c in record.get("checks") or [] if isinstance(c, dict)]

        # The gate exactly as the verdict's c360 gate applies it today
        # (c360_correctness.gating_outcome over GATING_CHECKS), never the
        # record's stored gating flag, which predates the owner's approval
        # on older records.
        outcome, why = cc.gating_outcome(record)
        gated = self.judged_gating_ids(record)
        present = {str(c.get("id")) for c in checks}
        absent = sorted(g for g in gated if g not in present)

        if not cc.GATING_CHECKS:
            gate_text, colour = "reporting only", "warning"
        elif outcome == "FAIL":
            gate_text, colour = f"fails the run: {why}", "danger"
        else:
            n_gated = len(gated)  # GATING_CHECKS, not a record count
            gate_text = f"{n_gated} gating checks passed" if gated else "reporting only"
            colour = "success"
        n_passed = len([c for c in checks if c.get("status") == "pass"])
        raw = record.get("checks") or []
        # The span counts the record's list; a list holding non-check
        # entries is shown plain, since the block counts checks only.
        if checks and len(raw) == len(checks):
            total_html = dv.count(len(checks), path="c360_correctness.checks")
        else:
            total_html = str(len(checks))
        chip = (
            f'<span class="c360-chip" style="color: var(--{colour}); font-weight: 600;">'
            f"{n_passed}/{total_html} checks passed; gate: {escape(gate_text)}</span>"
        )
        notes = []
        if record.get("reason"):
            notes.append(str(record["reason"]))
        if record.get("note"):
            notes.append(f"Recorded with the run: {record['note']}")
        reason_html = "".join(
            f'<p style="color: var(--text-muted); font-size: 0.8125rem;">{escape(n)}</p>'
            for n in notes
        )
        parts = ["<section><h3>Expected results (Customer 360)</h3>", f"<p>{chip}</p>", reason_html]
        if not checks:
            parts.append("<p>No check ran.</p></section>")
            return "\n".join(p for p in parts if p)

        def _v(v) -> str:
            if isinstance(v, (dict, list)):
                return escape(json.dumps(v, sort_keys=True, default=str))
            return escape(str(v))

        def _row(c: dict, *, with_status: bool) -> str:
            cid = str(c.get("id"))
            tag = " <small>(gating)</small>" if cid in gated else ""
            state = f"<td>{escape(str(c.get('status')))}</td>" if with_status else ""
            detail = f"<br><small>{escape(str(c['detail']))}</small>" if c.get("detail") else ""
            return (
                f"<tr><td><code class='mono'>{escape(cid)}</code>{tag}{detail}</td>"
                f"<td>{escape(str(c.get('kind')))}</td>{state}"
                f"<td>{_v(c.get('observed'))}</td><td>{_v(c.get('expected'))}</td>"
                f"<td>{_v(c.get('tolerance'))}</td></tr>"
            )

        # Gated checks absent from the record fail the run ("not evaluated").
        not_passed = [c for c in checks if c.get("status") != "pass"] + [
            {
                "id": gid,
                "kind": "-",
                "status": "not evaluated",
                "observed": "-",
                "expected": "-",
                "tolerance": "-",
                "detail": "a gating check absent from the record",
            }
            for gid in absent
        ]
        order = {name: i for i, (name, _k) in enumerate(self.FAMILIES)}
        # Checks that fail the run first, then other failures, then the rest.
        not_passed.sort(
            key=lambda c: (
                0 if str(c.get("id")) in gated else 1,
                0 if c.get("status") == "fail" else 1,
                order.get(self._family(str(c.get("kind"))), 99),
            )
        )
        head = "<th>Check</th><th>Kind</th>{status}<th>Observed</th><th>Expected</th><th>Tolerance</th>"
        if not_passed:
            parts.append(
                "<h4>Not passed</h4><table><thead><tr>"
                + head.format(status="<th>Status</th>")
                + "</tr></thead><tbody>"
                + "".join(_row(c, with_status=True) for c in not_passed)
                + "</tbody></table>"
            )
        known = {k for _n, kinds in self.FAMILIES for k in kinds}
        groups = [(name, kinds) for name, kinds in self.FAMILIES]
        for name, kinds in groups + [("other", ())]:
            group = [
                c
                for c in checks
                if c.get("status") == "pass"
                and (c.get("kind") in kinds if kinds else c.get("kind") not in known)
            ]
            if not group:
                continue
            parts.append(
                f"<details><summary>{escape(name)} checks that passed</summary>"
                "<table><thead><tr>"
                + head.format(status="")
                + "</tr></thead><tbody>"
                + "".join(_row(c, with_status=False) for c in group)
                + "</tbody></table></details>"
            )
        parts.append("</section>")
        return "\n".join(p for p in parts if p)


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
        # Continuous (covered mode): recall over the instances the last
        # completed tick covered, with coverage beside it; never "recall".
        covered_by_typology: dict[str, tuple[int, dict]] = {}
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
            covered = scoring.get("covered") if scoring.get("mode") == "covered" else None
            for ci, t in enumerate((covered or {}).get("typologies") or []):
                if isinstance(t, dict) and t.get("typology_type"):
                    covered_by_typology[t["typology_type"]] = (ci, t)
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
            elif typ is not None and typ in covered_by_typology:
                alerts_cell = _alerts(rule, alerts)
                ci, crow = covered_by_typology[typ]
                base = dv.path("financial_scoring", "covered", "typologies", ci)
                if crow.get("recall_covered") is None:
                    recall_cell = "n/a (no covered instance)"
                    status = "ran"
                else:
                    recall_cell = (
                        dv.pct(float(crow["recall_covered"]), num_path=f"{base}.recall_covered")
                        + " covered, coverage "
                        + (
                            dv.pct(float(crow["coverage"]), num_path=f"{base}.coverage")
                            if crow.get("coverage") is not None
                            else "n/a"
                        )
                    )
                    status = '<span style="color: var(--success, green);">scored (covered)</span>'
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
            typ_cell = typ if typ is not None else "-"
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
                "not counted), not gold.alerts rows." + _continuous_recall_note(scoring) + "</div>"
            )
        if total_alerts is not None:
            fp_str = (
                dv.pct(float(fp_rate), num_path="financial_scoring.fp_rate")
                if fp_rate is not None
                else "n/a"
            )
            from lakebench.reports.formatter import format_measurement

            # The totals cover only the rules that ran. A rule skipped on a
            # Lakebench cap (the verdict's rule_caps) bounds them: labelled.
            cap_lines = [
                f"rule {r} skipped: {why} (Lakebench cap)"
                for r, why in _rule_caps(metrics, rules_skipped)
            ]
            if rules_skipped:
                scope = (
                    " over the rules that ran ("
                    + ", ".join(f"{r} skipped: {why}" for r, why in sorted(rules_skipped.items()))
                    + ")"
                )
            elif continuous_run:
                scope = " over the rules continuous mode runs"
            else:
                scope = ""
            total_html = format_measurement(f"{total_alerts:,}", caps_bound=cap_lines)
            fp_html = (
                fp_str + format_measurement("", caps_bound=cap_lines)
                if cap_lines and fp_rate is not None
                else fp_str
            )
            footer += (
                '<div style="margin-top: 0.5rem; color: var(--text-muted); '
                'font-size: 0.8125rem;">'
                f"Total alerts{escape(scope)}: <strong>{total_html}</strong> | "
                f"Overall off-target rate{' (covered)' if (scoring or {}).get('mode') == 'covered' else ''} "
                f"(alerts touching no planted txn): <strong>{fp_html}</strong>" + "</div>"
            )

        alerts_th = (
            '<th title="New alert versions measured by time to detect, summed over '
            'gold-refresh ticks">New alert versions</th>'
            if continuous_alerts
            else '<th title="Alerts emitted by this rule">Alerts</th>'
        )
        look_role = registered_look_role(getattr(metrics, "run_id", None))
        sc: dict = scoring if isinstance(scoring, dict) else {}
        covered_mode = sc.get("mode") == "covered"
        if covered_mode and sc.get("status") == "not_scored":
            footer += (
                '<div style="margin-top: 0.5rem; color: var(--warning); font-size: 0.8125rem;">'
                "Recall over covered instances not scored: "
                f"{escape(str(sc.get('reason') or 'no reason recorded'))}</div>"
            )
        if look_role:
            recall_label = (
                f"Recall over covered instances (registered look: {escape(look_role)})"
                if covered_mode
                else f"Recall (registered look: {escape(look_role)})"
            )
            recall_title = (
                "Fraction of planted instances detected by this rule, on the "
                f"registered {escape(look_role)} look that names this run."
            )
        else:
            recall_label = (
                "Recall over covered instances (uncalibrated, in-sample)"
                if covered_mode
                else "Recall (uncalibrated, in-sample)"
            )
            recall_title = (
                "Fraction of planted instances detected by this rule. Uncalibrated and "
                "in-sample: measured on this run's own corpus, which no registered look "
                "names (docs/aml-scoring.md)."
            )
        # In covered mode chance and off-target are over the covered
        # instances too.
        covered_suffix = " (covered)" if covered_mode else ""
        footer += _subject_check_html(scoring)
        footer += _reason_code_html(scoring)
        footer += _leakage_html()
        tm_html = _safe_tm_section(metrics, gold_jobs)
        return (
            _safe_funnel_html(metrics, scoring, gold_jobs, rules_skipped)
            + tm_html
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
                        <th title="Share of random-control instances this rule's alerts touch; recall at or below it is chance">Chance{covered_suffix}</th>
                        <th title="Fraction detected by any rule (includes chance overlap)">Incidental</th>
                        <th title="Share of this rule's alerts that touch none of its target typology's txns. NOT a production ops-queue false-positive rate; see docs/aml-scoring.md.">Off-target{covered_suffix}</th>
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


def _rule_caps(metrics, rules_skipped: dict | None = None) -> list[tuple[str, str]]:
    """(rule, reason) for each AML rule skipped on a Lakebench cap: a skip
    reason naming a cap, from the scorecard's own skip list and the stored
    experiment block, the rule metrics/bounds.py applies to limits.bound."""
    found: dict[str, str] = {}
    try:
        exp = metrics.experiment_block() or {}
    except Exception:  # noqa: BLE001 -- a bad block must not break the render
        exp = {}
    rules = exp.get("rules") if isinstance(exp, dict) else None
    stored = rules.get("skipped") if isinstance(rules, dict) else None
    for source in (rules_skipped, stored):
        if isinstance(source, dict):
            for rule, why in source.items():
                if "cap" in str(why):
                    found.setdefault(str(rule), str(why))
    return sorted(found.items())


def _tm_cap_line(metrics, ops: dict) -> list[str]:
    """The TM per-customer cap as a bound line, when it held alerts back."""
    over = ops.get("alerts_over_capacity")
    if not isinstance(over, (int, float)) or isinstance(over, bool) or over <= 0:
        return []
    return [f"TM max_alerts_per_customer: {int(over):,} alerts over capacity"]


def _reason_code_html(scoring) -> str:
    """Per-reason-code recall and FP (``recall_by_code``, ``fp_by_code``:
    ``{rule: {code: fraction}}``, with ``alerts_by_code`` and
    ``by_code_status`` beside them), or why there is none."""
    from html import escape

    from lakebench.reports import derived as dv

    if not isinstance(scoring, dict):
        return ""
    note_style = 'style="margin-top: 0.5rem; color: var(--text-muted); font-size: 0.8125rem;"'
    status = scoring.get("by_code_status")
    recall = scoring.get("recall_by_code")
    fp = scoring.get("fp_by_code")
    alerts = scoring.get("alerts_by_code")
    recall = recall if isinstance(recall, dict) else {}
    fp = fp if isinstance(fp, dict) else {}
    alerts = alerts if isinstance(alerts, dict) else {}
    rows = []
    for rule in sorted(set(recall) | set(fp)):
        r_entry, f_entry, a_entry = recall.get(rule), fp.get(rule), alerts.get(rule)
        codes_r: dict = r_entry if isinstance(r_entry, dict) else {}
        codes_f: dict = f_entry if isinstance(f_entry, dict) else {}
        codes_a: dict = a_entry if isinstance(a_entry, dict) else {}
        for code in sorted(set(codes_r) | set(codes_f)):

            def cell(table, key, code=code, rule=rule):
                v = table.get(code)
                if v is None:
                    return "-"
                return dv.pct(float(v), num_path=dv.path("financial_scoring", key, rule, code))

            n_alerts = codes_a.get(code)
            note = (
                " <small>(no alert carries this code)</small>"
                if isinstance(n_alerts, (int, float)) and n_alerts == 0
                else ""
            )
            rows.append(
                f"<tr><td>{escape(rule)}</td><td><code class='mono'>{escape(code)}</code>{note}</td>"
                f"<td>{cell(codes_r, 'recall_by_code')}</td><td>{cell(codes_f, 'fp_by_code')}</td></tr>"
            )
    status_html = f"<div {note_style}>Reason codes: {escape(str(status))}</div>" if status else ""
    if not rows:
        return status_html or (
            f"<div {note_style}>Per-reason-code recall and FP: reason codes not recorded "
            "in this run.</div>"
        )
    return (
        "<h4>By reason code</h4><table><thead><tr><th>Rule</th><th>Reason code</th>"
        '<th title="Share of the typology\'s instances hit by an alert of this rule carrying this code">'
        "Recall (uncalibrated, in-sample)</th>"
        '<th title="1 minus the on-target share of this rule\'s alerts carrying this code">FP</th>'
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>" + status_html
    )


def _leakage_html() -> str:
    """No run stage records a leakage result in v1.7: the AML fidelity gate
    (``scripts/aml_gate.py``) runs outside ``lakebench run``, so the block
    says so rather than leave the reader to infer it."""
    return (
        '<div style="margin-top: 0.5rem; color: var(--text-muted); font-size: 0.8125rem;">'
        "Leakage: not measured in this run (the AML fidelity gate, "
        "<code>scripts/aml_gate.py</code>, runs outside <code>lakebench run</code>).</div>"
    )


def _safe_funnel_html(metrics, scoring, gold_jobs: list, rules_skipped: dict) -> str:
    try:
        return _funnel_html(metrics, scoring, gold_jobs, rules_skipped)
    except Exception as exc:  # noqa: BLE001 -- render must never crash the report
        from html import escape

        logger.exception("AML funnel could not be rendered")
        return (
            "<section><h3>AML results funnel</h3>"
            '<p style="color: var(--danger);">The funnel could not be rendered: '
            f"{escape(type(exc).__name__)}</p></section>"
        )


def _funnel_html(metrics, scoring, gold_jobs: list, rules_skipped: dict) -> str:
    """Alerts to SARs, each count with the record path it comes from, nested
    counts shown as "of which", and the identities tm_operations holds
    checked, with the size of any difference."""
    from html import escape

    from lakebench.reports import derived as dv
    from lakebench.reports.formatter import format_measurement

    tm = getattr(metrics, "tm_operations", None)
    ops = tm.get("ops") if isinstance(tm, dict) and isinstance(tm.get("ops"), dict) else None
    base = "tm_operations.ops" if ops else None
    if ops is None:
        all_jobs = list(getattr(metrics, "jobs", None) or [])
        for j in reversed(gold_jobs or []):
            if isinstance(getattr(j, "tm_ops", None), dict) and j.tm_ops:
                ops = j.tm_ops
                base = f"jobs[{next(i for i, x in enumerate(all_jobs) if x is j)}].tm_ops"
                break
    total = scoring.get("total_alerts") if isinstance(scoring, dict) else None
    if ops is None and total is None:
        return ""
    ops_d: dict = ops if isinstance(ops, dict) else {}
    f_raw, r_raw, d_raw = (
        ops_d.get("funnel"),
        ops_d.get("reconciliation"),
        ops_d.get("alerts_by_disposition"),
    )
    funnel: dict = f_raw if isinstance(f_raw, dict) else {}
    rec: dict = r_raw if isinstance(r_raw, dict) else {}
    disp: dict = d_raw if isinstance(d_raw, dict) else {}

    def p(*parts) -> str:
        return f"{base}.{dv.path(*parts)}" if base else ""

    def _int(v):
        return int(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None

    def num(v) -> str:
        n = _int(v)
        return f"{n:,}" if n is not None else "n/a"

    # Every count here comes from the rules that ran: a rule skipped on a
    # Lakebench cap bounds them all (invariant 6).
    rule_caps = [f"rule {r} skipped: {why}" for r, why in _rule_caps(metrics, rules_skipped)]

    def bounded(v, caps) -> str:
        return format_measurement(num(v), caps_bound=caps) if caps else num(v)

    rows: list[tuple[str, str, str]] = []
    if total is not None:
        rows.append(
            ("Rule alerts (scoring)", bounded(total, rule_caps), "financial_scoring.total_alerts")
        )
    if "alerts_total" in ops_d:
        rows.append(
            (
                "Rule alerts in gold.alerts (TM)",
                bounded(ops_d.get("alerts_total"), rule_caps),
                p("alerts_total"),
            )
        )
    if "alerts" in funnel:
        rows.append(
            (
                "dispositioned on customers (monitored)",
                num(funnel.get("alerts")),
                p("funnel", "alerts"),
            )
        )
    if "alerts_over_capacity" in ops_d:
        rows.append(
            (
                "&nbsp;&nbsp;of which over the per-customer cap (held back, not worked)",
                num(ops_d.get("alerts_over_capacity")),
                p("alerts_over_capacity"),
            )
        )
    if "alerts_withdrawn_carried" in ops_d:
        rows.append(
            (
                "&nbsp;&nbsp;of which withdrawn, carried from an earlier cycle",
                num(ops_d.get("alerts_withdrawn_carried")),
                p("alerts_withdrawn_carried"),
            )
        )
    if "alerts_out_of_scope" in ops_d:
        rows.append(
            (
                "dispositioned on non-customers (outside the monitored population)",
                num(ops_d.get("alerts_out_of_scope")),
                p("alerts_out_of_scope"),
            )
        )
    if "alerts_noncustomer_undeclared" in ops_d:
        rows.append(
            (
                "&nbsp;&nbsp;of which not declared as counterparties",
                num(ops_d.get("alerts_noncustomer_undeclared")),
                p("alerts_noncustomer_undeclared"),
            )
        )
    customers = rec.get("completeness.customers")
    if _int(funnel.get("alerts")) is not None and _int(customers):
        per = dv.ratio(
            funnel["alerts"],
            customers,
            a_path=p("funnel", "alerts"),
            b_path=p("reconciliation", "completeness.customers"),
            fmt=".2f",
            suffix="",
        )
        rows.append(
            (
                "customer alerts per customer",
                per,
                f"{p('funnel', 'alerts')} / {p('reconciliation', 'completeness.customers')}",
            )
        )
    for k in sorted(disp):
        rows.append((f"disposition: {escape(str(k))}", num(disp[k]), p("alerts_by_disposition", k)))
    # The per-customer cap held alerts back from the analysts: everything
    # worked after it is bounded by it.
    tm_cap = _tm_cap_line(metrics, ops_d)
    if "escalated" in funnel:
        rows.append(
            ("escalated", bounded(funnel.get("escalated"), tm_cap), p("funnel", "escalated"))
        )
    if "cases" in funnel:
        rows.append(("alert cases", bounded(funnel.get("cases"), tm_cap), p("funnel", "cases")))
    if "funnel.continuing_review_cases" in rec:
        rows.append(
            (
                "continuing-activity review cases",
                num(rec.get("funnel.continuing_review_cases")),
                p("reconciliation", "funnel.continuing_review_cases"),
            )
        )
    if "sars_filed" in ops_d:
        rows.append(("SARs filed", bounded(ops_d.get("sars_filed"), tm_cap), p("sars_filed")))

    # The identities tm_operations holds, checked; a difference is sized,
    # and named as unexplained when the record does not say why.
    checks: list[str] = []
    alerts_tm = _int(ops_d.get("alerts_total"))
    if total is not None and alerts_tm is not None:
        diff = int(total) - alerts_tm
        checks.append(
            "scoring and TM rule alerts agree"
            if diff == 0
            else f"scoring rule alerts differ from TM's by {diff:+,}; the record does not say why"
        )
    cust, non = _int(funnel.get("alerts")), _int(ops_d.get("alerts_out_of_scope"))
    withdrawn = _int(ops_d.get("alerts_withdrawn_carried")) or 0
    if alerts_tm is not None and cust is not None and non is not None:
        current = cust + non - withdrawn
        checks.append(
            "TM rule alerts = customer + non-customer dispositions - withdrawn carried alerts"
            if current == alerts_tm
            else "customer + non-customer dispositions - withdrawn carried alerts differ from "
            f"TM rule alerts by {current - alerts_tm:+,}; the record does not say why"
        )
    if disp and cust is not None and non is not None:
        dsum = sum(_int(v) or 0 for v in disp.values())
        checks.append(
            "dispositions sum to customer + non-customer dispositions"
            if dsum == cust + non
            else f"dispositions sum to {dsum:,}, {dsum - cust - non:+,} against customer + "
            "non-customer dispositions"
        )
    sars, alert_sars = _int(ops_d.get("sars_filed")), _int(funnel.get("sars"))
    cont = _int(rec.get("funnel.continuing_sars"))
    if sars is not None and alert_sars is not None:
        if cont is not None and sars == alert_sars + cont:
            checks.append(
                f"SARs filed = {alert_sars:,} on alert cases + {cont:,} continuing-activity SARs"
            )
        else:
            other = sars - alert_sars - (cont or 0)
            checks.append(
                f"SARs filed differ from alert-case plus continuing-activity SARs by {other:+,}; "
                "the record does not say why"
            )
    body = "".join(
        f"<tr><td>{label}</td><td>{value}</td><td><code class='mono'>{escape(src)}</code></td></tr>"
        for label, value, src in rows
    )
    recon = "".join(f"<li>{escape(c)}</li>" for c in checks)
    cap_note = (
        '<p style="color: var(--text-muted); font-size: 0.8125rem;">Every count here comes '
        "from the rules that ran; " + escape("; ".join(rule_caps)) + ".</p>"
        if rule_caps
        else ""
    )
    return (
        "<section><h3>AML results funnel</h3>"
        "<table><thead><tr><th>Step</th><th>Count</th><th>Source</th></tr></thead>"
        f"<tbody>{body}</tbody></table>{cap_note}"
        + (f"<h4>Reconciliation</h4><ul>{recon}</ul>" if recon else "")
        + "</section>"
    )


def registered_look_role(run_id: str | None) -> str | None:
    """The role of the completed registered look whose run_ids names this
    run (reports/front_matter.py reads the look record)."""
    from lakebench.reports.front_matter import registered_look_role as _role

    return _role(run_id)


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


def _continuous_recall_note(scoring) -> str:
    """The continuous footer's recall sentence. A covered score is
    not the batch recall and is not rendered in the table yet; a covered
    record that holds no score says why."""
    import html

    if not scoring:
        return (
            " Recall is not scored in continuous mode: stopping the streams "
            "can interrupt a detection pass, and scoring refuses a partial one."
        )
    if scoring.get("mode") != "covered":
        return ""
    if scoring.get("status") == "scored":
        return (
            " Recall is scored over the instances the last drained tick covered "
            "(recall_covered in financial_scoring.covered); it is not the batch "
            "recall and the table does not show it."
        )
    return " Recall is not scored: " + html.escape(str(scoring.get("reason") or "no reason")) + "."
