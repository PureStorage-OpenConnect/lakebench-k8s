"""Report generation for Lakebench.

The *delivered* HTML report is written into the per-run directory managed by
:class:`MetricsStorage`, once, at the end of a benchmark run::

    lakebench-output/runs/run-<id>/report.html

That file is the artifact operators reference and share; it is never
rewritten in place by a later ``lakebench report`` invocation. Regenerating
the HTML from saved metrics writes a fresh file at::

    lakebench-output/reports/report-<run_id>-<UTC-timestamp>.html

To rewrite the delivered ``report.html`` (or any other pre-existing path),
the caller must both name the target explicitly via ``output_path`` and pass
``force=True``. The ``output_dir`` constructor argument is accepted for
backward compatibility and only shifts the default scratch directory.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from html import escape as _html_escape
from pathlib import Path
from typing import Any

from lakebench.metrics import MetricsStorage, PipelineMetrics
from lakebench.metrics.maintenance_policy import LEGACY_MAINTENANCE_POLICY_ID
from lakebench.metrics.metric_registry import direction_hint

logger = logging.getLogger(__name__)

# Subdirectory (relative to ``metrics_dir``'s parent) where the timestamped
# rendered reports land when no explicit ``output_path`` is supplied.
_DEFAULT_REPORTS_SUBDIR = "reports"


def _format_duration_ms(ms: float | None) -> str:
    """Format a duration in milliseconds for display.

    Returns seconds when >= 1000ms, milliseconds otherwise, or '-' for zero/None.
    """
    if ms is None:
        return "-"
    if ms >= 1000:
        return f"{ms / 1000:.1f}s"
    elif ms > 0:
        return f"{ms:.0f}ms"
    return "-"


# Owner decision D-6 (AML-GOALS #46): continuous Delta ships with no effective
# table maintenance in v1.6 (VACUUM keeps the 7 d default while streams are
# live and OPTIMIZE is skipped), and the report says so.
DELTA_CONTINUOUS_NO_MAINTENANCE = "no effective table maintenance in continuous mode (v1.6)"


def _post_qph_caveat(pb) -> str:
    """Why post-maintenance QpH is not a clean measurement, or ""."""
    if pb is None:
        return ""
    parts = []
    if getattr(pb, "maintenance_stopped", False):
        parts.append(
            "maintenance stopped before completion ("
            + (getattr(pb, "maintenance_stop_reason", "") or "unknown")
            + "); a statement may still have been running"
        )
    if getattr(pb, "maintenance_live_streams", False):
        parts.append(
            "streams were live during maintenance ("
            + (getattr(pb, "maintenance_live_streams_reason", "") or "unknown")
            + "); the benchmark ran with writers active"
        )
    return "; ".join(parts)


_STAGE_TO_JOB_TYPES = {
    "bronze": ("bronze-verify", "bronze-ingest"),
    "silver": ("silver-build", "silver-stream"),
    "gold": ("gold-finalize", "gold-refresh"),
}


def _stage_matches_cap(stage_name: str, caps_bound: list[str]) -> bool:
    """Whether any cap in *caps_bound* names *stage_name*'s job type.

    The bound lines carry the job type (``bronze-verify: executor cap 28``);
    a stage row only gets a BOUNDED tag when a cap that bound that stage
    is in the list, so a gold stage does not wear a bronze cap's tag.
    """
    if not caps_bound:
        return False
    job_types = _STAGE_TO_JOB_TYPES.get(stage_name, ())
    for cap in caps_bound:
        text = str(cap)
        for jt in job_types:
            if jt in text:
                return True
    return False


_ARROWS = {"higher is better": "&#8593; ", "lower is better": "&#8595; "}


def _direction_hint(key: str, mode: str, detail: str = "") -> str:
    """A card's "<arrow> higher is better | <detail>" line, the direction
    from the metric registry; the detail alone when the metric has no
    better side."""
    hint = direction_hint(key, mode)
    text = f"{_ARROWS[hint]}{hint}" if hint else ""
    if detail:
        return f"{text} | {detail}" if text else detail
    return text


def _qph_stop_warning(metrics) -> str:
    """Warning on a headline QpH measured after a stopped maintenance or with
    streams live."""
    caveat = _post_qph_caveat(getattr(metrics, "pipeline_benchmark", None))
    if not caveat:
        return ""
    from html import escape

    return (
        ' <span class="qph-stop-warning" style="color: var(--danger); font-size: 0.5em;" '
        f'title="{escape(caveat)}">'
        f"WARNING: {escape(caveat)}, so this is not a clean measurement</span>"
    )


# Continuous jobs whose executor count the concurrent budget can cap at
# submit (recorded in experiment.limits).
_STREAMING_JOBS = frozenset({"bronze-ingest", "silver-stream", "gold-refresh"})


def _executor_rows(config: dict[str, Any]) -> list[tuple[str, Any]]:
    """One row per job in the snapshot's job profiles: what its manifest asked for,
    before any cluster-dependent cap (the continuous concurrent budget)."""
    inputs = config.get("fingerprint_inputs")
    profiles = inputs.get("job_profiles") if isinstance(inputs, dict) else None
    if not isinstance(profiles, dict):
        return []
    rows: list[tuple[str, Any]] = []
    for job, p in profiles.items():
        if not isinstance(p, dict):
            continue
        rows.append(
            (
                f"Executors, {_html_escape(str(job))} (job profile)",
                _html_escape(
                    f"{p.get('executor_instances')} x {p.get('executor_cores')} cores, "
                    f"{p.get('executor_memory')} + {p.get('executor_memory_overhead')} overhead"
                    + (" before the concurrent budget" if str(job) in _STREAMING_JOBS else "")
                ),
            )
        )
    return rows


class ReportGenerator:
    """Generates HTML benchmark reports."""

    def __init__(
        self,
        metrics_dir: Path | str | None = None,
        output_dir: Path | str | None = None,
    ):
        """Initialize report generator.

        Args:
            metrics_dir: Directory containing metrics (runs parent).
                         Defaults to MetricsStorage default.
            output_dir:  Fallback output directory when the per-run
                         directory is not available.  Accepted for
                         backward compatibility.
        """
        if metrics_dir is not None:
            self.storage = MetricsStorage(metrics_dir)
        else:
            self.storage = MetricsStorage()
        self._fallback_output_dir = Path(output_dir) if output_dir else None

    def _default_reports_dir(self) -> Path:
        """Directory where timestamped scratch renders land by default.

        Prefers the ``output_dir`` passed to the constructor (legacy) and
        falls back to ``<metrics_dir>/../reports/``.
        """
        if self._fallback_output_dir is not None:
            return self._fallback_output_dir
        return self.storage.metrics_dir.parent / _DEFAULT_REPORTS_SUBDIR

    def _timestamped_report_path(self, run_id: str) -> Path:
        """Compute the default timestamped scratch path for *run_id*.

        Timestamp is UTC ``YYYYMMDD-HHMMSS``. Second-granularity: two
        renders inside the same wall-clock second land on the same path
        and the second refuses unless ``force=True`` (see the module
        docstring).
        """
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        return self._default_reports_dir() / f"report-{run_id}-{ts}.html"

    def generate_report(
        self,
        run_id: str | None = None,
        output_path: Path | str | None = None,
        *,
        force: bool = False,
        deployment_name: str | None = None,
    ) -> Path:
        """Generate an HTML report.

        Args:
            run_id: Run ID to report on (default: latest).
            output_path: Where to write the HTML.
                ``None`` (default) writes to a timestamped scratch file at
                ``<metrics_dir>/../reports/report-<run_id>-<ts>.html``; the
                delivered ``<run_dir>/report.html`` is never touched under
                this default. To rewrite the delivered artifact the caller
                must name that path explicitly and pass ``force=True``.
            force: If True, overwrite an existing file at the target path.
                Without ``force``, an existing target raises
                ``FileExistsError`` so the delivered artifact cannot be
                mutated by mistake.
            deployment_name: When ``run_id`` is not supplied, scope the
                "latest run" lookup to this deployment so a parallel
                deployment's newer run is not rendered by mistake. ``None``
                keeps the unscoped behaviour for callers that do not know
                the deployment (SP-2 owns the deployment_id follow-up).

        Returns:
            Path to the written HTML file.

        Raises:
            ValueError: run not found or no runs saved.
            FileExistsError: target path already exists and ``force`` is False.
        """
        # Load metrics
        if run_id:
            metrics = self.storage.load_run(run_id)
            if not metrics:
                raise ValueError(f"Run not found: {run_id}")
        else:
            metrics = self.storage.get_latest_run_for_deployment(deployment_name)
            if not metrics:
                raise ValueError("No runs found")

        # Resolve target path. Without an explicit override the caller is
        # asking for a fresh timestamped scratch render, so the delivered
        # per-run ``report.html`` is not a candidate.
        if output_path is None:
            target = self._timestamped_report_path(metrics.run_id)
        else:
            target = Path(output_path)

        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists() and not force:
            raise FileExistsError(
                f"Refusing to overwrite existing report at {target}. "
                "Pass force=True (CLI: --force) to overwrite."
            )

        # Generate HTML (pass platform_metrics if present in the run data)
        html = self._generate_html(metrics, platform_metrics=metrics.platform_metrics)

        target.write_text(html)
        logger.info(f"Generated report: {target}")

        return target

    def _compute_overall_status(
        self, metrics: PipelineMetrics
    ) -> tuple[bool, list[str], list[str]]:
        """Compute whether the run should be considered a pass.

        Returns (passed, fail_reasons, warnings) where:
        - passed: True if the run's verdict is PASSED (v1.6) or the record
          has no verdict block and ``success`` is True (legacy v1.5)
        - fail_reasons: list of failure messages
        - warnings: list of anomaly messages (run passed but has caveats)

        The badge share the fail-reason and warning rules with
        ``metrics.verdict.compute_verdict`` via ``compute_badge_status``.
        The pass/fail bit itself is taken from the run's verdict when one
        was computed (OD-6: ``success == (verdict.status == "PASSED")``),
        so the badge and the wired readers agree on the same outcome.
        """
        from lakebench.metrics.verdict import compute_badge_status, compute_verdict

        _, reasons, warnings = compute_badge_status(metrics)

        # Take the pass/fail bit from the run's verdict so the badge
        # agrees with every other reader wired in A2b (CLI list, perf
        # gate, compare all read verdict.status). Reasons stay from the
        # badge helper so the tooltip still names what went wrong.
        # Warnings are amber-badge only and never flip the pass/fail bit.
        verdict = compute_verdict(metrics)
        passed = verdict.status == "PASSED"

        # If the verdict is FAILED but the badge helper found no reason,
        # surface the verdict reasons so the tooltip is not empty. This
        # only happens when a gate outside the badge rules (for example
        # ``c360``) failed the verdict.
        if not passed and not reasons:
            reasons = list(verdict.reasons) or ["Verdict FAILED"]

        return (passed, reasons, warnings)

    def _is_sustained(self, metrics: PipelineMetrics) -> bool:
        """Return True if this run used the sustained/streaming pipeline.

        Accepts both ``"sustained"`` (current) and ``"continuous"`` (legacy
        metrics.json files) for backward compatibility.
        """
        if metrics.pipeline_benchmark:
            return metrics.pipeline_benchmark.pipeline_mode in ("sustained", "continuous")
        return bool(metrics.streaming)

    @staticmethod
    def _bound_with_cap_names(bound: list[str]) -> str:
        """Bound rows with the cap name in [brackets] next to each entry.

        The tooltip reader in the HTML report is code inside a <code> tag,
        so the cap name goes as plain text ([_MAX_EXECUTORS_SAFE=28],
        [w1_max_vertices], ...) rather than a span; the reader sees both
        the reason and the constant name in one row.
        """
        from lakebench.reports.formatter import _cap_short_name

        return "; ".join(f"{b} [{_cap_short_name(b)}]" for b in bound)

    def _generate_read_first_panel(
        self,
        metrics: PipelineMetrics,
        *,
        passed: bool,
        warnings: list[str],
        fail_reasons: list[str],
        n_runs: int,
    ) -> str:
        """Dense header panel: verdict, headline, corpus label, n, limits count, digest.

        Small and dense so it sits above the identity strip without pushing
        the score cards below the fold. Its five fields carry what a reader
        should know before reading a single card: whether the run passed,
        why in one sentence, the corpus it ran against, the number of
        independent samples, how many Lakebench caps were in effect during
        the run, and the record's identity digest for cross-referencing.
        """
        from lakebench.reports.formatter import caps_bound_from

        e = _html_escape
        exp = metrics.experiment_block() or {}
        corpus = exp.get("corpus") or {}
        corpus_role = corpus.get("corpus_role") or "none"
        corpus_id = corpus.get("id") or "unknown"
        scale = corpus.get("scale")
        corpus_label = f"{corpus_role} (id {corpus_id[:12]}, scale {scale})"

        from lakebench.reports.formatter import trickle_caps_from

        caps_bound = caps_bound_from(metrics)
        # The trickle counts once, whether the stored block lists it or not.
        n_limits = len(caps_bound) + len(trickle_caps_from(metrics))

        if passed and not warnings:
            verdict_word = "PASSED"
            verdict_color = "var(--success)"
        elif passed and warnings:
            verdict_word = "WARNING"
            verdict_color = "var(--warning)"
        else:
            verdict_word = "FAILED"
            verdict_color = "var(--danger)"

        if fail_reasons:
            headline = fail_reasons[0]
        elif warnings:
            headline = warnings[0]
        else:
            mode = "sustained" if self._is_sustained(metrics) else "batch"
            headline = f"{mode} pipeline completed, all gates passed"

        # Identity digest: a short hash of the experiment identity so two
        # runs of the same experiment print the same digest.
        try:
            from lakebench.metrics.experiment import identity_hash

            record_digest = identity_hash(exp) if exp else "unresolved"
        except Exception:  # noqa: BLE001
            record_digest = "unresolved"

        return f"""
        <section class="read-first" style="padding: 1rem 1.25rem; margin-bottom: 1rem; border-left: 3px solid {verdict_color};">
            <div style="font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--text-muted); margin-bottom: 0.5rem;">
                Read this first
            </div>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 0.5rem 1.5rem; font-size: 0.85rem;">
                <div><span class="read-first-key" style="color: var(--text-muted);">Verdict:</span> <strong style="color: {verdict_color};">{verdict_word}</strong></div>
                <div><span class="read-first-key" style="color: var(--text-muted);">Headline:</span> {e(headline)}</div>
                <div><span class="read-first-key" style="color: var(--text-muted);">Corpus:</span> <code class="mono">{e(corpus_label)}</code></div>
                <div><span class="read-first-key" style="color: var(--text-muted);">n:</span> {int(n_runs)}</div>
                <div><span class="read-first-key" style="color: var(--text-muted);">Limits present:</span> {n_limits}</div>
                <div><span class="read-first-key" style="color: var(--text-muted);">Record digest:</span> <code class="mono">{e(record_digest)}</code></div>
            </div>
        </section>
        """

    def _generate_run_context(self, metrics: PipelineMetrics) -> str:
        """Generate a one-line run context banner below the header."""
        from lakebench.reports.scorecard import get_scorecard_block

        cs = metrics.config_snapshot or {}
        mode = "Continuous" if self._is_sustained(metrics) else "Batch"
        scale = cs.get("scale", "-")
        catalog = cs.get("catalog", "-")
        table_fmt = cs.get("table_format", "-")
        pipe_engine = cs.get("pipeline_engine", "spark")
        engine = cs.get("query_engine", "-")
        storage = cs.get("storage_backend", "")
        duration_m = int(metrics.total_elapsed_seconds // 60)
        duration_s = int(metrics.total_elapsed_seconds % 60)

        storage_segment = f" | {storage}" if storage else ""
        domain_label = get_scorecard_block(cs.get("workload_schema")).domain_label

        return f"""
        <div style="margin-bottom: 1.5rem; color: var(--text-muted); font-size: 0.875rem;">
            <strong>{mode}</strong> pipeline |
            {domain_label} at scale {scale} |
            {catalog}-{table_fmt}-{pipe_engine}-{engine}{storage_segment} |
            {duration_m}m {duration_s}s
        </div>
        """

    def _generate_sustained_detail_cards(self, pb) -> str:
        """Generate detail cards for Pipeline Stages section (sustained mode only).

        Shows Ingest Ratio below the stage table.  Compute Efficiency and
        Total CPU-hours are now in the Layer 1 summary cards.
        """
        # Ingest ratio badge -- None means the denominator (datagen row count)
        # was not measured. Report as N/A rather than fabricating a status.
        if pb.ingest_ratio is None:
            ratio_badge = '<span style="color: var(--text-muted);">N/A</span>'
            ratio_value = "N/A"
        elif (
            pb.ingest_ratio < 0.95
            and pb.intake_limit == "trickle_rate"
            and pb.pipeline_saturated is False
        ):
            note = _html_escape(pb.trickle_note() or "", quote=True)
            ratio_badge = (
                f'<span style="color: var(--warning);" title="{note}">'
                "Held to trickle rate (not saturated)</span>"
            )
            ratio_value = f"{pb.ingest_ratio:.2f}"
        elif (
            pb.ingest_ratio < 0.95
            and pb.intake_limit == "trickle_rate"
            and pb.pipeline_saturated is None
        ):
            ratio_badge = (
                '<span style="color: var(--warning);">Held to trickle rate '
                "(silver pace unmeasured)</span>"
            )
            ratio_value = f"{pb.ingest_ratio:.2f}"
        elif pb.ingest_ratio < 0.95:
            ratio_badge = '<span style="color: var(--danger);">SATURATED</span>'
            ratio_value = f"{pb.ingest_ratio:.2f}"
        elif pb.ingest_ratio > 1.05:
            ratio_badge = (
                f'<span style="color: var(--warning);"'
                f' title="Ingest ratio above 1.0 means total rows processed exceeds'
                f" rows generated. Expected when gold refreshes multiple times,"
                f' re-reading the full silver table each cycle.">'
                f"{pb.ingest_ratio:.2f} (gold re-reads exceed input)</span>"
            )
            ratio_value = f"{pb.ingest_ratio:.2f}"
        else:
            ratio_badge = '<span style="color: var(--success);">Healthy</span>'
            ratio_value = f"{pb.ingest_ratio:.2f}"

        return f"""
        <div class="cards" style="margin-top: 1.5rem;">
            <div class="card">
                <div class="card-label">Ingest Ratio</div>
                <div class="card-value">{ratio_value}</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    {ratio_badge}
                </div>
                <div class="card-hint">bronze rows / datagen rows</div>
            </div>
        </div>
        """

    # ------------------------------------------------------------------
    # Layer 2: Diagnosis sections
    # ------------------------------------------------------------------

    _STAGE_COLORS = {
        "datagen": "#94a3b8",
        "bronze": "#f59e0b",
        "silver": "#6366f1",
        "gold": "#eab308",
        "query": "#06b6d4",
    }

    def _generate_bottleneck_section(self, metrics: PipelineMetrics) -> str:
        """Generate bottleneck identification section (Layer 2).

        Shows time and compute distribution across stages as a stacked bar
        with a text call-out identifying the dominant stage.
        """
        pb = metrics.pipeline_benchmark
        if not pb or not pb.stages:
            return ""

        is_sustained = self._is_sustained(metrics)

        # For sustained mode, use latency_ms as the weight (stages run
        # concurrently so elapsed times are similar).  For batch, use
        # elapsed_seconds.
        #
        # Trino CPU: the query stage has executor_count/cores == 0 because
        # it runs on Trino, not Spark.  Pull Trino pod CPU from the config
        # snapshot so the chart reflects actual cluster resource usage.
        cs = metrics.config_snapshot or {}
        trino_cfg = cs.get("trino", {})
        trino_total_cores = float(trino_cfg.get("coordinator", {}).get("cpu", 0)) + int(
            trino_cfg.get("worker", {}).get("replicas", 0)
        ) * float(trino_cfg.get("worker", {}).get("cpu", 0))

        stage_data = []
        for s in pb.stages:
            if s.stage_name in ("datagen",) and is_sustained:
                continue
            if s.stage_name == "query" and trino_total_cores > 0:
                cpu_sec = trino_total_cores * s.elapsed_seconds
            else:
                cpu_sec = s.executor_count * s.executor_cores * s.elapsed_seconds
            weight = (
                s.latency_ms
                if (is_sustained and s.latency_ms is not None and s.latency_ms > 0)
                else s.elapsed_seconds
            )
            stage_data.append(
                {
                    "name": s.stage_name,
                    "weight": weight,
                    "cpu_sec": cpu_sec,
                    "elapsed": s.elapsed_seconds,
                }
            )

        if not stage_data:
            return ""

        total_weight = sum(d["weight"] for d in stage_data) or 1.0
        total_cpu = sum(d["cpu_sec"] for d in stage_data) or 1.0

        for d in stage_data:
            d["weight_pct"] = d["weight"] / total_weight * 100
            d["cpu_pct"] = d["cpu_sec"] / total_cpu * 100

        # Identify bottleneck -- in sustained mode latency is the primary
        # dimension (stages run concurrently); in batch mode use compute.
        if is_sustained:
            dominant = max(stage_data, key=lambda d: d["weight_pct"])
            sorted_by = sorted(stage_data, key=lambda d: d["weight_pct"], reverse=True)
            dim_label = "latency"
        else:
            dominant = max(stage_data, key=lambda d: d["cpu_pct"])
            sorted_by = sorted(stage_data, key=lambda d: d["cpu_pct"], reverse=True)
            dim_label = "compute"

        dom_pct = dominant["weight_pct"] if is_sustained else dominant["cpu_pct"]
        if (
            len(sorted_by) >= 2
            and abs(
                sorted_by[0]["weight_pct" if is_sustained else "cpu_pct"]
                - sorted_by[1]["weight_pct" if is_sustained else "cpu_pct"]
            )
            < 10
        ):
            callout = f"{'Latency' if is_sustained else 'Compute'} is balanced across stages (no single bottleneck)."
        elif dom_pct >= 50:
            callout = (
                f"{dominant['name'].capitalize()} consumed {dominant['weight_pct']:.0f}% "
                f"of {'latency' if is_sustained else 'wall-clock time'} and "
                f"{dominant['cpu_pct']:.0f}% of compute."
            )
        else:
            callout = (
                f"{dominant['name'].capitalize()} is the largest stage at "
                f"{dom_pct:.0f}% of {dim_label}."
            )

        # Stacked bar -- always shows CPU share (the resource dimension
        # common to all stages including Trino query).
        bar_segments = []
        for d in stage_data:
            color = self._STAGE_COLORS.get(d["name"], "#94a3b8")
            bar_segments.append(
                f'<div style="flex: {d["cpu_pct"]:.2f}; background: {color}; '
                f'height: 28px; min-width: 0;" '
                f'title="{d["name"]}: {d["cpu_pct"]:.1f}% CPU"></div>'
            )

        legend_items = []
        for d in stage_data:
            color = self._STAGE_COLORS.get(d["name"], "#94a3b8")
            legend_items.append(
                f'<span style="display: inline-flex; align-items: center; margin-right: 1rem;">'
                f'<span style="width: 12px; height: 12px; background: {color}; '
                f'border-radius: 2px; margin-right: 0.3rem; display: inline-block;"></span>'
                f"{d['name']} ({d['cpu_pct']:.0f}%)</span>"
            )

        weight_col = "Latency" if is_sustained else "Time (s)"
        table_rows = ""
        for d in stage_data:
            if is_sustained:
                weight_str = _format_duration_ms(d["weight"])
            else:
                weight_str = f"{d['weight']:,.0f}s"
            table_rows += (
                f"<tr><td>{d['name']}</td>"
                f"<td>{weight_str}</td>"
                f"<td>{d['weight_pct']:.1f}%</td>"
                f"<td>{d['cpu_sec']:,.0f}</td>"
                f"<td>{d['cpu_pct']:.1f}%</td></tr>"
            )
        bar_html = "".join(bar_segments)
        legend_html = "".join(legend_items)

        return f"""
        <section>
            <h2>Bottleneck Identification</h2>
            <p style="margin-bottom: 1rem; color: var(--text-muted); font-style: italic;">
                {callout}
            </p>
            <div style="display: flex; width: 100%; border-radius: 4px; overflow: hidden; margin-bottom: 0.75rem;">
                {bar_html}
            </div>
            <div style="font-size: 0.75rem; color: var(--text-muted); margin-bottom: 1rem;">
                {legend_html}
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Stage</th>
                        <th>{weight_col}</th>
                        <th>% of Total</th>
                        <th>CPU-sec</th>
                        <th>% of CPU</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </section>
        """

    def _generate_maintenance_section(self, metrics: PipelineMetrics) -> str:
        """Generate table maintenance scoring section.

        Always states the maintenance policy the run was measured under, and
        for a continuous Delta run that it had no effective maintenance.
        """
        from html import escape as _pol_esc

        pb = metrics.pipeline_benchmark
        policy_rows = [
            "<tr><td>Maintenance policy</td>"
            f'<td><code class="mono">{_pol_esc(metrics.maintenance_policy_id)}</code></td></tr>'
        ]
        cs = metrics.config_snapshot or {}
        # The statement describes this policy; earlier continuous Delta runs
        # vacuumed at the requested retention (m1-legacy).
        if (
            self._is_sustained(metrics)
            and cs.get("table_format") == "delta"
            and metrics.maintenance_policy_id != LEGACY_MAINTENANCE_POLICY_ID
        ):
            policy_rows.append(
                f"<tr><td>Continuous maintenance</td><td>{DELTA_CONTINUOUS_NO_MAINTENANCE}</td></tr>"
            )
        if self._is_sustained(metrics):
            from lakebench.reports.scorecard import continuous_trend_rows

            policy_rows.extend(
                continuous_trend_rows(
                    pb,
                    delta_limitation=cs.get("table_format") == "delta"
                    and metrics.maintenance_policy_id != LEGACY_MAINTENANCE_POLICY_ID,
                )
            )
        if not pb:
            return self._maintenance_table(policy_rows)

        maint_elapsed = pb.maintenance_elapsed_seconds
        pre_files = pb.pre_compaction_file_count
        post_files = pb.post_compaction_file_count
        pre_qph = pb.pre_compaction_qph
        post_qph = pb.post_compaction_qph
        value_pct = pb.maintenance_value_pct

        # Only the policy to show if maintenance didn't run
        if maint_elapsed == 0 and pre_files == 0:
            return self._maintenance_table(policy_rows)

        rows = list(policy_rows)
        if pre_files > 0:
            rows.append(f"<tr><td>Files before</td><td>{pre_files:,}</td></tr>")
        if post_files > 0:
            rows.append(f"<tr><td>Files after</td><td>{post_files:,}</td></tr>")
        if pre_files > 0 and post_files > 0:
            ratio = pre_files / max(post_files, 1)
            rows.append(f"<tr><td>Compaction ratio</td><td>{ratio:.1f}x</td></tr>")
        if maint_elapsed > 0:
            rows.append(f"<tr><td>Maintenance time</td><td>{maint_elapsed:.0f}s</td></tr>")
        if pb.maintenance_stopped:
            from html import escape as _ms_esc

            # Holds for the headline QpH too: it was measured after this.
            rows.append(
                "<tr><td>Maintenance outcome</td>"
                '<td style="color: var(--danger); font-weight: 600">stopped before '
                f"completion ({_ms_esc(pb.maintenance_stop_reason)}); QpH measured after "
                "maintenance may include a statement still running</td></tr>"
            )
        if pb.maintenance_live_streams:
            from html import escape as _ls_esc

            rows.append(
                "<tr><td>Streams during maintenance</td>"
                '<td style="color: var(--danger); font-weight: 600">live '
                f"({_ls_esc(pb.maintenance_live_streams_reason)}); QpH measured after "
                "maintenance ran with writers active</td></tr>"
            )
        if pre_qph > 0:
            rows.append(f"<tr><td>Pre-compaction QpH</td><td>{pre_qph:.1f}</td></tr>")
        if post_qph > 0:
            caveat = _post_qph_caveat(pb)
            if caveat:
                from html import escape as _stop_esc

                rows.append(
                    f"<tr><td>Post-compaction QpH</td><td>{post_qph:.1f} "
                    '<span style="color: var(--danger); font-weight: 600">'
                    f"(warning: {_stop_esc(caveat)}, so this is not a clean "
                    "measurement)</span></td></tr>"
                )
            else:
                rows.append(f"<tr><td>Post-compaction QpH</td><td>{post_qph:.1f}</td></tr>")
        if value_pct is not None and pre_qph > 0:
            color = "var(--success)" if value_pct > 0 else "var(--danger)"
            rows.append(
                f"<tr><td>QpH improvement</td>"
                f'<td style="color: {color}; font-weight: 600">{value_pct:+.1f}%</td></tr>'
            )
        elif pre_qph > 0 and pb.maintenance_value_reason:
            from html import escape

            rows.append(
                "<tr><td>QpH improvement</td>"
                f"<td>not reported ({escape(pb.maintenance_value_reason)})</td></tr>"
            )
        settle_s = pb.maintenance_settle_seconds
        if settle_s is not None:
            from html import escape as _esc

            detail = pb.maintenance_settle or {}
            probes = detail.get("probes") or []
            times = ", ".join(
                "fail" if p.get("seconds") is None else f"{p['seconds']:.1f}s" for p in probes
            )
            if pb.maintenance_settled and pb.maintenance_settle_verified is False:
                state = (
                    f"probes stable after {settle_s:.0f}s (unverified: no "
                    "pre-maintenance time to compare against)"
                )
            elif pb.maintenance_settled:
                state = f"settled after {settle_s:.0f}s"
            elif pb.maintenance_settle_capped:
                state = f"did not settle within {float(detail.get('max_seconds', settle_s)):.0f}s"
            else:
                state = f"did not settle ({detail.get('reason') or 'unknown'})"
            rows.append(
                "<tr><td>Storage settle wait</td>"
                f"<td>{_esc(state)}; not counted in time to value</td></tr>"
            )
            if probes:
                rows.append(
                    f"<tr><td>Settle probes ({_esc(str(detail.get('probe_query', '')))})</td>"
                    f"<td>{_esc(times)}</td></tr>"
                )

        return self._maintenance_table(rows)

    @staticmethod
    def _maintenance_table(rows: list[str]) -> str:
        if not rows:
            return ""
        table_rows = "\n                    ".join(rows)
        return f"""
        <section>
            <h2>Table Maintenance</h2>
            <table>
                <thead>
                    <tr><th>Metric</th><th>Value</th></tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </section>
        """

    def _generate_data_validity_section(self, metrics: PipelineMetrics) -> str:
        """Generate data validity panel (Layer 2).

        Green/red indicators showing whether the run's numbers can be
        trusted for cross-run comparison.
        """
        pb = metrics.pipeline_benchmark
        is_sustained = self._is_sustained(metrics)

        indicators: list[tuple[str, str, str]] = []  # (label, status_class, value)

        # Scale Ratio / Ingest Ratio
        if is_sustained and pb:
            ratio = pb.ingest_ratio
            if ratio is None:
                indicators.append(("Ingest Ratio", "status-warning", "N/A unmeasured"))
            elif (
                ratio < 0.95
                and pb.intake_limit == "trickle_rate"
                and pb.pipeline_saturated is False
            ):
                indicators.append(
                    ("Ingest Ratio", "status-warning", f"{ratio:.2f} held to trickle rate")
                )
            elif (
                ratio < 0.95 and pb.intake_limit == "trickle_rate" and pb.pipeline_saturated is None
            ):
                indicators.append(
                    ("Ingest Ratio", "status-warning", f"{ratio:.2f} trickle, silver unmeasured")
                )
            elif ratio < 0.95:
                indicators.append(("Ingest Ratio", "status-failed", f"{ratio:.2f} SATURATED"))
            elif ratio > 1.05:
                indicators.append(
                    (
                        "Ingest Ratio",
                        "status-warning",
                        f'{ratio:.2f} <span title="Gold re-reads inflate total rows'
                        f' above datagen output">(gold re-reads exceed input)</span>',
                    )
                )
            else:
                indicators.append(("Ingest Ratio", "status-success", f"{ratio:.2f} Healthy"))
        elif pb:
            ratio = pb.scale_ratio
            if not ratio:
                # 0 means the bronze size was not measured, not a full corpus.
                indicators.append(("Scale Ratio", "status-warning", "Unmeasured"))
            elif ratio < 0.95:
                indicators.append(("Scale Ratio", "status-failed", f"{ratio:.1%} INCOMPLETE"))
            else:
                indicators.append(("Scale Ratio", "status-success", f"{ratio:.1%} Complete"))

        # Job success
        if is_sustained and metrics.streaming:
            total = len(metrics.streaming)
            passed = sum(1 for s in metrics.streaming if s.success)
            cls = "status-success" if passed == total else "status-failed"
            indicators.append(("Streaming Jobs", cls, f"{passed}/{total} passed"))
        elif metrics.jobs:
            total = len(metrics.jobs)
            passed = sum(1 for j in metrics.jobs if j.success)
            cls = "status-success" if passed == total else "status-failed"
            indicators.append(("Batch Jobs", cls, f"{passed}/{total} passed"))

        # Failed queries
        if metrics.benchmark_error:
            indicators.append(("Benchmark", "status-failed", "Did not complete, no QpH"))
        if metrics.benchmark:
            failed = sum(
                1
                for q in (metrics.benchmark.queries or [])
                if isinstance(q, dict) and not q.get("success", True)
            )
            cls = "status-success" if failed == 0 else "status-failed"
            indicators.append(("Query Failures", cls, f"{failed} failed"))

        # Benchmark rounds validity
        if pb and pb.benchmark_rounds:
            n_rounds = len(pb.benchmark_rounds)
            if n_rounds < 5:
                indicators.append(
                    (
                        "Benchmark Rounds",
                        "status-warning",
                        f"{n_rounds} rounds completed (minimum 5 for trend analysis)",
                    )
                )
            else:
                indicators.append(
                    (
                        "Benchmark Rounds",
                        "status-success",
                        f"{n_rounds} rounds completed",
                    )
                )

        if not indicators:
            return ""

        indicator_html = []
        for label, cls, value in indicators:
            # status-warning uses the warning color
            if cls == "status-warning":
                style = "background: #fef3c7; color: var(--warning);"
            elif cls == "status-success":
                style = "background: #dcfce7; color: var(--success);"
            else:
                style = "background: #fecaca; color: var(--danger);"

            indicator_html.append(
                f'<div style="display: flex; align-items: center; gap: 0.5rem; '
                f'padding: 0.5rem 1rem; border-radius: 0.375rem; {style}">'
                f"<strong>{label}:</strong> {value}</div>"
            )

        return f"""
        <section>
            <h2>Data Validity</h2>
            <div style="display: flex; flex-wrap: wrap; gap: 0.75rem;">
                {"".join(indicator_html)}
            </div>
        </section>
        """

    # ------------------------------------------------------------------
    # Layer 2: Stability & Contention (sustained mode only)
    # ------------------------------------------------------------------

    def _render_line_chart_svg(
        self,
        series1: list[float],
        series2: list[float],
        label1: str,
        label2: str,
        color1: str = "#2563eb",
        color2: str = "#d97706",
        width: int = 700,
        height: int = 200,
    ) -> str:
        """Render a dual-axis line chart as inline SVG.

        Args:
            series1: Values for left Y axis
            series2: Values for right Y axis
            label1: Label for series 1
            label2: Label for series 2
            color1: Color for series 1
            color2: Color for series 2
            width: SVG width in pixels
            height: SVG height in pixels

        Returns:
            SVG string (no JS dependencies)
        """
        if len(series1) < 2:
            return ""

        margin_l, margin_r, margin_t, margin_b = 60, 60, 20, 40
        plot_w = width - margin_l - margin_r
        plot_h = height - margin_t - margin_b
        n = len(series1)

        def _scale(values: list[float]) -> tuple[float, float]:
            lo = min(values) if values else 0
            hi = max(values) if values else 1
            if lo == hi:
                lo -= 1
                hi += 1
            return lo, hi

        min1, max1 = _scale(series1)
        min2, max2 = _scale(series2) if series2 else (0, 1)

        def _points(values: list[float], vmin: float, vmax: float, x_off: int) -> str:
            pts = []
            span = vmax - vmin or 1
            for i, v in enumerate(values):
                x = x_off + (i / max(n - 1, 1)) * plot_w
                y = margin_t + plot_h - ((v - vmin) / span) * plot_h
                pts.append(f"{x:.1f},{y:.1f}")
            return " ".join(pts)

        pts1 = _points(series1, min1, max1, margin_l)
        pts2 = _points(series2, min2, max2, margin_l) if series2 else ""

        # Y-axis labels (3 ticks each side)
        y_labels_1 = ""
        y_labels_2 = ""
        for i in range(3):
            frac = i / 2
            y = margin_t + plot_h - frac * plot_h
            v1 = min1 + frac * (max1 - min1)
            y_labels_1 += (
                f'<text x="{margin_l - 8}" y="{y + 4}" text-anchor="end" '
                f'fill="{color1}" font-size="10">{v1:,.0f}</text>'
            )
            if series2:
                v2 = min2 + frac * (max2 - min2)
                y_labels_2 += (
                    f'<text x="{margin_l + plot_w + 8}" y="{y + 4}" text-anchor="start" '
                    f'fill="{color2}" font-size="10">{v2:.1f}</text>'
                )

        # X-axis labels
        x_labels = ""
        for i in range(n):
            x = margin_l + (i / max(n - 1, 1)) * plot_w
            x_labels += (
                f'<text x="{x}" y="{margin_t + plot_h + 18}" '
                f'text-anchor="middle" fill="#64748b" font-size="10">{i}</text>'
            )

        s2_line = ""
        s2_dots = ""
        if series2 and pts2:
            s2_line = f'<polyline points="{pts2}" fill="none" stroke="{color2}" stroke-width="2"/>'
            for i, v in enumerate(series2):
                x = margin_l + (i / max(n - 1, 1)) * plot_w
                span = max2 - min2 or 1
                y = margin_t + plot_h - ((v - min2) / span) * plot_h
                s2_dots += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="{color2}"/>'

        s1_dots = ""
        for i, v in enumerate(series1):
            x = margin_l + (i / max(n - 1, 1)) * plot_w
            span = max1 - min1 or 1
            y = margin_t + plot_h - ((v - min1) / span) * plot_h
            s1_dots += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="{color1}"/>'

        legend = (
            f'<text x="{margin_l}" y="{height - 2}" fill="{color1}" font-size="11">{label1}</text>'
        )
        if series2:
            legend += (
                f'<text x="{margin_l + plot_w}" y="{height - 2}" fill="{color2}" '
                f'font-size="11" text-anchor="end">{label2}</text>'
            )

        return (
            f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" '
            f'style="width: 100%; max-width: {width}px; height: auto;">'
            f'<rect x="{margin_l}" y="{margin_t}" width="{plot_w}" height="{plot_h}" '
            f'fill="none" stroke="#e2e8f0"/>'
            f"{y_labels_1}{y_labels_2}{x_labels}"
            f'<polyline points="{pts1}" fill="none" stroke="{color1}" stroke-width="2"/>'
            f"{s1_dots}"
            f"{s2_line}{s2_dots}"
            f"{legend}"
            f"</svg>"
        )

    def _generate_stability_section(self, metrics: PipelineMetrics) -> str:
        """Generate stability over time section (Layer 2, sustained mode only).

        Shows the QpH trend across in-stream benchmark rounds. The per-round
        gold event-date age is not plotted: it is corpus event time, not
        freshness, and rises with the wall clock whatever the pipeline does.
        """
        if not self._is_sustained(metrics):
            return ""

        pb = metrics.pipeline_benchmark
        if not pb or not pb.benchmark_rounds or len(pb.benchmark_rounds) < 2:
            return ""

        qph_values = [r.qph for r in pb.benchmark_rounds]

        chart = self._render_line_chart_svg(
            series1=qph_values,
            series2=[],
            label1="QpH",
            label2="",
        )

        if not chart:
            return ""

        # Trend analysis -- requires minimum 5 rounds per spec Section 7.5
        if len(qph_values) >= 5:
            first_half = qph_values[: len(qph_values) // 2]
            second_half = qph_values[len(qph_values) // 2 :]
            avg_first = sum(first_half) / len(first_half)
            avg_second = sum(second_half) / len(second_half)
            if avg_second < avg_first * 0.9:
                trend = "QpH is declining over time -- the pipeline may not sustain this load."
            elif avg_second > avg_first * 1.1:
                trend = "QpH is improving over time (system warming up)."
            else:
                trend = "QpH is stable across rounds."
        elif len(qph_values) >= 2:
            trend = "Insufficient data for trend analysis (minimum 5 rounds required)."
        else:
            trend = ""

        trend_html = (
            f'<p style="color: var(--text-muted); font-style: italic; margin-top: 0.75rem;">'
            f"{trend}</p>"
            if trend
            else ""
        )

        return f"""
        <section>
            <h2>Stability Over Time</h2>
            <p style="color: var(--text-muted); font-size: 0.8rem; margin-bottom: 0.75rem;">
                X-axis: round index
            </p>
            {chart}
            {trend_html}
        </section>
        """

    def _generate_contention_section(self, metrics: PipelineMetrics) -> str:
        """Generate Q9 contention map (Layer 2, sustained mode only).

        Shows Q9 contention events across benchmark rounds.
        """
        if not self._is_sustained(metrics):
            return ""

        pb = metrics.pipeline_benchmark
        if not pb or not pb.benchmark_rounds:
            return ""

        rounds_with_meta = [(i, r) for i, r in enumerate(pb.benchmark_rounds) if r.round_meta]

        contention_count = sum(
            1 for _, r in rounds_with_meta if r.round_meta.q9_contention_observed
        )

        if contention_count == 0:
            return ""

        total = len(rounds_with_meta)
        pct = (contention_count / total * 100) if total > 0 else 0

        rows = []
        for idx, r in rounds_with_meta:
            meta = r.round_meta
            if not meta.q9_contention_observed:
                continue
            ts = meta.timestamp or "-"
            freshness = (
                f"{meta.gold_event_age_seconds / 86400:.1f} d"
                if (meta.gold_event_age_seconds or 0) > 0
                else "-"
            )
            if meta.q9_retry_used:
                status = '<span style="color: var(--warning);">retry</span>'
            else:
                status = '<span style="color: var(--danger);">FAIL</span>'
            rows.append(
                f"<tr><td>{idx}</td><td>{ts}</td><td>{status}</td><td>{freshness}</td></tr>"
            )

        return f"""
        <section>
            <h2>Q9 Contention</h2>
            <p style="margin-bottom: 0.75rem;">
                Q9 contention observed in <strong>{contention_count}</strong> of
                {total} rounds ({pct:.0f}%).
            </p>
            <table>
                <thead>
                    <tr>
                        <th>Round</th>
                        <th>Time</th>
                        <th>Q9 Status</th>
                        <th title="Query time minus gold's newest event date: corpus event time, not freshness">Gold event age</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
            <p style="color: var(--text-muted); font-size: 0.75rem; margin-top: 0.75rem; font-style: italic;">
                Gold table was being rewritten during {pct:.0f}% of query rounds.
                Benchmark rounds are offset by half the gold refresh interval to
                minimize overlap.
            </p>
        </section>
        """

    def _generate_html(
        self,
        metrics: PipelineMetrics,
        platform_metrics: dict | None = None,
    ) -> str:
        """Generate HTML content.

        Args:
            metrics: Pipeline metrics
            platform_metrics: Optional platform metrics dict from PlatformCollector

        Returns:
            HTML string
        """
        run_context_html = self._generate_run_context(metrics)
        jobs_html = self._generate_jobs_table(metrics)
        streaming_html = self._generate_streaming_table(metrics)
        queries_html = self._generate_queries_table(metrics)
        benchmark_html = self._generate_benchmark_section(metrics)
        benchmark_rounds_html = self._generate_benchmark_rounds_section(metrics)
        pipeline_bench_html = self._generate_pipeline_benchmark_section(metrics)
        summary_html = self._generate_summary(metrics)
        config_html = self._generate_config_section(metrics) + self._generate_experiment_section(
            metrics
        )
        platform_html = self._generate_platform_section(platform_metrics)
        # Layer 2: Diagnosis
        bottleneck_html = self._generate_bottleneck_section(metrics)
        maintenance_html = self._generate_maintenance_section(metrics)
        validity_html = self._generate_data_validity_section(metrics)
        stability_html = self._generate_stability_section(metrics)
        contention_html = self._generate_contention_section(metrics)
        overall_passed, fail_reasons, warnings = self._compute_overall_status(metrics)
        if overall_passed and not warnings:
            _badge_cls = "status-success"
            _badge_text = "PASSED"
            _badge_tip = "Pipeline completed, data complete (ingest/scale ratio 0.95-1.05), all jobs succeeded, no failed queries"
        elif overall_passed and warnings:
            _badge_cls = "status-warning"
            _badge_text = "WARNING"
            _badge_tip = "; ".join(warnings)
        else:
            _badge_cls = "status-failed"
            _badge_text = "FAILED"
            _badge_tip = "; ".join(fail_reasons)

        # Confidence chip next to the verdict badge: n=1 -> single_run,
        # n>=3 -> replicated_n=N, n>=5 with sub-10% spread -> high.
        from lakebench.reports.formatter import (
            confidence_chip_html,
            n_runs_of,
        )

        # Confidence chip counts INDEPENDENT runs only (invariant 7). Do
        # NOT fall back to qph_samples_of(metrics): that value includes
        # in-stream benchmark rounds within one continuous run, so a
        # sustained run with 5 rounds would render replicated_n=5 and
        # claim replication from within-run variation.
        _n_runs = n_runs_of(metrics) or 1
        _confidence_chip = confidence_chip_html(_n_runs, spread=None)

        # "Read this first" panel + provenance label.
        _read_first_html = self._generate_read_first_panel(
            metrics,
            passed=overall_passed,
            warnings=warnings,
            fail_reasons=fail_reasons,
            n_runs=_n_runs,
        )

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lakebench Scorecard - {metrics.deployment_name}</title>
    <style>
        :root {{
            --primary: #2563eb;
            --success: #16a34a;
            --danger: #dc2626;
            --warning: #d97706;
            --bg: #f8fafc;
            --card-bg: #ffffff;
            --text: #1e293b;
            --text-muted: #64748b;
            --border: #e2e8f0;
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
            padding: 2rem;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}

        header {{
            margin-bottom: 2rem;
            padding-bottom: 1rem;
            border-bottom: 2px solid var(--border);
        }}

        h1 {{
            font-size: 1.875rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }}

        .subtitle {{
            color: var(--text-muted);
            font-size: 0.875rem;
        }}

        .status {{
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }}

        .status-success {{
            background: #dcfce7;
            color: var(--success);
        }}

        .status-failed {{
            background: #fecaca;
            color: var(--danger);
        }}

        .status-warning {{
            background: #fef3c7;
            color: var(--warning);
        }}

        .cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }}

        .card {{
            background: var(--card-bg);
            border-radius: 0.5rem;
            padding: 1.25rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}

        .card-label {{
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            margin-bottom: 0.25rem;
        }}

        .card-value {{
            font-size: 1.5rem;
            font-weight: 700;
        }}

        .card-delta {{
            font-size: 0.75rem;
            margin-top: 0.25rem;
        }}

        .card-hint {{
            font-size: 0.7rem;
            color: var(--text-muted);
            margin-top: 0.25rem;
            font-style: italic;
        }}

        .card-hint2 {{
            font-size: 0.7rem;
            color: var(--text-muted);
            margin-top: 0.125rem;
        }}

        .delta-positive {{
            color: var(--success);
        }}

        .delta-negative {{
            color: var(--danger);
        }}

        section {{
            background: var(--card-bg);
            border-radius: 0.5rem;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}

        section h2 {{
            font-size: 1.125rem;
            font-weight: 600;
            margin-bottom: 1rem;
            padding-bottom: 0.5rem;
            border-bottom: 1px solid var(--border);
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
        }}

        th, td {{
            text-align: left;
            padding: 0.75rem;
            border-bottom: 1px solid var(--border);
        }}

        th {{
            font-weight: 600;
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
        }}

        tr:last-child td {{
            border-bottom: none;
        }}

        .mono {{
            font-family: 'SF Mono', Monaco, Consolas, monospace;
            font-size: 0.875rem;
        }}

        .config-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 1rem;
        }}

        .config-item {{
            display: flex;
            justify-content: space-between;
            padding: 0.5rem 0;
            border-bottom: 1px solid var(--border);
        }}

        .config-key {{
            color: var(--text-muted);
        }}

        footer {{
            text-align: center;
            padding-top: 2rem;
            color: var(--text-muted);
            font-size: 0.75rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="provenance-label" style="font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem;">
            internal benchmark, single-owner recorded
        </div>
        <header>
            <h1>Lakebench Scorecard</h1>
            <div class="subtitle">
                Deployment: <strong>{metrics.deployment_name}</strong> |
                Run ID: <code class="mono">{metrics.run_id}</code> |
                <span class="status {_badge_cls}" title="{_badge_tip}">
                    {_badge_text}
                </span>
                {_confidence_chip}
            </div>
            {"" if overall_passed and not warnings else '<div style="margin-top: 0.5rem; font-size: 0.8rem; color: ' + ("var(--danger)" if not overall_passed else "var(--warning)") + ';">' + "; ".join(fail_reasons or warnings) + "</div>"}
        </header>

        {_read_first_html}

        {run_context_html}

        {summary_html}

        {bottleneck_html}

        {validity_html}

        {maintenance_html}

        {pipeline_bench_html}

        {jobs_html}

        {streaming_html}

        {queries_html}

        {benchmark_html}

        {benchmark_rounds_html}

        {stability_html}


        {contention_html}

        {platform_html}

        {config_html}

        <footer>
            Generated by Lakebench | {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        </footer>
    </div>
</body>
</html>"""

        return html

    def _generate_summary(
        self,
        metrics: PipelineMetrics,
    ) -> str:
        """Generate summary cards HTML.

        Sustained mode shows streaming-specific KPIs.
        Batch mode shows the original batch KPIs.
        """
        if self._is_sustained(metrics):
            return self._generate_sustained_summary(metrics)
        return self._generate_batch_summary(metrics)

    def _generate_sustained_summary(self, metrics: PipelineMetrics) -> str:
        """Generate summary cards for sustained/streaming mode.

        Five primary cards (the scores users compare across runs) plus a
        smaller metadata row with contextual info.
        """
        from lakebench.reports.formatter import (
            caps_bound_from,
            format_measurement,
            support_state_of,
            trickle_caps_from,
        )

        total_time = metrics.total_elapsed_seconds
        pb = metrics.pipeline_benchmark
        throughput = pb.sustained_throughput_rps if pb else 0.0
        data_gb = pb.total_data_processed_gb if pb else 0.0
        efficiency = pb.compute_efficiency_gb_per_core_hour if pb else 0.0
        core_hours = pb.total_core_hours if pb else 0.0

        caps_bound = caps_bound_from(metrics)
        support_state = support_state_of(metrics)

        qph: float | None = None
        qph_note = ""
        qph_n_samples: int | None = None
        if pb and pb.benchmark_rounds:
            round_qphs = [r.qph for r in pb.benchmark_rounds if r.qph > 0]
            if round_qphs:
                import statistics

                qph = statistics.median(round_qphs)
                qph_note = f"median of {len(round_qphs)} rounds"
                qph_n_samples = len(round_qphs)
        elif metrics.benchmark and metrics.benchmark.qph > 0:
            qph = metrics.benchmark.qph
            qph_note = "single benchmark"
            qph_n_samples = 1

        freshness_val: float | None = None
        freshness_hint = "worst-case gold staleness during streaming window"
        if pb and pb.data_freshness_seconds is not None and pb.data_freshness_seconds > 0:
            freshness_val = pb.data_freshness_seconds

        # Format values -- use N/A for unmeasurable metrics
        avg_throughput = pb.pipeline_throughput_gb_per_second if pb else 0.0
        duration_m = int(total_time // 60)
        duration_s = int(total_time % 60)
        freshness_display = f"{freshness_val:.1f}s" if freshness_val is not None else "N/A"
        if freshness_val is None:
            freshness_hint = "insufficient gold cycles to measure"
        qph_raw = f"{qph:,.1f}" if qph is not None else "N/A"
        qph_display = format_measurement(
            qph_raw,
            "",
            caps_bound=caps_bound if qph is not None else None,
            n_runs=qph_n_samples if qph is not None else None,
            support_state=support_state if qph is not None else None,
        )
        if qph is None:
            qph_note = "no benchmark rounds completed"

        throughput_raw = f"{throughput:,.0f} rows/s"
        # The trickle bounds intake: the rows/s, GB/s and efficiency figures
        # are the offered load, not capacity (metrics/bounds.py).
        intake_caps = caps_bound + trickle_caps_from(metrics)
        throughput_display = format_measurement(
            throughput_raw,
            "",
            caps_bound=intake_caps if throughput > 0 else None,
            n_runs=1 if throughput > 0 else None,
            support_state=support_state if throughput > 0 else None,
        )

        # Derived context values for hint line 2
        _cont = "sustained"
        freshness_hint2 = (
            _direction_hint("data_freshness_seconds", _cont, f"{freshness_val / 60:.1f} min lag")
            if freshness_val is not None
            else "< 2 gold refresh cycles completed"
        )
        throughput_hint2 = _direction_hint(
            "sustained_throughput_rps",
            _cont,
            f"{throughput * 3600:,.0f} rows/hr" if throughput > 0 else "",
        )
        qph_hint2 = (
            _direction_hint("in_stream_composite_qph", _cont, f"~{qph / 60:.0f} queries/min")
            if qph is not None
            else "no benchmark rounds completed"
        )
        # Continuous core-hours follow the window length, so they have no
        # better side (metrics/metric_registry.py).
        cpu_hint2 = _direction_hint(
            "total_core_hours",
            _cont,
            f"extrapolates to ~{core_hours * 86400 / total_time:,.0f}/day"
            if total_time > 0
            else "",
        )
        efficiency_hint2 = _direction_hint("compute_efficiency_gb_per_core_hour", _cont)

        return f"""
        <div class="cards">
            <div class="card">
                <div class="card-label">Data Freshness</div>
                <div class="card-value">{freshness_display}</div>
                <div class="card-hint">{freshness_hint}</div>
                <div class="card-hint2">{freshness_hint2}</div>
            </div>
            <div class="card">
                <div class="card-label">Sustained Throughput</div>
                <div class="card-value">{throughput_display}</div>
                <div class="card-hint">rows entering bronze per second</div>
                <div class="card-hint2">{throughput_hint2}</div>
            </div>
            <div class="card">
                <div class="card-label">Compute Efficiency</div>
                <div class="card-value">{format_measurement(f"{efficiency:.2f} GB/core-hr", "", caps_bound=intake_caps if efficiency > 0 else None)}</div>
                <div class="card-hint">GB processed per core-hour requested</div>
                <div class="card-hint2">{efficiency_hint2}</div>
            </div>
            <div class="card">
                <div class="card-label">In-Stream QpH</div>
                <div class="card-value">{qph_display}</div>
                <div class="card-hint">{qph_note or direction_hint("in_stream_composite_qph", "sustained")}</div>
                <div class="card-hint2">{qph_hint2}</div>
            </div>
            <div class="card">
                <div class="card-label">Total CPU-hours</div>
                <div class="card-value">{core_hours:.1f}</div>
                <div class="card-hint">total compute across all stages</div>
                <div class="card-hint2">{cpu_hint2}</div>
            </div>
        </div>
        <div style="display: flex; gap: 2rem; color: var(--text-muted); font-size: 0.8rem; margin-bottom: 1.5rem;">
            <span>Duration: {duration_m}m {duration_s}s</span>
            <span>Data Processed: {data_gb:.2f} GB</span>
            <span>Avg Pipeline Throughput: {format_measurement(f"{avg_throughput:.2f} GB/s", "", caps_bound=intake_caps if avg_throughput > 0 else None)}</span>
        </div>
        """

    def _generate_batch_summary(self, metrics: PipelineMetrics) -> str:
        """Generate summary cards for batch mode.

        Five primary cards (the scores users compare across runs) plus a
        smaller metadata row with contextual info.  When pipeline_benchmark
        is absent (old data), falls back to a simple layout.
        """
        from lakebench.reports.formatter import (
            caps_bound_from,
            format_measurement,
            support_state_of,
        )

        pb = metrics.pipeline_benchmark
        total_time = metrics.total_elapsed_seconds
        job_count = len(metrics.jobs)
        successful = sum(1 for j in metrics.jobs if j.success)
        caps_bound = caps_bound_from(metrics)
        support_state = support_state_of(metrics)

        if not pb:
            # Fallback for old metrics without pipeline_benchmark
            total_input = sum(j.input_size_gb for j in metrics.jobs)
            avg_tp = total_input / total_time if total_time > 0 else 0
            qph_card = self._generate_qph_card(metrics)
            return f"""
            <div class="cards">
                <div class="card">
                    <div class="card-label">Total Time</div>
                    <div class="card-value">{total_time:.1f}s</div>
                </div>
                <div class="card">
                    <div class="card-label">Jobs</div>
                    <div class="card-value">{successful}/{job_count}</div>
                </div>
                <div class="card">
                    <div class="card-label">Data Processed</div>
                    <div class="card-value">{total_input:.2f} GB</div>
                </div>
                <div class="card">
                    <div class="card-label">Avg Throughput</div>
                    <div class="card-value">{avg_tp:.2f} GB/s</div>
                </div>
                {qph_card}
            </div>
            """

        # QpH from pipeline benchmark or standalone benchmark
        qph = pb.query_benchmark.qph if pb.query_benchmark else 0.0
        qph_n: int | None = None
        if qph == 0.0 and metrics.benchmark:
            qph = metrics.benchmark.qph
        # Samples: benchmark iterations if a standalone benchmark ran, else
        # unknown for a pipeline_benchmark record (kept as n=1 as a floor so
        # the user is not told the value is repeated when it may not be).
        if metrics.benchmark is not None:
            qph_n = int(getattr(metrics.benchmark, "iterations", 0) or 0) or 1
        elif qph > 0:
            qph_n = 1

        scale_warning = (
            ' <span style="color: var(--danger);">INCOMPLETE</span>'
            if 0 < pb.scale_ratio < 0.95
            else ""
        )

        ttv = pb.time_to_value_seconds
        ttv_hint2 = _direction_hint(
            "time_to_value_seconds",
            "batch",
            f"{int(ttv // 60)}m {int(ttv % 60)}s" if ttv > 0 else "",
        )
        qph_hint2 = _direction_hint(
            "composite_qph", "batch", f"~{qph / 60:.0f} queries/min" if qph > 0 else ""
        )
        throughput_hint2 = _direction_hint("pipeline_throughput_gb_per_second", "batch")
        efficiency_hint2 = _direction_hint("compute_efficiency_gb_per_core_hour", "batch")
        qph_display = format_measurement(
            f"{qph:,.1f}" if qph > 0 else "N/A",
            "",
            caps_bound=caps_bound if qph > 0 else None,
            n_runs=qph_n if qph > 0 else None,
            support_state=support_state if qph > 0 else None,
        )
        pipeline_throughput_display = format_measurement(
            f"{pb.pipeline_throughput_gb_per_second:.3f} GB/s",
            "",
            caps_bound=caps_bound if pb.pipeline_throughput_gb_per_second > 0 else None,
            n_runs=1 if pb.pipeline_throughput_gb_per_second > 0 else None,
            support_state=support_state if pb.pipeline_throughput_gb_per_second > 0 else None,
        )

        return f"""
        <div class="cards">
            <div class="card">
                <div class="card-label">Time to Value</div>
                <div class="card-value">{ttv:.1f}s</div>
                <div class="card-hint">wall-clock to queryable gold</div>
                <div class="card-hint2">{ttv_hint2}</div>
            </div>
            <div class="card">
                <div class="card-label">Pipeline Throughput</div>
                <div class="card-value">{pipeline_throughput_display}</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    {pb.total_data_processed_gb:.1f} GB total
                </div>
                <div class="card-hint">data volume / wall-clock time</div>
                <div class="card-hint2">{throughput_hint2}</div>
            </div>
            <div class="card">
                <div class="card-label">Compute Efficiency</div>
                <div class="card-value">{pb.compute_efficiency_gb_per_core_hour:.2f} GB/core-hr</div>
                <div class="card-hint">GB processed per core-hour requested</div>
                <div class="card-hint2">{efficiency_hint2}</div>
            </div>
            <div class="card">
                <div class="card-label">QpH</div>
                <div class="card-value">{qph_display}{_qph_stop_warning(metrics)}</div>
                <div class="card-hint">queries per hour -- {direction_hint("composite_qph", "batch")}</div>
                <div class="card-hint2">{qph_hint2}</div>
            </div>
            <div class="card">
                <div class="card-label">Scale Ratio</div>
                <div class="card-value">{pb.scale_ratio:.1%}{scale_warning}</div>
                <div class="card-hint">actual vs expected data volume</div>
                <div class="card-hint2">1.0 = complete</div>
            </div>
        </div>
        <div style="display: flex; gap: 2rem; color: var(--text-muted); font-size: 0.8rem; margin-bottom: 1.5rem;">
            <span>Total Time: {int(total_time // 60)}m {int(total_time % 60)}s</span>
            <span>Jobs: {successful}/{job_count}</span>
        </div>
        """

    def _generate_jobs_table(
        self,
        metrics: PipelineMetrics,
    ) -> str:
        """Generate batch jobs table HTML.

        Returns empty string when no batch jobs exist (sustained mode).
        """
        if not metrics.jobs:
            return ""

        rows = []
        for job in metrics.jobs:
            status_class = "status-success" if job.success else "status-failed"
            status_text = "Passed" if job.success else "Failed"

            execs = f"{job.executor_count}" if job.executor_count > 0 else "-"
            cores = f"{job.executor_cores}" if job.executor_cores > 0 else "-"
            cpu_s = f"{job.cpu_seconds_requested:,.0f}" if job.cpu_seconds_requested > 0 else "-"
            elapsed = f"{job.elapsed_seconds:.1f}s"
            n_fail = len(job.submission_failures)
            if n_fail:
                # The stage waited on operator submission retries; say so
                # rather than let it read as a slower stage.
                elapsed += (
                    f"<br><small>incl. {job.submission_retry_seconds:.0f}s on "
                    f"{n_fail} failed submission{'s' if n_fail != 1 else ''}</small>"
                )

            rows.append(f"""
            <tr>
                <td><code class="mono">{job.job_name}</code></td>
                <td><span class="status {status_class}">{status_text}</span></td>
                <td>{elapsed}</td>
                <td>{job.input_size_gb:.2f} GB</td>
                <td>{job.output_rows:,}</td>
                <td>{job.throughput_gb_per_second:.2f} GB/s</td>
                <td>{execs}</td>
                <td>{cores}</td>
                <td>{cpu_s}</td>
            </tr>
            """)

        return f"""
        <section>
            <h2>Batch Job Performance</h2>
            <table>
                <thead>
                    <tr>
                        <th>Job</th>
                        <th>Status</th>
                        <th>Duration</th>
                        <th>Input</th>
                        <th>Output Rows</th>
                        <th>Throughput</th>
                        <th>Executors</th>
                        <th>Cores</th>
                        <th>CPU-sec</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
        </section>
        """

    def _generate_streaming_table(self, metrics: PipelineMetrics) -> str:
        """Generate streaming pipeline table HTML.

        Args:
            metrics: Pipeline metrics

        Returns:
            HTML string (empty if no streaming metrics recorded)
        """
        from lakebench.reports.formatter import caps_bound_from, format_measurement

        if not metrics.streaming:
            return ""

        # Build stage lookup from pipeline_benchmark for compute columns
        _JOB_TYPE_TO_STAGE = {
            "bronze-ingest": "bronze",
            "silver-stream": "silver",
            "gold-refresh": "gold",
        }
        stage_map: dict[str, object] = {}
        if metrics.pipeline_benchmark and metrics.pipeline_benchmark.stages:
            for st in metrics.pipeline_benchmark.stages:
                stage_map[st.stage_name] = st

        caps_bound = caps_bound_from(metrics)

        rows = []
        total_rows = 0
        total_cpu_sec = 0.0
        total_executors = 0
        total_mem_gb = 0.0
        for s in metrics.streaming:
            status_class = "status-success" if s.success else "status-failed"
            status_text = "Pass" if s.success else "Fail"
            total_rows += s.total_rows_processed

            if s.throughput_rps > 0:
                stage_name = _JOB_TYPE_TO_STAGE.get(s.job_type, "")
                stage_bound = caps_bound if _stage_matches_cap(stage_name, caps_bound) else []
                throughput = format_measurement(
                    f"{s.throughput_rps:,.0f} rows/s",
                    "",
                    caps_bound=stage_bound,
                    n_runs=1,
                )
            else:
                throughput = "-"
            freshness = f"{s.freshness_seconds:.0f}s" if s.freshness_seconds else "-"

            # Compute columns from stage metrics
            stage_name = _JOB_TYPE_TO_STAGE.get(s.job_type, "")
            stage = stage_map.get(stage_name)
            if stage:
                execs = str(stage.executor_count) if stage.executor_count > 0 else "-"
                cores_mem = (
                    f"{stage.executor_cores}c x {stage.executor_memory_gb:.0f}G"
                    if stage.executor_cores > 0
                    else "-"
                )
                cpu_sec = stage.executor_count * stage.executor_cores * s.elapsed_seconds
                cpu_sec_str = f"{cpu_sec:,.0f}"
                total_cpu_sec += cpu_sec
                total_executors += stage.executor_count
                total_mem_gb += stage.executor_count * stage.executor_memory_gb
            else:
                execs = "-"
                cores_mem = "-"
                cpu_sec_str = "-"

            rows.append(f"""
            <tr>
                <td><code class="mono">{s.job_name}</code></td>
                <td><span class="status {status_class}">{status_text}</span></td>
                <td>{s.elapsed_seconds:.0f}s</td>
                <td>{s.total_batches:,}</td>
                <td>{s.total_rows_processed:,}</td>
                <td>{throughput}</td>
                <td>{_format_duration_ms(s.micro_batch_duration_ms)}</td>
                <td>{freshness}</td>
                <td>{execs}</td>
                <td>{cores_mem}</td>
                <td>{cpu_sec_str}</td>
            </tr>
            """)

        cpu_hours = total_cpu_sec / 3600
        compute_summary = ""
        if total_executors > 0:
            compute_summary = (
                f'<div style="margin-top: 0.75rem; color: var(--text-muted); font-size: 0.8rem;">'
                f"Total compute: {total_executors} executors | "
                f"{cpu_hours:.1f} CPU-hours requested | "
                f"{total_mem_gb:.0f} GB memory"
                f"</div>"
            )

        return f"""
        <section>
            <h2>Continuous Pipeline</h2>
            <div style="margin-bottom: 1rem; color: var(--text-muted); font-size: 0.875rem;">
                {len(metrics.streaming)} continuous jobs | {total_rows:,} total rows processed
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Job</th>
                        <th>Status</th>
                        <th>Duration</th>
                        <th>Batches</th>
                        <th>Rows Processed</th>
                        <th>Throughput</th>
                        <th title="Average micro-batch processing time">Avg Batch</th>
                        <th title="Worst-case staleness of stage output">Freshness</th>
                        <th>Executors</th>
                        <th>Cores x Mem</th>
                        <th title="executor_count x cores x elapsed_seconds">CPU-sec</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
            {compute_summary}
        </section>
        """

    def _generate_queries_table(self, metrics: PipelineMetrics) -> str:
        """Generate query performance table HTML.

        Args:
            metrics: Pipeline metrics

        Returns:
            HTML string (empty if no queries recorded)
        """
        if not metrics.queries:
            return ""

        rows = []
        total_time = 0.0
        for q in metrics.queries:
            status_class = "status-success" if q.success else "status-failed"
            status_text = "Pass" if q.success else "Fail"
            total_time += q.elapsed_seconds

            error_html = ""
            if q.error_message:
                error_html = f'<div style="color: var(--danger); font-size: 0.75rem;">{q.error_message[:120]}</div>'

            rows.append(f"""
            <tr>
                <td><code class="mono">{q.query_name}</code></td>
                <td>{q.elapsed_seconds:.2f}s</td>
                <td>{q.rows_returned:,}</td>
                <td><span class="status {status_class}">{status_text}</span>{error_html}</td>
            </tr>
            """)

        successful = sum(1 for q in metrics.queries if q.success)
        total = len(metrics.queries)

        return f"""
        <section>
            <h2>Query Performance</h2>
            <div style="margin-bottom: 1rem; color: var(--text-muted); font-size: 0.875rem;">
                {successful}/{total} queries passed | Total query time: {total_time:.2f}s
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        <th>Duration</th>
                        <th>Rows</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
        </section>
        """

    def _generate_qph_card(
        self,
        metrics: PipelineMetrics,
    ) -> str:
        """Generate QpH summary card if benchmark data exists.

        Args:
            metrics: Pipeline metrics

        Returns:
            HTML string (empty if no benchmark)
        """
        from lakebench.reports.formatter import (
            caps_bound_from,
            format_measurement,
            support_state_of,
        )

        if not metrics.benchmark:
            return ""

        b = metrics.benchmark
        mode_label = b.mode
        if b.streams > 1:
            mode_label += f", {b.streams} streams"
        qph_display = format_measurement(
            f"{b.qph:.1f}",
            "",
            caps_bound=caps_bound_from(metrics),
            n_runs=int(getattr(b, "iterations", 0) or 0) or 1,
            support_state=support_state_of(metrics),
        )

        return f"""
            <div class="card">
                <div class="card-label">QpH ({b.cache})</div>
                <div class="card-value">{qph_display}{_qph_stop_warning(metrics)}</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    {mode_label}, scale {b.scale}
                </div>
            </div>
        """

    def _generate_benchmark_section(
        self,
        metrics: PipelineMetrics,
    ) -> str:
        """Generate benchmark results section HTML.

        Args:
            metrics: Pipeline metrics

        Returns:
            HTML string (empty if no benchmark data)
        """
        if not metrics.benchmark:
            return ""

        b = metrics.benchmark
        queries = b.queries

        rows = []
        for q in queries:
            status_class = "status-success" if q.get("success") else "status-failed"
            status_text = "Pass" if q.get("success") else "Fail"
            name = q.get("name", "")
            display = q.get("display_name", name)
            qclass = q.get("class", "")
            elapsed = q.get("elapsed_seconds", 0)
            row_count = q.get("rows_returned", 0)

            rows.append(f"""
            <tr>
                <td><code class="mono">{name}</code></td>
                <td>{display}</td>
                <td>{qclass}</td>
                <td>{elapsed:.2f}s</td>
                <td>{row_count:,}</td>
                <td><span class="status {status_class}">{status_text}</span></td>
            </tr>
            """)

        passed = sum(1 for q in queries if q.get("success"))
        total = len(queries)
        mode_label = b.mode
        if b.streams > 1:
            mode_label += f", {b.streams} streams"

        # Stream results sub-table (when throughput/composite)
        stream_html = ""
        if b.stream_results:
            stream_rows = []
            for sr in b.stream_results:
                sr_status_class = "status-success" if sr.get("success") else "status-failed"
                sr_status_text = "Pass" if sr.get("success") else "Fail"
                sr_total = sr.get("total_seconds", 0)
                sr_query_count = len(sr.get("queries", []))
                stream_rows.append(f"""
                <tr>
                    <td>Stream {sr.get("stream_id", "?")}</td>
                    <td>{sr_query_count}</td>
                    <td>{sr_total:.1f}s</td>
                    <td><span class="status {sr_status_class}">{sr_status_text}</span></td>
                </tr>
                """)
            stream_html = f"""
            <h3 style="margin-top: 1.5rem;">Stream Results</h3>
            <table>
                <thead>
                    <tr>
                        <th>Stream</th>
                        <th>Queries</th>
                        <th>Duration</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(stream_rows)}
                </tbody>
            </table>
            """

        return f"""
        <section>
            <h2>Trino Query Benchmark</h2>
            <div style="margin-bottom: 1rem; color: var(--text-muted); font-size: 0.875rem;">
                {passed}/{total} queries passed |
                QpH: <strong>{b.qph:.1f}</strong> |
                Mode: {mode_label} ({b.iterations} iter) |
                Cache: {b.cache} |
                Total: {b.total_seconds:.2f}s
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        <th>Description</th>
                        <th>Class</th>
                        <th>Duration</th>
                        <th>Rows</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
            {stream_html}
        </section>
        """

    def _generate_benchmark_rounds_section(
        self,
        metrics: PipelineMetrics,
    ) -> str:
        """Generate in-stream benchmark rounds section HTML.

        Transposed layout: queries as rows, rounds as columns.  This fits
        the viewport and naturally groups the comparison users want --
        "how did this query perform across rounds?"

        Only renders when benchmark_rounds is non-empty (sustained mode).
        Batch reports are unaffected.
        """
        if not metrics.benchmark_rounds:
            return ""

        import statistics

        rounds = metrics.benchmark_rounds

        # Collect query names from the first round
        query_names: list[str] = []
        if rounds[0].queries:
            query_names = [q.get("name", "") for q in rounds[0].queries]

        # Build query_name -> [time_per_round] matrix
        query_times: dict[str, list[float | None]] = {qn: [] for qn in query_names}
        query_success: dict[str, list[bool]] = {qn: [] for qn in query_names}
        qph_values: list[float] = []
        freshness_values: list[float] = []
        contention_count = 0

        for rnd in rounds:
            qph_values.append(rnd.qph)
            meta = rnd.round_meta
            if meta and (meta.gold_event_age_seconds or 0) > 0:
                freshness_values.append(meta.gold_event_age_seconds)
            if meta and meta.q9_contention_observed:
                contention_count += 1

            for qname in query_names:
                matched = next((q for q in rnd.queries if q.get("name") == qname), None)
                if matched:
                    query_times[qname].append(matched.get("elapsed_seconds", 0))
                    query_success[qname].append(matched.get("success", True))
                else:
                    query_times[qname].append(None)
                    query_success[qname].append(True)

        # Sort queries by Round 1 time descending (slowest first)
        sorted_queries = sorted(
            query_names,
            key=lambda q: query_times[q][0] if query_times[q][0] is not None else 0,
            reverse=True,
        )

        # Column headers: Query | Round 1..N | delta
        n_rounds = len(rounds)
        round_headers = "".join(f"<th>Round {i + 1}</th>" for i in range(n_rounds))

        # Query rows
        rows = []
        for qname in sorted_queries:
            times = query_times[qname]
            successes = query_success[qname]
            cells = []
            for i, t in enumerate(times):
                if t is not None:
                    style = ' style="color: var(--danger);"' if not successes[i] else ""
                    cells.append(f"<td{style}>{t:.1f}s</td>")
                else:
                    cells.append("<td>-</td>")

            # Delta: last - first
            first = times[0]
            last = times[-1]
            if first is not None and last is not None and n_rounds > 1:
                delta = last - first
                sign = "+" if delta >= 0 else ""
                color = (
                    "var(--danger)"
                    if delta > 0.5
                    else "var(--success)"
                    if delta < -0.5
                    else "var(--text-muted)"
                )
                delta_cell = f'<td style="color: {color};">{sign}{delta:.1f}s</td>'
            else:
                delta_cell = "<td>-</td>"

            rows.append(
                f"<tr><td><code class='mono'>{qname}</code></td>{''.join(cells)}{delta_cell}</tr>"
            )

        # QpH summary row
        qph_cells = "".join(f"<td><strong>{rnd.qph:.1f}</strong></td>" for rnd in rounds)
        qph_delta_cell = ""
        if n_rounds > 1:
            qph_delta = rounds[-1].qph - rounds[0].qph
            sign = "+" if qph_delta >= 0 else ""
            color = (
                "var(--success)"
                if qph_delta > 0
                else "var(--danger)"
                if qph_delta < 0
                else "var(--text-muted)"
            )
            qph_delta_cell = (
                f'<td style="color: {color};"><strong>{sign}{qph_delta:.1f}</strong></td>'
            )
        else:
            qph_delta_cell = "<td>-</td>"

        median_qph = statistics.median(qph_values) if qph_values else 0.0
        median_freshness = statistics.median(freshness_values) if freshness_values else 0.0

        summary_parts = [
            f"{n_rounds} rounds during streaming",
            f"Median QpH: <strong>{median_qph:.1f}</strong>",
        ]
        if median_freshness > 0:
            summary_parts.append(
                f"Median gold event age: {median_freshness / 86400:.1f} d "
                "(corpus event time, not freshness)"
            )
        if contention_count > 0:
            summary_parts.append(f"Q9 contention: {contention_count}x")

        return f"""
        <section>
            <h2>In-Stream Benchmark Rounds</h2>
            <div style="margin-bottom: 1rem; color: var(--text-muted); font-size: 0.875rem;">
                {" | ".join(summary_parts)}
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        {round_headers}
                        <th>&#916;</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                    <tr style="border-top: 2px solid var(--border); font-weight: 600;">
                        <td>QpH</td>
                        {qph_cells}
                        {qph_delta_cell}
                    </tr>
                </tbody>
            </table>
        </section>
        """

    def _generate_pipeline_score_cards(self, metrics: PipelineMetrics) -> str:
        """Generate pipeline benchmark score cards for the batch summary.

        Sustained mode uses _generate_sustained_summary() and
        _generate_sustained_detail_cards() instead -- this method is
        only called from _generate_batch_summary().
        """
        from lakebench.reports.formatter import (
            caps_bound_from,
            format_measurement,
            support_state_of,
        )

        pb = metrics.pipeline_benchmark
        if not pb:
            return ""

        scale_warning = (
            ' <span style="color: var(--danger);">INCOMPLETE</span>'
            if 0 < pb.scale_ratio < 0.95
            else ""
        )
        caps_bound = caps_bound_from(metrics)
        support_state = support_state_of(metrics)
        throughput_display = format_measurement(
            f"{pb.pipeline_throughput_gb_per_second:.3f} GB/s",
            "",
            caps_bound=caps_bound if pb.pipeline_throughput_gb_per_second > 0 else None,
            n_runs=1 if pb.pipeline_throughput_gb_per_second > 0 else None,
            support_state=support_state if pb.pipeline_throughput_gb_per_second > 0 else None,
        )
        return f"""
            <div class="card">
                <div class="card-label">Time to Value</div>
                <div class="card-value">{pb.time_to_value_seconds:.1f}s</div>
                <div class="card-hint">wall-clock to queryable gold</div>
            </div>
            <div class="card">
                <div class="card-label">Pipeline Throughput</div>
                <div class="card-value">{throughput_display}</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    {pb.total_data_processed_gb:.1f} GB total
                </div>
                <div class="card-hint">data volume / wall-clock time</div>
            </div>
            <div class="card">
                <div class="card-label">Compute Efficiency</div>
                <div class="card-value">{pb.compute_efficiency_gb_per_core_hour:.2f} GB/core-hr</div>
                <div class="card-hint">GB processed per core-hour requested</div>
            </div>
            <div class="card">
                <div class="card-label">Scale Ratio</div>
                <div class="card-value">{pb.scale_ratio:.1%}{scale_warning}</div>
                <div class="card-hint">actual vs expected data volume</div>
            </div>
        """

    def _generate_pipeline_benchmark_section(self, metrics: PipelineMetrics) -> str:
        """Generate the pipeline benchmark stage matrix section."""
        from lakebench.reports.formatter import caps_bound_from, format_measurement

        pb = metrics.pipeline_benchmark
        if not pb or not pb.stages:
            return ""

        rows = []
        is_sustained = self._is_sustained(metrics)
        caps_bound = caps_bound_from(metrics)

        for stage in pb.stages:
            status_class = "status-success" if stage.success else "status-failed"
            status_text = "OK" if stage.success else "FAIL"

            in_gb = f"{stage.input_size_gb:.3f}" if stage.input_size_gb > 0 else "-"
            out_gb = f"{stage.output_size_gb:.3f}" if stage.output_size_gb > 0 else "-"
            in_rows = f"{stage.input_rows:,}" if stage.input_rows > 0 else "-"
            out_rows = f"{stage.output_rows:,}" if stage.output_rows else "-"
            gb_s = (
                f"{stage.throughput_gb_per_second:.4f}"
                if stage.throughput_gb_per_second > 0
                else "-"
            )
            # Per-stage rows/s goes through the shared formatter so a capped
            # stage carries its BOUNDED BY tag next to the throughput cell.
            if stage.throughput_rows_per_second > 0:
                stage_bound = caps_bound if _stage_matches_cap(stage.stage_name, caps_bound) else []
                rows_s = format_measurement(
                    f"{stage.throughput_rows_per_second:,.0f}",
                    "",
                    caps_bound=stage_bound,
                    n_runs=1,
                )
            else:
                rows_s = "-"
            execs = str(stage.executor_count) if stage.executor_count > 0 else "-"
            cores = str(stage.executor_cores) if stage.executor_cores > 0 else "-"
            mem = f"{stage.executor_memory_gb:.0f}" if stage.executor_memory_gb > 0 else "-"
            qph = f"{stage.queries_per_hour:.1f}" if stage.queries_per_hour > 0 else "-"
            latency = _format_duration_ms(stage.latency_ms)
            freshness = (
                f"{stage.freshness_seconds:.1f}"
                if stage.freshness_seconds is not None and stage.freshness_seconds > 0
                else "-"
            )

            # CPU-hours per stage
            if stage.executor_count > 0 and stage.executor_cores > 0:
                stage_core_hours = (
                    stage.executor_count * stage.executor_cores * stage.elapsed_seconds / 3600.0
                )
                cpu_hrs = f"{stage_core_hours:.1f}"
            else:
                cpu_hrs = "-"

            elapsed = f"{stage.elapsed_seconds:.1f}s"
            n_fail = len(stage.submission_failures)
            if is_sustained and n_fail:
                # The stream waited on operator submission retries before
                # the window could open.
                # Records from before lost_seconds was kept: the time is
                # unknown, not zero.
                lost = (
                    f"{stage.submission_retry_seconds:.0f}s before it ran"
                    if all("lost_seconds" in f for f in stage.submission_failures)
                    else "time lost not recorded"
                )
                elapsed += (
                    f"<br><small>{n_fail} failed submission{'s' if n_fail != 1 else ''}, "
                    f"{lost}</small>"
                )

            if is_sustained:
                rows.append(f"""
                <tr>
                    <td><strong>{stage.stage_name}</strong></td>
                    <td>{stage.engine}</td>
                    <td>{elapsed}</td>
                    <td>{in_rows}</td>
                    <td>{rows_s}</td>
                    <td>{latency}</td>
                    <td>{freshness}</td>
                    <td>{execs}</td>
                    <td>{cores} x {mem}G</td>
                    <td>{cpu_hrs}</td>
                    <td>{qph}</td>
                    <td><span class="status {status_class}">{status_text}</span></td>
                </tr>
                """)
            else:
                rows.append(f"""
                <tr>
                    <td><strong>{stage.stage_name}</strong></td>
                    <td>{stage.engine}</td>
                    <td>{stage.elapsed_seconds:.1f}s</td>
                    <td>{in_gb}</td>
                    <td>{out_gb}</td>
                    <td>{in_rows}</td>
                    <td>{out_rows}</td>
                    <td>{gb_s}</td>
                    <td>{rows_s}</td>
                    <td>{execs}</td>
                    <td>{cores} x {mem}G</td>
                    <td>{cpu_hrs}</td>
                    <td>{qph}</td>
                    <td><span class="status {status_class}">{status_text}</span></td>
                </tr>
                """)

        if is_sustained:
            section_title = "Pipeline Stages"
            freshness_str = (
                f"{pb.data_freshness_seconds:.1f}s"
                if pb.data_freshness_seconds is not None
                else "N/A"
            )
            summary = (
                f"Freshness: <strong>{freshness_str}</strong> | "
                f"Throughput: <strong>{pb.sustained_throughput_rps:,.0f} rows/s</strong> | "
                f"Data: <strong>{pb.total_data_processed_gb:.1f} GB</strong> | "
                f"Rows: {pb.total_rows_processed:,}"
            )
            header_row = """
                        <th>Stage</th>
                        <th>Engine</th>
                        <th>Time</th>
                        <th>In Rows</th>
                        <th title="Rows processed per second">Rows/s</th>
                        <th title="Average micro-batch processing time">Latency</th>
                        <th title="Worst-case staleness of stage output">Freshness (s)</th>
                        <th>Executors</th>
                        <th>Cores x Mem</th>
                        <th title="executor_count x cores x elapsed / 3600">CPU-hours</th>
                        <th>QpH</th>
                        <th>Status</th>"""
            detail_cards = self._generate_sustained_detail_cards(pb)
        else:
            section_title = "Pipeline Benchmark"
            summary = (
                f"Time-to-Value: <strong>{pb.time_to_value_seconds:.1f}s</strong> | "
                f"Pipeline Throughput: <strong>{pb.pipeline_throughput_gb_per_second:.3f} GB/s</strong> | "
                f"Total Data: {pb.total_data_processed_gb:.1f} GB"
            )
            header_row = """
                        <th>Stage</th>
                        <th>Engine</th>
                        <th>Time</th>
                        <th>In (GB)</th>
                        <th>Out (GB)</th>
                        <th>In Rows</th>
                        <th>Out Rows</th>
                        <th>GB/s</th>
                        <th>Rows/s</th>
                        <th>Executors</th>
                        <th>Cores x Mem</th>
                        <th title="executor_count x cores x elapsed / 3600">CPU-hours</th>
                        <th>QpH</th>
                        <th>Status</th>"""
            detail_cards = ""

        # Per-domain detail (e.g. the AML detection scorecard). Empty string
        # for domains without extra rows. Rendered after the stage table.
        from lakebench.reports.scorecard import get_scorecard_block

        cs = metrics.config_snapshot or {}
        domain_detail = get_scorecard_block(cs.get("workload_schema")).render_detail_html(metrics)

        return f"""
        <section>
            <h2>{section_title}</h2>
            <div style="margin-bottom: 1rem; color: var(--text-muted); font-size: 0.875rem;">
                Mode: {pb.pipeline_mode} | {summary}
            </div>
            <table>
                <thead>
                    <tr>
                        {header_row}
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
            {detail_cards}
            {domain_detail}
        </section>
        """

    def _generate_config_section(self, metrics: PipelineMetrics) -> str:
        """Generate configuration section HTML.

        Args:
            metrics: Pipeline metrics

        Returns:
            HTML string
        """
        config = metrics.config_snapshot
        if not config:
            return ""

        items = []

        # Extract key config values (supports both old inline and new snapshot formats)
        config_keys = [
            ("Name", config.get("name")),
            ("Scale", config.get("scale")),
            ("Approx Bronze GB", config.get("approx_bronze_gb")),
            ("Processing Pattern", config.get("processing_pattern")),
            (
                "S3 Endpoint",
                config.get("s3", {}).get("endpoint")
                or config.get("platform", {}).get("storage", {}).get("s3", {}).get("endpoint"),
            ),
            # Per-job executor sizing as each job's manifest asked for it.
            # Records before v1.7 carry spark.executor values that sized
            # nothing, so they are not shown.
            *_executor_rows(config),
            (
                "Catalog Type",
                config.get("catalog")
                or config.get("architecture", {}).get("catalog", {}).get("type"),
            ),
            (
                "Table Format",
                config.get("table_format")
                or config.get("architecture", {}).get("table_format", {}).get("type"),
            ),
            ("Query Engine", config.get("query_engine")),
            ("Trino Workers", config.get("trino", {}).get("worker", {}).get("replicas")),
            ("Datagen Image", config.get("images", {}).get("datagen")),
        ]

        for label, value in config_keys:
            if value:
                items.append(f"""
                <div class="config-item">
                    <span class="config-key">{label}</span>
                    <span class="mono">{value}</span>
                </div>
                """)

        if not items:
            return ""

        return f"""
        <section>
            <h2>Configuration</h2>
            <div class="config-grid">
                {"".join(items)}
            </div>
        </section>
        """

    def _generate_experiment_section(self, metrics: PipelineMetrics) -> str:
        """The experiment block (metrics/experiment.py): what produced the run."""
        from lakebench.benchmark.fingerprint import describe

        e = _html_escape
        exp = metrics.experiment_block()
        if not exp:
            return (
                "<section><h2>Experiment</h2><p>No provenance: this run was recorded before "
                "the experiment block, so it cannot be compared with another run.</p></section>"
            )
        w = exp.get("workload") or {}
        c = exp.get("corpus") or {}
        dg = c.get("datagen") or {}
        a = exp.get("architecture") or {}
        lim = exp.get("limits") or {}
        st = exp.get("stages") or {}
        rules = exp.get("rules") or {}
        res = exp.get("results") or {}
        sup = exp.get("support") or {}
        eff = exp.get("effective_maintenance") or {}
        rep = exp.get("repetitions") or {}

        def comp(key: str) -> str:
            v = a.get(key) or {}
            return f"{v.get('type')} ({v.get('version') or v.get('image') or 'version unknown'})"

        digest = dg.get("digest") or f"unresolved: {dg.get('digest_reason', 'unknown')}"
        caps_hit = [x["job_type"] for x in lim.get("executors") or [] if x.get("cap_hit")]
        rows = [
            ("Workload", f"{w.get('name')} {w.get('version')}"),
            ("Generator model version", w.get("generator_model_version") or "none"),
            ("Corpus id", c.get("id")),
            ("Seed", c.get("seed")),
            ("Corpus role", c.get("corpus_role") or "none"),
            ("Scale", c.get("scale")),
            ("Mode", exp.get("mode")),
            ("Datagen image", dg.get("pod_image") or c.get("generator_image")),
            ("Datagen digest", digest),
            ("Recipe", a.get("recipe")),
            ("Catalog", comp("catalog")),
            ("Table format", comp("table_format")),
            ("Pipeline engine", comp("pipeline_engine")),
            ("Query engine", comp("query_engine")),
            ("Query access path", a.get("query_access_path") or "none"),
            ("Support state", f"{sup.get('state', 'unknown')} ({sup.get('basis', '')})"),
            ("Maintenance policy (requested)", exp.get("maintenance_policy_id")),
            (
                "Maintenance (effective, what ran)",
                f"{eff.get('id')} [{eff.get('detail_id') or ''}; {eff.get('basis') or ''}]"
                + (f" -- {'; '.join(eff.get('reasons') or [])}" if eff.get("reasons") else ""),
            ),
            *(
                [("Maintenance known limitations", "; ".join(eff["known_limitations"]))]
                if eff.get("known_limitations")
                else []
            ),
            ("Maintenance settings", exp.get("maintenance_settings") or "none"),
            *(
                [("In-stream QpH rounds", lim.get("benchmark_rounds"))]
                if exp.get("mode") == "sustained"
                else []
            ),
            ("System", exp.get("system") or "unknown"),
            (
                "Corpus observed",
                "yes (from the datagen pods)"
                if c.get("observed")
                else c.get("observed_note") or "no",
            ),
            ("Corpus problems", "; ".join(c.get("problems") or []) or "none"),
            (
                "Repetitions",
                f"runs n={rep.get('runs', 1)}, benchmark samples per query "
                f"{rep.get('benchmark_samples_per_query') or 'none'}"
                + (
                    f", in-stream rounds {rep['benchmark_rounds']}"
                    if rep.get("benchmark_rounds")
                    else ""
                ),
            ),
            ("Stages executed", ", ".join(st.get("executed") or []) or "none"),
            ("Stages skipped or failed", ", ".join(st.get("skipped") or []) or "none"),
            ("Executor caps hit (Lakebench limit)", ", ".join(caps_hit) or "none"),
            (
                "Lakebench limits that bound the run",
                self._bound_with_cap_names(lim.get("bound") or []) or "none",
            ),
            (
                "Auto-sizing cuts (Lakebench limit)",
                "; ".join(lim.get("autosize_cuts") or []) or "none",
            ),
        ]
        if rules:
            rows.append(("Rules executed", ", ".join(rules.get("executed") or []) or "none"))
            skipped = rules.get("skipped") or {}
            rows.append(
                ("Rules skipped", ", ".join(f"{k} ({v})" for k, v in skipped.items()) or "none")
            )
        if lim.get("max_files_per_trigger") is not None:
            rows.append(("Trickle rate (files per trigger)", lim["max_files_per_trigger"]))
        body = "".join(
            f"<tr><td>{e(str(k))}</td><td><code class='mono'>{e(str(v))}</code></td></tr>"
            for k, v in rows
        )
        fps = res.get("fingerprints") or {}
        fp_rows = "".join(
            f"<tr><td>{e(n)}</td><td><code class='mono'>{e(describe(f))}</code></td></tr>"
            for n, f in sorted(fps.items())
        )
        note = res.get("not_checked")
        fp_html = (
            f"<p>{e(note)}</p>"
            if note
            else f"<table><thead><tr><th>Query</th><th>Result fingerprint</th></tr></thead>"
            f"<tbody>{fp_rows}</tbody></table>"
        )
        return (
            "<section><h2>Experiment</h2>"
            f"<table><tbody>{body}</tbody></table>"
            "<h3>Result fingerprints</h3>"
            f"{fp_html}</section>"
        )

    # Infrastructure pods excluded from the per-stage summary table
    # (observability stack overhead, not pipeline performance data).
    _INFRA_PREFIXES = (
        "alertmanager",
        "grafana",
        "kube-state-metrics",
        "prometheus",
        "node-exporter",
        "operator",
    )

    @staticmethod
    def _classify_pod_stage(pod: dict) -> str:
        """Map a pod to its pipeline stage for aggregation."""
        name = pod.get("pod_name", "")
        component = pod.get("component", "")

        # Spark executor/driver -- stage is in the pod name
        if "spark" in component or "exec" in name or "driver" in name:
            lower = name.lower()
            if "bronze" in lower:
                return "bronze"
            if "silver" in lower:
                return "silver"
            if "gold" in lower:
                return "gold"
            return "spark-other"

        if "trino" in component or "trino" in name:
            return "trino"
        if "duckdb" in component or "duckdb" in name:
            return "trino"
        if any(c in component for c in ("hive", "polaris", "postgres")):
            return "catalog"
        if "datagen" in component or "datagen" in name:
            return "datagen"
        return "other"

    def _generate_platform_section(self, platform_metrics: dict | None) -> str:
        """Generate platform metrics section HTML.

        Per-stage summary table as primary view with ghost/infra pod
        filtering at render time.  Full per-pod data in collapsed detail.
        """
        if not platform_metrics:
            return ""

        pods = platform_metrics.get("pods", [])
        if not pods and not platform_metrics.get("collection_error"):
            return ""

        duration = platform_metrics.get("duration_seconds", 0)

        # Ghost filter -- render-time only, JSON keeps full list
        def _is_ghost(p: dict) -> bool:
            return (
                p.get("cpu_max_cores", 0) < 0.01 and p.get("memory_max_bytes", 0) < 10 * 1024 * 1024
            )

        active_pods = [p for p in pods if not _is_ghost(p)]
        ghost_count = len(pods) - len(active_pods)

        # Exclude infra pods from the per-stage summary (kept in detail)
        pipeline_pods = [
            p
            for p in active_pods
            if not any(p.get("pod_name", "").startswith(pfx) for pfx in self._INFRA_PREFIXES)
        ]

        # Aggregate by stage
        stage_agg: dict[str, dict] = {}
        for pod in pipeline_pods:
            stage = self._classify_pod_stage(pod)
            if stage not in stage_agg:
                stage_agg[stage] = {
                    "pods": 0,
                    "cpu_sum": 0.0,
                    "cpu_max": 0.0,
                    "mem_sum": 0,
                    "mem_max": 0,
                }
            agg = stage_agg[stage]
            agg["pods"] += 1
            agg["cpu_sum"] += pod.get("cpu_avg_cores", 0)
            agg["cpu_max"] += pod.get("cpu_max_cores", 0)
            agg["mem_sum"] += pod.get("memory_avg_bytes", 0)
            agg["mem_max"] += pod.get("memory_max_bytes", 0)

        # Render stage rows in logical order
        _STAGE_ORDER = [
            "bronze",
            "silver",
            "gold",
            "trino",
            "catalog",
            "datagen",
            "spark-other",
            "other",
        ]
        stage_rows = []
        for stage in _STAGE_ORDER:
            agg = stage_agg.get(stage)
            if not agg:
                continue
            stage_rows.append(
                f"<tr><td><strong>{stage}</strong></td>"
                f"<td>{agg['pods']}</td>"
                f"<td>{agg['cpu_sum']:.2f}</td>"
                f"<td>{agg['cpu_max']:.2f}</td>"
                f"<td>{agg['mem_sum'] / (1024**3):.1f} GiB</td>"
                f"<td>{agg['mem_max'] / (1024**3):.1f} GiB</td></tr>"
            )

        # Collection error
        error_note = ""
        collection_error = platform_metrics.get("collection_error")
        if collection_error:
            error_note = f'<p style="color: var(--warning); font-size: 0.875rem;">Collection warning: {collection_error}</p>'

        # Tier 2 status indicator
        engine = platform_metrics.get("engine")
        tier2_items = []
        if engine:
            gc = engine.get("spark_gc_seconds_total")
            if gc is not None:
                tier2_items.append(f"Spark GC: {gc:.1f}s total")
            sr = engine.get("spark_shuffle_read_bytes")
            if sr is not None:
                tier2_items.append(f"Shuffle read: {sr / (1024**3):.1f} GiB")
            sw = engine.get("spark_shuffle_write_bytes")
            if sw is not None:
                tier2_items.append(f"Shuffle write: {sw / (1024**3):.1f} GiB")
            tc = engine.get("trino_completed_queries")
            tf = engine.get("trino_failed_queries")
            if tc is not None:
                tier2_items.append(
                    f"Trino queries completed: {tc}" + (f" | failed: {tf}" if tf else "")
                )
        tier2_html = ""
        if tier2_items:
            tier2_html = (
                '<div style="margin-bottom: 1rem; padding: 0.75rem; '
                "background: var(--bg); border-radius: 0.25rem; "
                'font-size: 0.85rem;">'
                f"<strong>Engine Metrics (Tier 2):</strong> {' | '.join(tier2_items)}"
                "</div>"
            )
        elif not collection_error:
            tier2_html = (
                '<p style="color: var(--text-muted); font-size: 0.8rem; '
                'margin-bottom: 0.75rem; font-style: italic;">'
                "Engine-level metrics not available. Spark Prometheus sink and "
                "Trino JMX exporter are enabled when observability is on.</p>"
            )

        # S3 summary
        s3_total = platform_metrics.get("s3_requests_total", 0)
        s3_errors = platform_metrics.get("s3_errors_total", 0)
        s3_latency = platform_metrics.get("s3_avg_latency_ms", 0)
        s3_html = ""
        if s3_total > 0 or s3_errors > 0:
            s3_html = (
                f'<div style="margin-bottom: 1rem; font-size: 0.85rem; color: var(--text-muted);">'
                f"S3: {s3_total:,} requests | {s3_errors} errors | {s3_latency:.1f}ms avg latency"
                f"</div>"
            )

        # Per-pod detail rows (full list minus true ghosts)
        detail_rows = []
        for pod in active_pods:
            cpu_avg = pod.get("cpu_avg_cores", 0)
            cpu_max = pod.get("cpu_max_cores", 0)
            mem_avg_gb = pod.get("memory_avg_bytes", 0) / (1024**3)
            mem_max_gb = pod.get("memory_max_bytes", 0) / (1024**3)
            detail_rows.append(
                f'<tr><td class="mono">{pod.get("pod_name", "unknown")}</td>'
                f"<td>{pod.get('component', 'unknown')}</td>"
                f"<td>{cpu_avg:.2f}</td><td>{cpu_max:.2f}</td>"
                f"<td>{mem_avg_gb:.1f} GiB</td><td>{mem_max_gb:.1f} GiB</td></tr>"
            )

        ghost_note = f" ({ghost_count} ghost pods filtered)" if ghost_count > 0 else ""

        return f"""
        <section>
            <h2>Platform Metrics</h2>
            <div style="margin-bottom: 1rem; color: var(--text-muted); font-size: 0.875rem;">
                Platform metrics collected over {duration:.0f}s | {len(pods)} pods observed | {len(active_pods)} active{ghost_note}
            </div>
            {error_note}
            {tier2_html}
            {s3_html}
            <table>
                <thead>
                    <tr>
                        <th>Stage</th>
                        <th>Pods</th>
                        <th>CPU Avg (cores)</th>
                        <th>CPU Max (cores)</th>
                        <th>Mem Avg</th>
                        <th>Mem Max</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(stage_rows)}
                </tbody>
            </table>
            <details style="margin-top: 1rem;">
                <summary style="cursor: pointer; color: var(--text-muted); font-size: 0.85rem;">
                    Per-pod detail ({len(active_pods)} pods)
                </summary>
                <table style="margin-top: 0.5rem;">
                    <thead>
                        <tr>
                            <th>Pod</th>
                            <th>Component</th>
                            <th>CPU Avg (cores)</th>
                            <th>CPU Max (cores)</th>
                            <th>Mem Avg</th>
                            <th>Mem Max</th>
                        </tr>
                    </thead>
                    <tbody>
                        {"".join(detail_rows)}
                    </tbody>
                </table>
            </details>
        </section>
        """
