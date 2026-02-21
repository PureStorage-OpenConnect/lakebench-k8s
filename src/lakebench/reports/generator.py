"""Report generation for Lakebench.

Creates HTML reports from pipeline metrics.  Reports are written into the
per-run directory managed by :class:`MetricsStorage`::

    lakebench-output/runs/run-<id>/report.html

The *output_dir* parameter is accepted for backward compatibility but is
only used as a fallback when a report is generated without a matching run
directory.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from lakebench.metrics import MetricsStorage, PipelineMetrics

logger = logging.getLogger(__name__)


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

    def generate_report(
        self,
        run_id: str | None = None,
    ) -> Path:
        """Generate an HTML report.

        The report is written into the per-run directory as ``report.html``.

        Args:
            run_id: Run ID to report on (default: latest)

        Returns:
            Path to generated report
        """
        # Load metrics
        if run_id:
            metrics = self.storage.load_run(run_id)
            if not metrics:
                raise ValueError(f"Run not found: {run_id}")
        else:
            metrics = self.storage.get_latest_run()
            if not metrics:
                raise ValueError("No runs found")

        # Generate HTML (pass platform_metrics if present in the run data)
        html = self._generate_html(metrics, platform_metrics=metrics.platform_metrics)

        # Write into the per-run directory
        run_dir = self.storage.run_dir(metrics.run_id)
        filepath = run_dir / "report.html"

        filepath.write_text(html)
        logger.info(f"Generated report: {filepath}")

        return filepath

    def _compute_overall_status(
        self, metrics: PipelineMetrics
    ) -> tuple[bool, list[str], list[str]]:
        """Compute whether the run should be considered a pass.

        Returns (passed, fail_reasons, warnings) where:
        - passed: True if no failures
        - fail_reasons: list of failure messages
        - warnings: list of anomaly messages (run passed but has caveats)

        Checks:
        - Pipeline completed without crash
        - Continuous: ingest_ratio >= 0.95 (pipeline kept pace with datagen)
        - Batch: scale_ratio >= 0.95 (all expected data was processed)
        - All jobs / streaming stages succeeded
        - No failed benchmark queries
        """
        reasons: list[str] = []
        warnings: list[str] = []

        if not metrics.success:
            reasons.append("Pipeline crashed or was interrupted")

        pb = metrics.pipeline_benchmark
        is_cont = self._is_continuous(metrics)

        # Data completeness
        if pb:
            if is_cont and pb.ingest_ratio < 0.95:
                reasons.append(f"Ingest ratio {pb.ingest_ratio:.2f} < 0.95 (pipeline saturated)")
            elif is_cont and pb.ingest_ratio > 1.05:
                warnings.append(
                    f"Ingest ratio {pb.ingest_ratio:.2f} > 1.05 (gold re-reads exceed input)"
                )
            if not is_cont and 0 < pb.scale_ratio < 0.95:
                reasons.append(f"Scale ratio {pb.scale_ratio:.1%} < 95% (incomplete data)")

        # Job / streaming success
        if is_cont and metrics.streaming:
            failed = [s.job_name for s in metrics.streaming if not s.success]
            if failed:
                reasons.append(f"Streaming jobs failed: {', '.join(failed)}")
        elif metrics.jobs:
            failed = [j.job_name for j in metrics.jobs if not j.success]
            if failed:
                reasons.append(f"Batch jobs failed: {', '.join(failed)}")

        # Benchmark query failures
        if metrics.benchmark and metrics.benchmark.queries:
            n_failed = sum(
                1
                for q in metrics.benchmark.queries
                if isinstance(q, dict) and not q.get("success", True)
            )
            if n_failed:
                reasons.append(f"{n_failed} benchmark queries failed")

        return (len(reasons) == 0, reasons, warnings)

    def _is_continuous(self, metrics: PipelineMetrics) -> bool:
        """Return True if this run used the continuous/streaming pipeline."""
        if metrics.pipeline_benchmark:
            return metrics.pipeline_benchmark.pipeline_mode == "continuous"
        return bool(metrics.streaming)

    def _generate_run_context(self, metrics: PipelineMetrics) -> str:
        """Generate a one-line run context banner below the header."""
        cs = metrics.config_snapshot or {}
        mode = "Continuous" if self._is_continuous(metrics) else "Batch"
        scale = cs.get("scale", "-")
        catalog = cs.get("catalog", "-")
        table_fmt = cs.get("table_format", "-")
        pipe_engine = cs.get("pipeline_engine", "spark")
        engine = cs.get("query_engine", "-")
        storage = cs.get("storage_backend", "")
        duration_m = int(metrics.total_elapsed_seconds // 60)
        duration_s = int(metrics.total_elapsed_seconds % 60)

        storage_segment = f" | {storage}" if storage else ""

        return f"""
        <div style="margin-bottom: 1.5rem; color: var(--text-muted); font-size: 0.875rem;">
            <strong>{mode}</strong> pipeline |
            Customer360 at scale {scale} |
            {catalog}-{table_fmt}-{pipe_engine}-{engine}{storage_segment} |
            {duration_m}m {duration_s}s
        </div>
        """

    def _generate_continuous_detail_cards(self, pb) -> str:
        """Generate detail cards for Pipeline Stages section (continuous only).

        Shows Ingest Ratio below the stage table.  Compute Efficiency and
        Total CPU-hours are now in the Layer 1 summary cards.
        """
        # Ingest ratio badge
        if pb.ingest_ratio < 0.95:
            ratio_badge = '<span style="color: var(--danger);">SATURATED</span>'
        elif pb.ingest_ratio > 1.05:
            ratio_badge = (
                f'<span style="color: var(--warning);"'
                f' title="Ingest ratio above 1.0 means total rows processed exceeds'
                f" rows generated. Expected when gold refreshes multiple times,"
                f' re-reading the full silver table each cycle.">'
                f"{pb.ingest_ratio:.2f} (gold re-reads exceed input)</span>"
            )
        else:
            ratio_badge = '<span style="color: var(--success);">Healthy</span>'

        return f"""
        <div class="cards" style="margin-top: 1.5rem;">
            <div class="card">
                <div class="card-label">Ingest Ratio</div>
                <div class="card-value">{pb.ingest_ratio:.2f}</div>
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

        is_cont = self._is_continuous(metrics)

        # For continuous mode, use latency_ms as the weight (stages run
        # concurrently so elapsed times are similar).  For batch, use
        # elapsed_seconds.
        stage_data = []
        for s in pb.stages:
            if s.stage_name in ("datagen",) and is_cont:
                continue
            cpu_sec = s.executor_count * s.executor_cores * s.elapsed_seconds
            weight = (
                s.latency_ms
                if (is_cont and s.latency_ms is not None and s.latency_ms > 0)
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

        # Identify bottleneck
        dominant = max(stage_data, key=lambda d: d["cpu_pct"])
        sorted_by_cpu = sorted(stage_data, key=lambda d: d["cpu_pct"], reverse=True)
        if (
            len(sorted_by_cpu) >= 2
            and abs(sorted_by_cpu[0]["cpu_pct"] - sorted_by_cpu[1]["cpu_pct"]) < 10
        ):
            callout = "Compute is balanced across stages (no single bottleneck)."
        elif dominant["cpu_pct"] >= 60:
            callout = (
                f"{dominant['name'].capitalize()} consumed {dominant['cpu_pct']:.0f}% "
                f"of total compute and {dominant['weight_pct']:.0f}% of "
                f"{'latency' if is_cont else 'wall-clock time'}."
            )
        else:
            callout = (
                f"{dominant['name'].capitalize()} is the largest stage at "
                f"{dominant['cpu_pct']:.0f}% of compute."
            )

        # Stacked bar (pure CSS)
        bar_segments = []
        for d in stage_data:
            color = self._STAGE_COLORS.get(d["name"], "#94a3b8")
            bar_segments.append(
                f'<div style="width: {d["cpu_pct"]:.1f}%; background: {color}; '
                f'height: 28px; display: inline-block;" '
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

        weight_col = "Latency" if is_cont else "Time (s)"
        table_rows = ""
        for d in stage_data:
            if is_cont:
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
            <div style="width: 100%; border-radius: 4px; overflow: hidden; font-size: 0; margin-bottom: 0.75rem;">
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

    def _generate_data_validity_section(self, metrics: PipelineMetrics) -> str:
        """Generate data validity panel (Layer 2).

        Green/red indicators showing whether the run's numbers can be
        trusted for cross-run comparison.
        """
        pb = metrics.pipeline_benchmark
        is_cont = self._is_continuous(metrics)

        indicators: list[tuple[str, str, str]] = []  # (label, status_class, value)

        # Scale Ratio / Ingest Ratio
        if is_cont and pb:
            ratio = pb.ingest_ratio
            if ratio < 0.95:
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
            if 0 < ratio < 0.95:
                indicators.append(("Scale Ratio", "status-failed", f"{ratio:.1%} INCOMPLETE"))
            else:
                indicators.append(("Scale Ratio", "status-success", f"{ratio:.1%} Complete"))

        # Job success
        if is_cont and metrics.streaming:
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
    # Layer 2: Stability & Contention (continuous only)
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
        """Generate stability over time section (Layer 2, continuous only).

        Shows QpH and freshness trends across in-stream benchmark rounds.
        """
        if not self._is_continuous(metrics):
            return ""

        pb = metrics.pipeline_benchmark
        if not pb or not pb.benchmark_rounds or len(pb.benchmark_rounds) < 2:
            return ""

        qph_values = [r.qph for r in pb.benchmark_rounds]
        raw_freshness = [
            r.round_meta.gold_freshness_seconds
            if r.round_meta and r.round_meta.gold_freshness_seconds > 0
            else 0.0
            for r in pb.benchmark_rounds
        ]
        # Suppress freshness line when all values are zero (adds no information)
        freshness_values = raw_freshness if any(v > 0 for v in raw_freshness) else []

        chart = self._render_line_chart_svg(
            series1=qph_values,
            series2=freshness_values,
            label1="QpH",
            label2="Freshness (s)",
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

    def _generate_query_time_freshness_section(self, metrics: PipelineMetrics) -> str:
        """Generate query-time freshness diagnostic (Layer 2, spec 7.6).

        Shows median query-time freshness vs worst-case data freshness,
        with a gap analysis call-out.
        """
        pb = metrics.pipeline_benchmark
        if not pb or pb.pipeline_mode != "continuous":
            return ""
        if pb.query_time_freshness_seconds <= 0 or not pb.data_freshness_seconds:
            return ""

        qt = pb.query_time_freshness_seconds
        wc = pb.data_freshness_seconds
        gap = wc - qt

        if gap > 60:
            call_out = (
                "Freshness is highly variable -- it depends on when queries land "
                "relative to gold refresh cycles."
            )
        else:
            call_out = "Freshness is relatively consistent across the streaming window."

        return f"""
        <section>
            <h2>Query-Time Freshness</h2>
            <div style="display: flex; gap: 3rem; margin-bottom: 1rem;">
                <div>
                    <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase;">
                        Query-Time (median)
                    </div>
                    <div style="font-size: 1.5rem; font-weight: 700;">{qt:.1f}s</div>
                </div>
                <div>
                    <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase;">
                        Worst-Case
                    </div>
                    <div style="font-size: 1.5rem; font-weight: 700;">{wc:.1f}s</div>
                </div>
                <div>
                    <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase;">
                        Gap
                    </div>
                    <div style="font-size: 1.5rem; font-weight: 700;">{gap:.1f}s</div>
                </div>
            </div>
            <p style="color: var(--text-muted); font-style: italic;">{call_out}</p>
        </section>
        """

    def _generate_contention_section(self, metrics: PipelineMetrics) -> str:
        """Generate Q9 contention map (Layer 2, continuous only).

        Shows Q9 contention events across benchmark rounds.
        """
        if not self._is_continuous(metrics):
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
                f"{meta.gold_freshness_seconds:.1f}s" if meta.gold_freshness_seconds > 0 else "-"
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
                        <th>Freshness</th>
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
        config_html = self._generate_config_section(metrics)
        platform_html = self._generate_platform_section(platform_metrics)
        # Layer 2: Diagnosis
        bottleneck_html = self._generate_bottleneck_section(metrics)
        validity_html = self._generate_data_validity_section(metrics)
        stability_html = self._generate_stability_section(metrics)
        qt_freshness_html = self._generate_query_time_freshness_section(metrics)
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

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LakeBench Scorecard - {metrics.deployment_name}</title>
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
        <header>
            <h1>LakeBench Scorecard</h1>
            <div class="subtitle">
                Deployment: <strong>{metrics.deployment_name}</strong> |
                Run ID: <code class="mono">{metrics.run_id}</code> |
                <span class="status {_badge_cls}" title="{_badge_tip}">
                    {_badge_text}
                </span>
            </div>
            {"" if overall_passed and not warnings else '<div style="margin-top: 0.5rem; font-size: 0.8rem; color: ' + ("var(--danger)" if not overall_passed else "var(--warning)") + ';">' + "; ".join(fail_reasons or warnings) + "</div>"}
        </header>

        {run_context_html}

        {summary_html}

        {bottleneck_html}

        {validity_html}

        {pipeline_bench_html}

        {jobs_html}

        {streaming_html}

        {queries_html}

        {benchmark_html}

        {benchmark_rounds_html}

        {stability_html}

        {qt_freshness_html}

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

        Continuous mode shows streaming-specific KPIs.
        Batch mode shows the original batch KPIs.
        """
        if self._is_continuous(metrics):
            return self._generate_continuous_summary(metrics)
        return self._generate_batch_summary(metrics)

    def _generate_continuous_summary(self, metrics: PipelineMetrics) -> str:
        """Generate summary cards for continuous/streaming mode.

        Five primary cards (the scores users compare across runs) plus a
        smaller metadata row with contextual info.
        """
        total_time = metrics.total_elapsed_seconds
        pb = metrics.pipeline_benchmark
        throughput = pb.sustained_throughput_rps if pb else 0.0
        data_gb = pb.total_data_processed_gb if pb else 0.0
        efficiency = pb.compute_efficiency_gb_per_core_hour if pb else 0.0
        core_hours = pb.total_core_hours if pb else 0.0

        qph: float | None = None
        qph_note = ""
        if pb and pb.benchmark_rounds:
            round_qphs = [r.qph for r in pb.benchmark_rounds if r.qph > 0]
            if round_qphs:
                import statistics

                qph = statistics.median(round_qphs)
                qph_note = f"median of {len(round_qphs)} rounds"
        elif metrics.benchmark and metrics.benchmark.qph > 0:
            qph = metrics.benchmark.qph
            qph_note = "single benchmark"

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
        qph_display = f"{qph:,.1f}" if qph is not None else "N/A"
        if qph is None:
            qph_note = "no benchmark rounds completed"

        # Derived context values for hint line 2
        freshness_hint2 = (
            f"&#8595; lower is better | {freshness_val / 60:.1f} min lag"
            if freshness_val is not None
            else "< 2 gold refresh cycles completed"
        )
        throughput_hint2 = (
            f"&#8593; higher is better | {throughput * 3600:,.0f} rows/hr"
            if throughput > 0
            else "&#8593; higher is better"
        )
        qph_hint2 = (
            f"&#8593; higher is better | ~{qph / 60:.0f} queries/min"
            if qph is not None
            else "no benchmark rounds completed"
        )
        cpu_hint2 = (
            f"&#8595; lower is better | extrapolates to ~{core_hours * 86400 / total_time:,.0f}/day"
            if total_time > 0
            else "&#8595; lower is better"
        )

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
                <div class="card-value">{throughput:,.0f} rows/s</div>
                <div class="card-hint">rows entering bronze per second</div>
                <div class="card-hint2">{throughput_hint2}</div>
            </div>
            <div class="card">
                <div class="card-label">Compute Efficiency</div>
                <div class="card-value">{efficiency:.2f} GB/core-hr</div>
                <div class="card-hint">GB processed per core-hour requested</div>
                <div class="card-hint2">&#8593; higher is better</div>
            </div>
            <div class="card">
                <div class="card-label">In-Stream QpH</div>
                <div class="card-value">{qph_display}</div>
                <div class="card-hint">{qph_note or "higher is better"}</div>
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
            <span>Avg Pipeline Throughput: {avg_throughput:.2f} GB/s</span>
        </div>
        """

    def _generate_batch_summary(self, metrics: PipelineMetrics) -> str:
        """Generate summary cards for batch mode.

        Five primary cards (the scores users compare across runs) plus a
        smaller metadata row with contextual info.  When pipeline_benchmark
        is absent (old data), falls back to a simple layout.
        """
        pb = metrics.pipeline_benchmark
        total_time = metrics.total_elapsed_seconds
        job_count = len(metrics.jobs)
        successful = sum(1 for j in metrics.jobs if j.success)

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
        if qph == 0.0 and metrics.benchmark:
            qph = metrics.benchmark.qph

        scale_warning = (
            ' <span style="color: var(--danger);">INCOMPLETE</span>'
            if 0 < pb.scale_ratio < 0.95
            else ""
        )

        ttv = pb.time_to_value_seconds
        ttv_hint2 = (
            f"&#8595; lower is better | {int(ttv // 60)}m {int(ttv % 60)}s"
            if ttv > 0
            else "&#8595; lower is better"
        )
        qph_hint2 = (
            f"&#8593; higher is better | ~{qph / 60:.0f} queries/min"
            if qph > 0
            else "&#8593; higher is better"
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
                <div class="card-value">{pb.pipeline_throughput_gb_per_second:.3f} GB/s</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    {pb.total_data_processed_gb:.1f} GB total
                </div>
                <div class="card-hint">data volume / wall-clock time</div>
                <div class="card-hint2">&#8593; higher is better</div>
            </div>
            <div class="card">
                <div class="card-label">Compute Efficiency</div>
                <div class="card-value">{pb.compute_efficiency_gb_per_core_hour:.2f} GB/core-hr</div>
                <div class="card-hint">GB processed per core-hour requested</div>
                <div class="card-hint2">&#8593; higher is better</div>
            </div>
            <div class="card">
                <div class="card-label">QpH</div>
                <div class="card-value">{qph:,.1f}</div>
                <div class="card-hint">queries per hour -- higher is better</div>
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

        Returns empty string when no batch jobs exist (continuous mode).
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

            rows.append(f"""
            <tr>
                <td><code class="mono">{job.job_name}</code></td>
                <td><span class="status {status_class}">{status_text}</span></td>
                <td>{job.elapsed_seconds:.1f}s</td>
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

        rows = []
        total_rows = 0
        total_cpu_sec = 0.0
        total_executors = 0
        total_mem_gb = 0.0
        for s in metrics.streaming:
            status_class = "status-success" if s.success else "status-failed"
            status_text = "Pass" if s.success else "Fail"
            total_rows += s.total_rows_processed

            throughput = f"{s.throughput_rps:,.0f} rows/s" if s.throughput_rps > 0 else "-"
            freshness = f"{s.freshness_seconds:.0f}s" if s.freshness_seconds > 0 else "-"

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
            <h2>Streaming Pipeline</h2>
            <div style="margin-bottom: 1rem; color: var(--text-muted); font-size: 0.875rem;">
                {len(metrics.streaming)} streaming jobs | {total_rows:,} total rows processed
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
        if not metrics.benchmark:
            return ""

        b = metrics.benchmark
        mode_label = b.mode
        if b.streams > 1:
            mode_label += f", {b.streams} streams"

        return f"""
            <div class="card">
                <div class="card-label">QpH ({b.cache})</div>
                <div class="card-value">{b.qph:.1f}</div>
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

        Only renders when benchmark_rounds is non-empty (continuous mode).
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
            if meta and meta.gold_freshness_seconds > 0:
                freshness_values.append(meta.gold_freshness_seconds)
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
            summary_parts.append(f"Median freshness: {median_freshness:.1f}s")
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

        Continuous mode uses _generate_continuous_summary() and
        _generate_continuous_detail_cards() instead -- this method is
        only called from _generate_batch_summary().
        """
        pb = metrics.pipeline_benchmark
        if not pb:
            return ""

        scale_warning = (
            ' <span style="color: var(--danger);">INCOMPLETE</span>'
            if 0 < pb.scale_ratio < 0.95
            else ""
        )
        return f"""
            <div class="card">
                <div class="card-label">Time to Value</div>
                <div class="card-value">{pb.time_to_value_seconds:.1f}s</div>
                <div class="card-hint">wall-clock to queryable gold</div>
            </div>
            <div class="card">
                <div class="card-label">Pipeline Throughput</div>
                <div class="card-value">{pb.pipeline_throughput_gb_per_second:.3f} GB/s</div>
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
        pb = metrics.pipeline_benchmark
        if not pb or not pb.stages:
            return ""

        rows = []
        is_continuous = pb.pipeline_mode == "continuous"

        for stage in pb.stages:
            status_class = "status-success" if stage.success else "status-failed"
            status_text = "OK" if stage.success else "FAIL"

            in_gb = f"{stage.input_size_gb:.3f}" if stage.input_size_gb > 0 else "-"
            out_gb = f"{stage.output_size_gb:.3f}" if stage.output_size_gb > 0 else "-"
            in_rows = f"{stage.input_rows:,}" if stage.input_rows > 0 else "-"
            out_rows = f"{stage.output_rows:,}" if stage.output_rows > 0 else "-"
            gb_s = (
                f"{stage.throughput_gb_per_second:.4f}"
                if stage.throughput_gb_per_second > 0
                else "-"
            )
            rows_s = (
                f"{stage.throughput_rows_per_second:,.0f}"
                if stage.throughput_rows_per_second > 0
                else "-"
            )
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

            if is_continuous:
                rows.append(f"""
                <tr>
                    <td><strong>{stage.stage_name}</strong></td>
                    <td>{stage.engine}</td>
                    <td>{stage.elapsed_seconds:.1f}s</td>
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

        if is_continuous:
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
            detail_cards = self._generate_continuous_detail_cards(pb)
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
            (
                "Executor Instances",
                config.get("spark", {}).get("executor", {}).get("instances")
                or config.get("platform", {})
                .get("compute", {})
                .get("spark", {})
                .get("executor", {})
                .get("instances"),
            ),
            ("Executor Cores", config.get("spark", {}).get("executor", {}).get("cores")),
            (
                "Executor Memory",
                config.get("spark", {}).get("executor", {}).get("memory")
                or config.get("platform", {})
                .get("compute", {})
                .get("spark", {})
                .get("executor", {})
                .get("memory"),
            ),
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
