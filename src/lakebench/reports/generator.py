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

        # Generate HTML
        html = self._generate_html(metrics)

        # Write into the per-run directory
        run_dir = self.storage.run_dir(metrics.run_id)
        filepath = run_dir / "report.html"

        filepath.write_text(html)
        logger.info(f"Generated report: {filepath}")

        return filepath

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
        jobs_html = self._generate_jobs_table(metrics)
        streaming_html = self._generate_streaming_table(metrics)
        queries_html = self._generate_queries_table(metrics)
        benchmark_html = self._generate_benchmark_section(metrics)
        pipeline_bench_html = self._generate_pipeline_benchmark_section(metrics)
        summary_html = self._generate_summary(metrics)
        config_html = self._generate_config_section(metrics)
        platform_html = self._generate_platform_section(platform_metrics)

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lakebench Report - {metrics.deployment_name}</title>
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
            <h1>Lakebench Benchmark Report</h1>
            <div class="subtitle">
                Deployment: <strong>{metrics.deployment_name}</strong> |
                Run ID: <code class="mono">{metrics.run_id}</code> |
                <span class="status {"status-success" if metrics.success else "status-failed"}">
                    {"Passed" if metrics.success else "Failed"}
                </span>
            </div>
        </header>

        {summary_html}

        {pipeline_bench_html}

        <section>
            <h2>Job Performance</h2>
            {jobs_html}
        </section>

        {streaming_html}

        {queries_html}

        {benchmark_html}

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

        Args:
            metrics: Pipeline metrics

        Returns:
            HTML string
        """
        total_time = metrics.total_elapsed_seconds
        job_count = len(metrics.jobs)
        successful = sum(1 for j in metrics.jobs if j.success)
        total_input = sum(j.input_size_gb for j in metrics.jobs)
        total_output_rows = sum(j.output_rows for j in metrics.jobs)

        avg_throughput = total_input / total_time if total_time > 0 else 0

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
                <div class="card-label">Output Rows</div>
                <div class="card-value">{total_output_rows:,}</div>
            </div>
            <div class="card">
                <div class="card-label">Avg Throughput</div>
                <div class="card-value">{avg_throughput:.2f} GB/s</div>
            </div>
            {self._generate_qph_card(metrics)}
            {self._generate_pipeline_score_cards(metrics)}
        </div>
        """

    def _generate_jobs_table(
        self,
        metrics: PipelineMetrics,
    ) -> str:
        """Generate jobs table HTML.

        Args:
            metrics: Pipeline metrics

        Returns:
            HTML string
        """
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

        rows = []
        total_rows = 0
        for s in metrics.streaming:
            status_class = "status-success" if s.success else "status-failed"
            status_text = "Pass" if s.success else "Fail"
            total_rows += s.total_rows_processed

            throughput = f"{s.throughput_rps:,.0f} rows/s" if s.throughput_rps > 0 else "-"
            freshness = f"{s.freshness_seconds:.0f}s" if s.freshness_seconds > 0 else "-"

            rows.append(f"""
            <tr>
                <td><code class="mono">{s.job_name}</code></td>
                <td><span class="status {status_class}">{status_text}</span></td>
                <td>{s.elapsed_seconds:.0f}s</td>
                <td>{s.total_batches:,}</td>
                <td>{s.total_rows_processed:,}</td>
                <td>{throughput}</td>
                <td>{s.micro_batch_duration_ms:.0f}ms</td>
                <td>{freshness}</td>
            </tr>
            """)

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
                        <th>Avg Batch</th>
                        <th>Freshness</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join(rows)}
                </tbody>
            </table>
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

    def _generate_pipeline_score_cards(self, metrics: PipelineMetrics) -> str:
        """Generate pipeline benchmark score cards for the summary."""
        pb = metrics.pipeline_benchmark
        if not pb:
            return ""

        if pb.pipeline_mode == "continuous":
            latency_str = (
                "/".join(f"{v:.0f}" for v in pb.stage_latency_profile)
                if pb.stage_latency_profile
                else "-"
            )
            saturated_badge = (
                '<span style="color: var(--status-failed);">SATURATED</span>'
                if pb.pipeline_saturated
                else "OK"
            )
            return f"""
            <div class="card">
                <div class="card-label">Data Freshness</div>
                <div class="card-value">{pb.data_freshness_seconds:.1f}s</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    continuous pipeline
                </div>
            </div>
            <div class="card">
                <div class="card-label">Sustained Throughput</div>
                <div class="card-value">{pb.sustained_throughput_rps:,.0f} rows/s</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    {pb.total_rows_processed:,} rows total
                </div>
            </div>
            <div class="card">
                <div class="card-label">Stage Latency</div>
                <div class="card-value">{latency_str}ms</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    bronze/silver/gold avg
                </div>
            </div>
            <div class="card">
                <div class="card-label">Completeness</div>
                <div class="card-value">{pb.ingestion_completeness_ratio:.2%}</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    {saturated_badge}
                </div>
            </div>
            <div class="card">
                <div class="card-label">Compute Efficiency</div>
                <div class="card-value">{pb.compute_efficiency_gb_per_core_hour:.2f} GB/core-hr</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    requested resources
                </div>
            </div>
            """

        scale_warning = (
            ' <span style="color: var(--status-failed);">INCOMPLETE</span>'
            if 0 < pb.scale_verified_ratio < 0.95
            else ""
        )
        return f"""
            <div class="card">
                <div class="card-label">Time to Value</div>
                <div class="card-value">{pb.time_to_value_seconds:.1f}s</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    batch pipeline
                </div>
            </div>
            <div class="card">
                <div class="card-label">Pipeline Throughput</div>
                <div class="card-value">{pb.pipeline_throughput_gb_per_second:.3f} GB/s</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    {pb.total_data_processed_gb:.1f} GB total
                </div>
            </div>
            <div class="card">
                <div class="card-label">Compute Efficiency</div>
                <div class="card-value">{pb.compute_efficiency_gb_per_core_hour:.2f} GB/core-hr</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    requested resources
                </div>
            </div>
            <div class="card">
                <div class="card-label">Scale Verified</div>
                <div class="card-value">{pb.scale_verified_ratio:.1%}{scale_warning}</div>
                <div class="card-delta" style="color: var(--text-muted);">
                    actual / expected GB
                </div>
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
            latency = f"{stage.latency_ms:.0f}" if stage.latency_ms > 0 else "-"
            freshness = f"{stage.freshness_seconds:.1f}" if stage.freshness_seconds > 0 else "-"

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
                    <td>{cores}</td>
                    <td>{mem}</td>
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
                    <td>{cores}</td>
                    <td>{mem}</td>
                    <td>{qph}</td>
                    <td><span class="status {status_class}">{status_text}</span></td>
                </tr>
                """)

        if is_continuous:
            latency_str = (
                "/".join(f"{v:.0f}" for v in pb.stage_latency_profile)
                if pb.stage_latency_profile
                else "-"
            )
            summary = (
                f"Freshness: <strong>{pb.data_freshness_seconds:.1f}s</strong> | "
                f"Throughput: <strong>{pb.sustained_throughput_rps:,.0f} rows/s</strong> | "
                f"Latency: <strong>{latency_str}ms</strong> | "
                f"Rows: {pb.total_rows_processed:,}"
            )
            header_row = """
                        <th>Stage</th>
                        <th>Engine</th>
                        <th>Time</th>
                        <th>In Rows</th>
                        <th>Rows/s</th>
                        <th>Latency (ms)</th>
                        <th>Freshness (s)</th>
                        <th>Executors</th>
                        <th>Cores</th>
                        <th>Mem (GB)</th>
                        <th>QpH</th>
                        <th>Status</th>"""
        else:
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
                        <th>Cores</th>
                        <th>Mem (GB)</th>
                        <th>QpH</th>
                        <th>Status</th>"""

        return f"""
        <section>
            <h2>Pipeline Benchmark</h2>
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

    def _generate_platform_section(self, platform_metrics: dict | None) -> str:
        """Generate platform metrics section HTML.

        Args:
            platform_metrics: Dict from PlatformMetrics.to_dict(), or None

        Returns:
            HTML string (empty if no platform metrics)
        """
        if not platform_metrics:
            return ""

        pods = platform_metrics.get("pods", [])
        if not pods and not platform_metrics.get("collection_error"):
            return ""

        # Build pod table rows
        pod_rows = []
        for pod in pods:
            cpu_avg = pod.get("cpu_avg_cores", 0)
            cpu_max = pod.get("cpu_max_cores", 0)
            mem_avg_gb = pod.get("memory_avg_bytes", 0) / (1024**3)
            mem_max_gb = pod.get("memory_max_bytes", 0) / (1024**3)
            pod_rows.append(f"""
                <tr>
                    <td class="mono">{pod.get("pod_name", "unknown")}</td>
                    <td>{pod.get("component", "unknown")}</td>
                    <td>{cpu_avg:.2f}</td>
                    <td>{cpu_max:.2f}</td>
                    <td>{mem_avg_gb:.1f} GiB</td>
                    <td>{mem_max_gb:.1f} GiB</td>
                </tr>
            """)

        # S3 summary
        s3_total = platform_metrics.get("s3_requests_total", 0)
        s3_errors = platform_metrics.get("s3_errors_total", 0)
        s3_latency = platform_metrics.get("s3_avg_latency_ms", 0)
        duration = platform_metrics.get("duration_seconds", 0)

        error_note = ""
        collection_error = platform_metrics.get("collection_error")
        if collection_error:
            error_note = f'<p style="color: var(--warning); font-size: 0.875rem;">Collection warning: {collection_error}</p>'

        return f"""
        <section>
            <h2>Platform Metrics</h2>
            {error_note}
            <div class="cards">
                <div class="card">
                    <div class="card-label">Collection Window</div>
                    <div class="card-value">{duration:.0f}s</div>
                </div>
                <div class="card">
                    <div class="card-label">S3 Requests (CLI)</div>
                    <div class="card-value">{s3_total}</div>
                </div>
                <div class="card">
                    <div class="card-label">S3 Errors</div>
                    <div class="card-value">{s3_errors}</div>
                </div>
                <div class="card">
                    <div class="card-label">S3 Avg Latency</div>
                    <div class="card-value">{s3_latency:.1f} ms</div>
                </div>
            </div>
            <table>
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
                    {"".join(pod_rows)}
                </tbody>
            </table>
        </section>
        """
