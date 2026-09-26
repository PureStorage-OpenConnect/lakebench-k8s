"""Run command implementation -- extracted from cli/__init__.py."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.panel import Panel

from lakebench.cli._helpers import (
    _journal_safe,
    console,
    journal_open,
    print_error,
    print_info,
    print_success,
    print_warning,
    resolve_config_path,
)
from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    load_config,
    parse_spark_memory,
)
from lakebench.config.schema import PipelineMode
from lakebench.journal import CommandName, EventType
from lakebench.k8s import K8sConnectionError

logger = logging.getLogger(__name__)


def _load_latest_datagen_fleet(namespace: str | None = None) -> dict | None:
    """Load the per-pod datagen metrics sidecar written by `lakebench generate`.

    Sidecar is keyed by namespace, so `lakebench run` only ever attributes
    fleet metrics that come from a `lakebench generate` against the same
    namespace as the run. This closes the cross-run silent-attribution hole
    that would let two parallel UAT runs pollute each other's scorecards.

    Returns None if the sidecar for `namespace` does not exist, cannot be
    parsed, or does not carry a matching `namespace` payload. Never raises;
    the fleet rollup is best-effort enrichment for the pipeline scorecard.
    """
    import json as _json

    from lakebench._constants import DEFAULT_OUTPUT_DIR

    if not namespace:
        return None
    path = Path(DEFAULT_OUTPUT_DIR) / "datagen" / f"{namespace}-datagen-metrics.json"
    if not path.exists():
        return None
    try:
        data = _json.loads(path.read_text())
    except Exception as e:  # noqa: BLE001 -- best-effort
        logger.warning("could not read datagen fleet sidecar %s: %s", path, e)
        return None
    payload_ns = data.get("namespace")
    if payload_ns and payload_ns != namespace:
        logger.warning(
            "datagen fleet sidecar %s has namespace %r but run is in %r; ignoring",
            path,
            payload_ns,
            namespace,
        )
        return None
    return data


def _print_pipeline_scorecard(
    pb,
    stage_results: list[tuple[str, bool, float]],
    datagen_elapsed: float,
    benchmark_qph: float | None,
) -> None:
    """Print the pipeline complete panel with full scorecard."""
    # Stage timing lines
    lines: list[str] = []
    if datagen_elapsed > 0:
        lines.append(f"  data-generation {datagen_elapsed:>8.0f}s")
    for name, _ok, elapsed in stage_results:
        lines.append(f"  {name:<16}{elapsed:>8.0f}s")
    total_time = datagen_elapsed + sum(r[2] for r in stage_results)

    body = "[green]Pipeline completed successfully[/green]\n\n"
    body += "\n".join(lines)

    # Scores section
    scores: list[str] = []
    if pb.pipeline_mode == PipelineMode.SUSTAINED.value:
        if (pb.data_freshness_seconds or 0) > 0:
            scores.append(f"  Freshness:      {pb.data_freshness_seconds:>8.1f}s")
        if pb.sustained_throughput_rps > 0:
            scores.append(f"  Throughput:     {pb.sustained_throughput_rps:>8,.0f} rows/s")
        if pb.stage_latency_profile:
            lat = "/".join(f"{v:.0f}" for v in pb.stage_latency_profile)
            scores.append(f"  Latency (b/s/g):  {lat}ms")
        if pb.ingest_ratio is None:
            scores.append("  Completeness:   [dim]unmeasured[/dim]")
        elif pb.ingest_ratio > 0:
            pct = pb.ingest_ratio * 100
            scores.append(f"  Completeness:   {pct:>7.1f}%")
        # pipeline_saturated is bool | None. None means unmeasurable and must
        # not be silently coerced to "not saturated" via a truthy check.
        if pb.pipeline_saturated is True:
            scores.append("  [yellow]Pipeline saturated (completeness < 95%)[/yellow]")
            if pb.intake_limit == "bronze_capacity":
                scores.append("  [dim]Bronze ran back to back: its processing is the limit[/dim]")
            elif pb.intake_limit == "trickle_rate":
                scores.append(
                    "  [dim]Intake held to the trickle rate; silver did not keep pace with it[/dim]"
                )
        elif pb.intake_limit == "trickle_rate":
            scores.append(
                "  [dim]Intake held to the trickle rate, not saturated: "
                "rows/s is the offered load[/dim]"
            )
        if pb.time_to_detect_seconds is not None:
            scores.append(
                f"  Time to detect: {pb.time_to_detect_seconds:>8.1f}s p50, "
                f"{pb.time_to_detect_p95_seconds or 0:.0f}s p95"
            )
    else:
        if pb.time_to_value_seconds > 0:
            scores.append(f"  Time to Value:  {pb.time_to_value_seconds:>8.1f}s")
        if pb.pipeline_throughput_gb_per_second > 0:
            scores.append(f"  Throughput:     {pb.pipeline_throughput_gb_per_second:>8.3f} GB/s")
        if pb.total_data_processed_gb > 0:
            scores.append(f"  Data Processed: {pb.total_data_processed_gb:>8.1f} GB")
        if pb.compute_efficiency_gb_per_core_hour > 0:
            scores.append(
                f"  Efficiency:     {pb.compute_efficiency_gb_per_core_hour:>8.3f} GB/core-hr"
            )
        if pb.scale_ratio > 0:
            pct = pb.scale_ratio * 100
            label = "[green]verified[/green]" if pct >= 95 else "[yellow]incomplete[/yellow]"
            scores.append(f"  Scale:          {pct:>7.1f}% {label}")

    if benchmark_qph is not None:
        scores.append(f"  QpH:            {benchmark_qph:>8,.1f}")

    if scores:
        body += "\n\n[bold]Scores[/bold]\n" + "\n".join(scores)

    body += f"\n\nTotal: {total_time:.0f}s\n\nFull report: [bold]lakebench report[/bold]"

    console.print()
    console.print(Panel(body, title="Pipeline Complete", expand=False))


def _run_preflight_infra_check(cfg) -> None:
    """Check that required infrastructure is deployed before running the pipeline.

    Verifies the namespace, Postgres, configured catalog, and configured query
    engine are present and ready. Exits with actionable guidance if anything is
    missing.
    """
    ns = cfg.get_namespace()

    try:
        # Import via lakebench.cli so tests can patch lakebench.cli.get_k8s_client
        import lakebench.cli as _cli_mod

        k8s = _cli_mod.get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=ns,
        )
    except K8sConnectionError as e:
        print_error(f"Cannot connect to Kubernetes: {e}")
        print_info("Check your kubectl context and cluster connectivity")
        raise typer.Exit(1) from None

    # 1. Namespace must exist
    if not k8s.namespace_exists(ns):
        print_error(f"Namespace '{ns}' does not exist")
        print_info("Run 'lakebench deploy' to create the deployment first")
        raise typer.Exit(1)

    # 2. Build config-aware list of required components
    from kubernetes import client as k8s_client

    apps_v1 = k8s_client.AppsV1Api()

    required: list[tuple[str, str, str]] = [
        ("lakebench-postgres", "StatefulSet", "PostgreSQL"),
    ]
    cat = cfg.architecture.catalog.type.value
    if cat == "hive":
        required.append(("lakebench-hive-metastore-default", "StatefulSet", "Hive Metastore"))
    elif cat == "polaris":
        required.append(("lakebench-polaris", "Deployment", "Polaris Catalog"))

    engine = cfg.architecture.query_engine.type.value
    if engine == "trino":
        required.append(("lakebench-trino-coordinator", "Deployment", "Trino coordinator"))
        required.append(("lakebench-trino-worker", "StatefulSet", "Trino workers"))
    elif engine == "spark-thrift":
        required.append(("lakebench-spark-thrift", "Deployment", "Spark Thrift Server"))
    elif engine == "duckdb":
        required.append(("lakebench-duckdb", "Deployment", "DuckDB"))

    # 3. Check each component
    missing: list[str] = []
    not_ready: list[str] = []
    for name, kind, label in required:
        try:
            if kind == "StatefulSet":
                obj = apps_v1.read_namespaced_stateful_set(name, ns)
                ready = obj.status.ready_replicas or 0
                desired = obj.spec.replicas or 1
            else:
                obj = apps_v1.read_namespaced_deployment(name, ns)
                ready = obj.status.ready_replicas or 0
                desired = obj.spec.replicas or 1
            if ready < desired:
                not_ready.append(f"{label} ({ready}/{desired} replicas ready)")
        except k8s_client.rest.ApiException as e:
            if e.status == 404:
                missing.append(label)
            else:
                logger.warning("Preflight check error for %s: %s", name, e.reason)
                not_ready.append(f"{label} (API error: {e.reason})")

    # 4. Report and block
    if missing or not_ready:
        print_error("Infrastructure not ready -- cannot run pipeline")
        if missing:
            console.print(f"  [red]Missing:[/red] {', '.join(missing)}")
        if not_ready:
            console.print(f"  [yellow]Not ready:[/yellow] {', '.join(not_ready)}")
        console.print()
        if missing:
            print_info("Run 'lakebench deploy' to create the missing components")
        else:
            print_info("Wait for components to become ready, or check 'lakebench status'")
        # Show config mismatch hint if catalog/engine might be wrong
        console.print(f"  Config expects: catalog={cat}, query_engine={engine} (namespace: {ns})")
        raise typer.Exit(1)

    print_success("Infrastructure check passed")


def _record_local_jobs(collector, cfg, result) -> None:
    """Record one JobMetrics per stage from a local run.

    Resources come from the local profile rather than the cluster one. A
    ``local[N]`` job has no executors, so executor_count is 1 and the memory
    figure is the single JVM heap -- reporting the cluster's 8 x 48g here would
    make compute_efficiency_gb_per_core_hour meaningless.
    """
    from datetime import timedelta

    from lakebench.metrics import JobMetrics
    from lakebench.modules.pipeline_engines.spark.job import get_local_job_profile

    now = datetime.now()
    for stage_name, ok, elapsed in result.stages:
        profile = get_local_job_profile(stage_name) or {}
        cores = int(profile.get("cores", 2))
        memory_gb = float(str(profile.get("driver_memory", "2g")).rstrip("g"))

        # Input sizes and row counts only exist in the driver's own output.
        # Reuse the cluster parser so both paths read the same log block; fall
        # back to a bare record if the job died before printing it.
        driver_output = result.logs.get(stage_name, "")
        metrics = (
            collector.parse_driver_logs(driver_output, stage_name)
            if driver_output
            else JobMetrics(job_name=f"lakebench-{stage_name}", job_type=stage_name)
        )

        # Wall-clock from the runner wins over the script's self-reported
        # figure: it includes JVM start and jar resolution, which the user
        # waits through and the script never sees.
        metrics.elapsed_seconds = elapsed
        metrics.start_time = now - timedelta(seconds=elapsed)
        metrics.end_time = now
        metrics.success = ok
        metrics.executor_count = 1
        metrics.executor_cores = cores
        metrics.executor_memory_gb = memory_gb
        metrics.cpu_seconds_requested = cores * elapsed
        metrics.memory_gb_requested = memory_gb
        if elapsed > 0:
            metrics.throughput_gb_per_second = metrics.input_size_gb / elapsed
            metrics.throughput_rows_per_second = metrics.input_rows / elapsed

        collector.record_job(metrics)


def _record_local_queries(collector, cfg, bench_results, qph: float) -> None:
    """Record benchmark queries so `results` and `compare` can read them.

    Shaped exactly like the cluster path's record: queries are dicts, not
    QueryMetrics, and the single-round case goes through record_benchmark.
    """
    from lakebench.metrics import BenchmarkMetrics

    if not bench_results:
        return

    collector.record_benchmark(
        BenchmarkMetrics(
            mode="local",
            cache="cold",
            # This field is int and display-only. A sub-1 local scale would
            # truncate to 0 and read as "no data", so floor at 1; the exact
            # value stays in the config snapshot, which is what compare reads.
            scale=max(1, int(cfg.architecture.workload.datagen.scale)),
            qph=qph,
            total_seconds=sum(elapsed for _, _, elapsed, _ in bench_results),
            queries=[
                {
                    "query_name": name,
                    "elapsed_seconds": elapsed,
                    "success": ok,
                    "rows_returned": rows,
                }
                for name, ok, elapsed, rows in bench_results
            ],
            iterations=1,
        )
    )


def _save_local_metrics(
    collector,
    metrics_storage,
    cfg,
    deployment,
    success: bool,
    datagen_elapsed: float,
):
    """Measure layer sizes, build the scorecard, and persist. Returns the path."""
    from lakebench.s3 import S3Client

    run_metrics = collector.end_run(success=success)
    if not run_metrics:
        return None

    buckets = {
        "bronze": cfg.platform.storage.s3.buckets.bronze,
        "silver": cfg.platform.storage.s3.buckets.silver,
        "gold": cfg.platform.storage.s3.buckets.gold,
    }
    try:
        client = S3Client(
            endpoint=deployment.endpoint,
            access_key=deployment.credentials.access_key,
            secret_key=deployment.credentials.secret_key,
            region=deployment.credentials.region,
            path_style=True,
        )
        collector.current_run = run_metrics
        collector.record_actual_sizes_local(client, buckets)
    except Exception as e:  # noqa: BLE001 -- sizes are best-effort
        logger.warning("Could not measure local bucket sizes: %s", e)

    try:
        from lakebench.metrics import build_pipeline_benchmark

        fleet = _load_latest_datagen_fleet(cfg.get_namespace())
        if fleet is not None:
            run_metrics.datagen_fleet = fleet
        run_metrics.pipeline_benchmark = build_pipeline_benchmark(
            run_metrics,
            datagen_elapsed=datagen_elapsed,
            datagen_output_gb=run_metrics.bronze_size_gb,
            datagen_fleet=fleet,
        )
    except Exception as e:  # noqa: BLE001
        console.print(f"  [yellow]Could not build pipeline benchmark: {e}[/yellow]")

    try:
        return metrics_storage.save_run(run_metrics)
    except Exception as e:  # noqa: BLE001
        console.print(f"  [yellow]Could not save metrics: {e}[/yellow]")
        return None


def _apply_parsed_job_metrics(job_metrics, parsed) -> None:
    """Copy the data + AML detection fields parsed from driver logs onto the
    stage's JobMetrics.

    Kept as one function, unit-tested, so a field that parse_driver_logs
    populates can never again be silently dropped by the cluster run path. The
    original LB-123 defect was exactly that: a hand-written field-by-field copy
    omitted the three detection dicts, so the disk-saved scorecard showed 0
    alerts and lost skip reasons on every real cluster run.
    """
    job_metrics.input_size_gb = parsed.input_size_gb
    job_metrics.output_size_gb = parsed.output_size_gb
    job_metrics.input_rows = parsed.input_rows
    job_metrics.output_rows = parsed.output_rows
    job_metrics.throughput_gb_per_second = parsed.throughput_gb_per_second
    job_metrics.throughput_rows_per_second = parsed.throughput_rows_per_second
    # AML per-rule detection metrics (LB-116). The ONLY source of alert counts
    # + skip reasons for the scorecard; must be carried or the report shows
    # every rule "0 alerts" and drops skips (defeating the LB-119
    # never-misreport-a-skip invariant).
    job_metrics.alerts_by_rule = parsed.alerts_by_rule
    job_metrics.rule_errors = parsed.rule_errors
    job_metrics.rules_skipped = parsed.rules_skipped
    # P10 TM operations invariants and summary; the batch gate reads them.
    job_metrics.tm_invariants = parsed.tm_invariants
    job_metrics.tm_ops = parsed.tm_ops
    job_metrics.tm_status = parsed.tm_status


# Upstream failures a benchmark may carry without failing the run, as
# (table format, query engine, query name). Each must be a documented bug
# outside lakebench. Delta + Thrift Q2 (CLAUDE.md gotcha 22 / LB-034) left
# this list with LB-148: the metadata-query rewrite that crashes it is now
# disabled, so a Q2 failure there is a regression, not an upstream bug.
_KNOWN_QUERY_FAILURES: set[tuple[str, str, str]] = set()


def _paired_qph(pre, post) -> tuple[float, float, int] | None:
    """(pre QpH, post QpH, n) over the queries that succeeded in BOTH runs.

    Each run's own QpH averages over its own successful queries, so a query
    that failed before and passed after changed the query set, not the speed.
    None when no query succeeded in both.
    """
    pre_ok = {q.query.name: q.elapsed_seconds for q in pre if q.success}
    post_ok = {q.query.name: q.elapsed_seconds for q in post if q.success}
    common = sorted(set(pre_ok) & set(post_ok))
    pre_s = sum(pre_ok[n] for n in common)
    post_s = sum(post_ok[n] for n in common)
    if not common or pre_s <= 0 or post_s <= 0:
        return None
    return len(common) / pre_s * 3600, len(common) / post_s * 3600, len(common)


def _data_file_total(health: dict[str, int]) -> int:
    """Data files across the probed tables; 0 (unknown) when any probe failed.

    ``_probe_table_health`` reports a failed probe as -1. Summed in, it
    shifted the total by one per failure, and a probe that failed on one
    side only read as a file-count change.
    """
    counts = [v for k, v in health.items() if "file_count" in k and isinstance(v, int)]
    if not counts or any(v < 0 for v in counts):
        return 0
    return sum(counts)


def _maintenance_value(
    pre, post, pre_files: int, post_files: int, maint_elapsed: float, settle=None
) -> tuple[float | None, int, str]:
    """(value %, paired queries, reason) for the pre/post maintenance rounds.

    The value is None, with the reason, unless maintenance ran and compaction
    reduced the data file count. With the file count unchanged the two rounds
    differ only in what compaction did not change, and the difference is
    run-to-run noise: on 2026-09-24 four runs with the same file count before
    and after read -12.4%, -8.9%, +31.2% and +46.8% (LB-141).

    *settle* is the ``SettleResult`` of the wait before the post round, or
    None when the wait was disabled. A wait that did not settle leaves the
    value None: the post round measured storage still working off the
    maintenance burst (LB-150).
    """
    if maint_elapsed <= 0:
        return None, 0, "maintenance did not run"
    if pre_files <= 0 or post_files <= 0:
        return None, 0, "data file counts unavailable"
    if post_files >= pre_files:
        return None, 0, f"compaction changed no files ({pre_files:,} -> {post_files:,})"
    paired = _paired_qph(pre, post)
    if paired is None:
        return None, 0, "no query succeeded in both rounds"
    pre_q, post_q, n = paired
    if settle is not None and not settle.settled:
        return None, n, settle.value_reason()
    value = (post_q - pre_q) / pre_q * 100
    noise = _paired_noise(pre, post)
    if noise is None:
        return (
            None,
            n,
            f"one sample per query, so the within-round spread is unmeasured "
            f"(raw difference {value:+.1f}%); set architecture.benchmark.iterations >= 2",
        )
    if noise["overlap"]:
        return (
            None,
            n,
            f"within noise: {value:+.1f}% is inside the within-round spread "
            f"(pre {noise['pre_range_pct']:.1f}%, post {noise['post_range_pct']:.1f}% "
            "of median seconds)",
        )
    if abs(value) <= _ROUND_DRIFT_FLOOR_PCT:
        return (
            None,
            n,
            f"within noise: {value:+.1f}% is under the {_ROUND_DRIFT_FLOOR_PCT:g}% "
            "drift between rounds of the same run",
        )
    return value, n, ""


# Samples inside a round run back to back, so their range misses drift
# between rounds: two rounds of the same run with nothing changed between
# them differed 3-11% in QpH on the live cluster (GOALS repeatability entry,
# 2026-09-25). A difference within this floor is not reported as a
# maintenance effect even when the within-round ranges do not overlap.
# Replace with the measured spread once the repeatability runs land.
_ROUND_DRIFT_FLOOR_PCT = 11.0


def _paired_noise(pre, post) -> dict[str, Any] | None:
    """Within-round spread of the paired queries, or None when unmeasured.

    For the queries that succeeded in both rounds, each round's total seconds
    can land anywhere between the sum of per-query fastest samples and the
    sum of per-query slowest samples. When those two ranges overlap, the
    rounds are not distinguishable at the repeats taken and the difference is
    reported as noise. With one sample per query there is no range to
    compare, so the spread is unmeasured (None).
    """
    pre_ok = {q.query.name: q for q in pre if q.success}
    post_ok = {q.query.name: q for q in post if q.success}
    common = sorted(set(pre_ok) & set(post_ok))
    if not common:
        return None
    if (
        min(len(pre_ok[c].sample_times()) for c in common) < 2
        or min(len(post_ok[c].sample_times()) for c in common) < 2
    ):
        return None

    def _band(results: dict) -> tuple[float, float, float]:
        lo = sum(min(results[c].sample_times()) for c in common)
        hi = sum(max(results[c].sample_times()) for c in common)
        med = sum(results[c].elapsed_seconds for c in common)
        return lo, hi, med

    pre_lo, pre_hi, pre_med = _band(pre_ok)
    post_lo, post_hi, post_med = _band(post_ok)
    return {
        "overlap": not (post_hi < pre_lo or post_lo > pre_hi),
        "pre_range_pct": (pre_hi - pre_lo) / pre_med * 100 if pre_med > 0 else 0.0,
        "post_range_pct": (post_hi - post_lo) / post_med * 100 if post_med > 0 else 0.0,
    }


def _sample_note(kwargs: dict) -> str:
    """ " (median of 3, 9.8-12.1s)" for a repeated query; "" for one sample."""
    samples = kwargs.get("samples") or []
    if len(samples) < 2:
        return ""
    return f" [dim](median of {len(samples)}, {min(samples):.1f}-{max(samples):.1f}s)[/dim]"


def _warm_benchmark(runner, query_timeout: int) -> None:
    """One unmeasured pass, so a measured round does not pay first-touch costs.

    Before this, the pre-maintenance round was the first query ever against
    the new tables (cold metadata and file caches) and the post round the
    first against the post-maintenance snapshot; each paid a different set of
    first-touch costs, and the difference was read as the maintenance value.
    """
    try:
        # One sample: the pass exists to touch every table, not to be timed.
        runner.run_power(cache="hot", query_timeout=query_timeout, iterations=1)
    except Exception as e:  # noqa: BLE001
        logger.warning("benchmark warm-up pass failed: %s", e)


def _settle_after_maintenance(
    cfg, runner, pre_queries, query_timeout: int, started_at: float, *, clock=None, sleep=None
):
    """Probe until storage settles after batch maintenance (LB-150).

    Returns the ``SettleResult``, or None when the wait is disabled. Raises
    ValueError when the configured probe query is not in the query set. *started_at* is ``time.monotonic()`` at
    maintenance end. The wait is not a pipeline stage, so it adds to the
    run's wall clock but not to time to value or any stage time.
    """
    from lakebench.benchmark.settle import wait_for_settle

    sc = cfg.architecture.benchmark.maintenance_settle
    if not sc.enabled:
        print_info("Storage settle wait: disabled (benchmark.maintenance_settle.enabled)")
        return None
    query = runner.probe_query(sc.probe_query)  # ValueError: recorded by the caller

    # The same query's pre-maintenance median, when the pre round ran and the
    # query succeeded there.
    reference = None
    for q in pre_queries or []:
        if q.query.name == query.name and q.success and q.elapsed_seconds > 0:
            reference = q.elapsed_seconds
    ref_note = f", pre-maintenance {reference:.1f}s" if reference else ", no pre-maintenance time"
    print_info(
        f"Waiting for storage to settle: probe {query.name} every {sc.interval_seconds}s, "
        f"within {sc.tolerance_pct:g}%{ref_note}, cap {sc.max_seconds}s"
    )

    def _probe(remaining: float) -> float:
        # Bound each sample by the time left before the cap, so a hung probe
        # cannot run up to probe_samples * query_timeout past it.
        per_sample = max(30, min(query_timeout, int(remaining / sc.probe_samples) + 1))
        r = runner.time_query(query, iterations=sc.probe_samples, query_timeout=per_sample)
        if not r.success:
            raise RuntimeError(r.error_message or "probe failed")
        return r.elapsed_seconds

    def _show(p) -> None:
        if p.seconds is None:
            console.print(f"  +{p.offset_seconds:.0f}s probe [red]FAIL[/red] ({p.error[:60]})")
        else:
            console.print(f"  +{p.offset_seconds:.0f}s probe {p.seconds:.1f}s")

    kwargs = {}
    if clock is not None:
        kwargs["clock"] = clock
    if sleep is not None:
        kwargs["sleep"] = sleep
    result = wait_for_settle(
        _probe,
        probe_query=query.name,
        started_at=started_at,
        max_seconds=sc.max_seconds,
        interval_seconds=sc.interval_seconds,
        tolerance_pct=sc.tolerance_pct,
        reference_seconds=reference,
        on_probe=_show,
        **kwargs,
    )
    if result.settled and not result.verified:
        print_warning(
            f"Storage probes stable {result.settle_seconds:.0f}s after maintenance, "
            "unverified: no pre-maintenance time to compare against"
        )
    elif result.settled:
        print_info(f"Storage settled {result.settle_seconds:.0f}s after maintenance")
    else:
        print_warning(f"Storage did not settle: {result.reason}; post round runs anyway")
    return result


def _benchmark_gate_problems(cfg, queries) -> list[str]:
    """Reasons the benchmark result is not a valid score.

    QpH is computed over the queries that succeeded, so a run where most
    queries failed still printed a QpH (live AML run, 2026-09-24: 1 of 8
    passed, QpH 166, exit 0). Every failure outside the known upstream list
    fails the run.
    """
    fmt = cfg.architecture.table_format.type.value
    engine = cfg.architecture.query_engine.type.value

    # QueryResult objects (batch) or their to_dict() form (in-stream rounds).
    def _name(q):
        return q["name"] if isinstance(q, dict) else q.query.name

    def _ok(q):
        return bool(q["success"] if isinstance(q, dict) else q.success)

    bad = [
        q for q in queries if not _ok(q) and (fmt, engine, _name(q)) not in _KNOWN_QUERY_FAILURES
    ]
    if not bad:
        return []
    names = ", ".join(_name(q) for q in bad)
    return [
        f"Benchmark gate: {len(bad)} of {len(queries)} queries failed ({names}); "
        "QpH over the rest is not a valid score. Marking FAILURE."
    ]


def _aml_batch_gate_problems(
    gold_jobs: list, scoring: dict | None = None
) -> tuple[list[str], list[str]]:
    """Reasons an AML batch run must not be reported as a success.

    Returns ``(problems, warnings)``. Uses the last gold-finalize job (it
    re-detects over the whole corpus, so last-cycle-wins). A crashed rule
    fails the run. Zero alerts fails it, judged from the scorer's own alert
    count when scoring ran; per-rule counts parsed from driver logs are the
    fallback. Missing logs with no scoring result is unknown, a warning, not
    proof that nothing was detected.
    """
    if not gold_jobs:
        return [], []
    last = gold_jobs[-1]
    errors = dict(getattr(last, "rule_errors", None) or {})
    by_rule = dict(getattr(last, "alerts_by_rule", None) or {})
    problems = [f"Detection rule {rule} failed: {err}" for rule, err in sorted(errors.items())]
    warnings: list[str] = []
    zero = (
        "AML batch run produced zero alerts: detection did not measure "
        "anything. Check the gold-finalize driver log."
    )
    if scoring is not None and scoring.get("total_alerts") is not None:
        if int(scoring["total_alerts"]) == 0:
            problems.append(zero)
    elif by_rule:
        if sum(by_rule.values()) == 0:
            problems.append(zero)
    elif not errors:
        warnings.append(
            "Could not confirm that detection produced alerts: no per-rule "
            "counts in the gold-finalize driver log and no scoring result."
        )
    # A skipped rule is honest ("not run"), but when its designated typology
    # is in the pre-registered behavioural subset the benchmark has no
    # detector for a typology it claims to measure. Say so every run.
    skipped = dict(getattr(last, "rules_skipped", None) or {})
    if skipped:
        from lakebench.benchmark.aml_queries import RULE_TARGETS

        behavioural = _behavioural_subset()
        for rule, reason in sorted(skipped.items()):
            target = RULE_TARGETS.get(rule)
            if target in behavioural:
                warnings.append(
                    f"{rule} not run ({reason}): pre-registered behavioural "
                    f"typology {target} has no detector in this run."
                )
    return problems, warnings


def _aml_tm_verdict(gold_jobs: list, enabled: bool = True) -> dict:
    """The P10 TM operations verdict over every gold-finalize cycle.

    Separate from detection: only ``fail`` (the layer ran and an invariant is
    violated) fails the run. ``not_run`` says why the layer did not run and
    leaves detection scoring alone; ``unknown`` means no driver log was
    parsed, the same treatment continuous gives a missing log.
    """
    from lakebench.metrics.tm_ops import tm_verdict

    inv: dict = {}
    sts: dict = {}
    ops = None
    parsed = False
    unparsed = []
    for idx, job in enumerate(gold_jobs, start=1):
        j_inv = getattr(job, "tm_invariants", None) or {}
        j_sts = getattr(job, "tm_status", None) or {}
        inv.update({int(c): v for c, v in j_inv.items()})
        sts.update({int(c): v for c, v in j_sts.items()})
        ops = getattr(job, "tm_ops", None) or ops
        job_parsed = bool(
            j_inv
            or j_sts
            or getattr(job, "alerts_by_rule", None)
            or getattr(job, "rules_skipped", None)
            or getattr(job, "rule_errors", None)
        )
        parsed = parsed or job_parsed
        if not job_parsed:
            unparsed.append(idx)
    verdict = tm_verdict(inv, sts, enabled=enabled, logs_captured=parsed, label="gold-finalize")
    if unparsed and verdict["status"] == "pass":
        # A cycle whose log was not read was not checked; the others passing
        # does not make the run pass.
        verdict.update(
            status="unknown",
            reason=f"no driver log parsed for gold-finalize job(s) {unparsed}; "
            "those cycles are unchecked",
        )
    verdict["invariants"] = {str(c): v for c, v in sorted(inv.items())}
    verdict["ops"] = ops
    verdict["mode"] = "batch"
    return verdict


def _report_tm_verdict(verdict: dict, label: str) -> bool:
    """Print the P10 verdict; True when it must fail the run."""
    status = verdict.get("status")
    if status == "fail":
        for p in verdict.get("problems") or []:
            print_error(p)
        return True
    if status == "pass":
        print_success(f"{label}: TM operations ran; every workflow invariant passed.")
    elif status == "disabled":
        print_info(f"{label}: TM operations disabled (workload.tm_operations.enabled).")
    else:
        print_warning(
            f"{label}: TM operations {str(status).replace('_', ' ')}: {verdict.get('reason')}. "
            "The P10 gate is not met; detection results are unaffected."
        )
    return False


def _behavioural_subset() -> set[str]:
    """Behavioural typologies from the AML pre-registration file."""
    import json

    from lakebench._resources import get_aml_data_dir

    d = get_aml_data_dir()
    try:
        data = json.loads((d / "aml_preregistration.json").read_text()) if d else {}
    except (OSError, ValueError):
        return set()
    return set(data.get("behavioural_subset", []))


def _run_financial_scoring(cfg, run_id, job_manager, monitor, timeout):
    """Fold ``financial score`` into a batch run (LB-123).

    After gold-finalize, score recall/precision against the datagen manifest
    and return the recall.json summary so the batch scorecard can render real
    recall, not just alert counts. Best-effort: a scoring failure never fails
    the pipeline (the pipeline result is still valid), it just leaves the
    scorecard without recall. Returns the parsed recall.json dict, or None.
    """
    # Whole body is best-effort: NOTHING here (imports, config access, submit,
    # wait, S3 read) may propagate and fail a pipeline that already reported
    # success. One outer try guarantees that.
    try:
        import json as _json

        from lakebench.s3 import S3Client
        from lakebench.spark.job import JobState, JobType

        s3 = cfg.platform.storage.s3
        # Manifest URI mirrors bronze_verify_financial:
        # {bronze}/{prefix}/manifest/manifest.parquet. Datagen maps the C360
        # default path_template ("customer/interactions") to "pacs008".
        prefix = cfg.architecture.pipeline.medallion.bronze.path_template
        if prefix == "customer/interactions":
            prefix = "pacs008"
        prefix = prefix.rstrip("/")
        # Glob over every cycle's manifest (manifest.parquet, manifest-cNNN.parquet).
        manifest_uri = f"s3a://{s3.buckets.bronze}/{prefix}/manifest/manifest*.parquet"
        json_key = f"scoring/{run_id}/recall.json"
        output_uri = f"s3a://{s3.buckets.gold}/scoring/{run_id}/recall.parquet"
        # Derive the SparkApplication name from the enum rather than a literal
        # so it can never drift from submit_job's f"lakebench-{value}".
        app_name = f"lakebench-{JobType.SCORE_FINANCIAL.value}"

        console.print()
        console.print("[bold]Stage: financial score[/bold]")
        print_info("Scoring recall/precision against the datagen manifest...")

        status = job_manager.submit_job(
            JobType.SCORE_FINANCIAL,
            arguments=["--manifest", manifest_uri, "--output", output_uri],
        )
        if status.state == JobState.FAILED:
            print_warning(f"Could not submit score job: {status.message}")
            return None
        result = monitor.wait_for_completion(
            app_name,
            timeout_seconds=timeout,
            poll_interval=15,
        )
        if not result.success:
            print_warning(f"Financial scoring did not complete: {result.message}")
            # Surface the score driver's own error -- scoring is best-effort so
            # its failure is easy to miss, and without the driver tail the only
            # signal is a generic "driver container failed".
            if getattr(result, "driver_logs", None):
                console.print("[dim]Score driver logs (last 25 lines):[/dim]")
                for line in result.driver_logs.split("\n")[-25:]:
                    console.print(f"  {line}")
            return None

        # Read the recall.json sidecar (boto3 only -- the CLI has no
        # pandas/pyarrow to read recall.parquet).
        client = S3Client(
            endpoint=s3.endpoint,
            access_key=s3.access_key,
            secret_key=s3.secret_key,
            region=s3.region,
            path_style=s3.path_style,
            ca_cert=s3.ca_cert,
            verify_ssl=s3.verify_ssl,
        )
        body = client.raw_client.get_object(Bucket=s3.buckets.gold, Key=json_key)["Body"].read()
        summary = _json.loads(body)
        n = len(summary.get("typologies", []))
        print_success(f"Financial scoring complete ({n} typologies scored)")
        return summary
    except Exception as e:  # noqa: BLE001 -- scoring is best-effort enrichment
        print_warning(f"Financial scoring failed ({e}); scorecard will omit recall.")
        return None


def _run_local_mode(
    cfg,
    config_file: Path,
    workdir: Path | None,
    timeout: int | None,
    stage: str | None,
    yes: bool,
    include_datagen: bool = False,
    skip_generate: bool = False,
    skip_benchmark: bool = False,
) -> None:
    """Run the pipeline locally. Raises typer.Exit on failure."""
    from lakebench.cli._local import (
        LOCAL_JOB_ORDER,
        LocalModeError,
        benchmark_local,
        check_local_supported,
        default_workdir,
        deploy_local,
        generate_local,
        print_local_benchmark,
        print_local_summary,
        run_local,
        scale_advisory,
    )

    try:
        check_local_supported(cfg)
    except LocalModeError as e:
        print_error(str(e))
        raise typer.Exit(1)  # noqa: B904

    if stage and stage not in LOCAL_JOB_ORDER:
        print_error(f"Unknown stage {stage!r}. Expected one of: {', '.join(LOCAL_JOB_ORDER)}")
        raise typer.Exit(1)
    stages = (stage,) if stage else LOCAL_JOB_ORDER

    advisory = scale_advisory(cfg)
    if advisory:
        console.print(f"[yellow]{advisory}[/yellow]")
        if not yes:
            typer.confirm("Continue anyway?", abort=True)

    resolved_workdir = workdir or default_workdir(cfg.name)

    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.RUN, {"local": True, "stages": list(stages)})

    # Same collector and storage the cluster path uses, so `results`,
    # `report`, and `compare` read local runs without special-casing them.
    import uuid as _uuid

    from lakebench.metrics import MetricsCollector, MetricsStorage, build_config_snapshot

    collector = MetricsCollector()
    metrics_storage = MetricsStorage()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + _uuid.uuid4().hex[:6]
    snapshot = build_config_snapshot(cfg)
    snapshot["local"] = True
    collector.start_run(run_id, cfg.name, snapshot)

    # Reconnect to the running stack. Garage keeps metadata on the host, so
    # this reuses the existing key and buckets rather than minting new ones.
    try:
        deployment = deploy_local(cfg, workdir=resolved_workdir, timeout=180)
    except Exception as e:  # noqa: BLE001 -- container CLI failures vary widely
        print_error(f"Could not reach the local stack: {e}")
        print_info(f"Run 'lakebench deploy {config_file} --local' first")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904

    datagen_elapsed = 0.0
    if include_datagen and not skip_generate:
        console.print()
        datagen_start = time.time()
        if not generate_local(cfg, deployment, timeout=timeout or 3600):
            _journal_safe(j.end_command, success=False, message="Datagen failed")
            raise typer.Exit(1)
        datagen_elapsed = time.time() - datagen_start

    console.print()
    result = run_local(
        cfg,
        deployment,
        workdir=resolved_workdir,
        timeout=timeout or 3600,
        stages=stages,
    )
    print_local_summary(result)
    _record_local_jobs(collector, cfg, result)

    _journal_safe(
        j.record,
        EventType.PIPELINE_COMPLETE,
        message=("Local pipeline completed" if result.success else "Local pipeline failed"),
        success=result.success,
        details={
            "local": True,
            "elapsed_seconds": result.elapsed_seconds,
            "stages": [{"stage": n, "success": ok, "elapsed": e} for n, ok, e in result.stages],
        },
    )
    # Benchmark only after a clean pipeline: querying a half-built gold table
    # produces a number that looks real and means nothing.
    qph = 0.0
    if result.success and not skip_benchmark and not stage:
        console.print()
        console.print("[bold dim]Benchmark[/bold dim]")
        bench_results, qph = benchmark_local(cfg, deployment, workdir=resolved_workdir)
        print_local_benchmark(bench_results, qph)
        _record_local_queries(collector, cfg, bench_results, qph)
        _journal_safe(
            j.record,
            EventType.BENCHMARK_COMPLETE,
            message=f"Local benchmark: {qph:,.0f} QpH",
            success=bool(bench_results),
            details={
                "local": True,
                "queries_per_hour": qph,
                "queries": [
                    {"name": n, "success": ok, "elapsed": e, "rows": r}
                    for n, ok, e, r in bench_results
                ],
            },
        )

    # Persist on failure too: a partial run is still evidence, and `results`
    # showing nothing after a failed pipeline hides what did complete.
    metrics_path = _save_local_metrics(
        collector,
        metrics_storage,
        cfg,
        deployment,
        success=result.success,
        datagen_elapsed=datagen_elapsed,
    )
    if metrics_path:
        print_info(f"Metrics saved to {metrics_path}")
        print_info(f"Run ID: {run_id}")
        _journal_safe(
            j.record,
            EventType.METRICS_SAVED,
            message=f"Metrics saved for run {run_id}",
            details={"run_id": run_id, "metrics_path": str(metrics_path), "local": True},
        )

    _journal_safe(j.end_command, success=result.success)

    if not result.success:
        raise typer.Exit(1)


def run(
    config_file: Annotated[
        Path | None,
        typer.Argument(
            help="Path to configuration YAML file (default: ./lakebench.yaml)",
        ),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option(
            "--file",
            "-f",
            help="Path to configuration YAML file (alternative to positional argument)",
        ),
    ] = None,
    stage: Annotated[
        str | None,
        typer.Option(
            "--stage",
            "-s",
            help="Run specific stage only (bronze-verify, silver-build, gold-finalize)",
        ),
    ] = None,
    timeout: Annotated[
        int | None,
        typer.Option(
            "--timeout",
            "-t",
            help="Timeout per job in seconds (auto-scaled from data scale if omitted)",
        ),
    ] = None,
    skip_benchmark: Annotated[
        bool,
        typer.Option(
            "--skip-benchmark",
            help="Skip the query benchmark after pipeline completion",
        ),
    ] = False,
    sustained: Annotated[
        bool,
        typer.Option(
            "--sustained",
            help="Run in continuous (sustained) mode: bronze-ingest -> silver-stream -> gold-refresh",
        ),
    ] = False,
    continuous: Annotated[
        bool,
        typer.Option(
            "--continuous",
            help="Deprecated alias for --sustained",
            hidden=True,
        ),
    ] = False,
    duration: Annotated[
        int | None,
        typer.Option(
            "--duration",
            help="Continuous run duration in seconds (default: from config, typically 1800)",
        ),
    ] = None,
    include_datagen: Annotated[
        bool,
        typer.Option(
            "--generate",
            help="Run datagen before pipeline stages (batch mode only; sustained always runs datagen)",
        ),
    ] = False,
    skip_deploy: Annotated[
        bool,
        typer.Option(
            "--skip-preflight",
            "--skip-deploy",
            help="Skip prerequisite checks and infrastructure validation",
        ),
    ] = False,
    skip_generate: Annotated[
        bool,
        typer.Option(
            "--skip-generate",
            help="Assume data already exists in bronze bucket",
        ),
    ] = False,
    skip_maintenance: Annotated[
        bool,
        typer.Option(
            "--skip-maintenance",
            help="Skip pre-benchmark maintenance (compaction, snapshot expiry)",
        ),
    ] = False,
    force_reset: Annotated[
        bool,
        typer.Option(
            "--force-reset",
            help=(
                "Continuous c360 only: allow the run to drop existing bronze_raw, silver "
                "and gold tables, stream checkpoints and raw data before starting"
            ),
        ),
    ] = False,
    deploy_only: Annotated[
        bool,
        typer.Option(
            "--deploy-only",
            help="Deploy infrastructure and exit (do not generate or run pipeline)",
        ),
    ] = False,
    generate_only: Annotated[
        bool,
        typer.Option(
            "--generate-only",
            help="Deploy + generate data and exit (do not run pipeline)",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip all confirmation prompts",
        ),
    ] = False,
    local: Annotated[
        bool,
        typer.Option(
            "--local",
            help="Run locally with podman/docker instead of Kubernetes",
        ),
    ] = False,
    workdir: Annotated[
        Path | None,
        typer.Option(
            "--workdir",
            help="Host directory for local mode state (default: ~/.lakebench/local/<name>)",
        ),
    ] = None,
) -> None:
    """Execute the data pipeline.

    Runs the medallion pipeline (bronze -> silver -> gold).
    Each stage is a separate Spark job submitted to the cluster.
    Metrics are automatically collected and saved for reporting.
    After gold finalize, runs a query benchmark (QpH).
    Use --skip-benchmark to skip the benchmark stage.

    With --generate (batch mode), generates data first, then runs the full
    pipeline. Sustained mode always runs datagen automatically.

    With --sustained, runs the continuous pipeline instead:
    starts datagen, then launches bronze-ingest, silver-stream,
    and gold-refresh as concurrent streaming jobs. Monitors for
    the configured duration, then stops streaming and runs benchmark.
    """
    import uuid

    from lakebench.cli._sustained import (
        _collect_platform_metrics,
        _probe_table_health,
        _run_iceberg_compaction,
        _run_iceberg_maintenance,
        _run_sustained,
        _wait_for_query_engine_ready,
        resolve_maintenance_retention,
    )
    from lakebench.engine import get_engine
    from lakebench.metrics import JobMetrics, MetricsCollector, MetricsStorage
    from lakebench.spark import SparkJobMonitor, SparkOperatorManager
    from lakebench.spark.job import (
        JobState,
        JobType,
        SparkJobManager,
        get_executor_count,
        get_job_profile,
    )

    config_file = resolve_config_path(config_file, file_option)

    # Load configuration
    try:
        cfg = load_config(config_file)
    except ConfigFileNotFoundError as e:
        print_error(f"File not found: {e}")
        raise typer.Exit(1)  # noqa: B904
    except ConfigValidationError as e:
        print_error("Config validation failed:")
        for err in e.errors:
            loc = ".".join(str(x) for x in err["loc"])
            console.print(f"  [red]*[/red] {loc}: {err['msg']}")
        raise typer.Exit(1)  # noqa: B904
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    # Local mode runs before auto-sizing: there is no cluster to size against,
    # and the local profiles are fixed rather than derived from capacity.
    if local:
        _run_local_mode(
            cfg,
            config_file,
            workdir,
            timeout,
            stage,
            yes,
            include_datagen=include_datagen,
            skip_generate=skip_generate,
            skip_benchmark=skip_benchmark,
        )
        return

    # Auto-size resources based on scale + cluster capacity
    from lakebench.config.autosizer import resolve_auto_sizing

    try:
        from lakebench.k8s import get_k8s_client

        k8s_for_cap = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        cluster_cap = k8s_for_cap.get_cluster_capacity()
    except Exception as e:
        logger.warning("Could not get cluster capacity for auto-sizing: %s", e)
        cluster_cap = None
    resolve_auto_sizing(cfg, cluster_cap)

    # Auto-scale timeout if not explicitly set
    if timeout is None:
        scale = cfg.architecture.workload.datagen.get_effective_scale()
        # int() because scale is a float: a float timeout would propagate into
        # manifests and log lines that expect a whole number of seconds.
        # Financial adds ~15 min of detection rules to gold-finalize on top
        # of the batch stages; add a 900s cushion so gold-finalize does not
        # blow the timeout at scale ~5+ (adversarial-review finding). The
        # cushion is applied uniformly since the timeout is per-job and
        # gold-finalize is the tightest budget in the pipeline.
        base = max(3600, int(scale * 120))
        is_financial = cfg.architecture.workload.schema_type.value == "financial"
        detection_cushion = 900 if is_financial else 0
        timeout = base + detection_cushion
        if is_financial:
            # This one per-job timeout is applied to EVERY batch stage, and
            # AML bronze-verify (CTAS fallback over the full pacs.008 corpus,
            # measured 4278s at scale 10) is the tightest of the three. Floor
            # the budget at the shared bronze-verify budget so bronze-verify
            # keeps real headroom over its measured cost -- base+cushion alone
            # left only 222s (5%) at scale 10, a false-failure risk under a
            # cold Ivy fetch or an OOM retry.
            from lakebench.spark.job import aml_bronze_verify_timeout_budget

            timeout = max(timeout, aml_bronze_verify_timeout_budget(scale))
        if scale >= 50 or is_financial:
            print_info(f"Per-job timeout: {timeout}s (auto-scaled for scale {scale})")

    # Flag mutual exclusivity
    if deploy_only and generate_only:
        print_error("--deploy-only and --generate-only are mutually exclusive")
        raise typer.Exit(1)

    # deploy_only: deploy infrastructure and exit
    if deploy_only:
        from lakebench.cli._deploy import deploy as _deploy_cmd

        print_info("--deploy-only: deploying infrastructure...")
        _deploy_cmd(config_file=config_file, yes=yes)
        return

    # generate_only: deploy + generate and exit
    if generate_only:
        from lakebench.cli._deploy import deploy as _deploy_cmd
        from lakebench.cli._generate import generate as _generate_cmd

        print_info("--generate-only: deploying and generating data...")
        _deploy_cmd(config_file=config_file, yes=yes)
        _generate_cmd(config_file=config_file, wait=True, timeout=timeout or 14400, yes=yes)
        return

    # -- Phase 1/7: Prerequisites ------------------------------------------------
    console.print()
    console.print("[bold dim]Phase 1/7: Prerequisites[/bold dim]")

    if not skip_deploy:
        from lakebench.cli._prerequisites import run_prerequisites

        prereq_report = run_prerequisites(cfg)
        for check in prereq_report.checks:
            icon = "[green]+[/green]" if check.passed else "[red]x[/red]"
            console.print(f"  {icon} {check.name}: {check.message}")
            if not check.passed and check.hint:
                for line in check.hint.split("\n"):
                    console.print(f"      [dim]{line}[/dim]")

        if not prereq_report.all_passed:
            print_error("Prerequisites not met -- cannot proceed")
            raise typer.Exit(1)
        print_success("All prerequisites passed")

        # Also run infrastructure readiness check
        # If namespace doesn't exist and --yes is set, auto-deploy first
        ns = cfg.get_namespace()
        try:
            from lakebench.k8s import get_k8s_client

            _k8s_check = get_k8s_client(
                context=cfg.platform.kubernetes.context,
                namespace=ns,
            )
            if not _k8s_check.namespace_exists(ns):
                if yes:
                    from lakebench.cli._deploy import deploy as _deploy_cmd

                    print_info(f"Namespace '{ns}' not found -- auto-deploying...")
                    _deploy_cmd(config_file=config_file, yes=True)
                else:
                    print_error(f"Namespace '{ns}' does not exist")
                    print_info("Run 'lakebench deploy' first, or use --yes to auto-deploy")
                    raise typer.Exit(1)
        except K8sConnectionError:
            pass  # preflight will catch this

        _run_preflight_infra_check(cfg)
    else:
        print_info("Skipping prerequisites (--skip-preflight)")

    # Branch: sustained streaming pipeline (CLI flag overrides config)
    use_sustained = sustained or continuous or cfg.architecture.pipeline.mode == "sustained"
    if use_sustained:
        _run_sustained(
            cfg,
            config_file,
            timeout,
            skip_benchmark,
            duration,
            skip_generate=skip_generate,
            skip_maintenance=skip_maintenance,
            force_reset=force_reset,
        )
        return

    # -- Phase 2/7: Deploy (handled by prerequisite check above) ---------------
    console.print()
    console.print("[bold dim]Phase 2/7: Infrastructure[/bold dim]")
    print_success("Infrastructure verified (deploy with 'lakebench deploy' if needed)")

    console.print(
        Panel(
            f"Running pipeline for: [bold]{cfg.name}[/bold]\n\n"
            f"Stages: bronze-verify \u2192 silver-build \u2192 gold-finalize",
            expand=False,
        )
    )

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.RUN, {"stage": stage, "timeout": timeout})

    # Initialize metrics collection
    collector = MetricsCollector()
    metrics_storage = MetricsStorage()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    from lakebench.metrics import build_config_snapshot

    config_snapshot = build_config_snapshot(cfg)
    collector.start_run(run_id, cfg.name, config_snapshot)

    pipeline_success = True
    _datagen_elapsed = 0.0
    _datagen_output_gb = 0.0
    _datagen_output_rows = 0
    results: list[tuple[str, bool, float]] = []
    benchmark_qph: float | None = None
    _financial_scoring: dict | None = None
    # Set when this run's TM layer ran (verdict pass or fail); the benchmark
    # includes the investigator queries only then.
    _tm_run_id: str | None = None

    try:
        # Check Spark operator
        print_info("Checking Spark Operator...")
        spark_op_cfg = cfg.platform.compute.spark.operator
        operator = SparkOperatorManager(
            namespace=spark_op_cfg.namespace,
            version=spark_op_cfg.version if spark_op_cfg.install else None,
            job_namespace=cfg.get_namespace(),
            kube_context=cfg.platform.kubernetes.context,
        )
        status = operator.check_status()

        if not status.ready:
            hint = ""
            if not status.installed and spark_op_cfg.install:
                hint = " -- run 'lakebench deploy' first to install it"
            print_error(f"Spark Operator not ready: {status.message}{hint}")
            pipeline_success = False
            raise typer.Exit(1)

        # Ensure operator watches the target namespace (always try to heal)
        ns_status = operator.ensure_namespace_watched(can_heal=True)
        if ns_status.watching_namespace is False:
            print_error(ns_status.message)
            pipeline_success = False
            raise typer.Exit(1)

        print_success(f"Spark Operator ready (version: {status.version or 'unknown'})")

        # Initialize job manager
        from lakebench.k8s import get_k8s_client

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )

        job_manager: SparkJobManager = get_engine(cfg, k8s)  # type: ignore[assignment]
        monitor = SparkJobMonitor(cfg, k8s, job_manager=job_manager)

        # Deploy scripts ConfigMap -- must succeed or pipeline jobs will fail
        print_info("Deploying Spark scripts...")
        if not job_manager.deploy_scripts_configmap():
            print_error("Failed to deploy Spark scripts ConfigMap -- pipeline cannot proceed")
            _journal_safe(j.end_command, success=False, message="Scripts ConfigMap deploy failed")
            raise typer.Exit(1)
        print_success("Spark scripts deployed")

        # -- Phase 3/7: Generate data -----------------------------------------------
        console.print()
        console.print("[bold dim]Phase 3/7: Generate[/bold dim]")
        if include_datagen and not skip_generate:
            console.print("[bold]Stage: datagen (ingest)[/bold]")
            print_info("Generating data for pipeline benchmark...")
            datagen_start = datetime.now()
            try:
                import time as _time

                from rich.progress import (
                    BarColumn,
                    Progress,
                    SpinnerColumn,
                    TextColumn,
                    TimeElapsedColumn,
                    TimeRemainingColumn,
                )

                from lakebench.deploy import DatagenDeployer, DeploymentEngine

                dg_engine = DeploymentEngine(cfg)
                datagen_deployer = DatagenDeployer(dg_engine)
                datagen_deployer.deploy()

                # Progress bar (same as standalone generate command)
                _dg_start = _time.time()
                _initial = datagen_deployer.get_progress()
                _total_pods = _initial.get("completions", 1)

                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("{task.completed}/{task.total} pods"),
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    console=console,
                ) as _dg_bar:
                    _dg_task = _dg_bar.add_task("Generating data", total=_total_pods)
                    while _time.time() - _dg_start < timeout:
                        _dg_prog = datagen_deployer.get_progress()
                        if not _dg_prog.get("running", False):
                            if _dg_prog.get("error"):
                                _dg_bar.stop()
                                print_error(_dg_prog["error"])
                                raise typer.Exit(1)
                            _dg_bar.update(_dg_task, completed=_total_pods)
                            break
                        if _dg_prog.get("oom_pods"):
                            _dg_bar.stop()
                            print_error(f"OOMKilled: {', '.join(_dg_prog['oom_pods'])}")
                            raise typer.Exit(1)
                        _dg_bar.update(_dg_task, completed=_dg_prog.get("succeeded", 0))
                        _time.sleep(15)

                datagen_end = datetime.now()
                _datagen_elapsed = (datagen_end - datagen_start).total_seconds()
                print_success(f"Datagen completed in {_datagen_elapsed:.0f}s")

                # Measure bronze bucket after datagen
                try:
                    from lakebench.s3 import S3Client

                    s3_cfg = cfg.platform.storage.s3
                    _s3_dg = S3Client(
                        endpoint=s3_cfg.endpoint,
                        access_key=s3_cfg.access_key,
                        secret_key=s3_cfg.secret_key,
                        region=s3_cfg.region,
                        path_style=s3_cfg.path_style,
                        ca_cert=s3_cfg.ca_cert,
                        verify_ssl=s3_cfg.verify_ssl,
                    )
                    dg_info = _s3_dg.get_bucket_size(s3_cfg.buckets.bronze)
                    if dg_info.size_bytes:
                        _datagen_output_gb = dg_info.size_bytes / (1024**3)
                    # datagen row count is not measurable from S3 metadata.
                    # Leaving _datagen_output_rows at 0 signals "unmeasurable"
                    # so ingest_ratio and pipeline_saturated stay None instead
                    # of being computed against a fictional `scale * 1_500_000`
                    # denominator (LB-044 pattern).
                except Exception as e:
                    logger.warning("Could not measure bronze bucket size: %s", e)
            except Exception as e:
                print_error(f"Datagen failed: {e}")
                pipeline_success = False
                raise typer.Exit(1)  # noqa: B904
        else:
            print_info("Skipped (use --generate to include datagen)")

        # -- Phase 4/7: Pipeline stages ---------------------------------------------
        console.print()
        console.print("[bold dim]Phase 4/7: Pipeline[/bold dim]")
        all_stages = [
            (JobType.BRONZE_VERIFY, "bronze-verify", "Verifying bronze data"),
            (JobType.SILVER_BUILD, "silver-build", "Building silver layer"),
            (JobType.GOLD_FINALIZE, "gold-finalize", "Finalizing gold layer"),
        ]

        if stage:
            stages = [(jt, name, desc) for jt, name, desc in all_stages if name == stage]
            if not stages:
                print_error(f"Unknown stage: {stage}")
                print_info("Valid stages: bronze-verify, silver-build, gold-finalize")
                raise typer.Exit(1)
        else:
            stages = all_stages

        # Multi-cycle batch support (v1.1.0)
        total_cycles = cfg.architecture.pipeline.cycles

        # A continuous gold-refresh left by an aborted run restarts forever
        # (restartPolicy Always) with a fresh run id, and each restart deletes
        # every other run's rows from gold.alerts, including this run's.
        if cfg.architecture.workload.schema_type.value == "financial":
            from lakebench.cli._sustained import _stop_leftover_streams

            _stop_leftover_streams(job_manager, cfg.get_namespace())

        for cycle_idx in range(total_cycles):
            # Track per-cycle metrics (v1.1.0)
            _cycle_start = datetime.now()
            _cycle_jobs: list[JobMetrics] = []
            _cycle_ts_start = ""
            _cycle_ts_end = ""
            _cycle_dg_elapsed = 0.0

            # Cycle header for multi-cycle runs
            if total_cycles > 1:
                console.print()
                console.print(f"[bold cyan]Cycle {cycle_idx + 1}/{total_cycles}[/bold cyan]")

                # Run datagen for this cycle's time window
                try:
                    from lakebench.deploy import (
                        DatagenDeployer,
                        DeploymentEngine,
                        DeploymentStatus,
                    )

                    _cycle_engine = DeploymentEngine(cfg)
                    _cycle_datagen = DatagenDeployer(_cycle_engine)
                    datagen_result = _cycle_datagen.deploy_cycle(cycle_idx, total_cycles)
                    if datagen_result.status != DeploymentStatus.SUCCESS:
                        # Fatal: continuing would rebuild this cycle from the
                        # previous cycle's bronze, and incremental silver would
                        # append it a second time.
                        print_error(
                            f"Datagen cycle {cycle_idx + 1} failed: {datagen_result.message}"
                        )
                        pipeline_success = False
                        break
                    else:
                        ts_start = datagen_result.details.get("timestamp_start", "")
                        ts_end = datagen_result.details.get("timestamp_end", "")
                        _cycle_ts_start = ts_start
                        _cycle_ts_end = ts_end
                        print_info(f"Datagen: {ts_start} to {ts_end}")

                        # Wait for datagen completion
                        _dg_start = _time.time()
                        dg_wait = _cycle_datagen.wait_for_completion(timeout_seconds=timeout)
                        _cycle_dg_elapsed = _time.time() - _dg_start
                        if dg_wait.status != DeploymentStatus.SUCCESS:
                            print_error(f"Datagen did not complete: {dg_wait.message}")
                            pipeline_success = False
                            break
                except Exception as e:
                    print_error(f"Cycle datagen failed: {e}")
                    pipeline_success = False
                    break

            # Cycle env vars for incremental mode (cycles 2+)
            # LB_RUN_ID ties gold.alerts / gold.detection_status rows to this
            # run's metrics.json; without it every pod drew its own uuid.
            cycle_env: dict[str, str] = {"LB_RUN_ID": f"{run_id}-c{cycle_idx + 1}"}
            if cycle_idx > 0:
                cycle_env.update(
                    {
                        "LB_SILVER_INCREMENTAL": "true",
                        "LB_GOLD_INCREMENTAL": "true",
                    }
                )

            # Run each stage
            for job_type, stage_name, description in stages:
                console.print()
                if total_cycles > 1:
                    console.print(f"[bold]Stage: {stage_name} (cycle {cycle_idx + 1})[/bold]")
                else:
                    console.print(f"[bold]Stage: {stage_name}[/bold]")
                print_info(description)

                job_start = datetime.now()

                # Submit job
                job_status = job_manager.submit_job(job_type, cycle_env=cycle_env)
                if job_status.state == JobState.FAILED:
                    print_error(f"Failed to submit job: {job_status.message}")
                    collector.record_job(
                        JobMetrics(
                            job_name=f"lakebench-{stage_name}",
                            job_type=stage_name,
                            start_time=job_start,
                            end_time=datetime.now(),
                            elapsed_seconds=(datetime.now() - job_start).total_seconds(),
                            success=False,
                            error_message=job_status.message,
                        )
                    )
                    pipeline_success = False
                    raise typer.Exit(1)

                print_success(f"Job submitted: lakebench-{stage_name}")

                # Wait for completion -- capture max executor count seen
                _max_executors = 0
                _last_reported_executors = -1

                _last_heartbeat_ts = job_start

                def on_progress(status, _start=job_start, _hb=[job_start]):  # noqa: B006
                    nonlocal _max_executors, _last_reported_executors
                    if status.state == JobState.RUNNING:
                        _max_executors = max(_max_executors, status.executor_count)
                        elapsed = (datetime.now() - _start).total_seconds()
                        if status.executor_count != _last_reported_executors:
                            console.print(f"  Running... (executors: {status.executor_count})")
                            _last_reported_executors = status.executor_count
                            _hb[0] = datetime.now()
                        elif (datetime.now() - _hb[0]).total_seconds() >= 60:
                            console.print(f"  Running... ({int(elapsed)}s elapsed)")
                            _hb[0] = datetime.now()

                result = monitor.wait_for_completion(
                    f"lakebench-{stage_name}",
                    timeout_seconds=timeout,
                    poll_interval=15,
                    progress_callback=on_progress,
                )

                job_end = datetime.now()

                # Build job metrics
                job_metrics = JobMetrics(
                    job_name=f"lakebench-{stage_name}",
                    job_type=stage_name,
                    start_time=job_start,
                    end_time=job_end,
                    elapsed_seconds=result.elapsed_seconds,
                    success=result.success,
                    error_message=result.message if not result.success else None,
                    executor_count=_max_executors,
                )

                # Parse driver logs for data metrics if available
                if result.driver_logs:
                    parsed = collector.parse_driver_logs(result.driver_logs, stage_name)
                    _apply_parsed_job_metrics(job_metrics, parsed)

                # Populate resource metrics from job profile. Pass the schema so
                # AML overrides (e.g. bronze-verify 20Gi, 8-per-100 executors)
                # are reflected -- otherwise the scorecard under-reports the
                # deployed resources (LB-135 review finding).
                _schema = cfg.architecture.workload.schema_type.value
                _profile = get_job_profile(stage_name, _schema)
                if _profile:
                    _scale = cfg.architecture.workload.datagen.get_effective_scale()
                    _expected_executors = get_executor_count(stage_name, _scale, _schema)

                    # Check per-job executor override
                    _override_map = {
                        "bronze-verify": cfg.platform.compute.spark.bronze_executors,
                        "silver-build": cfg.platform.compute.spark.silver_executors,
                        "gold-finalize": cfg.platform.compute.spark.gold_executors,
                    }
                    _override = _override_map.get(stage_name)
                    if _override is not None:
                        _expected_executors = _override

                    # Use deterministic count when progress callback didn't capture
                    if job_metrics.executor_count == 0:
                        job_metrics.executor_count = _expected_executors

                    job_metrics.executor_cores = _profile["executor_cores"]
                    _mem_gb = parse_spark_memory(_profile["executor_memory"]) / (1024**3)
                    _overhead_gb = parse_spark_memory(_profile["executor_memory_overhead"]) / (
                        1024**3
                    )
                    job_metrics.executor_memory_gb = _mem_gb

                    # Requested CPU-seconds and peak memory
                    job_metrics.cpu_seconds_requested = (
                        job_metrics.executor_count
                        * job_metrics.executor_cores
                        * job_metrics.elapsed_seconds
                    )
                    job_metrics.memory_gb_requested = job_metrics.executor_count * (
                        _mem_gb + _overhead_gb
                    )

                # Gold input fallback: gold reads from silver
                if (
                    stage_name == "gold-finalize"
                    and job_metrics.input_size_gb == 0.0
                    and collector.current_run
                ):
                    for _prev in collector.current_run.jobs:
                        if _prev.job_type == "silver-build" and _prev.output_size_gb > 0:
                            job_metrics.input_size_gb = _prev.output_size_gb
                            if job_metrics.elapsed_seconds > 0:
                                job_metrics.throughput_gb_per_second = (
                                    job_metrics.input_size_gb / job_metrics.elapsed_seconds
                                )
                            break

                # Measure per-stage S3 output size
                _stage_bucket_map = {
                    "bronze-verify": cfg.platform.storage.s3.buckets.bronze,
                    "silver-build": cfg.platform.storage.s3.buckets.silver,
                    "gold-finalize": cfg.platform.storage.s3.buckets.gold,
                }
                if stage_name in _stage_bucket_map and job_metrics.output_size_gb == 0:
                    try:
                        from lakebench.s3 import S3Client

                        s3_cfg = cfg.platform.storage.s3
                        _s3 = S3Client(
                            endpoint=s3_cfg.endpoint,
                            access_key=s3_cfg.access_key,
                            secret_key=s3_cfg.secret_key,
                            region=s3_cfg.region,
                            path_style=s3_cfg.path_style,
                            ca_cert=s3_cfg.ca_cert,
                            verify_ssl=s3_cfg.verify_ssl,
                        )
                        _bucket_info = _s3.get_bucket_size(_stage_bucket_map[stage_name])
                        if _bucket_info.size_bytes:
                            job_metrics.output_size_gb = _bucket_info.size_bytes / (1024**3)
                    except Exception as e:
                        logger.warning("Could not measure %s bucket size: %s", stage_name, e)

                collector.record_job(job_metrics)
                _cycle_jobs.append(job_metrics)

                if result.success:
                    print_success(f"{stage_name} completed in {result.elapsed_seconds:.0f}s")
                    results.append((stage_name, True, result.elapsed_seconds))
                    _journal_safe(
                        j.record,
                        EventType.PIPELINE_STAGE,
                        message=f"{stage_name} completed",
                        success=True,
                        details={
                            "stage": stage_name,
                            "success": True,
                            "elapsed_seconds": result.elapsed_seconds,
                            "input_gb": job_metrics.input_size_gb,
                            "output_rows": job_metrics.output_rows,
                        },
                    )
                else:
                    print_error(f"{stage_name} failed: {result.message}")
                    if result.driver_logs:
                        console.print("[dim]Driver logs (last 20 lines):[/dim]")
                        for line in result.driver_logs.split("\n")[-20:]:
                            console.print(f"  {line}")
                    results.append((stage_name, False, result.elapsed_seconds))
                    _journal_safe(
                        j.record,
                        EventType.PIPELINE_STAGE,
                        message=f"{stage_name} failed: {result.message}",
                        success=False,
                        details={
                            "stage": stage_name,
                            "success": False,
                            "elapsed_seconds": result.elapsed_seconds,
                        },
                    )
                    pipeline_success = False
                    raise typer.Exit(1)

            # Record CycleMetrics after all stages for this cycle (v1.1.0)
            if total_cycles > 1:
                _cycle_health: dict[str, int] = {}
                try:
                    _cycle_health = _probe_table_health(cfg, k8s)
                except Exception:
                    pass
                from lakebench.metrics.collector import CycleMetrics as _CM

                _cm = _CM(
                    cycle_index=cycle_idx,
                    timestamp_start=_cycle_ts_start,
                    timestamp_end=_cycle_ts_end,
                    datagen_elapsed_seconds=_cycle_dg_elapsed,
                    jobs=list(_cycle_jobs),
                    table_health=_cycle_health,
                )
                if collector.current_run is not None:
                    collector.current_run.cycles.append(_cm)

        # Summary
        console.print()
        total_time = sum(r[2] for r in results)

        stages_succeeded = sum(1 for _, ok, _ in results if ok)
        stages_failed = sum(1 for _, ok, _ in results if not ok)
        _journal_safe(
            j.record,
            EventType.PIPELINE_COMPLETE,
            message=f"Pipeline complete: {stages_succeeded} stages succeeded",
            success=stages_failed == 0,
            details={
                "run_id": run_id,
                "stages_succeeded": stages_succeeded,
                "stages_failed": stages_failed,
                "total_seconds": total_time,
            },
        )

        # LB-123: fold financial recall scoring into the batch run so the
        # scorecard shows real recall/precision, not just alert counts. Only
        # for a full financial pipeline run (not a single --stage), and only
        # when the pipeline succeeded (gold.alerts + manifest must both exist).
        if (
            cfg.architecture.workload.schema_type.value == "financial"
            and not stage
            and pipeline_success
        ):
            _financial_scoring = _run_financial_scoring(cfg, run_id, job_manager, monitor, timeout)

        # AML batch honesty gate (LB-044 class), after scoring so a single
        # crashed rule does not also throw away the other rules' recall.
        # Crashed rules or zero alerts mean detection measured nothing,
        # whatever the stage exit codes say; skipped rules are "not run".
        if (
            cfg.architecture.workload.schema_type.value == "financial"
            and not stage
            and pipeline_success
            and collector.current_run is not None
        ):
            _gold_jobs = [
                jm
                for jm in collector.current_run.jobs
                if getattr(jm, "job_type", "") == "gold-finalize"
            ]
            _problems, _warnings = _aml_batch_gate_problems(_gold_jobs, _financial_scoring)
            for _w in _warnings:
                print_warning(_w)
            for _problem in _problems:
                print_error(_problem)
                pipeline_success = False
            # P10 TM operations: its own verdict, recorded for the scorecard.
            _tm = _aml_tm_verdict(
                _gold_jobs, enabled=cfg.architecture.workload.tm_operations.enabled
            )
            collector.current_run.tm_operations = _tm
            if _tm.get("status") in ("pass", "fail"):
                # The layer ran for this run: the investigator queries read
                # its tables. Otherwise they are left out of the benchmark.
                _tm_run_id = run_id
            if _report_tm_verdict(_tm, "AML batch gate"):
                pipeline_success = False

        # -- Phase 5/7: Maintenance ------------------------------------------------
        console.print()
        console.print("[bold dim]Phase 5/7: Maintenance[/bold dim]")
        # Flow: [pre-compaction benchmark] -> maintenance -> [post-compaction benchmark]
        # Pre-compaction benchmark only runs at scale < 50 (OOMs at higher scales
        # due to 200K+ uncompacted files overwhelming Trino memory).
        pre_compaction_qph = 0.0
        _pre_record = None
        _pre_result = None
        _maint_value = None
        _maint_end = None
        _settle = None
        pre_file_count = 0
        post_file_count = 0
        maint_elapsed = 0.0

        do_maintenance = (
            not skip_benchmark
            and not skip_maintenance
            and cfg.architecture.pipeline.pre_benchmark_maintenance
        )

        if not do_maintenance:
            if skip_benchmark and cfg.architecture.query_engine.type.value == "none":
                print_info("Skipped (no query engine)")
            elif skip_benchmark:
                print_info("Skipped (--skip-benchmark)")
            elif skip_maintenance:
                print_info("Skipped (--skip-maintenance)")

        if do_maintenance:
            from lakebench.benchmark import BenchmarkRunner as _BR

            def _bench_progress(idx, total, name, phase, **kwargs):
                if phase == "start":
                    console.print(f"  [{idx}/{total}] {name}...", end=" ")
                elif phase == "done":
                    elapsed = kwargs.get("elapsed", 0)
                    success = kwargs.get("success", False)
                    error = kwargs.get("error", "")
                    if success:
                        console.print(f"[green]{elapsed:.1f}s OK[/green]{_sample_note(kwargs)}")
                    else:
                        short_err = (error[:60] + "...") if len(error) > 60 else error
                        console.print(f"[red]FAIL[/red] ({short_err})")

            _scale = cfg.architecture.workload.datagen.get_effective_scale()

            try:
                # 1. Pre-maintenance file count
                try:
                    _pre_health = _probe_table_health(cfg, k8s)
                    pre_file_count = _data_file_total(_pre_health)
                except Exception:
                    pass

                # 2. Pre-compaction benchmark (scale < 50 only)
                if _scale < 50:
                    console.print()
                    console.print("[bold]Pre-compaction benchmark[/bold]")
                    print_info("Benchmarking before maintenance (uncompacted data)...")
                    _pre_runner = _BR(cfg)
                    # LB-117: 60s is too tight for AML pre-compaction
                    # queries even at small scale; bump to 180s for AML.
                    # Same timeout as the post-compaction run: with 180 s here
                    # and 900 s there, a query that timed out before counted
                    # only after, and the maintenance delta read -40% on a
                    # run where every query got faster.
                    _pre_timeout = (
                        900 if cfg.architecture.workload.schema_type.value == "financial" else 300
                    )
                    print_info("Warm-up pass (not measured)...")
                    _warm_benchmark(_pre_runner, _pre_timeout)
                    _pre_result = _pre_runner.run_power(
                        cache="hot",
                        iterations=cfg.architecture.benchmark.iterations,
                        progress_callback=_bench_progress,
                        query_timeout=_pre_timeout,
                    )
                    pre_compaction_qph = _pre_result.qph
                    _pre_record = _pre_result.to_dict()
                    _succeeded = sum(1 for q in _pre_result.queries if q.success)
                    console.print(
                        f"  Pre-compaction QpH: {pre_compaction_qph:.1f} "
                        f"({_succeeded}/{len(_pre_result.queries)} queries succeeded)"
                    )
                else:
                    console.print()
                    print_info(
                        f"Pre-compaction benchmark skipped at scale {_scale} "
                        f"({pre_file_count:,} files) -- runs at scale < 50"
                    )

                # 3. Run maintenance (expire snapshots + compaction)
                console.print()
                console.print("[bold]Running maintenance[/bold]")
                _maint_start = datetime.now()
                _run_iceberg_maintenance(
                    cfg, k8s, console, j, retention_threshold=resolve_maintenance_retention(cfg)
                )
                _run_iceberg_compaction(cfg, k8s, console, j)
                _wait_for_query_engine_ready(cfg, k8s, console, timeout=120)
                maint_elapsed = (datetime.now() - _maint_start).total_seconds()
                _maint_end = time.monotonic()

                # 4. Post-maintenance file count
                try:
                    _post_health = _probe_table_health(cfg, k8s)
                    post_file_count = _data_file_total(_post_health)
                except Exception:
                    pass

            except Exception as e:
                print_warning(f"Maintenance failed (non-fatal): {e}")

            # 5. Wait for storage to settle before the post round (LB-150).
            # Runs after maint_elapsed is taken and outside every stage, so it
            # cannot move time to value or maintenance_pct_of_pipeline.
            if maint_elapsed > 0 and _maint_end is not None:
                console.print()
                console.print("[bold]Storage settle wait[/bold]")
                try:
                    _settle = _settle_after_maintenance(
                        cfg,
                        _BR(cfg),
                        _pre_result.queries if _pre_result is not None else None,
                        900 if cfg.architecture.workload.schema_type.value == "financial" else 300,
                        _maint_end,
                    )
                except Exception as e:  # noqa: BLE001
                    print_warning(f"Storage settle wait failed (non-fatal): {e}")
                    # Recorded as not settled, so the maintenance value is
                    # not reported as if the wait had run.
                    from lakebench.benchmark.settle import SettleResult

                    _sc = cfg.architecture.benchmark.maintenance_settle
                    _settle = SettleResult(
                        probe_query=_sc.probe_query or "",
                        settled=False,
                        settle_seconds=max(0.0, time.monotonic() - _maint_end),
                        capped=False,
                        max_seconds=float(_sc.max_seconds),
                        tolerance_pct=float(_sc.tolerance_pct),
                        reference_seconds=None,
                        reason=f"settle wait failed: {e}",
                        verified=False,
                    )

        # -- Phase 6/7: Benchmark --------------------------------------------------
        console.print()
        console.print("[bold dim]Phase 6/7: Benchmark[/bold dim]")
        # Run post-compaction benchmark (or the only benchmark if maintenance skipped)
        if skip_benchmark:
            if cfg.architecture.query_engine.type.value == "none":
                print_info("Skipped (no query engine)")
            else:
                print_info("Skipped (--skip-benchmark)")
        if not skip_benchmark:
            try:
                from lakebench.benchmark import BenchmarkRunner
                from lakebench.benchmark.runner import round_spread
                from lakebench.metrics import BenchmarkMetrics

                console.print()
                console.print("[bold]Stage: benchmark[/bold]")
                label = "post-compaction " if do_maintenance else ""
                print_info(f"Running {label}query benchmark (hot cache, power)...")

                _journal_safe(
                    j.record,
                    EventType.BENCHMARK_START,
                    message="Benchmark started",
                    details={"mode": "power", "cache": "hot"},
                )

                def _post_bench_progress(idx, total, name, phase, **kwargs):
                    if phase == "start":
                        console.print(f"  [{idx}/{total}] {name}...", end=" ")
                    elif phase == "done":
                        elapsed = kwargs.get("elapsed", 0)
                        success = kwargs.get("success", False)
                        error = kwargs.get("error", "")
                        if success:
                            console.print(f"[green]{elapsed:.1f}s OK[/green]{_sample_note(kwargs)}")
                        else:
                            short_err = (error[:60] + "...") if len(error) > 60 else error
                            console.print(f"[red]FAIL[/red] ({short_err})")

                bench_runner = BenchmarkRunner(cfg, tm_run_id=_tm_run_id)
                # LB-117: AML analytical queries (aggregate_typology_coverage
                # etc) can exceed the 300s default at scale >= 5; a timeout
                # here masquerades as a failed query and drops QpH to 0.
                _bench_timeout = (
                    900 if cfg.architecture.workload.schema_type.value == "financial" else 300
                )
                if pre_compaction_qph > 0:
                    # The pre round was measured after a warm-up pass; give
                    # this one the same, or the comparison measures the
                    # warm-up rather than maintenance (LB-141).
                    print_info("Warm-up pass (not measured)...")
                    _warm_benchmark(bench_runner, _bench_timeout)
                bench_result = bench_runner.run_power(
                    cache="hot",
                    iterations=cfg.architecture.benchmark.iterations,
                    progress_callback=_post_bench_progress,
                    query_timeout=_bench_timeout,
                )

                console.print(f"\n  Total: {bench_result.total_seconds:.2f}s")
                console.print(f"  [bold]QpH:   {bench_result.qph:.1f}[/bold]")
                _spread = round_spread(bench_result.queries)
                if _spread["samples_per_query"] >= 2:
                    console.print(
                        f"  Spread: QpH {_spread['qph_low']:.1f}-{_spread['qph_high']:.1f} "
                        f"over {_spread['samples_per_query']} samples per query "
                        f"(median-scored)"
                    )
                benchmark_qph = bench_result.qph

                # Maintenance summary
                if pre_compaction_qph > 0 and _pre_result is not None:
                    _maint_value = _maintenance_value(
                        _pre_result.queries,
                        bench_result.queries,
                        pre_file_count,
                        post_file_count,
                        maint_elapsed,
                        _settle,
                    )
                    if _maint_value[0] is not None:
                        console.print(
                            f"  [bold]Maintenance value: {_maint_value[0]:+.1f}% QpH "
                            f"improvement[/bold] (over the {_maint_value[1]} queries that "
                            "succeeded in both runs)"
                        )
                    else:
                        console.print(f"  Maintenance value: not measured ({_maint_value[2]})")
                if _settle is not None:
                    _state = "settled" if _settle.settled else "did not settle"
                    console.print(
                        f"  Storage {_state} {_settle.settle_seconds:.0f}s after maintenance "
                        f"({len(_settle.probes)} probes of {_settle.probe_query}; "
                        "not counted in time to value)"
                    )
                if maint_elapsed > 0 and pre_file_count > 0 and post_file_count > 0:
                    ratio = pre_file_count / max(post_file_count, 1)
                    console.print(
                        f"  Files: {pre_file_count:,} -> {post_file_count:,} "
                        f"({ratio:.1f}x compaction in {maint_elapsed:.0f}s)"
                    )

                # Record in metrics
                bench_metrics = BenchmarkMetrics(
                    mode=bench_result.mode,
                    cache=bench_result.cache,
                    scale=bench_result.scale,
                    qph=bench_result.qph,
                    total_seconds=bench_result.total_seconds,
                    queries=[q.to_dict() for q in bench_result.queries],
                    iterations=bench_result.iterations,
                )
                collector.record_benchmark(bench_metrics)

                _bench_problems = _benchmark_gate_problems(cfg, bench_result.queries)
                for _p in _bench_problems:
                    print_error(_p)
                if _bench_problems:
                    pipeline_success = False

                _journal_safe(
                    j.record,
                    EventType.BENCHMARK_COMPLETE,
                    message=f"Benchmark complete: QpH={bench_result.qph:.1f}",
                    success=True,
                    details={
                        "qph": round(bench_result.qph, 1),
                        "total_seconds": round(bench_result.total_seconds, 2),
                        "queries_passed": sum(1 for q in bench_result.queries if q.success),
                        "queries_total": len(bench_result.queries),
                    },
                )

            except Exception as e:
                print_warning(f"Benchmark failed: {e}")
                print_info(
                    "Pipeline results are still valid. Run 'lakebench benchmark' separately."
                )

        # Summary panel is printed in the finally block (after pipeline
        # benchmark scores are computed) so it can include the full scorecard.

    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        pipeline_success = False
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904
    finally:
        # -- Phase 7/7: Results ----------------------------------------------------
        console.print()
        console.print("[bold dim]Phase 7/7: Results[/bold dim]")
        # Measure actual S3 bucket sizes before saving metrics
        try:
            from lakebench.s3 import S3Client

            s3_cfg = cfg.platform.storage.s3
            s3_client = S3Client(
                endpoint=s3_cfg.endpoint,
                access_key=s3_cfg.access_key,
                secret_key=s3_cfg.secret_key,
                region=s3_cfg.region,
                path_style=s3_cfg.path_style,
                ca_cert=s3_cfg.ca_cert,
                verify_ssl=s3_cfg.verify_ssl,
            )
            print_info("Measuring actual S3 bucket sizes...")
            collector.record_actual_sizes(
                s3_client,
                s3_cfg.buckets.bronze,
                s3_cfg.buckets.silver,
                s3_cfg.buckets.gold,
            )
        except Exception as e:
            console.print(f"  [yellow]Could not measure S3 sizes: {e}[/yellow]")

        # Always save metrics, even on failure
        run_metrics = collector.end_run(success=pipeline_success)
        if run_metrics:
            # LB-123: attach folded-in financial recall scoring (if any) so it
            # persists into metrics.json and renders in the scorecard.
            if _financial_scoring is not None:
                run_metrics.financial_scoring = _financial_scoring

            # Collect platform metrics from Prometheus (best-effort)
            _collect_platform_metrics(cfg, run_metrics)

            # Build pipeline benchmark (stage-matrix view)
            try:
                from lakebench.metrics import build_pipeline_benchmark

                fleet = _load_latest_datagen_fleet(cfg.get_namespace())
                if fleet is not None:
                    run_metrics.datagen_fleet = fleet
                pb = build_pipeline_benchmark(
                    run_metrics,
                    datagen_elapsed=_datagen_elapsed,
                    datagen_output_gb=_datagen_output_gb,
                    datagen_output_rows=_datagen_output_rows,
                    datagen_fleet=fleet,
                )
                run_metrics.pipeline_benchmark = pb

                # Populate maintenance cost metrics (v1.3)
                try:
                    if maint_elapsed > 0:
                        pb.maintenance_elapsed_seconds = maint_elapsed
                        if pb.total_elapsed_seconds > 0:
                            pb.maintenance_pct_of_pipeline = (
                                maint_elapsed / pb.total_elapsed_seconds
                            ) * 100
                    if pre_file_count > 0 and post_file_count > 0:
                        pb.pre_compaction_file_count = pre_file_count
                        pb.post_compaction_file_count = post_file_count
                        pb.compaction_ratio = pre_file_count / max(post_file_count, 1)
                    if pre_compaction_qph > 0 and benchmark_qph:
                        pb.pre_compaction_qph = pre_compaction_qph
                        pb.post_compaction_qph = benchmark_qph
                        # Only a paired comparison after a compaction that
                        # changed files is a maintenance value; otherwise
                        # leave it null (LB-141).
                        if _maint_value is not None and _maint_value[0] is not None:
                            pb.maintenance_value_pct = _maint_value[0]
                            pb.maintenance_paired_queries = _maint_value[1]
                        elif _maint_value is not None:
                            pb.maintenance_value_reason = _maint_value[2]
                            pb.maintenance_paired_queries = _maint_value[1]
                        pb.pre_compaction_benchmark = _pre_record
                    if _settle is not None:
                        pb.maintenance_settle_seconds = _settle.settle_seconds
                        pb.maintenance_settled = _settle.settled
                        pb.maintenance_settle_capped = _settle.capped
                        pb.maintenance_settle_verified = _settle.verified
                        pb.maintenance_settle = _settle.to_dict()
                except Exception:
                    pass  # Maintenance metrics are best-effort

                # Print full scorecard panel
                if pipeline_success:
                    _print_pipeline_scorecard(pb, results, _datagen_elapsed, benchmark_qph)
            except Exception as e:
                console.print(f"  [yellow]Could not build pipeline benchmark: {e}[/yellow]")
                # Fallback summary if scorecard build failed
                if pipeline_success and results:
                    _total = sum(r[2] for r in results)
                    _qph = f"\nQpH: {benchmark_qph:.1f}" if benchmark_qph else ""
                    console.print(
                        Panel(
                            "[green]Pipeline complete[/green]\n\n"
                            + "\n".join(f"  {n}: {el:.0f}s" for n, _, el in results)
                            + f"\n\nTotal: {_total:.0f}s{_qph}"
                            + "\n\nFull report: [bold]lakebench report[/bold]",
                            title="Pipeline Complete",
                            expand=False,
                        )
                    )

            metrics_path = metrics_storage.save_run(run_metrics)
            print_info(f"Metrics saved to {metrics_path}")
            print_info(f"Run ID: {run_id}")

            _journal_safe(
                j.record,
                EventType.METRICS_SAVED,
                message=f"Metrics saved for run {run_id}",
                details={"run_id": run_id, "metrics_path": str(metrics_path)},
            )

        _journal_safe(j.end_command, success=pipeline_success)
        if not pipeline_success:
            # Metrics are saved above for diagnosis; the exit code must still
            # say the run did not succeed.
            raise typer.Exit(1)
