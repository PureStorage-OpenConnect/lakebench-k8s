"""Run command implementation -- extracted from cli/__init__.py."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.panel import Panel

from lakebench._clock import utc_now
from lakebench.cli._helpers import (
    EXIT_DATAGEN_TIMEOUT,
    _journal_safe,
    console,
    enforce_bronze_regenerate,
    journal_open,
    print_error,
    print_info,
    print_success,
    print_warning,
    resolve_config_path,
    write_run_report,
)
from lakebench.config import (
    ConfigError,
    ConfigFileNotFoundError,
    ConfigValidationError,
    load_config,
    parse_spark_memory,
)
from lakebench.config.schema import is_continuous_mode
from lakebench.journal import CommandName, EventType
from lakebench.k8s import K8sConnectionError
from lakebench.k8s.target import ContextConflictError

logger = logging.getLogger(__name__)

# Per-statement kubectl-exec timeout for pre-benchmark compaction (seconds).
PRE_BENCHMARK_COMPACTION_TIMEOUT = 1800
# Same bound for pre-benchmark expire_snapshots / remove_orphan_files.
PRE_BENCHMARK_MAINTENANCE_TIMEOUT = 1800
# Overall cap for pre-benchmark maintenance and compaction together (seconds).
PRE_BENCHMARK_MAINTENANCE_CAP = 1800


def _bump_silver_rebuild_epoch(cfg) -> None:
    """Atomically increment this deployment's silver rebuild-epoch counter (B1).

    Called by --force-rebuild before submitting the silver job so Delta's
    (txnAppId, txnVersion) commits from the previous epoch cannot short-
    circuit the fresh rebuild's cycle 0. The key is one of the three names
    on ``lakebench-silver-state``; job.py reads the same key back into
    ``LB_REBUILD_EPOCH`` for the silver pod.

    Raises RuntimeError on any error the caller cannot recover from; the CLI
    downgrades to a warning and passes LB_FORCE_REBUILD=1 anyway, because
    the Iceberg cycle-0 guard is orthogonal to Delta idempotency.
    """
    from kubernetes import client as _kclient
    from kubernetes.client.exceptions import ApiException

    from lakebench.deploy.engine import DeploymentEngine
    from lakebench.k8s import get_k8s_client as _get_k8s

    _get_k8s(
        context=cfg.platform.kubernetes.context or "",
        namespace=cfg.get_namespace(),
    )
    core_v1 = _kclient.CoreV1Api()
    namespace = cfg.get_namespace()

    # Same key resolution as job.py._rebuild_epoch_key.
    schema_type = cfg.architecture.workload.schema_type.value
    table_format = cfg.architecture.table_format.type.value
    if schema_type == "financial":
        key = "rebuild_epoch_aml_iceberg" if table_format == "iceberg" else None
    else:
        key = {
            "delta": "rebuild_epoch_c360_delta",
            "iceberg": "rebuild_epoch_c360_iceberg",
        }.get(table_format)
    if key is None:
        raise RuntimeError(f"no rebuild-epoch key for schema={schema_type} format={table_format}")

    # Best-effort optimistic-concurrency bump: read, +1, replace under the
    # same resourceVersion. On a concurrent write, the API server rejects
    # with 409 and we retry from the current version.
    for _ in range(5):
        try:
            cm = core_v1.read_namespaced_config_map(
                DeploymentEngine.SILVER_STATE_CONFIGMAP, namespace
            )
        except ApiException as e:
            if e.status != 404:
                raise
            # ConfigMap missing (older deployment): create with a fresh set
            # and the target key at 1 (the bump itself).
            manifest = {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": DeploymentEngine.SILVER_STATE_CONFIGMAP,
                    "namespace": namespace,
                },
                "data": {
                    k: ("1" if k == key else "0") for k in DeploymentEngine._SILVER_STATE_KEYS
                },
            }
            try:
                core_v1.create_namespaced_config_map(namespace, manifest)
                return
            except ApiException as e2:
                if e2.status == 409:
                    continue
                raise
        data = dict(cm.data or {})
        try:
            current = int(data.get(key, "0"))
        except ValueError:
            current = 0
        data[key] = str(current + 1)
        cm.data = data
        try:
            core_v1.replace_namespaced_config_map(
                DeploymentEngine.SILVER_STATE_CONFIGMAP, namespace, cm
            )
            return
        except ApiException as e:
            if e.status == 409:
                continue
            raise
    raise RuntimeError(
        f"could not bump {key} in {DeploymentEngine.SILVER_STATE_CONFIGMAP} "
        "after 5 concurrent-write retries"
    )


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
    if is_continuous_mode(pb.pipeline_mode):
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
        trickle = pb.trickle_summary()
        if trickle:
            scores.append(f"  [dim]{trickle}[/dim]")
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

    body += f"\n\nTotal: {total_time:.0f}s\n\nReport: report.html in the run directory"

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

    now = utc_now()
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
        metrics.timing_source = "local_runner"
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
            # --local always queries through DuckDB.
            engine="duckdb",
            # This field is int and display-only. A sub-1 local scale would
            # truncate to 0 and read as "no data", so floor at 1; the exact
            # value stays in the config snapshot, which is what compare reads.
            scale=max(1, int(cfg.architecture.workload.datagen.scale)),
            qph=qph,
            total_seconds=sum(r[2] for r in bench_results),
            queries=[
                {
                    "query_name": r[0],
                    "elapsed_seconds": r[2],
                    "success": r[1],
                    "rows_returned": r[3],
                    # (name, ok, elapsed, rows[, fingerprint]): see benchmark_local.
                    "result_fingerprint": r[4] if len(r) > 4 else None,
                }
                for r in bench_results
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


# Fields the cluster run path owns authoritatively; parse_driver_logs may
# fill them for local runs but must not overwrite them here. Anything not
# in this set is copied from parsed onto job_metrics automatically, so a
# new parsed field never silently disappears in the cluster path.
#
# Repeatable-fix contract: adding a field to JobMetrics does not need a
# corresponding edit to _apply_parsed_job_metrics. The only decision the
# author has to make is "is this field cluster-owned or parser-owned"
# and if cluster-owned, add it here.
#
# The original LB-123 defect (three detection dicts silently dropped)
# and the two live-caught silver-plan-r3 omissions (silver_tables +
# extra_metrics at 87feefd, streaming per-batch labels at e31514d) were
# all the same shape: a hand-written field-by-field copy that lagged
# behind the dataclass. Iterating fields removes that shape.
_CLUSTER_OWNED_JOB_METRICS_FIELDS: frozenset[str] = frozenset(
    {
        "job_name",
        "job_type",
        "start_time",
        "end_time",
        "elapsed_seconds",
        "success",
        "error_message",
        "timing_source",
        "timing_resolution_seconds",
        "submission_failures",
        "submission_retry_seconds",
        "executor_count",
        "executor_cores",
        "executor_memory_gb",
        "cpu_seconds_requested",
        "memory_gb_requested",
    }
)


def _apply_parsed_job_metrics(job_metrics, parsed) -> None:
    """Copy every parser-owned field from parsed onto job_metrics.

    Iterates the dataclass so a new field lands automatically as long as
    it is not in _CLUSTER_OWNED_JOB_METRICS_FIELDS. See the set docstring
    for the rationale; the LB-123 detection-dict defect and the two
    silver-plan-r3 live-caught omissions (silver_tables + extra_metrics,
    streaming per-batch labels) were three instances of the same
    hand-copy drift.
    """
    import dataclasses

    for f in dataclasses.fields(parsed):
        if f.name in _CLUSTER_OWNED_JOB_METRICS_FIELDS:
            continue
        setattr(job_metrics, f.name, getattr(parsed, f.name))


# Batch stage status poll. Stage times come from the cluster (_stage_timing),
# not from this poll; it bounds how long lakebench takes to notice a stage has
# ended, which sits inside time to value between stages. Was 15 s.
_STAGE_POLL_S = 5


def _handle_datagen_timeout(
    *,
    datagen_deployer,
    job_manager,
    namespace: str,
    timeout_s: int,
    elapsed_s: float,
) -> None:
    """A4 (v1.6): fail the run and stop orphaned compute after a datagen timeout.

    Before this branch the wait loop just fell through to a "Datagen
    completed" success line even when the timeout had expired: the datagen
    Job was still running (burning pod resources), any streaming Spark
    application consuming the trickle was still running, and the follow-up
    pipeline stages would build on a partial bronze (invariant 3). The fix:
    delete the datagen Job so it stops writing, delete each streaming
    SparkApplication that was consuming the trickle so it stops reading,
    then exit with a distinct code (``EXIT_DATAGEN_TIMEOUT``) so wrapper
    scripts can tell a timeout apart from other datagen failures.
    """
    from lakebench.cli._sustained import _STREAM_APPS

    print_error(
        f"Datagen exceeded the {timeout_s}s wait budget "
        f"(elapsed ~{elapsed_s:.0f}s). Invariant 3: exit 0 is not a pass; "
        "failing the run rather than proceeding on a partial bronze."
    )
    # A hung K8s API can itself be the reason for the wait-budget timeout.
    # Cap each cleanup call at 15 s so the handler always reaches the
    # ``raise typer.Exit`` below rather than blocking indefinitely.
    _CLEANUP_TIMEOUT_S = 15
    try:
        datagen_deployer._delete_existing_job(namespace, request_timeout=_CLEANUP_TIMEOUT_S)
    except Exception as e:  # noqa: BLE001 -- best-effort cleanup
        logger.warning("Failed to delete datagen Job on timeout: %s", e)
    else:
        print_info("Stopped datagen Job.")
    for app in _STREAM_APPS:
        try:
            job_manager._delete_job(app, request_timeout=_CLEANUP_TIMEOUT_S)
        except Exception as e:  # noqa: BLE001 -- best-effort cleanup
            logger.warning("Failed to delete leftover SparkApplication %s on timeout: %s", app, e)
    print_info(
        "Deleted any leftover SparkApplication that was consuming the trickle "
        f"(bronze-ingest, silver-stream, gold-refresh) in namespace {namespace}."
    )
    raise typer.Exit(EXIT_DATAGEN_TIMEOUT)


def _submission_failure_reporter(stage_name: str, journal):
    """Print and journal each SUBMISSION_FAILED of a batch stage as it happens.

    The operator retries on its own, so the stage keeps waiting; without a
    line here a 60-90 s retry reads as a slow stage with no reason (the
    2026-09-27 controller evictions).
    """

    def _report(failure: dict[str, Any]) -> None:
        print_warning(
            f"lakebench-{stage_name}: submission attempt {failure['attempt']} failed: "
            f"{failure['reason']}. The Spark Operator retries it; the stage time "
            "includes the wait."
        )
        _journal_safe(
            journal.record,
            EventType.PIPELINE_STAGE,
            message=f"{stage_name} submission failed",
            details={
                "stage": stage_name,
                "event": "submission_failed",
                "attempt": failure["attempt"],
                "reason": failure["reason"],
            },
        )

    return _report


def _retry_note(job_metrics) -> str:
    """' (includes Ns ...)' when a stage's elapsed includes submission retries."""
    n = len(job_metrics.submission_failures)
    if not n:
        return ""
    return (
        f" (includes {job_metrics.submission_retry_seconds:.0f}s waiting on {n} failed "
        f"operator submission{'s' if n != 1 else ''})"
    )


def _stage_timing(monitor, app_name: str, result, submitted_at, observed_end):
    """When a batch stage ran: submission to the Spark application's real end.

    The end is the driver container's finish time (else the
    SparkApplication's terminationTime) mapped to this host's clock through
    the API server's clock offset, measured now. Anything unreadable or
    inconsistent falls back to the poll that saw the job finish, and the
    record says which (JobMetrics.timing_source).
    """
    from lakebench.cli._sustained import cluster_clock_offset_seconds
    from lakebench.modules.pipeline_engines.spark.monitor import (
        parse_k8s_time,
        stage_timing,
    )

    status = getattr(result, "final_status", None)
    try:
        cluster_end, source = monitor.application_end(app_name, status)
    except Exception as e:  # noqa: BLE001 -- best effort; the poll time stands
        logger.debug("application end for %s not read: %s", app_name, e)
        cluster_end, source = None, ""
    offset = None
    if cluster_end is not None:
        # Read once per monitor (one run): it sits between stages, inside
        # time to value, and can take up to its timeout.
        offset = getattr(monitor, "_lb_clock_offset", None)
        if not isinstance(offset, float):
            offset = cluster_clock_offset_seconds()
            if offset is not None:
                try:
                    monitor._lb_clock_offset = offset
                except AttributeError:
                    pass
    return stage_timing(
        submitted_at,
        observed_end,
        cluster_end,
        source,
        offset,
        _STAGE_POLL_S,
        last_submission=parse_k8s_time(status.start_time) if status is not None else None,
        last_running=(
            submitted_at + timedelta(seconds=result.last_running_elapsed)
            if isinstance(getattr(result, "last_running_elapsed", None), (int, float))
            else None
        ),
    )


def _exclude_c360_check_time(job_metrics) -> float:
    """Take the c360 expected-result check off a gold-finalize stage's time.

    The check runs inside the gold-finalize pod after the stage's work
    (common.log_c360_check) and logs its own ``check_seconds``. Left in, it
    would count lakebench's correctness scan as pipeline time in the
    stage's elapsed seconds and in time to value. Returns the seconds
    removed (0 when there is nothing to remove).
    """
    from datetime import timedelta

    facts = getattr(job_metrics, "c360_check", None) or {}
    try:
        secs = float(facts.get("check_seconds") or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if secs <= 0 or secs >= (job_metrics.elapsed_seconds or 0.0):
        return 0.0
    job_metrics.elapsed_seconds -= secs
    if job_metrics.end_time is not None:
        job_metrics.end_time -= timedelta(seconds=secs)
    return secs


# Upstream failures a benchmark may carry without failing the run, as
# (table format, query engine, query name). Each must be a documented bug
# outside lakebench. Delta + Thrift Q2 (LB-034, Delta MIN/MAX on date partitions) left
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


def compaction_measurable(cfg) -> bool:
    """Whether pre/post data file counts can measure a compaction: not on
    Delta, where OPTIMIZE never runs (metrics/maintenance_policy)."""
    return cfg.architecture.table_format.type.value != "delta"


def _data_file_total(health: dict[str, int]) -> int:
    """Data files across the probed tables; 0 (unknown) when any probe failed.

    ``_probe_table_health`` leaves a failed probe's key out (older code
    wrote -1, still refused here). Summing what is left read a probe that
    failed on one side only as a file-count change.
    """
    counts = [v for k, v in health.items() if "file_count" in k and isinstance(v, int)]
    if not counts or any(v < 0 for v in counts):
        return 0
    # A probe that found no count leaves its key out. A total over fewer
    # tables than the other side's reads as a compaction that never ran.
    if not all(isinstance(health.get(f"{t}_data_file_count"), int) for t in ("silver", "gold")):
        return 0
    return sum(counts)


def _maintenance_value(
    pre,
    post,
    pre_files: int,
    post_files: int,
    maint_elapsed: float,
    settle=None,
    stopped_reason: str = "",
    live_streams_reason: str = "",
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

    *stopped_reason* is set when pre-benchmark maintenance was stopped (a
    statement timed out or the overall cap hit). A timed-out statement may
    still be running server-side, so the post round is not measurable.
    """
    if maint_elapsed <= 0:
        return None, 0, "maintenance did not run"
    if stopped_reason:
        return None, 0, f"maintenance stopped before completion ({stopped_reason})"
    if live_streams_reason:
        return None, 0, f"streams were live during maintenance ({live_streams_reason})"
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
        runner.run_power(cache="hot", query_timeout=query_timeout, iterations=1, fingerprint=False)
    except Exception as e:  # noqa: BLE001
        logger.warning("benchmark warm-up pass failed: %s", e)


def _maintenance_statements_attempted(outcomes: list | None) -> int | None:
    """Maintenance and compaction statements the batch round attempted, or
    None when that is not known.

    A statement that failed or timed out counts: a timed-out one may still
    be running server-side, and a failed one may have deleted or rewritten
    files before it failed. None (unknown) when a phase raised after its
    statements may have started, or when there are no outcomes at all; the
    caller then waits as before.
    """
    if not outcomes:
        return None
    attempted = 0
    for o in outcomes:
        if o.get("error") and not o.get("before_statements"):
            return None
        if "total" in o:
            attempted += (
                int(o.get("succeeded") or 0)
                + int(o.get("failed") or 0)
                + int(o.get("timed_out") or 0)
            )
    return attempted


def _settle_after_maintenance(
    cfg,
    runner,
    pre_queries,
    query_timeout: int,
    started_at: float,
    *,
    clock=None,
    sleep=None,
    trigger: str = "",
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
    # Its samples widen the tolerance to the query's own noise (capped).
    reference = None
    reference_samples: list[float] = []
    for q in pre_queries or []:
        if q.query.name == query.name and q.success and q.elapsed_seconds > 0:
            reference = q.elapsed_seconds
            reference_samples = [float(x) for x in q.sample_times()]
    ref_note = f", pre-maintenance {reference:.1f}s" if reference else ", no pre-maintenance time"
    from lakebench.benchmark.settle import effective_tolerance_pct

    tol = effective_tolerance_pct(sc.tolerance_pct, reference_samples if reference else None)
    tol_note = (
        f" (widened from {sc.tolerance_pct:g}% to the pre samples' spread)"
        if tol > sc.tolerance_pct
        else ""
    )
    print_info(
        f"Waiting for storage to settle: probe {query.name} every {sc.interval_seconds}s, "
        f"within {sc.tolerance_pct:g}% of each other and {tol:.3g}%{tol_note}{ref_note}, "
        f"cap {sc.max_seconds}s"
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
        reference_samples=reference_samples,
        on_probe=_show,
        **kwargs,
    )
    result.trigger = trigger
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


def _benchmark_gate_problems(cfg, queries, check_empty: bool = True) -> list[str]:
    """Reasons the benchmark result is not a valid score.

    QpH is computed over the queries that succeeded, so a run where most
    queries failed still printed a QpH (live AML run, 2026-09-24: 1 of 8
    passed, QpH 166, exit 0). Every failure outside the known upstream list
    fails the run. With *check_empty* (the default), so does a successful
    query that returned no rows and is not declared allow_empty; the
    continuous path turns it off for rounds before its last.
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
    # A query that "succeeded" with no rows measured nothing (invariant 3):
    # an empty table, a filter that matched nothing, or a reader that read
    # nothing. Only a query declared allow_empty may return none.
    empty_names = set(empty_benchmark_queries(queries)) if check_empty else set()
    empty = [q for q in queries if _name(q) in empty_names]
    problems = []
    if bad:
        names = ", ".join(_name(q) for q in bad)
        problems.append(
            f"Benchmark gate: {len(bad)} of {len(queries)} queries failed ({names}); "
            "QpH over the rest is not a valid score. Marking FAILURE."
        )
    if empty:
        names = ", ".join(_name(q) for q in empty)
        problems.append(
            f"Benchmark gate: {len(empty)} of {len(queries)} queries returned no rows "
            f"({names}); an empty result measures nothing. Marking FAILURE."
        )
    return problems


def empty_benchmark_queries(queries) -> list[str]:
    """Names of successful queries (QueryResult or dict) that returned no
    rows and are not declared allow_empty."""
    from lakebench.benchmark.queries import BENCHMARK_QUERIES_BY_DOMAIN

    allow_empty = {
        bq.name for qs in BENCHMARK_QUERIES_BY_DOMAIN.values() for bq in qs if bq.allow_empty
    }
    out = []
    for q in queries:
        d = isinstance(q, dict)
        name = q.get("name") if d else q.query.name
        ok = q.get("success") if d else q.success
        rows = (q.get("rows_returned") if d else q.rows_returned) or 0
        if ok and int(rows) == 0 and name not in allow_empty:
            out.append(str(name))
    return out


def scoring_count_line(summary: dict) -> str:
    """'6 of 15 typologies scored; 8 no rule, 1 rule skipped' from a
    recall.json summary: never every manifest typology as scored."""
    typs = summary.get("typologies", []) or []
    counts = summary.get("typology_counts")
    if counts is None:
        counts = {}
        for t in typs:
            st = t.get("detection_status") or "unknown"
            counts[st] = counts.get(st, 0) + 1
    labels = (
        ("partial", "partial"),
        ("no_rule", "no rule"),
        ("rule_skipped", "rule skipped"),
        ("rule_error", "rule error"),
        ("unknown", "unknown"),
    )
    rest = [f"{counts[k]} {lab}" for k, lab in labels if counts.get(k)]
    line = f"{counts.get('scored', 0)} of {len(typs)} typologies scored"
    return line + (f"; {', '.join(rest)}" if rest else "")


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
        print_success(f"Financial scoring complete ({scoring_count_line(summary)})")
        return summary
    except Exception as e:  # noqa: BLE001 -- scoring is best-effort enrichment
        print_warning(f"Financial scoring failed ({e}); scorecard will omit recall.")
        return None


def no_query_engine_skip(cfg, skip_benchmark: bool) -> tuple[bool, bool]:
    """(no_query_engine, skip_benchmark) for a batch run of *cfg*.

    A recipe with no query engine has nothing to benchmark and no engine to
    run table maintenance through. The run skips both and says so, instead
    of failing a correct pipeline on "Cannot run queries without a query
    engine" (every ``*-none`` recipe exited 1 without --skip-benchmark).
    """
    none = cfg.architecture.query_engine.type.value == "none"
    if none and not skip_benchmark:
        print_info(
            "No query engine in this recipe (query_engine.type=none): "
            "benchmark and table maintenance skipped"
        )
    return none, skip_benchmark or none


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
    # Share the run id with datagen pods and Spark drivers (live observability
    # grouping label) via the orchestrator process env.
    os.environ["LB_RUN_ID"] = run_id
    snapshot = build_config_snapshot(cfg, run_mode="batch", system="local")
    snapshot["local"] = True
    collector.start_run(run_id, cfg.name, snapshot)
    if collector.current_run is not None:
        # Local mode runs no table maintenance.
        from lakebench.metrics.maintenance_policy import skipped_policy_id

        collector.current_run.maintenance_policy_id = skipped_policy_id()

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
                    for n, ok, e, r, *_ in bench_results
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
        write_run_report(metrics_storage, run_id)
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
    continuous: Annotated[
        bool,
        typer.Option(
            "--continuous",
            help="Run in continuous mode: bronze-ingest -> silver-stream -> gold-refresh",
        ),
    ] = False,
    sustained: Annotated[
        bool,
        typer.Option(
            "--sustained",
            help="Deprecated alias for --continuous",
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
            help="Run datagen before pipeline stages (batch mode only; continuous always runs datagen)",
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
    regenerate: Annotated[
        bool,
        typer.Option(
            "--regenerate",
            help=(
                "With --generate: empty the bronze bucket before generating. "
                "Without this flag, a non-empty bronze prefix is refused "
                "(exit 2) so existing datagen output is never overwritten "
                "silently. No effect without --generate."
            ),
        ),
    ] = False,
    skip_maintenance: Annotated[
        bool,
        typer.Option(
            "--skip-maintenance",
            help="Skip pre-benchmark maintenance (compaction, snapshot expiry)",
        ),
    ] = False,
    force_rebuild: Annotated[
        bool,
        typer.Option(
            "--force-rebuild",
            help=(
                "Silver batch only: opt in to a full rebuild that would drop an existing "
                "populated silver table. Atomically bumps the deployment's silver rebuild "
                "epoch so downstream Delta idempotency keys move to a new namespace."
            ),
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
    pipeline. Continuous mode always runs datagen automatically.

    With --continuous, runs the continuous pipeline instead:
    starts datagen, then launches bronze-ingest, silver-stream,
    and gold-refresh as concurrent Spark jobs. Runs in-stream benchmark
    rounds during the configured duration, then lets the corpus settle,
    stops the jobs and fingerprints the query set over the settled tables.
    """
    import uuid

    from lakebench.cli._sustained import (
        MaintenanceBudget,
        _collect_platform_metrics,
        _live_stream_apps,
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
    if sustained:
        print_warning("--sustained is deprecated and will be removed; use --continuous")

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

    # DESIGN 6.5: an unsupported workload x architecture x mode is refused
    # before anything runs. Load already checks the config's own mode;
    # --continuous and --sustained do not write the mode back, so check the
    # mode this run will use. --local runs Customer 360 batch only.
    from lakebench.config.support import UNSUPPORTED, support_state_for_config

    _run_mode = "continuous" if (sustained or continuous) else cfg.architecture.pipeline.mode
    if local:
        _support = support_state_for_config(cfg, _run_mode, system="local")
    else:
        _support = support_state_for_config(cfg, _run_mode)
    if _support["state"] == UNSUPPORTED:
        print_error(f"Unsupported combination, refused: {_support['basis']}")
        raise typer.Exit(1)
    if _support.get("scale_note"):
        print_warning(f"Unverified scale: {_support['scale_note']}")

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
    except ContextConflictError:
        raise
    except Exception as e:
        logger.warning("Could not get cluster capacity for auto-sizing: %s", e)
        cluster_cap = None
    # Cuts to fit the cluster are shown with their reason, never silent (LB-160).
    autosize_cuts = [str(c) for c in resolve_auto_sizing(cfg, cluster_cap) or []]
    for cut in autosize_cuts:
        print_warning(f"Auto-sizing: {cut}")

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
        _generate_cmd(
            config_file=config_file,
            wait=True,
            timeout=timeout or 14400,
            yes=yes,
            regenerate=regenerate,
        )
        return

    # -- Phase 1/7: Prerequisites ------------------------------------------------
    console.print()
    console.print("[bold dim]Phase 1/7: Prerequisites[/bold dim]")

    if not skip_deploy:
        from lakebench.cli._prerequisites import run_prerequisites

        # The --sustained flag does not write back to the config, so the
        # capacity check is told the mode the run will use (LB-155). Datagen
        # is left out only where the run itself releases its cores: under
        # --skip-generate with a finished lakebench-datagen Job (LB-158).
        _use_sustained = bool(
            sustained or continuous or is_continuous_mode(cfg.architecture.pipeline.mode)
        )
        _datagen_runs = True
        if _use_sustained and skip_generate:
            from lakebench.cli._sustained import _datagen_job_state

            _datagen_runs = _datagen_job_state(cfg.get_namespace())[0] != "finished"
        prereq_report = run_prerequisites(cfg, sustained=_use_sustained, datagen_runs=_datagen_runs)
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
    use_sustained = sustained or continuous or is_continuous_mode(cfg.architecture.pipeline.mode)
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
            autosize_cuts=autosize_cuts,
        )
        return

    no_query_engine, skip_benchmark = no_query_engine_skip(cfg, skip_benchmark)

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
    # Share the run id with datagen pods and Spark drivers (live observability
    # grouping label) via the orchestrator process env.
    os.environ["LB_RUN_ID"] = run_id
    from lakebench.metrics import build_config_snapshot

    config_snapshot = build_config_snapshot(cfg, run_mode="batch")
    collector.start_run(run_id, cfg.name, config_snapshot)
    if collector.current_run is not None:
        collector.current_run.autosize_cuts = autosize_cuts
        # [] from the start: a run that ends before the maintenance phase is
        # then stamped "not run", never with the policy's request.
        collector.current_run.maintenance_outcomes = []
    if skip_maintenance and collector.current_run is not None:
        # No table maintenance: not comparable with runs under the policy.
        from lakebench.metrics.maintenance_policy import skipped_policy_id

        collector.current_run.maintenance_policy_id = skipped_policy_id()

    pipeline_success = True
    # A4 (v1.6): the finally block below rewrites the exit code to 1 when
    # pipeline_success is False, which clobbers any distinct code the try
    # block raised (e.g. EXIT_DATAGEN_TIMEOUT). Any specific code is
    # written here first so the finally can honour it.
    _pipeline_exit_code = 1
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

                # A4 (v1.6): CLI-level bronze safety. Refuse a non-empty
                # bronze prefix unless --regenerate was passed; with the
                # flag, empty the bronze bucket first.
                enforce_bronze_regenerate(cfg, regenerate)

                dg_engine = DeploymentEngine(cfg)
                datagen_deployer = DatagenDeployer(dg_engine)
                datagen_deployer.deploy()

                # Progress bar (same as standalone generate command)
                _dg_start = _time.time()
                _initial = datagen_deployer.get_progress()
                _total_pods = _initial.get("completions", 1)

                _dg_timed_out = True  # set False on any break out of the loop
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
                            _dg_timed_out = False
                            break
                        if _dg_prog.get("oom_pods"):
                            _dg_bar.stop()
                            print_error(f"OOMKilled: {', '.join(_dg_prog['oom_pods'])}")
                            raise typer.Exit(1)
                        _dg_bar.update(_dg_task, completed=_dg_prog.get("succeeded", 0))
                        _time.sleep(15)

                # A4 (v1.6): if the wait budget ran out we must NOT print
                # "Datagen completed" -- the pods are still running, and the
                # follow-up pipeline stages would build on partial bronze
                # (invariant 3). Report a distinct exit code, stop the
                # datagen Job so it stops burning pods, and kill any
                # SparkApplication that was consuming the trickle from a
                # prior/concurrent run so the timed-out generate does not
                # leave orphan compute behind.
                if _dg_timed_out:
                    _datagen_elapsed = (datetime.now() - datagen_start).total_seconds()
                    # Preserve the timeout code past the finally block below.
                    _pipeline_exit_code = EXIT_DATAGEN_TIMEOUT
                    pipeline_success = False
                    _handle_datagen_timeout(
                        datagen_deployer=datagen_deployer,
                        job_manager=job_manager,
                        namespace=cfg.get_namespace(),
                        timeout_s=timeout,
                        elapsed_s=_datagen_elapsed,
                    )

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
            except typer.Exit as e:
                # A4 (v1.6): _handle_datagen_timeout, the OOM / error
                # branches above and enforce_bronze_regenerate raise their
                # own typer.Exit with a specific code (2 for the regenerate
                # refusal, EXIT_DATAGEN_TIMEOUT for a wait-budget timeout).
                # Do not swallow it into a generic Exit(1) -- carry the
                # code through the finally block so wrappers can tell them
                # apart from other failures.
                pipeline_success = False
                _pipeline_exit_code = e.exit_code or _pipeline_exit_code
                raise
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

        # B1 --force-rebuild: bump the deployment's rebuild-epoch counter
        # ONCE per `lakebench run` invocation, before the cycle loop, so
        # all cycles in this run share one (txnAppId, txnVersion) namespace
        # keyed on the new epoch. Bumping per-cycle would give each cycle
        # its own appId, defeating the invariant that a single rebuild is
        # one epoch.
        #
        # Any bump failure is a hard exit. The previous behaviour of "print
        # a warning and still submit with LB_FORCE_REBUILD=1" left Delta
        # reading the un-bumped epoch from the ConfigMap and short-
        # circuiting cycle 0 against the previous rebuild's cycle 0 --
        # silent zero-row write, exit 0, silver missing all of cycle 2
        # (invariant 3: exit 0 is not a pass).
        if force_rebuild:
            schema_type = cfg.architecture.workload.schema_type.value
            table_format = cfg.architecture.table_format.type.value
            # AML+delta has no rebuild-epoch key; the CLI flag on that
            # combination is a no-op, not a bumped epoch.
            if schema_type != "financial" or table_format == "iceberg":
                try:
                    _bump_silver_rebuild_epoch(cfg)
                except Exception as e:
                    print_error(
                        f"Could not bump silver rebuild-epoch: {e}\n"
                        "Refusing to submit silver: without a bumped epoch, "
                        "Delta's SetTransaction log short-circuits the "
                        "rebuild's cycle 0 as a duplicate of the previous "
                        "epoch, silently writing zero rows (invariant 3)."
                    )
                    raise typer.Exit(1) from e

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
            if total_cycles > 1:
                # c360 silver appends read only this cycle's bronze files
                # (common.c360_bronze_path); the whole prefix double-counted.
                cycle_env["LB_BRONZE_CYCLE"] = str(cycle_idx)
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

                job_start = utc_now()

                # B1 --force-rebuild: the epoch bump ran once above this
                # cycle loop. The Iceberg cycle-0 guard (silver already
                # populated + no --force-rebuild) is orthogonal to the
                # Delta epoch, and the flag is safely ignored by Delta
                # after cycle 0, so setting it on every silver_build stage
                # for this run is correct.
                stage_env = dict(cycle_env)
                if force_rebuild and job_type == JobType.SILVER_BUILD:
                    stage_env["LB_FORCE_REBUILD"] = "1"

                # Submit job
                job_status = job_manager.submit_job(job_type, cycle_env=stage_env)
                if job_status.state == JobState.FAILED:
                    print_error(f"Failed to submit job: {job_status.message}")
                    collector.record_job(
                        JobMetrics(
                            job_name=f"lakebench-{stage_name}",
                            job_type=stage_name,
                            start_time=job_start,
                            end_time=utc_now(),
                            elapsed_seconds=(utc_now() - job_start).total_seconds(),
                            timing_source="submit_failed",
                            success=False,
                            error_message=job_status.message,
                        )
                    )
                    pipeline_success = False
                    raise typer.Exit(1)

                print_success(f"Job submitted: lakebench-{stage_name}")
                # The stage starts when the SparkApplication exists: the
                # same point the monitor's elapsed counted from.
                job_submitted = utc_now()

                # Wait for completion -- capture max executor count seen
                _max_executors = 0
                _last_reported_executors = -1

                _last_heartbeat_ts = job_start

                def on_progress(status, _start=job_start, _hb=[job_start]):  # noqa: B006
                    nonlocal _max_executors, _last_reported_executors
                    if status.state == JobState.RUNNING:
                        _max_executors = max(_max_executors, status.executor_count)
                        elapsed = (utc_now() - _start).total_seconds()
                        if status.executor_count != _last_reported_executors:
                            console.print(f"  Running... (executors: {status.executor_count})")
                            _last_reported_executors = status.executor_count
                            _hb[0] = utc_now()
                        elif (utc_now() - _hb[0]).total_seconds() >= 60:
                            console.print(f"  Running... ({int(elapsed)}s elapsed)")
                            _hb[0] = utc_now()

                result = monitor.wait_for_completion(
                    f"lakebench-{stage_name}",
                    timeout_seconds=timeout,
                    poll_interval=_STAGE_POLL_S,
                    progress_callback=on_progress,
                    on_submission_failure=_submission_failure_reporter(stage_name, j),
                )
                # The poll that saw the end, less the driver-log fetch the
                # monitor did after it.
                job_observed_end = job_submitted + timedelta(seconds=result.elapsed_seconds)
                timing = _stage_timing(
                    monitor, f"lakebench-{stage_name}", result, job_submitted, job_observed_end
                )

                # Build job metrics. start_time stays the moment before
                # submission (time to value counts lakebench's resubmit of
                # the stage); elapsed runs from the SparkApplication's
                # creation to the application's real end.
                job_metrics = JobMetrics(
                    job_name=f"lakebench-{stage_name}",
                    job_type=stage_name,
                    start_time=job_start,
                    end_time=timing.end,
                    elapsed_seconds=timing.elapsed_seconds,
                    timing_source=timing.source,
                    timing_resolution_seconds=timing.resolution_seconds,
                    success=result.success,
                    error_message=result.message if not result.success else None,
                    executor_count=_max_executors,
                    submission_failures=list(result.submission_failures),
                    submission_retry_seconds=result.submission_retry_seconds,
                )
                if timing.note:
                    logger.warning("%s timed by poll: %s", stage_name, timing.note)

                # Parse driver logs for data metrics if available
                if result.driver_logs:
                    parsed = collector.parse_driver_logs(result.driver_logs, stage_name)
                    _apply_parsed_job_metrics(job_metrics, parsed)
                    _exclude_c360_check_time(job_metrics)

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
                    print_success(
                        f"{stage_name} completed in {job_metrics.elapsed_seconds:.1f}s"
                        f"{_retry_note(job_metrics)}"
                    )
                    results.append((stage_name, True, job_metrics.elapsed_seconds))
                    _journal_safe(
                        j.record,
                        EventType.PIPELINE_STAGE,
                        message=f"{stage_name} completed",
                        success=True,
                        details={
                            "stage": stage_name,
                            "success": True,
                            "elapsed_seconds": job_metrics.elapsed_seconds,
                            "input_gb": job_metrics.input_size_gb,
                            "output_rows": job_metrics.output_rows,
                            "submission_failures": len(job_metrics.submission_failures),
                            "submission_retry_seconds": job_metrics.submission_retry_seconds,
                        },
                    )
                else:
                    print_error(f"{stage_name} failed: {result.message}{_retry_note(job_metrics)}")
                    if result.driver_logs:
                        console.print("[dim]Driver logs (last 20 lines):[/dim]")
                        for line in result.driver_logs.split("\n")[-20:]:
                            console.print(f"  {line}")
                    results.append((stage_name, False, job_metrics.elapsed_seconds))
                    _journal_safe(
                        j.record,
                        EventType.PIPELINE_STAGE,
                        message=f"{stage_name} failed: {result.message}",
                        success=False,
                        details={
                            "stage": stage_name,
                            "success": False,
                            "elapsed_seconds": job_metrics.elapsed_seconds,
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

        # Customer 360 expected results (D6): reported, never gating until the
        # owner approves what each check means (c360_correctness.GATING).
        if (
            cfg.architecture.workload.schema_type.value == "customer360"
            and not stage
            and collector.current_run is not None
        ):
            from lakebench.metrics import c360_correctness as _c360

            try:
                _jobs = collector.current_run.jobs
                _c360_rec = _c360.evaluate_run(
                    [jm for jm in _jobs if getattr(jm, "job_type", "") == "gold-finalize"],
                    [jm for jm in _jobs if getattr(jm, "job_type", "") == "bronze-verify"],
                    _c360.expected_context(cfg),
                )
                collector.current_run.c360_correctness = _c360_rec
                for _line in _c360.summary_lines(_c360_rec):
                    (print_warning if _c360_rec["status"] != "pass" else print_info)(_line)
            except Exception as e:  # noqa: BLE001 -- reporting only
                _c360_rec = None
                print_warning(f"Customer 360 expected-result check could not run: {e}")
            # Empty until the owner approves the checks' meaning (D6).
            for _p in _c360.gating_problems(_c360_rec):
                print_error(_p)
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
        # Set when pre-benchmark maintenance was stopped (timeout or cap): a
        # statement may still be running, so the post-maintenance QpH is not
        # a clean measurement.
        maint_stop_reason = ""
        # Set when streams were (or may have been) writing during pre-benchmark
        # maintenance: the post-maintenance QpH was then measured under load.
        maint_live_reason = ""
        _maint_end = None
        _settle = None
        # Set when the settle wait was skipped because no statement ran.
        _settle_skip: dict | None = None
        pre_file_count = 0
        post_file_count = 0
        maint_elapsed = 0.0
        # What each maintenance and compaction call actually did, for the
        # experiment block's effective maintenance. [] when none ran.
        maint_outcomes: list = []
        if collector.current_run is not None:
            if collector.current_run.maintenance_outcomes is None:
                collector.current_run.maintenance_outcomes = []
            maint_outcomes = collector.current_run.maintenance_outcomes

        do_maintenance = (
            not skip_benchmark
            and not skip_maintenance
            and cfg.architecture.pipeline.pre_benchmark_maintenance
        )

        if not do_maintenance:
            why = (
                "no query engine"
                if no_query_engine
                else "--skip-benchmark"
                if skip_benchmark
                else "--skip-maintenance"
                if skip_maintenance
                else "pre_benchmark_maintenance is off"
            )
            for kind in ("expire", "compaction"):
                maint_outcomes.append({"kind": kind, "user_skip": why})
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
                        # Results are checked on the post-maintenance benchmark.
                        fingerprint=False,
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
                # The benchmark must not start while a maintenance statement
                # still runs (a kubectl-exec timeout does not stop it), so each
                # statement may take up to 30 min; one budget caps expire,
                # orphan removal and compaction together, and the first
                # timeout stops the rest.
                maint_budget = MaintenanceBudget(PRE_BENCHMARK_MAINTENANCE_CAP)
                # Stream apps (restartPolicy Always) can still be writing: a
                # c360 run never stops leftovers. Any present means live.
                live_apps, live_errors = _live_stream_apps(cfg.get_namespace())
                if live_apps:
                    maint_live_reason = (
                        f"stream apps present or unreadable: {', '.join(live_apps)}"
                        + (f" (read errors: {'; '.join(live_errors)})" if live_errors else "")
                    )
                    console.print(
                        "  [yellow]Stream apps present during pre-benchmark maintenance: "
                        f"{', '.join(live_apps)}; using live-stream retention[/yellow]"
                    )
                    _journal_safe(
                        j.record,
                        EventType.STREAMING_HEALTH,
                        message="Pre-benchmark maintenance with live streams",
                        details={"stream_apps": live_apps, "read_errors": live_errors},
                    )
                _run_iceberg_maintenance(
                    cfg,
                    k8s,
                    console,
                    j,
                    retention_threshold=resolve_maintenance_retention(cfg),
                    timeout=PRE_BENCHMARK_MAINTENANCE_TIMEOUT,
                    live_streams=bool(live_apps),
                    budget=maint_budget,
                    outcomes=maint_outcomes,
                )
                _run_iceberg_compaction(
                    cfg,
                    k8s,
                    console,
                    j,
                    live_streams=bool(live_apps),
                    timeout=PRE_BENCHMARK_COMPACTION_TIMEOUT,
                    budget=maint_budget,
                    outcomes=maint_outcomes,
                )
                maint_stop_reason = maint_budget.stopped
                if maint_budget.stopped:
                    console.print(
                        f"  [yellow]Pre-benchmark maintenance stopped: {maint_budget.stopped}; "
                        "the remaining statements were not attempted. The benchmark runs "
                        "anyway; a timed-out statement may still be running.[/yellow]"
                    )
                    _journal_safe(
                        j.record,
                        EventType.STREAMING_HEALTH,
                        message="Pre-benchmark maintenance stopped",
                        details={"outcome": "timed_out", "reason": maint_budget.stopped},
                    )
                _wait_for_query_engine_ready(cfg, k8s, console, timeout=120)
                maint_elapsed = (datetime.now() - _maint_start).total_seconds()
                _maint_end = time.monotonic()

                # 4. Post-maintenance file count
                try:
                    _post_health = _probe_table_health(cfg, k8s)
                    post_file_count = _data_file_total(_post_health)
                except Exception:
                    pass
                if not compaction_measurable(cfg):
                    # Delta OPTIMIZE never runs: equal counts are table
                    # health, not a compaction result, and must not publish
                    # a compaction ratio or a maintenance value.
                    pre_file_count = post_file_count = 0
                if pre_file_count > 0 and post_file_count > 0:
                    # Detail, not identity: a compaction that changed no
                    # files still ran (effective maintenance reasons).
                    maint_outcomes.append(
                        {
                            "kind": "compaction",
                            "files_before": pre_file_count,
                            "files_after": post_file_count,
                            **(
                                {"note": f"no-op: data files {pre_file_count} -> {post_file_count}"}
                                if post_file_count == pre_file_count
                                else {}
                            ),
                        }
                    )

            except Exception as e:
                reached = any(o.get("kind") in ("expire", "compaction") for o in maint_outcomes)
                maint_outcomes.append(
                    {"kind": "maintenance", "error": str(e), "before_statements": not reached}
                )
                print_warning(f"Maintenance failed (non-fatal): {e}")

            # 5. Wait for storage to settle before the post round (LB-150).
            # Runs after maint_elapsed is taken and outside every stage, so it
            # cannot move time to value or maintenance_pct_of_pipeline.
            # Nothing to settle when no statement ran (DuckDB, or every
            # operation skipped): run 20260927-001340-6ab705 waited 783 s
            # after a DuckDB round that ran none.
            _attempted = _maintenance_statements_attempted(maint_outcomes)
            if maint_elapsed > 0 and _maint_end is not None and _attempted == 0:
                _settle_skip = {
                    "skipped": True,
                    "settle_seconds": 0.0,
                    "reason": "no maintenance or compaction statement ran; nothing to settle",
                }
                print_info("Storage settle wait: skipped (no maintenance statement ran)")
            elif maint_elapsed > 0 and _maint_end is not None:
                console.print()
                console.print("[bold]Storage settle wait[/bold]")
                try:
                    _settle = _settle_after_maintenance(
                        cfg,
                        _BR(cfg),
                        _pre_result.queries if _pre_result is not None else None,
                        900 if cfg.architecture.workload.schema_type.value == "financial" else 300,
                        _maint_end,
                        trigger=(
                            f"{_attempted} maintenance and compaction statements ran"
                            if _attempted is not None
                            else "maintenance statement count unknown; waiting to be safe"
                        ),
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
            _bench_recorded = False
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
                        # No statement ran: there is no maintenance to value.
                        maint_elapsed if _settle_skip is None else 0.0,
                        _settle,
                        stopped_reason=maint_stop_reason,
                        live_streams_reason=maint_live_reason,
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
                    engine=bench_result.engine,
                )
                collector.record_benchmark(bench_metrics)

                _bench_problems = _benchmark_gate_problems(cfg, bench_result.queries)
                for _p in _bench_problems:
                    print_error(_p)
                if _bench_problems:
                    pipeline_success = False
                _bench_recorded = True

                # Customer 360 benchmark row counts (reporting only, D6).
                if (
                    collector.current_run is not None
                    and collector.current_run.c360_correctness is not None
                ):
                    from lakebench.metrics import c360_correctness as _c360

                    try:
                        _c360_rec = _c360.add_benchmark_checks(
                            collector.current_run.c360_correctness, bench_result.queries
                        )
                        collector.current_run.c360_correctness = _c360_rec
                        for _line in _c360.summary_lines(_c360_rec):
                            (print_warning if _c360_rec["status"] != "pass" else print_info)(_line)
                    except Exception as e:  # noqa: BLE001 -- reporting only
                        print_warning(f"Customer 360 benchmark row check could not run: {e}")
                    # Empty until the owner approves the checks' meaning (D6).
                    for _p in _c360.gating_problems(
                        collector.current_run.c360_correctness, only=("benchmark_rows_",)
                    ):
                        print_error(_p)
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
                if _bench_recorded:
                    # The benchmark completed and is recorded; only the
                    # bookkeeping after it raised. Keep the result.
                    print_warning(f"Benchmark post-processing failed: {e}")
                else:
                    pipeline_success = False
                    benchmark_qph = None
                    _bench_error = f"{type(e).__name__}: {e}"
                    if collector.current_run is not None:
                        collector.current_run.benchmark = None
                        collector.current_run.benchmark_error = _bench_error
                    print_error(f"Benchmark did not complete ({_bench_error}); QpH not recorded")
                    print_info(
                        "The pipeline stages finished; re-run the benchmark with 'lakebench benchmark'."
                    )
                    _journal_safe(
                        j.record,
                        EventType.BENCHMARK_COMPLETE,
                        message=f"Benchmark did not complete: {_bench_error}",
                        success=False,
                        details={"error": _bench_error, "qph": None},
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
                    if maint_stop_reason:
                        pb.maintenance_stopped = True
                        pb.maintenance_stop_reason = maint_stop_reason
                    if maint_live_reason:
                        pb.maintenance_live_streams = True
                        pb.maintenance_live_streams_reason = maint_live_reason
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
                    if _settle_skip is not None:
                        pb.maintenance_settle = _settle_skip
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
                            + "\n\nReport: report.html in the run directory",
                            title="Pipeline Complete",
                            expand=False,
                        )
                    )

            metrics_path = metrics_storage.save_run(run_metrics)
            print_info(f"Metrics saved to {metrics_path}")
            print_info(f"Run ID: {run_id}")
            write_run_report(metrics_storage, run_id)

            _journal_safe(
                j.record,
                EventType.METRICS_SAVED,
                message=f"Metrics saved for run {run_id}",
                details={"run_id": run_id, "metrics_path": str(metrics_path)},
            )

        _journal_safe(j.end_command, success=pipeline_success)
        if not pipeline_success:
            # Metrics are saved above for diagnosis; the exit code must still
            # say the run did not succeed. A4 (v1.6): honour a specific code
            # (e.g. EXIT_DATAGEN_TIMEOUT) that the try block set before
            # raising, so wrappers can tell datagen timeout apart from
            # generic failure.
            raise typer.Exit(_pipeline_exit_code)
