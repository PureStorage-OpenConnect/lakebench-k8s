"""Sustained pipeline helpers for Lakebench CLI.

Extracted from cli/__init__.py to reduce file size.
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import typer
from rich.panel import Panel
from rich.table import Table

if TYPE_CHECKING:
    from rich.console import Console

from lakebench.cli._helpers import (
    _journal_safe,
    console,
    journal_open,
    print_error,
    print_info,
    print_success,
    print_warning,
)
from lakebench.config.schema import PipelineMode
from lakebench.journal import CommandName, EventType
from lakebench.k8s import K8sConnectionError, get_k8s_client

logger = logging.getLogger(__name__)


def _find_prometheus_svc(namespace: str) -> str | None:
    """Find the Prometheus service name in the given namespace.

    The kube-prometheus-stack Helm chart truncates the service name based on
    the release name length, so we cannot predict it. We try the K8s Python
    client first, then fall back to kubectl.
    """
    from lakebench.deploy.observability import HELM_RELEASE_NAME

    label = f"release={HELM_RELEASE_NAME},app=kube-prometheus-stack-prometheus"

    # Attempt 1: K8s Python client
    try:
        from kubernetes import client as k8s_client
        from kubernetes import config as k8s_config

        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()

        v1 = k8s_client.CoreV1Api()
        svcs = v1.list_namespaced_service(namespace, label_selector=label)
        if svcs.items:
            return svcs.items[0].metadata.name
    except Exception:
        pass

    # Attempt 2: kubectl fallback
    try:
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "svc",
                "-n",
                namespace,
                "-l",
                label,
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        svc_name = result.stdout.strip()
        if result.returncode == 0 and svc_name:
            return svc_name
    except Exception:
        pass

    return None


def _collect_platform_metrics(cfg, run_metrics) -> None:
    """Collect platform metrics from Prometheus and attach to run_metrics.

    Only runs when observability is enabled. Best-effort -- failures are
    logged as warnings but do not affect the pipeline result.

    When running outside the K8s cluster (no CoreDNS), uses kubectl
    port-forward to reach Prometheus.
    """
    if not cfg.observability.enabled:
        return

    try:
        from lakebench.observability.platform_collector import PlatformCollector

        namespace = cfg.get_namespace()
        svc_name = _find_prometheus_svc(namespace)
        if not svc_name:
            console.print("  [yellow]Could not find Prometheus service[/yellow]")
            return

        # Try in-cluster DNS first (fast path when running inside K8s)
        prometheus_url = f"http://{svc_name}.{namespace}.svc:9090"
        try:
            import httpx

            httpx.get(f"{prometheus_url}/api/v1/status/config", timeout=5)
            # DNS resolved and Prometheus responded -- use this URL
        except Exception:
            # DNS failed -- we are outside the cluster. Use port-forward.
            import socket

            local_port = _find_free_port()
            pf_proc = subprocess.Popen(
                [
                    "kubectl",
                    "port-forward",
                    f"svc/{svc_name}",
                    f"{local_port}:9090",
                    "-n",
                    namespace,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            # Wait for port-forward to be ready
            import time

            for _ in range(20):
                time.sleep(0.5)
                try:
                    with socket.create_connection(("127.0.0.1", local_port), timeout=1):
                        break
                except OSError:
                    continue
            else:
                pf_proc.kill()
                console.print("  [yellow]Could not establish port-forward to Prometheus[/yellow]")
                return

            prometheus_url = f"http://127.0.0.1:{local_port}"

        print_info("Collecting platform metrics from Prometheus...")
        collector = PlatformCollector(prometheus_url, namespace)
        pm = collector.collect(run_metrics.start_time, run_metrics.end_time or datetime.now())
        run_metrics.platform_metrics = pm.to_dict()

        # Clean up port-forward if we started one
        if "pf_proc" in locals():
            pf_proc.kill()
            pf_proc.wait()

        if pm.collection_error:
            console.print(f"  [yellow]Platform metrics partial: {pm.collection_error}[/yellow]")
        else:
            pod_count = len(pm.pods)
            engine_tag = ""
            if pm.engine.has_data:
                engine_tag = " (+ engine metrics)"
            print_success(f"Platform metrics collected: {pod_count} pods{engine_tag}")
    except Exception as e:
        # Clean up port-forward on error
        if "pf_proc" in locals():
            pf_proc.kill()
            pf_proc.wait()
        console.print(f"  [yellow]Could not collect platform metrics: {e}[/yellow]")


def _find_free_port() -> int:
    """Find an available local TCP port."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _parse_spark_interval(interval_str: str) -> int:
    """Parse a Spark-style interval string to seconds.

    Handles: ``"30 seconds"``, ``"5 minutes"``, ``"1 minute"``.
    Falls back to 300 if unparseable.
    """
    parts = interval_str.strip().lower().split()
    if len(parts) != 2:
        return 300
    try:
        value = int(parts[0])
    except ValueError:
        return 300
    unit = parts[1].rstrip("s")  # "minutes" -> "minute"
    if unit == "second":
        return value
    elif unit == "minute":
        return value * 60
    elif unit == "hour":
        return value * 3600
    return 300


def _run_iceberg_maintenance(
    cfg,
    k8s,
    console: Console,
    j,
    retention_threshold: str,
) -> None:
    """Run table maintenance (format-aware).

    - Iceberg: expire_snapshots + remove_orphan_files
    - Delta: VACUUM

    Engine-aware: uses Trino (preferred) or Spark Thrift Server.
    DuckDB cannot run maintenance -- skipped with a warning.
    Failures on individual tables are logged but do not abort.
    """
    from lakebench.deploy.iceberg import (
        exec_sql,
        find_maintenance_engine,
    )

    namespace = cfg.get_namespace()
    engine_type = cfg.architecture.query_engine.type.value
    table_format = cfg.architecture.table_format.type.value

    if engine_type == "duckdb":
        console.print(
            f"  [dim]{table_format.title()} maintenance skipped (DuckDB cannot run maintenance)[/dim]"
        )
        return

    if table_format == "delta" and engine_type == "spark-thrift":
        console.print("  [dim]Delta maintenance skipped (VACUUM OOMs Spark Thrift at 4Gi)[/dim]")
        return

    engine, pod_name, catalog = find_maintenance_engine(cfg, namespace)
    if engine is None or pod_name is None or catalog is None:
        console.print(
            f"  [dim]{table_format.title()} maintenance skipped (no capable engine pod found)[/dim]"
        )
        return

    tables = cfg.architecture.tables
    table_names = [
        f"{catalog}.{tables.bronze}",
        f"{catalog}.{tables.silver}",
        f"{catalog}.{tables.gold}",
    ]

    # Build SQL based on table format
    if table_format == "delta":
        from lakebench.deploy.delta_maintenance import (
            build_delta_maintenance_sql,
            parse_retention_to_hours,
        )

        retention_hours = parse_retention_to_hours(retention_threshold)

        def build_sql(tbl):
            return build_delta_maintenance_sql(engine, catalog, tbl, retention_hours)
    else:
        from lakebench.deploy.iceberg import build_maintenance_sql

        def build_sql(tbl):
            return build_maintenance_sql(engine, catalog, tbl, retention_threshold)

    maintained = 0
    expected_ops = 0
    for table in table_names:
        ops = build_sql(table)
        expected_ops += len(ops)
        for sql in ops:
            try:
                exec_sql(engine, k8s, pod_name, namespace, sql)
                maintained += 1
            except Exception as e:
                logger.warning("%s maintenance failed for %s: %s", table_format.title(), table, e)

    console.print(
        f"  {table_format.title()} maintenance ({engine}): {maintained}/{expected_ops} "
        f"operations (threshold: {retention_threshold})"
    )
    _journal_safe(
        j.record,
        EventType.STREAMING_HEALTH,
        message=f"{table_format.title()} maintenance",
        details={
            "engine": engine,
            "table_format": table_format,
            "retention_threshold": retention_threshold,
            "operations_succeeded": maintained,
            "operations_total": expected_ops,
        },
    )


def _run_iceberg_compaction(
    cfg,
    k8s,
    console: Console,
    j,
    file_size_threshold: str = "128MB",
) -> None:
    """Run table compaction (format-aware).

    - Iceberg: rewrite_data_files / optimize
    - Delta: OPTIMIZE

    Merges small files produced by streaming micro-batches or repeated
    incremental writes.  DuckDB cannot run compaction -- skipped.
    """
    from lakebench.deploy.iceberg import (
        exec_sql,
        find_maintenance_engine,
    )

    namespace = cfg.get_namespace()
    engine_type = cfg.architecture.query_engine.type.value
    table_format = cfg.architecture.table_format.type.value

    if engine_type == "duckdb":
        console.print(f"  [dim]{table_format.title()} compaction skipped (DuckDB read-only)[/dim]")
        return

    # Delta OPTIMIZE rewrites the entire table in a single pass.  Both Trino
    # workers (~8GiB) and Spark Thrift Server (~4GiB) can OOM and restart,
    # causing benchmark queries to fail.  In batch mode, Delta tables are
    # written in a single Spark job and don't accumulate the small files that
    # OPTIMIZE is designed to fix.  Skip it pre-benchmark to avoid crashing
    # the query engine.  OPTIMIZE is still run in the sustained monitoring
    # loop where small-file proliferation is the actual problem.
    if table_format == "delta" and engine_type in ("trino", "spark-thrift"):
        console.print("  [dim]Delta compaction skipped (OPTIMIZE not run pre-benchmark)[/dim]")
        return

    engine, pod_name, catalog = find_maintenance_engine(cfg, namespace)
    if engine is None or pod_name is None or catalog is None:
        console.print(
            f"  [dim]{table_format.title()} compaction skipped (no capable engine pod found)[/dim]"
        )
        return

    tables = cfg.architecture.tables
    table_names = [
        f"{catalog}.{tables.silver}",
        f"{catalog}.{tables.gold}",
    ]

    # Build SQL based on table format
    if table_format == "delta":
        from lakebench.deploy.delta_maintenance import build_delta_compaction_sql

        def build_sql(tbl):
            return build_delta_compaction_sql(engine, catalog, tbl)
    else:
        from lakebench.deploy.iceberg import build_compaction_sql

        def build_sql(tbl):
            return build_compaction_sql(engine, catalog, tbl, file_size_threshold)

    compacted = 0
    for table in table_names:
        for sql in build_sql(table):
            try:
                exec_sql(engine, k8s, pod_name, namespace, sql)
                compacted += 1
            except Exception as e:
                logger.warning("%s compaction failed for %s: %s", table_format.title(), table, e)

    console.print(
        f"  {table_format.title()} compaction ({engine}): {compacted}/{len(table_names)} "
        f"tables (threshold: {file_size_threshold})"
    )


def _wait_for_query_engine_ready(cfg, k8s, console, timeout: int = 180) -> None:
    """Poll the Trino cluster until all expected worker nodes are active.

    Compaction (e.g. Delta OPTIMIZE via Trino) can exhaust per-node memory
    and cause Trino worker pods to restart.  A SELECT 1 health check passes
    as soon as the coordinator responds, even while workers are still
    reconnecting.  Instead, query system.runtime.nodes and wait until the
    expected worker count is present -- this guarantees distributed queries
    will succeed.

    Silently returns if the engine is DuckDB or Spark Thrift (no pod restart risk).
    """
    import time

    engine_type = cfg.architecture.query_engine.type.value
    if engine_type not in ("trino",):
        return

    from lakebench.benchmark.executor import get_executor

    namespace = cfg.get_namespace()
    executor = get_executor(cfg, namespace)

    # Determine expected worker count from Trino StatefulSet replica count
    try:
        import subprocess as _sp

        result = _sp.run(
            [
                "kubectl",
                "get",
                "statefulset",
                "lakebench-trino-worker",
                "-n",
                namespace,
                "-o",
                "jsonpath={.spec.replicas}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        expected_workers = int(result.stdout.strip() or "1")
    except Exception:
        expected_workers = 1

    deadline = time.monotonic() + timeout
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        try:
            qr = executor.execute_query(
                "SELECT COUNT(*) FROM system.runtime.nodes WHERE state = 'active' AND coordinator = false",
                timeout=15,
            )
            if qr.success and qr.raw_output.strip().strip('"').isdigit():
                active_workers = int(qr.raw_output.strip().strip('"'))
                if active_workers >= expected_workers:
                    if attempt > 1:
                        console.print(
                            f"  [dim]Trino workers ready: {active_workers}/{expected_workers} "
                            f"(waited {attempt * 5}s)[/dim]"
                        )
                    return
        except Exception:
            pass
        time.sleep(5)

    console.print(
        f"  [dim]Trino worker readiness check timed out after {timeout}s -- proceeding[/dim]"
    )


def _probe_table_health(cfg, k8s) -> dict[str, int]:
    """Query table metadata for file/snapshot counts on silver and gold.

    Format-aware: uses Iceberg system tables or Delta DESCRIBE DETAIL.
    Returns a dict like {"silver_data_file_count": N, "silver_snapshot_count": N, ...}.
    Returns empty dict if no engine is available.
    """
    from lakebench.deploy.iceberg import (
        find_maintenance_engine,
        query_sql,
    )

    namespace = cfg.get_namespace()
    engine_type = cfg.architecture.query_engine.type.value
    table_format = cfg.architecture.table_format.type.value

    if engine_type == "duckdb":
        return {}

    engine, pod_name, catalog = find_maintenance_engine(cfg, namespace)
    if engine is None or pod_name is None or catalog is None:
        return {}

    tables = cfg.architecture.tables
    health: dict[str, int] = {}

    # Format-conditional health SQL builder
    if table_format == "delta":
        from lakebench.deploy.delta_maintenance import build_delta_table_health_sql

        def _build_health(eng, tbl):
            return build_delta_table_health_sql(eng, catalog, tbl)
    else:
        from lakebench.deploy.iceberg import build_table_health_sql

        def _build_health(eng, tbl):
            return build_table_health_sql(eng, tbl)

    for label, table_ref in [("silver", tables.silver), ("gold", tables.gold)]:
        fq = f"{catalog}.{table_ref}"
        for metric_name, sql in _build_health(engine, fq).items():
            try:
                import re as _re

                stdout = query_sql(engine, k8s, pod_name, namespace, sql)
                # Parse count from output.  Trino CLI prints a bare number;
                # beeline wraps results in pipes and column headers.  Extract
                # the first integer from any non-header line.
                _found = False
                for line in stdout.strip().splitlines():
                    cleaned = line.strip().strip("|").strip().strip('"').strip()
                    if not cleaned or cleaned.startswith("-"):
                        continue
                    # Skip column header lines (contain letters like "count")
                    if _re.fullmatch(r"\d+", cleaned):
                        health[f"{label}_{metric_name}"] = int(cleaned)
                        _found = True
                        break
                    # Beeline tabular: number may be padded with spaces
                    m = _re.fullmatch(r"\s*(\d+)\s*", cleaned)
                    if m:
                        health[f"{label}_{metric_name}"] = int(m.group(1))
                        _found = True
                        break
                if not _found:
                    health[f"{label}_{metric_name}"] = -1
            except Exception:
                health[f"{label}_{metric_name}"] = -1

    return health


def _run_benchmark_round(
    cfg,
    bench_runner,
    collector,
    console,
    round_index: int,
    j,
    k8s=None,
) -> None:
    """Execute a single in-stream benchmark round.

    Flushes the Trino metadata cache, probes gold-table freshness, runs
    the full 8-query power benchmark, and records the result as a
    benchmark round.  If Q9 (the gold-table query) fails, retries up to
    twice with 30s/60s backoff to ride out the ``createOrReplace()``
    window.
    """
    from lakebench.metrics import BenchmarkMetrics, BenchmarkRoundMeta

    round_meta = BenchmarkRoundMeta(
        round_index=round_index,
        timestamp=datetime.now(),
    )

    # Table health probe (v1.1.0)
    if k8s is not None:
        try:
            health = _probe_table_health(cfg, k8s)
            round_meta.silver_data_file_count = health.get("silver_data_file_count", 0)
            round_meta.silver_snapshot_count = health.get("silver_snapshot_count", 0)
            round_meta.gold_data_file_count = health.get("gold_data_file_count", 0)
            round_meta.gold_snapshot_count = health.get("gold_snapshot_count", 0)
        except Exception:
            pass  # Health probe failure should not block the benchmark

    # 1. Flush Trino metadata cache
    try:
        bench_runner.executor.flush_cache()
    except Exception:
        pass

    # 2. Freshness probe: measure gold table staleness at query time
    try:
        catalog = bench_runner.catalog
        gold_table = bench_runner.gold_table
        freshness_sql = (
            f"SELECT date_diff('second', CAST(MAX(interaction_date) AS timestamp), current_timestamp) "
            f"FROM {catalog}.{gold_table}"
        )
        freshness_sql = bench_runner.executor.adapt_query(freshness_sql)
        freshness_result = bench_runner.executor.execute_query(freshness_sql, timeout=30)
        if freshness_result.success and freshness_result.raw_output.strip():
            try:
                round_meta.gold_freshness_seconds = float(
                    freshness_result.raw_output.strip().split("\n")[0]
                )
            except (ValueError, IndexError):
                pass
    except Exception:
        pass

    # 3. Run the full 8-query power benchmark
    bench_result = bench_runner.run_power(cache="hot")

    # 4. Check Q9 for contention (gold-table query)
    q9_failed = False
    for qr in bench_result.queries:
        if qr.query.name == "q9" and not qr.success:
            q9_failed = True
            break

    if q9_failed:
        round_meta.q9_contention_observed = True
        q9_query = None
        for qr in bench_result.queries:
            if qr.query.name == "q9":
                q9_query = qr.query
                break
        if q9_query is not None:
            # Two retries with increasing backoff (30s, 60s) to ride out
            # the gold createOrReplace() window at larger scales.
            for attempt, wait in enumerate([30, 60], start=1):
                console.print(f"    Q9 failed (gold contention) -- retry {attempt}/2 in {wait}s...")
                time.sleep(wait)
                try:
                    bench_runner.executor.flush_cache()
                except Exception:
                    pass
                retry_result = bench_runner._execute_single_query(q9_query)
                if retry_result.success:
                    round_meta.q9_retry_used = True
                    # Replace Q9 in results
                    for i, qr in enumerate(bench_result.queries):
                        if qr.query.name == "q9":
                            bench_result.queries[i] = retry_result
                            break
                    # Recompute total_seconds and qph
                    bench_result.total_seconds = sum(
                        qr.elapsed_seconds for qr in bench_result.queries
                    )
                    if bench_result.total_seconds > 0:
                        bench_result.qph = (
                            len(bench_result.queries) / bench_result.total_seconds
                        ) * 3600
                    break

    # 5. Build BenchmarkMetrics with round_meta
    passed = sum(1 for qr in bench_result.queries if qr.success)
    total = len(bench_result.queries)

    bench_metrics = BenchmarkMetrics(
        mode=bench_result.mode,
        cache=bench_result.cache,
        scale=bench_result.scale,
        qph=bench_result.qph,
        total_seconds=bench_result.total_seconds,
        queries=[q.to_dict() for q in bench_result.queries],
        iterations=bench_result.iterations,
        round_meta=round_meta,
    )

    # 6. Record the round
    collector.record_benchmark_round(bench_metrics)

    # 7. Print inline result
    freshness_str = (
        f" | Freshness: {round_meta.gold_freshness_seconds:.1f}s"
        if round_meta.gold_freshness_seconds > 0
        else ""
    )
    q9_str = ""
    if round_meta.q9_contention_observed:
        q9_str = " | Q9: retry" if round_meta.q9_retry_used else " | Q9: contention"
    console.print(
        f"  Round {round_index}: {passed}/{total} passed "
        f"| QpH: {bench_result.qph:.1f}{freshness_str}{q9_str}"
    )

    _journal_safe(
        j.record,
        EventType.STREAMING_HEALTH,
        message=f"Benchmark round {round_index}",
        details={
            "round": round_index,
            "qph": round(bench_result.qph, 1),
            "passed": passed,
            "total": total,
            "freshness_seconds": round(round_meta.gold_freshness_seconds, 2),
            "q9_contention": round_meta.q9_contention_observed,
        },
    )


def _print_rounds_summary(console, rounds: list) -> None:
    """Print a Rich table summarizing all in-stream benchmark rounds."""
    table = Table(title="Benchmark Rounds (In-Stream)", expand=False)
    table.add_column("Round", justify="right", style="bold")
    table.add_column("QpH", justify="right")

    # Collect query names from the first round
    query_names: list[str] = []
    if rounds and rounds[0].queries:
        for q in rounds[0].queries:
            name = q.get("name", "?")
            query_names.append(name)
            table.add_column(name, justify="right")

    table.add_column("Freshness", justify="right")
    table.add_column("Q9", justify="center")

    import statistics

    qph_values: list[float] = []
    freshness_values: list[float] = []

    for rnd in rounds:
        meta = rnd.round_meta
        row: list[str] = [str(meta.round_index if meta else "?")]
        row.append(f"{rnd.qph:.1f}")
        qph_values.append(rnd.qph)

        # Per-query elapsed times
        for qname in query_names:
            matched = False
            for q in rnd.queries:
                if q.get("name") == qname:
                    row.append(f"{q['elapsed_seconds']:.1f}s")
                    matched = True
                    break
            if not matched:
                row.append("-")

        # Freshness
        if meta and meta.gold_freshness_seconds > 0:
            row.append(f"{meta.gold_freshness_seconds:.1f}s")
            freshness_values.append(meta.gold_freshness_seconds)
        else:
            row.append("-")

        # Q9 status
        if meta and meta.q9_contention_observed:
            row.append("retry" if meta.q9_retry_used else "FAIL")
        else:
            row.append("OK")

        table.add_row(*row)

    console.print(table)

    # Summary line
    median_qph = statistics.median(qph_values) if qph_values else 0.0
    median_freshness = statistics.median(freshness_values) if freshness_values else 0.0
    parts = [f"Median QpH: {median_qph:.1f}"]
    if median_freshness > 0:
        parts.append(f"Median freshness: {median_freshness:.1f}s")
    console.print(f"  {' | '.join(parts)}")


def _run_sustained(
    cfg,
    config_file: Path,
    timeout: int,
    skip_benchmark: bool,
    duration: int | None,
) -> None:
    """Run the sustained streaming pipeline.

    Starts datagen concurrently with bronze-ingest, silver-stream, and
    gold-refresh streaming SparkApplications. Monitors for the configured
    duration, then stops streaming jobs and optionally runs the benchmark.
    """
    import uuid

    from lakebench.deploy import DatagenDeployer, DeploymentEngine, DeploymentStatus
    from lakebench.engine import get_engine
    from lakebench.metrics import MetricsCollector, MetricsStorage, StreamingJobMetrics
    from lakebench.spark import SparkJobMonitor, SparkOperatorManager
    from lakebench.spark.job import JobState, JobType, SparkJobManager

    run_duration = duration or cfg.architecture.pipeline.sustained.run_duration

    console.print(
        Panel(
            f"Running sustained pipeline for: [bold]{cfg.name}[/bold]\n\n"
            f"Stages: bronze-ingest + silver-stream + gold-refresh (concurrent)\n"
            f"Duration: {run_duration}s ({run_duration / 60:.0f} min)",
            expand=False,
        )
    )

    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.RUN, {"sustained": True, "duration": run_duration})

    collector = MetricsCollector()
    metrics_storage = MetricsStorage()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    from lakebench.metrics import build_config_snapshot

    config_snapshot = build_config_snapshot(cfg)
    collector.start_run(run_id, cfg.name, config_snapshot)

    pipeline_success = True
    _datagen_output_gb = 0.0
    _datagen_output_rows = 0
    streaming_jobs = [
        (JobType.BRONZE_INGEST, "bronze-ingest"),
        (JobType.SILVER_STREAM, "silver-stream"),
        (JobType.GOLD_REFRESH, "gold-refresh"),
    ]

    try:
        # Check Spark operator
        print_info("Checking Spark Operator...")
        spark_op_cfg = cfg.platform.compute.spark.operator
        operator = SparkOperatorManager(
            namespace=spark_op_cfg.namespace,
            version=spark_op_cfg.version if spark_op_cfg.install else None,
            job_namespace=cfg.get_namespace(),
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

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        job_manager: SparkJobManager = get_engine(cfg, k8s)  # type: ignore[assignment]
        monitor = SparkJobMonitor(cfg, k8s)

        # Deploy scripts ConfigMap (includes streaming scripts) -- must succeed
        print_info("Deploying Spark scripts...")
        if not job_manager.deploy_scripts_configmap():
            print_error("Failed to deploy Spark scripts ConfigMap -- pipeline cannot proceed")
            _journal_safe(j.end_command, success=False, message="Scripts ConfigMap deploy failed")
            raise typer.Exit(1)
        print_success("Spark scripts deployed")

        # Start datagen (runs concurrently with streaming jobs)
        console.print()
        console.print("[bold]Starting datagen...[/bold]")
        engine = DeploymentEngine(cfg)
        datagen = DatagenDeployer(engine)
        datagen_result = datagen.deploy()
        if datagen_result.status != DeploymentStatus.SUCCESS:
            print_error(f"Failed to start datagen: {datagen_result.message}")
            pipeline_success = False
            raise typer.Exit(1)
        print_success("Datagen started (sustained mode)")
        dims = cfg.get_scale_dimensions()
        console.print(f"  Scale: {dims.scale}")
        console.print(f"  Parallelism: {cfg.architecture.workload.datagen.parallelism} pods")
        _journal_safe(
            j.record,
            EventType.GENERATE_START,
            message="Datagen started for sustained pipeline",
            details={
                "scale": dims.scale,
                "parallelism": cfg.architecture.workload.datagen.parallelism,
                "target_gb": round(dims.approx_bronze_gb, 1),
            },
        )

        # Launch all streaming jobs concurrently
        console.print()
        console.print("[bold]Launching streaming jobs...[/bold]")

        _journal_safe(
            j.record,
            EventType.STREAMING_START,
            message="Starting streaming pipeline",
            details={"jobs": [name for _, name in streaming_jobs]},
        )

        submitted = []
        for job_type, job_name in streaming_jobs:
            job_status = job_manager.submit_job(job_type)
            if job_status.state == JobState.FAILED:
                print_error(f"Failed to submit {job_name}: {job_status.message}")
                pipeline_success = False
                raise typer.Exit(1)
            print_success(f"Submitted: lakebench-{job_name}")
            submitted.append((job_type, job_name))

        # Monitor for configured duration, running benchmark rounds at intervals
        console.print()
        print_info(
            f"Streaming pipeline running for {run_duration}s ({run_duration / 60:.0f} min)..."
        )

        # In-stream benchmark scheduling
        sustained_cfg = cfg.architecture.pipeline.sustained
        bench_warmup = sustained_cfg.benchmark_warmup
        bench_interval = sustained_cfg.benchmark_interval
        gold_interval_s = _parse_spark_interval(sustained_cfg.gold_refresh_interval)

        # Hard floor: both warmup and interval must be >= gold_refresh_interval.
        # Gold rewrites the entire table each cycle via createOrReplace().
        # Warmup < gold interval -> Q9 hits an empty/stale table, inflated QpH.
        # Interval < gold interval -> rounds overlap with gold rewrites, Q9
        # contention and inconsistent QpH across rounds.
        if bench_warmup < gold_interval_s and not skip_benchmark:
            print_warning(
                f"benchmark_warmup ({bench_warmup}s) is less than "
                f"gold_refresh_interval ({gold_interval_s}s) -- "
                f"clamping warmup to {gold_interval_s}s."
            )
            bench_warmup = gold_interval_s
        if bench_interval < gold_interval_s and not skip_benchmark:
            print_warning(
                f"benchmark_interval ({bench_interval}s) is less than "
                f"gold_refresh_interval ({gold_interval_s}s) -- "
                f"clamping interval to {gold_interval_s}s."
            )
            bench_interval = gold_interval_s

        # Guard: skip in-stream rounds if run duration is too short
        can_run_rounds = not skip_benchmark and run_duration >= bench_warmup + bench_interval
        if not skip_benchmark and not can_run_rounds:
            print_warning(
                f"Run duration ({run_duration}s) too short for in-stream benchmarking "
                f"(need >= {bench_warmup + bench_interval}s). "
                f"No benchmark rounds will run."
            )

        # Pre-create benchmark runner for in-stream rounds
        bench_runner_instream = None
        if can_run_rounds:
            try:
                from lakebench.benchmark import BenchmarkRunner

                bench_runner_instream = BenchmarkRunner(cfg)
                print_info(
                    f"In-stream benchmarking: warmup {bench_warmup}s, then every {bench_interval}s"
                )
            except Exception as e:
                print_warning(f"Could not create benchmark runner: {e}")
                can_run_rounds = False

        # Iceberg retention scheduling
        retention_interval = sustained_cfg.retention_interval
        retention_threshold = sustained_cfg.retention_threshold
        next_maintenance_at = float(retention_interval)  # first run after one interval
        print_info(
            f"Iceberg retention: every {retention_interval}s (threshold: {retention_threshold})"
        )

        # Iceberg compaction scheduling (v1.1.0)
        compaction_enabled = sustained_cfg.compaction_enabled
        compaction_interval = sustained_cfg.compaction_interval
        next_compaction_at = float(compaction_interval) if compaction_enabled else float("inf")
        if compaction_enabled:
            print_info(f"Iceberg compaction: every {compaction_interval}s")

        start = time.time()
        check_interval = 30
        # First round fires after warmup (no offset -- Q9 contention
        # is handled by the retry logic inside _run_benchmark_round).
        next_round_at = bench_warmup if can_run_rounds else float("inf")
        round_index = 0
        # Adaptive guard: after the first round completes, use the
        # observed duration * 1.2 as the minimum remaining time for
        # subsequent rounds.  Before any round completes, use a small
        # fixed floor (60s) so we don't skip the first attempt on
        # short runs.  This scales naturally with cluster performance
        # -- fast clusters get more rounds, slow clusters don't start
        # rounds they can't finish.
        last_round_seconds: float = 0.0
        min_remaining_floor = 60

        while time.time() - start < run_duration:
            elapsed = time.time() - start
            remaining = run_duration - elapsed

            # Adaptive guard: use observed round time when available
            min_remaining = max(
                min_remaining_floor,
                last_round_seconds * 1.2,
            )

            # Check if it's time for a benchmark round
            if (
                can_run_rounds
                and elapsed >= next_round_at
                and remaining >= min_remaining
                and bench_runner_instream is not None
            ):
                round_index += 1
                console.print(
                    f"\n  [{elapsed:.0f}s / {run_duration}s] "
                    f"Starting benchmark round {round_index}..."
                )
                round_start = time.time()
                _run_benchmark_round(
                    cfg=cfg,
                    bench_runner=bench_runner_instream,
                    collector=collector,
                    console=console,
                    round_index=round_index,
                    j=j,
                    k8s=k8s,
                )
                last_round_seconds = time.time() - round_start
                # Next round at interval from round completion
                next_round_at = (time.time() - start) + bench_interval
                continue

            # Iceberg retention maintenance
            if elapsed >= next_maintenance_at:
                _run_iceberg_maintenance(cfg, k8s, console, j, retention_threshold)
                next_maintenance_at = (time.time() - start) + retention_interval

            # Iceberg compaction (v1.1.0)
            if compaction_enabled and elapsed >= next_compaction_at:
                _run_iceberg_compaction(cfg, k8s, console, j)
                next_compaction_at = (time.time() - start) + compaction_interval

            # Sleep until next event (health check, benchmark round, maintenance, or compaction)
            sleep_until = min(
                elapsed + check_interval,
                next_round_at if can_run_rounds else float("inf"),
                next_maintenance_at,
                next_compaction_at,
                run_duration,
            )
            sleep_time = max(0, sleep_until - (time.time() - start))
            if sleep_time > 0:
                time.sleep(sleep_time)

            # Periodic health check
            elapsed = time.time() - start
            remaining = run_duration - elapsed
            console.print(
                f"  [{elapsed:.0f}s / {run_duration}s] "
                f"Streaming jobs running... ({remaining:.0f}s remaining)"
            )

            _journal_safe(
                j.record,
                EventType.STREAMING_HEALTH,
                message="Health check",
                details={"elapsed_seconds": elapsed, "remaining_seconds": remaining},
            )

        # Measure bronze bucket for datagen stats (datagen runs concurrently,
        # so we measure after the monitoring window to capture all output)
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
            if dg_info.object_count:
                _datagen_output_rows = cfg.architecture.workload.datagen.scale * 1_500_000
        except Exception as e:
            logger.warning("Could not measure streaming bronze bucket size: %s", e)

        # Capture driver logs BEFORE stopping jobs (pods are deleted on stop)
        console.print()
        console.print("[bold]Collecting streaming metrics...[/bold]")
        namespace = cfg.get_namespace()
        driver_logs: dict[str, str | None] = {}
        for _job_type, job_name in submitted:
            try:
                logs = monitor._get_driver_logs(
                    f"lakebench-{job_name}",
                    tail_lines=None,
                )
                driver_logs[job_name] = logs
                # Diagnostic: report log capture status
                if logs is None:
                    print_warning(f"{job_name}: no driver logs (pod not found)")
                elif len(logs) == 0:
                    print_warning(f"{job_name}: driver logs empty (0 bytes)")
                else:
                    lines = logs.strip().split("\n")
                    console.print(
                        f"  {job_name}: captured {len(logs):,} bytes ({len(lines)} lines)"
                    )
                    # Show first line to verify format
                    if lines:
                        first = lines[0][:80] + "..." if len(lines[0]) > 80 else lines[0]
                        console.print(f"    first: {first}")
            except Exception as e:
                driver_logs[job_name] = None
                print_warning(f"{job_name}: log capture failed: {e}")

        # Stop streaming jobs
        console.print("[bold]Stopping streaming jobs...[/bold]")
        for _job_type, job_name in submitted:
            try:
                k8s.delete_custom_resource(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    plural="sparkapplications",
                    name=f"lakebench-{job_name}",
                    namespace=namespace,
                )
                print_success(f"Stopped: lakebench-{job_name}")
            except Exception as e:
                print_warning(f"Could not stop {job_name}: {e}")

        _journal_safe(
            j.record,
            EventType.STREAMING_STOP,
            message="Streaming pipeline stopped",
            details={"duration_seconds": run_duration},
        )

        # Record streaming metrics (from pre-captured driver logs)
        console.print()
        console.print("[bold]Parsing streaming metrics...[/bold]")
        for _job_type, job_name in submitted:
            logs = driver_logs.get(job_name)
            if logs:
                try:
                    streaming_metrics = collector.parse_streaming_logs(
                        logs,
                        job_name,
                    )
                    # Diagnostic: report what was parsed
                    console.print(
                        f"  {job_name}: {streaming_metrics.total_batches} batches, "
                        f"{streaming_metrics.total_rows_processed:,} rows"
                    )
                    if streaming_metrics.total_batches == 0:
                        # Show a sample of the logs to debug pattern mismatch
                        lines = [line for line in logs.split("\n") if line.strip()]
                        if lines:
                            console.print("    [dim]no batches parsed -- sample lines:[/dim]")
                            for sample in lines[:3]:
                                truncated = sample[:100] + "..." if len(sample) > 100 else sample
                                console.print(f"      {truncated}")
                except Exception as e:
                    print_warning(f"{job_name}: parse failed: {e}")
                    streaming_metrics = StreamingJobMetrics(
                        job_name=f"lakebench-{job_name}",
                        job_type=job_name,
                    )
            else:
                console.print(f"  {job_name}: no logs to parse")
                streaming_metrics = StreamingJobMetrics(
                    job_name=f"lakebench-{job_name}",
                    job_type=job_name,
                )

            streaming_metrics.elapsed_seconds = run_duration
            streaming_metrics.success = pipeline_success
            if streaming_metrics.elapsed_seconds > 0 and streaming_metrics.total_rows_processed > 0:
                streaming_metrics.throughput_rps = (
                    streaming_metrics.total_rows_processed / streaming_metrics.elapsed_seconds
                )
            collector.record_streaming(streaming_metrics)

        # Aggregate in-stream benchmark rounds
        if not skip_benchmark:
            try:
                from lakebench.metrics import aggregate_benchmark_rounds

                rounds = collector.current_run.benchmark_rounds if collector.current_run else []
                if rounds:
                    aggregated = aggregate_benchmark_rounds(rounds)
                    collector.record_benchmark(aggregated)
                    console.print()
                    console.print(
                        f"  In-stream median QpH: [bold]{aggregated.qph:.1f}[/bold] "
                        f"({len(rounds)} rounds)"
                    )
                    _print_rounds_summary(console, rounds)
            except Exception as e:
                print_warning(f"Benchmark aggregation failed: {e}")

        # Summary
        console.print(
            Panel(
                f"[green]Sustained pipeline completed![/green]\n\n"
                f"  Duration: {run_duration}s ({run_duration / 60:.0f} min)\n"
                f"  Streaming jobs: {len(submitted)}\n\n"
                f"Query results: lakebench query --example count\n"
                f"Generate report: lakebench report",
                title="Sustained Pipeline Complete",
                expand=False,
            )
        )

    except K8sConnectionError as e:
        print_error(f"Kubernetes connection failed: {e}")
        pipeline_success = False
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904
    finally:
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
            _total_s3_objects = collector.record_actual_sizes(
                s3_client,
                s3_cfg.buckets.bronze,
                s3_cfg.buckets.silver,
                s3_cfg.buckets.gold,
            )
        except Exception as e:
            _total_s3_objects = 0
            console.print(f"  [yellow]Could not measure S3 sizes: {e}[/yellow]")

        run_metrics = collector.end_run(success=pipeline_success)
        if run_metrics:
            # Collect platform metrics from Prometheus (best-effort)
            _collect_platform_metrics(cfg, run_metrics)

            # Build pipeline benchmark (stage-matrix view)
            try:
                from lakebench.metrics import build_pipeline_benchmark

                pb = build_pipeline_benchmark(
                    run_metrics,
                    datagen_output_gb=_datagen_output_gb,
                    datagen_output_rows=_datagen_output_rows,
                )
                pb.total_s3_objects = _total_s3_objects
                run_metrics.pipeline_benchmark = pb
                if pb.pipeline_mode == PipelineMode.SUSTAINED.value:
                    if pb.sustained_throughput_rps > 0:
                        latency_str = (
                            "/".join(f"{v:.0f}" for v in pb.stage_latency_profile)
                            if pb.stage_latency_profile
                            else "n/a"
                        )
                        freshness_label = "freshness"
                        freshness_val = pb.data_freshness_seconds
                        if (pb.query_time_freshness_seconds or 0) > 0:
                            freshness_label = "freshness (at query time)"
                            freshness_val = pb.query_time_freshness_seconds
                        freshness_str = (
                            f"{freshness_val:.1f}s" if freshness_val is not None else "n/a"
                        )
                        print_info(
                            f"Pipeline Score: {freshness_str} {freshness_label}"
                            f" | {pb.sustained_throughput_rps:,.0f} rows/s sustained"
                            f" | {latency_str}ms latency (b/s/g)"
                        )
                elif pb.time_to_value_seconds > 0:
                    print_info(
                        f"Pipeline Score: {pb.time_to_value_seconds:.1f}s time-to-value"
                        f" | {pb.pipeline_throughput_gb_per_second:.3f} GB/s throughput"
                    )
            except Exception as e:
                console.print(f"  [yellow]Could not build pipeline benchmark: {e}[/yellow]")

            metrics_path = metrics_storage.save_run(run_metrics)
            print_info(f"Metrics saved to {metrics_path}")
            print_info(f"Run ID: {run_id}")

        _journal_safe(j.end_command, success=pipeline_success)
