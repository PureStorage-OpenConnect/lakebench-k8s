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
    write_run_report,
)
from lakebench.config.schema import PipelineMode
from lakebench.journal import CommandName, EventType
from lakebench.k8s import K8sConnectionError, get_k8s_client

logger = logging.getLogger(__name__)


_C360_REQUIRED_STREAM_JOBS = ("bronze-ingest", "silver-stream")


def _c360_continuous_gate_problems(rows_by_job: dict[str, int | None]) -> list[str]:
    """Reasons a c360 continuous run must not pass: a required stream with no
    parseable logs, or one that processed zero rows."""
    problems = []
    for job in _C360_REQUIRED_STREAM_JOBS:
        if job not in rows_by_job:
            continue  # stage not part of this run
        rows = rows_by_job[job]
        if rows is None:
            problems.append(
                f"c360 continuous gate: no parseable {job} driver logs; cannot "
                "confirm data moved. Marking FAILURE."
            )
        elif rows == 0:
            problems.append(
                f"c360 continuous gate: {job} processed 0 rows over the whole run. Marking FAILURE."
            )
    return problems


def _reset_ownership_problem(cfg) -> str | None:
    """Why this run may not delete continuous state, or None when it may.

    Same verification destroy and clean use: the namespace must exist and
    carry this deployment's identity, and no other namespace may use the
    deployment name (they would share its buckets). Anything short of a
    verified match refuses, so a config pointing at another deployment's
    buckets cannot wipe that deployment's live checkpoints.
    """
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    from lakebench.deploy.ownership import (
        IdentityVerdict,
        build_identity_from_config,
        check_data_ownership,
        verify_namespace_identity,
    )

    ns = cfg.get_namespace()
    kube_ctx = cfg.platform.kubernetes.context or ""
    core_v1 = k8s_client.CoreV1Api()
    try:
        core_v1.read_namespace(ns)
    except ApiException as e:
        return f"namespace {ns} not readable ({e.status}); cannot verify bucket ownership"
    except Exception as e:  # noqa: BLE001
        return f"cannot reach the cluster to verify ownership: {e}"
    identity = build_identity_from_config(cfg, context=kube_ctx)
    v = verify_namespace_identity(core_v1, ns, identity.name, identity.api_server)
    if v.verdict is not IdentityVerdict.MATCH:
        return f"namespace {ns} identity not verified ({v.verdict.name}): {v.hint}"
    decision = check_data_ownership(
        core_v1,
        namespace=ns,
        deployment_name=cfg.name,
        namespace_present=True,
        namespace_verified=True,
        force_legacy=False,
        context_name=kube_ctx,
    )
    if not decision.allowed:
        return decision.hint
    return _bucket_ownership_problem(cfg, core_v1)


def _bucket_ownership_problem(cfg, core_v1) -> str | None:
    """Per-bucket check, same rules destroy applies before emptying buckets.

    The namespace check alone only catches another namespace with the SAME
    deployment name; two deployments with different names sharing bucket
    names would still pass it and one reset would delete the other's live
    checkpoints and raw data.
    """
    from lakebench.deploy.ownership import (
        IdentityVerdict,
        bucket_name_matches_deployment,
        list_lakebench_deployment_names,
        verify_bucket_ownership,
    )
    from lakebench.s3 import S3Client

    s3_cfg = cfg.platform.storage.s3
    raw = S3Client(
        endpoint=s3_cfg.endpoint,
        access_key=s3_cfg.access_key,
        secret_key=s3_cfg.secret_key,
        region=s3_cfg.region,
        path_style=s3_cfg.path_style,
        ca_cert=s3_cfg.ca_cert,
        verify_ssl=s3_cfg.verify_ssl,
    ).raw_client
    others = None
    b = s3_cfg.buckets
    for bucket in (b.bronze, b.silver, b.gold):
        v = verify_bucket_ownership(raw, bucket, cfg.name)
        if v.verdict is IdentityVerdict.MATCH:
            continue
        if v.verdict is IdentityVerdict.UNSUPPORTED:
            if others is None:
                others = list_lakebench_deployment_names(core_v1, exclude=cfg.get_namespace())
            if others is not None and bucket_name_matches_deployment(bucket, cfg.name, others):
                continue
            return (
                f"bucket {bucket}: backend has no bucket tagging and the name does not "
                f"prove it belongs to deployment {cfg.name!r}"
            )
        return f"bucket {bucket}: {v.verdict.name} ({v.hint})"
    return None


_STREAM_APPS = ("lakebench-bronze-ingest", "lakebench-silver-stream", "lakebench-gold-refresh")


def _stop_leftover_streams(job_manager, namespace: str, timeout_s: int = 120) -> None:
    """Delete stream apps left by an earlier run and wait for their drivers.

    A stream still alive during the reset could write its checkpoint right
    after it was deleted, and the new stream would resume stale offsets.
    Raises typer.Exit if a driver is still present after ``timeout_s``.
    """
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    for app in _STREAM_APPS:
        job_manager._delete_job(app)
    core = k8s_client.CoreV1Api()
    deadline = time.time() + timeout_s
    for app in _STREAM_APPS:
        while True:
            try:
                core.read_namespaced_pod(f"{app}-driver", namespace)
            except ApiException as e:
                if e.status == 404:
                    break
                raise
            if time.time() > deadline:
                print_error(f"{app}-driver still running {timeout_s}s after deletion")
                raise typer.Exit(1)
            time.sleep(3)


def _require_reset_ownership(cfg) -> None:
    """typer.Exit unless this run provably owns the namespace and buckets."""
    try:
        problem = _reset_ownership_problem(cfg)
    except Exception as e:  # noqa: BLE001
        problem = f"ownership could not be verified: {e}"
    if problem:
        print_error(f"Refusing to reset continuous state: {problem}")
        print_info(
            "Continuous runs delete the previous run's checkpoints, tables and raw "
            "data, so they require proof of ownership: buckets tagged by "
            "`lakebench deploy`, or on backends without bucket tagging, bucket "
            "names prefixed with the deployment name."
        )
        raise typer.Exit(1)


def _reset_continuous_state(cfg, *, clear_raw: bool) -> None:
    """Start a continuous run clean: delete the stream checkpoints and,
    when this run generates its own data, the previous raw datagen files.

    Must run BEFORE datagen starts. Stale checkpoints made a rerun ingest
    nothing and still pass; stale raw files from a larger earlier run were
    streamed in next to the new corpus (ingest_ratio above 1). Raises
    typer.Exit on an ownership refusal or any delete failure.
    """
    from lakebench.s3 import S3Client

    _require_reset_ownership(cfg)

    s3_cfg = cfg.platform.storage.s3
    base = cfg.architecture.pipeline.sustained.checkpoint_base.strip("/")
    client = S3Client(
        endpoint=s3_cfg.endpoint,
        access_key=s3_cfg.access_key,
        secret_key=s3_cfg.secret_key,
        region=s3_cfg.region,
        path_style=s3_cfg.path_style,
        ca_cert=s3_cfg.ca_cert,
        verify_ssl=s3_cfg.verify_ssl,
    )
    b = s3_cfg.buckets
    targets = [
        (b.bronze, f"{base}/bronze-ingest"),
        (b.silver, f"{base}/silver-stream"),
        (b.gold, f"{base}/gold-refresh"),
    ]
    if clear_raw:
        raw = cfg.architecture.pipeline.medallion.bronze.path_template
        if raw == "customer/interactions" and cfg.architecture.workload.schema_type.value == (
            "financial"
        ):
            raw = "pacs008"
        targets.append((b.bronze, raw.strip("/")))
    for bucket, prefix in targets:
        try:
            n = client.delete_prefix(bucket, prefix)
        except Exception as e:
            print_error(f"Could not clear {bucket}/{prefix}: {e}")
            raise typer.Exit(1) from e
        if n:
            print_info(f"Cleared {n} objects from {bucket}/{prefix}")


def _c360_existing_state(cfg, *, clear_raw: bool) -> list[str]:
    """Bucket prefixes holding state a c360 continuous reset would delete.

    Read-only listing, one key per prefix. A prefix that cannot be listed is
    reported too, so the caller fails closed.
    """
    from lakebench.s3 import S3Client

    s3_cfg = cfg.platform.storage.s3
    raw_client = S3Client(
        endpoint=s3_cfg.endpoint,
        access_key=s3_cfg.access_key,
        secret_key=s3_cfg.secret_key,
        region=s3_cfg.region,
        path_style=s3_cfg.path_style,
        ca_cert=s3_cfg.ca_cert,
        verify_ssl=s3_cfg.verify_ssl,
    ).raw_client
    b = s3_cfg.buckets
    base = cfg.architecture.pipeline.sustained.checkpoint_base.strip("/")
    # Silver and gold buckets hold only the tables and stream checkpoints.
    prefixes = [
        (b.silver, ""),
        (b.gold, ""),
        (b.bronze, f"{base}/bronze-ingest/"),
        (b.bronze, "default/bronze_raw/"),
        (b.bronze, "warehouse/default.db/bronze_raw/"),
    ]
    if clear_raw:
        raw = cfg.architecture.pipeline.medallion.bronze.path_template.strip("/")
        prefixes.append((b.bronze, f"{raw}/"))
    found = []
    for bucket, prefix in prefixes:
        try:
            resp = raw_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        except Exception as e:  # noqa: BLE001
            if "NoSuchBucket" in str(e):
                continue
            found.append(f"{bucket}/{prefix} (could not list: {e})")
            continue
        if resp.get("KeyCount", len(resp.get("Contents", []))):
            found.append(f"{bucket}/{prefix}")
    return found


def _c360_only_fresh_generate(cfg, existing: list[str]) -> bool:
    """True when the only state found is the raw landing zone (LB-154).

    That is the documented deploy -> generate -> run flow on a deployment
    that has never run: no tables, no stream checkpoints, just a raw corpus.
    Any other entry, including a prefix that could not be listed, is not a
    fresh generate and keeps the refusal. Whether replacing the corpus is
    safe is a separate question, see ``_c360_raw_replace_problem``.
    """
    raw = cfg.architecture.pipeline.medallion.bronze.path_template.strip("/")
    raw_entry = f"{cfg.platform.storage.s3.buckets.bronze}/{raw}/"
    return bool(existing) and all(e == raw_entry for e in existing)


# A raw corpus up to this multiple of the run's own approx_bronze_gb is
# replaced without --force-reset: regenerating it costs no more than the
# datagen this run does anyway. Measured c360 corpora land at about 1.2x.
_RAW_REPLACE_SIZE_FACTOR = 1.5


def _datagen_job_state(namespace: str) -> tuple[str, str]:
    """State of the lakebench-datagen Job: absent, finished, unfinished or
    unknown, plus a detail for unknown.

    Unfinished unless a terminal condition (Complete or Failed) says
    otherwise: a Job just created, or backing off between pod retries, has
    no active pods yet but will still write and still hold cores.
    """
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    try:
        job = k8s_client.BatchV1Api().read_namespaced_job("lakebench-datagen", namespace)
    except ApiException as e:
        if e.status == 404:
            return "absent", ""
        return "unknown", str(e.reason)
    except Exception as e:  # noqa: BLE001
        return "unknown", str(e)
    conditions = getattr(job.status, "conditions", None) or []
    finished = any(
        getattr(c, "type", None) in ("Complete", "Failed")
        and str(getattr(c, "status", "")) == "True"
        for c in conditions
    )
    return ("finished" if finished else "unfinished"), ""


# How long a continuous run waits for its datagen Job to finish before it
# budgets the streams with datagen's cores still reserved (LB-158).
_DATAGEN_RELEASE_WAIT_S = 300


def _datagen_released(namespace: str, *, deployed_here: bool, poll_s: float = 10.0) -> bool:
    """True when the streaming budget may drop datagen's reservation.

    Finished (Complete or Failed) releases. No lakebench-datagen Job releases
    only when this run deployed datagen itself: with --skip-generate another
    writer may be populating bronze under a different name, and the budget
    keeps its old reservation then. An unfinished Job is polled for up to
    ``_DATAGEN_RELEASE_WAIT_S``; unknown never releases.
    """
    deadline = time.time() + _DATAGEN_RELEASE_WAIT_S
    announced = False
    while True:
        state, detail = _datagen_job_state(namespace)
        if state == "finished" or (state == "absent" and deployed_here):
            print_info("Datagen has finished; the streaming budget does not reserve its cores")
            return True
        # Unfinished and unknown are polled until the deadline: one API blip
        # must not keep the reservation when datagen finished long ago.
        if state not in ("unfinished", "unknown") or time.time() >= deadline:
            break
        if not announced:
            print_info(
                f"Waiting up to {_DATAGEN_RELEASE_WAIT_S}s for datagen to finish before "
                "sizing the streams..."
            )
            announced = True
        time.sleep(poll_s)
    reason = {
        "unfinished": f"datagen still running after {_DATAGEN_RELEASE_WAIT_S}s",
        "absent": "--skip-generate and no lakebench-datagen Job (another writer may be active)",
        "unknown": f"datagen state unknown ({detail})",
    }.get(state, state)
    print_warning(f"Streams are budgeted with datagen's cores reserved: {reason}")
    return False


def _c360_raw_replace_problem(cfg) -> str | None:
    """Why a raw-only corpus must not be replaced silently, or None.

    Refuses (fails closed) when a datagen Job is still active, since its
    pods would keep writing into the cleared prefix, and when the corpus is
    larger than this run regenerates (an earlier generate at a larger
    scale is hours of work the continuous run would not rebuild).
    """
    from lakebench.s3 import S3Client

    state, detail = _datagen_job_state(cfg.get_namespace())
    if state == "unfinished":
        return "a lakebench-datagen Job has not finished; wait for it to complete"
    if state == "unknown":
        return f"could not check for a running datagen Job: {detail}"

    s3_cfg = cfg.platform.storage.s3
    raw = cfg.architecture.pipeline.medallion.bronze.path_template.strip("/")
    try:
        info = S3Client(
            endpoint=s3_cfg.endpoint,
            access_key=s3_cfg.access_key,
            secret_key=s3_cfg.secret_key,
            region=s3_cfg.region,
            path_style=s3_cfg.path_style,
            ca_cert=s3_cfg.ca_cert,
            verify_ssl=s3_cfg.verify_ssl,
        ).get_bucket_size(s3_cfg.buckets.bronze, prefix=f"{raw}/")
    except Exception as e:  # noqa: BLE001
        return f"could not size the raw corpus: {e}"
    size_gb = (info.size_bytes or 0) / 1024**3
    limit_gb = cfg.get_scale_dimensions().approx_bronze_gb * _RAW_REPLACE_SIZE_FACTOR
    if size_gb > limit_gb:
        return (
            f"the raw corpus is {size_gb:,.0f} GB, more than this run regenerates "
            f"(limit {limit_gb:,.0f} GB at this scale)"
        )
    return None


def _refuse_c360_reset(cfg, existing: list[str]) -> None:
    tables = cfg.architecture.tables
    print_error(
        "Refusing to reset continuous state: this deployment already holds data "
        "a continuous run would delete."
    )
    print_info(f"Tables that would be dropped: {tables.bronze}, {tables.silver}, {tables.gold}")
    for p in existing:
        print_info(f"  non-empty: {p}")
    print_info(
        "Re-run with --force-reset to drop them, or --skip-generate to keep the raw "
        "data and only reset checkpoints and tables (still needs --force-reset)."
    )


def _run_c360_continuous_reset(job_manager, monitor, console, *, timeout_seconds: int) -> bool:
    """Drop the c360 continuous tables through a bronze-verify preflight.

    False when the job fails; the caller then starts no stream over tables
    the reset could not clear. ``timeout_seconds`` scales with the data: at
    scale 100 the silver directory alone is hundreds of thousands of files.
    """
    from lakebench.spark.job import JobState, JobType

    console.print()
    console.print("[bold]Preflight: resetting continuous tables via bronze-verify...[/bold]")
    status = job_manager.submit_job(JobType.BRONZE_VERIFY, cycle_env={"LB_CONTINUOUS_RESET": "1"})
    if status.state == JobState.FAILED:
        print_error(f"continuous reset submit failed: {status.message}")
        return False
    result = monitor.wait_for_completion(
        "lakebench-bronze-verify", timeout_seconds=timeout_seconds, poll_interval=15
    )
    if not result.success:
        print_error(f"continuous reset failed: {result.message}")
        return False
    print_success(f"Continuous tables reset in {result.elapsed_seconds:.0f}s")
    return True


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


_DAYS_PER_MONTH = 30.5
_RETENTION_HEADROOM_MONTHS = 6


def resolve_maintenance_retention(cfg) -> str:
    """Resolve the pre-benchmark maintenance retention threshold.

    Returns ``"0s"`` (expire every snapshot older than now) for standard
    workloads. When ``workload.retention_workload`` is True, returns a
    day-string long enough to cover ``retention_months + 6`` months of
    headroom, so historical replay (W8) and time-travel reproduction
    (W10) can still resolve their target snapshots after maintenance.
    Expressed in days because the maintenance parser accepts only
    s/m/h/d suffixes (V-23 in the FinServ-Crime spec).
    """
    workload = cfg.architecture.workload
    if not workload.retention_workload:
        return "0s"
    total_months = workload.retention_months + _RETENTION_HEADROOM_MONTHS
    days = int(total_months * _DAYS_PER_MONTH)
    return f"{days}d"


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
    schema = cfg.architecture.workload.schema_type.value
    table_names = [f"{catalog}.{t}" for t in tables.workload_tables(schema)]

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

    from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout

    maintained = 0
    expected_ops = 0
    failures: list[str] = []
    timed_out: list[str] = []
    for table in table_names:
        ops = build_sql(table)
        expected_ops += len(ops)
        for sql in ops:
            # exec_sql raises on a real failure (non-zero exit) and on a
            # kubectl-exec timeout; a round is recorded, never fatal to the run.
            try:
                exec_sql(engine, k8s, pod_name, namespace, sql)
                maintained += 1
            except ExecSqlTimeout as e:
                # Not a failure: the engine may still be running it.
                timed_out.append(f"{table}: {e}")
                logger.warning(
                    "%s maintenance timed out for %s (may still be running)",
                    table_format.title(),
                    table,
                )
            except Exception as e:
                failures.append(f"{table}: {e}")
                logger.warning("%s maintenance failed for %s: %s", table_format.title(), table, e)

    colour = "yellow" if failures or timed_out else "green"
    extra = f", {len(timed_out)} timed out (may still be running)" if timed_out else ""
    console.print(
        f"  [{colour}]{table_format.title()} maintenance ({engine}): {maintained}/{expected_ops} "
        f"operations{extra}[/{colour}] (threshold: {retention_threshold})"
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
            "operations_failed": len(failures),
            "operations_timed_out": len(timed_out),
            "failures": failures[:5],
            "timed_out": timed_out[:5],
        },
    )


def _run_iceberg_compaction(
    cfg,
    k8s,
    console: Console,
    j,
    file_size_threshold: str = "128MB",
    live_streams: bool = False,
    timeout: int = 30,
) -> None:
    """Run table compaction (format-aware).

    ``live_streams``: continuous jobs are writing. For AML the gold tables
    are then skipped: gold-refresh deletes and rewrites each rule's alerts
    every tick, so a concurrent rewrite conflicts with it and buys nothing.

    - Iceberg: rewrite_data_files / optimize
    - Delta: OPTIMIZE

    Merges small files produced by streaming micro-batches or repeated
    incremental writes.  DuckDB cannot run compaction -- skipped.

    ``timeout`` bounds each statement's kubectl exec. A timeout does not stop
    the rewrite server-side, so the pre-benchmark caller passes a long one:
    the benchmark must not start while rewrite_data_files still runs.
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
    schema = cfg.architecture.workload.schema_type.value
    # Bronze is never compacted: for AML it holds add_files-registered
    # datagen files, and a rewrite followed by expire_snapshots would
    # delete the raw corpus (see bronze_verify_financial).
    layers: tuple[str, ...] = ("silver", "gold")
    if live_streams and schema == "financial":
        layers = ("silver",)
    table_names = [f"{catalog}.{t}" for t in tables.workload_tables(schema, layers=layers)]

    # Build SQL based on table format
    if table_format == "delta":
        from lakebench.deploy.delta_maintenance import build_delta_compaction_sql

        def build_sql(tbl):
            return build_delta_compaction_sql(engine, catalog, tbl)
    else:
        from lakebench.deploy.iceberg import build_compaction_sql

        def build_sql(tbl):
            return build_compaction_sql(engine, catalog, tbl, file_size_threshold)

    import time as _time

    from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout

    compacted = 0
    total_ops = 0
    failures: list[str] = []
    timed_out: list[str] = []
    started = _time.monotonic()
    for table in table_names:
        for sql in build_sql(table):
            total_ops += 1
            # exec_sql raises on a real failure (non-zero exit) and on a
            # kubectl-exec timeout; neither is fatal to the run.
            try:
                exec_sql(engine, k8s, pod_name, namespace, sql, timeout=timeout)
                compacted += 1
            except ExecSqlTimeout as e:
                timed_out.append(f"{table}: {e}")
                logger.warning(
                    "%s compaction timed out for %s after %ss (may still be running)",
                    table_format.title(),
                    table,
                    timeout,
                )
            except Exception as e:
                failures.append(f"{table}: {e}")
                logger.warning("%s compaction failed for %s: %s", table_format.title(), table, e)
    elapsed = _time.monotonic() - started

    colour = "yellow" if failures or timed_out else "green"
    extra = f", {len(timed_out)} timed out (may still be running)" if timed_out else ""
    console.print(
        f"  [{colour}]{table_format.title()} compaction ({engine}): {compacted}/{total_ops} "
        f"operations on {len(table_names)} tables{extra}[/{colour}] "
        f"(threshold: {file_size_threshold}, {elapsed:.0f}s)"
    )
    _journal_safe(
        j.record,
        EventType.STREAMING_HEALTH,
        message=f"{table_format.title()} compaction",
        details={
            "engine": engine,
            "table_format": table_format,
            "file_size_threshold": file_size_threshold,
            "operations_succeeded": compacted,
            "operations_total": total_ops,
            "operations_failed": len(failures),
            "operations_timed_out": len(timed_out),
            "failures": failures[:5],
            "timed_out": timed_out[:5],
            "elapsed_seconds": round(elapsed, 1),
            "statement_timeout_seconds": timeout,
        },
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
    # One sample per query: gold refreshes under the round, so repeats would
    # time different snapshots. The rounds themselves are the repeats, and
    # the scores take their median (qph_degradation_pct, composite_qph).
    bench_result = bench_runner.run_power(cache="hot", iterations=1)

    # 4. Check Q9 for contention (gold-table query)
    q9_failed = False
    for qr in bench_result.queries:
        if qr.query.name.startswith("Q9") and not qr.success:
            q9_failed = True
            break

    if q9_failed:
        round_meta.q9_contention_observed = True
        q9_query = None
        for qr in bench_result.queries:
            if qr.query.name.startswith("Q9"):
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
                        if qr.query.name.startswith("Q9"):
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


def _wait_for_bronze_data(cfg, timeout_seconds: int = 300) -> bool:
    """Poll the bronze bucket for the first ``.parquet`` file to land.

    Called before the AML bronze-verify preflight so
    ``spark.read.parquet(prefix)`` does not hit AnalysisException on
    an empty prefix. Returns True when a parquet is visible, False
    on timeout. Best-effort: falls through (returns True) if the S3
    client cannot be constructed, so a config with a rotated key
    doesn't wedge the sustained CLI here -- the preflight itself
    will surface any real credential issues.
    """
    import time as _t

    try:
        from lakebench.s3 import S3Client
    except Exception:  # noqa: BLE001
        return True

    s3_cfg = cfg.platform.storage.s3
    try:
        client = S3Client(
            endpoint=s3_cfg.endpoint,
            access_key=s3_cfg.access_key,
            secret_key=s3_cfg.secret_key,
            region=s3_cfg.region,
            path_style=s3_cfg.path_style,
            ca_cert=getattr(s3_cfg, "ca_cert", None) or "",
            verify_ssl=getattr(s3_cfg, "verify_ssl", True),
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not construct S3 client for preflight wait: %s", e)
        return True

    bronze = s3_cfg.buckets.bronze
    raw = client.raw_client
    deadline = _t.time() + timeout_seconds
    interval = 5.0
    while _t.time() < deadline:
        try:
            resp = raw.list_objects_v2(Bucket=bronze, MaxKeys=50)
        except Exception as e:  # noqa: BLE001
            logger.warning("list_objects_v2 on %s failed: %s", bronze, e)
            _t.sleep(interval)
            continue
        for item in resp.get("Contents") or []:
            key = item.get("Key", "")
            if key.endswith(".parquet"):
                return True
        _t.sleep(interval)
    logger.warning(
        "No parquet under s3://%s/ after %ds; running preflight anyway (it will "
        "fail loudly if the prefix is still empty).",
        bronze,
        timeout_seconds,
    )
    return False


def _streaming_job_env(run_id: str, run_duration: int) -> dict[str, str]:
    """Env for the continuous SparkApplications.

    LB_RUN_ID is the CLI run id. It lives in the manifest, so a driver the
    operator restarts keeps it: gold.alerts and the TM ledger stay this run's
    instead of a fresh uuid per driver, which erased the ledger and restarted
    alert identity. LB_CONTINUOUS_WINDOW_S is the window length; gold-refresh
    anchors it on its first driver's start (persisted in its checkpoint), not
    on the CLI's clock at submit, which precedes the driver-ready wait.
    """
    return {"LB_RUN_ID": run_id, "LB_CONTINUOUS_WINDOW_S": str(int(run_duration))}


def _aml_cumulative_alerts(gold_refresh_logs: str | None) -> int | None:
    """Return the peak gold.alerts row count reported by the continuous gold
    stage, or None if the logs show no detection activity.

    gold_refresh_financial logs one ``[detection] cumulative gold.alerts
    rows: N`` line per tick. Because each tick re-detects the full corpus and
    rewrites per rule (DELETE + INSERT), the per-tick total can rise or fall
    slightly, so this is the peak across ticks, not necessarily the last one.
    The peak is the right signal for the gate's only decision -- did detection
    ever produce alerts (>0) or not (0/None). None (no such line at all) is
    distinct from 0 (line present, no alerts) so the caller can tell
    "detection never ran" from "detection ran and found nothing".
    """
    if not gold_refresh_logs:
        return None
    import re as _re

    counts = [
        int(m) for m in _re.findall(r"cumulative gold\.alerts rows:\s*(\d+)", gold_refresh_logs)
    ]
    return max(counts) if counts else None


def _run_sustained(
    cfg,
    config_file: Path,
    timeout: int,
    skip_benchmark: bool,
    duration: int | None,
    skip_generate: bool = False,
    skip_maintenance: bool = False,
    force_reset: bool = False,
) -> None:
    """Run the sustained streaming pipeline.

    Starts datagen concurrently with bronze-ingest, silver-stream, and
    gold-refresh streaming SparkApplications. Monitors for the configured
    duration, then stops streaming jobs and optionally runs the benchmark.

    Args:
        skip_generate: If True, do not start a datagen deployment. Assumes
            bronze already has data (or another process is populating it).
        skip_maintenance: If True, do not run the periodic Iceberg
            expire_snapshots / remove_orphan_files loop during monitoring.
            The maintenance loop is on by default because unmaintained
            streaming snapshots grow unbounded, but a run whose primary
            goal is measuring raw freshness/throughput may want it off.
    """
    import uuid

    from lakebench.deploy import DatagenDeployer, DeploymentEngine, DeploymentStatus
    from lakebench.engine import get_engine
    from lakebench.metrics import MetricsCollector, MetricsStorage, StreamingJobMetrics
    from lakebench.spark import SparkJobMonitor, SparkOperatorManager
    from lakebench.spark.job import (
        JobState,
        JobType,
        SparkJobManager,
        aml_bronze_verify_timeout_budget,
    )

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

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        job_manager: SparkJobManager = get_engine(cfg, k8s)  # type: ignore[assignment]
        monitor = SparkJobMonitor(cfg, k8s, job_manager=job_manager)

        # Deploy scripts ConfigMap (includes streaming scripts) -- must succeed
        print_info("Deploying Spark scripts...")
        if not job_manager.deploy_scripts_configmap():
            print_error("Failed to deploy Spark scripts ConfigMap -- pipeline cannot proceed")
            _journal_safe(j.end_command, success=False, message="Scripts ConfigMap deploy failed")
            raise typer.Exit(1)
        print_success("Spark scripts deployed")

        # Start datagen before the stages below: the AML preflight needs parquet
        # data to infer a schema from, and datagen then runs concurrently with
        # the continuous jobs, which consume the same prefix as it lands.
        # Skipping is only sensible when bronze is already being populated by
        # another process; otherwise the continuous stages have no input.
        engine = DeploymentEngine(cfg)
        # Every continuous run starts clean; see _reset_continuous_state, and
        # the table reset in bronze_verify_financial CONTINUOUS_RESET (AML) or
        # bronze_verify LB_CONTINUOUS_RESET (c360, LB-142).
        # Ownership first: stopping streams in a namespace this run does
        # not own would already be the damage the gate exists to prevent.
        _require_reset_ownership(cfg)
        if cfg.architecture.workload.schema_type.value != "financial" and not force_reset:
            # c360 keeps existing state unless the operator asks to drop it:
            # a large raw corpus or a batch run's tables took hours to build.
            # A raw-only corpus no larger than this run regenerates, with no
            # datagen still writing, is the plain deploy -> generate -> run
            # flow and is replaced (LB-154).
            _existing = _c360_existing_state(cfg, clear_raw=not skip_generate)
            _raw_only = _c360_only_fresh_generate(cfg, _existing)
            _raw_problem = _c360_raw_replace_problem(cfg) if _raw_only else None
            if _raw_only and _raw_problem is None:
                print_info(
                    f"Found only raw data in {_existing[0]} (no tables, no stream "
                    "checkpoints). A continuous run generates its own data, so it "
                    "will be replaced; a separate generate is not needed before "
                    "`run --sustained`."
                )
            elif _existing:
                if _raw_problem:
                    print_info(f"Raw data is not replaced automatically: {_raw_problem}.")
                _refuse_c360_reset(cfg, _existing)
                pipeline_success = False
                raise typer.Exit(1)
        _stop_leftover_streams(job_manager, cfg.get_namespace())
        _reset_continuous_state(cfg, clear_raw=not skip_generate)
        if skip_generate:
            console.print()
            print_info("Skipping datagen deploy (--skip-generate)")
        else:
            console.print()
            console.print("[bold]Starting datagen...[/bold]")
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

        # LB-091: AML sustained mode needs the bronze Iceberg table to
        # exist before bronze_ingest_financial starts -- the streaming
        # source cannot infer a schema from parquet files, so it hard-
        # exits with `sys.exit(2)` when the table is missing. Only
        # bronze_verify_financial creates the table (with
        # LB_REGISTER_TABLE=1); nothing else in the sustained path
        # writes it. Run one bronze-verify pass here so the streaming
        # jobs have a target to write to. C360 uses `bronze_ingest.py`
        # which does not have this dependency, so we scope the
        # preflight to workload.schema=financial.
        #
        # Ordering is intentional: bronze_verify_financial's schema
        # inference calls ``spark.read.parquet(prefix)`` which fails
        # with AnalysisException on an empty prefix, so we start
        # datagen first and poll for the first parquet to land before
        # submitting bronze-verify. Adversarial-review finding: without
        # this ordering the preflight hard-fails on the first ever
        # sustained deploy of a fresh bronze bucket.
        if cfg.architecture.workload.schema_type.value == "financial":
            console.print()
            print_info("Waiting for first parquet to land in bronze before preflight...")
            _wait_for_bronze_data(cfg, timeout_seconds=300)

            console.print("[bold]Preflight: registering bronze table via bronze-verify...[/bold]")
            preflight_status = job_manager.submit_job(
                JobType.BRONZE_VERIFY,
                # Reset, not register: see bronze_verify_financial
                # CONTINUOUS_RESET.
                cycle_env={"LB_REGISTER_TABLE": "schema"},
            )
            if preflight_status.state == JobState.FAILED:
                print_error(f"bronze-verify preflight submit failed: {preflight_status.message}")
                pipeline_success = False
                raise typer.Exit(1)
            # Preflight budget must scale with data. This bronze-verify reads
            # whatever raw pacs.008 already sits under the bronze prefix -- and
            # when a `generate` step precedes `run --sustained` (the standard
            # cycle) that is close to the full corpus (~100 GB observed at
            # scale 10 before preflight), not a trickle. bronze_verify_financial
            # trips the CTAS fallback on it, so the old fixed 1200s cap could
            # never pass at scale >= 10 -- it timed out with the job still
            # legitimately RUNNING. The budget is shared with the batch path via
            # aml_bronze_verify_timeout_budget so the two cannot diverge for
            # the same job. Using the CLI's sustained ``timeout`` here would be
            # wrong -- that's the streaming window, not a preflight step, and
            # the streaming clock only starts after preflight returns.
            _preflight_scale = cfg.architecture.workload.datagen.get_effective_scale()
            _preflight_timeout = aml_bronze_verify_timeout_budget(_preflight_scale)
            preflight_result = monitor.wait_for_completion(
                "lakebench-bronze-verify",
                timeout_seconds=_preflight_timeout,
                poll_interval=15,
            )
            if not preflight_result.success:
                print_error(f"bronze-verify preflight failed: {preflight_result.message}")
                pipeline_success = False
                raise typer.Exit(1)
            print_success(
                f"bronze-verify preflight complete in {preflight_result.elapsed_seconds:.0f}s"
            )
        else:
            # c360 (LB-142): a batch run, or an earlier continuous run, leaves
            # bronze_raw, silver and gold full. The checkpoints were deleted
            # above, and silver-stream refuses a fresh checkpoint over a full
            # table, so drop the tables before any stream starts. Unlike the
            # AML preflight this needs no schema, so it does not wait for
            # datagen; datagen keeps writing while it runs.
            _reset_timeout = aml_bronze_verify_timeout_budget(
                cfg.architecture.workload.datagen.get_effective_scale()
            )
            if not _run_c360_continuous_reset(
                job_manager, monitor, console, timeout_seconds=_reset_timeout
            ):
                pipeline_success = False
                raise typer.Exit(1)

        # The corpus is finite and usually written within minutes; a finished
        # datagen Job holds no cores, so the streaming budget stops reserving
        # them (LB-158). Wait a bounded time for it so the executor counts do
        # not depend on a race with one API read. Unfinished or unknown keeps
        # the reservation, so the streams never over-commit the cluster.
        job_manager.datagen_running = not _datagen_released(
            cfg.get_namespace(), deployed_here=not skip_generate
        )

        # Launch all streaming jobs concurrently
        console.print()
        console.print("[bold]Launching continuous jobs...[/bold]")

        _journal_safe(
            j.record,
            EventType.STREAMING_START,
            message="Starting streaming pipeline",
            details={
                "jobs": [name for _, name in streaming_jobs],
                # LB-158: whether the budget reserved datagen's cores, so
                # runs with different executor counts can be told apart.
                "datagen_cores_reserved": job_manager.datagen_running,
            },
        )

        submitted = []
        # Executors each stream actually requested (after the concurrent
        # budget), for the scorecard's CPU-hours.
        requested_executors: dict[str, int] = {}
        stream_env = _streaming_job_env(run_id, run_duration)
        for job_type, job_name in streaming_jobs:
            job_status = job_manager.submit_job(job_type, cycle_env=stream_env)
            if job_status.state == JobState.FAILED:
                print_error(f"Failed to submit {job_name}: {job_status.message}")
                pipeline_success = False
                raise typer.Exit(1)
            print_success(f"Submitted: lakebench-{job_name}")
            for warning in getattr(job_manager, "budget_warnings", None) or []:
                print_warning(warning)
            if isinstance(getattr(job_manager, "budget_warnings", None), list):
                job_manager.budget_warnings.clear()
            if isinstance(job_status.executor_count, int) and job_status.executor_count > 0:
                requested_executors[job_name] = job_status.executor_count
            submitted.append((job_type, job_name))

        # A streaming submission that failed used to go unnoticed until the
        # end-of-run gates reported zero rows. Confirm each driver is running
        # first; dependency-download races on the shared operator are retried.
        # One shared deadline: the streams start concurrently, so waiting
        # for each in turn with its own budget could take N times as long.
        _start_deadline = time.time() + 1800
        for _job_type, job_name in submitted:
            running = monitor.wait_until_running(
                f"lakebench-{job_name}",
                timeout_seconds=max(0, int(_start_deadline - time.time())),
            )
            if not running.success:
                print_error(f"lakebench-{job_name} did not start: {running.message}")
                pipeline_success = False
                raise typer.Exit(1)

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

        # Iceberg retention scheduling. --skip-maintenance disables both
        # the expire_snapshots loop and periodic compaction; without
        # skipping, mid-run maintenance would distort a raw
        # freshness/throughput measurement.
        retention_interval = sustained_cfg.retention_interval
        retention_threshold = sustained_cfg.retention_threshold
        if skip_maintenance:
            next_maintenance_at = float("inf")
            print_info("Iceberg retention: disabled (--skip-maintenance)")
        else:
            next_maintenance_at = float(retention_interval)  # first run after one interval
            print_info(
                f"Iceberg retention: every {retention_interval}s (threshold: {retention_threshold})"
            )

        # Iceberg compaction scheduling (v1.1.0)
        compaction_enabled = sustained_cfg.compaction_enabled and not skip_maintenance
        compaction_interval = sustained_cfg.compaction_interval
        next_compaction_at = float(compaction_interval) if compaction_enabled else float("inf")
        if compaction_enabled:
            print_info(f"Iceberg compaction: every {compaction_interval}s")
        elif sustained_cfg.compaction_enabled and skip_maintenance:
            print_info("Iceberg compaction: disabled (--skip-maintenance)")

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
                _run_iceberg_compaction(cfg, k8s, console, j, live_streams=True)
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
            # datagen row count is not measurable from S3 metadata (would need
            # to parse Parquet footers). Leaving _datagen_output_rows at 0
            # correctly signals "unmeasurable" downstream -- ingest_ratio and
            # pipeline_saturated become None rather than being computed against
            # a fictional `scale * 1_500_000` denominator (LB-044 pattern).
        except Exception as e:
            logger.warning("Could not measure streaming bronze bucket size: %s", e)

        # LB-136: when every datagen pod has finished and reported, their
        # summed rows_written IS the produced-row count, for either workload
        # (both generators are finite and emit LB_METRICS_JSON). A window
        # that ends before the corpus is consumed then honestly reads as
        # saturated. Unmeasured (None) when any pod has not reported, or the
        # pods were already garbage-collected (ttlSecondsAfterFinished).
        try:
            from lakebench.metrics.datagen_aggregator import collect_from_k8s

            _fleet = collect_from_k8s(
                namespace=cfg.get_namespace(),
                job_completions=cfg.architecture.workload.datagen.parallelism,
            )
            if _fleet.data_quality == "complete" and _fleet.total_rows_written > 0:
                _datagen_output_rows = _fleet.total_rows_written
        except Exception as e:
            logger.warning("Could not read datagen row counts: %s", e)

        # Capture driver logs BEFORE stopping jobs (pods are deleted on stop)
        console.print()
        console.print("[bold]Collecting continuous-mode metrics...[/bold]")
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
        console.print("[bold]Stopping continuous jobs...[/bold]")
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

        # LB-127 honest continuous runner (closes LB-044 for AML). A
        # continuous AML run whose gold stage produced ZERO alerts is a
        # FAILURE, not a PASS: it means detection never fired (empty silver,
        # a data-clock/window miss, or a broken rule), and the whole point of
        # the run -- measuring detection under a sustained trickle -- did not
        # happen. Exit-code-only success let this masquerade as PASS for two
        # UAT rounds on the C360 side (LB-044); AML asserts on real output.
        # Evaluated BEFORE the per-stage record loop so streaming_metrics.success
        # is recorded consistent with the run-level verdict, and it only sets
        # the flag here -- the non-zero exit is raised at the end of the try so
        # metrics + streaming stats still persist. Gated to financial so
        # non-detection C360 sustained runs (no alerts by design) are unaffected.
        # None gold-refresh logs => FAILURE by deliberate LB-044 policy
        # (absence of proof is not proof of success); the trade-off is a
        # possible false-fail if driver-log capture times out on a very long
        # run, which is preferred over silently passing an unverifiable run.
        if cfg.architecture.workload.schema_type.value == "financial":
            gold_logs = driver_logs.get("gold-refresh")
            alert_count = _aml_cumulative_alerts(gold_logs)
            if gold_logs is None:
                print_error(
                    "AML continuous gate: no gold-refresh driver logs captured; "
                    "cannot confirm detection ran. Marking FAILURE."
                )
                pipeline_success = False
            elif alert_count is None:
                print_error(
                    "AML continuous gate: gold-refresh logs show no detection "
                    "activity (no '[detection] cumulative gold.alerts rows:' line). "
                    "Detection did not run. Marking FAILURE."
                )
                pipeline_success = False
            elif alert_count == 0:
                print_error(
                    "AML continuous gate: detection ran but produced 0 alerts over "
                    "the whole run. Either silver stayed empty, or every detection "
                    "rule errored. Marking FAILURE (a real continuous run must "
                    "detect something)."
                )
                pipeline_success = False
            else:
                print_success(
                    f"AML continuous gate: detection produced {alert_count:,} "
                    "alerts over the window."
                )
            # P10 TM operations: its own verdict on every operations pass.
            # Only violated invariants fail the run; a layer that could not
            # run (no manifest within the window, an error) is reported as
            # not run, and a missing log as unknown, as in batch.
            from lakebench.cli._run import _report_tm_verdict
            from lakebench.metrics.tm_ops import (
                parse_tm_invariants,
                parse_tm_ops,
                parse_tm_status,
                tm_verdict,
            )

            _inv = parse_tm_invariants(gold_logs)
            _tm = tm_verdict(
                _inv,
                parse_tm_status(gold_logs),
                enabled=cfg.architecture.workload.tm_operations.enabled,
                logs_captured=gold_logs is not None,
                continuous=True,
                label="AML continuous gate",
            )
            _tm["invariants"] = {str(c): v for c, v in sorted(_inv.items())}
            _tm["ops"] = parse_tm_ops(gold_logs)
            _tm["mode"] = "continuous"
            if collector.current_run is not None:
                collector.current_run.tm_operations = _tm
            if _report_tm_verdict(_tm, "AML continuous gate"):
                pipeline_success = False

        # c360 honest continuous gate (LB-044 for c360; AML has its own above).
        # A continuous run whose bronze or silver stream processed zero rows
        # moved no data, whatever the exit codes say.
        if cfg.architecture.workload.schema_type.value != "financial":
            _rows_by_job: dict[str, int | None] = {}
            for _jt, _jn in submitted:
                if _jn in _C360_REQUIRED_STREAM_JOBS:
                    _logs = driver_logs.get(_jn)
                    try:
                        _rows_by_job[_jn] = (
                            collector.parse_streaming_logs(_logs, _jn).total_rows_processed
                            if _logs
                            else None
                        )
                    except Exception:  # noqa: BLE001
                        _rows_by_job[_jn] = None
            for _problem in _c360_continuous_gate_problems(_rows_by_job):
                print_error(_problem)
                pipeline_success = False

        # Record streaming metrics (from pre-captured driver logs)
        console.print()
        console.print("[bold]Parsing continuous-mode metrics...[/bold]")
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
            streaming_metrics.requested_executors = requested_executors.get(job_name)
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
                    # Same rule as batch: a round with failed queries is not
                    # a score (QpH counts only the queries that passed).
                    from lakebench.cli._run import _benchmark_gate_problems

                    # Q9 reads the c360 gold table that gold-refresh replaces
                    # while rounds run; after its retries a Q9 failure is
                    # expected contention, reported but not a run failure.
                    for idx, rnd in enumerate(rounds, 1):
                        q9 = [
                            q
                            for q in rnd.queries
                            if str(q.get("name", "")).startswith("Q9") and not q.get("success")
                        ]
                        for q in q9:
                            print_warning(
                                f"Round {idx}: {q['name']} failed after contention retries "
                                "(gold refresh replaces the table it reads)"
                            )
                        rest = [q for q in rnd.queries if q not in q9]
                        for problem in _benchmark_gate_problems(cfg, rest):
                            print_error(f"Round {idx}: {problem}")
                            pipeline_success = False
                    if not pipeline_success and collector.current_run:
                        # The stage records were written before this gate;
                        # keep them consistent with the run's verdict.
                        for sm in collector.current_run.streaming:
                            sm.success = False
            except Exception as e:
                print_warning(f"Benchmark aggregation failed: {e}")

        # Summary. Only the green "completed" panel is success-gated: printing
        # it after the gate flagged FAILURE would contradict the red error and
        # read as a pass to an operator scanning stdout (adversarial-review P1).
        if pipeline_success:
            console.print(
                Panel(
                    f"[green]Sustained pipeline completed![/green]\n\n"
                    f"  Duration: {run_duration}s ({run_duration / 60:.0f} min)\n"
                    f"  Streaming jobs: {len(submitted)}\n\n"
                    f"Query results: lakebench query --example count\n"
                    f"Report: report.html in the run directory",
                    title="Sustained Pipeline Complete",
                    expand=False,
                )
            )

        # LB-127 P0 fix: a flagged failure MUST exit non-zero. Every other
        # failure site in this function raises typer.Exit(1); the gate above
        # only set the flag (so the record loop + benchmark aggregation could
        # still persist). Raise now, inside the try, so the finally block still
        # runs (metrics + journal persist with success=False) and the process
        # exits 1 -- the exact signal an exit-code-only UAT runner reads, which
        # is the whole point of closing LB-044.
        if not pipeline_success:
            raise typer.Exit(1)

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
                        # Query-time freshness is event-date based and keeps
                        # growing after a drained corpus (LB-145); show the
                        # gold-cycle figure then.
                        if (pb.query_time_freshness_seconds or 0) > 0 and not pb.corpus_drained:
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
                    if pb.corpus_drained:
                        print_warning(
                            "The corpus was fully ingested before the window ended: freshness "
                            "covers only gold cycles that saw new data, and rows/s is a lower "
                            "bound set by corpus size. Use a longer corpus or a shorter window "
                            "for a throughput figure (LB-145)."
                        )
                    _trickle = pb.trickle_note()
                    if _trickle:
                        print_info(_trickle)
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
            write_run_report(metrics_storage, run_id)

        _journal_safe(j.end_command, success=pipeline_success)
