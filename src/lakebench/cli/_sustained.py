"""Sustained pipeline helpers for Lakebench CLI.

Extracted from cli/__init__.py to reduce file size.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer
from rich.panel import Panel
from rich.table import Table

if TYPE_CHECKING:
    from rich.console import Console

from lakebench._clock import utc_now
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
from lakebench.cli._interrupt import restores_handlers
from lakebench.config.schema import is_continuous_mode
from lakebench.exit_codes import ExitCode
from lakebench.journal import CommandName, EventType
from lakebench.k8s import (
    K8sConnectionError,
    get_k8s_client,
    pinned_kubectl,
    pinned_kubectl_popen,
)
from lakebench.metrics.continuous_window import parse_events, utc_naive
from lakebench.metrics.verdict import apply_save_gate

logger = logging.getLogger(__name__)

# Seconds a statement may run past the pre-benchmark budget before it is cut.
_BUDGET_GRACE_SECONDS = 30

# Delta's default VACUUM retention (hours).
_DELTA_DEFAULT_RETENTION_HOURS = 168.0


_C360_REQUIRED_STREAM_JOBS = ("bronze-ingest", "silver-stream")


def tolerated_q9_results(queries: list[dict], *, final: bool) -> list[dict]:
    """Q9 results a continuous benchmark round reports but does not gate.

    Gold refresh replaces the table Q9 reads, so a failed Q9 after its
    contention retries is expected in any round. An empty Q9 is tolerated
    only before the last round: Q9 is the one c360 query that reads gold,
    so an empty final Q9 means gold never held rows and the gate must see it.
    """
    return [
        q
        for q in queries
        if str(q.get("name", "")).startswith("Q9")
        and (not q.get("success") or (not final and not q.get("rows_returned")))
    ]


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


class _OwnershipUnverifiable(Exception):
    """The reset's ownership check could not run (cluster or API unreadable)."""


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
        # Could not check: a prerequisite (exit 4), not a refusal.
        raise _OwnershipUnverifiable(
            f"namespace {ns} not readable ({e.status}); cannot verify bucket ownership"
        ) from e
    except Exception as e:  # noqa: BLE001
        raise _OwnershipUnverifiable(f"cannot reach the cluster to verify ownership: {e}") from e
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
    from lakebench.deploy.ownership import (
        api_server_fingerprint,
        read_created_buckets,
    )

    my_cluster = api_server_fingerprint(cfg.platform.kubernetes.context or "")
    try:
        # The created record only (not 1.6's adopted-empty one).
        ns_record = read_created_buckets(core_v1, cfg.get_namespace())
    except Exception:  # noqa: BLE001 -- unreadable: nothing is proven by it
        ns_record = set()
    for bucket in (b.bronze, b.silver, b.gold):
        v = verify_bucket_ownership(
            raw, bucket, cfg.name, expected_cluster=my_cluster, created_record=ns_record
        )
        if v.verdict is IdentityVerdict.MATCH:
            continue
        if v.verdict is IdentityVerdict.LEGACY_PROVEN:
            if v.tagged:
                continue  # ownership row 3: the record proves this cluster made it
            # Tagless: the record rule below decides, as before cluster stamps.
            v = dataclasses.replace(v, verdict=IdentityVerdict.UNSUPPORTED)
        if v.verdict is IdentityVerdict.UNSUPPORTED:
            if others is None:
                others = list_lakebench_deployment_names(core_v1, exclude=cfg.get_namespace())
            if others is not None and bucket_name_matches_deployment(bucket, cfg.name, others):
                # The name alone does not prove the data is ours (deploy adopts
                # a pre-existing matching bucket on these backends); same
                # record rule as destroy.
                from lakebench.deploy.ownership import tagless_contents_are_ours

                try:
                    recorded = tagless_contents_are_ours(core_v1, cfg.get_namespace(), bucket)
                except Exception:  # noqa: BLE001
                    recorded = False
                if recorded:
                    continue
                return (
                    f"bucket {bucket}: backend has no bucket tagging and the namespace does "
                    "not record creating it or adopting it empty, so its data may not be "
                    f"deployment {cfg.name!r}'s"
                )
            return (
                f"bucket {bucket}: backend has no bucket tagging and the name does not "
                f"prove it belongs to deployment {cfg.name!r}"
            )
        return f"bucket {bucket}: {v.verdict.name} ({v.hint})"
    return None


_STREAM_APPS = ("lakebench-bronze-ingest", "lakebench-silver-stream", "lakebench-gold-refresh")


# Per-read bound when looking for stream apps; a timeout counts as live.
_STREAM_PROBE_TIMEOUT_S = 10
# Spark Operator states after which an app no longer runs (retries exhausted
# or finished). Anything else, including no status yet, counts as live.
_TERMINAL_APP_STATES = frozenset({"COMPLETED", "FAILED"})


def _live_stream_apps(namespace: str) -> tuple[list[str], list[str]]:
    """Stream SparkApplications present in *namespace* (any schema).

    Returns ``(live, read_errors)``. Stream apps run with restartPolicy
    Always, so one that exists is writing or about to. A read error other
    than 404, including a timeout, counts the app as live (maintenance then
    takes the safe, live-stream settings) and is listed in ``read_errors``.
    """
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    live: list[str] = []
    errors: list[str] = []
    try:
        api = k8s_client.CustomObjectsApi()
    except Exception as e:  # noqa: BLE001
        return list(_STREAM_APPS), [f"no Kubernetes client: {e}"]
    for app in _STREAM_APPS:
        try:
            obj = api.get_namespaced_custom_object(
                "sparkoperator.k8s.io",
                "v1beta2",
                namespace,
                "sparkapplications",
                app,
                _request_timeout=_STREAM_PROBE_TIMEOUT_S,
            )
        except ApiException as e:
            if e.status == 404:
                continue
            live.append(app)
            errors.append(f"{app}: HTTP {e.status}")
            continue
        except Exception as e:  # noqa: BLE001
            # Timeouts (urllib3 ReadTimeoutError) and transport errors.
            live.append(app)
            errors.append(f"{app}: {type(e).__name__}: {e}")
            continue
        state = ""
        if isinstance(obj, dict):
            state = str(
                ((obj.get("status") or {}).get("applicationState") or {}).get("state") or ""
            )
        if state.upper() in _TERMINAL_APP_STATES:
            # A leftover app that has finished writes nothing; counting it
            # would turn QpH gating off for every later run here.
            continue
        live.append(app)
    return live, errors


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
                raise typer.Exit(ExitCode.FAILED)
            time.sleep(3)


def _require_reset_ownership(cfg) -> None:
    """typer.Exit unless this run provably owns the namespace and buckets."""
    unverifiable = False
    try:
        problem = _reset_ownership_problem(cfg)
    except _OwnershipUnverifiable as e:
        problem = str(e)
        unverifiable = True
    except Exception as e:  # noqa: BLE001
        problem = f"ownership could not be verified: {e}"
        unverifiable = True
    if problem:
        print_error(f"Refusing to reset continuous state: {problem}")
        print_info(
            "Continuous runs delete the previous run's checkpoints, tables and raw "
            "data, so they require proof of ownership: buckets tagged by "
            "`lakebench deploy`, or on backends without bucket tagging, bucket "
            "names prefixed with the deployment name."
        )
        # Checked and refused is 3; could not check (S3 or the API) is 4.
        raise typer.Exit(ExitCode.PREREQUISITE if unverifiable else ExitCode.REFUSED)


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
        from lakebench.deploy.datagen import bronze_datagen_prefix

        targets.append((b.bronze, bronze_datagen_prefix(cfg).strip("/")))
    for bucket, prefix in targets:
        try:
            n = client.delete_prefix(bucket, prefix)
        except Exception as e:
            print_error(f"Could not clear {bucket}/{prefix}: {e}")
            raise typer.Exit(ExitCode.FAILED) from e
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
        from lakebench.deploy.datagen import bronze_datagen_prefix

        raw = bronze_datagen_prefix(cfg).strip("/")
        prefixes.append((b.bronze, f"{raw}/"))
    from lakebench.s3.client import has_user_objects

    found = []
    for bucket, prefix in prefixes:
        try:
            holds = has_user_objects(raw_client, bucket, prefix)
        except Exception as e:  # noqa: BLE001
            if "NoSuchBucket" in str(e):
                continue
            found.append(f"{bucket}/{prefix} (could not list: {e})")
            continue
        if holds:
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
    from lakebench.deploy.datagen import bronze_datagen_prefix

    raw = bronze_datagen_prefix(cfg).strip("/")
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
    from lakebench.deploy.datagen import bronze_datagen_prefix

    raw = bronze_datagen_prefix(cfg).strip("/")
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


def _run_c360_continuous_reset(
    job_manager, monitor, console, *, timeout_seconds: int, interrupt=None
) -> bool:
    """Drop the c360 continuous tables through a bronze-verify preflight.

    False when the job fails; the caller then starts no stream over tables
    the reset could not clear. ``timeout_seconds`` scales with the data: at
    scale 100 the silver directory alone is hundreds of thousands of files.
    """
    from lakebench.spark.job import JobState, JobType

    console.print()
    console.print("[bold]Preflight: resetting continuous tables via bronze-verify...[/bold]")
    if interrupt is not None:
        interrupt.creating("SparkApplication", "lakebench-bronze-verify")
    status = job_manager.submit_job(JobType.BRONZE_VERIFY, cycle_env={"LB_CONTINUOUS_RESET": "1"})
    if interrupt is not None:
        interrupt.submitted(status)
    if status.state == JobState.FAILED:
        print_error(f"continuous reset submit failed: {status.message}")
        return False
    result = monitor.wait_for_completion(
        "lakebench-bronze-verify", timeout_seconds=timeout_seconds, poll_interval=15
    )
    if interrupt is not None and result.success:
        interrupt.finished("SparkApplication", "lakebench-bronze-verify")
    if not result.success:
        print_error(f"continuous reset failed: {result.message}")
        return False
    print_success(f"Continuous tables reset in {result.elapsed_seconds:.0f}s")
    return True


def _find_prometheus_svc(namespace: str, context: str | None = None) -> str | None:
    """Find the Prometheus service name in the given namespace.

    The kube-prometheus-stack Helm chart truncates the service name based on
    the release name length, so we cannot predict it. We try the K8s Python
    client first, then fall back to kubectl.
    """
    from lakebench.deploy.observability import HELM_RELEASE_NAME

    label = f"release={HELM_RELEASE_NAME},app=kube-prometheus-stack-prometheus"

    # Attempt 1: K8s Python client, on the process's active cluster target
    # (or this context's, when none is active yet). Never reloads another
    # context; a conflicting one raises rather than reaching another cluster.
    from lakebench.k8s.target import ClusterTarget, ContextConflictError

    try:
        from kubernetes import client as k8s_client

        ClusterTarget.resolve(context=context or "").activate()
        v1 = k8s_client.CoreV1Api()
        svcs = v1.list_namespaced_service(namespace, label_selector=label)
        if svcs.items:
            return svcs.items[0].metadata.name
    except ContextConflictError:
        raise
    except Exception:
        pass

    # Attempt 2: kubectl fallback
    try:
        result = pinned_kubectl(
            context,
            [
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
        from lakebench.deploy.observability import (
            ObservabilityLookupError,
            find_observability_release,
        )
        from lakebench.observability.platform_collector import PlatformCollector

        namespace = cfg.get_namespace()
        ctx = cfg.platform.kubernetes.context or None
        # The stack is shared and lives in its own namespace; metrics are
        # still filtered to this deployment's namespace below.
        try:
            prom_ns = find_observability_release(ctx) or namespace
        except ObservabilityLookupError:
            prom_ns = namespace
        svc_name = _find_prometheus_svc(prom_ns, context=ctx)
        if not svc_name:
            console.print("  [yellow]Could not find Prometheus service[/yellow]")
            return

        # Try in-cluster DNS first (fast path when running inside K8s)
        prometheus_url = f"http://{svc_name}.{prom_ns}.svc:9090"
        try:
            import httpx

            httpx.get(f"{prometheus_url}/api/v1/status/config", timeout=5)
            # DNS resolved and Prometheus responded -- use this URL
        except Exception:
            # DNS failed -- we are outside the cluster. Use port-forward.
            import socket

            local_port = _find_free_port()
            pf_proc = pinned_kubectl_popen(
                ctx,
                [
                    "port-forward",
                    f"svc/{svc_name}",
                    f"{local_port}:9090",
                    "-n",
                    prom_ns,
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
        pm = collector.collect(run_metrics.start_time, run_metrics.end_time or utc_now())
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


class MaintenanceBudget:
    """One bound shared by the pre-benchmark maintenance and compaction.

    The benchmark must not start while a statement still runs, so those
    callers use long per-statement timeouts; this caps the total. The first
    statement timeout (the engine may be stuck, and the statement may still
    be running) or the deadline stops every remaining statement, which is
    then reported as not attempted.
    """

    def __init__(self, seconds: float, label: str = "pre-benchmark maintenance") -> None:
        import time as _time

        self._clock = _time.monotonic
        self.seconds = seconds
        self.label = label
        self.deadline = self._clock() + seconds
        self.stopped = ""

    def remaining(self) -> float:
        return self.deadline - self._clock()

    def statement_timeout(self, per_statement: int) -> int:
        """Per-statement timeout clipped to what is left of the budget."""
        return max(1, min(per_statement, int(self.remaining()) + _BUDGET_GRACE_SECONDS))

    def exhausted(self) -> bool:
        if not self.stopped and self._clock() > self.deadline:
            self.stopped = f"{self.label} exceeded its {int(self.seconds)}s cap"
        return bool(self.stopped)


# Continuous maintenance: a statement may run for at most this long, and never
# more than half the interval between rounds.
CONTINUOUS_STATEMENT_TIMEOUT_CAP = 600
_CONTINUOUS_MIN_SECONDS = 30


def continuous_round_bounds(
    interval_seconds: int, remaining_seconds: float
) -> tuple[int, float] | None:
    """(per-statement timeout, round budget) for a continuous round, or None.

    The default 30 s exec timeout reported most expire_snapshots and
    rewrite_data_files statements on real tables as timed out while the
    engine kept running them, and the next statement then overlapped it.
    Each statement now gets min(600 s, interval / 2). The round is capped at
    interval / 2, so it finishes before the next one is due, and at the time
    left in the run minus the budget grace, so it cannot hold the loop past
    run_duration. None when too little of the run is left for a round.
    As with the pre-benchmark budget, the first timeout stops the rest of
    the round (reported as not attempted) rather than piling statements onto
    an engine that may still be busy; the next round starts at the table
    after the one that timed out, so one slow table cannot starve the others.
    """
    half = max(_CONTINUOUS_MIN_SECONDS, int(interval_seconds) // 2)
    usable = remaining_seconds - _BUDGET_GRACE_SECONDS
    if usable < _CONTINUOUS_MIN_SECONDS:
        return None
    per_statement = min(CONTINUOUS_STATEMENT_TIMEOUT_CAP, half)
    return per_statement, min(float(half), usable)


def _rotated(items: list, start: int) -> list:
    """items starting at index start (mod len)."""
    if not items or not start:
        return items
    k = start % len(items)
    return items[k:] + items[:k]


def _next_start(all_tables: list[str], out: dict) -> int:
    """Index in all_tables where the next continuous round should begin.

    After a timeout, the table after the one that timed out (so a table that
    always times out costs one statement per round, not the whole round);
    after the deadline, the first table not attempted; otherwise 0.
    """
    if not all_tables:
        return 0
    if out.get("timed_out_table") in all_tables:
        return (all_tables.index(out["timed_out_table"]) + 1) % len(all_tables)
    if out.get("resume_table") in all_tables:
        return all_tables.index(out["resume_table"])
    return 0


def late_benchmark_round_skip(
    can_run_rounds: bool,
    elapsed: float,
    next_round_at: float,
    remaining: float,
    min_remaining: float,
) -> str | None:
    """Message when a due in-stream benchmark round cannot fit in the run, else None.

    The caller journals it once and moves next_round_at past the run end.
    """
    if not can_run_rounds or elapsed < next_round_at or remaining >= min_remaining:
        return None
    return f"benchmark round skipped: {remaining:.0f} s left, need {min_remaining:.0f} s"


# Shortest sleep between monitoring passes. An event already due (a round
# that could not run, a maintenance time just rescheduled) must not turn the
# loop into a busy spin that journals a health line every pass.
_MIN_LOOP_SLEEP_SECONDS = 1.0


def loop_sleep_seconds(now: float, *event_times: float) -> float:
    """Seconds to sleep until the earliest event, at least a second.

    The last event time passed is the run end; the sleep never goes past it.
    """
    run_end = event_times[-1]
    wake = min(event_times)
    if now >= run_end:
        return 0.0
    return min(max(wake - now, _MIN_LOOP_SLEEP_SECONDS), run_end - now)


def _operative(sql: str) -> str:
    """The statement that matters in a combined submission ("SET ...; VACUUM")."""
    last = sql.strip().rstrip(";").split(";")[-1].split()
    return last[0] if last else sql


# kubectl's own notice on stderr when a pod has more than one container; it
# comes before the engine's error and says nothing about it.
_KUBECTL_NOTICE = re.compile(r"^Defaulted container ")
_EXEC_PREFIX = re.compile(r"^(?:exec_sql|query_sql) failed \(rc=-?\d+\):\s*")


def _error_line(text: str, limit: int = 300) -> str:
    """The cause of an exec_sql or query_sql error on one line, at most
    *limit* characters.

    The message is "exec_sql failed (rc=N): <stdout> | <stderr>", and on a
    live Trino pod stderr starts with kubectl's "Defaulted container ..."
    notice (journal of run-20260929-204941-1d17f4), so the first line is
    noise. The prefix and the notice are dropped, then
    ``summarise_engine_error`` picks the line that states the failure
    (Trino's "Query <id> failed:", beeline's "Error:" or "FAILED:", a
    Python traceback's exception) over log lines printed before it.
    """
    from lakebench.benchmark.result import summarise_engine_error

    lines: list[str] = []
    for raw in (text or "").splitlines():
        for part in raw.split(" | "):
            line = _EXEC_PREFIX.sub("", part.rstrip())
            if line.strip() and not _KUBECTL_NOTICE.match(line.strip()):
                lines.append(line)
    if not lines:
        return ""
    return " ".join(summarise_engine_error("\n".join(lines), limit=limit).split())[:limit]


def _run_statements(
    plan: list[tuple[str, str]],
    *,
    engine: str,
    k8s,
    pod_name: str,
    namespace: str,
    timeout: int,
    what: str,
    budget: MaintenanceBudget | None = None,
) -> dict:
    """Run ``(table, sql)`` statements; never raises for a statement.

    Returns counts plus failures, timeouts and statements not attempted.
    With ``budget`` the first timeout or the deadline stops the rest.
    """
    from lakebench.deploy.iceberg import exec_sql
    from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout

    out: dict = {
        "succeeded": 0,
        "failures": [],
        "timed_out": [],
        "not_attempted": [],
        # Per plan entry: ok, failed, timed_out or not_attempted.
        "status": ["not_attempted"] * len(plan),
        # Per failed statement: {table, statement, error (first line)}.
        "failure_records": [],
    }
    for n, (table, sql) in enumerate(plan):
        if budget is not None and budget.exhausted():
            out["not_attempted"] = [f"{_operative(q)} {t}" for t, q in plan[n:]]
            out["resume_table"] = table
            break
        # exec_sql raises on a real failure (non-zero exit) and on a
        # kubectl-exec timeout; neither is fatal to the run. Under a budget
        # the statement gets at most what is left of it (plus a small grace),
        # so the budget bounds the total; a statement cut short by it counts
        # as timed out.
        stmt_timeout = budget.statement_timeout(timeout) if budget is not None else timeout
        try:
            exec_sql(engine, k8s, pod_name, namespace, sql, timeout=stmt_timeout)
            out["succeeded"] += 1
            out["status"][n] = "ok"
        except ExecSqlTimeout as e:
            # Not a failure: the engine may still be running it.
            out["status"][n] = "timed_out"
            out["timed_out"].append(f"{_operative(sql)} {table}: {e}")
            logger.warning("%s timed out for %s (may still be running)", what, table)
            if budget is not None:
                budget.stopped = f"{_operative(sql)} {table} timed out after {stmt_timeout}s"
                out["timed_out_table"] = table
                out["not_attempted"] = [f"{_operative(q)} {t}" for t, q in plan[n + 1 :]]
                break
        except Exception as e:
            out["status"][n] = "failed"
            out["failures"].append(f"{_operative(sql)} {table}: {e}")
            out["failure_records"].append(
                {"table": table, "statement": sql, "error": _error_line(str(e))}
            )
            logger.warning("%s failed for %s: %s", what, table, e)
    return out


def _print_outcome(console, label: str, out: dict, total: int, tail: str) -> None:
    bad = out["failures"] or out["timed_out"] or out["not_attempted"]
    colour = "yellow" if bad else "green"
    extra = ""
    if out["timed_out"]:
        extra += f", {len(out['timed_out'])} timed out (may still be running)"
    if out["not_attempted"]:
        extra += f", {len(out['not_attempted'])} not attempted"
    console.print(
        f"  [{colour}]{label}: {out['succeeded']}/{total} operations{extra}[/{colour}] {tail}"
    )


def _outcome_details(out: dict, total: int, budget: MaintenanceBudget | None) -> dict:
    return {
        "operations_succeeded": out["succeeded"],
        "operations_total": total,
        "operations_failed": len(out["failures"]),
        "operations_timed_out": len(out["timed_out"]),
        "operations_not_attempted": len(out["not_attempted"]),
        "failures": out["failures"][:5],
        "timed_out": out["timed_out"][:5],
        "not_attempted": out["not_attempted"][:20],
        "stopped": budget.stopped if budget is not None else "",
    }


def scalar_from_output(engine: str, output: str) -> float | None:
    """The single numeric value of a one-row, one-column result as each
    executor prints it, or None. Trino: one CSV line, quoted. Spark Thrift:
    a tsv2 header, then the value. DuckDB: a JSON payload whose ``data``
    holds the row's Python repr, e.g. ``(1234,)``."""
    import re

    text = (output or "").strip()
    if not text:
        return None
    if engine == "duckdb":
        from lakebench.benchmark.fingerprint import last_json_line

        payload = last_json_line(text)
        data = (payload or {}).get("data") or []
        if payload is None or payload.get("rows") != 1 or not data:
            return None
        m = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", str(data[0]))
        cell = m.group(0) if m else ""
    elif engine == "spark-thrift":
        lines = text.split("\n")
        if len(lines) != 2:
            return None
        cell = lines[1]
    else:
        lines = text.split("\n")
        if len(lines) != 1:
            return None
        cell = lines[0].strip().strip('"')
    try:
        return float(cell)
    except ValueError:
        return None


def maintained_tables(cfg) -> list[str]:
    """Tables table maintenance (expire, orphan removal) runs on: those the
    pipeline writes for this workload and mode. Batch Customer 360 has no
    bronze table (bronze-verify reads the datagen Parquet in place); the
    continuous bronze_raw only exists in continuous mode. Maintenance on it
    failed 2 of 6 statements on every batch c360 run (live, 61489ab)."""
    schema = cfg.architecture.workload.schema_type.value
    continuous = cfg.architecture.pipeline.mode.value in ("sustained", "continuous")
    layers: tuple[str, ...] = ("bronze", "silver", "gold")
    if schema != "financial" and not continuous:
        layers = ("silver", "gold")
    return cfg.architecture.tables.workload_tables(schema, layers=layers)


def _note_outcome(outcomes: list | None, kind: str, **details) -> None:
    """Record what a maintenance or compaction call actually did, for the
    experiment block's effective maintenance (metrics/maintenance_policy)."""
    if outcomes is not None:
        outcomes.append({"kind": kind, **details})


def _statement_outcome(out: dict, total: int, engine: str) -> dict:
    return {
        "engine": engine,
        "total": total,
        "succeeded": out["succeeded"],
        "failed": len(out["failures"]),
        "timed_out": len(out["timed_out"]),
        "not_attempted": len(out["not_attempted"]),
    }


def _maintenance_operation(sql: str) -> str:
    """Which table-maintenance operation a statement runs."""
    low = sql.lower()
    for op in ("remove_orphan_files", "expire_snapshots", "vacuum"):
        if op in low:
            return op
    return _operative(sql).lower()


def _operation_outcomes(plan: list[tuple[str, str]], out: dict, retention: dict) -> list[dict]:
    """Statement counts per operation kind, each with the retention it was
    run at, in first-seen order (expire_snapshots, remove_orphan_files, or
    Delta's vacuum)."""
    ops: dict[str, dict] = {}
    status = out.get("status") or []
    for n, (_table, sql) in enumerate(plan):
        op = _maintenance_operation(sql)
        rec = ops.setdefault(
            op,
            {
                "operation": op,
                "retention": retention.get(op),
                "total": 0,
                "succeeded": 0,
                "failed": 0,
                "timed_out": 0,
                "not_attempted": 0,
            },
        )
        rec["total"] += 1
        st = status[n] if n < len(status) else "not_attempted"
        rec[{"ok": "succeeded"}.get(st, st)] += 1
    return list(ops.values())


def applied_retentions(table_format: str, retention_threshold: str, live_streams: bool) -> dict:
    """The retentions table maintenance runs at, from the configured threshold.

    Iceberg: orphan removal never below 24 h 10 min, on any engine or path
    (build_maintenance_sql enforces it too); expire at the threshold, floored
    at 1 h while streams are live. Delta: one VACUUM retention, never below
    Delta's 7 d default while streams are live. The single source for what
    runs and for what the run records (continuous.retention).
    """
    from lakebench.modules.table_formats.iceberg.maintenance import (
        LIVE_EXPIRE_MIN_RETENTION_SECONDS,
        ORPHAN_MIN_RETENTION_SECONDS,
        _format_duration,
        _parse_threshold_seconds,
    )

    if table_format == "delta":
        from lakebench.deploy.delta_maintenance import parse_retention_to_hours

        expire = retention_threshold
        if live_streams and parse_retention_to_hours(retention_threshold) < (
            _DELTA_DEFAULT_RETENTION_HOURS
        ):
            expire = "168h"
        return {"expire": expire, "orphan": expire}
    configured_s = _parse_threshold_seconds(retention_threshold)
    expire = retention_threshold
    if live_streams:
        expire = _format_duration(max(configured_s, LIVE_EXPIRE_MIN_RETENTION_SECONDS))
    return {
        "expire": expire,
        "orphan": _format_duration(max(configured_s, ORPHAN_MIN_RETENTION_SECONDS)),
    }


def continuous_retention_record(cfg) -> dict:
    """What the continuous maintenance loop runs at, for the metrics record.

    Every continuous maintenance round runs beside live streams, so the
    applied expiry is the floored one. ``configured_by`` says whether the
    config set retention_threshold or the default stood.
    """
    sustained = cfg.architecture.pipeline.sustained
    applied = applied_retentions(
        cfg.architecture.table_format.type.value, sustained.retention_threshold, live_streams=True
    )
    return {
        "configured": sustained.retention_threshold,
        "configured_by": (
            "config" if "retention_threshold" in sustained.model_fields_set else "default"
        ),
        "applied_expire": applied["expire"],
        "applied_orphan": applied["orphan"],
    }


def _run_iceberg_maintenance(
    cfg,
    k8s,
    console: Console,
    j,
    retention_threshold: str,
    timeout: int = 30,
    live_streams: bool = False,
    budget: MaintenanceBudget | None = None,
    start_at: int = 0,
    outcomes: list | None = None,
) -> int | None:
    """Run table maintenance (format-aware).

    - Iceberg: expire_snapshots + remove_orphan_files
    - Delta: VACUUM

    Engine-aware: uses Trino (preferred) or Spark Thrift Server.
    DuckDB cannot run maintenance -- skipped with a warning.
    Failures on individual tables are logged but do not abort.

    ``timeout`` bounds each statement's kubectl exec. A timeout does not stop
    the statement server-side, so the pre-benchmark caller passes a long one
    and a ``budget``. ``live_streams``: continuous streams are reading; Delta
    VACUUM then keeps Delta's default 7-day retention (a lagging stream
    would otherwise hit FileNotFound on a vacuumed file).
    """
    from lakebench.deploy.iceberg import find_maintenance_engine

    namespace = cfg.get_namespace()
    engine_type = cfg.architecture.query_engine.type.value
    table_format = cfg.architecture.table_format.type.value

    if engine_type == "duckdb":
        console.print(
            f"  [dim]{table_format.title()} maintenance skipped (DuckDB cannot run maintenance)[/dim]"
        )
        _note_outcome(outcomes, "expire", skipped="DuckDB cannot run maintenance")
        return None

    if table_format == "delta" and engine_type == "spark-thrift":
        console.print("  [dim]Delta maintenance skipped (VACUUM OOMs Spark Thrift at 4Gi)[/dim]")
        _note_outcome(outcomes, "expire", skipped="Delta VACUUM is skipped on Spark Thrift")
        return None

    engine, pod_name, catalog = find_maintenance_engine(cfg, namespace)
    if engine is None or pod_name is None or catalog is None:
        console.print(
            f"  [dim]{table_format.title()} maintenance skipped (no capable engine pod found)[/dim]"
        )
        _note_outcome(outcomes, "expire", skipped="no capable engine pod found")
        return None

    all_tables = [f"{catalog}.{t}" for t in maintained_tables(cfg)]
    table_names = _rotated(all_tables, start_at)

    # Build SQL based on table format. Delta VACUUM has one retention.
    applied = applied_retentions(table_format, retention_threshold, live_streams)
    if table_format == "delta":
        from lakebench.deploy.delta_maintenance import (
            build_delta_maintenance_sql,
            parse_retention_to_hours,
        )

        if applied["expire"] != retention_threshold:
            # Policy: never VACUUM below Delta's default while streams are
            # live; no retention override is sent.
            console.print("  [dim]Delta VACUUM at default 7d retention (live streams)[/dim]")
            _journal_safe(
                j.record,
                EventType.STREAMING_HEALTH,
                message="Delta VACUUM at default 7d retention (live streams)",
                details={"requested_retention": retention_threshold},
            )
        retention_threshold = applied["expire"]
        orphan_retention = applied["orphan"]
        retention_hours = parse_retention_to_hours(retention_threshold)

        def build_sql(tbl):
            return build_delta_maintenance_sql(engine, catalog, tbl, retention_hours)
    else:
        from lakebench.deploy.iceberg import build_maintenance_sql

        retention_threshold = applied["expire"]
        orphan_retention = applied["orphan"]

        def build_sql(tbl):
            return build_maintenance_sql(
                engine, catalog, tbl, retention_threshold, orphan_retention=orphan_retention
            )

    import time as _time

    plan = [(table, sql) for table in table_names for sql in build_sql(table)]
    started = _time.monotonic()
    out = _run_statements(
        plan,
        engine=engine,
        k8s=k8s,
        pod_name=pod_name,
        namespace=namespace,
        timeout=timeout,
        what=f"{table_format.title()} maintenance",
        budget=budget,
    )
    elapsed = _time.monotonic() - started
    # One row per operation with the retention it ran at: expire and orphan
    # removal run at different retentions (orphan removal floored at
    # 24 h 10 min), and one merged "expire" retention hid the orphan one.
    _note_outcome(
        outcomes,
        "expire",
        retention=retention_threshold,
        operations=_operation_outcomes(
            plan,
            out,
            {
                "expire_snapshots": retention_threshold,
                "remove_orphan_files": orphan_retention,
                "vacuum": retention_threshold,
            },
        ),
        **_statement_outcome(out, len(plan), engine),
    )
    _print_outcome(
        console,
        f"{table_format.title()} maintenance ({engine})",
        out,
        len(plan),
        f"(threshold: {retention_threshold}, {elapsed:.0f}s)",
    )
    _journal_safe(
        j.record,
        EventType.STREAMING_HEALTH,
        message=f"{table_format.title()} maintenance",
        details={
            "engine": engine,
            "table_format": table_format,
            "retention_threshold": retention_threshold,
            "expire_retention": retention_threshold,
            "orphan_retention": orphan_retention,
            **_outcome_details(out, len(plan), budget),
            "elapsed_seconds": round(elapsed, 1),
            "statement_timeout_seconds": timeout,
        },
    )
    return _next_start(all_tables, out)


def _run_iceberg_compaction(
    cfg,
    k8s,
    console: Console,
    j,
    file_size_threshold: str = "128MB",
    live_streams: bool = False,
    timeout: int = 30,
    budget: MaintenanceBudget | None = None,
    start_at: int = 0,
    outcomes: list | None = None,
) -> int | None:
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
        find_maintenance_engine,
    )

    namespace = cfg.get_namespace()
    engine_type = cfg.architecture.query_engine.type.value
    table_format = cfg.architecture.table_format.type.value

    if engine_type == "duckdb":
        console.print(f"  [dim]{table_format.title()} compaction skipped (DuckDB read-only)[/dim]")
        _note_outcome(outcomes, "compaction", skipped="DuckDB cannot run compaction")
        return None

    # Delta OPTIMIZE rewrites the entire table in a single pass.  Both Trino
    # workers (~8GiB) and Spark Thrift Server (~4GiB) can OOM and restart,
    # causing benchmark queries to fail.  In batch mode, Delta tables are
    # written in a single Spark job and don't accumulate the small files that
    # OPTIMIZE is designed to fix.  Skip it pre-benchmark to avoid crashing
    # the query engine.  The continuous loop calls this function too, so
    # Delta OPTIMIZE never runs in either path.
    if table_format == "delta" and engine_type in ("trino", "spark-thrift"):
        console.print("  [dim]Delta compaction skipped (OPTIMIZE not run pre-benchmark)[/dim]")
        _note_outcome(outcomes, "compaction", skipped="Delta OPTIMIZE is never run")
        return None

    engine, pod_name, catalog = find_maintenance_engine(cfg, namespace)
    if engine is None or pod_name is None or catalog is None:
        console.print(
            f"  [dim]{table_format.title()} compaction skipped (no capable engine pod found)[/dim]"
        )
        _note_outcome(outcomes, "compaction", skipped="no capable engine pod found")
        return None

    tables = cfg.architecture.tables
    schema = cfg.architecture.workload.schema_type.value
    # Bronze is never compacted: for AML it holds add_files-registered
    # datagen files, and a rewrite followed by expire_snapshots would
    # delete the raw corpus (see bronze_verify_financial).
    layers: tuple[str, ...] = ("silver", "gold")
    if live_streams and schema == "financial":
        layers = ("silver",)
    all_tables = [f"{catalog}.{t}" for t in tables.workload_tables(schema, layers=layers)]
    table_names = _rotated(all_tables, start_at)

    import time as _time

    started = _time.monotonic()
    notes: list[str] = []
    from lakebench.deploy.iceberg import build_compaction_plan

    # Build SQL based on table format
    if table_format == "delta":
        from lakebench.deploy.delta_maintenance import build_delta_compaction_sql

        def build_sql(tbl):
            return build_delta_compaction_sql(engine, catalog, tbl)
    else:

        def build_sql(tbl):
            partitions = _compaction_partitions(
                engine, k8s, pod_name, namespace, tbl, budget, notes
            )
            return build_compaction_plan(engine, catalog, tbl, file_size_threshold, partitions)

    plan = [(table, sql) for table in table_names for sql in build_sql(table)]
    out = _run_statements(
        plan,
        engine=engine,
        k8s=k8s,
        pod_name=pod_name,
        namespace=namespace,
        timeout=timeout,
        what=f"{table_format.title()} compaction",
        budget=budget,
    )
    elapsed = _time.monotonic() - started
    per_table = _table_outcome(plan, out)
    # What the statements do, from the module that builds them (Delta
    # compaction returned above: OPTIMIZE never runs).
    from lakebench.modules.table_formats.iceberg.maintenance import compaction_operation

    op = compaction_operation(engine, file_size_threshold)
    _note_outcome(
        outcomes,
        "compaction",
        unit="tables",
        engine=engine,
        **(op or {}),
        **per_table,
        statements_total=len(plan),
        statements_succeeded=out["succeeded"],
        statements_attempted=sum(1 for st in out["status"] if st != "not_attempted"),
        failures=out["failure_records"],
        # Attempted: succeeded, failed or timed out.
        statements=[sql for n, (_t, sql) in enumerate(plan) if out["status"][n] != "not_attempted"],
        **({"note": "; ".join(notes)} if notes else {}),
    )
    _print_outcome(
        console,
        f"{table_format.title()} compaction ({engine}) on {len(table_names)} tables",
        out,
        len(plan),
        f"(threshold: {file_size_threshold}, {elapsed:.0f}s)",
    )
    _journal_safe(
        j.record,
        EventType.STREAMING_HEALTH,
        message=f"{table_format.title()} compaction",
        details={
            "engine": engine,
            "table_format": table_format,
            "file_size_threshold": file_size_threshold,
            **_outcome_details(out, len(plan), budget),
            "elapsed_seconds": round(elapsed, 1),
            "statement_timeout_seconds": timeout,
        },
    )
    return _next_start(all_tables, out)


#: Seconds the Trino partition read before a chunked compaction may take.
_PARTITION_READ_TIMEOUT = 120


def _compaction_partitions(
    engine: str,
    k8s,
    pod_name: str,
    namespace: str,
    table: str,
    budget: MaintenanceBudget | None,
    notes: list[str],
) -> list[str | None] | None:
    """The partition values a chunked Trino compaction of *table* needs, or None.

    None (one unchunked statement) when the table is not on the partition
    map, the engine is not Trino, the budget is already spent, or the read
    fails; a failed read is added to *notes* for the effective maintenance
    reasons.
    """
    from lakebench.deploy.iceberg import query_sql
    from lakebench.modules.table_formats.iceberg.maintenance import (
        build_partition_values_sql,
        compaction_partition_column,
        parse_partition_values,
    )

    column = compaction_partition_column(table)
    if engine != "trino" or column is None:
        return None
    if budget is not None and budget.exhausted():
        return None
    read_timeout = _PARTITION_READ_TIMEOUT
    if budget is not None:
        read_timeout = budget.statement_timeout(_PARTITION_READ_TIMEOUT)
    try:
        output = query_sql(
            engine,
            k8s,
            pod_name,
            namespace,
            build_partition_values_sql(table, column),
            timeout=read_timeout,
        )
        return parse_partition_values(output)
    except Exception as e:
        notes.append(
            f"partition read failed on {table}, ran one unchunked statement: {_error_line(str(e))}"
        )
        logger.warning("Compaction partition read failed for %s: %s", table, e)
        return None


def _table_outcome(plan: list[tuple[str, str]], out: dict) -> dict:
    """Compaction counts per table: a table succeeds only when every one of
    its statements (chunks) does. Otherwise it counts once, as failed, timed
    out or not attempted, in that order of precedence."""
    tables: dict[str, list[str]] = {}
    for n, (table, _sql) in enumerate(plan):
        tables.setdefault(table, []).append(out["status"][n])
    counts = {"total": len(tables), "succeeded": 0, "failed": 0, "timed_out": 0, "not_attempted": 0}
    for statuses in tables.values():
        if all(st == "ok" for st in statuses):
            counts["succeeded"] += 1
        elif "failed" in statuses:
            counts["failed"] += 1
        elif "timed_out" in statuses:
            counts["timed_out"] += 1
        else:
            counts["not_attempted"] += 1
    return counts


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
        result = pinned_kubectl(
            cfg,
            [
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
    parse_detail = None
    if table_format == "delta":
        from lakebench.deploy.delta_maintenance import (
            DELTA_HEALTH_UNAVAILABLE,
            build_delta_table_health_sql,
            parse_describe_detail,
        )

        if engine in DELTA_HEALTH_UNAVAILABLE:
            logger.warning(
                "table health: Delta data file counts are unavailable on %s (%s); "
                "pre/post file counts are not recorded",
                engine,
                DELTA_HEALTH_UNAVAILABLE[engine],
            )
            return {}
        parse_detail = parse_describe_detail

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
                if parse_detail is not None:
                    # Delta DESCRIBE DETAIL: a table, not a bare count.
                    count = parse_detail(stdout)
                    if count is None:
                        logger.warning(
                            "table health %s_%s: count unavailable (no numFiles in "
                            "DESCRIBE DETAIL output)",
                            label,
                            metric_name,
                        )
                    else:
                        health[f"{label}_{metric_name}"] = count
                    continue
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
                    logger.warning(
                        "table health %s_%s: count unavailable (no number in the engine output)",
                        label,
                        metric_name,
                    )
            except Exception as e:  # noqa: BLE001 -- a failed probe is absent, never -1
                logger.warning("table health %s_%s failed: %s", label, metric_name, e)

    return health


def gold_event_age_target(
    schema_type: str, gold_table: str, gold_alerts_table: str
) -> tuple[str, str]:
    """The (table, event-time column) the event-age probe reads for a workload.

    Financial reads ``gold.alerts.alert_ts``; every other workload reads its
    gold table's ``interaction_date``. Financial's ``daily_dashboards`` carries
    a baseline row the detection rules fill every tick, so its newest date is
    never empty even in a window that produced no alerts; reading it would report
    a freshness age for a window that detected nothing. The probe reads alerts
    instead, so an empty-alert window yields MAX(alert_ts) = NULL and the caller
    records nothing (UNRECORDED) rather than back-filling from the baseline.
    """
    if schema_type == "financial":
        return gold_alerts_table, "alert_ts"
    return gold_table, "interaction_date"


def gold_event_age_sql(engine: str, fq_gold: str, ts_column: str = "interaction_date") -> str:
    """Seconds from gold's newest event time to now, in *engine*'s dialect.

    ``ts_column`` is the gold table's event-time column: ``interaction_date``
    for c360's dashboards, ``alert_ts`` for financial's alerts. When the table
    holds no rows for the window (an empty-alert financial tick), MAX(...) is
    NULL and the probe returns NULL, which the caller leaves as UNRECORDED.

    Trino and DuckDB take ``date_diff('second', start, end)``. Spark SQL has
    no string-unit form: Spark Thrift rejected it on every round with
    INVALID_PARAMETER_VALUE.DATETIME_UNIT (lb16-cs), and adapt_query only
    rewrites the 'day' form. Spark gets the difference of unix_timestamp
    values, which is whole seconds like date_diff.
    """
    newest = f"CAST(MAX({ts_column}) AS TIMESTAMP)"
    if engine == "spark-thrift":
        expr = f"CAST(unix_timestamp(current_timestamp()) - unix_timestamp({newest}) AS BIGINT)"
    else:
        expr = f"date_diff('second', {newest}, current_timestamp)"
    return f"SELECT {expr} FROM {fq_gold}"


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
        timestamp=utc_now(),
    )

    # Table health probe (v1.1.0)
    if k8s is not None:
        try:
            health = _probe_table_health(cfg, k8s)
            round_meta.silver_data_file_count = health.get("silver_data_file_count")
            round_meta.silver_snapshot_count = health.get("silver_snapshot_count")
            round_meta.gold_data_file_count = health.get("gold_data_file_count")
            round_meta.gold_snapshot_count = health.get("gold_snapshot_count")
        except Exception:
            pass  # Health probe failure should not block the benchmark

    # 1. Flush Trino metadata cache
    try:
        bench_runner.executor.flush_cache()
    except Exception:
        pass

    # 2. Event-age probe: query time minus gold's newest event date. This is
    # where the corpus's event timestamps sit (a 2025 corpus reads ~600 days),
    # not pipeline freshness; data_freshness_seconds is that score. Recorded
    # as a diagnostic and never printed as freshness.
    try:
        catalog = bench_runner.catalog
        gold_table = bench_runner.gold_table
        target_table, ts_column = gold_event_age_target(
            bench_runner.config.architecture.workload.schema_type.value,
            gold_table,
            bench_runner._extra_tables["gold_alerts"],
        )
        freshness_sql = gold_event_age_sql(
            bench_runner.executor.engine_name(), f"{catalog}.{target_table}", ts_column
        )
        freshness_sql = bench_runner.executor.adapt_query(freshness_sql)
        freshness_result = bench_runner.executor.execute_query(freshness_sql, timeout=30)
        if freshness_result.success:
            value = scalar_from_output(freshness_result.engine, freshness_result.raw_output)
            if value is None:
                print_warning(
                    "Gold event-age probe: could not read a number from the "
                    f"{freshness_result.engine} output; event age not recorded for this round"
                )
            else:
                round_meta.gold_event_age_seconds = value
        else:
            print_warning(f"Gold event-age probe failed: {freshness_result.error}")
    except Exception as e:  # noqa: BLE001
        print_warning(f"Gold event-age probe failed: {e}")

    # 3. Run the power benchmark: the 8-query set, plus the investigator
    # queries once this run has a case (AML with TM operations).
    # One sample per query: gold refreshes under the round, so repeats would
    # time different snapshots. The rounds themselves are the repeats, and
    # the scores take their median (qph_degradation_pct, composite_qph).
    # No result fingerprints: each round reads tables still being written.
    tm_run = (
        bench_runner.tm_run_id
        if isinstance(getattr(bench_runner, "tm_run_id", None), str)
        else None
    )
    investigator_queries = _investigator_state(bench_runner, tm_run) if tm_run else None
    if tm_run and investigator_queries != "included":
        bench_runner.tm_run_id = None  # this round runs without IQ1 to IQ4
    round_started = utc_now()
    try:
        bench_result = bench_runner.run_power(cache="hot", iterations=1, fingerprint=False)
    finally:
        if tm_run:
            bench_runner.tm_run_id = tm_run

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
        engine=bench_result.engine,
    )

    # 6. Record the round (the one writer of the round record)
    collector.record_round(
        bench_metrics,
        started_at=round_started,
        ended_at=utc_now(),
        investigator_queries=investigator_queries,
    )

    # 7. Print inline result
    freshness_str = (
        f" | Gold event age: {event_age_label(round_meta.gold_event_age_seconds)}"
        if (round_meta.gold_event_age_seconds or 0) > 0
        else ""
    )
    q9_str = ""
    if round_meta.q9_contention_observed:
        q9_str = " | Q9: retry" if round_meta.q9_retry_used else " | Q9: contention"
    iq_str = {
        "absent_no_cases": " | investigator queries: no case yet",
        "probe_failed": " | investigator queries: case probe failed",
    }.get(investigator_queries or "", "")
    console.print(
        f"  Round {round_index}: {passed}/{total} passed "
        f"| QpH: {bench_result.qph:.1f}{freshness_str}{q9_str}{iq_str}"
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
            "gold_event_age_seconds": (
                round(round_meta.gold_event_age_seconds, 2)
                if round_meta.gold_event_age_seconds is not None
                else None
            ),
            "q9_contention": round_meta.q9_contention_observed,
            "investigator_queries": investigator_queries,
        },
    )


def _investigator_state(bench_runner, run_id: str) -> str:
    """Whether this run has a case yet, for the investigator queries: an
    untimed ``SELECT 1`` on the cases table for the run's ``base_run_id``
    (``collector.INVESTIGATOR_QUERY_STATES``). ``included`` when a row
    comes back, ``absent_no_cases`` when none does, ``probe_failed`` when
    the probe errors (that round runs without the investigator queries)."""
    try:
        cases = bench_runner._extra_tables["gold_cases"]
        sql = (
            f"SELECT 1 FROM {bench_runner.catalog}.{cases} "
            f"WHERE base_run_id = '{run_id.replace(chr(39), chr(39) * 2)}' LIMIT 1"
        )
        result = bench_runner.executor.execute_query(
            bench_runner.executor.adapt_query(sql), timeout=60
        )
    except Exception:  # noqa: BLE001 -- the probe never fails the round
        return "probe_failed"
    if not result.success:
        return "probe_failed"
    return "included" if (result.rows_returned or 0) > 0 else "absent_no_cases"


def event_age_label(seconds: float | None) -> str:
    """Gold's newest event date, as an age in days.

    The probe is day-resolution (MAX(interaction_date)) and measures where the
    corpus's event timestamps sit, so seconds would read as a precise
    freshness figure. It never is one.
    """
    if seconds is None:
        return "n/a"
    return f"{seconds / 86400:.1f} d"


def pipeline_score_freshness(pb) -> str:
    """The freshness the Pipeline Score line prints: the scored one.

    data_freshness_seconds is the continuous primary score. The query-time
    probe measured event-date age (about 635 days on a 2025 corpus, lb16) and
    headlined the score line until v1.6; it stays a labelled diagnostic.
    """
    value = pb.data_freshness_seconds
    return f"{value:.1f}s" if value is not None else "n/a"


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

    # Event-date age, not freshness: see event_age_label.
    table.add_column("Gold event age", justify="right")
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

        # Gold event-date age
        if meta and (meta.gold_event_age_seconds or 0) > 0:
            row.append(event_age_label(meta.gold_event_age_seconds))
            freshness_values.append(meta.gold_event_age_seconds)
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
        parts.append(
            f"Median gold event age: {event_age_label(median_freshness)} "
            "(corpus event time, not freshness)"
        )
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
        from lakebench.s3.client import list_user_keys

        try:
            keys = list_user_keys(raw, bronze, limit=50)
        except Exception as e:  # noqa: BLE001
            logger.warning("listing s3://%s failed: %s", bronze, e)
            _t.sleep(interval)
            continue
        if any(key.endswith(".parquet") for key in keys):
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


class StreamStartWatch:
    """``wait_until_running`` callback for one stream: reports every
    submission failure while the operator retries it, and a heartbeat while
    the wait goes on, instead of waiting silently (the 2026-09-27 discovery
    run printed nothing for 9 minutes while two streams failed five times).
    ``failures`` is what the metrics record: ``{"at", "attempt", "reason",
    "lost_seconds"}``, lost_seconds from the first poll that saw the failure
    to the first that saw another state (good to one poll interval), as for
    batch stages (JobResult.submission_failures)."""

    HEARTBEAT_S = 60

    def __init__(self, job_name: str, journal=None, clock=time.monotonic):
        self.job_name = job_name
        self.journal = journal
        self.failures: list[dict] = []
        self.running_at: str | None = None
        self._last_key: tuple | None = None
        self._next_beat: float = float(self.HEARTBEAT_S)
        self._clock = clock
        self._open: tuple[dict, float] | None = None

    @property
    def submission_retry_seconds(self) -> float:
        """Seconds this stream spent in failed submissions before it ran."""
        return round(sum(f.get("lost_seconds") or 0.0 for f in self.failures), 1)

    def close(self) -> None:
        """Close the failure still open (the wait ended while it failed)."""
        if self._open is not None:
            record, seen = self._open
            record["lost_seconds"] = round(self._clock() - seen, 1)
            self._open = None

    def __call__(self, status, elapsed: float, *, heartbeat: bool = True) -> None:
        from lakebench.metrics.continuous_window import classify_submission_failure
        from lakebench.modules.pipeline_engines.spark.job import SUCCESS_STATES, JobState

        if status.state != JobState.SUBMISSION_FAILED:
            self.close()
        if status.state == JobState.RUNNING or status.state in SUCCESS_STATES:
            if self.running_at is None:
                self.running_at = datetime.now(timezone.utc).isoformat()
            return
        if status.state == JobState.SUBMISSION_FAILED:
            # A stream seen running that fails a resubmission is not running:
            # running_at is the time it ran again, not the first sighting.
            self.running_at = None
            key = (status.submission_attempts, status.message)
            if key != self._last_key:
                self._last_key = key
                self.close()
                reason = classify_submission_failure(status.message)
                attempt = status.submission_attempts or len(self.failures) + 1
                record = {
                    "at": datetime.now(timezone.utc).isoformat(),
                    "attempt": attempt,
                    "reason": reason,
                    "lost_seconds": 0.0,
                }
                self.failures.append(record)
                self._open = (record, self._clock())
                print_warning(
                    f"lakebench-{self.job_name}: submission attempt {attempt} failed: {reason}. "
                    "The Spark Operator retries it; the window opens only when every stream runs."
                )
                if self.journal is not None:
                    _journal_safe(
                        self.journal.record,
                        EventType.STREAMING_HEALTH,
                        message=f"{self.job_name} submission failed",
                        details={"job": self.job_name, "attempt": attempt, "reason": reason},
                    )
                return
        if heartbeat and elapsed >= self._next_beat:
            self._next_beat = elapsed + self.HEARTBEAT_S
            console.print(
                f"  [dim]Waiting for lakebench-{self.job_name} to start "
                f"({status.state.value or 'NEW'}, {elapsed:.0f}s)...[/dim]"
            )


def maintenance_schedule_lines(
    table_format: str,
    query_engine: str,
    *,
    skip_maintenance: bool,
    retention_interval: int,
    retention_source: str,
    retention_threshold: str,
    compaction_configured: bool,
    compaction_interval: int,
    compaction_source: str,
) -> list[tuple[str, str]]:
    """The run header's maintenance lines, as ``(level, text)``: what the
    table format and query engine will actually run, not the Iceberg
    schedule for every run (lb16-cf Delta runs printed "Iceberg retention").
    """
    fmt = (table_format or "").lower()
    engine = (query_engine or "").lower()
    if fmt != "delta":
        if engine == "duckdb":
            return [("info", "Iceberg maintenance: not run (DuckDB cannot run table maintenance)")]
        lines = [
            (
                "info",
                "Iceberg retention: disabled (--skip-maintenance)"
                if skip_maintenance
                else f"Iceberg retention: every {retention_interval}s "
                f"({retention_source}; threshold: {retention_threshold})",
            )
        ]
        if compaction_configured and not skip_maintenance:
            lines.append(
                ("info", f"Iceberg compaction: every {compaction_interval}s ({compaction_source})")
            )
        elif compaction_configured:
            lines.append(("info", "Iceberg compaction: disabled (--skip-maintenance)"))
        return lines

    from lakebench.metrics.maintenance_policy import DELTA_CONTINUOUS_LIMITATION

    if skip_maintenance:
        vacuum = "Delta VACUUM: disabled (--skip-maintenance)"
    elif engine == "duckdb":
        vacuum = "Delta VACUUM: not run (DuckDB cannot run table maintenance)"
    elif engine == "spark-thrift":
        vacuum = "Delta VACUUM: not run (it OOMs Spark Thrift at 4Gi)"
    else:
        applied = applied_retentions("delta", retention_threshold, live_streams=True)["expire"]
        vacuum = (
            f"Delta VACUUM: every {retention_interval}s ({retention_source}; retention "
            f"{applied}, Delta's 7 d default or longer while streams are live, so nothing "
            "written in the window is eligible)"
        )
    return [
        ("info", vacuum),
        ("info", "Delta OPTIMIZE: not run (it exhausts engine memory)"),
        ("warning", DELTA_CONTINUOUS_LIMITATION[0].upper() + DELTA_CONTINUOUS_LIMITATION[1:]),
    ]


def watch_all_streams(job_manager, watches: dict[str, StreamStartWatch], current: str):
    """``wait_until_running`` callback for the stream being waited on that
    also polls every other stream not yet running.

    The streams are submitted together but waited on one at a time, so a
    stream whose submission failed while an earlier one was waited on was
    RUNNING again by its own turn and its failures were never seen: lb16-cf
    recorded only bronze-ingest's, while silver-stream's and gold-refresh's
    showed only in the operator watch. Streams already seen running stay
    polled: one that dies and fails its resubmission before its own turn
    would otherwise go unrecorded the same way.
    """

    def on_status(status, elapsed: float) -> None:
        watches[current](status, elapsed)
        for name, watch in watches.items():
            if name == current:
                continue
            try:
                other = job_manager.get_job_status(f"lakebench-{name}")
            except Exception as e:  # noqa: BLE001 -- a missed poll is retried next interval
                logger.debug("stream status poll for %s failed: %s", name, e)
                continue
            watch(other, elapsed, heartbeat=False)

    return on_status


def _size_mb(size: str) -> float:
    """MB in a datagen size string ("64MB", "1GB"); bare numbers are bytes."""
    text = str(size).strip().upper()
    for suffix, mb in (("TB", 1024.0**2), ("GB", 1024.0), ("MB", 1.0), ("KB", 1 / 1024)):
        if text.endswith(suffix):
            return float(text[: -len(suffix)]) * mb
    return float(text.rstrip("B")) / (1024 * 1024)


class NamespaceGone(Exception):
    """The run's namespace was deleted (or deleted and created again) mid-run."""

    def __init__(self, reason: str, at_elapsed: float) -> None:
        super().__init__(reason)
        self.reason = reason
        self.at_elapsed = at_elapsed


#: Consecutive failed namespace reads (other than 404) before the run stops,
#: and the least time they must span: one 503, or a burst of quick failures
#: in one loop pass, must not end a good run.
NAMESPACE_READ_STRIKES = 3
NAMESPACE_STRIKE_SPAN_S = 60.0


class NamespaceWatch:
    """Notices, within one poll interval, that the run's namespace is gone.

    ``start`` records the namespace's uid when the window opens; ``check``
    reads it again and raises :class:`NamespaceGone` on a 404, a namespace
    being deleted (deletion timestamp or phase Terminating), a different uid
    (destroyed and deployed again), or after ``NAMESPACE_READ_STRIKES``
    consecutive failed reads spanning ``NAMESPACE_STRIKE_SPAN_S``. A single
    failed read is not a reason. Reads go through a client that does not
    retry, so one costs at most 15 s.
    """

    def __init__(self, namespace: str) -> None:
        self.namespace = namespace
        self.uid: str | None = None
        self.failures = 0
        self._first_failure: float | None = None
        self._api_client: Any = None

    def _read(self):
        from kubernetes import client as k8s_client

        from lakebench.cli._interrupt import no_retry_api_client

        if self._api_client is None:
            self._api_client = no_retry_api_client()
        api = k8s_client.CoreV1Api(api_client=self._api_client)
        return api.read_namespace(self.namespace, _request_timeout=(5, 10))

    def close(self) -> None:
        if self._api_client is not None:
            try:
                self._api_client.close()
            except Exception:  # noqa: BLE001
                pass
            self._api_client = None

    def start(self) -> None:
        """Record the namespace's uid; tried three times, and if it still
        cannot be read, the first later read that answers sets it."""
        for attempt in range(NAMESPACE_READ_STRIKES):
            try:
                ns = self._read()
            except Exception as e:  # noqa: BLE001 -- the checks still see a 404
                logger.warning(
                    "Could not read namespace %s at the window start (%d of %d): %s",
                    self.namespace,
                    attempt + 1,
                    NAMESPACE_READ_STRIKES,
                    e,
                )
                continue
            self._baseline(ns)
            return

    def gone(self) -> str | None:
        """Why the namespace is gone, or None while it is there (or unread)."""
        from kubernetes.client.rest import ApiException

        try:
            ns = self._read()
        except ApiException as e:
            if e.status == 404:
                return f"namespace {self.namespace} was deleted"
            return self._strike(f"{e.status} {e.reason}")
        except Exception as e:  # noqa: BLE001 -- transport errors
            return self._strike(str(e))
        self.failures = 0
        self._first_failure = None
        meta = getattr(ns, "metadata", None)
        phase = getattr(getattr(ns, "status", None), "phase", None)
        if getattr(meta, "deletion_timestamp", None) is not None or phase == "Terminating":
            return f"namespace {self.namespace} is being deleted"
        uid = getattr(meta, "uid", None)
        if self.uid is None:
            self._baseline(ns)
        elif isinstance(uid, str) and uid and uid != self.uid:
            return f"namespace {self.namespace} was deleted and created again"
        return None

    def _baseline(self, ns: Any) -> None:
        uid = getattr(getattr(ns, "metadata", None), "uid", None)
        self.uid = uid if isinstance(uid, str) and uid else None

    def _strike(self, error: str) -> str | None:
        self.failures += 1
        now = time.time()
        if self._first_failure is None:
            self._first_failure = now
        logger.warning(
            "Could not read namespace %s (%d of %d): %s",
            self.namespace,
            self.failures,
            NAMESPACE_READ_STRIKES,
            error,
        )
        if (
            self.failures >= NAMESPACE_READ_STRIKES
            and now - self._first_failure >= NAMESPACE_STRIKE_SPAN_S
        ):
            return f"namespace {self.namespace} unreadable ({error})"
        return None

    def check(self, elapsed: float) -> None:
        reason = self.gone()
        if reason is not None:
            raise NamespaceGone(reason, elapsed)


def _record_not_observed(run_metrics, reason: str) -> None:
    """The record's corpus observation when the run did not observe it (the
    namespace went, and its bucket may be a redeployment's): said so, not
    left absent, which reads as a record from before observations. Never
    raises."""
    try:
        from lakebench.metrics.corpus_identity import MarkerSet

        inputs = (run_metrics.config_snapshot or {}).get("experiment_inputs")
        if isinstance(inputs, dict):
            inputs["corpus_observation"] = {
                "format": 1,
                "markers": MarkerSet(error=f"corpus not observed: {reason}").to_dict(),
                "series": None,
                "bronze_listing_sha256": None,
                "observed_at": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as e:  # noqa: BLE001 -- evidence, never a save failure
        logger.warning("Could not record the missing corpus observation: %s", e)


def _stop_streams(k8s, namespace: str, submitted: list) -> None:
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
        except Exception as e:  # noqa: BLE001
            print_warning(f"Could not stop {job_name}: {e}")


def _measure_bucket_sizes(cfg, collector) -> int:
    """Record the bronze/silver/gold bucket sizes on the run; the total
    object count, 0 when they cannot be measured."""
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
        return collector.record_actual_sizes(
            s3_client, s3_cfg.buckets.bronze, s3_cfg.buckets.silver, s3_cfg.buckets.gold
        )
    except Exception as e:  # noqa: BLE001
        console.print(f"  [yellow]Could not measure S3 sizes: {e}[/yellow]")
        return 0


#: Auto trickle: aim for this many window lengths of arrival, and never more
#: than this many files per trigger (the pre-v1.6 fixed default).
AUTO_ARRIVAL_MARGIN = 1.2
AUTO_TRICKLE_CEILING = 50


#: Seconds of window a continuous maintenance or compaction round needs left
#: when it comes due (continuous_round_bounds returns None below this).
MAINTENANCE_ROUND_MIN_LEFT = _CONTINUOUS_MIN_SECONDS + _BUDGET_GRACE_SECONDS


def _fires_in_window(interval: int, run_duration: int) -> bool:
    """True when a round first due at *interval* seconds still gets a budget."""
    return continuous_round_bounds(interval, run_duration - interval) is not None


def resolve_maintenance_schedule(sustained, run_duration: int, *, skip_maintenance: bool) -> dict:
    """The maintenance and compaction intervals this continuous run uses.

    Unset intervals are derived from the window (retention: run_duration / 3,
    within 300..7200; compaction: 2 x retention). The old fixed default,
    retention_interval 1800 with the default run_duration 1800, never fired,
    so a default run measured a pipeline with no table maintenance (lb16-cs).
    An explicit interval that cannot fire inside the window is refused
    (``problem``) unless maintenance is disabled: --skip-maintenance for both,
    compaction_enabled false for compaction. A derived one that cannot fire
    (a window under about six minutes) is a warning; the effective
    maintenance record then says not run.
    """
    out: dict = {"problem": None, "warnings": []}
    retention = sustained.effective_retention_interval(run_duration)
    compaction = sustained.effective_compaction_interval(run_duration)
    out["retention_interval"] = retention
    out["compaction_interval"] = compaction
    out["retention_source"] = (
        "set in config" if sustained.retention_interval is not None else "auto: run_duration / 3"
    )
    out["compaction_source"] = (
        "set in config" if sustained.compaction_interval else "auto: 2 x retention_interval"
    )
    if skip_maintenance:
        return out
    latest = run_duration - MAINTENANCE_ROUND_MIN_LEFT
    if not _fires_in_window(retention, run_duration):
        if sustained.retention_interval is not None:
            out["problem"] = (
                f"architecture.pipeline.continuous.retention_interval is {retention} s but "
                f"the run window is {run_duration} s: no maintenance round would run inside "
                f"it (the first is due at {retention} s and needs "
                f"{MAINTENANCE_ROUND_MIN_LEFT} s left). Set it to at most {latest} s, remove "
                "it (auto: run_duration / 3), or pass --skip-maintenance to run without "
                "table maintenance."
            )
            return out
        out["warnings"].append(
            f"No table maintenance round fits in a {run_duration} s window (the shortest "
            f"interval is 300 s); the run records maintenance as not run."
        )
    if sustained.compaction_enabled and not _fires_in_window(compaction, run_duration):
        if sustained.compaction_interval:
            out["problem"] = (
                f"architecture.pipeline.continuous.compaction_interval is {compaction} s but "
                f"the run window is {run_duration} s: no compaction round would run inside "
                f"it. Set it to at most {latest} s, set it to 0 (auto), set "
                "compaction_enabled: false, or pass --skip-maintenance."
            )
            return out
        out["warnings"].append(
            f"No compaction round fits in the {run_duration} s window (auto interval "
            f"{compaction} s = 2 x retention_interval); set compaction_interval to run one."
        )
    return out


def resolve_trickle(cfg, run_duration: int) -> dict:
    """The max_files_per_trigger this run uses, and why.

    Unset (auto): the most files per trigger, up to AUTO_TRICKLE_CEILING,
    whose arrival still lasts AUTO_ARRIVAL_MARGIN x run_duration, from the
    nominal corpus size (scale dimensions / datagen file size). Returns
    {"value", "source" ("auto" | "config"), "arrival_seconds" (estimate or
    None), "problem" (why the run cannot keep data arriving, or None)}.
    """
    from lakebench.metrics.continuous_window import expected_arrival_seconds

    sustained = cfg.architecture.pipeline.sustained
    explicit = sustained.max_files_per_trigger
    trigger_s = _parse_spark_interval(sustained.bronze_trigger_interval)
    try:
        dims = cfg.get_scale_dimensions()
        file_mb = _size_mb(cfg.architecture.workload.datagen.file_size)
        files = dims.approx_bronze_gb * 1024 / file_mb
    except Exception:  # noqa: BLE001 -- unknown size: keep the ceiling, no refusal
        files = None
    if files is None or files <= 0 or not trigger_s:
        value = explicit or AUTO_TRICKLE_CEILING
        return {
            "value": value,
            "source": "config" if explicit else "auto",
            "arrival_seconds": None,
            "problem": None,
        }
    if explicit:
        value, source = explicit, "config"
    else:
        value = int(files * trigger_s // (AUTO_ARRIVAL_MARGIN * run_duration))
        value, source = max(1, min(AUTO_TRICKLE_CEILING, value)), "auto"
    arrival = expected_arrival_seconds(dims.approx_bronze_gb, file_mb, value, trigger_s)
    problem = None
    if arrival is not None and arrival < run_duration:
        if source == "config":
            fit = max(1, int(files * trigger_s // (AUTO_ARRIVAL_MARGIN * run_duration)))
            problem = (
                f"max_files_per_trigger {value} offers this corpus (~{files:.0f} files of "
                f"{file_mb:.0f} MB) in about {arrival:.0f}s, less than the {run_duration}s "
                f"window, so data would stop arriving before it ends. Set "
                f"architecture.pipeline.continuous.max_files_per_trigger to {fit} or lower "
                "(or remove it to let the run derive it), or shorten run_duration."
            )
        else:
            fit = int(files * trigger_s)
            problem = (
                f"This corpus (~{files:.0f} files) lasts at most {fit}s at one file per "
                f"{trigger_s}s trigger, less than the {run_duration}s window. Set "
                f"run_duration to {fit} or lower, raise the scale, or lengthen "
                "bronze_trigger_interval."
            )
    return {"value": value, "source": source, "arrival_seconds": arrival, "problem": problem}


def cluster_clock_offset_seconds(timeout_s: float = 5.0) -> float | None:
    """Seconds the Kubernetes API server's clock is ahead of this host's,
    from its HTTP Date header (1 s resolution). The stage log timestamps
    come from pod clocks, which are assumed synced with the cluster; the
    window is shifted by this offset so a workstation clock does not move
    rows across the window's edges. None when it cannot be read."""
    import email.utils

    try:
        from kubernetes import client as k8s_client

        before = time.time()
        _data, _status, headers = k8s_client.VersionApi().get_code_with_http_info(
            _request_timeout=timeout_s
        )
        after = time.time()
        date = (headers or {}).get("Date") or (headers or {}).get("date")
        if not date:
            return None
        server = email.utils.parsedate_to_datetime(date).timestamp()
        # The header truncates to the second: its true time is up to 1 s later.
        return server + 0.5 - (before + after) / 2
    except Exception:  # noqa: BLE001 -- best effort; recorded as unknown
        return None


def stream_identities(job_manager, job_names: list[str]) -> dict[str, tuple]:
    """(driver pod, submission attempts) per stream, to tell at the window's
    end whether a stream restarted inside it."""
    out: dict[str, tuple] = {}
    for name in job_names:
        try:
            st = job_manager.get_job_status(f"lakebench-{name}")
            out[name] = (st.driver_pod, st.submission_attempts, st.start_time)
        except Exception:  # noqa: BLE001
            out[name] = (None, None, None)
    return out


def end_of_window_problems(
    job_manager, job_names: list[str], opened: dict[str, tuple] | None = None
) -> list[str]:
    """Streams that were not running when the window closed, or that
    restarted inside it (a new driver pod or another submission): a stream
    that died mid-window did not process continuously, and its earlier
    driver's log is gone, whatever the current one shows."""
    from lakebench.spark.job import JobState

    problems = []
    for name in job_names:
        try:
            st = job_manager.get_job_status(f"lakebench-{name}")
        except Exception as e:  # noqa: BLE001
            problems.append(f"continuous gate: could not read lakebench-{name} status: {e}")
            continue
        if st.state != JobState.RUNNING:
            problems.append(
                f"continuous gate: lakebench-{name} was {st.state.value or 'NEW'} when the window "
                f"closed, not RUNNING ({st.message})"
            )
            continue
        pod0, attempts0, submitted0 = ((opened or {}).get(name) or (None, None, None))[:3]
        # The operator reuses the driver pod name and may reset the attempt
        # count on a rerun; lastSubmissionAttemptTime moves on every one.
        if submitted0 and st.start_time and st.start_time != submitted0:
            problems.append(
                f"continuous gate: lakebench-{name} was resubmitted inside the window (last "
                f"submission {submitted0} -> {st.start_time}); its earlier driver's work is not "
                "in the log"
            )
        elif pod0 and st.driver_pod and st.driver_pod != pod0:
            problems.append(
                f"continuous gate: lakebench-{name} restarted inside the window (driver "
                f"{pod0} replaced by {st.driver_pod}); its earlier driver's work is not in the log"
            )
        elif attempts0 and st.submission_attempts and st.submission_attempts != attempts0:
            problems.append(
                f"continuous gate: lakebench-{name} was resubmitted inside the window "
                f"(submission attempts {attempts0} -> {st.submission_attempts})"
            )
    return problems


def short_window_problem(cfg, run_duration: int) -> str | None:
    """Why *run_duration* cannot pass the continuous gate whatever the
    pipeline does: gold refreshes every gold_refresh_interval, and the gate
    needs MIN_WINDOW_COMMITS refreshes on new data inside the window, whose
    phase relative to gold's timer is arbitrary."""
    from lakebench.metrics.continuous_window import MIN_WINDOW_COMMITS

    gold_s = _parse_spark_interval(cfg.architecture.pipeline.sustained.gold_refresh_interval)
    need = (MIN_WINDOW_COMMITS + 1) * gold_s
    if run_duration >= need:
        return None
    return (
        f"run_duration {run_duration}s is too short for a continuous run: the gate needs "
        f"{MIN_WINDOW_COMMITS} gold refreshes on new data inside the window, and with "
        f"gold_refresh_interval {gold_s}s that needs a window of at least {need}s. Lengthen "
        "run_duration or shorten gold_refresh_interval."
    )


#: Longest the CLI keeps the streams running after the window so the corpus
#: can finish passing through for the result check. Longer corpora are not
#: settled and the run records why its results were not checked.
SETTLE_MAX_SECONDS = 1800


def settle_budget_seconds(cfg, datagen_rows: int, bronze_rows: int, rows_per_s: float) -> float:
    """Seconds the remaining corpus needs to reach gold: the rows bronze has
    still to take in at the rate it held, plus two silver triggers and two
    gold refreshes, plus a minute."""
    sustained = cfg.architecture.pipeline.sustained
    remaining = max(0, datagen_rows - bronze_rows)
    intake = remaining / rows_per_s if rows_per_s > 0 else (0.0 if remaining == 0 else float("inf"))
    return (
        intake
        + 2 * _parse_spark_interval(sustained.silver_trigger_interval)
        + 2 * _parse_spark_interval(sustained.gold_refresh_interval)
        + 60
    )


def wait_for_settle(
    monitor,
    job_names: list[str],
    datagen_rows: int,
    budget_s: float,
    poll_s: float = 30.0,
    probe=None,
) -> dict:
    """Keep polling the stream logs until the whole corpus has reached gold
    (continuous_window.settle_state) or *budget_s* runs out. *probe* runs
    after every sleep (the namespace read, which raises when it is gone).
    Returns {"settled", "seconds", "reason"}."""
    from lakebench.metrics.continuous_window import settle_state

    start = time.time()
    reason = "not polled"
    while True:
        events = {}
        for name in job_names:
            try:
                logs = monitor._get_driver_logs(f"lakebench-{name}", tail_lines=None)
            except Exception:  # noqa: BLE001
                logs = None
            events[name] = parse_events(logs, name)
        settled, reason = settle_state(events, datagen_rows)
        waited = time.time() - start
        if settled:
            return {"settled": True, "seconds": round(waited, 1), "reason": reason}
        if waited + poll_s > budget_s:
            return {"settled": False, "seconds": round(waited, 1), "reason": reason}
        time.sleep(poll_s)
        if probe is not None:
            probe()


def continuous_result_check(bench_runner) -> tuple[dict, list[dict]]:
    """Run the query set once over the settled tables (streams stopped) and
    fingerprint every result, as a batch run does after its benchmark.
    Failed queries are recorded with no fingerprint. Returns the record and
    the query results (for the benchmark gate)."""
    try:
        result = bench_runner.run_power(cache="hot", iterations=1, fingerprint=True)
    except Exception as e:  # noqa: BLE001
        return {"not_checked": f"the result check could not run: {e}"}, []
    queries = [qr.to_dict() for qr in result.queries]
    fps = {qr.query.name: (qr.result_fingerprint if qr.success else None) for qr in result.queries}
    if not fps:
        return {"not_checked": "the result check ran no queries"}, []
    from lakebench.benchmark.queries import query_set_id

    return {
        "query_set_id": query_set_id(fps),
        "fingerprints": fps,
        "failed": sorted(n for n, f in fps.items() if f is None),
    }, queries


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


@restores_handlers
def _run_sustained(
    cfg,
    config_file: Path,
    timeout: int,
    skip_benchmark: bool,
    duration: int | None,
    skip_generate: bool = False,
    skip_maintenance: bool = False,
    force_reset: bool = False,
    autosize_cuts: list[str] | None = None,
    preflight: dict | None = None,
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
        preflight: The run preflight's capacity outcome, recorded as
            ``provenance.preflight``.
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

    # The trickle this run offers: derived from the corpus and the window
    # unless the config sets it, and refused when data would stop arriving
    # before the window ends (DESIGN 5: a corpus that keeps arriving).
    trickle = resolve_trickle(cfg, run_duration)
    if trickle["problem"]:
        print_error(trickle["problem"])
        raise typer.Exit(ExitCode.USAGE)
    cfg.architecture.pipeline.sustained.max_files_per_trigger = trickle["value"]
    _arrival = trickle["arrival_seconds"]
    print_info(
        f"Trickle: {trickle['value']} files per bronze trigger ({trickle['source']})"
        + (f", about {_arrival:.0f}s of arrival for the {run_duration}s window" if _arrival else "")
    )

    # Maintenance inside the window (DESIGN 5: continuous mode includes
    # periodic maintenance). Resolved values are written back so the config
    # snapshot and the experiment block record the intervals that ran.
    sustained_cfg = cfg.architecture.pipeline.sustained
    schedule = resolve_maintenance_schedule(
        sustained_cfg, run_duration, skip_maintenance=skip_maintenance
    )
    if schedule["problem"]:
        print_error(schedule["problem"])
        raise typer.Exit(ExitCode.USAGE)
    for msg in schedule["warnings"]:
        print_warning(msg)
    sustained_cfg.retention_interval = schedule["retention_interval"]
    sustained_cfg.compaction_interval = schedule["compaction_interval"]

    console.print(
        Panel(
            f"Running continuous pipeline for: [bold]{cfg.name}[/bold]\n\n"
            f"Stages: bronze-ingest + silver-stream + gold-refresh (concurrent)\n"
            f"Duration: {run_duration}s ({run_duration / 60:.0f} min)",
            expand=False,
        )
    )

    from lakebench.cli._helpers import (
        load_deps_handle,
        record_deps_pods,
        record_deps_provenance,
    )

    # Before anything is recorded: the streams need the deployment's verified
    # dependency set; a refusal exits 3 or 4 with no run saved.
    deps_handle = load_deps_handle(cfg, config_file)

    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.RUN, {"sustained": True, "duration": run_duration})
    _deps_check_failed = False
    _verdict_failed = False
    _exception_in_flight = False
    _k8s_unreachable = False

    collector = MetricsCollector()
    metrics_storage = MetricsStorage()
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    # Share the run id with datagen pods and Spark drivers (live observability
    # grouping label) via the orchestrator process env.
    os.environ["LB_RUN_ID"] = run_id
    from lakebench.metrics import build_config_snapshot

    config_snapshot = build_config_snapshot(cfg, run_mode="continuous", config_path=config_file)
    collector.start_run(run_id, cfg.name, config_snapshot, config_path=config_file)
    record_deps_provenance(collector.current_run, deps_handle)
    collector.record_preflight(preflight)
    # System identity and cluster load at run start; never raises.
    from lakebench.metrics.system_identity import sample_run_end, sample_run_start

    sample_run_start(collector.current_run, cfg)
    if collector.current_run is not None:
        collector.current_run.autosize_cuts = autosize_cuts
        # [] from the start: a run that ends before any maintenance round is
        # then stamped "not run", never with the policy's request.
        collector.current_run.maintenance_outcomes = []
    if skip_maintenance and collector.current_run is not None:
        # No table maintenance: not comparable with runs under the policy.
        from lakebench.metrics.maintenance_policy import skipped_policy_id

        collector.current_run.maintenance_policy_id = skipped_policy_id()

    pipeline_success = True
    _datagen_output_gb = 0.0
    _datagen_output_rows = 0
    _datagen_output_files = 0
    # The fleet record of this run's own datagen pods (None with --skip-generate).
    _run_fleet: dict | None = None
    # Streams submitted and not yet stopped: the finally block stops them on
    # any early exit, so an error or Ctrl-C never leaves them running.
    submitted: list = []
    streams_stopped = False
    k8s = None
    _total_s3_objects: int | None = None
    streaming_jobs = [
        (JobType.BRONZE_INGEST, "bronze-ingest"),
        (JobType.SILVER_STREAM, "silver-stream"),
        (JobType.GOLD_REFRESH, "gold-refresh"),
    ]
    # Ctrl-C and SIGTERM seal the record INTERRUPTED and stop the streams,
    # the datagen Job and any preflight this run created, by uid
    # (cli/_interrupt.py). Installed before the operator check, whose
    # watch-list heal can take the cluster lease.
    from lakebench.cli._interrupt import RunInterrupt

    _interrupt = RunInterrupt(cfg.get_namespace(), run_id)
    _interrupt.install()
    _stage = "operator-check"
    _interrupted: dict | None = None
    # Exit 130 at the end: an interrupt, or a late signal in a run that had
    # not failed (a failed run keeps its own exit).
    _exit_interrupted = False
    # Set when the namespace went away mid-window: {reason, at_elapsed}.
    _abort: dict | None = None
    _ns_watch = NamespaceWatch(cfg.get_namespace())

    try:
        # Check Spark operator
        print_info("Checking Spark Operator...")
        spark_op_cfg = cfg.platform.compute.spark.operator
        operator = SparkOperatorManager(
            namespace=spark_op_cfg.namespace,
            job_namespace=cfg.get_namespace(),
            kube_context=cfg.platform.kubernetes.context,
        )
        status = operator.check_status()
        if not status.ready:
            hint = ""
            if status.installed is False:
                hint = (
                    " -- a cluster admin installs it once with 'lakebench admin install "
                    "--component spark-operator <config>'"
                )
            print_error(f"Spark Operator not ready: {status.message}{hint}")
            pipeline_success = False
            raise typer.Exit(ExitCode.PREREQUISITE)

        # Ensure operator watches the target namespace (always try to heal)
        ns_status = operator.ensure_namespace_watched(can_heal=True)
        if ns_status.watching_namespace is False or not ns_status.ready:
            print_error(ns_status.message)
            pipeline_success = False
            raise typer.Exit(ExitCode.PREREQUISITE)

        print_success(f"Spark Operator ready (version: {status.version or 'unknown'})")

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        job_manager: SparkJobManager = get_engine(cfg, k8s)  # type: ignore[assignment]
        job_manager.deps = deps_handle
        monitor = SparkJobMonitor(cfg, k8s, job_manager=job_manager)

        _short = short_window_problem(cfg, run_duration)
        if _short:
            print_error(_short)
            pipeline_success = False
            raise typer.Exit(ExitCode.USAGE)
        if not skip_benchmark and cfg.architecture.query_engine.type.value == "none":
            print_info("No query engine in this recipe: no in-stream rounds and no result check")
            skip_benchmark = True

        # The benchmark runner serves the in-stream rounds and the end-of-run
        # result check. A run that cannot create it cannot produce the query
        # evidence continuous mode claims, so it fails here, before any
        # stream starts (it used to warn and pass with no rounds).
        bench_runner = None
        if not skip_benchmark:
            try:
                from lakebench.benchmark import BenchmarkRunner

                # AML with the TM operations layer: the investigator queries
                # (IQ1 to IQ4) read this run's cases, so each round runs
                # them once a case exists (_run_benchmark_round's probe).
                workload = cfg.architecture.workload
                investigators = (
                    workload.schema_type.value == "financial" and workload.tm_operations.enabled
                )
                bench_runner = BenchmarkRunner(cfg, tm_run_id=run_id if investigators else None)
            except Exception as e:  # noqa: BLE001
                print_error(f"Could not create the benchmark runner: {e}")
                pipeline_success = False
                raise typer.Exit(ExitCode.FAILED) from None

        # Packaging and size errors in the scripts maps surface here, before
        # the reset below drops any state; the maps are applied after it.
        from lakebench.modules.pipeline_engines.spark.scripts_maps import (
            ScriptsMapError,
            build_script_configmaps,
        )

        try:
            build_script_configmaps(cfg, cfg.get_namespace())
        except ScriptsMapError as e:
            print_error(f"Spark scripts not deployed: {e}")
            _journal_safe(j.end_command, success=False, message=f"Scripts ConfigMaps: {e}")
            raise typer.Exit(ExitCode.FAILED) from None

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
        _stage = "reset"
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
                    "`run --continuous`."
                )
            elif _existing:
                if _raw_problem:
                    print_info(f"Raw data is not replaced automatically: {_raw_problem}.")
                _refuse_c360_reset(cfg, _existing)
                pipeline_success = False
                raise typer.Exit(ExitCode.REFUSED)
        _stop_leftover_streams(job_manager, cfg.get_namespace())
        if not skip_generate:
            # An earlier datagen Job's pods would keep writing into the raw
            # prefix the reset is about to clear, and silver would count
            # their files as this run's rows: delete the Job and wait
            # (bounded) for its pods to stop before anything is cleared.
            from lakebench.cli._helpers import stop_previous_datagen_or_exit

            try:
                stop_previous_datagen_or_exit(cfg, "Refusing to reset continuous state")
            except typer.Exit:
                pipeline_success = False
                raise
        if not skip_generate:
            # The namespace's fleet sidecar describes the corpus this run
            # clears and regenerates.
            from lakebench.metrics.datagen_aggregator import drop_sidecar

            drop_sidecar(cfg.get_namespace())
        _reset_continuous_state(cfg, clear_raw=not skip_generate)
        # Deploy the scripts ConfigMaps (includes streaming scripts) -- must
        # succeed. After the leftover streams are stopped: a changed map is not
        # replaced while a live SparkApplication mounts it, since the pod
        # would read a mix of old and new scripts.
        print_info("Deploying Spark scripts...")
        try:
            scripts_ok = job_manager.deploy_scripts_configmap()
        except ScriptsMapError as e:
            print_error(f"Spark scripts not deployed: {e}")
            _journal_safe(j.end_command, success=False, message=f"Scripts ConfigMaps: {e}")
            raise typer.Exit(ExitCode.FAILED) from None
        if not scripts_ok:
            print_error("Failed to deploy Spark scripts ConfigMap -- pipeline cannot proceed")
            _journal_safe(j.end_command, success=False, message="Scripts ConfigMap deploy failed")
            raise typer.Exit(ExitCode.FAILED)
        print_success("Spark scripts deployed")
        dims = cfg.get_scale_dimensions()
        collector.record_job_manager(job_manager)
        if skip_generate:
            console.print()
            print_info("Skipping datagen deploy (--skip-generate)")
        else:
            _stage = "datagen"
            console.print()
            console.print("[bold]Starting datagen...[/bold]")
            datagen = DatagenDeployer(engine, continuous=True)
            _interrupt.creating("Job", "lakebench-datagen")
            datagen_result = datagen.deploy()
            if datagen_result.status != DeploymentStatus.SUCCESS:
                _interrupt.not_created("Job", "lakebench-datagen")
                print_error(f"Failed to start datagen: {datagen_result.message}")
                pipeline_success = False
                # A refusal (stale bronze, live datagen pods) exits 3.
                from lakebench.cli._exit import refused_result_code

                raise typer.Exit(refused_result_code([datagen_result]) or ExitCode.FAILED)
            _interrupt.datagen_created()
            print_success("Datagen started (continuous mode)")
            # Only when this run started datagen: --skip-generate journalled
            # a datagen start that never happened.
            _journal_safe(
                j.record,
                EventType.GENERATE_START,
                message="Datagen started for continuous pipeline",
                details={
                    "scale": dims.scale,
                    "parallelism": cfg.architecture.workload.datagen.parallelism,
                    "target_gb": round(dims.approx_bronze_gb, 1),
                },
            )
        console.print(f"  Scale: {dims.scale}")
        console.print(f"  Parallelism: {cfg.architecture.workload.datagen.parallelism} pods")

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
            _stage = "preflight"
            _interrupt.creating("SparkApplication", "lakebench-bronze-verify")
            preflight_status = job_manager.submit_job(
                JobType.BRONZE_VERIFY,
                # Reset, not register: see bronze_verify_financial
                # CONTINUOUS_RESET.
                cycle_env={"LB_REGISTER_TABLE": "schema"},
            )
            _interrupt.submitted(preflight_status)
            if preflight_status.state == JobState.FAILED:
                print_error(f"bronze-verify preflight submit failed: {preflight_status.message}")
                pipeline_success = False
                raise typer.Exit(ExitCode.FAILED)
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
                raise typer.Exit(ExitCode.FAILED)
            _interrupt.finished("SparkApplication", "lakebench-bronze-verify")
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
            _stage = "preflight"
            if not _run_c360_continuous_reset(
                job_manager, monitor, console, timeout_seconds=_reset_timeout, interrupt=_interrupt
            ):
                pipeline_success = False
                raise typer.Exit(ExitCode.FAILED)

        # The corpus is finite and usually written within minutes; a finished
        # datagen Job holds no cores, so the streaming budget stops reserving
        # them (LB-158). Wait a bounded time for it so the executor counts do
        # not depend on a race with one API read. Unfinished or unknown keeps
        # the reservation, so the streams never over-commit the cluster.
        job_manager.datagen_running = not _datagen_released(
            cfg.get_namespace(), deployed_here=not skip_generate
        )
        if not job_manager.datagen_running:
            # Finished (or gone): nothing of it to stop on an interrupt.
            _interrupt.finished("Job", "lakebench-datagen")

        # Launch all streaming jobs concurrently
        _stage = "streams-start"
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
            _interrupt.creating("SparkApplication", f"lakebench-{job_name}")
            job_status = job_manager.submit_job(job_type, cycle_env=stream_env)
            _interrupt.submitted(job_status)
            if job_status.state == JobState.FAILED:
                print_error(f"Failed to submit {job_name}: {job_status.message}")
                pipeline_success = False
                raise typer.Exit(ExitCode.FAILED)
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
        # Each stream's submission failures are printed, journaled and
        # recorded as they happen (StreamStartWatch).
        _start_deadline = time.time() + 1800
        stream_watch = {name: StreamStartWatch(name, j) for _, name in submitted}
        if collector.current_run is not None:
            collector.current_run.continuous = {"streams": {}}
        try:
            for _job_type, job_name in submitted:
                running = monitor.wait_until_running(
                    f"lakebench-{job_name}",
                    timeout_seconds=max(0, int(_start_deadline - time.time())),
                    on_status=watch_all_streams(job_manager, stream_watch, job_name),
                )
                if not running.success:
                    print_error(f"lakebench-{job_name} did not start: {running.message}")
                    pipeline_success = False
                    raise typer.Exit(ExitCode.FAILED)
                collector.record_scratch(job_name, running.final_status)
        finally:
            for w in stream_watch.values():
                w.close()
            if collector.current_run is not None:
                if collector.current_run.continuous is None:
                    collector.current_run.continuous = {}
                collector.current_run.continuous["streams"] = {
                    name: {
                        "running_at": w.running_at,
                        "submission_failures": w.failures,
                        "submission_retry_seconds": w.submission_retry_seconds,
                    }
                    for name, w in stream_watch.items()
                }
        collector.observe_images(
            cfg.get_namespace(),
            at="streams running",
            apps={f"lakebench-{n}" for _, n in submitted},
        )
        # The window in cluster time (pod log clocks), not this host's.
        clock_offset = cluster_clock_offset_seconds()
        from datetime import timedelta as _td

        _shift = _td(seconds=clock_offset or 0.0)
        _host_window_start = utc_naive(datetime.now(timezone.utc))
        window_start = _host_window_start + _shift
        opened_identity = stream_identities(job_manager, [n for _, n in submitted])
        # The namespace as the window opens: destroyed (or destroyed and
        # deployed again) mid-run ends the run within one loop interval.
        _ns_watch.start()
        _first_up = min(
            (datetime.fromisoformat(w.running_at) for w in stream_watch.values() if w.running_at),
            default=None,
        )
        if _first_up is not None:
            # Both on this host's clock (running_at is stamped here).
            _skew = (_host_window_start - utc_naive(_first_up)).total_seconds()
            if _skew > 60:
                print_warning(
                    f"The first stream was running {_skew:.0f}s before the last one started. "
                    "Rows taken in before the window opens are recorded as pre-window rows and "
                    "left out of every window score."
                )

        # Monitor for configured duration, running benchmark rounds at intervals
        _stage = "window"
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

        bench_runner_instream = bench_runner if can_run_rounds else None
        if bench_runner_instream is not None:
            print_info(
                f"In-stream benchmarking: warmup {bench_warmup}s, then every {bench_interval}s"
            )

        # Iceberg retention scheduling. --skip-maintenance disables both
        # the expire_snapshots loop and periodic compaction; without
        # skipping, mid-run maintenance would distort a raw
        # freshness/throughput measurement.
        retention_interval = sustained_cfg.effective_retention_interval(run_duration)
        retention_threshold = sustained_cfg.retention_threshold
        compaction_enabled = sustained_cfg.compaction_enabled and not skip_maintenance
        compaction_interval = sustained_cfg.effective_compaction_interval(run_duration)
        for _level, _line in maintenance_schedule_lines(
            cfg.architecture.table_format.type.value,
            cfg.architecture.query_engine.type.value,
            skip_maintenance=skip_maintenance,
            retention_interval=retention_interval,
            retention_source=schedule["retention_source"],
            retention_threshold=retention_threshold,
            compaction_configured=sustained_cfg.compaction_enabled,
            compaction_interval=compaction_interval,
            compaction_source=schedule["compaction_source"],
        ):
            (print_warning if _level == "warning" else print_info)(_line)
        if skip_maintenance:
            next_maintenance_at = float("inf")
        else:
            next_maintenance_at = float(retention_interval)  # first run after one interval
            from lakebench.config.loader import retention_floor_advisory

            floor_msg = retention_floor_advisory(cfg)
            if floor_msg and not is_continuous_mode(cfg.architecture.pipeline.mode):
                # Continuous configs already warned at load.
                print_warning(floor_msg)

        # Compaction scheduling (v1.1.0); Delta OPTIMIZE never runs.
        next_compaction_at = float(compaction_interval) if compaction_enabled else float("inf")

        start = time.time()
        check_interval = 30
        # Where the next round starts in the table list: after a timeout,
        # the table after the one that timed out (see _next_start).
        maintenance_start = 0
        compaction_start = 0
        # What each maintenance and compaction round actually did (the
        # experiment block's effective maintenance). [] = the loop ran none.
        maintenance_outcomes: list = []
        if collector.current_run is not None:
            if collector.current_run.maintenance_outcomes is None:
                collector.current_run.maintenance_outcomes = []
            maintenance_outcomes = collector.current_run.maintenance_outcomes
        if skip_maintenance:
            for kind in ("expire", "compaction"):
                maintenance_outcomes.append({"kind": kind, "user_skip": "--skip-maintenance"})
        elif not compaction_enabled:
            maintenance_outcomes.append(
                {"kind": "compaction", "user_skip": "continuous compaction is disabled"}
            )
        # A timed-out maintenance statement may still be running; compaction
        # waits this long (seconds into the run) before touching the tables.
        compaction_hold_until = 0.0
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
                _ns_watch.check(elapsed)
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
                # A round takes minutes, and the loop goes straight on to
                # the next pass without a sleep.
                _ns_watch.check(time.time() - start)
                last_round_seconds = time.time() - round_start
                # Next round at interval from round completion
                next_round_at = (time.time() - start) + bench_interval
                continue
            skip_msg = late_benchmark_round_skip(
                can_run_rounds, elapsed, next_round_at, remaining, min_remaining
            )
            if skip_msg:
                # Too little run left for the round. Without this the round
                # stayed due, next_round_at stayed in the past, and the loop
                # slept 0 s and journaled a health line on every pass.
                console.print(f"  [dim]{skip_msg}[/dim]")
                _journal_safe(
                    j.record,
                    EventType.STREAMING_HEALTH,
                    message=skip_msg,
                    details={"remaining_seconds": remaining, "needed_seconds": min_remaining},
                )
                next_round_at = float("inf")

            # Iceberg retention maintenance
            if elapsed >= next_maintenance_at:
                _ns_watch.check(elapsed)
                # A plan-building error (a bad retention string, an engine
                # lookup failure) must not end the continuous run.
                bounds = continuous_round_bounds(
                    retention_interval, run_duration - (time.time() - start)
                )
                try:
                    if bounds is None:
                        console.print("  [dim]Maintenance round skipped (run window ending)[/dim]")
                    else:
                        maint_budget = MaintenanceBudget(
                            bounds[1], label="continuous maintenance round"
                        )
                        resume = _run_iceberg_maintenance(
                            cfg,
                            k8s,
                            console,
                            j,
                            retention_threshold,
                            timeout=bounds[0],
                            live_streams=True,
                            budget=maint_budget,
                            start_at=maintenance_start,
                            outcomes=maintenance_outcomes,
                        )
                        if resume is not None:
                            maintenance_start = resume
                        if "timed out" in maint_budget.stopped:
                            compaction_hold_until = (time.time() - start) + bounds[0]
                except Exception as e:  # noqa: BLE001
                    _note_outcome(maintenance_outcomes, "expire", error=str(e))
                    logger.warning("maintenance round failed: %s", e)
                    console.print(f"  [yellow]Maintenance round failed: {e}[/yellow]")
                    _journal_safe(
                        j.record,
                        EventType.STREAMING_HEALTH,
                        message="Maintenance round failed",
                        details={"error": str(e)},
                    )
                next_maintenance_at = (time.time() - start) + retention_interval

            # Iceberg compaction (v1.1.0). Bounds use the time now, after any
            # maintenance round above, so the two cannot overrun the window.
            if compaction_enabled and elapsed >= next_compaction_at:
                now = time.time() - start
                _ns_watch.check(now)
                bounds = continuous_round_bounds(compaction_interval, run_duration - now)
                if now < compaction_hold_until:
                    console.print(
                        "  [dim]Compaction deferred: a maintenance statement timed out "
                        "and may still be running[/dim]"
                    )
                    next_compaction_at = compaction_hold_until
                else:
                    if bounds is None:
                        console.print("  [dim]Compaction round skipped (run window ending)[/dim]")
                    else:
                        resume = _run_iceberg_compaction(
                            cfg,
                            k8s,
                            console,
                            j,
                            live_streams=True,
                            timeout=bounds[0],
                            budget=MaintenanceBudget(
                                bounds[1], label="continuous compaction round"
                            ),
                            start_at=compaction_start,
                            outcomes=maintenance_outcomes,
                        )
                        if resume is not None:
                            compaction_start = resume
                    next_compaction_at = (time.time() - start) + compaction_interval

            # Sleep until next event (health check, benchmark round, maintenance, or compaction)
            sleep_time = loop_sleep_seconds(
                time.time() - start,
                elapsed + check_interval,
                next_round_at if can_run_rounds else float("inf"),
                next_maintenance_at,
                next_compaction_at,
                run_duration,
            )
            if sleep_time > 0:
                time.sleep(sleep_time)

            # Periodic health check
            elapsed = time.time() - start
            remaining = run_duration - elapsed
            _ns_watch.check(elapsed)
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

        window_end = utc_naive(datetime.now(timezone.utc)) + _shift
        _stage = "collect"
        window_seconds = (window_end - window_start).total_seconds()
        # A stream that died or restarted inside the window did not process
        # continuously.
        window_problems = end_of_window_problems(
            job_manager, [n for _, n in submitted], opened_identity
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
                _datagen_output_files = int(_fleet.total_files_written or 0)
        except Exception as e:
            _fleet = None
            logger.warning("Could not read datagen row counts: %s", e)
        if _fleet is not None and not skip_generate:
            # This run's own datagen pods: the record's fleet, and the
            # namespace's sidecar for a later run over this corpus.
            try:
                from lakebench.metrics.datagen_aggregator import fleet_record, write_sidecar

                _run_fleet = fleet_record(_fleet, cfg.get_namespace())
                write_sidecar(_run_fleet, cfg.get_namespace())
            except Exception as e:  # noqa: BLE001 -- the record then says no fleet
                logger.warning("Could not record the datagen fleet: %s", e)

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

        # What each stream did inside the window, and the continuous gate:
        # data arriving during the window, several silver commits and gold
        # refreshes on new data, gold freshness measured (invariant 3).
        from lakebench.metrics.continuous_window import window_gate_problems

        parsed: dict[str, StreamingJobMetrics] = {}
        window_stats_by_job: dict[str, dict | None] = {}
        for _job_type, job_name in submitted:
            logs = driver_logs.get(job_name)
            sm = StreamingJobMetrics(job_name=f"lakebench-{job_name}", job_type=job_name)
            if logs:
                try:
                    sm = collector.parse_streaming_logs(logs, job_name)
                except Exception as e:  # noqa: BLE001
                    print_warning(f"{job_name}: parse failed: {e}")
            stats = sm.apply_window(logs, window_start, window_end) if logs else None
            # A log with no timestamped stage line cannot place anything in
            # the window.
            if stats is not None and not parse_events(logs, job_name):
                stats = None
            window_stats_by_job[job_name] = stats
            watch = stream_watch.get(job_name)
            if watch is not None:
                sm.running_at = watch.running_at
                sm.submission_failures = list(watch.failures)
            parsed[job_name] = sm
        window_problems.extend(window_gate_problems(window_stats_by_job, window_seconds))
        for _problem in window_problems:
            print_error(_problem)
        if window_problems:
            pipeline_success = False
        else:
            print_success(
                "Continuous gate: data arrived during the window and silver and gold "
                "committed continuously"
            )
        continuous_record = {
            "window": {
                "start": window_start.isoformat() + "Z",
                "end": window_end.isoformat() + "Z",
                "seconds": round(window_seconds, 1),
                # Cluster clock minus this host's; None: unknown, the
                # window is on this host's clock.
                "cluster_clock_offset_seconds": (
                    round(clock_offset, 1) if clock_offset is not None else None
                ),
            },
            "gate_problems": list(window_problems),
            "trickle": trickle,
            # Applied retention (expiry floored while streams are live), so a
            # default below the floor is recorded as what ran.
            "retention": (
                {"skipped": "--skip-maintenance"}
                if skip_maintenance
                else continuous_retention_record(cfg)
            ),
        }
        if collector.current_run is not None:
            if collector.current_run.continuous is None:
                collector.current_run.continuous = {}
            collector.current_run.continuous.update(continuous_record)
        _journal_safe(
            j.record,
            EventType.STREAMING_HEALTH,
            message="Continuous window closed",
            details={
                **continuous_record,
                "window_stats": {
                    k: (dict(v) if v else None) for k, v in window_stats_by_job.items()
                },
            },
        )

        # Result check (DESIGN 4.1 for continuous): keep the streams running
        # until the whole corpus has reached gold, stop them, and fingerprint
        # the query set over tables that are then a function of the corpus
        # alone. Not part of the window: nothing here is scored.
        settle: dict = {"settled": False}
        _stage = "settle"
        result_check: dict = {}
        # Bucket sizes and object counts as of the window, before any settle
        # micro-batches add files.
        _total_s3_objects = _measure_bucket_sizes(cfg, collector)
        if bench_runner is None:
            result_check = {"not_checked": "no benchmark (--skip-benchmark)"}
        elif not pipeline_success:
            result_check = {"not_checked": "the run failed its gates"}
        elif cfg.architecture.workload.schema_type.value == "financial":
            result_check = {
                "not_checked": (
                    "AML continuous results depend on when detection and TM passes ran "
                    "relative to arrival, so they are not a function of the corpus alone"
                )
            }
        elif _datagen_output_rows <= 0:
            result_check = {"not_checked": "datagen row count not measured, so settling is unknown"}
        else:
            _bronze = parsed.get("bronze-ingest")
            _rps = (
                (_bronze.window_input_rows or 0) / window_seconds
                if _bronze is not None and window_seconds > 0
                else 0.0
            )
            _need = settle_budget_seconds(
                cfg,
                _datagen_output_rows,
                _bronze.total_rows_processed if _bronze is not None else 0,
                _rps,
            )
            if _need > SETTLE_MAX_SECONDS:
                result_check = {
                    "not_checked": (
                        f"the rest of the corpus needs about {_need:,.0f}s to reach gold, over "
                        f"the {SETTLE_MAX_SECONDS}s settle limit"
                    )
                }
                settle = {"settled": False, "reason": result_check["not_checked"]}
            else:
                print_info(
                    f"Letting the pipeline take in the rest of the corpus for the result "
                    f"check (up to {_need:.0f}s, not scored)..."
                )
                settle = wait_for_settle(
                    monitor,
                    [n for _, n in submitted],
                    _datagen_output_rows,
                    _need,
                    probe=lambda: _ns_watch.check(time.time() - start),
                )
                if settle["settled"]:
                    print_success(f"Corpus settled in gold after {settle['seconds']:.0f}s")
                else:
                    result_check = {"not_checked": f"the corpus did not settle: {settle['reason']}"}
                    print_warning(
                        f"Result check skipped: {result_check['not_checked']}. The run's "
                        "results are not established, so it cannot be compared."
                    )

        # Stop streaming jobs. By name: so first make sure the namespace is
        # still this run's (a redeployment's streams have the same names).
        _stage = "stop-streams"
        _ns_watch.check(time.time() - start)
        # Images again before the streams stop: a driver or executor that
        # restarted inside the window may have pulled another digest.
        collector.observe_images(
            namespace, at="before stop", apps={f"lakebench-{n}" for _, n in submitted}
        )
        _stop_streams(k8s, namespace, submitted)
        streams_stopped = True
        for _job_type, job_name in submitted:
            _interrupt.finished("SparkApplication", f"lakebench-{job_name}")
        _stage = "result-check"

        _journal_safe(
            j.record,
            EventType.STREAMING_STOP,
            message="Streaming pipeline stopped",
            details={"duration_seconds": run_duration},
        )

        if settle.get("settled") and bench_runner is not None:
            print_info("Result check: fingerprinting the query set over the settled tables...")
            result_check, _check_queries = continuous_result_check(bench_runner)
            if _check_queries:
                # The settled tables answer every query; a failure here is a
                # failed query, as in batch.
                from lakebench.cli._run import _benchmark_gate_problems

                for problem in _benchmark_gate_problems(cfg, _check_queries, check_empty=True):
                    print_error(f"Result check: {problem}")
                    pipeline_success = False
            if result_check.get("fingerprints"):
                _n = len(result_check["fingerprints"])
                _bad = result_check.get("failed") or []
                print_success(
                    f"Result check: {_n - len(_bad)} of {_n} query results fingerprinted"
                    + (f" (failed: {', '.join(_bad)})" if _bad else "")
                )
            else:
                print_warning(f"Result check: {result_check.get('not_checked')}")
        if collector.current_run is not None:
            if collector.current_run.continuous is None:
                collector.current_run.continuous = {}
            collector.current_run.continuous["settle"] = settle
            collector.current_run.continuous["result_check"] = result_check

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
                    _rows_by_job[_jn] = (
                        parsed[_jn].total_rows_processed if driver_logs.get(_jn) else None
                    )
            for _problem in _c360_continuous_gate_problems(_rows_by_job):
                print_error(_problem)
                pipeline_success = False

        # Record streaming metrics (from pre-captured driver logs)
        console.print()
        console.print("[bold]Parsing continuous-mode metrics...[/bold]")
        for _job_type, job_name in submitted:
            logs = driver_logs.get(job_name)
            streaming_metrics = parsed[job_name]
            if logs:
                inside = streaming_metrics.window_input_rows
                console.print(
                    f"  {job_name}: {streaming_metrics.total_batches} batches, "
                    f"{streaming_metrics.total_rows_processed:,} rows"
                    + (
                        f" ({streaming_metrics.window_commits} commits, "
                        f"{inside:,} rows inside the window)"
                        if inside is not None
                        else (
                            f" ({streaming_metrics.window_commits} refreshes inside the window)"
                            if streaming_metrics.window_commits is not None
                            else ""
                        )
                    )
                )
                if streaming_metrics.total_batches == 0:
                    # Show a sample of the logs to debug pattern mismatch
                    lines = [line for line in logs.split("\n") if line.strip()]
                    if lines:
                        console.print("    [dim]no batches parsed -- sample lines:[/dim]")
                        for sample in lines[:3]:
                            truncated = sample[:100] + "..." if len(sample) > 100 else sample
                            console.print(f"      {truncated}")
            else:
                console.print(f"  {job_name}: no logs to parse")

            # The measured window, not the configured run_duration.
            streaming_metrics.elapsed_seconds = window_seconds
            streaming_metrics.requested_executors = requested_executors.get(job_name)
            streaming_metrics.success = pipeline_success
            _rows = streaming_metrics.window_input_rows
            if streaming_metrics.elapsed_seconds > 0 and _rows:
                streaming_metrics.throughput_rps = _rows / streaming_metrics.elapsed_seconds
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
                    # Q9 reads the c360 gold table that gold-refresh replaces
                    # while rounds run; after its retries a Q9 failure is
                    # expected contention, reported but not a run failure.
                    from lakebench.cli._run import _benchmark_gate_problems, empty_benchmark_queries

                    for idx, rnd in enumerate(rounds, 1):
                        final = idx == len(rounds)
                        q9 = tolerated_q9_results(rnd.queries, final=final)
                        for q in q9:
                            what = "failed" if not q.get("success") else "returned no rows"
                            print_warning(
                                f"Round {idx}: {q['name']} {what} after contention retries "
                                "(gold refresh replaces the table it reads)"
                            )
                        rest = [q for q in rnd.queries if q not in q9]
                        # Early rounds can run before gold or the alert tables
                        # hold rows; an empty result there is reported, and
                        # only the last round is held to the empty-result gate.
                        if not final:
                            for name in empty_benchmark_queries(rest):
                                print_warning(f"Round {idx}: {name} returned no rows")
                        for problem in _benchmark_gate_problems(cfg, rest, check_empty=final):
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
                    f"[green]Continuous pipeline completed![/green]\n\n"
                    f"  Duration: {run_duration}s ({run_duration / 60:.0f} min)\n"
                    f"  Streaming jobs: {len(submitted)}\n\n"
                    f"Query results: lakebench query --example count\n"
                    f"Report: report.html in the run directory",
                    title="Continuous Pipeline Complete",
                    expand=False,
                )
            )

        # LB-127 P0 fix: a flagged failure MUST exit non-zero. Every other
        # failed step in this function raises typer.Exit(ExitCode.FAILED); the gate above
        # only set the flag (so the record loop + benchmark aggregation could
        # still persist). Raise now, inside the try, so the finally block still
        # runs (metrics + journal persist with success=False) and the process
        # exits 1 -- the exact signal an exit-code-only UAT runner reads, which
        # is the whole point of closing LB-044.
        if not pipeline_success:
            raise typer.Exit(ExitCode.FAILED)

    except K8sConnectionError as e:
        # K8sConnectionError means the kube config did not load
        # (k8s/client.py), so nothing was submitted: a prerequisite (4).
        print_error(f"Kubernetes connection failed: {e}")
        pipeline_success = False
        _exception_in_flight = True
        _k8s_unreachable = True
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904
    except typer.Exit as e:
        # Several refusals above exit without clearing the flag; a run that
        # exits non-zero is never recorded as a success.
        if e.exit_code:
            pipeline_success = False
            _exception_in_flight = True
        raise
    except KeyboardInterrupt as e:
        # SIGINT or SIGTERM anywhere (the settle phase can last 30 min). Sealed
        # first; then the streams, the datagen Job and an unfinished preflight
        # this run created are deleted by uid. The finally must not then stop
        # the streams by name: one left with 409 is not ours, and one skipped
        # was skipped on request. Not re-raised: the finally exits 130.
        _interrupted = _interrupt.seal(at_stage=_stage, prior_failure=not pipeline_success, exc=e)
        pipeline_success = False
        streams_stopped = True
        _exit_interrupted = True
        console.print()
        print_warning(f"Interrupted by {_interrupted['signal']} during {_stage}")
        _interrupt.stop_owned(_interrupted, console)
    except NamespaceGone as e:
        # Destroyed under the run: its streams, datagen Job and pods went
        # with it, so nothing is stopped (a redeployed namespace's objects
        # are not this run's either).
        _abort = {"reason": e.reason, "at_elapsed": round(e.at_elapsed, 1)}
        pipeline_success = False
        streams_stopped = True
        print_error(f"{e.reason} mid-run; stopping")
    except BaseException:
        # Any other error is not a pass.
        pipeline_success = False
        _exception_in_flight = True
        raise
    finally:
        # A signal from here on does not stop the record being written (a
        # third one still does, cli/_interrupt.py).
        _interrupt.begin_seal()
        # The query engine pods (Thrift, DuckDB) against the run's set; not
        # after an interrupt, once the namespace is gone, or without a cluster.
        _pods_skipped = (
            "interrupted"
            if _interrupted is not None
            else (
                "namespace gone"
                if _abort is not None
                else ("cluster unreachable" if _k8s_unreachable else None)
            )
        )
        if record_deps_pods(collector.current_run, cfg, deps_handle, skipped=_pods_skipped):
            pipeline_success = False
            _deps_check_failed = True
        if submitted and not streams_stopped and k8s is not None:
            _stop_streams(k8s, cfg.get_namespace(), submitted)
        _ns_watch.close()
        if _total_s3_objects is None and _interrupted is None and _abort is None:
            # Not after an interrupt: partial data, and a long listing would
            # hold the record back from a user who has just pressed Ctrl-C.
            # Not after the namespace went: its buckets may be a redeployment's.
            _total_s3_objects = _measure_bucket_sizes(cfg, collector)
        if _interrupted is None and _interrupt.late_signal():
            # Interrupted while the run was being wound up: sealed the same way.
            _interrupted = _interrupt.seal(at_stage="results", prior_failure=not pipeline_success)
            _exit_interrupted = pipeline_success
            pipeline_success = False

        run_metrics = collector.end_run(success=pipeline_success)
        if run_metrics:
            run_metrics.interrupted = _interrupted
            run_metrics.abort_reason = _abort
            if _run_fleet is not None:
                run_metrics.datagen_fleet = _run_fleet
            # Collect platform metrics from Prometheus (best-effort)
            if _interrupted is None and _abort is None:
                _collect_platform_metrics(cfg, run_metrics)

            # Build pipeline benchmark (stage-matrix view)
            try:
                from lakebench.metrics import build_pipeline_benchmark

                pb = build_pipeline_benchmark(
                    run_metrics,
                    datagen_output_gb=_datagen_output_gb,
                    datagen_output_rows=_datagen_output_rows,
                    datagen_output_files=_datagen_output_files,
                )
                # 0 when not measured (an interrupted run), as before for a
                # failed listing.
                pb.total_s3_objects = _total_s3_objects or 0
                run_metrics.pipeline_benchmark = pb
                if is_continuous_mode(pb.pipeline_mode):
                    if pb.sustained_throughput_rps > 0:
                        latency_str = (
                            "/".join(f"{v:.0f}" for v in pb.stage_latency_profile)
                            if pb.stage_latency_profile
                            else "n/a"
                        )
                        from lakebench.metrics.bounds import trickle_note

                        print_info(
                            f"Pipeline Score: {pipeline_score_freshness(pb)} freshness"
                            f" | {pb.sustained_throughput_rps:,.0f} rows/s continuous"
                            f"{trickle_note(run_metrics)}"
                            f" | {latency_str}ms latency (b/s/g)"
                        )
                    if pb.corpus_drained:
                        _frac = pb.window_arrival_fraction
                        print_warning(
                            "The corpus was fully ingested before the window ended"
                            + (
                                f" (data arrived for {_frac:.0%} of it)"
                                if _frac is not None
                                else ""
                            )
                            + ": freshness covers only gold cycles that saw new data, and rows/s "
                            "is taken over the seconds data was arriving. Lower "
                            "max_files_per_trigger or shorten the window so arrival lasts it "
                            "(LB-145)."
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

            if _interrupted is None and _interrupt.late_signal():
                # A signal since the check above (Prometheus, the scores):
                # the record still says so. After the save, it is too late.
                _interrupted = _interrupt.seal(
                    at_stage="results", prior_failure=not pipeline_success
                )
                _exit_interrupted = pipeline_success
                pipeline_success = False
                run_metrics.success = False
                run_metrics.interrupted = _interrupted
            # The exit code follows the verdict of the record as it is
            # saved (the samples below write nothing the verdict reads).
            _ok = apply_save_gate(run_metrics, pipeline_success, print_error)
            _verdict_failed, pipeline_success = pipeline_success and not _ok, _ok
            # The end load sample, then the corpus this run read (corpus id
            # v2), both after an interrupt too: they never raise, the sample is
            # bounded and reads the nodes and the other namespaces' pods, and
            # a corpus cut short records as incomplete. After a lost namespace
            # the sample is still taken, but the corpus is not read: its
            # bucket may already be a redeployment's.
            from lakebench.metrics.corpus_identity import record_corpus_observation

            sample_run_end(run_metrics, cfg)
            if _abort is None:
                record_corpus_observation(run_metrics, cfg)
            else:
                _record_not_observed(run_metrics, _abort["reason"])
            metrics_path = metrics_storage.save_run(run_metrics)
            print_info(f"Metrics saved to {metrics_path}")
            print_info(f"Run ID: {run_id}")
            write_run_report(metrics_storage, run_id)
            # The same front matter the report opens with, read back from
            # the saved record.
            from lakebench.reports.front_matter import print_front_matter

            print_front_matter(run_metrics, console, storage=metrics_storage, run_id=run_id)

        _journal_safe(
            j.end_command,
            success=pipeline_success,
            message=(
                f"interrupted ({_interrupted['signal']} during {_interrupted['at_stage']})"
                if _interrupted is not None
                else (f"stopped: {_abort['reason']}" if _abort is not None else "")
            ),
        )
        _interrupt.restore()
        if _exit_interrupted:
            raise typer.Exit(ExitCode.INTERRUPTED)
        if _abort is not None:
            raise typer.Exit(ExitCode.FAILED)
        if (_deps_check_failed or _verdict_failed) and not _exception_in_flight:
            # The run-end dependency check or the record's verdict failed
            # after the pipeline itself finished: the exit code says so
            # (exit 0 is not a pass).
            raise typer.Exit(ExitCode.FAILED)
