"""AML steps after the detection stages: recall scoring for batch and
continuous runs, the continuous gold-refresh drain, and the time-travel
reads of the snapshots the continuous ticks read.

The drain: the CLI writes a marker object under the gold-refresh checkpoint
whose body is the run id; the driver finishes its current tick, logs
``Drain complete: last completed cycle N run=<run>`` and idles until the
application is deleted. The CLI waits for that line, keeps the full driver
log, and scores recall over what tick N saw (``score_financial.py`` covered
mode), so ``gold.alerts`` is never half rewritten when it is scored.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from lakebench.cli._helpers import console, esc, print_info, print_success, print_warning

GOLD_REFRESH_APP = "lakebench-gold-refresh"
#: Seconds the window end waits for the drain. Fixed (the design derived it
#: from the window's longest tick, capped at 1800): the wait ends as soon as
#: the driver reports the drain, so the budget only bounds a drain that never
#: comes. A tick longer than this fails the run, with the reason.
DRAIN_BUDGET_S = 1800
#: `lakebench stop`'s budget: a stop should not wait as long as a run end.
STOP_DRAIN_BUDGET_S = 300
DRAIN_POLL_S = 10
#: Log lines read per poll. The driver repeats the drain line after
#: spark.stop() and then once a minute, so it is always near the tail.
_DRAIN_TAIL_LINES = 2000
#: (connect, read) seconds for one log or status read: a half-open socket
#: must not outlast the budget.
_READ_TIMEOUT = (10, 60)
_FULL_READ_TIMEOUT = (10, 300)
#: Seconds between "still draining" lines.
_HEARTBEAT_S = 60

#: The covered-mode options of score_financial.py, in tick-record terms.
COVERED_OPTIONS = (
    ("txns", "pinned_txns"),
    ("entities", "pinned_entities"),
    ("accounts", "pinned_accounts"),
    ("versions", "pinned_versions"),
    ("alerts", "committed_alerts"),
    ("status", "committed_status"),
)


@dataclass
class DrainResult:
    """``state``: ``drained`` (the driver reported the drain), ``timeout``,
    ``no_marker`` (the marker could not be written; nothing was asked), or
    ``driver_gone`` (the application was deleted first). ``last_cycle`` 0
    means the driver had restarted and found the marker before any tick."""

    state: str
    waited_s: float = 0.0
    reason: str = ""
    logs: str | None = None
    last_cycle: int | None = None

    def record(self) -> dict[str, Any]:
        out: dict[str, Any] = {"state": self.state, "waited_s": round(self.waited_s, 1)}
        if self.last_cycle is not None:
            out["last_completed_cycle"] = self.last_cycle
        if self.reason:
            out["reason"] = self.reason
        return out


def _s3_client(cfg):
    from lakebench.s3 import S3Client

    s3 = cfg.platform.storage.s3
    return S3Client(
        endpoint=s3.endpoint,
        access_key=s3.access_key,
        secret_key=s3.secret_key,
        region=s3.region,
        path_style=s3.path_style,
        ca_cert=s3.ca_cert,
        verify_ssl=s3.verify_ssl,
    )


def write_stop_marker(cfg, run_id: str) -> tuple[str, str]:
    """Write the drain marker for *run_id*; returns (bucket, key). Raises on
    any failure."""
    from lakebench.modules.pipeline_engines.spark.job import gold_refresh_stop_marker

    bucket, key = gold_refresh_stop_marker(cfg)
    _s3_client(cfg).raw_client.put_object(Bucket=bucket, Key=key, Body=run_id.encode("utf-8"))
    return bucket, key


def driver_log_reader(namespace: str) -> Callable[..., str | None]:
    """Read the gold-refresh driver's log (its last *tail* lines, or all of
    it for None), with bounded reads. A running driver pod is preferred over
    one that is terminating. None when there is no driver pod."""
    from kubernetes import client as k8s_client

    core = k8s_client.CoreV1Api()

    def read(tail: int | None, timeout: float | None = None) -> str | None:
        pods = core.list_namespaced_pod(
            namespace,
            label_selector=f"spark-role=driver,sparkoperator.k8s.io/app-name={GOLD_REFRESH_APP}",
            _request_timeout=_READ_TIMEOUT,
        ).items
        if not pods:
            return None
        pods = sorted(pods, key=lambda p: getattr(p.status, "phase", None) != "Running")
        kwargs: dict[str, Any] = {
            "container": "spark-kubernetes-driver",
            "_request_timeout": (
                _READ_TIMEOUT
                if tail is not None
                else (10, min(_FULL_READ_TIMEOUT[1], timeout or _FULL_READ_TIMEOUT[1]))
            ),
        }
        if tail is not None:
            kwargs["tail_lines"] = tail
        return core.read_namespaced_pod_log(pods[0].metadata.name, namespace, **kwargs)

    return read


def app_state_reader(namespace: str) -> Callable[[], str | None]:
    """The gold-refresh SparkApplication's state, or None when it is gone."""
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    custom = k8s_client.CustomObjectsApi()

    def read() -> str | None:
        try:
            app = custom.get_namespaced_custom_object(
                "sparkoperator.k8s.io",
                "v1beta2",
                namespace,
                "sparkapplications",
                GOLD_REFRESH_APP,
                _request_timeout=_READ_TIMEOUT,
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise
        return str(((app or {}).get("status") or {}).get("applicationState", {}).get("state") or "")

    return read


def request_drain(
    cfg,
    k8s,
    budget_s: float,
    *,
    run_id: str,
    read_log: Callable[..., str | None] | None = None,
    app_state: Callable[[], str | None] | None = None,
    before_write: Callable[[], None] | None = None,
    poll_s: float = DRAIN_POLL_S,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> DrainResult:
    """Ask the gold-refresh driver of *run_id* to finish its tick and stop,
    and wait up to *budget_s* for it to say so.

    ``before_write`` runs before the marker is written (the run's namespace
    check); what it raises propagates. A failed marker write returns
    ``no_marker`` without waiting. The log is polled by its tail; once the
    drain line is there the full log is read once and returned. A failed
    log or status read counts as "not yet" (every read is bounded), so one
    API error never ends the wait early or outlasts the budget. Only an
    application that is gone ends it as ``driver_gone``: under the
    streams' ``Always`` restart policy a failed driver is restarted, finds
    the marker and drains at start.
    """
    from lakebench.metrics.tick_records import drain_cycle

    namespace = cfg.get_namespace()
    if read_log is None:
        read_log = driver_log_reader(namespace)
    if app_state is None:
        app_state = app_state_reader(namespace)

    if before_write is not None:
        before_write()
    try:
        write_stop_marker(cfg, run_id)
    except Exception as e:  # noqa: BLE001 -- recorded; the caller stops anyway
        return DrainResult("no_marker", reason=f"drain marker not written: {e}")

    start = clock()
    beat = start
    warned: set[str] = set()

    def safe(what: str, fn, *args):
        try:
            return fn(*args)
        except Exception as e:  # noqa: BLE001 -- "not yet"; said once
            if what not in warned:
                warned.add(what)
                print_warning(f"gold-refresh {what} failed during the drain (retrying): {e}")
            return None

    tail = None
    while True:
        tail = safe("log read", read_log, _DRAIN_TAIL_LINES)
        if drain_cycle(tail, run_id) is not None:
            # Bounded by what is left of the budget, so a hung read cannot
            # outlast it.
            left = max(10.0, budget_s - (clock() - start))
            full = safe("full log read", lambda left=left: read_log(None, timeout=left))
            n = drain_cycle(full, run_id)
            if n is not None:
                return DrainResult("drained", clock() - start, logs=full, last_cycle=n)
        waited = clock() - start
        if waited >= budget_s:
            return DrainResult(
                "timeout", waited, reason=f"no drain line within {budget_s:.0f}s", logs=tail
            )
        # (state,) when the read worked; None when it failed.
        status = safe("status read", lambda: (app_state(),))
        if status is not None and status[0] is None:
            return DrainResult(
                "driver_gone",
                waited,
                reason="the gold-refresh application was deleted before the drain completed",
                logs=tail,
            )
        if clock() - beat >= _HEARTBEAT_S:
            beat = clock()
            print_info(f"still draining gold-refresh ({waited:.0f}s of {budget_s:.0f}s)...")
        sleep(max(0.0, min(poll_s, budget_s - waited)))


def scoring_count_line(summary: dict) -> str:
    """'6 of 15 typologies scored; 8 no rule, 1 rule skipped' from a
    recall.json summary: never every manifest typology as scored. A covered
    summary says how many instances its last tick covered."""
    if summary.get("mode") == "covered":
        if summary.get("status") != "scored":
            return f"not scored: {summary.get('reason') or 'no reason given'}"
        cov = summary.get("covered") or {}
        return (
            f"covered mode: {cov.get('covered_instances', 0):,} of "
            f"{cov.get('corpus_instances', 0):,} instances covered by the last tick"
        )
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


def run_financial_scoring(
    cfg,
    run_id,
    job_manager,
    monitor,
    timeout,
    interrupt=None,
    covered: dict | None = None,
    read_snapshots: list | None = None,
):
    """Score recall against the datagen manifest and return the
    recall.json summary, or None when scoring did not complete.

    Batch: after gold-finalize, over the whole run. Continuous:
    *covered* is the drained run's last completed tick record, and the
    scorer runs in covered mode over exactly the snapshots that tick read.
    Best-effort: a scoring failure never fails the pipeline (the pipeline
    result is still valid), it just leaves the record without recall.
    *read_snapshots* (batch) are the silver snapshots gold-finalize read; the
    scorer fingerprints them for financial reproduce
    (``financial_scoring.read_snapshots``).
    """
    # Whole body is best-effort: NOTHING here (imports, config access, submit,
    # wait, S3 read) may propagate and fail a pipeline that already reported
    # success. One outer try guarantees that.
    try:
        import json as _json

        from lakebench.spark.job import JobState, JobType

        s3 = cfg.platform.storage.s3
        # Manifest URI mirrors bronze_verify_financial:
        # {bronze}/{prefix}/manifest/manifest.parquet.
        from lakebench.deploy.datagen import bronze_datagen_prefix

        prefix = bronze_datagen_prefix(cfg).rstrip("/")
        # Glob over every cycle's manifest (manifest.parquet, manifest-cNNN.parquet).
        manifest_uri = f"s3a://{s3.buckets.bronze}/{prefix}/manifest/manifest*.parquet"
        json_key = f"scoring/{run_id}/recall.json"
        output_uri = f"s3a://{s3.buckets.gold}/scoring/{run_id}/recall.parquet"
        # Derive the SparkApplication name from the enum rather than a literal
        # so it can never drift from submit_job's f"lakebench-{value}".
        app_name = f"lakebench-{JobType.SCORE_FINANCIAL.value}"
        arguments = ["--manifest", manifest_uri, "--output", output_uri]
        if covered is not None:
            for option, key in COVERED_OPTIONS:
                arguments += [f"--covered-{option}-snapshot", str(covered[key])]
        elif read_snapshots:
            from lakebench.metrics.read_snapshots import score_arguments

            arguments += score_arguments(read_snapshots)

        console.print()
        console.print("[bold]Stage: financial score[/bold]")
        if covered is not None:
            print_info(
                "Scoring recall over what the last completed tick saw "
                f"(cycle {covered.get('cycle')})..."
            )
        else:
            print_info("Scoring recall/precision against the datagen manifest...")

        if interrupt is not None:
            interrupt.creating("SparkApplication", app_name)
        status = job_manager.submit_job(
            JobType.SCORE_FINANCIAL,
            arguments=arguments,
            # The score refuses a gold.detection_status another run wrote; it
            # needs this run's id for that (job.py exports one only with
            # observability on).
            cycle_env={"LB_RUN_ID": run_id},
        )
        if interrupt is not None:
            interrupt.submitted(status)
        if status.state == JobState.FAILED:
            print_warning(f"Could not submit score job: {status.message}")
            return None
        result = monitor.wait_for_completion(
            app_name,
            timeout_seconds=timeout,
            poll_interval=15,
        )
        if interrupt is not None and result.success:
            interrupt.finished("SparkApplication", app_name)
        if not result.success:
            print_warning(f"Financial scoring did not complete: {result.message}")
            # Surface the score driver's own error -- scoring is best-effort so
            # its failure is easy to miss, and without the driver tail the only
            # signal is a generic "driver container failed".
            if getattr(result, "driver_logs", None):
                console.print("[dim]Score driver logs (last 25 lines):[/dim]")
                for line in result.driver_logs.split("\n")[-25:]:
                    console.print(f"  {esc(line)}")
            return None

        # Read the recall.json sidecar (boto3 only -- the CLI has no
        # pandas/pyarrow to read recall.parquet).
        client = _s3_client(cfg)
        body = client.raw_client.get_object(Bucket=s3.buckets.gold, Key=json_key)["Body"].read()
        summary = _json.loads(body)
        if summary.get("status") == "not_scored":
            print_warning(f"Financial scoring: {scoring_count_line(summary)}")
        else:
            print_success(f"Financial scoring complete ({scoring_count_line(summary)})")
        return summary
    except Exception as e:  # noqa: BLE001 -- scoring is best-effort enrichment
        print_warning(f"Financial scoring failed ({e}); scorecard will omit recall.")
        return None


def not_scored(reason: str) -> dict[str, Any]:
    """A continuous ``financial_scoring`` that holds no score, and why."""
    return {"mode": "covered", "status": "not_scored", "reason": reason}


def continuous_scoring(
    cfg,
    run_id: str,
    drain: DrainResult | None,
    tick: dict | None,
    tick_reason: str,
    job_manager,
    monitor,
    timeout,
    *,
    interrupt=None,
    run_failed: bool = False,
) -> dict[str, Any]:
    """``financial_scoring`` for a continuous AML run: the covered score of
    the drained run's last completed tick, or ``not_scored`` with the reason.
    Never scores a run whose drain did not complete or whose last tick record
    lacks a pin."""
    if drain is None:
        return not_scored("the gold-refresh drain did not run")
    if drain.state != "drained":
        return not_scored(f"gold drain {drain.state}: {drain.reason}")
    if run_failed:
        return not_scored("the run failed its gates")
    if tick is None:
        return not_scored(tick_reason)
    summary = run_financial_scoring(
        cfg, run_id, job_manager, monitor, timeout, interrupt=interrupt, covered=tick
    )
    if summary is None:
        return not_scored("the score job did not complete")
    if summary.get("mode") != "covered":
        # A scorer that ignored the covered options must not pass for one.
        return not_scored("the score job did not run in covered mode")
    summary["tick"] = {
        "cycle": tick.get("cycle"),
        "pinned_at": tick.get("pinned_at"),
        "completed_at": tick.get("completed_at"),
        # The scored tick can begin after the window closed: the drain waits
        # for the tick in progress, and the window's bucket listing runs first.
        "pinned_after_window_end_s": tick.get("pinned_after_window_end_s"),
    }
    return summary


def stop_drain(cfg, k8s, custom_api: Any = None) -> DrainResult | None:
    """`lakebench stop`'s drain of a financial continuous deployment: None
    when there is nothing to drain (not financial, no running gold-refresh,
    or no run id on it); else the drain's result."""
    from lakebench.cli._cluster_ops import API_TIMEOUT

    if cfg.architecture.workload.schema_type.value != "financial":
        return None
    if custom_api is None:
        from kubernetes import client as k8s_client

        custom_api = k8s_client.CustomObjectsApi()
    try:
        app = custom_api.get_namespaced_custom_object(
            "sparkoperator.k8s.io",
            "v1beta2",
            cfg.get_namespace(),
            "sparkapplications",
            GOLD_REFRESH_APP,
            _request_timeout=API_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001 -- a 404 means there is no stream
        if getattr(e, "status", None) == 404:
            return None
        raise
    state = (((app or {}).get("status") or {}).get("applicationState") or {}).get("state")
    if state != "RUNNING":
        return None
    env = (((app or {}).get("spec") or {}).get("driver") or {}).get("env") or []
    run_id = next(
        (str(e.get("value") or "") for e in env if e.get("name") == "LB_RUN_ID"), ""
    ).strip()
    if not run_id:
        return None
    print_info("Draining gold-refresh: finishing its current tick before the stop...")
    return request_drain(cfg, k8s, STOP_DRAIN_BUDGET_S, run_id=run_id)


# -- time-travel reads (after the window) -------------------------------------

#: Seconds the job keeps back from the per-job budget, so it writes its
#: partial result (``incomplete``) before the CLI stops waiting.
_TT_BUDGET_MARGIN_S = 120


def run_time_travel(
    cfg,
    run_id: str,
    continuous: dict | None,
    drain: DrainResult | None,
    job_manager,
    monitor,
    timeout,
    *,
    interrupt=None,
) -> dict[str, Any] | None:
    """Re-read every transactions snapshot the drained run's ticks recorded
    (``TIME_TRAVEL_FINANCIAL``) and record ``continuous.time_travel``.

    Runs after the covered scorer, with the streams stopped: nothing it
    does is inside a measured interval, and it writes only under the gold
    ``scoring/<run_id>/`` prefix. Never raises and never fails the run; a
    job that cannot run reads ``not_run``. Returns the record, or None
    when the run has no continuous block."""
    from lakebench.metrics import time_travel as tt_mod

    if continuous is None:
        return None
    policy = tt_mod.policy(
        continuous.get("retention"), cfg.architecture.pipeline.sustained.retention_threshold
    )
    try:
        if drain is None or drain.state != "drained":
            state = "did not run" if drain is None else drain.state
            return tt_mod.not_run(
                continuous, f"the gold-refresh drain {state}: no tick records were read", policy
            )
        records = list((continuous.get("time_travel") or {}).get("ticks") or [])
        if not records:
            return tt_mod.not_run(continuous, "zero recorded snapshots", policy, verdict="fail")
        import json as _json
        import uuid

        from lakebench.spark.job import JobState, JobType

        gold = cfg.platform.storage.s3.buckets.gold
        prefix = f"scoring/{run_id}"
        nonce = uuid.uuid4().hex
        client = _s3_client(cfg)
        fields = (
            "start",
            "cycle",
            "table",
            "snapshot",
            "committed_at",
            "total_records",
            "pos_deletes",
            "eq_deletes",
            "count_source",
        )
        client.raw_client.put_object(
            Bucket=gold,
            Key=f"{prefix}/tt_input.json",
            Body=_json.dumps(
                {
                    "run_id": run_id,
                    "nonce": nonce,
                    "records": [{k: r.get(k) for k in fields} for r in records],
                }
            ).encode("utf-8"),
        )
        app_name = f"lakebench-{JobType.TIME_TRAVEL_FINANCIAL.value}"
        budget = max(60, int(timeout) - _TT_BUDGET_MARGIN_S)
        console.print()
        console.print("[bold]Stage: time-travel reads[/bold]")
        print_info(
            f"Re-reading {len(records)} recorded transactions snapshot(s) after the window..."
        )
        if interrupt is not None:
            interrupt.creating("SparkApplication", app_name)
        status = job_manager.submit_job(
            JobType.TIME_TRAVEL_FINANCIAL,
            arguments=[
                "--input",
                f"s3a://{gold}/{prefix}/tt_input.json",
                "--hashes",
                f"s3a://{gold}/{prefix}/tt_hashes.json",
                "--output",
                f"s3a://{gold}/{prefix}/time_travel.json",
                "--budget-s",
                str(budget),
            ],
            cycle_env={"LB_RUN_ID": run_id},
        )
        if interrupt is not None:
            interrupt.submitted(status)
        if status.state == JobState.FAILED:
            return tt_mod.not_run(
                continuous, f"the job was not submitted: {status.message}", policy
            )
        result = monitor.wait_for_completion(app_name, timeout_seconds=timeout, poll_interval=15)
        if interrupt is not None and result.success:
            interrupt.finished("SparkApplication", app_name)
        if not result.success:
            return tt_mod.not_run(continuous, f"the job did not complete: {result.message}", policy)
        body = client.raw_client.get_object(Bucket=gold, Key=f"{prefix}/time_travel.json")
        out = _json.loads(body["Body"].read())
        if not isinstance(out, dict) or out.get("nonce") != nonce:
            return tt_mod.not_run(continuous, "time_travel.json is not this submission's", policy)
        tt = tt_mod.merge(continuous, out, policy)
        (print_success if tt["verdict"] == "pass" else print_warning)(tt_mod.line(tt))
        return tt
    except Exception as e:  # noqa: BLE001 -- a measurement; never fails the run
        print_warning(f"Time-travel reads failed ({e}); recorded as not run.")
        return tt_mod.not_run(continuous, f"the time-travel step failed: {e}", policy)


def settle_time_travel(cfg, run: Any, reason: str) -> None:
    """A continuous AML run that ended before the time-travel step reads
    ``not_run`` with *reason*, so the record never lacks a verdict."""
    if run is None or cfg.architecture.workload.schema_type.value != "financial":
        return
    continuous = getattr(run, "continuous", None)
    if continuous is None:
        return
    if (continuous.get("time_travel") or {}).get("verdict") is None:
        from lakebench.metrics import time_travel as tt_mod

        tt_mod.not_run(
            continuous,
            reason,
            tt_mod.policy(
                continuous.get("retention"),
                cfg.architecture.pipeline.sustained.retention_threshold,
            ),
        )
