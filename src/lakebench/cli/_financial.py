"""Financial (FinServ-Crime, AML) operator subcommands.

Grouped under ``lakebench financial <verb>`` so the operator-facing
Financial ops surface stays discoverable via ``lakebench financial --help``
and out of the top-level command namespace.

Three verbs shipped in ENG-2C.3:

- ``replay``    -- W8 historical replay of a detection rule (SparkApplication).
- ``reproduce`` -- W10 time-travel reproduction of a specific alert.
- ``score``     -- Compute recall from datagen manifest + gold.alerts.

Each verb loads the config, gets the shared k8s client + SparkJobManager,
ensures the scripts ConfigMap is deployed (idempotent), then submits the
appropriate JobType with CLI --arguments passed through to the Python
script.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from lakebench.cli._helpers import esc, print_error
from lakebench.exit_codes import ExitCode, LakebenchError

logger = logging.getLogger(__name__)

financial_app = typer.Typer(
    name="financial",
    help="Financial (FinServ-Crime, AML) operator actions.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
console = Console()


def _assert_financial_schema(cfg) -> None:
    from lakebench.config.schema import WorkloadSchema

    schema = cfg.architecture.workload.schema_type
    if schema != WorkloadSchema.FINANCIAL:
        raise typer.BadParameter(
            f"lakebench financial subcommands require workload.schema=financial "
            f"(config has schema={schema.value})."
        )


def _load_config(config_path: Path, verb: str = "financial"):
    from lakebench.config import ConfigError, LoadPurpose, load_config

    try:
        cfg = load_config(str(config_path), purpose=LoadPurpose.MUTATE)
    except ConfigError as e:
        # One line, not a traceback.
        print_error(str(e))  # one ERROR line on stderr, like every load error
        raise typer.Exit(ExitCode.USAGE) from None  # config.validation, config.name_required
    _assert_financial_schema(cfg)
    # Every financial subcommand reads or scores the corpus: a protected one
    # is refused before any cluster call (scored only as its registered look).
    from lakebench.aml.look_guard import refuse_if_protected

    refuse_if_protected(cfg, verb)
    return cfg


def _get_job_manager(cfg):
    """Build k8s client + SparkJobManager + ensure scripts ConfigMap is
    up-to-date. Matches the pattern in cli/_run.py so replay/reproduce/score
    use the same script-mount path as bronze_verify/silver_build/gold_finalize.
    """
    from lakebench.engine import get_engine
    from lakebench.k8s import get_k8s_client

    k8s = get_k8s_client(
        context=cfg.platform.kubernetes.context,
        namespace=cfg.get_namespace(),
    )
    from lakebench.cli._helpers import load_deps_handle
    from lakebench.modules.pipeline_engines.spark.scripts_maps import ScriptsMapError

    # Before the scripts or any job: the deployment's verified set.
    deps_handle = load_deps_handle(cfg)
    job_manager = get_engine(cfg, k8s)
    job_manager.deps = deps_handle  # type: ignore[attr-defined]
    try:
        scripts_ok = job_manager.deploy_scripts_configmap()
    except ScriptsMapError as e:
        console.print(f"Spark scripts not deployed: {esc(e)}", style="red")
        raise typer.Exit(ExitCode.FAILED) from None
    if not scripts_ok:
        raise LakebenchError("Failed to deploy Spark scripts ConfigMap")
    return job_manager


def _require_submitted(status) -> None:
    """Exit 1 when the job was not submitted. Waiting on it would find
    nothing (a 30-minute 404 poll) or, if a previous application of the same
    name survived, report that one's result as this run's."""
    from lakebench.modules.pipeline_engines.spark.job import JobState

    if status.state is JobState.FAILED:
        console.print(f"Not submitted: {esc(status.message)}", style="red")
        raise typer.Exit(ExitCode.FAILED)


def _wait_for_sparkapp(namespace: str, name: str, timeout: int = 1800) -> str:
    """Poll a SparkApplication until it reaches a terminal state, return state."""
    from kubernetes import client
    from kubernetes.client.rest import ApiException

    api = client.CustomObjectsApi()
    start = time.time()
    last_state = ""
    while time.time() - start < timeout:
        try:
            obj = api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=name,
            )
        except ApiException as e:
            if e.status == 404:
                time.sleep(5)
                continue
            raise
        state = (obj.get("status", {}) or {}).get("applicationState", {}).get("state", "")
        if state != last_state:
            console.print(f"  [dim]{name}: {state}[/dim]")
            last_state = state
        if state in ("COMPLETED", "FAILED"):
            return state
        time.sleep(10)
    return "TIMEOUT"


@financial_app.command("replay")
def replay(
    config: Annotated[Path, typer.Argument(help="Lakebench config YAML")],
    rule: Annotated[str, typer.Option(help="Rule id, e.g. W2_structuring")],
    depth_months: Annotated[int, typer.Option(help="Snapshot depth in months")] = 60,
    threshold: Annotated[
        float | None, typer.Option(help="Rule-specific threshold override")
    ] = None,
    output_alerts: Annotated[
        str,
        typer.Option(
            help=(
                "Fully-qualified output alerts table (catalog.namespace.table). "
                "Defaults to the config's gold alerts table with an _replay "
                "suffix, so a replay never overwrites the batch run's alerts. "
                "Multiple rules can share the table: replay does DELETE WHERE "
                "rule_id=X before appending, so each rule owns its rows."
            ),
        ),
    ] = "",
    wait: Annotated[bool, typer.Option(help="Wait for job completion")] = True,
) -> None:
    """Rerun a detection rule against a historical Iceberg snapshot (W8)."""
    from lakebench.modules.pipeline_engines.spark.job import JobType

    cfg = _load_config(config, "financial replay")

    # Default target is a SEPARATE table (gold alerts + "_replay"). Replay
    # deletes its rule's rows in the target and writes new ones under a fresh
    # run_id; defaulting to gold.alerts wiped the batch run's rows for that
    # rule, and scoring (scoped to the batch run's run_id) then read that
    # typology as 0% recall. Pass --output-alerts to write elsewhere.
    if not output_alerts:
        catalog = cfg.architecture.query_engine.trino.catalog_name
        output_alerts = f"{catalog}.{cfg.architecture.tables.gold_alerts}_replay"

    args = [
        "--rule",
        rule,
        "--depth-months",
        str(depth_months),
        "--output-alerts",
        output_alerts,
    ]
    if threshold is not None:
        args += ["--threshold", str(threshold)]

    console.print(f"[bold]lakebench financial replay[/bold] rule={rule} depth={depth_months}mo")
    job_manager = _get_job_manager(cfg)
    status = job_manager.submit_job(JobType.REPLAY_FINANCIAL, arguments=args)
    console.print(f"  submitted: {status.message}")
    _require_submitted(status)

    if wait:
        result = _wait_for_sparkapp(cfg.get_namespace(), "lakebench-replay-financial")
        console.print(f"[bold]replay result:[/bold] {result}")
        if result != "COMPLETED":
            raise typer.Exit(ExitCode.FAILED)


#: Characters an alert id may hold (uuids and their prefixes): the id goes
#: into the job's SQL and an S3 key.
_ALERT_ID_ALLOWED = set("0123456789abcdefABCDEF-_")

#: Each determined reproduce outcome and its exit path.
REPRODUCE_PATHS = {
    "not_found": "financial.reproduce.not_found",
    "snapshot_gone": "financial.reproduce.snapshot_gone",
    "mismatch": "financial.reproduce.mismatch",
    "rule_skipped": "financial.reproduce.mismatch",
}


def _s3(cfg):
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


def reproduce_record(cfg, run_id: str | None, metrics_dir: Path) -> tuple[dict | None, str]:
    """The run record a reproduction reads, and where it was looked for:
    ``--run``'s record, else the latest AML batch run record of this
    deployment in *metrics_dir*. None when there is none (the record may be
    on another host: nothing is guessed from the cluster)."""
    import json

    def load(path: Path) -> dict | None:
        try:
            data = json.loads(path.read_text())
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def aml_batch_run(rec: dict | None) -> bool:
        if rec is None or rec.get("deployment_name") != cfg.name:
            return False
        if (rec.get("record_kind") or "run") != "run":
            return False
        exp = rec.get("experiment") or {}
        workload = (exp.get("workload") or {}).get("name") or (
            rec.get("config_snapshot") or {}
        ).get("schema_type")
        return workload == "financial" and exp.get("mode", "batch") == "batch"

    if run_id:
        path = metrics_dir / f"run-{run_id}" / "metrics.json"
        rec = load(path)
        return (rec if aml_batch_run(rec) else None), str(path)
    best: tuple[str, dict] | None = None
    for path in sorted(metrics_dir.glob("run-*/metrics.json")):
        rec = load(path)
        if not aml_batch_run(rec):
            continue
        assert rec is not None
        start = str(rec.get("start_time") or "")
        if best is None or start > best[0]:
            best = (start, rec)
    return (best[1] if best else None), str(metrics_dir)


def snapshots_problem(record: dict) -> tuple[str | None, list, str]:
    """``(problem, read_snapshots, scoring run id)`` of a run record: why it
    cannot drive a reproduction (None when it can). The scoring run id is the
    one gold-finalize stamped on its alerts (a cycle's, e.g. ``<run>-c1``)."""
    from lakebench.metrics.read_snapshots import usable

    run = record.get("run_id")
    scoring = record.get("financial_scoring")
    if not isinstance(scoring, dict) or not scoring.get("run_id"):
        return (
            f"run {run} was not scored (a `run --stage` subset, or its scoring did not "
            "complete), so it recorded no read snapshots",
            [],
            "",
        )
    snaps = scoring.get("read_snapshots")
    if snaps is None:
        return f"run {run} recorded no read snapshots (it predates 1.7)", [], ""
    problem = usable(snaps)
    if problem:
        return f"run {run}: {problem}", [], ""
    return None, snaps, str(scoring["run_id"])


@financial_app.command("reproduce")
def reproduce(
    config: Annotated[Path, typer.Argument(help="Lakebench config YAML")],
    alert_id: Annotated[str, typer.Option(help="Alert id (gold.alerts.alert_id) to reproduce")],
    run: Annotated[
        str | None,
        typer.Option(
            "--run",
            help="Run id whose record holds the snapshots gold read; default: the latest "
            "AML batch run of this deployment",
        ),
    ] = None,
    wait: Annotated[bool, typer.Option(help="Wait for the result")] = True,
) -> None:
    """Reproduce one batch alert from the snapshots its run's gold read.

    Reads the run record's ``financial_scoring.read_snapshots`` (before any
    cluster call), then runs the alert's rule on those snapshots (or on
    content-equal current tables when they expired) with gold's parameters,
    and compares the alert. Exit 0 when reproduced; 1 when not found or not
    reproduced; 4 when the snapshots are gone (or the run recorded none).
    """
    from lakebench.exit_codes import UsageError, path_code
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.modules.pipeline_engines.spark.job import JobType

    if not alert_id or len(alert_id) > 128 or set(alert_id) - _ALERT_ID_ALLOWED:
        raise UsageError(
            "--alert-id must be 1 to 128 characters of hex digits, dashes and underscores",
            path="click.usage",
        )
    if run is not None and not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", run):
        raise UsageError("--run must be a run id (letters, digits, - and _)", path="click.usage")
    cfg = _load_config(config, "financial reproduce")
    record, where = reproduce_record(cfg, run, MetricsStorage().metrics_dir)
    if record is None:
        raise UsageError(
            f"No AML batch run record of {cfg.name} to reproduce from",
            where=where,
            next="pass --run RUN_ID with a record of this deployment on this host",
            path="financial.reproduce.no_record",
        )
    # A run on a protected corpus is read only by its registered look.
    from lakebench.aml.look_guard import refuse_protected_records

    refuse_protected_records([(str(record.get("run_id")), record)], "financial reproduce")
    problem, snapshots, run_id = snapshots_problem(record)
    if problem:
        raise LakebenchError(
            f"Cannot reproduce: {problem}",
            next="reproduce an alert of a scored 1.7 AML batch run (--run RUN_ID)",
            path="financial.reproduce.snapshot_gone",
            code=path_code("financial.reproduce.snapshot_gone"),
        )

    gold = cfg.platform.storage.s3.buckets.gold
    prefix = f"scoring/reproduce/{alert_id}"
    console.print(
        f"[bold]lakebench financial reproduce[/bold] alert_id={esc(alert_id)} run={esc(run_id)}"
    )
    job_manager = _get_job_manager(cfg)
    import json
    import uuid

    client = _s3(cfg)
    nonce = uuid.uuid4().hex
    client.raw_client.put_object(
        Bucket=gold,
        Key=f"{prefix}/input.json",
        Body=json.dumps({"run_id": run_id, "nonce": nonce, "read_snapshots": snapshots}).encode(),
    )
    try:  # a result left by an earlier reproduction of this alert is not this one's
        client.raw_client.delete_object(Bucket=gold, Key=f"{prefix}/result.json")
    except Exception:  # noqa: BLE001 -- absent is fine; a stale one is caught below
        pass
    status = job_manager.submit_job(
        JobType.REPRODUCE_FINANCIAL,
        arguments=[
            "--alert-id",
            alert_id,
            "--input",
            f"s3a://{gold}/{prefix}/input.json",
            "--output",
            f"s3a://{gold}/{prefix}/result.json",
        ],
    )
    console.print(f"  submitted: {esc(status.message)}")
    _require_submitted(status)
    if not wait:
        console.print(f"  result: s3://{esc(gold)}/{esc(prefix)}/result.json")
        return

    state = _wait_for_sparkapp(cfg.get_namespace(), "lakebench-reproduce-financial")
    result = None
    try:
        body = client.raw_client.get_object(Bucket=gold, Key=f"{prefix}/result.json")["Body"]
        result = json.loads(body.read())
    except Exception as e:  # noqa: BLE001 -- no result: a crash
        print_error(f"reproduce wrote no result ({state}): {e}")
        raise typer.Exit(ExitCode.FAILED) from None
    if not isinstance(result, dict) or result.get("nonce") != nonce:
        print_error(f"reproduce wrote no result for this reproduction ({state})")
        raise typer.Exit(ExitCode.FAILED)
    outcome = result.get("outcome")
    console.print(
        f"[bold]reproduce:[/bold] {esc(outcome)} rule={esc(result.get('rule_id'))} "
        f"basis={esc(result.get('basis'))} matched={esc(result.get('matched'))} "
        f"diff={esc(result.get('diff_size'))}"
        + (f" ({esc(result.get('reason'))})" if result.get("reason") else "")
        + (
            f" [not pinned: {esc(', '.join(result['not_pinned']))}]"
            if outcome == "reproduced" and result.get("not_pinned")
            else ""
        )
    )
    if outcome == "reproduced":
        return
    path = REPRODUCE_PATHS.get(str(outcome))
    if path is None:
        raise typer.Exit(ExitCode.FAILED)
    raise typer.Exit(path_code(path))


@financial_app.command("score")
def score(
    config: Annotated[Path, typer.Argument(help="Lakebench config YAML")],
    manifest: Annotated[str, typer.Option(help="S3 URI to datagen manifest.parquet")],
    output: Annotated[str, typer.Option(help="S3 URI for recall.parquet output")],
    wait: Annotated[bool, typer.Option(help="Wait for job completion")] = True,
) -> None:
    """Compute recall from datagen manifest and gold.alerts."""
    from lakebench.modules.pipeline_engines.spark.job import JobType

    cfg = _load_config(config, "financial score")
    console.print("[bold]lakebench financial score[/bold]")
    job_manager = _get_job_manager(cfg)
    status = job_manager.submit_job(
        JobType.SCORE_FINANCIAL,
        arguments=["--manifest", manifest, "--output", output],
    )
    console.print(f"  submitted: {status.message}")
    _require_submitted(status)

    if wait:
        result = _wait_for_sparkapp(cfg.get_namespace(), "lakebench-score-financial")
        console.print(f"[bold]score result:[/bold] {result}")
        if result != "COMPLETED":
            raise typer.Exit(ExitCode.FAILED)


@financial_app.command("reference-score")
def reference_score(
    config: Annotated[Path, typer.Argument(help="Lakebench config YAML")],
    manifest: Annotated[str, typer.Option(help="S3 URI to datagen manifest.parquet")],
    output_prefix: Annotated[
        str,
        typer.Option(help="S3 URI prefix for leakage_report.parquet + reference_metrics.parquet"),
    ],
    leakage_threshold: Annotated[
        float,
        typer.Option(help="Min baseline/typology ratio in a structuring band to pass the gate"),
    ] = 0.10,
    wait: Annotated[bool, typer.Option(help="Wait for job completion")] = True,
) -> None:
    """Run the reference detector + leakage gate over silver + manifest.

    This is the "distribution checks do not prove semantics" gate: it measures
    whether a canonical detector (a GBT trained on non-leaky features) can
    recover each typology, and whether any planted signal leaks through the raw
    amount band. A relative-threshold rule rewrite (e.g. the W4/W8 precision
    work, LB-130) is validated against this envelope before it ships -- a rule
    whose precision/recall diverges sharply from the reference is scoring
    against label knowledge it should not have. The job installs scikit-learn
    and its pinned dependencies for the GBT half in an init container on each
    run, from the deployment's dependency set (each wheel hash-checked); if
    that install fails after its retries, the driver does not start and the
    job fails. The dependency set's pinset is printed with the submission.
    """
    from lakebench.modules.pipeline_engines.spark.job import JobType

    cfg = _load_config(config, "financial reference-score")
    console.print("[bold]lakebench financial reference-score[/bold]")
    job_manager = _get_job_manager(cfg)
    deps = job_manager.deps
    console.print(
        f"  dependency set {esc(deps.pinset_sha256)} (request {esc(deps.request_sha256)}); "
        "reference wheels: "
        + ", ".join(
            f"{esc(e['file'])}@{esc(e['sha256'][:12])}"
            for e in deps.manifest["groups"].get("py-reference", [])
        )
    )
    status = job_manager.submit_job(
        JobType.SCORE_FINANCIAL_REFERENCE,
        arguments=[
            "--manifest",
            manifest,
            "--output-prefix",
            output_prefix,
            "--leakage-threshold",
            str(leakage_threshold),
        ],
    )
    console.print(f"  submitted: {status.message}")
    _require_submitted(status)

    if wait:
        result = _wait_for_sparkapp(cfg.get_namespace(), "lakebench-score-financial-reference")
        console.print(f"[bold]reference-score result:[/bold] {result}")
        if result != "COMPLETED":
            raise typer.Exit(ExitCode.FAILED)
