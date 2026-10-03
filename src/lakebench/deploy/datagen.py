"""Data generation deployment for Lakebench."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import yaml

from lakebench.config.datagen_seed import config_perturbation, config_seed
from lakebench.exit_codes import REFUSAL_DETAIL, ExitCode

from .engine import DeploymentResult, DeploymentStatus

logger = logging.getLogger(__name__)


def _pod_owned_by_job(pod, job_uid: str) -> bool:
    """True when ``pod`` is controlled by the Job with ``job_uid`` (LB-200).

    Scopes datagen progress to the current job incarnation so a prior
    generation's stale pod (same name/label, still Terminating) cannot be
    counted. Kubernetes stamps each Job pod with an owner reference to its
    Job; match on that uid.
    """
    for ref in getattr(pod.metadata, "owner_references", None) or []:
        if getattr(ref, "kind", None) == "Job" and getattr(ref, "uid", None) == job_uid:
            return True
    return False


if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig

    from .engine import DeploymentEngine


def bronze_datagen_prefix(config: LakebenchConfig) -> str:
    """Return the S3 prefix under bronze where datagen writes.

    The one prefix function: the deployer, the CLI's ``--regenerate`` guard,
    the continuous reset and the financial Spark jobs
    (``LB_FINANCIAL_BRONZE_PREFIX``) all read it. The layout is fixed per
    workload, because the Spark stages read a fixed path: ``pacs008`` for
    financial (AML), ``customer/interactions`` otherwise.
    """
    if config.architecture.workload.schema_type.value == "financial":
        return "pacs008"
    return "customer/interactions"


def clear_bronze_data_clock(namespace: str) -> None:
    """Set ``lakebench-silver-state``'s ``bronze_data_clock`` to "".

    The clock is bronze-verify's max(event_ts) of the bronze data; once that
    data is gone or replaced (destroy emptied bronze, a fresh generate or
    regenerate) it is stale, and silver's LB_DATA_CLOCK ladder must fall
    through instead. Conditional on the resourceVersion read. A missing
    ConfigMap or namespace raises a 404 ApiException for the caller to
    ignore; the rebuild-epoch counters are never touched.
    """
    from kubernetes import client as k8s_client

    from lakebench.deploy.engine import DeploymentEngine

    core_v1 = k8s_client.CoreV1Api()
    cm = core_v1.read_namespaced_config_map(DeploymentEngine.SILVER_STATE_CONFIGMAP, namespace)
    key = DeploymentEngine._SILVER_STATE_CLOCK_KEY  # noqa: SLF001
    if not (cm.data or {}).get(key):
        return
    core_v1.patch_namespaced_config_map(
        DeploymentEngine.SILVER_STATE_CONFIGMAP,
        namespace,
        {"data": {key: ""}, "metadata": {"resourceVersion": cm.metadata.resource_version}},
    )


def _clear_clock_best_effort(cfg: Any) -> None:
    try:
        clear_bronze_data_clock(cfg.get_namespace())
    except Exception as e:  # noqa: BLE001
        if getattr(e, "status", None) != 404:
            logger.warning("could not clear the bronze data clock: %s", e)


def parse_size_to_bytes(size_str: str) -> int:
    """Bytes in a size string such as "64MB" or "1TB" (bytes without a suffix)."""
    size_str = size_str.strip().upper()
    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }

    for suffix, multiplier in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if size_str.endswith(suffix):
            value = float(size_str[: -len(suffix)])
            return int(value * multiplier)

    # Assume bytes if no suffix
    return int(size_str)


class DatagenRefused(RuntimeError):
    """Datagen did not start because starting it could corrupt the corpus.

    ``exit_path`` names the ``lakebench.exit_codes`` path a caller reports
    (``deploy`` puts it in ``details[REFUSAL_DETAIL]``); None means the
    state could not be checked, an ordinary failure.
    """

    exit_path: str | None = None


class StaleBronzeRefused(DatagenRefused):
    """Datagen would write over objects in a bronze bucket this deployment may not empty.

    Raised by ``DatagenDeployer``'s cycle-0 path when the CLI gate was
    skipped: stale ``part-*`` files there would be read by silver as
    this run's data and over-counted.
    """

    exit_path = "run.bronze_nonempty"


class DatagenPodsStillRunning(DatagenRefused):
    """An earlier datagen Job's pods were still running when the wait ran out."""

    exit_path = "datagen.pods_live"


class DatagenPodsUnknown(DatagenRefused):
    """The datagen pods could not be listed, so a fresh generate cannot start."""


#: Label every datagen pod carries (templates/datagen/job.yaml.j2).
DATAGEN_POD_SELECTOR = "app=lakebench-datagen"

#: How long a fresh generate waits for an earlier datagen Job's pods to stop
#: after the Job is deleted. The pods get SIGTERM and the default 30 s grace
#: period; this leaves room for a slow kubelet.
DATAGEN_POD_STOP_WAIT_S = 300.0
_DATAGEN_POD_POLL_S = 3.0


def _kubectl_pods_hint(cfg: Any) -> str:
    ctx = cfg.platform.kubernetes.context or ""
    ctx_arg = f" --context {ctx}" if ctx else ""
    return f"kubectl get pods{ctx_arg} -n {cfg.get_namespace()} -l {DATAGEN_POD_SELECTOR}"


def live_datagen_pods(namespace: str) -> list[str]:
    """Names of the namespace's datagen pods that may still write.

    A pod whose phase is Succeeded or Failed has stopped (its containers
    exited and do not restart); any other pod, including one already being
    deleted, may still land a file. Raises on an API error: a caller that
    cannot list the pods must not assume there are none.
    """
    from kubernetes import client as k8s_client

    pods = k8s_client.CoreV1Api().list_namespaced_pod(
        namespace, label_selector=DATAGEN_POD_SELECTOR, _request_timeout=30
    )
    return sorted(
        p.metadata.name
        for p in (pods.items or [])
        if getattr(p.status, "phase", None) not in ("Succeeded", "Failed")
    )


def wait_for_datagen_pods_stopped(
    cfg: Any, *, timeout_s: float | None = None, poll_s: float | None = None
) -> None:
    """Return once no datagen pod in the namespace may still write.

    Bounded: raises ``DatagenPodsStillRunning`` (refused) when a pod is
    still running after ``timeout_s``, and ``DatagenPodsUnknown`` when the
    pods could not be listed by then. Call it after the old Job is deleted
    and before anything clears or checks the datagen prefix.
    """
    timeout_s = DATAGEN_POD_STOP_WAIT_S if timeout_s is None else timeout_s
    poll_s = _DATAGEN_POD_POLL_S if poll_s is None else poll_s
    namespace = cfg.get_namespace()
    deadline = time.time() + timeout_s
    announced = False
    while True:
        error: Exception | None = None
        live: list[str] = []
        try:
            live = live_datagen_pods(namespace)
        except Exception as e:  # noqa: BLE001 -- retried until the deadline
            error = e
        if error is None and not live:
            return
        if time.time() >= deadline:
            if error is not None:
                raise DatagenPodsUnknown(
                    f"could not list the datagen pods in {namespace} ({error}); a fresh "
                    "generate does not start while an earlier datagen pod may still write"
                )
            raise DatagenPodsStillRunning(
                f"datagen pod(s) {', '.join(live)} of an earlier lakebench-datagen Job "
                f"are still running {timeout_s:.0f}s after the Job was deleted. A fresh "
                "generate would let them write into the new corpus, where silver would "
                f"count their files as this run's rows. Re-run once `{_kubectl_pods_hint(cfg)}` "
                "lists none. Do not force-delete a pod on an unreachable node until the "
                "node is confirmed down: its container may still be writing."
            )
        if not announced and live:
            logger.info(
                "waiting up to %.0fs for %d earlier datagen pod(s) to stop", timeout_s, len(live)
            )
            announced = True
        time.sleep(poll_s)


def stop_previous_datagen(cfg: Any) -> None:
    """Delete an earlier lakebench-datagen Job and wait for its pods to stop.

    Every fresh generate calls this before it lists, clears or writes the
    datagen prefix: the CLI before the bronze gate and before a continuous
    reset, and the deployer before its cycle-0 clear. The delete uses
    Background propagation, so the pods outlive the Job by their grace
    period and may still land files; the wait is bounded
    (``wait_for_datagen_pods_stopped``). Raises ``DatagenPodsStillRunning``
    or ``DatagenPodsUnknown``; a failed delete is ``DatagenPodsUnknown``.
    """
    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    namespace = cfg.get_namespace()
    try:
        k8s_client.BatchV1Api().delete_namespaced_job(
            name="lakebench-datagen",
            namespace=namespace,
            body=k8s_client.V1DeleteOptions(propagation_policy="Background"),
            _request_timeout=30,
        )
    except ApiException as e:
        if e.status != 404:
            raise DatagenPodsUnknown(
                f"could not delete the earlier lakebench-datagen Job in {namespace} "
                f"(HTTP {e.status} {e.reason}); a fresh generate does not start while "
                "its pods may still write"
            ) from e
    except Exception as e:  # noqa: BLE001 -- transport errors, a missing kubeconfig
        raise DatagenPodsUnknown(
            f"could not delete the earlier lakebench-datagen Job in {namespace} ({e}); a "
            "fresh generate does not start while its pods may still write"
        ) from e
    wait_for_datagen_pods_stopped(cfg)


def _refusal_details(e: BaseException) -> dict[str, Any]:
    """``details`` for a failed deploy result: the refusal's exit path, if any."""
    path = getattr(e, "exit_path", None) if isinstance(e, DatagenRefused) else None
    return {REFUSAL_DETAIL: path} if path else {}


def _s3_client_for(cfg: Any) -> Any:
    from lakebench.s3 import S3Client

    s3_cfg = cfg.platform.storage.s3
    return S3Client(
        endpoint=s3_cfg.endpoint,
        access_key=s3_cfg.access_key,
        secret_key=s3_cfg.secret_key,
        region=s3_cfg.region,
        path_style=s3_cfg.path_style,
        ca_cert=s3_cfg.ca_cert,
        verify_ssl=s3_cfg.verify_ssl,
    )


def deployment_may_empty(cfg: Any, bucket: str, s3: Any = None, *, strict: bool = False) -> bool:
    """Whether this deployment may delete data in ``bucket``.

    ``lakebench.deploy.ownership.deployment_may_empty``: the rule destroy
    uses to empty a bucket. Kept here as the gate's seam.
    """
    from lakebench.deploy.ownership import deployment_may_empty as _rule

    if s3 is None:
        s3 = _s3_client_for(cfg)
    return _rule(cfg, bucket, s3, strict=strict)


@dataclass
class BronzeGateResult:
    """What ``bronze_prefix_gate`` decided before datagen."""

    proceed: bool
    bucket: str
    prefix: str
    owned: bool
    objects_before: int = 0
    stale_allowed: bool = False
    cleared: int = 0
    message: str = ""
    #: The exit code of a refusal: refused, or prerequisite when bronze could not
    #: be read, or failed when --regenerate could not clear it.
    exit_code: ExitCode = ExitCode.REFUSED

    def record(self) -> dict[str, Any] | None:
        """``metrics.json`` ``datagen.stale_bronze``, when stale objects were allowed."""
        if not self.stale_allowed:
            return None
        return {
            "allowed": True,
            "objects_before": self.objects_before,
            "bucket": self.bucket,
            "prefix": self.prefix,
        }


def bronze_prefix_gate(
    cfg: Any,
    *,
    regenerate: bool,
    allow_stale_bronze: bool,
    s3: Any = None,
) -> BronzeGateResult:
    """The one bronze safety gate, on the CLI host before any datagen Job.

    ``owned`` is ``deployment_may_empty``; ``nonempty`` means the datagen
    prefix holds an object that is not Lakebench's own.

    ======  =========  ======================  ====================================
    owned   non-empty  flags                   result
    ======  =========  ======================  ====================================
    yes     no         any                     proceed
    yes     yes        none                    refuse (pass --regenerate)
    yes     yes        --regenerate            clear the datagen prefix, proceed
    no      no         any                     proceed
    no      yes        none                    refuse (--allow-stale-bronze or clear
                                               it yourself)
    no      yes        --regenerate            refuse: never empties a bucket this
                                               deployment did not create
    no      yes        --allow-stale-bronze    proceed, recorded
    ======  =========  ======================  ====================================

    ``--regenerate`` deletes only the datagen prefix, aborting its incomplete
    multipart uploads (GOTCHAS 2); an empty prefix is refused, never widened
    to the bucket. A read failure refuses. The caller exits with the
    result's ``exit_code``. Every "proceed" means bronze is about to be
    replaced, so the silver-state data clock is cleared.

    Every generate takes it before its first datagen Job: ``generate``,
    ``run --generate`` and a multi-cycle run before cycle 0. A leftover
    corpus series marker (``_corpus/series.json``) makes the prefix
    non-empty like any other object.
    """
    bucket = cfg.platform.storage.s3.buckets.bronze
    prefix = bronze_datagen_prefix(cfg).strip("/")
    shown = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"

    def refuse(
        message: str, owned: bool, n: int = 0, code: ExitCode = ExitCode.REFUSED
    ) -> BronzeGateResult:
        return BronzeGateResult(False, bucket, prefix, owned, n, message=message, exit_code=code)

    if s3 is None:
        s3 = _s3_client_for(cfg)
    if getattr(s3, "_init_error", None):
        return refuse(
            f"cannot check {shown} for existing data ({s3._init_error}); refusing to generate",
            owned=False,
            code=ExitCode.PREREQUISITE,
        )
    try:
        if not s3.bucket_exists(bucket):
            _clear_clock_best_effort(cfg)
            return BronzeGateResult(True, bucket, prefix, owned=False)
        nonempty = s3.has_user_objects(bucket, prefix + "/" if prefix else "")
    except Exception as e:  # noqa: BLE001
        return refuse(f"could not list {shown}: {e}", owned=False, code=ExitCode.PREREQUISITE)
    if not nonempty:
        _clear_clock_best_effort(cfg)
        return BronzeGateResult(True, bucket, prefix, deployment_may_empty(cfg, bucket, s3))
    try:
        owned = deployment_may_empty(cfg, bucket, s3, strict=True)
    except Exception as e:  # noqa: BLE001
        # Not knowing who owns the bucket is not a refusal on ownership.
        return refuse(
            f"could not check who owns {bucket} ({e}); refusing to generate over {shown}",
            owned=False,
            code=ExitCode.PREREQUISITE,
        )
    try:
        info = s3.get_bucket_size(bucket, prefix=prefix + "/" if prefix else "")
        n = int(info.object_count or 0)
        size_gb = (info.size_bytes or 0) / (1024**3)
    except Exception:  # noqa: BLE001 -- the count is for the message only
        n, size_gb = 0, 0.0
    held = f"{shown} holds {n} object(s) ({size_gb:.2f} GB)"
    if regenerate and not prefix:
        return refuse(
            f"{held}, and the datagen prefix is empty: --regenerate clears only the "
            "datagen prefix and never a whole bucket. Clear the bucket yourself.",
            owned,
            n,
        )
    if owned:
        if not regenerate:
            return refuse(
                f"Bronze prefix {held}. Refusing to generate over it: pass --regenerate "
                "to clear the datagen prefix first, or --skip-generate to reuse the "
                "existing data.",
                owned,
                n,
            )
        try:
            cleared = s3.delete_prefix(bucket, prefix, abort_multipart=True)
        except Exception as e:  # noqa: BLE001
            return refuse(
                f"--regenerate: could not clear {shown}: {e}", owned, n, code=ExitCode.FAILED
            )
        _clear_clock_best_effort(cfg)
        return BronzeGateResult(True, bucket, prefix, owned, n, cleared=cleared)
    if regenerate:
        return refuse(
            f"{held} and this deployment cannot prove it owns {bucket}: Lakebench does "
            "not empty a bucket this deployment did not create. Clear the prefix "
            "yourself, or claim the bucket with `lakebench admin reclaim-bucket` (owner) "
            "and use --regenerate; --allow-stale-bronze generates over it.",
            owned,
            n,
        )
    if not allow_stale_bronze:
        return refuse(
            f"{held} and this deployment cannot prove it owns {bucket} (it did not "
            "create it, or no cluster stamp or record proves it). Clear the prefix "
            "yourself, or claim the bucket with `lakebench admin reclaim-bucket` "
            "(owner) and use --regenerate; --allow-stale-bronze generates over the "
            "objects, and rows may then be over-counted.",
            owned,
            n,
        )
    _clear_clock_best_effort(cfg)
    return BronzeGateResult(True, bucket, prefix, owned, n, stale_allowed=True)


class DatagenDeployer:
    """Deploys and monitors the datagen job."""

    TEMPLATES = ["datagen/job.yaml.j2"]

    def __init__(
        self,
        engine: DeploymentEngine,
        allow_stale_bronze: bool = False,
        *,
        continuous: bool = False,
        stale_record: dict[str, Any] | None = None,
    ):
        self.allow_stale_bronze = allow_stale_bronze
        # The bronze gate's ``datagen.stale_bronze`` record when it allowed
        # generating over existing objects; the series marker carries it so a
        # run that reuses the corpus keeps the label.
        self.stale_record = stale_record
        self._wrote_over_stale = False
        # A continuous run's datagen: it never takes --allow-stale-bronze,
        # and its reset has already cleared the datagen prefix, so a refusal
        # names that path's remedy instead of the flag.
        self.continuous = continuous
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    # Payload size was a template context knob feeding --payload-kb; dropped
    # with the CLI arg 2026-09-28 (LB-191 companion). The Rust binary now
    # hardcodes 2 KiB. Kept here as documentation of what the fixed value is.
    _PAYLOAD_SIZE_BYTES = 2048  # 2 KiB hex payloads for compression profiling

    def _build_datagen_context(self) -> dict[str, Any]:
        """Build context for datagen template."""
        cfg = self.config
        workload = cfg.architecture.workload
        datagen = workload.datagen

        # Derive dimensions from scale factor
        dims = cfg.get_scale_dimensions()
        file_size_bytes = self._parse_size_to_bytes(datagen.file_size)
        file_size_mb = file_size_bytes // (1024 * 1024)

        # Target in TB for CLI args interface
        target_tb = dims.approx_bronze_gb / 1024.0

        # Resolve effective mode (auto → batch/continuous)
        from lakebench.config.autosizer import _resolve_datagen_mode

        effective_mode = _resolve_datagen_mode(cfg)

        # Route datagen to the right Generator + schema-appropriate S3 prefix,
        # the same prefix the financial Spark scripts read from
        # (LB_FINANCIAL_BRONZE_PREFIX), so the datagen upload and
        # bronze_verify_financial.py point at the same S3 location.
        schema_value = workload.schema_type.value
        path_prefix = bronze_datagen_prefix(cfg)

        context = dict(self.context)  # Copy base context
        context.update(
            {
                "datagen_parallelism": datagen.parallelism,
                "datagen_target_tb": f"{target_tb:.6f}",
                # Explicit lakebench scale factor (1, 5, 10, ...) -- schema
                # generators like FinancialGenerator use this directly for
                # typology instance counts. Not derivable from target_tb
                # inside the container without assuming ~10GB/scale.
                "datagen_scale_factor": f"{datagen.get_effective_scale():.6f}",
                # c360 customer id space: scale-derived (100K per scale unit)
                # or customer360.unique_customers. From the TOTAL scale, so
                # every multi-cycle cycle draws from the same customers.
                "datagen_customer_id_max": dims.customers,
                "datagen_file_size_mb": file_size_mb,
                # datagen_payload_kb context var dropped 2026-09-28; template
                # no longer renders --payload-kb; Rust hardcodes 2 KiB.
                "datagen_path_prefix": path_prefix,
                "datagen_schema": schema_value,
                # From config, or the pre-registration's calibration seed for
                # financial (config/datagen_seed.py); spent seeds are refused.
                "datagen_seed": config_seed(cfg),
                # Robustness corpus flag (financial only), checked against
                # the declared corpus role (config/datagen_seed.py).
                "datagen_robustness_perturbation": config_perturbation(cfg),
                "datagen_cpu": datagen.cpu,
                "datagen_memory": datagen.memory,
                "datagen_mode": effective_mode,
                "datagen_workers": datagen.generators,
                "datagen_dirty_ratio": datagen.dirty_data_ratio,
                "datagen_image": cfg.images.datagen,
                "datagen_timestamp_start": datagen.timestamp_start,
                "datagen_timestamp_end": datagen.timestamp_end,
            }
        )

        # datagen_duration context var dropped 2026-09-28 (Wave 2 D8). The
        # entrypoint.py argparse layer silently discarded --duration; the Rust
        # binary never read it; a continuous / sustained pipeline is driven by
        # the pipeline side trickle-reading a pre-written finite corpus (LB-156)
        # or, in a future release, by a producer-driven delivery mode
        # (--delivery-mode). Passing --duration was misleading.

        # Live observability: point datagen pods at the per-deployment
        # Pushgateway. Best-effort -- the Rust binary no-ops when the env is
        # unset. Only when observability + the pushgateway are enabled. LB_RUN_ID
        # is shared across datagen and the Spark stages via the orchestrator's
        # env (set by the run flow) so the dashboard can correlate a run and a
        # re-run does not read as the previous run's series.
        obs = cfg.observability
        if obs.enabled and obs.pushgateway_enabled:
            namespace = cfg.get_namespace()
            context["pushgateway_url"] = f"http://lakebench-pushgateway.{namespace}.svc:9091"
            context["run_id"] = os.environ.get("LB_RUN_ID", "")

        return context

    def _parse_size_to_bytes(self, size_str: str) -> int:
        """``parse_size_to_bytes``."""
        return parse_size_to_bytes(size_str)

    @staticmethod
    def _cycle_timestamp_range(
        cycle_index: int,
        total_cycles: int,
        timestamp_start: str | None = None,
        timestamp_end: str | None = None,
    ) -> tuple[str, str]:
        """The event-time window of one cycle: ``config.c360_run.cycle_windows``,
        the one copy of the window rule (non-overlapping, chronological, the
        last cycle takes the remainder)."""
        from lakebench.config.c360_run import cycle_windows

        return cycle_windows(total_cycles, timestamp_start, timestamp_end)[cycle_index]

    def deploy_cycle(
        self,
        cycle_index: int,
        total_cycles: int,
    ) -> DeploymentResult:
        """Deploy datagen for a specific batch cycle.

        Overrides the timestamp range and scale to produce a per-cycle
        slice of the total data.  Data appends to the same bronze bucket.
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.engine.dry_run:
            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.SUCCESS,
                message=f"Would deploy datagen job for cycle {cycle_index + 1}/{total_cycles}",
                elapsed_seconds=0,
            )

        try:
            context = self._build_datagen_context()

            # Per-cycle timestamp window
            datagen = self.config.architecture.workload.datagen
            ts_start, ts_end = self._cycle_timestamp_range(
                cycle_index,
                total_cycles,
                datagen.timestamp_start,
                datagen.timestamp_end,
            )
            context["datagen_timestamp_start"] = ts_start
            context["datagen_timestamp_end"] = ts_end
            # Each cycle gets its own seed stream / time slice and object keys
            # (WORKPLAN B4); without these every cycle rewrote cycle 0's files
            # with the same seed.
            context["datagen_cycle"] = cycle_index
            context["datagen_cycles"] = total_cycles

            # Per-cycle scale (divide total evenly, minimum 1)
            total_scale = datagen.get_effective_scale()
            scale_per_cycle = max(1, total_scale // total_cycles)
            dims = self.config.get_scale_dimensions()
            target_tb = (dims.approx_bronze_gb / total_cycles) / 1024.0
            context["datagen_target_tb"] = f"{target_tb:.6f}"

            self.stop_previous_job()

            # Cycle 0 is a fresh write: clear stale files a prior generate left,
            # after the previous job's pods have stopped so nothing writes
            # mid-clear. Append cycles (n > 0) keep the earlier cycles' files.
            self._clear_bronze_prefix_if_fresh(cycle_index, context["datagen_path_prefix"])
            if cycle_index == 0:
                self._begin_series(total_cycles)

            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, context)
                manifest = yaml.safe_load(yaml_content)
                self.k8s.apply_manifest(manifest, namespace=namespace)

            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.SUCCESS,
                message=f"Datagen cycle {cycle_index + 1}/{total_cycles} submitted ({ts_start} to {ts_end})",
                elapsed_seconds=time.time() - start,
                details={
                    "job_name": "lakebench-datagen",
                    "namespace": namespace,
                    "cycle_index": cycle_index,
                    "total_cycles": total_cycles,
                    "timestamp_start": ts_start,
                    "timestamp_end": ts_end,
                    "scale_per_cycle": scale_per_cycle,
                    "target_tb": context["datagen_target_tb"],
                },
            )

        except Exception as e:
            if isinstance(e, DatagenRefused):
                logger.warning("Datagen cycle deployment failed: %s", e)
            else:
                logger.exception("Datagen cycle deployment failed")
            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.FAILED,
                message=f"Datagen cycle {cycle_index + 1} failed: {e}",
                elapsed_seconds=time.time() - start,
                details=_refusal_details(e),
            )

    def _clear_bronze_prefix_if_fresh(self, cycle_index: int, path_prefix: str) -> None:
        """Before cycle 0: clear stale datagen files, or refuse to write over them.

        A re-generate into a reused bronze bucket must not inherit part-*
        files a larger earlier generate left behind: they share the
        ``part-NNNNNN`` naming, so silver cannot tell them apart and
        over-counts. Append cycles (n > 0) keep earlier cycles'
        files. On a bucket this deployment may empty
        (``deployment_may_empty``) the datagen prefix is cleared, scoped by
        ``delete_prefix`` with its incomplete uploads aborted; a failure
        raises and fails the generate (invariant 3). On any other bucket a
        non-empty prefix raises ``StaleBronzeRefused`` unless the deployer was
        built with ``allow_stale_bronze`` (the defence for a caller
        that skipped the CLI's ``bronze_prefix_gate``).
        """
        if cycle_index != 0:
            return
        prefix = path_prefix.strip("/")
        bucket = self.config.platform.storage.s3.buckets.bronze
        s3 = _s3_client_for(self.config)
        if s3._init_error:
            raise RuntimeError(
                f"cannot check the bronze prefix s3://{bucket}/{prefix} before a "
                f"fresh generate: {s3._init_error}"
            )
        if not s3.bucket_exists(bucket):
            return
        if not prefix:
            # Nothing to scope a clear to: a whole bucket is never cleared here.
            if s3.has_user_objects(bucket) and not self.allow_stale_bronze:
                raise StaleBronzeRefused(
                    f"s3://{bucket} holds objects and the datagen prefix is empty, so "
                    "they cannot be cleared before a fresh generate. Clear the bucket "
                    + (
                        "yourself."
                        if self.continuous
                        else "yourself, or pass --allow-stale-bronze."
                    )
                )
            return
        if deployment_may_empty(self.config, bucket, s3):
            n = s3.delete_prefix(bucket, prefix, abort_multipart=True)
            if n:
                logger.info(
                    "cleared %d stale object(s) under s3://%s/%s before a fresh generate",
                    n,
                    bucket,
                    prefix,
                )
            return
        if not s3.has_user_objects(bucket, prefix + "/"):
            return
        if self.allow_stale_bronze:
            logger.warning(
                "s3://%s/%s holds objects and this deployment did not create %s; "
                "generating over them (--allow-stale-bronze)",
                bucket,
                prefix,
                bucket,
            )
            self._wrote_over_stale = True
            return
        if self.continuous:
            raise StaleBronzeRefused(
                f"s3://{bucket}/{prefix} holds objects and this deployment cannot prove "
                f"it may empty {bucket}. This run's continuous reset cleared the prefix "
                "before datagen, so another writer put them there since. Re-run once "
                f"`{_kubectl_pods_hint(self.config)}` lists none and nothing else writes "
                "there; the reset clears the prefix again. A continuous run's own "
                "datagen does not take --allow-stale-bronze, and --force-reset does not "
                "change this check."
            )
        raise StaleBronzeRefused(
            f"s3://{bucket}/{prefix} holds objects and this deployment did not create "
            f"{bucket}. Pass --allow-stale-bronze to generate over them, or clear the "
            "prefix yourself."
        )

    def _begin_series(self, total_cycles: int) -> None:
        """Write the corpus series marker of a generate that is starting
        (``deploy.corpus.begin_series``), after the fresh clear and before
        the Job, so a generate that stops part way leaves a marker that says
        so. Raises when it cannot be written; skipped when the bronze bucket
        does not exist yet."""
        from lakebench.deploy.corpus import begin_series

        s3 = _s3_client_for(self.config)
        if s3._init_error:
            raise RuntimeError(f"cannot write the series marker: {s3._init_error}")
        if not s3.bucket_exists(self.config.platform.storage.s3.buckets.bronze):
            return
        stale = self.stale_record or ({"allowed": True} if self._wrote_over_stale else None)
        begin_series(self.config, s3, total_cycles, os.environ.get("LB_RUN_ID", ""), stale=stale)

    def deploy(self) -> DeploymentResult:
        """Deploy the datagen job.

        Returns:
            DeploymentResult with status
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.engine.dry_run:
            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.SUCCESS,
                message="Would deploy datagen job",
                elapsed_seconds=0,
            )

        try:
            context = self._build_datagen_context()

            self.stop_previous_job()

            # A single-cycle generate is a fresh write: clear stale files after
            # the previous job's pods have stopped, so nothing writes into the
            # prefix mid-clear.
            self._clear_bronze_prefix_if_fresh(0, context["datagen_path_prefix"])
            self._begin_series(1)

            # Render and apply job template
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, context)
                manifest = yaml.safe_load(yaml_content)
                self.k8s.apply_manifest(manifest, namespace=namespace)

            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.SUCCESS,
                message="Datagen job submitted",
                elapsed_seconds=time.time() - start,
                details={
                    "job_name": "lakebench-datagen",
                    "namespace": namespace,
                    "parallelism": context["datagen_parallelism"],
                    "target_tb": context["datagen_target_tb"],
                },
            )

        except Exception as e:
            if isinstance(e, DatagenRefused):
                logger.warning("Datagen deployment failed: %s", e)
            else:
                logger.exception("Datagen deployment failed")
            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.FAILED,
                message=f"Datagen job submission failed: {e}",
                elapsed_seconds=time.time() - start,
                details=_refusal_details(e),
            )

    def stop_previous_job(self) -> None:
        """``stop_previous_datagen`` for this deployer's config."""
        stop_previous_datagen(self.config)

    def _delete_existing_job(self, namespace: str, *, request_timeout: int | None = None) -> None:
        """Delete existing datagen job if present.

        ``request_timeout`` caps the API call so a caller invoked because
        the K8s API itself hung (datagen timeout handler) does not block
        indefinitely on the delete.
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        batch_v1 = k8s_client.BatchV1Api()

        try:
            kwargs: dict = {
                "name": "lakebench-datagen",
                "namespace": namespace,
                "body": k8s_client.V1DeleteOptions(propagation_policy="Background"),
            }
            if request_timeout is not None:
                kwargs["_request_timeout"] = request_timeout
            batch_v1.delete_namespaced_job(**kwargs)
            # Wait for job to be deleted
            time.sleep(2)
        except ApiException as e:
            if e.status != 404:
                raise

    def wait_for_completion(
        self,
        timeout_seconds: int = 7200,
        poll_interval: int = 30,
    ) -> DeploymentResult:
        """Wait for datagen job to complete.

        Args:
            timeout_seconds: Maximum wait time (default 2 hours)
            poll_interval: Seconds between status checks

        Returns:
            DeploymentResult with completion status
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        start = time.time()
        namespace = self.config.get_namespace()
        batch_v1 = k8s_client.BatchV1Api()

        while time.time() - start < timeout_seconds:
            try:
                job = batch_v1.read_namespaced_job_status(
                    name="lakebench-datagen",
                    namespace=namespace,
                )

                status = job.status
                completions = job.spec.completions or 1
                succeeded = status.succeeded or 0
                failed = status.failed or 0
                active = status.active or 0

                if succeeded >= completions:
                    return DeploymentResult(
                        component="datagen",
                        status=DeploymentStatus.SUCCESS,
                        message=f"Datagen completed: {succeeded}/{completions} pods succeeded",
                        elapsed_seconds=time.time() - start,
                        details={
                            "succeeded": succeeded,
                            "failed": failed,
                            "completions": completions,
                        },
                    )

                # Check for failure
                if failed > 0 and active == 0:
                    return DeploymentResult(
                        component="datagen",
                        status=DeploymentStatus.FAILED,
                        message=f"Datagen failed: {failed} pods failed",
                        elapsed_seconds=time.time() - start,
                        details={
                            "succeeded": succeeded,
                            "failed": failed,
                            "completions": completions,
                        },
                    )

                # Still running
                time.sleep(poll_interval)

            except ApiException as e:
                if e.status == 404:
                    return DeploymentResult(
                        component="datagen",
                        status=DeploymentStatus.FAILED,
                        message="Datagen job not found",
                        elapsed_seconds=time.time() - start,
                    )
                raise

        return DeploymentResult(
            component="datagen",
            status=DeploymentStatus.FAILED,
            message=f"Datagen timed out after {timeout_seconds}s",
            elapsed_seconds=time.time() - start,
        )

    def get_progress(self) -> dict[str, Any]:
        """Get current progress of datagen job.

        Returns:
            Dict with progress information
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        namespace = self.config.get_namespace()
        batch_v1 = k8s_client.BatchV1Api()
        core_v1 = k8s_client.CoreV1Api()

        try:
            job = batch_v1.read_namespaced_job_status(
                name="lakebench-datagen",
                namespace=namespace,
            )

            status = job.status
            completions = job.spec.completions or 1
            succeeded = status.succeeded or 0
            failed = status.failed or 0
            active = status.active or 0
            # LB-200: scope progress to the CURRENT job's pods. A re-generate
            # reuses the name/label, so a prior job's OOMKilled pod (still
            # Terminating) would otherwise be scanned and false-abort the new,
            # healthy generation. Pods carry the owning Job's uid in an owner
            # reference; keep only pods owned by this job incarnation.
            job_uid = job.metadata.uid if job.metadata else None

            # Get pod logs for progress
            pods = core_v1.list_namespaced_pod(
                namespace,
                label_selector="app=lakebench-datagen",
            )

            pod_status = []
            oom_pods: list[str] = []
            crash_pods: list[str] = []
            crash_details: dict[str, str] = {}
            pending_pods: list[str] = []
            for pod in pods.items:
                if job_uid and not _pod_owned_by_job(pod, job_uid):
                    continue  # a stale pod from a previous generation
                pod_name = pod.metadata.name
                pod_info = {
                    "name": pod_name,
                    "phase": pod.status.phase,
                    "index": pod.metadata.annotations.get(
                        "batch.kubernetes.io/job-completion-index", "?"
                    ),
                }
                pod_status.append(pod_info)

                # Detect OOM and crash-looping containers
                if pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        terminated = cs.last_state and cs.last_state.terminated
                        if terminated and terminated.reason == "OOMKilled":
                            oom_pods.append(pod_name)
                        elif (
                            cs.restart_count
                            and cs.restart_count >= 3
                            and pod.status.phase != "Succeeded"
                            and cs.state is not None
                            and cs.state.waiting is not None
                            and cs.state.waiting.reason == "CrashLoopBackOff"
                        ):
                            # restart_count never resets, so require the pod to
                            # be crash-looping NOW: one that recovered after a
                            # few transient failures is still making progress.
                            crash_pods.append(pod_name)
                            if terminated:
                                crash_details[pod_name] = f"exit {terminated.exit_code}" + (
                                    f" ({terminated.reason})" if terminated.reason else ""
                                )

                # Detect pending pods
                if pod.status.phase == "Pending":
                    pending_pods.append(pod_name)

            return {
                "running": active > 0 or (succeeded < completions and failed == 0),
                "succeeded": succeeded,
                "failed": failed,
                "active": active,
                "completions": completions,
                "progress_pct": (succeeded / completions * 100) if completions > 0 else 0,
                "pods": pod_status,
                "oom_pods": oom_pods,
                "crash_pods": crash_pods,
                "crash_details": crash_details,
                "pending_pods": pending_pods,
            }

        except ApiException as e:
            if e.status == 404:
                return {"running": False, "error": "Job not found"}
            raise
