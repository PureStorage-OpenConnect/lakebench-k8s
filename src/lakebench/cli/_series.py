"""``run --repeat N``: N batch repetitions over one corpus, as one series.

Repetition 1 runs as the options ask (it may generate). Repetitions 2 to N
never generate and rebuild silver and gold from the same bronze. One corpus
per series (ch03 section 6 "Series corpus identity"):

- the config is loaded once, by ``run``, and every repetition runs a copy of
  that load and records the hash of the bytes it was loaded from;
- D1 is the ``bronze_listing_sha256`` of repetition 1's run-end corpus
  observation, read from its saved record together with its corpus block
  (``corpus_identity.inherited_corpus_from``);
- before repetition 1 (when it does not generate), right after it, and
  before every later repetition, the datagen scope is listed again with the
  same computation the observation makes, and must give D1;
- every later repetition persists repetition 1's block as
  ``experiment_inputs.inherited_corpus``; the record decides between its own
  markers and the inherited block when it is built, and the series reads the
  saved record back to decide whether it is a member
  (``metrics.series.member_of_series``). A repetition that is not a member
  stops the series with exit 3, ``series.corpus_changed``.

A repetition that fails its verdict does not stop the series. An interrupt
stops it with exit 130. A SparkApplication of the deployment still running
before a repetition (or one that cannot be listed) stops it with exit 1. The
manifest ``lakebench-output/series/<id>.json`` is rewritten after every
repetition. Exit: 0 when every repetition passed and is a member, 1 when any
did not (or repetition 1 left no verified corpus to reuse), 3 when bronze or
the corpus changed, 130 on an interrupt. A repetition that stops before
saving a record, and repetition 1 when it exits 2 to 5, stop the series
with that repetition's own code.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from lakebench.cli._helpers import (
    console,
    esc,
    print_error,
    print_info,
    print_success,
    print_warning,
)
from lakebench.exit_codes import ExitCode, LakebenchError, SafetyRefusal

logger = logging.getLogger(__name__)

#: Options a later repetition always runs with: no datagen (so nothing a
#: generate reads, --allow-stale-bronze included), a rebuild of silver and
#: gold from the bronze repetition 1 verified.
LATER_REPETITION = {
    "include_datagen": False,
    "regenerate": False,
    "allow_stale_bronze": False,
    "skip_generate": True,
    "force_rebuild": True,
}


class _Stop(Exception):
    """The series stops here with *code*; *reason* goes into the manifest.
    *corpus* marks a stop this module raised because bronze or the corpus
    differs (exit 3, ``series.corpus_changed``)."""

    def __init__(self, code: int, reason: str, *, corpus: bool = False) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason
        self.corpus = corpus


def _corpus_stop(reason: str) -> _Stop:
    return _Stop(ExitCode.REFUSED, f"series.corpus_changed: {reason}", corpus=True)


def bronze_listing(cfg: Any) -> tuple[str | None, int, int, str]:
    """``(digest, objects, bytes, scope)`` of the datagen scope in the bronze
    bucket, computed exactly as the run-end observation computes its
    ``bronze_listing_sha256`` (``corpus_digest.list_scope`` and
    ``listing_sha256``; None for an empty scope). Errors propagate."""
    from lakebench.corpus_digest import datagen_scope, list_scope, listing_sha256
    from lakebench.deploy.datagen import bronze_datagen_prefix
    from lakebench.s3 import S3Client

    s3 = cfg.platform.storage.s3
    client = S3Client(
        endpoint=s3.endpoint,
        access_key=s3.access_key,
        secret_key=s3.secret_key,
        region=s3.region,
        path_style=s3.path_style,
        ca_cert=s3.ca_cert,
        verify_ssl=s3.verify_ssl,
    ).raw_client
    if client is None:
        raise RuntimeError("the S3 client did not initialise")
    scope = datagen_scope(bronze_datagen_prefix(cfg))
    objects = list_scope(client, s3.buckets.bronze, scope)
    digest = listing_sha256(objects) if objects else None
    total = sum(int(o.get("Size") or 0) for o in objects)
    return digest, len(objects), total, f"{s3.buckets.bronze}/{scope}"


def _listing_or_stop(cfg: Any, when: str) -> tuple[str | None, int, int, str]:
    try:
        return bronze_listing(cfg)
    except Exception as e:  # noqa: BLE001 -- never assume bronze is unchanged
        raise _corpus_stop(f"bronze could not be listed {when} ({e})") from None


def _call(run_once: Any, *args: Any, **kwargs: Any) -> tuple[int, BaseException | None]:
    """One repetition: its exit code (0 when it returned, else its
    typer.Exit code; 130 for an interrupt that escaped its handlers) and an
    error it raised, which the series records and then re-raises."""
    try:
        run_once(*args, **kwargs)
    except typer.Exit as e:
        return int(e.exit_code or 0), None
    except KeyboardInterrupt:
        return int(ExitCode.INTERRUPTED), None
    except Exception as e:  # noqa: BLE001 -- recorded in the manifest, then re-raised
        from lakebench.cli._exit import exit_code_for

        # The code the CLI exits with once the series re-raises *e*.
        return int(exit_code_for(e) or ExitCode.FAILED), e
    return 0, None


def _unfinished_apps(cfg: Any) -> list[str] | None:
    """This deployment's lakebench SparkApplications that have not ended
    (a stage a timeout left running); None when the list call fails. Raises
    what ``get_k8s_client`` raises (a context refusal, an unloadable
    kubeconfig), which the CLI maps to its exit code."""
    from kubernetes import client as k8s_client

    import lakebench.k8s

    # Before repetition 1 nothing has loaded the kubeconfig yet: the
    # K8sClient pins this process to the deployment's context and configures
    # the default client a bare API object uses. Its own failures (a context
    # refusal, an unloadable kubeconfig) exit with their codes, as a run does.
    lakebench.k8s.get_k8s_client(
        context=cfg.platform.kubernetes.context, namespace=cfg.get_namespace()
    )
    try:
        resp = k8s_client.CustomObjectsApi().list_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=cfg.get_namespace(),
            plural="sparkapplications",
            _request_timeout=(5, 30),
        )
    except Exception as e:  # noqa: BLE001 -- unknown is not "none running"
        logger.warning("series: could not list SparkApplications: %s", e)
        return None
    out = []
    for item in (resp or {}).get("items") or []:
        name = str(((item or {}).get("metadata") or {}).get("name") or "")
        state = (((item or {}).get("status") or {}).get("applicationState") or {}).get("state")
        # SUCCEEDING and FAILING are the monitor's terminal states too.
        if name.startswith("lakebench-") and state not in (
            "COMPLETED",
            "FAILED",
            "SUCCEEDING",
            "FAILING",
        ):
            out.append(name)
    return out


def _record(storage: Any, run_id: str | None) -> dict[str, Any] | None:
    if not run_id:
        return None
    import json

    path = storage.run_dir(run_id) / "metrics.json"
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError) as e:
        logger.warning("series: could not read the record of %s: %s", run_id, e)
        return None


def _datagen_state(cfg: Any) -> str:
    from lakebench.cli._sustained import _datagen_job_state

    return _datagen_job_state(cfg.get_namespace())[0]


def run_series(
    cfg: Any,
    config_file: Path,
    options: dict[str, Any],
    n: int,
    *,
    config_sha256: str | None,
) -> None:
    """Run *n* repetitions of the loaded *cfg* as one series (module
    docstring); ends with ``typer.Exit`` (or the stop's error) unless every
    repetition passed and is a member."""
    from lakebench._constants import DEFAULT_OUTPUT_DIR
    from lakebench.cli._run import _run_once
    from lakebench.cli._run_args import RunArgs, validate_run_args
    from lakebench.corpus_digest import is_sha256_hex
    from lakebench.metrics import MetricsStorage
    from lakebench.metrics.corpus_identity import inherited_corpus_from
    from lakebench.metrics.series import (
        CHANGED_REASON,
        ID_MISMATCH_PROBLEM,
        SeriesContext,
        SeriesManifest,
        bronze_verified,
        member_of_series,
        new_series_id,
        stage_succeeded,
        verdict_of,
    )

    series_id = new_series_id()
    manifest = SeriesManifest(
        series_id=series_id,
        config_path=str(Path(config_file).resolve()),
        config_sha256=config_sha256,
        deployment_name=cfg.name,
        requested=n,
    )
    out_dir = Path(DEFAULT_OUTPUT_DIR) / "series"
    storage = MetricsStorage()
    pristine = cfg.model_copy(deep=True)
    generates = bool(options.get("include_datagen")) and not options.get("skip_generate")
    print_info(f"Series {series_id}: {n} repetition(s) over one corpus")

    rep1: dict[str, Any] | None = None
    inherited: dict[str, Any] | None = None
    d1: str | None = None
    all_passed = True
    code = 0
    stop: _Stop | None = None
    path: Path | None = None
    try:
        d0 = None if generates else _listing_or_stop(pristine, "before repetition 1")[0]
        for index in range(1, n + 1):
            opts = dict(options)
            # Never measure next to a stage still running (one a timeout
            # left behind, from this series or an earlier run).
            running = _unfinished_apps(pristine)
            if running is None or running:
                raise _Stop(
                    ExitCode.FAILED,
                    f"before repetition {index}: "
                    + (
                        "SparkApplications could not be listed"
                        if running is None
                        else f"still running: {', '.join(running)}"
                    ),
                )
            if index > 1:
                digest = _listing_or_stop(pristine, f"before repetition {index}")[0]
                if digest != d1:
                    raise _corpus_stop(f"bronze changed before repetition {index}")
                opts.update(LATER_REPETITION)
            plan = validate_run_args(RunArgs(**opts), pristine)
            ctx = SeriesContext(
                series_id=series_id,
                index=index,
                size=n,
                config_sha256=config_sha256,
                inherited=inherited,
                d1=d1,
            )
            console.print()
            console.print(
                f"[bold cyan]Series {esc(series_id)}: repetition {esc(index)}/{esc(n)}[/bold cyan]"
            )
            rep_code, rep_error = _call(
                _run_once,
                pristine.model_copy(deep=True),
                config_file,
                plan,
                series=ctx,
                allow_auto_deploy=index == 1,
                **opts,
            )
            record = _record(storage, ctx.run_id) if ctx.sealed else None
            verdict = verdict_of(record)
            interrupted = (
                rep_code == ExitCode.INTERRUPTED
                or ctx.signalled
                or bool(record and record.get("interrupted"))
            )
            member, reason = True, None
            if record is None:
                member, reason = False, f"exited {rep_code} before saving a record"
            elif index > 1:
                assert rep1 is not None and d1 is not None
                member, reason = member_of_series(record, rep1, d1)
            manifest.add_run(
                index=index,
                run_id=ctx.run_id if record is not None else None,
                exit_code=rep_code,
                verdict=verdict,
                member=member,
                reason=reason,
                corpus_changed=reason in (CHANGED_REASON, ID_MISMATCH_PROBLEM),
            )
            if verdict != "PASSED" or not member:
                all_passed = False
            manifest.write(out_dir)
            if rep_error is not None:
                manifest.stopped_reason = f"repetition {index} raised {type(rep_error).__name__}"
                raise rep_error
            if interrupted:
                raise _Stop(ExitCode.INTERRUPTED, "interrupted")
            if record is None:
                # Not a corpus question: the repetition stopped before its save.
                raise _Stop(rep_code or ExitCode.FAILED, f"repetition {index} {reason}")

            if index == 1:
                if rep_code not in (0, 1):
                    raise _Stop(rep_code, f"repetition 1 exited {rep_code}")
                if not bronze_verified(record):
                    raise _Stop(
                        ExitCode.FAILED,
                        "repeat.no_verified_corpus: repetition 1's bronze-verify did not pass",
                    )
                if not stage_succeeded(record, "silver-build"):
                    raise _Stop(
                        ExitCode.FAILED,
                        "repetition 1 did not build silver; a later repetition would rebuild "
                        "(drop) a silver table this series did not build",
                    )
                state = _datagen_state(pristine)
                if state not in ("finished", "absent"):
                    raise _Stop(
                        ExitCode.FAILED,
                        f"repeat.no_verified_corpus: the datagen Job is {state}, so bronze "
                        "may still be changing",
                    )
                inherited = inherited_corpus_from(record)
                d1 = inherited.get("bronze_listing_sha256")
                if not is_sha256_hex(d1) or not isinstance(inherited.get("corpus"), dict):
                    raise _Stop(
                        ExitCode.FAILED,
                        "repeat.no_verified_corpus: repetition 1's record carries no observed "
                        "corpus to reuse",
                    )
                if not generates and d0 != d1:
                    raise _corpus_stop("bronze changed during repetition 1")
                now, objects, size, scope = _listing_or_stop(pristine, "after repetition 1")
                if now != d1:
                    raise _corpus_stop("bronze changed after repetition 1 was saved")
                rep1 = record
                corpus = (record.get("experiment") or {}).get("corpus") or {}
                manifest.corpus = {
                    "id": corpus.get("id"),
                    "id_v2": corpus.get("id_v2"),
                    "bronze_objects": objects,
                    "bronze_bytes": size,
                    "bronze_listing_sha256": d1,
                    "digest_scope": scope,
                    "from_run_id": record.get("run_id"),
                }
                manifest.write(out_dir)
            elif not member:
                if reason in (CHANGED_REASON, ID_MISMATCH_PROBLEM):
                    raise _corpus_stop(f"repetition {index}: {reason}")
                raise _Stop(ExitCode.FAILED, f"repetition {index}: {reason}")
        code = 0 if all_passed else int(ExitCode.FAILED)
    except _Stop as s_:
        stop = s_
        manifest.stopped_reason = s_.reason
        code = int(s_.code)
        if s_.corpus and manifest.runs and "during repetition 1" in s_.reason:
            # Repetition 1 read a corpus that did not hold still: not a member.
            manifest.runs[0]["member"] = False
            manifest.runs[0]["corpus_changed"] = True
            manifest.runs[0]["not_member_reason"] = s_.reason
    except KeyboardInterrupt:
        manifest.stopped_reason = "interrupted"
        code = int(ExitCode.INTERRUPTED)
    except BaseException as e:
        if manifest.stopped_reason is None:
            manifest.stopped_reason = f"error: {type(e).__name__}: {e}"[:300]
        raise
    finally:
        try:
            path = manifest.write(out_dir)
        except OSError as e:
            print_warning(f"Series manifest not written: {e}")
    summary = manifest.to_dict()
    line = (
        f"Series {series_id}: {summary['passed']} of {summary['requested']} passed"
        + (f" ({manifest.stopped_reason})" if manifest.stopped_reason else "")
        + (f"; manifest {path}" if path else "")
    )
    (print_success if code == 0 else print_warning)(line)
    if stop is not None and stop.corpus:
        raise SafetyRefusal(
            "bronze changed during or between repetitions; the series stopped",
            why=stop.reason,
            next="regenerate the corpus or start a new series",
            path="series.corpus_changed",
        )
    if stop is not None and stop.reason.startswith("repeat.no_verified_corpus"):
        raise LakebenchError(
            "no verified corpus to reuse; the series stopped after repetition 1",
            why=stop.reason,
            path="repeat.no_verified_corpus",
        )
    if stop is not None and code == ExitCode.FAILED:
        print_error(f"Series {series_id} stopped: {stop.reason}")
    if code:
        raise typer.Exit(code)
