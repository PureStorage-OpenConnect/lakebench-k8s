"""The CLI's top-level error handler and exit-code mapping.

``LakebenchGroup`` is the Typer group class of the root ``lakebench`` app.
Its ``invoke`` wraps every command, so a typed error raised anywhere below it
exits with its documented code, and ``CliRunner(app)`` tests see the same
codes as the console script. Codes, errors and ``PATHS`` live in the
standard-library module ``lakebench.exit_codes`` and are re-exported here.

Mapping (``exit_code_for``):

- ``typer.Exit`` / ``SystemExit`` and Click's own usage errors pass through
  unchanged (Click prints a usage error and exits 2);
- ``LakebenchError``: its own code;
- ``ConfigError`` (including ``ConfigValidationError``): 2; a
  ``ConfigProtectedCorpusError`` (a protected AML seed or role) is reported
  as ``run.protected_corpus``;
- ``ClusterLockHeld`` (another process holds the cluster lease): 3;
- ``K8sConnectionError`` and ``kubernetes.config.ConfigException``: 4;
- Click ``Abort`` (a declined ``typer.confirm(abort=True)``, end of input
  with no terminal, or Ctrl-C at a prompt): 5;
- a broken stdout pipe (``lakebench ... | head``) passes through to Typer,
  which exits 1 without a message;
- ``KeyboardInterrupt``: 130;
- anything else: 1, printed as one line; ``LAKEBENCH_DEBUG=1`` adds the
  traceback.

Machine-readable exit path: with ``LB_EXIT_PATH_FILE`` set, the root group
appends one line ``<code> <path> [<path>...]`` (or ``<code> -`` when no path
is known) to that file on every way out, so a harness can tell apart
refusals that share exit 3 without reading message text. A typed error gives
its own path; a deploy or destroy that refuses inside its step results gives
the ``details[REFUSAL_DETAIL]`` path of each refused step
(``note_exit_paths``).
"""

from __future__ import annotations

import errno
import os
import sys
import traceback
from typing import Any

import typer
from typer.core import TyperGroup

from lakebench.exit_codes import (
    FOLLOWS_REFUSAL_DETAIL,
    MEANINGS,
    PATHS,
    PATHS_BY_NAME,
    REFUSAL_DETAIL,
    ExitCode,
    ExitPath,
    Incomplete,
    LakebenchError,
    NotConfirmed,
    PrerequisiteError,
    SafetyRefusal,
    UsageError,
    path_code,
)

__all__ = [
    "MEANINGS",
    "PATHS",
    "PATHS_BY_NAME",
    "ExitCode",
    "ExitPath",
    "Incomplete",
    "LakebenchError",
    "LakebenchGroup",
    "NotConfirmed",
    "PrerequisiteError",
    "SafetyRefusal",
    "UsageError",
    "error_for",
    "exit_code_for",
    "path_code",
    "note_exit_paths",
    "quiet_urllib3",
    "refusal_paths",
    "refused_result_code",
]

DEBUG_ENV = "LAKEBENCH_DEBUG"

#: When set, the root group appends ``<code> <path>...`` to this file on exit.
EXIT_PATH_FILE_ENV = "LB_EXIT_PATH_FILE"

# Paths noted by a command that exits through ``typer.Exit`` (the deploy and
# destroy step refusals), reset at the start of every root invocation.
_noted_paths: list[str] = []


def note_exit_paths(paths: list[str]) -> None:
    """Record exit paths for ``LB_EXIT_PATH_FILE`` before a ``typer.Exit``."""
    _noted_paths.extend(p for p in paths if p and p not in _noted_paths)


def refusal_paths(results: Any) -> list[str]:
    """The ``details[REFUSAL_DETAIL]`` path of every failed step, in order."""
    paths: list[str] = []
    for r in results:
        if getattr(getattr(r, "status", None), "value", None) != "failed":
            continue
        path = (getattr(r, "details", None) or {}).get(REFUSAL_DETAIL)
        if isinstance(path, str) and path and path not in paths:
            paths.append(path)
    return paths


def _write_exit_path(code: int, paths: list[str]) -> None:
    """Append ``<code> <paths>`` to ``LB_EXIT_PATH_FILE``; never raises."""
    target = os.environ.get(EXIT_PATH_FILE_ENV, "")
    if not target:
        return
    line = f"{int(code)} {' '.join(paths) if paths else '-'}\n"
    try:
        with open(target, "a", encoding="utf-8") as fh:
            fh.write(line)
    except (OSError, ValueError):
        pass


_CLICK_ROOTS = frozenset({"typer", "click"})


def _click_family(exc: BaseException) -> bool:
    """True when *exc* is a Typer or Click class, vendored or stock.

    Typer moves these classes between releases: 0.12 to 0.2x re-export
    ``click``'s, 0.27.0 vendors Click as ``typer._click``, and 0.27.2 defines
    ``Exit`` and ``Abort`` in ``typer.exceptions``. Checked on 0.20.0, 0.27.0
    and 0.27.2. Matching on the defining package instead of a class name
    keeps working when a release renames or moves a class, and covers stock
    ``click`` raised by a dependency.
    """
    return any(c.__module__.split(".")[0] in _CLICK_ROOTS for c in type(exc).__mro__)


def _is_abort(exc: BaseException) -> bool:
    """Typer's or Click's ``Abort``: a declined confirm or no terminal."""
    if isinstance(exc, typer.Abort):
        return True
    return any(
        c.__name__ == "Abort" and c.__module__.split(".")[0] in _CLICK_ROOTS
        for c in type(exc).__mro__
    )


def refused_result_code(results: Any) -> ExitCode | None:
    """``REFUSED`` when the failed deploy or destroy steps were safety refusals.

    *results* are ``DeploymentResult``s. A producer marks a refusal with
    ``details[REFUSAL_DETAIL]`` (``destroy.redeployed``, ``lease.held``,
    ``deploy.identity_foreign``) and a step that failed only because of one
    with ``details[FOLLOWS_REFUSAL_DETAIL]``. Every failed step must be one of
    the two: any other failure keeps the caller's code (1), so a refusal never
    hides a failure a re-run could fix. None when no failed step is a refusal.
    """
    refused = False
    for r in results:
        if getattr(getattr(r, "status", None), "value", None) != "failed":
            continue
        details = getattr(r, "details", None) or {}
        if details.get(REFUSAL_DETAIL):
            refused = True
        elif not details.get(FOLLOWS_REFUSAL_DETAIL):
            return None
    return ExitCode.REFUSED if refused else None


def _debug() -> bool:
    return os.environ.get(DEBUG_ENV, "") not in ("", "0")


def _first_line(text: str) -> str:
    text = text.strip()
    return text.splitlines()[0] if text else ""


def error_for(exc: BaseException) -> LakebenchError | None:
    """The error to report for *exc*, or None when it must pass through.

    Pure apart from the lazy imports: it prints nothing and exits nothing.
    """
    if _is_abort(exc):
        return LakebenchError(
            "Not confirmed: the prompt was declined or there was no terminal to answer it.",
            next="Answer the prompt, or pass --yes where the command offers it.",
            path="confirm.non_tty",
            code=ExitCode.NOT_CONFIRMED,
        )
    if isinstance(exc, (SystemExit, GeneratorExit)) or _click_family(exc):
        return None  # typer.Exit and Click's usage errors: Click prints and exits
    if isinstance(exc, BrokenPipeError) or (isinstance(exc, OSError) and exc.errno == errno.EPIPE):
        return None  # the reader went away; Typer exits 1 quietly
    if isinstance(exc, LakebenchError):
        return exc
    if isinstance(exc, KeyboardInterrupt):
        return LakebenchError("Interrupted.", path="sigint", code=ExitCode.INTERRUPTED)
    if not isinstance(exc, Exception):
        return None

    from lakebench.deploy.cluster_lock import ClusterLockHeld

    if isinstance(exc, ClusterLockHeld):
        return LakebenchError(
            f"Refused: {_first_line(str(exc))}",
            path="lease.held",
            code=ExitCode.REFUSED,
        )

    from lakebench.k8s.target import ContextConflictError

    if isinstance(exc, ContextConflictError):
        # A refusal, not a retryable failure: a fresh process would pin to
        # whatever the kubeconfig names now.
        return LakebenchError(
            f"Refused: {_first_line(str(exc))}",
            next="Point the kubeconfig back at the deployment's cluster, or set "
            "platform.kubernetes.context, and re-run.",
            path="context.changed",
            code=ExitCode.REFUSED,
        )

    from lakebench.config import ConfigError, ConfigProtectedCorpusError, ConfigValidationError

    if isinstance(exc, ConfigProtectedCorpusError):
        details = "; ".join(str(err.get("msg", "")) for err in exc.errors)
        return LakebenchError(
            "Refused: the config names a protected AML corpus.",
            why=details or None,
            next="Registered looks run only through `scripts/aml_gate.py --registered`; "
            "use the calibration seed or another unregistered seed for everything else.",
            path="run.protected_corpus",
            code=ExitCode.USAGE,
        )
    if isinstance(exc, ConfigValidationError):
        details = "; ".join(
            f"{'.'.join(str(x) for x in err.get('loc', ()))}: {err.get('msg', '')}"
            for err in exc.errors
        )
        return LakebenchError(
            f"Config validation failed: {_first_line(str(exc))}",
            why=details or None,
            path="config.validation",
            code=ExitCode.USAGE,
        )
    if isinstance(exc, ConfigError):
        return LakebenchError(
            f"Config error: {_first_line(str(exc))}",
            path="config.validation",
            code=ExitCode.USAGE,
        )

    from lakebench.k8s import K8sConnectionError

    k8s_config_exc: tuple[type[BaseException], ...] = ()
    try:
        from kubernetes.config import ConfigException

        k8s_config_exc = (ConfigException,)
    except ImportError:  # pragma: no cover -- kubernetes is a hard dependency
        pass
    if isinstance(exc, K8sConnectionError) or (k8s_config_exc and isinstance(exc, k8s_config_exc)):
        return LakebenchError(
            f"Cannot reach the Kubernetes cluster: {_first_line(str(exc))}",
            next="Check `kubectl config current-context` and that the API server answers.",
            path="k8s.unreachable",
            code=ExitCode.PREREQUISITE,
        )

    message = _first_line(str(exc))
    return LakebenchError(
        f"{type(exc).__name__}: {message}" if message else type(exc).__name__,
        next=None if _debug() else f"Set {DEBUG_ENV}=1 for the traceback.",
        path="unhandled_exception",
        code=ExitCode.FAILED,
    )


def exit_code_for(exc: BaseException) -> ExitCode | None:
    """The exit code *exc* maps to, or None when it passes through."""
    err = error_for(exc)
    return None if err is None else err.code


def quiet_urllib3() -> None:
    """Silence urllib3's retry log lines and warnings for this process.

    An unreachable API server otherwise prints a "Retrying (Retry(...))"
    trace per attempt before the one-line error.
    """
    import logging
    import warnings

    if _debug():
        return  # LAKEBENCH_DEBUG=1 keeps the retry trace for diagnosis
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    warnings.filterwarnings("ignore", module="urllib3")
    try:
        import urllib3

        urllib3.disable_warnings()
    except ImportError:  # pragma: no cover -- a dependency of kubernetes and boto3
        pass


def _report(exc: BaseException, err: LakebenchError) -> None:
    """Print *err*; never raise, so the exit code survives a closed stderr."""
    try:
        if err.path == "unhandled_exception" and _debug():
            traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
        if isinstance(exc, KeyboardInterrupt) or _is_abort(exc):
            # A prompt cut off by EOF or Ctrl-C leaves the cursor after it.
            sys.stderr.write("\n")
        from lakebench.cli._helpers import emit_error

        emit_error(err)
    except (OSError, AttributeError, ValueError):  # stderr closed, None or gone
        pass
    except SystemExit:
        # Rich answers a BrokenPipeError on stderr by pointing stdout at
        # /dev/null and raising SystemExit(1); keep our code instead.
        pass


class LakebenchGroup(TyperGroup):
    """Root group: maps exceptions from any command to the documented codes."""

    def invoke(self, ctx: Any) -> Any:
        from lakebench.cli import _json

        quiet_urllib3()
        # A JSON run left behind by an invocation that bypassed this group
        # (a sub-app invoked directly) never leaks into this one.
        _json.abandon()
        # The subcommand's name is in the protected args (Typer's Click keeps
        # them in ``_protected_args``), its options in ``args``.
        protected = getattr(ctx, "_protected_args", None) or getattr(ctx, "protected_args", None)
        _json.start_from_args(self, [*(protected or []), *ctx.args])
        _noted_paths.clear()
        try:
            rv = super().invoke(ctx)
        except BaseException as exc:
            err = error_for(exc)
            if err is not None:
                _write_exit_path(int(err.code), [p for p in (err.path,) if p] + _noted_paths)
            else:
                passed_code = _exit_code_of(exc)
                _write_exit_path(1 if passed_code is None else passed_code, list(_noted_paths))
            if _json.active():
                # --json: one document on every way out, its exit_code the
                # process's own.
                if err is not None:
                    _report(exc, err)
                    _json.add_error(
                        err.what,
                        code=int(err.code),
                        path=err.path,
                        why=err.why,
                        next=err.next,
                        where=err.where,
                    )
                    _json.finish(int(err.code), keep_data=False)
                    raise typer.Exit(int(err.code)) from exc
                code = _exit_code_of(exc)
                if code is None:
                    _json.abandon()
                    raise
                if code and hasattr(exc, "format_message"):  # a Click usage error
                    _json.add_error(exc.format_message(), code=code, path="click.usage")
                _json.finish(code)
                raise
            if err is None:
                raise
            _report(exc, err)
            raise typer.Exit(int(err.code)) from exc
        finally:
            _json.root_done()
        _write_exit_path(0, list(_noted_paths))
        if _json.active():
            _json.finish(0)
        return rv


def _exit_code_of(exc: BaseException) -> int | None:
    """The exit code a pass-through exception ends the process with:
    typer.Exit / SystemExit, or 2 for a Click usage error; None otherwise."""
    code = getattr(exc, "exit_code", None)
    if isinstance(code, int):
        return code
    if isinstance(exc, SystemExit):
        return exc.code if isinstance(exc.code, int) else (0 if exc.code is None else 1)
    return None
