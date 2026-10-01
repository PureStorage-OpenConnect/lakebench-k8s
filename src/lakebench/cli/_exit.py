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
- ``ConfigError`` (including ``ConfigValidationError``): 2;
- ``K8sConnectionError`` and ``kubernetes.config.ConfigException``: 4;
- Click ``Abort`` (a declined ``typer.confirm(abort=True)``, or end of
  input with no terminal) and ``EOFError``: 5;
- ``KeyboardInterrupt``: 130;
- anything else: 1, printed as one line; ``LAKEBENCH_DEBUG=1`` adds the
  traceback.
"""

from __future__ import annotations

import os
import sys
import traceback
from types import ModuleType
from typing import Any

import typer
from typer.core import TyperGroup

from lakebench.exit_codes import (
    MEANINGS,
    PATHS,
    PATHS_BY_NAME,
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
    "print_error_shape",
]

DEBUG_ENV = "LAKEBENCH_DEBUG"


def _click_exceptions() -> ModuleType:
    """The exceptions module of the Click that Typer runs on.

    Typer 0.12 to 0.2x use the ``click`` package; newer Typer vendors it as
    ``typer._click``. ``typer.Exit`` is always that module's ``Exit``.
    """
    return sys.modules[typer.Exit.__module__]


def _debug() -> bool:
    return os.environ.get(DEBUG_ENV, "") not in ("", "0")


def _first_line(text: str) -> str:
    text = text.strip()
    return text.splitlines()[0] if text else ""


def error_for(exc: BaseException) -> LakebenchError | None:
    """The error to report for *exc*, or None when it must pass through.

    Pure apart from the lazy imports: it prints nothing and exits nothing.
    """
    ce = _click_exceptions()
    if isinstance(exc, (ce.Exit, ce.ClickException, SystemExit, GeneratorExit)):
        return None
    if isinstance(exc, LakebenchError):
        return exc
    if isinstance(exc, KeyboardInterrupt):
        return LakebenchError("Interrupted.", path="sigint", code=ExitCode.INTERRUPTED)
    if isinstance(exc, (ce.Abort, EOFError)):
        return LakebenchError(
            "Not confirmed: the prompt was declined or there was no terminal to answer it.",
            next="Answer the prompt, or pass --yes where the command offers it.",
            path="confirm.non_tty",
            code=ExitCode.NOT_CONFIRMED,
        )
    if not isinstance(exc, Exception):
        return None

    from lakebench.config import ConfigError, ConfigValidationError

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


def print_error_shape(err: LakebenchError) -> None:
    """Print *err* on stderr in the ERROR / Why / Next / Where shape.

    Text is never parsed as Rich markup, so ``[/tmp]`` or ``[main]`` in a
    message prints verbatim.
    """
    from rich.text import Text

    from lakebench.cli._helpers import err_console

    styles = {"ERROR": "red"}
    for label, text in err.lines():
        line = Text(label.ljust(6), style=styles.get(label, "bold"))
        line.append(" ")
        line.append(text)
        err_console.print(line, soft_wrap=True)


class LakebenchGroup(TyperGroup):
    """Root group: maps exceptions from any command to the documented codes."""

    def invoke(self, ctx: Any) -> Any:
        try:
            return super().invoke(ctx)
        except BaseException as exc:
            err = error_for(exc)
            if err is None:
                raise
            if err.path == "unhandled_exception" and _debug():
                traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
            if isinstance(exc, (KeyboardInterrupt, EOFError, _click_exceptions().Abort)):
                # A prompt cut off by EOF or Ctrl-C leaves the cursor after it.
                sys.stderr.write("\n")
            print_error_shape(err)
            raise typer.Exit(int(err.code)) from exc
