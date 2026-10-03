"""Old command names and flags: the ones that still work, and the ones refused.

Two tables are the one list of what 1.7 renamed or removed from the CLI:

* ``ALIASES``: an old command that still runs its replacement. It prints
  exactly one stderr line naming the new command (``alias_notice``), then
  runs it. ``ALIASED_FLAGS`` holds the flags that still parse but do
  nothing, each with its one line.
* ``REFUSED``: an old command, or a command with an argument, that is
  refused with exit 2 (``alias.refused``, or the command's own path) and the
  replacement, without echoing any argument. ``REFUSED_FLAGS`` holds the
  refused flags of commands that remain.

The commands read their lines from here. The documentation lint fails on a
doc line that uses any entry, so docs only ever show the live command.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

from lakebench._constants import DEFAULT_OUTPUT_DIR
from lakebench.cli._helpers import DEPRECATED_SHORT_F_HELP, print_error, warn_deprecated_short_f
from lakebench.exit_codes import ExitCode, UsageError

#: When the aliases stop working.
REMOVED_IN = "v1.8"


@dataclass(frozen=True)
class Alias:
    """``target``: the command line that replaces the old one."""

    target: str


@dataclass(frozen=True)
class Refusal:
    """``replacement``: what to run instead, or None when nothing replaces
    it; ``reason``: why it was removed."""

    replacement: str | None
    reason: str


ALIASES: dict[str, Alias] = {
    "results": Alias("report --format table"),
    "admin install-spark-operator": Alias("admin install --component spark-operator"),
    "admin install-scratch-storage-class": Alias("admin install --component scratch-storage-class"),
}

#: Flags that still parse and do nothing but print one line.
_WIZARD_REMOVED = "the init wizard is removed; init writes a default config (see init --help)"
ALIASED_FLAGS: dict[str, dict[str, str]] = {
    "init": dict.fromkeys(("--interactive", "-i", "--advanced"), _WIZARD_REMOVED),
}

_REGENERATE = Refusal(
    "lakebench run CONFIG --generate --regenerate",
    "a run regenerates its own corpus, so the corpus a record names is the one it read",
)
_EVIDENCE = Refusal(None, "evidence is not deleted by the CLI")

REFUSED: dict[str, Refusal] = {
    "config upgrade": Refusal(
        "lakebench init --from OLD.yaml -o NEW.yaml",
        "it rewrote configs lossily and wrote secrets in plaintext",
    ),
    "clean bronze": _REGENERATE,
    "clean data": _REGENERATE,
    "clean metrics": _EVIDENCE,
    "clean journal": _EVIDENCE,
}

_COMPARE_RUNS = Refusal(
    "lakebench run A.yaml --repeat 3; lakebench run B.yaml --repeat 3; "
    "lakebench compare A.yaml B.yaml",
    "compare reads stored records and no longer runs configs",
)
_INIT_CREDENTIALS = Refusal(
    "--credentials-env PREFIX, or export LAKEBENCH_S3_ACCESS_KEY and LAKEBENCH_S3_SECRET_KEY",
    "init writes a ${VAR} reference, never a key",
)

#: Refused flags of commands that remain (each command refuses its own).
REFUSED_FLAGS: dict[str, dict[str, Refusal]] = {
    "compare": dict.fromkeys(
        (
            "--keep",
            "--generate",
            "--local",
            "--skip-benchmark",
            "--timeout",
            "--scale",
            "--yes",
            "-y",
        ),
        _COMPARE_RUNS,
    ),
    "init": {"--access-key": _INIT_CREDENTIALS, "--secret-key": _INIT_CREDENTIALS},
}


def alias_notice(old: str) -> None:
    """The one stderr line an alias prints before running its target."""
    target = ALIASES[old].target
    typer.echo(
        f"`lakebench {old}` is now `lakebench {target}`; the old name is removed in {REMOVED_IN}",
        err=True,
    )


def refusal(old: str, *, path: str = "alias.refused") -> UsageError:
    """The error a refused command raises: what was removed, why, and what
    to run instead. Never carries an argument the caller gave."""
    r = REFUSED[old]
    return UsageError(
        f"`lakebench {old}` is removed: {r.reason}",
        next=r.replacement or "nothing replaces it",
        path=path,
    )


def results(
    target: Annotated[
        str | None,
        typer.Argument(
            metavar="[RUN|CONFIG]",
            help="A run id or a configuration YAML file, as for `report`",
            show_default=False,
        ),
    ] = None,
    metrics_dir: Annotated[
        Path,
        typer.Option(
            "--metrics",
            "-m",
            help="Directory containing run subdirectories",
        ),
    ] = Path(DEFAULT_OUTPUT_DIR) / "runs",
    run_id: Annotated[
        str | None,
        typer.Option(
            "--run",
            "-r",
            help="Specific run ID (default: latest)",
        ),
    ] = None,
    output_format: Annotated[
        str | None,
        typer.Option(
            "--format",
            "-o",
            help="Output format: table, json, csv (default: table)",
        ),
    ] = None,
    format_short_f: Annotated[
        str | None,
        typer.Option("-f", hidden=True, help=DEPRECATED_SHORT_F_HELP),
    ] = None,
) -> None:
    """Alias of `lakebench report --format table` (removed in v1.8)."""
    alias_notice("results")
    if format_short_f is not None:
        warn_deprecated_short_f("--format / -o")
        if output_format is not None and output_format != format_short_f:
            print_error(f"both --format {output_format} and -f {format_short_f} given")
            raise typer.Exit(ExitCode.USAGE)
        output_format = format_short_f
    from lakebench.cli import report

    report(
        target=target,
        metrics_dir=metrics_dir,
        run_id=run_id,
        list_runs=False,
        render=False,
        output_path=None,
        force=False,
        summary=False,
        output_format=output_format or "table",
    )


def register(app: typer.Typer) -> None:
    """Add the top-level aliases to *app*, hidden from help and the
    generated reference. The ``admin`` aliases live in ``cli/_admin.py`` and
    ``config upgrade`` in ``cli/_config.py``; both read these tables."""
    app.command("results", hidden=True)(results)
