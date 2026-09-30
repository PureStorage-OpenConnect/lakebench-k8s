"""Shared CLI helpers for Lakebench.

Extracted from cli/__init__.py so that submodules (_sustained.py,
_compare.py, _config.py) can import these without circular dependencies.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import typer
from rich.console import Console

from lakebench.journal import Journal

logger = logging.getLogger(__name__)

# Default config file name for auto-discovery
DEFAULT_CONFIG = "lakebench.yaml"

# Exit code for a declined interactive confirmation prompt (C3, v1.6).
# Distinct from 0 (success) and 1 (failure) so wrapper scripts can tell a
# "user said no" apart from either. Every CLI site that reaches a
# ``typer.confirm`` and takes the "no" branch exits with this code.
EXIT_DECLINED = 3

# Exit code when datagen exceeds its wait budget (A4, v1.6). Distinct from
# 1 (generic failure), 2 (usage/refusal), 3 (declined prompt) and 4
# (EXIT_NAMESPACE_STILL_TERMINATING in cli/_destroy.py). A wrapper that
# retries on 4 must not retry on this; a wrapper that watches for 5 must
# not treat it as namespace-still-terminating.
EXIT_DATAGEN_TIMEOUT = 5

# ANSI escape code stripper for log output
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

# Shared console instance -- all CLI modules use this one
console = Console()
# Warnings that must not mix into machine-readable stdout (results -o json)
err_console = Console(stderr=True)

# Global journal instance (lazy-initialized)
_journal: Journal | None = None

# Suppress repeated journal warnings
_journal_warned = False


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    return _ANSI_RE.sub("", text)


def resolve_config_path(
    config_file: Path | None,
    file_option: Path | None = None,
) -> Path:
    """Resolve config file path, using ./lakebench.yaml as default."""
    path = file_option or config_file
    if path is not None:
        return path

    default = Path(DEFAULT_CONFIG)
    if default.exists():
        return default

    console.print(f"[red]ERROR[/red] No config file specified and ./{DEFAULT_CONFIG} not found")
    console.print("[blue]INFO[/blue] Create one with: lakebench init")
    raise typer.Exit(1)


def get_journal() -> Journal:
    """Get or create the global journal instance."""
    global _journal
    if _journal is None:
        _journal = Journal()
    return _journal


def journal_open(config_path: Path | None, config_name: str = "") -> Journal:
    """Open journal session for a command that loads config."""
    j = get_journal()
    j.open_session(config_path=config_path, config_name=config_name)
    return j


def print_success(message: str) -> None:
    """Print a success message."""
    console.print(f"[green]OK[/green] {message}")


def print_error(message: str) -> None:
    """Print an error message."""
    console.print(f"[red]ERROR[/red] {message}")


def print_warning(message: str) -> None:
    """Print a warning message."""
    console.print(f"[yellow]WARN[/yellow] {message}")


def check_datagen_scale(cfg: object) -> None:
    """Refuse a scale above the workload's datagen ceiling; warn above the
    largest measured scale (config/support.py DATAGEN_SCALE_BANDS)."""
    from lakebench.config.support import UNSUPPORTED, datagen_scale_problem

    arch = cfg.architecture  # type: ignore[attr-defined]
    band = datagen_scale_problem(
        arch.workload.schema_type.value, float(arch.workload.datagen.get_effective_scale())
    )
    if band is None:
        return
    state, basis = band
    if state == UNSUPPORTED:
        print_error(f"Unsupported scale, refused: {basis}")
        raise typer.Exit(1)
    print_warning(f"Unverified scale: {basis}")


# ``-f`` means ``--file`` (the config path) on every command. Commands where it
# used to mean something else keep the old meaning for one release behind a
# hidden option and call this, so scripts keep working and users see the move.
DEPRECATED_SHORT_F_HELP = "Deprecated short flag; see the warning it prints."


def warn_deprecated_short_f(new_spelling: str) -> None:
    """Warn that this command's ``-f`` is deprecated in favour of *new_spelling*.

    Goes to stderr so that, for example, ``results -f json | jq`` still parses.
    """
    err_console.print(
        f"[yellow]WARN[/yellow] '-f' here is deprecated: use {new_spelling}. In a future "
        "release '-f' will mean --file (the config path), as it does on every other command."
    )


def stdin_is_tty() -> bool:
    """True only for an interactive stdin; a closed stdin (None) is not one."""
    import sys

    stdin = sys.stdin
    try:
        return stdin is not None and stdin.isatty()
    except (AttributeError, ValueError):  # closed or replaced stream
        return False


LEGACY_SHORT_F_ENV = "LAKEBENCH_LEGACY_SHORT_F"


def deprecated_short_f_force(new_spelling: str, force_given: bool) -> bool:
    """Handle ``-f`` on destroy/clean, where it used to mean --force.

    ``-f`` means --file on every other command, so on a destructive command
    it is refused outright rather than treated as consent to skip the
    confirmation: AI agents, IDE task runners and ``ssh host cmd`` all run
    without a terminal, so a TTY test cannot tell a script from a person.
    Scripts that need the old meaning for one release set
    LAKEBENCH_LEGACY_SHORT_F=1. When --force is also given, ``-f`` is
    redundant and only warned about. Returns the resulting force value.
    """
    import os

    if force_given:
        warn_deprecated_short_f(new_spelling)
        return True
    if os.environ.get(LEGACY_SHORT_F_ENV) == "1":
        warn_deprecated_short_f(new_spelling)
        return True
    err_console.print(
        f"[red]ERROR[/red] '-f' no longer skips confirmation here: use {new_spelling}. "
        "'-f' will mean --file (the config path), as it does on every other command. "
        f"Set {LEGACY_SHORT_F_ENV}=1 to keep the old meaning for this release."
    )
    raise typer.Exit(2)


def print_info(message: str) -> None:
    """Print an info message."""
    console.print(f"[blue]...[/blue] {message}")


def _journal_safe(fn, *args, **kwargs) -> None:
    """Call a journal function, logging failures instead of silently dropping them."""
    global _journal_warned
    try:
        fn(*args, **kwargs)
    except Exception:
        if not _journal_warned:
            logger.debug("Journal write failed (further warnings suppressed)", exc_info=True)
            _journal_warned = True


def enforce_bronze_regenerate(cfg, regenerate: bool) -> None:
    """Refuse to start datagen over a non-empty bronze prefix (A4, v1.6).

    Before A4 both ``lakebench generate`` and ``run --generate`` deployed
    datagen straight onto whatever was in bronze; the deployer's LB-185
    clear ran only for buckets this deployment recorded creating, so any
    other case silently over-wrote existing part-* files. This gate is the
    CLI-level counterpart: without ``--regenerate`` the command exits 2 and
    names the prefix, so the operator opts in explicitly instead of
    discovering the wipe later. With ``--regenerate`` the whole bronze
    bucket is emptied via ``S3Client.empty_bucket`` (which also aborts
    dangling multipart uploads on FlashBlade) before datagen submits.
    """
    from lakebench.deploy.datagen import bronze_datagen_prefix
    from lakebench.s3 import S3Client

    prefix = bronze_datagen_prefix(cfg)
    prefix_norm = prefix.strip("/")
    s3_cfg = cfg.platform.storage.s3
    bucket = s3_cfg.buckets.bronze
    s3 = S3Client(
        endpoint=s3_cfg.endpoint,
        access_key=s3_cfg.access_key,
        secret_key=s3_cfg.secret_key,
        region=s3_cfg.region,
        path_style=s3_cfg.path_style,
        ca_cert=s3_cfg.ca_cert,
        verify_ssl=s3_cfg.verify_ssl,
    )
    if s3._init_error:
        # No S3 available: cannot check safely, so refuse rather than proceed
        # blind and wipe under --regenerate what we cannot even list.
        print_error(
            f"Cannot check bronze bucket {bucket} for existing data "
            f"({s3._init_error}); refusing to generate."
        )
        raise typer.Exit(2)
    try:
        list_prefix = prefix_norm + "/" if prefix_norm else ""
        info = s3.get_bucket_size(bucket, prefix=list_prefix)
    except Exception as e:  # noqa: BLE001
        print_error(f"Could not list bronze prefix s3://{bucket}/{prefix}: {e}")
        raise typer.Exit(2) from e
    object_count = info.object_count or 0
    if not info.exists or object_count == 0:
        return
    if not regenerate:
        size_gb = (info.size_bytes or 0) / (1024**3)
        print_error(
            f"Bronze prefix s3://{bucket}/{prefix} is not empty "
            f"({object_count} object(s), {size_gb:.2f} GB). "
            "Refusing to generate over it: pass --regenerate to empty the "
            "bronze bucket first, or --skip-generate to reuse the existing "
            "data."
        )
        raise typer.Exit(2)
    # empty_bucket does NOT accept a Prefix filter today (LB-185's helper
    # aborts multipart uploads across the bucket to catch FlashBlade
    # ghosts, so the wipe is bucket-wide). Anything else the bronze bucket
    # holds (streaming checkpoints, previous-run scratch, a co-tenant that
    # shares this bucket via a distinct path_template) goes with it.
    # Prefix-scoped regenerate is a follow-up.
    print_info(
        f"--regenerate: emptying the ENTIRE bucket s3://{bucket} before "
        f"datagen. Datagen prefix {prefix!r} has {object_count} object(s); "
        "any other prefixes in the same bucket (streaming checkpoints, "
        "previous-run scratch, or a co-tenant sharing this bucket) will "
        "also be removed."
    )
    try:
        deleted = s3.empty_bucket(bucket)
    except Exception as e:
        print_error(f"--regenerate: could not empty s3://{bucket}: {e}")
        raise typer.Exit(1) from e
    print_success(f"--regenerate: removed {deleted} object(s) from s3://{bucket}")


def write_run_report(metrics_storage, run_id: str) -> Path | None:
    """Write report.html next to a run's saved metrics.json (P1.4).

    Every run writes its report, failed ones included: a run that exits 1
    still saved metrics for diagnosis, and the report is how they are read.
    Report rendering never changes the run's outcome; a failure here is
    printed as a warning and returns None.

    This is the one and only in-place delivery of ``<run_dir>/report.html``.
    ``ReportGenerator.generate_report`` refuses to write that path unless a
    caller passes it as ``output_path`` (see ``reports/generator.py``); this
    helper is that caller. ``force=True`` covers the edge case of a saved
    run that already has a ``report.html`` from an earlier attempt: the
    delivery is idempotent for the same run id.
    """
    try:
        from lakebench.reports import ReportGenerator

        run_dir = metrics_storage.run_dir(run_id)
        delivered = run_dir / "report.html"
        path = ReportGenerator(metrics_storage.metrics_dir).generate_report(
            run_id, output_path=delivered, force=True
        )
    except Exception as e:  # noqa: BLE001 -- the report must not fail the run
        print_warning(f"Could not write report.html for run {run_id}: {e}")
        return None
    print_info(f"Report written to {path}")
    return path
