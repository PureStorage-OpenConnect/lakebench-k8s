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
