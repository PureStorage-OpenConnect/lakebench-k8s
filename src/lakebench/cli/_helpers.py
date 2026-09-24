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


def _stdin_is_tty() -> bool:
    import sys

    return sys.stdin.isatty()


def deprecated_short_f_force(new_spelling: str) -> None:
    """Handle ``-f`` on destroy/clean, where it used to mean --force.

    At a terminal, where a person may have typed ``-f`` expecting it to name
    the config file as it does elsewhere, refuse rather than skip the
    confirmation. In a script, keep the old meaning for this release and warn.
    """
    if _stdin_is_tty():
        err_console.print(
            f"[red]ERROR[/red] '-f' no longer skips confirmation here: use {new_spelling}. "
            "'-f' will mean --file (the config path), as it does on every other command."
        )
        raise typer.Exit(2)
    warn_deprecated_short_f(new_spelling)


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
