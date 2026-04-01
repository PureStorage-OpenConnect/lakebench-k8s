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
