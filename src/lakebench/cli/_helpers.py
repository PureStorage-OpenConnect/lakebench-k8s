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
from rich.markup import escape
from rich.text import Text

from lakebench.exit_codes import ExitCode, LakebenchError
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

    console.print(
        f"[red]ERROR[/red] No config file specified and ./{esc(DEFAULT_CONFIG)} not found"
    )
    console.print("[blue]INFO[/blue] Create one with: lakebench init")
    raise typer.Exit(ExitCode.USAGE)


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


def esc(value: object) -> str:
    """``str(value)`` with Rich markup escaped.

    For values interpolated into an f-string that goes to ``console.print``:
    without it ``[/tmp]`` raises ``MarkupError`` and ``[main]`` vanishes.
    """
    return escape(str(value))


def markup(text: str) -> str:
    """Mark *text* as Rich markup on purpose (identity).

    The markup-safety lint accepts an f-string value wrapped in ``markup()``
    (or in a function whose name ends in ``_markup``); use it only for text
    built from literals, such as a coloured PASS/FAIL label.
    """
    return text


def _status_line(prefix: str, style: str, message: object) -> None:
    """``prefix message`` on stderr; the message is never parsed as markup.

    ``soft_wrap`` keeps a long message on one line, so a pipe or a log grep
    sees it whole.
    """
    line = Text(prefix, style=style)
    line.append(" ")
    line.append(str(message))
    err_console.print(line, soft_wrap=True)


def print_success(message: object) -> None:
    """Print a success message on stderr (text is printed verbatim)."""
    _status_line("OK", "green", message)


def print_error(message: object) -> None:
    """Print an error message on stderr (text is printed verbatim)."""
    _status_line("ERROR", "red", message)


def print_warning(message: object) -> None:
    """Print a warning message on stderr (text is printed verbatim)."""
    _status_line("WARN", "yellow", message)


def emit_error(err: LakebenchError) -> None:
    """Print *err* on stderr in the TUD 11.1 shape, at most four lines.

    ``ERROR what``, then ``Why``, ``Next`` and ``Where`` only when set. No
    field is parsed as markup and none is wrapped.
    """
    for label, text in err.lines():
        line = Text(label.ljust(6), style="red" if label == "ERROR" else "bold")
        line.append(" ")
        line.append(text)
        err_console.print(line, soft_wrap=True)


def emit_data(text: str) -> None:
    """Write machine output (JSON, CSV) to plain stdout.

    Not through Rich: Rich wraps at the terminal width and parses markup,
    which breaks parsers on long values or brackets.
    """
    import sys

    sys.stdout.write(text if text.endswith("\n") else text + "\n")
    sys.stdout.flush()


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
        raise typer.Exit(ExitCode.USAGE)
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
        f"[yellow]WARN[/yellow] '-f' here is deprecated: use {esc(new_spelling)}. In a future "
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
        f"[red]ERROR[/red] '-f' no longer skips confirmation here: use {esc(new_spelling)}. "
        "'-f' will mean --file (the config path), as it does on every other command. "
        f"Set {esc(LEGACY_SHORT_F_ENV)}=1 to keep the old meaning for this release."
    )
    raise typer.Exit(ExitCode.USAGE)


def print_info(message: object) -> None:
    """Print an info message on stderr (text is printed verbatim)."""
    _status_line("...", "blue", message)


def _journal_safe(fn, *args, **kwargs) -> None:
    """Call a journal function, logging failures instead of silently dropping them."""
    global _journal_warned
    try:
        fn(*args, **kwargs)
    except Exception:
        if not _journal_warned:
            logger.debug("Journal write failed (further warnings suppressed)", exc_info=True)
            _journal_warned = True


def stop_previous_datagen_or_exit(cfg, what: str = "Refusing to generate") -> None:
    """``stop_previous_datagen``; a refusal or an unknown pod state exits.

    Called before the bronze gate (``generate``, ``run --generate``, the
    multi-cycle loop) and before a continuous reset, so no pod of an
    earlier datagen Job writes after the prefix is checked or cleared.
    Live pods exit 3 (``datagen.pods_live``); pods that cannot be listed
    or a Job that cannot be deleted exit 1.
    """
    from lakebench.deploy.datagen import DatagenRefused, stop_previous_datagen
    from lakebench.exit_codes import path_code

    try:
        stop_previous_datagen(cfg)
    except DatagenRefused as e:
        print_error(f"{what}: {e}")
        raise typer.Exit(path_code(e.exit_path) if e.exit_path else ExitCode.FAILED) from None


def enforce_bronze_gate(
    cfg, regenerate: bool, allow_stale_bronze: bool = False, clear_owned: bool = False
):
    """Run the bronze gate before datagen; a refusal exits with its code.

    ``lakebench.deploy.datagen.bronze_prefix_gate`` decides (one table for
    every caller: ``generate``, ``run --generate``, the multi-cycle loop
    before cycle 0). A non-empty datagen prefix is refused unless
    ``--regenerate`` (a bucket this deployment may empty: the datagen prefix
    is cleared) or ``--allow-stale-bronze`` (any other bucket: datagen writes
    over the objects and the run records it). ``--regenerate`` never clears a
    bucket this deployment did not create. Returns the gate's result; a refusal exits with the gate's code
    (refused, ``run.bronze_nonempty``; prerequisite when bronze cannot be read).
    """
    from lakebench.deploy.datagen import bronze_prefix_gate

    result = bronze_prefix_gate(
        cfg,
        regenerate=regenerate,
        allow_stale_bronze=allow_stale_bronze,
        clear_owned=clear_owned,
    )
    if not result.proceed:
        print_error(result.message)
        raise typer.Exit(result.exit_code)
    record_stale_bronze(cfg, result.record())
    shown = f"s3://{result.bucket}/{result.prefix}"
    if result.cleared:
        print_success(f"cleared {result.cleared} object(s) under {shown} before generating")
    if result.stale_allowed:
        print_warning(
            f"{shown} held {result.objects_before} object(s) before generate and this "
            "deployment did not create the bucket; generating over them "
            "(--allow-stale-bronze). Rows may be over-counted; the scale-ratio check "
            "flags it."
        )
    return result


def _stale_bronze_path(cfg) -> Path:
    from lakebench._constants import DEFAULT_OUTPUT_DIR

    return Path(DEFAULT_OUTPUT_DIR) / "datagen" / f"{cfg.get_namespace()}-stale-bronze.json"


def record_stale_bronze(cfg, record: dict | None) -> None:
    """Keep the gate's ``datagen.stale_bronze`` for a later ``run``.

    ``generate`` writes no metrics, so a ``run`` after it reads the record
    from here; a generate that wrote over nothing removes it.
    """
    import json

    path = _stale_bronze_path(cfg)
    try:
        if record is None:
            path.unlink(missing_ok=True)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record, sort_keys=True))
    except OSError as e:
        print_warning(f"could not record the stale-bronze note at {path}: {e}")


def load_stale_bronze(cfg) -> dict | None:
    """The ``datagen.stale_bronze`` record the last generate left, if any.

    Only a note for this config's bronze bucket and datagen prefix counts; a
    note for another bucket (the config changed) is ignored.
    """
    import json

    from lakebench.deploy.datagen import bronze_datagen_prefix

    try:
        data = json.loads(_stale_bronze_path(cfg).read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("bucket") != cfg.platform.storage.s3.buckets.bronze or data.get(
        "prefix"
    ) != bronze_datagen_prefix(cfg).strip("/"):
        return None
    return data


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


def load_deps_handle(cfg, config_file=None):
    """The deployment's verified dependency set for the Spark jobs:
    called before anything is recorded or submitted. A typed refusal
    (``run.deps_missing``, ``run.deps_stale``, ``run.deps_mismatch``)
    propagates to the CLI's error handler."""
    from lakebench.deps import runtime

    handle = runtime.load_handle(cfg, None, config_path=config_file)
    print_info(f"Dependency set {handle.pinset_sha256[:12]} verified")
    return handle


def record_deps_provenance(run, handle) -> None:
    """``provenance.deps`` from the run-start handle."""
    from lakebench.deps.manifest import provenance_block

    if run is not None and handle is not None:
        run.provenance = {**(run.provenance or {}), "deps": provenance_block(handle)}


def record_deps_pods(run, cfg, handle, skipped: str | None = None) -> bool:
    """The run-end check that the query engine pods ran the run's set
    (``provenance.deps.pods_checked`` and ``pod_mismatches``). True when the
    run must fail: a pod on another set, or pods that could not be read.
    With ``skipped`` (why: the run was interrupted, the namespace went,
    nothing reached the cluster) nothing is read, and the record says why
    in ``pods_check_skipped``."""
    from lakebench.deps import runtime

    if run is None or handle is None or not isinstance((run.provenance or {}).get("deps"), dict):
        return False
    if skipped:
        run.provenance["deps"]["pods_check_skipped"] = skipped
        return False
    result = runtime.check_pods(cfg, handle, run.start_time)
    run.provenance["deps"].update(result)
    if result.get("pods_checked") is None:
        print_warning(f"Query engine dependency set not checked: {result.get('pods_check_error')}")
    elif result.get("pod_mismatches"):
        print_error(
            "Pods ran different dependency sets: "
            + ", ".join(f"{p['pod']} on {p['pinset'][:12]}" for p in result["pod_mismatches"])
        )
    return bool(result.get("pod_mismatches")) or result.get("pods_checked") is None
