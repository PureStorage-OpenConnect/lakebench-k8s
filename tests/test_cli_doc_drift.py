"""Doc-vs-CLI drift check.

Parses ``docs/cli-reference.md`` and asserts every flag it documents actually
exists on the corresponding Typer command. Written for Phase B3 of v1.6 UX
review after several drift instances (a documented ``--no-wait`` that the
code never registered, ``query --file`` documented with its direction
backwards, a ``run --force-rebuild`` in code that no doc mentioned).

The check is CLI-surface-based: for each documented command it invokes
``<command> --help`` through Typer's ``CliRunner`` at a wide terminal width
and pulls the option list from the rendered help, then compares that to
the flags parsed out of the reference's flag tables. This matches what a
user sees, not what the source annotations suggest.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from lakebench.cli import app

# --- Discovery --------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
CLI_REFERENCE = REPO_ROOT / "docs" / "cli-reference.md"

# Level-3 (``### <name>``) headings that name a real top-level Typer command.
# Sub-command groups (config, admin, financial) are documented as compact
# tables that do not follow the ``| Flag | Short | ...`` shape, so they are
# out of scope for this test.
TOP_LEVEL_COMMANDS: tuple[str, ...] = (
    "init",
    "compare",
    "validate",
    "deploy",
    "generate",
    "run",
    "stop",
    "benchmark",
    "query",
    "status",
    "info",
    "recommend",
    "clean",
    "destroy",
    "report",
    "results",
    "logs",
    "journal",
    "reproduce",
    "version",
)

# Common options the intro documents once for every command. Never flagged as
# missing from a command's own flag table.
COMMON_OPTIONS: frozenset[str] = frozenset({"--file", "--help"})

# Flags that only appear in tables but the CLI recognises under a different
# name because of Typer aliasing. `--yes` on destroy/clean is a real alias
# for `--force` (same param, three decls), so the doc listing either is fine.
KNOWN_ALIASES: dict[str, dict[str, str]] = {
    "destroy": {"--yes": "--force"},
    "clean": {"--yes": "--force"},
}

FLAG_RE = re.compile(r"--[a-zA-Z][a-zA-Z0-9-]*")
# First-column cell of a flag row: ``| `--wait` |`` or ``| `--foo` / `--bar` |``
# followed by whatever short-form and description columns exist.
TABLE_ROW_RE = re.compile(r"^\|\s*(?P<flag>`--[^|`]+`(?:\s*/\s*`--[^|`]+`)?)\s*\|")


def _flags_from_row(cell: str) -> set[str]:
    r"""Extract every ``--foo`` token from a table's first cell.

    Handles single flags (``\`--wait\```), alias pairs
    (``\`--force\` / \`--yes\```) and inline notes such as
    ``\`--skip-preflight\` (alias \`--skip-deploy\`)``.
    """

    return set(FLAG_RE.findall(cell))


def _parse_doc_flags() -> dict[str, set[str]]:
    """Walk cli-reference.md and collect the documented flags per command.

    Only top-level headings in TOP_LEVEL_COMMANDS are considered. A flag
    table's rows are the ones whose first cell is a backticked ``--foo``
    (the intro's ``| Flag | Short | Default | Description |`` header row
    itself is skipped, since its first cell is the literal word ``Flag``).
    """

    if not CLI_REFERENCE.exists():
        pytest.skip(f"CLI reference not found at {CLI_REFERENCE}")

    text = CLI_REFERENCE.read_text(encoding="utf-8")
    current: str | None = None
    result: dict[str, set[str]] = {c: set() for c in TOP_LEVEL_COMMANDS}

    for line in text.splitlines():
        # Track the current level-3 heading. A deeper (####) heading does
        # not reset the current top-level command, so ``#### config storage``
        # inside ``### config`` stays inside ``config``.
        h3 = re.match(r"^###\s+(\S+)\s*$", line)
        if h3:
            name = h3.group(1).strip()
            current = name if name in TOP_LEVEL_COMMANDS else None
            continue
        if current is None:
            continue

        m = TABLE_ROW_RE.match(line)
        if not m:
            continue
        result[current].update(_flags_from_row(m.group("flag")))

    return result


def _actual_flags(command: str) -> set[str]:
    """The set of long-form options ``<command> --help`` shows.

    Terminal width is forced high so Typer / Click do not wrap and truncate
    long option names such as ``--allow-unverified-cluster``.
    """

    runner = CliRunner()
    # Some versions of Click read COLUMNS instead of the runner's width arg.
    old_columns = os.environ.get("COLUMNS")
    os.environ["COLUMNS"] = "800"
    try:
        result = runner.invoke(app, [command, "--help"], terminal_width=800)
    finally:
        if old_columns is None:
            os.environ.pop("COLUMNS", None)
        else:
            os.environ["COLUMNS"] = old_columns
    assert result.exit_code == 0, (
        f"lakebench {command} --help exited {result.exit_code}:\n{result.output}"
    )
    return set(FLAG_RE.findall(result.output))


DOC_FLAGS = _parse_doc_flags()


@pytest.mark.parametrize("command", TOP_LEVEL_COMMANDS)
def test_every_documented_flag_exists_in_cli(command: str) -> None:
    """Each ``--foo`` in the reference must exist on the actual command."""

    documented = DOC_FLAGS.get(command, set())
    if not documented:
        # Commands with no per-flag table (version, info) trivially pass.
        return

    actual = _actual_flags(command)
    aliases = KNOWN_ALIASES.get(command, {})

    missing: list[str] = []
    for flag in sorted(documented):
        if flag in COMMON_OPTIONS or flag in actual:
            continue
        if aliases.get(flag) in actual:
            continue
        missing.append(flag)

    assert not missing, (
        f"lakebench {command}: flags in docs/cli-reference.md that do not "
        f"exist on the command: {missing}. Actual CLI flags: {sorted(actual)}"
    )


def test_generate_has_no_no_wait_flag() -> None:
    """Regression pin: docs must not add ``--no-wait`` to ``generate``.

    The code registers ``--wait`` explicitly, so Typer does not synthesise
    the paired ``--no-wait``. Documenting one would mislead operators into
    trying a flag the CLI rejects. Phase A4 owns adding it to the code;
    until then the doc says only ``--wait``.
    """

    generate_flags = DOC_FLAGS.get("generate", set())
    assert "--no-wait" not in generate_flags, (
        "docs/cli-reference.md documents --no-wait for generate, but the "
        "code (src/lakebench/cli/_generate.py) only registers --wait. "
        "Remove --no-wait from the reference; adding it to the code is "
        "Phase A4 territory."
    )
    actual = _actual_flags("generate")
    assert "--no-wait" not in actual, (
        "generate --help now shows --no-wait; update the reference and delete this pin."
    )


def test_query_sql_file_flag_documented_not_file() -> None:
    """Regression pin: reading SQL from a file uses ``--sql-file``.

    Before LB-027 the flag was ``--file`` (colliding with the config-file
    option). The rename shipped in v1.2; documenting ``--file`` for SQL
    input again would reintroduce the collision and confuse operators
    who now expect ``--file`` to mean the config.
    """

    query_flags = DOC_FLAGS.get("query", set())
    assert "--sql-file" in query_flags, (
        "docs/cli-reference.md no longer lists --sql-file in the query "
        "flag table; it is the correct name for the SQL-file input."
    )
    actual = _actual_flags("query")
    assert "--sql-file" in actual, (
        "lakebench query --help does not offer --sql-file; the SQL file "
        "input flag was renamed or removed."
    )
