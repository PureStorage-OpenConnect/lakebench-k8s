"""The CLI reference is generated, and the docs use only live commands (CLI-7).

Two directions:

* code to doc: ``docs/cli-reference.md``'s command blocks are what
  ``scripts/gen_cli_reference.py`` writes from the Typer app, byte for byte;
* doc to code: every ``lakebench ...`` command line in ``README.md``,
  ``docs/**/*.md`` (not ``docs/internal/``, not the generated blocks) and
  ``examples/`` parses with Click (indented and ``~~~`` fences, inline
  spans, table cells and the generated blocks included), names no unknown
  command or flag and no bad value, and
  uses an alias, a refused command or a refused or aliased flag of
  ``lakebench.cli._aliases``. The deprecated ``info`` and ``recommend``
  stay verbs (design C5b) and may be named.
"""

from __future__ import annotations

import functools
import importlib.util
import re
import shlex
from pathlib import Path

import pytest
import typer

try:  # newer Typer ships its own Click; its exceptions are not click's
    from typer._click import exceptions as click_exc
except ImportError:  # pragma: no cover -- Typer built on the click package
    from click import exceptions as click_exc  # type: ignore[no-redef]

from lakebench.cli import app
from lakebench.cli._aliases import (
    ALIASED_FLAGS,
    ALIASES,
    REFUSED,
    REFUSED_FLAGS,
)

ROOT = Path(__file__).resolve().parents[1]


def _generator():
    spec = importlib.util.spec_from_file_location(
        "gen_cli_reference", ROOT / "scripts" / "gen_cli_reference.py"
    )
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_cli_reference_generated():
    assert _generator().drift() == [], "run: python3.11 scripts/gen_cli_reference.py"


# ---------------------------------------------------------------------------
# Doc to code
# ---------------------------------------------------------------------------

# A fence may be indented (a code block inside a list item) and use ``` or ~~~.
_FENCE = re.compile(r"^[ \t]*(```|~~~)[^\n]*\n(.*?)^[ \t]*\1", re.S | re.M)
_INLINE = re.compile(r"`(lakebench [^`]+)`")
_ENV = re.compile(r"^[A-Z_][A-Z0-9_]*=\S*$")
#: Shell words that end the command's own arguments (redirections, background).
#: (``<id>`` is a placeholder, not an input redirection.)
_SHELL_END = re.compile(r"^(?:\d?>>?\S*|&|<<?)$")
#: Wrappers that run the next word as the command.
_WRAPPERS = {"sudo", "env", "time", "watch", "uvx", "nohup"}


def doc_files() -> list[Path]:
    files = [ROOT / "README.md"]
    files += sorted(p for p in (ROOT / "docs").rglob("*.md") if "internal" not in p.parts)
    examples = ROOT / "examples"
    if examples.is_dir():
        files += sorted(p for p in examples.rglob("*") if p.suffix in (".md", ".sh", ".txt"))
    return files


def _commands_in_line(line: str) -> list[list[str]]:
    """The ``lakebench`` invocations on one shell line, as argument lists."""
    line = line.strip()
    if line.startswith("$ "):
        line = line[2:]
    line = line.replace("\\|", "|")  # a pipe escaped in a Markdown table cell
    out = []
    for part in re.split(r"\s*(?:&&|\|\||;|\|)\s*", line):
        try:
            words = shlex.split(part, comments=True)
        except ValueError:
            continue
        while words and (_ENV.match(words[0]) or words[0] in _WRAPPERS):
            words = words[1:]
        if len(words) >= 3 and words[1:3] == ["-m", "lakebench"] and "python" in words[0]:
            words = ["lakebench", *words[3:]]
        if words and words[0] == "lakebench":
            args = words[1:]
            end = next((i for i, w in enumerate(args) if _SHELL_END.match(w)), len(args))
            out.append(args[:end])
    return out


def doc_commands(text: str) -> list[tuple[int, list[str]]]:
    """``(line number, args)`` for each ``lakebench`` command in a document:
    every line of a fenced block, and each inline code span (its args start
    with ``INLINE``: prose may name a flag without its value)."""
    found: list[tuple[int, list[str]]] = []
    for m in _FENCE.finditer(text):
        start = text.count("\n", 0, m.start(2)) + 1
        # A continued line (trailing backslash) is one command; it is
        # reported at the line it starts on.
        k = 0
        for logical in re.split(r"(?<!\\)\n", m.group(2)):
            for args in _commands_in_line(logical.replace("\\\n", " ")):
                found.append((start + k, args))
            k += logical.count("\n") + 1
    outside = _FENCE.sub(lambda m: "\n" * m.group(0).count("\n"), text)
    for m in _INLINE.finditer(outside):
        line_no = outside.count("\n", 0, m.start()) + 1
        for args in _commands_in_line(m.group(1)):
            found.append((line_no, [INLINE, *args]))
    return found


#: Marks an inline span's args in ``doc_commands``.
INLINE = "\x00inline"


#: Placeholder words that stand for "the rest of the line".
_PLACEHOLDERS = {"...", "[OPTIONS]", "[options]"}

#: Flags a doc may name before the work item that adds them merges, mapped to
#: that work item (``init --from`` left with CC-16).
PLANNED_FLAGS: dict[tuple[str, str], str] = {}


def _placeholder(word: str) -> bool:
    """A command position written as a placeholder (``COMMAND``, ``<cmd>``)."""
    return word.startswith(("<", "[")) or bool(re.fullmatch(r"[A-Z_]+", word))


@functools.cache
def _tree():
    return typer.main.get_command(app)


def problem(args: list[str]) -> str | None:
    """Why *args* (after ``lakebench``) is not a live command line, or None."""
    inline = bool(args) and args[0] == INLINE
    args = [a for a in args if a not in _PLACEHOLDERS and a != INLINE]
    cmd = _tree()
    path: list[str] = []
    ctx = None
    rest = list(args)
    while hasattr(cmd, "commands"):
        words = [a for a in rest if not a.startswith("-")]
        if not words:
            return None  # a group named alone, or with options only
        name = words[0]
        if _placeholder(name):
            return None  # a synopsis line, not a command
        if rest[0] != name:
            return f"options before the command: {' '.join(rest)}"
        sub = cmd.commands.get(name)
        if sub is None:
            return f"no command `{' '.join([*path, name])}`"
        path.append(name)
        rest = rest[1:]
        cmd = sub
    full = " ".join(path)
    if full in ALIASES:
        return f"`{full}` is an alias of `{ALIASES[full].target}`; use that"
    if full in REFUSED:
        return f"`{full}` is refused"
    if rest and f"{full} {rest[0].lower()}" in REFUSED:
        return f"`{full} {rest[0]}` is refused"
    for flag in rest:
        name = flag.split("=", 1)[0]
        for table, kind in ((REFUSED_FLAGS, "refused"), (ALIASED_FLAGS, "an old flag")):
            if name in table.get(full, {}):
                return f"`{full} {name}` is {kind}"
    if "--help" in rest or "-h" in rest:
        return None
    kept: list[str] = []
    it = iter(rest)
    for a in it:
        if (full, a.split("=", 1)[0]) in PLANNED_FLAGS:
            if "=" not in a:
                next(it, None)  # and its value
            continue
        kept.append(a)
    rest = kept
    try:
        cmd.make_context(full, rest, parent=ctx, resilient_parsing=False)
    except click_exc.MissingParameter:
        return None  # a doc line may show part of a command
    except click_exc.UsageError as e:  # unknown flag, bad value, extra argument
        msg = e.format_message()
        if "does not exist" in msg:
            return None  # a path checked on disk: the doc's file is an example
        if inline and "requires an argument" in msg:
            return None  # prose names a flag without its value
        return f"`{full}`: {msg}"
    except (click_exc.Exit, click_exc.Abort):
        return None
    return None


def test_docs_use_live_commands():
    failures = []
    for path in doc_files():
        for line_no, args in doc_commands(path.read_text()):
            why = problem(args)
            if why:
                failures.append(
                    f"{path.relative_to(ROOT)}:{line_no}: lakebench {' '.join(args)}: {why}"
                )
    assert failures == [], "\n" + "\n".join(failures)


def test_docs_are_found():
    """Not vacuous: the scan sees the commands the README shows."""
    cmds = [
        [a for a in args if a != INLINE]
        for _, args in doc_commands((ROOT / "README.md").read_text())
    ]
    assert any(a[:1] == ["init"] for a in cmds)
    assert sum(1 for p in doc_files() for _ in doc_commands(p.read_text())) > 200


@pytest.mark.parametrize(
    ("line", "why"),
    [
        ("lakebench results lakebench.yaml", "is an alias of"),
        ("lakebench clean bronze my.yaml", "is refused"),
        ("lakebench config upgrade old.yaml", "is refused"),
        ("lakebench compare a.yaml b.yaml --keep", "is refused"),
        ("lakebench run c.yaml --no-such-flag", "No such option"),
        ("lakebench frobnicate", "no command"),
        ("lakebench admin install-spark-operator c.yaml", "is an alias of"),
        ("lakebench init --interactive", "is an old flag"),
    ],
)
def test_a_planted_line_fails(line, why, tmp_path):
    doc = f"Intro.\n\n```bash\n{line}\n```\n"
    found = doc_commands(doc)
    assert found, line
    assert why in (problem(found[0][1]) or ""), line


def test_inline_and_continued_lines_are_parsed():
    doc = (
        "Run `lakebench results` now.\n\n```bash\nlakebench run c.yaml \\\n  --no-such-flag\n```\n"
    )
    problems = [problem(a) for _, a in doc_commands(doc)]
    assert any("alias" in (p or "") for p in problems)
    assert any("No such option" in (p or "") for p in problems)


def test_live_lines_pass():
    for line in (
        "lakebench run lakebench.yaml --generate --yes",
        "lakebench report 20260201-143052-a1b2c3 --format json",
        "lakebench admin install --component all lakebench.yaml",
        "lakebench config recommend lakebench.yaml",
        "LAKEBENCH_LEGACY_SHORT_F=1 lakebench destroy c.yaml --force",
        "lakebench logs c.yaml trino --follow  # tail it",
    ):
        (args,) = _commands_in_line(line)
        assert problem(args) is None, line


def test_planned_flags_are_not_live_yet():
    """An entry leaves PLANNED_FLAGS when its work item adds the flag."""
    tree = _tree()
    for (command, flag), wi in PLANNED_FLAGS.items():
        cmd = tree
        for word in command.split():
            cmd = cmd.commands[word]
        opts = {o for p in cmd.params for o in getattr(p, "opts", [])}
        assert flag not in opts, f"{command} {flag} is live ({wi}); delete it from PLANNED_FLAGS"


@pytest.mark.parametrize(
    "line",
    [
        "lakebench run c.yaml --repeat 50",
        "lakebench logs c.yaml trino --lines 0",
        "lakebench run c.yaml --timeout",
    ],
)
def test_bad_values_fail(line):
    (args,) = _commands_in_line(line)
    assert problem(args), line


def test_indented_fences_redirections_and_wrappers():
    doc = "1. Step:\n\n   ```bash\n   lakebench results c.yaml\n   ```\n"
    assert any("alias" in (problem(a) or "") for _, a in doc_commands(doc))
    for line in (
        "lakebench report c.yaml --format json > out.json",
        "lakebench run c.yaml --generate --yes 2>&1 &",
        "nohup lakebench run c.yaml --yes",
        "lakebench init --from old.yaml -o new.yaml",
        "lakebench report --run <id> --format csv",
    ):
        (args,) = _commands_in_line(line)
        assert problem(args) is None, line


def test_a_flag_without_value_fails_in_a_block_not_in_prose():
    block = "```bash\nlakebench report --run\n```\n"
    prose = "Pass `lakebench report --run` a run id.\n"
    assert problem(doc_commands(block)[0][1])
    assert problem(doc_commands(prose)[0][1]) is None
