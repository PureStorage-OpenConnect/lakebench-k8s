#!/usr/bin/env python3
"""Regenerate the command reference blocks in docs/cli-reference.md from the CLI.

The generator walks ``typer.main.get_command(app)``, groups included, and
skips hidden commands and options (the aliases and refusals of
``lakebench.cli._aliases``). For each visible command it writes one block
between ``<!-- BEGIN GENERATED: cli <command> -->`` and
``<!-- END GENERATED: cli <command> -->``:

- the usage line;
- the arguments and the options (names, short form, type, default, help);
- the exit paths of ``lakebench.exit_codes.PATHS`` named after the command,
  live ones only (the shared paths are in docs/exit-codes.md);
- for ``run``, the refused arguments of ``cli/_run_args.RUN_RULES``.

A group's block holds each of its visible subcommands. One more block,
``cli aliases``, lists the renamed, refused and deprecated names from
``lakebench.cli._aliases``. Hand-written prose
lives outside the blocks. A visible command with no block, or a block for
a command that is gone, fails ``--check``.

Usage:
    python3.11 scripts/gen_cli_reference.py           # rewrite the blocks
    python3.11 scripts/gen_cli_reference.py --check   # exit 1 on drift

``--check`` writes nothing; ``tests/test_cli_reference.py`` runs the same
comparison in the unit suite.
"""

from __future__ import annotations

import argparse
import enum
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

DOC = "docs/cli-reference.md"
PROG = "lakebench"

#: Exit path prefixes that belong to a command besides its own name.
EXTRA_PATH_PREFIXES: dict[str, tuple[str, ...]] = {
    "run": ("repeat.", "series.", "capacity."),
}


def _app() -> Any:
    import typer

    from lakebench.cli import app

    return typer.main.get_command(app)


def commands(root: Any = None) -> list[tuple[str, Any]]:
    """``(path, command)`` for every visible top-level command and group, in
    the order the CLI lists them."""
    root = root if root is not None else _app()
    return [(name, cmd) for name, cmd in root.commands.items() if not getattr(cmd, "hidden", False)]


def _subcommands(group: Any, prefix: str) -> list[tuple[str, Any]]:
    out: list[tuple[str, Any]] = []
    for name, cmd in group.commands.items():
        if getattr(cmd, "hidden", False):
            continue
        path = f"{prefix} {name}"
        if hasattr(cmd, "commands"):
            out += _subcommands(cmd, path)
        else:
            out.append((path, cmd))
    return out


def _cell(text: Any) -> str:
    return " ".join(str(text).split()).replace("|", "\\|")


def _is_option(p: Any) -> bool:
    return getattr(p, "param_type_name", "") == "option"


def _metavar(p: Any) -> str:
    name = getattr(p, "metavar", None) or p.name.upper()
    return str(name).strip("[]")


def usage(path: str, cmd: Any) -> str:
    parts = [PROG, path]
    for p in cmd.params:
        if _is_option(p) or getattr(p, "hidden", False):
            continue
        mv = _metavar(p)
        parts.append(mv if p.required else f"[{mv}]")
    if any(_is_option(p) and not getattr(p, "hidden", False) for p in cmd.params):
        parts.append("[OPTIONS]")
    return " ".join(parts)


def _type_text(p: Any) -> str:
    if getattr(p, "is_flag", False) and not getattr(p, "secondary_opts", None):
        return "flag"
    if getattr(p, "is_flag", False):
        return "flag"
    t = p.type
    choices = getattr(t, "choices", None)
    if choices:
        return " \\| ".join(f"`{c.value if isinstance(c, enum.Enum) else c}`" for c in choices)
    name = getattr(t, "name", "text") or "text"
    text = {"str": "text", "int": "integer"}.get(str(name).lower(), str(name).lower())
    if getattr(p, "multiple", False):
        text += ", repeatable"
    return text


def _default_text(p: Any) -> str:
    d = p.default
    if callable(d):
        return ""
    if getattr(p, "is_flag", False):
        if getattr(p, "secondary_opts", None):
            return f"`{p.opts[0] if d else p.secondary_opts[0]}`" if d is not None else ""
        return "`true`" if d else ""
    if d is None or d == () or d == []:
        return ""
    if isinstance(d, enum.Enum):
        d = d.value
    if isinstance(d, bool):
        return f"`{str(d).lower()}`"
    return f"`{d}`"


def _names(p: Any) -> tuple[str, str]:
    opts = [o for o in [*p.opts, *getattr(p, "secondary_opts", [])] if o]
    longs = [o for o in opts if o.startswith("--")]
    shorts = [o for o in opts if not o.startswith("--")]
    return " / ".join(f"`{o}`" for o in longs), " ".join(f"`{o}`" for o in shorts)


def _help(p: Any) -> str:
    return _cell(getattr(p, "help", None) or "")


def _first_line(text: str | None) -> str:
    return _cell((text or "").strip().split("\n\n", 1)[0])


def exit_paths(path: str) -> list[Any]:
    from lakebench.exit_codes import PATHS

    words = path.split()
    prefixes = (f"{'.'.join(words)}.", *EXTRA_PATH_PREFIXES.get(path, ()))
    return [p for p in PATHS if p.live and p.name.startswith(prefixes)]


def render_command(path: str, cmd: Any, *, heading: bool = False) -> list[str]:
    lines: list[str] = []
    if heading:
        lines += [f"#### `{path}`", "", _first_line(cmd.help), ""]
    lines += ["```", usage(path, cmd), "```", ""]
    args = [p for p in cmd.params if not _is_option(p) and not getattr(p, "hidden", False)]
    if args:
        lines += ["| Argument | Required | Description |", "|---|---|---|"]
        for p in args:
            req = "yes" if p.required else "no"
            lines.append(f"| `{_cell(_metavar(p))}` | {req} | {_help(p)} |")
        lines.append("")
    opts = [
        p
        for p in cmd.params
        if _is_option(p) and not getattr(p, "hidden", False) and p.name != "help"
    ]
    if opts:
        lines += ["| Flag | Short | Type | Default | Description |", "|---|---|---|---|---|"]
        for p in opts:
            long_, short = _names(p)
            lines.append(
                f"| {long_} | {short} | {_type_text(p)} | {_default_text(p)} | {_help(p)} |"
            )
        lines.append("")
    paths = exit_paths(path)
    if paths:
        lines.append("Exit paths named after this command:")
        lines.append("")
        lines += [f"- `{int(p.code)}` `{p.name}`: {_cell(p.when)}" for p in paths]
        lines.append("")
    if path == "run":
        lines += _run_rules()
    return lines


def _run_rules() -> list[str]:
    from lakebench.cli._run_args import RUN_RULES

    out = [
        "**Refused arguments.** `run` checks every option before it makes any",
        "cluster call, and exits 2 (usage) naming the first refused one:",
        "",
    ]
    for i, rule in enumerate(RUN_RULES):
        out.append(f"- {rule.doc}{'.' if i == len(RUN_RULES) - 1 else ';'}")
    out.append("")
    return out


def render_aliases() -> str:
    """The renamed, refused and deprecated names, from ``cli/_aliases.py``."""
    from lakebench.cli import _aliases as al

    rows = ["| Old | What happens | Use instead |", "|---|---|---|"]
    for old, a in al.ALIASES.items():
        rows.append(
            f"| `{old}` | alias: one line on stderr, then runs the new command; "
            f"removed in {al.REMOVED_IN} | `{a.target}` |"
        )
    for old, new in al.DEPRECATED_COMMANDS.items():
        rows.append(f"| `{old}` | hidden and deprecated since 1.3; still runs | `{new}` |")
    for command, flags in al.ALIASED_FLAGS.items():
        for flag, note in flags.items():
            rows.append(f"| `{command} {flag}` | accepted; {_cell(note)} | |")
    for old, r in al.REFUSED.items():
        rows.append(
            f"| `{old}` | refused (exit 2): {_cell(r.reason)} | {_cell(r.replacement or 'nothing')} |"
        )
    for command, flags in al.REFUSED_FLAGS.items():
        by_refusal: dict[Any, list[str]] = {}
        for flag, r in flags.items():
            by_refusal.setdefault(r, []).append(flag)
        for r, names in by_refusal.items():
            listed = ", ".join(f"`{command} {f}`" for f in names)
            rows.append(
                f"| {listed} | refused (exit 2): {_cell(r.reason)} | "
                f"{_cell(r.replacement or 'nothing')} |"
            )
    return "\n".join(rows)


#: Blocks that are not one command's reference.
SPECIAL_BLOCKS = {"aliases": render_aliases}


def render(path: str, cmd: Any) -> str:
    if path in SPECIAL_BLOCKS:
        return SPECIAL_BLOCKS[path]()
    if hasattr(cmd, "commands"):
        lines: list[str] = []
        for sub_path, sub in _subcommands(cmd, path):
            lines += render_command(sub_path, sub, heading=True)
    else:
        lines = render_command(path, cmd)
    return "\n".join(lines).rstrip("\n")


def expected_block(path: str, cmd: Any) -> str:
    return (
        f"<!-- BEGIN GENERATED: cli {path} (scripts/gen_cli_reference.py) -->\n"
        f"{render(path, cmd)}\n"
        f"<!-- END GENERATED: cli {path} -->"
    )


_BLOCK = re.compile(
    r"<!-- BEGIN GENERATED: cli (?P<path>[a-z0-9 -]+?)( \(scripts/gen_cli_reference\.py\))? -->"
    r".*?<!-- END GENERATED: cli (?P=path) -->",
    re.S,
)


def blocks_in(text: str) -> dict[str, str]:
    return {m.group("path"): m.group(0) for m in _BLOCK.finditer(text)}


def drift(root: Path = REPO_ROOT) -> list[str]:
    text = (root / DOC).read_text()
    found = blocks_in(text)
    problems: list[str] = []
    live: dict[str, Any] = {**dict(commands()), **dict.fromkeys(SPECIAL_BLOCKS)}
    for path, cmd in live.items():
        if path not in found:
            problems.append(f"{DOC}: no block for `lakebench {path}` (add its markers)")
        elif found[path] != expected_block(path, cmd):
            problems.append(f"{DOC}: block for `lakebench {path}` is stale")
    for path in found:
        if path not in live:
            problems.append(f"{DOC}: block for `lakebench {path}`, which is not a visible command")
    return problems


def regenerate(root: Path = REPO_ROOT) -> bool:
    path = root / DOC
    text = path.read_text()
    found = blocks_in(text)
    live: dict[str, Any] = {**dict(commands()), **dict.fromkeys(SPECIAL_BLOCKS)}
    missing = [p for p in live if p not in found]
    if missing:
        raise SystemExit(f"{DOC}: no block for {', '.join(missing)}; add the markers first")
    gone = [p for p in found if p not in live]
    if gone:
        raise SystemExit(f"{DOC}: blocks for commands that are not visible: {', '.join(gone)}")
    new = text
    for p, block in found.items():
        new = new.replace(block, expected_block(p, live[p]))
    if new != text:
        path.write_text(new)
        return True
    return False


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if a block is stale")
    args = ap.parse_args(argv)
    if args.check:
        problems = drift()
        for p in problems:
            print(p, file=sys.stderr)
        if problems:
            print("Run: python3.11 scripts/gen_cli_reference.py", file=sys.stderr)
            return 1
        return 0
    if regenerate():
        print(f"updated {DOC}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
