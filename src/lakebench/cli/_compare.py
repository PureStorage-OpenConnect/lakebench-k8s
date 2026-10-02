"""``lakebench compare SIDE_A SIDE_B``: compare stored run records.

Read-only. compare reads metrics.json files (and series manifests) and
writes nothing unless ``-o`` is given; it deploys, runs and destroys
nothing and makes no cluster call. This module imports no cluster client.
Resolution, the verdict, the missing condition and the per-metric
assessment are in ``metrics/compare.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

from lakebench._constants import DEFAULT_OUTPUT_DIR
from lakebench.cli._helpers import emit_data, err_console, esc, print_error, print_warning
from lakebench.exit_codes import ExitCode

console = Console()

_FORMATS = ("table", "json", "csv")


def removed_flag_message(side_a: str, side_b: str) -> str:
    """What a flag of the command that ran both configs is refused with."""

    def cfg(ref: str, default: str) -> str:
        return ref if ref.endswith((".yaml", ".yml")) else default

    a, b = cfg(side_a, "a.yaml"), cfg(side_b, "b.yaml")
    return (
        "compare reads stored records and no longer runs configs. Run each side first: "
        f"`lakebench run {a}` and `lakebench run {b}` (add `--repeat 3` for repeated "
        f"runs), then `lakebench compare {a} {b}`."
    )


def compare(
    side_a: Annotated[
        str,
        typer.Argument(
            help="Side A (the baseline): run ids, run directories, metrics.json files, "
            "series:<id> or a config, comma-separated"
        ),
    ],
    side_b: Annotated[str, typer.Argument(help="Side B (the candidate), in the same forms")],
    runs_dir: Annotated[
        list[Path] | None,
        typer.Option(
            "--runs-dir",
            help="Directory of run-<id>/ records (repeatable; default lakebench-output/runs)",
        ),
    ] = None,
    output_format: Annotated[
        str, typer.Option("--format", help="Output format: table, json, csv")
    ] = "table",
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write the comparison (json, or csv) to this file"),
    ] = None,
    # The flags of the command that ran both configs: declared (hidden) so
    # each is refused with the replacement, not read as an unknown option.
    # The value flags take a string so any value reaches the refusal.
    keep: Annotated[bool, typer.Option("--keep", hidden=True)] = False,
    scale: Annotated[str | None, typer.Option("--scale", hidden=True)] = None,
    skip_benchmark: Annotated[bool, typer.Option("--skip-benchmark", hidden=True)] = False,
    timeout: Annotated[str | None, typer.Option("--timeout", hidden=True)] = None,
    local: Annotated[bool, typer.Option("--local", hidden=True)] = False,
    generate: Annotated[bool, typer.Option("--generate", hidden=True)] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y", hidden=True)] = False,
) -> None:
    """Compare two sides of stored run records.

    Each side is one or more stored runs: run ids, run directories,
    metrics.json paths, ``series:<id>``, or a config (its latest run, or
    every member of that run's series). compare prints how each side
    resolved, the verdict, the one condition the pair is missing and the
    command that supplies it, then each metric's medians and spread. It
    never runs, deploys or destroys anything, and names no winner.

    Exit codes: 0 like-for-like, 10 NOT COMPARABLE, 11 NOT ESTABLISHED,
    12 comparable but not like-for-like, 13 confounded, 2 usage (a ref that
    resolves to nothing, the same runs on both sides, an unreadable record,
    a removed flag).
    """
    from lakebench.metrics import compare as cmpmod

    if any((keep, scale is not None, skip_benchmark, timeout is not None, local, generate, yes)):
        print_error(removed_flag_message(side_a, side_b))
        raise typer.Exit(ExitCode.USAGE)
    fmt = output_format.lower()
    if fmt == "html":
        print_error(
            "--format html is not supported by `compare`. Use `lakebench report --render` "
            "for an HTML report of one run (or --format json / --format csv here)."
        )
        raise typer.Exit(ExitCode.USAGE)
    if fmt not in _FORMATS:
        print_error(f"--format {output_format!r} is not supported. Use one of: table, json, csv.")
        raise typer.Exit(ExitCode.USAGE)

    dirs = list(runs_dir) if runs_dir else [Path(DEFAULT_OUTPUT_DIR) / "runs"]
    try:
        a, b = cmpmod.resolve(side_a, side_b, dirs)
    except cmpmod.CompareError as e:
        print_error(str(e))
        raise typer.Exit(ExitCode.USAGE) from None

    # A run on a protected AML corpus (evaluation or robustness, by role or
    # by seed hash) is read only by its registered look: refused, exit 2.
    from lakebench.aml.look_guard import refuse_protected_records

    refuse_protected_records(
        ((m.run_id, m.record) for side in (a, b) for m in side.members), "compare"
    )

    if output is not None:
        problem = _output_problem(output, [*a.inputs, *b.inputs], dirs)
        if problem:
            print_error(f"-o {output} {problem}; write the comparison elsewhere")
            raise typer.Exit(ExitCode.USAGE)

    doc = cmpmod.build_comparison(a, b)
    hidden = cmpmod.hidden_for(a, b)
    # The resolution comes first, on stderr, so machine output stays clean.
    for side in (a, b):
        line = cmpmod.redact(cmpmod.resolution_line(side), hidden)
        err_console.print(esc(line), highlight=False, soft_wrap=True)
    for w in doc["warnings"]:
        print_warning(w)

    if fmt == "table":
        _print_table(doc)
    elif output is None:
        emit_data(cmpmod.to_csv(doc) if fmt == "csv" else cmpmod.to_json(doc))

    if output is not None:
        text = cmpmod.to_csv(doc) if fmt == "csv" else cmpmod.to_json(doc)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text if text.endswith("\n") else text + "\n")
        err_console.print(f"[dim]Comparison written to {esc(output)}[/dim]")

    code = int(doc["exit_code"])
    if code:
        raise typer.Exit(code)


def _output_problem(output: Path, inputs: list[Path], runs_dirs: list[Path]) -> str | None:
    """Why ``-o`` may not be written: it is an input, a run record, or a
    file inside a runs or series directory."""
    target = output.resolve()
    for p in inputs:
        try:
            same = target == p.resolve() or (output.exists() and output.samefile(p))
        except OSError:
            same = False
        if same:
            return "is one of the inputs"
    if output.name == "metrics.json":
        return "would overwrite a run record"
    for d in runs_dirs:
        for root in (d.resolve(), d.resolve().parent / "series"):
            if target.is_relative_to(root):
                return f"is inside {root}"
    return None


_VERDICT_STYLE = {
    "LIKE-FOR-LIKE": "bold",
    "NOT LIKE-FOR-LIKE": "bold yellow",
    "CONFOUNDED": "bold yellow",
    "NOT ESTABLISHED": "bold yellow",
    "NOT COMPARABLE": "bold red",
}


def _num(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        if v == int(v) and abs(v) < 1e12:
            return str(int(v))
        return f"{v:.4g}" if abs(v) < 1e4 else f"{v:,.0f}"
    return str(v)


def _cell(s: dict[str, Any] | None) -> str:
    if not s:
        return "-"
    if s["n"] == 1:
        return _num(s["median"])
    return f"{_num(s['median'])} ({_num(s['min'])}-{_num(s['max'])}, n={s['n']})"


def _print_table(doc: dict[str, Any]) -> None:
    """The verdict line, the missing condition and its command, the groups
    that differ, then one row per metric. No colour marks a better side."""
    verdict = str(doc["verdict"])
    style = _VERDICT_STYLE.get(verdict, "bold")
    head = f"[{style}]{esc(verdict)}[/{style}]"
    if doc.get("attribution"):
        head += f" ({esc(doc['attribution'])})"
    console.print(head)
    for r in doc.get("reasons") or []:
        console.print(f"  {esc(r)}", highlight=False, soft_wrap=True)
    missing = doc.get("missing") or {}
    if missing.get("hint"):
        console.print(f"Missing: {esc(missing['condition'])}", highlight=False)
        console.print(f"  {esc(missing['hint'])}", highlight=False, soft_wrap=True)
    for group, diffs in (doc.get("groups") or {}).items():
        keys = ", ".join(str(d["key"]) for d in diffs)
        console.print(f"[dim]{esc(group)} differs: {esc(keys)}[/dim]")
    for n in doc.get("notes") or []:
        console.print(f"[dim]note: {esc(n)}[/dim]")

    rows = doc.get("metrics") or []
    if not rows:
        console.print("No metrics recorded on either side.")
        return
    table = Table(show_header=True, header_style="bold")
    table.add_column("Metric", overflow="fold")
    table.add_column("A (median, range, n)", justify="right")
    table.add_column("B (median, range, n)", justify="right")
    table.add_column("Delta of medians", justify="right")
    table.add_column("Assessment")
    for r in rows:
        delta = r.get("delta_pct")
        dtext = "-" if delta is None else f"{delta:+.2f}%"
        assessment = str(r["assessment"])
        if r.get("rounds"):
            assessment += f": {r['rounds']}"
        if r.get("capped_by"):
            assessment += " (BOUNDED BY " + ", ".join(r["capped_by"]) + ")"
        table.add_row(
            esc(r["metric"]),
            esc(_cell(r.get("a"))),
            esc(_cell(r.get("b"))),
            dtext,
            esc(assessment),
        )
    console.print(table)
    if not doc.get("winner_rule"):
        console.print("[dim]No winner is named: the winner rule is not in this release.[/dim]")
