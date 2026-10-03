"""``--json``: one machine-readable document per command run (``lb-cli/1``).

With ``--json`` a read verb (``plan``, ``status``, ``report``, ``config
recipes``, ``compare``, ``query``) writes exactly one document to stdout::

    {"schema": "lb-cli/1", "command": "status", "exit_code": 1,
     "data": {...} | null,
     "errors": [{"code", "path", "what", "why", "next", "where"}]}

and every human line goes to stderr. The root group
(``cli/_exit.LakebenchGroup``) writes the document on every way out: the
verb's ``data`` on success or on a verdict exit (``status`` drift), and
``data: null`` with the error when the command failed, so ``exit_code`` and
the process's exit code always agree. Each verb's ``data`` has a TypedDict
here; those shapes are the version-1 promise: a key may be added, none is
removed or retyped without a new schema.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from typing import Any, TypedDict

SCHEMA = "lb-cli/1"


class ErrorItem(TypedDict):
    code: int
    path: str | None
    what: str
    why: str | None
    next: str | None
    where: str | None


class Envelope(TypedDict):
    schema: str
    command: str
    exit_code: int
    data: Any
    errors: list[ErrorItem]


# -- data shapes, one per verb -------------------------------------------------


class StatusComponent(TypedDict):
    name: str
    kind: str
    state: str  # ok | unready | missing | error
    detail: str


class StatusData(TypedDict):
    namespace: str
    exists: bool
    verdict: str  # ok | drift | unverified | namespace_missing
    components: list[StatusComponent]
    datagen: dict[str, int] | None  # {"succeeded", "completions", "active"} while it runs


class ReportStage(TypedDict):
    stage_name: str
    stage_type: str
    elapsed_seconds: float
    input_size_gb: float
    output_size_gb: float
    throughput_gb_per_second: float
    executor_count: int


class ReportRun(TypedDict):
    run_id: str
    record_kind: str  # run | benchmark
    parent_run_id: str | None
    deployment_name: str
    start_time: str
    verdict: str | None
    pipeline_mode: str | None
    scores: dict[str, Any]
    stages: list[ReportStage]
    delivered_report: str | None


class ReportListRow(TypedDict):
    run_id: str
    record_kind: str
    parent_run_id: str | None
    deployment_name: str | None
    start_time: str | None
    verdict: str | None
    total_elapsed_seconds: float | None


class ReportListData(TypedDict):
    runs: list[ReportListRow]


class ReportRenderData(TypedDict):
    run_id: str | None
    report: str


class RecipeRow(TypedDict):
    recipe: str
    when: str
    local: bool
    support: dict[str, str]  # "<workload> <mode>": supported | unverified | unsupported


class RecipesData(TypedDict):
    recipes: list[RecipeRow]


class RecipeSupport(TypedDict):
    workload: str
    mode: str
    state: str
    basis: str


class RecipeDetailData(TypedDict):
    recipe: str
    catalog: str
    table_format: str
    query_engine: str
    when: str | None
    local: bool
    support: list[RecipeSupport]
    caveats: list[str]


class QueryData(TypedDict):
    query_name: str
    count: int
    elapsed_seconds: float
    rows: list[dict[str, str]]


class PlanData(TypedDict):
    plans: list[dict[str, Any]]
    differences: list[dict[str, Any]]


#: ``compare``'s data is the ``cmp2`` document (metrics/compare.py), and
#: ``plan``'s each plan is ``cli/_plan.plan_one``'s dict.


# -- the run -------------------------------------------------------------------


@dataclass
class _Run:
    command: str
    data: Any = None
    errors: list[ErrorItem] = field(default_factory=list)
    redirected: list[Any] = field(default_factory=list)


_active: _Run | None = None


def active() -> bool:
    return _active is not None


def _stdout_consoles() -> list[Any]:
    """The module-level Rich consoles that print to stdout."""
    from lakebench.cli import _compare, _config, _financial, _helpers, _recommend

    return [m.console for m in (_helpers, _compare, _config, _financial, _recommend)]


def start(command: str) -> None:
    """Enter JSON mode for *command*: human output goes to stderr until
    ``finish``. Idempotent within one command run."""
    global _active
    if _active is not None:
        return
    run = _Run(command)
    for c in _stdout_consoles():
        # The set file, not the property: unset, Rich follows sys.stdout as
        # it is at each write, and restoring a captured stream would pin it.
        run.redirected.append((c, getattr(c, "_file", None)))
        c.file = sys.stderr
    _active = run


def option_callback(ctx: Any, value: bool) -> bool:
    """The ``--json`` option's callback (eager): enter JSON mode before the
    command body, named by its command path."""
    if value:
        path = ctx.command_path.split(" ", 1)
        start(path[1] if len(path) > 1 else path[0])
    return value


def json_option() -> Any:
    """The ``--json`` option, a fresh instance per command."""
    import typer

    return typer.Option(
        "--json",
        is_eager=True,
        callback=option_callback,
        help=f"Write one {SCHEMA} JSON document to stdout; human text goes to stderr",
    )


def set_data(data: Any) -> None:
    if _active is not None:
        _active.data = data


def add_error(
    what: str,
    *,
    code: int,
    path: str | None = None,
    why: str | None = None,
    next: str | None = None,  # noqa: A002 -- the field name of the error shape
    where: str | None = None,
) -> None:
    if _active is not None:
        _active.errors.append(
            {
                "code": int(code),
                "path": path,
                "what": what,
                "why": why,
                "next": next,
                "where": where,
            }
        )


def note_printed_error(message: object) -> None:
    """A verb's ``print_error`` line, kept for the document's ``errors``
    (its code is the command's exit code, set by ``finish``)."""
    if _active is not None:
        _active.errors.append(
            {
                "code": -1,
                "path": None,
                "what": str(message),
                "why": None,
                "next": None,
                "where": None,
            }
        )


def document(command: str, data: Any, exit_code: int, errors: list[ErrorItem]) -> Envelope:
    return {
        "schema": SCHEMA,
        "command": command,
        "exit_code": int(exit_code),
        "data": data,
        "errors": [{**e, "code": int(exit_code) if e["code"] == -1 else e["code"]} for e in errors],
    }


def finish(exit_code: int, *, keep_data: bool = True) -> None:
    """Write the document for the active run and leave JSON mode. With
    ``keep_data=False`` (the command raised) ``data`` is null."""
    global _active
    run, _active = _active, None
    if run is None:
        return
    for console, previous in run.redirected:
        console._file = previous
    doc = document(run.command, run.data if keep_data else None, exit_code, run.errors)
    try:
        sys.stdout.write(json.dumps(doc, indent=2, default=str) + "\n")
        sys.stdout.flush()
    except (OSError, ValueError):  # stdout closed: the exit code still stands
        pass


def abandon() -> None:
    """Leave JSON mode without writing (the process is going away)."""
    global _active
    run, _active = _active, None
    if run is not None:
        for console, previous in run.redirected:
            console._file = previous
