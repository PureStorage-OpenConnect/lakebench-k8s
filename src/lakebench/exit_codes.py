"""Lakebench exit codes, the typed CLI errors and the named producer paths.

This module imports only the standard library. The release harness and the
scenario shim import ``ExitCode`` from here without loading the CLI (importing
anything under ``lakebench.cli`` runs ``lakebench/cli/__init__.py``, which
builds the whole Typer app and pulls in the Kubernetes client).
``lakebench.cli._exit`` re-exports every name for code inside the CLI.

The codes follow the v1.7 target UX (one enum, generated into the docs):
``docs/exit-codes.md`` is rendered from ``ExitCode`` and ``PATHS`` by
``scripts/gen_exit_codes.py`` and a test fails when the file drifts.

``PATHS`` names each way a command reaches a code. An entry that is not
``planned`` is produced today, and ``tests/test_exit_codes.py`` has a scenario
that drives the real CLI to it. A ``planned`` entry is a path a later change
makes the command produce; until then the docs leave it out.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class ExitCode(IntEnum):
    """Process exit codes of the ``lakebench`` command."""

    OK = 0
    FAILED = 1
    USAGE = 2
    REFUSED = 3
    PREREQUISITE = 4
    NOT_CONFIRMED = 5
    INCOMPLETE = 6
    COMPARE_NOT_COMPARABLE = 10
    COMPARE_NOT_ESTABLISHED = 11
    COMPARE_NOT_LIKE_FOR_LIKE = 12
    COMPARE_CONFOUNDED = 13
    REQUIREMENT_UNMET = 14
    INTERRUPTED = 130


MEANINGS: dict[ExitCode, str] = {
    ExitCode.OK: "Success: the run passed, the read succeeded, or the comparison is like-for-like.",
    ExitCode.FAILED: (
        "Negative verdict of the command's own object: the run failed, a record "
        "was modified, status found drift. Also any error Lakebench did not classify."
    ),
    ExitCode.USAGE: (
        "Usage or config error, nothing ran: a bad flag or flag combination, a config "
        "that fails to load, an unsupported combination."
    ),
    ExitCode.REFUSED: (
        "Refused by the safety or protocol model: identity, ownership, context or "
        "fingerprint mismatch, corpus state, live jobs, a held lease, a spent seed."
    ),
    ExitCode.PREREQUISITE: (
        "Prerequisites not met, nothing ran: an operator or StorageClass missing, a "
        "permission gap, capacity below the peak, the cluster or S3 unreachable."
    ),
    ExitCode.NOT_CONFIRMED: (
        "Not confirmed: a prompt was declined, or there was no terminal to answer it "
        "and the command was not given --yes."
    ),
    ExitCode.INCOMPLETE: "Incomplete and safe to re-run: for example the namespace is still terminating.",
    ExitCode.COMPARE_NOT_COMPARABLE: "compare: NOT COMPARABLE.",
    ExitCode.COMPARE_NOT_ESTABLISHED: "compare: COMPARABILITY NOT ESTABLISHED.",
    ExitCode.COMPARE_NOT_LIKE_FOR_LIKE: "compare: comparable, not like-for-like.",
    ExitCode.COMPARE_CONFOUNDED: "compare: comparable, confounded.",
    ExitCode.REQUIREMENT_UNMET: (
        "Requirement unmet: a reproduction drifted outside its tolerance, was asked "
        "to verify at another commit, or could only be verified out of band."
    ),
    ExitCode.INTERRUPTED: "Interrupted (SIGINT, Ctrl-C).",
}


# -- typed errors ------------------------------------------------------------


class LakebenchError(Exception):
    """An error the CLI reports in the ERROR / Why / Next / Where shape.

    ``what`` is the one-line statement of what went wrong; ``why``, ``next``
    (the fix) and ``where`` (cluster, context, namespace or file) are
    optional. ``path`` is the ``PATHS`` name of the producer, when it has one.
    The CLI's top-level handler prints it and exits with ``code``.
    """

    code: ExitCode = ExitCode.FAILED

    def __init__(
        self,
        what: str,
        *,
        why: str | None = None,
        next: str | None = None,  # noqa: A002 -- the field name of the error shape
        where: str | None = None,
        path: str | None = None,
        code: ExitCode | None = None,
    ) -> None:
        super().__init__(what)
        self.what = what
        self.why = why
        self.next = next
        self.where = where
        self.path = path
        if code is not None:
            self.code = code

    def lines(self) -> list[tuple[str, str]]:
        """The (label, text) pairs of the error shape, unset fields left out."""
        out = [("ERROR", self.what)]
        for label, text in (("Why", self.why), ("Next", self.next), ("Where", self.where)):
            if text:
                out.append((label, text))
        return out


class UsageError(LakebenchError):
    """A bad flag, flag combination or config: nothing ran (exit 2)."""

    code = ExitCode.USAGE


class SafetyRefusal(LakebenchError):
    """Refused by the safety or protocol model (exit 3)."""

    code = ExitCode.REFUSED


class PrerequisiteError(LakebenchError):
    """A prerequisite is missing: nothing ran (exit 4)."""

    code = ExitCode.PREREQUISITE


class NotConfirmed(LakebenchError):
    """A confirmation was declined or could not be asked (exit 5)."""

    code = ExitCode.NOT_CONFIRMED


class Incomplete(LakebenchError):
    """The command stopped part way and is safe to re-run (exit 6)."""

    code = ExitCode.INCOMPLETE


# -- named producer paths ----------------------------------------------------


@dataclass(frozen=True)
class ExitPath:
    """One named way a command reaches an exit code.

    ``planned`` is True for a path no command produces yet; the test suite
    records which change is to produce it.
    ``v16_code`` is the code Lakebench 1.6 exited with on this path, when it
    differs; the UPGRADING table of renumbered codes is built from it.
    """

    name: str
    code: ExitCode
    when: str
    planned: bool = False
    v16_code: int | None = None

    @property
    def live(self) -> bool:
        return not self.planned


_C = ExitCode

PATHS: tuple[ExitPath, ...] = (
    # 0
    ExitPath("version.ok", _C.OK, "`lakebench version` prints the version"),
    ExitPath("run.pass", _C.OK, "`run` finished and its verdict passed"),
    ExitPath(
        "compare.like_for_like", _C.OK, "`compare` finds the sides like-for-like", planned=True
    ),
    ExitPath("status.ok", _C.OK, "`status` finds the deployment as configured", planned=True),
    ExitPath("plan.ok", _C.OK, "`plan` finds every prerequisite and enough capacity", planned=True),
    # 1
    ExitPath(
        "unhandled_exception",
        _C.FAILED,
        "an error Lakebench does not classify; one line, with the traceback only "
        "under LAKEBENCH_DEBUG=1",
    ),
    ExitPath("run.verdict_failed", _C.FAILED, "`run` finished with a failing verdict"),
    ExitPath(
        "run.datagen_timeout",
        _C.FAILED,
        'datagen did not finish in time; the record says "datagen timed out" in verdict.reasons',
        v16_code=5,
    ),
    ExitPath(
        "run.namespace_gone", _C.FAILED, "the namespace disappeared during `run`", planned=True
    ),
    ExitPath(
        "repeat.no_verified_corpus",
        _C.FAILED,
        "`run --repeat` found no verified corpus to reuse",
        planned=True,
    ),
    ExitPath(
        "status.drift",
        _C.FAILED,
        "`status` finds the deployment drifted from its config",
        planned=True,
    ),
    ExitPath("status.namespace_missing", _C.FAILED, "`status` finds no namespace", planned=True),
    ExitPath("stop.api_error", _C.FAILED, "`stop` hit an API error stopping a job", planned=True),
    ExitPath("logs.no_pod", _C.FAILED, "`logs` found no pod for the component", planned=True),
    ExitPath(
        "financial.reproduce.mismatch",
        _C.FAILED,
        "`financial reproduce` ran but did not reproduce the alert",
        planned=True,
    ),
    # 2
    ExitPath("click.usage", _C.USAGE, "an unknown flag, a missing argument or a bad value"),
    ExitPath("config.validation", _C.USAGE, "the config fails to load or validate", v16_code=1),
    ExitPath(
        "config.unsupported",
        _C.USAGE,
        "the workload, recipe and mode combination is unsupported, or the scale is "
        "above the workload's datagen ceiling",
        v16_code=1,
    ),
    ExitPath(
        "cli.bad_argument",
        _C.USAGE,
        "a command refuses an argument it checks itself: an unknown recipe, component, "
        "stage or example, a missing file, conflicting options",
        v16_code=1,
    ),
    ExitPath(
        "config.name_required",
        _C.USAGE,
        "a command that changes data or tears a deployment down was given a config with "
        "no name; a read command (`status`, `logs`, `report`) too, in a directory whose "
        "v1.6 state names a deployment",
        v16_code=0,
    ),
    ExitPath("run.args", _C.USAGE, "a `run` argument or combination is refused", planned=True),
    ExitPath(
        "config.upgrade_refused",
        _C.USAGE,
        "`config upgrade` is removed; the message names `init --from`",
    ),
    ExitPath(
        "run.protected_corpus",
        _C.USAGE,
        "the config names a protected AML corpus role or seed",
        planned=True,
    ),
    ExitPath(
        "alias.refused",
        _C.USAGE,
        "a removed command or flag; the message names the replacement",
        planned=True,
    ),
    ExitPath(
        "compare.equal_names",
        _C.USAGE,
        "`compare` was given two sides with the same name",
        planned=True,
    ),
    ExitPath(
        "admin.version_change_needs_flag",
        _C.USAGE,
        "`admin install` would change a component version without --allow-version-change",
        planned=True,
    ),
    # 3
    ExitPath(
        "reproduce.existing_namespace",
        _C.REFUSED,
        "`reproduce` would reuse a namespace or bucket that already exists",
        planned=True,
        v16_code=2,
    ),
    ExitPath(
        "reproduce.nonce_changed",
        _C.REFUSED,
        "the deployment `reproduce` created was replaced before its destroy",
        planned=True,
    ),
    ExitPath(
        "destroy.incarnation_mismatch",
        _C.REFUSED,
        "`destroy` was told to expect a different deployment incarnation",
        planned=True,
    ),
    ExitPath(
        "destroy.redeployed",
        _C.REFUSED,
        '"Destroy NOT completed": the namespace now belongs to a newer deployment',
        v16_code=1,
    ),
    ExitPath(
        "nameless.nonce_mismatch",
        _C.REFUSED,
        "a nameless config's nonce does not match the namespace",
        planned=True,
    ),
    ExitPath(
        "nameless.copied_dir",
        _C.REFUSED,
        "a nameless config's state directory was copied",
        planned=True,
    ),
    ExitPath(
        "nameless.moved", _C.REFUSED, "a nameless config moved away from its state", planned=True
    ),
    ExitPath(
        "nameless.stamp_mismatch",
        _C.REFUSED,
        "a nameless config's stamp does not match the namespace",
        planned=True,
    ),
    ExitPath(
        "nameless.v17_state_elsewhere",
        _C.REFUSED,
        "the namespace carries v1.7 state that lives with another config",
        planned=True,
    ),
    ExitPath(
        "deploy.identity_foreign",
        _C.REFUSED,
        "the namespace or a bucket is owned by another deployment, or has no lakebench "
        "ownership proof (`deploy`, `destroy`, `clean`)",
        v16_code=1,
    ),
    ExitPath(
        "run.bronze_nonempty",
        _C.REFUSED,
        "datagen would write over a non-empty bronze prefix without --regenerate",
        v16_code=2,
    ),
    ExitPath(
        "series.corpus_changed",
        _C.REFUSED,
        "the corpus changed between repetitions of `run --repeat`",
        planned=True,
    ),
    ExitPath("lease.held", _C.REFUSED, "another command holds the cluster lock lease", v16_code=1),
    ExitPath(
        "destroy.unverified_cluster",
        _C.REFUSED,
        '"Destroy NOT completed": this cluster has no fingerprint, so buckets are kept',
        planned=True,
    ),
    ExitPath(
        "admin.version_change_in_use",
        _C.REFUSED,
        "`admin install` would change a component version that deployments use",
        planned=True,
    ),
    # 4
    ExitPath("run.prereq_failed", _C.PREREQUISITE, "a `run` preflight check failed", v16_code=1),
    ExitPath(
        "capacity.shortfall",
        _C.PREREQUISITE,
        "free cluster capacity is below the run's peak",
        planned=True,
    ),
    ExitPath(
        "capacity.unknown",
        _C.PREREQUISITE,
        "capacity could not be read on every node",
        planned=True,
    ),
    ExitPath(
        "plan.missing_storage_class",
        _C.PREREQUISITE,
        "`plan` finds the scratch StorageClass missing",
        planned=True,
    ),
    ExitPath(
        "k8s.unreachable",
        _C.PREREQUISITE,
        "the Kubernetes config does not load or the API is unreachable; nothing ran",
        v16_code=1,
    ),
    ExitPath(
        "s3.unreachable",
        _C.PREREQUISITE,
        "`generate` or `run --generate` cannot read the bronze bucket to check it is empty",
        v16_code=2,
    ),
    ExitPath(
        "financial.k8s_unreachable",
        _C.PREREQUISITE,
        "a `financial` command cannot reach the Kubernetes API",
        v16_code=1,
    ),
    ExitPath(
        "financial.reproduce.snapshot_gone",
        _C.PREREQUISITE,
        "`financial reproduce` cannot read the snapshot the alert came from",
        planned=True,
    ),
    ExitPath(
        "run.deps_missing",
        _C.PREREQUISITE,
        "the deployment predates the v1.7 dependency server and needs a new deploy",
        planned=True,
    ),
    # 5
    ExitPath(
        "confirm.non_tty",
        _C.NOT_CONFIRMED,
        "a confirmation prompt got no answer (no terminal, end of input) or was declined",
        v16_code=1,
    ),
    ExitPath(
        "confirm.declined",
        _C.NOT_CONFIRMED,
        "a confirmation prompt was answered no",
        v16_code=3,
    ),
    ExitPath(
        "run.namespace_missing_no_yes",
        _C.NOT_CONFIRMED,
        "`run` would create a missing namespace and was not given --yes",
        v16_code=1,
    ),
    # 6
    ExitPath(
        "destroy.namespace_terminating",
        _C.INCOMPLETE,
        "`destroy` finished its steps but the namespace is still terminating",
        v16_code=4,
    ),
    # 10 to 14
    ExitPath(
        "compare.not_comparable", _C.COMPARE_NOT_COMPARABLE, "`compare` verdict", planned=True
    ),
    ExitPath(
        "compare.not_established", _C.COMPARE_NOT_ESTABLISHED, "`compare` verdict", planned=True
    ),
    ExitPath(
        "compare.not_like_for_like", _C.COMPARE_NOT_LIKE_FOR_LIKE, "`compare` verdict", planned=True
    ),
    ExitPath("compare.confounded", _C.COMPARE_CONFOUNDED, "`compare` verdict", planned=True),
    ExitPath(
        "reproduce.drift",
        _C.REQUIREMENT_UNMET,
        "`reproduce` ran and a metric drifted outside its tolerance band (correctness, "
        "or performance), or the run did not follow the package's protocol",
        v16_code=2,
    ),
    ExitPath(
        "reproduce.commit_drift",
        _C.REQUIREMENT_UNMET,
        "`reproduce` was asked to verify a package recorded at another commit, "
        "without --allow-commit-drift",
        v16_code=2,
    ),
    ExitPath(
        "reproduce.verify_out_of_band",
        _C.REQUIREMENT_UNMET,
        "`reproduce` could not verify the result in band",
        planned=True,
    ),
    # 130
    ExitPath(
        "sigint",
        _C.INTERRUPTED,
        "a command interrupted with Ctrl-C outside a prompt (Ctrl-C at a prompt is 5)",
    ),
    ExitPath(
        "run.interrupted",
        _C.INTERRUPTED,
        "`run` interrupted; the record is sealed as interrupted",
        planned=True,
    ),
)

PATHS_BY_NAME: dict[str, ExitPath] = {p.name: p for p in PATHS}

# v1.6 codes that an unconverted command still produced, with what they meant
# there. Every command is converted now, so this is empty; ``render_markdown``
# prints a transition section only while it has entries.
LEGACY_CODES: dict[int, str] = {}


# Deploy and destroy report a safety refusal as a FAILED step result, not an
# exception. The producer marks it with ``details[REFUSAL_DETAIL] = <path
# name>``, and a step that failed only because a refused step came first (the
# namespace destroy keeps as the record of a bucket it refused to empty)
# carries ``details[FOLLOWS_REFUSAL_DETAIL] = True``. The CLI exits 3 when
# every failed step is one of these, and 1 when any other step failed.
REFUSAL_DETAIL = "refusal"
FOLLOWS_REFUSAL_DETAIL = "follows_refusal"


def path_code(name: str) -> ExitCode:
    """The exit code of the named path; KeyError for an unknown name."""
    return PATHS_BY_NAME[name].code


# -- generated documentation -------------------------------------------------

GENERATED_MARKER = (
    "<!-- Generated by scripts/gen_exit_codes.py from src/lakebench/exit_codes.py. "
    "Do not edit by hand. -->"
)


def _cell(text: str) -> str:
    return text.replace("|", "\\|")


def render_markdown() -> str:
    """The text of ``docs/exit-codes.md``.

    Only paths produced today are listed; a code with none says so.
    """
    live = [p for p in PATHS if p.live]
    lines = [
        "# Exit codes",
        "",
        GENERATED_MARKER,
        "",
        "Every `lakebench` command exits with one of these codes. Scripts should",
        "test the code, not the message text. Python callers can import the enum:",
        "`from lakebench.exit_codes import ExitCode` (standard library only, no CLI",
        "import).",
        "",
        '"Produced by" lists the named paths the test suite drives to each code.',
        "Commands reach the same codes on other paths too; a code with no named",
        "path yet says so.",
        "",
        "| Code | Name | Meaning | Produced by |",
        "|---|---|---|---|",
    ]
    for code in ExitCode:
        names = ", ".join(f"`{p.name}`" for p in live if p.code == code)
        lines.append(
            f"| {int(code)} | `{code.name}` | {_cell(MEANINGS[code])} | {names or 'no command yet'} |"
        )
    lines += [
        "",
        "## Named paths",
        "",
        "Each path is one way a command reaches its code. The test suite drives",
        "the CLI down every path listed here and checks the code.",
        "",
        "| Path | Code | When |",
        "|---|---|---|",
    ]
    for p in sorted(live, key=lambda p: (int(p.code), p.name)):
        lines.append(f"| `{p.name}` | {int(p.code)} | {_cell(p.when)} |")
    lines += [
        "",
        "## Errors and output",
        "",
        "Status lines (`ERROR`, `WARN`, `OK` and `...`) go to stderr; panels,",
        "tables and stage headers are still on stdout. An error starts with",
        "one `ERROR` line saying what went wrong; typed errors add `Why`, `Next`",
        "(the fix) and `Where` lines when they apply. An error Lakebench does",
        "not classify prints one line, not a traceback; set `LAKEBENCH_DEBUG=1`",
        "to get the traceback. Machine output (`--format json` and `--format csv`",
        "on `query`, `results` and `compare`) goes to plain stdout, unwrapped, so",
        "it can be piped to a parser.",
    ]
    if LEGACY_CODES:
        lines += [
            "",
            "## Codes still in transition",
            "",
            "These commands still exit with their 1.6 code, which means something",
            "else in the table above, until they are converted:",
            "",
            "| Code | 1.6 meaning |",
            "|---|---|",
        ]
        for code_value, meaning in sorted(LEGACY_CODES.items()):
            lines.append(f"| {code_value} | {_cell(meaning)} |")
    lines.append("")
    return "\n".join(lines)
