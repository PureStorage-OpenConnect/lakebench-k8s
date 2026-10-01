"""Hint-string hygiene for user-visible CLI and deploy messages (C3, v1.6).

Guards against a class of hazardous hint strings that walk the user into
cluster-corrupting actions:

- ``helm install spark-operator ...``: raw helm install bypasses the
  cluster lease and read-modify-write on the operator watch list; it has
  caused crash loops of the shared Spark Operator that affected every
  running deployment. The managed path is ``lakebench admin
  install-spark-operator``.

- ``kubectl create namespace <ns>``: pre-creating a namespace bypasses
  ``lakebench deploy``'s ownership stamp and the leased operator watch-list
  mutation. This has previously caused a real destroy cascade that
  crash-looped the shared operator after a pre-created namespace.

- ``kubectl delete ns``: bypasses the ownership lease, bucket ownership
  record and operator watch-list removal.

- ``Pass --force to release`` (the cluster lease): the release-lock command
  must first steer the user to check the holder and, if the lease is
  expired, to release it without ``--force``. ``--force`` on a live lease can
  corrupt a concurrent deploy or destroy and must only appear as a
  last-resort suggestion, never as a bare one-liner.

- ``--force-legacy`` unaccompanied by a cluster-context check within 200
  characters: ``--force-legacy`` bypasses ownership checks that catch
  wrong-context mistakes, and a stale kubeconfig context can point at
  another team's live deployment on a shared object store. Every hint
  recommending ``--force-legacy`` MUST first steer the user to verify
  ``oc whoami && kubectl config current-context`` (or equivalent
  cluster-context phrasing).

The test parses source files as text (no CLI execution): it extracts the
user-facing string blocks (``hint=``, ``print_error``, ``print_warning``,
``print_info``, ``print_success``, ``message=`` in DeploymentResult, and
``console.print`` calls) and applies the rules only to those.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "lakebench"
SCAN_DIRS = ("cli", "deploy")

# Files whose text is inspected. Only .py sources.
SCAN_FILES: list[Path] = []
for _sub in SCAN_DIRS:
    SCAN_FILES.extend(sorted((SRC_ROOT / _sub).rglob("*.py")))

# Sanity: the scan must find files. A refactor that renames or moves the
# directories without updating this test would otherwise silently pass.
assert SCAN_FILES, f"no source files found under {SRC_ROOT}/{{{','.join(SCAN_DIRS)}}}"

# A "user-facing hint block" starts at one of these tokens and ends at the
# closing paren of the call or the ``,`` that terminates the keyword value.
# We do a simple bracket-balancing scan rather than an AST walk: hint text
# is always a string-literal argument at a fixed spelling, and the balance
# scan handles the multi-line concatenated string form these files use.
_HINT_STARTERS = (
    "hint=",
    "message=",
    "print_error(",
    "print_warning(",
    "print_info(",
    "print_success(",
    "console.print(",
    "err_console.print(",
    "raise typer.BadParameter(",
    "raise ValueError(",
    "raise RuntimeError(",
)
# Logger diagnostics (``logger.warning`` / ``logger.error``) are stderr
# operator logs, not the primary hint mechanism, and are excluded to keep
# this test focused on the strings users read on their terminal.


def _extract_hint_blocks(text: str) -> list[tuple[int, str]]:
    """Return a list of (start_offset, block_text) pairs.

    Each block is the text of a call argument or kwarg value that would be
    shown to the user. Comments and CLI option ``help=`` (which typer echoes
    verbatim) are also inspected because their text reaches the terminal.
    """
    blocks: list[tuple[int, str]] = []
    for start in _iter_starts(text, _HINT_STARTERS + ("help=",)):
        end = _find_arg_end(text, start)
        if end is None:
            continue
        blocks.append((start, text[start:end]))
    return blocks


def _iter_starts(text: str, tokens: tuple[str, ...]):
    for tok in tokens:
        i = 0
        while True:
            j = text.find(tok, i)
            if j == -1:
                break
            yield j + len(tok)
            i = j + len(tok)


def _find_arg_end(text: str, start: int) -> int | None:
    """Return the offset of the character after this argument.

    Handles nested parens and brackets; stops at the matching paren for the
    outer call, or at a top-level comma that ends the kwarg value.
    """
    depth = 0
    in_str: str | None = None
    n = len(text)
    i = start
    while i < n:
        c = text[i]
        if in_str is not None:
            if c == "\\":
                i += 2
                continue
            if c == in_str:
                in_str = None
            i += 1
            continue
        if c in ('"', "'"):
            # Triple-string handling: peek ahead.
            if text[i : i + 3] in ('"""', "'''"):
                in_str = text[i : i + 3]  # type: ignore[assignment]
                i += 3
                continue
            in_str = c
            i += 1
            continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            if depth == 0:
                return i
            depth -= 1
        elif c == "," and depth == 0:
            return i
        i += 1
    return None


# Phrases that count as "cluster-context check" wording within 200 chars of a
# ``--force-legacy`` mention in a hint. Case-insensitive substring match.
_CONTEXT_CHECK_PHRASES = (
    "cluster context",
    "kubectl config current-context",
    "current-context",
    "kubeconfig context",
    "oc whoami",
    "verify your cluster",
    "check your cluster context",
    "context first",
    "verify the cluster",
    "correct cluster",
    "expected cluster",
    "confirmed the namespace is yours",  # destroy.py flow: user assertion is the check
    "confirmed no other lakebench",  # engine.py flow: user assertion is the check
    "confirmed these are yours",  # destroy.py bulk refusal
    "confirmed this is yours",  # engine.py unsupported-backend flow
    "confirmed it is yours",  # destroy.py per-bucket flow
)


def _has_context_check_in_block(block: str) -> bool:
    """True when the hint BLOCK contains a cluster-context check phrase.

    Scoped to the hint block only. A docstring or unrelated code near the
    hint in the source file must not satisfy the guard; only wording the
    user actually reads counts.
    """
    slab = block.lower()
    return any(p in slab for p in _CONTEXT_CHECK_PHRASES)


@pytest.mark.parametrize("path", SCAN_FILES, ids=lambda p: str(p.relative_to(SRC_ROOT)))
def test_no_raw_helm_install_spark_operator_in_hints(path: Path) -> None:
    """Refuse any hint that tells the user to run ``helm install spark-operator``.

    Raw helm install bypasses the cluster lease and the operator watch-list
    read-modify-write, and has crash-looped the shared operator for every
    running deployment. The safe path is ``lakebench admin
    install-spark-operator``.
    """
    text = path.read_text(encoding="utf-8")
    for start, block in _extract_hint_blocks(text):
        assert "helm install spark-operator" not in block, (
            f"{path}: hint block at offset {start} recommends `helm install "
            "spark-operator`, which bypasses the cluster lease and can "
            "crash-loop the shared Spark Operator. Use `lakebench admin "
            "install-spark-operator` instead."
        )


@pytest.mark.parametrize("path", SCAN_FILES, ids=lambda p: str(p.relative_to(SRC_ROOT)))
def test_no_kubectl_create_namespace_in_hints(path: Path) -> None:
    """Refuse any hint that tells the user to pre-create the namespace.

    ``lakebench deploy`` creates the namespace under the cluster lease and
    adds it to the operator watch list atomically. A pre-created namespace
    once triggered a destroy cascade that crash-looped the shared operator.
    """
    text = path.read_text(encoding="utf-8")
    for start, block in _extract_hint_blocks(text):
        # ``kubectl`` and ``oc`` are the same operation on this OpenShift
        # cluster; the guard must cover both.
        m = re.search(r"(?:kubectl|oc)\s+create\s+n(?:s|amespace)\b", block)
        assert m is None, (
            f"{path}: hint block at offset {start} recommends "
            f"`{m.group(0) if m else ''}`. Never suggest pre-creating a "
            "namespace: `lakebench deploy` creates it under the cluster "
            "lease. Pre-creation has crash-looped the shared Spark Operator."
        )


@pytest.mark.parametrize("path", SCAN_FILES, ids=lambda p: str(p.relative_to(SRC_ROOT)))
def test_no_kubectl_delete_ns_in_hints(path: Path) -> None:
    """Refuse any hint that tells the user to run ``kubectl delete ns``.

    That bypasses ownership records, bucket cleanup, and operator watch-list
    removal. ``lakebench destroy <config>`` is the safe path.
    """
    text = path.read_text(encoding="utf-8")
    for start, block in _extract_hint_blocks(text):
        # Match both `kubectl delete ns` and `kubectl delete namespace`.
        # Exclude descriptive uses that say what it *looks like* (comments
        # about a kubectl-delete-ns being indistinguishable): those are not
        # user-facing hints, so they never appear inside a hint block.
        assert not re.search(r"(?:kubectl|oc)\s+delete\s+n(?:s|amespace)\b", block), (
            f"{path}: hint block at offset {start} suggests `kubectl/oc "
            "delete ns/namespace`. Use `lakebench destroy <config>` "
            "instead: raw delete bypasses ownership records and can leave "
            "orphan buckets."
        )


@pytest.mark.parametrize("path", SCAN_FILES, ids=lambda p: str(p.relative_to(SRC_ROOT)))
def test_no_bare_force_release_of_lease(path: Path) -> None:
    """Refuse a bare ``Pass --force to release`` hint for the cluster lease.

    ``admin release-lock`` must first steer the user to check the holder and,
    if the lease is expired, release it without ``--force``. ``--force`` on a live
    lease can corrupt a concurrent deploy or destroy and must only appear as
    a last-resort suggestion accompanied by an explicit check.
    """
    text = path.read_text(encoding="utf-8")
    for start, block in _extract_hint_blocks(text):
        # Case-insensitive, tolerate a trailing word: "Pass --force to release
        # anyway.", "Pass --force to release the lease." etc.
        m = re.search(r"pass\s+--force\s+to\s+release\b", block, re.IGNORECASE)
        if not m:
            continue
        window = block.lower()
        # Require at least one of: "last resort", or a check of the holder
        # nearby (the safer paths).
        allowed = ("last resort", "check the holder", "check who")
        assert any(w in window for w in allowed), (
            f"{path}: hint block at offset {start} suggests `Pass --force to "
            "release` for the cluster lease without steering the user to "
            "check the holder first and without flagging `--force` as a "
            "last-resort with confirmation. Rephrase to check the holder, "
            "prefer `release-lock` without `--force`, mention `--force` last."
        )


@pytest.mark.parametrize("path", SCAN_FILES, ids=lambda p: str(p.relative_to(SRC_ROOT)))
def test_force_legacy_hints_include_context_check(path: Path) -> None:
    """Every ``--force-legacy`` recommendation must be within 200 chars of a
    cluster-context check phrase.

    ``--force-legacy`` bypasses the ownership check that catches wrong-context
    mistakes (a stale kubeconfig pointing at another team's live deployment
    on a shared object store). The hint must first steer the user to
    ``oc whoami && kubectl config current-context`` or equivalent.
    """
    text = path.read_text(encoding="utf-8")
    # Phrases that mean the hint is restricting `--force-legacy` (saying
    # it does NOT help) rather than recommending it. Those are safe.
    _restriction_phrases = (
        "does not override",
        "cannot bypass",
        "is never bypassable",
        "cannot be bypassed",
        "does not waive",
    )
    # Verbs that mark a hint as *recommending* --force-legacy to the user.
    # A --force-legacy occurrence with none of these near it is a status
    # or diagnostic (e.g. "--force-legacy: proceeding on unannotated
    # namespace X"), not a recommendation, and does not need the check.
    _recommendation_verbs = (
        "pass --force-legacy",
        "re-run with --force-legacy",
        "re-run destroy with --force-legacy",
        "requires --force-legacy",
        "--force-legacy required",
        "or pass --force-legacy",
        "--force-legacy to claim",
        "--force-legacy to clean",
        "--force-legacy to take",
        "--force-legacy on deploy",
        "--force-legacy on destroy",
        "--force-legacy on `lakebench",
        "--force-legacy after",
        "--force-legacy if you",
    )
    for start, block in _extract_hint_blocks(text):
        # Skip blocks that don't contain --force-legacy at all.
        if not re.search(r"--force-legacy\b", block):
            continue
        block_lower = block.lower()
        # All three scans (restriction, recommendation-verb, context-check)
        # look at the hint BLOCK only. A docstring or adjacent code in the
        # same file must not satisfy any of them; only wording the user
        # actually reads inside this hint counts.
        if any(p in block_lower for p in _restriction_phrases):
            continue
        if not any(v in block_lower for v in _recommendation_verbs):
            continue
        if _has_context_check_in_block(block):
            continue
        pytest.fail(
            f"{path}: hint block at offset {start} recommends "
            "`--force-legacy` without a cluster-context check inside "
            "the same hint block. Add wording that steers the user to "
            "run `oc whoami && kubectl config current-context` first, "
            "or assert the buckets/namespace are theirs."
        )


def test_prerequisites_recommends_managed_spark_operator_install() -> None:
    """The Spark Operator prerequisite hint must recommend the managed
    ``lakebench admin install-spark-operator`` path.
    """
    text = (SRC_ROOT / "cli" / "_prerequisites.py").read_text(encoding="utf-8")
    assert "lakebench admin install-spark-operator" in text, (
        "cli/_prerequisites.py must recommend `lakebench admin "
        "install-spark-operator` as the safe managed install path."
    )


def test_prerequisites_namespace_hint_no_kubectl_create() -> None:
    """The namespace-missing prerequisite hint must not suggest ``kubectl create``."""
    text = (SRC_ROOT / "cli" / "_prerequisites.py").read_text(encoding="utf-8")
    # The hint text must reference the safe path. The literal namespace is
    # an f-string interpolation, so match by keyword phrase rather than a
    # continuous substring.
    assert "lakebench deploy" in text and "automatically" in text, (
        "cli/_prerequisites.py namespace-missing hint must tell the user "
        "that `lakebench deploy <config>` creates the namespace automatically."
    )
    assert "platform.kubernetes.context" in text, (
        "cli/_prerequisites.py namespace-missing hint must remind the user "
        "to set platform.kubernetes.context to the intended cluster before "
        "deploying."
    )


# ---------------------------------------------------------------------------
# Declined-prompt exit code (C3, v1.6; renumbered by CLI-1). A user who
# answers "n" to a confirmation prompt exits 5 (not confirmed), distinct from
# 0 (success), 1 (failure), 2 (usage) and 3 (refused), so wrapper scripts can
# tell "user cancelled" apart from the rest.
# ---------------------------------------------------------------------------


def test_declined_code_is_not_confirmed() -> None:
    """A declined prompt is ``ExitCode.NOT_CONFIRMED`` (5); the 1.6 constant is gone."""
    import lakebench.cli._helpers as helpers
    from lakebench.exit_codes import ExitCode

    assert ExitCode.NOT_CONFIRMED == 5
    assert not hasattr(helpers, "EXIT_DECLINED")


def test_declined_confirm_exits_five_via_clirunner() -> None:
    """A declined ``typer.confirm`` exits 5 through Typer's ``CliRunner``.

    Uses a mini in-process Typer app on the lakebench group class rather than
    driving a real command (which needs live cluster access to reach the
    prompt) so the test stays hermetic.
    """
    import typer
    from typer.testing import CliRunner

    from lakebench.cli._exit import LakebenchGroup
    from lakebench.exit_codes import ExitCode

    app = typer.Typer(cls=LakebenchGroup)

    @app.command()
    def cancellable() -> None:
        if not typer.confirm("Proceed?"):
            raise typer.Exit(ExitCode.NOT_CONFIRMED)

    @app.command()
    def other() -> None:  # a second command keeps Typer in group mode
        pass

    runner = CliRunner()
    result = runner.invoke(app, ["cancellable"], input="n\n")
    assert result.exit_code == 5, f"expected 5, got {result.exit_code}: {result.output!r}"


def test_cli_confirm_sites_use_not_confirmed() -> None:
    """Every ``typer.confirm(...)`` in ``src/lakebench/cli/`` whose ``False``
    branch exits must exit ``ExitCode.NOT_CONFIRMED`` (5), by
    ``typer.Exit(ExitCode.NOT_CONFIRMED)`` or ``raise NotConfirmed(...)``.

    Missed sites are the whole point of standardising this exit code:
    scripts that check for 5 to distinguish cancellation would silently
    treat one that still exits 0 or 1 as something else.
    """
    cli_dir = SRC_ROOT / "cli"
    offenders: list[str] = []
    for path in sorted(cli_dir.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        lines = text.split("\n")
        for m in re.finditer(r"typer\.confirm\s*\(", text):
            # A confirm with abort=True raises click.Abort, which the
            # top-level handler maps to 5.
            head_end_of_call = text.find(")", m.end())
            call_args = text[m.end() : head_end_of_call] if head_end_of_call != -1 else ""
            if "abort=True" in call_args:
                continue
            line_no = text.count("\n", 0, m.end()) + 1
            # The declined branch is the 3 lines after the confirm: an
            # ``if not typer.confirm(): raise ...`` two-liner, or a
            # three-liner with an intervening ``print_info``.
            snippet = "\n".join(lines[line_no - 1 : line_no + 3])
            exits = re.search(r"typer\.Exit\(|raise \w+\(", snippet)
            if exits and not re.search(r"ExitCode\.NOT_CONFIRMED|NotConfirmed\(", snippet):
                offenders.append(
                    f"{path.relative_to(SRC_ROOT)}:{line_no}: typer.confirm() declined "
                    "branch does not exit ExitCode.NOT_CONFIRMED."
                )
    assert not offenders, "declined-prompt exit not standardised:\n  " + "\n  ".join(offenders)
