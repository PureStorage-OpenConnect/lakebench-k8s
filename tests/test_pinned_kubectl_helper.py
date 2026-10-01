"""Guardrails around ``lakebench.k8s._pinned``.

Two things are checked here:

1. The helper functions themselves append the correct
   ``--context``/``--kube-context`` flag when a config is set and, with
   no cluster target active in the process, pass the argv through
   unchanged when it is not (with a target active they use its context;
   ``tests/test_k8s_target.py``).
2. A repo-wide lint (AST-based) refuses any bare ``subprocess.run``,
   ``subprocess.Popen``, ``subprocess.check_call``, ``subprocess.check_output``
   or ``subprocess.call`` whose first positional argument is a list
   literal starting with ``"kubectl"``, ``"helm"`` or ``"oc"``. Those
   sites bypass the configured kube-context and act on whatever the
   ambient current-context happens to be, which has, in prod, sent a
   real ``helm upgrade`` to the wrong cluster. The lint runs against
   the whole ``src/lakebench/`` tree so a new site added in any module
   fails CI instead of shipping.
"""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from lakebench.k8s._pinned import (
    _pinned_argv,
    _resolve_context,
    pinned_helm,
    pinned_kubectl,
    pinned_kubectl_popen,
    pinned_oc,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src" / "lakebench"

# Files that are *allowed* to invoke ``subprocess`` on a literal
# kubectl/helm/oc argv. Only the helper module belongs here: the whole
# point of the lint is that every other call site routes through it.
LINT_WHITELIST = {SRC_ROOT / "k8s" / "_pinned.py"}


# ---------------------------------------------------------------------------
# Helper behaviour
# ---------------------------------------------------------------------------


def _make_cfg(context: str = "") -> SimpleNamespace:
    """Duck-typed stand-in for LakebenchConfig.platform.kubernetes.context."""
    return SimpleNamespace(platform=SimpleNamespace(kubernetes=SimpleNamespace(context=context)))


def test_resolve_context_accepts_config_string_and_none() -> None:
    assert _resolve_context(None) is None
    assert _resolve_context("") is None
    assert _resolve_context("prod") == "prod"
    assert _resolve_context(_make_cfg("prod")) == "prod"
    assert _resolve_context(_make_cfg("")) is None
    # A stray object without the expected attributes must not raise.
    assert _resolve_context(SimpleNamespace()) is None


def test_pinned_argv_appends_kubectl_context_when_set() -> None:
    argv = _pinned_argv("kubectl", "prod", ["get", "pods"])
    assert argv == ["kubectl", "--context", "prod", "get", "pods"]


def test_pinned_argv_appends_helm_kube_context_when_set() -> None:
    argv = _pinned_argv("helm", _make_cfg("prod"), ["list"])
    assert argv == ["helm", "--kube-context", "prod", "list"]


def test_pinned_argv_appends_oc_context_when_set() -> None:
    argv = _pinned_argv("oc", "prod", ["whoami"])
    assert argv == ["oc", "--context", "prod", "whoami"]


def test_pinned_argv_no_flag_when_context_missing() -> None:
    assert _pinned_argv("kubectl", None, ["get", "pods"]) == ["kubectl", "get", "pods"]
    assert _pinned_argv("helm", "", ["list"]) == ["helm", "list"]
    assert _pinned_argv("oc", _make_cfg(""), ["whoami"]) == ["oc", "whoami"]


def test_pinned_kubectl_runs_with_context_flag() -> None:
    with patch("lakebench.k8s._pinned.subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        pinned_kubectl(_make_cfg("prod"), ["get", "pods"], capture_output=True)
    argv = run.call_args[0][0]
    assert argv == ["kubectl", "--context", "prod", "get", "pods"]
    assert run.call_args[1] == {"capture_output": True}


def test_pinned_helm_uses_kube_context_flag_spelling() -> None:
    with patch("lakebench.k8s._pinned.subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        pinned_helm("prod", ["upgrade", "release", "chart"])
    argv = run.call_args[0][0]
    assert argv[:3] == ["helm", "--kube-context", "prod"]


def test_pinned_oc_pins_context() -> None:
    with patch("lakebench.k8s._pinned.subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        pinned_oc("prod", ["adm", "policy", "add-scc-to-user", "anyuid", "-z", "sa"])
    argv = run.call_args[0][0]
    assert argv == [
        "oc",
        "--context",
        "prod",
        "adm",
        "policy",
        "add-scc-to-user",
        "anyuid",
        "-z",
        "sa",
    ]


def test_pinned_kubectl_no_flag_when_context_missing() -> None:
    with patch("lakebench.k8s._pinned.subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        pinned_kubectl(None, ["get", "pods"])
        pinned_kubectl(_make_cfg(""), ["get", "pods"])
    for call in run.call_args_list:
        assert call[0][0] == ["kubectl", "get", "pods"]


def test_pinned_kubectl_popen_pins_context() -> None:
    with patch("lakebench.k8s._pinned.subprocess.Popen") as popen:
        pinned_kubectl_popen("prod", ["logs", "-f", "pod"])
    argv = popen.call_args[0][0]
    assert argv == ["kubectl", "--context", "prod", "logs", "-f", "pod"]


# ---------------------------------------------------------------------------
# Repo-wide lint: no bare subprocess.run(["kubectl"|"helm"|"oc", ...])
# ---------------------------------------------------------------------------


_SUBPROCESS_CALL_NAMES = {"run", "Popen", "check_call", "check_output", "call"}
_TARGET_TOOLS = {"kubectl", "helm", "oc"}


def _iter_python_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.py") if p.is_file())


def _first_positional(call: ast.Call) -> ast.expr | None:
    for arg in call.args:
        if isinstance(arg, ast.Starred):
            return None
        return arg
    return None


def _is_subprocess_call(call: ast.Call) -> bool:
    """Match ``subprocess.<fn>(...)`` and bare imports like ``run(...)``.

    We match on attribute *and* name form so a file that did
    ``from subprocess import run`` still gets linted.
    """
    func = call.func
    if isinstance(func, ast.Attribute):
        return (
            isinstance(func.value, ast.Name)
            and func.value.id == "subprocess"
            and func.attr in _SUBPROCESS_CALL_NAMES
        )
    if isinstance(func, ast.Name):
        return func.id in _SUBPROCESS_CALL_NAMES
    return False


def _bare_tool_argv(call: ast.Call) -> str | None:
    """Return the tool name if this call's first positional is
    ``["kubectl", ...]`` etc., else None."""
    first = _first_positional(call)
    if not isinstance(first, ast.List) or not first.elts:
        return None
    head = first.elts[0]
    if not isinstance(head, ast.Constant) or not isinstance(head.value, str):
        return None
    return head.value if head.value in _TARGET_TOOLS else None


def test_no_bare_kubectl_helm_or_oc_subprocess_calls_outside_helpers() -> None:
    violations: list[str] = []
    for path in _iter_python_files(SRC_ROOT):
        if path in LINT_WHITELIST:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as e:  # pragma: no cover
            pytest.fail(f"{path} failed to parse: {e}")
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not _is_subprocess_call(node):
                continue
            tool = _bare_tool_argv(node)
            if tool is None:
                continue
            rel = path.relative_to(REPO_ROOT)
            violations.append(f"{rel}:{node.lineno} bare {tool!r} argv")
    assert violations == [], (
        "Found bare kubectl/helm/oc subprocess calls; route them through "
        "lakebench.k8s.pinned_kubectl / pinned_helm / pinned_oc so the "
        "configured kube-context is applied:\n  " + "\n  ".join(violations)
    )
