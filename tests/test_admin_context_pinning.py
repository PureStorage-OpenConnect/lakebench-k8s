"""Adversarial-review fix: every mutating admin command that loads a
config must thread ``platform.kubernetes.context`` into the K8s client
(and into the operator manager where used). Without this, a stale
KUBECONFIG plus ``--file prod.yaml`` operates on the wrong cluster and
can crash-loop the shared Spark Operator (LB-070 class).

Complements ``tests/test_pinned_kubectl_helper.py`` (which lints
subprocess sites); this file locks the specific admin call sites
against a regression that would drop the ``context=`` kwarg.
"""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_ADMIN_PY = _REPO_ROOT / "src" / "lakebench" / "cli" / "_admin.py"


def _find_function(module: ast.Module, name: str) -> ast.FunctionDef:
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name!r} not found in {_ADMIN_PY}")


def _get_core_v1_takes_context(fn: ast.FunctionDef) -> bool:
    """True when this function calls ``_get_core_v1(context=...)`` at least once."""
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = None
        if isinstance(func, ast.Name):
            name = func.id
        elif isinstance(func, ast.Attribute):
            name = func.attr
        if name != "_get_core_v1":
            continue
        for kw in node.keywords:
            if kw.arg == "context":
                return True
    return False


def _spark_operator_manager_takes_kube_context(fn: ast.FunctionDef) -> bool:
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = None
        if isinstance(func, ast.Name):
            name = func.id
        elif isinstance(func, ast.Attribute):
            name = func.attr
        if name != "SparkOperatorManager":
            continue
        for kw in node.keywords:
            if kw.arg == "kube_context":
                return True
    return False


def _admin_module() -> ast.Module:
    return ast.parse(_ADMIN_PY.read_text(encoding="utf-8"), filename=str(_ADMIN_PY))


def test_admin_install_pins_context_and_operator_kube_context():
    """The BLOCKER the adversarial review named: the install path must
    thread context into both ``_get_core_v1`` and ``SparkOperatorManager``,
    otherwise ``--file prod.yaml`` with a stale KUBECONFIG installs the
    shared operator on the wrong cluster (LB-070 class). Both old verbs
    and ``admin install`` go through ``_run_admin_install``.
    """
    fn = _find_function(_admin_module(), "_run_admin_install")
    assert _get_core_v1_takes_context(fn), (
        "cli/_admin.py _run_admin_install no longer passes "
        "context= to _get_core_v1(); LB-070-class regression."
    )
    sc = _REPO_ROOT / "src" / "lakebench" / "deploy" / "shared_components.py"
    mod = ast.parse(sc.read_text(encoding="utf-8"), filename=str(sc))
    manager = _find_function(mod, "_manager")
    assert _spark_operator_manager_takes_kube_context(manager), (
        "shared_components SparkOperator._manager no longer passes kube_context= "
        "to SparkOperatorManager(); LB-070-class regression."
    )
    for name in ("install_spark_operator", "install_scratch_storage_class", "install"):
        calls = {
            n.func.id
            for n in ast.walk(_find_function(_admin_module(), name))
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        }
        assert "_run_admin_install" in calls, name


def test_migrate_deployment_pins_context():
    fn = _find_function(_admin_module(), "migrate_deployment")
    assert _get_core_v1_takes_context(fn), (
        "migrate_deployment no longer pins the K8s context; a stale "
        "KUBECONFIG would stamp the wrong cluster's namespace."
    )


def test_reclaim_bucket_pins_context():
    fn = _find_function(_admin_module(), "reclaim_bucket")
    assert _get_core_v1_takes_context(fn), (
        "reclaim_bucket no longer pins the K8s context; a stale "
        "KUBECONFIG would enumerate the wrong cluster's lakebench "
        "deployments."
    )


def test_doctor_pins_context_when_a_config_is_supplied():
    fn = _find_function(_admin_module(), "doctor")
    assert _get_core_v1_takes_context(fn), (
        "doctor no longer pins the K8s context when a config is supplied; "
        "the report would read a different cluster's state."
    )
