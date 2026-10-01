"""SAF-7: every API client pins the config's context (CC-7).

A bare ``XxxApi()`` copies the kubernetes client's process-default
configuration, and only the kubeconfig load calls set that default. So the
lint fails on every path that can produce an unpinned configuration:

1. a kubeconfig or in-cluster load, or a hand-set default configuration,
   outside ``k8s/target.py``;
2. ``get_k8s_client(...)`` without ``context=`` or ``target=``, or a direct
   ``K8sClient(...)`` outside ``k8s/client.py``;
3. a ``kubectl``/``oc``/``helm`` argv (list or tuple literal) that is not
   built with ``target.cli_args()``: ``["kubectl", *cli_args("kubectl", c),
   ...]``. ``k8s/_pinned.py`` and ``k8s/target.py`` build the flag, and
   ``SparkOperatorManager`` sends every literal argv through its ``_run``,
   which calls the pinned helpers; those files are exempt;
4. a shell string starting with one of the tools (``os.system``,
   ``shell=True``, ``asyncio.create_subprocess_shell``).

The tree covered is ``src/lakebench/`` (minus ``spark/scripts/``, which runs
inside the Spark driver pod) and ``scripts/``.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src" / "lakebench"
SCRIPTS_ROOT = REPO_ROOT / "scripts"
TARGET_PY = SRC_ROOT / "k8s" / "target.py"
CLIENT_PY = SRC_ROOT / "k8s" / "client.py"
PINNED_PY = SRC_ROOT / "k8s" / "_pinned.py"
OPERATOR_PY = SRC_ROOT / "modules" / "pipeline_engines" / "spark" / "operator.py"
ARGV_EXEMPT = {TARGET_PY, PINNED_PY, OPERATOR_PY}

_LOADERS = {
    "load_kube_config",
    "load_incluster_config",
    "new_client_from_config",
    "new_client_from_config_dict",
    "load_kube_config_from_dict",
    "load_and_set",
    "KubeConfigLoader",
    "InClusterConfigLoader",
    "set_default",
}
_TOOLS = {"kubectl", "oc", "helm"}


def _sources() -> list[Path]:
    src = [
        p
        for p in SRC_ROOT.rglob("*.py")
        if "spark/scripts" not in p.relative_to(SRC_ROOT).as_posix()
    ]
    return sorted(src + list(SCRIPTS_ROOT.rglob("*.py")))


def _is_tool(value: object) -> bool:
    return isinstance(value, str) and value.rsplit("/", 1)[-1] in _TOOLS


def _imports(tree: ast.Module) -> tuple[set[str], set[str], set[str]]:
    """(names bound to kubernetes.config, loader aliases, get_k8s_client aliases)."""
    kc: set[str] = set()
    loaders: set[str] = set()
    gkc = {"get_k8s_client"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            for a in node.names:
                bound = a.asname or a.name
                if mod in ("kubernetes", "kubernetes.config") and a.name in (
                    "config",
                    "kube_config",
                ):
                    kc.add(bound)
                if mod.startswith("kubernetes.config") and a.name in (_LOADERS | {"load_config"}):
                    loaders.add(bound)
                if a.name == "get_k8s_client":
                    gkc.add(bound)
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name.startswith("kubernetes.config") and a.asname:
                    kc.add(a.asname)
    return kc, loaders, gkc


def _call_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _is_kube_config_attr(func: ast.expr, kc: set[str]) -> bool:
    """``<kubernetes.config alias>.X`` or ``kubernetes.config.X``."""
    if not isinstance(func, ast.Attribute):
        return False
    base = func.value
    if isinstance(base, ast.Name) and base.id in kc:
        return True
    return (
        isinstance(base, ast.Attribute)
        and base.attr == "config"
        and isinstance(base.value, ast.Name)
        and base.value.id == "kubernetes"
    )


def _argv_pinned(elts: list[ast.expr]) -> bool:
    """``[tool, *cli_args(...), ...]`` or ``[tool, *x.cli_args(...), ...]``."""
    if len(elts) < 2 or not isinstance(elts[1], ast.Starred):
        return False
    inner = elts[1].value
    return isinstance(inner, ast.Call) and _call_name(inner) == "cli_args"


def _shell_string_runs_tool(first: ast.expr) -> bool:
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return _is_tool(first.value.split(" ", 1)[0])
    if isinstance(first, ast.JoinedStr) and first.values:
        head = first.values[0]
        return isinstance(head, ast.Constant) and _is_tool(str(head.value).split(" ", 1)[0])
    return False


def _violations(path: Path, tree: ast.Module) -> list[str]:
    try:
        rel = path.relative_to(SRC_ROOT).as_posix()
    except ValueError:
        rel = path.relative_to(REPO_ROOT).as_posix()
    out: list[str] = []
    kc, loader_aliases, gkc_names = _imports(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _call_name(node)
            # Rule 1: kubeconfig / in-cluster loads live only in k8s/target.py.
            if path != TARGET_PY:
                if name in _LOADERS or (
                    isinstance(node.func, ast.Name) and node.func.id in loader_aliases
                ):
                    out.append(f"{rel}:{node.lineno}: {name}() outside k8s/target.py")
                elif name == "load_config" and _is_kube_config_attr(node.func, kc):
                    out.append(
                        f"{rel}:{node.lineno}: kubernetes load_config() outside k8s/target.py"
                    )
            # Rule 2: get_k8s_client needs context= or target=; K8sClient is
            # built only through it.
            if name in gkc_names and not any(
                kw.arg in ("context", "target") for kw in node.keywords
            ):
                out.append(f"{rel}:{node.lineno}: {name}() without context= or target=")
            if name == "K8sClient" and path != CLIENT_PY:
                out.append(f"{rel}:{node.lineno}: K8sClient() outside k8s/client.py")
            # Rule 4: shell strings.
            shell_call = name in ("system", "create_subprocess_shell", "popen") or any(
                kw.arg == "shell" and isinstance(kw.value, ast.Constant) and kw.value.value is True
                for kw in node.keywords
            )
            if shell_call and node.args and _shell_string_runs_tool(node.args[0]):
                out.append(f"{rel}:{node.lineno}: shell string runs a cluster tool")
        # Rule 3: argv literals.
        if isinstance(node, (ast.List, ast.Tuple)) and path not in ARGV_EXEMPT:
            elts = node.elts
            if not elts or not isinstance(elts[0], ast.Constant) or not _is_tool(elts[0].value):
                continue
            if len(elts) > 1 and all(
                isinstance(e, ast.Constant) and _is_tool(e.value) for e in elts
            ):
                continue  # a list of tool names, not an argv
            if not _argv_pinned(elts):
                out.append(
                    f"{rel}:{node.lineno}: {elts[0].value} argv not built with "
                    "k8s.target.cli_args()"
                )
    return out


def lint_tree() -> list[str]:
    found: list[str] = []
    for path in _sources():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        found.extend(_violations(path, tree))
    return found


def test_no_unpinned_kubernetes_client_construction() -> None:
    found = lint_tree()
    assert not found, (
        "Unpinned Kubernetes client paths (SAF-7). Load through "
        "lakebench.k8s.target.ClusterTarget, pass context=/target= to "
        "get_k8s_client, and build tool argv with target.cli_args():\n  " + "\n  ".join(found)
    )


_FAKE = SRC_ROOT / "cli" / "_fake_lint_target.py"


@pytest.mark.parametrize(
    ("snippet", "expected"),
    [
        ("from kubernetes import config as k\nk.load_kube_config()\n", "load_kube_config()"),
        ("from kubernetes import config\nconfig.load_incluster_config()\n", "load_incluster"),
        ("import kubernetes.config as kc\nkc.load_config()\n", "kubernetes load_config()"),
        ("import kubernetes\nkubernetes.config.load_config()\n", "kubernetes load_config()"),
        ("from kubernetes.config import load_config\nload_config()\n", "load_config()"),
        ("from kubernetes.config import load_kube_config as lkc\nlkc()\n", "lkc()"),
        ("from kubernetes.config import new_client_from_config\nnew_client_from_config()\n", "new"),
        ("from kubernetes import client\nclient.Configuration.set_default(c)\n", "set_default"),
        ("KubeConfigLoader(config_dict=d).load_and_set(c)\n", "load_and_set"),
        ("get_k8s_client(namespace='x')\n", "without context= or target="),
        ("get_k8s_client()\n", "without context= or target="),
        ("from lakebench.k8s import get_k8s_client as g\ng(namespace='x')\n", "g() without"),
        ("import m\nm.get_k8s_client(namespace='x')\n", "without context="),
        ("K8sClient(namespace='x')\n", "K8sClient() outside"),
        ("x = ['kubectl']\n", "kubectl argv"),
        ("x = ['helm', '--kube-context', c]\n", "helm argv"),
        ("x = ['oc', '--context', c, 'whoami']\n", "oc argv"),
        ("cmd = ['kubectl', 'get', 'pods']\nsubprocess.run(cmd)\n", "kubectl argv"),
        ("cmd = ('kubectl', 'get', 'pods')\n", "kubectl argv"),
        ("cmd = ['/usr/bin/kubectl', 'get']\n", "argv not built"),
        ("os.system('kubectl get pods')\n", "shell string"),
        ("subprocess.run(f'helm list -n {ns}', shell=True)\n", "shell string"),
    ],
)
def test_lint_catches(snippet: str, expected: str) -> None:
    found = _violations(_FAKE, ast.parse(snippet))
    assert any(expected in f for f in found), found


@pytest.mark.parametrize(
    "snippet",
    [
        "from lakebench.config import load_config\nload_config(p)\n",
        "get_k8s_client(context=c, namespace='x')\n",
        "get_k8s_client(target=t)\n",
        "x = ['kubectl', *cli_args('kubectl', c), 'get']\n",
        "x = ['kubectl', *target.cli_args('kubectl'), 'get']\n",
        "for tool in ['kubectl', 'helm']:\n    pass\n",
        "pinned_kubectl(cfg, ['get', 'pods'])\n",
        "from kubernetes import client\nclient.CoreV1Api()\n",
    ],
)
def test_lint_allows(snippet: str) -> None:
    assert _violations(_FAKE, ast.parse(snippet)) == []
