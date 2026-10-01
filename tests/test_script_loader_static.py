"""Spark scripts reach tests only through the load_script fixtures (QA-2).

The scripts import each other by plain name, so a test that puts the scripts
directory on sys.path, or pops ``common`` out of sys.modules, shares or
replaces the ``common`` every other test in the process sees. This scan
fails on each such line under tests/, naming the file and line:

- any sys.path edit (no test needs one: the loader's finder resolves the
  script names, and an inserted path cannot be resolved statically);
- ``syspath_prepend`` and ``setattr(sys, "path", ...)``;
- popping, deleting or (by literal name) setting a script module in
  sys.modules, directly or through monkeypatch;
- ``spec_from_file_location`` under a plain script name.

conftest.py files (where the loader lives) and this module's own fixtures
in tests/test_script_loader.py are exempt. A Spark child (a test file run
as a script) gets the scripts directory from ``spark_subprocess``'s
PYTHONPATH (tests/spark/conftest.py), so ``__main__`` blocks are not
exempt either. The scan is the fast, named message; the
pytest_runtest_teardown hook in tests/conftest.py is the backstop for forms
it cannot see (a non-literal sys.modules key, for one, is allowed here
because tests stub pyspark modules in loops).
"""

from __future__ import annotations

import ast
from pathlib import Path

from tests.conftest import shipped_script_modules

TESTS = Path(__file__).resolve().parent
_SCRIPT_NAMES = frozenset(shipped_script_modules())
_PATH_MUTATORS = {"insert", "append", "extend", "__setitem__", "__iadd__"}
_EXEMPT = {"conftest.py", "test_script_loader.py"}


def _dotted(node: ast.AST) -> str:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _script_key(node: ast.AST | None) -> bool:
    """A sys.modules key that is (or may be) a script name: a literal script
    name, or anything not a literal, since it cannot be checked."""
    if isinstance(node, ast.Constant):
        return node.value in _SCRIPT_NAMES
    return True


def _sets_sys_path(call: ast.Call) -> bool:
    """setattr(sys, "path", ...) or monkeypatch.setattr("sys.path", ...)."""
    args = call.args
    if len(args) >= 2 and _dotted(args[0]) == "sys":
        return isinstance(args[1], ast.Constant) and args[1].value == "path"
    return bool(args) and isinstance(args[0], ast.Constant) and args[0].value == "sys.path"


def violations(source: str, rel: str) -> list[str]:
    tree = ast.parse(source)
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        msg = None
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            target = _dotted(node.func.value)
            attr = node.func.attr
            if target == "sys.path" and attr in _PATH_MUTATORS:
                msg = f"sys.path.{attr}"
            elif attr == "syspath_prepend":
                msg = "syspath_prepend"
            elif (
                target == "sys.modules"
                and attr == "pop"
                and _script_key(node.args[0] if node.args else None)
            ):
                msg = "sys.modules.pop of a script module"
            elif target == "sys.modules" and attr == "update":
                msg = "sys.modules.update"
            elif attr in ("setitem", "delitem") and len(node.args) >= 2:
                if _dotted(node.args[0]) == "sys.modules":
                    key = node.args[1]
                    literal = isinstance(key, ast.Constant) and key.value in _SCRIPT_NAMES
                    if literal or (attr == "delitem" and _script_key(key)):
                        msg = f"{attr}(sys.modules, ...) of a script module"
            elif attr == "setattr" and _sets_sys_path(node):
                msg = "setattr of sys.path"
            elif attr == "spec_from_file_location" and node.args:
                key = node.args[0]
                if isinstance(key, ast.Constant) and key.value in _SCRIPT_NAMES:
                    msg = f"spec_from_file_location({key.value!r}, ...)"
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id == "setattr" and _sets_sys_path(node):
                msg = "setattr of sys.path"
        elif isinstance(node, ast.Delete):
            for t in node.targets:
                if (
                    isinstance(t, ast.Subscript)
                    and _dotted(t.value) == "sys.modules"
                    and _script_key(t.slice)
                ):
                    msg = "del sys.modules[...] of a script module"
        elif isinstance(node, (ast.Assign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for t in targets:
                if _dotted(t) == "sys.path" or (
                    isinstance(t, ast.Subscript) and _dotted(t.value) == "sys.path"
                ):
                    msg = "assignment to sys.path"
                elif (
                    isinstance(t, ast.Subscript)
                    and _dotted(t.value) == "sys.modules"
                    and isinstance(t.slice, ast.Constant)
                    and t.slice.value in _SCRIPT_NAMES
                ):
                    msg = f"sys.modules[{t.slice.value!r}] assignment"
        if msg:
            out.append(
                (
                    node.lineno,
                    f"{rel}:{node.lineno}: {msg} (load Spark scripts with load_script, "
                    f"repository scripts with exec_repo_script)",
                )
            )
    return [m for _, m in sorted(out)]


def _scan_tree() -> list[str]:
    found = []
    for path in sorted(TESTS.rglob("*.py")):
        if path.name in _EXEMPT:
            continue
        rel = str(path.relative_to(TESTS.parent))
        found += violations(path.read_text(), rel)
    return found


def test_no_test_reaches_the_scripts_by_hand():
    found = _scan_tree()
    assert not found, "\n".join(found)


def test_planted_violations_are_found():
    src = (
        "import sys\n"
        "sys.path.insert(0, 'src/lakebench/spark/scripts')\n"
        "sys.modules.pop('common', None)\n"
        "del sys.modules['silver_build_financial']\n"
        "sys.path[:0] = ['x']\n"
        "def f(monkeypatch):\n"
        "    monkeypatch.syspath_prepend('x')\n"
        "    sys.modules['common'] = object()\n"
        "    monkeypatch.setitem(sys.modules, 'common', object())\n"
        "    monkeypatch.delitem(sys.modules, 'common')\n"
        "    monkeypatch.setattr(sys, 'path', [])\n"
        "    importlib.util.spec_from_file_location('common', 'x/common.py')\n"
        "    sys.modules.update(common=None)\n"
        "if __name__ == '__main__':\n"
        "    sys.path.insert(0, 'child too')\n"
    )
    lines = [v.split(":")[1] for v in violations(src, "t.py")]
    assert lines == ["2", "3", "4", "5", "7", "8", "9", "10", "11", "12", "13", "15"]


def test_stub_modules_and_private_names_are_allowed():
    src = (
        "import sys\n"
        "def f(monkeypatch, name, mod):\n"
        "    monkeypatch.setitem(sys.modules, name, mod)\n"
        "    monkeypatch.setitem(sys.modules, 'pyspark', mod)\n"
        "    importlib.util.spec_from_file_location('lb_common_ttd', 'x/common.py')\n"
    )
    assert violations(src, "t.py") == []


def test_non_script_module_pops_are_allowed():
    assert violations("import sys\nsys.modules.pop('lakebench.cli', None)\n", "t.py") == []
