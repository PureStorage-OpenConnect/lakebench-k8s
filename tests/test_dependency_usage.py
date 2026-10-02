"""Every runtime dependency is imported by the package, so none ships unused.

pydantic-settings was declared from the first release and imported nowhere;
every install pulled it in and the PyInstaller binary bundled it. A runtime
dependency now needs an importer under src/lakebench/, or an entry in
NOT_IMPORTED saying why it is declared anyway.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from packaging.requirements import Requirement

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "lakebench"

#: Distribution name -> top-level module, where they differ.
MODULE_OF = {"pyyaml": "yaml", "pydantic-settings": "pydantic_settings"}

#: Declared without a direct import, and why.
NOT_IMPORTED = {
    # A floor past known CVEs for typer's own dependency.
    "click": "security floor for typer's dependency",
}


def _runtime_dependencies() -> set[str]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return {Requirement(r).name.lower() for r in data["project"]["dependencies"]}


def _imported_top_levels(root: Path) -> set[str]:
    names: set[str] = set()
    for path in root.rglob("*.py"):
        if "spark/scripts" in path.relative_to(root).as_posix():
            continue  # runs on driver pods, not from the installed package
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names.add(node.module.split(".")[0])
    return names


def _module(dist: str) -> str:
    return MODULE_OF.get(dist, dist.replace("-", "_"))


def test_every_runtime_dependency_is_imported():
    imported = _imported_top_levels(SRC)
    unused = sorted(
        d for d in _runtime_dependencies() if d not in NOT_IMPORTED and _module(d) not in imported
    )
    assert not unused, (
        f"runtime dependencies nothing under src/lakebench imports: {unused}; "
        "drop them from pyproject.toml, or add an entry to NOT_IMPORTED with the reason"
    )


def test_not_imported_entries_are_still_dependencies():
    deps = _runtime_dependencies()
    stale = sorted(set(NOT_IMPORTED) - deps)
    assert not stale, f"NOT_IMPORTED names packages that are no longer dependencies: {stale}"


#: Packages lakebench.spec names that arrive through a declared dependency.
SPEC_TRANSITIVE = {"botocore"}  # boto3's


def test_pyinstaller_spec_collects_only_declared_packages():
    """lakebench.spec must not collect or hidden-import a package the wheel
    does not depend on."""
    spec = ast.parse((ROOT / "lakebench.spec").read_text())
    collected: set[str] = set()
    for node in ast.walk(spec):
        if isinstance(node, ast.keyword) and node.arg == "hiddenimports":
            assert isinstance(node.value, ast.List), "hiddenimports is not a literal list"
            collected.update(
                str(e.value).split(".")[0]
                for e in node.value.elts
                if isinstance(e, ast.Constant) and isinstance(e.value, str)
            )
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in ("collect_data_files", "collect_submodules")
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            collected.add(str(node.args[0].value).split(".")[0])
    declared = {_module(d) for d in _runtime_dependencies()} | SPEC_TRANSITIVE
    assert collected <= declared, (
        f"lakebench.spec collects undeclared packages: {collected - declared}"
    )
