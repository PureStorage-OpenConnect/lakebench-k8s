"""Runtime dependencies and the package's imports match in both directions.

pydantic-settings was declared from the first release and imported nowhere;
every install pulled it in and the PyInstaller binary bundled it. A runtime
dependency now needs an importer under src/lakebench/, or an entry in
NOT_IMPORTED saying why it is declared anyway.

The other way round, botocore, pydantic_core and urllib3 were imported
directly but arrived only through boto3, pydantic and kubernetes, so a
release of those that dropped or replaced them would break lakebench with
nothing in pyproject.toml saying so. A direct import of a third-party
package now needs that package declared, as a runtime dependency or in a
user-facing extra, or an entry in IMPORTED_UNDECLARED with the reason.

Limits: the scan reads import statements, so ``importlib.import_module``
strings are not seen, and an import under ``if TYPE_CHECKING:`` counts as
a runtime one. A package declared only in an extra passes here wherever it
is imported; the CI clean-venv job imports every module from the wheel
without extras, which fails if such a package is imported at module level.
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
MODULE_OF = {"pyyaml": "yaml", "scikit-learn": "sklearn"}

#: Extras for contributors and builds, not for users: a package that only
#: they declare must not be imported by the package.
NON_USER_EXTRAS = {"dev", "build"}

#: Imported directly without being declared, and why.
IMPORTED_UNDECLARED: dict[str, str] = {}

#: Declared without a direct import, and why.
NOT_IMPORTED = {
    # A floor past known CVEs for typer's own dependency.
    "click": "security floor for typer's dependency",
}


def _runtime_dependencies() -> set[str]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return {Requirement(r).name.lower() for r in data["project"]["dependencies"]}


def _user_extras() -> set[str]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    extras = data["project"].get("optional-dependencies", {})
    return {
        Requirement(r).name.lower()
        for name, reqs in extras.items()
        if name not in NON_USER_EXTRAS
        for r in reqs
    }


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
    declared = {_module(d) for d in _runtime_dependencies()}
    assert collected <= declared, (
        f"lakebench.spec collects undeclared packages: {collected - declared}"
    )


def test_every_direct_import_is_declared():
    third_party = (
        _imported_top_levels(SRC) - set(sys.stdlib_module_names) - {"lakebench", "__future__"}
    )
    declared = {_module(d) for d in _runtime_dependencies() | _user_extras()}
    undeclared = sorted(third_party - declared - set(IMPORTED_UNDECLARED))
    assert not undeclared, (
        f"src/lakebench imports packages pyproject.toml does not declare: {undeclared}; "
        "add each to [project].dependencies (or a user extra) with a floor no higher than "
        "what the current dependencies already pull in, or to IMPORTED_UNDECLARED with the reason"
    )


def test_imported_undeclared_entries_are_still_imported_and_undeclared():
    imported = _imported_top_levels(SRC)
    declared = {_module(d) for d in _runtime_dependencies() | _user_extras()}
    stale = sorted(m for m in IMPORTED_UNDECLARED if m not in imported or m in declared)
    assert not stale, f"IMPORTED_UNDECLARED entries no longer needed: {stale}"
