#!/usr/bin/env python3
"""Frozen-file sha list and its guard (the AML freeze).

The AML looks are scored with code that must not change between the
pre-registered predictions and the looks: the four bronze and silver
scripts, the feature and reference scorers, the fidelity gate, the
pre-registration, the datagen sources and image inputs, and every symbol
those scripts import. ``scripts/frozen_aml_files.json`` lists them with
their hashes; this guard checks the tree and the history against it.

Entry kinds:

- ``file``: sha256 of the bytes.
- ``closed-glob``: every file matching the glob has its own ``file`` entry
  and the count matches, so a new or deleted file fails.
- ``pyattr``: named module-level assignments, ``ast.literal_eval`` of each
  value, hashed as sorted JSON. A comment edit does not move it.
- ``pysym``: one module-level name: its binding statement plus every other
  module-level statement (not a ``def`` or ``class``) that names it, as
  ``ast.dump`` with docstrings stripped. A body that only reads it is not
  hashed; a body that mutates it is refused (``check-tree``).
- ``prelude``: everything a module in the import closure runs at import
  time, other than ``def`` bodies and literal constants: imports, calls,
  non-literal assignments, decorators, default values, class bases and
  class-level statements. Code that runs when a frozen script imports the
  module is pinned even when it names no pinned symbol.
- ``ci-job``: a job of ``.github/workflows/ci.yml``, loaded as YAML.
- ``append-only``: bytes in ``check-tree``; history through
  ``datagen_seed.heldout_history_problems``.

The pinned closure is derived, not maintained: ``regen`` follows every
import of the frozen Python files (bare script imports resolve through
``scripts_maps.SCRIPT_MAPS``, as the Spark pods resolve them) and every
module-level name each imported symbol references, recursively.

Subcommands: ``check-tree``, ``check-range BASE..HEAD``, ``check-history``,
``regen``, ``manifest``. Hashes are defined on Python 3.11 only, because
``ast.dump`` differs between minors.

Freeze costs (``Freeze-cost:`` trailer): None, None (append), Parity,
Rebuild, Re-derive. Void needs an owner decision.
"""

from __future__ import annotations

import argparse
import ast
import fnmatch
import hashlib
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

AST_PYTHON = "3.11"
LIST_PATH = "scripts/frozen_aml_files.json"
SCHEMA = 2
SCRIPTS_DIR = "src/lakebench/spark/scripts"
PKG_DIR = "src/lakebench"
SCRIPT_MAPS_PATH = "src/lakebench/modules/pipeline_engines/spark/scripts_maps.py"
CI_PATH = ".github/workflows/ci.yml"
TRAILER_KEY = "Freeze-cost"
TRAILER_VALUES = ("None", "None (append)", "Parity", "Rebuild", "Re-derive")
PARITY_VALUES = ("None", "Parity")
PARITY_CHECKS = ("AML parity (pyspark 4.0.1)", "AML parity (pyspark 4.1.1)")
PREDICTIONS_PATH = "src/lakebench/spark/data/aml/aml_level2_predictions.json"
PREREG_PATH = "src/lakebench/spark/data/aml/aml_preregistration.json"
LOOKS_PATH = "src/lakebench/spark/data/aml/aml_registered_looks.json"
HELDOUT_PATH = "src/lakebench/spark/data/aml/heldout_hashes.json"
DATAGEN_SEED_PATH = "src/lakebench/config/datagen_seed.py"
PROTECTED_ROLES = ("evaluation", "robustness")

# The floor: what the list must hold, defined here and not in the list, so a
# list edit (a bad conflict resolution, say) cannot drop a frozen file.
REQUIRED_FILES: tuple[str, ...] = (
    f"{SCRIPTS_DIR}/bronze_ingest_financial.py",
    f"{SCRIPTS_DIR}/bronze_verify_financial.py",
    f"{SCRIPTS_DIR}/silver_build_financial.py",
    f"{SCRIPTS_DIR}/silver_stream_financial.py",
    f"{SCRIPTS_DIR}/aml_features.py",
    f"{SCRIPTS_DIR}/score_financial_reference.py",
    "src/lakebench/aml/fidelity_gate.py",
    "src/lakebench/aml/reference_score.py",
    PREREG_PATH,
    "datagen_rs/Cargo.lock",
    "datagen_rs/Cargo.toml",
    "datagen_rs/Dockerfile",
    "datagen_rs/entrypoint.py",
    # The parity proof itself: the guards, their mutation check and this
    # guard. Weakening one of them changes what "parity green" means.
    "scripts/frozen_guard.py",
    "scripts/check_parity_job.py",
    "tests/spark/aml_parity_guards.txt",
    "tests/spark/_parity_mutation.py",
    "tests/spark/_d_full_helpers.py",
    "tests/spark/_foreach_batch.py",
    "tests/spark/test_parity_guard_mutations.py",
    "tests/spark/test_aml_batch_stream_statements_parity.py",
    "tests/spark/test_aml_batch_stream_profiles_parity.py",
    "tests/spark/test_aml_stream_dimension_parity_full.py",
    "tests/spark/test_aml_stream_statements_replay_idempotent.py",
    # What the guards run on: the Spark-tier harness (its hooks decide each
    # test's outcome) and the pinned test jars.
    "tests/spark/conftest.py",
    "tests/spark/jars.lock.json",
    "scripts/fetch_test_jars.py",
)
REQUIRED_GLOBS: tuple[str, ...] = ("datagen_rs/src/**",)
REQUIRED_PYATTRS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "src/lakebench/deploy/financial_ddl.py",
        (
            "SILVER_TRANSACTIONS_DDL",
            "SILVER_ENTITIES_DDL",
            "SILVER_ACCOUNTS_DDL",
            "SILVER_ACCOUNT_STATEMENTS_DDL",
            "SILVER_COUNTERPARTY_EDGES_DDL",
            "SILVER_ENTITY_PROFILES_DDL",
            "SILVER_BATCH_VERSIONS_DDL",
        ),
    ),
    ("src/lakebench/modules/pipeline_engines/spark/job.py", ("REFERENCE_PY_DEPS",)),
)
# Pinned symbols that no frozen script imports but that decide what the
# frozen code is or does: the DDL renderer, the shipped-file map (it decides
# what a bare import resolves to on a pod).
REQUIRED_PYSYMS: tuple[tuple[str, str], ...] = (
    ("src/lakebench/deploy/financial_ddl.py", "render_ddl"),
    (SCRIPT_MAPS_PATH, "SCRIPT_MAPS"),
)
REQUIRED_CI_JOBS: tuple[str, ...] = ("aml-parity", "frozen-guard")

# Modules the frozen scripts may import that live outside the repository.
# A bare import that is neither a shipped script, a paired fallback nor one
# of these fails: it is a local module the guard cannot see.
EXTERNAL_TOP: frozenset[str] = frozenset(
    set(sys.stdlib_module_names)
    | {
        "pyspark",
        "py4j",
        "delta",
        "numpy",
        "pandas",
        "pyarrow",
        "sklearn",
        "scipy",
        "joblib",
        "threadpoolctl",
        "yaml",
        "boto3",
        "botocore",
        "kubernetes",
    }
)
MUTATING_METHODS = frozenset(
    {
        "update",
        "setdefault",
        "pop",
        "popitem",
        "clear",
        "append",
        "extend",
        "insert",
        "remove",
        "add",
        "discard",
        "__setitem__",
        "__delitem__",
        "sort",
        "reverse",
    }
)


class GuardError(Exception):
    """A check failed; the message names what and where."""


# ---------------------------------------------------------------------------
# Trees: the working tree, or a commit read through git
# ---------------------------------------------------------------------------


def _git(root: Path, *args: str, check: bool = True, input_: bytes | None = None) -> bytes:
    proc = subprocess.run(
        ["git", "-C", str(root), *args],
        input=input_,
        capture_output=True,
        check=False,
    )
    if check and proc.returncode != 0:
        raise GuardError(f"git {' '.join(args)}: {proc.stderr.decode(errors='replace').strip()}")
    return proc.stdout


class Tree:
    """Read-only view of the files of one revision."""

    def read(self, path: str) -> bytes | None:
        raise NotImplementedError

    def files(self) -> list[str]:
        raise NotImplementedError


class WorkTree(Tree):
    def __init__(self, root: Path):
        self.root = root
        self._files: list[str] | None = None

    def read(self, path: str) -> bytes | None:
        p = self.root / path
        if p.is_symlink():
            raise GuardError(f"{path}: is a symlink; frozen paths are files")
        return p.read_bytes() if p.is_file() else None

    def files(self) -> list[str]:
        if self._files is None:
            out = _git(self.root, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
            self._files = sorted(
                f for f in out.decode().split("\0") if f and (self.root / f).is_file()
            )
        return self._files


class _BlobReader:
    """One ``git cat-file --batch`` process per repository, and a cache of
    blob contents by id (blobs are immutable), so walking hundreds of
    commits reads each distinct file version once."""

    _readers: dict[str, _BlobReader] = {}

    def __init__(self, root: Path):
        self.proc = subprocess.Popen(
            ["git", "-C", str(root), "cat-file", "--batch"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        self.cache: dict[str, bytes] = {}

    @classmethod
    def for_root(cls, root: Path) -> _BlobReader:
        key = str(root)
        if key not in cls._readers:
            cls._readers[key] = cls(root)
        return cls._readers[key]

    def read(self, blob: str) -> bytes:
        if blob not in self.cache:
            assert self.proc.stdin is not None and self.proc.stdout is not None
            self.proc.stdin.write(blob.encode() + b"\n")
            self.proc.stdin.flush()
            header = self.proc.stdout.readline().split()
            if len(header) != 3:
                raise GuardError(f"git cat-file: cannot read blob {blob}")
            size = int(header[2])
            data = self.proc.stdout.read(size)
            self.proc.stdout.read(1)  # the newline after the object
            self.cache[blob] = data
        return self.cache[blob]


class GitTree(Tree):
    def __init__(self, root: Path, commit: str):
        self.root = root
        self.commit = commit
        self._blobs: dict[str, str] | None = None
        self._symlinks: set[str] = set()

    def _index(self) -> dict[str, str]:
        if self._blobs is None:
            out = _git(self.root, "ls-tree", "-r", "-z", self.commit)
            blobs: dict[str, str] = {}
            for rec in out.decode().split("\0"):
                if not rec:
                    continue
                meta, path = rec.split("\t", 1)
                parts = meta.split()
                if len(parts) == 3 and parts[1] == "blob":
                    blobs[path] = parts[2]
                    if parts[0] == "120000":
                        self._symlinks.add(path)
            self._blobs = blobs
        return self._blobs

    def files(self) -> list[str]:
        return sorted(self._index())

    def read(self, path: str) -> bytes | None:
        blob = self._index().get(path)
        if blob is None:
            return None
        if path in self._symlinks:
            raise GuardError(f"{path}: is a symlink at {self.commit[:12]}; frozen paths are files")
        return _BlobReader.for_root(self.root).read(blob)


def _sha(data: bytes | None) -> str:
    return "missing" if data is None else hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------


def _strip_docstrings(tree: ast.AST) -> ast.AST:
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:] or [ast.Pass()]
    return tree


def _dump(node: ast.AST) -> str:
    return ast.dump(node, annotate_fields=True, include_attributes=False)


def _target_names(target: ast.AST) -> list[str]:
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        return [n for elt in target.elts for n in _target_names(elt)]
    if isinstance(target, ast.Starred):
        return _target_names(target.value)
    return []


def _pattern_names(pattern: ast.AST) -> list[str]:
    out = []
    for n in ast.walk(pattern):
        if isinstance(n, (ast.MatchAs, ast.MatchStar)) and n.name:
            out.append(n.name)
        if isinstance(n, ast.MatchMapping) and n.rest:
            out.append(n.rest)
    return out


def _module_level_stmts(body: list[ast.stmt]) -> Iterable[ast.stmt]:
    """Statements that run at module scope, through module-level control
    flow, not into def or class bodies."""
    for stmt in body:
        yield stmt
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for attr in ("body", "orelse", "finalbody"):
            yield from _module_level_stmts(getattr(stmt, attr, []) or [])
        for handler in getattr(stmt, "handlers", []) or []:
            yield from _module_level_stmts(handler.body)
        for case in getattr(stmt, "cases", []) or []:
            yield from _module_level_stmts(case.body)


def _bound_names(stmt: ast.stmt) -> list[str]:
    """Names this one statement binds at module scope (not its children)."""
    names: list[str] = []
    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        names.append(stmt.name)
    elif isinstance(stmt, ast.Assign):
        for t in stmt.targets:
            names += _target_names(t)
    elif isinstance(stmt, (ast.AnnAssign, ast.AugAssign)):
        names += _target_names(stmt.target)
    elif isinstance(stmt, (ast.For, ast.AsyncFor)):
        names += _target_names(stmt.target)
    elif isinstance(stmt, (ast.With, ast.AsyncWith)):
        for item in stmt.items:
            if item.optional_vars is not None:
                names += _target_names(item.optional_vars)
    elif isinstance(stmt, (ast.Import, ast.ImportFrom)):
        for alias in stmt.names:
            names.append(alias.asname or alias.name.split(".")[0])
    elif isinstance(stmt, ast.Delete):
        for t in stmt.targets:
            names += _target_names(t)
    elif isinstance(stmt, ast.Try):
        for h in stmt.handlers:
            if h.name:
                names.append(h.name)
    elif isinstance(stmt, ast.Match):
        for case in stmt.cases:
            names += _pattern_names(case.pattern)
    if not isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        for n in _walk_no_scopes(stmt):
            if isinstance(n, ast.NamedExpr) and isinstance(n.target, ast.Name):
                names.append(n.target.id)
    return names


def _walk_no_scopes(node: ast.AST) -> Iterable[ast.AST]:
    """ast.walk of one statement that does not enter def, class or lambda
    bodies, nor the bodies of compound statements (those are separate
    module-level statements)."""
    stack: list[ast.AST] = [node]
    while stack:
        n = stack.pop()
        yield n
        for name, value in ast.iter_fields(n):
            if isinstance(n, ast.stmt) and name in (
                "body",
                "orelse",
                "finalbody",
                "handlers",
                "cases",
            ):
                continue
            for item in value if isinstance(value, list) else [value]:
                if isinstance(item, ast.AST) and not isinstance(
                    item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
                ):
                    stack.append(item)


def _names_in(node: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


@dataclass
class Module:
    path: str
    tree: ast.Module
    # name -> module-level statements that bind it (source order)
    bindings: dict[str, list[ast.stmt]] = field(default_factory=dict)
    # module-level statements in source order (through control flow)
    stmts: list[ast.stmt] = field(default_factory=list)


_PARSED: dict[tuple[str, bytes], Module] = {}


def parse_module(path: str, source: bytes) -> Module:
    """Parsed (and docstring-stripped) module; cached by path and content,
    since a Module is never changed after parsing."""
    key = (path, hashlib.sha1(source).digest())
    if key not in _PARSED:
        _PARSED[key] = _parse_module(path, source)
    return _PARSED[key]


def _parse_module(path: str, source: bytes) -> Module:
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as e:
        raise GuardError(f"{path}: cannot parse: {e}") from e
    _strip_docstrings(tree)
    mod = Module(path=path, tree=tree)
    for stmt in _module_level_stmts(tree.body):
        mod.stmts.append(stmt)
        for name in _bound_names(stmt):
            mod.bindings.setdefault(name, []).append(stmt)
    return mod


def _top_level_of(mod: Module, stmt: ast.stmt) -> ast.stmt:
    """The module body statement that contains ``stmt``."""
    for top in mod.tree.body:
        for n in _module_level_stmts([top]):
            if n is stmt:
                return top
    return stmt


# ---------------------------------------------------------------------------
# Import resolution
# ---------------------------------------------------------------------------


@dataclass
class Resolver:
    """Resolves the imports of repository Python files to repository paths."""

    tree: Tree
    shipped: dict[str, str]  # flat module name -> repo path (SCRIPT_MAPS)
    _extensions: set[str] | None = None

    def lakebench_module(self, dotted: str) -> str | None:
        """The file Python would import: a package wins over a module of
        the same name; an extension module next to it is refused."""
        rel = dotted.replace(".", "/")
        base = f"src/{rel}"
        if self._extensions is None:
            self._extensions = {
                f.split(".", 1)[0] for f in self.tree.files() if f.endswith((".so", ".pyd"))
            }
        if base in self._extensions:
            raise GuardError(f"{base}: an extension module shadows the Python source")
        for cand in (f"{base}/__init__.py", f"{base}.py"):
            if self.tree.read(cand) is not None:
                return cand
        return None

    def package_of(self, path: str) -> str | None:
        if not path.startswith("src/"):
            return None
        parts = path[len("src/") :].removesuffix(".py").split("/")
        if parts[-1] == "__init__":
            parts = parts[:-1]
        else:
            parts = parts[:-1]
        return ".".join(parts)

    def resolve(
        self, importer: str, module: str | None, level: int, pairing: dict[str, str]
    ) -> str | None:
        """Repository path of the imported module, None for an external
        one, GuardError for a local one it cannot place."""
        if level:
            pkg = self.package_of(importer)
            if pkg is None or not pkg:
                raise GuardError(f"{importer}: relative import outside a package")
            base = pkg.split(".")
            if level - 1 > len(base):
                raise GuardError(f"{importer}: relative import beyond the package root")
            base = base[: len(base) - (level - 1)]
            dotted = ".".join(base + ([module] if module else []))
            found = self.lakebench_module(dotted)
            if found is None:
                raise GuardError(f"{importer}: cannot resolve relative import {dotted}")
            return found
        assert module is not None
        top = module.split(".")[0]
        if top == "lakebench":
            found = self.lakebench_module(module)
            if found is None:
                raise GuardError(f"{importer}: cannot resolve import {module}")
            return found
        if module in self.shipped:
            return self.shipped[module]
        if module in pairing:
            return pairing[module]
        if top in EXTERNAL_TOP:
            return None
        raise GuardError(f"{importer}: cannot resolve import {module}")


def shipped_modules(tree: Tree) -> dict[str, str]:
    """Flat module name -> repo path, read statically from SCRIPT_MAPS
    (``_scripts("x.py", ...)`` and ``_src("pkg/x.py")`` calls)."""
    source = tree.read(SCRIPT_MAPS_PATH)
    if source is None:
        raise GuardError(f"{SCRIPT_MAPS_PATH} is missing: bare script imports cannot be resolved")
    mod = ast.parse(source)
    target = None
    for stmt in mod.body:
        if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
            names = (
                _target_names(stmt.target)
                if isinstance(stmt, ast.AnnAssign)
                else [n for t in stmt.targets for n in _target_names(t)]
            )
            if "SCRIPT_MAPS" in names:
                target = stmt.value
    if target is None:
        raise GuardError(f"{SCRIPT_MAPS_PATH}: no SCRIPT_MAPS assignment")
    out: dict[str, str] = {}
    for call in ast.walk(target):
        if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Name)):
            continue
        if call.func.id not in ("_scripts", "_src"):
            continue
        for arg in call.args:
            if not (isinstance(arg, ast.Constant) and isinstance(arg.value, str)):
                if isinstance(arg, ast.JoinedStr):
                    continue  # data files (f-strings), not modules
                raise GuardError(f"{SCRIPT_MAPS_PATH}: non-literal {call.func.id} argument")
            rel = arg.value if call.func.id == "_src" else f"spark/scripts/{arg.value}"
            if rel.endswith(".py"):
                out[Path(rel).stem] = f"{PKG_DIR}/{rel}"
    return out


# ---------------------------------------------------------------------------
# Closure
# ---------------------------------------------------------------------------


@dataclass
class Closure:
    symbols: set[tuple[str, str]] = field(default_factory=set)
    modules: set[str] = field(default_factory=set)  # modules a frozen file imports from
    problems: list[str] = field(default_factory=list)


def _imports_in(node: ast.AST) -> list[tuple[ast.Import | ast.ImportFrom, ast.Try | None]]:
    """Every Import/ImportFrom under ``node``, with the Try whose except
    handler holds it (for the paired bare fallback)."""
    out: list[tuple[ast.Import | ast.ImportFrom, ast.Try | None]] = []

    def visit(n: ast.AST, handler_of: ast.Try | None) -> None:
        if isinstance(n, (ast.Import, ast.ImportFrom)):
            out.append((n, handler_of))
            return
        if isinstance(n, ast.Try):
            for s in n.body + n.orelse + n.finalbody:
                visit(s, handler_of)
            for h in n.handlers:
                for s in h.body:
                    visit(s, n)
            return
        for child in ast.iter_child_nodes(n):
            visit(child, handler_of)

    visit(node, None)
    return out


def _pairing_for(try_node: ast.Try | None, resolver: Resolver, importer: str) -> dict[str, str]:
    """Bare module -> repo path, for the ``try: from lakebench.x.m import f /
    except ImportError: from m import f`` pattern."""
    if try_node is None:
        return {}
    pairs: dict[str, str] = {}
    for stmt in try_node.body:
        for n, _ in _imports_in(stmt):
            if isinstance(n, ast.ImportFrom) and n.module and n.module.startswith("lakebench."):
                found = resolver.lakebench_module(n.module)
                if found:
                    pairs[n.module.split(".")[-1]] = found
    return pairs


def _called_names(node: ast.AST) -> set[str]:
    """The root names of every call target under ``node`` (``f()``,
    ``m.f()``, ``f()()``)."""
    out: set[str] = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Call):
            f = n.func
            while isinstance(f, (ast.Attribute, ast.Call, ast.Subscript)):
                f = f.func if isinstance(f, ast.Call) else f.value
            if isinstance(f, ast.Name):
                out.add(f.id)
    return out


def _main_guarded(mod: Module) -> set[int]:
    """Ids of statements under ``if __name__ == "__main__":``."""
    out: set[int] = set()
    for top in mod.tree.body:
        if (
            isinstance(top, ast.If)
            and isinstance(top.test, ast.Compare)
            and isinstance(top.test.left, ast.Name)
            and top.test.left.id == "__name__"
        ):
            for n in _module_level_stmts(top.body):
                out.add(id(n))
    return out


class ClosureBuilder:
    def __init__(
        self,
        tree: Tree,
        roots: list[str],
        dynamic_ok: list[dict],
        seeds: Iterable[tuple[str, str]] = (),
    ):
        self.tree = tree
        self.roots = set(roots)
        self.resolver = Resolver(tree, shipped_modules(tree))
        self.modules: dict[str, Module] = {}
        self.result = Closure()
        self.dynamic_ok = dynamic_ok
        self._todo: list[tuple[str, str]] = []
        self._seeds = list(seeds)

    def module(self, path: str) -> Module:
        if path not in self.modules:
            source = self.tree.read(path)
            if source is None:
                raise GuardError(f"{path}: imported but missing")
            self.modules[path] = parse_module(path, source)
        return self.modules[path]

    def _resolve(
        self, importer: str, module: str | None, level: int, pairing: dict[str, str]
    ) -> str | None:
        try:
            return self.resolver.resolve(importer, module, level, pairing)
        except GuardError as e:
            self.result.problems.append(str(e))
            return None

    def _allowed(self, path: str, what: str, occurrence: int) -> bool:
        return any(
            e.get("path") == path
            and e.get("what") == what
            and int(e.get("occurrence", 1)) == occurrence
            for e in self.dynamic_ok
        )

    def _scan_dynamic(self, path: str, node: ast.AST, where: str) -> None:
        """``importlib.import_module`` and ``__import__`` calls, keyed by the
        enclosing function's qualified name and their occurrence in it, so a
        dynamic_ok entry covers one call, not every identical line."""
        counts: dict[str, int] = {}

        def visit(n: ast.AST, qual: str) -> None:
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                qual = f"{qual}.{n.name}" if qual else n.name
            if isinstance(n, ast.Call):
                f = n.func
                name = (
                    f.id
                    if isinstance(f, ast.Name)
                    else (f.attr if isinstance(f, ast.Attribute) else None)
                )
                if name in ("import_module", "__import__"):
                    what = f"{qual or where}:{name}"
                    counts[what] = counts.get(what, 0) + 1
                    if not self._allowed(path, what, counts[what]):
                        self.result.problems.append(
                            f"{path}:{n.lineno}: dynamic import {name} in {qual or where}; "
                            f'list it in dynamic_ok ({{"path": "{path}", "what": "{what}", '
                            f'"occurrence": {counts[what]}, "reason": ...}}) or remove it'
                        )
            for child in ast.iter_child_nodes(n):
                visit(child, qual)

        visit(node, "")

    def add_imports(
        self, importer: str, scope: ast.AST, module_aliases_scope: ast.AST, where: str
    ) -> None:
        """Pin what ``scope`` imports from repository modules."""
        for stmt, handler_try in _imports_in(scope):
            pairing = _pairing_for(handler_try, self.resolver, importer)
            if isinstance(stmt, ast.ImportFrom):
                target = self._resolve(importer, stmt.module, stmt.level, pairing)
                if target is None:
                    continue
                for alias in stmt.names:
                    if alias.name == "*":
                        self.result.problems.append(
                            f"{importer}: `from {stmt.module} import *` (line {stmt.lineno}) "
                            "cannot be pinned; import names"
                        )
                        continue
                    sub = None
                    if target.endswith("__init__.py"):
                        sub = self.resolver.lakebench_module(
                            f"{target[len('src/') : -len('/__init__.py')].replace('/', '.')}"
                            f".{alias.name}"
                        )
                    if sub is not None:
                        self._module_import(importer, sub, alias.asname or alias.name, scope)
                    else:
                        self._symbol(target, alias.name, importer, stmt.lineno)
            else:
                for alias in stmt.names:
                    target = self._resolve(importer, alias.name, 0, pairing)
                    if target is None:
                        continue
                    bound = alias.asname or alias.name.split(".")[0]
                    if alias.asname is None and "." in alias.name:
                        self.result.problems.append(
                            f"{importer}: `import {alias.name}` (line {stmt.lineno}): "
                            "use `import ... as` or `from ... import`"
                        )
                        continue
                    self._module_import(importer, target, bound, module_aliases_scope)

    def _module_import(self, importer: str, target: str, bound: str, scope: ast.AST) -> None:
        """``import m as bound``: pin each ``bound.attr`` used in ``scope``;
        any other use of the module object escapes the guard."""
        if target in self.roots:
            return
        self.result.modules.add(target)
        attrs: set[str] = set()
        parents: dict[int, ast.AST] = {}
        for p in ast.walk(scope):
            for c in ast.iter_child_nodes(p):
                parents[id(c)] = p
        for n in ast.walk(scope):
            if not (isinstance(n, ast.Name) and n.id == bound and isinstance(n.ctx, ast.Load)):
                continue
            parent = parents.get(id(n))
            if isinstance(parent, ast.Attribute) and parent.value is n:
                attrs.add(parent.attr)
                continue
            if (
                isinstance(parent, ast.Call)
                and isinstance(parent.func, ast.Name)
                and parent.func.id == "getattr"
                and parent.args
                and parent.args[0] is n
            ):
                if (
                    len(parent.args) >= 2
                    and isinstance(parent.args[1], ast.Constant)
                    and isinstance(parent.args[1].value, str)
                ):
                    attrs.add(parent.args[1].value)
                    continue
            self.result.problems.append(
                f"{importer}: module {target} (as {bound}) is used other than by "
                f"attribute access (line {n.lineno}); the guard cannot see what it reaches"
            )
        for a in sorted(attrs):
            self._symbol(target, a, importer, 0)

    def _symbol(self, path: str, name: str, importer: str, lineno: int) -> None:
        if path in self.roots:
            return
        if importer != "floor":
            self.result.modules.add(path)
        key = (path, name)
        if key in self.result.symbols:
            return
        mod = self.module(path)
        if name not in mod.bindings:
            sub = None
            if path.endswith("__init__.py"):
                sub = self.resolver.lakebench_module(
                    f"{path[len('src/') : -len('/__init__.py')].replace('/', '.')}.{name}"
                )
            if sub is None:
                self.result.problems.append(
                    f"{importer}:{lineno}: imports {name} from {path}, which does not define it"
                )
            return
        self.result.symbols.add(key)
        self._todo.append(key)

    def expand(self, path: str, name: str) -> None:
        mod = self.module(path)
        binders = mod.bindings[name]
        nodes = [_top_level_of(mod, b) for b in binders]
        for node in nodes:
            self._scan_dynamic(path, node, f"{name}")
            # Imports inside the binding (re-exports at module level, and
            # imports inside a pinned function body).
            self.add_imports(path, node, node, name)
            for ref in _names_in(node):
                if ref == name or ref not in mod.bindings:
                    continue
                for b in mod.bindings[ref]:
                    if isinstance(b, (ast.Import, ast.ImportFrom)):
                        # A name bound by a module-level import: follow it.
                        self._follow_bound_import(mod, b, ref, node)
                    else:
                        self._symbol(path, ref, path if path in self.result.modules else "floor", 0)

    def _follow_bound_import(
        self, mod: Module, stmt: ast.Import | ast.ImportFrom, ref: str, usage: ast.AST
    ) -> None:
        handler_try = None
        for top in mod.tree.body:
            if isinstance(top, ast.Try):
                for h in top.handlers:
                    if any(s is stmt for s in ast.walk(h)):
                        handler_try = top
        pairing = _pairing_for(handler_try, self.resolver, mod.path)
        if isinstance(stmt, ast.ImportFrom):
            target = self._resolve(mod.path, stmt.module, stmt.level, pairing)
            if target is None:
                return
            for alias in stmt.names:
                if (alias.asname or alias.name) == ref:
                    self._symbol(target, alias.name, mod.path, stmt.lineno)
        else:
            for alias in stmt.names:
                bound = alias.asname or alias.name.split(".")[0]
                if bound != ref:
                    continue
                target = self._resolve(mod.path, alias.name, 0, pairing)
                if target is not None:
                    self._module_import(mod.path, target, bound, usage)

    def _drain(self) -> None:
        while self._todo:
            path, name = self._todo.pop()
            self.expand(path, name)

    def build(self) -> Closure:
        for root in sorted(self.roots):
            if not root.endswith(".py"):
                continue
            source = self.tree.read(root)
            if source is None:
                self.result.problems.append(f"{root}: frozen file missing")
                continue
            mod = parse_module(root, source)
            self._scan_dynamic(root, mod.tree, "module")
            self.add_imports(root, mod.tree, mod.tree, "module")
        for path, name in self._seeds:
            # Pinned symbols no frozen script imports (the DDL renderer, the
            # shipped-file map): expanded like imported ones, so what they
            # call is pinned too.
            self._symbol(path, name, "floor", 0)
        self._drain()
        # Code a module runs at import is pinned by its prelude; what that
        # code calls (a module-level ``X = _make()``, a decorator, a default)
        # is pinned as a symbol, so changing the callee's body moves a pin.
        done: set[str] = set()
        while True:
            todo = sorted(self.result.modules - done)
            if not todo:
                break
            for path in todo:
                done.add(path)
                mod = self.module(path)
                rebound = set(mod.bindings)
                guarded = _main_guarded(mod)
                for stmt in mod.stmts:
                    if id(stmt) in guarded:
                        continue  # runs only when the module is a script's main
                    own = set(_bound_names(stmt))
                    parts = _runs_at_import(stmt, rebound)
                    if isinstance(stmt, ast.ClassDef):
                        # A class's bases are classes it builds on, not code
                        # its definition runs.
                        parts = [p for p in parts if p not in stmt.bases]
                    header = isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                    for part in parts:
                        # What runs: the functions a statement calls; for a
                        # def or class, its decorators and defaults whole.
                        refs = _names_in(part) if header else _called_names(part)
                        for ref in refs - own:
                            for b in mod.bindings.get(ref, []):
                                if isinstance(b, (ast.Import, ast.ImportFrom)):
                                    self._follow_bound_import(mod, b, ref, part)
                                elif not isinstance(b, ast.ClassDef) or ref != getattr(
                                    stmt, "name", None
                                ):
                                    self._symbol(path, ref, path, 0)
            self._drain()
        # A lakebench.* import runs every package __init__ on the way: their
        # import-time code is pinned as preludes.
        for path in sorted(self.result.modules):
            segs = path.split("/")
            for i in range(2, len(segs)):
                init = "/".join(segs[:i]) + "/__init__.py"
                if init.startswith(PKG_DIR) and self.tree.read(init) is not None:
                    self.result.modules.add(init)
        return self.result


# ---------------------------------------------------------------------------
# Entry hashing
# ---------------------------------------------------------------------------


def _single_binding(mod: Module, name: str) -> list[ast.stmt]:
    binders = mod.bindings.get(name, [])
    if len(binders) == 2 and all(isinstance(b, (ast.Import, ast.ImportFrom)) for b in binders):
        # try: from lakebench.x import name / except ImportError: from x import name
        return binders
    if len(binders) != 1:
        raise GuardError(
            f"{mod.path}: pinned name {name} is bound {len(binders)} times at module "
            "scope; a pinned name has exactly one binding"
        )
    return binders


def hash_pysym(mod: Module, name: str) -> str:
    binders = _single_binding(mod, name)
    tops = []
    for b in binders:
        top = _top_level_of(mod, b)
        if top not in tops:
            tops.append(top)
    parts = [_dump(t) for t in tops]
    for stmt in mod.tree.body:
        if stmt in tops or isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        if name in _names_in(stmt):
            parts.append(_dump(stmt))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


_PURE_BUILTIN_CALLS = frozenset({"frozenset", "tuple", "set", "dict", "list"})


def _pure(node: ast.AST | None, rebound: set[str]) -> bool:
    """A display that runs no code when evaluated: constants, names,
    attribute reads, containers, arithmetic and f-strings of those, and
    ``frozenset``/``tuple``/``set``/``dict``/``list`` of them (when the
    module does not rebind those names)."""
    if node is None:
        return True
    if isinstance(node, (ast.Constant, ast.Name)):
        return True
    if isinstance(node, ast.Attribute):
        return _pure(node.value, rebound)
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return all(_pure(e, rebound) for e in node.elts)
    if isinstance(node, ast.Dict):
        return all(_pure(k, rebound) for k in node.keys) and all(
            _pure(v, rebound) for v in node.values
        )
    if isinstance(node, ast.Starred):
        return _pure(node.value, rebound)
    if isinstance(node, ast.UnaryOp):
        return _pure(node.operand, rebound)
    if isinstance(node, ast.BinOp):
        return _pure(node.left, rebound) and _pure(node.right, rebound)
    if isinstance(node, ast.JoinedStr):
        return all(_pure(v, rebound) for v in node.values)
    if isinstance(node, ast.FormattedValue):
        return _pure(node.value, rebound) and _pure(node.format_spec, rebound)
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _STR_METHODS
        and isinstance(node.func.value, ast.Constant)
        and isinstance(node.func.value.value, str)
        and not node.args
        and not node.keywords
    ):
        return True
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in _PURE_BUILTIN_CALLS
        and node.func.id not in rebound
        and not node.keywords
    ):
        return all(_pure(a, rebound) for a in node.args)
    return False


def _is_docstring_like(stmt: ast.stmt) -> bool:
    return isinstance(stmt, ast.Pass) or (
        isinstance(stmt, ast.Expr)
        and isinstance(stmt.value, ast.Constant)
        and isinstance(stmt.value.value, str)
    )


def _pure_assignment(stmt: ast.stmt, rebound: set[str]) -> bool:
    if isinstance(stmt, ast.Assign):
        targets = stmt.targets
    elif isinstance(stmt, ast.AnnAssign):
        targets = [stmt.target]
    else:
        return False
    if not all(isinstance(t, ast.Name) for t in targets):
        return False
    return _pure(stmt.value, rebound)


def _runs_at_import(stmt: ast.stmt, rebound: set[str]) -> list[ast.AST]:
    """The parts of a module-level statement that run code when the module
    is imported (other than imports): the statement itself, unless it is a
    docstring or a pure assignment; for defs, decorators and impure
    defaults; for classes, decorators, bases, keywords and class-body
    statements that are not methods, docstrings or pure assignments."""

    def header(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.AST]:
        items: list[ast.AST] = list(fn.decorator_list)
        items += [d for d in fn.args.defaults if not _pure(d, rebound)]
        items += [d for d in fn.args.kw_defaults if d is not None and not _pure(d, rebound)]
        return items

    if isinstance(stmt, (ast.Import, ast.ImportFrom)):
        return []
    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return header(stmt)
    if isinstance(stmt, ast.ClassDef):
        items: list[ast.AST] = [*stmt.decorator_list, *stmt.bases, *stmt.keywords]
        for sub in stmt.body:
            if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                items += header(sub)
            elif not (_is_docstring_like(sub) or _pure_assignment(sub, rebound)):
                items.append(sub)
        return items
    if _is_docstring_like(stmt) or _pure_assignment(stmt, rebound):
        return []
    if isinstance(stmt, (ast.If, ast.Try, ast.With, ast.For, ast.While, ast.Match)):
        return [
            v
            for k, v in ast.iter_fields(stmt)
            if k not in ("body", "orelse", "finalbody", "handlers", "cases")
            and isinstance(v, ast.AST)
        ] + [
            x
            for k, v in ast.iter_fields(stmt)
            if k not in ("body", "orelse", "finalbody", "handlers", "cases") and isinstance(v, list)
            for x in v
            if isinstance(x, ast.AST)
        ]
    return [stmt]


def hash_prelude(
    mod: Module, referenced: set[str], is_repo_import: Callable[[ast.stmt], bool]
) -> str:
    """What a module runs at import: the parts of each module-level
    statement that run code (``_runs_at_import``; a class header counts
    only with decorators, keywords or a repository base), and the imports
    that run repository code or bind a name pinned code in this module
    references (so an external name a pinned function uses cannot be
    rebound silently). A new helper, constant, literal table or external
    import that pinned code does not use leaves it unchanged."""
    rebound = set(mod.bindings)
    repo_names = {
        n
        for st in mod.stmts
        if isinstance(st, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        or (isinstance(st, (ast.Import, ast.ImportFrom)) and is_repo_import(st))
        for n in _bound_names(st)
    }
    parts: list[str] = []
    for stmt in mod.stmts:
        if isinstance(stmt, (ast.Import, ast.ImportFrom)):
            if is_repo_import(stmt) or set(_bound_names(stmt)) & referenced:
                parts.append(_dump(stmt))
            continue
        items = _runs_at_import(stmt, rebound)
        if isinstance(stmt, ast.ClassDef):
            plain_header = not stmt.decorator_list and not stmt.keywords
            bases_repo = any(_names_in(b) & repo_names for b in stmt.bases)
            if plain_header and not bases_repo:
                items = [i for i in items if i not in stmt.bases]
        if not items:
            continue
        label = getattr(stmt, "name", type(stmt).__name__)
        parts.append(f"{label}:" + "|".join(_dump(i) for i in items))
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _repo_import_checker(tree: Tree, importer: str) -> Callable[[ast.stmt], bool]:
    resolver = Resolver(tree, shipped_modules(tree))

    def check(stmt: ast.stmt) -> bool:
        mods = (
            [(stmt.module, stmt.level)]
            if isinstance(stmt, ast.ImportFrom)
            else [(a.name, 0) for a in getattr(stmt, "names", [])]
        )
        for m, level in mods:
            try:
                if resolver.resolve(importer, m, level, {}) is not None:
                    return True
            except GuardError:
                return True  # an import the guard cannot place is treated as local
        return False

    return check


def _dump_any(v: Any) -> Any:
    if isinstance(v, ast.AST):
        return _dump(v)
    if isinstance(v, list):
        return [_dump_any(x) for x in v]
    return v


_NOT_LITERAL = object()
_STR_METHODS = ("strip", "lstrip", "rstrip")


def _literal_value(node: ast.AST) -> Any:
    """``ast.literal_eval``, plus a string literal with ``.strip()`` (the
    DDL constants are written that way)."""
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _STR_METHODS
        and not node.args
        and not node.keywords
        and isinstance(node.func.value, ast.Constant)
        and isinstance(node.func.value.value, str)
    ):
        return getattr(node.func.value.value, node.func.attr)()
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
        return _NOT_LITERAL


def _json_default(o: Any) -> Any:
    if isinstance(o, (set, frozenset)):
        return sorted(o, key=repr)
    return list(o)


def hash_pyattr(mod: Module, names: Iterable[str]) -> str:
    values: dict[str, Any] = {}
    for name in names:
        binders = _single_binding(mod, name)
        stmt = binders[0]
        if not isinstance(stmt, (ast.Assign, ast.AnnAssign)) or stmt.value is None:
            raise GuardError(f"{mod.path}: {name} is not a plain assignment")
        value = _literal_value(stmt.value)
        if value is _NOT_LITERAL:
            raise GuardError(f"{mod.path}: {name} is not a literal")
        values[name] = value
    return hashlib.sha256(json.dumps(values, sort_keys=True, default=list).encode()).hexdigest()


def hash_ci_job(tree: Tree, job: str) -> str:
    import yaml

    source = tree.read(CI_PATH)
    if source is None:
        return "missing"
    doc = yaml.safe_load(source) or {}
    body = (doc.get("jobs") or {}).get(job)
    if body is None:
        return "missing"
    # Workflow-level settings that change what a job's steps do (the default
    # shell, the environment) are part of each pinned job.
    workflow = {str(k): v for k, v in doc.items() if str(k) in ("defaults", "env")}
    return hashlib.sha256(
        json.dumps({"job": body, "workflow": workflow}, sort_keys=True, default=str).encode()
    ).hexdigest()


def entry_key(e: dict) -> tuple:
    kind = e["kind"]
    if kind == "pysym":
        return (kind, e["path"], e["name"])
    if kind == "pyattr":
        return (kind, e["path"], tuple(e["names"]))
    if kind == "closed-glob":
        return (kind, e["glob"])
    if kind == "ci-job":
        return (kind, e["path"], e["job"])
    return (kind, e["path"])


@dataclass
class State:
    entries: dict[tuple, dict]
    problems: list[str]
    closure: Closure
    # Paths the entries were computed from (read, or looked up and missing)
    # and the glob prefixes: a commit touching none of them has the same
    # entries (the mutation scan, which reads every module, is not counted;
    # it only matters to check-tree).
    deps: set[str] = field(default_factory=set)
    dep_prefixes: tuple[str, ...] = ()


class _RecordingTree(Tree):
    def __init__(self, inner: Tree):
        self.inner = inner
        self.read_paths: set[str] = set()

    def read(self, path: str) -> bytes | None:
        self.read_paths.add(path)
        return self.inner.read(path)

    def files(self) -> list[str]:
        return self.inner.files()


def _glob_files(tree: Tree, pattern: str) -> list[str]:
    if pattern.endswith("/**"):
        prefix = pattern[:-2]
        return [f for f in tree.files() if f.startswith(prefix)]
    return [f for f in tree.files() if fnmatch.fnmatch(f, pattern)]


def compute(tree: Tree, doc: dict | None = None) -> State:
    """Every entry the tree must have, with its hash, derived from the
    floor and the closure (never from the stored list), plus the list's own
    extra ``file`` and ``append-only`` entries."""
    problems: list[str] = []
    entries: dict[tuple, dict] = {}
    dynamic_ok = (doc or {}).get("dynamic_ok", [])
    base_tree = tree
    tree = _RecordingTree(tree)

    def add(e: dict) -> None:
        entries[entry_key(e)] = e

    files = set(REQUIRED_FILES)
    globs = set(REQUIRED_GLOBS)
    for g in globs:
        matched = _glob_files(tree, g)
        files |= set(matched)
        add({"kind": "closed-glob", "glob": g, "files": len(matched)})
    extra_append: list[str] = []
    for e in (doc or {}).get("entries", []):
        if e.get("kind") == "file":
            files.add(e["path"])
        if e.get("kind") == "append-only":
            extra_append.append(e["path"])
    if tree.read(HELDOUT_PATH) is not None and HELDOUT_PATH not in extra_append:
        extra_append.append(HELDOUT_PATH)
    for f in sorted(files):
        add({"kind": "file", "path": f, "sha256": _sha(tree.read(f))})
    for p in extra_append:
        add({"kind": "append-only", "path": p, "sha256": _sha(tree.read(p))})

    roots = sorted(f for f in files if f.endswith(".py") and f.startswith("src/"))
    try:
        closure = ClosureBuilder(tree, roots, dynamic_ok, REQUIRED_PYSYMS).build()
    except GuardError as e:
        closure = Closure(problems=[str(e)])
    problems += closure.problems
    modules: dict[str, Module] = {}

    def module(path: str) -> Module | None:
        if path not in modules:
            source = tree.read(path)
            if source is None:
                problems.append(f"{path}: missing")
                return None
            modules[path] = parse_module(path, source)
        return modules[path]

    pysyms = set(closure.symbols) | set(REQUIRED_PYSYMS)
    for path, name in sorted(pysyms):
        mod = module(path)
        if mod is None:
            continue
        try:
            sha = hash_pysym(mod, name)
        except GuardError as e:
            problems.append(str(e))
            sha = "error"
        add({"kind": "pysym", "path": path, "name": name, "sha256": sha})
    for path in sorted(closure.modules):
        mod = module(path)
        if mod is None:
            continue
        for stmt in mod.stmts:
            if isinstance(stmt, ast.ImportFrom) and any(a.name == "*" for a in stmt.names):
                problems.append(
                    f"{path}:{stmt.lineno}: `from {stmt.module} import *` in a module the "
                    "frozen scripts import: name what it imports"
                )
        # Names pinned code in this module reads: its pinned symbols, what
        # their binding statements and every module-level statement naming
        # them read, and what import-time code reads.
        mine = {n for (p, n) in pysyms if p == path}
        referenced = set(mine)
        for n in mine:
            for b in mod.bindings.get(n, []):
                referenced |= _names_in(_top_level_of(mod, b))
        rebound_all = set(mod.bindings)
        for stmt in mod.tree.body:
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            if _names_in(stmt) & mine:
                referenced |= _names_in(stmt)
        for stmt in mod.stmts:
            for part in _runs_at_import(stmt, rebound_all):
                referenced |= _names_in(part)
        add(
            {
                "kind": "prelude",
                "path": path,
                "sha256": hash_prelude(mod, referenced, _repo_import_checker(tree, path)),
            }
        )
    for path, names in REQUIRED_PYATTRS:
        mod = module(path)
        if mod is None:
            continue
        try:
            sha = hash_pyattr(mod, names)
        except GuardError as e:
            problems.append(str(e))
            sha = "error"
        add({"kind": "pyattr", "path": path, "names": list(names), "sha256": sha})
    for job in REQUIRED_CI_JOBS:
        add({"kind": "ci-job", "path": CI_PATH, "job": job, "sha256": hash_ci_job(tree, job)})
    deps = set(tree.read_paths) | {LIST_PATH, CI_PATH}
    problems += mutation_problems(base_tree, pysyms, dynamic_ok)
    prefixes = tuple(g[:-2] for g in REQUIRED_GLOBS if g.endswith("/**"))
    return State(
        entries=entries, problems=problems, closure=closure, deps=deps, dep_prefixes=prefixes
    )


_RAW: dict[bytes, ast.Module | None] = {}


def _raw_ast(source: bytes) -> ast.Module | None:
    key = hashlib.sha1(source).digest()
    if key not in _RAW:
        try:
            _RAW[key] = ast.parse(source)
        except SyntaxError:
            _RAW[key] = None
    return _RAW[key]


_MUTATING_FUNCS = (
    frozenset({"setitem", "delitem", "setattr", "delattr", "iadd", "ior", "iand", "isub"})
    | MUTATING_METHODS
)


def mutation_problems(
    tree: Tree, pinned: set[tuple[str, str]], dynamic_ok: list[dict]
) -> list[str]:
    """A body or module top level that mutates a pinned name, in any module
    that defines or imports it (directly, through a module alias, or
    relatively). Module-level mutations in the defining module are hashed
    into the pin; anything else that mutates one is refused unless
    dynamic_ok lists it."""
    by_module: dict[str, set[str]] = {}
    for path, name in pinned:
        by_module.setdefault(path, set()).add(name)
    candidates = [
        f
        for f in tree.files()
        if f.endswith(".py") and (f.startswith(PKG_DIR + "/") or f.startswith(SCRIPTS_DIR + "/"))
    ]
    resolver = Resolver(tree, shipped_modules(tree))
    problems: list[str] = []
    for f in candidates:
        source = tree.read(f)
        if source is None:
            continue
        mod = _raw_ast(source)
        if mod is None:
            continue
        names: dict[str, tuple[str, str]] = {n: (f, n) for n in by_module.get(f, ())}
        modules: dict[str, str] = {}
        for n in ast.walk(mod):
            if isinstance(n, ast.ImportFrom):
                try:
                    target = resolver.resolve(f, n.module, n.level, {})
                except GuardError:
                    continue
                if target is None:
                    continue
                for al in n.names:
                    bound = al.asname or al.name
                    sub = None
                    if target.endswith("__init__.py"):
                        try:
                            sub = resolver.lakebench_module(
                                target[len("src/") : -len("/__init__.py")].replace("/", ".")
                                + "."
                                + al.name
                            )
                        except GuardError:
                            sub = None
                    if sub in by_module:
                        modules[bound] = sub
                    elif target in by_module and al.name in by_module[target]:
                        names[bound] = (target, al.name)
            elif isinstance(n, ast.Import):
                for al in n.names:
                    try:
                        target = resolver.resolve(f, al.name, 0, {})
                    except GuardError:
                        continue
                    if target in by_module:
                        modules[al.asname or al.name.split(".")[0]] = target
        if not names and not modules:
            continue

        def pinned_ref(
            node: ast.AST,
            names: dict[str, tuple[str, str]] = names,
            modules: dict[str, str] = modules,
        ) -> tuple[str, str] | None:
            if isinstance(node, ast.Name) and node.id in names:
                return names[node.id]
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id in modules
                and node.attr in by_module[modules[node.value.id]]
            ):
                return (modules[node.value.id], node.attr)
            return None

        def mutation(
            n: ast.AST,
            names: dict[str, tuple[str, str]] = names,
            modules: dict[str, str] = modules,
        ) -> tuple[str, str] | None:
            if isinstance(n, ast.Global):
                for x in n.names:
                    if x in names:
                        return names[x]
                return None
            if isinstance(n, (ast.Assign, ast.AugAssign, ast.AnnAssign, ast.Delete)):
                targets = n.targets if isinstance(n, (ast.Assign, ast.Delete)) else [n.target]
                for t in targets:
                    if isinstance(n, ast.AugAssign) and pinned_ref(t):
                        return pinned_ref(t)  # in-place: x |= ..., m.X += ...
                    if isinstance(t, ast.Attribute) and pinned_ref(t):
                        return pinned_ref(t)  # m.X = ... rebinds the module's name
                    base = t
                    while isinstance(base, (ast.Subscript, ast.Attribute)):
                        base = base.value
                        hit = pinned_ref(base)
                        if hit:
                            return hit
                return None
            if isinstance(n, ast.Call):
                f_ = n.func
                if isinstance(f_, ast.Attribute) and f_.attr in MUTATING_METHODS:
                    hit = pinned_ref(f_.value)
                    if hit:
                        return hit
                fname = (
                    f_.attr
                    if isinstance(f_, ast.Attribute)
                    else (f_.id if isinstance(f_, ast.Name) else None)
                )
                if fname in _MUTATING_FUNCS and n.args:
                    hit = pinned_ref(n.args[0])
                    if hit:
                        return hit
                    first = n.args[0]
                    if (
                        fname in ("setattr", "delattr")
                        and isinstance(first, ast.Name)
                        and first.id in modules
                        and len(n.args) > 1
                        and isinstance(n.args[1], ast.Constant)
                        and n.args[1].value in by_module[modules[first.id]]
                    ):
                        return (modules[first.id], str(n.args[1].value))
            return None

        scopes: list[tuple[str, list[ast.AST], int]] = [
            (fn.name, list(ast.walk(fn)), fn.lineno)
            for fn in ast.walk(mod)
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        ]
        if f not in by_module:
            # Another module's top level, mutating a name it imports, runs
            # whenever that module is imported (a module-level mutation in
            # the defining module is hashed into the pin instead).
            top = [
                n
                for stmt in _module_level_stmts(mod.body)
                if not isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                for n in _walk_no_scopes(stmt)
            ]
            scopes.append(("<module>", top, 1))
        for scope_name, nodes, scope_line in scopes:
            counter: dict[str, int] = {}
            for n in nodes:
                hit = mutation(n)
                if hit is None:
                    continue
                what = f"{scope_name}:{hit[1]}"
                counter[what] = counter.get(what, 0) + 1
                ok = any(
                    e.get("path") == f
                    and e.get("what") == what
                    and int(e.get("occurrence", 1)) == counter[what]
                    for e in dynamic_ok
                )
                if not ok:
                    problems.append(
                        f"{f}:{getattr(n, 'lineno', scope_line)}: {scope_name} mutates pinned "
                        f"{hit[0]}:{hit[1]}; list it in dynamic_ok "
                        f'({{"path": "{f}", "what": "{what}", "occurrence": {counter[what]}, '
                        '"reason": ...}) or move the change to module scope'
                    )
    return problems


# ---------------------------------------------------------------------------
# The list
# ---------------------------------------------------------------------------


def require_python() -> None:
    have = f"{sys.version_info[0]}.{sys.version_info[1]}"
    if have != AST_PYTHON:
        raise GuardError(f"frozen guard hashes are defined on Python {AST_PYTHON}, not {have}")


def load_list(tree: Tree) -> dict | None:
    raw = tree.read(LIST_PATH)
    if raw is None:
        return None
    doc = json.loads(raw)
    if doc.get("schema") != SCHEMA:
        raise GuardError(f"{LIST_PATH}: schema {doc.get('schema')!r}, expected {SCHEMA}")
    if doc.get("ast_python") != AST_PYTHON:
        raise GuardError(
            f"{LIST_PATH}: ast_python {doc.get('ast_python')!r}, expected {AST_PYTHON}"
        )
    return doc


def canonical(entries: Iterable[dict], dynamic_ok: Iterable[dict]) -> bytes:
    body = {
        "entries": sorted(entries, key=lambda e: json.dumps(entry_key(e))),
        "dynamic_ok": sorted(dynamic_ok, key=lambda e: json.dumps(e, sort_keys=True)),
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def tree_problems(tree: Tree, doc: dict | None) -> tuple[list[str], State | None]:
    if doc is None:
        return [f"{LIST_PATH} is missing"], None
    state = compute(tree, doc)
    problems = list(state.problems)
    stored = {entry_key(e): e for e in doc.get("entries", [])}
    for key, e in sorted(state.entries.items(), key=lambda kv: json.dumps(kv[0])):
        s = stored.get(key)
        if s is None:
            problems.append(f"{_describe(key)}: not in {LIST_PATH} (run `frozen_guard.py regen`)")
        elif s != e:
            problems.append(
                f"{_describe(key)}: changed ({s.get('sha256', s)} -> {e.get('sha256', e)}); "
                "a frozen change updates the list in the same commit and carries a "
                f"{TRAILER_KEY}: trailer"
            )
    for key in stored:
        if key not in state.entries:
            problems.append(
                f"{_describe(key)}: in {LIST_PATH} but not pinned by the floor or the closure "
                "(stale entry; run `frozen_guard.py regen`)"
            )
    if doc.get("state") not in ("open", "locked", "spent"):
        problems.append(f"{LIST_PATH}: unknown state {doc.get('state')!r}")
    if doc.get("state") == "locked":
        problems += _locked_problems(tree, doc, state)
    return problems, state


def _locked_problems(tree: Tree, doc: dict, state: State) -> list[str]:
    locked = doc.get("locked") or {}
    out = []
    preds = tree.read(PREDICTIONS_PATH)
    if preds is None or hashlib.sha256(preds).hexdigest() != locked.get("predictions_sha256"):
        out.append("locked: predictions_sha256 does not match the committed predictions")
    manifest = hashlib.sha256(
        canonical(state.entries.values(), doc.get("dynamic_ok", []))
    ).hexdigest()
    if manifest != locked.get("manifest_sha256"):
        out.append("locked: manifest_sha256 does not match the tree")
    if preds is not None:
        try:
            recorded = json.loads(preds).get("frozen_manifest_sha256")
        except json.JSONDecodeError:
            recorded = None
        if recorded != locked.get("manifest_sha256"):
            out.append("locked: the predictions' frozen_manifest_sha256 differs from the lock's")
    return out


def _describe(key: tuple) -> str:
    return ":".join(str(k if not isinstance(k, tuple) else ",".join(k)) for k in key)


def manifest_sha256(tree: Tree) -> str:
    """The manifest the predictions record, recomputed from the tree; raises
    when the tree does not match the list."""
    require_python()
    doc = load_list(tree)
    problems, state = tree_problems(tree, doc)
    if problems:
        raise GuardError("frozen tree does not match the list:\n" + "\n".join(problems))
    assert state is not None and doc is not None
    return hashlib.sha256(canonical(state.entries.values(), doc.get("dynamic_ok", []))).hexdigest()


def regen(root: Path) -> list[str]:
    require_python()
    tree = WorkTree(root)
    doc = load_list(tree) or {
        "schema": SCHEMA,
        "state": "open",
        "locked": None,
        "ast_python": AST_PYTHON,
        "dynamic_ok": [],
        "entries": [],
    }
    if doc.get("state") != "open":
        raise GuardError(f"regen refused: the list is {doc.get('state')}")
    state = compute(tree, doc)
    doc["entries"] = sorted(state.entries.values(), key=lambda e: json.dumps(entry_key(e)))
    (root / LIST_PATH).write_text(json.dumps(doc, indent=1, sort_keys=False) + "\n")
    return state.problems


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------


def _rev_parents(root: Path, rev_range: list[str]) -> list[tuple[str, list[str]]]:
    out = _git(root, "rev-list", "--parents", "--topo-order", "--reverse", *rev_range)
    rows = []
    for line in out.decode().splitlines():
        parts = line.split()
        rows.append((parts[0], parts[1:]))
    return rows


def _blob(root: Path, commit: str, path: str) -> str | None:
    out = _git(root, "rev-parse", "--verify", "--quiet", f"{commit}:{path}", check=False)
    s = out.decode().strip()
    return s or None


def creation_commit(root: Path, head: str) -> str | None:
    out = _git(root, "log", "--format=%H", "--diff-filter=A", "--reverse", head, "--", LIST_PATH)
    lines = out.decode().split()
    return lines[0] if lines else None


@dataclass
class CommitState:
    """A commit's frozen content as this guard computes it, and its stored
    list's keys and meta (state, lock, dynamic_ok)."""

    hashes: dict[tuple, str]
    meta: str
    stored_keys: set[tuple]
    error: str | None = None
    deps: set[str] = field(default_factory=set)
    dep_prefixes: tuple[str, ...] = ()


def frozen_state(root: Path, commit: str, cache: dict[str, CommitState]) -> CommitState:
    if commit in cache:
        return cache[commit]
    tree = GitTree(root, commit)
    raw = tree.read(LIST_PATH)
    try:
        doc = json.loads(raw) if raw else None
    except json.JSONDecodeError as e:
        doc = None
        error: str | None = f"{LIST_PATH} is not JSON: {e}"
    else:
        error = None
    deps: set[str] = set()
    prefixes: tuple[str, ...] = ()
    try:
        state = compute(tree, doc)
        hashes = {
            k: e.get("sha256") or json.dumps(e, sort_keys=True) for k, e in state.entries.items()
        }
        deps, prefixes = state.deps, state.dep_prefixes
    except GuardError as e:
        hashes, error = {}, str(e)
    meta = (
        json.dumps({k: doc.get(k) for k in ("state", "locked", "dynamic_ok")}, sort_keys=True)
        if doc
        else "no list"
    )
    stored = {entry_key(e) for e in (doc or {}).get("entries", [])}
    cs = CommitState(
        hashes=hashes, meta=meta, stored_keys=stored, error=error, deps=deps, dep_prefixes=prefixes
    )
    cache[commit] = cs
    return cs


def _changed_paths(root: Path, commit: str, parent: str) -> tuple[list[str], list[str]]:
    """(changed paths, added paths) between parent and commit."""
    out = _git(root, "diff-tree", "-r", "-z", "--no-commit-id", "--name-status", parent, commit)
    fields = [f for f in out.decode().split("\0") if f]
    changed, added = [], []
    i = 0
    while i < len(fields):
        status = fields[i]
        if status[0] in "RC":
            changed += [fields[i + 1], fields[i + 2]]
            added.append(fields[i + 2])
            i += 3
            continue
        changed.append(fields[i + 1])
        if status[0] == "A":
            added.append(fields[i + 1])
        i += 2
    return changed, added


def trailers(root: Path, commit: str) -> list[str]:
    out = _git(
        root,
        "log",
        "-1",
        f"--format=%(trailers:key={TRAILER_KEY},valueonly,separator=%x1f)",
        commit,
    )
    text = out.decode().strip()
    return [t.strip() for t in text.split("\x1f") if t.strip()] if text else []


def _api_get(url: str, token: str) -> Any:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        raise GuardError(f"GitHub API {url}: {e}") from e


def parity_runs_ok(commit: str, token: str, repo: str, api: str) -> tuple[bool, str]:
    """The latest ci.yml workflow run on this commit has both AML parity
    jobs concluded success. Only runs of ci.yml count, so a job of the same
    name in another workflow cannot stand in for it."""
    runs = _api_get(f"{api}/repos/{repo}/actions/runs?head_sha={commit}&per_page=100", token)
    # Only push runs test the commit itself (a pull_request run tests the
    # merge with its base).
    ci_runs = [
        r
        for r in runs.get("workflow_runs", [])
        if r.get("path") == CI_PATH and r.get("event") == "push"
    ]
    if not ci_runs:
        return False, f"no {CI_PATH} push run on this commit"
    run = max(ci_runs, key=lambda r: (r.get("run_number", 0), r.get("run_attempt", 0)))
    jobs = _api_get(f"{run['jobs_url']}?per_page=100&filter=latest", token).get("jobs", [])
    conclusions = {j.get("name"): j.get("conclusion") for j in jobs}
    missing = [n for n in PARITY_CHECKS if conclusions.get(n) != "success"]
    if missing:
        return False, f"run {run.get('id')}: " + ", ".join(
            f"{n}={conclusions.get(n)}" for n in missing
        )
    return True, ""


def _guard_digest() -> str:
    """Identifies this guard's rules: a cached state from another guard
    version is never reused."""
    source = Path(__file__).read_bytes()
    floor = repr(
        (REQUIRED_FILES, REQUIRED_GLOBS, REQUIRED_PYATTRS, REQUIRED_PYSYMS, REQUIRED_CI_JOBS)
    )
    return hashlib.sha256(source + floor.encode()).hexdigest()


def _tuplify(x: Any) -> Any:
    return tuple(_tuplify(i) for i in x) if isinstance(x, list) else x


def _load_state_cache(path: Path | None) -> dict[str, CommitState]:
    if path is None or not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if raw.get("guard") != _guard_digest():
        return {}
    out: dict[str, CommitState] = {}
    for sha, d in (raw.get("states") or {}).items():
        out[sha] = CommitState(
            hashes={_tuplify(json.loads(k)): v for k, v in d["hashes"].items()},
            meta=d["meta"],
            stored_keys={_tuplify(json.loads(k)) for k in d["stored_keys"]},
            error=d.get("error"),
            deps=set(d.get("deps", [])),
            dep_prefixes=tuple(d.get("dep_prefixes", [])),
        )
    return out


def _save_state_cache(path: Path | None, cache: dict[str, CommitState]) -> None:
    if path is None:
        return
    states = {
        sha: {
            "hashes": {json.dumps(k): v for k, v in cs.hashes.items()},
            "meta": cs.meta,
            "stored_keys": sorted(json.dumps(k) for k in cs.stored_keys),
            "error": cs.error,
            "deps": sorted(cs.deps),
            "dep_prefixes": list(cs.dep_prefixes),
        }
        for sha, cs in cache.items()
    }
    path.write_text(json.dumps({"guard": _guard_digest(), "states": states}))


def range_problems(
    root: Path,
    head: str,
    parity_result: str | None,
    token: str | None,
    repo: str | None,
    api: str = "https://api.github.com",
    require_all: bool = False,
    evidence: Callable[[str], tuple[bool, str]] | None = None,
    state_cache: Path | None = None,
) -> tuple[list[str], list[str]]:
    """(problems, notes) for the list's creation commit and every commit
    descended from it up to head (``--ancestry-path``). A commit is walked
    when one of the entries its or its parents' list holds, or the list's
    state, lock or dynamic_ok, has a value that differs from every parent's
    (for a merge: an evil merge, or the merge that brings a pre-guard lane's
    frozen edits in). Walking always starts at the creation commit, so a
    force push or a new branch cannot shrink the range; a lane forked before
    the guard is judged at the merge that brings it in."""
    require_python()
    head = _git(root, "rev-parse", head).decode().strip()
    creation = creation_commit(root, head)
    if creation is None:
        return [f"{LIST_PATH} does not exist at {head[:12]}"], []
    first = _git(root, "rev-list", "--parents", "-n", "1", creation).decode().split()
    rows = [(first[0], first[1:])]
    rows += _rev_parents(root, ["--ancestry-path", f"{creation}..{head}"])
    # Frozen states of earlier runs (CI restores them), keyed by commit and
    # valid only for this guard's rules; a commit's state is a function of
    # its tree, so a cached one is as good as a recomputed one.
    cache: dict[str, CommitState] = _load_state_cache(state_cache)
    problems: list[str] = []
    notes: list[str] = []
    head_state = frozen_state(root, head, cache)
    evidence_cache: dict[str, tuple[bool, str]] = {}
    guarded = {c for c, _ in rows}
    for commit, all_parents in rows:
        # A parent from before the list (a lane forked before the guard) was
        # never checked, so the content it brings in is judged at this
        # commit: only guarded parents count as already accepted.
        parents = [p for p in all_parents if p in guarded] or all_parents
        if len(all_parents) == 1 and commit != creation:
            # A commit that touches no path its parent's entries depend on
            # (and adds no Python file under src/, which could change how an
            # import resolves) has its parent's entries.
            parent_state = frozen_state(root, parents[0], cache)
            touched, added = _changed_paths(root, commit, parents[0])
            if (
                not parent_state.error
                and not any(
                    p in parent_state.deps or p.startswith(parent_state.dep_prefixes)
                    for p in touched
                )
                and not any(p.startswith("src/") and p.endswith(".py") for p in added)
            ):
                cache[commit] = parent_state
                continue
        here = frozen_state(root, commit, cache)
        short = commit[:12]
        if here.error:
            problems.append(f"{short}: frozen state cannot be computed: {here.error}")
            continue
        pstates = [frozen_state(root, p, cache) for p in parents]
        if commit == creation:
            changed_keys: set[tuple] = {("list",)}
        else:
            # Keys any of the lists hold, and keys the computation finds
            # that no parent's does (a change committed apart from its regen
            # still shows here, as a new pin).
            keys = set(here.stored_keys).union(*(ps.stored_keys for ps in pstates))
            keys |= {k for k in here.hashes if all(k not in ps.hashes for ps in pstates)}
            changed_keys = {
                k for k in keys if all(ps.hashes.get(k) != here.hashes.get(k) for ps in pstates)
            }
            if all(ps.meta != here.meta for ps in pstates):
                changed_keys.add(("meta",))
        if not changed_keys:
            continue
        values = trailers(root, commit)
        if len(values) != 1:
            problems.append(
                f"{short}: changes frozen content ({_describe(sorted(changed_keys)[0])}"
                f"{' and more' if len(changed_keys) > 1 else ''}) and carries {len(values)} "
                f"{TRAILER_KEY}: trailers (exactly one of {', '.join(TRAILER_VALUES)} is required)"
            )
            continue
        value = values[0]
        if value == "Void":
            problems.append(
                f"{short}: Void needs an owner decision and a new seed (SPEC section 10)"
            )
            continue
        if value not in TRAILER_VALUES:
            problems.append(f"{short}: unknown {TRAILER_KEY}: {value!r}")
            continue
        if value == "None (append)":
            if not all(k[0] == "append-only" for k in changed_keys):
                problems.append(f"{short}: None (append) on a change beyond an append-only entry")
            continue
        # Every other value claims something about the frozen output, so
        # each needs green parity on the content it claims it for.
        if commit == head or here.hashes == head_state.hashes:
            if parity_result != "success":
                problems.append(
                    f"{short}: {value} needs green AML parity; this run's parity "
                    f"result is {parity_result!r}"
                )
            elif commit != head:
                notes.append(f"{short}: {value}, covered by this run (same frozen content as head)")
            continue
        if commit not in evidence_cache:
            if evidence is not None:
                evidence_cache[commit] = evidence(commit)
            elif token and repo:
                try:
                    evidence_cache[commit] = parity_runs_ok(commit, token, repo, api)
                except GuardError as e:
                    evidence_cache[commit] = (False, str(e))
            else:
                msg = f"{short}: SKIP parity evidence (no GITHUB_TOKEN / GITHUB_REPOSITORY)"
                (problems if require_all else notes).append(msg)
                continue
        ok, why = evidence_cache[commit]
        if not ok:
            problems.append(
                f"{short}: {value} without green AML parity on this commit ({why}); push a "
                "branch at this commit and let its CI run finish, or have its frozen content "
                "be the head's"
            )
        elif value in ("Rebuild", "Re-derive"):
            notes.append(
                f"{short}: {value} (its rebuild or re-derivation is reviewed, not checked)"
            )
    _save_state_cache(state_cache, cache)
    return problems, notes


def history_problems(root: Path, head: str = "HEAD") -> list[str]:
    """Per commit, against each of its own parents: the list's state
    machine, and the append-only files' rules. Every commit whose list is
    locked has the frozen content of the commit that locked it, both
    computed by this guard's rules (so a later rule change moves both sides
    alike, and an edit made under a lock stays visible after the lock
    ends)."""
    require_python()
    rows = _rev_parents(root, [head])
    problems: list[str] = []
    seen_creation = False
    lock_start: dict[str, str] = {}
    state_cache: dict[str, CommitState] = {}
    for commit, parents in rows:
        blob = _blob(root, commit, LIST_PATH)
        parent_blobs = [_blob(root, p, LIST_PATH) for p in parents]
        short = commit[:12]
        if blob is None:
            if any(b is not None for b in parent_blobs):
                problems.append(f"{short}: deletes {LIST_PATH}")
            continue
        if all(b is None for b in parent_blobs):
            if seen_creation:
                problems.append(f"{short}: re-creates {LIST_PATH}")
            seen_creation = True
            continue
        new = json.loads(_git(root, "show", f"{commit}:{LIST_PATH}"))
        for parent, pblob in zip(parents, parent_blobs, strict=True):
            if pblob is None:
                problems.append(f"{short}: re-creates {LIST_PATH} against parent {parent[:12]}")
                continue
            if pblob == blob:
                old = new
            else:
                old = json.loads(_git(root, "show", f"{parent}:{LIST_PATH}"))
            problems += _transition_problems(root, commit, parent, old, new)
        if new.get("state") == "locked":
            tree = GitTree(root, commit)
            preds = tree.read(PREDICTIONS_PATH)
            if _sha(preds) != (new.get("locked") or {}).get("predictions_sha256"):
                problems.append(f"{short}: the predictions differ from the locked ones")
            starts = {lock_start[p] for p in parents if p in lock_start}
            start = sorted(starts)[0] if starts else commit
            lock_start[commit] = start
            if start != commit:
                here = frozen_state(root, commit, state_cache)
                then = frozen_state(root, start, state_cache)
                if here.hashes != then.hashes:
                    differ = sorted(
                        k
                        for k in set(here.hashes) | set(then.hashes)
                        if here.hashes.get(k) != then.hashes.get(k)
                    )
                    problems.append(
                        f"{short}: frozen content differs from the locked list "
                        f"({_describe(differ[0])}; an edit after the lock at {start[:12]})"
                    )
        problems += _append_only_problems(root, commit, parents)
    return problems


def _transition_problems(root: Path, commit: str, parent: str, old: dict, new: dict) -> list[str]:
    short = commit[:12]
    a, b = old.get("state"), new.get("state")
    out: list[str] = []
    if a == b == "open":
        return out
    if a == "open" and b == "locked":
        return out
    if a == "locked" and b == "locked":
        if old != new:
            out.append(f"{short}: changes the list while it is locked")
        return out
    if a == "locked" and b == "spent":
        changed = {
            entry_key(e) for e in new.get("entries", []) if e not in old.get("entries", [])
        } | {entry_key(e) for e in old.get("entries", []) if e not in new.get("entries", [])}
        allowed = {("file", PREREG_PATH), ("append-only", HELDOUT_PATH)}
        if not changed <= allowed:
            out.append(f"{short}: locked -> spent changes more than the prereg and heldout entries")
        out += _spent_commit_problems(root, commit, parent)
        return out
    if a == "spent" and b in ("spent", "open"):
        return out
    out.append(f"{short}: list state {a!r} -> {b!r} is not allowed")
    return out


def _spent_commit_problems(root: Path, commit: str, parent: str) -> list[str]:
    short = commit[:12]
    out: list[str] = []
    looks_raw = GitTree(root, commit).read(LOOKS_PATH)
    looks = json.loads(looks_raw).get("looks", []) if looks_raw else []
    for role in PROTECTED_ROLES:
        if not any(
            e.get("role") == role and e.get("state") == "complete" and e.get("report_sha256")
            for e in looks
        ):
            out.append(f"{short}: spent, but the {role} look has no recorded report sha256")
    old_raw = GitTree(root, parent).read(PREREG_PATH)
    new_raw = GitTree(root, commit).read(PREREG_PATH)
    if old_raw != new_raw:
        old_p = json.loads(old_raw or b"{}")
        new_p = json.loads(new_raw or b"{}")
        old_spent = (old_p.get("corpora") or {}).pop("spent_seeds", [])
        new_spent = (new_p.get("corpora") or {}).pop("spent_seeds", [])
        if old_p != new_p:
            out.append(f"{short}: the spent commit changes the prereg beyond corpora.spent_seeds")
        if list(new_spent[: len(old_spent)]) != list(old_spent):
            out.append(
                f"{short}: the spent commit rewrites corpora.spent_seeds instead of appending"
            )
    return out


def _load_history_function(root: Path) -> Callable[[Any, Any, Any], list[str]] | None:
    """datagen_seed.heldout_history_problems, loaded from this checkout by
    file path (never from an installed lakebench, which may be another
    tree)."""
    import importlib.util

    path = root / DATAGEN_SEED_PATH
    if not path.is_file():
        return None
    spec = importlib.util.spec_from_file_location("_frozen_guard_datagen_seed", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as e:  # noqa: BLE001
        raise GuardError(f"loading {DATAGEN_SEED_PATH}: {e}") from e
    return getattr(module, "heldout_history_problems", None)


_HISTORY_FN: dict[str, Any] = {}


def _append_only_problems(root: Path, commit: str, parents: list[str]) -> list[str]:
    blob = _blob(root, commit, HELDOUT_PATH)
    if blob is None:
        if any(_blob(root, p, HELDOUT_PATH) for p in parents):
            return [f"{commit[:12]}: deletes {HELDOUT_PATH}"]
        return []
    out: list[str] = []
    for p in parents:
        pblob = _blob(root, p, HELDOUT_PATH)
        if pblob == blob or pblob is None:
            continue
        if "fn" not in _HISTORY_FN:
            _HISTORY_FN["fn"] = _load_history_function(root)
        fn = _HISTORY_FN["fn"]
        if fn is None:
            out.append(
                f"{commit[:12]}: {HELDOUT_PATH} changed, and this checkout has no "
                "datagen_seed.heldout_history_problems to check it with"
            )
            continue
        old = json.loads(_git(root, "show", f"{p}:{HELDOUT_PATH}"))
        new = json.loads(_git(root, "show", f"{commit}:{HELDOUT_PATH}"))
        looks_raw = GitTree(root, commit).read(LOOKS_PATH)
        looks = json.loads(looks_raw) if looks_raw else {"looks": []}
        out += [f"{commit[:12]}: {msg}" for msg in fn(old, new, looks)]
    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", default=".", help="repository root (default: .)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check-tree")
    r = sub.add_parser("check-range")
    r.add_argument("head", nargs="?", default="HEAD")
    r.add_argument("--parity-result", default=None)
    r.add_argument("--require-all", action="store_true")
    r.add_argument(
        "--state-cache", type=Path, default=None, help="JSON file of frozen states to reuse"
    )
    h = sub.add_parser("check-history")
    h.add_argument("head", nargs="?", default="HEAD")
    sub.add_parser("regen")
    sub.add_parser("manifest")
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    try:
        require_python()
        if args.cmd == "check-tree":
            tree = WorkTree(root)
            problems, _ = tree_problems(tree, load_list(tree))
        elif args.cmd == "check-range":
            problems, notes = range_problems(
                root,
                args.head,
                args.parity_result,
                os.environ.get("GITHUB_TOKEN"),
                os.environ.get("GITHUB_REPOSITORY"),
                require_all=args.require_all,
                state_cache=args.state_cache,
            )
            for n in notes:
                print(n)
        elif args.cmd == "check-history":
            problems = history_problems(root, args.head)
        elif args.cmd == "regen":
            problems = regen(root)
            print(f"wrote {LIST_PATH}")
        else:
            print(manifest_sha256(WorkTree(root)))
            return 0
    except GuardError as e:
        print(f"FAIL {e}", file=sys.stderr)
        return 1
    for p in problems:
        print(f"FAIL {p}", file=sys.stderr)
    if problems:
        return 1
    print(f"OK {args.cmd}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
