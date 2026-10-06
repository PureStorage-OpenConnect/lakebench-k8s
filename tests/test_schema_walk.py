"""CFG-9 (CC-17): every config field has a reader, or the walk fails.

The walk turns ``LakebenchConfig`` into dotted leaf paths and checks each
against ``config/_readers.py``: the named function exists, reads the field
through its parent (or an alias of it, or ``self`` in the owning model), and,
when it lives in ``config/schema.py``, is a validator or is called from
outside that file. Recording a value is not reading it. The snapshot the
perf gate and the records carry (``collector.build_config_snapshot``) may only
record fields that are read.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import re
import textwrap
import typing
from functools import cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from lakebench.config import _readers
from lakebench.config.schema import ConfigModel, LakebenchConfig

SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench"
RECORDING_MODULES = (
    "lakebench.metrics.collector",
    "lakebench.metrics.experiment",
    "lakebench.metrics.fingerprint_inputs",
)
# Readers that only show or record a value: displaying a setting is not
# honouring it (a status line once reported a Prometheus switch nothing
# read).
DISPLAY_PREFIXES = ("lakebench.metrics.", "lakebench.reports.", "lakebench.journal")
DISPLAY_FUNCTIONS = ("lakebench.cli.__init__:info",)
# Functions in config/schema.py that honour a field: env builders the Spark
# jobs receive, accessors the deployers call, and validators that transform
# the value. A schema.py function is a reader only when listed here.
SCHEMA_READERS = frozenset(
    {
        "TmOperationsConfig.env",
        "TableNamesConfig.financial_env",
        "SustainedConfig.effective_silver_bronze_wait_seconds",
        "DatagenConfig.resolve_scale_from_target_size",
        "LakebenchConfig.get_namespace",
        # operator.install: true is refused (deploy never installs operators).
        "SparkOperatorConfig._refuse_install",
        "StackableOperatorConfig._refuse_install",
        "LakebenchConfig.get_scale_dimensions",
        "LakebenchConfig.apply_recipe_defaults",
    }
)
# Functions that turn the config into the template context: a field read
# there is honoured only if the context key it feeds is rendered.
CONTEXT_BUILDERS = frozenset(
    {
        "lakebench.deploy.engine:DeploymentEngine._build_context",
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context",
    }
)
TEMPLATES = SRC / "templates"


def _model_of(annotation: Any) -> type[BaseModel] | None:
    if typing.get_origin(annotation) is None:
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            return annotation
        return None
    for arg in typing.get_args(annotation):
        found = _model_of(arg)
        if found is not None:
            return found
    return None


def leaves(root: type[BaseModel] = LakebenchConfig) -> dict[str, type[BaseModel]]:
    """Every leaf field of *root* as ``{dotted path: owning model}``."""
    out: dict[str, type[BaseModel]] = {}

    def walk(model: type[BaseModel], prefix: str, seen: frozenset) -> None:
        for name, field in model.model_fields.items():
            path = f"{prefix}.{name}" if prefix else name
            sub = _model_of(field.annotation)
            if sub is not None and sub not in seen:
                walk(sub, path, seen | {sub})
            else:
                out[path] = model

    walk(root, "", frozenset({root}))
    return out


def _models(root: type[BaseModel] = LakebenchConfig) -> set[type[BaseModel]]:
    found: set[type[BaseModel]] = set()

    def walk(model: type[BaseModel]) -> None:
        if model in found:
            return
        found.add(model)
        for field in model.model_fields.values():
            sub = _model_of(field.annotation)
            if sub is not None:
                walk(sub)

    walk(root)
    return found


def tail_parent(path: str, paths: typing.Iterable[str]) -> str:
    """The shortest dotted parent that names *path*'s field uniquely."""
    parts = path.split(".")
    all_paths = list(paths)
    for k in range(2, len(parts) + 1):
        tail = ".".join(parts[-k:])
        if sum(1 for p in all_paths if p == tail or p.endswith("." + tail)) == 1:
            return ".".join(parts[-k:-1])
    return ".".join(parts[:-1])


@cache
def _function(ref: str) -> tuple[ast.AST, str, str | None]:
    """(function AST, module name, owning class name) for ``module:qualname``."""
    module_name, qualname = ref.split(":")
    obj: Any = importlib.import_module(module_name)
    owner = None
    for part in qualname.split("."):
        if inspect.isclass(obj):
            owner = obj.__name__
        obj = getattr(obj, part)
    obj = inspect.unwrap(getattr(obj, "__func__", obj))
    source = textwrap.dedent(inspect.getsource(obj))
    return ast.parse(source).body[0], module_name, owner


def reads(fn: ast.AST, owner: str | None, model: str, parent: str, leaf: str) -> bool:
    """Whether *fn* reads ``<parent>.<leaf>``."""
    aliases: dict[str, str] = {}
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            aliases[node.targets[0].id] = ast.unparse(node.value)
    for node in ast.walk(fn):
        if isinstance(node, ast.Attribute) and node.attr == leaf:
            value = ast.unparse(node.value)
            if parent and (value == parent or value.endswith("." + parent)):
                return True
            if isinstance(node.value, ast.Name):
                alias = aliases.get(node.value.id, "")
                if parent and (alias == parent or alias.endswith("." + parent)):
                    return True
                if node.value.id in ("self", "cls") and owner == model:
                    return True
            if not parent and value in ("cfg", "config", "self.config", "self.cfg"):
                return True
            if not parent and value == "self" and owner == model == "LakebenchConfig":
                return True
        # A raw-dict read in a before-validator: data.get("<leaf>") or data["<leaf>"].
        if not parent and owner == model:
            if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
                if node.slice.value == leaf:
                    return True
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "get"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == leaf
            ):
                return True
    return False


@cache
def _rendered_context_keys() -> frozenset[str]:
    """Every identifier inside a Jinja expression or statement in a template."""
    words: set[str] = set()
    for f in TEMPLATES.rglob("*.j2"):
        for block in re.findall(r"\{\{(.*?)\}\}|\{%(.*?)%\}", f.read_text(), re.S):
            for part in block:
                words.update(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", part))
    return frozenset(words)


def _context_keys_fed(fn: ast.AST, parent: str, leaf: str) -> list[str]:
    """Dict keys in *fn* whose value reads ``<parent>.<leaf>``."""
    keys = []
    for node in ast.walk(fn):
        if not isinstance(node, ast.Dict):
            continue
        for k, v in zip(node.keys, node.values, strict=True):
            if not (isinstance(k, ast.Constant) and isinstance(k.value, str)):
                continue
            for sub in ast.walk(v):
                if isinstance(sub, ast.Attribute) and sub.attr == leaf:
                    value = ast.unparse(sub.value)
                    if value == parent or value.endswith("." + parent):
                        keys.append(k.value)
    return keys


def walk_problems(
    tree: dict[str, type[BaseModel]],
    readers: dict[str, str],
    exempt: dict[str, str],
) -> list[str]:
    problems = []
    for path, model in sorted(tree.items()):
        if path in exempt:
            continue
        ref = readers.get(path)
        if ref is None:
            problems.append(f"{path}: no reader in config/_readers.py")
            continue
        try:
            fn, module, owner = _function(ref)
        except (AttributeError, ImportError, OSError, TypeError, ValueError) as e:
            problems.append(f"{path}: reader {ref} not found ({e})")
            continue
        if module in RECORDING_MODULES:
            problems.append(f"{path}: {ref} only records the value")
            continue
        if module.startswith(DISPLAY_PREFIXES) or ref.startswith(DISPLAY_FUNCTIONS):
            problems.append(f"{path}: {ref} only displays or records the value")
            continue
        if module == "lakebench.config.schema" and ref.split(":")[1] not in SCHEMA_READERS:
            problems.append(f"{path}: {ref} is not an honouring function in config/schema.py")
            continue
        parts = path.split(".")
        parent = tail_parent(path, tree)
        if not reads(fn, owner, model.__name__, parent, parts[-1]):
            problems.append(f"{path}: {ref} does not read {parent}.{parts[-1]}")
            continue
        if ref in CONTEXT_BUILDERS:
            fed = _context_keys_fed(fn, parent, parts[-1])
            unrendered = [k for k in fed if k not in _rendered_context_keys()]
            if fed and len(unrendered) == len(fed):
                problems.append(
                    f"{path}: {ref} puts it only in context keys no template renders "
                    f"({', '.join(unrendered)})"
                )
    for path in sorted(set(readers) - set(tree)):
        problems.append(f"{path}: stale READERS entry, not a config field")
    for path in sorted(set(exempt) - set(tree)):
        problems.append(f"{path}: stale WALK_EXEMPT entry")
    return problems


def test_every_config_field_has_a_reader():
    problems = walk_problems(leaves(), _readers.READERS, _readers.WALK_EXEMPT)
    assert not problems, "\n".join(problems)


def test_walk_exempt_is_empty():
    # A field nothing reads is removed or refused, not exempted. ML-6 adds the
    # one planned exception (workload.ml_loop) itself.
    assert _readers.WALK_EXEMPT == {}


def test_no_dead_field_lists_remain():
    dead = {m.__name__: m._dead_fields for m in _models() if m._dead_fields}
    assert not dead, dead


def test_schema_walk_fails_on_unread_field():
    class FixtureModel(ConfigModel):
        never_read_anywhere_xyz: int = 1

    class FixtureRoot(ConfigModel):
        child: FixtureModel = FixtureModel()

    tree = leaves(FixtureRoot)
    assert "child.never_read_anywhere_xyz" in tree
    problems = walk_problems(tree, {}, {})
    assert problems == ["child.never_read_anywhere_xyz: no reader in config/_readers.py"]
    # A reader that does not read it does not count either.
    wrong = {"child.never_read_anywhere_xyz": "lakebench.deploy.datagen:bronze_datagen_prefix"}
    assert "does not read" in walk_problems(tree, wrong, {})[0]


def test_displaying_or_validating_is_not_reading():
    tree = {"architecture.pipeline.pattern": LakebenchConfig}
    shown = {"architecture.pipeline.pattern": "lakebench.cli.__init__:info"}
    assert "only displays" in walk_problems(tree, shown, {})[0]
    # A schema.py validator that only looks at a value (warns, bounds it) is
    # not on the honouring list.
    warned = {
        "architecture.pipeline.pattern": "lakebench.config.schema:ProcessingConfig._warn_pattern"
    }
    assert "not an honouring function" in walk_problems(tree, warned, {})[0]


def test_unrendered_context_key_is_not_reading():
    # A field copied into the template context under a key no template uses
    # (how polaris.version stayed dead) does not count.
    tree = {"architecture.query_engine.duckdb.catalog_name": LakebenchConfig}
    ref = {
        "architecture.query_engine.duckdb.catalog_name": (
            "lakebench.deploy.engine:DeploymentEngine._build_context"
        )
    }
    problems = walk_problems(tree, ref, {})
    assert problems and "no template renders" in problems[0], problems


def test_recording_is_not_reading():
    tree = {"datagen_probe.file_size": LakebenchConfig}
    ref = {"datagen_probe.file_size": "lakebench.metrics.collector:build_config_snapshot"}
    assert "only records" in walk_problems(tree, ref, {})[0]


# -- the snapshot records only fields that are read ---------------------------


def _chain(node: ast.AST) -> list[str] | None:
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return list(reversed(parts))
    return None


def snapshot_paths() -> set[str]:
    """The config paths ``build_config_snapshot`` reads, from its AST.

    Chains rooted at ``cfg`` or at a local alias of a ``cfg`` chain; a
    trailing ``.value`` (enums) is dropped, ``model_dump()`` of a sub-model
    stands for all its leaves, and other method calls (computed accessors)
    and helpers handed the whole config are not field reads.
    """
    from lakebench.metrics import collector

    fn = ast.parse(textwrap.dedent(inspect.getsource(collector.build_config_snapshot))).body[0]
    aliases: dict[str, list[str]] = {}
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            c = _chain(node.value)
            if c and c[0] == "cfg":
                aliases[node.targets[0].id] = c[1:]
    tree = leaves()
    called = {id(n.func) for n in ast.walk(fn) if isinstance(n, ast.Call)}
    inner = {id(n.value) for n in ast.walk(fn) if isinstance(n, ast.Attribute)}
    found: set[str] = set()
    for node in ast.walk(fn):
        if not isinstance(node, ast.Attribute) or id(node) in inner:
            continue
        c = _chain(node)
        if not c:
            continue
        if c[0] == "cfg":
            parts = c[1:]
        elif c[0] in aliases:
            parts = aliases[c[0]] + c[1:]
        else:
            continue
        if parts and parts[-1] == "value":
            parts = parts[:-1]
        if id(node) in called:
            method = parts.pop() if parts else ""
            if method != "model_dump":
                continue
            prefix = ".".join(parts) + "."
            found.update(p for p in tree if p.startswith(prefix))
            continue
        path = ".".join(parts)
        if path in tree:
            found.add(path)
    return found


def test_snapshot_source_fields_declared():
    from lakebench.metrics.collector import SNAPSHOT_SOURCE_FIELDS

    assert set(SNAPSHOT_SOURCE_FIELDS) == snapshot_paths()


def test_snapshot_records_only_read_fields():
    from lakebench.metrics.collector import SNAPSHOT_SOURCE_FIELDS

    tree = leaves()
    missing = [p for p in SNAPSHOT_SOURCE_FIELDS if p not in tree]
    assert not missing, missing
    unread = [p for p in SNAPSHOT_SOURCE_FIELDS if p not in _readers.READERS]
    assert not unread, unread
    # Read somewhere other than where it is defined or recorded.
    inside = [
        p
        for p in SNAPSHOT_SOURCE_FIELDS
        if _readers.READERS[p].split(":")[0]
        in ("lakebench.config.schema", "lakebench.metrics.collector")
    ]
    assert not inside, inside
