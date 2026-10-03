#!/usr/bin/env python3
"""Regenerate the field reference in docs/configuration.md from the schema.

Two blocks are written from ``LakebenchConfig.model_fields``:

- ``config-reference``: every key, grouped by section, with its type,
  default, tier and description. The description is the field's
  ``Field(description=...)`` or the string literal written right after it in
  ``config/schema.py`` (``use_attribute_docstrings``). The tier is "first
  day" for the keys ``lakebench init`` writes (``cli._init.first_day_dict``)
  and "advanced" for the rest.
- ``config-removed``: every key in a model's ``_removed_keys``, with what
  to do instead.

Each sits between ``<!-- BEGIN GENERATED: <name> -->`` and
``<!-- END GENERATED: <name> -->`` markers.

Usage:
    python3.11 scripts/gen_config_reference.py           # rewrite the blocks
    python3.11 scripts/gen_config_reference.py --check   # exit 1 on drift

``--check`` writes nothing; ``tests/test_config_reference_drift.py`` runs the
same comparison in the unit suite.
"""

from __future__ import annotations

import argparse
import enum
import json
import re
import sys
import types
import typing
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from pydantic import BaseModel  # noqa: E402
from pydantic_core import PydanticUndefined  # noqa: E402

DOC = "docs/configuration.md"
BLOCKS = ("config-reference", "config-removed")

#: Schema paths shown under the key a config writes: the workload block is
#: written at the top level, and ``pipeline.continuous`` is the canonical
#: spelling of the ``sustained`` field.
DISPLAY_RENAMES = (
    ("architecture.workload.", "workload."),
    ("architecture.pipeline.sustained.", "architecture.pipeline.continuous."),
)

#: (heading, path prefixes, intro). A key belongs to the first section with
#: a matching prefix; a key that matches none fails the generator, so a new
#: top-level block needs a section here.
SECTIONS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("Root", ("name", "recipe"), ""),
    (
        "Images",
        ("images.",),
        "Container images for every deployed component. Override for air-gapped "
        "registries or custom builds.",
    ),
    ("Platform -- Kubernetes", ("platform.kubernetes.",), ""),
    ("Platform -- S3 Storage", ("platform.storage.s3.",), ""),
    (
        "Platform -- Scratch Storage",
        ("platform.storage.scratch.",),
        "Scratch PVCs for Spark shuffle data. Only needed with Portworx or similar CSI.",
    ),
    ("Platform -- Spark Compute", ("platform.compute.spark.",), ""),
    ("Platform -- PostgreSQL", ("platform.compute.postgres.",), ""),
    (
        "Platform -- Dependency server",
        ("platform.deps.",),
        "What `lb-deps` is and when it resolves again: see "
        "[Dependency server](#dependency-server).",
    ),
    ("Architecture -- Catalog", ("architecture.catalog.",), ""),
    ("Architecture -- Table Format", ("architecture.table_format.",), ""),
    ("Architecture -- Pipeline engine", ("architecture.pipeline_engine",), ""),
    ("Architecture -- Query Engine", ("architecture.query_engine.",), ""),
    (
        "Architecture -- Pipeline",
        ("architecture.pipeline.",),
        "The legacy name `processing` is still accepted with a deprecation warning.",
    ),
    (
        "Workload & Datagen",
        ("workload.schema", "workload.datagen."),
        "",
    ),
    (
        "Workload -- Customer 360 and AML",
        ("workload.",),
        "Advanced workload overrides. Most users should leave these at defaults and "
        "control volume via `datagen.scale`. The AML TM operations layer "
        "(`workload.tm_operations.*`) is described in "
        "[aml-scoring.md](aml-scoring.md#the-transaction-monitoring-operations-layer).",
    ),
    ("Architecture -- Benchmark", ("architecture.benchmark.",), ""),
    (
        "Architecture -- Table Names",
        ("architecture.tables.",),
        "Fully-qualified table names (`namespace.table`). The catalog prefix is added at runtime.",
    ),
    ("Observability", ("observability.",), ""),
    (
        "Spark",
        ("spark.",),
        "How `spark.conf` layers over the job defaults and what it refuses: see "
        "[Spark Configuration Overrides](#spark-configuration-overrides).",
    ),
)

#: Default cells for keys whose schema default is not what a run uses: a
#: value derived from the deployment name, or one auto-sizing or the run
#: resolves. Keys must exist (tests/test_config_reference_drift.py).
DEFAULT_OVERRIDES: dict[str, str] = {
    "name": "**(required)**",
    "platform.storage.s3.endpoint": "**(required)**",
    "platform.storage.s3.buckets.bronze": "`<name>-bronze`",
    "platform.storage.s3.buckets.silver": "`<name>-silver`",
    "platform.storage.s3.buckets.gold": "`<name>-gold`",
    "architecture.pipeline.continuous.max_files_per_trigger": "auto",
    "architecture.pipeline.continuous.retention_interval": "auto",
    "workload.datagen.parallelism": "auto (by scale and cluster)",
    "workload.datagen.cpu": "auto (by scale and cluster)",
    "workload.datagen.memory": "auto (by workload, scale and cpu)",
}

_SCALARS = {str: "string", int: "integer", float: "number", bool: "boolean"}


def display(path: str) -> str:
    for old, new in DISPLAY_RENAMES:
        if path.startswith(old):
            return new + path[len(old) :]
    return path


def _models_in(annotation: Any) -> list[type[BaseModel]]:
    args = typing.get_args(annotation) or (annotation,)
    return [a for a in args if isinstance(a, type) and issubclass(a, BaseModel)]


def _is_model_block(annotation: Any) -> bool:
    return bool(_models_in(annotation)) and typing.get_origin(annotation) not in (dict, list)


def type_text(annotation: Any) -> str:
    """The YAML type of a field, for the reference table."""
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        parts = [type_text(a) for a in typing.get_args(annotation) if a is not type(None)]
        nullable = type(None) in typing.get_args(annotation)
        return " or ".join(parts) + (" or null" if nullable else "")
    if origin is typing.Literal:
        return "one of " + ", ".join(f"`{v}`" for v in typing.get_args(annotation))
    if origin is dict:
        return "mapping"
    if origin is list:
        return "list"
    if isinstance(annotation, type) and issubclass(annotation, enum.Enum):
        return "one of " + ", ".join(f"`{m.value}`" for m in annotation)
    if annotation in _SCALARS:
        return _SCALARS[annotation]
    return getattr(annotation, "__name__", str(annotation))


def default_text(field: Any) -> str:
    if field.is_required():
        return "**(required)**"
    if field.default_factory is not None:
        value = field.default_factory()
    else:
        value = field.default
    if value is PydanticUndefined:
        return "**(required)**"
    if isinstance(value, enum.Enum):
        value = value.value
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    if value is None:
        return "`null`"
    if isinstance(value, bool):
        return f"`{str(value).lower()}`"
    if isinstance(value, str):
        return f"`{value}`" if value else '`""`'
    if isinstance(value, (dict, list)):
        return f"`{json.dumps(value, sort_keys=True)}`"
    return f"`{value}`"


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def first_day_keys() -> set[str]:
    """The keys ``lakebench init`` writes."""
    from lakebench.cli._init import first_day_dict

    tree = first_day_dict(
        name="x",
        recipe="x",
        workload="x",
        scale=1,
        endpoint="x",
        credentials_env="X",
        namespace="x",
    )
    keys: set[str] = set()

    def walk(node: dict[str, Any], prefix: str) -> None:
        for k, v in node.items():
            if isinstance(v, dict):
                walk(v, f"{prefix}{k}.")
            else:
                keys.add(f"{prefix}{k}")

    walk(tree, "")
    return keys


def leaves() -> list[tuple[str, Any]]:
    """``(displayed path, FieldInfo)`` of every key, in schema order."""
    from lakebench.config.schema import LakebenchConfig

    out: list[tuple[str, Any]] = []

    def walk(model: type[BaseModel], prefix: str) -> None:
        for name, field in model.model_fields.items():
            key = f"{prefix}{field.alias or name}"
            if _is_model_block(field.annotation):
                walk(_models_in(field.annotation)[0], f"{key}.")
            else:
                out.append((display(key), field))

    walk(LakebenchConfig, "")
    return out


def removed_keys() -> list[tuple[str, str]]:
    """``(displayed path, what to do instead)`` of every removed key."""
    from lakebench.config.schema import LakebenchConfig

    out: dict[str, str] = {}

    def walk(model: type[BaseModel], prefix: str) -> None:
        for key, fix in getattr(model, "_removed_keys", {}).items():
            out.setdefault(display(f"{prefix}{key}"), fix)
        for name, field in model.model_fields.items():
            if _is_model_block(field.annotation):
                walk(_models_in(field.annotation)[0], f"{prefix}{field.alias or name}.")

    walk(LakebenchConfig, "")
    return sorted(out.items())


def _section_of(path: str) -> int:
    for i, (_, prefixes, _) in enumerate(SECTIONS):
        if any(path == p or path.startswith(p) for p in prefixes):
            return i
    raise SystemExit(f"{path}: no section in scripts/gen_config_reference.py SECTIONS")


def render_reference() -> str:
    first = first_day_keys()
    grouped: dict[int, list[str]] = {}
    for path, field in leaves():
        tier = "first day" if path in first else "advanced"
        row = (
            f"| `{path}` | {_cell(type_text(field.annotation))} | "
            f"{_cell(DEFAULT_OVERRIDES.get(path) or default_text(field))} | {tier} | "
            f"{_cell(field.description or '')} |"
        )
        grouped.setdefault(_section_of(path), []).append(row)
    parts: list[str] = []
    for i, (heading, _, intro) in enumerate(SECTIONS):
        if i not in grouped:
            continue
        parts.append(f"### {heading}\n")
        if intro:
            parts.append(f"{intro}\n")
        parts.append("| Field | Type | Default | Tier | Description |\n|---|---|---|---|---|")
        parts.append("\n".join(grouped[i]) + "\n")
    return "\n".join(parts)


def render_removed() -> str:
    rows = [f"| `{path}` | {_cell(fix)} |" for path, fix in removed_keys()]
    return "| Removed key | Instead |\n|---|---|\n" + "\n".join(rows) + "\n"


RENDERERS = {"config-reference": render_reference, "config-removed": render_removed}


def expected_block(name: str) -> str:
    return (
        f"<!-- BEGIN GENERATED: {name} (scripts/gen_config_reference.py) -->\n"
        f"{RENDERERS[name]()}"
        f"<!-- END GENERATED: {name} -->"
    )


def block_in(text: str, name: str) -> str | None:
    m = re.search(
        rf"<!-- BEGIN GENERATED: {re.escape(name)}\b.*?<!-- END GENERATED: {re.escape(name)} -->",
        text,
        re.DOTALL,
    )
    return m.group(0) if m else None


def drift(root: Path = REPO_ROOT) -> list[str]:
    text = (root / DOC).read_text()
    problems = []
    for name in BLOCKS:
        found = block_in(text, name)
        if found is None:
            problems.append(f"{DOC}: block {name!r} not found (markers missing)")
        elif found != expected_block(name):
            problems.append(f"{DOC}: block {name!r} is stale")
    return problems


def regenerate(root: Path = REPO_ROOT) -> bool:
    path = root / DOC
    text = path.read_text()
    new = text
    for name in BLOCKS:
        found = block_in(new, name)
        if found is None:
            raise SystemExit(f"{DOC}: block {name!r} not found; add its markers first")
        new = new.replace(found, expected_block(name))
    if new != text:
        path.write_text(new)
        return True
    return False


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if a block is stale")
    args = ap.parse_args(argv)
    if args.check:
        problems = drift()
        for p in problems:
            print(p, file=sys.stderr)
        if problems:
            print("Run: python3.11 scripts/gen_config_reference.py", file=sys.stderr)
            return 1
        return 0
    if regenerate():
        print(f"updated {DOC}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
