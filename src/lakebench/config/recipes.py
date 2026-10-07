"""Recipe definitions for Lakebench architecture presets.

Each recipe encodes four architecture axes: catalog, table format, pipeline
engine, and query engine.  Everything else (file format, Spark version,
resource sizing) is a YAML override.

Naming convention: ``<catalog>-<format>-<engine>-<query_engine>``

One alias exists: ``default`` = ``hive-iceberg-spark-trino``.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Recipe defaults
# ---------------------------------------------------------------------------
# Each recipe maps 1:1 to a validated entry in _SUPPORTED_COMBINATIONS.
# User-specified values always take precedence over recipe defaults.

RECIPES: dict[str, dict[str, Any]] = {
    "hive-iceberg-spark-trino": {
        "images": {"spark": "apache/spark:4.1.1-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "trino"},
        },
    },
    "hive-iceberg-spark-thrift": {
        "images": {"spark": "apache/spark:4.1.1-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "hive-iceberg-spark-none": {
        "images": {"spark": "apache/spark:4.1.1-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "none"},
        },
    },
    "polaris-iceberg-spark-trino": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "trino"},
        },
    },
    "polaris-iceberg-spark-thrift": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "polaris-iceberg-spark-none": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "none"},
        },
    },
    "hive-iceberg-spark-duckdb": {
        "images": {"spark": "apache/spark:4.1.1-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "duckdb"},
        },
    },
    "polaris-iceberg-spark-duckdb": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "duckdb"},
        },
    },
    # -- Hive + Delta Lake (v1.2) --
    # delta.version omitted so DeltaConfig.version="auto" resolves per Spark image.
    "hive-delta-spark-trino": {
        "images": {"spark": "apache/spark:4.1.1-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta"},
            "query_engine": {"type": "trino"},
        },
    },
    "hive-delta-spark-thrift": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta"},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "hive-delta-spark-none": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta"},
            "query_engine": {"type": "none"},
        },
    },
    # Unity + Delta excluded from v1.2. See schema.py for rationale.
}

# Alias
RECIPES["default"] = RECIPES["hive-iceberg-spark-trino"]

# Human-readable descriptions for CLI interactive flow
RECIPE_DESCRIPTIONS: dict[str, str] = {
    "default": "Hive + Iceberg + Spark + Trino (recommended)",
    "hive-iceberg-spark-thrift": "Hive + Iceberg + Spark + Spark Thrift",
    "hive-iceberg-spark-duckdb": "Hive + Iceberg + Spark + DuckDB",
    "hive-iceberg-spark-none": "Hive + Iceberg + Spark, no query engine",
    "polaris-iceberg-spark-trino": "Polaris + Iceberg + Spark + Trino",
    "polaris-iceberg-spark-thrift": "Polaris + Iceberg + Spark + Spark Thrift",
    "polaris-iceberg-spark-duckdb": "Polaris + Iceberg + Spark + DuckDB",
    "polaris-iceberg-spark-none": "Polaris + Iceberg + Spark, no query engine",
    "hive-delta-spark-trino": "Hive + Delta + Spark + Trino",
    "hive-delta-spark-thrift": "Hive + Delta + Spark + Spark Thrift",
    "hive-delta-spark-none": "Hive + Delta + Spark, no query engine",
}


@dataclass(frozen=True)
class RecipeNote:
    """What choosing a recipe costs and what it cannot do.

    A recipe name says what the components are; it says nothing about the
    trade-off. Someone comparing architectures needs both, and these are the
    facts that otherwise only surface after a failed run. Notes are shown to
    users, so each caveat explains itself and cites nothing internal.
    """

    when: str
    caveats: tuple[str, ...] = ()
    runs_locally: bool = False


# Keep an entry per recipe. A recipe with no note is a recipe whose trade-offs
# nobody has written down, which is what this table exists to prevent.
RECIPE_NOTES: dict[str, RecipeNote] = {
    "hive-iceberg-spark-trino": RecipeNote(
        when="The baseline. Start here unless you have a reason not to.",
        caveats=("Deploys PostgreSQL, Hive Metastore, and Trino: the heaviest footprint.",),
    ),
    "hive-iceberg-spark-thrift": RecipeNote(
        when="Query through Spark itself rather than a separate engine.",
        caveats=(
            "Spark Thrift shares the Spark runtime, so query and pipeline resources compete.",
        ),
    ),
    "hive-iceberg-spark-duckdb": RecipeNote(
        when="Single-node querying with the smallest possible query tier.",
        caveats=(
            "DuckDB reads Iceberg but cannot run maintenance: no "
            "expire_snapshots or remove_orphan_files, so continuous runs "
            "accumulate metadata.",
            "Needs HOME=/tmp on OpenShift; the non-root UID cannot write ~/.local.",
        ),
        runs_locally=True,
    ),
    "hive-iceberg-spark-none": RecipeNote(
        when="Measure the pipeline alone, with no query benchmark.",
        caveats=("No query engine, so QpH is not produced and --skip-benchmark is implied.",),
    ),
    "polaris-iceberg-spark-trino": RecipeNote(
        when="A REST catalog instead of Thrift, closer to a managed lakehouse.",
        caveats=(
            "The Polaris bootstrap job adds roughly 300s to deploy, against about 150s for Hive.",
            "Requires Polaris 1.3.0 or later: 1.1 and 1.2 ignore the setting "
            "that skips credential subscoping and fail on object stores "
            "without STS.",
            "Trino must request oauth2.scope=PRINCIPAL_ROLE:ALL from Polaris; "
            "the property exists only in Trino 454 and later.",
        ),
    ),
    "polaris-iceberg-spark-thrift": RecipeNote(
        when="REST catalog with Spark-native querying.",
        caveats=("The Polaris bootstrap job adds roughly 300s to deploy.",),
    ),
    "polaris-iceberg-spark-duckdb": RecipeNote(
        when="REST catalog with the lightest query tier.",
        caveats=(
            "The Polaris bootstrap job adds roughly 300s to deploy.",
            "DuckDB cannot run Iceberg maintenance.",
        ),
    ),
    "polaris-iceberg-spark-none": RecipeNote(
        when="Exercise the REST catalog path without a query engine.",
        caveats=("The Polaris bootstrap job adds roughly 300s to deploy.",),
    ),
    "hive-delta-spark-trino": RecipeNote(
        when="Compare Delta against Iceberg on the same pipeline.",
        caveats=(
            "Pre-benchmark OPTIMIZE is skipped: it rewrites the whole table in "
            "one pass and exhausts Trino worker memory.",
            "Delta version must match the Spark minor -- 4.0 for Spark 4.0, "
            "4.1 for Spark 4.1. Leave version at auto.",
        ),
    ),
    "hive-delta-spark-thrift": RecipeNote(
        when="Delta queried through Spark rather than Trino.",
        caveats=(
            "delta-spark's metadata-only MIN/MAX rewrite is disabled "
            "(optimizeMetadataQuery.enabled=false) to avoid its ClassCastException "
            "on date partition columns, so Q2 and Q6 scan instead.",
            "Pre-benchmark OPTIMIZE is skipped: it rewrites the whole table in "
            "one pass and exhausts Spark Thrift memory.",
        ),
    ),
    "hive-delta-spark-none": RecipeNote(
        when="Measure the Delta pipeline with no query engine.",
        caveats=("No query engine, so QpH is not produced.",),
    ),
}


def get_recipe_note(name: str) -> RecipeNote | None:
    """Return the trade-off note for a recipe, resolving the 'default' alias."""
    if name == "default":
        name = "hive-iceberg-spark-trino"
    return RECIPE_NOTES.get(name)


def local_recipes() -> tuple[str, ...]:
    """Recipes that run under ``--local``.

    Local mode is Iceberg-only (DuckDB cannot read Delta on non-AWS S3) and
    uses a hadoop catalog, so nothing that needs a catalog service qualifies.
    """
    return tuple(sorted(n for n, note in RECIPE_NOTES.items() if note.runs_locally))


#: The component keys a recipe owns. A config that names a recipe may leave
#: them out, or write the value the recipe sets; any other value is refused
#: at load. Images and engine resources stay overridable.
RECIPE_OWNED_KEYS: tuple[str, ...] = (
    "architecture.catalog.type",
    "architecture.table_format.type",
    "architecture.pipeline_engine",
    "architecture.query_engine.type",
)


def recipe_components(name: str) -> dict[str, str]:
    """The value of each of ``RECIPE_OWNED_KEYS`` that recipe *name* sets.

    The pipeline engine is not written in the recipe dicts; it is the third
    slot of the name (``spark`` for every recipe today).
    """
    arch = RECIPES[name]["architecture"]
    slots = name.split("-")
    return {
        "architecture.catalog.type": str(arch["catalog"]["type"]),
        "architecture.table_format.type": str(arch["table_format"]["type"]),
        "architecture.pipeline_engine": str(
            arch.get("pipeline_engine") or (slots[2] if len(slots) == 4 else "spark")
        ),
        "architecture.query_engine.type": str(arch["query_engine"]["type"]),
    }


_MISSING = object()


def _raw_value(data: Any, dotted: str) -> Any:
    """The value at *dotted* in a raw config dict, or ``_MISSING``.

    Programmatic callers can pass a sub-model instead of a dict, so an
    attribute is read as well as a key.
    """
    node = data
    for part in dotted.split("."):
        if isinstance(node, Mapping):
            if part not in node:
                return _MISSING
            node = node[part]
        elif hasattr(node, part):
            node = getattr(node, part)
        else:
            return _MISSING
    return node


def recipe_conflicts(data: Mapping[str, Any], recipe: str) -> list[str]:
    """One message per recipe-owned key that *data* sets to another value.

    ``default`` is the deprecated alias, under which written components keep
    resolving the config as before, so it has no conflicts.
    """
    if recipe == "default" or recipe not in RECIPES:
        return []
    problems = []
    for dotted, want in recipe_components(recipe).items():
        got = _raw_value(data, dotted)
        if got is _MISSING or got is None:
            continue
        got_text = str(getattr(got, "value", got))
        if got_text != want:
            problems.append(
                f"{dotted} is '{got_text}' but recipe '{recipe}' sets '{want}'; delete one of them"
            )
    return problems


def written_recipe(data: Mapping[str, Any], recipe: str) -> str | None:
    """The recipe the components resolve to when *data*'s written ones win
    over *recipe*'s (v1.6 precedence), or None when no recipe has them."""
    from lakebench.config.support import recipe_for

    merged = recipe_components(recipe)
    for dotted in merged:
        got = _raw_value(data, dotted)
        if got is not _MISSING and got is not None:
            merged[dotted] = str(getattr(got, "value", got))
    return recipe_for(*(merged[k] for k in RECIPE_OWNED_KEYS))


def _leaf_paths(value: Any, prefix: tuple[str, ...]) -> list[str]:
    if isinstance(value, dict) and value:
        out: list[str] = []
        for key, sub in value.items():
            out.extend(_leaf_paths(sub, (*prefix, str(key))))
        return out
    return [".".join(prefix)]


def _deep_setdefault(
    target: dict,
    defaults: dict,
    injected: list[str] | None = None,
    _prefix: tuple[str, ...] = (),
) -> None:
    """Recursively merge *defaults* into *target* without overwriting existing keys.

    Only dict values are merged recursively; scalar and list values in *target*
    are never replaced. Inserted values are copies, so a later change to the
    config dict never reaches ``RECIPES``. When *injected* is given, the
    dotted path of every leaf inserted is appended to it.
    """
    for key, default_value in defaults.items():
        if key not in target:
            target[key] = copy.deepcopy(default_value)
            if injected is not None:
                injected.extend(_leaf_paths(default_value, (*_prefix, str(key))))
        elif isinstance(target[key], dict) and isinstance(default_value, dict):
            _deep_setdefault(target[key], default_value, injected, (*_prefix, str(key)))


def user_set(cfg: Any, path: str) -> bool:
    """Whether the user wrote the leaf field *path* in the config.

    *path* is a dotted model path (``images.spark``,
    ``architecture.catalog.type``); a leading ``workload`` means
    ``architecture.workload``, where the model stores it. A field the
    recipe filled in looks set to Pydantic (``_deep_setdefault`` writes it
    into the dict before validation), so the paths the recipe injected
    (``LakebenchConfig._recipe_injected``) are subtracted. A config rebuilt
    from ``model_dump`` sets every field, so this only answers for a config
    validated from what the user wrote.
    """
    from pydantic import BaseModel

    parts = path.split(".")
    if parts[0] == "workload":
        parts = ["architecture", *parts]
    node = cfg
    for i, part in enumerate(parts):
        if isinstance(node, BaseModel):
            fields = type(node).model_fields
            if part not in fields:
                # A config spelling (``workload.schema``) names the field by
                # its alias; the model records the field name.
                part = next((n for n, f in fields.items() if f.alias == part), part)
                parts[i] = part
            if part not in node.model_fields_set:
                return False
            node = getattr(node, part)
        elif isinstance(node, Mapping):
            if part not in node:
                return False
            node = node[part]
        else:
            return False
    return ".".join(parts) not in (getattr(cfg, "_recipe_injected", None) or ())
