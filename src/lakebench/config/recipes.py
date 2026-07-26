"""Recipe definitions for Lakebench architecture presets.

Each recipe encodes four architecture axes: catalog, table format, pipeline
engine, and query engine.  Everything else (file format, Spark version,
resource sizing) is a YAML override.

Naming convention: ``<catalog>-<format>-<engine>-<query_engine>``

One alias exists: ``default`` = ``hive-iceberg-spark-trino``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Recipe defaults
# ---------------------------------------------------------------------------
# Each recipe maps 1:1 to a validated entry in _SUPPORTED_COMBINATIONS.
# User-specified values always take precedence over recipe defaults.

RECIPES: dict[str, dict[str, Any]] = {
    "hive-iceberg-spark-trino": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "trino"},
        },
    },
    "hive-iceberg-spark-thrift": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "hive-iceberg-spark-none": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "none"},
        },
    },
    "polaris-iceberg-spark-trino": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "trino"},
        },
    },
    "polaris-iceberg-spark-thrift": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "polaris-iceberg-spark-none": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "none"},
        },
    },
    "hive-iceberg-spark-duckdb": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "duckdb", "duckdb": {"cores": 2, "memory": "4g"}},
        },
    },
    "polaris-iceberg-spark-duckdb": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "duckdb", "duckdb": {"cores": 2, "memory": "4g"}},
        },
    },
    # -- Hive + Delta Lake (v1.2) --
    "hive-delta-spark-trino": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta", "delta": {"version": "4.0.0"}},
            "query_engine": {"type": "trino"},
        },
    },
    "hive-delta-spark-thrift": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta", "delta": {"version": "4.0.0"}},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "hive-delta-spark-none": {
        "images": {"spark": "apache/spark:4.0.2-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta", "delta": {"version": "4.0.0"}},
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
    facts that otherwise only surface after a failed run. ``caveats`` entries
    cite the CLAUDE.md gotcha number where one applies.
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
            "expire_snapshots or remove_orphan_files, so sustained runs "
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
            "Bootstrap is not idempotent and adds roughly 300s to deploy, "
            "against ~150s for Hive. (gotcha 12)",
            "Requires Polaris 1.3.0+; earlier versions fail on object stores "
            "without STS. (gotcha 9)",
            "Trino needs oauth2.scope=PRINCIPAL_ROLE:ALL, which exists only in "
            "Trino 454+. (gotcha 14)",
        ),
    ),
    "polaris-iceberg-spark-thrift": RecipeNote(
        when="REST catalog with Spark-native querying.",
        caveats=("Polaris bootstrap adds roughly 300s to deploy. (gotcha 12)",),
    ),
    "polaris-iceberg-spark-duckdb": RecipeNote(
        when="REST catalog with the lightest query tier.",
        caveats=(
            "Polaris bootstrap adds roughly 300s to deploy. (gotcha 12)",
            "DuckDB cannot run Iceberg maintenance.",
        ),
    ),
    "polaris-iceberg-spark-none": RecipeNote(
        when="Exercise the REST catalog path without a query engine.",
        caveats=("Polaris bootstrap adds roughly 300s to deploy. (gotcha 12)",),
    ),
    "hive-delta-spark-trino": RecipeNote(
        when="Compare Delta against Iceberg on the same pipeline.",
        caveats=(
            "Pre-benchmark OPTIMIZE is skipped: it rewrites the whole table in "
            "one pass and exhausts Trino worker memory. (gotcha 21)",
            "Delta version must match the Spark minor -- 4.0 for Spark 4.0, "
            "4.1 for Spark 4.1. Leave version at auto. (gotcha 31)",
        ),
    ),
    "hive-delta-spark-thrift": RecipeNote(
        when="Delta queried through Spark rather than Trino.",
        caveats=(
            "Q2 of the benchmark is a known failure: delta-spark 4.0 throws "
            "ClassCastException on MIN/MAX over a date partition column. "
            "(gotcha 22)",
            "Pre-benchmark OPTIMIZE is skipped; it exhausts Thrift memory. (gotcha 21)",
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


def _deep_setdefault(target: dict, defaults: dict) -> None:
    """Recursively merge *defaults* into *target* without overwriting existing keys.

    Only dict values are merged recursively; scalar and list values in *target*
    are never replaced.
    """
    for key, default_value in defaults.items():
        if key not in target:
            target[key] = default_value
        elif isinstance(target[key], dict) and isinstance(default_value, dict):
            _deep_setdefault(target[key], default_value)
