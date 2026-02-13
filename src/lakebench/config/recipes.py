"""Recipe definitions for Lakebench architecture presets.

Each recipe encodes three architecture axes: catalog, table format, and query engine.
Everything else (file format, Spark version, resource sizing) is a YAML override.

Naming convention: ``<catalog>-<format>-<engine>``

One alias exists: ``default`` = ``hive-iceberg-trino``.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Recipe defaults
# ---------------------------------------------------------------------------
# Each recipe maps 1:1 to a validated entry in _SUPPORTED_COMBINATIONS.
# User-specified values always take precedence over recipe defaults.

RECIPES: dict[str, dict[str, Any]] = {
    "hive-iceberg-trino": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "trino"},
        },
    },
    "hive-iceberg-spark": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "hive-iceberg-none": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "none"},
        },
    },
    "hive-delta-trino": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta"},
            "query_engine": {"type": "trino"},
        },
    },
    "hive-delta-none": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "delta"},
            "query_engine": {"type": "none"},
        },
    },
    "polaris-iceberg-trino": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "trino"},
        },
    },
    "polaris-iceberg-spark": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "spark-thrift"},
        },
    },
    "polaris-iceberg-none": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "none"},
        },
    },
    "hive-iceberg-duckdb": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "duckdb", "duckdb": {"cores": 2, "memory": "4g"}},
        },
    },
    "polaris-iceberg-duckdb": {
        "images": {"spark": "apache/spark:3.5.4-python3", "postgres": "postgres:17"},
        "architecture": {
            "catalog": {"type": "polaris"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.10.1"}},
            "query_engine": {"type": "duckdb", "duckdb": {"cores": 2, "memory": "4g"}},
        },
    },
}

# Alias
RECIPES["default"] = RECIPES["hive-iceberg-trino"]

# Human-readable descriptions for CLI interactive flow
RECIPE_DESCRIPTIONS: dict[str, str] = {
    "default": "Hive + Iceberg + Trino (recommended)",
    "hive-iceberg-spark": "Hive + Iceberg + Spark SQL",
    "hive-iceberg-duckdb": "Hive + Iceberg + DuckDB",
    "hive-iceberg-none": "Hive + Iceberg, no query engine",
    "hive-delta-trino": "Hive + Delta + Trino",
    "hive-delta-none": "Hive + Delta, no query engine",
    "polaris-iceberg-trino": "Polaris + Iceberg + Trino",
    "polaris-iceberg-spark": "Polaris + Iceberg + Spark SQL",
    "polaris-iceberg-duckdb": "Polaris + Iceberg + DuckDB",
    "polaris-iceberg-none": "Polaris + Iceberg, no query engine",
}


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
