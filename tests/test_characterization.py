"""Characterization tests for v1.2 -> v1.3 module refactor.

These tests capture the current behavior of recipe expansion, config
resolution, and supported combinations. They detect accidental behavior
changes during the module extraction (Phases 1-5).

Do NOT update golden values unless the behavior change is intentional.
"""

from __future__ import annotations

import pytest

from lakebench.config import LakebenchConfig
from lakebench.config.recipes import RECIPES
from lakebench.config.schema import _SUPPORTED_COMBINATIONS

# ---- helpers ---------------------------------------------------------------


def _make_config(recipe: str | None = None, **overrides) -> LakebenchConfig:
    base: dict = {
        "name": "char-test",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                }
            }
        },
    }
    if recipe:
        base["recipe"] = recipe
    base.update(overrides)
    return LakebenchConfig(**base)


_NAMED_RECIPES = sorted(k for k in RECIPES if k != "default")


# ===========================================================================
# 1. Supported combinations are stable
# ===========================================================================


class TestSupportedCombinations:
    """Verify the compat matrix hasn't changed accidentally."""

    def test_combination_count(self):
        assert len(_SUPPORTED_COMBINATIONS) == 11

    def test_hive_iceberg_combinations(self):
        hive_ice = [c for c in _SUPPORTED_COMBINATIONS if c[0] == "hive" and c[1] == "iceberg"]
        assert len(hive_ice) == 4
        engines = sorted(c[3] for c in hive_ice)
        assert engines == ["duckdb", "none", "spark-thrift", "trino"]

    def test_polaris_iceberg_combinations(self):
        pol_ice = [c for c in _SUPPORTED_COMBINATIONS if c[0] == "polaris" and c[1] == "iceberg"]
        assert len(pol_ice) == 4
        engines = sorted(c[3] for c in pol_ice)
        assert engines == ["duckdb", "none", "spark-thrift", "trino"]

    def test_hive_delta_combinations(self):
        hive_delta = [c for c in _SUPPORTED_COMBINATIONS if c[0] == "hive" and c[1] == "delta"]
        assert len(hive_delta) == 3
        engines = sorted(c[3] for c in hive_delta)
        assert engines == ["none", "spark-thrift", "trino"]

    def test_no_unity_combinations(self):
        """Unity is excluded from v1.2 -- no combinations should exist."""
        unity = [c for c in _SUPPORTED_COMBINATIONS if c[0] == "unity"]
        assert unity == []

    def test_no_delta_duckdb_combination(self):
        delta_duck = [c for c in _SUPPORTED_COMBINATIONS if c[1] == "delta" and c[3] == "duckdb"]
        assert delta_duck == []

    def test_all_use_spark_pipeline(self):
        """Every combination uses spark as the pipeline engine."""
        for c in _SUPPORTED_COMBINATIONS:
            assert c[2] == "spark", f"Non-spark pipeline in {c}"


# ===========================================================================
# 2. Recipe count and names are stable
# ===========================================================================


class TestRecipeStability:
    """Verify recipe definitions haven't changed accidentally."""

    def test_named_recipe_count(self):
        assert len(_NAMED_RECIPES) == 11

    def test_default_alias(self):
        assert "default" in RECIPES
        assert RECIPES["default"] is RECIPES["hive-iceberg-spark-trino"]

    @pytest.mark.parametrize("recipe_name", _NAMED_RECIPES)
    def test_recipe_expands_to_valid_config(self, recipe_name: str):
        """Every recipe produces a valid LakebenchConfig."""
        cfg = _make_config(recipe=recipe_name)
        assert cfg.architecture is not None

    @pytest.mark.parametrize("recipe_name", _NAMED_RECIPES)
    def test_recipe_resolves_spark_image(self, recipe_name: str):
        """Every recipe has a Spark image set."""
        cfg = _make_config(recipe=recipe_name)
        assert "apache/spark:" in cfg.images.spark


# ===========================================================================
# 3. Config resolution snapshots per recipe
# ===========================================================================


class TestConfigResolution:
    """Capture resolved architecture fields for each recipe."""

    @pytest.mark.parametrize("recipe_name", _NAMED_RECIPES)
    def test_catalog_type_matches_recipe_prefix(self, recipe_name: str):
        cfg = _make_config(recipe=recipe_name)
        catalog_name = recipe_name.split("-")[0]
        assert cfg.architecture.catalog.type.value == catalog_name

    @pytest.mark.parametrize("recipe_name", _NAMED_RECIPES)
    def test_table_format_matches_recipe(self, recipe_name: str):
        cfg = _make_config(recipe=recipe_name)
        format_name = recipe_name.split("-")[1]
        assert cfg.architecture.table_format.type.value == format_name

    @pytest.mark.parametrize("recipe_name", _NAMED_RECIPES)
    def test_query_engine_matches_recipe_suffix(self, recipe_name: str):
        cfg = _make_config(recipe=recipe_name)
        engine_name = recipe_name.split("-")[3]
        # "thrift" in recipe name maps to "spark-thrift" engine type
        if engine_name == "thrift":
            engine_name = "spark-thrift"
        assert cfg.architecture.query_engine.type.value == engine_name

    def test_iceberg_version_default(self):
        cfg = _make_config(recipe="hive-iceberg-spark-trino")
        assert cfg.architecture.table_format.iceberg.version == "1.10.1"

    def test_delta_version_default(self):
        cfg = _make_config(recipe="hive-delta-spark-trino")
        assert cfg.architecture.table_format.delta.version == "4.0.0"

    def test_delta_version_auto_resolution_spark41(self):
        """Spark 4.1 with delta auto -> 4.1.0."""
        cfg = _make_config(
            recipe="hive-delta-spark-trino",
            images={"spark": "apache/spark:4.1.1-python3"},
            architecture={
                "table_format": {"type": "delta", "delta": {"version": "auto"}},
            },
        )
        assert cfg.architecture.table_format.delta.version == "4.1.0"


# ===========================================================================
# 4. Module registry (new infrastructure, verify importable)
# ===========================================================================


class TestModuleInfrastructure:
    """Verify the new module system is importable and functional."""

    def test_import_protocols(self):
        from lakebench.modules import base

        for name in (
            "CatalogModule",
            "PipelineEngineModule",
            "QueryEngineModule",
            "TableFormatModule",
        ):
            assert hasattr(base, name)

    def test_registry_empty_by_default(self):
        from lakebench.modules.registry import ModuleRegistry

        reg = ModuleRegistry()
        assert reg.catalog_names == []
        assert reg.query_engine_names == []
        assert reg.pipeline_engine_names == []
        assert reg.table_format_names == []

    def test_registry_lookup_raises_on_unknown(self):
        from lakebench.modules.registry import ModuleRegistry

        reg = ModuleRegistry()
        with pytest.raises(KeyError, match="Unknown catalog"):
            reg.get_catalog("nonexistent")

    def test_registry_validate_empty(self):
        from lakebench.modules.registry import ModuleRegistry

        reg = ModuleRegistry()
        assert not reg.validate_combination("hive", "iceberg", "spark", "trino")

    def test_registry_register_combination(self):
        from lakebench.modules.registry import ModuleRegistry

        reg = ModuleRegistry()
        reg.register_combination("hive", "iceberg", "spark", "trino")
        assert reg.validate_combination("hive", "iceberg", "spark", "trino")
        assert not reg.validate_combination("hive", "delta", "spark", "trino")
