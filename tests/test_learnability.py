"""Tests for the parts of the CLI that exist to explain rather than execute.

A refusal that does not say why sends the user to the source, and a workflow
with no on-ramp sends them to a cluster they may not have. These assert the
explanations exist and stay attached to the things they describe.
"""

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.config import load_config
from lakebench.config.recipes import (
    RECIPE_NOTES,
    RECIPES,
    get_recipe_note,
    local_recipes,
)
from lakebench.config.schema import (
    _SUPPORTED_COMBINATIONS,
    explain_combination,
    nearest_supported,
)

runner = CliRunner()


class TestCombinationReasons:
    def test_delta_duckdb_explains_the_imds_hang(self):
        """Gotcha 18. Without this the user just sees a list and retries."""
        reason = explain_combination("hive", "delta", "spark", "duckdb")
        assert "IMDS" in reason or "imds" in reason.lower()
        assert "delta-kernel-rs" in reason

    def test_polaris_delta_explains_iceberg_native(self):
        reason = explain_combination("polaris", "delta", "spark", "trino")
        assert "iceberg-native" in reason.lower() or "iceberg native" in reason.lower()

    def test_unknown_combination_returns_empty_not_a_guess(self):
        """Some combinations are untested rather than known-broken."""
        assert explain_combination("nope", "nope", "nope", "nope") == ""

    def test_every_reason_states_a_mechanism(self):
        """A reason with no mechanism is an assertion, not an explanation."""
        for combo in (
            ("hive", "delta", "spark", "duckdb"),
            ("polaris", "delta", "spark", "trino"),
            ("unity", "iceberg", "spark", "trino"),
        ):
            text = explain_combination(*combo)
            assert len(text) > 40, f"{combo} reason is too thin to be useful"


class TestNearestSupported:
    def test_suggests_a_real_combination(self):
        nearest = nearest_supported("hive", "delta", "spark", "duckdb")
        assert nearest in _SUPPORTED_COMBINATIONS

    def test_prefers_changing_the_query_engine(self):
        """Delta was the point of the request; the query engine was incidental."""
        nearest = nearest_supported("hive", "delta", "spark", "duckdb")
        assert nearest[1] == "delta", "should keep the format the user asked for"

    def test_supported_combination_suggests_itself(self):
        combo = ("hive", "iceberg", "spark", "trino")
        assert nearest_supported(*combo) == combo


class TestValidationErrorMessage:
    def _config_with(self, tmp_path, table_format, query_engine):
        cfg = yaml.safe_load(
            (
                Path(__file__).resolve().parents[1] / "examples" / "hive-iceberg-spark-duckdb.yaml"
            ).read_text()
        )
        cfg["architecture"]["table_format"] = {"type": table_format}
        cfg["architecture"]["query_engine"]["type"] = query_engine
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.safe_dump(cfg))
        return path

    def test_error_leads_with_why(self, tmp_path):
        path = self._config_with(tmp_path, "delta", "duckdb")
        with pytest.raises(Exception) as exc:
            load_config(path)
        message = str(exc.value)
        assert "Why:" in message
        assert message.index("Why:") < message.index("All supported combinations")

    def test_error_suggests_the_closest_option(self, tmp_path):
        path = self._config_with(tmp_path, "delta", "duckdb")
        with pytest.raises(Exception) as exc:
            load_config(path)
        assert "Closest supported" in str(exc.value)


class TestRecipeNotes:
    def test_every_recipe_has_a_note(self):
        """A recipe with no note is one whose trade-offs nobody wrote down."""
        for name in RECIPES:
            if name == "default":
                continue
            assert get_recipe_note(name), f"{name} has no RecipeNote"

    def test_default_alias_resolves(self):
        assert get_recipe_note("default") == get_recipe_note("hive-iceberg-spark-trino")

    def test_notes_do_not_describe_missing_recipes(self):
        for name in RECIPE_NOTES:
            assert name in RECIPES, f"note for unknown recipe {name}"

    def test_polaris_notes_the_bootstrap_cost(self):
        """The 300s bootstrap is the main surprise when switching to Polaris."""
        note = get_recipe_note("polaris-iceberg-spark-trino")
        assert any("300s" in c for c in note.caveats)

    def test_delta_thrift_notes_the_known_query_failure(self):
        """Gotcha 22: Q2 fails, and a user should know before benchmarking."""
        note = get_recipe_note("hive-delta-spark-thrift")
        assert any("Q2" in c for c in note.caveats)

    def test_duckdb_notes_it_cannot_run_maintenance(self):
        note = get_recipe_note("hive-iceberg-spark-duckdb")
        assert any("maintenance" in c.lower() for c in note.caveats)

    def test_local_recipes_are_iceberg_only(self):
        """DuckDB cannot read Delta on non-AWS S3."""
        for name in local_recipes():
            assert "delta" not in name

    def test_at_least_one_recipe_runs_locally(self):
        assert local_recipes()


class TestRecipesCommand:
    def _run(self, *args):
        from lakebench.cli import app

        return runner.invoke(app, ["config", "recipes", *args])

    def test_lists_every_recipe(self):
        result = self._run()
        assert result.exit_code == 0
        for name in RECIPES:
            if name != "default":
                assert name.split("-")[0] in result.output

    def test_local_filter_narrows_the_list(self):
        listed = self._run().output
        local = self._run("--local").output
        assert len(local) < len(listed)

    def test_detail_shows_caveats(self):
        result = self._run("polaris-iceberg-spark-trino")
        assert result.exit_code == 0
        assert "Caveats" in result.output
        assert "300s" in result.output

    def test_unknown_recipe_lists_the_valid_ones(self):
        result = self._run("not-a-recipe")
        assert result.exit_code == 1
        assert "hive-iceberg-spark-trino" in result.output


class TestInitLocal:
    def _run(self, tmp_path, *args):
        from lakebench.cli import app

        out = tmp_path / "lakebench.yaml"
        result = runner.invoke(app, ["init", "--local", "-o", str(out), *args])
        return result, out

    def test_generates_a_loadable_config(self, tmp_path):
        result, out = self._run(tmp_path)
        assert result.exit_code == 0, result.output
        cfg = load_config(out)
        assert cfg.architecture.table_format.type.value == "iceberg"

    def test_defaults_to_a_laptop_sized_scale(self, tmp_path):
        """The cluster default of 10 would ask a laptop for ~100 GB."""
        _, out = self._run(tmp_path)
        assert load_config(out).architecture.workload.datagen.scale == 0.1

    def test_explicit_scale_is_respected(self, tmp_path):
        _, out = self._run(tmp_path, "--scale", "0.5")
        assert load_config(out).architecture.workload.datagen.scale == 0.5

    def test_config_passes_local_validation(self, tmp_path):
        from lakebench.cli._local import check_local_supported

        _, out = self._run(tmp_path)
        check_local_supported(load_config(out))

    def test_buckets_are_namespaced_by_name(self, tmp_path):
        """Two local configs on one host must not share buckets."""
        _, out = self._run(tmp_path, "--name", "proj-a")
        buckets = load_config(out).platform.storage.s3.buckets
        assert buckets.bronze.startswith("proj-a")

    def test_tells_the_user_what_to_run_next(self, tmp_path):
        result, _ = self._run(tmp_path)
        assert "--local" in result.output
        assert "deploy" in result.output
