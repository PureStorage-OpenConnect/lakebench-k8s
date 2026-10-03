"""Each recipe's default Spark image is the Spark minor of its release-matrix
row (SPEC section 11; owner decision 10-03): the Hive rows run Spark 4.1,
the Polaris rows and hive-delta-spark-thrift Spark 4.0."""

from __future__ import annotations

import pytest

from lakebench.config.recipes import RECIPES
from lakebench.modules.pipeline_engines.spark.job import _parse_spark_major_minor
from tests.conftest import make_config

#: SPEC section 11, recipe -> Spark minor and format version of its rows.
SPEC_ROWS = {
    "hive-iceberg-spark-trino": ((4, 1), "1.11.0"),
    "polaris-iceberg-spark-trino": ((4, 0), "1.11.0"),
    "hive-delta-spark-trino": ((4, 1), "4.1.0"),
    "hive-delta-spark-thrift": ((4, 0), "4.0.0"),
    "hive-iceberg-spark-thrift": ((4, 1), "1.11.0"),
    "polaris-iceberg-spark-thrift": ((4, 0), "1.11.0"),
    "hive-iceberg-spark-duckdb": ((4, 1), "1.11.0"),
    "polaris-iceberg-spark-duckdb": ((4, 0), "1.11.0"),
    "hive-iceberg-spark-none": ((4, 1), "1.11.0"),
}


def _format_version(cfg):
    fmt = cfg.architecture.table_format
    return fmt.delta.version if fmt.type.value == "delta" else fmt.iceberg.version


@pytest.mark.parametrize("recipe", sorted(SPEC_ROWS))
def test_recipe_default_is_its_matrix_row(recipe):
    minor, version = SPEC_ROWS[recipe]
    assert _parse_spark_major_minor(RECIPES[recipe]["images"]["spark"]) == minor
    assert _format_version(make_config(recipe=recipe)) == version


def test_the_default_recipe_and_schema_default_run_spark_41():
    assert RECIPES["default"] is RECIPES["hive-iceberg-spark-trino"]
    assert make_config().images.spark == "apache/spark:4.1.1-python3"
    assert _parse_spark_major_minor(make_config(recipe="default").images.spark) == (4, 1)


@pytest.mark.parametrize(
    ("architecture", "minor"),
    [
        ({}, (4, 1)),
        ({"catalog": {"type": "polaris"}}, (4, 0)),
        ({"table_format": {"type": "delta"}, "query_engine": {"type": "spark-thrift"}}, (4, 0)),
        ({"table_format": {"type": "delta"}}, (4, 1)),
        ({"query_engine": {"type": "duckdb"}}, (4, 1)),
    ],
)
def test_a_config_without_a_recipe_takes_its_components_recipe_image(architecture, minor):
    cfg = make_config(architecture=architecture)
    assert _parse_spark_major_minor(cfg.images.spark) == minor


def test_a_written_image_wins():
    cfg = make_config(
        architecture={"catalog": {"type": "polaris"}},
        images={"spark": "apache/spark:4.1.1-python3"},
    )
    assert cfg.images.spark == "apache/spark:4.1.1-python3"
    cfg = make_config(images={"spark": "apache/spark:4.0.2-python3"})
    assert cfg.images.spark == "apache/spark:4.0.2-python3"


def test_matches_the_release_matrix_table_when_present():
    """ER-14's RELEASE_MATRIX_VERSIONS (train 1002-ak) is the code copy of
    SPEC section 11: every row's recipe default is that row's Spark minor."""
    from lakebench.metrics import release_record

    table = getattr(release_record, "RELEASE_MATRIX_VERSIONS", None)
    if table is None:
        pytest.skip("RELEASE_MATRIX_VERSIONS arrives with ER-14")
    for (_workload, _mode, recipe), (spark, version) in table.items():
        major, minor = _parse_spark_major_minor(RECIPES[recipe]["images"]["spark"])
        assert f"{major}.{minor}" == spark, recipe
        assert _format_version(make_config(recipe=recipe)) == version, recipe


@pytest.mark.parametrize("recipe", [None, "hive-delta-spark-trino"])
def test_a_written_delta_400_keeps_spark_40_so_old_configs_still_load(tmp_path, recipe):
    """A v1.6 config that writes delta.version 4.0.0 and no image ran Spark
    4.0.2; it still does, under every purpose, so it can be torn down."""
    import yaml

    from lakebench.config._load_context import LoadPurpose
    from lakebench.config.loader import load_config

    raw = {
        "name": "old-delta",
        "platform": {
            "storage": {
                "s3": {"endpoint": "http://10.0.1.50:80", "access_key": "a", "secret_key": "b"}
            }
        },
        "architecture": {"table_format": {"type": "delta", "delta": {"version": "4.0.0"}}},
    }
    if recipe:
        raw["recipe"] = recipe
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(raw))
    for purpose in (LoadPurpose.TEARDOWN, LoadPurpose.READ, LoadPurpose.RUN):
        cfg = load_config(path, purpose=purpose, print_notes=False)
        assert cfg.images.spark == "apache/spark:4.0.2-python3", purpose
        assert cfg.architecture.table_format.delta.version == "4.0.0"


def test_a_recipe_image_is_not_user_set():
    from lakebench.config.recipes import user_set

    assert not user_set(make_config(recipe="hive-iceberg-spark-trino"), "images.spark")
    assert not user_set(make_config(recipe="polaris-iceberg-spark-trino"), "images.spark")
    assert user_set(make_config(images={"spark": "apache/spark:4.0.2-python3"}), "images.spark")


def test_a_local_run_records_the_local_image():
    from lakebench.metrics.experiment import experiment_inputs
    from lakebench.modules.pipeline_engines.spark.local_job import DEFAULT_SPARK_IMAGE

    arch = experiment_inputs(make_config(), run_mode="batch", system="local")["architecture"]
    assert arch["pipeline_engine"]["image"] == DEFAULT_SPARK_IMAGE
    arch = experiment_inputs(make_config(), run_mode="batch")["architecture"]
    assert arch["pipeline_engine"]["image"] == "apache/spark:4.1.1-python3"


@pytest.mark.parametrize(
    ("architecture", "minor"),
    [
        ({}, (4, 1)),
        ({"catalog": {"type": "polaris"}}, (4, 0)),
        ({"table_format": {"type": "delta"}, "query_engine": {"type": "spark-thrift"}}, (4, 0)),
    ],
)
def test_recipe_default_resolves_like_no_recipe(architecture, minor):
    cfg = make_config(recipe="default", architecture=architecture)
    assert _parse_spark_major_minor(cfg.images.spark) == minor
