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
