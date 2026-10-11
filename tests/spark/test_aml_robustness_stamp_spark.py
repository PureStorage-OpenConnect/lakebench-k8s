"""Executed: aml_features.manifest_stamp_groups reads the generator's
robustness stamp from a MAP column (lane T2 review). Skipped without pyspark."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pyspark")
ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.usefixtures("load_script")


def test_stamp_groups_from_a_map_column(spark_session):
    spark = spark_session
    import aml_features as af
    from pyspark.sql.types import MapType, StringType, StructField, StructType

    from lakebench.config import datagen_seed as ds

    stamp = {
        ds.MANIFEST_STAMP_KEY: "true",
        **dict.fromkeys(ds.MANIFEST_MULTIPLIER_KEYS.values(), "1.2"),
    }
    schema = StructType(
        [
            StructField("typology_id", StringType()),
            StructField("injection_parameters", MapType(StringType(), StringType())),
        ]
    )
    plain = spark.createDataFrame(
        [(f"t_{i}", {"rows_per_instance": "3"}) for i in range(4)], schema
    )
    stamped = spark.createDataFrame(
        [(f"t_{i}", {"rows_per_instance": "3", **stamp}) for i in range(4)], schema
    )
    s0 = ds.summarise_stamp(af.manifest_stamp_groups(plain, ds.MANIFEST_KEYS))
    s1 = ds.summarise_stamp(af.manifest_stamp_groups(stamped, ds.MANIFEST_KEYS))
    assert (s0["n_instances"], s0["n_stamped"]) == (4, 0)
    assert (s1["n_instances"], s1["n_stamped"]) == (4, 4)
    assert s1["multipliers"] == dict.fromkeys(ds.MANIFEST_MULTIPLIER_KEYS.values(), ["1.2"])
    corpora = {"robustness_perturbation": dict.fromkeys(ds.MANIFEST_MULTIPLIER_KEYS, 1.2)}
    assert ds.perturbation_stamp_error(corpora, "robustness", s1) is None
    unstamped = ds.perturbation_stamp_error(corpora, "robustness", s0)
    assert unstamped is not None
    assert "carries no robustness stamp" in unstamped
    # A manifest without the column reads as unstamped.
    s2 = ds.summarise_stamp(
        af.manifest_stamp_groups(plain.drop("injection_parameters"), ds.MANIFEST_KEYS)
    )
    assert (s2["n_instances"], s2["n_stamped"]) == (4, 0)
