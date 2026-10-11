"""Support states (DESIGN 6.5): workload x recipe x mode declarations, the
release validation record, the computed state and the docs generated from it."""

from __future__ import annotations

import ast
from pathlib import Path
from unittest import mock

import pytest

from lakebench.config import support

REPO = Path(__file__).resolve().parents[1]


def _record(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "validated_combinations.yaml"
    p.write_text(body)
    return p


def _entry(
    workload="customer360",
    recipe="hive-iceberg-spark-trino",
    mode="batch",
    runs="[run-1]",
    spark='"4.1"',
    version="1.11.0",
):
    return (
        f"validated:\n  - workload: {workload}\n    recipe: {recipe}\n    mode: {mode}\n"
        f"    spark: {spark}\n    table_format_version: {version}\n"
        f"    runs: {runs}\n"
    )


ICEBERG_41 = {"spark": "4.1", "table_format_version": "1.11.0"}


# -- declarations -----------------------------------------------------------


def test_matrix_covers_every_workload_recipe_mode_and_refuses_only_aml_on_delta():
    rows = support.support_matrix(record={})
    for r in rows:
        fmt = support.components_of(r["recipe"])[1]
        refused = r["workload"] == "financial" and fmt == "delta"
        assert (r["state"] == support.UNSUPPORTED) is refused, r
        if not refused:
            assert r["state"] == support.UNVERIFIED, r


def test_aml_continuous_rule_note_matches_the_gold_refresh_script():
    src = (REPO / "src/lakebench/spark/scripts/gold_refresh_financial.py").read_text()
    consts = {
        t.id: ast.literal_eval(node.value)
        for node in ast.parse(src).body
        if isinstance(node, ast.Assign)
        for t in node.targets
        if isinstance(t, ast.Name) and t.id in ("CONTINUOUS_RULES", "CONTINUOUS_SKIPPED_RULES")
    }
    assert set(consts["CONTINUOUS_RULES"]) == set(support.AML_CONTINUOUS_RULES)
    assert set(consts["CONTINUOUS_SKIPPED_RULES"]) == set(support.AML_CONTINUOUS_SKIPPED_RULES)


# -- the validation record --------------------------------------------------


def test_shipped_record_loads():
    # A malformed record would leave every run unverified with a basis nobody
    # reads; CI catches it here instead.
    support.load_validation_record()


def test_supported_only_when_listed_with_runs(tmp_path):
    rec = support.load_validation_record(_record(tmp_path, _entry()))
    args = ("hive", "iceberg", "spark", "trino")
    s = support.support_state("customer360", *args, "batch", record=rec, **ICEBERG_41)
    assert s["state"] == support.SUPPORTED and s["validation_runs"] == ["run-1"]


@pytest.mark.parametrize(
    "workload, mode, listed",
    [
        ("customer360", "sustained", True),
        ("customer360", "batch", False),
        ("financial", "batch", True),
    ],
)
def test_not_supported_unless_the_exact_entry_is_listed(tmp_path, workload, mode, listed):
    rec = support.load_validation_record(_record(tmp_path, _entry())) if listed else {}
    s = support.support_state(
        workload, "hive", "iceberg", "spark", "trino", mode, record=rec, **ICEBERG_41
    )
    assert s["state"] == support.UNVERIFIED


def test_record_refuses_entries_that_would_stamp_supported_wrongly(tmp_path):
    for body, fragment in [
        (_entry(workload="financial", recipe="hive-delta-spark-trino"), "is refused"),
        (_entry(recipe="default"), "not a recipe name"),
        (_entry(recipe="unity-delta-spark-thrift"), "not a recipe name"),
        (_entry(runs="[]"), "at least one run id"),
        (_entry(runs="['']"), "at least one run id"),
        (_entry(mode="sustained"), "mode must be"),
        (_entry() + _entry().replace("validated:\n", ""), "listed twice"),
        (_entry().replace("    runs:", "    tree: 3d304d1\n    runs:"), "unknown keys"),
        (_entry().replace('    spark: "4.1"\n', ""), "rows need spark and table_format_version"),
        (_entry().replace("    table_format_version: 1.11.0\n", ""), "since 1.7"),
        (_entry(spark="4.1.1"), "Spark minor such as"),
        (_entry(spark='"4.0"', version="1.9.1"), "not compatible with Spark 4.0"),
        (_entry(recipe="hive-delta-spark-trino", spark='"3.5"', version="4.0.0"), "Delta"),
        (_entry(spark='"4.0"'), "release matrix runs customer360 hive-iceberg-spark-trino"),
        (_entry(spark='"3.5"'), "release matrix runs"),
        (_entry(recipe="polaris-iceberg-spark-none"), "not a release-matrix row"),
        (_entry(mode="continuous", recipe="hive-iceberg-spark-duckdb"), "not a release-matrix"),
        (_entry() + "extra: 1\n", "only top-level key"),
        (_entry().replace("runs:", "run_ids:"), "unknown keys"),
    ]:
        with pytest.raises(support.ValidationRecordError, match=fragment):
            support.load_validation_record(_record(tmp_path, body))


def test_unreadable_record_never_promotes(tmp_path):
    bad = _record(tmp_path, _entry(workload="financial", recipe="hive-delta-spark-trino"))
    with mock.patch.object(support, "VALIDATION_RECORD", bad):
        s = support.support_state("customer360", "hive", "iceberg", "spark", "trino", "batch")
    assert s["state"] == support.UNVERIFIED and "unreadable" in s["basis"]


def test_local_runs_are_never_supported(tmp_path):
    rec = support.load_validation_record(_record(tmp_path, _entry()))
    s = support.support_state(
        "customer360",
        "hive",
        "iceberg",
        "spark",
        "trino",
        "batch",
        system="local",
        record=rec,
        **ICEBERG_41,
    )
    assert s["state"] == support.UNVERIFIED and "local" in s["basis"]


@pytest.mark.parametrize(
    "args, system",
    [
        (("financial", "hive", "delta", "spark", "trino", "batch"), None),
        (("customer360", "unity", "delta", "spark", "trino", "batch"), None),
        (("financial", "none", "iceberg", "spark", "duckdb", "batch"), "local"),
    ],
)
def test_unsupported_is_stamped_for_combinations_outside_layers_1_to_3(args, system):
    kw = {"system": system} if system else {}
    s = support.support_state(*args, record={}, **kw)
    assert s["state"] == support.UNSUPPORTED


# -- local mode -------------------------------------------------------------


def test_local_mode_refuses_aml_and_continuous():
    """--local runs the Customer 360 batch job map, datagen and benchmark
    whatever the config names; anything else ran C360 under another label."""
    from lakebench.cli._local import LocalModeError, check_local_supported
    from tests.conftest import make_config

    aml = make_config(architecture={"workload": {"schema": "financial"}})
    with pytest.raises(LocalModeError):
        check_local_supported(aml)
    cont = make_config(architecture={"pipeline": {"mode": "continuous"}})
    with pytest.raises(LocalModeError):
        check_local_supported(cont)
    c360 = make_config()
    check_local_supported(c360)
    with pytest.raises(LocalModeError):
        check_local_supported(c360, continuous=True)


def test_matrix_degrades_when_the_record_is_malformed(tmp_path):
    bad = _record(tmp_path, "validated: {not: a list}\n")
    with mock.patch.object(support, "VALIDATION_RECORD", bad):
        rows = support.support_matrix()
    assert rows and all(r["state"] != support.SUPPORTED for r in rows)
    assert any("unreadable" in r["basis"] for r in rows)


# -- the key carries the component versions (K5) ----------------------------


def test_support_key_versions(tmp_path):
    # A row for Spark 4.1 does not make a Spark 4.0 config supported, nor a
    # run whose versions are unknown.
    rec = support.load_validation_record(_record(tmp_path, _entry()))
    args = ("customer360", "hive", "iceberg", "spark", "trino", "batch")
    on_40 = support.support_state(*args, record=rec, spark="4.0", table_format_version="1.11.0")
    assert on_40["state"] == support.UNVERIFIED
    assert "validated on Spark 4.1, Iceberg 1.11.0 only" in on_40["basis"]
    assert "Spark 4.0, Iceberg 1.11.0" in on_40["basis"]
    other_format = support.support_state(
        *args, record=rec, spark="4.1", table_format_version="1.10.1"
    )
    assert other_format["state"] == support.UNVERIFIED
    unknown = support.support_state(*args, record=rec)
    assert unknown["state"] == support.UNVERIFIED and "not known" in unknown["basis"]
    assert support.support_state(*args, record=rec, **ICEBERG_41)["state"] == support.SUPPORTED


def _cfg(spark_image: str, recipe: str = "hive-iceberg-spark-trino", **arch):
    from tests.conftest import make_config

    extra = {"architecture": arch} if arch else {}
    return make_config(recipe=recipe, images={"spark": spark_image}, **extra)


@pytest.mark.parametrize(
    "image, recipe, arch, want",
    [
        ("apache/spark:4.1.1-python3", "hive-iceberg-spark-trino", {}, ("4.1", "1.11.0")),
        ("apache/spark:4.0.2-python3", "polaris-iceberg-spark-trino", {}, ("4.0", "1.11.0")),
        ("apache/spark:4.0.2-python3", "hive-delta-spark-trino", {}, ("4.0", "4.0.0")),
        ("apache/spark:4.1.1-python3", "hive-delta-spark-trino", {}, ("4.1", "4.1.0")),
        (
            "apache/spark:4.0.2-python3",
            "hive-iceberg-spark-trino",
            {"table_format": {"iceberg": {"version": "1.10.1"}}},
            ("4.0", "1.10.1"),
        ),
    ],
)
def test_config_and_record_name_the_same_versions(image, recipe, arch, want):
    # The run's stamp is computed from the config at run start and the
    # support record is generated from the record: both must read one pair.
    from lakebench.metrics.experiment import experiment_inputs

    cfg = _cfg(image, recipe, **arch)
    assert support.config_versions(cfg) == want
    inputs = experiment_inputs(cfg)
    record = {"experiment": {**inputs, "schema": "exp2", "mode": "batch"}}
    assert support.record_versions(record) == want


def test_spark_minor_parses_like_the_job_builder():
    assert support.spark_minor("apache/spark:4.1.1-python3") == "4.1"
    assert support.spark_minor("apache/spark:4.0.2-java17-python3") == "4.0"
    assert support.spark_minor("registry.example:5000/apache/spark:4.0.2") == "4.0"
    assert support.spark_minor("apache/spark:4.1.1-python3@sha256:" + "a" * 64) is None
    # Not the validated build: another repository, a custom tag, no tag.
    assert support.spark_minor("registry.example/spark:4.0.2") is None
    assert support.spark_minor("myreg/forked-spark:4.1.1-python3") is None
    assert support.spark_minor("apache/spark:4.1.1-python3-patched") is None
    assert support.spark_minor("apache/spark@sha256:" + "a" * 64) is None
    assert support.spark_minor(None) is None


def test_a_custom_spark_image_is_never_supported(tmp_path):
    rec = support.load_validation_record(_record(tmp_path, _entry()))
    cfg = _cfg("myreg/forked-spark:4.1.1-python3")
    assert support.config_versions(cfg)[0] is None
    s = support.support_state(
        "customer360",
        "hive",
        "iceberg",
        "spark",
        "trino",
        "batch",
        record=rec,
        spark=support.config_versions(cfg)[0],
        table_format_version="1.11.0",
    )
    assert s["state"] == support.UNVERIFIED and "not known" in s["basis"]
