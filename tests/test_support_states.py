"""Support states (DESIGN 6.5): workload x recipe x mode declarations, the
release validation record, the computed state and the docs generated from it."""

from __future__ import annotations

import ast
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from lakebench.config import support
from lakebench.config.schema import _SUPPORTED_COMBINATIONS

REPO = Path(__file__).resolve().parents[1]


def _record(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "validated_combinations.yaml"
    p.write_text(body)
    return p


TREE = "3d304d1" + "0" * 33


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
        f"    tree: {TREE}\n    runs: {runs}\n"
    )


ICEBERG_41 = {"spark": "4.1", "table_format_version": "1.11.0"}


# -- declarations -----------------------------------------------------------


def test_recipes_are_exactly_the_architecture_list():
    comps = {support.components_of(n) for n in support.recipe_names()}
    assert comps == set(_SUPPORTED_COMBINATIONS)
    assert len(support.recipe_names()) == len(_SUPPORTED_COMBINATIONS)


def test_matrix_covers_every_workload_recipe_mode_and_refuses_only_aml_on_delta():
    rows = support.support_matrix(record={})
    assert len(rows) == len(support.recipe_names()) * len(support.workloads()) * 2
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
    assert "Spark 4.1, Iceberg 1.11.0" in s["basis"]
    # Legacy spelling of the mode resolves to the same entry.
    assert support.support_state("customer360", *args, "sustained", record=rec, **ICEBERG_41)[
        "state"
    ] == (support.UNVERIFIED)
    assert support.support_state("customer360", *args, "batch", record={})["state"] == (
        support.UNVERIFIED
    )
    assert support.support_state("financial", *args, "batch", record=rec)["state"] == (
        support.UNVERIFIED
    )


@pytest.mark.parametrize(
    "body, fragment",
    [
        (_entry(workload="financial", recipe="hive-delta-spark-trino"), "is refused"),
        (_entry(recipe="default"), "not a recipe name"),
        (_entry(recipe="unity-delta-spark-thrift"), "not a recipe name"),
        (_entry(runs="[]"), "at least one run id"),
        (_entry(runs="['']"), "at least one run id"),
        (_entry(mode="sustained"), "mode must be"),
        (_entry() + _entry().replace("validated:\n", ""), "listed twice"),
        (_entry().replace(f"    tree: {TREE}\n", ""), "'tree'"),
        (_entry().replace(f"    tree: {TREE}\n", "    tree: 3d304d1\n"), "40-hex"),
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
    ],
)
def test_record_refuses_entries_that_would_stamp_supported_wrongly(tmp_path, body, fragment):
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


def test_unsupported_is_stamped_for_combinations_outside_layers_1_to_3():
    s = support.support_state("financial", "hive", "delta", "spark", "trino", "batch", record={})
    assert s["state"] == support.UNSUPPORTED and "iceberg" in s["basis"]
    s = support.support_state("customer360", "unity", "delta", "spark", "trino", "batch", record={})
    assert s["state"] == support.UNSUPPORTED


# -- refusal before a run ---------------------------------------------------


def test_run_refuses_a_mode_the_workload_does_not_declare(tmp_path, monkeypatch):
    """--continuous does not write the mode back to the config, so load
    cannot refuse it; run must."""
    from lakebench.cli import app
    from lakebench.config import schema

    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "name: s\nplatform:\n  storage:\n    s3:\n      endpoint: http://127.0.0.1:1\n"
        "      access_key: x\n      secret_key: y\n"
    )
    monkeypatch.setitem(schema.WORKLOAD_MODES, "customer360", ("batch",))
    reached = []
    monkeypatch.setattr(
        "lakebench.config.autosizer.resolve_auto_sizing",
        lambda *a, **k: reached.append(1) or [],
    )
    monkeypatch.setattr(
        "lakebench.cli._sustained._run_sustained", lambda *a, **k: reached.append(2)
    )
    res = CliRunner().invoke(app, ["run", str(cfg), "--continuous", "--skip-deploy"])
    assert res.exit_code == 2, res.output  # unsupported combination: usage
    assert "Unsupported combination, refused" in res.output
    assert not reached


# -- CLI display ------------------------------------------------------------


@pytest.fixture
def wide_consoles(monkeypatch):
    """The CLI's module-level Rich consoles read COLUMNS once, at import, and
    otherwise ask the process's terminal; an xdist worker has none, so its
    tables were cut at 80 columns. Fix the width for these display tests."""
    import sys

    from rich.console import Console

    import lakebench.cli  # noqa: F401  (the consoles exist once the CLI is imported)

    for name, mod in list(sys.modules.items()):
        if name.startswith("lakebench.cli") and mod is not None:
            for value in vars(mod).values():
                if isinstance(value, Console):
                    monkeypatch.setattr(value, "_width", 300)


def test_config_recipes_shows_states(wide_consoles):
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["config", "recipes"], env={"COLUMNS": "300"})
    assert res.exit_code == 0, res.output
    line = next(ln for ln in res.output.splitlines() if "hive-delta-spark-trino" in ln)
    assert line.count("unsupported") == 2 and line.count("unverified") == 2
    res = CliRunner().invoke(app, ["config", "recipes", "hive-delta-spark-trino"])
    assert "AML (financial) batch: unsupported" in res.output


def test_config_show_shows_the_state(tmp_path, monkeypatch, wide_consoles):
    from lakebench.cli import app

    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "name: s\nplatform:\n  storage:\n    s3:\n      endpoint: http://127.0.0.1:1\n"
        "      access_key: x\n      secret_key: y\n"
    )
    res = CliRunner().invoke(app, ["config", "show", str(cfg)], env={"COLUMNS": "300"})
    assert res.exit_code == 0, res.output
    assert "unverified (customer360 x hive-iceberg-spark-trino x batch)" in res.output


# -- docs -------------------------------------------------------------------


@pytest.mark.parametrize("rel", sorted(support.DOCS_WITH_BLOCKS))
def test_docs_tables_match_the_code(rel):
    text = (REPO / rel).read_text()
    for name in support.DOCS_WITH_BLOCKS[rel]:
        assert support.block_in(text, name) == support.expected_block(name), (
            f"{rel}: generated block {name!r} is stale; run "
            "`PYTHONPATH=src python3.11 -m lakebench.config.support .`"
        )


def test_docs_do_not_present_unity_as_working():
    text = (REPO / "docs/compatibility-matrix.md").read_text()
    unity_rows = [ln for ln in text.splitlines() if ln.startswith("| Unity")]
    assert unity_rows and all("Not supported" in ln for ln in unity_rows)


# -- local mode -------------------------------------------------------------


def test_local_mode_refuses_aml_and_continuous():
    """--local runs the Customer 360 batch job map, datagen and benchmark
    whatever the config names; anything else ran C360 under another label."""
    from lakebench.cli._local import LocalModeError, check_local_supported
    from tests.conftest import make_config

    aml = make_config(architecture={"workload": {"schema": "financial"}})
    with pytest.raises(LocalModeError, match="Customer 360 workload only"):
        check_local_supported(aml)
    cont = make_config(architecture={"pipeline": {"mode": "continuous"}})
    with pytest.raises(LocalModeError, match="batch mode only"):
        check_local_supported(cont)
    c360 = make_config()
    check_local_supported(c360)
    with pytest.raises(LocalModeError, match="batch mode only"):
        check_local_supported(c360, continuous=True)
    s = support.support_state(
        "financial", "none", "iceberg", "spark", "duckdb", "batch", system="local", record={}
    )
    assert s["state"] == support.UNSUPPORTED


def test_run_local_refuses_the_continuous_flag(tmp_path, monkeypatch):
    from lakebench.cli import app

    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "name: s\nplatform:\n  storage:\n    s3:\n      endpoint: http://127.0.0.1:1\n"
        "      access_key: x\n      secret_key: y\n"
    )
    reached = []
    monkeypatch.setattr("lakebench.cli._run._run_local_mode", lambda *a, **k: reached.append(1))
    res = CliRunner().invoke(app, ["run", str(cfg), "--local", "--continuous"])
    assert res.exit_code == 2, res.output  # unsupported combination: usage
    assert "batch mode only" in res.output and not reached


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


def test_matrix_cell_lists_its_version_pairs():
    # The loader admits one pair per cell (the matrix's); the matrix and
    # the table still list every pair a record holds.
    v41 = support.Validation(
        "customer360", "hive-iceberg-spark-trino", "batch", "4.1", "1.11.0", TREE, ("r1",)
    )
    v40 = support.Validation(
        "customer360", "hive-iceberg-spark-trino", "batch", "4.0", "1.11.0", TREE, ("r2",)
    )
    rec = {v41.key: v41, v40.key: v40}
    rows = {(r["recipe"], r["workload"], r["mode"]): r for r in support.support_matrix(record=rec)}
    cell = rows[("hive-iceberg-spark-trino", "customer360", "batch")]
    assert cell["state"] == support.SUPPORTED
    assert cell["versions"] == [("4.0", "1.11.0"), ("4.1", "1.11.0")]
    table = support.render_support_table(rec)
    assert "supported (Spark 4.0, Iceberg 1.11.0; Spark 4.1, Iceberg 1.11.0)" in table
    outside = rows[("polaris-iceberg-spark-none", "customer360", "batch")]
    assert outside["state"] == support.UNVERIFIED and outside["basis"] == support.NOT_IN_MATRIX
