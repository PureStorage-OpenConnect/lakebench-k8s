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


def _entry(workload="customer360", recipe="hive-iceberg-spark-trino", mode="batch", runs="[run-1]"):
    return (
        f"validated:\n  - workload: {workload}\n    recipe: {recipe}\n    mode: {mode}\n"
        f"    tree: 3d304d1\n    runs: {runs}\n"
    )


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
    s = support.support_state("customer360", *args, "batch", record=rec)
    assert s["state"] == support.SUPPORTED and s["validation_runs"] == ["run-1"]
    # Legacy spelling of the mode resolves to the same entry.
    assert support.support_state("customer360", *args, "sustained", record=rec)["state"] == (
        support.UNVERIFIED
    )
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
        (_entry().replace("    tree: 3d304d1\n", ""), "'tree'"),
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
        "customer360", "hive", "iceberg", "spark", "trino", "batch", system="local", record=rec
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
    assert res.exit_code == 1, res.output
    assert "Unsupported combination, refused" in res.output
    assert not reached


# -- CLI display ------------------------------------------------------------


def test_config_recipes_shows_states():
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["config", "recipes"], env={"COLUMNS": "300"})
    assert res.exit_code == 0, res.output
    line = next(ln for ln in res.output.splitlines() if "hive-delta-spark-trino" in ln)
    assert line.count("unsupported") == 2 and line.count("unverified") == 2
    res = CliRunner().invoke(app, ["config", "recipes", "hive-delta-spark-trino"])
    assert "AML (financial) batch: unsupported" in res.output


def test_config_show_shows_the_state(tmp_path, monkeypatch):
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
    assert res.exit_code == 1, res.output
    assert "batch mode only" in res.output and not reached


def test_matrix_degrades_when_the_record_is_malformed(tmp_path):
    bad = _record(tmp_path, "validated: {not: a list}\n")
    with mock.patch.object(support, "VALIDATION_RECORD", bad):
        rows = support.support_matrix()
    assert rows and all(r["state"] != support.SUPPORTED for r in rows)
    assert any("unreadable" in r["basis"] for r in rows)
