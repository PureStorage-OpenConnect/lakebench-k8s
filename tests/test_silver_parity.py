"""scripts/release/silver_parity.py: business columns from the silver DDL,
the refusals, and the comparison against a fake query."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


SP = _load(ROOT / "scripts" / "release" / "silver_parity.py", "lb_release_silver_parity")
DDL_SYNC = _load(ROOT / "tests" / "test_silver_ddl_sync.py", "lb_test_silver_ddl_sync")


def _independent_columns(key: str) -> list[str]:
    """The table's columns as the DDL-sync test parses them."""
    from lakebench.deploy.financial_ddl import FINANCIAL_TABLE_DDLS

    return list(DDL_SYNC._parse_columns(FINANCIAL_TABLE_DDLS[key]))


def _hashed(spec) -> list[str]:
    return [c for c, _t in spec.hashed]


def test_every_ddl_column_is_hashed_merged_or_excluded():
    for spec in SP.table_specs():
        got = [*_hashed(spec), *spec.merged, *spec.excluded]
        assert sorted(got) == sorted(_independent_columns(spec.key)), spec.key


def test_only_sentinels_and_per_mode_columns_are_excluded():
    excluded = {c for s in SP.table_specs() for c in s.excluded}
    assert excluded == {"_batch_id", "_stream_id", "ingest_ts", "profile_updated_ts"}


def test_only_the_merged_profile_accumulators_get_a_tolerance():
    by_key = {s.key: s for s in SP.table_specs()}
    assert set(by_key["silver_entity_profiles"].merged) == set(SP.MERGED)
    assert "active_span_days" in _hashed(by_key["silver_entity_profiles"])
    assert all(not s.merged for k, s in by_key.items() if k != "silver_entity_profiles")
    assert "initial_risk_score" in _hashed(by_key["silver_entities"])


def test_no_column_list_is_kept_by_hand(tmp_path):
    ddl = (ROOT / "src" / "lakebench" / "deploy" / "financial_ddl.py").read_text()
    changed = tmp_path / "financial_ddl.py"
    changed.write_text(
        ddl.replace(
            "    source_message_ref      STRING,\n",
            "    source_message_ref      STRING,\n    new_business_col        STRING,\n",
            1,
        )
    )
    spec = next(s for s in SP.table_specs(changed) if s.key == "silver")
    assert "new_business_col" in _hashed(spec)


def test_an_edge_column_the_view_does_not_know_refuses(tmp_path):
    ddl = (ROOT / "src" / "lakebench" / "deploy" / "financial_ddl.py").read_text()
    changed = tmp_path / "financial_ddl.py"
    changed.write_text(ddl.replace("    txn_count ", "    weight DOUBLE,\n    txn_count ", 1))
    with pytest.raises(SP.Refused, match="edges view does not know"):
        SP.table_specs(changed)


def test_missing_ddl_constant_refuses(tmp_path):
    bad = tmp_path / "f.py"
    bad.write_text("FINANCIAL_TABLE_DDLS = {}\n")
    with pytest.raises(SP.Refused, match="no DDL constant"):
        SP.ddl_strings(bad)


def test_the_row_hash_is_one_hash_per_row_over_every_hashed_column():
    spec = next(s for s in SP.table_specs() if s.key == "silver")
    sql = SP.table_sql("lakehouse.silver.transactions", spec)
    assert sql.startswith("SELECT count(*), to_hex(checksum(xxhash64(to_utf8(concat_ws(chr(30), ")
    assert sql.count("coalesce(") == len(spec.hashed)
    assert "array_join(correspondent_chain, chr(31)" in sql
    assert "_batch_id" not in sql and "ingest_ts" not in sql


def test_structs_are_hashed_as_json():
    spec = next(s for s in SP.table_specs() if s.key == "silver_entities")
    assert "json_format(CAST(address AS JSON))" in SP.table_sql("t", spec)


def test_edges_are_compared_through_their_consumer_view():
    spec = next(s for s in SP.table_specs() if s.key == "silver_counterparty_edges")
    sql = SP.table_sql("lakehouse.silver.counterparty_edges", spec)
    assert "GROUP BY source_entity_id, target_entity_id" in sql
    assert "min(first_seen_ts) AS first_seen_ts" in sql and "sum(txn_count) AS txn_count" in sql


# -- records -------------------------------------------------------------------


def _record(mode: str, name: str, **extra: Any) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "deployment_name": name,
        "success": True,
        "experiment": {
            "workload": {"name": "financial"},
            "corpus": {"seed": 43, "scale": 1.0, "generator_image": "img@sha256:aa"},
        },
        "pipeline_benchmark": {"pipeline_mode": mode},
        "config_snapshot": {"datagen": {"parallelism": 1 if mode == "sustained" else 4}},
    }
    if mode == "sustained":
        rec["pipeline_benchmark"]["corpus_drained"] = True
        rec["continuous"] = {"drain": {"state": "drained"}, "gate_problems": []}
    for path, value in extra.items():
        node = rec
        *heads, last = path.split("__")
        for h in heads:
            node = node.setdefault(h, {})
        node[last] = value
    return rec


NAMES = ("rel17-sp-b", "rel17-sp-c")


def test_drained_matching_records_can_be_compared():
    assert (
        SP.record_problems(_record("batch", NAMES[0]), _record("sustained", NAMES[1]), NAMES) == []
    )


@pytest.mark.parametrize(
    ("cont", "why"),
    [
        (
            _record("sustained", NAMES[1], pipeline_benchmark__corpus_drained=False),
            "drained corpus",
        ),
        (_record("sustained", NAMES[1], continuous__drain__state="timed_out"), "drain state"),
        (_record("sustained", NAMES[1], continuous__gate_problems=["x"]), "gate problems"),
        (_record("sustained", NAMES[1], experiment__corpus__seed=44), "not of one corpus"),
        (
            _record("sustained", NAMES[1], experiment__corpus__generator_image="other"),
            "generator_image",
        ),
        (_record("batch", NAMES[1]), "continuous record's mode"),
        (_record("sustained", NAMES[1], config_snapshot__datagen__parallelism=2), "datagen pods"),
        (_record("sustained", NAMES[1], success=False), "did not succeed"),
        (_record("sustained", "someone-else"), "not 'rel17-sp-c'"),
    ],
)
def test_unfit_records_refuse(cont, why):
    assert any(why in p for p in SP.record_problems(_record("batch", NAMES[0]), cont, NAMES))


def test_the_same_deployment_twice_refuses():
    problems = SP.record_problems(
        _record("batch", NAMES[0]), _record("sustained", NAMES[0]), (NAMES[0], NAMES[0])
    )
    assert any("same deployment" in p for p in problems)


# -- comparison ------------------------------------------------------------------


TABLES = {k: f"lakehouse.silver.{k}" for k in (*SP.SILVER_KEYS, SP.VERSIONS_KEY)}


class FakeQuery:
    def __init__(self, differ: dict[str, Any] | None = None) -> None:
        self.differ = differ or {}

    def __call__(self, config: Path, sql: str) -> list[list[str | None]]:
        side = config.name
        if sql.startswith("SELECT count(*) FROM") and "checksum" not in sql:
            return [["1" if side == "b" else "40"]]
        table = sql.rsplit("FROM ", 1)[1].split(" GROUP BY")[0].split()[0].rstrip(")")
        if sql.startswith("SELECT entity_id,"):
            rows = [["1", "0.5", "2.0", "3.0", "4.0", None], ["2", "0.1", "1.0", "1.0", "1.0", "7"]]
            if side == "c" and "merged" in self.differ:
                rows[1][1] = str(float(rows[1][1]) + self.differ["merged"])
            if side == "c" and self.differ.get("m2_zero"):
                rows[0][5] = "0"
            return rows
        if sql.startswith("SELECT to_hex(checksum(xxhash64(to_utf8(coalesce("):
            n = sql.count("checksum(")
            cells: list[str | None] = ["h"] * n
            if side == "c" and table in self.differ.get("columns", {}):
                cells[self.differ["columns"][table]] = "x"
            return [cells]
        row: list[str | None] = ["100", "abc"]
        if side == "c" and table in self.differ.get("hash", ()):
            row[1] = "def"
        if table in self.differ.get("empty", ()):
            row = ["0", None]
        return [row]


def _run(query):
    return SP.compare(Path("b"), Path("c"), TABLES, SP.table_specs(), query)


def test_equal_tables_compare_equal_and_versions_are_reported():
    results, versions = _run(FakeQuery())
    assert all(r.equal for r in results)
    assert versions == {"lakehouse.silver.silver_batch_versions": ("1", "40")}


def test_a_different_hash_names_the_differing_column():
    t = "lakehouse.silver.silver_accounts"
    results, _ = _run(FakeQuery({"hash": [t], "columns": {t: 3}}))
    bad = [r for r in results if not r.equal]
    assert [r.table for r in bad] == [t]
    spec = next(s for s in SP.table_specs() if s.key == "silver_accounts")
    assert any(_hashed(spec)[3] in n for n in bad[0].notes)


def test_empty_tables_never_pass():
    t = "lakehouse.silver.silver_entities"
    results, _ = _run(FakeQuery({"empty": [t]}))
    assert [r.table for r in results if not r.equal] == [t]


def test_merged_values_within_tolerance_pass_and_beyond_it_fail():
    results, _ = _run(FakeQuery({"merged": 1e-12}))
    assert all(r.equal for r in results)
    results, _ = _run(FakeQuery({"merged": 1e-6}))
    assert [r.table for r in results if not r.equal] == ["lakehouse.silver.silver_entity_profiles"]


def test_a_null_m2_equals_zero_as_in_the_spark_parity_test():
    results, _ = _run(FakeQuery({"m2_zero": True}))
    assert all(r.equal for r in results)


# -- the CLI -------------------------------------------------------------------


@pytest.fixture
def configs(tmp_path):
    out = {}
    for side, name in zip(("b", "c"), NAMES, strict=True):
        d = tmp_path / side
        d.mkdir()
        cfg = d / f"{side}.yaml"
        subprocess.run(
            [
                sys.executable,
                "-m",
                "lakebench",
                "init",
                "-r",
                "hive-iceberg-spark-trino",
                "-w",
                "financial",
                "-n",
                name,
                "--endpoint",
                "http://10.0.1.50:80",
                "-o",
                str(cfg),
            ],
            check=True,
            capture_output=True,
            env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        )
        out[side] = cfg
    (tmp_path / "b.json").write_text(json.dumps(_record("batch", NAMES[0])))
    (tmp_path / "c.json").write_text(json.dumps(_record("sustained", NAMES[1])))
    return out, tmp_path


def _argv(configs, cont="c.json"):
    cfgs, d = configs
    return [
        str(cfgs["b"]),
        str(cfgs["c"]),
        "--batch-record",
        str(d / "b.json"),
        "--continuous-record",
        str(d / cont),
    ]


def _named_query(differ=None):
    fake = FakeQuery(differ)

    def q(config, sql):
        return fake(Path("b" if config.name.startswith("b") else "c"), sql)

    return q


@pytest.fixture
def creds(monkeypatch):
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "x")
    monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", "x")


def test_cli_exit_0_on_parity(configs, creds, capsys):
    assert SP.main(_argv(configs), query=_named_query()) == 0
    out = capsys.readouterr().out
    assert "lakehouse.silver.transactions: equal" in out
    assert "not compared" in out


def test_cli_exit_1_on_a_difference(configs, creds):
    q = _named_query({"hash": ["lakehouse.silver.transactions"], "columns": {}})
    assert SP.main(_argv(configs), query=q) == 1


def test_cli_refuses_an_undrained_continuous_record(configs, creds):
    _cfgs, d = configs
    (d / "u.json").write_text(
        json.dumps(_record("sustained", NAMES[1], continuous__drain__state="x"))
    )
    assert SP.main(_argv(configs, "u.json"), query=_named_query()) == 2


def test_cli_refuses_records_of_other_deployments(configs, creds):
    _cfgs, d = configs
    (d / "o.json").write_text(json.dumps(_record("sustained", "rel17-other")))
    assert SP.main(_argv(configs, "o.json"), query=_named_query()) == 2


def test_cli_exit_4_when_a_query_fails_or_returns_garbage(configs, creds):
    def broken(config, sql):
        raise SP.QueryFailed("trino down")

    assert SP.main(_argv(configs), query=broken) == 4
    assert SP.main(_argv(configs), query=lambda c, s: []) == 4


def test_cli_refuses_a_config_that_does_not_load(configs, creds, tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("name: [unclosed\n")
    argv = _argv(configs)
    argv[0] = str(bad)
    assert SP.main(argv, query=_named_query()) == 2
