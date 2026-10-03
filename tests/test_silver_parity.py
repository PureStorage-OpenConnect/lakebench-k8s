"""scripts/release/silver_parity.py: business columns from the silver DDL,
the drained-corpus refusal and the comparison, against a fake query."""

from __future__ import annotations

import importlib.util
import json
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


def test_every_ddl_column_is_hashed_summed_or_excluded():
    for spec in SP.table_specs():
        got = [*spec.hashed, *spec.summed, *spec.excluded]
        assert sorted(got) == sorted(_independent_columns(spec.key)), spec.key


def test_only_sentinels_and_per_mode_columns_are_excluded():
    excluded = {c for s in SP.table_specs() for c in s.excluded}
    assert excluded == {"_batch_id", "_stream_id", "ingest_ts", "profile_updated_ts"}
    assert "committed_at" in SP.SENTINELS  # batch_versions, reported by count only


def test_only_entity_profile_doubles_are_summed():
    by_key = {s.key: s for s in SP.table_specs()}
    assert set(by_key["silver_entity_profiles"].summed) == {
        "active_span_days",
        "avg_amount_usd",
        "stddev_amount_usd",
        "avg_gap_days",
        "passthrough_ratio",
        "_m2",
    }
    assert "initial_risk_score" in by_key["silver_entities"].hashed
    assert all(not s.summed for k, s in by_key.items() if k != "silver_entity_profiles")
    assert "correspondent_chain" in by_key["silver"].hashed
    assert "address" in by_key["silver_entities"].hashed


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
    assert "new_business_col" in spec.hashed


def test_missing_ddl_constant_refuses(tmp_path):
    bad = tmp_path / "f.py"
    bad.write_text("FINANCIAL_TABLE_DDLS = {}\n")
    with pytest.raises(SP.Refused, match="no DDL constant"):
        SP.ddl_strings(bad)


def test_sql_hashes_business_columns_and_sums_merged_doubles():
    spec = next(s for s in SP.table_specs() if s.key == "silver_entity_profiles")
    sql = SP.table_sql("lakehouse.silver.entity_profiles", spec)
    assert sql.startswith("SELECT count(*), to_hex(checksum(ROW(entity_id, first_seen_ts")
    assert "sum(_m2)" in sql and "profile_updated_ts" not in sql and "_batch_id" not in sql


# -- records -------------------------------------------------------------------


def _record(mode: str, seed: int = 43, scale: float = 1.0, **extra: Any) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "experiment": {"workload": {"name": "financial"}, "corpus": {"seed": seed, "scale": scale}},
        "pipeline_benchmark": {"pipeline_mode": mode},
    }
    if mode == "sustained":
        rec["pipeline_benchmark"]["corpus_drained"] = True
        rec["continuous"] = {"drain": {"state": "drained"}}
    for path, value in extra.items():
        node = rec
        *heads, last = path.split("__")
        for h in heads:
            node = node.setdefault(h, {})
        node[last] = value
    return rec


def test_drained_matching_records_can_be_compared():
    assert SP.record_problems(_record("batch"), _record("sustained")) == []


@pytest.mark.parametrize(
    ("cont", "why"),
    [
        (_record("sustained", pipeline_benchmark__corpus_drained=False), "drained corpus"),
        (_record("sustained", continuous__drain__state="timed_out"), "drain state"),
        (_record("sustained", seed=44), "different or unknown corpora"),
        (_record("sustained", scale=10.0), "different or unknown corpora"),
        (_record("batch"), "continuous record's mode"),
    ],
)
def test_undrained_or_mismatched_records_refuse(cont, why):
    assert any(why in p for p in SP.record_problems(_record("batch"), cont))


# -- comparison ------------------------------------------------------------------


TABLES = {k: f"lakehouse.silver.{k}" for k in (*SP.SILVER_KEYS, SP.VERSIONS_KEY)}


class FakeQuery:
    def __init__(self, differ: dict[str, Any] | None = None) -> None:
        self.differ = differ or {}
        self.calls: list[tuple[str, str]] = []

    def __call__(self, config: Path, sql: str) -> list[str]:
        self.calls.append((config.name, sql))
        side = config.name
        table = sql.rsplit("FROM ", 1)[1]
        if sql.startswith("SELECT count(*) FROM"):
            return ["1" if side == "b" else "40"]
        if sql.startswith("SELECT to_hex(checksum("):
            n = sql.count("checksum(")
            cells = ["h"] * n
            if side == "c" and table in self.differ.get("columns", {}):
                cells[self.differ["columns"][table]] = "x"
            return cells
        n_sums = sql.count("sum(")
        row = ["100", "abc"] + ["1.0"] * n_sums
        if side == "c":
            if table in self.differ.get("hash", ()):
                row[1] = "def"
            if table in self.differ.get("sum", {}):
                row[2] = str(self.differ["sum"][table])
        return row


def _run(query):
    return SP.compare(Path("b"), Path("c"), TABLES, SP.table_specs(), query)


def test_equal_tables_compare_equal_and_versions_are_not_compared():
    results, versions = _run(FakeQuery())
    assert all(r.equal for r in results)
    assert versions == {"lakehouse.silver.silver_batch_versions": ("1", "40")}


def test_a_different_hash_names_the_differing_column():
    t = "lakehouse.silver.silver_accounts"
    results, _ = _run(FakeQuery({"hash": [t], "columns": {t: 3}}))
    bad = [r for r in results if not r.equal]
    assert [r.table for r in bad] == [t]
    spec = next(s for s in SP.table_specs() if s.key == "silver_accounts")
    assert any(spec.hashed[3] in n for n in bad[0].notes)


def test_doubles_within_tolerance_are_equal_and_beyond_it_differ():
    t = "lakehouse.silver.silver_entity_profiles"
    results, _ = _run(FakeQuery({"sum": {t: 1.0 + 1e-12}}))
    assert all(r.equal for r in results)
    results, _ = _run(FakeQuery({"sum": {t: 1.0 + 1e-6}}))
    assert [r.table for r in results if not r.equal] == [t]


# -- the CLI -------------------------------------------------------------------


@pytest.fixture
def configs(tmp_path):
    out = {}
    for side in ("b", "c"):
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
                f"rel17-sp-{side}",
                "--endpoint",
                "http://10.0.1.50:80",
                "-o",
                str(cfg),
            ],
            check=True,
            capture_output=True,
            env={**__import__("os").environ, "PYTHONPATH": str(ROOT / "src")},
        )
        out[side] = cfg
    (tmp_path / "b.json").write_text(json.dumps(_record("batch")))
    (tmp_path / "c.json").write_text(json.dumps(_record("sustained")))
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


def test_cli_exit_0_on_parity(configs, monkeypatch, capsys):
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "x")
    monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", "x")
    assert SP.main(_argv(configs), query=_named_query()) == 0
    out = capsys.readouterr().out
    assert "lakehouse.silver.transactions: equal" in out
    assert "not compared" in out


def test_cli_exit_1_on_a_difference(configs, monkeypatch):
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "x")
    monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", "x")
    q = _named_query({"hash": ["lakehouse.silver.transactions"], "columns": {}})
    assert SP.main(_argv(configs), query=q) == 1


def test_cli_refuses_an_undrained_continuous_record(configs):
    _cfgs, d = configs
    (d / "u.json").write_text(json.dumps(_record("sustained", continuous__drain__state="x")))
    assert SP.main(_argv(configs, "u.json"), query=_named_query()) == 2


def test_cli_exit_4_when_a_query_fails(configs, monkeypatch):
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "x")
    monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", "x")

    def broken(config, sql):
        raise RuntimeError("trino down")

    assert SP.main(_argv(configs), query=broken) == 4
