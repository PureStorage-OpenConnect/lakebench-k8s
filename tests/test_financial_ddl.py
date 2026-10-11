"""Tests for the Financial Iceberg DDL constants (ENG-2C.3a)."""

import pytest

from lakebench.deploy.financial_ddl import (
    FINANCIAL_TABLE_DDLS,
    render_ddl,
)


class TestRegistry:
    def test_registry_keys_match_tablenamesconfig_fields(self):
        """The registry key must be a valid attribute on TableNamesConfig."""
        from lakebench.config.schema import TableNamesConfig

        cfg = TableNamesConfig()
        for key in FINANCIAL_TABLE_DDLS:
            assert hasattr(cfg, key), f"TableNamesConfig missing field '{key}'"


class TestRenderDDL:
    def test_placeholder_substitution(self):
        rendered = render_ddl(
            "CREATE TABLE {catalog}.{table} (id BIGINT)",
            catalog="lakehouse",
            table="silver.entities",
        )
        assert rendered == "CREATE TABLE lakehouse.silver.entities (id BIGINT)"


def _ddl_columns(ddl: str) -> list[tuple[str, str]]:
    """(name, type) per column line of a CREATE TABLE body, comments stripped."""
    import re

    body = re.split(r"\n\)", ddl.split("(", 1)[1], maxsplit=1)[0]
    out = []
    depth = 0
    for raw in body.splitlines():
        line = raw.split("--", 1)[0].rstrip().rstrip(",")
        if not line.strip():
            continue
        if depth == 0:
            parts = line.split()
            out.append((parts[0], " ".join(parts[1:2])))
        depth += line.count("<") - line.count(">")
    return out


class TestTmOperationsLockstep:
    """P10 gold tables: the deployer DDL and tm_operations.py's inline DDL
    agree column for column, name and type."""

    @pytest.mark.parametrize(
        "key,inline_name",
        [
            ("gold_tm_reconciliation", "DDL_RECON"),
            ("gold_scenario_coverage", "DDL_COVERAGE"),
            ("gold_alert_dispositions", "DDL_DISPOSITIONS"),
            ("gold_cases", "DDL_CASES"),
        ],
    )
    def test_inline_ddl_matches_deployer_ddl(self, key, inline_name):
        from pathlib import Path

        src = Path("src/lakebench/spark/scripts/tm_operations.py").read_text()
        inline = src[src.index(f"{inline_name} = ") :].split('"""')[1]
        assert _ddl_columns(FINANCIAL_TABLE_DDLS[key]) == _ddl_columns(inline)
        assert _ddl_columns(inline), f"{inline_name} parsed to no columns"

    def test_every_tm_table_is_named_in_config_and_workload_tables(self):
        from lakebench.config.schema import TableNamesConfig

        t = TableNamesConfig()
        env = t.financial_env()
        gold = t.workload_tables("financial", layers=("gold",))
        for key in (
            "gold_tm_reconciliation",
            "gold_scenario_coverage",
            "gold_alert_dispositions",
            "gold_cases",
        ):
            assert getattr(t, key) in gold
            assert getattr(t, key) in env.values()


def test_datagen_fatf_list_matches_the_reference_file():
    """kyc.rs FATF_LISTED is exactly the home codes on the FATF list."""
    import json
    import re
    from pathlib import Path

    kyc = Path("datagen_rs/src/kyc.rs").read_text()
    world = Path("datagen_rs/src/world.rs").read_text()
    listed = re.findall(r'"([A-Z]{2})"', re.search(r"FATF_LISTED[^=]*=\s*\[(.*?)\];", kyc).group(1))
    home = re.findall(
        r'"([A-Z]{2})"', re.search(r"HOME_CODES[^=]*=\s*\[(.*?)\];", world, re.S).group(1)
    )
    ref = json.loads(Path("src/lakebench/spark/data/aml/high_risk_jurisdictions.json").read_text())
    fatf = {e["country_code"] for e in ref["entries"]}
    assert sorted(listed) == sorted(set(home) & fatf)


def test_synthetic_corridor_list_matches_the_generator():
    """synthetic_corridors.json (W7's synthetic list), typology.rs
    HIGH_RISK_CC and aml_features.HIGH_RISK_COUNTRIES name one set, and none
    of it is presented as FATF."""
    import json
    import re
    from pathlib import Path

    typ = Path("datagen_rs/src/typology.rs").read_text()
    cc = re.findall(r'"([A-Z]{2})"', re.search(r"HIGH_RISK_CC[^=]*=\s*\[(.*?)\];", typ).group(1))
    ref = json.loads(Path("src/lakebench/spark/data/aml/synthetic_corridors.json").read_text())
    codes = {e["country_code"] for e in ref["entries"]}
    feats = Path("src/lakebench/spark/scripts/aml_features.py").read_text()
    hr = re.findall(r'"([A-Z]{2})"', re.search(r"HIGH_RISK_COUNTRIES = \((.*?)\)", feats).group(1))
    assert codes == set(cc) == set(hr)
    assert ref["source"].startswith("SYNTHETIC")
    assert {e["risk_tier"] for e in ref["entries"]} == {"synthetic_corridor"}
