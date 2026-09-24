"""Tests for the Financial Iceberg DDL constants (ENG-2C.3a)."""

import pytest

from lakebench.deploy.financial_ddl import (
    BRONZE_PACS008_DDL,
    FINANCIAL_TABLE_DDLS,
    GOLD_ALERTS_DDL,
    GOLD_DAILY_DASHBOARDS_DDL,
    GOLD_ENTITY_CLUSTERS_DDL,
    GOLD_RISK_SCORES_DDL,
    SILVER_ACCOUNT_STATEMENTS_DDL,
    SILVER_ACCOUNTS_DDL,
    SILVER_COUNTERPARTY_EDGES_DDL,
    SILVER_ENTITIES_DDL,
    SILVER_TRANSACTIONS_DDL,
    render_ddl,
)

ALL_DDLS = [
    BRONZE_PACS008_DDL,
    SILVER_TRANSACTIONS_DDL,
    SILVER_ENTITIES_DDL,
    SILVER_ACCOUNTS_DDL,
    SILVER_ACCOUNT_STATEMENTS_DDL,
    SILVER_COUNTERPARTY_EDGES_DDL,
    GOLD_ALERTS_DDL,
    GOLD_RISK_SCORES_DDL,
    GOLD_ENTITY_CLUSTERS_DDL,
    GOLD_DAILY_DASHBOARDS_DDL,
]


class TestDDLShape:
    """Structural checks -- catches obvious template drift or copy-paste rot."""

    @pytest.mark.parametrize("ddl", ALL_DDLS)
    def test_creates_iceberg_table_if_not_exists(self, ddl):
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "USING iceberg" in ddl

    @pytest.mark.parametrize("ddl", ALL_DDLS)
    def test_has_catalog_and_table_placeholders(self, ddl):
        assert "{catalog}" in ddl
        assert "{table}" in ddl

    @pytest.mark.parametrize("ddl", ALL_DDLS)
    def test_declares_format_version_2(self, ddl):
        assert "'format-version' = '2'" in ddl

    @pytest.mark.parametrize("ddl", ALL_DDLS)
    def test_uses_snappy_compression(self, ddl):
        assert "'write.parquet.compression-codec' = 'snappy'" in ddl


class TestBronzeSchema:
    """pacs.008 must carry the fields the silver_build_financial script joins on."""

    def test_carries_uetr(self):
        assert "uetr                         STRING NOT NULL" in BRONZE_PACS008_DDL

    def test_carries_settlement_date_partition_column(self):
        assert "intr_bk_sttlm_dt             DATE" in BRONZE_PACS008_DDL

    def test_bronze_is_unpartitioned(self):
        # Unpartitioned: add_files cannot register into a days() transform.
        assert "PARTITIONED BY" not in BRONZE_PACS008_DDL

    def test_carries_party_chain(self):
        # Debtor, creditor, ultimate parties: essential for entity resolution (W5).
        for party in ("ultmt_dbtr", "dbtr ", "cdtr ", "ultmt_cdtr"):
            assert party in BRONZE_PACS008_DDL, f"missing {party}"


class TestSilverSchema:
    def test_transactions_partitioned_by_day(self):
        assert "PARTITIONED BY (months(txn_timestamp))" in SILVER_TRANSACTIONS_DDL

    def test_counterparty_edges_bucketed_by_source(self):
        assert "PARTITIONED BY (bucket(64, source_entity_id))" in SILVER_COUNTERPARTY_EDGES_DDL

    def test_entities_carries_sanctions_and_pep(self):
        assert "sanctions_status" in SILVER_ENTITIES_DDL
        assert "pep_status" in SILVER_ENTITIES_DDL

    def test_transactions_carries_originator_beneficiary_keys(self):
        assert "originator_id           BIGINT NOT NULL" in SILVER_TRANSACTIONS_DDL
        assert "beneficiary_id          BIGINT NOT NULL" in SILVER_TRANSACTIONS_DDL


class TestGoldSchema:
    def test_alerts_carries_run_id_for_W9_writeback(self):
        # W9 writeback in the spec sets run_id = spark.conf.get("spark.lb.run_id")
        assert "run_id             STRING NOT NULL" in GOLD_ALERTS_DDL

    def test_alerts_carries_rule_and_model_versioning(self):
        for col in ("rule_id", "rule_version", "model_id", "model_version"):
            assert col in GOLD_ALERTS_DDL

    def test_alerts_partitioned_by_day(self):
        # Case-management systems read the last-N-days alerts hot.
        assert "PARTITIONED BY (months(alert_ts))" in GOLD_ALERTS_DDL

    def test_risk_scores_supports_merge_key(self):
        # W9 MERGE keys on (entity_id, model_id).
        assert "entity_id              BIGINT NOT NULL" in GOLD_RISK_SCORES_DDL
        assert "model_id               STRING NOT NULL" in GOLD_RISK_SCORES_DDL

    def test_entity_clusters_carries_member_list(self):
        assert "member_entity_ids     ARRAY<BIGINT> NOT NULL" in GOLD_ENTITY_CLUSTERS_DDL

    def test_daily_dashboards_avoids_precision_keyword_collision(self):
        # 'precision' is reserved in some SQL dialects; column named precision_val.
        assert "precision_val" in GOLD_DAILY_DASHBOARDS_DDL
        assert "\n    precision  " not in GOLD_DAILY_DASHBOARDS_DDL


class TestRegistry:
    def test_registry_covers_all_ddls(self):
        expected = {
            "bronze",
            "silver",
            "silver_entities",
            "silver_accounts",
            "silver_account_statements",
            "silver_counterparty_edges",
            "silver_entity_profiles",
            "gold_alerts",
            "gold_risk_scores",
            "gold_entity_clusters",
            "gold_daily_dashboards",
        }
        assert set(FINANCIAL_TABLE_DDLS) == expected

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

    def test_bronze_ddl_renders_without_error(self):
        rendered = render_ddl(BRONZE_PACS008_DDL, "lakehouse", "default.pacs008_raw")
        assert "lakehouse.default.pacs008_raw" in rendered
        assert "{catalog}" not in rendered
        assert "{table}" not in rendered


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


class TestKycLockstep:
    """P10 stage 0/2 columns: deployer DDL and the silver job's inline DDL agree."""

    @pytest.mark.parametrize(
        "deployer_ddl,inline_name",
        [(SILVER_ENTITIES_DDL, "DDL_ENTITIES"), (SILVER_ACCOUNTS_DDL, "DDL_ACCOUNTS")],
    )
    def test_inline_ddl_matches_deployer_ddl(self, deployer_ddl, inline_name):
        from pathlib import Path

        src = Path("src/lakebench/spark/scripts/silver_build_financial.py").read_text()
        inline = src[src.index(f"{inline_name} = ") :].split('"""')[1]
        names = [n for n, _ in _ddl_columns(deployer_ddl)]
        assert names == [n for n, _ in _ddl_columns(inline)]

    def test_entities_carry_kyc_columns(self):
        names = [n for n, _ in _ddl_columns(SILVER_ENTITIES_DDL)]
        for c in (
            "is_customer",
            "home_fi",
            "customer_since",
            "customer_type",
            "expected_monthly_volume_usd",
            "crr_score",
            "crr_tier",
            "crr_factors",
        ):
            assert c in names

    def test_accounts_carry_home_fi(self):
        names = [n for n, _ in _ddl_columns(SILVER_ACCOUNTS_DDL)]
        assert "home_fi" in names and "is_customer" in names

    def test_kyc_column_tuples_match_ddl(self):
        """silver_build's ensure_column list covers every KYC column in the DDL."""
        import ast
        from pathlib import Path

        tree = ast.parse(Path("src/lakebench/spark/scripts/silver_build_financial.py").read_text())
        consts = {
            t.id: ast.literal_eval(n.value)
            for n in tree.body
            if isinstance(n, ast.Assign)
            for t in n.targets
            if isinstance(t, ast.Name) and t.id in ("KYC_ENTITY_COLUMNS", "KYC_ACCOUNT_COLUMNS")
        }
        ent = [n for n, _ in _ddl_columns(SILVER_ENTITIES_DDL)]
        acc = [n for n, _ in _ddl_columns(SILVER_ACCOUNTS_DDL)]
        assert [n for n, _ in consts["KYC_ENTITY_COLUMNS"]] == ent[ent.index("is_customer") :]
        assert [n for n, _ in consts["KYC_ACCOUNT_COLUMNS"]] == acc[acc.index("home_fi") :]


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
