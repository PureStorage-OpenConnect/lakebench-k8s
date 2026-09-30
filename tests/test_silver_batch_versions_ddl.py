"""I10: silver_batch_versions DDL is declared correctly at both sites.

Two sites carry the CREATE TABLE text: ``deploy/financial_ddl.py`` renders
it via the deployer's create-if-not-exists loop, and the inline
``DDL_BATCH_VERSIONS`` in ``silver_build_financial.py`` renders it at
silver-job startup. A drift between the two silently ships one schema in
one deploy path and another in the other. This test locks the column
list, types, and NOT NULL constraints at both sites.

The broader cross-site column-name and column-type sync check runs in
``tests/test_silver_ddl_sync.py`` for every silver table; that file's
``_TABLES`` list now also includes ``silver_batch_versions``. This module
adds the specific-column assertions the general parser cannot express.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from lakebench.deploy.financial_ddl import (
    FINANCIAL_TABLE_DDLS,
    SILVER_BATCH_VERSIONS_DDL,
    render_ddl,
)

_REPO = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO / "src/lakebench/spark/scripts/silver_build_financial.py"


def _load_ddl_batch_versions_source() -> str:
    """Return the inline ``DDL_BATCH_VERSIONS`` f-string body, catalog and
    table placeholders substituted so column checks match the deploy DDL."""
    text = _SCRIPT.read_text()
    m = re.search(
        r'DDL_BATCH_VERSIONS\s*=\s*f"""(.*?)"""',
        text,
        re.DOTALL,
    )
    assert m, "DDL_BATCH_VERSIONS constant not found in silver_build_financial.py"
    body = m.group(1)
    # Substitute the f-string placeholders the module resolves at import.
    return (
        body.replace("{CATALOG}", "lakehouse")
        .replace("{SILVER_BATCH_VERSIONS}", "silver.silver_batch_versions")
        .replace(
            "{ICEBERG_V2_SNAPPY_PROPS_SQL}",
            "'format-version' = '2', 'write.parquet.compression-codec' = 'snappy'",
        )
    )


class TestDeployDDL:
    def test_registered_in_ddl_registry(self):
        assert "silver_batch_versions" in FINANCIAL_TABLE_DDLS
        assert FINANCIAL_TABLE_DDLS["silver_batch_versions"] is SILVER_BATCH_VERSIONS_DDL

    def test_creates_iceberg_v2_snappy_table(self):
        assert "CREATE TABLE IF NOT EXISTS" in SILVER_BATCH_VERSIONS_DDL
        assert "USING iceberg" in SILVER_BATCH_VERSIONS_DDL
        assert "'format-version' = '2'" in SILVER_BATCH_VERSIONS_DDL
        assert "'write.parquet.compression-codec' = 'snappy'" in SILVER_BATCH_VERSIONS_DDL

    def test_stream_id_is_not_null_string(self):
        assert "stream_id      STRING NOT NULL" in SILVER_BATCH_VERSIONS_DDL

    def test_batch_id_is_not_null_bigint(self):
        assert "batch_id       BIGINT NOT NULL" in SILVER_BATCH_VERSIONS_DDL

    def test_committed_at_is_not_null_timestamp(self):
        assert "committed_at   TIMESTAMP NOT NULL" in SILVER_BATCH_VERSIONS_DDL

    def test_no_partitioning(self):
        # One row per (stream_id, batch_id). Partitioning at this cardinality
        # is a manifest bloat cost with no read-time benefit.
        assert "PARTITIONED BY" not in SILVER_BATCH_VERSIONS_DDL

    def test_placeholders_render(self):
        rendered = render_ddl(
            SILVER_BATCH_VERSIONS_DDL,
            catalog="lakehouse",
            table="silver.silver_batch_versions",
        )
        assert "lakehouse.silver.silver_batch_versions" in rendered
        assert "{catalog}" not in rendered
        assert "{table}" not in rendered


class TestInlineDDL:
    """The inline ``DDL_BATCH_VERSIONS`` in the silver script matches the
    deploy-side DDL column-for-column."""

    @pytest.fixture(scope="class")
    def inline_ddl(self):
        return _load_ddl_batch_versions_source()

    def test_stream_id_is_not_null_string(self, inline_ddl):
        assert "stream_id      STRING NOT NULL" in inline_ddl

    def test_batch_id_is_not_null_bigint(self, inline_ddl):
        assert "batch_id       BIGINT NOT NULL" in inline_ddl

    def test_committed_at_is_not_null_timestamp(self, inline_ddl):
        assert "committed_at   TIMESTAMP NOT NULL" in inline_ddl

    def test_iceberg_v2_snappy(self, inline_ddl):
        assert "USING iceberg" in inline_ddl
        assert "'format-version' = '2'" in inline_ddl
        assert "'write.parquet.compression-codec' = 'snappy'" in inline_ddl


class TestConfigWiring:
    def test_tablenamesconfig_field_present(self):
        from lakebench.config.schema import TableNamesConfig

        cfg = TableNamesConfig()
        assert hasattr(cfg, "silver_batch_versions")
        assert cfg.silver_batch_versions == "silver.silver_batch_versions"

    def test_financial_env_exports_batch_versions(self):
        from lakebench.config.schema import TableNamesConfig

        env = TableNamesConfig().financial_env()
        assert env.get("LB_FINANCIAL_SILVER_BATCH_VERSIONS") == "silver.silver_batch_versions"

    def test_workload_tables_includes_sidecar(self):
        from lakebench.config.schema import TableNamesConfig

        silver_tables = TableNamesConfig().workload_tables("financial", layers=("silver",))
        assert "silver.silver_batch_versions" in silver_tables
