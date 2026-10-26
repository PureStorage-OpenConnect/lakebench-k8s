"""bronze_verify_financial re-registers bronze.manifest and bronze.accounts on
every register path, including the CTAS taken when the source is over the
add_files limits. Left in place, the previous corpus's manifest and account
map would score this corpus's alerts.

Runs main() on a local Hadoop Iceberg catalog named like the scripts' default.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg")

CAT = "lakehouse"


class _Session:
    """The module's Spark session with ``stop`` a no-op."""

    def __init__(self, spark):
        self._spark = spark

    def stop(self):
        pass

    def __getattr__(self, name):
        return getattr(self._spark, name)


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(spark_session, CAT, tmp_path_factory.mktemp("aml-bronze-wh"))
    return spark_session


def _corpus(spark, root):
    cols = (
        "msg_id",
        "cre_dt_tm",
        "intr_bk_sttlm_dt",
        "txn_id",
        "end_to_end_id",
        "uetr",
        "intr_bk_sttlm_amt",
        "intr_bk_sttlm_ccy",
        "dbtr",
        "cdtr",
        "dbtr_agt",
        "cdtr_agt",
        "purp_cd",
    )
    rows = [tuple(f"{c}-{i}" for c in cols) for i in range(3)]
    schema = ", ".join(f"{c} string" for c in cols)
    spark.createDataFrame(rows, schema).write.parquet(str(root / "pacs008/bronze/pacs008"))
    spark.createDataFrame([("T1", 7)], "typology_id string, seed bigint").write.parquet(
        str(root / "pacs008/manifest/manifest.parquet")
    )
    spark.createDataFrame(
        [(1, "DE01"), (2, "DE02")], "holder_entity_id bigint, iban string"
    ).write.parquet(str(root / "pacs008/bronze/account.parquet"))


def test_force_ctas_reregisters_manifest_and_accounts(spark, load_script, monkeypatch, tmp_path):
    monkeypatch.setenv("LB_CATALOG_TYPE", "polaris")  # no explicit bronze location
    bvf = load_script("bronze_verify_financial")
    _corpus(spark, tmp_path)
    monkeypatch.setattr(bvf, "BRONZE_URI", f"file://{tmp_path}/")
    monkeypatch.setattr(bvf, "ACCOUNT_URI", f"file://{tmp_path}/pacs008/bronze/account.parquet")
    monkeypatch.setattr(bvf, "ADD_FILES_MAX_FILES", 0)  # every source is over the limit
    monkeypatch.setattr(bvf, "refuse_protected_corpus", lambda s: None)
    monkeypatch.setattr(bvf, "write_bronze_data_clock", lambda *a, **k: None)
    # The previous corpus's tables, which this run must replace.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CAT}.bronze")
    for t in ("bronze.manifest", "bronze.accounts"):
        spark.sql(f"DROP TABLE IF EXISTS {CAT}.{t}")
        spark.createDataFrame([("stale",)], "old string").writeTo(f"{CAT}.{t}").create()
    builder = type(
        "B", (), {"appName": lambda self, n: self, "getOrCreate": lambda self: _Session(spark)}
    )
    monkeypatch.setattr("pyspark.sql.SparkSession.builder", builder())

    bvf.main()

    assert spark.table(f"{CAT}.default.pacs008_raw").count() == 3
    manifest = spark.table(f"{CAT}.bronze.manifest")
    assert manifest.columns == ["typology_id", "seed"] and manifest.count() == 1
    accounts = spark.table(f"{CAT}.bronze.accounts")
    assert accounts.columns == ["holder_entity_id", "iban"] and accounts.count() == 2
