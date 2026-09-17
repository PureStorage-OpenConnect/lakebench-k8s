"""Silver Build (Financial) -- normalise bronze pacs.008 into the 4 silver tables.

Output tables (DDL in src/lakebench/deploy/financial_ddl.py):
- silver.transactions          -- flat transaction facts, partitioned by day
- silver.entities              -- Person/Company/FI dimension
- silver.accounts              -- IBAN-to-entity linkage
- silver.counterparty_edges    -- entity-to-entity aggregate edges (bucketed)

The normalisation is deliberately deterministic in ``(originator/beneficiary
name-hash, seed=0)`` so re-runs on the same bronze snapshot produce the same
entity_id assignments -- important for W1 (connected components) and W5
(Splink) reproducibility.

Strategy note: v1 uses a single SIMPLE pass. The
``spark.lb.silver.strategy`` conf is honoured for parity with Customer 360's
silver_build; STREAMING / SALTED variants are future work when live UAT
surfaces skew.
"""

from __future__ import annotations

import time

from common import env, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    array_distinct,
    array_remove,
    coalesce,
    col,
    concat_ws,
    lit,
    to_date,
    trim,
    upper,
    xxhash64,
)
from pyspark.sql.functions import (
    count as count_,
)
from pyspark.sql.functions import (
    max as max_,
)
from pyspark.sql.functions import (
    min as min_,
)
from pyspark.sql.functions import (
    sum as sum_,
)

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
SILVER_TRANSACTIONS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_ACCOUNTS = env("LB_FINANCIAL_SILVER_ACCOUNTS", "silver.accounts")
SILVER_EDGES = env("LB_FINANCIAL_SILVER_EDGES", "silver.counterparty_edges")
STRATEGY = env("spark.lb.silver.strategy", "simple")


# ---------------------------------------------------------------------------
# DDL bootstrap (create-if-not-exists; matches src/lakebench/deploy/financial_ddl.py)
# ---------------------------------------------------------------------------


DDL_TXNS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_TRANSACTIONS} (
    txn_id                  STRING NOT NULL,
    uetr                    STRING NOT NULL,
    originator_id           BIGINT NOT NULL,
    beneficiary_id          BIGINT NOT NULL,
    originator_bank_bic     STRING,
    beneficiary_bank_bic    STRING,
    txn_amount              DECIMAL(18, 2) NOT NULL,
    txn_currency            STRING NOT NULL,
    txn_amount_usd          DECIMAL(18, 2),
    txn_timestamp           TIMESTAMP NOT NULL,
    txn_type                STRING NOT NULL,
    purpose_code            STRING,
    correspondent_chain     ARRAY<STRING>,
    cross_border            BOOLEAN NOT NULL,
    regulatory_reported     BOOLEAN NOT NULL,
    rptd_originator_name    STRING,
    rptd_originator_address STRING,
    rptd_beneficiary_name   STRING,
    rptd_beneficiary_address STRING,
    source_message_ref      STRING
) USING iceberg PARTITIONED BY (days(txn_timestamp))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""

DDL_ENTITIES = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_ENTITIES} (
    entity_id           BIGINT NOT NULL,
    entity_type         STRING NOT NULL,
    name                STRING NOT NULL,
    legal_name          STRING,
    address             STRUCT<street: STRING, town: STRING, region: STRING, postcode: STRING, country: STRING>,
    email_addr          STRING,
    phone_number        STRING,
    country             STRING,
    lei                 STRING,
    bic                 STRING,
    sanctions_status    STRING,
    pep_status          BOOLEAN,
    initial_risk_score  DOUBLE
) USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""

DDL_ACCOUNTS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_ACCOUNTS} (
    account_id         BIGINT NOT NULL,
    iban               STRING,
    holder_entity_id   BIGINT NOT NULL,
    bank_bic           STRING NOT NULL,
    currency           STRING NOT NULL,
    opened_date        DATE   NOT NULL,
    closed_date        DATE,
    current_balance    DECIMAL(18, 2)
) USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""

DDL_EDGES = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_EDGES} (
    source_entity_id       BIGINT NOT NULL,
    target_entity_id       BIGINT NOT NULL,
    first_seen_ts          TIMESTAMP NOT NULL,
    last_seen_ts           TIMESTAMP NOT NULL,
    cumulative_amount_usd  DECIMAL(18, 2) NOT NULL,
    txn_count              BIGINT NOT NULL
) USING iceberg PARTITIONED BY (bucket(64, source_entity_id))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _entity_id_from(name_col, country_col):
    """Deterministic BIGINT entity_id from (uppercased trimmed name, country)."""
    key = concat_ws("|", upper(trim(name_col)), coalesce(country_col, lit("")))
    return xxhash64(key)


def build_transactions(bronze):
    """Flatten pacs.008 -> silver.transactions shape."""
    return bronze.select(
        col("txn_id"),
        col("uetr"),
        _entity_id_from(col("dbtr.nm"), col("dbtr.ctry_of_res")).alias("originator_id"),
        _entity_id_from(col("cdtr.nm"), col("cdtr.ctry_of_res")).alias("beneficiary_id"),
        col("dbtr_agt.bicfi").alias("originator_bank_bic"),
        col("cdtr_agt.bicfi").alias("beneficiary_bank_bic"),
        col("intr_bk_sttlm_amt").cast("decimal(18,2)").alias("txn_amount"),
        col("intr_bk_sttlm_ccy").alias("txn_currency"),
        (col("intr_bk_sttlm_amt") * col("xchg_rate")).cast("decimal(18,2)").alias("txn_amount_usd"),
        col("cre_dt_tm").alias("txn_timestamp"),
        lit("wire").alias("txn_type"),
        col("purp_cd").alias("purpose_code"),
        array_distinct(
            array_remove(
                array(
                    col("intrmy_agt_1.bicfi"),
                    col("intrmy_agt_2.bicfi"),
                    col("intrmy_agt_3.bicfi"),
                ),
                None,
            )
        ).alias("correspondent_chain"),
        (col("dbtr.ctry_of_res") != col("cdtr.ctry_of_res")).alias("cross_border"),
        (col("rgltry_rptg").isNotNull()).alias("regulatory_reported"),
        col("dbtr.nm").alias("rptd_originator_name"),
        col("dbtr.pstl_adr.strt_nm").alias("rptd_originator_address"),
        col("cdtr.nm").alias("rptd_beneficiary_name"),
        col("cdtr.pstl_adr.strt_nm").alias("rptd_beneficiary_address"),
        col("msg_id").alias("source_message_ref"),
    )


def build_entities(txns_df):
    """Distinct entity dimension from originator+beneficiary sides."""
    orig = txns_df.select(
        col("originator_id").alias("entity_id"),
        col("rptd_originator_name").alias("name"),
    )
    bene = txns_df.select(
        col("beneficiary_id").alias("entity_id"),
        col("rptd_beneficiary_name").alias("name"),
    )
    all_entities = orig.unionByName(bene).dropDuplicates(["entity_id"])
    return all_entities.select(
        col("entity_id"),
        lit("Person").alias("entity_type"),
        col("name"),
        col("name").alias("legal_name"),
        lit(None)
        .cast(
            "struct<street: string, town: string, region: string, postcode: string, country: string>"
        )
        .alias("address"),
        lit(None).cast("string").alias("email_addr"),
        lit(None).cast("string").alias("phone_number"),
        lit(None).cast("string").alias("country"),
        lit(None).cast("string").alias("lei"),
        lit(None).cast("string").alias("bic"),
        lit("clear").alias("sanctions_status"),
        lit(False).alias("pep_status"),
        lit(0.0).alias("initial_risk_score"),
    )


def build_accounts(bronze):
    """Distinct IBAN -> holder_entity from the pacs.008 payload."""
    dbtr = bronze.select(
        col("dbtr_acct.iban").alias("iban"),
        _entity_id_from(col("dbtr.nm"), col("dbtr.ctry_of_res")).alias("holder_entity_id"),
        col("dbtr_agt.bicfi").alias("bank_bic"),
        col("dbtr_acct.ccy").alias("currency"),
        to_date(col("cre_dt_tm")).alias("opened_date"),
    )
    cdtr = bronze.select(
        col("cdtr_acct.iban").alias("iban"),
        _entity_id_from(col("cdtr.nm"), col("cdtr.ctry_of_res")).alias("holder_entity_id"),
        col("cdtr_agt.bicfi").alias("bank_bic"),
        col("cdtr_acct.ccy").alias("currency"),
        to_date(col("cre_dt_tm")).alias("opened_date"),
    )
    all_accts = dbtr.unionByName(cdtr).filter(col("iban").isNotNull()).dropDuplicates(["iban"])
    return all_accts.select(
        xxhash64(col("iban")).alias("account_id"),
        col("iban"),
        col("holder_entity_id"),
        col("bank_bic"),
        col("currency"),
        col("opened_date"),
        lit(None).cast("date").alias("closed_date"),
        lit(None).cast("decimal(18,2)").alias("current_balance"),
    )


def build_edges(txns_df):
    """Aggregate (originator, beneficiary) pairs into edges."""
    return (
        txns_df.groupBy("originator_id", "beneficiary_id")
        .agg(
            min_(col("txn_timestamp")).alias("first_seen_ts"),
            max_(col("txn_timestamp")).alias("last_seen_ts"),
            sum_(col("txn_amount_usd")).cast("decimal(18,2)").alias("cumulative_amount_usd"),
            count_(lit(1)).alias("txn_count"),
        )
        .select(
            col("originator_id").alias("source_entity_id"),
            col("beneficiary_id").alias("target_entity_id"),
            col("first_seen_ts"),
            col("last_seen_ts"),
            col("cumulative_amount_usd"),
            col("txn_count"),
        )
    )


def main() -> None:
    spark = SparkSession.builder.appName("lb-silver-build-financial").getOrCreate()
    start = time.time()

    log("=" * 60)
    log("Silver Build (Financial)")
    log(f"Strategy: {STRATEGY}")
    log("=" * 60)

    for name, ddl in (
        ("transactions", DDL_TXNS),
        ("entities", DDL_ENTITIES),
        ("accounts", DDL_ACCOUNTS),
        ("edges", DDL_EDGES),
    ):
        spark.sql(ddl)
        log(f"Bootstrapped silver.{name}")

    bronze = spark.table(f"{CATALOG}.{BRONZE_TABLE}")
    log(f"Read bronze: {CATALOG}.{BRONZE_TABLE} ({bronze.count():,} rows)")

    txns = build_transactions(bronze)
    txns.writeTo(f"{CATALOG}.{SILVER_TRANSACTIONS}").createOrReplace()
    log("Wrote silver.transactions")

    build_entities(txns).writeTo(f"{CATALOG}.{SILVER_ENTITIES}").createOrReplace()
    log("Wrote silver.entities")

    build_accounts(bronze).writeTo(f"{CATALOG}.{SILVER_ACCOUNTS}").createOrReplace()
    log("Wrote silver.accounts")

    build_edges(txns).writeTo(f"{CATALOG}.{SILVER_EDGES}").createOrReplace()
    log("Wrote silver.counterparty_edges")

    log("=" * 60)
    log(f"Silver build complete in {time.time() - start:.1f}s")
    log("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
