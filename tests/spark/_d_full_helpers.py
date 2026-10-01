"""Shared bootstrap for the D-full-simple stream tests.

Every test in this block needs a local Iceberg catalog with the three
silver tables silver_stream_financial writes (transactions, edges,
account_statements) plus silver.accounts / silver.entities for the
current_balance MERGE. Instead of repeating the ~150 lines of DDL and
Spark bootstrap in each file, this module centralises the shape.

The tests each run a Spark child through the ``spark_subprocess`` fixture
(tests/spark/conftest.py), which passes the test jars, and import this
module there to build the catalog. Nothing in this file runs Spark
at import time -- ``pyspark`` and the Iceberg jar are only touched from
inside the child.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

PACS_SCHEMA = (
    "txn_id string, uetr string, "
    "dbtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "cdtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "dbtr_agt struct<bicfi:string>, cdtr_agt struct<bicfi:string>, "
    "dbtr_acct struct<iban:string>, cdtr_acct struct<iban:string>, "
    "intrmy_agt_1 struct<bicfi:string>, "
    "intrmy_agt_2 struct<bicfi:string>, "
    "intrmy_agt_3 struct<bicfi:string>, "
    "intr_bk_sttlm_amt decimal(18,2), intr_bk_sttlm_ccy string, "
    "cre_dt_tm timestamp, purp_cd string, "
    "rgltry_rptg array<string>, msg_id string"
)


TXNS_DDL = """
CREATE TABLE lh.silver.transactions (
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
    cross_border            BOOLEAN,
    regulatory_reported     BOOLEAN NOT NULL,
    rptd_originator_name    STRING,
    rptd_originator_address STRING,
    rptd_beneficiary_name   STRING,
    rptd_beneficiary_address STRING,
    source_message_ref      STRING,
    _batch_id               BIGINT,
    _stream_id              STRING,
    ingest_ts               TIMESTAMP
) USING iceberg PARTITIONED BY (months(txn_timestamp))
"""

EDGES_DDL = """
CREATE TABLE lh.silver.counterparty_edges (
    source_entity_id       BIGINT NOT NULL,
    target_entity_id       BIGINT NOT NULL,
    first_seen_ts          TIMESTAMP NOT NULL,
    last_seen_ts           TIMESTAMP NOT NULL,
    cumulative_amount_usd  DECIMAL(38, 2) NOT NULL,
    txn_count              BIGINT NOT NULL,
    _batch_id              BIGINT,
    _stream_id             STRING
) USING iceberg PARTITIONED BY (bucket(64, source_entity_id))
"""

STATEMENTS_DDL = """
CREATE TABLE lh.silver.account_statements (
    account_id     BIGINT NOT NULL,
    iban           STRING NOT NULL,
    entry_seq      BIGINT NOT NULL,
    book_ts        TIMESTAMP NOT NULL,
    val_ts         TIMESTAMP NOT NULL,
    cdt_dbt_ind    STRING NOT NULL,
    amt            DECIMAL(18, 2) NOT NULL,
    ccy            STRING NOT NULL,
    bal_before     DECIMAL(38, 2) NOT NULL,
    bal_after      DECIMAL(38, 2) NOT NULL,
    txn_id         STRING NOT NULL,
    uetr           STRING NOT NULL,
    bk_tx_cd       STRING NOT NULL,
    _batch_id      BIGINT,
    _stream_id     STRING
) USING iceberg PARTITIONED BY (months(book_ts))
"""

# silver.accounts DDL: only the columns _maintain_statements' MERGE touches.
# (Test setups pre-populate iban -> current_balance pairs; other columns are
# NOT NULL in production but the test uses the minimal schema the MERGE needs.)
ACCOUNTS_DDL = """
CREATE TABLE lh.silver.accounts (
    account_id         BIGINT NOT NULL,
    iban               STRING,
    holder_entity_id   BIGINT NOT NULL,
    bank_bic           STRING NOT NULL,
    currency           STRING NOT NULL,
    opened_date        DATE   NOT NULL,
    closed_date        DATE,
    current_balance    DECIMAL(38, 2),
    home_fi            STRING,
    is_customer        BOOLEAN
) USING iceberg
"""

ENTITIES_DDL = """
CREATE TABLE lh.silver.entities (
    entity_id BIGINT NOT NULL,
    entity_type STRING NOT NULL,
    name STRING NOT NULL,
    legal_name STRING,
    address STRUCT<street:STRING, town:STRING, region:STRING, postcode:STRING, country:STRING>,
    email_addr STRING,
    phone_number STRING,
    country STRING,
    lei STRING,
    bic STRING,
    sanctions_status STRING,
    pep_status BOOLEAN,
    initial_risk_score DOUBLE,
    is_customer BOOLEAN,
    home_fi STRING,
    customer_since DATE,
    customer_type STRING,
    expected_monthly_volume_usd DECIMAL(18, 2),
    crr_score INT,
    crr_tier STRING,
    crr_factors STRING
) USING iceberg
"""


def bronze_row(spark, txn_id, ts, dbtr_iban="GB01", cdtr_iban="US02", amt="100.00"):
    """One-row bronze DataFrame in pacs.008 shape."""

    def party(nm):
        return (nm, "US", ("NYC", "MAIN ST"), (f"LEI-{nm}",))

    row = (
        txn_id,
        f"UETR-{txn_id}",
        party(f"O-{txn_id}"),
        party(f"B-{txn_id}"),
        ("MERIUS2L",),
        ("NRTHGB3X",),
        (dbtr_iban,),
        (cdtr_iban,),
        (None,),
        (None,),
        (None,),
        Decimal(amt),
        "USD",
        ts,
        "SALA",
        [],
        f"MSG-{txn_id}",
    )
    return spark.createDataFrame([row], PACS_SCHEMA)


def bronze_batch(spark, rows):
    """rows: list of (txn_id, ts, dbtr_iban, cdtr_iban, amt) tuples."""
    dfs = [bronze_row(spark, r[0], r[1], r[2], r[3], r[4]) for r in rows]
    out = dfs[0]
    for df in dfs[1:]:
        out = out.unionByName(df)
    return out


def build_spark(work_dir, jars):
    """Local[1] SparkSession with a Hadoop-catalog Iceberg pointing at work_dir."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lh.type", "hadoop")
        .config("spark.sql.catalog.lh.cache-enabled", "false")
        .config("spark.sql.catalog.lh.warehouse", f"file://{work_dir}/wh")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def bootstrap_catalog(spark):
    """Create silver namespace and every table _maintain_statements needs."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
    for ddl in (TXNS_DDL, EDGES_DDL, STATEMENTS_DDL, ACCOUNTS_DDL, ENTITIES_DDL):
        spark.sql(ddl)


def seed_account(spark, iban, holder_entity_id=1, bank_bic="BICFI", currency="USD"):
    """Insert one silver.accounts row so the current_balance MERGE has a target."""
    from pyspark.sql.functions import col, lit, xxhash64

    df = spark.createDataFrame(
        [(iban, holder_entity_id, bank_bic, currency)],
        "iban string, holder_entity_id bigint, bank_bic string, currency string",
    ).select(
        xxhash64(col("iban")).alias("account_id"),
        col("iban"),
        col("holder_entity_id"),
        col("bank_bic"),
        col("currency"),
        lit(datetime(2024, 1, 1).date()).alias("opened_date"),
        lit(None).cast("date").alias("closed_date"),
        lit(None).cast("decimal(38,2)").alias("current_balance"),
        lit(None).cast("string").alias("home_fi"),
        lit(None).cast("boolean").alias("is_customer"),
    )
    df.writeTo("lh.silver.accounts").append()


def bind_stream_module(spark):
    """Point the silver_stream_financial module at the test's Iceberg catalog
    and stub out KYC + dimensions so tests can focus on statements."""
    import silver_stream_financial as ss

    ss.CATALOG = "lh"
    ss.SILVER_TXNS = "silver.transactions"
    ss.SILVER_EDGES = "silver.counterparty_edges"
    ss.SILVER_STATEMENTS = "silver.account_statements"
    ss.SILVER_ACCOUNTS = "silver.accounts"
    ss.SILVER_ENTITIES = "silver.entities"

    ss._KYC = None
    ss._KYC_LOADED = True
    ss._kyc = lambda _s: None
    ss.append_new_dimensions = lambda *_a, **_kw: (0, 0)
    return ss


def opening_balance_sql(iban_literal):
    """Match ``build_statements`` deterministic opening_balance formula in
    Spark SQL. Callers use this to compute expected values via ``spark.sql``
    without reproducing xxhash64 in Python.
    """
    return f"(abs(xxhash64('{iban_literal}')) % 200000) + 10000"


BASE_TS = datetime(2024, 6, 1)


def batch_rows(batch_id, count, iban_pairs=(("GB01", "US02"),)):
    """rows for one micro-batch: `count` payments across the given iban pairs."""
    out = []
    for i in range(count):
        dbtr, cdtr = iban_pairs[i % len(iban_pairs)]
        ts = BASE_TS + timedelta(hours=batch_id * 24 + i)
        txn_id = f"B{batch_id}T{i}"
        amt = "100.00"
        out.append((txn_id, ts, dbtr, cdtr, amt))
    return out
