"""Silver Build (Financial) -- normalise bronze pacs.008 into the 5 silver tables.

Output tables (DDL in src/lakebench/deploy/financial_ddl.py):
- silver.transactions          -- flat transaction facts, partitioned by month
- silver.entities              -- Person/Company/FI dimension
- silver.accounts              -- IBAN-to-entity linkage + current_balance
- silver.account_statements    -- camt.053-shaped statement lines with running balance
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

import os
import time

from common import (
    ICEBERG_V2_SNAPPY_PROPS_SQL,
    SilverAbort,
    assert_progress,
    ensure_column,
    ensure_namespaces_for_ddl,
    ensure_partition_transform,
    env,
    iceberg_table_stats,
    log,
    log_job_metrics,
    refuse_batch_while_stream_active,
    resolve_data_clock,
)
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    abs as abs_,
)
from pyspark.sql.functions import (
    array,
    array_distinct,
    array_remove,
    coalesce,
    col,
    concat_ws,
    create_map,
    date_format,
    datediff,
    greatest,
    least,
    lit,
    row_number,
    size,
    to_date,
    trim,
    upper,
    when,
    xxhash64,
)
from pyspark.sql.functions import (
    avg as avg_,
)
from pyspark.sql.functions import (
    count as count_,
)
from pyspark.sql.functions import (
    countDistinct as count_distinct_,
)
from pyspark.sql.functions import (
    max as max_,
)
from pyspark.sql.functions import (
    min as min_,
)
from pyspark.sql.functions import (
    stddev as stddev_,
)
from pyspark.sql.functions import (
    sum as sum_,
)

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
BRONZE_TABLE = env("LB_FINANCIAL_BRONZE_TABLE", "default.pacs008_raw")
SILVER_TRANSACTIONS = env("LB_FINANCIAL_SILVER_TRANSACTIONS", "silver.transactions")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_ACCOUNTS = env("LB_FINANCIAL_SILVER_ACCOUNTS", "silver.accounts")
SILVER_STATEMENTS = env("LB_FINANCIAL_SILVER_STATEMENTS", "silver.account_statements")
SILVER_EDGES = env("LB_FINANCIAL_SILVER_EDGES", "silver.counterparty_edges")
SILVER_PROFILES = env("LB_FINANCIAL_SILVER_PROFILES", "silver.entity_profiles")
SILVER_BATCH_VERSIONS = env("LB_FINANCIAL_SILVER_BATCH_VERSIONS", "silver.silver_batch_versions")
STRATEGY = env("spark.lb.silver.strategy", "simple")

# Reference zones the datagen writes next to pacs.008 (party = the reporting
# FI's party master with KYC, account = account master). Same root the bronze
# jobs read: LB_BRONZE_URI + LB_FINANCIAL_BRONZE_PREFIX.
_BRONZE_URI = env("LB_BRONZE_URI", "s3a://lb-bronze/")
_BRONZE_ROOT = env("LB_FINANCIAL_BRONZE_PREFIX", "pacs008/").rstrip("/")
PARTY_PATH = env("LB_FINANCIAL_PARTY_PATH", f"{_BRONZE_URI}{_BRONZE_ROOT}/bronze/party.parquet")
ACCOUNT_PATH = env(
    "LB_FINANCIAL_ACCOUNT_PATH", f"{_BRONZE_URI}{_BRONZE_ROOT}/bronze/account.parquet"
)
# Every cycle's manifest; its model_version says whether the corpus has KYC.
# party.parquet and account.parquet are written once, by cycle 0 of a
# multi-cycle run (they are the same for every cycle), so the plain names
# cover cycle n > 0 runs too.
MANIFEST_GLOB = env(
    "LB_FINANCIAL_MANIFEST_GLOB", f"{_BRONZE_URI}{_BRONZE_ROOT}/manifest/manifest*.parquet"
)
# Datagen model_versions from before the KYC reference columns. Only these
# excuse missing masters; datagen-v2-rs-0.2 is the first KYC version.
PRE_KYC_MODEL_VERSIONS = frozenset({"datagen-v2-rs-0.1"})

# Columns silver.entities takes from the party master (GOALS P10 stages 0 and
# 2). The KYC columns are NULL for non-customers: the reporting FI holds no
# CDD file on another bank's customer.
KYC_ENTITY_COLUMNS = (
    ("is_customer", "boolean"),
    ("home_fi", "string"),
    ("customer_since", "date"),
    ("customer_type", "string"),
    ("expected_monthly_volume_usd", "decimal(18,2)"),
    ("crr_score", "int"),
    ("crr_tier", "string"),
    ("crr_factors", "string"),
)
KYC_ACCOUNT_COLUMNS = (("home_fi", "string"), ("is_customer", "boolean"))

# Reference USD rates by settlement currency, mirroring
# datagen_rs::amounts::fx_to_usd (a drift test keeps the two in step). The
# generator expresses each amount in its account's currency, so silver needs a
# reference rate to put every row on one USD scale (LB-137).
_FX_TO_USD = {
    "USD": 1.0,
    "GBP": 1.30,
    "EUR": 1.10,
    "CHF": 1.15,
    "JPY": 0.0068,
    "AED": 0.27,
    "SGD": 0.74,
    "CAD": 0.73,
    "MXN": 0.055,
    "CNY": 0.14,
    "INR": 0.012,
    "AUD": 0.66,
    "HKD": 0.128,
    "KRW": 0.00075,
    "BRL": 0.19,
}


def _usd_rate(ccy_col):
    """Reference USD rate for a currency column; unknown currencies get 1.0,
    matching the generator's fallback."""
    pairs = []
    for k, v in _FX_TO_USD.items():
        pairs += [lit(k), lit(v)]
    return coalesce(create_map(*pairs)[ccy_col], lit(1.0))


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
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""
# `_batch_id` supports the silver_stream two-phase batchId idempotency
# protocol (LB-109). Batch-mode writes leave it NULL; streaming writes
# tag each row with the Structured Streaming batchId so that on retry the
# handler can DELETE WHERE _batch_id = X + reinsert without double-count.
# Nullable so batch-mode `_replace_data(build_transactions(...))` works
# unchanged.

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
    initial_risk_score  DOUBLE,
    is_customer         BOOLEAN,
    home_fi             STRING,
    customer_since      DATE,
    customer_type       STRING,
    expected_monthly_volume_usd DECIMAL(18, 2),
    crr_score           INT,
    crr_tier            STRING,
    crr_factors         STRING
) USING iceberg
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
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
    current_balance    DECIMAL(38, 2),
    home_fi            STRING,
    is_customer        BOOLEAN
) USING iceberg
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""
# current_balance is DECIMAL(38, 2) (not 18, 2) to match bal_after in
# silver.account_statements. Storing the running-balance roll-up in a narrower
# type would truncate to NULL on overflow the moment update_accounts_balance
# rolls up a large aggregator account. Keep this DDL in lock-step with
# lakebench/deploy/financial_ddl.py:SILVER_ACCOUNTS_DDL.

DDL_STATEMENTS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_STATEMENTS} (
    account_id     BIGINT NOT NULL,
    iban           STRING NOT NULL,
    entry_seq      BIGINT NOT NULL,
    book_ts        TIMESTAMP NOT NULL,
    val_ts         TIMESTAMP NOT NULL,
    cdt_dbt_ind    STRING NOT NULL,     -- 'CRDT' | 'DBIT'
    amt            DECIMAL(18, 2) NOT NULL,
    ccy            STRING NOT NULL,
    bal_before     DECIMAL(38, 2) NOT NULL,
    bal_after      DECIMAL(38, 2) NOT NULL,
    txn_id         STRING NOT NULL,
    uetr           STRING NOT NULL,
    bk_tx_cd       STRING NOT NULL      -- ISO 20022 bank txn code, e.g. PMNT-ICDT
) USING iceberg PARTITIONED BY (months(book_ts))
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""

DDL_EDGES = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_EDGES} (
    source_entity_id       BIGINT NOT NULL,
    target_entity_id       BIGINT NOT NULL,
    first_seen_ts          TIMESTAMP NOT NULL,
    last_seen_ts           TIMESTAMP NOT NULL,
    cumulative_amount_usd  DECIMAL(38, 2) NOT NULL,
    txn_count              BIGINT NOT NULL,
    _batch_id              BIGINT,
    _stream_id             STRING
) USING iceberg PARTITIONED BY (bucket(64, source_entity_id))
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""

# C-PROFILES (LB-130): per-entity behavioural baseline. Kept in lock-step with
# SILVER_ENTITY_PROFILES_DDL in deploy/financial_ddl.py.
DDL_PROFILES = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_PROFILES} (
    entity_id                   BIGINT NOT NULL,
    first_seen_ts               TIMESTAMP,
    last_seen_ts                TIMESTAMP,
    active_span_days            DOUBLE,
    txn_count_out               BIGINT NOT NULL,
    txn_count_in                BIGINT NOT NULL,
    txn_count_total             BIGINT NOT NULL,
    total_sent_usd              DECIMAL(38, 2),
    total_received_usd          DECIMAL(38, 2),
    avg_amount_usd              DOUBLE,
    stddev_amount_usd           DOUBLE,
    avg_gap_days                DOUBLE,
    distinct_counterparties_out BIGINT NOT NULL,
    distinct_counterparties_in  BIGINT NOT NULL,
    passthrough_ratio           DOUBLE,
    profile_updated_ts          TIMESTAMP,
    _batch_id                   BIGINT
) USING iceberg PARTITIONED BY (bucket(64, entity_id))
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""
# `_batch_id` is reserved for the continuous profile-maintenance path (not yet
# built -- silver_stream does not refresh profiles today; C-PROFILES continuous
# MERGE is the follow-up before W4/W8 read this table in continuous mode).
# Batch silver_build writes one row per entity with _batch_id = NULL.

# I10 sidecar: keep in lock-step with deploy/financial_ddl.py:SILVER_BATCH_VERSIONS_DDL.
# A single-row insert after every successful micro-batch (stream) or cycle
# (batch) makes (stream_id, batch_id) the "sealed" key downstream consumers
# semi-join against. Missing row => partial batch, must be hidden.
DDL_BATCH_VERSIONS = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_BATCH_VERSIONS} (
    stream_id      STRING NOT NULL,
    batch_id       BIGINT NOT NULL,
    committed_at   TIMESTAMP NOT NULL
) USING iceberg
TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})
"""


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _entity_id_from(name_col, country_col, city_col=None, lei_col=None):
    """Deterministic BIGINT entity_id, LEI-first with name-hash fallback.

    LB-101 (P1): bronze carries a role-independent LEI on both dbtr and
    cdtr sides (``dbtr.id.lei``, ``cdtr.id.lei``); the Rust datagen
    stamps ``lei_for(entity_id)`` for both roles, so the same real
    entity appears with the same LEI on either side of a wire. Keying
    entity_id on LEI collapses the bipartite split at its root: the
    same real entity gets one silver entity_id regardless of whether
    the row is a debit or credit.

    Name-hash fallback preserves compatibility with historical bronze
    or upstream sources that do not populate LEI. When ``lei_col`` is
    None or the value is NULL / empty for a row, the fallback hashes
    ``(upper(trim(name)), country, upper(trim(city)))`` -- Even so,
    two real distinct entities with identical name+country+city will
    still merge; for a real system, BIC/DOB would be additional keys.
    Splink (W5) is the intended resolution layer for name-only rows.

    Mixed-LEI risk (real-world only): if the same real entity appears
    with LEI populated on some rows and NULL on others (a real-world
    data-quality pattern that does NOT occur in the current Rust
    datagen, which stamps LEI unconditionally at
    ``datagen_rs/src/emit.rs:242-243``), silver produces two entity_ids
    for that entity -- one LEI-hash, one name-hash. Not a defect of
    this fix; a downstream Splink pass or an ingest-time LEI-backfill
    handles it. Documented as a known constraint on non-synthetic
    bronze rather than papered over here.

    Each fallback column is coalesced to "" BEFORE concat_ws because
    concat_ws silently skips NULL columns rather than emitting a
    delimiter -- so without the coalesce, a NULL city collides with a
    missing city segment and NULL country collides with a missing
    country segment, letting anonymous entities collapse across
    otherwise-distinct rows.
    """
    if city_col is None:
        city_col = lit("")
    fallback_key = concat_ws(
        "|",
        coalesce(upper(trim(name_col)), lit("")),
        coalesce(country_col, lit("")),
        coalesce(upper(trim(city_col)), lit("")),
    )
    fallback = xxhash64(fallback_key)
    if lei_col is None:
        return fallback
    # LEI is a 20-char string; treat empty / whitespace-only as absent.
    trimmed_lei = trim(lei_col)
    return when(
        trimmed_lei.isNotNull() & (trimmed_lei != lit("")),
        xxhash64(trimmed_lei),
    ).otherwise(fallback)


def build_transactions(bronze):
    """Flatten pacs.008 -> silver.transactions shape.

    Notes on approximations:
    - `txn_amount_usd` is the settlement amount times a fixed reference rate
      for the settlement currency (`_FX_TO_USD`). `xchg_rate` is the
      settlement-to-instructed rate in pacs.008, not a USD rate, so it is not
      used here. Fixed rates are fine for a benchmark; a bank would use a
      dated FX table.
    - `cross_border` is NULL when either party's country is NULL: the answer
      is genuinely unknown and coalescing to False silently claimed "same
      country" for missing-country corpora (I2). The DDL column is nullable.
    - `regulatory_reported` uses `size(rgltry_rptg) > 0`, not `isNotNull`,
      because an empty array is still a non-null value under pyspark
      semantics and would flip this to True for every row.
    - `originator_id` / `beneficiary_id` also feed on city to reduce name
      collision (see `_entity_id_from`).
    """
    return bronze.select(
        col("txn_id"),
        col("uetr"),
        _entity_id_from(
            col("dbtr.nm"),
            col("dbtr.ctry_of_res"),
            col("dbtr.pstl_adr.twn_nm"),
            col("dbtr.id.lei"),
        ).alias("originator_id"),
        _entity_id_from(
            col("cdtr.nm"),
            col("cdtr.ctry_of_res"),
            col("cdtr.pstl_adr.twn_nm"),
            col("cdtr.id.lei"),
        ).alias("beneficiary_id"),
        col("dbtr_agt.bicfi").alias("originator_bank_bic"),
        col("cdtr_agt.bicfi").alias("beneficiary_bank_bic"),
        col("intr_bk_sttlm_amt").cast("decimal(18,2)").alias("txn_amount"),
        col("intr_bk_sttlm_ccy").alias("txn_currency"),
        (col("intr_bk_sttlm_amt") * _usd_rate(col("intr_bk_sttlm_ccy")))
        .cast("decimal(18,2)")
        .alias("txn_amount_usd"),
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
        # I2: preserve NULL when either party's country is unknown. Coalescing
        # a NULL != NULL to False silently wrote "same country" for corpora
        # where one side's country was missing, hiding the DQ signal for any
        # reader that would want to `WHERE cross_border IS NULL`. Existing
        # readers (`cross_border = TRUE` / `SUM(CASE WHEN cross_border ...)`)
        # keep their behaviour: NULL is treated as False by those forms.
        when(
            col("dbtr.ctry_of_res").isNull() | col("cdtr.ctry_of_res").isNull(),
            lit(None).cast("boolean"),
        )
        .otherwise(col("dbtr.ctry_of_res") != col("cdtr.ctry_of_res"))
        .alias("cross_border"),
        (size(coalesce(col("rgltry_rptg"), array())) > 0).alias("regulatory_reported"),
        col("dbtr.nm").alias("rptd_originator_name"),
        col("dbtr.pstl_adr.strt_nm").alias("rptd_originator_address"),
        col("cdtr.nm").alias("rptd_beneficiary_name"),
        col("cdtr.pstl_adr.strt_nm").alias("rptd_beneficiary_address"),
        col("msg_id").alias("source_message_ref"),
        # LB-109: _batch_id populated by silver_stream, NULL for batch mode.
        # Present in every DataFrame that writes silver.transactions so the
        # DataFrameWriterV2.overwrite() column set matches the target schema.
        lit(None).cast("bigint").alias("_batch_id"),
        # B2: _stream_id scopes _batch_id to one streaming query so a fresh
        # checkpoint's batch 0 does not collide with the previous stream's.
        # Batch-mode writes stamp the sentinel 'batch' so a batch overwrite
        # followed by a stream never DELETEs batch-mode rows (streams key on
        # _stream_id = streaming_query_id, never 'batch').
        lit("batch").alias("_stream_id"),
        # Continuous clock: bronze-ingest stamps each micro-batch; batch
        # bronze has no ingest time, so freshness stays undefined there.
        (col("ingest_ts") if "ingest_ts" in bronze.columns else lit(None).cast("timestamp")).alias(
            "ingest_ts"
        ),
    )


def _entity_countries(bronze, with_iban=False):
    """entity_id -> country of residence, from both sides of every payment.

    silver.transactions does not store the parties' countries (they only feed
    the entity_id hash and cross_border), so they are read from bronze with
    the same entity_id expression build_transactions uses. Country is part of
    the name-hash key, so each non-LEI entity has exactly one country; for
    LEI-keyed entities the lexically smallest is taken (deterministic).

    ``with_iban`` also returns the IBAN the entity's payments use (smallest if
    several; the datagen gives each entity one), which is the key to its KYC
    file. Same pass, so KYC costs no extra bronze scan.
    """
    from pyspark.sql.functions import min as _min

    sides = []
    for side in ("dbtr", "cdtr"):
        cols = [
            _entity_id_from(
                col(f"{side}.nm"),
                col(f"{side}.ctry_of_res"),
                col(f"{side}.pstl_adr.twn_nm"),
                col(f"{side}.id.lei"),
            ).alias("entity_id"),
            col(f"{side}.ctry_of_res").alias("country"),
        ]
        if with_iban:
            cols.append(col(f"{side}_acct.iban").alias("iban"))
        sides.append(bronze.select(*cols))
    aggs = [_min("country").alias("country")]
    if with_iban:
        aggs.append(_min("iban").alias("iban"))
    # min() skips NULLs, so an entity with no country (or IBAN) on any row
    # gets NULL, the same as a missing row on the left join below.
    return sides[0].unionByName(sides[1]).groupBy("entity_id").agg(*aggs)


def build_kyc(party, account):
    """KYC per payment IBAN, from the datagen's party and account masters.

    A payment names its parties' accounts, so the bank links a payment to its
    customer file through the account: IBAN -> account holder -> party. The
    datagen writes each entity's payment IBAN as its first account, so the
    join is one-to-one. Returns None when the reference files predate the
    KYC columns (older corpora), so silver writes NULLs instead of failing.
    """
    if party is None or account is None or "is_customer" not in party.columns:
        return None
    acct = account.select(
        col("iban"),
        col("holder_entity_id").alias("_dg_id"),
        col("home_fi").alias("_acct_home_fi"),
    )
    kyc_cols = [
        col(name).cast(sql_type).alias(name)
        for name, sql_type in KYC_ENTITY_COLUMNS
        if name != "home_fi"
    ]
    # The party master carries no sanctions or PEP flag (AML-GOALS #50): that
    # is the answer the screening rules W5/W6 are scored against.
    prt = party.select(
        col("entity_id").alias("_dg_id"),
        col("home_fi").alias("home_fi"),
        *kyc_cols,
    )
    return acct.join(prt, "_dg_id", "inner").drop("_dg_id")


def build_entities(txns_df, bronze=None, kyc=None):
    """Distinct entity dimension from originator+beneficiary sides.

    Determinism note: `dropDuplicates(["entity_id"])` picks arbitrarily on
    collision, which breaks the docstring's "same bronze -> same silver"
    contract when two txns for the same entity_id have different reported
    names (spelling drift, casing). Fix: group by entity_id and take the
    lexically-smallest name / country deterministically.
    """
    orig = txns_df.select(
        col("originator_id").alias("entity_id"),
        col("rptd_originator_name").alias("name"),
    )
    bene = txns_df.select(
        col("beneficiary_id").alias("entity_id"),
        col("rptd_beneficiary_name").alias("name"),
    )
    from pyspark.sql.functions import min as _min

    picked = orig.unionByName(bene).groupBy("entity_id").agg(_min("name").alias("_min_name"))
    # Coalesce to an explicit "UNKNOWN" so an entity whose reported name is
    # NULL for every occurrence (plausible when a party name field is
    # missing) doesn't violate silver.entities.name NOT NULL. Emitting
    # "UNKNOWN" here is loud in a downstream dashboard; a NULL would
    # crash the write with a delayed error.
    picked = picked.withColumn("name", coalesce(col("_min_name"), lit("UNKNOWN"))).drop("_min_name")
    # Country was a NULL literal, so W7 (high-risk corridor), which inner-joins
    # on a non-NULL beneficiary country, could never fire and reported
    # "ran, 0 alerts" (2026-09-24 audit).
    use_kyc = kyc is not None and bronze is not None
    if bronze is not None:
        picked = picked.join(_entity_countries(bronze, with_iban=use_kyc), "entity_id", "left")
    else:
        picked = picked.withColumn("country", lit(None).cast("string"))
    if use_kyc:
        # Prefix the KYC columns so they cannot collide with the name-derived
        # columns above; the final select renames them.
        k = kyc.select(
            col("iban"),
            *[col(c).alias(f"_{c}") for c in kyc.columns if c != "iban"],
        )
        picked = picked.join(k, "iban", "left")
    return picked.select(
        col("entity_id"),
        # We can't tell Person from Company from FI from pacs.008 name alone;
        # a common heuristic is "the name ends with a corporate suffix
        # (LTD/INC/GMBH/PLC etc.) -> Company", else default Person. Applied
        # to the END of the name only (via $) so "MARIA SA" doesn't match
        # SA as a Company (SA at word-end common in personal names) and
        # short two-letter tokens (AG, BV, SA) don't false-positive
        # anywhere in the middle. Corporate names put the suffix at the
        # end by convention. "L.L.C." is intentionally not detected here
        # -- the dotted form is rare in pacs.008 dbtr/cdtr fields.
        when(
            upper(col("name")).rlike(
                r"(LTD|LIMITED|INC|CORP|LLC|GMBH|AG|PLC|SA|SARL|BV|BANK|CAPITAL|HOLDINGS|GROUP|INTERNATIONAL|COMPANY|CO)$"
            ),
            lit("Company"),
        )
        .otherwise(lit("Person"))
        .alias("entity_type"),
        col("name"),
        col("name").alias("legal_name"),
        lit(None)
        .cast(
            "struct<street: string, town: string, region: string, postcode: string, country: string>"
        )
        .alias("address"),
        lit(None).cast("string").alias("email_addr"),
        lit(None).cast("string").alias("phone_number"),
        col("country").cast("string").alias("country"),
        lit(None).cast("string").alias("lei"),
        lit(None).cast("string").alias("bic"),
        # Not screened in silver: screening outcomes are W5/W6 alerts in
        # gold.alerts, and the corpus carries no answer key to copy here.
        lit(None).cast("string").alias("sanctions_status"),
        lit(None).cast("boolean").alias("pep_status"),
        lit(None).cast("double").alias("initial_risk_score"),
        *[
            (col(f"_{name}") if use_kyc else lit(None)).cast(sql_type).alias(name)
            for name, sql_type in KYC_ENTITY_COLUMNS
        ],
    )


def build_accounts(bronze, kyc=None):
    """Distinct IBAN -> holder_entity from the pacs.008 payload.

    Fixes LB-104-shape non-determinism and the follow-up cross-side blending
    hazard (I3). An IBAN that appears both as a debtor account (with dbtr's
    entity as holder) and as a creditor account (with cdtr's entity as holder)
    previously had holder_entity_id chosen coin-flip by dropDuplicates, and
    then per-column min mixed the debtor's holder_entity_id with the
    creditor's bank_bic on the same iban. Now: a row_number over
    (holder_entity_id, bank_bic, opened_date) picks one deterministic winning
    row per iban, so every field on the row belongs to the same observation.
    This still doesn't tell us WHICH entity really holds the account --
    pacs.008 does not carry that -- but at least the assignment is stable
    and internally consistent.
    """
    dbtr = bronze.select(
        col("dbtr_acct.iban").alias("iban"),
        _entity_id_from(
            col("dbtr.nm"),
            col("dbtr.ctry_of_res"),
            col("dbtr.pstl_adr.twn_nm"),
            col("dbtr.id.lei"),
        ).alias("holder_entity_id"),
        col("dbtr_agt.bicfi").alias("bank_bic"),
        col("dbtr_acct.ccy").alias("currency"),
        to_date(col("cre_dt_tm")).alias("opened_date"),
    )
    cdtr = bronze.select(
        col("cdtr_acct.iban").alias("iban"),
        _entity_id_from(
            col("cdtr.nm"),
            col("cdtr.ctry_of_res"),
            col("cdtr.pstl_adr.twn_nm"),
            col("cdtr.id.lei"),
        ).alias("holder_entity_id"),
        col("cdtr_agt.bicfi").alias("bank_bic"),
        col("cdtr_acct.ccy").alias("currency"),
        to_date(col("cre_dt_tm")).alias("opened_date"),
    )
    # I3: pick one deterministic winning row per iban. The prior per-column
    # min mixed field values across the debtor and creditor sides for the same
    # iban (e.g. holder_entity_id from the debtor row, bank_bic from the
    # creditor row), which produced a synthetic row no side actually observed.
    # row_number over (holder_entity_id, bank_bic, opened_date) gives a
    # single, reproducible winning row without cross-side blending.
    # NULLS LAST on bank_bic and opened_date preserves the prior _min behaviour
    # of preferring rows whose non-key fields are all populated: silver.accounts
    # declares those columns NOT NULL, so a NULL row winning rn=1 would abort
    # the write.
    _winner_window = Window.partitionBy("iban").orderBy(
        col("holder_entity_id").asc_nulls_last(),
        col("bank_bic").asc_nulls_last(),
        col("opened_date").asc_nulls_last(),
    )
    all_accts = (
        dbtr.unionByName(cdtr)
        .filter(col("iban").isNotNull())
        .withColumn("_rn", row_number().over(_winner_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
    if kyc is not None:
        all_accts = all_accts.join(
            kyc.select(
                col("iban"),
                col("_acct_home_fi").alias("_home_fi"),
                col("is_customer").alias("_is_customer"),
            ),
            "iban",
            "left",
        )
    return all_accts.select(
        xxhash64(col("iban")).alias("account_id"),
        col("iban"),
        col("holder_entity_id"),
        col("bank_bic"),
        col("currency"),
        col("opened_date"),
        lit(None).cast("date").alias("closed_date"),
        lit(None).cast("decimal(38,2)").alias("current_balance"),
        *[
            (col(f"_{name}") if kyc is not None else lit(None)).cast(sql_type).alias(name)
            for name, sql_type in KYC_ACCOUNT_COLUMNS
        ],
    )


def build_statements(bronze, accounts_df):
    """Explode each pacs.008 into two camt.053-shaped statement lines (one DBIT
    on the debtor's account, one CRDT on the creditor's account), then compute
    a running balance per account with a window function.

    This is the heaviest new Spark workload in the pipeline: it doubles the
    transaction row count, joins to `accounts` on IBAN to recover account_id,
    and runs SUM/ROW_NUMBER OVER (PARTITION BY account_id ORDER BY book_ts)
    over ~5B rows at scale 100. That is the point -- the statements table
    exists both to give the schema the balance dimension real payment data has
    (bronze pacs.008 messages carry no balance because it is a ledger concept)
    and to exercise the window-function code path a lakehouse benchmark should
    stress.

    Opening balances are seeded deterministically from account_id so the
    resulting running balances are reproducible on the same bronze snapshot.
    """
    common_cols = [
        col("cre_dt_tm").alias("book_ts"),
        col("cre_dt_tm").alias("val_ts"),  # same-day settlement for wires
        col("intr_bk_sttlm_amt").cast("decimal(18,2)").alias("amt"),
        col("intr_bk_sttlm_ccy").alias("ccy"),
        col("txn_id"),
        col("uetr"),
    ]
    dbit = bronze.select(
        col("dbtr_acct.iban").alias("iban"), lit("DBIT").alias("cdt_dbt_ind"), *common_cols
    )
    crdt = bronze.select(
        col("cdtr_acct.iban").alias("iban"), lit("CRDT").alias("cdt_dbt_ind"), *common_cols
    )
    entries = dbit.unionByName(crdt).filter(col("iban").isNotNull())

    # Join to accounts to recover the BIGINT account_id and a deterministic
    # opening balance in the $10k..$210k range, derived from account_id so the
    # balances are reproducible without carrying state in the datagen.
    acc = accounts_df.select(
        col("account_id"),
        col("iban").alias("_ac_iban"),
        # xxhash64 (used to derive account_id) returns signed BIGINT; Spark's
        # `%` preserves sign, so negative account_ids yielded opening_balance
        # in roughly (-190000, 10000) -- half the accounts started underwater
        # for reasons unrelated to any transaction. abs() before modulo
        # forces the range into (10000, 210000] as intended.
        (((abs_(col("account_id")) % lit(200_000)) + lit(10_000)).cast("decimal(18,2)")).alias(
            "opening_balance"
        ),
    )
    entries = entries.join(acc, entries["iban"] == acc["_ac_iban"], "inner").drop("_ac_iban")

    entries = entries.withColumn(
        "signed_amt",
        when(col("cdt_dbt_ind") == lit("CRDT"), col("amt")).otherwise(-col("amt")),
    )

    # Deterministic ordering: the same bronze row emits both a DBIT and a CRDT
    # entry with identical book_ts and txn_id. When dbtr_iban == cdtr_iban
    # (self-transfer, or an internal book transfer between two of a customer's
    # own IBANs), both entries also land on the same account_id, so
    # (book_ts, txn_id) alone is NOT a stable tie-break -- row_number picks
    # between them arbitrarily and bal_before/bal_after flip between runs
    # even though the terminal balance is the same. A third-level tie-break
    # by cdt_dbt_ind pins it, but ORDER BY on the string column is fragile
    # (a future ISO 20022 code like 'CCTR' or 'CHRG' would resort silently),
    # so map to an explicit numeric priority that documents the intent:
    # DBIT first (the outflow posts before the inflow arrives, matching
    # real bank-ledger convention for self-transfers).
    entries = entries.withColumn(
        "_cdt_dbt_ord",
        when(col("cdt_dbt_ind") == lit("DBIT"), lit(0)).otherwise(lit(1)),
    )
    w = Window.partitionBy("account_id").orderBy(col("book_ts"), col("txn_id"), col("_cdt_dbt_ord"))
    entries = entries.withColumn("entry_seq", row_number().over(w))
    # Cast to decimal(38,2), not (18,2): Spark widens SUM(decimal(18,2))
    # over (..) to (38,2), and truncating back to (18,2) silently returns NULL
    # on overflow (ANSI off is the default). At scale 100 a correspondent /
    # aggregator account can plausibly push the running sum past 10^16, so
    # the narrow cast is a silent-corruption hazard that would surface as
    # "cannot write null to NOT NULL column" late in the job. The DDL matches.
    entries = entries.withColumn(
        "bal_after",
        (col("opening_balance") + sum_(col("signed_amt")).over(w)).cast("decimal(38,2)"),
    )
    entries = entries.withColumn(
        "bal_before", (col("bal_after") - col("signed_amt")).cast("decimal(38,2)")
    )

    # Drop the internal ordering helper before writing to Iceberg.
    return entries.select(
        col("account_id"),
        col("iban"),
        col("entry_seq"),
        col("book_ts"),
        col("val_ts"),
        col("cdt_dbt_ind"),
        col("amt"),
        col("ccy"),
        col("bal_before"),
        col("bal_after"),
        col("txn_id"),
        col("uetr"),
        lit("PMNT-ICDT").alias("bk_tx_cd"),
    )


def update_accounts_balance(accounts_df, statements_df):
    """Overwrite `current_balance` on the account row with the last known
    `bal_after` from the statements table. This is the slow-changing summary
    that a card / mobile app reads; the statements table is the transaction
    log the running-balance window computes over.

    Uses a groupBy(...).agg(max_by(bal_after, entry_seq)) aggregate instead
    of a second window-sort over the entire statements table. Roughly one
    shuffle vs one shuffle + a full sort; at ~5B rows and ~15M accounts that
    matters. Accounts with zero statements retain a NULL current_balance
    rather than being coalesced to $0 -- $0 is a real balance and would
    misreport 'account exists but has never transacted' as 'account is
    empty', a customer-facing wrong answer.
    """
    from pyspark.sql.functions import expr

    latest = statements_df.groupBy("account_id").agg(
        # max_by returns the bal_after row that has the max entry_seq. Spark
        # 3.5+ builtin; falls back to the struct-max trick otherwise.
        expr("max_by(bal_after, entry_seq) as _cb")
    )
    return (
        accounts_df.drop("current_balance")
        .join(latest, on="account_id", how="left")
        # coalesce with a decimal(38,2) NULL cast to preserve nullability of
        # the target column; do NOT default to 0.
        .withColumn("current_balance", col("_cb").cast("decimal(38,2)"))
        .drop("_cb")
        # Back to the DDL column order (the drop/join moved current_balance).
        .select(*accounts_df.columns)
    )


def build_edges(txns_df):
    """Aggregate (originator, beneficiary) pairs into edges."""
    return (
        txns_df.groupBy("originator_id", "beneficiary_id")
        .agg(
            min_(col("txn_timestamp")).alias("first_seen_ts"),
            max_(col("txn_timestamp")).alias("last_seen_ts"),
            # decimal(38,2) not (18,2): a correspondent / aggregator entity at
            # scale 100+ can plausibly accumulate past 10^16 in USD over the
            # corpus window. Truncating back to (18,2) silently returns NULL
            # on overflow (ANSI off is the default) which then fails the DDL
            # NOT NULL constraint on write. DDL is decimal(38,2) to match.
            sum_(col("txn_amount_usd")).cast("decimal(38,2)").alias("cumulative_amount_usd"),
            count_(lit(1)).alias("txn_count"),
        )
        .select(
            col("originator_id").alias("source_entity_id"),
            col("beneficiary_id").alias("target_entity_id"),
            col("first_seen_ts"),
            col("last_seen_ts"),
            col("cumulative_amount_usd"),
            col("txn_count"),
            # LB-109: NULL in batch mode; silver_stream overrides in its
            # per-batch build_edges wrapper. Present so overwrite() writes
            # match the target schema.
            lit(None).cast("bigint").alias("_batch_id"),
            # B2: batch-mode edges carry the same 'batch' sentinel as
            # transactions so a subsequent stream never DELETEs batch rows.
            lit("batch").alias("_stream_id"),
        )
    )


def build_entity_profiles(txns_df, data_clock):
    """Per-entity behavioural baseline (C-PROFILES, LB-130).

    ``data_clock`` (I1, silver-plan): the resolved data clock anchor
    (``datetime.date``) that stamps ``profile_updated_ts``. Passing a
    fixed anchor rather than reading wall-clock inside the transform
    keeps rebuilds byte-identical for the same bronze -- a rebuild that
    differs only in ``profile_updated_ts`` cannot be diffed against an
    earlier run to catch a real regression.

    One row per entity_id, aggregating the originator side and the beneficiary
    side separately then full-outer-joining, so a rule can compare an event
    against the entity's OWN history instead of a population-wide constant.

    Design choices:
    - ``avg_gap_days = active_span_days / (txn_count_out - 1)`` is the MEAN
      inter-transaction gap on the originator side. This is exactly the
      baseline W8 needs: a >=90-day reactivation gap is only anomalous if the
      entity's typical gap is much smaller. It is NULL when txn_count_out < 2
      (no gap is defined for a single send; W8 already declines first-ever
      activity). A cheap span/(n-1) proxy is used deliberately -- the exact
      mean of consecutive gaps equals span/(n-1) by telescoping, so this is not
      an approximation, and it avoids a per-entity ordered window over the whole
      corpus.
    - ``passthrough_ratio = total_sent_usd / total_received_usd`` is W4's
      baseline for "does this entity normally forward what it receives". NULL
      when the entity never received (ratio undefined) -- a pure originator is
      not a pass-through.
    - amount stats are over the originator side (money the entity sends), which
      is what the velocity/structuring rules reason about.
    - _batch_id is NULL in batch mode (mirrors the other silver tables); the
      continuous MERGE path sets it.
    """
    amt = col("txn_amount_usd").cast("double")
    out_side = txns_df.groupBy(col("originator_id").alias("entity_id")).agg(
        min_(col("txn_timestamp")).alias("first_seen_ts"),
        max_(col("txn_timestamp")).alias("last_seen_ts"),
        count_(lit(1)).alias("txn_count_out"),
        sum_(col("txn_amount_usd")).cast("decimal(38,2)").alias("total_sent_usd"),
        avg_(amt).alias("avg_amount_usd"),
        stddev_(amt).alias("stddev_amount_usd"),
        count_distinct_(col("beneficiary_id")).alias("distinct_counterparties_out"),
    )
    in_side = txns_df.groupBy(col("beneficiary_id").alias("entity_id")).agg(
        min_(col("txn_timestamp")).alias("first_seen_in_ts"),
        max_(col("txn_timestamp")).alias("last_seen_in_ts"),
        count_(lit(1)).alias("txn_count_in"),
        sum_(col("txn_amount_usd")).cast("decimal(38,2)").alias("total_received_usd"),
        count_distinct_(col("originator_id")).alias("distinct_counterparties_in"),
    )
    joined = out_side.join(in_side, on="entity_id", how="fullouter")
    # coalesce counts to 0 so the NOT NULL columns are satisfied for entities
    # that only ever appear on one side.
    c_out = coalesce(col("txn_count_out"), lit(0))
    c_in = coalesce(col("txn_count_in"), lit(0))
    # Profile-level first/last span across BOTH sides. greatest/least skip NULLs
    # in Spark, so an entity present on only one side still resolves correctly --
    # and when present on both, first_seen is the EARLIEST of the two sides and
    # last_seen the LATEST (coalesce would wrongly keep the originator side even
    # when the beneficiary side is earlier/later).
    profile_first = least(col("first_seen_ts"), col("first_seen_in_ts"))
    profile_last = greatest(col("last_seen_ts"), col("last_seen_in_ts"))
    span_days = datediff(profile_last, profile_first).cast("double")
    # W8 baseline: mean gap between consecutive SENDS = originator-side span
    # (last send - first send) / (sends - 1). Uses the ORIGINATOR span only, not
    # the combined in+out span, so receive activity does not inflate the gap.
    out_span_days = datediff(col("last_seen_ts"), col("first_seen_ts")).cast("double")
    return joined.select(
        col("entity_id"),
        profile_first.alias("first_seen_ts"),
        profile_last.alias("last_seen_ts"),
        span_days.alias("active_span_days"),
        c_out.alias("txn_count_out"),
        c_in.alias("txn_count_in"),
        (c_out + c_in).alias("txn_count_total"),
        col("total_sent_usd"),
        col("total_received_usd"),
        col("avg_amount_usd"),
        col("stddev_amount_usd"),
        # mean inter-send gap on the originator side; NULL when < 2 sends.
        when(c_out >= lit(2), out_span_days / (c_out - lit(1)))
        .otherwise(lit(None).cast("double"))
        .alias("avg_gap_days"),
        coalesce(col("distinct_counterparties_out"), lit(0)).alias("distinct_counterparties_out"),
        coalesce(col("distinct_counterparties_in"), lit(0)).alias("distinct_counterparties_in"),
        # pass-through ratio = sent / received. A receive-only "hoarder"
        # (received > 0, sent NULL) gets 0.0, a real low baseline -- so W4 can
        # flag the mule-onboarding case (historically hoards, then suddenly
        # forwards) as a deviation from ~0. NULL only when the entity NEVER
        # received (ratio genuinely undefined; a pure originator is not a
        # pass-through subject).
        when(
            coalesce(col("total_received_usd"), lit(0)) > lit(0),
            coalesce(col("total_sent_usd"), lit(0)).cast("double")
            / col("total_received_usd").cast("double"),
        )
        .otherwise(lit(None).cast("double"))
        .alias("passthrough_ratio"),
        # I1 (silver-plan): the anchor is the resolved data clock, not
        # ``current_timestamp()``. A wall-clock stamp makes every rebuild
        # of a fixed bronze write a different value into this per-entity
        # column, so diffing rebuilds for regression detection is
        # impossible. Cast the date to TIMESTAMP so the column type stays
        # unchanged (the DDL declares it as TIMESTAMP).
        lit(data_clock.isoformat()).cast("timestamp").alias("profile_updated_ts"),
        lit(None).cast("bigint").alias("_batch_id"),
    )


def reference_frames(spark):
    """(party, account) DataFrames, each None when its file is absent. Only a
    missing path is tolerated; any other read error (credentials, S3 outage,
    a corrupt file) raises."""
    frames = []
    for path in (PARTY_PATH, ACCOUNT_PATH):
        try:
            frames.append(spark.read.parquet(path))
        except Exception as e:
            msg = str(e)
            if "PATH_NOT_FOUND" not in msg and "Path does not exist" not in msg:
                raise
            log(f"reference file not found: {msg.splitlines()[0][:200]}")
            frames.append(None)
    return frames[0], frames[1]


# F1: schema-drift check inputs. `build_kyc` reads these columns from the
# reference frames; a silent drop of any one would silently NULL out KYC for
# every customer and every downstream customer-scoped rule. Kept in step with
# the KYC_ENTITY_COLUMNS / KYC_ACCOUNT_COLUMNS constants and `build_kyc`.
_EXPECTED_PARTY_COLUMNS: tuple[str, ...] = ("entity_id", "home_fi") + tuple(
    name for name, _ in KYC_ENTITY_COLUMNS if name != "home_fi"
)
_EXPECTED_ACCOUNT_COLUMNS: tuple[str, ...] = ("iban", "holder_entity_id", "home_fi")


def _assert_reference_schema(df, path, expected):
    """Raise ``SilverAbort`` on KYC schema drift: the reference file exists
    but is missing one or more of the columns silver reads from it."""
    have = set(df.columns)
    missing = [c for c in expected if c not in have]
    if missing:
        raise SilverAbort(f"KYC schema drift: missing column(s) {', '.join(missing)} from {path}")


def _read_reference(spark):
    """(party, account) DataFrames, or (None, None) for a corpus that
    provably predates KYC.

    is_customer defines the monitored population, so silently writing NULL
    KYC would turn every customer-scoped rule into "ran, 0 alerts". Missing
    masters are therefore tolerated only when the corpus manifest is readable
    and every model_version in it is a known pre-KYC one. A missing manifest
    proves nothing (the reference pod writes it too), so it raises, as does
    exactly one master being readable (a pod that died between uploads, or a
    mistyped path).
    """
    party, account = reference_frames(spark)
    if (party is None) != (account is None):
        raise RuntimeError(
            f"only one KYC reference file is readable: party={PARTY_PATH} "
            f"({'missing' if party is None else 'ok'}), account={ACCOUNT_PATH} "
            f"({'missing' if account is None else 'ok'})"
        )
    if party is None and not _corpus_predates_kyc(spark):
        raise RuntimeError(
            f"KYC reference files missing ({PARTY_PATH}, {ACCOUNT_PATH}) and the "
            f"manifest ({MANIFEST_GLOB}) does not show a pre-KYC datagen"
        )
    # F1: schema-drift check. If the reference files are present but a KYC
    # column silently disappears (a datagen bump that drops a field, a
    # migration that never replayed it), build_kyc's guard returned None and
    # the silver run continued with NULL KYC for every entity, which turns
    # every customer-scoped rule into "ran, 0 alerts". Fail loud instead:
    # a schema drift on a present corpus is never "old corpus" and never
    # tolerable.
    if party is not None:
        _assert_reference_schema(party, PARTY_PATH, _EXPECTED_PARTY_COLUMNS)
    if account is not None:
        _assert_reference_schema(account, ACCOUNT_PATH, _EXPECTED_ACCOUNT_COLUMNS)
    return party, account


def predates_kyc(model_version) -> bool:
    """True only for a known datagen model_version from before KYC. Anything
    else (a later version, a future naming scheme, NULL) is not proof."""
    return model_version in PRE_KYC_MODEL_VERSIONS


def _corpus_predates_kyc(spark) -> bool:
    """Whether the corpus manifest proves a pre-KYC datagen: readable, and
    every model_version in it is a known pre-KYC one."""
    try:
        versions = [
            r[0]
            for r in spark.read.parquet(MANIFEST_GLOB).select("model_version").distinct().collect()
        ]
    except Exception as e:
        log(f"manifest unreadable for the KYC version check: {str(e).splitlines()[0][:200]}")
        return False
    return bool(versions) and all(predates_kyc(v) for v in versions)


def main() -> None:
    spark = SparkSession.builder.appName("lb-silver-build-financial").getOrCreate()
    start = time.time()

    # H4 fat-finger guard: refuse to run while an AML stream is active on
    # this deployment. silver_build_financial.main() overwrites every
    # silver table via .overwrite(lit(True)); doing that while
    # silver_stream_financial is writing wipes every streamed row (see the
    # docstring at silver_stream_financial.py:46-53). The stream writes a
    # ``_STARTED`` marker file at its checkpoint on start-up and removes
    # it on clean shutdown; the guard reads it here. LB_FORCE_REBUILD=1
    # bypasses the check for the deliberate rebuild path (job.py already
    # bumps LB_REBUILD_EPOCH before setting this).
    _stream_checkpoint = os.environ.get("LB_FINANCIAL_SILVER_CHECKPOINT")
    _force_rebuild = os.environ.get("LB_FORCE_REBUILD", "0") == "1"
    refuse_batch_while_stream_active(
        spark=spark,
        checkpoint_location=_stream_checkpoint,
        force_rebuild=_force_rebuild,
    )

    # Pin the session timezone to UTC. `to_date(cre_dt_tm)` and `days(...)`
    # partitioning use session tz for the day boundary; a non-UTC executor
    # tz shifts opened_date and partition assignments by up to a day for
    # late-night UTC timestamps, so same-bronze different-cluster runs
    # produce different partition layouts (reproducibility failure).
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Multi-cycle mode contract (LB-121). The orchestrator sets
    # LB_SILVER_INCREMENTAL=true for cycles 2+ of a multi-cycle batch run.
    # Customer 360's silver_build honours that by APPENDING the cycle's
    # bronze read. AML must NOT: its bronze is CUMULATIVE across cycles
    # (the datagen writes every cycle to the same pacs008 prefix, and
    # bronze_verify_financial DROPs and re-registers the whole prefix each
    # cycle), so silver_build already reads the full corpus 1..N. Appending
    # that full read every cycle would duplicate the entire prior corpus,
    # and the derived-state tables (account_statements running balance,
    # entity/account dimensions) cannot be correctly maintained by a naive
    # append anyway. The correct and only-correct behaviour here is a full
    # rebuild from cumulative bronze every cycle -- expensive but exact,
    # and cycle-progression is measured as the rebuild cost growing with
    # the corpus. Do NOT "optimise" this to an append without implementing
    # slice-scoped reads + MERGE on the dimensions + running-balance
    # carry-forward, and validating it on a live multi-cycle run.
    incremental_flag = env("LB_SILVER_INCREMENTAL", "false").lower() == "true"

    log("=" * 60)
    log("Silver Build (Financial)")
    log(f"Strategy: {STRATEGY}")
    if incremental_flag:
        log(
            "Multi-cycle: LB_SILVER_INCREMENTAL=true -- AML does a FULL "
            "rebuild from cumulative bronze (append would double-count; see "
            "the mode-contract note in main())."
        )
    log(f"Session TZ: {spark.conf.get('spark.sql.session.timeZone')}")
    log("=" * 60)

    ensure_namespaces_for_ddl(
        spark,
        CATALOG,
        (
            DDL_TXNS,
            DDL_ENTITIES,
            DDL_ACCOUNTS,
            DDL_STATEMENTS,
            DDL_EDGES,
            DDL_PROFILES,
            DDL_BATCH_VERSIONS,
        ),
    )
    for name, ddl in (
        ("transactions", DDL_TXNS),
        ("entities", DDL_ENTITIES),
        ("accounts", DDL_ACCOUNTS),
        ("account_statements", DDL_STATEMENTS),
        ("edges", DDL_EDGES),
        ("entity_profiles", DDL_PROFILES),
        ("silver_batch_versions", DDL_BATCH_VERSIONS),
    ):
        spark.sql(ddl)
        log(f"Bootstrapped silver.{name}")

    # LB-109 upgrade path: on a reused catalog whose tables predate these
    # columns, CREATE TABLE IF NOT EXISTS above is a no-op and the writes
    # below (which carry them) would fail on a schema mismatch.
    for table in (SILVER_TRANSACTIONS, SILVER_EDGES):
        ensure_column(spark, f"{CATALOG}.{table}", "_batch_id", "BIGINT")
        # B2: _stream_id scopes _batch_id per streaming query. Batch build
        # writes 'batch'; streams write streaming_query_id. Old catalogs
        # predate the column, so add it before the first write below.
        ensure_column(spark, f"{CATALOG}.{table}", "_stream_id", "STRING")
    ensure_column(spark, f"{CATALOG}.{SILVER_TRANSACTIONS}", "ingest_ts", "TIMESTAMP")
    ensure_partition_transform(
        spark, f"{CATALOG}.{SILVER_TRANSACTIONS}", "days(txn_timestamp)", "months(txn_timestamp)"
    )
    ensure_partition_transform(
        spark, f"{CATALOG}.{SILVER_STATEMENTS}", "days(book_ts)", "months(book_ts)"
    )
    for table, columns in (
        (SILVER_ENTITIES, KYC_ENTITY_COLUMNS),
        (SILVER_ACCOUNTS, KYC_ACCOUNT_COLUMNS),
    ):
        for name, sql_type in columns:
            ensure_column(spark, f"{CATALOG}.{table}", name, sql_type.upper())

    bronze = spark.table(f"{CATALOG}.{BRONZE_TABLE}")
    bronze_rows = bronze.count()
    log(f"Read bronze: {CATALOG}.{BRONZE_TABLE} ({bronze_rows:,} rows)")

    # I1 (silver-plan): resolve the data clock once for the run and pass it
    # to build_entity_profiles so ``profile_updated_ts`` is deterministic
    # across rebuilds of the same bronze. AML mains use strict=False (this
    # module does not compute customer_recency_score, so a missing anchor
    # is not a correctness hazard here; C2's env fallback still supplies
    # one for silver jobs). A None fallback would leave ``data_clock`` as
    # None and the .isoformat() call below would crash; use the bronze
    # frame's max(txn_timestamp) day as the safety net.
    from datetime import date as _date

    _resolved = resolve_data_clock(df_fallback=None)
    if _resolved is None:
        # Datagen may not have written a timestamp end, bronze may be a
        # legacy corpus without one -- fall back to today at 00:00 UTC so
        # the timestamp cast succeeds.
        from datetime import datetime as _dt
        from datetime import timezone as _tz

        _resolved = _dt.now(_tz.utc).date()
        log(f"I1: data_clock unresolved; using today={_resolved} for profile_updated_ts")
    assert isinstance(_resolved, _date), _resolved
    data_clock = _resolved

    # NB: `.overwrite(lit(True))` (not `.createOrReplace()`) preserves the
    # table's partition spec and schema. The original design used
    # createOrReplace, which under the DataFrameWriterV2 semantics REPLACES
    # the table -- destroying any PARTITIONED BY established at CREATE and
    # reverting the table to unpartitioned on every silver-build re-run.
    # Downstream Trino/Spark scans then degraded from partition pruning to
    # full scans (per-run silent regression, invisible to unit tests).
    #
    # G4: re-assert TBLPROPERTIES before the overwrite so a table whose
    # properties drifted (in-place ALTER, older CREATE) writes with the
    # DDL's declared retention and codec every cycle (invariant 5). The
    # property set matches ICEBERG_V2_SNAPPY_PROPS_SQL, which every silver
    # DDL above sets at CREATE.
    def _replace_data(df, table):
        fq = f"{CATALOG}.{table}"
        spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ({ICEBERG_V2_SNAPPY_PROPS_SQL})")
        df.writeTo(fq).overwrite(lit(True))

    # I10: stamp batch-mode rows with (_stream_id='batch', _batch_id=cycle) so
    # the same versions-table semi-join that guards the stream's mid-batch
    # crash window also guards a batch run that crashes between the
    # transactions overwrite and the versions-row insert below. Downstream
    # consumers apply exactly one filter rule (semi-join against
    # silver_batch_versions on _stream_id + _batch_id) across both modes.
    import os as _os

    _cycle = int(_os.environ.get("LB_BRONZE_CYCLE", "0"))
    txns = build_transactions(bronze).withColumn("_batch_id", lit(int(_cycle)).cast("bigint"))
    _replace_data(txns, SILVER_TRANSACTIONS)
    log("Wrote silver.transactions")

    kyc = build_kyc(*_read_reference(spark))
    if kyc is None:
        log(
            "WARNING: no KYC party/account master under "
            f"{PARTY_PATH} / {ACCOUNT_PATH}; silver.entities and silver.accounts "
            "KYC columns are NULL"
        )
    _replace_data(build_entities(txns, bronze, kyc), SILVER_ENTITIES)
    log("Wrote silver.entities")

    # Build silver.accounts first (placeholder current_balance = NULL) so we
    # can read it back as a table for the balance roll-up. This avoids
    # rescanning bronze twice (build_accounts is a bronze->distinct-IBAN pass)
    # and gives update_accounts_balance a durable input independent of cache
    # eviction between the two writes.
    _replace_data(build_accounts(bronze, kyc), SILVER_ACCOUNTS)
    log("Wrote silver.accounts (placeholder current_balance)")

    accounts = spark.table(f"{CATALOG}.{SILVER_ACCOUNTS}")
    statements = build_statements(bronze, accounts)
    _replace_data(statements, SILVER_STATEMENTS)
    log("Wrote silver.account_statements")

    # Read the just-written statements back rather than reusing the cached
    # DataFrame: eviction between writes silently re-triggers the entire
    # window computation, which is the most expensive stage in the job.
    stmts_read = spark.table(f"{CATALOG}.{SILVER_STATEMENTS}")
    _replace_data(update_accounts_balance(accounts, stmts_read), SILVER_ACCOUNTS)
    log("Wrote silver.accounts (current_balance rolled up from statements)")

    # I3: enforce one row per iban post-write. build_accounts uses a
    # row_number filter to pick a single winning row per iban; this
    # assertion catches any regression that reintroduces duplicates
    # (e.g. an overlooked dropDuplicates or a per-column min).
    accounts_final = spark.table(f"{CATALOG}.{SILVER_ACCOUNTS}")
    total_rows = accounts_final.count()
    distinct_ibans = accounts_final.select(col("iban")).distinct().count()
    if total_rows != distinct_ibans:
        raise SilverAbort(
            f"silver.accounts iban uniqueness violated: {total_rows} rows, "
            f"{distinct_ibans} distinct iban values"
        )

    # I10: mirror the batch-mode stamp on counterparty_edges so the same
    # semi-join guards edges as well as transactions.
    _replace_data(
        build_edges(txns).withColumn("_batch_id", lit(int(_cycle)).cast("bigint")),
        SILVER_EDGES,
    )
    log("Wrote silver.counterparty_edges")

    # C-PROFILES (LB-130): per-entity behavioural baseline for relative-anomaly
    # detection (W4/W8 over-firing). Full rebuild from the transaction frame.
    # I1: ``data_clock`` fixes ``profile_updated_ts`` so rebuilds are
    # byte-identical for the same bronze.
    _replace_data(build_entity_profiles(txns, data_clock), SILVER_PROFILES)
    log("Wrote silver.entity_profiles")

    # I10 sealed marker: after every silver table for this cycle is written,
    # seal the ('batch', cycle) row so downstream consumers' semi-join on
    # (_stream_id, _batch_id) sees the cycle. A crash before this row lands
    # leaves the cycle's transactions hidden from gold + score. _cycle was
    # resolved once above and stamped on every batch-written silver row.
    #
    # MERGE (not INSERT): silver_build's rebuild-cycle semantics allow the
    # same cycle to be re-driven (a re-run of the same cycle overwrites the
    # silver tables). A plain INSERT on rerun would leave two rows for the
    # same ('batch', cycle) key, which is harmless for the semi-join but
    # bad for any future COUNT(*) over silver_batch_versions.
    spark.sql(
        f"MERGE INTO {CATALOG}.{SILVER_BATCH_VERSIONS} v "
        f"USING (SELECT 'batch' AS stream_id, "
        f"CAST({int(_cycle)} AS BIGINT) AS batch_id, "
        f"current_timestamp() AS committed_at) s "
        f"ON v.stream_id = s.stream_id AND v.batch_id = s.batch_id "
        f"WHEN NOT MATCHED THEN INSERT *"
    )
    log(f"Wrote silver.silver_batch_versions sealed marker for cycle {_cycle}")

    # Guard against ruff unused-import warnings for symbols kept for clarity.
    _ = (date_format,)

    elapsed = time.time() - start
    log("=" * 60)
    log(f"Silver build complete in {elapsed:.1f}s")
    log("=" * 60)
    _, bronze_gb = iceberg_table_stats(spark, f"{CATALOG}.{BRONZE_TABLE}")
    # A2: per-table row counts so `metrics.json` records what silver-build
    # actually wrote across the six tables, not just silver.transactions.
    # An unmaintained continuous-mode table (D-safe) returns 0 here; the
    # A1 gate reads silver_transactions_rows, not the sum.
    silver_rows, _ = iceberg_table_stats(spark, f"{CATALOG}.{SILVER_TRANSACTIONS}")
    per_table = {
        "silver_transactions_rows": silver_rows,
        "silver_entities_rows": iceberg_table_stats(spark, f"{CATALOG}.{SILVER_ENTITIES}")[0],
        "silver_accounts_rows": iceberg_table_stats(spark, f"{CATALOG}.{SILVER_ACCOUNTS}")[0],
        "silver_statements_rows": iceberg_table_stats(spark, f"{CATALOG}.{SILVER_STATEMENTS}")[0],
        "silver_edges_rows": iceberg_table_stats(spark, f"{CATALOG}.{SILVER_EDGES}")[0],
        "silver_profiles_rows": iceberg_table_stats(spark, f"{CATALOG}.{SILVER_PROFILES}")[0],
    }
    # C2 (silver-plan): record which rung of the resolution ladder produced
    # LB_DATA_CLOCK so metrics.json labels the run.
    _clock_source = env("LB_DATA_CLOCK_SOURCE", "unknown")
    log_job_metrics(
        "silver-build",
        input_size_gb=bronze_gb,
        input_rows=bronze_rows,
        output_rows=silver_rows,
        elapsed_seconds=elapsed,
        data_clock_source=_clock_source,
        **per_table,
    )
    # A1: LB-044 gate; refuse exit-0 if the primary output table is empty.
    # Emitted after metrics so a failed run still leaves the block on stdout.
    assert_progress(silver_rows, "silver-build")
    spark.stop()


if __name__ == "__main__":
    main()
