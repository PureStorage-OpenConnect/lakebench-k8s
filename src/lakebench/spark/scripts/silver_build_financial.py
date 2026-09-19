"""Silver Build (Financial) -- normalise bronze pacs.008 into the 5 silver tables.

Output tables (DDL in src/lakebench/deploy/financial_ddl.py):
- silver.transactions          -- flat transaction facts, partitioned by day
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

import time

from common import env, log
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
    date_format,
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
SILVER_STATEMENTS = env("LB_FINANCIAL_SILVER_STATEMENTS", "silver.account_statements")
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
    current_balance    DECIMAL(38, 2)
) USING iceberg
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
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
) USING iceberg PARTITIONED BY (days(book_ts), bucket(64, account_id))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""

DDL_EDGES = f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_EDGES} (
    source_entity_id       BIGINT NOT NULL,
    target_entity_id       BIGINT NOT NULL,
    first_seen_ts          TIMESTAMP NOT NULL,
    last_seen_ts           TIMESTAMP NOT NULL,
    cumulative_amount_usd  DECIMAL(38, 2) NOT NULL,
    txn_count              BIGINT NOT NULL
) USING iceberg PARTITIONED BY (bucket(64, source_entity_id))
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'snappy')
"""


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _entity_id_from(name_col, country_col, city_col=None):
    """Deterministic BIGINT entity_id from (uppercased trimmed name, country,
    optional city). Adding city reduces the collision rate for common names
    ("HSBC BANK PLC" in "GB", "MOHAMMED ALI" in "PK") from what
    name+country alone can achieve. Even so, two real distinct entities with
    identical name+country+city will still merge -- for a real system, LEI/
    BIC/DOB would be additional keys, but pacs.008 dbtr/cdtr do not always
    carry them. Splink (W5) is the intended resolution layer for that.

    Each column is coalesced to "" BEFORE concat_ws because concat_ws
    silently skips NULL columns rather than emitting a delimiter -- so
    without the coalesce, a NULL city collides with a missing city
    segment and NULL country collides with a missing country segment,
    letting anonymous entities collapse across otherwise-distinct rows.
    """
    if city_col is None:
        city_col = lit("")
    key = concat_ws(
        "|",
        coalesce(upper(trim(name_col)), lit("")),
        coalesce(country_col, lit("")),
        coalesce(upper(trim(city_col)), lit("")),
    )
    return xxhash64(key)


def build_transactions(bronze):
    """Flatten pacs.008 -> silver.transactions shape.

    Notes on approximations:
    - `txn_amount_usd` uses `xchg_rate` as if it converts settlement currency
      to USD. In real pacs.008, xchg_rate is the settlement<->instructed rate
      and may point at any reference currency. A NULL xchg_rate coalesces to
      1.0 so downstream sums don't NULL-propagate; where currency == USD the
      value is exact, otherwise it is an approximation good enough for
      distribution-band scoring but NOT for real settlement.
    - `cross_border` defaults to False when either country column is NULL,
      matching the DDL NOT NULL constraint (a NULL country is more likely a
      data-quality issue than a signal of cross-border-ness).
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
            col("dbtr.nm"), col("dbtr.ctry_of_res"), col("dbtr.pstl_adr.twn_nm"),
        ).alias("originator_id"),
        _entity_id_from(
            col("cdtr.nm"), col("cdtr.ctry_of_res"), col("cdtr.pstl_adr.twn_nm"),
        ).alias("beneficiary_id"),
        col("dbtr_agt.bicfi").alias("originator_bank_bic"),
        col("cdtr_agt.bicfi").alias("beneficiary_bank_bic"),
        col("intr_bk_sttlm_amt").cast("decimal(18,2)").alias("txn_amount"),
        col("intr_bk_sttlm_ccy").alias("txn_currency"),
        (col("intr_bk_sttlm_amt") * coalesce(col("xchg_rate"), lit(1.0)))
            .cast("decimal(18,2)").alias("txn_amount_usd"),
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
        coalesce(
            col("dbtr.ctry_of_res") != col("cdtr.ctry_of_res"),
            lit(False),
        ).alias("cross_border"),
        (size(coalesce(col("rgltry_rptg"), array())) > 0).alias("regulatory_reported"),
        col("dbtr.nm").alias("rptd_originator_name"),
        col("dbtr.pstl_adr.strt_nm").alias("rptd_originator_address"),
        col("cdtr.nm").alias("rptd_beneficiary_name"),
        col("cdtr.pstl_adr.strt_nm").alias("rptd_beneficiary_address"),
        col("msg_id").alias("source_message_ref"),
    )


def build_entities(txns_df):
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

    picked = (
        orig.unionByName(bene)
        .groupBy("entity_id")
        .agg(_min("name").alias("_min_name"))
    )
    # Coalesce to an explicit "UNKNOWN" so an entity whose reported name is
    # NULL for every occurrence (plausible when a party name field is
    # missing) doesn't violate silver.entities.name NOT NULL. Emitting
    # "UNKNOWN" here is loud in a downstream dashboard; a NULL would
    # crash the write with a delayed error.
    picked = picked.withColumn("name", coalesce(col("_min_name"), lit("UNKNOWN"))).drop("_min_name")
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
        lit(None).cast("string").alias("country"),
        lit(None).cast("string").alias("lei"),
        lit(None).cast("string").alias("bic"),
        lit("clear").alias("sanctions_status"),
        lit(False).alias("pep_status"),
        lit(0.0).alias("initial_risk_score"),
    )


def build_accounts(bronze):
    """Distinct IBAN -> holder_entity from the pacs.008 payload.

    Fixes LB-104-shape non-determinism: an IBAN that appears both as a
    debtor account (with dbtr's entity as holder) and as a creditor account
    (with cdtr's entity as holder) previously had holder_entity_id chosen
    coin-flip by dropDuplicates. Now: group by iban and take the min
    holder_entity_id (deterministic across runs). This still doesn't tell
    us WHICH entity really holds the account -- pacs.008 does not carry
    that -- but at least the assignment is stable.
    """
    dbtr = bronze.select(
        col("dbtr_acct.iban").alias("iban"),
        _entity_id_from(
            col("dbtr.nm"), col("dbtr.ctry_of_res"), col("dbtr.pstl_adr.twn_nm"),
        ).alias("holder_entity_id"),
        col("dbtr_agt.bicfi").alias("bank_bic"),
        col("dbtr_acct.ccy").alias("currency"),
        to_date(col("cre_dt_tm")).alias("opened_date"),
    )
    cdtr = bronze.select(
        col("cdtr_acct.iban").alias("iban"),
        _entity_id_from(
            col("cdtr.nm"), col("cdtr.ctry_of_res"), col("cdtr.pstl_adr.twn_nm"),
        ).alias("holder_entity_id"),
        col("cdtr_agt.bicfi").alias("bank_bic"),
        col("cdtr_acct.ccy").alias("currency"),
        to_date(col("cre_dt_tm")).alias("opened_date"),
    )
    from pyspark.sql.functions import min as _min

    all_accts = (
        dbtr.unionByName(cdtr)
        .filter(col("iban").isNotNull())
        .groupBy("iban")
        .agg(
            _min("holder_entity_id").alias("holder_entity_id"),
            _min("bank_bic").alias("bank_bic"),
            _min("currency").alias("currency"),
            _min("opened_date").alias("opened_date"),
        )
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
    dbit = bronze.select(col("dbtr_acct.iban").alias("iban"), lit("DBIT").alias("cdt_dbt_ind"), *common_cols)
    crdt = bronze.select(col("cdtr_acct.iban").alias("iban"), lit("CRDT").alias("cdt_dbt_ind"), *common_cols)
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
        (((abs_(col("account_id")) % lit(200_000)) + lit(10_000))
            .cast("decimal(18,2)")).alias("opening_balance"),
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
        )
    )


def main() -> None:
    spark = SparkSession.builder.appName("lb-silver-build-financial").getOrCreate()
    start = time.time()

    # Pin the session timezone to UTC. `to_date(cre_dt_tm)` and `days(...)`
    # partitioning use session tz for the day boundary; a non-UTC executor
    # tz shifts opened_date and partition assignments by up to a day for
    # late-night UTC timestamps, so same-bronze different-cluster runs
    # produce different partition layouts (reproducibility failure).
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    log("=" * 60)
    log("Silver Build (Financial)")
    log(f"Strategy: {STRATEGY}")
    log(f"Session TZ: {spark.conf.get('spark.sql.session.timeZone')}")
    log("=" * 60)

    for name, ddl in (
        ("transactions", DDL_TXNS),
        ("entities", DDL_ENTITIES),
        ("accounts", DDL_ACCOUNTS),
        ("account_statements", DDL_STATEMENTS),
        ("edges", DDL_EDGES),
    ):
        spark.sql(ddl)
        log(f"Bootstrapped silver.{name}")

    bronze = spark.table(f"{CATALOG}.{BRONZE_TABLE}")
    log(f"Read bronze: {CATALOG}.{BRONZE_TABLE} ({bronze.count():,} rows)")

    # NB: `.overwrite(lit(True))` (not `.createOrReplace()`) preserves the
    # table's partition spec and schema. The original design used
    # createOrReplace, which under the DataFrameWriterV2 semantics REPLACES
    # the table -- destroying any PARTITIONED BY established at CREATE and
    # reverting the table to unpartitioned on every silver-build re-run.
    # Downstream Trino/Spark scans then degraded from partition pruning to
    # full scans (per-run silent regression, invisible to unit tests).
    def _replace_data(df, table):
        df.writeTo(f"{CATALOG}.{table}").overwrite(lit(True))

    txns = build_transactions(bronze)
    _replace_data(txns, SILVER_TRANSACTIONS)
    log("Wrote silver.transactions")

    _replace_data(build_entities(txns), SILVER_ENTITIES)
    log("Wrote silver.entities")

    # Build silver.accounts first (placeholder current_balance = NULL) so we
    # can read it back as a table for the balance roll-up. This avoids
    # rescanning bronze twice (build_accounts is a bronze->distinct-IBAN pass)
    # and gives update_accounts_balance a durable input independent of cache
    # eviction between the two writes.
    _replace_data(build_accounts(bronze), SILVER_ACCOUNTS)
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

    _replace_data(build_edges(txns), SILVER_EDGES)
    log("Wrote silver.counterparty_edges")

    # Guard against ruff unused-import warnings for symbols kept for clarity.
    _ = (date_format,)

    log("=" * 60)
    log(f"Silver build complete in {time.time() - start:.1f}s")
    log("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
