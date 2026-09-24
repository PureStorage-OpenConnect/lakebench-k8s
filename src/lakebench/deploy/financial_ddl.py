"""Iceberg DDL constants for the Financial (FinServ-Crime, AML) workload.

Bronze schema is a flattened pacs.008 ISO 20022 credit-transfer message
(the SWIFT/T2 wire-payment shape). Silver is the four-table lakehouse
normalisation the workloads (W1-W11) read from. Gold is modelled on the
Databricks Financial Crimes accelerator pattern (open source, cited in
spec §2C.4 NB1) extended with fields the workloads reference and
regulator-facing metadata for FinCEN SAR / EBA STR mapping.

All tables are Iceberg format-version=2, snappy-compressed. Partitioning
choices follow the query patterns in ENG-2C.4:

- Bronze pacs.008: ``days(intr_bk_sttlm_dt)`` -- date-range scans dominate.
- Silver transactions: ``days(txn_timestamp)`` -- W2/W3 motif scans are
  window-bounded; W8 replay walks historical snapshots.
- Silver counterparty_edges: ``bucket(64, source_entity_id)`` -- keeps
  per-entity edge fan-in reads local.
- Silver entities/accounts: unpartitioned; low cardinality vs facts.
- Gold: unpartitioned; per-day rollup tables are small enough to scan.

The DDL strings are parameterised with ``{catalog}`` and ``{table}``
placeholders that the deployer templates from
:class:`~lakebench.config.schema.TableNamesConfig`. This module does not
execute DDL; ``bronze_verify_financial.py`` and ``silver_build_financial.py``
render and submit them at pipeline-init time.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

BRONZE_PACS008_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
    msg_id                       STRING NOT NULL,
    cre_dt_tm                    TIMESTAMP NOT NULL,
    nb_of_txs                    INT NOT NULL,
    ctrl_sum                     DECIMAL(18, 5),
    ttl_intr_bk_sttlm_amt        DECIMAL(18, 5),
    intr_bk_sttlm_dt             DATE,
    sttlm_inf                    STRUCT<sttlm_mtd: STRING>,
    pmt_tp_inf                   STRUCT<
                                     instr_prty: STRING,
                                     clr_chanl:  STRING,
                                     svc_lvl:    STRING,
                                     lcl_instrm: STRING,
                                     ctgy_purp:  STRING
                                 >,
    instg_agt                    STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    instd_agt                    STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    -- Per-transaction fields flattened for analytical use
    txn_id                       STRING NOT NULL,
    instr_id                     STRING,
    end_to_end_id                STRING NOT NULL,
    uetr                         STRING NOT NULL,
    clr_sys_ref                  STRING,
    intr_bk_sttlm_amt            DECIMAL(18, 5) NOT NULL,
    intr_bk_sttlm_ccy            STRING NOT NULL,
    instd_amt                    DECIMAL(18, 5),
    instd_ccy                    STRING,
    xchg_rate                    DECIMAL(11, 10),
    chrg_br                      STRING,
    -- Correspondent chain
    intrmy_agt_1                 STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    intrmy_agt_2                 STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    intrmy_agt_3                 STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    prvs_instg_agt_1             STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    prvs_instg_agt_2             STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    prvs_instg_agt_3             STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    -- Party chain
    ultmt_dbtr                   STRUCT<nm: STRING, lei: STRING, ctry: STRING>,
    initg_pty                    STRUCT<nm: STRING, lei: STRING>,
    dbtr                         STRUCT<
                                     nm: STRING,
                                     pstl_adr: STRUCT<strt_nm: STRING, twn_nm: STRING, ctry: STRING>,
                                     id: STRUCT<any_bic: STRING, lei: STRING>,
                                     ctry_of_res: STRING
                                 >,
    dbtr_acct                    STRUCT<iban: STRING, othr: STRING, ccy: STRING>,
    dbtr_agt                     STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    cdtr_agt                     STRUCT<bicfi: STRING, lei: STRING, nm: STRING>,
    cdtr                         STRUCT<
                                     nm: STRING,
                                     pstl_adr: STRUCT<strt_nm: STRING, twn_nm: STRING, ctry: STRING>,
                                     id: STRUCT<any_bic: STRING, lei: STRING>,
                                     ctry_of_res: STRING
                                 >,
    cdtr_acct                    STRUCT<iban: STRING, othr: STRING, ccy: STRING>,
    ultmt_cdtr                   STRUCT<nm: STRING, lei: STRING, ctry: STRING>,
    -- Purpose and reporting
    purp_cd                      STRING,
    purp_prtry                   STRING,
    rgltry_rptg                  ARRAY<STRUCT<
                                     dbt_cdt_rptg_ind: STRING,
                                     authrty_nm:       STRING,
                                     authrty_ctry:     STRING,
                                     details:          ARRAY<STRING>
                                 >>,
    -- Remittance
    rmt_inf_ustrd                ARRAY<STRING>,
    rmt_inf_strd                 ARRAY<STRUCT<ref_doc: STRING, amt: DECIMAL(18, 5)>>
)
USING iceberg
PARTITIONED BY (days(intr_bk_sttlm_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

SILVER_TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
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
    txn_type                STRING NOT NULL,          -- wire, ach, rtp, internal, card
    purpose_code            STRING,
    correspondent_chain     ARRAY<STRING>,
    cross_border            BOOLEAN NOT NULL,
    regulatory_reported     BOOLEAN NOT NULL,
    rptd_originator_name    STRING,
    rptd_originator_address STRING,
    rptd_beneficiary_name   STRING,
    rptd_beneficiary_address STRING,
    source_message_ref      STRING,
    _batch_id               BIGINT
)
USING iceberg
PARTITIONED BY (days(txn_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()


SILVER_ENTITIES_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
    entity_id           BIGINT NOT NULL,
    entity_type         STRING NOT NULL,          -- Person, Company, FI
    name                STRING NOT NULL,
    legal_name          STRING,
    address             STRUCT<
                            street:   STRING,
                            town:     STRING,
                            region:   STRING,
                            postcode: STRING,
                            country:  STRING
                        >,
    email_addr          STRING,
    phone_number        STRING,
    country             STRING,
    lei                 STRING,
    bic                 STRING,
    sanctions_status    STRING,                   -- clear, sdn, warn, pep, NULL
    pep_status          BOOLEAN,
    initial_risk_score  DOUBLE
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()


SILVER_ACCOUNTS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
    account_id         BIGINT NOT NULL,
    iban               STRING,
    holder_entity_id   BIGINT NOT NULL,
    bank_bic           STRING NOT NULL,
    currency           STRING NOT NULL,
    opened_date        DATE   NOT NULL,
    closed_date        DATE,
    current_balance    DECIMAL(38, 2)
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()


SILVER_ACCOUNT_STATEMENTS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
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
    bk_tx_cd       STRING NOT NULL
)
USING iceberg
PARTITIONED BY (days(book_ts), bucket(64, account_id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()
# bal_before / bal_after are DECIMAL(38,2), not (18,2). Spark widens
# SUM(decimal(18,2)) OVER (..) to decimal(38,2); casting the running sum back
# to (18,2) silently returns NULL on overflow (ANSI off by default), which
# then violates the NOT NULL constraint and fails the Iceberg write partway
# through a multi-hour job. Peak balance at scale 100 on a correspondent /
# aggregator account can plausibly exceed 10^16, so the truncation was a
# real hazard. Storing the wider type is cheap (a few extra bytes per row on
# a table that already carries billions of rows).


SILVER_COUNTERPARTY_EDGES_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
    source_entity_id       BIGINT NOT NULL,
    target_entity_id       BIGINT NOT NULL,
    first_seen_ts          TIMESTAMP NOT NULL,
    last_seen_ts           TIMESTAMP NOT NULL,
    cumulative_amount_usd  DECIMAL(38, 2) NOT NULL,
    txn_count              BIGINT NOT NULL,
    _batch_id              BIGINT
)
USING iceberg
PARTITIONED BY (bucket(64, source_entity_id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()
# LB-109: `_batch_id` mirrors silver_build_financial's DDL so
# silver_stream's DELETE+append idempotency protocol works against the
# tables the deployer creates. `cumulative_amount_usd` widened to
# decimal(38, 2) to match silver_build's schema; the previous (18, 2)
# would NULL-overflow the moment an aggregator entity crossed 10^16
# USD (already possible at scale 100+).


# Entity profiles: rolling per-entity behavioural baseline (C-PROFILES). One row
# per entity_id, aggregating both sides (as originator and as beneficiary) so a
# detection rule can ask "is this behaviour anomalous FOR THIS ENTITY" instead of
# applying a population-wide absolute threshold. This is the enabler for reducing
# W4/W8 over-firing (LB-130): W8 compares a reactivation gap against the entity's
# own typical inter-transaction gap; W4 compares pass-through behaviour against
# the entity's own baseline (a payment intermediary that always forwards funds is
# not anomalous). Full-rebuild in batch; MERGE in continuous.
SILVER_ENTITY_PROFILES_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
    entity_id                  BIGINT NOT NULL,
    first_seen_ts              TIMESTAMP,
    last_seen_ts               TIMESTAMP,
    active_span_days           DOUBLE,
    txn_count_out              BIGINT NOT NULL,
    txn_count_in               BIGINT NOT NULL,
    txn_count_total            BIGINT NOT NULL,
    total_sent_usd             DECIMAL(38, 2),
    total_received_usd         DECIMAL(38, 2),
    avg_amount_usd             DOUBLE,
    stddev_amount_usd          DOUBLE,
    avg_gap_days               DOUBLE,
    distinct_counterparties_out BIGINT NOT NULL,
    distinct_counterparties_in  BIGINT NOT NULL,
    passthrough_ratio          DOUBLE,
    profile_updated_ts         TIMESTAMP,
    _batch_id                  BIGINT
)
USING iceberg
PARTITIONED BY (bucket(64, entity_id))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()
# Sizing notes:
# * total_*_usd are decimal(38, 2) (aggregator entities cross 10^16 USD at
#   scale 100+, same reason SILVER_COUNTERPARTY_EDGES widened).
# * avg_gap_days = originator_span / (txn_count_out - 1), where originator_span
#   is (last send - first send) on the ORIGINATOR side only (NOT active_span_days,
#   which spans both sides). This is exactly the baseline W8 needs to judge whether
#   a >=90-day reactivation gap is anomalous for the entity. NULL when
#   txn_count_out < 2 (no gap defined; W8 already declines first-ever activity).
# * passthrough_ratio = total_sent_usd / total_received_usd -- W4's baseline for
#   "does this entity normally forward what it receives". A receive-only entity
#   gets 0.0 (a real low baseline, so a sudden forward is a deviation); NULL only
#   when the entity never received (ratio undefined).
# * _batch_id mirrors the other silver tables for the continuous DELETE+append
#   idempotency protocol.


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------

# Alerts: one row per detection firing. Modelled on the Databricks Financial
# Crimes accelerator alerts pattern; extended with the run_id every workload
# writes (W9) and evidence / narrative columns for the investigator queries
# (W6, W7). Nullable narrative + evidence lets v1 ship without them; they
# are populated as the investigator layer matures.
GOLD_ALERTS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
    alert_id           STRING NOT NULL,          -- UUID
    rule_id            STRING NOT NULL,          -- W2_structuring, W3_round_tripping, ...
    rule_version       STRING NOT NULL,
    model_id           STRING NOT NULL,          -- 'rule' for rule-based, or model name
    model_version      STRING NOT NULL,
    entity_id          BIGINT NOT NULL,          -- primary alerted entity
    related_txn_ids    ARRAY<STRING>,            -- transactions triggering the alert
    related_entity_ids ARRAY<BIGINT>,            -- other entities in the pattern
    alert_ts           TIMESTAMP NOT NULL,
    alert_score        DOUBLE,                   -- normalised 0.0-1.0
    priority           STRING,                   -- low, medium, high, critical
    status             STRING,                   -- new, under_review, escalated, sar_filed, closed
    disposition        STRING,                   -- true_positive, false_positive, inconclusive
    alert_type         STRING,                   -- typology label (structuring, cycle, fan_in, ...)
    run_id             STRING NOT NULL,          -- lakebench execution id
    narrative          STRING,                   -- regulator-facing summary (optional in v1)
    evidence           MAP<STRING, STRING>,      -- rule-specific evidence pointers
    detected_ts        TIMESTAMP                 -- LB-125: wall-clock at rule execution (freshness/TTD)
)
USING iceberg
PARTITIONED BY (days(alert_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()


# Risk scores: one row per (entity, model). W9 writeback MERGEs into this
# table on (entity_id, model_id). Version history is preserved via Iceberg
# snapshots (time-travel), not a separate history table -- consistent with
# W10 reproduction requirements.
GOLD_RISK_SCORES_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
    entity_id              BIGINT NOT NULL,
    model_id               STRING NOT NULL,          -- 'rule', 'pregel_v1', 'splink_v1', ...
    model_version          STRING NOT NULL,
    risk_score             DOUBLE NOT NULL,          -- normalised 0.0-1.0
    risk_tier              STRING NOT NULL,          -- low, medium, high, critical
    contributing_rule_ids  ARRAY<STRING>,            -- which rules pushed this score
    computed_ts            TIMESTAMP NOT NULL,
    run_id                 STRING NOT NULL
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()


# Entity clusters: output of W1 (Connected Components synthetic-identity
# detection). One row per detected cluster. Consumed by the investigator
# queries (W6/W7) and by downstream case-management systems.
GOLD_ENTITY_CLUSTERS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
    cluster_id            STRING NOT NULL,          -- UUID
    detection_run_id      STRING NOT NULL,
    detection_algorithm   STRING NOT NULL,          -- connected_components, louvain, ...
    member_entity_ids     ARRAY<BIGINT> NOT NULL,
    cluster_size          INT NOT NULL,
    first_seen_ts         TIMESTAMP NOT NULL,
    detected_ts           TIMESTAMP NOT NULL,
    suspicion_score       DOUBLE,                   -- 0.0-1.0
    cluster_type          STRING                    -- synthetic_id, shell_network, ...
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()


# Daily dashboards: pre-aggregated rollups for the exec dashboard. One row
# per (day, rule_id, disposition). Small table (thousands of rows per year);
# unpartitioned.
GOLD_DAILY_DASHBOARDS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{table} (
    dashboard_date        DATE NOT NULL,
    rule_id               STRING NOT NULL,
    disposition           STRING,                   -- null = all
    priority              STRING,                   -- null = all
    alert_count           BIGINT NOT NULL,
    entity_count          BIGINT NOT NULL,
    total_alerted_amount_usd DECIMAL(20, 2),
    tp_count              BIGINT,                   -- when ground truth known
    fp_count              BIGINT,
    recall                DOUBLE,                   -- when ground truth known
    precision_val         DOUBLE,                   -- when ground truth known (avoids SQL keyword)
    computed_ts           TIMESTAMP NOT NULL,
    run_id                STRING NOT NULL
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'snappy'
)
""".strip()


# ---------------------------------------------------------------------------
# Table -> DDL registry
# ---------------------------------------------------------------------------

# Maps ``TableNamesConfig`` attribute name to its DDL. Consumed by
# ``bronze_verify_financial.py`` (bronze) and ``silver_build_financial.py``
# (silver + gold) to create-if-not-exists at pipeline init.
FINANCIAL_TABLE_DDLS: dict[str, str] = {
    "bronze": BRONZE_PACS008_DDL,
    "silver": SILVER_TRANSACTIONS_DDL,
    "silver_entities": SILVER_ENTITIES_DDL,
    "silver_accounts": SILVER_ACCOUNTS_DDL,
    "silver_account_statements": SILVER_ACCOUNT_STATEMENTS_DDL,
    "silver_counterparty_edges": SILVER_COUNTERPARTY_EDGES_DDL,
    "silver_entity_profiles": SILVER_ENTITY_PROFILES_DDL,
    "gold_alerts": GOLD_ALERTS_DDL,
    "gold_risk_scores": GOLD_RISK_SCORES_DDL,
    "gold_entity_clusters": GOLD_ENTITY_CLUSTERS_DDL,
    "gold_daily_dashboards": GOLD_DAILY_DASHBOARDS_DDL,
}


def render_ddl(ddl_template: str, catalog: str, table: str) -> str:
    """Substitute the ``{catalog}`` and ``{table}`` placeholders in a DDL string.

    Kept as a distinct helper so tests can render without importing Spark.
    """
    return ddl_template.format(catalog=catalog, table=table)
