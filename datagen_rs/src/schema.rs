//! pacs.008 Arrow schema, field-for-field identical to datagen_v2/schema.py.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

pub fn agent_fields() -> Fields {
    Fields::from(vec![
        Field::new("bicfi", DataType::Utf8, true),
        Field::new("lei", DataType::Utf8, true),
        Field::new("nm", DataType::Utf8, true),
    ])
}

fn addr_fields() -> Fields {
    Fields::from(vec![
        Field::new("strt_nm", DataType::Utf8, true),
        Field::new("twn_nm", DataType::Utf8, true),
        Field::new("ctry", DataType::Utf8, true),
    ])
}

fn party_id_fields() -> Fields {
    Fields::from(vec![
        Field::new("any_bic", DataType::Utf8, true),
        Field::new("lei", DataType::Utf8, true),
    ])
}

pub fn party_fields() -> Fields {
    Fields::from(vec![
        Field::new("nm", DataType::Utf8, true),
        Field::new("pstl_adr", DataType::Struct(addr_fields()), true),
        Field::new("id", DataType::Struct(party_id_fields()), true),
        Field::new("ctry_of_res", DataType::Utf8, true),
    ])
}

pub fn acct_fields() -> Fields {
    Fields::from(vec![
        Field::new("iban", DataType::Utf8, true),
        Field::new("othr", DataType::Utf8, true),
        Field::new("ccy", DataType::Utf8, true),
    ])
}

pub fn ultmt_fields() -> Fields {
    Fields::from(vec![
        Field::new("nm", DataType::Utf8, true),
        Field::new("lei", DataType::Utf8, true),
        Field::new("ctry", DataType::Utf8, true),
    ])
}

pub fn initg_fields() -> Fields {
    Fields::from(vec![
        Field::new("nm", DataType::Utf8, true),
        Field::new("lei", DataType::Utf8, true),
    ])
}

pub fn rgltry_detail_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Utf8, true))
}

pub fn rgltry_item_fields() -> Fields {
    Fields::from(vec![
        Field::new("dbt_cdt_rptg_ind", DataType::Utf8, true),
        Field::new("authrty_nm", DataType::Utf8, true),
        Field::new("authrty_ctry", DataType::Utf8, true),
        Field::new(
            "details",
            DataType::List(rgltry_detail_field()),
            true,
        ),
    ])
}

pub fn rmt_strd_fields() -> Fields {
    Fields::from(vec![
        Field::new("ref_doc", DataType::Utf8, true),
        Field::new("amt", DataType::Decimal128(18, 5), true),
    ])
}

fn dec18() -> DataType {
    DataType::Decimal128(18, 5)
}

pub fn pacs008_schema() -> SchemaRef {
    let agent = || DataType::Struct(agent_fields());
    let ultmt = || DataType::Struct(ultmt_fields());
    Arc::new(Schema::new(vec![
        Field::new("msg_id", DataType::Utf8, true),
        Field::new("cre_dt_tm", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("nb_of_txs", DataType::Int32, true),
        Field::new("ctrl_sum", dec18(), true),
        Field::new("ttl_intr_bk_sttlm_amt", dec18(), true),
        Field::new("intr_bk_sttlm_dt", DataType::Date32, true),
        Field::new(
            "sttlm_inf",
            DataType::Struct(Fields::from(vec![Field::new("sttlm_mtd", DataType::Utf8, true)])),
            true,
        ),
        Field::new(
            "pmt_tp_inf",
            DataType::Struct(Fields::from(vec![
                Field::new("instr_prty", DataType::Utf8, true),
                Field::new("clr_chanl", DataType::Utf8, true),
                Field::new("svc_lvl", DataType::Utf8, true),
                Field::new("lcl_instrm", DataType::Utf8, true),
                Field::new("ctgy_purp", DataType::Utf8, true),
            ])),
            true,
        ),
        Field::new("instg_agt", agent(), true),
        Field::new("instd_agt", agent(), true),
        Field::new("txn_id", DataType::Utf8, true),
        Field::new("instr_id", DataType::Utf8, true),
        Field::new("end_to_end_id", DataType::Utf8, true),
        Field::new("uetr", DataType::Utf8, true),
        Field::new("clr_sys_ref", DataType::Utf8, true),
        Field::new("intr_bk_sttlm_amt", dec18(), true),
        Field::new("intr_bk_sttlm_ccy", DataType::Utf8, true),
        Field::new("instd_amt", dec18(), true),
        Field::new("instd_ccy", DataType::Utf8, true),
        Field::new("xchg_rate", DataType::Decimal128(11, 10), true),
        Field::new("chrg_br", DataType::Utf8, true),
        Field::new("intrmy_agt_1", agent(), true),
        Field::new("intrmy_agt_2", agent(), true),
        Field::new("intrmy_agt_3", agent(), true),
        Field::new("prvs_instg_agt_1", agent(), true),
        Field::new("prvs_instg_agt_2", agent(), true),
        Field::new("prvs_instg_agt_3", agent(), true),
        Field::new("ultmt_dbtr", ultmt(), true),
        Field::new("initg_pty", DataType::Struct(initg_fields()), true),
        Field::new("dbtr", DataType::Struct(party_fields()), true),
        Field::new("dbtr_acct", DataType::Struct(acct_fields()), true),
        Field::new("dbtr_agt", agent(), true),
        Field::new("cdtr_agt", agent(), true),
        Field::new("cdtr", DataType::Struct(party_fields()), true),
        Field::new("cdtr_acct", DataType::Struct(acct_fields()), true),
        Field::new("ultmt_cdtr", ultmt(), true),
        Field::new("purp_cd", DataType::Utf8, true),
        Field::new("purp_prtry", DataType::Utf8, true),
        Field::new(
            "rgltry_rptg",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(rgltry_item_fields()),
                true,
            ))),
            true,
        ),
        Field::new(
            "rmt_inf_ustrd",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "rmt_inf_strd",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(rmt_strd_fields()),
                true,
            ))),
            true,
        ),
    ]))
}
