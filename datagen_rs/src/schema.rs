//! Arrow schemas for the datagen output. Two schemas live here:
//!   - `pacs008_schema()`: the financial ISO20022 payment message schema
//!   - `customer360_schema()`: the customer-interaction event schema
//!
//! Both are field-for-field identical to their Python counterparts
//! (datagen_v2/schema.py for pacs008, datagen/generate.py:883-933 for c360)
//! so Parquet consumers can read either image's output interchangeably.

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
        Field::new("details", DataType::List(rgltry_detail_field()), true),
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
        Field::new(
            "cre_dt_tm",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("nb_of_txs", DataType::Int32, true),
        Field::new("ctrl_sum", dec18(), true),
        Field::new("ttl_intr_bk_sttlm_amt", dec18(), true),
        Field::new("intr_bk_sttlm_dt", DataType::Date32, true),
        Field::new(
            "sttlm_inf",
            DataType::Struct(Fields::from(vec![Field::new(
                "sttlm_mtd",
                DataType::Utf8,
                true,
            )])),
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

/// customer360 Arrow schema. 41 columns matching `datagen/generate.py:883-933`.
///
/// All fields are nullable to match Python's `pa.schema` default. The realism
/// helpers in the emit module null out specific columns for specific rows
/// (login/support -> product columns null; store/call_center -> device columns
/// null; non-members -> loyalty_tier null; etc). Non-member columns like `id`,
/// `event_timestamp`, `customer_id` are never nulled by the emit path even
/// though the schema permits it -- keeping them nullable here matches the
/// Python schema so Parquet stats + downstream readers behave identically.
///
/// The event_timestamp carries an explicit "UTC" tz string. Naive
/// `Timestamp(Microsecond, None)` silently drops timezone metadata that
/// downstream engines then interpret as local time, breaking the (seed,
/// file_id) -> same-content contract when pods run under different TZ envs
/// (see the corresponding note at `datagen/generate.py:887-892`).
pub fn customer360_schema() -> SchemaRef {
    let utf8 = || DataType::Utf8;
    let i32t = || DataType::Int32;
    let i64t = || DataType::Int64;
    let f64t = || DataType::Float64;
    let boolt = || DataType::Boolean;
    let ts_utc = || DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
    Arc::new(Schema::new(vec![
        Field::new("id", i64t(), true),
        Field::new("row_id", i64t(), true),
        Field::new("event_timestamp", ts_utc(), true),
        Field::new("event_id", utf8(), true),
        Field::new("session_id", utf8(), true),
        Field::new("customer_id", i64t(), true),
        Field::new("email_raw", utf8(), true),
        Field::new("phone_raw", utf8(), true),
        Field::new("interaction_type", utf8(), true),
        Field::new("product_id", utf8(), true),
        Field::new("product_category", utf8(), true),
        Field::new("transaction_amount", f64t(), true),
        Field::new("currency", utf8(), true),
        Field::new("channel", utf8(), true),
        Field::new("device_type", utf8(), true),
        Field::new("browser", utf8(), true),
        Field::new("ip_address", utf8(), true),
        Field::new("city_raw", utf8(), true),
        Field::new("state_raw", utf8(), true),
        Field::new("zip_code", utf8(), true),
        Field::new("page_views", i32t(), true),
        Field::new("time_on_site_seconds", i32t(), true),
        Field::new("bounce_rate", f64t(), true),
        Field::new("click_count", i32t(), true),
        Field::new("cart_value", f64t(), true),
        Field::new("items_in_cart", i32t(), true),
        Field::new("support_ticket_id", utf8(), true),
        Field::new("issue_category", utf8(), true),
        Field::new("satisfaction_score", i32t(), true),
        Field::new("campaign_id", utf8(), true),
        Field::new("utm_source", utf8(), true),
        Field::new("utm_medium", utf8(), true),
        Field::new("loyalty_member", boolt(), true),
        Field::new("loyalty_tier", utf8(), true),
        Field::new("points_earned", i32t(), true),
        Field::new("points_redeemed", i32t(), true),
        Field::new("data_source", utf8(), true),
        Field::new("data_quality_flag", utf8(), true),
        Field::new("raw_user_agent", utf8(), true),
        Field::new("session_fingerprint", utf8(), true),
        Field::new("interaction_payload", utf8(), true),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn customer360_schema_has_41_fields() {
        let s = customer360_schema();
        assert_eq!(s.fields().len(), 41);
    }

    #[test]
    fn customer360_schema_field_names_match_python_order() {
        let s = customer360_schema();
        let expected = [
            "id",
            "row_id",
            "event_timestamp",
            "event_id",
            "session_id",
            "customer_id",
            "email_raw",
            "phone_raw",
            "interaction_type",
            "product_id",
            "product_category",
            "transaction_amount",
            "currency",
            "channel",
            "device_type",
            "browser",
            "ip_address",
            "city_raw",
            "state_raw",
            "zip_code",
            "page_views",
            "time_on_site_seconds",
            "bounce_rate",
            "click_count",
            "cart_value",
            "items_in_cart",
            "support_ticket_id",
            "issue_category",
            "satisfaction_score",
            "campaign_id",
            "utm_source",
            "utm_medium",
            "loyalty_member",
            "loyalty_tier",
            "points_earned",
            "points_redeemed",
            "data_source",
            "data_quality_flag",
            "raw_user_agent",
            "session_fingerprint",
            "interaction_payload",
        ];
        let actual: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn customer360_event_timestamp_carries_utc_tz() {
        let s = customer360_schema();
        let ts_field = s.field_with_name("event_timestamp").unwrap();
        match ts_field.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                assert_eq!(tz.as_ref(), "UTC");
            }
            other => panic!(
                "event_timestamp datatype = {:?}, expected Timestamp(us, UTC)",
                other
            ),
        }
    }

    #[test]
    fn customer360_all_fields_nullable() {
        // Matches the Python `pa.schema` default; realism logic decides which
        // ones actually carry nulls at runtime.
        let s = customer360_schema();
        for f in s.fields() {
            assert!(f.is_nullable(), "field {} not nullable", f.name());
        }
    }

    #[test]
    fn customer360_schema_field_types_match_spec() {
        // Pin every (name, type) so a silent type swap (int32 -> int64,
        // customer_id -> Int32, satisfaction_score -> Float64, loyalty_member
        // -> Utf8, etc.) trips this test rather than a parquet-consumer
        // schema-mismatch in production. Types verbatim from Python
        // datagen/generate.py:885-931.
        let s = customer360_schema();
        let expected: Vec<(&str, DataType)> = vec![
            ("id", DataType::Int64),
            ("row_id", DataType::Int64),
            (
                "event_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            ),
            ("event_id", DataType::Utf8),
            ("session_id", DataType::Utf8),
            ("customer_id", DataType::Int64),
            ("email_raw", DataType::Utf8),
            ("phone_raw", DataType::Utf8),
            ("interaction_type", DataType::Utf8),
            ("product_id", DataType::Utf8),
            ("product_category", DataType::Utf8),
            ("transaction_amount", DataType::Float64),
            ("currency", DataType::Utf8),
            ("channel", DataType::Utf8),
            ("device_type", DataType::Utf8),
            ("browser", DataType::Utf8),
            ("ip_address", DataType::Utf8),
            ("city_raw", DataType::Utf8),
            ("state_raw", DataType::Utf8),
            ("zip_code", DataType::Utf8),
            ("page_views", DataType::Int32),
            ("time_on_site_seconds", DataType::Int32),
            ("bounce_rate", DataType::Float64),
            ("click_count", DataType::Int32),
            ("cart_value", DataType::Float64),
            ("items_in_cart", DataType::Int32),
            ("support_ticket_id", DataType::Utf8),
            ("issue_category", DataType::Utf8),
            ("satisfaction_score", DataType::Int32),
            ("campaign_id", DataType::Utf8),
            ("utm_source", DataType::Utf8),
            ("utm_medium", DataType::Utf8),
            ("loyalty_member", DataType::Boolean),
            ("loyalty_tier", DataType::Utf8),
            ("points_earned", DataType::Int32),
            ("points_redeemed", DataType::Int32),
            ("data_source", DataType::Utf8),
            ("data_quality_flag", DataType::Utf8),
            ("raw_user_agent", DataType::Utf8),
            ("session_fingerprint", DataType::Utf8),
            ("interaction_payload", DataType::Utf8),
        ];
        assert_eq!(s.fields().len(), expected.len());
        for (i, (name, want)) in expected.iter().enumerate() {
            let f = s.field(i);
            assert_eq!(f.name(), name, "field {} name", i);
            assert_eq!(
                f.data_type(),
                want,
                "field {} ({}) type mismatch: got {:?}, want {:?}",
                i,
                name,
                f.data_type(),
                want
            );
        }
    }
}
