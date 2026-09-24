//! Party zone, account zone, and manifest tables.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    ListArray, MapArray, StringArray, StructArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use crate::hash::splitmix64;
use crate::ids::iban_for;
use crate::kyc;
use crate::model::{World, MODEL_VERSION};
use crate::realism as R;
use crate::typology::Instance;
use crate::world::{TYPE_FI, TYPE_LABELS, TYPE_PERSON};
use crate::writer::writer_properties;

/// Entities per row group when streaming the party/account zones, so node 0's
/// memory stays bounded at scale instead of materialising all ~11M rows at once.
const CHUNK: usize = 1_000_000;

fn props() -> parquet::file::properties::WriterProperties {
    // Uses the shared writer_properties helper so party/account inherit the
    // same DG_COMPRESSION as pacs008 and the manifest. Previously this
    // hard-coded SNAPPY, so a run advertised as ZSTD-1 silently shipped
    // party.parquet + account.parquet still SNAPPY-compressed -- caught
    // by a live read-back verification after the ZSTD-1 default flip.
    writer_properties()
}

#[derive(Default, Clone)]
struct Override {
    name: Option<String>,
    street: Option<String>,
    town: Option<String>,
    postcode: Option<String>,
    email: Option<String>,
    phone: Option<String>,
}

fn sarr(v: Vec<String>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(v))
}

pub fn party_schema() -> SchemaRef {
    let addr = DataType::Struct(Fields::from(vec![
        Field::new("street", DataType::Utf8, true),
        Field::new("town", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("postcode", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
    ]));
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Int64, false),
        Field::new("entity_type", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("legal_name", DataType::Utf8, true),
        Field::new("address", addr, true),
        Field::new("email_addr", DataType::Utf8, true),
        Field::new("phone_number", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("lei", DataType::Utf8, true),
        Field::new("bic", DataType::Utf8, true),
        Field::new("sanctions_status", DataType::Utf8, true),
        Field::new("pep_status", DataType::Boolean, true),
        Field::new("initial_risk_score", DataType::Float64, true),
        Field::new("model_version", DataType::Utf8, true),
        // Monitored population and KYC (GOALS P10 stages 0 and 2; see
        // crate::kyc). The KYC fields are NULL for non-customers: the
        // reporting FI holds no CDD file on another bank's customer.
        Field::new("is_customer", DataType::Boolean, false),
        Field::new("home_fi", DataType::Utf8, true),
        Field::new("customer_since", DataType::Date32, true),
        Field::new("customer_type", DataType::Utf8, true),
        Field::new("expected_monthly_volume_usd", DataType::Float64, true),
        Field::new("crr_score", DataType::Int32, true),
        Field::new("crr_tier", DataType::Utf8, true),
        Field::new("crr_factors", DataType::Utf8, true),
    ]))
}

/// synthetic_identity PII overrides, keyed by entity index. Small (only cluster
/// participants), so it stays in memory while the party zone streams in chunks.
fn syn_overrides(w: &World, instances: &[Instance]) -> HashMap<usize, Override> {
    let mut ov: HashMap<usize, Override> = HashMap::new();
    for inst in instances {
        if inst.typ != "synthetic_identity" {
            continue;
        }
        let cluster = &inst.participants;
        if cluster.len() < 2 {
            continue;
        }
        let base = cluster[0] as usize;
        let base_phone = R::phone(base as u64, w.country[base], w.seed);
        let half = (cluster.len() / 2).max(1);
        for &e in &cluster[1..=half.min(cluster.len() - 1)] {
            ov.insert(
                e as usize,
                Override {
                    street: Some(w.street.get(base).to_string()),
                    town: Some(w.town.get(base).to_string()),
                    postcode: Some(w.postcode[base].clone()),
                    email: Some(w.email[base].clone()),
                    phone: Some(base_phone.clone()),
                    ..Default::default()
                },
            );
        }
        for (k, &e) in cluster[1 + half..].iter().enumerate() {
            ov.insert(
                e as usize,
                Override {
                    name: Some(name_variant(w.name.get(base), k % 5)),
                    email: Some(email_typo(&w.email[base])),
                    phone: Some(phone_variant(&base_phone)),
                    ..Default::default()
                },
            );
        }
    }
    ov
}

fn party_chunk(w: &World, lo: usize, hi: usize, ov: &HashMap<usize, Override>) -> RecordBatch {
    let m = hi - lo + 1;
    let mut ids = Vec::with_capacity(m);
    let mut etype = Vec::with_capacity(m);
    let mut names = Vec::with_capacity(m);
    let (mut st, mut tw, mut rg, mut pc, mut ctry) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut em = Vec::with_capacity(m);
    let mut ph = Vec::with_capacity(m);
    let mut lei: Vec<Option<String>> = Vec::with_capacity(m);
    let mut bic: Vec<Option<String>> = Vec::with_capacity(m);
    let mut sanc = Vec::with_capacity(m);
    let mut pep = Vec::with_capacity(m);
    let mut risk = Vec::with_capacity(m);
    let mut is_cust = Vec::with_capacity(m);
    let mut home: Vec<String> = Vec::with_capacity(m);
    let mut since: Vec<Option<i32>> = Vec::with_capacity(m);
    let mut ctype: Vec<Option<&'static str>> = Vec::with_capacity(m);
    let mut exp_vol: Vec<Option<f64>> = Vec::with_capacity(m);
    let mut crr_score: Vec<Option<i32>> = Vec::with_capacity(m);
    let mut crr_tier: Vec<Option<&'static str>> = Vec::with_capacity(m);
    let mut crr_factors: Vec<Option<String>> = Vec::with_capacity(m);
    for i in lo..=hi {
        let o = ov.get(&i);
        let id = i as u64;
        let cust = kyc::is_customer(id, w.seed);
        is_cust.push(cust);
        home.push(kyc::home_fi(&w.bic[i]).to_string());
        if cust {
            since.push(Some(kyc::customer_since_day(
                id,
                w.seed,
                w.dims.corpus_months,
            )));
            ctype.push(Some(kyc::customer_type(w.ty[i])));
            let v = kyc::expected_monthly_volume_usd(
                id,
                w.seed,
                w.amount_logshift[i],
                w.activity[i] / w.total_activity.max(f64::MIN_POSITIVE),
                w.population,
                w.dims.txn_per_entity_per_month,
            );
            let (sc, tier, f) = kyc::crr(w.ty[i], w.country[i], w.pep[i], v);
            exp_vol.push(Some(v));
            crr_score.push(Some(sc));
            crr_tier.push(Some(tier));
            crr_factors.push(Some(f));
        } else {
            since.push(None);
            ctype.push(None);
            exp_vol.push(None);
            crr_score.push(None);
            crr_tier.push(None);
            crr_factors.push(None);
        }
        ids.push(i as i64);
        etype.push(TYPE_LABELS[w.ty[i] as usize].to_string());
        let nm = o
            .and_then(|x| x.name.clone())
            .unwrap_or_else(|| w.name.get(i).to_string());
        names.push(nm);
        st.push(
            o.and_then(|x| x.street.clone())
                .unwrap_or_else(|| w.street.get(i).to_string()),
        );
        tw.push(
            o.and_then(|x| x.town.clone())
                .unwrap_or_else(|| w.town.get(i).to_string()),
        );
        rg.push(w.region[i].clone());
        pc.push(
            o.and_then(|x| x.postcode.clone())
                .unwrap_or_else(|| w.postcode[i].clone()),
        );
        ctry.push(w.country[i].to_string());
        em.push(
            o.and_then(|x| x.email.clone())
                .unwrap_or_else(|| w.email[i].clone()),
        );
        ph.push(
            o.and_then(|x| x.phone.clone())
                .unwrap_or_else(|| R::phone(i as u64, w.country[i], w.seed)),
        );
        lei.push(if w.ty[i] == TYPE_PERSON {
            None
        } else {
            Some(w.lei[i].clone())
        });
        bic.push(if w.ty[i] == TYPE_FI {
            Some(w.bic[i].clone())
        } else {
            None
        });
        sanc.push(if w.sanctioned[i] {
            "SDN".to_string()
        } else {
            "clear".to_string()
        });
        pep.push(w.pep[i]);
        let mut r = match w.ty[i] {
            TYPE_FI => 0.3,
            TYPE_PERSON => 0.05,
            _ => 0.15,
        };
        if w.sanctioned[i] {
            r = (r + 0.6f64).min(0.99);
        }
        if w.pep[i] {
            r = (r + 0.3f64).min(0.99);
        }
        risk.push(r);
    }
    let legal = names.clone();
    let addr = StructArray::new(
        Fields::from(vec![
            Field::new("street", DataType::Utf8, true),
            Field::new("town", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
            Field::new("postcode", DataType::Utf8, true),
            Field::new("country", DataType::Utf8, true),
        ]),
        vec![sarr(st), sarr(tw), sarr(rg), sarr(pc), sarr(ctry.clone())],
        None,
    );
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        sarr(etype),
        sarr(names),
        sarr(legal),
        Arc::new(addr),
        sarr(em),
        sarr(ph),
        sarr(ctry),
        Arc::new(StringArray::from(lei)),
        Arc::new(StringArray::from(bic)),
        sarr(sanc),
        Arc::new(BooleanArray::from(pep)),
        Arc::new(Float64Array::from(risk)),
        sarr(vec![MODEL_VERSION.to_string(); m]),
        Arc::new(BooleanArray::from(is_cust)),
        sarr(home),
        Arc::new(Date32Array::from(since)),
        Arc::new(StringArray::from(ctype)),
        Arc::new(Float64Array::from(exp_vol)),
        Arc::new(Int32Array::from(crr_score)),
        Arc::new(StringArray::from(crr_tier)),
        Arc::new(StringArray::from(crr_factors)),
    ];
    RecordBatch::try_new(party_schema(), cols).unwrap()
}

/// Stream the party zone into any `Write` in row-group chunks, bounding memory.
/// synthetic_identity variants applied via the overrides map. Writing to a
/// `Vec<u8>` and PUTting the buffer keeps the pod stateless (no scratch disk).
pub fn write_party_to<W: Write + Send>(w: &World, instances: &[Instance], sink: W) {
    let ov = syn_overrides(w, instances);
    let mut wr = ArrowWriter::try_new(sink, party_schema(), Some(props())).unwrap();
    let n = w.population;
    let mut lo = 1;
    while lo <= n {
        let hi = (lo + CHUNK - 1).min(n);
        wr.write(&party_chunk(w, lo, hi, &ov)).unwrap();
        lo = hi + 1;
    }
    wr.close().unwrap();
}

pub fn account_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Int64, false),
        Field::new("iban", DataType::Utf8, true),
        Field::new("holder_entity_id", DataType::Int64, true),
        Field::new("bank_bic", DataType::Utf8, true),
        Field::new("currency", DataType::Utf8, true),
        Field::new("opened_date", DataType::Date32, true),
        Field::new("closed_date", DataType::Date32, true),
        Field::new("current_balance", DataType::Decimal128(18, 2), true),
        Field::new("model_version", DataType::Utf8, true),
        // BIC8 of the bank group holding the account; the reporting FI
        // (kyc::REPORTING_FI) for a customer's accounts.
        Field::new("home_fi", DataType::Utf8, true),
    ]))
}

fn account_chunk(w: &World, lo: usize, hi: usize) -> RecordBatch {
    let seed_u = w.seed as u64;
    let mut acct_id = Vec::new();
    let mut iban = Vec::new();
    let mut holder = Vec::new();
    let mut bank_bic = Vec::new();
    let mut currency = Vec::new();
    let mut opened = Vec::new();
    let mut closed: Vec<Option<i32>> = Vec::new();
    let mut home: Vec<String> = Vec::new();
    for id in lo as u64..=hi as u64 {
        let i = id as usize;
        let cc = w.country[i];
        let cb = [cc.as_bytes()[0], cc.as_bytes()[1]];
        for seq in 0..w.n_accounts[i] as u64 {
            let acc_seed = splitmix64(id ^ (seq << 20) ^ seed_u);
            acct_id.push((splitmix64(id ^ (seq << 30)) & 0x7FFF_FFFF_FFFF_FFFF) as i64);
            // The first account is the one the entity's payments use: the
            // pacs.008 emit writes iban_for(country, id) as the debtor and
            // creditor account, so the account zone carries that same IBAN
            // and silver can join a payment to its holder's KYC. Further
            // accounts keep their own IBANs.
            iban.push(if seq == 0 {
                w.iban[i].clone()
            } else {
                iban_for(&cb, acc_seed)
            });
            holder.push(id as i64);
            bank_bic.push(w.bic[i].clone());
            home.push(kyc::home_fi(&w.bic[i]).to_string());
            currency.push(w.ccy[i].to_string());
            let od = kyc::account_opened_day(id);
            opened.push(od);
            let cm = (splitmix64(id ^ 0xC1) as f64 / 18446744073709551616.0) < 0.02;
            closed.push(if cm {
                Some(od + (splitmix64(id) % 365) as i32 + 180)
            } else {
                None
            });
        }
    }
    let total = acct_id.len();
    let balance = Decimal128Array::from(vec![None as Option<i128>; total])
        .with_precision_and_scale(18, 2)
        .unwrap();
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(acct_id)),
        sarr(iban),
        Arc::new(Int64Array::from(holder)),
        sarr(bank_bic),
        sarr(currency),
        Arc::new(Date32Array::from(opened)),
        Arc::new(Date32Array::from(closed)),
        Arc::new(balance),
        sarr(vec![MODEL_VERSION.to_string(); total]),
        sarr(home),
    ];
    RecordBatch::try_new(account_schema(), cols).unwrap()
}

/// Stream the account zone into any `Write` in chunks of entities. Same
/// rationale as `write_party_to`: keeps the emit pod off local disk.
pub fn write_account_to<W: Write + Send>(w: &World, sink: W) {
    let mut wr = ArrowWriter::try_new(sink, account_schema(), Some(props())).unwrap();
    let n = w.population;
    let mut lo = 1;
    while lo <= n {
        let hi = (lo + CHUNK - 1).min(n);
        wr.write(&account_chunk(w, lo, hi)).unwrap();
        lo = hi + 1;
    }
    wr.close().unwrap();
}

// --- Manifest ------------------------------------------------------------
pub fn manifest_schema() -> SchemaRef {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        false,
    );
    Arc::new(Schema::new(vec![
        Field::new("typology_id", DataType::Utf8, true),
        Field::new("typology_type", DataType::Utf8, true),
        Field::new(
            "participant_entity_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        // Downstream tooling (`score_financial.py`, `verify_run.py`) joins
        // this against `gold.alerts.related_txn_ids` -- keep the name in
        // sync with those consumers, and keep the list populated (not just
        // the schema field).
        Field::new(
            "participant_uetrs",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "injection_ts_start",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "injection_ts_end",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "injection_parameters",
            DataType::Map(Arc::new(entries), false),
            true,
        ),
        Field::new("expected_workload", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
        Field::new("seed", DataType::Int64, true),
        Field::new("model_version", DataType::Utf8, true),
    ]))
}

/// Populate participant_uetrs from the (instance_id -> row uids) map that
/// bin/generate.rs builds at typology-scheduling time. `seed` matches the
/// pipeline seed so the UETR derivation here is bit-identical to the bronze
/// emit's (see `emit::build_batch` -- same splitmix64 + uuid_v4_into with the
/// same 0x0E7A / 0x5A1D salts). A drift in that derivation would silently
/// break `score_financial.py`'s recall join, so any change here must be
/// mirrored in the bronze emit and vice versa; the `manifest_uetr_round_trip`
/// regression test pins the identity.
pub fn build_manifest(
    instances: &[Instance],
    seed: i64,
    inst_uids: &std::collections::HashMap<String, Vec<u64>>,
) -> RecordBatch {
    use crate::ids::uuid_v4_into;
    use arrow::array::TimestampMicrosecondArray;
    let m = instances.len();
    let tid: Vec<String> = instances.iter().map(|i| i.id.clone()).collect();
    let ttype: Vec<String> = instances.iter().map(|i| i.typ.to_string()).collect();

    // participant list<int64>
    let mut pvals = Vec::new();
    let mut pcounts = Vec::new();
    for inst in instances {
        for &p in &inst.participants {
            pvals.push(p as i64);
        }
        pcounts.push(inst.participants.len() as i32);
    }
    let participants = ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        offsets(&pcounts),
        Arc::new(Int64Array::from(pvals)),
        None,
    );
    // participant_uetrs: for each instance, derive UETR per stored uid using
    // the same splitmix64 salts as the bronze emit's `build_batch`.
    let mut uetr_vals: Vec<String> = Vec::new();
    let mut uetr_counts: Vec<i32> = Vec::with_capacity(m);
    let mut buf = String::with_capacity(40);
    for inst in instances {
        let uids = inst_uids.get(&inst.id).map(|v| v.as_slice()).unwrap_or(&[]);
        for &uid in uids {
            let (us, us2) = crate::hash::uetr_seeds(uid, seed);
            buf.clear();
            uuid_v4_into(us, us2, &mut buf);
            uetr_vals.push(buf.clone());
        }
        uetr_counts.push(uids.len() as i32);
    }
    let participant_uetrs = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        offsets(&uetr_counts),
        sarr(uetr_vals),
        None,
    );
    let start: Vec<i64> = instances.iter().map(|i| i.start_us).collect();
    let end: Vec<i64> = instances.iter().map(|i| i.end_us).collect();

    // injection_parameters map: one entry per instance {rows_per_instance: N}
    let keys = StringArray::from_iter_values(vec!["rows_per_instance".to_string(); m]);
    let vals = StringArray::from_iter_values(
        instances
            .iter()
            .map(|i| i.rows_per_instance.to_string())
            .collect::<Vec<_>>(),
    );
    let entries = StructArray::new(
        Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]),
        vec![Arc::new(keys), Arc::new(vals)],
        None,
    );
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        false,
    ));
    let map = MapArray::new(entries_field, offsets(&vec![1i32; m]), entries, None, false);

    let cols: Vec<ArrayRef> = vec![
        sarr(tid),
        sarr(ttype),
        Arc::new(participants),
        Arc::new(participant_uetrs),
        Arc::new(TimestampMicrosecondArray::from(start)),
        Arc::new(TimestampMicrosecondArray::from(end)),
        Arc::new(map),
        sarr(instances.iter().map(|i| i.workload.to_string()).collect()),
        sarr(instances.iter().map(|i| i.severity.to_string()).collect()),
        Arc::new(Int64Array::from(
            instances.iter().map(|i| i.seed).collect::<Vec<_>>(),
        )),
        sarr(vec![MODEL_VERSION.to_string(); m]),
    ];
    RecordBatch::try_new(manifest_schema(), cols).unwrap()
}

fn offsets(counts: &[i32]) -> OffsetBuffer<i32> {
    let mut off = Vec::with_capacity(counts.len() + 1);
    off.push(0i32);
    let mut acc = 0i32;
    for &c in counts {
        acc += c;
        off.push(acc);
    }
    OffsetBuffer::new(ScalarBuffer::from(off))
}

// --- Fuzzy variants ------------------------------------------------------
fn name_variant(name: &str, kind: usize) -> String {
    let parts: Vec<&str> = name.split_whitespace().collect();
    match kind {
        0 if parts.len() >= 2 => format!("{} X. {}", parts[0], parts[parts.len() - 1]),
        1 if parts.len() >= 2 => format!("{}. {}", &parts[0][..1], parts[1..].join(" ")),
        2 if name.len() > 3 => {
            let b = name.as_bytes();
            let mut v = b.to_vec();
            v.swap(2, 3);
            String::from_utf8_lossy(&v).to_string()
        }
        3 => name.replace("Street", "St").replace("Avenue", "Ave"),
        _ => name.replace([',', '.'], ""),
    }
}

fn email_typo(email: &str) -> String {
    for (k, v) in [
        ("gmail", "gmial"),
        ("yahoo", "yaoo"),
        ("hotmail", "hotnail"),
        ("outlook", "outlok"),
    ] {
        if email.contains(k) {
            return email.replacen(k, v, 1);
        }
    }
    email.replacen('.', "..", 1)
}

fn phone_variant(phone: &str) -> String {
    if let Some(pos) = phone.find(' ') {
        phone[pos + 1..].to_string()
    } else {
        phone.replace('-', " ")
    }
}
