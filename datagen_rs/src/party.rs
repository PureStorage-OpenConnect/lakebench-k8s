//! Party zone, account zone, and manifest tables.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    ListArray, MapArray, StringArray, StructArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::hash::splitmix64;
use crate::ids::iban_for;
use crate::model::{World, MODEL_VERSION};
use crate::realism as R;
use crate::typology::Instance;
use crate::world::{TYPE_FI, TYPE_LABELS, TYPE_PERSON};

/// Entities per row group when streaming the party/account zones, so node 0's
/// memory stays bounded at scale instead of materialising all ~11M rows at once.
const CHUNK: usize = 1_000_000;

fn props() -> WriterProperties {
    WriterProperties::builder().set_compression(Compression::SNAPPY).build()
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
            ov.insert(e as usize, Override {
                street: Some(w.street.get(base).to_string()),
                town: Some(w.town.get(base).to_string()),
                postcode: Some(w.postcode[base].clone()),
                email: Some(w.email[base].clone()),
                phone: Some(base_phone.clone()),
                ..Default::default()
            });
        }
        for (k, &e) in cluster[1 + half..].iter().enumerate() {
            ov.insert(e as usize, Override {
                name: Some(name_variant(w.name.get(base), k % 5)),
                email: Some(email_typo(&w.email[base])),
                phone: Some(phone_variant(&base_phone)),
                ..Default::default()
            });
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
    for i in lo..=hi {
        let o = ov.get(&i);
        ids.push(i as i64);
        etype.push(TYPE_LABELS[w.ty[i] as usize].to_string());
        let nm = o.and_then(|x| x.name.clone()).unwrap_or_else(|| w.name.get(i).to_string());
        names.push(nm);
        st.push(o.and_then(|x| x.street.clone()).unwrap_or_else(|| w.street.get(i).to_string()));
        tw.push(o.and_then(|x| x.town.clone()).unwrap_or_else(|| w.town.get(i).to_string()));
        rg.push(w.region[i].clone());
        pc.push(o.and_then(|x| x.postcode.clone()).unwrap_or_else(|| w.postcode[i].clone()));
        ctry.push(w.country[i].to_string());
        em.push(o.and_then(|x| x.email.clone()).unwrap_or_else(|| w.email[i].clone()));
        ph.push(o.and_then(|x| x.phone.clone()).unwrap_or_else(|| R::phone(i as u64, w.country[i], w.seed)));
        lei.push(if w.ty[i] == TYPE_PERSON { None } else { Some(w.lei[i].clone()) });
        bic.push(if w.ty[i] == TYPE_FI { Some(w.bic[i].clone()) } else { None });
        sanc.push(if w.sanctioned[i] { "SDN".to_string() } else { "clear".to_string() });
        pep.push(w.pep[i]);
        let mut r = match w.ty[i] { TYPE_FI => 0.3, TYPE_PERSON => 0.05, _ => 0.15 };
        if w.sanctioned[i] { r = (r + 0.6f64).min(0.99); }
        if w.pep[i] { r = (r + 0.3f64).min(0.99); }
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
    ]))
}

fn account_chunk(w: &World, lo: usize, hi: usize) -> RecordBatch {
    let seed_u = w.seed as u64;
    let base_open: i32 = 18262; // 2020-01-01
    let span_open: i64 = 2191;
    let mut acct_id = Vec::new();
    let mut iban = Vec::new();
    let mut holder = Vec::new();
    let mut bank_bic = Vec::new();
    let mut currency = Vec::new();
    let mut opened = Vec::new();
    let mut closed: Vec<Option<i32>> = Vec::new();
    for id in lo as u64..=hi as u64 {
        let i = id as usize;
        let cc = w.country[i];
        let cb = [cc.as_bytes()[0], cc.as_bytes()[1]];
        for seq in 0..w.n_accounts[i] as u64 {
            let acc_seed = splitmix64(id ^ (seq << 20) ^ seed_u);
            acct_id.push((splitmix64(id ^ (seq << 30)) & 0x7FFF_FFFF_FFFF_FFFF) as i64);
            iban.push(iban_for(&cb, acc_seed));
            holder.push(id as i64);
            bank_bic.push(w.bic[i].clone());
            currency.push(w.ccy[i].to_string());
            let od = base_open + (splitmix64(id ^ 0x0DA7E) % span_open as u64) as i32;
            opened.push(od);
            let cm = (splitmix64(id ^ 0xC1) as f64 / 18446744073709551616.0) < 0.02;
            closed.push(if cm { Some(od + (splitmix64(id) % 365) as i32 + 180) } else { None });
        }
    }
    let total = acct_id.len();
    let balance =
        Decimal128Array::from(vec![None as Option<i128>; total]).with_precision_and_scale(18, 2).unwrap();
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
        Field::new(
            "transaction_ids",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("injection_ts_start", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), true),
        Field::new("injection_ts_end", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), true),
        Field::new("injection_parameters", DataType::Map(Arc::new(entries), false), true),
        Field::new("expected_workload", DataType::Utf8, true),
        Field::new("severity", DataType::Utf8, true),
        Field::new("seed", DataType::Int64, true),
        Field::new("model_version", DataType::Utf8, true),
    ]))
}

pub fn build_manifest(instances: &[Instance]) -> RecordBatch {
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
    // empty transaction_ids
    let txn_ids = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        offsets(&vec![0i32; m]),
        sarr(Vec::new()),
        None,
    );
    let start: Vec<i64> = instances.iter().map(|i| i.start_us).collect();
    let end: Vec<i64> = instances.iter().map(|i| i.end_us).collect();

    // injection_parameters map: one entry per instance {rows_per_instance: N}
    let keys = StringArray::from_iter_values(vec!["rows_per_instance".to_string(); m]);
    let vals = StringArray::from_iter_values(
        instances.iter().map(|i| i.rows_per_instance.to_string()).collect::<Vec<_>>(),
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
        Arc::new(txn_ids),
        Arc::new(TimestampMicrosecondArray::from(start)),
        Arc::new(TimestampMicrosecondArray::from(end)),
        Arc::new(map),
        sarr(instances.iter().map(|i| i.workload.to_string()).collect()),
        sarr(instances.iter().map(|i| i.severity.to_string()).collect()),
        Arc::new(Int64Array::from(instances.iter().map(|i| i.seed).collect::<Vec<_>>())),
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
    for (k, v) in [("gmail", "gmial"), ("yahoo", "yaoo"), ("hotmail", "hotnail"), ("outlook", "outlok")] {
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
