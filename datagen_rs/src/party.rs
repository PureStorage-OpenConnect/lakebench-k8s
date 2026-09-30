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
        // No sanctions_status, pep_status or initial_risk_score: whether a
        // party is listed is the answer the screening rules (W5, W6) are
        // scored against, so it lives only in the manifest (AML-GOALS #50).
        // Listed parties are external counterparties, never in this zone.
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
        // Base PII is recomputed on demand (LB-204). It is ty-branch-dependent
        // via w.name/w.email (person/company/fi), reproduced exactly here.
        let base_phone = R::phone(base as u64, w.country(base), w.seed);
        let base_name = w.name(base);
        let base_email = w.email(base);
        let base_street = w.street(base);
        let base_town = w.town(base);
        let base_postcode = w.postcode(base);
        let half = (cluster.len() / 2).max(1);
        for &e in &cluster[1..=half.min(cluster.len() - 1)] {
            ov.insert(
                e as usize,
                Override {
                    street: Some(base_street.clone()),
                    town: Some(base_town.clone()),
                    postcode: Some(base_postcode.clone()),
                    email: Some(base_email.clone()),
                    phone: Some(base_phone.clone()),
                    ..Default::default()
                },
            );
        }
        for (k, &e) in cluster[1 + half..].iter().enumerate() {
            ov.insert(
                e as usize,
                Override {
                    name: Some(name_variant(&base_name, k % 5)),
                    email: Some(email_typo(&base_email)),
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
        // Attributes recomputed on demand (LB-204); bind the ones read more
        // than once per row so the recompute happens at most once each.
        let ty_i = w.ty(i);
        let country_i = w.country(i);
        let cust = kyc::is_customer(id, w.seed);
        is_cust.push(cust);
        home.push(kyc::home_fi(&w.bic(i)).to_string());
        if cust {
            since.push(Some(kyc::customer_since_day(
                id,
                w.seed,
                w.dims.corpus_months,
            )));
            ctype.push(Some(kyc::customer_type(ty_i)));
            let v = kyc::expected_monthly_volume_usd(
                id,
                w.seed,
                w.amount_logshift(i),
                w.activity[i] / w.total_activity.max(f64::MIN_POSITIVE),
                w.population,
                w.dims.txn_per_entity_per_month,
            );
            let (sc, tier, f) = kyc::crr(ty_i, country_i, v);
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
        etype.push(TYPE_LABELS[ty_i as usize].to_string());
        let nm = o.and_then(|x| x.name.clone()).unwrap_or_else(|| w.name(i));
        names.push(nm);
        st.push(
            o.and_then(|x| x.street.clone())
                .unwrap_or_else(|| w.street(i)),
        );
        tw.push(o.and_then(|x| x.town.clone()).unwrap_or_else(|| w.town(i)));
        rg.push(w.region(i));
        pc.push(
            o.and_then(|x| x.postcode.clone())
                .unwrap_or_else(|| w.postcode(i)),
        );
        ctry.push(country_i.to_string());
        em.push(
            o.and_then(|x| x.email.clone())
                .unwrap_or_else(|| w.email(i)),
        );
        ph.push(
            o.and_then(|x| x.phone.clone())
                .unwrap_or_else(|| R::phone(i as u64, country_i, w.seed)),
        );
        lei.push(if ty_i == TYPE_PERSON {
            None
        } else {
            Some(w.lei(i))
        });
        // An FI's own identifier: a pool BIC that is never the reporting
        // FI's, even when the FI banks with the reporting FI (then w.bic, its
        // account-holding bank, is the reporting FI's BIC).
        bic.push(if ty_i == TYPE_FI {
            Some(w.bic_pool[kyc::own_bic_idx(i as u64, w.bic_pool.len())].clone())
        } else {
            None
        });
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

/// (account_id, IBAN seed) of account `seq` of entity `id`. id and seq are
/// hashed separately and then combined: packing them as id ^ (seq << 20)
/// (and id ^ (seq << 30) for the id) collided once ids passed 2^20 (scale
/// ~9.4), giving two entities the same IBAN. The IBAN seed of seq 0 is not
/// used (the payment account's IBAN is the entity's own, iban_for(id)).
pub fn account_keys(id: u64, seq: u64, seed_u: u64) -> (i64, u64) {
    let h = splitmix64(splitmix64(id) ^ splitmix64(seq.wrapping_add(0xACC7_0000_0000)));
    let aid = (h & 0x7FFF_FFFF_FFFF_FFFF) as i64;
    let iban_seed = splitmix64(h ^ splitmix64(seed_u ^ 0x1BA7_5EED));
    (aid, iban_seed)
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
        // Attributes recomputed on demand (LB-204); bind the per-entity ones so
        // the recompute happens once for the whole account-sequence loop.
        let country_i = w.country(i);
        let bic_i = w.bic(i);
        let home_fi = kyc::home_fi(&bic_i).to_string();
        let ccy_i = w.ccy(i);
        let iban_i = w.iban(i);
        let cc = kyc::account_country(kyc::is_customer(id, w.seed), country_i);
        let cb = [cc.as_bytes()[0], cc.as_bytes()[1]];
        let primary_od = kyc::primary_opened_day(id, w.seed, w.dims.corpus_months);
        for seq in 0..w.n_accounts(i) as u64 {
            let (aid, acc_seed) = account_keys(id, seq, seed_u);
            acct_id.push(aid);
            // The first account is the one the entity's payments use: the
            // pacs.008 emit writes iban_for(country, id) as the debtor and
            // creditor account, so the account zone carries that same IBAN
            // and silver can join a payment to its holder's KYC. Further
            // accounts keep their own IBANs.
            iban.push(if seq == 0 {
                iban_i.clone()
            } else {
                iban_for(&cb, acc_seed)
            });
            holder.push(id as i64);
            bank_bic.push(bic_i.clone());
            home.push(home_fi.clone());
            currency.push(ccy_i.to_string());
            let od = if seq == 0 {
                primary_od
            } else {
                kyc::account_opened_day(id, primary_od)
            };
            opened.push(od);
            // The payment account stays open: the entity pays from it until
            // the corpus ends. Further accounts close at a 2% rate.
            let cm = seq > 0 && (splitmix64(id ^ 0xC1) as f64 / 18446744073709551616.0) < 0.02;
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
    build_manifest_p(
        instances,
        seed,
        inst_uids,
        &crate::robustness::Perturbation::NONE,
    )
}

/// `build_manifest` with the robustness perturbation stamped into every row's
/// injection_parameters (crate::robustness::Perturbation::manifest_entries).
/// `Perturbation::NONE` adds nothing, so the manifest is unchanged.
pub fn build_manifest_p(
    instances: &[Instance],
    seed: i64,
    inst_uids: &std::collections::HashMap<String, Vec<u64>>,
    perturb: &crate::robustness::Perturbation,
) -> RecordBatch {
    build_manifest_x(instances, seed, inst_uids, perturb, &HashMap::new())
}

/// `build_manifest_p` plus per-instance injection_parameters entries (the
/// screening track's list_id, list_version, detectable_by, name_variants),
/// appended after the standard ones. An instance with no entry in `extra`
/// gets exactly the `build_manifest_p` row.
pub fn build_manifest_x(
    instances: &[Instance],
    seed: i64,
    inst_uids: &std::collections::HashMap<String, Vec<u64>>,
    perturb: &crate::robustness::Perturbation,
    extra: &HashMap<String, Vec<(&'static str, String)>>,
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

    // injection_parameters map: {rows_per_instance: N} per instance, plus the
    // robustness stamp on every instance of a perturbed corpus.
    let stamp = perturb.manifest_entries();
    let per_row = 1 + stamp.len();
    let mut key_vals: Vec<String> = Vec::with_capacity(m * per_row);
    let mut val_vals: Vec<String> = Vec::with_capacity(m * per_row);
    let mut entry_counts: Vec<i32> = Vec::with_capacity(m);
    for i in instances {
        key_vals.push("rows_per_instance".to_string());
        val_vals.push(i.rows_per_instance.to_string());
        for (k, v) in &stamp {
            key_vals.push((*k).to_string());
            val_vals.push(v.clone());
        }
        let ex = extra.get(&i.id).map(|v| v.as_slice()).unwrap_or(&[]);
        for (k, v) in ex {
            key_vals.push((*k).to_string());
            val_vals.push(v.clone());
        }
        entry_counts.push((per_row + ex.len()) as i32);
    }
    let keys = StringArray::from_iter_values(key_vals);
    let vals = StringArray::from_iter_values(val_vals);
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
    let map = MapArray::new(entries_field, offsets(&entry_counts), entries, None, false);

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

// --- Watchlist -----------------------------------------------------------

pub fn watchlist_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("list_id", DataType::Utf8, false),
        // "sanctions" or "pep".
        Field::new("list_type", DataType::Utf8, false),
        // The version in which the entry first appears. Version n of a list is
        // every entry with list_version <= n, published on its
        // version_published_date.
        Field::new("list_version", DataType::Int32, false),
        Field::new("version_published_date", DataType::Date32, false),
        Field::new("listed_date", DataType::Date32, false),
        Field::new("entity_type", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "aliases",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("country", DataType::Utf8, true),
        Field::new("town", DataType::Utf8, true),
        Field::new("program", DataType::Utf8, true),
        Field::new("position", DataType::Utf8, true),
        Field::new("model_version", DataType::Utf8, false),
    ]))
}

/// The published watchlist (crate::screening): every list entry with its
/// version and dates. Carries nothing about which entries were paid.
pub fn watchlist_batch(scr: &crate::screening::Screening) -> RecordBatch {
    const DAY: i64 = 86_400_000_000;
    let ps = &scr.parties;
    let n = ps.len();
    let mut alias_vals: Vec<String> = Vec::new();
    let mut alias_counts: Vec<i32> = Vec::with_capacity(n);
    for p in ps {
        alias_vals.extend(p.aliases.iter().cloned());
        alias_counts.push(p.aliases.len() as i32);
    }
    let aliases = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        offsets(&alias_counts),
        sarr(alias_vals),
        None,
    );
    let published: Vec<i32> = ps
        .iter()
        .map(|p| {
            let us = if p.list_version == 1 {
                scr.v1_published_us
            } else {
                scr.v2_published_us
            };
            us.div_euclid(DAY) as i32
        })
        .collect();
    let cols: Vec<ArrayRef> = vec![
        sarr(ps.iter().map(|p| p.list_id.clone()).collect()),
        sarr(ps.iter().map(|p| p.list_type.to_string()).collect()),
        Arc::new(Int32Array::from(
            ps.iter().map(|p| p.list_version).collect::<Vec<_>>(),
        )),
        Arc::new(Date32Array::from(published)),
        Arc::new(Date32Array::from(
            ps.iter()
                .map(|p| p.listed_us.div_euclid(DAY) as i32)
                .collect::<Vec<_>>(),
        )),
        sarr(ps.iter().map(|p| p.entity_type.to_string()).collect()),
        sarr(ps.iter().map(|p| p.name.clone()).collect()),
        Arc::new(aliases),
        sarr(ps.iter().map(|p| p.country.to_string()).collect()),
        sarr(ps.iter().map(|p| p.town.clone()).collect()),
        Arc::new(StringArray::from(
            ps.iter().map(|p| p.program).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            ps.iter().map(|p| p.position).collect::<Vec<_>>(),
        )),
        sarr(vec![MODEL_VERSION.to_string(); n]),
    ];
    RecordBatch::try_new(watchlist_schema(), cols).unwrap()
}
