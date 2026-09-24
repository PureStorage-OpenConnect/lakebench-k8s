//! Assemble one batch of transactions into a pacs.008 RecordBatch. Each string
//! column is built once into a single growing buffer (no per-cell allocation),
//! and child arrays are shared across structs via Arc clones.

use std::sync::Arc;

use arrow::array::builder::StringBuilder;
use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Int32Array, ListArray, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;

use crate::amounts::fx_to_usd;
use crate::hash::splitmix64;
use crate::ids::{bic_idx, iban_into, lei_into, msg_id_into, txn_id_into, uuid_v4_into};
use crate::model::World;
use crate::schema::*;

const PURPOSES: [&str; 15] = [
    "SALA", "COMC", "GDDS", "SUPP", "TRAD", "INTC", "TREA", "CASH", "DIVI", "INTE", "LOAN", "PENS",
    "TAXS", "RENT", "SCVE",
];
const PURPOSE_W: [f64; 15] = [
    0.15, 0.10, 0.08, 0.10, 0.15, 0.05, 0.03, 0.08, 0.02, 0.03, 0.04, 0.03, 0.05, 0.05, 0.04,
];
const CHRG_BR: [&str; 4] = ["SHAR", "DEBT", "CRED", "SLEV"];
const CLR_CHANL: [&str; 4] = ["RTGS", "RTNS", "MPNS", "BOOK"];
const SVC_LVL: [&str; 5] = ["SEPA", "URGP", "NURG", "PRPT", "G001"];
const INSTR_PRTY: [&str; 2] = ["NORM", "HIGH"];
const REGULATORS: [&str; 16] = [
    "FinCEN", "FCA", "BaFin", "ACPR", "FINMA", "MAS", "FSA", "CBUAE", "RBI", "FINTRAC", "AUSTRAC",
    "SAFE", "CNBV", "SBP", "CIMA", "APRA",
];

#[inline]
fn uf(x: u64) -> f64 {
    splitmix64(x) as f64 / 18446744073709551616.0
}

#[inline]
fn sb(n: usize) -> StringBuilder {
    StringBuilder::with_capacity(n, n * 12)
}

fn nb_from(mask: &[bool]) -> Option<NullBuffer> {
    Some(NullBuffer::new(BooleanBuffer::from(mask.to_vec())))
}

fn dec18(v: Vec<i128>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(v)
            .with_precision_and_scale(18, 5)
            .unwrap(),
    )
}

/// Constant string column of length n, built once.
fn const_str(val: &str, n: usize) -> ArrayRef {
    let mut b = StringBuilder::with_capacity(n, n * val.len());
    for _ in 0..n {
        b.append_value(val);
    }
    Arc::new(b.finish())
}

pub struct Batch<'a> {
    pub orig: Vec<u64>,
    pub bene: Vec<u64>,
    pub ts_us: Vec<i64>,
    pub amount: Vec<f64>,
    pub ccy: Vec<&'a str>,
    /// Globally-unique, per-row deterministic id (e.g. file_id<<40 | row_index).
    /// Seeds the UETR and msg_id so they cannot collide even when two rows share
    /// (originator, timestamp).
    pub uid: Vec<u64>,
}

fn agent(bicfi: ArrayRef, lei: ArrayRef, nm: ArrayRef, nulls: Option<NullBuffer>) -> ArrayRef {
    Arc::new(StructArray::new(
        agent_fields(),
        vec![bicfi, lei, nm],
        nulls,
    ))
}

pub fn build_batch(w: &World, b: &Batch) -> RecordBatch {
    let n = b.orig.len();
    let seed_u = w.seed as u64;
    let pool = &w.bic_pool;
    let plen = pool.len() as u64;

    // Purpose CDF.
    let mut pcdf = [0.0f64; 15];
    let ptot: f64 = PURPOSE_W.iter().sum();
    let mut acc = 0.0;
    for i in 0..15 {
        acc += PURPOSE_W[i] / ptot;
        pcdf[i] = acc;
    }

    // String builders (each a single growing buffer).
    let mut b_msgid = sb(n);
    let mut b_uetr = sb(n);
    let mut b_txnid = sb(n);
    let mut b_ccy = sb(n);
    let mut b_ictry = sb(n);
    let mut b_purp = sb(n);
    let mut b_chrg = sb(n);
    let mut b_clrref = sb(n);
    let mut b_iprty = sb(n);
    let mut b_clrch = sb(n);
    let mut b_svc = sb(n);
    let mut b_lcl = sb(n);
    let mut b_no = sb(n);
    let mut b_nc = sb(n);
    let mut b_sto = sb(n);
    let mut b_stc = sb(n);
    let mut b_two = sb(n);
    let mut b_twc = sb(n);
    let mut b_cto = sb(n);
    let mut b_ctc = sb(n);
    let mut b_ibo = sb(n);
    let mut b_ibc = sb(n);
    let mut b_leo = sb(n);
    let mut b_lec = sb(n);
    let mut b_bico = sb(n);
    let mut b_bicc = sb(n);
    let mut b_instg = sb(n);
    let mut b_instd = sb(n);
    let mut b_i1 = sb(n);
    let mut b_i2 = sb(n);
    let mut b_i3 = sb(n);
    let mut b_prvs = sb(n);

    let mut nb_txs = Vec::with_capacity(n);
    let mut amt_i = Vec::with_capacity(n);
    let mut instd_amt_i = Vec::with_capacity(n);
    let mut xchg_i = Vec::with_capacity(n);
    let mut day = Vec::with_capacity(n);
    let mut txn_seed = vec![0u64; n];

    let mut chain1 = vec![false; n];
    let mut chain2 = vec![false; n];
    let mut chain3 = vec![false; n];
    let mut prvs_m = vec![false; n];
    let mut ultd_m = vec![false; n];
    let mut ultc_m = vec![false; n];
    let mut initg_m = vec![false; n];
    let mut rg_m = vec![false; n];
    let mut rmt_m = vec![false; n];
    let mut strd_m = vec![false; n];
    let mut amount_usd = vec![0.0f64; n];
    let mut ct_o_ref: Vec<&str> = Vec::with_capacity(n);
    let mut ct_c_ref: Vec<&str> = Vec::with_capacity(n);

    let mut su = String::with_capacity(40);
    let mut sm = String::with_capacity(40);
    let mut stx = String::with_capacity(40);
    // Fixed-width identifiers are recomputed per row into these stack buffers
    // rather than gathered from the world (see ids::iban_into). At scale 100
    // the world's Vec<String> columns are ~11M entries and every gather is a
    // random cache miss; recomputing keeps the hot loop in registers.
    let mut ib_o = [0u8; 22];
    let mut ib_c = [0u8; 22];
    let mut le_o = [0u8; 20];
    let mut le_c = [0u8; 20];

    for i in 0..n {
        let o = b.orig[i] as usize;
        let c = b.bene[i] as usize;
        let ts = b.ts_us[i];
        let ts_u = ts as u64;
        let s = splitmix64(splitmix64(b.orig[i] ^ ts_u) ^ seed_u);
        txn_seed[i] = s;
        let ccy = b.ccy[i];

        // Identifiers are seeded from the globally-unique per-row uid, so they
        // are collision-free regardless of (orig, ts) duplicates. splitmix64 is
        // a bijection, so distinct uids yield distinct id seeds.
        let uid = b.uid[i];
        let us = splitmix64(uid ^ seed_u ^ 0x0E7A);
        let us2 = splitmix64(uid ^ seed_u ^ 0x5A1D);
        uuid_v4_into(us, us2, &mut su);
        b_uetr.append_value(&su);
        msg_id_into(us, us2, &mut sm);
        b_msgid.append_value(&sm);
        txn_id_into(b.orig[i], b.bene[i], &su[0..8], &mut stx);
        b_txnid.append_value(&stx);
        if uf(s ^ 0xCA) < 0.30 {
            b_clrref.append_value(&su[0..12]);
        } else {
            b_clrref.append_null();
        }

        nb_txs.push(if uf(s ^ 0xBB77) < 0.25 {
            1 + (splitmix64(s ^ 0xCC99) % 30) as i32
        } else {
            1
        });

        let amt = b.amount[i];
        amt_i.push((amt * 100_000.0).round() as i128);
        let cross_ccy = uf(s ^ 0xDDBB) < 0.10;
        let other = if ccy == "USD" { "EUR" } else { "USD" };
        let ic = if cross_ccy { other } else { ccy };
        let xf = if cross_ccy {
            fx_to_usd(ccy) / fx_to_usd(ic)
        } else {
            1.0
        };
        xchg_i.push((xf * 1e10).round() as i128);
        instd_amt_i.push((amt * xf * 100_000.0).round() as i128);
        b_ictry.append_value(ic);
        b_ccy.append_value(ccy);

        let up = uf(s ^ 0xAA55);
        let mut pi = 14;
        for k in 0..15 {
            if up <= pcdf[k] {
                pi = k;
                break;
            }
        }
        b_purp.append_value(PURPOSES[pi]);
        b_chrg.append_value(CHRG_BR[(splitmix64(s ^ 0xC5) % 4) as usize]);
        b_iprty.append_value(INSTR_PRTY[(splitmix64(s ^ 0xC6) % 2) as usize]);
        b_clrch.append_value(CLR_CHANL[(splitmix64(s ^ 0xC7) % 4) as usize]);
        b_svc.append_value(SVC_LVL[(splitmix64(s ^ 0xC8) % 5) as usize]);
        if uf(s ^ 0xC9) < 0.15 {
            b_lcl.append_value("INST");
        } else {
            b_lcl.append_null();
        }

        day.push((ts / 86_400_000_000) as i32);

        b_no.append_value(w.name.get(o));
        b_nc.append_value(w.name.get(c));
        b_sto.append_value(w.street.get(o));
        b_stc.append_value(w.street.get(c));
        b_two.append_value(w.town.get(o));
        b_twc.append_value(w.town.get(c));
        b_cto.append_value(w.country[o]);
        b_ctc.append_value(w.country[c]);
        // Recompute fixed-width ids (identical bytes to the world columns, which
        // were built by these same functions) instead of random-gathering them.
        let cco = w.country[o].as_bytes();
        let ccc = w.country[c].as_bytes();
        iban_into(&[cco[0], cco[1]], b.orig[i], &mut ib_o);
        iban_into(&[ccc[0], ccc[1]], b.bene[i], &mut ib_c);
        lei_into(b.orig[i], &mut le_o);
        lei_into(b.bene[i], &mut le_c);
        // Safe: all bytes are ASCII digits/letters.
        b_ibo.append_value(unsafe { std::str::from_utf8_unchecked(&ib_o) });
        b_ibc.append_value(unsafe { std::str::from_utf8_unchecked(&ib_c) });
        b_leo.append_value(unsafe { std::str::from_utf8_unchecked(&le_o) });
        b_lec.append_value(unsafe { std::str::from_utf8_unchecked(&le_c) });
        // BIC: the 500-entry pool is cache-resident; index it directly.
        let bic_o = &pool[bic_idx(b.orig[i], pool.len())];
        let bic_c = &pool[bic_idx(b.bene[i], pool.len())];
        b_bico.append_value(bic_o);
        b_bicc.append_value(bic_c);
        ct_o_ref.push(w.country[o]);
        ct_c_ref.push(w.country[c]);

        let cross = w.country[o] != w.country[c];
        let hop = splitmix64(s ^ 0xF00);
        if (hop & 0xFFFF) < 15_000 {
            b_instg.append_value(&pool[(splitmix64(b.orig[i] ^ 0x2222) % plen) as usize]);
        } else {
            b_instg.append_value(bic_o);
        }
        if (hop & 0xFF_FF00) < 15_000_00 {
            b_instd.append_value(&pool[(splitmix64(b.bene[i] ^ 0x3333) % plen) as usize]);
        } else {
            b_instd.append_value(bic_c);
        }

        let uch = uf(s ^ 0xC0AA);
        let rate = if cross { 0.60 } else { 0.08 };
        let m1 = uch < rate;
        let depth = (splitmix64(s ^ 0xC0BB) % 3) as i32 + 1;
        chain1[i] = m1;
        chain2[i] = m1 && depth >= 2;
        chain3[i] = m1 && depth >= 3;
        b_i1.append_value(&pool[(splitmix64(s ^ 0xC0CC) % plen) as usize]);
        b_i2.append_value(&pool[(splitmix64(s ^ 0xC0DD) % plen) as usize]);
        b_i3.append_value(&pool[(splitmix64(s ^ 0xC0EE) % plen) as usize]);
        prvs_m[i] = uf(s ^ 0xC0FF) < 0.07;
        b_prvs.append_value(&pool[(splitmix64(s ^ 0xC100) % plen) as usize]);

        ultd_m[i] = uf(s ^ 0xE1) < 0.12;
        ultc_m[i] = uf(s ^ 0xE2) < 0.12;
        initg_m[i] = uf(s ^ 0xE3) < 0.08;
        amount_usd[i] = amt * fx_to_usd(ccy);
        rg_m[i] = cross && amount_usd[i] >= 10_000.0;
        rmt_m[i] = uf(s ^ 0xE7) < 0.40;
        strd_m[i] = uf(s ^ 0xE9) < 0.15;
    }

    // Finish arrays (shared via Arc).
    let a = |mut x: StringBuilder| -> ArrayRef { Arc::new(x.finish()) };
    let msgid = a(b_msgid);
    let uetr = a(b_uetr);
    let txnid = a(b_txnid);
    let ccy_arr = a(b_ccy);
    let ictry = a(b_ictry);
    let purp = a(b_purp);
    let chrg = a(b_chrg);
    let clrref = a(b_clrref);
    let iprty = a(b_iprty);
    let clrch = a(b_clrch);
    let svc = a(b_svc);
    let lcl = a(b_lcl);
    let name_o = a(b_no);
    let name_c = a(b_nc);
    let st_o = a(b_sto);
    let st_c = a(b_stc);
    let tw_o = a(b_two);
    let tw_c = a(b_twc);
    let ct_o = a(b_cto);
    let ct_c = a(b_ctc);
    let iban_o = a(b_ibo);
    let iban_c = a(b_ibc);
    let lei_o = a(b_leo);
    let lei_c = a(b_lec);
    let bic_o = a(b_bico);
    let bic_c = a(b_bicc);
    let instg_bic = a(b_instg);
    let instd_bic = a(b_instd);
    let i1_bic = a(b_i1);
    let i2_bic = a(b_i2);
    let i3_bic = a(b_i3);
    let prvs_bic = a(b_prvs);

    let ts_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(b.ts_us.clone()));
    let day_arr: ArrayRef = Arc::new(Date32Array::from(day));
    let null_str: ArrayRef = Arc::new(StringArray::new_null(n));

    let sttlm_inf: ArrayRef = Arc::new(StructArray::new(
        Fields::from(vec![Field::new("sttlm_mtd", DataType::Utf8, true)]),
        vec![const_str("INDA", n)],
        None,
    ));
    let pmt_tp_inf: ArrayRef = Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("instr_prty", DataType::Utf8, true),
            Field::new("clr_chanl", DataType::Utf8, true),
            Field::new("svc_lvl", DataType::Utf8, true),
            Field::new("lcl_instrm", DataType::Utf8, true),
            Field::new("ctgy_purp", DataType::Utf8, true),
        ]),
        vec![iprty, clrch, svc, lcl, purp.clone()],
        None,
    ));

    let dbtr_bank = const_str("Debtor Bank", n);
    let cdtr_bank = const_str("Creditor Bank", n);
    let instg_bank = const_str("Instructing Bank", n);
    let instd_bank = const_str("Instructed Bank", n);
    let corr = const_str("Correspondent", n);

    let instg_agt = agent(instg_bic, lei_o.clone(), instg_bank, None);
    let instd_agt = agent(instd_bic, lei_c.clone(), instd_bank, None);
    let dbtr_agt = agent(bic_o, lei_o.clone(), dbtr_bank, None);
    let cdtr_agt = agent(bic_c, lei_c.clone(), cdtr_bank, None);
    let intrmy1 = agent(i1_bic, lei_o.clone(), corr.clone(), nb_from(&chain1));
    let intrmy2 = agent(i2_bic, lei_o.clone(), corr.clone(), nb_from(&chain2));
    let intrmy3 = agent(i3_bic, lei_o.clone(), corr.clone(), nb_from(&chain3));
    let prvs1 = agent(
        prvs_bic.clone(),
        lei_o.clone(),
        corr.clone(),
        nb_from(&prvs_m),
    );
    let allnull = vec![false; n];
    let prvs2 = agent(
        prvs_bic.clone(),
        lei_o.clone(),
        corr.clone(),
        nb_from(&allnull),
    );
    let prvs3 = agent(prvs_bic, lei_o.clone(), corr, nb_from(&allnull));

    let ultmt_dbtr = ultmt(
        name_o.clone(),
        lei_o.clone(),
        ct_o.clone(),
        nb_from(&ultd_m),
    );
    let ultmt_cdtr = ultmt(
        name_c.clone(),
        lei_c.clone(),
        ct_c.clone(),
        nb_from(&ultc_m),
    );
    let initg_pty: ArrayRef = Arc::new(StructArray::new(
        initg_fields(),
        vec![name_o.clone(), lei_o.clone()],
        nb_from(&initg_m),
    ));

    let dbtr = party(
        name_o,
        st_o,
        tw_o,
        ct_o.clone(),
        lei_o.clone(),
        null_str.clone(),
    );
    let cdtr = party(
        name_c,
        st_c,
        tw_c,
        ct_c.clone(),
        lei_c.clone(),
        null_str.clone(),
    );
    let dbtr_acct = acct(iban_o, ccy_arr.clone(), null_str.clone());
    let cdtr_acct = acct(iban_c, ccy_arr.clone(), null_str.clone());

    let rgltry = build_rgltry(&rg_m, &b.orig, &b.bene, &ct_o_ref, &ct_c_ref, &amount_usd);
    let rmt_ustrd = build_rmt_ustrd(&rmt_m, &txn_seed);
    let rmt_strd = build_rmt_strd(&strd_m, &txn_seed, &amt_i);

    let cols: Vec<ArrayRef> = vec![
        msgid,
        ts_arr,
        Arc::new(Int32Array::from(nb_txs)),
        dec18(amt_i.clone()),
        dec18(amt_i.clone()),
        day_arr,
        sttlm_inf,
        pmt_tp_inf,
        instg_agt,
        instd_agt,
        txnid.clone(),
        txnid.clone(),
        txnid,
        uetr,
        clrref,
        dec18(amt_i.clone()),
        ccy_arr,
        dec18(instd_amt_i),
        ictry,
        Arc::new(
            Decimal128Array::from(xchg_i)
                .with_precision_and_scale(11, 10)
                .unwrap(),
        ),
        chrg,
        intrmy1,
        intrmy2,
        intrmy3,
        prvs1,
        prvs2,
        prvs3,
        ultmt_dbtr,
        initg_pty,
        dbtr,
        dbtr_acct,
        dbtr_agt,
        cdtr_agt,
        cdtr,
        cdtr_acct,
        ultmt_cdtr,
        purp,
        null_str,
        rgltry,
        rmt_ustrd,
        rmt_strd,
    ];
    RecordBatch::try_new(pacs008_schema(), cols).unwrap()
}

fn ultmt(nm: ArrayRef, lei: ArrayRef, ctry: ArrayRef, nulls: Option<NullBuffer>) -> ArrayRef {
    Arc::new(StructArray::new(ultmt_fields(), vec![nm, lei, ctry], nulls))
}

fn party(
    nm: ArrayRef,
    st: ArrayRef,
    tw: ArrayRef,
    ct: ArrayRef,
    lei: ArrayRef,
    null_str: ArrayRef,
) -> ArrayRef {
    let addr = StructArray::new(
        Fields::from(vec![
            Field::new("strt_nm", DataType::Utf8, true),
            Field::new("twn_nm", DataType::Utf8, true),
            Field::new("ctry", DataType::Utf8, true),
        ]),
        vec![st, tw, ct.clone()],
        None,
    );
    let id = StructArray::new(
        Fields::from(vec![
            Field::new("any_bic", DataType::Utf8, true),
            Field::new("lei", DataType::Utf8, true),
        ]),
        vec![null_str, lei],
        None,
    );
    Arc::new(StructArray::new(
        party_fields(),
        vec![nm, Arc::new(addr), Arc::new(id), ct],
        None,
    ))
}

fn acct(iban: ArrayRef, ccy: ArrayRef, null_str: ArrayRef) -> ArrayRef {
    Arc::new(StructArray::new(
        acct_fields(),
        vec![iban, null_str, ccy],
        None,
    ))
}

fn offsets_from_counts(counts: &[i32]) -> OffsetBuffer<i32> {
    let mut off = Vec::with_capacity(counts.len() + 1);
    off.push(0i32);
    let mut acc = 0i32;
    for &c in counts {
        acc += c;
        off.push(acc);
    }
    OffsetBuffer::new(ScalarBuffer::from(off))
}

fn build_rmt_ustrd(mask: &[bool], txn_seed: &[u64]) -> ArrayRef {
    let n = mask.len();
    let mut vals = StringBuilder::new();
    let mut counts = vec![0i32; n];
    for i in 0..n {
        if mask[i] {
            let num = splitmix64(txn_seed[i] ^ 0x1DE7) % 10_000_000_000;
            vals.append_value(format!("INV {:010}", num));
            counts[i] = 1;
        }
    }
    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    Arc::new(ListArray::new(
        field,
        offsets_from_counts(&counts),
        Arc::new(vals.finish()),
        nb_from(mask),
    ))
}

fn build_rmt_strd(mask: &[bool], txn_seed: &[u64], amt_i: &[i128]) -> ArrayRef {
    let n = mask.len();
    let mut refs = StringBuilder::new();
    let mut amts = Vec::new();
    let mut counts = vec![0i32; n];
    for i in 0..n {
        if mask[i] {
            let d = splitmix64(txn_seed[i] ^ 0xD0C5) % 100_000_000;
            refs.append_value(format!("DOC-{:08}", d));
            amts.push(amt_i[i]);
            counts[i] = 1;
        }
    }
    let values = StructArray::new(
        rmt_strd_fields(),
        vec![Arc::new(refs.finish()), dec18(amts)],
        None,
    );
    let field = Arc::new(Field::new(
        "item",
        DataType::Struct(rmt_strd_fields()),
        true,
    ));
    Arc::new(ListArray::new(
        field,
        offsets_from_counts(&counts),
        Arc::new(values),
        nb_from(mask),
    ))
}

fn build_rgltry(
    mask: &[bool],
    orig: &[u64],
    bene: &[u64],
    ct_o: &[&str],
    ct_c: &[&str],
    amount_usd: &[f64],
) -> ArrayRef {
    let n = mask.len();
    let mut ind = StringBuilder::new();
    let mut auth_nm = StringBuilder::new();
    let mut auth_ct = StringBuilder::new();
    let mut det_vals = StringBuilder::new();
    let mut det_counts = Vec::new();
    let mut outer_counts = vec![0i32; n];
    for i in 0..n {
        if !mask[i] {
            continue;
        }
        outer_counts[i] = 2;
        let ro = (splitmix64(orig[i] ^ 0x9F) % 16) as usize;
        let rc = (splitmix64(bene[i] ^ 0x9F) % 16) as usize;
        let amt_str = format!("amount_usd:{:.2}", amount_usd[i]);
        ind.append_value("DEBT");
        auth_nm.append_value(REGULATORS[ro]);
        auth_ct.append_value(ct_o[i]);
        det_vals.append_value(&amt_str);
        det_vals.append_value("cross-border");
        det_counts.push(2i32);
        ind.append_value("CRED");
        auth_nm.append_value(REGULATORS[rc]);
        auth_ct.append_value(ct_c[i]);
        det_vals.append_value(&amt_str);
        det_vals.append_value("cross-border");
        det_counts.push(2i32);
    }
    let details = ListArray::new(
        rgltry_detail_field(),
        offsets_from_counts(&det_counts),
        Arc::new(det_vals.finish()),
        None,
    );
    let items = StructArray::new(
        rgltry_item_fields(),
        vec![
            Arc::new(ind.finish()),
            Arc::new(auth_nm.finish()),
            Arc::new(auth_ct.finish()),
            Arc::new(details),
        ],
        None,
    );
    let field = Arc::new(Field::new(
        "item",
        DataType::Struct(rgltry_item_fields()),
        true,
    ));
    Arc::new(ListArray::new(
        field,
        offsets_from_counts(&outer_counts),
        Arc::new(items),
        nb_from(mask),
    ))
}
