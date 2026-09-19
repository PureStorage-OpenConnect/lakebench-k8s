//! IBAN / LEI / BIC / UUID, bit-faithful to datagen_v2/identifiers.py where the
//! gate depends on it. The IBAN in particular MUST match Python, because the
//! gate derives each entity's graph key with the Python iban_matrix from
//! party.country; a divergent IBAN would break every graph and planted check.

use crate::hash::splitmix64;

const B36: &[u8; 36] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// mod-97 with letters expanded A=10..Z=35, matching mod97_matrix / _mod97_alnum.
#[inline]
fn mod97(bytes: &[u8]) -> u32 {
    let mut r: u32 = 0;
    for &b in bytes {
        if b.is_ascii_digit() {
            r = (r * 10 + (b - b'0') as u32) % 97;
        } else {
            r = (r * 100 + (b.to_ascii_uppercase() - b'A' + 10) as u32) % 97;
        }
    }
    r
}

/// Low `out.len()` decimal digits of `val`, big-endian, into a fixed buffer.
#[inline]
fn digits_into(val: u64, out: &mut [u8]) {
    let mut v = val;
    for i in (0..out.len()).rev() {
        out[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
}

/// Allocation-free IBAN into a 22-byte stack buffer. The emit hot loop uses
/// this to recompute the IBAN per row instead of gathering it from an
/// 11M-entity Vec<String>: the world gather is a random cache miss plus a
/// pointer chase, while this is a few dozen ALU ops on the id already in
/// registers. Byte-identical to `iban_for`, which delegates here.
///
/// The output is guaranteed ASCII (letters + digits) as long as the caller
/// passes an ASCII-alphabetic country code. The emit hot loop then wraps the
/// output in `from_utf8_unchecked` for a cheap `&str` view; that soundness
/// story rests on this invariant, so we double-check it in debug builds.
#[inline]
pub fn iban_into(country: &[u8; 2], id: u64, out: &mut [u8; 22]) {
    debug_assert!(country[0].is_ascii_alphabetic() && country[1].is_ascii_alphabetic());
    let mut body = [0u8; 18];
    digits_into(splitmix64(id ^ 0x1BA9), &mut body);
    // joined = body(18) + country(2) + "00"(2)
    let mut joined = [0u8; 22];
    joined[..18].copy_from_slice(&body);
    joined[18..20].copy_from_slice(country);
    joined[20..22].copy_from_slice(b"00");
    let r = mod97(&joined) as i64;
    let check = (1 - r).rem_euclid(97) as u64;
    out[..2].copy_from_slice(country);
    out[2] = b'0' + (check / 10) as u8;
    out[3] = b'0' + (check % 10) as u8;
    out[4..].copy_from_slice(&body);
}

/// Country-shaped 22-char IBAN with valid mod-97 check digits.
/// == identifiers.iban_matrix(country_bytes(country), id).
pub fn iban_for(country: &[u8; 2], id: u64) -> String {
    let mut out = [0u8; 22];
    iban_into(country, id, &mut out);
    // Safe: all bytes are ASCII digits/letters.
    unsafe { String::from_utf8_unchecked(out.to_vec()) }
}

/// Allocation-free LEI into a 20-byte stack buffer; see `iban_into` for why.
/// Byte-identical to `lei_for`, which delegates here.
#[inline]
pub fn lei_into(id: u64, out: &mut [u8; 20]) {
    let mut ent = [0u8; 12];
    let mut vv = splitmix64(id ^ 0x1E1_5A17);
    for i in (0..12).rev() {
        ent[i] = B36[(vv % 36) as usize];
        vv /= 36;
        if vv == 0 {
            vv = splitmix64(id ^ (i as u64 * 17));
        }
    }
    let mut body = [0u8; 18];
    body[..6].copy_from_slice(b"LBXX00");
    body[6..].copy_from_slice(&ent);
    let r_body = mod97(&body) as i64;
    let cd = (1 - r_body * 100).rem_euclid(97) as u64;
    out[..18].copy_from_slice(&body);
    out[18] = b'0' + (cd / 10) as u8;
    out[19] = b'0' + (cd % 10) as u8;
}

/// 20-char LEI with ISO 17442 mod-97 check digits. == identifiers.lei_matrix(id).
pub fn lei_for(id: u64) -> String {
    let mut out = [0u8; 20];
    lei_into(id, &mut out);
    unsafe { String::from_utf8_unchecked(out.to_vec()) }
}

/// The 500-BIC pool, identical order to identifiers.bic_pool().
pub fn bic_pool() -> Vec<String> {
    let inst = [
        "MERI", "NRTH", "BLKW", "CAMB", "PINN", "KSTR", "HALC", "CRNR", "IRNB", "SLVR",
        "ESTV", "RDWD", "ARCD", "BRWT", "LMBR", "STNM", "WLBK", "GRNF", "ASHF", "BRDG",
    ];
    let ctry = [
        "US", "GB", "DE", "FR", "CA", "JP", "SG", "CH", "AE", "IN", "MX", "CN", "AU",
        "HK", "KR", "NL",
    ];
    let loc = ["2L", "3X", "XX", "A1", "B2"];
    let branch = ["XXX", "001", "002", "LDN", "NYC"];
    (0..500)
        .map(|i| {
            format!(
                "{}{}{}{}",
                inst[i % inst.len()],
                ctry[(i / inst.len()) % ctry.len()],
                loc[(i / 7) % loc.len()],
                branch[(i / 11) % branch.len()],
            )
        })
        .collect()
}

/// BIC pool index for an entity, == splitmix64(id ^ 0xB1C0) % 500.
#[inline]
pub fn bic_idx(id: u64, pool_len: usize) -> usize {
    (splitmix64(id ^ 0xB1C0) % pool_len as u64) as usize
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Canonical v4-shaped UUID string from two 64-bit hashes. Uniqueness (not
/// byte-parity with Python) is what the gate requires (G2.11).
pub fn uuid_v4(a: u64, b: u64) -> String {
    let h1 = splitmix64(a);
    let h2 = splitmix64(b);
    let bytes = [h1.to_be_bytes(), h2.to_be_bytes()].concat();
    let mut out = Vec::with_capacity(36);
    for (i, &byte) in bytes.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            out.push(b'-');
        }
        if i == 6 {
            out.push(b'4'); // version
            out.push(HEX[(byte & 0xF) as usize]);
            continue;
        }
        if i == 8 {
            out.push(b'a'); // variant
            out.push(HEX[(byte & 0xF) as usize]);
            continue;
        }
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 0xF) as usize]);
    }
    unsafe { String::from_utf8_unchecked(out) }
}

/// 32-hex message id with full 128-bit entropy, distinct from the UETR.
pub fn msg_id(a: u64, b: u64) -> String {
    let h1 = splitmix64(a ^ 0x5AFE);
    let h2 = splitmix64(b ^ 0xBEEF);
    format!("MSG-{:016x}{:016x}", h1, h2)
}

use std::fmt::Write as _;

/// Allocation-free UETR writer: appends 36 chars into `buf` (cleared first).
pub fn uuid_v4_into(a: u64, b: u64, buf: &mut String) {
    buf.clear();
    let h1 = splitmix64(a);
    let h2 = splitmix64(b);
    let bytes = [h1.to_be_bytes(), h2.to_be_bytes()].concat();
    for (i, &byte) in bytes.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            buf.push('-');
        }
        if i == 6 {
            buf.push('4');
            buf.push(HEX[(byte & 0xF) as usize] as char);
            continue;
        }
        if i == 8 {
            buf.push('a');
            buf.push(HEX[(byte & 0xF) as usize] as char);
            continue;
        }
        buf.push(HEX[(byte >> 4) as usize] as char);
        buf.push(HEX[(byte & 0xF) as usize] as char);
    }
}

pub fn msg_id_into(a: u64, b: u64, buf: &mut String) {
    buf.clear();
    let h1 = splitmix64(a ^ 0x5AFE);
    let h2 = splitmix64(b ^ 0xBEEF);
    let _ = write!(buf, "MSG-{:016x}{:016x}", h1, h2);
}

/// TXN-{orig7}-{bene7}-{uetr8} into `buf`.
pub fn txn_id_into(orig: u64, bene: u64, uetr8: &str, buf: &mut String) {
    buf.clear();
    let _ = write!(buf, "TXN-{:07}-{:07}-{}", orig % 10_000_000, bene % 10_000_000, uetr8);
}
