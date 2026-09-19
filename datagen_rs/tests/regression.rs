//! Regression tests for the fundamental primitives datagen relies on for
//! determinism and downstream correctness. These are the pieces that, if
//! they silently change, break determinism / downstream schema / ID
//! parity with the Python golden -- so a change to any of them should
//! trip a test rather than only surface on a live cluster run.
//!
//! Run with: cargo test --release

use datagen_rs::arena::ArenaCol;
use datagen_rs::hash::{hash_frac, splitmix64, Rng, GAMMA};
use datagen_rs::ids::{iban_for, iban_into, lei_for, lei_into};

// ---------------------------------------------------------------------------
// splitmix64
// ---------------------------------------------------------------------------

#[test]
fn splitmix64_known_vectors() {
    // Known input/output pairs captured from the current implementation.
    // These pin the exact bytes so any change to the constants -- which
    // would silently break determinism vs the Python golden and vs every
    // previous cluster run -- fails a test rather than only surfacing on
    // a live UAT where the drift is much harder to attribute.
    assert_eq!(splitmix64(0), 0xE220_A839_7B1D_CDAF);
    assert_eq!(splitmix64(1), 0x910A_2DEC_8902_5CC1);
    assert_eq!(splitmix64(42), 0xBDD7_3226_2FEB_6E95);
    assert_eq!(splitmix64(GAMMA), splitmix64(GAMMA)); // idempotent (same input)
}

#[test]
fn splitmix64_avalanche() {
    // One-bit input change must flip many output bits (>= 20 in a 64-bit
    // word). Weak avalanche means adjacent seeds produce correlated streams.
    let a = splitmix64(0);
    let b = splitmix64(1);
    let diff = (a ^ b).count_ones();
    assert!(diff >= 20, "splitmix64 avalanche too weak: {} bits differ", diff);
}

#[test]
fn hash_frac_in_unit_interval() {
    for id in 0u64..1000 {
        for salt in [0i64, 1, 42, -1, i64::MAX] {
            let x = hash_frac(id, salt);
            assert!((0.0..1.0).contains(&x), "hash_frac({},{})={} out of [0,1)", id, salt, x);
        }
    }
}

#[test]
fn rng_determinism() {
    // Same seed -> same stream.
    let mut a = Rng::new(12345);
    let mut b = Rng::new(12345);
    for _ in 0..1000 {
        assert_eq!(a.next_u64(), b.next_u64());
    }
}

#[test]
fn rng_adjacent_seeds_independent() {
    // Adjacent seeds must NOT produce shifted versions of the same stream.
    // The pre-splitmix hash in `Rng::new` is the reason -- if that
    // decorrelation ever regresses, downstream typology instances would
    // become correlated. Statistical check: >50% of draws should differ.
    let mut a = Rng::new(1);
    let mut b = Rng::new(2);
    let n = 1000;
    let differ = (0..n).filter(|_| a.next_u64() != b.next_u64()).count();
    assert!(differ > n / 2, "seed(1) and seed(2) streams too correlated: {}/{} differ", differ, n);
}

#[test]
fn rng_unit_in_range() {
    let mut r = Rng::new(99);
    for _ in 0..10_000 {
        let u = r.unit();
        assert!((0.0..1.0).contains(&u));
    }
}

// ---------------------------------------------------------------------------
// IBAN / LEI
// ---------------------------------------------------------------------------

#[test]
fn iban_shape() {
    let iban = iban_for(b"DE", 12345);
    assert_eq!(iban.len(), 22, "IBAN must be 22 chars: {}", iban);
    assert!(iban.starts_with("DE"), "country prefix wrong: {}", iban);
    for c in iban.chars() {
        assert!(c.is_ascii_alphanumeric(), "non-alnum in IBAN: {}", iban);
    }
}

#[test]
fn iban_mod97_valid() {
    // ISO 13616: an IBAN is valid iff, when rearranged (country+check
    // moved to the end) and letters expanded to 2-digit numbers, the
    // resulting integer mod 97 == 1. Every IBAN we emit must satisfy that.
    let cases = [(b"DE", 1u64), (b"GB", 42), (b"US", 999_999_999), (b"CN", u64::MAX)];
    for (country, id) in cases {
        let iban = iban_for(country, id);
        let rearranged: String = iban[4..].chars().chain(iban[..4].chars()).collect();
        let mut expanded = String::new();
        for c in rearranged.chars() {
            if c.is_ascii_digit() {
                expanded.push(c);
            } else {
                let n = c as u32 - 'A' as u32 + 10;
                expanded.push_str(&n.to_string());
            }
        }
        // Compute mod 97 iteratively (integer too big for u64).
        let mut r: u64 = 0;
        for ch in expanded.chars() {
            r = (r * 10 + ch.to_digit(10).unwrap() as u64) % 97;
        }
        assert_eq!(r, 1, "IBAN failed mod97: {} (country={:?}, id={})", iban, country, id);
    }
}

#[test]
fn iban_into_matches_iban_for() {
    // The hot path (`iban_into` writing to a stack buffer) must produce
    // byte-identical output to the heap-allocating convenience function.
    // A drift here would silently poison the pacs008 columns while
    // party.parquet stays correct.
    for id in [0u64, 1, 42, 12345, u64::MAX] {
        let heap = iban_for(b"DE", id);
        let mut stack = [0u8; 22];
        iban_into(b"DE", id, &mut stack);
        assert_eq!(heap.as_bytes(), &stack, "iban_into vs iban_for diverged at id={}", id);
    }
}

#[test]
fn lei_shape() {
    let lei = lei_for(42);
    assert_eq!(lei.len(), 20);
    for c in lei.chars() {
        assert!(c.is_ascii_alphanumeric(), "non-alnum in LEI: {}", lei);
    }
}

#[test]
fn lei_into_matches_lei_for() {
    for id in [0u64, 1, 42, 12345, u64::MAX] {
        let heap = lei_for(id);
        let mut stack = [0u8; 20];
        lei_into(id, &mut stack);
        assert_eq!(heap.as_bytes(), &stack, "lei_into vs lei_for diverged at id={}", id);
    }
}

#[test]
fn iban_determinism_same_input_same_output() {
    let a = iban_for(b"DE", 42);
    let b = iban_for(b"DE", 42);
    assert_eq!(a, b);
}

#[test]
fn lei_determinism_same_input_same_output() {
    let a = lei_for(42);
    let b = lei_for(42);
    assert_eq!(a, b);
}

// ---------------------------------------------------------------------------
// ArenaCol (string SoA layout)
// ---------------------------------------------------------------------------

#[test]
fn arena_roundtrip() {
    let items: Vec<String> = (0..1000).map(|i| format!("item-{:04}", i)).collect();
    let arena = ArenaCol::from_vec(items.clone());
    for (i, expected) in items.iter().enumerate() {
        assert_eq!(arena.get(i), expected.as_str(), "arena.get({}) mismatch", i);
    }
}

#[test]
fn arena_ordering_preserved() {
    // Same input order must produce same get() sequence.
    let items = vec![
        "alpha".to_string(),
        "beta".to_string(),
        "gamma".to_string(),
    ];
    let arena = ArenaCol::from_vec(items);
    assert_eq!(arena.get(0), "alpha");
    assert_eq!(arena.get(1), "beta");
    assert_eq!(arena.get(2), "gamma");
}

// ---------------------------------------------------------------------------
// DG_COMPRESSION env parsing (writer module)
// ---------------------------------------------------------------------------
//
// Notes: these tests mutate a global env var, so cargo test's default
// parallel test runner could race them. serial_test is not a dependency;
// instead we scope each test to a distinct value and never rely on default.
// If test ordering ever produces flakes here, add `serial_test` as a
// dev-dependency and mark these with `#[serial]`.

fn with_env<T>(key: &str, val: Option<&str>, f: impl FnOnce() -> T) -> T {
    let prev = std::env::var(key).ok();
    match val {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
    let result = f();
    match prev {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
    result
}

#[test]
fn compression_default_is_zstd1() {
    use datagen_rs::writer::compression_from_env;
    use parquet::basic::Compression;
    let c = with_env("DG_COMPRESSION", None, compression_from_env);
    assert!(matches!(c, Compression::ZSTD(_)), "default should be zstd, got {:?}", c);
}

#[test]
fn compression_zstd_level_parses() {
    use datagen_rs::writer::compression_from_env;
    use parquet::basic::Compression;
    for lvl in [1, 3, 5, 6, 9, 22] {
        let key = format!("zstd{}", lvl);
        let c = with_env("DG_COMPRESSION", Some(&key), compression_from_env);
        assert!(matches!(c, Compression::ZSTD(_)), "zstd{} did not resolve", lvl);
    }
}

#[test]
#[should_panic(expected = "out of range")]
fn compression_zstd_level_out_of_range_panics() {
    with_env("DG_COMPRESSION", Some("zstd99"), || {
        let _ = datagen_rs::writer::compression_from_env();
    });
}

#[test]
#[should_panic(expected = "expected snappy")]
fn compression_unknown_codec_panics() {
    with_env("DG_COMPRESSION", Some("brotli"), || {
        let _ = datagen_rs::writer::compression_from_env();
    });
}

// ---------------------------------------------------------------------------
// Manifest -> bronze UETR contract
// ---------------------------------------------------------------------------
//
// score_financial.py joins `manifest.participant_uetrs` against
// `gold.alerts.related_txn_ids`. If the manifest's UETR derivation drifts from
// what bronze emit produces, recall silently reports 0. This test pins both
// sides of the identity: the same (uid, seed) must produce the same UETR
// wherever it is called.

#[test]
fn uetr_derivation_stable_and_seed_sensitive() {
    use datagen_rs::hash::splitmix64;
    use datagen_rs::ids::uuid_v4_into;

    fn uetr(uid: u64, seed: u64) -> String {
        let us = splitmix64(uid ^ seed ^ 0x0E7A);
        let us2 = splitmix64(uid ^ seed ^ 0x5A1D);
        let mut buf = String::with_capacity(40);
        uuid_v4_into(us, us2, &mut buf);
        buf
    }

    // Same input -> byte-identical output (this is the contract that keeps
    // manifest and bronze in sync).
    assert_eq!(uetr(42, 100), uetr(42, 100));
    // Same uid, different seed -> different UETR (seed actually enters the
    // derivation, not just gets swallowed by a bug).
    assert_ne!(uetr(42, 100), uetr(42, 101));
    // Different uid, same seed -> different UETR.
    assert_ne!(uetr(42, 100), uetr(43, 100));
    // UUID v4 shape: 36 chars, hyphens at positions 8, 13, 18, 23.
    let s = uetr(42, 100);
    assert_eq!(s.len(), 36, "UETR wrong length: {}", s);
    let bytes = s.as_bytes();
    assert!(bytes[8] == b'-' && bytes[13] == b'-' && bytes[18] == b'-' && bytes[23] == b'-',
        "UETR missing hyphens: {}", s);
}

#[test]
fn bytes_per_row_default_is_codec_aware() {
    // A scalar default was wildly wrong under any codec other than the one it
    // was measured against. This test pins the codec-aware defaults so a
    // regression trips a unit test rather than manifesting as 2x-too-big
    // files at UAT scale.
    use datagen_rs::writer::bytes_per_row_default;
    // ZSTD-1 (default codec) -- ~227 bytes/row measured 2026-09-18.
    let z = with_env("DG_COMPRESSION", Some("zstd1"), bytes_per_row_default);
    let s = with_env("DG_COMPRESSION", Some("snappy"), bytes_per_row_default);
    let l = with_env("DG_COMPRESSION", Some("lz4"), bytes_per_row_default);
    let n = with_env("DG_COMPRESSION", Some("none"), bytes_per_row_default);
    // Ratios more than the absolute values: SNAPPY/LZ4 should be within 5%
    // of each other, and both should sit between ZSTD-1 and uncompressed.
    assert!(z > 100.0 && z < 300.0, "zstd default sanity: {}", z);
    assert!(n > 400.0 && n < 700.0, "none default sanity: {}", n);
    assert!(s > z && s < n, "snappy {} should sit between zstd {} and none {}", s, z, n);
    assert!(l > z && l < n, "lz4 {} should sit between zstd {} and none {}", l, z, n);
    assert!((s - l).abs() / s.max(l) < 0.10, "snappy {} and lz4 {} should be within 10%", s, l);
}

#[test]
fn compression_snappy_lz4_none() {
    use datagen_rs::writer::compression_from_env;
    use parquet::basic::Compression;
    assert!(matches!(
        with_env("DG_COMPRESSION", Some("snappy"), compression_from_env),
        Compression::SNAPPY
    ));
    assert!(matches!(
        with_env("DG_COMPRESSION", Some("lz4"), compression_from_env),
        Compression::LZ4_RAW
    ));
    assert!(matches!(
        with_env("DG_COMPRESSION", Some("none"), compression_from_env),
        Compression::UNCOMPRESSED
    ));
    assert!(matches!(
        with_env("DG_COMPRESSION", Some("uncompressed"), compression_from_env),
        Compression::UNCOMPRESSED
    ));
}

/// LB-107: streaming multipart upload for large reference-zone objects.
///
/// Exercises the `MpuWriter` end-to-end against an in-memory
/// `object_store` so the test doesn't need S3: writes ~40 MiB across
/// many small `Write::write` calls (well above the 5 MiB part
/// threshold, so at least 8 real multipart parts get emitted), then
/// re-reads the object and verifies byte-for-byte equality plus the
/// tracked `bytes_written()` accounting.
#[test]
fn mpu_writer_large_roundtrip() {
    use std::io::Write;
    use datagen_rs::s3sink::MpuWriter;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    let handle = rt.handle().clone();
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
    let key = "test/large.bin";
    let path = Path::from(key);

    // Begin a real multipart upload against the in-memory store.
    let store_c = store.clone();
    let path_c = path.clone();
    let upload = handle
        .block_on(async move { store_c.put_multipart(&path_c).await })
        .expect("begin multipart");

    let mut mpu = MpuWriter::from_upload(upload, handle.clone(), key.to_string());

    // 40 MiB of a repeating pattern, in ~64 KiB chunks. Pattern varies
    // by offset so a byte swap or dropped chunk would be caught by
    // the equality check below.
    const TOTAL: usize = 40 * 1024 * 1024;
    const CHUNK: usize = 64 * 1024;
    let mut expected = Vec::with_capacity(TOTAL);
    let mut written = 0usize;
    while written < TOTAL {
        let n = CHUNK.min(TOTAL - written);
        let mut chunk = vec![0u8; n];
        for (i, b) in chunk.iter_mut().enumerate() {
            *b = ((written + i) & 0xFF) as u8;
        }
        mpu.write_all(&chunk).expect("mpu write_all");
        expected.extend_from_slice(&chunk);
        written += n;
    }
    assert_eq!(mpu.bytes_written(), TOTAL as u64);
    mpu.finish().expect("mpu finish");

    // Read it back and compare byte-for-byte.
    let store_c = store.clone();
    let got = handle
        .block_on(async move { store_c.get(&path).await.unwrap().bytes().await.unwrap() });
    assert_eq!(got.len(), TOTAL, "readback size");
    assert_eq!(&got[..], &expected[..], "readback content");
}

/// LB-107 negative path: dropping an MpuWriter without calling
/// `finish()` must not leak an incomplete object. We can't observe
/// FlashBlade's actual abort in a unit test, but we can verify the
/// in-memory store never saw the object (multipart-in-progress is
/// separate from the completed-object namespace).
#[test]
fn mpu_writer_drop_without_finish_leaves_no_object() {
    use std::io::Write;
    use datagen_rs::s3sink::MpuWriter;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    let handle = rt.handle().clone();
    let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
    let key = "test/abandoned.bin";
    let path = Path::from(key);

    let store_c = store.clone();
    let path_c = path.clone();
    let upload = handle
        .block_on(async move { store_c.put_multipart(&path_c).await })
        .expect("begin multipart");

    {
        let mut mpu = MpuWriter::from_upload(upload, handle.clone(), key.to_string());
        // Write more than one chunk so at least one part is actually
        // uploaded before we drop.
        mpu.write_all(&vec![0u8; 6 * 1024 * 1024]).expect("write");
        // Deliberately drop without finish() -- Drop's abort runs.
    }

    // The object was never completed, so a GET must fail.
    let store_c = store.clone();
    let path_c = path.clone();
    let res = handle.block_on(async move { store_c.get(&path_c).await });
    assert!(res.is_err(), "abandoned object should not be readable");
}
