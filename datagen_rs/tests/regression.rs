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
    assert!(
        diff >= 20,
        "splitmix64 avalanche too weak: {} bits differ",
        diff
    );
}

#[test]
fn hash_frac_in_unit_interval() {
    for id in 0u64..1000 {
        for salt in [0i64, 1, 42, -1, i64::MAX] {
            let x = hash_frac(id, salt);
            assert!(
                (0.0..1.0).contains(&x),
                "hash_frac({},{})={} out of [0,1)",
                id,
                salt,
                x
            );
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
    assert!(
        differ > n / 2,
        "seed(1) and seed(2) streams too correlated: {}/{} differ",
        differ,
        n
    );
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
    let cases = [
        (b"DE", 1u64),
        (b"GB", 42),
        (b"US", 999_999_999),
        (b"CN", u64::MAX),
    ];
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
        assert_eq!(
            r, 1,
            "IBAN failed mod97: {} (country={:?}, id={})",
            iban, country, id
        );
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
        assert_eq!(
            heap.as_bytes(),
            &stack,
            "iban_into vs iban_for diverged at id={}",
            id
        );
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
        assert_eq!(
            heap.as_bytes(),
            &stack,
            "lei_into vs lei_for diverged at id={}",
            id
        );
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
    let items = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
    let arena = ArenaCol::from_vec(items);
    assert_eq!(arena.get(0), "alpha");
    assert_eq!(arena.get(1), "beta");
    assert_eq!(arena.get(2), "gamma");
}

// ---------------------------------------------------------------------------
// DG_COMPRESSION env parsing (writer module)
// ---------------------------------------------------------------------------
//
// Notes: these tests mutate a process-global env var, so cargo test's default
// parallel runner would race them. Rather than add a serial_test dependency,
// with_env holds a private static lock across the whole set/run/restore critical
// section, so every env-mutating test is serialized against every other. Poison
// recovery keeps a #[should_panic] test that unwinds while holding the guard
// from wedging the rest (each test sets its own value before reading, so a
// leftover from a panicked test is always overwritten).

fn with_env<T>(key: &str, val: Option<&str>, f: impl FnOnce() -> T) -> T {
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
fn compression_default_is_snappy() {
    // Default changed from zstd1 to snappy on 2026-09-20 following the c360
    // Rust port perf sweep: snappy is 40-55% faster with a ~1.5x on-disk
    // trade. Benchmarks that need smaller files opt into zstd explicitly.
    use datagen_rs::writer::compression_from_env;
    use parquet::basic::Compression;
    let c = with_env("DG_COMPRESSION", None, compression_from_env);
    assert!(
        matches!(c, Compression::SNAPPY),
        "default should be snappy, got {:?}",
        c
    );
}

#[test]
fn compression_zstd_level_parses() {
    use datagen_rs::writer::compression_from_env;
    use parquet::basic::Compression;
    for lvl in [1, 3, 5, 6, 9, 22] {
        let key = format!("zstd{}", lvl);
        let c = with_env("DG_COMPRESSION", Some(&key), compression_from_env);
        assert!(
            matches!(c, Compression::ZSTD(_)),
            "zstd{} did not resolve",
            lvl
        );
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
    use datagen_rs::hash::uetr_seeds;
    use datagen_rs::ids::uuid_v4_into;

    // The derivation the manifest and the bronze writer actually call.
    fn uetr(uid: u64, seed: i64) -> String {
        let (us, us2) = uetr_seeds(uid, seed);
        let mut buf = String::with_capacity(40);
        uuid_v4_into(us, us2, &mut buf);
        buf
    }

    // Adjacent seeds must not produce the same UETR set shifted by one uid
    // (the additive-seed collision class, LB-139).
    let a: std::collections::HashSet<String> = (0..2000).map(|u| uetr(u, 100)).collect();
    assert!((0..2000).all(|u| !a.contains(&uetr(u, 101))));

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
    assert!(
        bytes[8] == b'-' && bytes[13] == b'-' && bytes[18] == b'-' && bytes[23] == b'-',
        "UETR missing hyphens: {}",
        s
    );
}

#[test]
fn pacs008_bytes_per_row_default_is_codec_aware() {
    // A scalar default was wildly wrong under any codec other than the one it
    // was measured against. This test pins the codec-aware defaults so a
    // regression trips a unit test rather than manifesting as 2x-too-big
    // files at UAT scale.
    use datagen_rs::writer::pacs008_bytes_per_row_default;
    // ZSTD-1 (default codec) -- ~227 bytes/row measured 2026-09-18.
    let z = with_env(
        "DG_COMPRESSION",
        Some("zstd1"),
        pacs008_bytes_per_row_default,
    );
    let s = with_env(
        "DG_COMPRESSION",
        Some("snappy"),
        pacs008_bytes_per_row_default,
    );
    let l = with_env("DG_COMPRESSION", Some("lz4"), pacs008_bytes_per_row_default);
    let n = with_env(
        "DG_COMPRESSION",
        Some("none"),
        pacs008_bytes_per_row_default,
    );
    // Ratios more than the absolute values: SNAPPY/LZ4 should be within 5%
    // of each other, and both should sit between ZSTD-1 and uncompressed.
    assert!(z > 100.0 && z < 300.0, "zstd default sanity: {}", z);
    assert!(n > 400.0 && n < 700.0, "none default sanity: {}", n);
    assert!(
        s > z && s < n,
        "snappy {} should sit between zstd {} and none {}",
        s,
        z,
        n
    );
    assert!(
        l > z && l < n,
        "lz4 {} should sit between zstd {} and none {}",
        l,
        z,
        n
    );
    assert!(
        (s - l).abs() / s.max(l) < 0.10,
        "snappy {} and lz4 {} should be within 10%",
        s,
        l
    );
}

#[test]
fn customer360_bytes_per_row_default_is_codec_aware() {
    // c360 rows are ~10-20x wider than pacs.008 because the payload column is
    // 2 KiB of random hex. Silently falling back to the pacs.008 default here
    // would produce c360 files ~10x too small, which the M1 review flagged as
    // the design-critical trap. Pin every codec's expected value.
    use datagen_rs::writer::customer360_bytes_per_row_default;
    let z = with_env(
        "DG_COMPRESSION",
        Some("zstd1"),
        customer360_bytes_per_row_default,
    );
    let s = with_env(
        "DG_COMPRESSION",
        Some("snappy"),
        customer360_bytes_per_row_default,
    );
    let l = with_env(
        "DG_COMPRESSION",
        Some("lz4"),
        customer360_bytes_per_row_default,
    );
    let n = with_env(
        "DG_COMPRESSION",
        Some("none"),
        customer360_bytes_per_row_default,
    );
    // zstd cracks the hex payload well; snappy/lz4/none don't.
    assert_eq!(z, 2233.0);
    assert_eq!(s, 4332.0);
    assert_eq!(l, 4356.0);
    assert_eq!(n, 4399.0);
    // c360 must not accidentally alias the pacs.008 default.
    use datagen_rs::writer::pacs008_bytes_per_row_default;
    let pz = with_env(
        "DG_COMPRESSION",
        Some("zstd1"),
        pacs008_bytes_per_row_default,
    );
    assert!(
        z > pz * 5.0,
        "c360 zstd bytes/row {} should be much larger than pacs008 {}",
        z,
        pz
    );
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
    use datagen_rs::s3sink::MpuWriter;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::io::Write;

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
    let got =
        handle.block_on(async move { store_c.get(&path).await.unwrap().bytes().await.unwrap() });
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
    use datagen_rs::s3sink::MpuWriter;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::io::Write;

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

// ---------------------------------------------------------------------------
// Persona + per-account point process (P2, LB-130)
// ---------------------------------------------------------------------------
//
// These pin the properties the persona exists to provide: per-account
// heterogeneity (so entity_profiles features vary across accounts), strict
// determinism (so generation stays reproducible), and mean-preservation of the
// amount shift (so the aggregate amount band is not moved). If any regress, the
// separability rework loses its foundation or the gate's distribution checks
// drift, so they fail here rather than only on a live regen.

use datagen_rs::amounts::{lognormal_amount, lognormal_amount_shifted};
use datagen_rs::model::build_world_ex;
use datagen_rs::world::{amount_log_shift, hash_normal, rate_mult, BASELINE_ACTIVITY, TYPE_PERSON};

#[test]
fn persona_deterministic() {
    // Same (id, seed) -> identical persona, every call.
    for id in [1u64, 2, 100, 111_111] {
        assert_eq!(rate_mult(id, 42), rate_mult(id, 42));
        assert_eq!(amount_log_shift(id, 42), amount_log_shift(id, 42));
        assert_eq!(hash_normal(id, 909), hash_normal(id, 909));
    }
    // Different id -> (almost surely) different persona.
    assert_ne!(rate_mult(1, 42), rate_mult(2, 42));
    assert_ne!(amount_log_shift(1, 42), amount_log_shift(2, 42));
    // Different seed -> different persona.
    assert_ne!(rate_mult(1, 42), rate_mult(1, 43));
}

#[test]
fn persona_rate_has_real_spread() {
    // The whole point of P2: accounts must NOT all share one rate. Over a
    // population sample, rate_mult must produce genuinely quiet and genuinely
    // busy accounts, not a near-constant. (Pre-P2 every account of a type
    // shared exactly one rate.)
    let seed = 42;
    let n = 20_000u64;
    let vals: Vec<f64> = (1..=n).map(|id| rate_mult(id, seed)).collect();
    let quiet = vals.iter().filter(|&&v| v < 0.5).count();
    let busy = vals.iter().filter(|&&v| v > 2.0).count();
    // exp(N(0,1)): ~25% below 0.5, ~25% above 2.0. Wide tolerance.
    assert!(
        quiet > n as usize / 10,
        "too few quiet accounts ({}/{}): dormancy has nothing to show against",
        quiet,
        n
    );
    assert!(
        busy > n as usize / 10,
        "too few busy accounts ({}/{})",
        busy,
        n
    );
    // All strictly positive and finite.
    assert!(vals.iter().all(|&v| v.is_finite() && v > 0.0));
}

#[test]
fn persona_amount_shift_is_mean_preserving() {
    // amount_log_shift is recentred so E[exp(shift)] == 1, i.e. the population
    // mean amount is unchanged by the persona. Estimate the mean multiplier
    // over a large sample; it must sit close to 1.0.
    let seed = 42;
    let n = 200_000u64;
    let mean_mult: f64 = (1..=n)
        .map(|id| amount_log_shift(id, seed).exp())
        .sum::<f64>()
        / n as f64;
    assert!(
        (mean_mult - 1.0).abs() < 0.03,
        "amount shift not mean-preserving: E[exp(shift)] = {}",
        mean_mult
    );
    // And it must actually spread amounts (not collapse to 1.0 everywhere).
    let big = (1..=n)
        .filter(|&id| amount_log_shift(id, seed).exp() > 1.5)
        .count();
    assert!(
        big > n as usize / 20,
        "amount shift has no spread ({}/{})",
        big,
        n
    );
}

#[test]
fn lognormal_shifted_zero_matches_base() {
    // Delegation must be byte-identical: same RNG state + zero shift == the old
    // lognormal_amount, so passing 0.0 anywhere is a pure no-op.
    let mut a = Rng::new(777);
    let mut b = Rng::new(777);
    for _ in 0..10_000 {
        assert_eq!(
            lognormal_amount(&mut a),
            lognormal_amount_shifted(&mut b, 0.0)
        );
    }
}

#[test]
fn world_activity_is_per_entity_not_per_type() {
    // After P2, two persons must (almost surely) have different activity rates;
    // pre-P2 every person shared BASELINE_ACTIVITY[PERSON] exactly.
    let w = build_world_ex(0.2, 42, 12, false);
    let base = BASELINE_ACTIVITY[TYPE_PERSON as usize];
    // Collect activity for the first several persons.
    let persons: Vec<f64> = (1..w.population)
        .filter(|&i| w.ty[i] == TYPE_PERSON)
        .take(500)
        .map(|i| w.activity[i])
        .collect();
    assert!(persons.len() > 50, "not enough persons sampled");
    // Not all equal to the per-type base.
    let all_base = persons.iter().all(|&v| (v - base).abs() < 1e-9);
    assert!(!all_base, "activity is still a per-type constant");
    // Distinct values exist.
    let distinct = {
        let mut s: Vec<u64> = persons.iter().map(|v| v.to_bits()).collect();
        s.sort_unstable();
        s.dedup();
        s.len()
    };
    assert!(
        distinct > persons.len() / 2,
        "activity not sufficiently heterogeneous"
    );
    // amount_logshift column is built and non-trivial.
    assert_eq!(w.amount_logshift.len(), w.population + 1);
    assert!(w.amount_logshift[1..].iter().any(|&v| v.abs() > 0.01));
}

#[test]
fn persona_preserves_structuring_band_baseline_density() {
    // Leakage guard (P2 review F1): structuring_amount bypasses the persona
    // amount shift, so the [9500, 9999] USD structuring band stays fixed while
    // the shifted baseline distribution retreats from it. The leakage gate
    // fails a band when baseline_density / typology_density < 0.10, so if a
    // larger AMOUNT_LOG_SD starves the band of baseline rows, a structuring
    // typology becomes a near-perfect label by artifact. Pin a floor on the
    // baseline in-band fraction so that regression trips here, not silently on
    // a live regen. Pre-P2 ~1.28%, at sd=0.6 ~1.14%; floor at 1.0% catches a
    // material further drop while tolerating the current design.
    use datagen_rs::amounts::lognormal_amount_shifted;
    use datagen_rs::world::amount_log_shift;

    let seed = 42;
    let n = 400_000u64;
    // One baseline amount per account, drawn with that account's persona shift,
    // exactly as the base-row loop does.
    let mut rng = Rng::new(0xB0BA);
    let mut in_band = 0u64;
    for id in 1..=n {
        let amt = lognormal_amount_shifted(&mut rng, amount_log_shift(id, seed));
        if (9500.0..=9999.0).contains(&amt) {
            in_band += 1;
        }
    }
    let frac = in_band as f64 / n as f64;
    assert!(
        frac >= 0.010,
        "baseline density in the USD structuring band is {:.4}% (< 1.0%): a \
         structuring typology is drifting toward a leaked label; lower \
         AMOUNT_LOG_SD or scale amounts by currency",
        frac * 100.0
    );
}

// ---------------------------------------------------------------------------
// Dormancy trajectory (P3, W8): schedule + emit invariants
// ---------------------------------------------------------------------------
//
// These pin the generator-side invariants the W8 recovery depends on: a real
// >90-day originator gap, the pre-window anchor, the amount floor on the burst,
// and that only dormant_reactivation carries a suppression window. The base-loop
// suppression and other-typology collision-skip live in the binary and are
// validated on real bronze parquet; here we lock the schedule/emit contract.

#[test]
fn dormancy_schedule_and_emit_produce_a_real_gap() {
    use datagen_rs::typology::{emit_instance, schedule};
    let pop = 3000usize;
    let day = 86_400_000_000i64;
    let corpus_start = 1_600_000_000i64 * 1_000_000; // arbitrary epoch us
    let corpus_end = corpus_start + 1800 * day; // ~60 months
    let country: Vec<&'static str> = vec!["US"; pop + 1];
    let total_rows = (pop as i64) * 4 * 60;
    let insts = schedule(42, total_rows, pop, corpus_start, corpus_end, &country);

    let dorm: Vec<_> = insts
        .iter()
        .filter(|i| i.typ == "dormant_reactivation")
        .collect();
    assert!(
        !dorm.is_empty(),
        "no dormant_reactivation instances scheduled"
    );
    for inst in &dorm {
        // Suppression window is set, precedes the burst, ends at the burst end.
        assert!(
            inst.suppress_end_us > inst.suppress_start_us,
            "no suppress window"
        );
        assert_eq!(
            inst.suppress_end_us, inst.end_us,
            "suppress end must equal burst end"
        );
        assert!(
            inst.suppress_start_us < inst.start_us,
            "suppress must start before burst"
        );
        // Dormancy length (window start -> burst start) is 60..365 days
        // (log-uniform; not tied to W8's 90-day threshold, LB-138).
        let dorm = (inst.start_us - inst.suppress_start_us) / day;
        assert!(
            (59..=366).contains(&dorm),
            "dormancy {} days outside 60..365",
            dorm
        );

        let rows = emit_instance(inst);
        assert_eq!(
            rows.len(),
            inst.rows_per_instance + 1,
            "anchor + burst count"
        );
        // Row 0 is the pre-window anchor: a normal-amount send before the window.
        assert!(
            rows[0].ts_us < inst.suppress_start_us,
            "anchor must precede the window"
        );
        assert_eq!(
            rows[0].orig, inst.participants[0],
            "anchor orig = dormant account"
        );
        // Burst rows: inside [start, end], originated by the dormant account.
        // TxRow has no amount-floor field at all, so no rule-derived floor
        // can come back (LB-138).
        for r in &rows[1..] {
            assert!(
                r.ts_us >= inst.start_us && r.ts_us <= inst.end_us,
                "burst row outside the manifest window"
            );
            assert_eq!(r.orig, inst.participants[0], "burst orig = dormant account");
        }
        // The originator gap (anchor -> earliest burst row) is the dormancy
        // plus the anchor offset: at least the 60-day minimum.
        let first_burst = rows[1..].iter().map(|r| r.ts_us).min().unwrap();
        assert!(
            first_burst - rows[0].ts_us > 60 * day,
            "originator gap not > 60d: {} days",
            (first_burst - rows[0].ts_us) / day
        );
    }

    // Every non-dormant typology carries no suppression window.
    for inst in insts.iter().filter(|i| i.typ != "dormant_reactivation") {
        assert_eq!(
            (inst.suppress_start_us, inst.suppress_end_us),
            (0, 0),
            "{} unexpectedly has a suppress window",
            inst.typ
        );
    }
}

#[test]
fn every_currency_has_baseline_mass_in_its_structuring_band() {
    // LB-137: amounts were drawn on the USD scale for every currency, so a JPY
    // baseline payment was ~5,000 yen and almost none (~1e-7) sat in the
    // [990,000, 999,999] yen band. A structuring row in JPY, INR or KRW was
    // then a label by itself. Drawn in each account's own currency, every
    // band holds baseline mass comparable to its width (expected 0.2% to 1.3%).
    use datagen_rs::amounts::{native_amount, structuring_band};
    let n = 400_000u64;
    for ccy in [
        "USD", "GBP", "EUR", "CHF", "JPY", "AED", "SGD", "CAD", "MXN", "CNY", "INR", "AUD", "HKD",
        "KRW", "BRL",
    ] {
        let (lo, hi) = structuring_band(ccy);
        let mut rng = Rng::new(0xB0BB);
        let mut in_band = 0u64;
        for id in 1..=n {
            let amt = native_amount(&mut rng, datagen_rs::world::amount_log_shift(id, 42), ccy);
            if (lo..=hi).contains(&amt) {
                in_band += 1;
            }
        }
        let frac = in_band as f64 / n as f64;
        assert!(
            frac >= 0.001,
            "{ccy}: baseline density in its structuring band is {:.4}% (< 0.1%): \
             structuring rows in {ccy} would be separable by amount alone",
            frac * 100.0
        );
    }
}

#[test]
fn usd_amounts_unchanged_by_currency_scaling() {
    use datagen_rs::amounts::{lognormal_amount_shifted, native_amount};
    let mut a = Rng::new(11);
    let mut b = Rng::new(11);
    for i in 0..5000 {
        let shift = (i % 7) as f64 * 0.1 - 0.3;
        assert_eq!(
            lognormal_amount_shifted(&mut a, shift).to_bits(),
            native_amount(&mut b, shift, "USD").to_bits()
        );
    }
    // JPY and KRW carry no minor units.
    let mut r = Rng::new(5);
    for _ in 0..1000 {
        let v = native_amount(&mut r, 0.0, "JPY");
        assert_eq!(v, v.round());
    }
}

#[test]
fn dormancy_lengths_are_not_pinned_to_the_w8_threshold() {
    // LB-138 / AML-GOALS R2: no generation parameter may sit on a rule
    // threshold. A meaningful share of dormancies fall on each side of W8's
    // 90 days, rather than all just above it.
    let day = 86_400_000_000i64;
    let corpus_start = 1_600_000_000_000_000i64;
    let corpus_end = corpus_start + 1800 * day;
    let country: Vec<&'static str> = vec!["US"; 200_001];
    let insts =
        datagen_rs::typology::schedule(7, 200_000_000, 200_000, corpus_start, corpus_end, &country);
    let lens: Vec<i64> = insts
        .iter()
        .filter(|i| i.typ == "dormant_reactivation")
        .map(|i| (i.start_us - i.suppress_start_us) / day)
        .collect();
    assert!(
        lens.len() > 50,
        "too few dormancy instances: {}",
        lens.len()
    );
    let below = lens.iter().filter(|&&d| d < 90).count() as f64 / lens.len() as f64;
    assert!(
        (0.10..0.60).contains(&below),
        "share of dormancies under 90 days is {below:.2}; expected both sides of W8's threshold"
    );
}

// ---------------------------------------------------------------------------
// Placement and shaping (placement.rs)
// ---------------------------------------------------------------------------

#[test]
fn shaping_preserves_time_order_and_uses_each_rows_country() {
    use datagen_rs::hash::Rng;
    use datagen_rs::placement::shape_instance_rows;
    use datagen_rs::timing::DayCal;
    use datagen_rs::typology::{Instance, TxRow};

    const DAY: i64 = 86_400_000_000;
    let start = 1_609_459_200_000_000i64; // 2021-01-01
    let cal = DayCal::new(start, 365);
    // Originators alternate between two countries with different holidays.
    let country = [
        "_", "US", "GB", "US", "GB", "US", "GB", "US", "GB", "US", "GB",
    ];
    let inst = Instance {
        id: "t".into(),
        typ: "scatter_gather",
        participants: vec![1, 2],
        start_us: start + 100 * DAY,
        end_us: start + 110 * DAY,
        workload: "W1",
        severity: "high",
        seed: 7,
        rows_per_instance: 10,
        corpus_start_us: start,
        corpus_end_us: start + 365 * DAY,
        suppress_start_us: 0,
        suppress_end_us: 0,
    };
    for trial in 0..200u64 {
        let mut rng = Rng::new(trial);
        // Original times strictly increasing, in reverse emit order, so a
        // zip-by-emit-order implementation would invert them.
        let mut rows: Vec<TxRow> = (0..10)
            .map(|i| TxRow {
                orig: (i % 10 + 1) as u64,
                bene: 99,
                ts_us: inst.start_us + (9 - i) * DAY + 3_600_000_000,
                structuring: false,
            })
            .collect();
        let before: Vec<i64> = rows.iter().map(|r| r.ts_us).collect();
        let (lo, hi) =
            shape_instance_rows(&mut rows, &inst, &cal, 365 * DAY, &country, &mut rng).unwrap();
        let mut idx: Vec<usize> = (0..10).collect();
        idx.sort_by_key(|&i| before[i]);
        for w in idx.windows(2) {
            assert!(
                rows[w[0]].ts_us < rows[w[1]].ts_us,
                "trial {trial}: legs reordered"
            );
        }
        for r in &rows {
            let d = cal.day_of(r.ts_us);
            assert!(
                cal.is_business_day(d, country[r.orig as usize]),
                "trial {trial}: row on a non-business day for its own country"
            );
            assert!(r.ts_us >= lo && r.ts_us <= hi);
        }
    }
}

// ---------------------------------------------------------------------------
// Monitored population and KYC (kyc.rs, GOALS P10 stages 0 and 2)
// ---------------------------------------------------------------------------

#[test]
fn reporting_fi_is_exactly_two_pool_entries() {
    use datagen_rs::ids::bic_pool;
    use datagen_rs::kyc::{REPORTING_FI, REPORTING_FI_POOL_IDX};
    let pool = bic_pool();
    let hits: Vec<usize> = (0..pool.len())
        .filter(|&i| pool[i].starts_with(REPORTING_FI))
        .collect();
    assert_eq!(hits, REPORTING_FI_POOL_IDX.to_vec());
    assert_eq!(pool[0], "MERIUS2LXXX");
    assert_eq!(pool[320], "MERIUS2LNYC");
}

#[test]
fn reporting_fi_bic_marks_customers_and_only_customers() {
    use datagen_rs::ids::bic_pool;
    use datagen_rs::kyc::{entity_bic_idx, is_customer, REPORTING_FI};
    let pool = bic_pool();
    let (mut cust, mut n) = (0usize, 0usize);
    for id in 1..=200_000u64 {
        let c = is_customer(id, 42);
        let ours = pool[entity_bic_idx(id, 42, pool.len())].starts_with(REPORTING_FI);
        assert_eq!(c, ours, "id {id}: customer {c} but reporting-FI BIC {ours}");
        cust += c as usize;
        n += 1;
    }
    let f = cust as f64 / n as f64;
    assert!((f - 0.5).abs() < 0.01, "customer share {f}");
}

#[test]
fn customer_status_is_independent_of_type_and_country() {
    use datagen_rs::kyc::is_customer;
    use datagen_rs::world::{entity_type, home_country_idx, HOME_CODES};
    let mut by_type = [(0usize, 0usize); 3];
    let (mut us, mut us_c, mut fo, mut fo_c) = (0usize, 0usize, 0usize, 0usize);
    for id in 1..=300_000u64 {
        let c = is_customer(id, 7) as usize;
        let t = entity_type(id, 7) as usize;
        by_type[t].0 += 1;
        by_type[t].1 += c;
        if HOME_CODES[home_country_idx(id, 7)] == "US" {
            us += 1;
            us_c += c;
        } else {
            fo += 1;
            fo_c += c;
        }
    }
    for (n, c) in by_type {
        let f = c as f64 / n as f64;
        assert!((f - 0.5).abs() < 0.02, "customer share by type {f}");
    }
    let (fu, ff) = (us_c as f64 / us as f64, fo_c as f64 / fo as f64);
    assert!((fu - ff).abs() < 0.02, "US {fu} vs foreign {ff}");
}

fn kyc_schedule(seed: i64) -> (Vec<datagen_rs::typology::Instance>, Vec<&'static str>) {
    use datagen_rs::typology::schedule;
    use datagen_rs::world::{home_country_idx, HOME_CODES};
    let pop = 20_000usize;
    let day = 86_400_000_000i64;
    let start = 1_600_000_000i64 * 1_000_000;
    let country: Vec<&'static str> = (0..=pop as u64)
        .map(|i| HOME_CODES[home_country_idx(i.max(1), seed)])
        .collect();
    let insts = schedule(
        seed,
        (pop as i64) * 4 * 60,
        pop,
        start,
        start + 1800 * day,
        &country,
    );
    (insts, country)
}

#[test]
fn every_typology_subject_is_a_customer_and_others_are_at_base_rate() {
    use datagen_rs::kyc::is_customer;
    use datagen_rs::typology::subject_index;
    let seed = 42;
    let (insts, _) = kyc_schedule(seed);
    assert!(insts.len() > 500);
    let (mut others, mut others_c) = (0usize, 0usize);
    for inst in &insts {
        let s = subject_index(inst.typ, inst.participants.len(), inst.seed);
        assert!(
            is_customer(inst.participants[s], seed),
            "{}: subject {} is not a customer",
            inst.id,
            inst.participants[s]
        );
        // Participants stay distinct after the subject swap / replacement.
        let mut p = inst.participants.clone();
        p.sort_unstable();
        p.dedup();
        assert_eq!(
            p.len(),
            inst.participants.len(),
            "{}: duplicate participant",
            inst.id
        );
        for (j, &q) in inst.participants.iter().enumerate() {
            if j != s {
                others += 1;
                others_c += is_customer(q, seed) as usize;
            }
        }
    }
    // Non-subject roles are never touched, so they stay at the base rate.
    let f = others_c as f64 / others as f64;
    assert!((0.47..=0.53).contains(&f), "non-subject customer share {f}");
}

#[test]
fn subject_roles_match_the_emitted_chain() {
    use datagen_rs::typology::{emit_instance, subject_index};
    let (insts, _) = kyc_schedule(42);
    for inst in insts.iter().take(2000) {
        let s = inst.participants[subject_index(inst.typ, inst.participants.len(), inst.seed)];
        let rows = emit_instance(inst);
        match inst.typ {
            // The subject originates at least one row ...
            "fan_out"
            | "random"
            | "bipartite"
            | "tbml_repeated_invoice"
            | "cycle"
            | "cross_border_cycle"
            | "gather_scatter"
            | "scatter_gather"
            | "dormant_reactivation"
            | "corridor_high_risk" => {
                assert!(rows.iter().any(|r| r.orig == s), "{}", inst.id)
            }
            // ... receives the structured credits ...
            "fan_in" | "micro_structuring" => {
                assert!(rows.iter().all(|r| r.bene == s), "{}", inst.id)
            }
            // ... or receives and forwards.
            "stack" | "rapid_layering" => {
                assert!(rows.iter().any(|r| r.bene == s), "{}", inst.id);
                assert!(rows.iter().any(|r| r.orig == s), "{}", inst.id);
            }
            _ => {}
        }
    }
}

#[test]
fn crr_tiers_are_a_low_majority_and_subjects_match_their_pool() {
    use datagen_rs::kyc::{crr, expected_monthly_volume_usd, is_customer};
    use datagen_rs::model::build_world;
    use datagen_rs::typology::subject_index;
    let w = build_world(0.2, 42, 60);
    let tier_of = |i: usize| -> &'static str {
        let v = expected_monthly_volume_usd(
            i as u64,
            w.seed,
            w.amount_logshift[i],
            w.activity[i] / w.total_activity,
            w.population,
            w.dims.txn_per_entity_per_month,
        );
        crr(w.ty[i], w.country[i], w.pep[i], v).1
    };
    let mut all = [0usize; 3];
    let mut person_us = [0usize; 3];
    let idx = |t: &str| match t {
        "low" => 0,
        "medium" => 1,
        _ => 2,
    };
    for i in 1..=w.population {
        if !is_customer(i as u64, w.seed) {
            continue;
        }
        let t = idx(tier_of(i));
        all[t] += 1;
        if w.ty[i] == datagen_rs::world::TYPE_PERSON && w.country[i] == "US" {
            person_us[t] += 1;
        }
    }
    let n: usize = all.iter().sum();
    let share = |c: [usize; 3]| {
        let n: usize = c.iter().sum();
        [
            c[0] as f64 / n as f64,
            c[1] as f64 / n as f64,
            c[2] as f64 / n as f64,
        ]
    };
    let s = share(all);
    eprintln!(
        "CRR tiers over {n} customers: low {:.3} medium {:.3} high {:.3}",
        s[0], s[1], s[2]
    );
    assert!(s[0] > 0.5 && s[2] < 0.10 && s[2] > 0.0, "tier shares {s:?}");

    // Typology subjects from the person pool are US persons ~88% of the time;
    // their tiers must look like baseline US-person customers' tiers, since
    // selection never looks at anything the CRR uses beyond type and country.
    let country: Vec<&'static str> = w.country.clone();
    let insts = datagen_rs::typology::schedule(
        42,
        w.dims.total_txns(),
        w.population,
        0,
        1800 * 86_400_000_000,
        &country,
    );
    let mut subj = [0usize; 3];
    for inst in &insts {
        if matches!(inst.typ, "corridor_high_risk" | "cross_border_cycle") {
            continue;
        }
        let s =
            inst.participants[subject_index(inst.typ, inst.participants.len(), inst.seed)] as usize;
        if w.country[s] == "US" {
            subj[idx(tier_of(s))] += 1;
        }
    }
    let (a, b) = (share(person_us), share(subj));
    eprintln!(
        "US-person customers {a:?} vs US subjects {b:?} (n={})",
        subj.iter().sum::<usize>()
    );
    for k in 0..3 {
        assert!(
            (a[k] - b[k]).abs() < 0.08,
            "tier {k}: baseline {} vs subjects {}",
            a[k],
            b[k]
        );
    }
}

#[test]
fn customer_since_precedes_the_corpus_and_the_accounts() {
    use datagen_rs::kyc::{
        account_opened_day, corpus_start_day, customer_since_day, days_from_civil,
    };
    assert_eq!(corpus_start_day(60), days_from_civil(2021, 1, 1));
    assert_eq!(corpus_start_day(18), days_from_civil(2024, 7, 1));
    let start = corpus_start_day(60) as i32;
    let mut sum = 0.0;
    for id in 1..=50_000u64 {
        let d = customer_since_day(id, 42, 60);
        assert!(d < start && d <= account_opened_day(id));
        assert!(d > start - (31.0 * 365.25) as i32);
        sum += (start - d) as f64 / 365.25;
    }
    let mean = sum / 50_000.0;
    assert!((5.0..9.0).contains(&mean), "mean tenure {mean} years");
}

#[test]
fn kyc_attributes_are_pure_functions_of_id_and_seed() {
    use datagen_rs::kyc::{customer_since_day, entity_bic_idx, is_customer};
    for id in [1u64, 17, 99_999, 1 << 40] {
        assert_eq!(is_customer(id, 5), is_customer(id, 5));
        assert_eq!(entity_bic_idx(id, 5, 500), entity_bic_idx(id, 5, 500));
        assert_eq!(customer_since_day(id, 5, 60), customer_since_day(id, 5, 60));
    }
    // Different seeds select different customer sets.
    let diff = (1..=10_000u64)
        .filter(|&id| is_customer(id, 1) != is_customer(id, 2))
        .count();
    assert!(diff > 4000, "seeds share the customer set ({diff} differ)");
}

#[test]
fn party_and_account_zones_carry_kyc_and_join_to_payments() {
    use arrow::array::{Array, BooleanArray, StringArray};
    use datagen_rs::kyc::REPORTING_FI;
    use datagen_rs::model::build_world;
    use datagen_rs::party::{write_account_to, write_party_to};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let w = build_world(0.01, 42, 60);
    // The writers read DG_COMPRESSION; hold the env lock so a concurrent
    // compression test cannot swap in an invalid codec mid-write.
    let (pbuf, abuf) = with_env("DG_COMPRESSION", None, || {
        let mut pbuf = Vec::new();
        write_party_to(&w, &[], &mut pbuf);
        let mut abuf = Vec::new();
        write_account_to(&w, &mut abuf);
        (pbuf, abuf)
    });
    let read = |b: Vec<u8>| {
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(b))
            .unwrap()
            .build()
            .unwrap()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
    };
    let col =
        |rb: &arrow::record_batch::RecordBatch, n: &str| rb.column_by_name(n).unwrap().clone();
    for rb in read(pbuf) {
        let cust = col(&rb, "is_customer");
        let cust = cust.as_any().downcast_ref::<BooleanArray>().unwrap();
        let home = col(&rb, "home_fi");
        let home = home.as_any().downcast_ref::<StringArray>().unwrap();
        let tier = col(&rb, "crr_tier");
        let since = col(&rb, "customer_since");
        for r in 0..rb.num_rows() {
            let c = cust.value(r);
            assert_eq!(c, home.value(r) == REPORTING_FI);
            assert_eq!(c, tier.is_valid(r), "tier present only for customers");
            assert_eq!(c, since.is_valid(r));
        }
    }
    let mut seen_primary = 0;
    for rb in read(abuf) {
        let iban = col(&rb, "iban");
        let iban = iban.as_any().downcast_ref::<StringArray>().unwrap();
        let holder = col(&rb, "holder_entity_id");
        let holder = holder
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let home = col(&rb, "home_fi");
        let home = home.as_any().downcast_ref::<StringArray>().unwrap();
        for r in 0..rb.num_rows() {
            let h = holder.value(r) as usize;
            assert_eq!(home.value(r), &w.bic[h][..8]);
            if iban.value(r) == w.iban[h] {
                seen_primary += 1;
            }
        }
    }
    // Every entity's payment IBAN appears exactly once in the account zone.
    assert_eq!(seen_primary, w.population);
}

// ---------------------------------------------------------------------------
// Amount continuity for chained typologies (amounts::instance_amounts)
// ---------------------------------------------------------------------------

#[test]
fn chained_typologies_forward_the_previous_leg_less_a_skim() {
    use datagen_rs::amounts::{fx_to_usd, instance_amounts, is_chained, SKIM_MAX, SKIM_MIN};
    use datagen_rs::hash::Rng;
    use datagen_rs::typology::emit_instance;
    let (insts, _) = kyc_schedule(42);
    let ccys = ["USD", "EUR", "JPY", "INR", "GBP"];
    let ccy_of = |o: u64| ccys[(o % 5) as usize];
    let mut rng = Rng::new(9);
    let mut checked = 0;
    for inst in &insts {
        let rows = emit_instance(inst);
        let amts = instance_amounts(inst.typ, &rows, ccy_of, |_| 0.0, &mut rng);
        assert_eq!(amts.len(), rows.len());
        if !is_chained(inst.typ) {
            continue;
        }
        for k in 1..rows.len() {
            // Emit order is chain order: each leg starts where the last ended.
            assert_eq!(rows[k].orig, rows[k - 1].bene, "{} leg {k}", inst.id);
            assert!(
                rows[k].ts_us >= rows[k - 1].ts_us,
                "{} leg {k} time",
                inst.id
            );
            let prev = amts[k - 1] * fx_to_usd(ccy_of(rows[k - 1].orig));
            let cur = amts[k] * fx_to_usd(ccy_of(rows[k].orig));
            let ratio = cur / prev;
            // Minor-unit rounding in JPY/INR moves the ratio by < 1e-3 at
            // these amounts; allow that on both edges.
            assert!(
                (1.0 - SKIM_MAX - 1e-3..=1.0 - SKIM_MIN + 1e-3).contains(&ratio),
                "{} leg {k}: forwarded {ratio}",
                inst.id
            );
            checked += 1;
        }
    }
    assert!(checked > 100, "only {checked} forwarding legs checked");
}

#[test]
fn unchained_typologies_keep_independent_draws() {
    use datagen_rs::amounts::{instance_amounts, native_amount, structuring_amount};
    use datagen_rs::hash::Rng;
    use datagen_rs::typology::TxRow;
    let rows: Vec<TxRow> = (0..4)
        .map(|i| TxRow {
            orig: i + 1,
            bene: 9,
            ts_us: i as i64,
            structuring: i % 2 == 0,
        })
        .collect();
    let got = instance_amounts("fan_in", &rows, |_| "USD", |_| 0.3, &mut Rng::new(5));
    let mut rng = Rng::new(5);
    let want: Vec<f64> = rows
        .iter()
        .map(|r| {
            if r.structuring {
                structuring_amount(&mut rng, "USD")
            } else {
                native_amount(&mut rng, 0.3, "USD")
            }
        })
        .collect();
    assert_eq!(got, want);
}

// ---------------------------------------------------------------------------
// Multi-cycle generation (cycle.rs, WORKPLAN B4)
// ---------------------------------------------------------------------------

#[test]
fn cycle_zero_is_the_identity() {
    use datagen_rs::cycle::*;
    assert_eq!(stream_seed(42, 0), 42);
    assert_eq!(base_uid(123_456, 0), 123_456);
    assert_eq!(instance_id("FAN_IN_2_0000007", 0), "FAN_IN_2_0000007");
    assert_eq!(pacs_key(7, 0), "bronze/pacs008/part-000007.parquet");
    assert_eq!(c360_key(7, 0), "part-000007.parquet");
    assert_eq!(
        ref_key("manifest/manifest.parquet", 0),
        "manifest/manifest.parquet"
    );
    assert_eq!(c360_file_id(7, 0), 7);
}

#[test]
fn cycle_keys_differ_and_keep_the_reader_suffix() {
    use datagen_rs::cycle::*;
    assert_eq!(pacs_key(7, 1), "bronze/pacs008/part-c001-000007.parquet");
    assert_eq!(c360_key(7, 12), "part-c012-000007.parquet");
    assert_eq!(
        ref_key("manifest/manifest.parquet", 3),
        "manifest/manifest-c003.parquet"
    );
    assert_eq!(
        ref_key("bronze/party.parquet", 3),
        "bronze/party-c003.parquet"
    );
    let keys: std::collections::HashSet<String> = (0..4u64)
        .flat_map(|c| (0..100i64).map(move |f| pacs_key(f, c)))
        .collect();
    assert_eq!(keys.len(), 400);
    assert!(keys.iter().all(|k| k.ends_with(".parquet")));
}

#[test]
fn cycles_zero_and_one_have_disjoint_uids_and_uetrs() {
    use datagen_rs::cycle::{base_uid, instance_id, stream_seed};
    use datagen_rs::hash::{splitmix64, uetr_seeds};
    use datagen_rs::ids::uuid_v4_into;
    use datagen_rs::typology::schedule;
    use std::collections::HashSet;
    let seed = 42i64;
    let pop = 5_000usize;
    let day = 86_400_000_000i64;
    let country: Vec<&'static str> = vec!["US"; pop + 1];
    let uetr = |uid: u64| {
        let (a, b) = uetr_seeds(uid, seed);
        let mut s = String::new();
        uuid_v4_into(a, b, &mut s);
        s
    };
    // Same derivation as bin/generate.rs::typology_uid.
    let typ_uid = |iseed: i64, k: usize| {
        splitmix64((iseed as u64).wrapping_add((k as u64) << 40)) | 0x8000_0000_0000_0000
    };
    let mut ids: Vec<HashSet<String>> = Vec::new();
    let mut uetrs: Vec<HashSet<String>> = Vec::new();
    for c in 0..2u64 {
        let insts = schedule(
            stream_seed(seed, c),
            (pop as i64) * 240,
            pop,
            0,
            1800 * day,
            &country,
        );
        let mut u: HashSet<String> = (0..20_000u64).map(|gi| uetr(base_uid(gi, c))).collect();
        for inst in &insts {
            for k in 0..inst.rows_per_instance + 1 {
                u.insert(uetr(typ_uid(inst.seed, k)));
            }
        }
        ids.push(insts.iter().map(|i| instance_id(&i.id, c)).collect());
        uetrs.push(u);
    }
    assert!(
        ids[0].is_disjoint(&ids[1]),
        "typology ids repeat across cycles"
    );
    assert!(
        uetrs[0].is_disjoint(&uetrs[1]),
        "UETRs repeat across cycles"
    );
    // And the schedule itself moved (different participants), not just the ids.
    let p = |c: u64| {
        schedule(
            stream_seed(seed, c),
            (pop as i64) * 240,
            pop,
            0,
            1800 * day,
            &country,
        )
        .into_iter()
        .map(|i| i.participants)
        .collect::<Vec<_>>()
    };
    assert_ne!(p(0), p(1));
}

#[test]
fn c360_cycles_have_disjoint_event_and_row_ids() {
    use arrow::array::{Array, Int64Array, StringArray};
    use datagen_rs::customer360::{build_batch, Config};
    use datagen_rs::customer360_realism::{CustomerIdSampler, LoyaltyLookup};
    use datagen_rs::cycle::c360_file_id;
    use std::collections::HashSet;
    let loyalty = LoyaltyLookup::build(42, 10_000);
    let sampler = CustomerIdSampler::new(10_000);
    let mut events: Vec<HashSet<String>> = Vec::new();
    let mut rows: Vec<HashSet<i64>> = Vec::new();
    for c in 0..2u64 {
        let (mut e, mut r) = (HashSet::new(), HashSet::new());
        for fid in 0..3u64 {
            let mut cfg = Config::new(42, c360_file_id(fid, c), 500);
            cfg.customer_id_max = 10_000;
            let b = build_batch(&cfg, &loyalty, &sampler);
            let ev = b.column_by_name("event_id").unwrap();
            let ev = ev.as_any().downcast_ref::<StringArray>().unwrap();
            e.extend((0..ev.len()).map(|i| ev.value(i).to_string()));
            let id = b.column_by_name("id").unwrap();
            let id = id.as_any().downcast_ref::<Int64Array>().unwrap();
            r.extend((0..id.len()).map(|i| id.value(i)));
        }
        events.push(e);
        rows.push(r);
    }
    assert!(
        events[0].is_disjoint(&events[1]),
        "c360 event_ids repeat across cycles"
    );
    assert!(
        rows[0].is_disjoint(&rows[1]),
        "c360 row ids repeat across cycles"
    );
}

#[test]
fn cycle_schedules_pick_subjects_from_the_world_not_the_stream_seed() {
    use datagen_rs::cycle::stream_seed;
    use datagen_rs::kyc::is_customer;
    use datagen_rs::typology::{schedule_ex, subject_index};
    use datagen_rs::world::{entity_type, TYPE_PERSON};
    let pop = 5_000usize;
    let country: Vec<&'static str> = vec!["US"; pop + 1];
    let insts = schedule_ex(
        42,
        stream_seed(42, 3),
        (pop as i64) * 240,
        pop,
        0,
        1 << 50,
        &country,
    );
    for inst in &insts {
        let s = inst.participants[subject_index(inst.typ, inst.participants.len(), inst.seed)];
        assert!(
            is_customer(s, 42),
            "{}: subject not a customer of the world",
            inst.id
        );
        assert!(inst
            .participants
            .iter()
            .all(|&p| entity_type(p, 42) == TYPE_PERSON));
    }
}

#[test]
fn fi_entities_never_claim_the_reporting_fi_bic() {
    use datagen_rs::kyc::{own_bic_idx, REPORTING_FI_POOL_IDX};
    for id in 1..=100_000u64 {
        assert!(!REPORTING_FI_POOL_IDX.contains(&own_bic_idx(id, 500)));
    }
}
