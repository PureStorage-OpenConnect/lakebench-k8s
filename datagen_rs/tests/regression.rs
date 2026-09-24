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
fn compression_default_is_snappy() {
    // Default changed from zstd1 to snappy on 2026-09-20 following the c360
    // Rust port perf sweep: snappy is 40-55% faster with a ~1.5x on-disk
    // trade. Benchmarks that need smaller files opt into zstd explicitly.
    use datagen_rs::writer::compression_from_env;
    use parquet::basic::Compression;
    let c = with_env("DG_COMPRESSION", None, compression_from_env);
    assert!(matches!(c, Compression::SNAPPY), "default should be snappy, got {:?}", c);
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
fn pacs008_bytes_per_row_default_is_codec_aware() {
    // A scalar default was wildly wrong under any codec other than the one it
    // was measured against. This test pins the codec-aware defaults so a
    // regression trips a unit test rather than manifesting as 2x-too-big
    // files at UAT scale.
    use datagen_rs::writer::pacs008_bytes_per_row_default;
    // ZSTD-1 (default codec) -- ~227 bytes/row measured 2026-09-18.
    let z = with_env("DG_COMPRESSION", Some("zstd1"), pacs008_bytes_per_row_default);
    let s = with_env("DG_COMPRESSION", Some("snappy"), pacs008_bytes_per_row_default);
    let l = with_env("DG_COMPRESSION", Some("lz4"), pacs008_bytes_per_row_default);
    let n = with_env("DG_COMPRESSION", Some("none"), pacs008_bytes_per_row_default);
    // Ratios more than the absolute values: SNAPPY/LZ4 should be within 5%
    // of each other, and both should sit between ZSTD-1 and uncompressed.
    assert!(z > 100.0 && z < 300.0, "zstd default sanity: {}", z);
    assert!(n > 400.0 && n < 700.0, "none default sanity: {}", n);
    assert!(s > z && s < n, "snappy {} should sit between zstd {} and none {}", s, z, n);
    assert!(l > z && l < n, "lz4 {} should sit between zstd {} and none {}", l, z, n);
    assert!((s - l).abs() / s.max(l) < 0.10, "snappy {} and lz4 {} should be within 10%", s, l);
}

#[test]
fn customer360_bytes_per_row_default_is_codec_aware() {
    // c360 rows are ~10-20x wider than pacs.008 because the payload column is
    // 2 KiB of random hex. Silently falling back to the pacs.008 default here
    // would produce c360 files ~10x too small, which the M1 review flagged as
    // the design-critical trap. Pin every codec's expected value.
    use datagen_rs::writer::customer360_bytes_per_row_default;
    let z = with_env("DG_COMPRESSION", Some("zstd1"), customer360_bytes_per_row_default);
    let s = with_env("DG_COMPRESSION", Some("snappy"), customer360_bytes_per_row_default);
    let l = with_env("DG_COMPRESSION", Some("lz4"), customer360_bytes_per_row_default);
    let n = with_env("DG_COMPRESSION", Some("none"), customer360_bytes_per_row_default);
    // zstd cracks the hex payload well; snappy/lz4/none don't.
    assert_eq!(z, 2233.0);
    assert_eq!(s, 4332.0);
    assert_eq!(l, 4356.0);
    assert_eq!(n, 4399.0);
    // c360 must not accidentally alias the pacs.008 default.
    use datagen_rs::writer::pacs008_bytes_per_row_default;
    let pz = with_env("DG_COMPRESSION", Some("zstd1"), pacs008_bytes_per_row_default);
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
use datagen_rs::world::{
    amount_log_shift, hash_normal, rate_mult, BASELINE_ACTIVITY, TYPE_PERSON,
};

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
        quiet, n
    );
    assert!(busy > n as usize / 10, "too few busy accounts ({}/{})", busy, n);
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
    let mean_mult: f64 =
        (1..=n).map(|id| amount_log_shift(id, seed).exp()).sum::<f64>() / n as f64;
    assert!(
        (mean_mult - 1.0).abs() < 0.03,
        "amount shift not mean-preserving: E[exp(shift)] = {}",
        mean_mult
    );
    // And it must actually spread amounts (not collapse to 1.0 everywhere).
    let big = (1..=n).filter(|&id| amount_log_shift(id, seed).exp() > 1.5).count();
    assert!(big > n as usize / 20, "amount shift has no spread ({}/{})", big, n);
}

#[test]
fn lognormal_shifted_zero_matches_base() {
    // Delegation must be byte-identical: same RNG state + zero shift == the old
    // lognormal_amount, so passing 0.0 anywhere is a pure no-op.
    let mut a = Rng::new(777);
    let mut b = Rng::new(777);
    for _ in 0..10_000 {
        assert_eq!(lognormal_amount(&mut a), lognormal_amount_shifted(&mut b, 0.0));
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
    assert!(distinct > persons.len() / 2, "activity not sufficiently heterogeneous");
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
