//! Shared parquet writer config. `bin/generate.rs` uses this for the pacs008
//! files and for the manifest; `party.rs` uses it for the party + account
//! zones. Kept in one place so `DG_COMPRESSION` (and future codec knobs) apply
//! uniformly across every file the datagen produces -- previously party and
//! account writers hard-coded SNAPPY and silently ignored the env var, which
//! meant a run advertised as ZSTD-1 shipped mixed codecs.

use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

/// Resolve `DG_COMPRESSION` from env. Default zstd1. Accepts:
/// `snappy | zstd | zstdN` for N in 1..=22 | `lz4` | `none|uncompressed`.
/// The parser must accept the same set as the Python datagen's
/// `_parquet_compression` -- an adversarial review caught that a
/// literal-match implementation silently mapped `zstd9` -> `zstd1` while
/// Python honored the same value, producing silently-different bytes in
/// two datagen images sharing one documented env-var contract. Panics on
/// bad input rather than silently defaulting: a startup panic is easier to
/// diagnose than a mysterious codec swap partway through a UAT.
pub fn compression_from_env() -> Compression {
    let v = std::env::var("DG_COMPRESSION").unwrap_or_default();
    let v = v.trim().to_ascii_lowercase();
    match v.as_str() {
        "" | "zstd" | "zstd1" => Compression::ZSTD(ZstdLevel::try_new(1).unwrap()),
        "snappy" => Compression::SNAPPY,
        "lz4" => Compression::LZ4_RAW,
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        other if other.starts_with("zstd") => {
            let n: i32 = other[4..].parse().unwrap_or_else(|_| {
                panic!("DG_COMPRESSION={:?}: could not parse level after 'zstd'", other)
            });
            ZstdLevel::try_new(n)
                .map(Compression::ZSTD)
                .unwrap_or_else(|e| {
                    panic!(
                        "DG_COMPRESSION={:?}: level out of range 1..=22 ({})",
                        other, e
                    )
                })
        }
        _ => panic!(
            "DG_COMPRESSION={:?}: expected snappy|zstd|zstdN|lz4|none",
            v
        ),
    }
}

/// Bytes/row default for the Rust datagen's pacs.008 rows at the current
/// DG_COMPRESSION. Used by the CLI when `--bytes-per-row` is not passed (or
/// passed as 0) to derive file_count from total_txns; a scalar default was
/// wrong under any codec other than the one it was measured against.
/// Measured 2026-09-18 against pacs.008 output at scale=0.05, seed=42,
/// corpus_months=12, file_size_mb=32 (bronze mode):
///   zstd1  227, snappy 345, lz4 350, none 495
/// A miscalibration here silently produces files 2x too big (none) or 1.4x
/// (snappy/lz4), which drives up per-file S3 latency and blows past the
/// pod's parquet-buffer cap.
///
/// NOTE: these numbers are for pacs.008 specifically, not for the Python
/// datagen's `financial` schema. The Python datagen's `_BYTES_PER_ROW`
/// table describes a different row layout, so `("financial", "zstd")=133`
/// there is NOT the same measurement as `zstd1 -> 227` here. Do not
/// reconcile the two tables against each other.
pub fn bytes_per_row_default() -> f64 {
    match compression_from_env() {
        Compression::ZSTD(_) => 227.0,
        Compression::SNAPPY => 345.0,
        Compression::LZ4_RAW => 350.0,
        Compression::UNCOMPRESSED => 495.0,
        // Any codec compression_from_env() would return that isn't listed
        // above didn't exist when this table was measured. Return the ZSTD
        // value as a safe middle: a future codec is more likely to be a
        // compressor than not.
        _ => 227.0,
    }
}

/// WriterProperties used across every parquet the datagen writes. Reads
/// `DG_STATS`, `DG_DICT`, `DG_PAGESZ`, `DG_COMPRESSION` from env.
pub fn writer_properties() -> WriterProperties {
    let stats = match std::env::var("DG_STATS").as_deref() {
        Ok("page") => EnabledStatistics::Page,
        Ok("chunk") => EnabledStatistics::Chunk,
        _ => EnabledStatistics::None,
    };
    let dict = std::env::var("DG_DICT").map(|v| v != "0").unwrap_or(true);
    let mut b = WriterProperties::builder()
        .set_compression(compression_from_env())
        .set_statistics_enabled(stats)
        .set_dictionary_enabled(dict);
    if let Ok(ps) = std::env::var("DG_PAGESZ") {
        if let Ok(n) = ps.parse::<usize>() {
            b = b.set_data_page_size_limit(n);
        }
    }
    b.build()
}
