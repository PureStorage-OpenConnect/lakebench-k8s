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
