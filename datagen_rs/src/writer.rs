//! Shared parquet writer config. `bin/generate.rs` uses this for the pacs008
//! files and for the manifest; `party.rs` uses it for the party + account
//! zones. Kept in one place so `DG_COMPRESSION` (and future codec knobs) apply
//! uniformly across every file the datagen produces -- previously party and
//! account writers hard-coded SNAPPY and silently ignored the env var, which
//! meant a run advertised as ZSTD-1 shipped mixed codecs.

use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

/// Resolve `DG_COMPRESSION` from env. **Default snappy** as of the c360 Rust
/// port perf sweep (2026-09-20): snappy is 40-55% faster than zstd on both
/// pacs.008 and customer360 while producing files ~1.5x larger on disk. For
/// datagen throughput that trade is a clear win; benchmarks that care about
/// on-disk footprint can still opt into zstd via `DG_COMPRESSION=zstd`.
///
/// Accepts: `snappy | zstd | zstdN` for N in 1..=22 | `lz4` | `none|uncompressed`.
/// Panics on bad input rather than silently defaulting: a startup panic is
/// easier to diagnose than a mysterious codec swap partway through a UAT.
pub fn compression_from_env() -> Compression {
    let v = std::env::var("DG_COMPRESSION").unwrap_or_default();
    let v = v.trim().to_ascii_lowercase();
    match v.as_str() {
        "" | "snappy" => Compression::SNAPPY,
        "zstd" | "zstd1" => Compression::ZSTD(ZstdLevel::try_new(1).unwrap()),
        "lz4" => Compression::LZ4_RAW,
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        other if other.starts_with("zstd") => {
            let n: i32 = other[4..].parse().unwrap_or_else(|_| {
                panic!(
                    "DG_COMPRESSION={:?}: could not parse level after 'zstd'",
                    other
                )
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
/// (This applies to pacs.008 only. For customer360, `customer360_bytes_per_row_default`
/// intentionally mirrors the Python `_BYTES_PER_ROW` table verbatim -- same
/// schema, same measurements.)
pub fn pacs008_bytes_per_row_default() -> f64 {
    match compression_from_env() {
        Compression::ZSTD(_) => 227.0,
        Compression::SNAPPY => 345.0,
        Compression::LZ4_RAW => 350.0,
        Compression::UNCOMPRESSED => 495.0,
        // Any Compression variant not measured above indicates a codec was
        // added to `compression_from_env()` without a matching row here. A
        // silent fallback would size files against the wrong compression
        // ratio; panicking forces the missing measurement.
        other => panic!(
            "pacs008_bytes_per_row_default: unmapped codec {:?}; add a bytes/row row here",
            other
        ),
    }
}

/// Bytes/row default for customer360 rows at the current DG_COMPRESSION.
///
/// Measured against the Python generator's output (`datagen/generate.py:305-315`
/// documents the same table). Copied verbatim so the file-count formula picks
/// the right shape:
///   snappy 4332, zstd1 2233, lz4 4356, none 4399.
///
/// The row is ~10-20x wider than pacs.008 because the payload column is 2 KiB
/// of random hex (compressed effectively by zstd, barely by snappy/lz4). A
/// silent fallback to `pacs008_bytes_per_row_default()` would produce c360
/// files ~10x too small -- the exact silent-corruption failure mode the M1
/// review flagged as design-critical. Keep the schema dispatch explicit.
pub fn customer360_bytes_per_row_default() -> f64 {
    match compression_from_env() {
        Compression::ZSTD(_) => 2233.0,
        Compression::SNAPPY => 4332.0,
        Compression::LZ4_RAW => 4356.0,
        Compression::UNCOMPRESSED => 4399.0,
        // Same rationale as `pacs008_bytes_per_row_default`: panic on an
        // unmapped codec rather than shipping silently-wrong file sizes.
        other => panic!(
            "customer360_bytes_per_row_default: unmapped codec {:?}; add a bytes/row row here",
            other
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
