//! Multi-cycle generation (`--cycle n --cycles N`, WORKPLAN B4).
//!
//! A multi-cycle run invokes the generator once per cycle. Before this every
//! cycle wrote the same object keys and replayed the same rows, so cycle k
//! overwrote cycle k-1's bronze files.
//!
//! AML: cycle n of N emits exactly the one-shot corpus rows whose calendar
//! mass lies in [n/N, (n+1)/N) (`mass_slice`). The world, the typology
//! schedule, every amount and the dormancy suppression are computed exactly
//! as in one shot, so the union of the N cycles is the one-shot corpus, row
//! for row, and cross-row truth (dormancy gaps, chains) is preserved. Files
//! are already laid out by mass, so a cycle generates only the files that
//! intersect its slice and splits a straddling one. Each cycle's manifest
//! lists the instances whose last row it emits. Cycle 0 of 1 is a one-shot
//! run, byte for byte.
//!
//! c360 has no cross-row labels and its per-cycle window comes from
//! --timestamp-start/end, so there cycle n > 0 shifts the per-file stream
//! and row ids instead (`c360_file_id`).

/// Largest supported cycle. The c360 row ids (file_id * rows_per_file with
/// file_id offset by cycle << 32) stay in i64 only while rows_per_file is
/// below 2^31 / n; bin/generate.rs refuses a c360 cycle that would overflow.
pub const MAX_CYCLE: u64 = (1 << 23) - 1;

/// Calendar-mass slice [lo, hi) of cycle n of N. The first slice starts at
/// -inf and the last ends at +inf, so no row is lost to a rounding edge.
pub fn mass_slice(cycle: u64, cycles: u64) -> (f64, f64) {
    let lo = if cycle == 0 {
        f64::NEG_INFINITY
    } else {
        cycle as f64 / cycles as f64
    };
    let hi = if cycle + 1 >= cycles {
        f64::INFINITY
    } else {
        (cycle + 1) as f64 / cycles as f64
    };
    (lo, hi)
}

/// Object key of an AML bronze file.
pub fn pacs_key(fid: i64, cycle: u64) -> String {
    if cycle == 0 {
        format!("bronze/pacs008/part-{fid:06}.parquet")
    } else {
        format!("bronze/pacs008/part-c{cycle:03}-{fid:06}.parquet")
    }
}

/// Object key of a c360 file (relative to --prefix).
pub fn c360_key(fid: i64, cycle: u64) -> String {
    if cycle == 0 {
        format!("part-{fid:06}.parquet")
    } else {
        format!("part-c{cycle:03}-{fid:06}.parquet")
    }
}

/// Key of an AML reference file (`bronze/party.parquet`,
/// `bronze/account.parquet`, `manifest/manifest.parquet`): `-c{n:03}` goes
/// before the extension for n > 0, so a cycle's manifest never replaces an
/// earlier cycle's.
pub fn ref_key(key: &str, cycle: u64) -> String {
    if cycle == 0 {
        return key.to_string();
    }
    match key.rsplit_once('.') {
        Some((stem, ext)) => format!("{stem}-c{cycle:03}.{ext}"),
        None => format!("{key}-c{cycle:03}"),
    }
}

/// c360 file id handed to `customer360::Config`. The file's RNG seeds on
/// (seed + file_id) and its row ids start at file_id * rows, so offsetting
/// the file id by cycle << 32 gives each cycle disjoint streams and disjoint
/// row ids (up to 2^32 files per cycle), while --seed itself, which also
/// seeds the per-customer loyalty lookup, stays the same across cycles.
#[inline]
pub fn c360_file_id(fid: u64, cycle: u64) -> u64 {
    fid + (cycle << 32)
}
