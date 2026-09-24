//! Multi-cycle generation (`--cycle n`, WORKPLAN B4).
//!
//! A multi-cycle run invokes the generator once per cycle with the same
//! --seed. Before this every cycle wrote the same object keys and replayed the
//! same id streams, so cycle k overwrote cycle k-1's bronze files and N cycles
//! gave N copies of every c360 event_id. Cycle n > 0 now draws from its own
//! streams and writes its own keys, so bronze accumulates. Cycle 0 is the
//! identity everywhere: its bytes and keys are exactly what a run without
//! --cycle produces.
//!
//! What a cycle changes and what it keeps:
//! - The world (entities, names, KYC, customer status, persona) keeps the
//!   plain --seed. A bank's customers do not change between daily cycles, and
//!   silver's entity keys must line up across cycles.
//! - Every event stream mixes the cycle in: the AML typology schedule,
//!   typology amounts and base rows (via `stream_seed`), the base-row uids
//!   that seed UETRs and message ids (`base_uid`), typology instance ids
//!   (`instance_id`), and the c360 per-file stream and row ids (`c360_file_id`).

use crate::hash::splitmix64;

/// Seed for the cycle's event streams. Identity for cycle 0.
#[inline]
pub fn stream_seed(seed: i64, cycle: u64) -> i64 {
    if cycle == 0 {
        seed
    } else {
        splitmix64((seed as u64) ^ splitmix64(0xC7C1_E000_0000_0000 ^ cycle)) as i64
    }
}

/// Base-row uid: the global row index with the cycle in bits 40..63. Base
/// uids keep the top bit clear (typology uids set it), and the global index
/// stays below 2^40 at every supported scale, so cycles never share a uid.
#[inline]
pub fn base_uid(gi: u64, cycle: u64) -> u64 {
    debug_assert!(gi < 1 << 40 && cycle < 1 << 23);
    gi | (cycle << 40)
}

/// Typology instance id, suffixed with the cycle for n > 0 so manifest
/// typology_ids stay unique across cycles.
pub fn instance_id(id: &str, cycle: u64) -> String {
    if cycle == 0 {
        id.to_string()
    } else {
        format!("{id}_C{cycle:03}")
    }
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
