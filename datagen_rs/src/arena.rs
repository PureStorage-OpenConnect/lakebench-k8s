//! Contiguous string column, indexed by entity_id, for the hot emit gather.
//!
//! `Vec<String>` stores a (ptr, len, cap) triple per entry inline in the vec,
//! so `vec[i]` reads 24 bytes then chases the pointer to a heap allocation
//! at some arbitrary address. At scale 100 the world has 11M entries, and the
//! emit path does several such gathers per row over random entity ids --
//! every one of those pointer chases is a full main-memory latency miss.
//!
//! ArenaCol replaces that with the same layout Arrow uses internally: one
//! contiguous byte buffer and a Vec<u32> of offsets. Access is now two
//! adjacent 4-byte reads (offsets[i], offsets[i+1]) then a slice into the flat
//! data buffer, with no per-entry heap allocation and no pointer chase to a
//! random address. Adjacent entities share cache lines in both offsets and
//! data, so any temporal locality (typology bursts on the same participants,
//! ring redraws that repeat counterparties) turns into cache hits instead of
//! misses.
//!
//! **Peak memory during build** is roughly 3x the final arena: `build_par`
//! first materialises the full `Vec<String>` (24 bytes struct + a small heap
//! allocation per entry), then concats it into `data`. At scale 100 the
//! transient overhead is ~1-2 GiB per column, which callers sizing pod memory
//! limits must budget for. Reducing this to ~1x needs a per-thread arena
//! merge and is left as future work.

use rayon::prelude::*;

/// Contiguous string column with 32-bit offsets. Total bytes per column must
/// fit in `u32::MAX` (~4 GiB). At scale 100 with typical realism content the
/// hot columns are well under 1 GiB, but the ceiling starts to bite around
/// scale ~1500 (name/street each ~25 bytes * ~170M entries). `from_vec`
/// asserts on overflow rather than silently truncating via the `as u32` cast.
pub struct ArenaCol {
    // offsets[i] .. offsets[i+1] is the byte range for entity i.
    // Convention: fed from a Vec of length n+1 (sentinel at 0, indices 1..=n
    // are real entities). offsets.len() == n+2 accordingly.
    offsets: Vec<u32>,
    data: Vec<u8>,
}

impl ArenaCol {
    /// Highest valid index into this column (== the number of real entities;
    /// sentinel at index 0 is not counted). Renamed from a former `len()`
    /// that returned `n+1` and invited off-by-one when callers wrote
    /// `for i in 0..arena.len()`. The valid access range is `0..=max_id()`.
    #[allow(dead_code)]
    pub fn max_id(&self) -> usize {
        // offsets.len() == n+2 (sentinel + real + trailing offset)
        self.offsets.len().saturating_sub(2)
    }

    /// Slice of bytes for entity `i` as a &str. Caller guarantees valid UTF-8
    /// was written; the builders in this module accept only `String` inputs
    /// which are always valid UTF-8, so all present call sites are sound.
    /// Debug builds double-check the invariant.
    #[inline]
    pub fn get(&self, i: usize) -> &str {
        let a = self.offsets[i] as usize;
        let b = self.offsets[i + 1] as usize;
        debug_assert!(b >= a && b <= self.data.len());
        let bytes = &self.data[a..b];
        debug_assert!(
            std::str::from_utf8(bytes).is_ok(),
            "ArenaCol produced non-UTF-8 bytes"
        );
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    /// Build from a parallel-produced Vec<String>. Single sequential pass to
    /// concat; the parallel produce phase already dominates cost and stays
    /// where it was.
    pub fn from_vec(v: Vec<String>) -> Self {
        let n = v.len();
        // Callers follow the sentinel-at-0 convention: v has length n+1 for
        // a world with n real entities. An empty input would leave get(0)
        // with an out-of-bounds offsets[1] read, so refuse it in debug.
        debug_assert!(
            !v.is_empty(),
            "ArenaCol::from_vec: empty input violates sentinel convention"
        );
        let total: usize = v.iter().map(|s| s.len()).sum();
        assert!(total <= u32::MAX as usize, "ArenaCol >4 GiB not supported");
        let mut offsets = Vec::with_capacity(n + 1);
        let mut data = Vec::with_capacity(total);
        offsets.push(0u32);
        for s in v {
            data.extend_from_slice(s.as_bytes());
            offsets.push(data.len() as u32);
        }
        Self { offsets, data }
    }

    /// Same, but the producer is a pure function of i in `0..=n`. Runs the
    /// producer in parallel, then folds into one contiguous arena. Peak
    /// memory during this call is the transient Vec<String> plus the growing
    /// arena; see module docs for the ~3x note.
    pub fn build_par<F: Fn(usize) -> String + Sync + Send>(n: usize, f: F) -> Self {
        let v: Vec<String> = (0..=n).into_par_iter().map(f).collect();
        Self::from_vec(v)
    }
}
