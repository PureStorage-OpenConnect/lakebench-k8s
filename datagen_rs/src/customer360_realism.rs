//! Vocabulary + realism helpers for the customer360 schema.
//!
//! Everything here is pure: deterministic in the caller-supplied `Rng` (from
//! `crate::hash`) so a `(seed, file_id)` pair reproduces the same batch. Static
//! tables mirror `datagen/generate.py:136-217` verbatim -- distribution parity
//! with the Python generator is the acceptance contract, not byte parity, but
//! keeping the tables identical is what makes benchmark results comparable.
//!
//! The 7 realism features (see `_build_customer360_table` in the Python) touch
//! specific column subsets; the helpers here are the primitives the emit module
//! composes into a batch.

use crate::hash::Rng;

// ---------------------------------------------------------------------------
// Vocabulary tables (mirror datagen/generate.py:136-217)
// ---------------------------------------------------------------------------

pub const INTERACTION_TYPES: [&str; 5] =
    ["purchase", "browse", "support", "login", "abandoned_cart"];
pub const INTERACTION_WEIGHTS: [f64; 5] = [0.18, 0.35, 0.12, 0.20, 0.15];

pub const PRODUCT_CATEGORIES: [&str; 5] =
    ["electronics", "clothing", "home_garden", "books", "sports"];
pub const CURRENCIES: [&str; 4] = ["USD", "EUR", "GBP", "CAD"];
pub const CHANNELS: [&str; 5] = ["web", "mobile_app", "store", "call_center", "social_media"];
pub const DEVICE_TYPES: [&str; 3] = ["desktop", "mobile", "tablet"];
pub const BROWSERS: [&str; 4] = ["chrome", "safari", "firefox", "edge"];
pub const LOYALTY_TIERS: [&str; 3] = ["bronze", "silver", "gold"];
pub const EMAIL_DOMAINS: [&str; 5] = [
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "icloud.com",
    "hotmail.com",
];
pub const OPERATING_SYSTEMS: [&str; 5] = [
    "Windows NT 10.0",
    "macOS 14.0",
    "Linux",
    "iOS 17",
    "Android 14",
];

pub const DATA_QUALITY_FLAGS: [&str; 4] = [
    "clean",
    "duplicate_suspected",
    "incomplete_data",
    "format_inconsistent",
];
pub const DATA_QUALITY_WEIGHTS: [f64; 4] = [0.92, 0.02, 0.03, 0.03];

pub const DATA_SOURCES: [&str; 4] = [
    "primary_system",
    "legacy_import",
    "manual_entry",
    "third_party_api",
];
pub const DATA_SOURCE_WEIGHTS: [f64; 4] = [0.70, 0.15, 0.10, 0.05];

/// Per-source dirty-data rate. Real pipelines don't corrupt data uniformly:
/// primary systems have tight validation (<1% dirty), legacy imports carry
/// pre-migration crust (~35%), manual entry has typos (~15%), third-party
/// APIs vary widely (~5%). Indexed by position in `DATA_SOURCES`.
///
/// Callers scale this by the configured `cfg.dirty_ratio` (default 0.08)
/// so operators can dial the *aggregate* dirty rate up or down while the
/// *distribution* across sources stays realistic.
pub const DIRTY_RATE_BY_SOURCE: [f64; 4] = [0.005, 0.35, 0.15, 0.05];

pub const ISSUE_CATEGORIES: [&str; 3] = ["billing", "technical", "general_inquiry"];

pub const UTM_SOURCES: [&str; 4] = ["google", "facebook", "email", "direct"];
pub const UTM_MEDIUMS: [&str; 3] = ["cpc", "organic", "referral"];

/// City/state pairs, intentionally inconsistent so Silver has cleaning work to
/// do (matches datagen/generate.py:165-188). Layout: (city, state).
pub const CITIES: [(&str, &str); 22] = [
    ("New York", "NY"),
    ("NYC", "New York"),
    ("New York City", "NY"),
    ("Los Angeles", "CA"),
    ("LA", "California"),
    ("Los Angeles", "California"),
    ("Chicago", "IL"),
    ("Chicago", "Illinois"),
    ("Houston", "TX"),
    ("Houston", "Texas"),
    ("Phoenix", "AZ"),
    ("Phoenix", "Arizona"),
    ("Philadelphia", "PA"),
    ("Philly", "Pennsylvania"),
    ("San Antonio", "TX"),
    ("San Antonio", "Texas"),
    ("San Diego", "CA"),
    ("San Diego", "California"),
    ("Dallas", "TX"),
    ("Dallas", "Texas"),
    ("San Jose", "CA"),
    ("San Jose", "California"),
];

/// City misspelling map. Kept as a linear search over &[(&str, &str)] rather
/// than a HashMap: 10 entries, called at most `dirty_ratio * rows` times per
/// file, and static so the compiler can lay it out inline.
pub const DIRTY_CITY_VARIANTS: [(&str, &str); 10] = [
    ("Chicago", "Chicgao"),
    ("New York", "Newyork"),
    ("New York City", "Newyork City"),
    ("NYC", "Nyc"),
    ("Phoenix", "Pheonix"),
    ("Los Angeles", "Los Angelas"),
    ("LA", "La"),
    ("Houston", "Huston"),
    ("Philadelphia", "Philidelphia"),
    ("Philly", "Phily"),
];

/// State corruption variants -- each state maps to a small list of candidates
/// and the caller picks uniformly. Mirrors DIRTY_STATE_VARIANTS in Python. Keep
/// slices `&'static` so we can `Arc`-free share them across rayon threads.
pub const DIRTY_STATE_VARIANTS: &[(&str, &[&str])] = &[
    ("CA", &["california", "CALIFORNIA", "Calif.", "Ca"]),
    ("NY", &["new york", "NEW YORK", "N.Y.", "Ny"]),
    ("New York", &["new york", "NEW YORK", "ny", "N.Y."]),
    ("IL", &["illinois", "ILLINOIS", "Ill.", "Il"]),
    ("Illinois", &["IL", "il", "Ill.", "ILLINOIS"]),
    ("TX", &["texas", "TEXAS", "Tex.", "Tx"]),
    ("Texas", &["TX", "tx", "Tex.", "TEXAS"]),
    ("AZ", &["arizona", "ARIZONA", "Ariz.", "Az"]),
    ("Arizona", &["AZ", "az", "Ariz.", "ARIZONA"]),
    ("PA", &["pennsylvania", "PENNSYLVANIA", "Penn.", "Pa"]),
    ("Pennsylvania", &["PA", "pa", "Penn.", "PENNSYLVANIA"]),
    ("California", &["CA", "ca", "Calif.", "CALIFORNIA"]),
];

// ---------------------------------------------------------------------------
// CDFs -- one-time cumulative sums for weighted-categorical sampling.
// ---------------------------------------------------------------------------

/// Build a cumulative-distribution vector from raw weights (must sum > 0).
///
/// Used at module init for the three weighted vocabs (`INTERACTION_WEIGHTS`,
/// `DATA_QUALITY_WEIGHTS`, `DATA_SOURCE_WEIGHTS`). Static const-fn arithmetic
/// on floats is not stable in this Rust edition, so we memoise per call site
/// with a `once_cell`-free approach: the emit hot loop calls this once at pod
/// start and stashes the vec in a local.
pub fn cdf(weights: &[f64]) -> Vec<f64> {
    let total: f64 = weights.iter().sum();
    assert!(total > 0.0, "weights must sum to > 0");
    let mut out = Vec::with_capacity(weights.len());
    let mut running = 0.0;
    for w in weights {
        running += w / total;
        out.push(running);
    }
    // Guard: last entry must be ~1.0 so `rng.unit()` never falls past it.
    if let Some(last) = out.last_mut() {
        *last = 1.0;
    }
    out
}

/// Return the index of the weighted-categorical bucket that `u` (in `[0, 1)`)
/// falls into. Bisects the CDF -- O(log N), and N is tiny (<=5) here anyway.
#[inline(always)]
pub fn pick_idx(cdf: &[f64], u: f64) -> usize {
    // partition_point returns the first index where cdf[i] > u, which is the
    // matching bucket. Safe because cdf is monotone non-decreasing and its
    // final entry is 1.0 (set in `cdf()` above), so u ∈ [0, 1) always lands.
    cdf.partition_point(|&c| c <= u)
}

/// Convenience wrapper: draw a category from `choices` weighted by `cdf`.
#[inline(always)]
pub fn weighted_pick<'a>(rng: &mut Rng, choices: &'a [&'a str], cdf: &[f64]) -> &'a str {
    choices[pick_idx(cdf, rng.unit())]
}

// ---------------------------------------------------------------------------
// Loyalty lookup (feature #7: customer-consistent loyalty)
// ---------------------------------------------------------------------------

/// Read-only lookups keyed by `customer_id` in `[0, customer_id_max]`.
///
/// `member[cid] = true` for the ~60% of customers who are program members.
/// `tier[cid]` is 0=bronze, 1=silver, 2=gold with a 70/20/10 split among
/// members (assigned deterministically from a second RNG roll so member and
/// tier are correlated but not identical).
///
/// Built once per pod, then borrowed shared-immutable by every rayon worker.
pub struct LoyaltyLookup {
    pub member: Vec<bool>,
    pub tier: Vec<u8>,
}

impl LoyaltyLookup {
    pub fn build(seed: u64, customer_id_max: u64) -> Self {
        let n = (customer_id_max as usize) + 1;
        // Two draws per id, one for membership, one for tier. Python uses
        // np.random.default_rng(seed) with two `.random(n)` calls back-to-back;
        // we run the equivalent with our splitmix64-backed Rng. Bytes will
        // differ but the shape and per-bucket mass will match within the small
        // deviation between numpy's PCG and our splitmix64.
        let mut rng = Rng::new(seed);
        let mut member = Vec::with_capacity(n);
        let mut tier = Vec::with_capacity(n);
        for _ in 0..n {
            member.push(rng.unit() < 0.6);
        }
        for _ in 0..n {
            let r = rng.unit();
            let t = if r < 0.7 {
                0u8
            } else if r < 0.9 {
                1u8
            } else {
                2u8
            };
            tier.push(t);
        }
        Self { member, tier }
    }
}

// ---------------------------------------------------------------------------
// Customer-id sampler (realism feature #1: hot-customer skew, bounded)
// ---------------------------------------------------------------------------

/// Truncated-Zipf customer id sampler. Matches the shape of a real bank / retail
/// customer distribution better than raw `numpy.zipf(1.5)`: top
/// `hot_customer_count` ids get `hot_customer_share` of activity (recurring /
/// loyal customers), the rest are uniform retail. Deterministic-per-file when
/// built once outside the row loop.
///
/// Design rationale (2026-09-20): raw `numpy.zipf(1.5) mod (cid_max+1)` puts
/// ~38% of events on `customer_id=1`, producing a supernode that dominates
/// every `GROUP BY customer_id` aggregate. Truncated Zipf(2.3) over the top
/// 500 ids bounds id=1's share to ~10-12% while keeping a realistic power-law
/// tail. Ported from the (deleted) Python financial `PartySelector` for the
/// same reason -- practitioner review 2026-09-17 flagged the supernode as
/// breaking every graph-shaped detection query.
pub struct CustomerIdSampler {
    /// CDF over the top-hot ids; length == hot_customer_count, monotone
    /// non-decreasing, last entry == 1.0. Bisect on `rng.unit()` picks one.
    hot_cdf: Vec<f64>,
    hot_share: f64,
    hot_count: u64,
    customer_id_max: u64,
}

impl CustomerIdSampler {
    /// Build the sampler once per pod (cheap: N=500 array + prefix sum).
    ///
    /// Defaults tuned 2026-09-20 to cap id=1's total share at ~10% (well below
    /// the 15% supernode-regression bar the test enforces):
    ///   - hot_customer_count = 500
    ///   - hot_customer_share = 0.40  (top 500 ids get 40% of activity)
    ///   - zipf_shape         = 1.2   (within-hot distribution shape)
    ///
    /// Shape rationale: raw Zipf(1.5) on the full id space put ~38% on id=1,
    /// producing the supernode. Zipf(2.3) truncated to top-500 still puts
    /// ~71% *within* the hot bucket on id=1 (so ~28% overall at hot_share
    /// 0.4). Shape 1.2 flattens the within-hot distribution to id=1 share
    /// ~24% of hot mass -> ~10% overall, matching real retail customer
    /// distributions where the top customer typically does 2-3x the next
    /// most-active, not 10x.
    pub fn new(customer_id_max: u64) -> Self {
        Self::with_params(customer_id_max, 500, 0.40, 1.2)
    }

    pub fn with_params(
        customer_id_max: u64,
        hot_customer_count: u64,
        hot_customer_share: f64,
        zipf_shape: f64,
    ) -> Self {
        let hot_count = hot_customer_count.min(customer_id_max.max(1));
        let mut weights: Vec<f64> = (1..=hot_count as usize)
            .map(|k| 1.0 / (k as f64).powf(zipf_shape))
            .collect();
        let total: f64 = weights.iter().sum();
        assert!(total > 0.0, "zipf weights must sum > 0");
        for w in weights.iter_mut() {
            *w /= total;
        }
        // Cumulative sum, force last to 1.0 so unit()-in-[0,1) always lands.
        let mut running = 0.0;
        let mut hot_cdf = Vec::with_capacity(weights.len());
        for w in &weights {
            running += w;
            hot_cdf.push(running);
        }
        if let Some(last) = hot_cdf.last_mut() {
            *last = 1.0;
        }
        Self {
            hot_cdf,
            hot_share: hot_customer_share,
            hot_count,
            customer_id_max,
        }
    }

    /// Sample one customer_id in `[1, customer_id_max]`. Returns 1-indexed ids
    /// (matches the Python PartySelector convention). Consumes 1-2 draws.
    #[inline]
    pub fn sample(&self, rng: &mut Rng) -> u64 {
        if rng.unit() < self.hot_share && self.hot_count > 0 {
            // Hot corp path: bisect the pre-built CDF.
            let u = rng.unit();
            let idx = self.hot_cdf.partition_point(|&c| c <= u);
            let idx = idx.min((self.hot_count - 1) as usize);
            (idx as u64) + 1
        } else if self.hot_count >= self.customer_id_max {
            // Retail range empty: fall back to full-id-space uniform so tiny
            // populations don't degenerate.
            1 + rng.below(self.customer_id_max)
        } else {
            // Retail: uniform over the non-hot band.
            let lo = self.hot_count + 1;
            let n = self.customer_id_max - self.hot_count;
            lo + rng.below(n)
        }
    }
}

// ---------------------------------------------------------------------------
// Reservoir-free sample of k distinct indices in [0, n) (Fisher-Yates walk)
// ---------------------------------------------------------------------------

/// Fill `out` with `k` distinct indices sampled uniformly from `[0, n)`.
/// Uses a partial Fisher-Yates on a scratch `Vec<u32>` -- O(n + k) memory-wise,
/// O(k) swaps. For k << n (the typical dirty-corruption case: 8% of rows) this
/// dominates smaller-schema shuffling too and stays branch-predictable.
///
/// Panics if `k > n`.
pub fn sample_distinct(rng: &mut Rng, n: usize, k: usize, out: &mut Vec<u32>) {
    assert!(k <= n, "cannot sample {} distinct from {}", k, n);
    out.clear();
    if k == 0 {
        return;
    }
    // Small-k optimization: for k <= 64, use a bit-set to reject duplicates.
    // Zero heap allocation in the fast path. Falls back to Fisher-Yates only
    // when k grows large enough that rejection-rate math starts to hurt.
    if k <= 64 && n >= k * 4 {
        // Expected trials ≈ k / (1 - k/n); with n >= 4k, factor <= 4/3.
        let mut seen: u64 = 0;
        while out.len() < k {
            let idx = rng.below(n as u64) as u32;
            let bit = 1u64 << (idx as u64 % 64);
            // The bit-set is only unique within its 64-bit window; validate by
            // scanning `out` for exact match to catch collisions past 64 ids.
            if seen & bit != 0 && out.contains(&idx) {
                continue;
            }
            seen |= bit;
            out.push(idx);
        }
        return;
    }
    // General case: Fisher-Yates on a materialised range. Allocates n u32s.
    let mut pool: Vec<u32> = (0..n as u32).collect();
    for i in 0..k {
        let j = i + (rng.below((n - i) as u64) as usize);
        pool.swap(i, j);
        out.push(pool[i]);
    }
}

// ---------------------------------------------------------------------------
// Dirty-value corruption (feature #4)
// ---------------------------------------------------------------------------

/// Look up a corrupted variant for `city` if one is registered. Linear scan
/// over the 10-entry `DIRTY_CITY_VARIANTS` table.
#[inline]
pub fn city_variant(city: &str) -> Option<&'static str> {
    DIRTY_CITY_VARIANTS
        .iter()
        .find(|(k, _)| *k == city)
        .map(|(_, v)| *v)
}

/// Look up the variants list for a state, if any. Returns an empty slice when
/// the state has no registered variants (the caller then leaves it alone).
#[inline]
pub fn state_variants(state: &str) -> &'static [&'static str] {
    for (k, v) in DIRTY_STATE_VARIANTS {
        if *k == state {
            return v;
        }
    }
    &[]
}

// ---------------------------------------------------------------------------
// Row-value generators (append into a caller-owned String buffer)
// ---------------------------------------------------------------------------

/// Reusable per-row scratch. Amortises the per-cell allocation that would
/// otherwise dominate hot-loop cost for the string columns. One instance per
/// worker thread, cleared before each append.
pub struct RowScratch {
    pub buf: String,
}

impl RowScratch {
    pub fn new() -> Self {
        Self {
            buf: String::with_capacity(128),
        }
    }
}

impl Default for RowScratch {
    fn default() -> Self {
        Self::new()
    }
}

/// Append one email of the form `userNNNNNN@domain` to `out`. With probability
/// `duplicate_pct`, injects `.DUPLICATE@` (a canary Silver looks for to dedupe).
///
/// The user id range matches Python `rng.integers(1000, 999999)` which is
/// half-open on the high end, so the largest emitted user id is 999_998. The
/// `below(998_999)` here + `+ 1000` reproduces exactly that inclusive range,
/// avoiding a `user999999@...` variant Python never emits.
pub fn append_email(rng: &mut Rng, out: &mut String, duplicate_pct: f64) {
    let user = 1000u64 + rng.below(998_999);
    let dom = EMAIL_DOMAINS[rng.below(EMAIL_DOMAINS.len() as u64) as usize];
    let is_dup = rng.unit() < duplicate_pct;
    out.push_str("user");
    push_u64(out, user);
    if is_dup {
        out.push_str(".DUPLICATE@");
    } else {
        out.push('@');
    }
    out.push_str(dom);
}

/// One of 6 dirty-mode transforms applied on top of an already-generated email.
/// Matches datagen/generate.py:421-434 mode-by-mode. Reads from `src` into
/// `out` so callers can double-buffer without churning allocations.
pub fn apply_email_corruption(src: &str, mode: u8, out: &mut String) {
    match mode % 6 {
        0 => {
            // missing @
            for c in src.chars() {
                if c != '@' {
                    out.push(c);
                }
            }
        }
        1 => {
            // ALL CAPS
            for c in src.chars() {
                out.extend(c.to_uppercase());
            }
        }
        2 => {
            // whitespace pad
            out.push_str("  ");
            out.push_str(src);
            out.push_str("  ");
        }
        3 => {
            // double @
            for c in src.chars() {
                if c == '@' {
                    out.push_str("@@");
                } else {
                    out.push(c);
                }
            }
        }
        4 => {
            // strip TLD
            if let Some(pos) = src.rfind('.') {
                out.push_str(&src[..pos]);
            } else {
                out.push_str(src);
            }
        }
        _ => {
            // @ -> .at.
            for c in src.chars() {
                if c == '@' {
                    out.push_str(".at.");
                } else {
                    out.push(c);
                }
            }
        }
    }
}

/// Append a phone number in one of two formats (50/50): `+1AAAPPPNNNN` or
/// `(AAA) PPP-NNNN`.
///
/// Ranges match Python: area/prefix `rng.integers(200, 999)` = [200, 998];
/// line `rng.integers(1000, 9999)` = [1000, 9998]. Rust equivalents use
/// `below(N-lo)` with N being the Python high (exclusive).
pub fn append_phone(rng: &mut Rng, out: &mut String) {
    let area = 200 + rng.below(799);
    let prefix = 200 + rng.below(799);
    let line = 1000 + rng.below(8999);
    if rng.unit() < 0.5 {
        out.push_str("+1");
        push_u64(out, area);
        push_u64(out, prefix);
        push_u64(out, line);
    } else {
        out.push('(');
        push_u64(out, area);
        out.push_str(") ");
        push_u64(out, prefix);
        out.push('-');
        push_u64(out, line);
    }
}

/// Corrupted phone: mode-selected replacement. Matches
/// datagen/generate.py:452-469. Note that the Python regenerates area/prefix/
/// line inside the corruption; we do the same so ids don't accidentally align.
pub fn append_phone_corrupt(rng: &mut Rng, out: &mut String, mode: u8) {
    let area = 200 + rng.below(799);
    let prefix = 200 + rng.below(799);
    let line = 1000 + rng.below(8999);
    match mode % 4 {
        0 => {
            // digits only
            push_u64(out, area);
            push_u64(out, prefix);
            push_u64(out, line);
        }
        1 => {
            // extra chars
            out.push_str("+1-");
            push_u64(out, area);
            out.push('-');
            push_u64(out, prefix);
            out.push('-');
            push_u64(out, line);
        }
        2 => {
            // "truncated" mode: emit area+prefix (6 digits max). Python does
            // `f"{area}{prefix}"[:8]` which is always 6 chars because area
            // and prefix are each 3 digits; the [:8] slice is a no-op and we
            // preserve that behavior for parity. If either component ever
            // grows past 3 digits, the .min(8) becomes a real cap.
            let mut tmp = String::with_capacity(6);
            push_u64(&mut tmp, area);
            push_u64(&mut tmp, prefix);
            let cut = tmp.len().min(8);
            out.push_str(&tmp[..cut]);
        }
        _ => {
            // letter O for first 0
            let mut tmp = String::with_capacity(10);
            push_u64(&mut tmp, area);
            push_u64(&mut tmp, prefix);
            push_u64(&mut tmp, line);
            let mut replaced = false;
            for c in tmp.chars() {
                if !replaced && c == '0' {
                    out.push('O');
                    replaced = true;
                } else {
                    out.push(c);
                }
            }
        }
    }
}

/// Append an IPv4 address as `a.b.c.d` with the Python ranges: a ∈ [1,254],
/// b ∈ [0,254], c ∈ [0,254], d ∈ [1,253].
pub fn append_ip(rng: &mut Rng, out: &mut String) {
    let a = 1 + rng.below(254);
    let b = rng.below(255);
    let c = rng.below(255);
    let d = 1 + rng.below(253);
    push_u64(out, a);
    out.push('.');
    push_u64(out, b);
    out.push('.');
    push_u64(out, c);
    out.push('.');
    push_u64(out, d);
}

/// Append a user-agent of shape `{browser}/{ver}.0 ({device}; {os})`.
pub fn append_user_agent(rng: &mut Rng, out: &mut String) {
    let browser = BROWSERS[rng.below(BROWSERS.len() as u64) as usize];
    let ver = 90 + rng.below(40);
    let device = DEVICE_TYPES[rng.below(DEVICE_TYPES.len() as u64) as usize];
    let os = OPERATING_SYSTEMS[rng.below(OPERATING_SYSTEMS.len() as u64) as usize];
    out.push_str(browser);
    out.push('/');
    push_u64(out, ver);
    out.push_str(".0 (");
    out.push_str(device);
    out.push_str("; ");
    out.push_str(os);
    out.push(')');
}

/// Append a 64-char lowercase-hex session fingerprint (32 random bytes).
pub fn append_fingerprint(rng: &mut Rng, out: &mut String) {
    for _ in 0..4 {
        let r = rng.next_u64();
        push_hex64(out, r);
    }
}

/// Append `size_kb * 1024` random bytes as lowercase hex (size = 2 * size_kb
/// KiB of hex chars in the output). Uses a chunked u64 draw to minimise RNG
/// call count -- each 64-bit draw produces 16 hex chars.
pub fn append_payload_hex(rng: &mut Rng, out: &mut String, size_kb: usize) {
    let total_bytes = size_kb * 1024;
    let full_u64s = total_bytes / 8;
    let tail = total_bytes % 8;
    for _ in 0..full_u64s {
        push_hex64(out, rng.next_u64());
    }
    if tail > 0 {
        let r = rng.next_u64().to_be_bytes();
        for i in 0..tail {
            push_hex_byte(out, r[i]);
        }
    }
}

// ---------------------------------------------------------------------------
// Small pure helpers used above -- keep inline so hot loops don't call across
// function boundaries per row.
// ---------------------------------------------------------------------------

const HEX: &[u8; 16] = b"0123456789abcdef";

#[inline(always)]
fn push_hex_byte(out: &mut String, b: u8) {
    // Safe because we only push ASCII bytes (0-9, a-f). Rust strings are UTF-8
    // and every byte in HEX is a 1-byte codepoint.
    unsafe {
        let v = out.as_mut_vec();
        v.push(HEX[(b >> 4) as usize]);
        v.push(HEX[(b & 0xf) as usize]);
    }
}

#[inline(always)]
fn push_hex64(out: &mut String, x: u64) {
    let bytes = x.to_be_bytes();
    for b in bytes {
        push_hex_byte(out, b);
    }
}

/// Format `n` as decimal into `out` without allocating a `String` per call.
/// `itoa`-style with a stack buffer, keeps the hot loop allocation-free.
#[inline(always)]
fn push_u64(out: &mut String, mut n: u64) {
    if n == 0 {
        out.push('0');
        return;
    }
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    // Safe: pushed bytes are ASCII digits.
    unsafe {
        out.as_mut_vec().extend_from_slice(&buf[i..]);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdf_normalises_and_ends_at_one() {
        let c = cdf(&INTERACTION_WEIGHTS);
        assert_eq!(c.len(), INTERACTION_WEIGHTS.len());
        assert_eq!(*c.last().unwrap(), 1.0);
        for w in c.windows(2) {
            assert!(w[1] >= w[0], "cdf must be monotone non-decreasing");
        }
    }

    #[test]
    fn pick_idx_landmarks() {
        let c = vec![0.18, 0.53, 0.65, 0.85, 1.0];
        assert_eq!(pick_idx(&c, 0.0), 0);
        assert_eq!(pick_idx(&c, 0.17), 0);
        assert_eq!(pick_idx(&c, 0.18), 1);
        assert_eq!(pick_idx(&c, 0.52), 1);
        assert_eq!(pick_idx(&c, 0.53), 2);
        assert_eq!(pick_idx(&c, 0.999), 4);
    }

    #[test]
    fn weighted_pick_hits_all_buckets_over_a_large_sample() {
        let c = cdf(&INTERACTION_WEIGHTS);
        let mut rng = Rng::new(42);
        let mut counts = [0u32; 5];
        for _ in 0..100_000 {
            let s = weighted_pick(&mut rng, &INTERACTION_TYPES, &c);
            let idx = INTERACTION_TYPES.iter().position(|&x| x == s).unwrap();
            counts[idx] += 1;
        }
        // Every bucket populated.
        for (i, c) in counts.iter().enumerate() {
            assert!(*c > 0, "bucket {} never sampled", i);
        }
        // Expected 18% for purchase, tolerance ±1.5 percentage points at 100k.
        let purchase_share = counts[0] as f64 / 100_000.0;
        assert!(
            (purchase_share - 0.18).abs() < 0.015,
            "purchase share {} outside band 0.165..0.195",
            purchase_share
        );
    }

    #[test]
    fn loyalty_lookup_determinism() {
        let a = LoyaltyLookup::build(42, 100);
        let b = LoyaltyLookup::build(42, 100);
        assert_eq!(a.member, b.member);
        assert_eq!(a.tier, b.tier);
    }

    #[test]
    fn loyalty_lookup_member_share_and_tier_split() {
        let lookup = LoyaltyLookup::build(42, 100_000);
        let members = lookup.member.iter().filter(|b| **b).count();
        let share = members as f64 / (lookup.member.len() as f64);
        assert!(
            (share - 0.6).abs() < 0.02,
            "member share {} outside 0.58..0.62",
            share
        );
        let mut tcounts = [0u64; 3];
        for &t in &lookup.tier {
            tcounts[t as usize] += 1;
        }
        let total = tcounts.iter().sum::<u64>() as f64;
        let bronze = tcounts[0] as f64 / total;
        let silver = tcounts[1] as f64 / total;
        let gold = tcounts[2] as f64 / total;
        assert!(
            (bronze - 0.70).abs() < 0.02,
            "bronze share {} outside band",
            bronze
        );
        assert!(
            (silver - 0.20).abs() < 0.02,
            "silver share {} outside band",
            silver
        );
        assert!(
            (gold - 0.10).abs() < 0.02,
            "gold share {} outside band",
            gold
        );
    }

    #[test]
    fn customer_id_sampler_id1_share_bounded() {
        // TruncatedZipf(2.3) over top 500 caps id=1 mass. Expected ~10-12%
        // (matches the financial PartySelector guard). Anything past ~15%
        // signals a supernode regression -- the exact thing this sampler
        // exists to prevent.
        let sampler = CustomerIdSampler::new(500_000);
        let mut rng = Rng::new(42);
        let mut counts = std::collections::HashMap::<u64, u64>::new();
        let n = 200_000u64;
        for _ in 0..n {
            *counts.entry(sampler.sample(&mut rng)).or_insert(0) += 1;
        }
        let id1 = *counts.get(&1).unwrap_or(&0) as f64 / n as f64;
        assert!(
            id1 < 0.15,
            "id=1 share {} exceeds 0.15 ceiling -- supernode regression?",
            id1
        );
        // id=1 should still be the head of the distribution though.
        let id2 = *counts.get(&2).unwrap_or(&0) as f64 / n as f64;
        assert!(id1 > id2, "id=1 ({}) should dominate id=2 ({})", id1, id2);
        // Distribution should be broad -- at least 1000 distinct ids.
        assert!(counts.len() >= 1000, "only {} distinct ids", counts.len());
    }

    #[test]
    fn customer_id_sampler_in_range() {
        let sampler = CustomerIdSampler::new(500_000);
        let mut rng = Rng::new(1234);
        for _ in 0..10_000 {
            let id = sampler.sample(&mut rng);
            assert!((1..=500_000).contains(&id), "id {} out of range", id);
        }
    }

    #[test]
    fn customer_id_sampler_determinism() {
        let s = CustomerIdSampler::new(10_000);
        let mut r1 = Rng::new(7);
        let mut r2 = Rng::new(7);
        for _ in 0..500 {
            assert_eq!(s.sample(&mut r1), s.sample(&mut r2));
        }
    }

    #[test]
    fn customer_id_sampler_tiny_population() {
        // hot_count(500) >= cid_max(100): retail band is empty, sampler falls
        // back to uniform over the full id space.
        let sampler = CustomerIdSampler::new(100);
        let mut rng = Rng::new(99);
        for _ in 0..500 {
            let id = sampler.sample(&mut rng);
            assert!((1..=100).contains(&id));
        }
    }

    #[test]
    fn customer_id_sampler_hot_share_roughly_matches_config() {
        // With hot_share=0.40, ~40% of draws should land on ids <= hot_count.
        // Not exact because the retail band is disjoint from the hot band.
        let sampler = CustomerIdSampler::new(500_000);
        let mut rng = Rng::new(5);
        let n = 100_000u64;
        let mut hot = 0u64;
        for _ in 0..n {
            if sampler.sample(&mut rng) <= 500 {
                hot += 1;
            }
        }
        let share = hot as f64 / n as f64;
        assert!(
            (0.36..0.44).contains(&share),
            "hot share {} outside 0.36..0.44",
            share
        );
    }

    #[test]
    fn sample_distinct_returns_unique() {
        let mut rng = Rng::new(7);
        let mut out = Vec::new();
        sample_distinct(&mut rng, 1000, 100, &mut out);
        assert_eq!(out.len(), 100);
        let mut sorted = out.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 100, "expected 100 unique, got dups");
        for &i in &out {
            assert!((i as usize) < 1000);
        }
    }

    #[test]
    fn sample_distinct_k_equals_n() {
        let mut rng = Rng::new(1);
        let mut out = Vec::new();
        sample_distinct(&mut rng, 200, 200, &mut out);
        assert_eq!(out.len(), 200);
        let mut sorted = out.clone();
        sorted.sort_unstable();
        for (i, v) in sorted.iter().enumerate() {
            assert_eq!(*v as usize, i);
        }
    }

    #[test]
    fn sample_distinct_k_zero_is_noop() {
        let mut rng = Rng::new(1);
        let mut out = vec![99u32];
        sample_distinct(&mut rng, 100, 0, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn city_variant_hits_and_misses() {
        assert_eq!(city_variant("Chicago"), Some("Chicgao"));
        assert_eq!(city_variant("New York"), Some("Newyork"));
        assert_eq!(city_variant("Dallas"), None);
    }

    #[test]
    fn state_variants_hits_and_misses() {
        let v = state_variants("CA");
        assert!(v.contains(&"california"));
        assert!(v.contains(&"CALIFORNIA"));
        assert_eq!(state_variants("XX"), &[] as &[&str]);
    }

    #[test]
    fn append_email_shape() {
        let mut rng = Rng::new(1);
        let mut buf = String::new();
        append_email(&mut rng, &mut buf, 0.0);
        assert!(buf.starts_with("user"), "email = {}", buf);
        assert!(buf.contains('@'), "email = {}", buf);
        assert!(
            EMAIL_DOMAINS.iter().any(|d| buf.ends_with(d)),
            "email = {}",
            buf
        );
    }

    #[test]
    fn append_email_duplicate_when_pct_is_one() {
        let mut rng = Rng::new(1);
        for _ in 0..20 {
            let mut buf = String::new();
            append_email(&mut rng, &mut buf, 1.0);
            assert!(buf.contains(".DUPLICATE@"), "email = {}", buf);
        }
    }

    #[test]
    fn apply_email_corruption_exact_output_per_mode() {
        // Pin exact outputs per mode so a subtle regression (e.g. rfind('.')
        // -> split_once('.'), or doubled substitutions) trips a unit test
        // rather than showing up as a Silver dedupe-detector drift months
        // later. Matches datagen/generate.py:421-434 mode-by-mode.
        let src = "user1234@gmail.com";
        let cases: [(u8, &str); 6] = [
            (0, "user1234gmail.com"),      // missing @
            (1, "USER1234@GMAIL.COM"),     // ALL CAPS
            (2, "  user1234@gmail.com  "), // whitespace pad
            (3, "user1234@@gmail.com"),    // double @
            (4, "user1234@gmail"),         // strip TLD (rfind('.'))
            (5, "user1234.at.gmail.com"),  // @ -> .at.
        ];
        for (mode, want) in cases {
            let mut out = String::new();
            apply_email_corruption(src, mode, &mut out);
            assert_eq!(out, want, "mode {} produced {:?}", mode, out);
        }
    }

    #[test]
    fn append_phone_shape_matches_one_of_two_formats() {
        let mut rng = Rng::new(1);
        for _ in 0..50 {
            let mut buf = String::new();
            append_phone(&mut rng, &mut buf);
            let ok = buf.starts_with("+1") || (buf.starts_with('(') && buf.contains(") "));
            assert!(ok, "unexpected phone shape: {}", buf);
        }
    }

    #[test]
    fn append_phone_corrupt_never_matches_clean_shape() {
        let mut rng = Rng::new(2);
        for mode in 0..4u8 {
            let mut buf = String::new();
            append_phone_corrupt(&mut rng, &mut buf, mode);
            assert!(!buf.is_empty(), "mode {} produced empty", mode);
        }
    }

    #[test]
    fn append_ip_within_ranges() {
        let mut rng = Rng::new(3);
        for _ in 0..500 {
            let mut buf = String::new();
            append_ip(&mut rng, &mut buf);
            let parts: Vec<&str> = buf.split('.').collect();
            assert_eq!(parts.len(), 4, "bad ip: {}", buf);
            let a: u32 = parts[0].parse().unwrap();
            let b: u32 = parts[1].parse().unwrap();
            let c: u32 = parts[2].parse().unwrap();
            let d: u32 = parts[3].parse().unwrap();
            assert!((1..=254).contains(&a), "a={} in {}", a, buf);
            assert!(b <= 254, "b={} in {}", b, buf);
            assert!(c <= 254, "c={} in {}", c, buf);
            assert!((1..=253).contains(&d), "d={} in {}", d, buf);
        }
    }

    #[test]
    fn append_user_agent_contains_known_browser() {
        let mut rng = Rng::new(4);
        for _ in 0..20 {
            let mut buf = String::new();
            append_user_agent(&mut rng, &mut buf);
            assert!(BROWSERS.iter().any(|b| buf.starts_with(b)), "ua = {}", buf);
            assert!(buf.contains('/'));
            assert!(buf.contains(") ") || buf.ends_with(')'));
        }
    }

    #[test]
    fn append_fingerprint_is_64_lowercase_hex() {
        let mut rng = Rng::new(5);
        for _ in 0..20 {
            let mut buf = String::new();
            append_fingerprint(&mut rng, &mut buf);
            assert_eq!(buf.len(), 64);
            assert!(buf
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
        }
    }

    #[test]
    fn append_payload_hex_length_matches_kb() {
        let mut rng = Rng::new(6);
        for kb in [0usize, 1, 2, 4, 8] {
            let mut buf = String::new();
            append_payload_hex(&mut rng, &mut buf, kb);
            assert_eq!(buf.len(), kb * 1024 * 2, "kb={}", kb);
            assert!(buf
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
        }
    }

    #[test]
    fn dirty_city_variants_are_all_distinct_from_their_key() {
        for (k, v) in DIRTY_CITY_VARIANTS {
            assert_ne!(k, v, "variant identical to key: {}", k);
        }
    }

    #[test]
    fn dirty_state_variants_cover_all_state_keys() {
        // Every state we might emit from CITIES should have variants OR be
        // absent from the corruption pipeline. This asserts the current design
        // choice: only US-state abbrevs and their long forms get corrupted.
        let states_in_cities: std::collections::HashSet<&str> =
            CITIES.iter().map(|(_, s)| *s).collect();
        for s in &states_in_cities {
            // Not every state MUST have a variant, but if it does, the list
            // must be non-empty.
            let v = state_variants(s);
            if !v.is_empty() {
                assert!(!v.contains(s), "variant equals key for {}", s);
            }
        }
    }
}
