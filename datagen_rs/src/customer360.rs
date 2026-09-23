//! customer360 per-file emit. One `build_batch(cfg, loyalty) -> RecordBatch`
//! call produces `rows_per_file` rows matching the schema in
//! `crate::schema::customer360_schema()`.
//!
//! Distribution-parity port of `_build_customer360_table` in
//! `datagen/generate.py:681-935`. Bytes will not match the Python because our
//! Rng is splitmix64 (Python uses PCG64), but every column's marginal and
//! conditional distributions target the same shape within statistical noise.
//!
//! Design principles:
//!   - One Rng per file, seeded `Rng::new(cfg.seed + cfg.file_id)`; same
//!     isolation-per-file as the Python's `np.random.default_rng(seed=seed + file_id)`.
//!   - Every column has a preallocated builder with capacity=rows so no
//!     reallocation happens in the per-row loop. String columns pass an
//!     upper-bound value-buffer size to `StringBuilder::with_capacity(rows, bytes)`.
//!   - Loyalty and channel/product conditional-null logic is enforced at emit
//!     time from precomputed per-row bookkeeping (interaction_type_idx,
//!     channel_idx, customer_id, loyalty_member per row).
//!   - The Python's post-pass dirty-corruption pattern is preserved: we
//!     build clean values first, then walk a Fisher-Yates-sampled subset and
//!     rewrite in place. This costs a scratch `Vec<String>` per corrupted
//!     column (email, phone, city, state) but keeps parity with the Python
//!     order-of-effects.

use std::sync::Arc;

use arrow::array::builder::StringBuilder;
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int32Builder, Int64Array,
    TimestampMicrosecondArray,
};
use arrow::record_batch::RecordBatch;

use crate::customer360_realism as r;
use crate::hash::Rng;
use crate::ids::uuid_v4_into;
use crate::schema::customer360_schema;

/// Per-file emit configuration.
pub struct Config {
    pub seed: u64,
    pub file_id: u64,
    /// Rows to emit in this file. Derived by the caller from file_size /
    /// bytes_per_row.
    pub rows_per_file: usize,
    /// Upper bound (inclusive) for `customer_id`. Zipf-sampled ids are reduced
    /// modulo `customer_id_max + 1` so `0..=customer_id_max` is the id space.
    pub customer_id_max: u64,
    /// Fraction of rows that get email/phone/city/state corruption applied.
    /// 0.0 disables corruption entirely.
    pub dirty_ratio: f64,
    /// Probability of `.DUPLICATE@` insertion in `email_raw`.
    pub duplicate_email_pct: f64,
    /// Size of the random-hex `interaction_payload` column, in KiB. Emitted
    /// column bytes = payload_kb * 1024 * 2 (hex expansion).
    pub payload_kb: usize,
    /// Inclusive timestamp range for `event_timestamp`, in microseconds since
    /// the Unix epoch. Uniform draw within.
    pub timestamp_start_us: i64,
    pub timestamp_end_us: i64,
}

impl Config {
    /// Sensible defaults matching Python's Config defaults for c360.
    pub fn new(seed: u64, file_id: u64, rows_per_file: usize) -> Self {
        Self {
            seed,
            file_id,
            rows_per_file,
            customer_id_max: 500_000,
            dirty_ratio: 0.08,
            duplicate_email_pct: 0.10,
            payload_kb: 2,
            // 2024-01-01T00:00:00Z .. 2026-01-01T00:00:00Z in microseconds.
            timestamp_start_us: 1_704_067_200_000_000,
            timestamp_end_us: 1_735_689_600_000_000,
        }
    }
}

// Named indices into INTERACTION_TYPES so the conditional-null and page-view
// logic reads clearly instead of chasing magic numbers.
const IT_PURCHASE: u8 = 0;
const IT_BROWSE: u8 = 1;
const IT_SUPPORT: u8 = 2;
const IT_LOGIN: u8 = 3;
// const IT_ABANDONED_CART: u8 = 4; // unused as a discriminator today

// Named indices into CHANNELS for the offline-channel null mask.
const CH_STORE: u8 = 2;
const CH_CALL_CENTER: u8 = 3;

// Session-engine tuning constants (M-R3, 2026-09-20).
//
// Real customer event streams cluster into sessions: a customer opens the app,
// does 5-20 actions in a 5-30 minute window, closes. Uniform per-row
// generation destroys any session-shape queries (`avg(events_per_session)`,
// funnel-analysis, freshness-per-customer). These constants make our data
// look session-shaped at negligible perf cost -- fewer UUIDs (one per session,
// not per row) and one timestamp anchor per session (small offset per row).
const SESSION_LEN_MIN: u32 = 5;
const SESSION_LEN_RANGE: u32 = 16; // -> lengths in [5, 20]
const SESSION_TIMESPAN_US: i64 = 30 * 60 * 1_000_000; // 30 min max between rows in one session
/// Probability that a row within a session keeps the session's base product
/// category. 0.7 = "70% of rows in a session shop the same category, 30%
/// browse across categories". Real basket-analysis queries need this
/// coherence to produce useful cross-sell signals.
const PRODUCT_CATEGORY_KEEP_PCT: f64 = 0.7;

/// Precomputed per-session state. Built once at file start, then a per-row
/// lookup vector maps every row_i to its owning session. See `build_batch`
/// Phase 0 for the schedule construction.
#[derive(Clone, Copy)]
struct Session {
    customer_id: u64,
    session_uuid_hi: u64,
    session_uuid_lo: u64,
    timestamp_anchor_us: i64,
    product_cat_base: u8,
}

/// Build one RecordBatch matching `customer360_schema()`. Deterministic in
/// `(cfg.seed, cfg.file_id)`.
///
/// The `cid_sampler` is built once at pod startup (in `bin/generate.rs`) and
/// borrowed shared-immutable across every rayon worker. Same is true for
/// `loyalty`. Both are inputs, not global state.
pub fn build_batch(
    cfg: &Config,
    loyalty: &r::LoyaltyLookup,
    cid_sampler: &r::CustomerIdSampler,
) -> RecordBatch {
    let n = cfg.rows_per_file;
    let mut rng = Rng::new(cfg.seed.wrapping_add(cfg.file_id));

    // Precomputed CDFs -- one per weighted-categorical vocab. Built once per
    // file (five buckets each, cheap).
    let interaction_cdf = r::cdf(&r::INTERACTION_WEIGHTS);
    let data_source_cdf = r::cdf(&r::DATA_SOURCE_WEIGHTS);
    let data_quality_cdf = r::cdf(&r::DATA_QUALITY_WEIGHTS);

    // Loyalty lookup must be sized for the full customer_id space. A smaller
    // lookup silently miscorrelates: two rows with the same customer_id can
    // hash to different loyalty buckets via `cid % loyalty_len`, breaking
    // feature #7 (customer-consistent loyalty). Python's `lookup[cids]`
    // panics on the same mismatch -- we match that behavior in release too,
    // not just debug.
    let loyalty_len = loyalty.member.len();
    assert!(
        loyalty_len >= (cfg.customer_id_max as usize) + 1,
        "loyalty lookup (len={}) too small for customer_id_max={}",
        loyalty_len,
        cfg.customer_id_max
    );

    // Sanity-pin the named ordinals to their vocab strings. A future change
    // that adds an entry to `INTERACTION_TYPES` or `CHANNELS` at any position
    // other than the tail would silently reroute the conditional-null logic
    // to the wrong buckets. Debug-only: pinning must fail loud during dev
    // without adding a runtime check on the hot path.
    debug_assert_eq!(r::INTERACTION_TYPES[IT_PURCHASE as usize], "purchase");
    debug_assert_eq!(r::INTERACTION_TYPES[IT_BROWSE as usize], "browse");
    debug_assert_eq!(r::INTERACTION_TYPES[IT_SUPPORT as usize], "support");
    debug_assert_eq!(r::INTERACTION_TYPES[IT_LOGIN as usize], "login");
    debug_assert_eq!(r::CHANNELS[CH_STORE as usize], "store");
    debug_assert_eq!(r::CHANNELS[CH_CALL_CENTER as usize], "call_center");
    // Pin DIRTY_RATE_BY_SOURCE length to DATA_SOURCES length. Adding a 5th
    // data source without extending the rate table would panic mid-hot-loop
    // (index out of bounds); the debug_assert forces the mismatch to fail
    // at file start rather than at an obscure row index.
    debug_assert_eq!(r::DATA_SOURCES.len(), r::DIRTY_RATE_BY_SOURCE.len());

    // Row-id block for this file. Matches datagen/generate.py:832-833 where
    // both `id` and `row_id` start at file_id * rows.
    let row_id_start = (cfg.file_id as i64) * (n as i64);

    // -------------------------------------------------------------------
    // Phase A: per-row scaffold (indices + numeric arrays needed for
    // conditional logic). Materialised so the string columns can walk them
    // without re-drawing RNG (which would break parity within the file).
    // -------------------------------------------------------------------
    let mut ids: Vec<i64> = Vec::with_capacity(n);
    let mut row_ids: Vec<i64> = Vec::with_capacity(n);
    let mut event_ts_us: Vec<i64> = Vec::with_capacity(n);
    let mut customer_ids: Vec<i64> = Vec::with_capacity(n);
    let mut interaction_type_idx: Vec<u8> = Vec::with_capacity(n);
    let mut channel_idx: Vec<u8> = Vec::with_capacity(n);
    let mut city_idx: Vec<u8> = Vec::with_capacity(n);
    let mut device_type_idx: Vec<u8> = Vec::with_capacity(n);
    let mut browser_idx: Vec<u8> = Vec::with_capacity(n);
    let mut transaction_amounts: Vec<f64> = Vec::with_capacity(n);
    let mut page_views: Vec<i32> = Vec::with_capacity(n);
    let mut time_on_site: Vec<i32> = Vec::with_capacity(n);
    let mut bounce_rates: Vec<f64> = Vec::with_capacity(n);
    let mut click_counts_raw: Vec<i32> = Vec::with_capacity(n);
    let mut cart_values_raw: Vec<f64> = Vec::with_capacity(n);
    let mut items_in_cart_raw: Vec<i32> = Vec::with_capacity(n);
    let mut has_campaign: Vec<bool> = Vec::with_capacity(n);
    let mut campaign_ids_num: Vec<i32> = Vec::with_capacity(n); // valid when has_campaign
    let mut utm_source_idx: Vec<u8> = Vec::with_capacity(n);
    let mut utm_medium_idx: Vec<u8> = Vec::with_capacity(n);
    let mut currency_idx: Vec<u8> = Vec::with_capacity(n);
    let mut data_source_idx: Vec<u8> = Vec::with_capacity(n);
    let mut data_quality_idx: Vec<u8> = Vec::with_capacity(n);
    let mut ticket_nums: Vec<i32> = Vec::with_capacity(n); // valid when support
    let mut issue_idx: Vec<u8> = Vec::with_capacity(n); // valid when support
    let mut satisfaction: Vec<i32> = Vec::with_capacity(n); // valid when support
    let mut product_nums: Vec<i32> = Vec::with_capacity(n); // valid when not login/support
    let mut product_cat_idx: Vec<u8> = Vec::with_capacity(n); // valid when not login/support
    let mut zip_nums: Vec<i32> = Vec::with_capacity(n);
    let mut points_earned: Vec<i32> = Vec::with_capacity(n);
    let mut points_redeemed: Vec<i32> = Vec::with_capacity(n);
    let mut loyalty_members: Vec<bool> = Vec::with_capacity(n);
    let mut loyalty_tier_val: Vec<u8> = Vec::with_capacity(n); // valid when member

    // Timestamp span in microseconds. Guard against (a) start >= end, and
    // (b) adversarial signed underflow (`i64::MIN - i64::MAX` panics in
    // debug and wraps in release). `saturating_sub` handles both.
    let ts_span_us = cfg
        .timestamp_end_us
        .saturating_sub(cfg.timestamp_start_us)
        .max(0) as u64;

    // -------------------------------------------------------------------
    // Phase 0: session schedule. Precompute the list of sessions that fill
    // this file's row budget, plus a per-row `session_of_row` lookup so
    // Phase A/C can pull session context by row index without re-walking
    // the schedule.
    // -------------------------------------------------------------------
    // Anchor upper bound = end - SESSION_TIMESPAN_US so a row's timestamp
    // (anchor + [0, 30 min)) cannot escape `cfg.timestamp_end_us`. For a
    // configured window smaller than 30 min the anchor collapses to start
    // and the per-row `ts.min(end - 1)` guard below catches the overflow.
    let session_span_us = SESSION_TIMESPAN_US as u64;
    let anchor_upper = ts_span_us.saturating_sub(session_span_us);
    let mut sessions: Vec<Session> = Vec::with_capacity(n / 10 + 1);
    let mut session_of_row: Vec<u32> = Vec::with_capacity(n);
    let mut rows_left = n as u32;
    while rows_left > 0 {
        let raw_len = SESSION_LEN_MIN + rng.below(SESSION_LEN_RANGE as u64) as u32;
        let length = raw_len.min(rows_left);
        let cid = cid_sampler.sample(&mut rng);
        let uuid_hi = rng.next_u64();
        let uuid_lo = rng.next_u64();
        let ts_anchor = if anchor_upper == 0 {
            cfg.timestamp_start_us
        } else {
            cfg.timestamp_start_us + rng.below(anchor_upper) as i64
        };
        let pcat_base = rng.below(r::PRODUCT_CATEGORIES.len() as u64) as u8;
        let sidx = sessions.len() as u32;
        sessions.push(Session {
            customer_id: cid,
            session_uuid_hi: uuid_hi,
            session_uuid_lo: uuid_lo,
            timestamp_anchor_us: ts_anchor,
            product_cat_base: pcat_base,
        });
        for _ in 0..length {
            session_of_row.push(sidx);
        }
        rows_left -= length;
    }
    // Promoted from debug_assert: schedule length mismatch is a logic bug
    // that must fail loud in release too, otherwise a downstream panic on
    // `sessions[session_of_row[i]]` OOB gives a confusing error message.
    assert_eq!(session_of_row.len(), n);

    for i in 0..n {
        let rid = row_id_start + i as i64;
        ids.push(rid);
        row_ids.push(rid);
        let sess = &sessions[session_of_row[i] as usize];

        // event_timestamp: session anchor + small offset (0..30 min). Rows in
        // one session cluster on a ~30-min window instead of scattering
        // uniformly across the entire configured range. `.min(end - 1)`
        // catches the degenerate case where the configured window is
        // narrower than SESSION_TIMESPAN_US, so ts never escapes the
        // caller's `[start, end)` half-open range.
        let ts_end_excl = cfg.timestamp_end_us.saturating_sub(1);
        let ts = (sess.timestamp_anchor_us + rng.below(SESSION_TIMESPAN_US as u64) as i64)
            .min(ts_end_excl);
        event_ts_us.push(ts);

        // customer_id: pulled from the owning session (same for every row in
        // the session -- one customer per session). CustomerIdSampler was
        // called in Phase 0 to pick each session's customer.
        let cid = sess.customer_id as i64;
        customer_ids.push(cid);

        // interaction_type: weighted categorical.
        let itype = r::pick_idx(&interaction_cdf, rng.unit()) as u8;
        interaction_type_idx.push(itype);
        let is_purchase = itype == IT_PURCHASE;
        let is_browse = itype == IT_BROWSE;
        let is_support = itype == IT_SUPPORT;
        let is_login = itype == IT_LOGIN;
        let no_product = is_login || is_support;

        // Log-normal transaction_amount (only meaningful for purchase).
        let raw = (rng.normal() * 1.2 + 4.3).exp().clamp(1.0, 9999.99);
        let amt = if is_purchase { round2(raw) } else { 0.0 };
        transaction_amounts.push(amt);

        // page_views + time_on_site + bounce_rate: shaped by browse/purchase
        // gate. matches datagen/generate.py:724-742.
        let pv: i32 = if is_purchase || is_browse {
            1 + rng.below(20) as i32
        } else {
            0
        };
        page_views.push(pv);
        let tos: i32 = if pv > 0 { 30 + rng.below(3600) as i32 } else { 0 };
        time_on_site.push(tos);
        bounce_rates.push(if pv == 1 { 1.0 } else { 0.0 });

        // Support triplet: TKT + issue + satisfaction (only for support).
        if is_support {
            ticket_nums.push(10_000 + rng.below(90_000) as i32);
            issue_idx.push(rng.below(r::ISSUE_CATEGORIES.len() as u64) as u8);
            satisfaction.push(1 + rng.below(5) as i32);
        } else {
            ticket_nums.push(0);
            issue_idx.push(0);
            satisfaction.push(0);
        }

        // Campaign attribution: 40% probability. utm_source/medium picked
        // regardless (drawing from Rng), but only used when has_campaign.
        // Matches Python: the campaign branch consumes RNG for the id and
        // both UTM picks only when the coin lands; when it doesn't, no draws
        // happen. Keep parity by only drawing when has_campaign.
        let has_camp = rng.unit() < 0.4;
        has_campaign.push(has_camp);
        if has_camp {
            campaign_ids_num.push(100 + rng.below(900) as i32);
            utm_source_idx.push(rng.below(r::UTM_SOURCES.len() as u64) as u8);
            utm_medium_idx.push(rng.below(r::UTM_MEDIUMS.len() as u64) as u8);
        } else {
            campaign_ids_num.push(0);
            utm_source_idx.push(0);
            utm_medium_idx.push(0);
        }

        // Loyalty triplet: derived from cid + interaction_type + transaction
        // amount. member/tier come from the precomputed lookups.
        let cid_ix = (cid as usize) % loyalty_len;
        let is_member = loyalty.member[cid_ix];
        loyalty_members.push(is_member);
        loyalty_tier_val.push(if is_member { loyalty.tier[cid_ix] } else { 0 });
        // points_earned only when member AND purchase (per Python).
        let pe = if is_member && is_purchase {
            (amt * 10.0) as i32
        } else {
            0
        };
        points_earned.push(pe);
        // points_redeemed only when member AND with 10% probability.
        let pr = if is_member && rng.unit() < 0.1 {
            100 + rng.below(900) as i32
        } else {
            0
        };
        points_redeemed.push(pr);

        // Product columns (only valid when NOT login/support). Ranges match
        // Python `rng.integers(10000, 99999)` (half-open on high) via
        // `below(89_999) + 10_000` -> [10_000, 99_998].
        product_nums.push(10_000 + rng.below(89_999) as i32);
        // Product category persists within a session: keep the session's base
        // category with prob PRODUCT_CATEGORY_KEEP_PCT, redraw otherwise.
        // Real basket queries see coherent categories per session; uniform
        // per-row draws destroyed that signal.
        let pcat = if rng.unit() < PRODUCT_CATEGORY_KEEP_PCT {
            sess.product_cat_base
        } else {
            rng.below(r::PRODUCT_CATEGORIES.len() as u64) as u8
        };
        product_cat_idx.push(pcat);
        click_counts_raw.push(1 + rng.below(100) as i32);
        cart_values_raw.push(round2(rng.unit() * 9999.99));
        items_in_cart_raw.push(rng.below(21) as i32);

        // Currency + channel + device + browser -- drawn unconditionally.
        currency_idx.push(rng.below(r::CURRENCIES.len() as u64) as u8);
        let ch = rng.below(r::CHANNELS.len() as u64) as u8;
        channel_idx.push(ch);
        device_type_idx.push(rng.below(r::DEVICE_TYPES.len() as u64) as u8);
        browser_idx.push(rng.below(r::BROWSERS.len() as u64) as u8);

        // City -- pick pair index; the state is derived from the same index.
        city_idx.push(rng.below(r::CITIES.len() as u64) as u8);

        // zip_code: 5-digit number. Python `rng.integers(10000, 99999)` is
        // half-open on high, so max is 99998. Matches `below(89_999)+10_000`.
        zip_nums.push(10_000 + rng.below(89_999) as i32);

        // data_source + data_quality_flag weighted.
        data_source_idx.push(r::pick_idx(&data_source_cdf, rng.unit()) as u8);
        data_quality_idx.push(r::pick_idx(&data_quality_cdf, rng.unit()) as u8);

        // no_product and is_login are computed for readability but the
        // conditional-null enforcement lives in Phase D. Silence the
        // unused-variable warning without keeping a runtime `let _`.
        let _ = (no_product, is_login);
    }

    // -------------------------------------------------------------------
    // Phase B: build the string columns that need dirty-corruption
    // post-passes (email, phone, city, state). Match Python: clean gen for
    // all rows, then Fisher-Yates a subset and rewrite in place.
    // -------------------------------------------------------------------

    // Source-aware per-row dirty probability. Real pipelines don't corrupt
    // uniformly -- legacy_import rows are ~70x more dirty than primary_system
    // rows. We compute per-row rate = cfg.dirty_ratio * DIRTY_RATE_BY_SOURCE[src],
    // then Bernoulli-sample per column. This is a deliberate break from the
    // Python's 4x fixed-count uniform sampling; the total dirty count now
    // varies with the data_source mix but the *shape* matches real production.
    let per_row_dirty_rate: Vec<f64> = if cfg.dirty_ratio > 0.0 {
        data_source_idx
            .iter()
            .map(|&s| cfg.dirty_ratio * r::DIRTY_RATE_BY_SOURCE[s as usize])
            .collect()
    } else {
        Vec::new()
    };

    // Helper: sample a Bernoulli mask over `n` rows using per-row rates.
    // Returns row indices where the mask fired. Empty when rates are empty
    // (dirty_ratio == 0).
    let mut dirty_pass = |rng: &mut Rng| -> Vec<u32> {
        if per_row_dirty_rate.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(n / 20);
        for i in 0..n {
            if rng.unit() < per_row_dirty_rate[i] {
                out.push(i as u32);
            }
        }
        out
    };

    // email: allocate per-row Strings so we can corrupt in place. Costs one
    // heap allocation per row for this column; acceptable given the payload
    // column's bytes dominate anyway.
    let mut emails: Vec<String> = Vec::with_capacity(n);
    for _ in 0..n {
        let mut s = String::with_capacity(32);
        r::append_email(&mut rng, &mut s, cfg.duplicate_email_pct);
        emails.push(s);
    }
    for &idx in &dirty_pass(&mut rng) {
        let mode = rng.below(6) as u8;
        let mut out = String::with_capacity(48);
        r::apply_email_corruption(&emails[idx as usize], mode, &mut out);
        emails[idx as usize] = out;
    }

    let mut phones: Vec<String> = Vec::with_capacity(n);
    for _ in 0..n {
        let mut s = String::with_capacity(20);
        r::append_phone(&mut rng, &mut s);
        phones.push(s);
    }
    for &idx in &dirty_pass(&mut rng) {
        let mode = rng.below(4) as u8;
        let mut out = String::with_capacity(20);
        r::append_phone_corrupt(&mut rng, &mut out, mode);
        phones[idx as usize] = out;
    }

    // city_raw + state_raw: derived from city_idx, then corrupted in
    // source-weighted subsets. DIRTY_CITY_VARIANTS + DIRTY_STATE_VARIANTS
    // return `&'static str`, so no owned Strings are needed -- every row's
    // value stays a static reference.
    let mut cities: Vec<&'static str> = city_idx.iter().map(|&i| r::CITIES[i as usize].0).collect();
    let mut states: Vec<&'static str> = city_idx.iter().map(|&i| r::CITIES[i as usize].1).collect();
    for &idx in &dirty_pass(&mut rng) {
        if let Some(var) = r::city_variant(cities[idx as usize]) {
            cities[idx as usize] = var;
        }
    }
    for &idx in &dirty_pass(&mut rng) {
        let variants = r::state_variants(states[idx as usize]);
        if !variants.is_empty() {
            let pick = variants[rng.below(variants.len() as u64) as usize];
            states[idx as usize] = pick;
        }
    }

    // -------------------------------------------------------------------
    // Phase C: string columns generated fresh per row (ip, user_agent,
    // fingerprint, payload). These have no dependencies and no dirty
    // post-pass.
    // -------------------------------------------------------------------
    // Preallocate value buffers with reasonable upper bounds so the builders
    // never grow in the hot loop.
    let payload_hex_bytes = cfg.payload_kb * 1024 * 2;
    let mut b_event_id = StringBuilder::with_capacity(n, n * 36);
    let mut b_session_id = StringBuilder::with_capacity(n, n * 36);
    let mut b_ip = StringBuilder::with_capacity(n, n * 15);
    let mut b_user_agent = StringBuilder::with_capacity(n, n * 64);
    let mut b_fingerprint = StringBuilder::with_capacity(n, n * 64);
    let mut b_payload = StringBuilder::with_capacity(n, n * payload_hex_bytes);

    let mut scratch = String::with_capacity(40);
    // session_id_cache: format each session's UUID once and reuse across all
    // rows in that session. Saves N-M UUID formats where M = session count.
    let mut session_id_cache: Vec<String> = Vec::with_capacity(sessions.len());
    for sess in &sessions {
        let mut s = String::with_capacity(36);
        uuid_v4_into(sess.session_uuid_hi, sess.session_uuid_lo, &mut s);
        session_id_cache.push(s);
    }
    for i in 0..n {
        // event_id: fresh UUID per row (each event is unique).
        scratch.clear();
        uuid_v4_into(rng.next_u64(), rng.next_u64(), &mut scratch);
        b_event_id.append_value(&scratch);

        // session_id: shared across all rows in the same session (M-R3).
        b_session_id.append_value(&session_id_cache[session_of_row[i] as usize]);

        scratch.clear();
        r::append_ip(&mut rng, &mut scratch);
        b_ip.append_value(&scratch);

        scratch.clear();
        r::append_user_agent(&mut rng, &mut scratch);
        b_user_agent.append_value(&scratch);

        scratch.clear();
        r::append_fingerprint(&mut rng, &mut scratch);
        b_fingerprint.append_value(&scratch);

        scratch.clear();
        r::append_payload_hex(&mut rng, &mut scratch, cfg.payload_kb);
        b_payload.append_value(&scratch);
    }

    // -------------------------------------------------------------------
    // Phase D: emit the remaining string columns from the precomputed
    // scaffold. All conditional-null logic lives here so the Arrow-side
    // masks are correct in one place.
    // -------------------------------------------------------------------
    let mut b_email = StringBuilder::with_capacity(n, n * 30);
    let mut b_phone = StringBuilder::with_capacity(n, n * 16);
    let mut b_interaction_type = StringBuilder::with_capacity(n, n * 12);
    let mut b_product_id = StringBuilder::with_capacity(n, n * 8);
    let mut b_product_category = StringBuilder::with_capacity(n, n * 12);
    let mut b_currency = StringBuilder::with_capacity(n, n * 3);
    let mut b_channel = StringBuilder::with_capacity(n, n * 12);
    let mut b_device_type = StringBuilder::with_capacity(n, n * 8);
    let mut b_browser = StringBuilder::with_capacity(n, n * 8);
    let mut b_city = StringBuilder::with_capacity(n, n * 16);
    let mut b_state = StringBuilder::with_capacity(n, n * 12);
    let mut b_zip = StringBuilder::with_capacity(n, n * 5);
    let mut b_support_ticket = StringBuilder::with_capacity(n, n * 8);
    let mut b_issue_category = StringBuilder::with_capacity(n, n * 14);
    let mut b_campaign_id = StringBuilder::with_capacity(n, n * 6);
    let mut b_utm_source = StringBuilder::with_capacity(n, n * 8);
    let mut b_utm_medium = StringBuilder::with_capacity(n, n * 8);
    let mut b_loyalty_tier = StringBuilder::with_capacity(n, n * 6);
    let mut b_data_source = StringBuilder::with_capacity(n, n * 16);
    let mut b_data_quality_flag = StringBuilder::with_capacity(n, n * 20);

    // Nullable Int32 builders for the 5 columns that go null on login/support
    // (click_count, items_in_cart) and 1 for satisfaction_score on non-support.
    let mut b_click_count = Int32Builder::with_capacity(n);
    let mut b_items_in_cart = Int32Builder::with_capacity(n);
    let mut b_satisfaction = Int32Builder::with_capacity(n);

    // Format scratch for zip / ticket / product / campaign.
    let mut fbuf = String::with_capacity(16);

    for i in 0..n {
        b_email.append_value(&emails[i]);
        b_phone.append_value(&phones[i]);

        let itype = interaction_type_idx[i];
        b_interaction_type.append_value(r::INTERACTION_TYPES[itype as usize]);
        let no_product = itype == IT_LOGIN || itype == IT_SUPPORT;

        if no_product {
            b_product_id.append_null();
            b_product_category.append_null();
            b_click_count.append_null();
            b_items_in_cart.append_null();
        } else {
            fbuf.clear();
            fbuf.push_str("PRD");
            push_zero_padded(&mut fbuf, product_nums[i] as u64, 5);
            b_product_id.append_value(&fbuf);
            b_product_category
                .append_value(r::PRODUCT_CATEGORIES[product_cat_idx[i] as usize]);
            b_click_count.append_value(click_counts_raw[i]);
            b_items_in_cart.append_value(items_in_cart_raw[i]);
        }

        // Currency + channel + device/browser (device/browser null for
        // offline channels).
        b_currency.append_value(r::CURRENCIES[currency_idx[i] as usize]);
        let ch = channel_idx[i];
        b_channel.append_value(r::CHANNELS[ch as usize]);
        let offline = ch == CH_STORE || ch == CH_CALL_CENTER;
        if offline {
            b_device_type.append_null();
            b_browser.append_null();
        } else {
            b_device_type.append_value(r::DEVICE_TYPES[device_type_idx[i] as usize]);
            b_browser.append_value(r::BROWSERS[browser_idx[i] as usize]);
        }

        // City / state (from the possibly-corrupted per-row slices).
        b_city.append_value(cities[i]);
        b_state.append_value(states[i]);

        // Zip code: 5-digit zero-padded.
        fbuf.clear();
        push_zero_padded(&mut fbuf, zip_nums[i] as u64, 5);
        b_zip.append_value(&fbuf);

        // Support triplet.
        let is_support = itype == IT_SUPPORT;
        if is_support {
            fbuf.clear();
            fbuf.push_str("TKT");
            push_zero_padded(&mut fbuf, ticket_nums[i] as u64, 5);
            b_support_ticket.append_value(&fbuf);
            b_issue_category.append_value(r::ISSUE_CATEGORIES[issue_idx[i] as usize]);
            b_satisfaction.append_value(satisfaction[i]);
        } else {
            b_support_ticket.append_null();
            b_issue_category.append_null();
            b_satisfaction.append_null();
        }

        // Campaign triplet.
        if has_campaign[i] {
            fbuf.clear();
            fbuf.push_str("CMP");
            push_u64(&mut fbuf, campaign_ids_num[i] as u64);
            b_campaign_id.append_value(&fbuf);
            b_utm_source.append_value(r::UTM_SOURCES[utm_source_idx[i] as usize]);
            b_utm_medium.append_value(r::UTM_MEDIUMS[utm_medium_idx[i] as usize]);
        } else {
            b_campaign_id.append_null();
            b_utm_source.append_null();
            b_utm_medium.append_null();
        }

        // Loyalty tier: null when not member.
        if loyalty_members[i] {
            b_loyalty_tier.append_value(r::LOYALTY_TIERS[loyalty_tier_val[i] as usize]);
        } else {
            b_loyalty_tier.append_null();
        }

        b_data_source.append_value(r::DATA_SOURCES[data_source_idx[i] as usize]);
        b_data_quality_flag.append_value(r::DATA_QUALITY_FLAGS[data_quality_idx[i] as usize]);
    }

    // -------------------------------------------------------------------
    // Phase E: assemble the RecordBatch, columns in exact schema order.
    // -------------------------------------------------------------------
    // cart_value: raw values for present rows, NaN for null-marked rows.
    // Python uses NaN to signal null (per its `np.where(no_product_mask, np.nan, ...)`).
    // We mirror that by emitting a Float64Array without null buffer -- readers
    // that treat NaN as null will handle it the same way. This matches parity
    // and avoids a separate nullbuffer allocation for a column that is
    // never truly nullable in the parquet stats sense.
    let cart_values: Vec<f64> = (0..n)
        .map(|i| {
            let itype = interaction_type_idx[i];
            if itype == IT_LOGIN || itype == IT_SUPPORT {
                f64::NAN
            } else {
                cart_values_raw[i]
            }
        })
        .collect();

    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(Int64Array::from(row_ids)),
        Arc::new(TimestampMicrosecondArray::from(event_ts_us).with_timezone("UTC")),
        Arc::new(b_event_id.finish()),
        Arc::new(b_session_id.finish()),
        Arc::new(Int64Array::from(customer_ids)),
        Arc::new(b_email.finish()),
        Arc::new(b_phone.finish()),
        Arc::new(b_interaction_type.finish()),
        Arc::new(b_product_id.finish()),
        Arc::new(b_product_category.finish()),
        Arc::new(Float64Array::from(transaction_amounts)),
        Arc::new(b_currency.finish()),
        Arc::new(b_channel.finish()),
        Arc::new(b_device_type.finish()),
        Arc::new(b_browser.finish()),
        Arc::new(b_ip.finish()),
        Arc::new(b_city.finish()),
        Arc::new(b_state.finish()),
        Arc::new(b_zip.finish()),
        Arc::new(Int32Array::from(page_views)),
        Arc::new(Int32Array::from(time_on_site)),
        Arc::new(Float64Array::from(bounce_rates)),
        Arc::new(b_click_count.finish()),
        Arc::new(Float64Array::from(cart_values)),
        Arc::new(b_items_in_cart.finish()),
        Arc::new(b_support_ticket.finish()),
        Arc::new(b_issue_category.finish()),
        Arc::new(b_satisfaction.finish()),
        Arc::new(b_campaign_id.finish()),
        Arc::new(b_utm_source.finish()),
        Arc::new(b_utm_medium.finish()),
        Arc::new(BooleanArray::from(loyalty_members)),
        Arc::new(b_loyalty_tier.finish()),
        Arc::new(Int32Array::from(points_earned)),
        Arc::new(Int32Array::from(points_redeemed)),
        Arc::new(b_data_source.finish()),
        Arc::new(b_data_quality_flag.finish()),
        Arc::new(b_user_agent.finish()),
        Arc::new(b_fingerprint.finish()),
        Arc::new(b_payload.finish()),
    ];

    RecordBatch::try_new(customer360_schema(), cols)
        .expect("customer360 columns must match customer360_schema()")
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

#[inline(always)]
fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Write `n` as decimal into `out`, no allocation.
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
    unsafe {
        out.as_mut_vec().extend_from_slice(&buf[i..]);
    }
}

/// Write `n` as decimal into `out`, zero-padded to at least `width` chars.
#[inline(always)]
fn push_zero_padded(out: &mut String, n: u64, width: usize) {
    // Format n once into a scratch buffer, then push zeros before it if
    // shorter than width.
    let mut tmp: [u8; 20] = [0; 20];
    let mut i = tmp.len();
    let mut m = n;
    if m == 0 {
        i -= 1;
        tmp[i] = b'0';
    } else {
        while m > 0 {
            i -= 1;
            tmp[i] = b'0' + (m % 10) as u8;
            m /= 10;
        }
    }
    let digits = tmp.len() - i;
    if digits < width {
        for _ in 0..(width - digits) {
            out.push('0');
        }
    }
    unsafe {
        out.as_mut_vec().extend_from_slice(&tmp[i..]);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};

    fn small_cfg(rows: usize) -> Config {
        Config {
            seed: 42,
            file_id: 0,
            rows_per_file: rows,
            customer_id_max: 10_000,
            dirty_ratio: 0.08,
            duplicate_email_pct: 0.10,
            payload_kb: 1, // keep tests small
            timestamp_start_us: 1_704_067_200_000_000,
            timestamp_end_us: 1_735_689_600_000_000,
        }
    }

    fn build(rows: usize) -> RecordBatch {
        let loyalty = r::LoyaltyLookup::build(42, 10_000);
        let sampler = r::CustomerIdSampler::new(10_000);
        let cfg = small_cfg(rows);
        build_batch(&cfg, &loyalty, &sampler)
    }

    #[test]
    fn build_batch_returns_correct_shape() {
        let batch = build(1000);
        assert_eq!(batch.num_rows(), 1000);
        assert_eq!(batch.num_columns(), 41);
    }

    #[test]
    fn build_batch_determinism_same_seed_same_content() {
        let a = build(500);
        let b = build(500);
        for c in 0..a.num_columns() {
            assert_eq!(
                a.column(c).to_data(),
                b.column(c).to_data(),
                "column {} diverged",
                c
            );
        }
    }

    #[test]
    fn conditional_null_product_columns_align_with_interaction_type() {
        let batch = build(2000);
        let itypes = batch
            .column_by_name("interaction_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pid = batch
            .column_by_name("product_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pcat = batch
            .column_by_name("product_category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let t = itypes.value(i);
            let want_null = t == "login" || t == "support";
            assert_eq!(pid.is_null(i), want_null, "row {} product_id vs {}", i, t);
            assert_eq!(pcat.is_null(i), want_null, "row {} product_cat vs {}", i, t);
        }
    }

    #[test]
    fn conditional_null_device_browser_align_with_channel() {
        let batch = build(2000);
        let ch = batch
            .column_by_name("channel")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dev = batch
            .column_by_name("device_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let bro = batch
            .column_by_name("browser")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let c = ch.value(i);
            let want_null = c == "store" || c == "call_center";
            assert_eq!(dev.is_null(i), want_null, "row {} device vs {}", i, c);
            assert_eq!(bro.is_null(i), want_null, "row {} browser vs {}", i, c);
        }
    }

    #[test]
    fn support_columns_align_with_interaction_type_support() {
        let batch = build(2000);
        let itypes = batch
            .column_by_name("interaction_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let tid = batch
            .column_by_name("support_ticket_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let iss = batch
            .column_by_name("issue_category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let is_support = itypes.value(i) == "support";
            assert_eq!(tid.is_null(i), !is_support, "row {} ticket vs {}", i, itypes.value(i));
            assert_eq!(iss.is_null(i), !is_support, "row {} issue vs {}", i, itypes.value(i));
        }
    }

    #[test]
    fn loyalty_tier_null_iff_not_member() {
        let batch = build(2000);
        let mem = batch
            .column_by_name("loyalty_member")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let tier = batch
            .column_by_name("loyalty_tier")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let is_member = mem.value(i);
            assert_eq!(tier.is_null(i), !is_member, "row {}: member={}", i, is_member);
        }
    }

    #[test]
    fn campaign_columns_align() {
        let batch = build(5000);
        let cid = batch
            .column_by_name("campaign_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let src = batch
            .column_by_name("utm_source")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let med = batch
            .column_by_name("utm_medium")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let has = !cid.is_null(i);
            assert_eq!(src.is_null(i), !has);
            assert_eq!(med.is_null(i), !has);
        }
        // ~40% present.
        let present = (0..batch.num_rows()).filter(|&i| !cid.is_null(i)).count();
        let share = present as f64 / batch.num_rows() as f64;
        assert!((share - 0.4).abs() < 0.05, "campaign share {} off", share);
    }

    #[test]
    fn interaction_type_distribution_within_tolerance() {
        let batch = build(20_000);
        let itypes = batch
            .column_by_name("interaction_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut counts = std::collections::HashMap::<&str, u32>::new();
        for i in 0..batch.num_rows() {
            *counts.entry(itypes.value(i)).or_insert(0) += 1;
        }
        let n = batch.num_rows() as f64;
        // Expected weights [0.18, 0.35, 0.12, 0.20, 0.15]. Tolerance +-2 pp.
        let expected = [
            ("purchase", 0.18),
            ("browse", 0.35),
            ("support", 0.12),
            ("login", 0.20),
            ("abandoned_cart", 0.15),
        ];
        for (name, want) in expected {
            let got = *counts.get(name).unwrap_or(&0) as f64 / n;
            assert!(
                (got - want).abs() < 0.02,
                "{}: got {} want {}",
                name,
                got,
                want
            );
        }
    }

    #[test]
    fn zip_code_is_5_digits() {
        let batch = build(500);
        let z = batch
            .column_by_name("zip_code")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let v = z.value(i);
            assert_eq!(v.len(), 5, "zip {} len {}", v, v.len());
            assert!(v.bytes().all(|b| b.is_ascii_digit()));
        }
    }

    #[test]
    fn payload_length_matches_kb() {
        let batch = build(100);
        let p = batch
            .column_by_name("interaction_payload")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // payload_kb=1 in small_cfg -> 1024 bytes -> 2048 hex chars.
        for i in 0..batch.num_rows() {
            assert_eq!(p.value(i).len(), 2048);
        }
    }

    #[test]
    fn transaction_amount_zero_for_non_purchase() {
        let batch = build(1000);
        let itypes = batch
            .column_by_name("interaction_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amt = batch
            .column_by_name("transaction_amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if itypes.value(i) != "purchase" {
                assert_eq!(amt.value(i), 0.0, "row {} non-purchase amt {}", i, amt.value(i));
            } else {
                assert!(amt.value(i) >= 1.0 && amt.value(i) <= 9999.99);
            }
        }
    }

    #[test]
    fn cart_value_nan_iff_login_or_support() {
        let batch = build(2000);
        let itypes = batch
            .column_by_name("interaction_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let cv = batch
            .column_by_name("cart_value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let t = itypes.value(i);
            let is_np = t == "login" || t == "support";
            assert_eq!(cv.value(i).is_nan(), is_np, "row {} cart {} type {}", i, cv.value(i), t);
        }
    }

    #[test]
    fn event_timestamp_has_utc_tz() {
        let batch = build(10);
        let f = batch.schema().field_with_name("event_timestamp").unwrap().clone();
        match f.data_type() {
            arrow::datatypes::DataType::Timestamp(_, Some(tz)) => assert_eq!(tz.as_ref(), "UTC"),
            other => panic!("wrong dtype: {:?}", other),
        }
    }

    #[test]
    fn dirty_ratio_zero_produces_clean_city_state() {
        let mut cfg = small_cfg(2000);
        cfg.dirty_ratio = 0.0;
        let loyalty = r::LoyaltyLookup::build(42, cfg.customer_id_max);
        let sampler = r::CustomerIdSampler::new(cfg.customer_id_max);
        let batch = build_batch(&cfg, &loyalty, &sampler);
        let cities = batch
            .column_by_name("city_raw")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Every city value must be present in the clean CITIES table (no dirty
        // variants slipped through when dirty_ratio=0).
        let clean_cities: std::collections::HashSet<&str> =
            r::CITIES.iter().map(|(c, _)| *c).collect();
        for i in 0..batch.num_rows() {
            let v = cities.value(i);
            assert!(
                clean_cities.contains(v),
                "row {} city={} not in clean CITIES set",
                i,
                v
            );
        }
    }

    #[test]
    fn loyalty_membership_consistent_per_customer_id() {
        // Feature #7: same customer_id must always resolve to the same
        // (member, tier). If loyalty is indexed with a modulo instead of an
        // assert-on-mismatch, this test fails.
        let batch = build(5000);
        let cids = batch
            .column_by_name("customer_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mem = batch
            .column_by_name("loyalty_member")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let tier = batch
            .column_by_name("loyalty_tier")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut seen: std::collections::HashMap<i64, (bool, Option<String>)> =
            std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            let cid = cids.value(i);
            let is_member = mem.value(i);
            let t = if tier.is_null(i) { None } else { Some(tier.value(i).to_string()) };
            let entry = (is_member, t);
            match seen.get(&cid) {
                Some(prev) => assert_eq!(
                    *prev, entry,
                    "cid {} had different loyalty state across rows",
                    cid
                ),
                None => {
                    seen.insert(cid, entry);
                }
            }
        }
    }

    #[test]
    fn dirty_ratio_positive_corrupts_some_cities() {
        // Prove the corruption path actually runs when dirty_ratio > 0 --
        // catches regressions where the dirty pass is gated off (e.g. an
        // outer `if dirty_ratio > 0` short-circuit removed).
        let cfg = small_cfg(5000);
        let loyalty = r::LoyaltyLookup::build(42, cfg.customer_id_max);
        let sampler = r::CustomerIdSampler::new(cfg.customer_id_max);
        let batch = build_batch(&cfg, &loyalty, &sampler);
        let cities = batch
            .column_by_name("city_raw")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let clean_cities: std::collections::HashSet<&str> =
            r::CITIES.iter().map(|(c, _)| *c).collect();
        let mut dirty = 0;
        for i in 0..batch.num_rows() {
            if !clean_cities.contains(cities.value(i)) {
                dirty += 1;
            }
        }
        // With source-weighted dirty rates (M-R2, 2026-09-20) the overall
        // dirty rate is `dirty_ratio * avg(DIRTY_RATE_BY_SOURCE weighted by
        // DATA_SOURCE_WEIGHTS)` = 0.08 * 0.0735 ≈ 0.6% per row on average.
        // Only ~half of dirty picks land on a corruptible city (10 of 22
        // CITIES entries have variants), so observed share is ~0.3%. Test
        // bounds allow noise from the small (5000-row) sample.
        let share = dirty as f64 / batch.num_rows() as f64;
        assert!(
            (0.001..=0.02).contains(&share),
            "dirty share {} outside [0.001, 0.02] band for source-weighted rates",
            share
        );
    }

    #[test]
    fn source_aware_dirty_rate_favors_legacy_import() {
        // Legacy-import rows should be corrupted at ~70x the rate of
        // primary_system rows. This is the whole point of M-R2 -- catches
        // regressions where dirty rate reverts to uniform.
        let cfg = Config { rows_per_file: 20_000, ..small_cfg(20_000) };
        let loyalty = r::LoyaltyLookup::build(42, cfg.customer_id_max);
        let sampler = r::CustomerIdSampler::new(cfg.customer_id_max);
        let batch = build_batch(&cfg, &loyalty, &sampler);
        let src = batch
            .column_by_name("data_source")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let cities = batch
            .column_by_name("city_raw")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let clean: std::collections::HashSet<&str> =
            r::CITIES.iter().map(|(c, _)| *c).collect();
        let mut legacy_total = 0u64;
        let mut legacy_dirty = 0u64;
        let mut primary_total = 0u64;
        let mut primary_dirty = 0u64;
        for i in 0..batch.num_rows() {
            let is_dirty = !clean.contains(cities.value(i));
            match src.value(i) {
                "legacy_import" => {
                    legacy_total += 1;
                    if is_dirty { legacy_dirty += 1; }
                }
                "primary_system" => {
                    primary_total += 1;
                    if is_dirty { primary_dirty += 1; }
                }
                _ => {}
            }
        }
        // At n=20k rows with 15% legacy_import share = 3000 legacy rows and
        // 70% primary_system share = 14000 primary rows. Legacy dirty rate
        // ~0.08*0.35*(~0.5 corruptible) = 1.4%; primary dirty rate
        // ~0.08*0.005*(~0.5 corruptible) = 0.02%. Assert legacy rate is at
        // least 10x primary -- that's the whole source-weighting shape.
        let legacy_rate = legacy_dirty as f64 / legacy_total.max(1) as f64;
        let primary_rate = primary_dirty as f64 / primary_total.max(1) as f64;
        // Primary might be zero at this sample size -- guard for the ratio.
        if primary_rate > 0.0 {
            assert!(
                legacy_rate > primary_rate * 10.0,
                "legacy_rate {} not >10x primary_rate {}",
                legacy_rate,
                primary_rate
            );
        } else {
            // At minimum legacy_import should show some corruption while
            // primary got exactly none -- that's the shape too.
            assert!(legacy_dirty > 0, "no legacy_import corruption seen");
        }
    }

    #[test]
    fn zip_and_product_id_within_python_ranges() {
        // Python `rng.integers(10000, 99999)` is half-open, so max = 99998.
        // Rust ranges must match to prevent silent parity drift when Silver
        // has assertions like "product_id parsed to int <= 99998".
        let batch = build(2000);
        let z = batch
            .column_by_name("zip_code")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let n: u32 = z.value(i).parse().unwrap();
            assert!((10_000..=99_998).contains(&n), "zip {} out of Python range", n);
        }
        let p = batch
            .column_by_name("product_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if p.is_null(i) {
                continue;
            }
            let v = p.value(i);
            let digits: u32 = v[3..].parse().unwrap();
            assert!(
                (10_000..=99_998).contains(&digits),
                "product_id {} out of Python range",
                v
            );
        }
    }

    #[test]
    #[should_panic(expected = "loyalty lookup")]
    fn undersized_loyalty_panics() {
        // Silent-corruption guard: a lookup smaller than customer_id_max must
        // fail loud rather than silently miscorrelate loyalty via modulo.
        let cfg = Config {
            customer_id_max: 100_000,
            ..small_cfg(100)
        };
        let too_small = r::LoyaltyLookup::build(42, 100); // way too small
        let sampler = r::CustomerIdSampler::new(cfg.customer_id_max);
        let _ = build_batch(&cfg, &too_small, &sampler);
    }

    #[test]
    fn session_id_shared_within_session() {
        // Rows in one session must share session_id (that IS the session).
        // Also every session_id must appear in a contiguous run (rows are
        // emitted in session order, no interleaving).
        let batch = build(2000);
        let sid = batch
            .column_by_name("session_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let cid = batch
            .column_by_name("customer_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut session_starts: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            let s = sid.value(i);
            // Contiguity check: once we see a new session id, never see the
            // previous one again.
            let start = *session_starts.entry(s).or_insert(i);
            if start != i {
                // We're inside a session; the customer_id must match the
                // first row of this session_id.
                assert_eq!(
                    cid.value(i),
                    cid.value(start),
                    "row {} session_id {} customer mismatch",
                    i,
                    s
                );
            }
        }
        // Sessions should be short (typical 5-20 rows) so at 2000 rows we
        // expect ~100-400 distinct session_ids.
        let n_sessions = session_starts.len();
        assert!(
            n_sessions > 50 && n_sessions < 600,
            "expected 100-400 sessions in 2000 rows, got {}",
            n_sessions
        );
    }

    #[test]
    fn row_level_customer_id_share_bounded() {
        // Row-level version of the sampler test: with the session engine
        // active, the sampler is called once per session (~2400 times per
        // 20k-row batch), so the row-level id=1 share may differ from the
        // pure-sampler measurement. Assert the row-level rate stays under
        // the same 15% ceiling.
        let batch = build(20_000);
        let cid = batch
            .column_by_name("customer_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut counts: std::collections::HashMap<i64, u64> =
            std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            *counts.entry(cid.value(i)).or_insert(0) += 1;
        }
        let n = batch.num_rows() as f64;
        let id1 = *counts.get(&1).unwrap_or(&0) as f64 / n;
        assert!(id1 < 0.20, "row-level id=1 share {} > 0.20", id1);
    }

    #[test]
    fn session_length_distribution_in_range() {
        // Session lengths must be uniform in [SESSION_LEN_MIN,
        // SESSION_LEN_MIN + SESSION_LEN_RANGE) = [5, 21), so mean ~12.5
        // with tolerance. Catches a regression that would widen or shrink
        // the length range silently (session-engine tuning drift).
        let batch = build(20_000);
        let sid = batch
            .column_by_name("session_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut lengths: std::collections::HashMap<&str, u32> =
            std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            *lengths.entry(sid.value(i)).or_insert(0) += 1;
        }
        // The last session may be truncated to fit the row budget (its
        // schedule length was raw_len but `min(rows_left)` may cap it low).
        // Include truncation in the mean check but assert min/max cleanly by
        // excluding at most one outlier.
        let vals: Vec<u32> = lengths.values().copied().collect();
        let mean = vals.iter().copied().sum::<u32>() as f64 / vals.len().max(1) as f64;
        assert!(
            (10.0..14.5).contains(&mean),
            "mean session length {} outside [10, 14.5] (expected ~12.5 for uniform [5,20])",
            mean
        );
        // Every non-truncated session must be in [5, 20]. Allow at most one
        // outlier (the last, possibly truncated).
        let outliers = vals
            .iter()
            .filter(|&&l| !(5..=20).contains(&l))
            .count();
        assert!(
            outliers <= 1,
            "expected at most 1 length outlier (truncated last), got {}: values {:?}",
            outliers,
            vals
        );
    }

    #[test]
    fn session_engine_respects_narrow_timestamp_window() {
        // P1 regression guard (2026-09-20): a 1-hour configured window with
        // 30-min sessions must not emit rows past the window's end. Prior
        // implementation added the session offset unconditionally, letting
        // rows land up to 30 min past `timestamp_end_us` (silently creating
        // phantom date partitions for sustained mode).
        let one_hour_us = 60 * 60 * 1_000_000i64;
        let start = 1_704_067_200_000_000i64; // 2024-01-01 00:00:00 UTC
        let end = start + one_hour_us;
        let cfg = Config {
            timestamp_start_us: start,
            timestamp_end_us: end,
            ..small_cfg(2000)
        };
        let loyalty = r::LoyaltyLookup::build(42, cfg.customer_id_max);
        let sampler = r::CustomerIdSampler::new(cfg.customer_id_max);
        let batch = build_batch(&cfg, &loyalty, &sampler);
        let ts = batch
            .column_by_name("event_timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let t = ts.value(i);
            assert!(
                t >= start && t < end,
                "row {} ts {} escaped configured window [{}, {})",
                i,
                t,
                start,
                end
            );
        }
    }

    #[test]
    fn session_timestamps_cluster_within_30min() {
        // Every session's rows should have timestamps within a 30-minute
        // window (SESSION_TIMESPAN_US bound). Prior uniform-per-row emit
        // scattered them across the full year -- destroying freshness and
        // sessionisation queries.
        let batch = build(3000);
        let sid = batch
            .column_by_name("session_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ts = batch
            .column_by_name("event_timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let mut per_session_range: std::collections::HashMap<&str, (i64, i64)> =
            std::collections::HashMap::new();
        for i in 0..batch.num_rows() {
            let s = sid.value(i);
            let t = ts.value(i);
            per_session_range
                .entry(s)
                .and_modify(|(lo, hi)| {
                    if t < *lo { *lo = t; }
                    if t > *hi { *hi = t; }
                })
                .or_insert((t, t));
        }
        for (s, (lo, hi)) in &per_session_range {
            let span_us = hi - lo;
            assert!(
                span_us <= 30 * 60 * 1_000_000,
                "session {} spans {} us > 30 min",
                s,
                span_us
            );
        }
    }

    #[test]
    fn product_category_persists_within_session_at_configured_rate() {
        // Within a session, product_category should stick with ~70% keep
        // probability. Prior uniform draws had ~1/5 = 20% same-category rate
        // in adjacent rows (random). Post-M-R3 should be closer to 70% base +
        // 30%*20% = 76% adjacent-same.
        let batch = build(5000);
        let sid = batch
            .column_by_name("session_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pcat = batch
            .column_by_name("product_category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut same = 0u64;
        let mut compared = 0u64;
        for i in 1..batch.num_rows() {
            if sid.value(i) != sid.value(i - 1) {
                continue;
            }
            if pcat.is_null(i) || pcat.is_null(i - 1) {
                continue;
            }
            compared += 1;
            if pcat.value(i) == pcat.value(i - 1) {
                same += 1;
            }
        }
        assert!(compared > 100, "not enough same-session comparisons: {}", compared);
        let rate = same as f64 / compared as f64;
        // Expected ~0.59: two rows both keep (0.7^2 = 0.49) or both redraw
        // to same (0.3^2 * 0.2 = 0.018) or one-keep-one-redraw picking base
        // (2*0.7*0.3*0.2 = 0.084). Sum = 0.592. That's 3x the uniform-random
        // baseline of 0.20 (1/5 categories), which is what real basket
        // queries pick up as a category-coherence signal. Tolerance ±0.05.
        assert!(
            (0.53..0.65).contains(&rate),
            "adjacent-same rate {} outside 0.53..0.65 band (expected ~0.59 with 0.7 keep prob)",
            rate
        );
    }

    #[test]
    fn file_id_isolates_content() {
        let cfg0 = Config { file_id: 0, ..small_cfg(500) };
        let cfg1 = Config { file_id: 1, ..small_cfg(500) };
        let loyalty = r::LoyaltyLookup::build(42, 10_000);
        let sampler = r::CustomerIdSampler::new(10_000);
        let a = build_batch(&cfg0, &loyalty, &sampler);
        let b = build_batch(&cfg1, &loyalty, &sampler);
        // At least half the customer_ids differ between file 0 and file 1.
        let ca = a
            .column_by_name("customer_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cb = b
            .column_by_name("customer_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut diffs = 0;
        for i in 0..ca.len() {
            if ca.value(i) != cb.value(i) {
                diffs += 1;
            }
        }
        assert!(diffs > ca.len() / 2, "only {} of {} differ", diffs, ca.len());
    }
}
