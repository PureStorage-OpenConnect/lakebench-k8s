//! Standalone Rust datagen driver. Produces bronze pacs.008 Parquet plus party,
//! account, and manifest tables, driven by (seed, file_id, scale, node). File
//! content depends only on those inputs. The pod is stateless: every parquet
//! file is built into an in-memory `Vec<u8>` and PUT directly to S3, so the
//! local filesystem is never used as a staging buffer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use rayon::prelude::*;

use parquet::arrow::ArrowWriter;

use datagen_rs::amounts::{instance_amounts, native_amount};
use datagen_rs::customer360;
use datagen_rs::customer360_realism::{CustomerIdSampler, LoyaltyLookup};
use datagen_rs::cycle;
use datagen_rs::emit::{build_batch, Batch};
use datagen_rs::hash::{hash_frac, splitmix64, Rng};
use datagen_rs::metrics::PodMetrics;
use datagen_rs::model::build_world_ex;
use datagen_rs::party::{build_manifest, write_account_to, write_party_to};
use datagen_rs::s3sink::{S3Cfg, S3Sink};
use datagen_rs::timing::{sample_ts_on_day, DayCal};
use datagen_rs::world::ring_member;
use datagen_rs::writer::{
    customer360_bytes_per_row_default, pacs008_bytes_per_row_default, writer_properties,
};

/// Stable uid for a typology row, used to seed the row's UETR in the bronze
/// emit AND recovered by the manifest builder to populate
/// `participant_uetrs`. Top bit = 1 so it cannot collide with the base
/// row uid namespace `(fid<<40)|i` -- base rows always have top bit 0
/// because fid fits in far fewer than 24 bits at any realistic scale.
#[inline]
fn typology_uid(inst_seed: i64, row_idx: usize) -> u64 {
    let mixed = splitmix64((inst_seed as u64).wrapping_add((row_idx as u64) << 40));
    mixed | 0x8000_0000_0000_0000
}

/// True when originator `o` has a dormancy suppression window (P3, W8) that
/// contains `ts`. Fast path: the bitmap is false for >99.9% of entities, so a
/// non-participant costs one array read. Only dormant participants pay the
/// small window scan.
#[inline]
fn in_suppress_window(
    is_suppressed: &[bool],
    windows: &HashMap<u64, Vec<(i64, i64)>>,
    o: u64,
    ts: i64,
) -> bool {
    if !is_suppressed[o as usize] {
        return false;
    }
    windows
        .get(&o)
        .map(|ws| ws.iter().any(|&(s, e)| ts >= s && ts < e))
        .unwrap_or(false)
}

const US_PER_DAY: i64 = 86_400_000_000;

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn arg<T: std::str::FromStr>(flag: &str, default: T) -> T {
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() {
        if args[i] == flag {
            if let Some(v) = args.get(i + 1) {
                if let Ok(p) = v.parse() {
                    return p;
                }
            }
        }
    }
    default
}

struct TypRow {
    orig: u64,
    bene: u64,
    ts_us: i64,
    amount: f64,
    ccy: &'static str,
    // Stable uid for this typology row. Chosen at scheduling time
    // (deterministic in `(inst.seed, row_idx)`) and carried through the
    // per-file sort so the row's bronze UETR can be reproduced in the
    // manifest without needing the sorted position.
    uid: u64,
}

fn encode_parquet(batch: &arrow::record_batch::RecordBatch, cap_hint: usize) -> Vec<u8> {
    // Build the parquet file into memory. Caller sizes cap_hint from the
    // configured file_size_mb so a small --file-size-mb doesn't pre-allocate
    // 64 MiB per worker; at 32-way rayon and --file-size-mb 32, transient
    // peak is roughly 32 * cap_hint plus the live RecordBatch, well under
    // the pod's memory request. writer_properties() reads DG_COMPRESSION /
    // DG_STATS / DG_DICT / DG_PAGESZ from env (shared with party/account
    // writers so codec choice is uniform across every parquet emitted).
    let props = writer_properties();
    let mut buf: Vec<u8> = Vec::with_capacity(cap_hint);
    {
        let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        w.write(batch).unwrap();
        w.close().unwrap();
    }
    buf
}

fn main() {
    // Schema dispatch: default "financial" for back-compat with any K8s Job
    // manifest that predates the c360 branch. `entrypoint.py` gates schema
    // choice before invoking us, but keep a defensive check here too so a
    // typo doesn't fall through to the pacs.008 path silently.
    let schema: String = arg("--schema", "financial".to_string());
    match schema.as_str() {
        "financial" => pacs008_main(),
        "customer360" => customer360_main(),
        other => {
            eprintln!(
                "--schema must be one of: financial | customer360; got {:?}",
                other
            );
            std::process::exit(2);
        }
    }
}

/// pacs.008 datagen driver. Unchanged from the pre-c360 codepath -- extracted
/// into a named function only to make room for schema dispatch in `main()`.
fn pacs008_main() {
    // Direct-to-S3: pod holds no state. --bucket / --prefix name where the files
    // go, S3 creds and endpoint come from env (AWS_ACCESS_KEY_ID,
    // AWS_SECRET_ACCESS_KEY, S3_ENDPOINT, AWS_REGION).
    let bucket: String = arg("--bucket", String::new());
    let prefix: String = arg("--prefix", String::new());
    if bucket.is_empty() {
        eprintln!("--bucket is required (destination S3 bucket)");
        std::process::exit(2);
    }
    let seed: i64 = arg("--seed", 42);
    // Multi-cycle runs (datagen_rs::cycle): cycle n > 0 draws its event
    // streams from a cycle-mixed seed and writes cycle-suffixed keys. The
    // world keeps --seed. 0 reproduces a run without --cycle byte for byte.
    let cycle_n: u64 = arg("--cycle", 0u64);
    let sseed: i64 = cycle::stream_seed(seed, cycle_n);
    let scale: f64 = arg("--scale", 0.01);
    let corpus_months: i64 = arg("--corpus-months", 60);
    let file_size_mb: i64 = arg("--file-size-mb", 32);
    // Bytes/row is used only to size total_files from total_txns. If the flag
    // is not passed we pick a codec-aware default from writer::pacs008_bytes_per_row_default
    // (a single scalar was wrong under any codec other than the one it was
    // measured against -- see writer.rs for the measured table).
    //
    // The generic `arg()` helper silently falls back to the default on parse
    // failure, which would let `--bytes-per-row abc` or `1e-999` (subnormal
    // underflow -> 0.0) or `-0.0` (== 0.0) silently take the codec default.
    // Distinguish "flag absent" from "flag present but unparseable"
    // explicitly for this one arg: unparseable exits 2, positive-finite is
    // the override, absent uses the codec default.
    let bytes_per_row: f64 = {
        let args: Vec<String> = std::env::args().collect();
        let mut present: Option<&str> = None;
        for i in 0..args.len() {
            if args[i] == "--bytes-per-row" {
                if let Some(v) = args.get(i + 1) {
                    present = Some(v.as_str());
                }
            }
        }
        match present {
            None => {
                let d = pacs008_bytes_per_row_default();
                eprintln!("--bytes-per-row not set: using codec-aware default {}", d);
                d
            }
            Some(raw) => match raw.parse::<f64>() {
                Ok(v) if v.is_finite() && v > 0.0 && !v.is_subnormal() => v,
                Ok(v) => {
                    eprintln!(
                        "--bytes-per-row must be a positive finite non-subnormal number; got {} ({})",
                        raw, v
                    );
                    std::process::exit(2);
                }
                Err(e) => {
                    eprintln!("--bytes-per-row could not parse {:?}: {}", raw, e);
                    std::process::exit(2);
                }
            },
        }
    };
    let node_id: i64 = arg("--node-id", 0);
    let total_nodes: i64 = arg("--total-nodes", 1);
    // Work split: "all" (node 0 also writes the reference zones), "bronze"
    // (transactions only -- every pod balanced), "reference" (party/account/
    // manifest only, on a dedicated pod). Offloading reference removes the
    // node-0 straggler so bronze pods finish together.
    let mode: String = arg("--mode", "all".to_string());
    // Reject typos explicitly so an operator's `--mode brozne` does not
    // silently succeed with zero files written (previously it fell through
    // to do_bronze=false, do_reference=false and exit 0 -- caught by an
    // arg-fuzzing pass).
    if !matches!(mode.as_str(), "all" | "bronze" | "reference") {
        eprintln!(
            "--mode must be one of: all | bronze | reference; got {:?}",
            mode
        );
        std::process::exit(2);
    }
    let do_bronze = mode == "all" || mode == "bronze";
    let do_reference = mode == "reference" || (mode == "all" && node_id == 0);
    // Validate --corpus-months FIRST, before rayon pool init and before the
    // ~11M-entity world build, so a misconfigured pod exits in milliseconds
    // rather than after minutes of setup times backoffLimit retries. Use
    // checked arithmetic here (unchecked (m+11) panics in debug builds on
    // pathological inputs like i64::MAX before the guard sees them).
    if !(1..=12 * 200).contains(&corpus_months) {
        eprintln!(
            "--corpus-months must be in 1..={} months; got {}",
            12 * 200,
            corpus_months
        );
        std::process::exit(2);
    }

    // Size the rayon pool explicitly so it matches the pod's CPU allotment
    // rather than the host-visible core count (rayon's default reflects CPU
    // affinity, not the Kubernetes CFS quota, so it oversubscribes and gets
    // throttled). 0 leaves rayon's default / RAYON_NUM_THREADS.
    let threads: usize = arg("--threads", 0usize);
    if threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .expect("failed to size rayon pool");
    }

    // Corpus bounds: end 2026-01-01, start = end - corpus_months. Compute the
    // start month-accurate: shift the (year, month) tuple by corpus_months
    // rather than integer-dividing months into years, so non-multiples of 12
    // (e.g. --corpus-months 18) actually produce an 18-month window instead
    // of silently rounding down to 12 while Dimensions.total_txns still
    // reflects the intended 18. That truncation was silent-wrong: total_txns
    // stayed at the requested months while the window shrank, so rows packed
    // into a narrower span and the gate's month-keyed distribution failed.
    let end_us = days_from_civil(2026, 1, 1) * US_PER_DAY;
    let start_year = 2026 - (corpus_months + 11) / 12;
    let start_month = ((12 - (corpus_months % 12)) % 12) + 1;
    let start_us = days_from_civil(start_year, start_month, 1) * US_PER_DAY;
    let span_us = end_us - start_us;
    // Belt-and-braces: the range check above already enforces the corpus_months
    // bounds, but a future change to days_from_civil could still push span_us
    // <= 0 for some pathological (year, month) combo.
    if span_us <= 0 {
        eprintln!(
            "computed corpus window is non-positive; corpus_months={}",
            corpus_months
        );
        std::process::exit(2);
    }

    // Validate S3 config + build the sink BEFORE the multi-minute world build,
    // so bad creds / missing endpoint surface in milliseconds. Building the
    // sink also proves the tokio runtime and object_store client init cleanly.
    let s3_cfg = match S3Cfg::try_from_env(bucket.clone(), prefix.clone()) {
        Ok(c) => c,
        Err(msg) => {
            eprintln!("s3 config error: {}", msg);
            std::process::exit(2);
        }
    };
    let sink = S3Sink::new(&s3_cfg);

    let t0 = std::time::Instant::now();
    // A dedicated-bronze pod (writes no reference zones) can skip the
    // reference-only world columns entirely.
    let bronze_only = do_bronze && !do_reference;
    let w = build_world_ex(scale, seed, corpus_months, bronze_only);
    let t_world = t0.elapsed().as_secs_f64();
    let dims = &w.dims;
    let total_txns = dims.total_txns();
    let pop = w.population;

    let file_size = file_size_mb * 1024 * 1024;
    // File count grows with data but has a floor so there is always enough
    // parallel work to keep a many-core pod busy, even at small scale. It stays
    // a pure function of (scale, corpus_months, file_size, bytes_per_row) and
    // never depends on the thread/core count, so determinism and
    // thread-invariance hold. Capped so files never fall below ~1000 rows.
    const MIN_FILES: i64 = 64;
    let ideal = (total_txns as f64 * bytes_per_row / file_size as f64).round() as i64;
    let max_by_rows = (total_txns / 1000).max(1);
    let total_files = ideal.max(MIN_FILES).min(max_by_rows).max(1);
    // One calendar for the whole corpus. Files cover equal slices of
    // day-weighted calendar MASS, not equal slices of time: with equal-time
    // windows every file got the same row count, so weekends and holidays got
    // weekday volume, and windows of fractional days dropped whole days (25%
    // of days had no baseline rows at scale 10). Rows are placed by mass, so
    // the day-of-week and salary-day shape is the same at every scale.
    let gcal = DayCal::new(start_us, (span_us / US_PER_DAY).max(1) as usize);

    // Schedule + emit typology rows, then bin by file.
    let t_typ0 = std::time::Instant::now();
    let mut instances =
        datagen_rs::typology::schedule(sseed, total_txns, pop, start_us, end_us, &w.country);
    for inst in instances.iter_mut() {
        inst.id = cycle::instance_id(&inst.id, cycle_n);
    }
    // Place every instance on the baseline calendar (see placement.rs).
    datagen_rs::placement::place_instances(&mut instances, &gcal, start_us, end_us);
    let mut typ_by_file: Vec<Vec<TypRow>> = (0..total_files).map(|_| Vec::new()).collect();
    // Instance-id -> list of uids for the rows emitted for that instance.
    // Populated at typology-scheduling time so it's deterministic (both
    // bronze and reference pods build the identical map from the same
    // schedule, but only the reference pod writes the manifest that
    // consumes it). Downstream (`score_financial.py`, `verify_run.py`)
    // joins `manifest.participant_uetrs` against `gold.alerts.related_txn_ids`;
    // an empty list here was silently making recall = 0/0.
    let mut inst_uids: HashMap<String, Vec<u64>> = HashMap::new();
    // Dormancy suppression (P3, W8): the dormant originator (participants[0]) of
    // each dormant_reactivation instance must have NO base row and no
    // other-typology row inside its [suppress_start, suppress_end) window, so its
    // only post-anchor send is the burst and W8 sees a real >90-day gap. Built
    // identically on every pod (schedule is deterministic). Fast-path bitmap +
    // small per-participant window list.
    let mut is_suppressed = vec![false; pop + 1];
    let mut suppress_windows: HashMap<u64, Vec<(i64, i64)>> = HashMap::new();
    for inst in &instances {
        if inst.suppress_end_us > inst.suppress_start_us {
            let a = inst.participants[0];
            is_suppressed[a as usize] = true;
            suppress_windows
                .entry(a)
                .or_default()
                .push((inst.suppress_start_us, inst.suppress_end_us));
        }
    }
    let mut trng = Rng::new((sseed as u64) ^ 0x7791);
    // Shaped [first, last] in-window row per instance, for the manifest.
    let mut inst_bounds: HashMap<String, (i64, i64)> = HashMap::new();
    for inst in &instances {
        let is_dormant = inst.typ == "dormant_reactivation";
        // Shape every row of the instance once, here, with an instance-keyed
        // RNG, preserving the rows' time order (see placement.rs).
        let mut rows = datagen_rs::typology::emit_instance(inst);
        let mut srng = Rng::new(splitmix64((inst.seed as u64) ^ 0x5A4E_0000_0000_0001));
        if let Some(b) = datagen_rs::placement::shape_instance_rows(
            &mut rows, inst, &gcal, span_us, &w.country, &mut srng,
        ) {
            inst_bounds.insert(inst.id.clone(), b);
        }
        // Typology rows carry the ORIGINATOR's persona amount shift and
        // currency, the same as that account's baseline rows, so a
        // participant's amounts stay consistent with its own history; chained
        // legs forward the previous leg less a skim (amounts::instance_amounts).
        // Amounts are assigned before the suppression drop below, so a dropped
        // leg still carries the chain forward.
        let amounts = instance_amounts(
            inst.typ,
            &rows,
            |o| w.ccy[o as usize],
            |o| w.amount_logshift[o as usize],
            &mut trng,
        );
        for (row_idx, (r, amount)) in rows.into_iter().zip(amounts).enumerate() {
            // A NON-dormant typology row whose originator is a dormant
            // participant inside its suppression window would fill the dormancy
            // gap -- drop it. Dormant-instance rows (anchor + burst) are exempt.
            if !is_dormant && in_suppress_window(&is_suppressed, &suppress_windows, r.orig, r.ts_us)
            {
                continue;
            }
            let ccy = w.ccy[r.orig as usize];
            let fid = ((gcal.mass_at(r.ts_us) * total_files as f64) as i64)
                .clamp(0, total_files - 1) as usize;
            let uid = typology_uid(inst.seed, row_idx);
            typ_by_file[fid].push(TypRow {
                orig: r.orig,
                bene: r.bene,
                ts_us: r.ts_us,
                amount,
                ccy,
                uid,
            });
            inst_uids.entry(inst.id.clone()).or_default().push(uid);
        }
    }

    // The manifest reports the window the rows actually occupy. The
    // scheduled window did not contain them: intraday redraws and business-
    // day rolls left 25% of fan_in rows and 35% of dormancy bursts outside it.
    for inst in instances.iter_mut() {
        if let Some(&(a, b)) = inst_bounds.get(&inst.id) {
            inst.start_us = a;
            inst.end_us = b;
        }
    }

    // Base rows are indexed globally, 0..n_base_total, so what a row contains
    // depends only on (seed, its index), never on how many files there are
    // (which depends on codec and file size). Row i sits at calendar mass
    // (i + jitter) / n_base_total; file fid holds the contiguous rows whose
    // mass falls in [fid/F, (fid+1)/F).
    let n_typ_total: i64 = typ_by_file.iter().map(|v| v.len() as i64).sum();
    let n_base_total: u64 = (total_txns - n_typ_total).max(0) as u64;
    let base_seed = splitmix64((sseed as u64) ^ 0xBA5E_0000_0000_0001);
    let base_mass = move |i: u64| -> f64 {
        (i as f64 + hash_frac(i, base_seed as i64)) / n_base_total.max(1) as f64
    };
    // First base row whose mass is >= fid/F.
    let base_start = |fid: i64| -> u64 {
        if fid <= 0 {
            return 0;
        }
        if fid >= total_files {
            return n_base_total;
        }
        let target = fid as f64 / total_files as f64;
        let mut i = ((target * n_base_total as f64) as u64).min(n_base_total);
        while i > 0 && base_mass(i - 1) >= target {
            i -= 1;
        }
        while i < n_base_total && base_mass(i) < target {
            i += 1;
        }
        i
    };
    let rows_per_file = (n_base_total as i64 / total_files).max(1);

    // Activity-weighted originator sampling: prefix sums.
    let mut cum = vec![0.0f64; pop + 1];
    for i in 1..=pop {
        cum[i] = cum[i - 1] + w.activity[i];
    }
    let total_w = cum[pop];
    fn sample_orig(cum: &[f64], total_w: f64, pop: usize, rng: &mut Rng) -> u64 {
        let u = rng.unit() * total_w;
        let (mut lo, mut hi) = (1usize, pop);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if cum[mid] < u {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo as u64
    }

    let t_typ = t_typ0.elapsed().as_secs_f64();

    let total_bytes = AtomicU64::new(0);
    let files_written = AtomicU64::new(0);
    // Rows THIS POD wrote (not corpus-wide `total_txns`; that is a
    // constant across every pod). Fleet aggregator sums this.
    let rows_written = AtomicU64::new(0);
    let upload_ns = AtomicU64::new(0);
    // Split gen time into batch-assembly (arrow builders) vs parquet write
    // (encode + SNAPPY + disk), summed across worker threads (thread-nanos).
    let build_ns = AtomicU64::new(0);
    let write_ns = AtomicU64::new(0);
    let t_gen0 = std::time::Instant::now();

    let my_files: Vec<i64> = if do_bronze {
        (0..total_files)
            .filter(|fid| fid % total_nodes == node_id)
            .collect()
    } else {
        Vec::new()
    };
    my_files.par_iter().for_each(|&fid| {
        let typ = &typ_by_file[fid as usize];
        let n_typ = typ.len();
        let i0 = base_start(fid);
        let i1 = base_start(fid + 1);
        let n_base = (i1 - i0) as usize;

        let cap = n_base + n_typ;
        let mut orig = Vec::with_capacity(cap);
        let mut bene = Vec::with_capacity(cap);
        let mut ts_us = Vec::with_capacity(cap);
        let mut amount = Vec::with_capacity(cap);
        let mut ccy: Vec<&'static str> = Vec::with_capacity(cap);
        // Pre-assigned uid per row: base rows use their global index (top
        // bit 0), typology rows carry their scheduling-time uid (top bit 1).
        // Kept through the sort so bronze UETRs stay recoverable by the
        // manifest builder.
        let mut uid_pre = Vec::with_capacity(cap);

        for gi in i0..i1 {
            // Per-row RNG keyed by the global index: row content does not
            // depend on the file layout.
            let mut rng = Rng::new(splitmix64(gi ^ base_seed));
            let day = gcal.day_for_mass(base_mass(gi));
            // Resample the whole row (originator, beneficiary, timestamp) if the
            // sampled originator is a dormant participant inside its suppression
            // window, so no base send fills the dormancy gap (P3, W8). Bounded
            // retries; on exhaustion keep the last draw -- at most a handful per
            // corpus, and a stray in-window base row can only SHORTEN one
            // instance's gap (a missed instance), never create a false dormancy
            // on another account. For a non-suppressed originator the loop runs
            // once and the RNG draw order is identical to the pre-P3 stream.
            const CORE: u64 = 40;
            let mut o;
            let mut b;
            let mut t;
            let mut tries = 0;
            loop {
                o = sample_orig(&cum, total_w, pop, &mut rng);
                // Beneficiary drawn from a bounded core counterparty set so that
                // recurring-counterparty volume stays concentrated at any scale
                // (uniform population draws never repeat once population is
                // large, which collapsed the repeat-edge share at scale 1). The
                // core is capped at CORE members; off-ring draws hit an extended
                // band.
                let rs = w.ring_sz[o as usize].max(1) as u64;
                let core = rs.min(CORE);
                if rng.unit() < w.ring_hit[o as usize] {
                    b = ring_member(o, rng.below(core), pop, seed);
                } else {
                    // Extended band: still bounded per originator, so it repeats.
                    let ext = (rs.min(4 * CORE)).max(core + 1);
                    b = ring_member(o, core + rng.below(ext), pop, seed);
                }
                if b == o {
                    b = (b % pop as u64) + 1;
                }
                t = sample_ts_on_day(&mut rng, &gcal, day, w.country[o as usize]);
                tries += 1;
                if tries >= 8 || !in_suppress_window(&is_suppressed, &suppress_windows, o, t) {
                    break;
                }
            }
            let cc = w.ccy[o as usize];
            orig.push(o);
            bene.push(b);
            ts_us.push(t);
            amount.push(native_amount(&mut rng, w.amount_logshift[o as usize], cc));
            ccy.push(cc);
            uid_pre.push(cycle::base_uid(gi, cycle_n));
        }
        for r in typ {
            orig.push(r.orig);
            bene.push(r.bene);
            // Already shaped (business day + intraday) at scheduling time.
            ts_us.push(r.ts_us);
            amount.push(r.amount);
            ccy.push(r.ccy);
            uid_pre.push(r.uid);
        }

        // sort by ts
        let mut idx: Vec<usize> = (0..orig.len()).collect();
        idx.sort_by_key(|&i| ts_us[i]);
        let g = |v: &Vec<u64>| idx.iter().map(|&i| v[i]).collect::<Vec<_>>();
        let orig2 = g(&orig);
        let bene2 = g(&bene);
        let ts2: Vec<i64> = idx.iter().map(|&i| ts_us[i]).collect();
        let amt2: Vec<f64> = idx.iter().map(|&i| amount[i]).collect();
        let ccy2: Vec<&'static str> = idx.iter().map(|&i| ccy[i]).collect();
        // Permute the pre-assigned uid[] with the same sort so each row's
        // UETR derivation lines up with its position in the batch.
        let uid: Vec<u64> = idx.iter().map(|&i| uid_pre[i]).collect();

        let tb = std::time::Instant::now();
        let batch = build_batch(
            &w,
            &Batch {
                orig: orig2,
                bene: bene2,
                ts_us: ts2,
                amount: amt2,
                ccy: ccy2,
                uid,
            },
        );
        build_ns.fetch_add(tb.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let tw = std::time::Instant::now();
        // Slight overshoot on the pre-alloc so ArrowWriter rarely reallocs.
        let cap_hint = (file_size as usize + file_size as usize / 8).max(1024 * 1024);
        let buf = encode_parquet(&batch, cap_hint);
        write_ns.fetch_add(tw.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let sz = buf.len() as u64;
        let key = cycle::pacs_key(fid, cycle_n);
        let tu = std::time::Instant::now();
        sink.put(&key, buf);
        upload_ns.fetch_add(tu.elapsed().as_nanos() as u64, Ordering::Relaxed);
        total_bytes.fetch_add(sz, Ordering::Relaxed);
        files_written.fetch_add(1, Ordering::Relaxed);
        rows_written.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
    });
    let total_bytes = total_bytes.load(Ordering::Relaxed);
    let files_written = files_written.load(Ordering::Relaxed);
    let rows_written = rows_written.load(Ordering::Relaxed);
    let t_gen = t_gen0.elapsed().as_secs_f64();

    let t_ref0 = std::time::Instant::now();
    // Track reference-zone bytes/files separately so the final summary line
    // reflects what a `--mode reference` pod produced. Previously the
    // reference path did not touch `total_bytes`/`files_written`, so a
    // reference pod always logged `files_written=0 bytes=0` even after
    // successfully uploading party/account/manifest -- confusing for
    // anyone monitoring aggregate throughput from pod logs.
    let mut ref_bytes: u64 = 0;
    let mut ref_files: u64 = 0;
    if do_reference {
        // Party and account stream through a real S3 multipart upload
        // (LB-107 fix). ArrowWriter emits parquet in row-group chunks
        // into MpuWriter, which enqueues 5 MiB parts against S3 as they
        // fill. Whole-object size is no longer bounded by process RAM
        // or the 5 GiB single-PUT ceiling -- S3 multipart supports up
        // to 5 TiB per object across 10k parts, so at 5 MiB parts we
        // top out around 50 GiB per file (well past scale >=1000's
        // ~5-10 GB party.parquet). If the caller wants larger, bump
        // WriteMultipart's chunk_size via a new S3Sink helper.
        //
        // finish() is fail-loud: on error we panic so the pod exits
        // non-zero and the datagen Job restarts (same semantics as the
        // single-PUT path). Drop's best-effort abort covers panics.
        let mut party_mpu = sink.put_multipart(&cycle::ref_key("bronze/party.parquet", cycle_n));
        write_party_to(&w, &instances, &mut party_mpu);
        let party_bytes = party_mpu.bytes_written();
        party_mpu
            .finish()
            .unwrap_or_else(|e| panic!("party.parquet mpu finish: {}", e));
        ref_bytes += party_bytes;
        ref_files += 1;

        let mut acct_mpu = sink.put_multipart(&cycle::ref_key("bronze/account.parquet", cycle_n));
        write_account_to(&w, &mut acct_mpu);
        let acct_bytes = acct_mpu.bytes_written();
        acct_mpu
            .finish()
            .unwrap_or_else(|e| panic!("account.parquet mpu finish: {}", e));
        ref_bytes += acct_bytes;
        ref_files += 1;

        // Manifest stays on the single-PUT path: it's a handful of MB
        // even at scale 1000 (one row per typology instance), so
        // multipart adds request overhead with no benefit.
        let man_bytes = encode_parquet(
            &build_manifest(&instances, seed, &inst_uids),
            8 * 1024 * 1024,
        );
        ref_bytes += man_bytes.len() as u64;
        ref_files += 1;
        sink.put(
            &cycle::ref_key("manifest/manifest.parquet", cycle_n),
            man_bytes,
        );
    }
    let total_bytes = total_bytes + ref_bytes;
    let files_written = files_written + ref_files;
    let t_ref = t_ref0.elapsed().as_secs_f64();
    let up_s = upload_ns.load(Ordering::Relaxed) as f64 / 1e9;
    let el = t0.elapsed().as_secs_f64();
    eprintln!(
        "population={} total_txns={} total_files={} files_written={} instances={} elapsed={:.2}s bytes={} ({:.1} MB/s)",
        pop, total_txns, total_files, files_written, instances.len(), el, total_bytes,
        total_bytes as f64 / el / 1e6
    );
    let build_s = build_ns.load(Ordering::Relaxed) as f64 / 1e9;
    let write_s = write_ns.load(Ordering::Relaxed) as f64 / 1e9;
    let cpu_tot = (build_s + write_s).max(1e-9);
    eprintln!(
        "phases: world={:.2}s typ={:.2}s gen={:.2}s ref={:.2}s (bronze_only={}) world_share={:.0}%",
        t_world,
        t_typ,
        t_gen,
        t_ref,
        bronze_only,
        100.0 * t_world / el.max(1e-9)
    );
    eprintln!(
        "gen split (thread-s): build_batch={:.1}s ({:.0}%) encode_parquet={:.1}s ({:.0}%) s3_put={:.1}s",
        build_s, 100.0 * build_s / cpu_tot, write_s, 100.0 * write_s / cpu_tot, up_s
    );

    // Machine-readable per-pod metrics line for the lakebench aggregator.
    // See datagen_rs::metrics for the schema; lakebench parses on prefix
    // `LB_METRICS_JSON `.
    PodMetrics {
        schema: "financial".into(),
        node_id,
        node_count: total_nodes,
        cores_used: rayon::current_num_threads(),
        cpu_request_millicores: read_cpu_request_millicores(),
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        scale: Some(scale),
        corpus_months: Some(corpus_months),
        population: Some(pop as u64),
        total_txns: Some(total_txns as u64),
        typology_instances: Some(instances.len() as u64),
        file_size_mb,
        rows_per_file: rows_per_file as u64,
        total_files,
        files_written,
        bytes_written: total_bytes,
        rows_written,
        elapsed_s: el,
        setup_s: 0.0,
        world_s: Some(t_world),
        typology_s: Some(t_typ),
        gen_s: t_gen,
        reference_s: Some(t_ref),
        build_batch_s: build_s,
        encode_parquet_s: write_s,
        s3_put_s: up_s,
        ..Default::default()
    }
    .emit();
}

/// Read LB_POD_CPU_REQUEST_MILLI from the environment. The K8s Job template
/// sets this to the pod's `resources.requests.cpu` in millicores; returns
/// None if the env is unset or malformed. When set, the aggregator prefers
/// this over the rayon pool size for CPU-seconds accounting.
fn read_cpu_request_millicores() -> Option<u64> {
    std::env::var("LB_POD_CPU_REQUEST_MILLI")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&m| m > 0)
}

// ---------------------------------------------------------------------------
// customer360 datagen driver.
//
// Wall-clock structure: parse args + init S3 in ms, then a single rayon
// parallel-for over `total_files` file ids. Each worker builds one RecordBatch
// via `customer360::build_batch`, encodes to Parquet in memory, and PUTs to
// S3. No world / typology / manifest phases: c360 is a flat interaction table
// with no reference zones.
//
// Sizing:
//   rows_per_file = file_size_bytes / customer360_bytes_per_row_default()
//   total_files   = target_bytes / file_size_bytes   (from --target-tb)
//   per_node_files = { fid : fid % total_nodes == node_id }
//
// Determinism: every file is `Rng::new(seed + file_id)` (mixed via splitmix64
// in Rng::new). Node id affects only which subset of file ids this pod owns;
// the file content depends only on (seed, file_id), so two pods writing the
// same fid produce identical bytes.
// ---------------------------------------------------------------------------
fn customer360_main() {
    let bucket: String = arg("--bucket", String::new());
    let prefix: String = arg("--prefix", "customer/interactions/".to_string());
    if bucket.is_empty() {
        eprintln!("--bucket is required (destination S3 bucket)");
        std::process::exit(2);
    }
    let seed: i64 = arg("--seed", 42);
    // See datagen_rs::cycle: n > 0 offsets the per-file stream and row ids and
    // suffixes the keys; 0 reproduces a run without --cycle.
    let cycle_n: u64 = arg("--cycle", 0u64);
    // Two sizing controls: --target-tb picks total file count, --file-size-mb
    // picks per-file size. --scale is accepted but ignored on the c360 path
    // (it's the Python-side abstraction and only informs row density; on the
    // Rust c360 path, target_tb is what actually drives file count).
    let target_tb: f64 = arg("--target-tb", 0.1);
    if !target_tb.is_finite() || target_tb <= 0.0 {
        eprintln!(
            "--target-tb must be a positive finite number; got {}",
            target_tb
        );
        std::process::exit(2);
    }
    let file_size_mb: i64 = arg("--file-size-mb", 64);
    if file_size_mb < 1 {
        eprintln!("--file-size-mb must be >= 1; got {}", file_size_mb);
        std::process::exit(2);
    }
    let node_id: i64 = {
        // Kubernetes Indexed Jobs set JOB_COMPLETION_INDEX; use it as
        // node_id default so the K8s Job template does not have to thread
        // it through explicitly.
        let env_default: i64 = std::env::var("JOB_COMPLETION_INDEX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        arg("--node-id", env_default)
    };
    let total_nodes: i64 = arg("--total-nodes", 1);
    if total_nodes < 1 || node_id < 0 || node_id >= total_nodes {
        eprintln!(
            "--total-nodes must be >= 1 and --node-id in [0, total_nodes); got node_id={} total_nodes={}",
            node_id, total_nodes
        );
        std::process::exit(2);
    }
    let customer_id_max: u64 = arg("--customer-id-max", 500_000u64);
    // `customer360_bytes_per_row_default()` is measured at payload_kb=2. Any
    // other value silently mis-sizes rows_per_file (files 1/N or Nx too
    // large). Refuse until per-payload measurements are folded in.
    let payload_kb: usize = arg("--payload-kb", 2usize);
    if payload_kb != 2 {
        eprintln!(
            "--payload-kb {} is not supported (bytes/row measurements are for payload_kb=2 only); \
             either pass --payload-kb 2 or extend customer360_bytes_per_row_default to be \
             payload-aware",
            payload_kb
        );
        std::process::exit(2);
    }
    let dirty_ratio: f64 = arg("--dirty-ratio", 0.08);
    let duplicate_email_pct: f64 = arg("--duplicate-email-pct", 0.10);
    // Timestamp range as YYYY-MM-DD; default 2024-01-01..2025-01-01 matching
    // the Python c360 defaults.
    let ts_start_str: String = arg("--timestamp-start", "2024-01-01".to_string());
    let ts_end_str: String = arg("--timestamp-end", "2025-01-01".to_string());
    let ts_start_us = parse_date_to_us(&ts_start_str).unwrap_or_else(|| {
        eprintln!(
            "--timestamp-start must be YYYY-MM-DD; got {:?}",
            ts_start_str
        );
        std::process::exit(2);
    });
    let ts_end_us = parse_date_to_us(&ts_end_str).unwrap_or_else(|| {
        eprintln!("--timestamp-end must be YYYY-MM-DD; got {:?}", ts_end_str);
        std::process::exit(2);
    });
    if ts_end_us <= ts_start_us {
        eprintln!(
            "--timestamp-end must be after --timestamp-start; got start={} end={}",
            ts_start_str, ts_end_str
        );
        std::process::exit(2);
    }
    // Rayon pool: honor --threads if set, fall back to --workers for K8s Job
    // templates that don't yet know about --threads. 0 leaves rayon's default.
    let threads: usize = {
        let t = arg::<usize>("--threads", 0);
        if t == 0 {
            arg::<usize>("--workers", 0)
        } else {
            t
        }
    };
    if threads > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .expect("failed to size rayon pool");
    }

    let s3_cfg = match S3Cfg::try_from_env(bucket.clone(), prefix.clone()) {
        Ok(c) => c,
        Err(msg) => {
            eprintln!("s3 config error: {}", msg);
            std::process::exit(2);
        }
    };
    let sink = S3Sink::new(&s3_cfg);

    let t0 = std::time::Instant::now();
    let file_size_bytes = (file_size_mb as usize) * 1024 * 1024;
    let bytes_per_row = customer360_bytes_per_row_default();
    if bytes_per_row <= 0.0 || !bytes_per_row.is_finite() {
        panic!(
            "customer360_bytes_per_row_default returned non-positive {}",
            bytes_per_row
        );
    }
    let rows_per_file: usize = ((file_size_bytes as f64) / bytes_per_row).max(1000.0) as usize;
    let target_bytes: u64 = (target_tb * 1024.0 * 1024.0 * 1024.0 * 1024.0) as u64;
    // Match Python's `max(1, target_bytes // file_size_bytes)` at
    // datagen/generate.py:271. Very small target_tb still gets at least 1
    // file so the run isn't a no-op.
    let total_files: i64 = (target_bytes / file_size_bytes as u64).max(1) as i64;

    // Build the loyalty lookup + customer_id sampler ONCE, then share via Arc
    // across rayon workers. Same lookup used by every file so
    // `loyalty_member` is consistent for a given customer_id across the whole
    // run; same sampler so the hot-customer distribution is stable across
    // files.
    let loyalty = std::sync::Arc::new(LoyaltyLookup::build(seed as u64, customer_id_max));
    let cid_sampler = std::sync::Arc::new(CustomerIdSampler::new(customer_id_max));
    let t_setup = t0.elapsed().as_secs_f64();

    let my_files: Vec<i64> = (0..total_files)
        .filter(|fid| fid % total_nodes == node_id)
        .collect();

    let total_bytes = AtomicU64::new(0);
    let files_written = AtomicU64::new(0);
    let rows_written = AtomicU64::new(0);
    let upload_ns = AtomicU64::new(0);
    let build_ns = AtomicU64::new(0);
    let write_ns = AtomicU64::new(0);
    let t_gen0 = std::time::Instant::now();

    my_files.par_iter().for_each(|&fid| {
        let cfg = customer360::Config {
            seed: seed as u64,
            file_id: cycle::c360_file_id(fid as u64, cycle_n),
            rows_per_file,
            customer_id_max,
            dirty_ratio,
            duplicate_email_pct,
            payload_kb,
            timestamp_start_us: ts_start_us,
            timestamp_end_us: ts_end_us,
        };
        let tb = std::time::Instant::now();
        let batch = customer360::build_batch(&cfg, &loyalty, &cid_sampler);
        build_ns.fetch_add(tb.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let tw = std::time::Instant::now();
        let cap_hint = (file_size_bytes + file_size_bytes / 8).max(1024 * 1024);
        let buf = encode_parquet(&batch, cap_hint);
        write_ns.fetch_add(tw.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let sz = buf.len() as u64;

        // key is relative to S3Sink.prefix (set from --prefix). Sink prepends.
        let key = cycle::c360_key(fid, cycle_n);
        let tu = std::time::Instant::now();
        sink.put(&key, buf);
        upload_ns.fetch_add(tu.elapsed().as_nanos() as u64, Ordering::Relaxed);
        total_bytes.fetch_add(sz, Ordering::Relaxed);
        files_written.fetch_add(1, Ordering::Relaxed);
        rows_written.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
    });

    let total_bytes = total_bytes.load(Ordering::Relaxed);
    let files_written = files_written.load(Ordering::Relaxed);
    let rows_written = rows_written.load(Ordering::Relaxed);
    let t_gen = t_gen0.elapsed().as_secs_f64();
    let up_s = upload_ns.load(Ordering::Relaxed) as f64 / 1e9;
    let build_s = build_ns.load(Ordering::Relaxed) as f64 / 1e9;
    let write_s = write_ns.load(Ordering::Relaxed) as f64 / 1e9;
    let cpu_tot = (build_s + write_s).max(1e-9);
    let el = t0.elapsed().as_secs_f64();
    eprintln!(
        "customer360: node={} of {} target_tb={} file_size_mb={} rows_per_file={} \
         total_files={} files_written={} elapsed={:.2}s bytes={} ({:.1} MB/s)",
        node_id,
        total_nodes,
        target_tb,
        file_size_mb,
        rows_per_file,
        total_files,
        files_written,
        el,
        total_bytes,
        total_bytes as f64 / el / 1e6
    );
    eprintln!(
        "phases: setup={:.2}s gen={:.2}s (build_batch={:.1}s ({:.0}%) encode_parquet={:.1}s ({:.0}%) s3_put={:.1}s)",
        t_setup,
        t_gen,
        build_s,
        100.0 * build_s / cpu_tot,
        write_s,
        100.0 * write_s / cpu_tot,
        up_s
    );

    // Machine-readable per-pod metrics line for the lakebench aggregator.
    // See datagen_rs::metrics for the schema; lakebench parses on prefix
    // `LB_METRICS_JSON `.
    PodMetrics {
        schema: "customer360".into(),
        node_id,
        node_count: total_nodes,
        cores_used: rayon::current_num_threads(),
        cpu_request_millicores: read_cpu_request_millicores(),
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        target_tb: Some(target_tb),
        customer_id_max: Some(customer_id_max),
        dirty_ratio: Some(dirty_ratio),
        file_size_mb,
        rows_per_file: rows_per_file as u64,
        total_files,
        files_written,
        bytes_written: total_bytes,
        rows_written,
        elapsed_s: el,
        setup_s: t_setup,
        gen_s: t_gen,
        build_batch_s: build_s,
        encode_parquet_s: write_s,
        s3_put_s: up_s,
        ..Default::default()
    }
    .emit();
}

/// Parse a YYYY-MM-DD string into microseconds since the Unix epoch (UTC).
/// Returns None on malformed input. Uses `days_from_civil` for the calendar
/// math so it agrees with the pacs.008 corpus-window computation.
fn parse_date_to_us(s: &str) -> Option<i64> {
    let s = s.trim();
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y: i64 = parts[0].parse().ok()?;
    let m: i64 = parts[1].parse().ok()?;
    let d: i64 = parts[2].parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d) * US_PER_DAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date_to_us_epoch() {
        assert_eq!(parse_date_to_us("1970-01-01"), Some(0));
    }

    #[test]
    fn parse_date_to_us_2024_01_01() {
        // 2024-01-01T00:00:00Z = 1704067200 seconds since epoch.
        assert_eq!(
            parse_date_to_us("2024-01-01"),
            Some(1_704_067_200 * 1_000_000)
        );
    }

    #[test]
    fn parse_date_to_us_rejects_bad() {
        assert_eq!(parse_date_to_us("2024/01/01"), None);
        assert_eq!(parse_date_to_us("2024-13-01"), None);
        assert_eq!(parse_date_to_us("2024-01-32"), None);
        assert_eq!(parse_date_to_us(""), None);
        assert_eq!(parse_date_to_us("abc"), None);
    }
}
