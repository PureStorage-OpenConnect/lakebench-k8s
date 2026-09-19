//! Standalone Rust datagen driver. Produces bronze pacs.008 Parquet plus party,
//! account, and manifest tables, driven by (seed, file_id, scale, node). File
//! content depends only on those inputs. The pod is stateless: every parquet
//! file is built into an in-memory `Vec<u8>` and PUT directly to S3, so the
//! local filesystem is never used as a staging buffer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use rayon::prelude::*;

use parquet::arrow::ArrowWriter;

use datagen_rs::amounts::{lognormal_amount, structuring_amount};
use datagen_rs::emit::{build_batch, Batch};
use datagen_rs::hash::{splitmix64, Rng};
use datagen_rs::model::build_world_ex;
use datagen_rs::party::{build_manifest, write_account_to, write_party_to};
use datagen_rs::s3sink::{S3Cfg, S3Sink};
use datagen_rs::timing::{sample_ts, shape_fixed_day, DayCal};
use datagen_rs::world::ring_member;
use datagen_rs::writer::writer_properties;

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
    let scale: f64 = arg("--scale", 0.01);
    let corpus_months: i64 = arg("--corpus-months", 60);
    let file_size_mb: i64 = arg("--file-size-mb", 32);
    let bytes_per_row: f64 = arg("--bytes-per-row", 246.0);
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
            12 * 200, corpus_months
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
        eprintln!("computed corpus window is non-positive; corpus_months={}", corpus_months);
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
    let rows_per_file = (total_txns / total_files).max(1);
    let step_us = span_us / total_files;

    // Schedule + emit typology rows, then bin by file.
    let t_typ0 = std::time::Instant::now();
    let instances = datagen_rs::typology::schedule(seed, total_txns, pop, start_us, end_us, &w.country);
    let mut typ_by_file: Vec<Vec<TypRow>> = (0..total_files).map(|_| Vec::new()).collect();
    // Instance-id -> list of uids for the rows emitted for that instance.
    // Populated at typology-scheduling time so it's deterministic (both
    // bronze and reference pods build the identical map from the same
    // schedule, but only the reference pod writes the manifest that
    // consumes it). Downstream (`score_financial.py`, `verify_run.py`)
    // joins `manifest.participant_uetrs` against `gold.alerts.related_txn_ids`;
    // an empty list here was silently making recall = 0/0.
    let mut inst_uids: HashMap<String, Vec<u64>> = HashMap::new();
    let mut trng = Rng::new((seed as u64) ^ 0x7791);
    for inst in &instances {
        for (row_idx, r) in datagen_rs::typology::emit_instance(inst).into_iter().enumerate() {
            let ccy = w.ccy[r.orig as usize];
            let amount = if r.structuring {
                structuring_amount(&mut trng, ccy)
            } else {
                lognormal_amount(&mut trng)
            };
            let fid = (((r.ts_us - start_us) / step_us).clamp(0, total_files - 1)) as usize;
            let uid = typology_uid(inst.seed, row_idx);
            typ_by_file[fid].push(TypRow {
                orig: r.orig, bene: r.bene, ts_us: r.ts_us, amount, ccy, uid,
            });
            inst_uids.entry(inst.id.clone()).or_default().push(uid);
        }
    }

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
    let upload_ns = AtomicU64::new(0);
    // Split gen time into batch-assembly (arrow builders) vs parquet write
    // (encode + SNAPPY + disk), summed across worker threads (thread-nanos).
    let build_ns = AtomicU64::new(0);
    let write_ns = AtomicU64::new(0);
    let t_gen0 = std::time::Instant::now();

    let my_files: Vec<i64> = if do_bronze {
        (0..total_files).filter(|fid| fid % total_nodes == node_id).collect()
    } else {
        Vec::new()
    };
    my_files.par_iter().for_each(|&fid| {
        let ws = start_us + step_us * fid;
        // midnight-align file window start
        let ws_day = ws / US_PER_DAY;
        let start_aligned = ws_day * US_PER_DAY;
        let we = start_us + step_us * (fid + 1);
        let span_days = (((we - ws) / US_PER_DAY).max(1)) as usize;

        let typ = &typ_by_file[fid as usize];
        let n_typ = typ.len();
        let n_base = (rows_per_file as usize).saturating_sub(n_typ);

        let mut rng = Rng::new((seed as u64).wrapping_add(1_000_003u64.wrapping_mul(fid as u64 + 1)));
        let cal = DayCal::new(start_aligned, span_days);

        let cap = n_base + n_typ;
        let mut orig = Vec::with_capacity(cap);
        let mut bene = Vec::with_capacity(cap);
        let mut ts_us = Vec::with_capacity(cap);
        let mut amount = Vec::with_capacity(cap);
        let mut ccy: Vec<&'static str> = Vec::with_capacity(cap);
        // Pre-assigned uid per row: base rows use (fid<<40)|base_idx
        // (top bit 0), typology rows carry their scheduling-time uid
        // (top bit 1). Kept through the sort so bronze UETRs stay
        // recoverable by the manifest builder.
        let mut uid_pre = Vec::with_capacity(cap);
        let base_uid_hi = (fid as u64) << 40;

        for base_idx in 0..n_base {
            let o = sample_orig(&cum, total_w, pop, &mut rng);
            // Beneficiary drawn from a bounded core counterparty set so that
            // recurring-counterparty volume stays concentrated at any scale
            // (uniform population draws never repeat once population is large,
            // which collapsed the repeat-edge share at scale 1). The core is
            // capped at CORE members; off-ring draws hit an extended band.
            const CORE: u64 = 40;
            let rs = w.ring_sz[o as usize].max(1) as u64;
            let core = rs.min(CORE);
            let mut b;
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
            let cc = w.ccy[o as usize];
            orig.push(o);
            bene.push(b);
            ts_us.push(sample_ts(&mut rng, &cal, w.country[o as usize]));
            amount.push(lognormal_amount(&mut rng));
            ccy.push(cc);
            uid_pre.push(base_uid_hi | base_idx as u64);
        }
        for r in typ {
            orig.push(r.orig);
            bene.push(r.bene);
            // keep typology ts as-is (already within window); shape intraday.
            ts_us.push(shape_fixed_day(&mut rng, r.ts_us, w.country[r.orig as usize]));
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
        let batch = build_batch(&w, &Batch { orig: orig2, bene: bene2, ts_us: ts2, amount: amt2, ccy: ccy2, uid });
        build_ns.fetch_add(tb.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let tw = std::time::Instant::now();
        // Slight overshoot on the pre-alloc so ArrowWriter rarely reallocs.
        let cap_hint = (file_size as usize + file_size as usize / 8).max(1024 * 1024);
        let buf = encode_parquet(&batch, cap_hint);
        write_ns.fetch_add(tw.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let sz = buf.len() as u64;
        let key = format!("bronze/pacs008/part-{:06}.parquet", fid);
        let tu = std::time::Instant::now();
        sink.put(&key, buf);
        upload_ns.fetch_add(tu.elapsed().as_nanos() as u64, Ordering::Relaxed);
        total_bytes.fetch_add(sz, Ordering::Relaxed);
        files_written.fetch_add(1, Ordering::Relaxed);
    });
    let total_bytes = total_bytes.load(Ordering::Relaxed);
    let files_written = files_written.load(Ordering::Relaxed);
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
        // Streamed in row-group chunks into an in-memory buffer, then PUT
        // as one object each. Party is the biggest (~ population * ~200 B),
        // still well under a pod's memory request at scale 100.
        let mut party_buf: Vec<u8> = Vec::with_capacity(256 * 1024 * 1024);
        write_party_to(&w, &instances, &mut party_buf);
        ref_bytes += party_buf.len() as u64;
        ref_files += 1;
        sink.put("bronze/party.parquet", party_buf);
        let mut acct_buf: Vec<u8> = Vec::with_capacity(128 * 1024 * 1024);
        write_account_to(&w, &mut acct_buf);
        ref_bytes += acct_buf.len() as u64;
        ref_files += 1;
        sink.put("bronze/account.parquet", acct_buf);
        let man_bytes = encode_parquet(
            &build_manifest(&instances, seed, &inst_uids),
            8 * 1024 * 1024,
        );
        ref_bytes += man_bytes.len() as u64;
        ref_files += 1;
        sink.put("manifest/manifest.parquet", man_bytes);
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
        t_world, t_typ, t_gen, t_ref, bronze_only, 100.0 * t_world / el.max(1e-9)
    );
    eprintln!(
        "gen split (thread-s): build_batch={:.1}s ({:.0}%) encode_parquet={:.1}s ({:.0}%) s3_put={:.1}s",
        build_s, 100.0 * build_s / cpu_tot, write_s, 100.0 * write_s / cpu_tot, up_s
    );
}
