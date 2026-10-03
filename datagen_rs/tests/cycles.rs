//! Multi-cycle AML generation (WORKPLAN B4): the union of `--cycles N` runs is
//! the one-shot corpus, row for row, and the manifest union is the one-shot
//! manifest. Runs the real binary against a local sink (DG_LOCAL_DIR).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const SCALE: &str = "0.005";

/// The tracked held-out hash file the generator needs for the financial
/// schema (`LB_HELDOUT_HASHES`; a ConfigMap on a pod).
fn heldout_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../src/lakebench/spark/data/aml/heldout_hashes.json")
}

/// The generator binary with the hash file set, as a pod runs it.
fn generate_cmd() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_generate"));
    c.env("LB_HELDOUT_HASHES", heldout_file());
    c
}

fn run(dir: &Path, extra: &[&str]) {
    let out = generate_cmd()
        .env("DG_LOCAL_DIR", dir)
        .args([
            "--bucket",
            "b",
            "--seed",
            "7777",
            "--scale",
            SCALE,
            "--threads",
            "2",
            "--mode",
            "all",
        ])
        .args(extra)
        .output()
        .expect("run generate");
    assert!(
        out.status.success(),
        "generate {:?} failed: {}",
        extra,
        String::from_utf8_lossy(&out.stderr)
    );
}

fn files(dir: &Path, sub: &str, stem: &str) -> Vec<PathBuf> {
    let d = dir.join("b").join(sub);
    let mut v: Vec<PathBuf> = std::fs::read_dir(&d)
        .map(|it| {
            it.map(|e| e.unwrap().path())
                .filter(|p| {
                    let n = p.file_name().unwrap().to_string_lossy();
                    n.starts_with(stem) && n.ends_with(".parquet")
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

fn batches(p: &Path) -> Vec<RecordBatch> {
    let f = std::fs::File::open(p).unwrap();
    ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap())
        .collect()
}

/// Every row rendered with all its columns, keyed by its first column after
/// sorting: a multiset of complete rows.
fn rows(paths: &[PathBuf]) -> Vec<String> {
    let mut out = Vec::new();
    for p in paths {
        for b in batches(p) {
            for r in 0..b.num_rows() {
                let cols: Vec<String> = (0..b.num_columns())
                    .map(|c| array_value_to_string(b.column(c), r).unwrap())
                    .collect();
                out.push(cols.join("\u{1f}"));
            }
        }
    }
    out.sort();
    out
}

fn uetr_col(paths: &[PathBuf]) -> Vec<String> {
    let mut v = Vec::new();
    for p in paths {
        for b in batches(p) {
            let c = b.column_by_name("uetr").unwrap();
            v.extend((0..b.num_rows()).map(|r| array_value_to_string(c, r).unwrap()));
        }
    }
    v
}

/// Per-originator inter-send gaps (seconds), from (dbtr iban, cre_dt_tm).
fn gaps(paths: &[PathBuf]) -> BTreeMap<String, Vec<i64>> {
    let mut sends: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for p in paths {
        for b in batches(p) {
            let acct = b.column_by_name("dbtr_acct").unwrap();
            let acct = acct
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .unwrap();
            let iban = acct.column_by_name("iban").unwrap();
            let ts = b.column_by_name("cre_dt_tm").unwrap();
            let ts = ts
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .unwrap();
            for r in 0..b.num_rows() {
                sends
                    .entry(array_value_to_string(iban, r).unwrap())
                    .or_default()
                    .push(ts.value(r));
            }
        }
    }
    sends
        .into_iter()
        .map(|(k, mut v)| {
            v.sort_unstable();
            (k, v.windows(2).map(|w| w[1] - w[0]).collect())
        })
        .collect()
}

#[test]
fn union_of_cycles_is_the_one_shot_corpus() {
    let tmp = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-cycles-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let one = tmp.join("one");
    let multi = tmp.join("multi");
    run(&one, &[]);
    for n in 0..3 {
        run(&multi, &["--cycle", &n.to_string(), "--cycles", "3"]);
    }

    let one_pacs = files(&one, "bronze/pacs008", "part-");
    let multi_pacs = files(&multi, "bronze/pacs008", "part-");
    assert!(multi_pacs
        .iter()
        .any(|p| p.to_string_lossy().contains("part-c002-")));
    // No key written twice by two cycles (each cycle's keys are its own).
    let one_rows = rows(&one_pacs);
    let multi_rows = rows(&multi_pacs);
    assert_eq!(one_rows.len(), multi_rows.len(), "row count differs");
    assert!(
        one_rows == multi_rows,
        "multi-cycle rows differ from one-shot rows"
    );
    let mut u = uetr_col(&multi_pacs);
    let n = u.len();
    u.sort();
    u.dedup();
    assert_eq!(u.len(), n, "a UETR appears in two cycles");

    // Manifest union == one-shot manifest, each instance exactly once.
    let one_man = rows(&files(&one, "manifest", "manifest"));
    let multi_man = rows(&files(&multi, "manifest", "manifest"));
    assert_eq!(files(&multi, "manifest", "manifest").len(), 3);
    assert!(
        one_man == multi_man,
        "manifest union differs from one-shot manifest"
    );

    // Dormancy (and every other) gap is unchanged.
    assert_eq!(gaps(&one_pacs), gaps(&multi_pacs));

    // Party and account are written once, by cycle 0, and match one shot.
    assert_eq!(files(&multi, "bronze", "party").len(), 1);
    assert_eq!(
        rows(&files(&one, "bronze", "party")),
        rows(&files(&multi, "bronze", "party"))
    );
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn cycle_arguments_are_strict() {
    for bad in [
        vec!["--cycle", "3", "--cycles", "3"],
        vec!["--cycle", "-1"],
        vec!["--cycles", "0"],
        vec!["--cycle=x"],
    ] {
        let st = generate_cmd()
            .env(
                "DG_LOCAL_DIR",
                std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-cycles-bad"),
            )
            .args(["--bucket", "b", "--seed", "7777", "--scale", SCALE])
            .args(&bad)
            .output()
            .unwrap();
        assert_eq!(st.status.code(), Some(2), "{bad:?} was accepted");
    }
}

/// Wave 1 A3 (2026-09-28): `--mode reference` with `--total-nodes > 1` must
/// refuse to start, because the reference zone is a single-writer artefact
/// and running two reference pods races the same S3 keys (manifest.parquet,
/// party.parquet, account.parquet, watchlist.parquet).
#[test]
fn reference_mode_refuses_multi_writer() {
    let st = generate_cmd()
        .env(
            "DG_LOCAL_DIR",
            std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-mode-ref-race"),
        )
        .args([
            "--bucket",
            "b",
            "--seed",
            "7777",
            "--scale",
            SCALE,
            "--mode",
            "reference",
            "--total-nodes",
            "2",
        ])
        .output()
        .unwrap();
    assert_eq!(
        st.status.code(),
        Some(2),
        "--mode reference with --total-nodes 2 was accepted (would race)"
    );
    let err = String::from_utf8_lossy(&st.stderr);
    assert!(
        err.contains("race the reference S3 keys"),
        "wrong refusal message: {err}"
    );
}

/// `--mode reference` with a single node still runs (the guard is on multi-
/// writer, not on the reference role itself).
#[test]
fn reference_mode_with_one_node_runs() {
    let d = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-mode-ref-ok");
    let _ = std::fs::remove_dir_all(&d);
    let st = generate_cmd()
        .env("DG_LOCAL_DIR", &d)
        .args([
            "--bucket",
            "b",
            "--seed",
            "7777",
            "--scale",
            SCALE,
            "--threads",
            "2",
            "--mode",
            "reference",
            "--total-nodes",
            "1",
        ])
        .output()
        .unwrap();
    assert!(
        st.status.success(),
        "--mode reference --total-nodes 1 failed: {}",
        String::from_utf8_lossy(&st.stderr)
    );
    // Reference-only pod writes manifest + account + party + watchlist and
    // nothing under bronze/pacs008/.
    let root = d.join("b");
    assert!(root.join("manifest/manifest.parquet").exists());
    assert!(root.join("bronze/account.parquet").exists());
    assert!(root.join("bronze/party.parquet").exists());
    assert!(root.join("bronze/watchlist.parquet").exists());
    let pacs = root.join("bronze/pacs008");
    assert!(
        !pacs.exists() || std::fs::read_dir(&pacs).unwrap().next().is_none(),
        "--mode reference wrote pacs008 bronze files"
    );
}

#[test]
fn financial_seed_is_required_strict_and_never_spent() {
    for bad in [
        vec![],
        vec!["--seed", "42"],
        vec!["--seed", "50000042"],
        vec!["--seed=42"],
        vec!["--seed", "43x"],
        vec!["--seed", "9223372036854775808"],
        vec!["--seed"],
    ] {
        let st = generate_cmd()
            .env(
                "DG_LOCAL_DIR",
                std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-seed-bad"),
            )
            .args(["--bucket", "b", "--scale", SCALE])
            .args(&bad)
            .output()
            .unwrap();
        assert_eq!(st.status.code(), Some(2), "{bad:?} was accepted");
        assert!(
            String::from_utf8_lossy(&st.stderr).contains("--seed"),
            "{bad:?} failed for another reason"
        );
    }
}

#[test]
fn financial_seed_from_env_is_the_same_corpus_and_never_echoed() {
    // A registered corpus's seed arrives in LB_DATAGEN_SEED (from a Secret)
    // instead of --seed: the same seed must give the same bytes, the two
    // together are refused, and a bad value is not printed.
    let base = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-seed-env");
    let (a, b) = (base.join("argv"), base.join("env"));
    for d in [&a, &b] {
        let _ = std::fs::remove_dir_all(d);
    }
    run(&a, &[]);
    let st = generate_cmd()
        .env("DG_LOCAL_DIR", &b)
        .env("LB_DATAGEN_SEED", "7777")
        .args([
            "--bucket",
            "b",
            "--scale",
            SCALE,
            "--threads",
            "2",
            "--mode",
            "all",
        ])
        .output()
        .unwrap();
    assert!(
        st.status.success(),
        "{}",
        String::from_utf8_lossy(&st.stderr)
    );
    assert_eq!(tree_digest(&a), tree_digest(&b));
    for (env, args) in [
        ("7777", vec!["--seed", "7777"]),
        ("77x77913", vec![]),
        ("", vec![]),
        ("-7777", vec![]),
    ] {
        let st = generate_cmd()
            .env("DG_LOCAL_DIR", base.join("bad"))
            .env("LB_DATAGEN_SEED", env)
            .args(["--bucket", "b", "--scale", SCALE])
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(st.status.code(), Some(2), "{env:?} {args:?} was accepted");
        let err = String::from_utf8_lossy(&st.stderr);
        assert!(err.contains("LB_DATAGEN_SEED"), "{err}");
        assert!(env.is_empty() || !err.contains(env), "the value was echoed");
    }
}

fn expected_rows(dir: &Path, extra: &[&str]) -> u64 {
    let out = generate_cmd()
        .env("DG_LOCAL_DIR", dir)
        // Large enough that 1 MB files outnumber the 64-file floor.
        .args([
            "--bucket", "b", "--seed", "7777", "--scale", "0.02", "--mode", "all",
        ])
        .args(extra)
        .output()
        .expect("run generate");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    let num = |key: &str| -> u64 {
        let v = text
            .split(key)
            .nth(1)
            .unwrap_or_else(|| panic!("{key} in output"));
        v.split_whitespace().next().unwrap().parse().unwrap()
    };
    // Screening payments (screening.rs) are added on top of the corpus row
    // budget, so the rows written are total_txns plus screen_rows.
    num("total_txns=") + num("screen_rows=")
}

#[test]
fn rows_are_independent_of_threads_file_size_and_nodes() {
    // Every row, scheduled (D2) ones included, lands in exactly one file
    // whatever the layout: same multiset of rows, and exactly total_txns.
    let tmp = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-layout-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let a = tmp.join("a");
    let b = tmp.join("b2");
    let want = expected_rows(&a, &["--threads", "2"]);
    for node in ["0", "1"] {
        expected_rows(
            &b,
            &[
                "--threads",
                "3",
                "--file-size-mb",
                "1",
                "--total-nodes",
                "2",
                "--node-id",
                node,
            ],
        );
    }
    let ra = rows(&files(&a, "bronze/pacs008", "part-"));
    let fb = files(&b, "bronze/pacs008", "part-");
    let na = files(&a, "bronze/pacs008", "part-").len();
    assert!(
        fb.len() > na,
        "layouts not distinct: {} vs {} files",
        fb.len(),
        na
    );
    let rb = rows(&fb);
    assert_eq!(ra.len() as u64, want, "rows != total_txns + screen_rows");
    assert!(ra == rb, "rows depend on threads, file size or nodes");
    let _ = std::fs::remove_dir_all(&tmp);
}

/// FNV-1a over every file the customer360 driver writes, in path order.
fn c360_driver_digest(threads: &str) -> (u64, usize) {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-c360-{}-{threads}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let out = generate_cmd()
        .env("DG_LOCAL_DIR", &dir)
        .args([
            "--schema",
            "customer360",
            "--bucket",
            "b",
            "--seed",
            "43",
            "--target-tb",
            "0.00002",
            "--file-size-mb",
            "4",
            "--threads",
            threads,
        ])
        .output()
        .expect("run generate");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let mut paths = Vec::new();
    let mut stack = vec![dir.clone()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                paths.push(p);
            }
        }
    }
    paths.sort();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for p in &paths {
        let rel = p.strip_prefix(&dir).unwrap().to_string_lossy().to_string();
        for &x in rel
            .as_bytes()
            .iter()
            .chain(std::fs::read(p).unwrap().iter())
        {
            h ^= x as u64;
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    (h, paths.len())
}

#[test]
fn c360_driver_output_is_pinned() {
    // The whole c360 driver path (argument defaults, id sizing, rows per
    // file, file ids), not just build_batch: the programme reuses c360
    // evidence only while this output is unchanged. Captured at ab585eb.
    // A change here is a c360 data change: re-run the c360 evidence, then
    // update the digest deliberately.
    let a = c360_driver_digest("2");
    assert_eq!(a, c360_driver_digest("3"), "c360 output depends on threads");
    // Updated 2026-09-28 (LB-191 dirty-ratio fix, Wave 1 C1). Pre-fix digest
    // was (8_993_469_679_856_816_545, 5).
    assert_eq!(
        a,
        (7_884_786_140_200_387_728, 5),
        "c360 driver output changed"
    );
}

// ---------------------------------------------------------------------------
// Wave 2 D3/D4 (2026-09-28): --delivery-mode {batch|continuous} switch.
// Both modes must produce the same corpus at a fixed seed. Batch is the
// current (buffered whole-file PUT) path; continuous streams each parquet
// file through MpuWriter as row-groups close. Test asserts multiset-equality
// on all rows across the two runs: rows may be identical or the ordering
// may differ (parquet's row-group layout is unchanged, so byte-identity
// often holds; we assert the weaker, more robust row-identity so a future
// row-group tuning does not silently red-CI this).
// ---------------------------------------------------------------------------

fn run_c360_mode(dir: &Path, delivery: &str) {
    let out = generate_cmd()
        .env("DG_LOCAL_DIR", dir)
        // DG_ROW_GROUP forces multiple row-groups per file so continuous
        // mode actually flushes mid-file via MpuWriter (parquet default is
        // ~1M rows/group which produces one group per file in every shipping
        // config, so a naive test would prove nothing about the streaming
        // path). 100 rows/group means every c360 file has dozens of groups.
        .env("DG_ROW_GROUP", "100")
        .args([
            "--schema",
            "customer360",
            "--bucket",
            "b",
            "--seed",
            "42",
            "--target-tb",
            "0.00001",
            "--file-size-mb",
            "1",
            "--threads",
            "2",
            "--delivery-mode",
            delivery,
        ])
        .output()
        .expect("run generate");
    assert!(
        out.status.success(),
        "c360 generate --delivery-mode {} failed: {}",
        delivery,
        String::from_utf8_lossy(&out.stderr)
    );
}

fn c360_files(dir: &Path) -> Vec<PathBuf> {
    let d = dir.join("b").join("customer").join("interactions");
    let mut v: Vec<PathBuf> = std::fs::read_dir(&d)
        .map(|it| {
            it.map(|e| e.unwrap().path())
                .filter(|p| {
                    let n = p.file_name().unwrap().to_string_lossy();
                    n.starts_with("part-") && n.ends_with(".parquet")
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// c360-specific row renderer: `array_value_to_string` on a
/// Timestamp(us, UTC) column fails without arrow's chrono-tz feature.
/// Render every column via its Debug (via TypedArray) which is TZ-agnostic,
/// then join. Sorted for multiset equality across modes.
fn c360_rows(paths: &[PathBuf]) -> Vec<String> {
    let mut out = Vec::new();
    for p in paths {
        for b in batches(p) {
            for r in 0..b.num_rows() {
                let mut cols: Vec<String> = Vec::with_capacity(b.num_columns());
                for c in 0..b.num_columns() {
                    let arr = b.column(c);
                    // Timestamps: render raw i64 microseconds (TZ-invariant).
                    let s = if let Some(ts) = arr
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    {
                        if ts.is_null(r) {
                            "null".to_string()
                        } else {
                            ts.value(r).to_string()
                        }
                    } else {
                        array_value_to_string(arr, r).unwrap()
                    };
                    cols.push(s);
                }
                out.push(cols.join("\u{1f}"));
            }
        }
    }
    out.sort();
    out
}

#[test]
fn c360_row_identity_across_delivery_modes() {
    let base = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-delivery-{}", std::process::id()));
    let batch_dir = base.join("batch");
    let continuous_dir = base.join("continuous");
    let _ = std::fs::remove_dir_all(&batch_dir);
    let _ = std::fs::remove_dir_all(&continuous_dir);
    run_c360_mode(&batch_dir, "batch");
    run_c360_mode(&continuous_dir, "continuous");
    let batch_rows = c360_rows(&c360_files(&batch_dir));
    let cont_rows = c360_rows(&c360_files(&continuous_dir));
    assert_eq!(
        batch_rows.len(),
        cont_rows.len(),
        "row count differs: batch={} continuous={}",
        batch_rows.len(),
        cont_rows.len()
    );
    assert_eq!(
        batch_rows, cont_rows,
        "c360 row content differs between --delivery-mode batch and continuous"
    );
}

fn run_aml_mode(dir: &Path, delivery: &str) {
    let out = generate_cmd()
        .env("DG_LOCAL_DIR", dir)
        // See run_c360_mode: DG_ROW_GROUP forces multi-row-group per file so
        // continuous mode actually flushes to S3 multipart mid-file. 100 rows
        // per group gives dozens of groups per AML bronze file.
        .env("DG_ROW_GROUP", "100")
        .args([
            "--bucket",
            "b",
            "--seed",
            "7777",
            "--scale",
            SCALE,
            "--threads",
            "2",
            "--mode",
            "all",
            "--delivery-mode",
            delivery,
        ])
        .output()
        .expect("run generate");
    assert!(
        out.status.success(),
        "aml generate --delivery-mode {} failed: {}",
        delivery,
        String::from_utf8_lossy(&out.stderr)
    );
}

fn aml_bronze_files(dir: &Path) -> Vec<PathBuf> {
    files(dir, "bronze/pacs008", "part-")
}

#[test]
fn aml_row_identity_across_delivery_modes() {
    let base = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-delivery-aml-{}", std::process::id()));
    let batch_dir = base.join("batch");
    let continuous_dir = base.join("continuous");
    let _ = std::fs::remove_dir_all(&batch_dir);
    let _ = std::fs::remove_dir_all(&continuous_dir);
    run_aml_mode(&batch_dir, "batch");
    run_aml_mode(&continuous_dir, "continuous");
    let batch_rows = rows(&aml_bronze_files(&batch_dir));
    let cont_rows = rows(&aml_bronze_files(&continuous_dir));
    assert_eq!(
        batch_rows.len(),
        cont_rows.len(),
        "aml pacs008 row count differs: batch={} continuous={}",
        batch_rows.len(),
        cont_rows.len()
    );
    assert_eq!(
        batch_rows, cont_rows,
        "aml pacs008 row content differs between --delivery-mode batch and continuous"
    );
}

// ---------------------------------------------------------------------------
// LB-204 (datagen per-pod memory redesign, Tier-1): the world's full-population
// attribute columns are recomputed on demand instead of held resident. The
// corpus must stay identical. These tests are the byte/row-multiset proof that
// the refactor is output-neutral, exercised across --total-nodes 1 and a
// --total-nodes 2 split (so the node partition actually fires), at a scale
// where synthetic_identity, dormant_reactivation and corridor_high_risk each
// have >=1 instance (so the reference syn_overrides base-PII recompute and the
// dormancy suppression path are actually walked).
// ---------------------------------------------------------------------------

/// Scale for the full-corpus checks below: verified to contain at least one
/// synthetic_identity, dormant_reactivation and corridor_high_risk instance at
/// seed 7777 (asserted in-test), while keeping the decoded corpus small enough
/// for a unit-tier run.
const NODE_SCALE: &str = "0.02";

fn run_at(dir: &Path, scale: &str, extra: &[&str]) {
    let out = generate_cmd()
        .env("DG_LOCAL_DIR", dir)
        .args([
            "--bucket", "b", "--seed", "7777", "--scale", scale, "--mode", "all",
        ])
        .args(extra)
        .output()
        .expect("run generate");
    assert!(
        out.status.success(),
        "generate {:?} failed: {}",
        extra,
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Whole-corpus row multiset (a single sorted Vec of complete rendered rows,
/// keyed only by content, so it is order- and file-partition-invariant). Each
/// table's rows are tagged by table so a bronze row can never coincidentally
/// equal a reference row.
fn corpus_multiset(dir: &Path) -> Vec<String> {
    let tag = |sub: &str, stem: &str, paths: &[PathBuf]| -> Vec<String> {
        rows(paths)
            .into_iter()
            .map(|r| format!("{sub}/{stem}\u{1e}{r}"))
            .collect()
    };
    let mut out = Vec::new();
    out.extend(tag(
        "bronze/pacs008",
        "part-",
        &files(dir, "bronze/pacs008", "part-"),
    ));
    out.extend(tag("bronze", "party", &files(dir, "bronze", "party")));
    out.extend(tag("bronze", "account", &files(dir, "bronze", "account")));
    out.extend(tag(
        "bronze",
        "watchlist",
        &files(dir, "bronze", "watchlist"),
    ));
    out.extend(tag(
        "manifest",
        "manifest",
        &files(dir, "manifest", "manifest"),
    ));
    out.sort();
    out
}

fn manifest_typology_types(dir: &Path) -> BTreeSet<String> {
    let mut s = BTreeSet::new();
    for p in files(dir, "manifest", "manifest") {
        for b in batches(&p) {
            let c = b.column_by_name("typology_type").unwrap();
            for r in 0..b.num_rows() {
                s.insert(array_value_to_string(c, r).unwrap());
            }
        }
    }
    s
}

/// The whole corpus (bronze + party + account + watchlist + manifest) is the
/// same row multiset whether generated as one node or split across two, after
/// the LB-204 recompute-on-demand refactor. A two-node run fires the file
/// partition on every table, so a recompute that diverged per node (or a
/// reference column that leaned on a now-removed world Vec) would surface here.
#[test]
fn full_corpus_row_multiset_is_node_count_invariant() {
    let tmp = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-node-multiset-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let one = tmp.join("one");
    let two = tmp.join("two");
    // One node, one shot.
    run_at(
        &one,
        NODE_SCALE,
        &["--threads", "3", "--total-nodes", "1", "--node-id", "0"],
    );
    // Two nodes into one tree: node 0 also writes the reference zones, node 1
    // writes only its bronze shards. Their union is the whole corpus.
    run_at(
        &two,
        NODE_SCALE,
        &["--threads", "3", "--total-nodes", "2", "--node-id", "0"],
    );
    run_at(
        &two,
        NODE_SCALE,
        &["--threads", "3", "--total-nodes", "2", "--node-id", "1"],
    );

    // The scale must actually exercise the three landmine typologies.
    let types = manifest_typology_types(&one);
    for t in [
        "synthetic_identity",
        "dormant_reactivation",
        "corridor_high_risk",
    ] {
        assert!(
            types.contains(t),
            "typology {t} absent at scale {NODE_SCALE}: {types:?}"
        );
    }

    // The two-node run really sharded the bronze zone across both nodes: file
    // fid is `part-{fid:06}.parquet`, and node n writes fids with fid % 2 == n.
    // Require at least one even-fid shard (node 0) AND one odd-fid shard (node
    // 1), so a degenerate "node 0 writes everything, node 1 writes nothing" bug
    // cannot pass this test.
    let bronze_two = files(&two, "bronze/pacs008", "part-");
    let fid_parity = |p: &PathBuf, want: i64| -> bool {
        let n = p.file_name().unwrap().to_string_lossy();
        n.strip_prefix("part-")
            .and_then(|s| s.strip_suffix(".parquet"))
            .and_then(|s| s.parse::<i64>().ok())
            .map(|fid| fid % 2 == want)
            .unwrap_or(false)
    };
    assert!(bronze_two.len() > 1, "two-node run did not shard bronze");
    assert!(
        bronze_two.iter().any(|p| fid_parity(p, 0)),
        "no node-0 (even-fid) bronze shard in the two-node run"
    );
    assert!(
        bronze_two.iter().any(|p| fid_parity(p, 1)),
        "no node-1 (odd-fid) bronze shard: the two-node split did not fire"
    );
    // Reference tables are single-writer: exactly one of each in both runs.
    assert_eq!(files(&two, "bronze", "party").len(), 1);
    assert_eq!(files(&two, "manifest", "manifest").len(), 1);
    assert_eq!(files(&two, "bronze", "account").len(), 1);
    assert_eq!(files(&two, "bronze", "watchlist").len(), 1);

    let ms_one = corpus_multiset(&one);
    let ms_two = corpus_multiset(&two);
    assert_eq!(
        ms_one.len(),
        ms_two.len(),
        "corpus row count differs: 1-node {} vs 2-node {}",
        ms_one.len(),
        ms_two.len()
    );
    assert!(
        ms_one == ms_two,
        "full-corpus row multiset differs between 1-node and 2-node builds"
    );
    // Sanity: the corpus is non-degenerate (bronze + all four reference tables
    // contributed rows).
    assert!(!files(&one, "bronze/pacs008", "part-").is_empty());
    assert!(ms_one.iter().any(|r| r.starts_with("bronze/party\u{1e}")));
    assert!(ms_one.iter().any(|r| r.starts_with("bronze/account\u{1e}")));
    assert!(ms_one
        .iter()
        .any(|r| r.starts_with("bronze/watchlist\u{1e}")));
    assert!(ms_one
        .iter()
        .any(|r| r.starts_with("manifest/manifest\u{1e}")));
    let _ = std::fs::remove_dir_all(&tmp);
}

/// LB-204 coverage: the only code path that reads `typ_by_file` to decide cycle
/// membership (the `my_files` filter's cycles>1 branch) fires only when BOTH
/// --cycles>1 and --total-nodes>1. The typ_by_file prune retains a file's payload
/// only on its owning node, so a prune that dropped an owned file's rows, or a
/// cycle filter that read a pruned file, would corrupt the corpus in exactly this
/// combination and nowhere else. Assert the union over (cycle x node) is the
/// one-shot corpus, byte-for-byte at the row-multiset level.
#[test]
fn cycles_and_nodes_together_union_to_the_one_shot_corpus() {
    let tmp = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-cyc-node-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let one = tmp.join("one");
    let split = tmp.join("split");
    run_at(
        &one,
        NODE_SCALE,
        &["--threads", "3", "--total-nodes", "1", "--node-id", "0"],
    );
    // 2 cycles x 2 nodes. Node 0 writes the reference zones (once for party/
    // account, once per cycle for the manifest); node 1 writes only bronze.
    for c in 0..2 {
        for n in 0..2 {
            run_at(
                &split,
                NODE_SCALE,
                &[
                    "--threads",
                    "3",
                    "--total-nodes",
                    "2",
                    "--node-id",
                    &n.to_string(),
                    "--cycle",
                    &c.to_string(),
                    "--cycles",
                    "2",
                ],
            );
        }
    }
    // Cycle 1 actually produced bronze (so the cycles>1 my_files path fired).
    let bronze = files(&split, "bronze/pacs008", "part-");
    assert!(
        bronze
            .iter()
            .any(|p| p.to_string_lossy().contains("part-c001-")),
        "cycle 1 wrote no bronze -- the cycle>1 x node>1 path did not fire"
    );
    // One manifest per cycle, single-writer reference zones.
    assert_eq!(files(&split, "manifest", "manifest").len(), 2);
    assert_eq!(files(&split, "bronze", "party").len(), 1);
    assert_eq!(files(&split, "bronze", "account").len(), 1);

    let ms_one = corpus_multiset(&one);
    let ms_split = corpus_multiset(&split);
    assert_eq!(
        ms_one.len(),
        ms_split.len(),
        "corpus row count differs: one-shot {} vs cycles x nodes {}",
        ms_one.len(),
        ms_split.len()
    );
    assert!(
        ms_one == ms_split,
        "full-corpus row multiset differs between one-shot and cycles x nodes"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}

/// The freeze-void razor's edges (LB-204 REVISION 2): total_activity feeds
/// crr_score/crr_tier in party.parquet, and inst_uids feeds participant_uetrs
/// in manifest.parquet -- both frozen. A byte-level (not just row-multiset)
/// comparison of these two files between a 1-node and a 2-node build catches a
/// low-bit drift a multiset would smear over, and proves the reference bytes do
/// not depend on the node count. Party/account/manifest each stream through the
/// same chunked writer regardless of node count, so byte identity is the bar.
#[test]
fn reference_files_are_byte_identical_across_node_counts() {
    let tmp = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-node-bytes-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    let one = tmp.join("one");
    let two = tmp.join("two");
    run_at(
        &one,
        NODE_SCALE,
        &["--threads", "3", "--total-nodes", "1", "--node-id", "0"],
    );
    run_at(
        &two,
        NODE_SCALE,
        &["--threads", "3", "--total-nodes", "2", "--node-id", "0"],
    );
    run_at(
        &two,
        NODE_SCALE,
        &["--threads", "3", "--total-nodes", "2", "--node-id", "1"],
    );

    for rel in [
        "b/bronze/party.parquet",
        "b/manifest/manifest.parquet",
        "b/bronze/account.parquet",
        "b/bronze/watchlist.parquet",
    ] {
        let a = std::fs::read(one.join(rel)).unwrap_or_else(|e| panic!("read {rel} (1-node): {e}"));
        let b = std::fs::read(two.join(rel)).unwrap_or_else(|e| panic!("read {rel} (2-node): {e}"));
        assert_eq!(
            a, b,
            "{rel} differs byte-for-byte between 1-node and 2-node builds"
        );
        assert!(!a.is_empty(), "{rel} is empty");
    }
    let _ = std::fs::remove_dir_all(&tmp);
}

/// FNV-1a over every file under `dir` (relative path, then bytes), sorted by
/// relative path string.
fn tree_digest(dir: &Path) -> (u64, usize) {
    let mut paths = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                paths.push(p.strip_prefix(dir).unwrap().to_string_lossy().to_string());
            }
        }
    }
    paths.sort();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for rel in &paths {
        for &x in rel
            .as_bytes()
            .iter()
            .chain(std::fs::read(dir.join(rel)).unwrap().iter())
        {
            h ^= x as u64;
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
    }
    (h, paths.len())
}

#[test]
fn financial_output_is_pinned_to_the_frozen_generator() {
    // Every financial object (pacs008 bronze, party, account, watchlist,
    // manifest) of a 2-node --mode all run, byte for byte. The digest was
    // captured from the frozen generator (datagen_rs at 9382420) with the same
    // arguments, so this pins the LB-204 changes (owned-file typology pruning,
    // on-demand world columns, mimalloc) as output-neutral. A change here is
    // an AML generator output change: it voids the AML freeze
    // (docs/internal/aml-protocol.md).
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-fin-pin-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for node in ["0", "1"] {
        let out = generate_cmd()
            .env("DG_LOCAL_DIR", &dir)
            .args([
                "--bucket",
                "b",
                "--seed",
                "7777",
                "--scale",
                "0.02",
                "--threads",
                "2",
                "--mode",
                "all",
                "--total-nodes",
                "2",
                "--node-id",
                node,
                "--file-size-mb",
                "1",
                "--delivery-mode",
                "batch",
            ])
            .output()
            .expect("run generate");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let got = tree_digest(&dir);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        got,
        (14_946_780_858_166_320_800, 179),
        "financial generator output changed"
    );
}

// ---------------------------------------------------------------------------
// Three more pins, captured at integrate 77a65d2 source (equal
// to the v1.6 release generator, which passes the two pins above). With the
// two above they cover the paths the look-image changes touch: the
// perturbation branch, and cycle slicing with cycle-suffixed keys on both
// schemas. A change here is a generator output change: on financial it
// voids the AML freeze (docs/internal/aml-protocol.md).
// ---------------------------------------------------------------------------

/// Run `generate` once per argv into one fresh local tree and digest it.
fn pin_tree(tag: &str, runs: &[Vec<&str>]) -> (u64, usize) {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-pin-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for argv in runs {
        let out = generate_cmd()
            .env("DG_LOCAL_DIR", &dir)
            .args(argv)
            .output()
            .expect("run generate");
        assert!(
            out.status.success(),
            "generate {:?} failed: {}",
            argv,
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let got = tree_digest(&dir);
    let _ = std::fs::remove_dir_all(&dir);
    got
}

/// The financial pin's argv (2 nodes, batch delivery) plus `extra`.
fn financial_pin_runs<'a>(extra: &[&'a str]) -> Vec<Vec<&'a str>> {
    ["0", "1"]
        .iter()
        .map(|node| {
            let mut a = vec![
                "--bucket",
                "b",
                "--seed",
                "7777",
                "--scale",
                "0.02",
                "--threads",
                "2",
                "--mode",
                "all",
                "--total-nodes",
                "2",
                "--node-id",
                node,
                "--file-size-mb",
                "1",
                "--delivery-mode",
                "batch",
            ];
            a.extend_from_slice(extra);
            a
        })
        .collect()
}

#[test]
fn financial_perturbed_output_is_pinned() {
    let got = pin_tree(
        "fin-pert",
        &financial_pin_runs(&["--robustness-perturbation"]),
    );
    assert_eq!(
        got,
        (2_899_328_701_438_880_645, 179),
        "financial perturbed output changed"
    );
}

#[test]
fn financial_two_cycle_output_is_pinned() {
    let mut runs = financial_pin_runs(&["--cycle", "0", "--cycles", "2"]);
    runs.extend(financial_pin_runs(&["--cycle", "1", "--cycles", "2"]));
    let got = pin_tree("fin-cyc", &runs);
    assert_eq!(
        got,
        (6_502_905_751_768_177_657, 181),
        "financial two-cycle output changed"
    );
}

#[test]
fn c360_two_cycle_output_is_pinned() {
    // The c360 pin's argv as two cycles, with the windows the deployer gives
    // two cycles over its default range.
    let windows = [
        ("0", "2024-01-01", "2024-12-31"),
        ("1", "2024-12-31", "2025-12-31"),
    ];
    let runs: Vec<Vec<&str>> = windows
        .iter()
        .map(|(n, start, end)| {
            vec![
                "--schema",
                "customer360",
                "--bucket",
                "b",
                "--seed",
                "43",
                "--target-tb",
                "0.00002",
                "--file-size-mb",
                "4",
                "--threads",
                "2",
                "--timestamp-start",
                start,
                "--timestamp-end",
                end,
                "--cycle",
                n,
                "--cycles",
                "2",
            ]
        })
        .collect();
    let got = pin_tree("c360-cyc", &runs);
    assert_eq!(
        got,
        (9_956_579_246_127_600_639, 10),
        "c360 two-cycle output changed"
    );
}
