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

/// FNV-1a over every file under `dir` (relative path, then bytes), sorted by
/// relative path string.
fn tree_digest(dir: &Path) -> (u64, usize) {
    let mut paths = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                // The per-node marker records build and time, so it is
                // excluded by path.
                if p.file_name().is_some_and(|n| n == "_corpus") {
                    continue;
                }
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

fn strict_run(args: &[&str]) -> std::process::Output {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-strict");
    let _ = std::fs::remove_dir_all(&dir);
    generate_cmd()
        .env("DG_LOCAL_DIR", &dir)
        .args(["--bucket", "b", "--mode", "reference"])
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn rust_bad_node_id_exits_2() {
    // Present but unparseable, missing its value, repeated, unknown,
    // positional, and a bare flag given a value: each exits 2 naming the
    // flag (a stray value is never printed) instead of a silent default.
    let ok = ["--seed", "7777", "--scale", SCALE, "--total-nodes", "1"];
    for (bad, names) in [
        (vec!["--node-id", "abc"], "--node-id"),
        (vec!["--corpus-months", "60x"], "--corpus-months"),
        (vec!["--file-size-mb", "1.5"], "--file-size-mb"),
        (vec!["--threads", "-2"], "--threads"),
        (vec!["--node-id"], "--node-id needs a value"),
        (vec!["--node-id", "0", "--node-id", "0"], "more than once"),
        (vec!["--nope", "1"], "--nope"),
        (vec!["--payload-kb", "2"], "--payload-kb"),
        (vec!["--target-tb", "0.1"], "--target-tb"),
        (vec!["4321"], "unexpected argument"),
    ] {
        let out = strict_run(&[&ok[..], &bad[..]].concat());
        assert_eq!(out.status.code(), Some(2), "{bad:?} was accepted");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains(names), "{bad:?}: {err}");
        assert!(!err.contains("4321"), "a stray value was echoed");
    }
    // Missing values for flags that are otherwise required.
    let out = strict_run(&["--seed", "7777", "--scale", SCALE, "--total-nodes"]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--total-nodes needs a value"));
    // The same run with valid flags works, and logs its delivery mode.
    let out = strict_run(&ok);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("delivery_mode=continuous"));
    assert!(
        err.contains("\"delivery_mode\":\"continuous\""),
        "delivery_mode not in metrics"
    );
}

#[test]
fn a_value_is_never_read_as_a_flag() {
    // `--prefix --scale=5` gives --prefix the value "--scale=5"; scale stays
    // the one given by --scale.
    let out = strict_run(&[
        "--seed",
        "7777",
        "--scale",
        "1",
        "--total-nodes",
        "1",
        "--prefix",
        "--scale=5",
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("\"scale\":1.000000"), "{err}");
}

#[test]
fn s3_transport_env_is_read_and_checked() {
    // Without DG_LOCAL_DIR the S3 sink reads S3_PATH_STYLE, S3_VERIFY_SSL and
    // S3_CA_CERT; a value it cannot use exits 2 naming the variable.
    for (var, val) in [
        ("S3_PATH_STYLE", "maybe"),
        ("S3_VERIFY_SSL", "off"),
        ("S3_CA_CERT", "/nonexistent/ca.pem"),
    ] {
        let out = generate_cmd()
            .env_remove("DG_LOCAL_DIR")
            .env("S3_ENDPOINT", "http://127.0.0.1:1")
            .env("AWS_ACCESS_KEY_ID", "k")
            .env("AWS_SECRET_ACCESS_KEY", "s")
            .env(var, val)
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
                "1",
            ])
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2), "{var}={val}");
        assert!(String::from_utf8_lossy(&out.stderr).contains(var), "{var}");
    }
}

#[test]
fn c360_refuses_financial_flags() {
    let out = generate_cmd()
        .env(
            "DG_LOCAL_DIR",
            PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-strict-c360"),
        )
        .args([
            "--schema",
            "customer360",
            "--bucket",
            "b",
            "--seed",
            "42",
            "--target-tb",
            "0.00002",
            "--corpus-months",
            "60",
        ])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("--corpus-months"));
}

#[test]
fn financial_scale_defaults_to_one() {
    let out = strict_run(&["--seed", "7777", "--total-nodes", "1"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    assert!(
        text.contains("\"scale\":1.000000"),
        "default scale is not 1.0"
    );
}

// ---------------------------------------------------------------------------
// Continuous c360 delivery (--deliver-until): silent-data
// invariants only. Argument refusals, log lines, timing and stop-marker
// behaviour are verified once per change, not here (they fail loud).
// ---------------------------------------------------------------------------

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// 5 files of 4 MB over 2024.
fn c360_continuous(dir: &Path, node: &str, until: i64, seed: &str) -> Command {
    let mut c = generate_cmd();
    c.env("DG_LOCAL_DIR", dir).args([
        "--schema",
        "customer360",
        "--bucket",
        "b",
        "--seed",
        seed,
        "--target-tb",
        "0.00002",
        "--file-size-mb",
        "4",
        "--threads",
        "2",
        "--total-nodes",
        "2",
        "--node-id",
        node,
        "--deliver-until",
        &until.to_string(),
    ]);
    c
}

type EpochFiles = BTreeMap<(u64, i64), (Vec<i64>, i64, i64, std::time::SystemTime)>;

fn epoch_files(dir: &Path) -> EpochFiles {
    use arrow::array::{Int64Array, TimestampMicrosecondArray};
    let mut out = BTreeMap::new();
    for p in c360_files(dir) {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let key = datagen_rs::cycle::parse_epoch_part_key(&name).expect("an epoch key");
        let (mut ids, mut lo, mut hi) = (Vec::new(), i64::MAX, i64::MIN);
        for b in batches(&p) {
            let id = b.column_by_name("row_id").unwrap();
            let id = id.as_any().downcast_ref::<Int64Array>().unwrap();
            let ts = b.column_by_name("event_timestamp").unwrap();
            let ts = ts
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                ids.push(id.value(i));
                lo = lo.min(ts.value(i));
                hi = hi.max(ts.value(i));
            }
        }
        let mtime = std::fs::metadata(&p).unwrap().modified().unwrap();
        out.insert(key, (ids, lo, hi, mtime));
    }
    out
}

fn node_count(files: &EpochFiles, node: i64) -> usize {
    files.keys().filter(|(_, f)| f % 2 == node).count()
}

#[test]
fn continuous_c360_silent_data_invariants() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-c360-cont-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // Both pods at once, as on a cluster.
    let until = unix_now() + 3;
    let pods: Vec<_> = ["0", "1"]
        .map(|n| {
            c360_continuous(&dir, n, until, "42")
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .into_iter()
        .collect();
    for p in pods {
        let out = p.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let files = epoch_files(&dir);

    // Invariant 1: row ids are disjoint across every file (duplicates would
    // break joins and recall silently).
    let mut seen = BTreeSet::new();
    for (k, (ids, ..)) in &files {
        for id in ids {
            assert!(seen.insert(*id), "row id {id} repeats at {k:?}");
        }
    }

    // Invariant 2: every file lies inside its round's time slice (silent
    // wrong-time rows would land in the wrong gold aggregation).
    // round r = (e * 5 + fid) / 2, slice = 2/5 of 2024.
    let (start, end) = (1_704_067_200_000_000i64, 1_735_689_600_000_000i64);
    let slice = (end - start) * 2 / 5;
    for (&(e, fid), (_, lo, hi, _)) in &files {
        let s_lo = start + (e as i64 * 5 + fid) / 2 * slice;
        assert!(
            *lo >= s_lo && *hi < s_lo + slice,
            "({e}, {fid}) [{lo},{hi}] out of [{s_lo},{})",
            s_lo + slice
        );
    }

    // Invariant 3: each node's indices are contiguous (a silent gap would
    // leave the gold stage short rows with no error).
    for node in [0, 1] {
        let own: Vec<i64> = (0..5).filter(|f| f % 2 == node).collect();
        for j in 0..node_count(&files, node) {
            let key = ((j / own.len()) as u64, own[j % own.len()]);
            assert!(
                files.contains_key(&key),
                "node {node} misses index {j} {key:?}"
            );
        }
    }

    // Invariant 4: a restart rewrites nothing (silent id and time collisions
    // if it did).
    let n0 = node_count(&files, 0);
    let out = c360_continuous(&dir, "0", unix_now() + 2, "42")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let again = epoch_files(&dir);
    for (k, v) in files.iter().filter(|((_, f), _)| f % 2 == 0) {
        assert_eq!(again[k].3, v.3, "restart rewrote {k:?}");
    }
    assert!(node_count(&again, 0) > n0, "restart wrote nothing");

    // Invariant 5: a run with another configuration is refused on resume
    // (a mixed corpus would be labelled with the new corpus_args).
    let out = c360_continuous(&dir, "0", unix_now() + 5, "7")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Continuous AML delivery: silent-data invariants only (refusals, timing and the
// stop marker fail loud and are checked once per change).
// ---------------------------------------------------------------------------

fn aml_continuous(dir: &Path, node: &str, until: i64) -> Command {
    let mut c = generate_cmd();
    c.env("DG_LOCAL_DIR", dir).args([
        "--bucket",
        "b",
        "--seed",
        "7777",
        "--scale",
        "0.02",
        "--corpus-months",
        "24",
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
        "--deliver-until",
        &until.to_string(),
    ]);
    c
}

/// Epoch of an AML bronze file name: 0 for the history's part-NNNNNN.
fn aml_epoch(name: &str) -> u64 {
    datagen_rs::cycle::parse_epoch_part_key(name).map_or(0, |(e, _)| e)
}

#[test]
fn continuous_aml_silent_data_invariants() {
    use arrow::array::{ListArray, StringArray, TimestampMicrosecondArray};
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("lb-aml-cont-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let until = unix_now() + 6;
    let pods: Vec<_> = ["0", "1"]
        .map(|n| {
            aml_continuous(&dir, n, until)
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .into_iter()
        .collect();
    for p in pods {
        let out = p.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // Bronze: UETRs per epoch, and each epoch's time range.
    let bronze = files(&dir, "bronze/pacs008", "part-");
    let mut uetrs: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    let mut span: BTreeMap<u64, (i64, i64)> = BTreeMap::new();
    let mut all = BTreeSet::new();
    for p in &bronze {
        let e = aml_epoch(&p.file_name().unwrap().to_string_lossy());
        for b in batches(p) {
            let ts = b.column_by_name("cre_dt_tm").unwrap();
            let ts = ts
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let s = span.entry(e).or_insert((i64::MAX, i64::MIN));
            for i in 0..b.num_rows() {
                s.0 = s.0.min(ts.value(i));
                s.1 = s.1.max(ts.value(i));
            }
        }
        for u in uetr_col(std::slice::from_ref(p)) {
            // Invariant 1: no UETR repeats, across files or epochs.
            assert!(all.insert(u.clone()), "UETR {u} repeats (epoch {e})");
            uetrs.entry(e).or_default().insert(u);
        }
    }
    let complete: Vec<u64> = (1..)
        .take_while(|e| {
            (0..2).all(|n| {
                dir.join(format!("b/_corpus/e{e:04}-node-{n:04}.json"))
                    .exists()
            })
        })
        .collect();
    assert!(
        complete.len() >= 2,
        "only {} live epochs completed",
        complete.len()
    );

    // Invariant 2: event time advances: each epoch lies after the previous.
    for (e, (lo, _)) in span.iter().skip(1) {
        assert!(*lo > span[&(e - 1)].1, "epoch {e} overlaps epoch {}", e - 1);
    }

    // Invariants 3 and 4: each complete epoch's manifest names exactly
    // planted rows of that epoch's bronze, and typology ids never repeat.
    let mut ids = BTreeSet::new();
    for e in std::iter::once(0).chain(complete.iter().copied()) {
        let name = if e == 0 {
            "manifest.parquet".into()
        } else {
            format!("manifest-e{e:04}.parquet")
        };
        let mut planted = 0;
        for b in batches(&dir.join("b/manifest").join(name)) {
            let tid = b.column_by_name("typology_id").unwrap();
            let tid = tid.as_any().downcast_ref::<StringArray>().unwrap();
            let pu = b.column_by_name("participant_uetrs").unwrap();
            let pu = pu.as_any().downcast_ref::<ListArray>().unwrap();
            for i in 0..b.num_rows() {
                assert!(
                    ids.insert(tid.value(i).to_string()),
                    "typology id {} repeats",
                    tid.value(i)
                );
                let v = pu.value(i);
                let v = v.as_any().downcast_ref::<StringArray>().unwrap();
                for j in 0..v.len() {
                    assert!(
                        uetrs[&e].contains(v.value(j)),
                        "epoch {e} manifest UETR not in its bronze"
                    );
                    planted += 1;
                }
            }
        }
        assert!(planted > 0, "epoch {e} plants nothing");
    }

    // Invariant 5: a restart rewrites nothing and continues.
    let mtimes = |d: &Path| -> BTreeMap<PathBuf, std::time::SystemTime> {
        files(d, "bronze/pacs008", "part-")
            .into_iter()
            .map(|p| {
                (
                    p.clone(),
                    std::fs::metadata(&p).unwrap().modified().unwrap(),
                )
            })
            .collect()
    };
    let before = mtimes(&dir);
    let out = aml_continuous(&dir, "0", unix_now() + 3).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let after = mtimes(&dir);
    for (p, t) in &before {
        assert_eq!(after[p], *t, "restart rewrote {}", p.display());
    }
    assert!(after.len() > before.len(), "restart wrote nothing");
    let _ = std::fs::remove_dir_all(&dir);
}
