//! Multi-cycle AML generation (WORKPLAN B4): the union of `--cycles N` runs is
//! the one-shot corpus, row for row, and the manifest union is the one-shot
//! manifest. Runs the real binary against a local sink (DG_LOCAL_DIR).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const SCALE: &str = "0.005";

fn run(dir: &Path, extra: &[&str]) {
    let out = Command::new(env!("CARGO_BIN_EXE_generate"))
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
    let tmp = std::env::temp_dir().join(format!("lb-cycles-{}", std::process::id()));
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
        let st = Command::new(env!("CARGO_BIN_EXE_generate"))
            .env("DG_LOCAL_DIR", std::env::temp_dir().join("lb-cycles-bad"))
            .args(["--bucket", "b", "--seed", "7777", "--scale", SCALE])
            .args(&bad)
            .output()
            .unwrap();
        assert_eq!(st.status.code(), Some(2), "{bad:?} was accepted");
    }
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
        let st = Command::new(env!("CARGO_BIN_EXE_generate"))
            .env("DG_LOCAL_DIR", std::env::temp_dir().join("lb-seed-bad"))
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
