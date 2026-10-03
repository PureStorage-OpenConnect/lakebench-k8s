//! The per-node corpus marker, `--print-resolved-args` and `--version`
//! (DAT-3). The marker's `corpus_args` is what the generator applied, so an
//! omitted flag and the same flag at its default hash the same, and every
//! writer setting that changes bytes is in it.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use datagen_rs::heldout::seed_hash;
use serde_json::Value;

const SCALE: &str = "0.005";

fn heldout_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../src/lakebench/spark/data/aml/heldout_hashes.json")
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/fixtures/heldout_test.json")
}

fn tmp(name: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn generate(dir: Option<&Path>, env: &[(&str, &str)], args: &[&str]) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_generate"));
    c.env("LB_HELDOUT_HASHES", heldout_file());
    for k in [
        "DG_COMPRESSION",
        "DG_STATS",
        "DG_DICT",
        "DG_PAGESZ",
        "DG_ROW_GROUP",
    ] {
        c.env_remove(k);
    }
    match dir {
        Some(d) => {
            c.env("DG_LOCAL_DIR", d);
        }
        None => {
            c.env_remove("DG_LOCAL_DIR");
        }
    }
    for (k, v) in env {
        c.env(k, v);
    }
    c.args(args).output().unwrap()
}

fn fin(extra: &[&str]) -> Vec<String> {
    let mut v: Vec<String> = [
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
        "--total-nodes",
        "1",
        "--file-size-mb",
        "1",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    v.extend(extra.iter().map(|s| s.to_string()));
    v
}

fn c360(extra: &[&str]) -> Vec<String> {
    let mut v: Vec<String> = [
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
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    v.extend(extra.iter().map(|s| s.to_string()));
    v
}

fn resolved(env: &[(&str, &str)], args: &[String]) -> Value {
    let mut a: Vec<&str> = args.iter().map(String::as_str).collect();
    a.push("--print-resolved-args");
    let out = generate(None, env, &a);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("one JSON document on stdout")
}

fn sha(env: &[(&str, &str)], args: &[String]) -> String {
    resolved(env, args)["corpus_args_sha256"]
        .as_str()
        .unwrap()
        .to_string()
}

fn markers(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.parent().unwrap().ends_with("_corpus") {
                out.push(p);
            }
        }
    }
    out
}

#[test]
fn print_resolved_args_equals_marker() {
    for (args, schema) in [(fin(&[]), "financial"), (c360(&[]), "customer360")] {
        let dir = tmp(&format!("lb-marker-{schema}"));
        let a: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = generate(Some(&dir), &[], &a);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let ms = markers(&dir);
        assert_eq!(ms.len(), 1, "{schema}: one node, one marker");
        let m: Value = serde_json::from_slice(&std::fs::read(&ms[0]).unwrap()).unwrap();
        let printed = resolved(&[], &args);
        assert_eq!(m["corpus_args"], printed["corpus_args"], "{schema}");
        assert_eq!(m["corpus_args_sha256"], printed["corpus_args_sha256"]);
        assert_eq!(m["schema"], schema);
        assert_eq!(m["format"], 1);
        assert!(m["files_written"].as_u64().unwrap() > 0);
        assert!(m["completed_utc"].as_str().unwrap().ends_with('Z'));
    }
}

#[test]
fn marker_written_last_and_skipped_by_glob() {
    let dir = tmp("lb-marker-last");
    let a: Vec<String> = fin(&["--prefix", "pacs008"]);
    let a: Vec<&str> = a.iter().map(String::as_str).collect();
    assert!(generate(Some(&dir), &[], &a).status.success());
    let ms = markers(&dir);
    assert_eq!(ms.len(), 1);
    // The key is <prefix>/_corpus/c000-node-0000.json: a directory starting
    // with `_`, which Spark's file index never lists as data.
    assert!(
        ms[0].ends_with("b/pacs008/_corpus/c000-node-0000.json"),
        "{:?}",
        ms[0]
    );
    // Written last: no data file is newer than the marker.
    let mtime = |p: &Path| std::fs::metadata(p).unwrap().modified().unwrap();
    let marker_t = mtime(&ms[0]);
    let mut stack = vec![dir.clone()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p != ms[0] {
                assert!(mtime(&p) <= marker_t, "{p:?} written after the marker");
            }
        }
    }
}

#[test]
fn print_resolved_args_writes_nothing() {
    // No DG_LOCAL_DIR and no S3 settings at all: it resolves, prints and
    // exits 0 without opening a client; with DG_LOCAL_DIR nothing is created.
    let _ = resolved(&[], &fin(&[]));
    let dir = tmp("lb-print-nothing");
    let mut a: Vec<String> = fin(&[]);
    a.push("--print-resolved-args".into());
    let a: Vec<&str> = a.iter().map(String::as_str).collect();
    assert!(generate(Some(&dir), &[], &a).status.success());
    assert!(!dir.exists(), "--print-resolved-args wrote output");
}

#[test]
fn print_resolved_args_runs_the_heldout_check_first() {
    let mut a: Vec<String> = fin(&[]);
    a.push("--print-resolved-args".into());
    let a: Vec<&str> = a.iter().map(String::as_str).collect();
    let out = Command::new(env!("CARGO_BIN_EXE_generate"))
        .env_remove("LB_HELDOUT_HASHES")
        .args(&a)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn corpus_args_resolved_not_raw() {
    let base = sha(&[], &fin(&[]));
    assert_eq!(base, sha(&[], &fin(&["--corpus-months", "60"])));
    assert_ne!(base, sha(&[], &fin(&["--corpus-months", "48"])));
    assert_eq!(base, sha(&[], &fin(&["--delivery-mode", "continuous"])));
    assert_ne!(base, sha(&[], &fin(&["--delivery-mode", "batch"])));
    // Thread count and destination are not corpus inputs.
    let mut more_threads = fin(&[]);
    let i = more_threads.iter().position(|a| a == "--threads").unwrap();
    more_threads[i + 1] = "3".into();
    assert_eq!(base, sha(&[], &more_threads));
    assert_eq!(base, sha(&[], &fin(&["--prefix", "elsewhere"])));
    let c = sha(&[], &c360(&[]));
    assert_eq!(c, sha(&[], &c360(&["--duplicate-email-pct", "0.1"])));
    assert_ne!(c, sha(&[], &c360(&["--duplicate-email-pct", "0.2"])));
}

#[test]
fn corpus_args_writer_settings_resolved() {
    let base = sha(&[], &fin(&[]));
    assert_eq!(base, sha(&[("DG_COMPRESSION", "snappy")], &fin(&[])));
    assert_ne!(base, sha(&[("DG_COMPRESSION", "zstd")], &fin(&[])));
    for rg in ["0", "abc"] {
        assert_eq!(base, sha(&[("DG_ROW_GROUP", rg)], &fin(&[])), "{rg}");
    }
    assert_ne!(base, sha(&[("DG_ROW_GROUP", "1000")], &fin(&[])));
    assert_ne!(base, sha(&[("DG_STATS", "page")], &fin(&[])));
    assert_ne!(base, sha(&[("DG_DICT", "0")], &fin(&[])));
    assert_ne!(base, sha(&[("DG_PAGESZ", "4096")], &fin(&[])));
    let w = &resolved(&[("DG_COMPRESSION", "zstd3")], &fin(&[]))["corpus_args"]["writer"];
    assert_eq!(w["compression"], "zstd(3)");
    assert_eq!(w["max_row_group_size"], "default");
}

#[test]
fn seed_ref_matches_python() {
    // Financial markers name the seed by its salted hash under the hash
    // file's salt (the definition tests/heldout.rs ties to Python's
    // datagen_seed.seed_hash); customer 360 by its decimal seed.
    let text = std::fs::read_to_string(fixture()).unwrap();
    let v: Value = serde_json::from_str(&text).unwrap();
    let salt_hex = v["salt"].as_str().unwrap();
    let salt: Vec<u8> = (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(&salt_hex[i..i + 2], 16).unwrap())
        .collect();
    let fixture_path = fixture();
    let r = resolved(
        &[("LB_HELDOUT_HASHES", fixture_path.to_str().unwrap())],
        &fin(&[]),
    );
    assert_eq!(r["corpus_args"]["seed_ref"], seed_hash(&salt, 7777));
    assert_eq!(resolved(&[], &c360(&[]))["corpus_args"]["seed_ref"], "43");
}

#[test]
fn version_flag_prints_model_version() {
    let out = Command::new(env!("CARGO_BIN_EXE_generate"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        s.split_whitespace().take(2).collect::<Vec<_>>(),
        vec!["datagen_rs", datagen_rs::model::MODEL_VERSION]
    );
    assert_eq!(s.split_whitespace().count(), 3, "{s}");
}

#[test]
fn every_corpus_arg_moves_the_hash() {
    // The printed key set is exactly the design's, and changing any
    // included input changes the hash (a key dropped from corpus_args would
    // let two different corpora read as one).
    let fin_keys = [
        "bytes_per_row",
        "corpus_months",
        "cycles",
        "delivery_mode",
        "file_size_mb",
        "mode",
        "model_version",
        "robustness_perturbation",
        "scale",
        "schema",
        "seed_ref",
        "total_nodes",
        "writer",
    ];
    let c360_keys = [
        "customer_id_max",
        "cycles",
        "delivery_mode",
        "dirty_ratio",
        "duplicate_email_pct",
        "file_size_mb",
        "model_version",
        "scale",
        "schema",
        "seed_ref",
        "target_tb",
        "timestamp_end",
        "timestamp_start",
        "total_nodes",
        "writer",
    ];
    for (args, want) in [(fin(&[]), &fin_keys[..]), (c360(&[]), &c360_keys[..])] {
        let r = resolved(&[], &args);
        let mut got: Vec<&str> = r["corpus_args"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        got.sort();
        assert_eq!(got, want);
    }
    let base = sha(&[], &fin(&[]));
    let swap = |args: Vec<String>, flag: &str, value: &str| -> Vec<String> {
        let mut a = args;
        if let Some(i) = a.iter().position(|x| x == flag) {
            a[i + 1] = value.to_string();
        } else {
            a.push(flag.to_string());
            a.push(value.to_string());
        }
        a
    };
    for (flag, value) in [
        ("--seed", "7778"),
        ("--scale", "0.006"),
        ("--corpus-months", "48"),
        ("--file-size-mb", "2"),
        ("--bytes-per-row", "400"),
        ("--total-nodes", "2"),
        ("--mode", "bronze"),
        ("--cycles", "2"),
    ] {
        assert_ne!(base, sha(&[], &swap(fin(&[]), flag, value)), "{flag}");
    }
    assert_ne!(base, sha(&[], &fin(&["--robustness-perturbation"])));
    let cbase = sha(&[], &c360(&[]));
    for (flag, value) in [
        ("--seed", "44"),
        ("--target-tb", "0.00003"),
        ("--customer-id-max", "250000"),
        ("--scale", "2"),
        ("--dirty-ratio", "0.1"),
        ("--timestamp-start", "2024-02-01"),
        ("--timestamp-end", "2024-12-01"),
        ("--total-nodes", "2"),
        ("--cycles", "2"),
    ] {
        assert_ne!(cbase, sha(&[], &swap(c360(&[]), flag, value)), "{flag}");
    }
}

#[test]
fn non_finite_floats_are_refused() {
    for (args, flag) in [
        (fin(&[]), "--scale"),
        (c360(&[]), "--dirty-ratio"),
        (c360(&[]), "--duplicate-email-pct"),
    ] {
        for bad in ["nan", "inf", "-inf"] {
            let mut a = args.clone();
            if let Some(i) = a.iter().position(|x| x == flag) {
                a[i + 1] = bad.to_string();
            } else {
                a.push(flag.to_string());
                a.push(bad.to_string());
            }
            a.push("--print-resolved-args".to_string());
            let a: Vec<&str> = a.iter().map(String::as_str).collect();
            let out = generate(None, &[], &a);
            assert_eq!(out.status.code(), Some(2), "{flag} {bad}");
        }
    }
}

#[test]
fn print_resolved_args_refuses_a_heldout_seed_misused() {
    // The registered robustness seed (test fixture) without the perturbation
    // is refused before anything is printed.
    let fixture_path = fixture();
    let a = fin(&[]);
    let mut a: Vec<String> = a
        .into_iter()
        .map(|x| {
            if x == "7777" {
                "4695594915748112205".to_string()
            } else {
                x
            }
        })
        .collect();
    a.push("--print-resolved-args".into());
    let a: Vec<&str> = a.iter().map(String::as_str).collect();
    let out = generate(
        None,
        &[("LB_HELDOUT_HASHES", fixture_path.to_str().unwrap())],
        &a,
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
}

#[test]
fn version_as_a_value_is_not_the_version_flag() {
    let mut a: Vec<String> = fin(&["--prefix", "--version"]);
    a.push("--print-resolved-args".into());
    let a: Vec<&str> = a.iter().map(String::as_str).collect();
    let out = generate(None, &[], &a);
    assert!(out.status.success());
    let v: Value = serde_json::from_slice(&out.stdout).expect("the resolved args, not a version");
    assert!(v.get("corpus_args").is_some());
}

#[test]
fn reference_only_pod_writes_no_marker() {
    let dir = tmp("lb-marker-ref");
    let mut a = fin(&[]);
    let i = a.iter().position(|x| x == "--mode").unwrap();
    a[i + 1] = "reference".into();
    let a: Vec<&str> = a.iter().map(String::as_str).collect();
    assert!(generate(Some(&dir), &[], &a).status.success());
    assert!(markers(&dir).is_empty());
}

#[test]
fn print_resolved_args_comes_after_every_check() {
    // A C360 cycle whose row ids overflow is refused by the run; the print
    // must refuse it too rather than hash arguments the run would not take.
    let mut a = c360(&["--cycle", "8388000", "--cycles", "8388607"]);
    a.push("--print-resolved-args".into());
    let a: Vec<&str> = a.iter().map(String::as_str).collect();
    let out = generate(None, &[], &a);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
}
