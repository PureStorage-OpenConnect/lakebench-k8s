//! The held-out hash file reader (heldout.rs) and the generator's use of it.
//! Every seed here is test-only (tests/fixtures/heldout_test_seeds.py).

use std::path::PathBuf;
use std::process::Command;

use datagen_rs::heldout::{seed_hash, HeldOut, Role, FLOOR, FLOOR_SALT};

const TEST_EVALUATION_SEED: i64 = 8_763_195_430_032_412_900;
const TEST_ROBUSTNESS_SEED: i64 = 4_695_594_915_748_112_205;
const TEST_SPENT_SEED: i64 = 5_783_979_690_583_702_767;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/fixtures/heldout_test.json")
}

fn fixture_text() -> String {
    std::fs::read_to_string(fixture_path()).unwrap()
}

fn doc(salt: &str, ev: &[String], rb: &[String], spent: &str) -> String {
    let q = |v: &[String]| {
        v.iter()
            .map(|h| format!("{h:?}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        r#"{{"format":1,"algorithm":"sha256(bytes.fromhex(salt) + b':' + decimal(seed))","salt":"{salt}","roles":{{"evaluation":[{}],"robustness":[{}]}},"spent":{spent},"absence_check":"report"}}"#,
        q(ev),
        q(rb)
    )
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn fixture_roles_and_spent() {
    let h = HeldOut::load(fixture_path().to_str().unwrap()).unwrap();
    assert_eq!(h.role_of(TEST_EVALUATION_SEED), Ok(Some(Role::Evaluation)));
    assert_eq!(h.role_of(TEST_ROBUSTNESS_SEED), Ok(Some(Role::Robustness)));
    assert_eq!(h.role_of(43), Ok(None));
    assert!(h.is_spent(TEST_SPENT_SEED) && h.is_spent(42) && !h.is_spent(43));
}

#[test]
fn floor_refuses_with_empty_file() {
    // A file with both role lists emptied still protects the floor's seeds,
    // checked under the floor's own salt.
    let floor_salt = "34".repeat(32);
    let floor = vec![
        (
            Role::Evaluation,
            seed_hash(&unhex(&floor_salt), TEST_EVALUATION_SEED),
        ),
        (
            Role::Robustness,
            seed_hash(&unhex(&floor_salt), TEST_ROBUSTNESS_SEED),
        ),
    ];
    let text = doc(&"56".repeat(32), &[], &[], "[]");
    let h = HeldOut::from_json_with_floor(&text, &floor_salt, floor).unwrap();
    assert_eq!(h.role_of(TEST_EVALUATION_SEED), Ok(Some(Role::Evaluation)));
    assert_eq!(h.role_of(TEST_ROBUSTNESS_SEED), Ok(Some(Role::Robustness)));
    assert_eq!(h.role_of(43), Ok(None));
}

#[test]
fn resalted_file_cannot_move_a_floor_seed() {
    let floor_salt = "34".repeat(32);
    let floor = vec![
        (
            Role::Evaluation,
            seed_hash(&unhex(&floor_salt), TEST_EVALUATION_SEED),
        ),
        (
            Role::Robustness,
            seed_hash(&unhex(&floor_salt), TEST_ROBUSTNESS_SEED),
        ),
    ];
    let salt = "56".repeat(32);
    let swapped = seed_hash(&unhex(&salt), TEST_ROBUSTNESS_SEED);
    let text = doc(&salt, &[swapped], &[], "[]");
    let h = HeldOut::from_json_with_floor(&text, &floor_salt, floor).unwrap();
    let e = h.role_of(TEST_ROBUSTNESS_SEED).unwrap_err();
    assert!(e.contains("different roles"));
    assert!(!e.contains(&TEST_ROBUSTNESS_SEED.to_string()));
}

#[test]
fn malformed_files_are_refused() {
    let good = fixture_text();
    assert!(HeldOut::from_json(&good).is_ok());
    for (from, to) in [
        ("\"format\": 1", "\"format\": 2"),
        (
            "\"absence_check\": \"report\"",
            "\"absence_check\": \"off\"",
        ),
        ("\"spent\": [", "\"spent\": [-1, "),
        ("\"roles\": {", "\"roles\": {\"calibration\": [], "),
        ("\"format\": 1", "\"extra\": 1, \"format\": 1"),
    ] {
        assert!(good.contains(from), "fixture lacks {from}");
        let bad = good.replacen(from, to, 1);
        assert!(HeldOut::from_json(&bad).is_err(), "accepted {to}");
    }
    assert!(HeldOut::from_json("not json").is_err());
    let short = doc(&"ab".repeat(32), &["00".to_string()], &[], "[]");
    assert!(HeldOut::from_json(&short).is_err());
}

#[test]
fn seed_hash_matches_the_python_definition() {
    // The fixture's role hashes were written by datagen_seed.seed_hash; the
    // Rust hash of the same seed under the same salt must equal them.
    let v: serde_json::Value = serde_json::from_str(&fixture_text()).unwrap();
    let salt = unhex(v["salt"].as_str().unwrap());
    assert_eq!(
        seed_hash(&salt, TEST_EVALUATION_SEED),
        v["roles"]["evaluation"][0].as_str().unwrap()
    );
}

#[test]
fn compiled_floor_is_initialised() {
    assert_eq!(FLOOR_SALT.len(), 64);
    for r in Role::ALL {
        assert!(FLOOR.iter().any(|(f, h)| *f == r && h.len() == 64));
    }
}

#[test]
fn compiled_floor_is_the_tracked_file() {
    // Every role hash of the tracked hash file, in order, under the file's
    // salt, is compiled in (SPEC section 10), and the production floor loads
    // with the production file.
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../src/lakebench/spark/data/aml/heldout_hashes.json");
    let text = std::fs::read_to_string(&path).unwrap();
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["salt"].as_str(), Some(FLOOR_SALT));
    for r in Role::ALL {
        let file: Vec<&str> = v["roles"][r.name()]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h.as_str().unwrap())
            .collect();
        let floor: Vec<&str> = FLOOR
            .iter()
            .filter(|(f, _)| *f == r)
            .map(|(_, h)| *h)
            .collect();
        assert_eq!(floor, file, "{}", r.name());
    }
    assert!(HeldOut::from_json(&text).is_ok());
}

fn generate(env: Option<&str>) -> std::process::Output {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-heldout-gen");
    let mut c = Command::new(env!("CARGO_BIN_EXE_generate"));
    c.env("DG_LOCAL_DIR", &dir).env_remove("LB_HELDOUT_HASHES");
    if let Some(p) = env {
        c.env("LB_HELDOUT_HASHES", p);
    }
    c.args([
        "--bucket",
        "b",
        "--seed",
        "7777",
        "--scale",
        "0.005",
        "--mode",
        "reference",
        "--total-nodes",
        "1",
    ])
    .output()
    .unwrap()
}

#[test]
fn missing_file_exits_2() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-heldout-gen");
    for env in [None, Some("/nonexistent/heldout.json")] {
        let _ = std::fs::remove_dir_all(&dir);
        let out = generate(env);
        assert_eq!(out.status.code(), Some(2), "{env:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("LB_HELDOUT_HASHES"));
        assert!(!dir.exists(), "wrote output before refusing");
    }
    let bad = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-heldout-bad.json");
    std::fs::write(&bad, "{}").unwrap();
    assert_eq!(generate(Some(bad.to_str().unwrap())).status.code(), Some(2));
    assert!(generate(Some(fixture_path().to_str().unwrap()))
        .status
        .success());
}

#[test]
fn a_spent_seed_in_the_file_is_refused() {
    let out = Command::new(env!("CARGO_BIN_EXE_generate"))
        .env(
            "DG_LOCAL_DIR",
            PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-heldout-spent"),
        )
        .env("LB_HELDOUT_HASHES", fixture_path())
        .args([
            "--bucket",
            "b",
            "--scale",
            "0.005",
            "--mode",
            "reference",
            "--total-nodes",
            "1",
        ])
        .args(["--seed", &TEST_SPENT_SEED.to_string()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("spent"));
    assert!(
        !err.contains(&TEST_SPENT_SEED.to_string()),
        "the spent seed was echoed"
    );
}

#[test]
fn customer360_runs_without_the_file() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("lb-heldout-c360");
    let _ = std::fs::remove_dir_all(&dir);
    let out = Command::new(env!("CARGO_BIN_EXE_generate"))
        .env("DG_LOCAL_DIR", &dir)
        .env_remove("LB_HELDOUT_HASHES")
        .args([
            "--schema",
            "customer360",
            "--bucket",
            "b",
            "--seed",
            "42",
            "--target-tb",
            "0.00002",
            "--file-size-mb",
            "4",
            "--threads",
            "2",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn env_names_match_the_literals_the_generator_reads() {
    // generate.rs reads both variables by literal name (so a source scan sees
    // every environment read); the constants must name the same variables.
    assert_eq!(datagen_rs::heldout::ENV, "LB_HELDOUT_HASHES");
    let src = include_str!("../src/bin/generate.rs");
    assert!(src.contains("std::env::var(\"LB_HELDOUT_HASHES\")"));
    assert!(src.contains("const SEED_ENV: &str = \"LB_DATAGEN_SEED\";"));
    assert!(src.contains("std::env::var(\"LB_DATAGEN_SEED\")"));
}
