//! The per-node corpus marker and the resolved corpus arguments.
//!
//! After a node's last file completes, the generator writes
//! `<prefix>/_corpus/c{cycle:03}-node-{node:04}.json`, so its presence means
//! the node finished. `corpus_args` is what the generator actually applied
//! (defaults, resolved values and the parquet writer settings), not what its
//! argv said, so an omitted flag and a flag passed at its default give the
//! same `corpus_args_sha256`. Destination, transport, credentials, thread
//! counts and metrics settings are not corpus inputs and are left out.
//! `--print-resolved-args` prints the same object without writing anything.
//! Paths starting with `_` are skipped by Spark's file index, so the marker
//! never reads as data.

use ring::digest::{digest, SHA256};
use serde_json::Value;

/// Marker format version.
pub const MARKER_FORMAT: u64 = 1;
/// Marker directory under the datagen prefix.
pub const MARKER_DIR: &str = "_corpus";

/// The commit the binary was built from (`LB_BUILD_COMMIT` at build time).
pub fn build_commit() -> &'static str {
    option_env!("LB_BUILD_COMMIT").unwrap_or("unknown")
}

/// Canonical JSON: object keys sorted at every level, no whitespace, UTF-8,
/// floats in their shortest round-trip form.
pub fn canonical_json(v: &Value) -> String {
    let mut out = String::new();
    write_canonical(v, &mut out);
    out
}

fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).expect("string serializes"));
                out.push(':');
                write_canonical(&map[*k], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, x) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(x, out);
            }
            out.push(']');
        }
        other => out.push_str(&serde_json::to_string(other).expect("scalar serializes")),
    }
}

/// Lowercase hex SHA-256 of `text`.
pub fn sha256_hex(text: &str) -> String {
    digest(&SHA256, text.as_bytes())
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// sha256 over the canonical JSON of `corpus_args`.
pub fn corpus_args_sha256(args: &Value) -> String {
    sha256_hex(&canonical_json(args))
}

/// What `--print-resolved-args` prints: `{"corpus_args", "corpus_args_sha256"}`.
pub fn resolved_args_document(args: &Value) -> String {
    canonical_json(&serde_json::json!({
        "corpus_args": args,
        "corpus_args_sha256": corpus_args_sha256(args),
    }))
}

/// The marker's key under the sink prefix.
pub fn marker_key(cycle: u64, node_id: i64) -> String {
    format!("{MARKER_DIR}/c{cycle:03}-node-{node_id:04}.json")
}

/// What one node wrote.
#[derive(Clone, Copy, Debug, Default)]
pub struct NodeTotals {
    pub files_written: u64,
    pub rows_written: u64,
    pub bytes_written: u64,
}

/// The per-node marker document (canonical JSON).
pub fn marker_json(
    args: &Value,
    cycle: u64,
    node_id: i64,
    totals: NodeTotals,
    completed_utc: &str,
) -> String {
    let get = |k: &str| args.get(k).cloned().unwrap_or(Value::Null);
    canonical_json(&serde_json::json!({
        "format": MARKER_FORMAT,
        "schema": get("schema"),
        "model_version": get("model_version"),
        "build_commit": build_commit(),
        "cycle": cycle,
        "cycles": get("cycles"),
        "node_id": node_id,
        "total_nodes": get("total_nodes"),
        "delivery_mode": get("delivery_mode"),
        "files_written": totals.files_written,
        "rows_written": totals.rows_written,
        "bytes_written": totals.bytes_written,
        "seed_ref": get("seed_ref"),
        "corpus_args": args,
        "corpus_args_sha256": corpus_args_sha256(args),
        "completed_utc": completed_utc,
    }))
}

/// `YYYY-MM-DDTHH:MM:SSZ` for a UNIX time in seconds.
pub fn utc_rfc3339(unix_s: i64) -> String {
    let days = unix_s.div_euclid(86_400);
    let secs = unix_s.rem_euclid(86_400);
    // civil_from_days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        secs % 3600 / 60,
        secs % 60
    )
}

/// Now, as `utc_rfc3339`.
pub fn utc_now() -> String {
    let s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    utc_rfc3339(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_json_sorts_and_compacts() {
        let v = serde_json::json!({"b": 1, "a": {"d": [1.5, "x"], "c": true}, "e": 0.1});
        assert_eq!(
            canonical_json(&v),
            r#"{"a":{"c":true,"d":[1.5,"x"]},"b":1,"e":0.1}"#
        );
    }

    #[test]
    fn utc_formatting() {
        assert_eq!(utc_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc_rfc3339(1_790_812_824), "2026-10-01T00:00:24Z");
        assert_eq!(utc_rfc3339(951_782_400), "2000-02-29T00:00:00Z");
    }

    #[test]
    fn marker_key_shape() {
        assert_eq!(marker_key(0, 3), "_corpus/c000-node-0003.json");
        assert_eq!(marker_key(12, 1234), "_corpus/c012-node-1234.json");
    }
}
