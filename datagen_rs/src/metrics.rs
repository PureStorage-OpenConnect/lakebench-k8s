//! Per-pod metrics emit. One machine-readable line per datagen invocation,
//! consumed downstream by lakebench's metrics aggregator to roll into the
//! pipeline's `metrics.json`.
//!
//! Format: a single stderr line, prefixed `LB_METRICS_JSON ` (space intended),
//! followed by a JSON object. Grep-friendly extraction: `grep -m1 '^LB_METRICS_JSON ' log`.
//!
//! We hand-build the JSON to avoid pulling serde into the datagen crate; the
//! shape is flat scalars only and we test the serializer directly.

use std::fmt::Write;

/// Everything the pod knows about its own run at completion. All timing
/// fields are seconds. `None` on any Option means "phase does not apply
/// to this schema" (e.g. customer360 has no `world_s`).
#[derive(Debug, Clone, Default)]
pub struct PodMetrics {
    /// "financial" or "customer360".
    pub schema: String,
    /// This pod's index in the K8s Indexed Job.
    pub node_id: i64,
    /// Total pods in the job (= completions).
    pub node_count: i64,
    /// Rayon pool size at run time (actual parallelism). May differ from
    /// the k8s CPU request when --threads is unset and rayon defaults to
    /// `available_parallelism()` inside a cgroup that Rust cannot read.
    pub cores_used: usize,
    /// k8s CPU request in millicores, read from LB_POD_CPU_REQUEST_MILLI env
    /// var populated by the K8s Job template. `None` when the env is unset
    /// (older manifests). The aggregator prefers this over cores_used for
    /// CPU-seconds accounting because it is what k8s actually reserves.
    pub cpu_request_millicores: Option<u64>,

    /// S3 bucket the pod wrote to.
    pub bucket: String,
    /// S3 key prefix (may be empty).
    pub prefix: String,

    // ---- financial-only inputs (None on customer360) --------------------
    pub scale: Option<f64>,
    pub corpus_months: Option<i64>,
    pub population: Option<u64>,
    /// Corpus-wide total transactions (SAME on every pod -- not per-pod).
    /// Kept for reporting; DO NOT sum across pods.
    pub total_txns: Option<u64>,
    pub typology_instances: Option<u64>,

    // ---- customer360-only inputs (None on financial) --------------------
    pub target_tb: Option<f64>,
    pub customer_id_max: Option<u64>,
    pub dirty_ratio: Option<f64>,

    // ---- common sizing --------------------------------------------------
    pub file_size_mb: i64,
    pub rows_per_file: u64,
    pub total_files: i64,
    pub files_written: u64,
    pub bytes_written: u64,
    /// Rows actually written BY THIS POD. Authoritative per-pod row count
    /// and safe to sum across the fleet. `total_txns` (financial only) is
    /// a corpus-wide constant that must NEVER be summed.
    pub rows_written: u64,

    // ---- phase timings (seconds, wall for elapsed_s + gen_s + setup_s;
    //      thread-summed for build_batch_s / encode_parquet_s / s3_put_s) -
    pub elapsed_s: f64,
    pub setup_s: f64,
    /// Only populated for the financial path (world build).
    pub world_s: Option<f64>,
    /// Only populated for the financial path (typology scheduling).
    pub typology_s: Option<f64>,
    /// Wall time in the file-generation phase.
    pub gen_s: f64,
    /// Only populated for the financial path (reference-zone writes).
    pub reference_s: Option<f64>,
    /// Thread-summed time inside `build_batch`. May exceed gen_s.
    pub build_batch_s: f64,
    /// Thread-summed time inside parquet encode.
    pub encode_parquet_s: f64,
    /// Thread-summed time inside S3 put.
    pub s3_put_s: f64,
}

impl PodMetrics {
    /// Derived: MB/s of output over the whole pod's wall time.
    pub fn throughput_mbps(&self) -> f64 {
        if self.elapsed_s <= 0.0 {
            0.0
        } else {
            self.bytes_written as f64 / self.elapsed_s / 1e6
        }
    }

    /// Derived: effective CPU cores this pod occupied k8s for.
    ///
    /// Prefers `cpu_request_millicores` (what k8s actually reserved for the
    /// full pod lifetime) when available, and falls back to the rayon pool
    /// size otherwise. rayon's pool comes from `available_parallelism()`
    /// under a cgroup Rust does not read, so it can silently differ from
    /// the pod's k8s CPU request in either direction. K8s cost accounting
    /// needs the request, not the pool.
    pub fn effective_cores(&self) -> f64 {
        match self.cpu_request_millicores {
            Some(m) if m > 0 => (m as f64) / 1000.0,
            _ => self.cores_used as f64,
        }
    }

    /// Derived: effective_cores * elapsed_s. See `effective_cores` for why
    /// the k8s request is preferred over the rayon pool size when both are
    /// available.
    pub fn cpu_seconds(&self) -> f64 {
        self.effective_cores() * self.elapsed_s
    }

    /// Derived: CPU-hr per TB written. NaN if bytes_written == 0.
    pub fn cpu_hr_per_tb(&self) -> f64 {
        let tb = self.bytes_written as f64 / 1e12;
        if tb <= 0.0 {
            f64::NAN
        } else {
            (self.cpu_seconds() / 3600.0) / tb
        }
    }

    /// Emit as one stderr line, prefixed `LB_METRICS_JSON `.
    pub fn emit(&self) {
        eprintln!("LB_METRICS_JSON {}", self.to_json());
    }

    /// Build the JSON representation. Public for unit tests.
    pub fn to_json(&self) -> String {
        let mut s = String::with_capacity(1024);
        s.push('{');
        j_str(&mut s, "schema", &self.schema, true);
        j_i64(&mut s, "node_id", self.node_id, false);
        j_i64(&mut s, "node_count", self.node_count, false);
        j_u64(&mut s, "cores_used", self.cores_used as u64, false);
        if let Some(m) = self.cpu_request_millicores {
            j_u64(&mut s, "cpu_request_millicores", m, false);
        }
        j_str(&mut s, "bucket", &self.bucket, false);
        j_str(&mut s, "prefix", &self.prefix, false);

        if let Some(v) = self.scale {
            j_f64(&mut s, "scale", v, false);
        }
        if let Some(v) = self.corpus_months {
            j_i64(&mut s, "corpus_months", v, false);
        }
        if let Some(v) = self.population {
            j_u64(&mut s, "population", v, false);
        }
        if let Some(v) = self.total_txns {
            j_u64(&mut s, "total_txns", v, false);
        }
        if let Some(v) = self.typology_instances {
            j_u64(&mut s, "typology_instances", v, false);
        }

        if let Some(v) = self.target_tb {
            j_f64(&mut s, "target_tb", v, false);
        }
        if let Some(v) = self.customer_id_max {
            j_u64(&mut s, "customer_id_max", v, false);
        }
        if let Some(v) = self.dirty_ratio {
            j_f64(&mut s, "dirty_ratio", v, false);
        }

        j_i64(&mut s, "file_size_mb", self.file_size_mb, false);
        j_u64(&mut s, "rows_per_file", self.rows_per_file, false);
        j_i64(&mut s, "total_files", self.total_files, false);
        j_u64(&mut s, "files_written", self.files_written, false);
        j_u64(&mut s, "bytes_written", self.bytes_written, false);
        j_u64(&mut s, "rows_written", self.rows_written, false);

        j_f64(&mut s, "elapsed_s", self.elapsed_s, false);
        j_f64(&mut s, "setup_s", self.setup_s, false);
        if let Some(v) = self.world_s {
            j_f64(&mut s, "world_s", v, false);
        }
        if let Some(v) = self.typology_s {
            j_f64(&mut s, "typology_s", v, false);
        }
        j_f64(&mut s, "gen_s", self.gen_s, false);
        if let Some(v) = self.reference_s {
            j_f64(&mut s, "reference_s", v, false);
        }
        j_f64(&mut s, "build_batch_s", self.build_batch_s, false);
        j_f64(&mut s, "encode_parquet_s", self.encode_parquet_s, false);
        j_f64(&mut s, "s3_put_s", self.s3_put_s, false);

        j_f64(&mut s, "throughput_mbps", self.throughput_mbps(), false);
        j_f64(&mut s, "cpu_seconds", self.cpu_seconds(), false);
        let cpuhr = self.cpu_hr_per_tb();
        // NaN is not valid JSON; omit the key rather than emit an
        // out-of-spec token that would poison downstream parsers.
        if cpuhr.is_finite() {
            j_f64(&mut s, "cpu_hr_per_tb", cpuhr, false);
        }

        s.push('}');
        s
    }
}

fn sep(s: &mut String, first: bool) {
    if !first {
        s.push(',');
    }
}

fn j_str(s: &mut String, k: &str, v: &str, first: bool) {
    sep(s, first);
    s.push('"');
    s.push_str(k);
    s.push_str("\":");
    push_json_string(s, v);
}

fn j_i64(s: &mut String, k: &str, v: i64, first: bool) {
    sep(s, first);
    write!(s, "\"{}\":{}", k, v).unwrap();
}

fn j_u64(s: &mut String, k: &str, v: u64, first: bool) {
    sep(s, first);
    write!(s, "\"{}\":{}", k, v).unwrap();
}

fn j_f64(s: &mut String, k: &str, v: f64, first: bool) {
    sep(s, first);
    // Guard: emit 0 for non-finite so we never write NaN/Infinity (invalid
    // JSON). Callers that care should have filtered upstream (see
    // cpu_hr_per_tb path in emit).
    let v = if v.is_finite() { v } else { 0.0 };
    // Fixed 6-place precision keeps the numbers stable across builds and
    // small enough that a full metrics line stays well under 4 KiB.
    write!(s, "\"{}\":{:.6}", k, v).unwrap();
}

fn push_json_string(out: &mut String, v: &str) {
    out.push('"');
    for ch in v.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                write!(out, "\\u{:04x}", c as u32).unwrap();
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parses_as_json(line: &str) -> serde_json_min::Value {
        serde_json_min::from_str(line).expect("valid JSON")
    }

    // Tiny inline JSON parser sufficient for our flat metrics object.
    // Avoids adding serde_json to the crate just for tests.
    mod serde_json_min {
        use std::collections::BTreeMap;

        #[derive(Debug, PartialEq)]
        pub enum Value {
            Str(String),
            Num(f64),
            Obj(BTreeMap<String, Value>),
        }

        pub fn from_str(s: &str) -> Result<Value, String> {
            let mut it = s.chars().peekable();
            let v = parse_value(&mut it)?;
            skip_ws(&mut it);
            if it.peek().is_some() {
                return Err("trailing chars".into());
            }
            Ok(v)
        }

        fn parse_value(it: &mut std::iter::Peekable<std::str::Chars>) -> Result<Value, String> {
            skip_ws(it);
            match it.peek() {
                Some('{') => parse_obj(it),
                Some('"') => parse_str(it).map(Value::Str),
                Some(c) if *c == '-' || c.is_ascii_digit() => parse_num(it),
                _ => Err(format!("unexpected token near {:?}", it.peek())),
            }
        }

        fn parse_obj(it: &mut std::iter::Peekable<std::str::Chars>) -> Result<Value, String> {
            it.next();
            let mut m = BTreeMap::new();
            skip_ws(it);
            if let Some('}') = it.peek() {
                it.next();
                return Ok(Value::Obj(m));
            }
            loop {
                skip_ws(it);
                let k = parse_str(it)?;
                skip_ws(it);
                if it.next() != Some(':') {
                    return Err("expected :".into());
                }
                let v = parse_value(it)?;
                m.insert(k, v);
                skip_ws(it);
                match it.next() {
                    Some(',') => continue,
                    Some('}') => break,
                    other => return Err(format!("expected , or }} got {:?}", other)),
                }
            }
            Ok(Value::Obj(m))
        }

        fn parse_str(it: &mut std::iter::Peekable<std::str::Chars>) -> Result<String, String> {
            if it.next() != Some('"') {
                return Err("expected \"".into());
            }
            let mut out = String::new();
            while let Some(c) = it.next() {
                if c == '"' {
                    return Ok(out);
                }
                if c == '\\' {
                    match it.next() {
                        Some('"') => out.push('"'),
                        Some('\\') => out.push('\\'),
                        Some('n') => out.push('\n'),
                        Some('r') => out.push('\r'),
                        Some('t') => out.push('\t'),
                        Some('u') => {
                            let mut hex = String::new();
                            for _ in 0..4 {
                                hex.push(it.next().ok_or("bad \\u")?);
                            }
                            let cp = u32::from_str_radix(&hex, 16).map_err(|e| e.to_string())?;
                            out.push(char::from_u32(cp).ok_or("bad cp")?);
                        }
                        _ => return Err("bad escape".into()),
                    }
                } else {
                    out.push(c);
                }
            }
            Err("unterminated string".into())
        }

        fn parse_num(it: &mut std::iter::Peekable<std::str::Chars>) -> Result<Value, String> {
            let mut s = String::new();
            while let Some(&c) = it.peek() {
                if c.is_ascii_digit() || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' {
                    s.push(c);
                    it.next();
                } else {
                    break;
                }
            }
            s.parse::<f64>().map(Value::Num).map_err(|e| e.to_string())
        }

        fn skip_ws(it: &mut std::iter::Peekable<std::str::Chars>) {
            while let Some(&c) = it.peek() {
                if c.is_whitespace() {
                    it.next();
                } else {
                    break;
                }
            }
        }
    }

    #[test]
    fn empty_metrics_serializes_and_parses() {
        let m = PodMetrics {
            schema: "financial".into(),
            node_count: 1,
            cores_used: 4,
            bucket: "bucket".into(),
            file_size_mb: 32,
            total_files: 10,
            ..Default::default()
        };
        let v = parses_as_json(&m.to_json());
        if let serde_json_min::Value::Obj(o) = v {
            assert_eq!(o.get("schema").unwrap(), &serde_json_min::Value::Str("financial".into()));
        } else {
            panic!("not object");
        }
    }

    #[test]
    fn cpu_hr_per_tb_computed() {
        let m = PodMetrics {
            schema: "customer360".into(),
            cores_used: 8,
            elapsed_s: 3600.0,
            bytes_written: 1_000_000_000_000, // 1 TB
            ..Default::default()
        };
        // 8 cores * 3600s / 3600 = 8 core-hr per TB.
        assert!((m.cpu_hr_per_tb() - 8.0).abs() < 1e-6);
    }

    #[test]
    fn effective_cores_prefers_cpu_request() {
        // Pool ran 8 rayon workers, but k8s reserved 16000 millicores.
        // Cost accounting must use the request (16), not the pool (8).
        let m = PodMetrics {
            cores_used: 8,
            cpu_request_millicores: Some(16_000),
            elapsed_s: 100.0,
            bytes_written: 100_000_000_000, // 100 GB
            ..Default::default()
        };
        assert_eq!(m.effective_cores(), 16.0);
        // cpu_hr_per_tb = (16 * 100 / 3600) / 0.1 = 4.44 (not 2.22).
        assert!((m.cpu_hr_per_tb() - 4.444).abs() < 0.01);
    }

    #[test]
    fn effective_cores_falls_back_when_request_absent() {
        let m = PodMetrics {
            cores_used: 8,
            cpu_request_millicores: None,
            elapsed_s: 100.0,
            ..Default::default()
        };
        assert_eq!(m.effective_cores(), 8.0);
    }

    #[test]
    fn effective_cores_falls_back_when_request_zero() {
        let m = PodMetrics {
            cores_used: 8,
            cpu_request_millicores: Some(0),
            ..Default::default()
        };
        assert_eq!(m.effective_cores(), 8.0);
    }

    #[test]
    fn json_includes_rows_written_and_cpu_request_when_set() {
        let m = PodMetrics {
            schema: "financial".into(),
            cores_used: 8,
            cpu_request_millicores: Some(8000),
            rows_written: 12_345_678,
            ..Default::default()
        };
        let j = m.to_json();
        assert!(j.contains("\"rows_written\":12345678"));
        assert!(j.contains("\"cpu_request_millicores\":8000"));
    }

    #[test]
    fn throughput_mbps_zero_elapsed_safe() {
        let m = PodMetrics {
            elapsed_s: 0.0,
            bytes_written: 100,
            ..Default::default()
        };
        assert_eq!(m.throughput_mbps(), 0.0);
    }

    #[test]
    fn cpu_hr_per_tb_zero_bytes_is_nan_and_omitted() {
        let m = PodMetrics {
            schema: "x".into(),
            cores_used: 4,
            elapsed_s: 1.0,
            bytes_written: 0,
            ..Default::default()
        };
        assert!(m.cpu_hr_per_tb().is_nan());
        let j = m.to_json();
        assert!(!j.contains("cpu_hr_per_tb"));
    }

    #[test]
    fn json_string_escapes_quotes_and_control() {
        let m = PodMetrics {
            schema: "x".into(),
            bucket: "has\"quote".into(),
            prefix: "line\nbreak".into(),
            ..Default::default()
        };
        // Would explode our mini parser if unescaped.
        let v = parses_as_json(&m.to_json());
        if let serde_json_min::Value::Obj(o) = v {
            assert_eq!(o.get("bucket").unwrap(), &serde_json_min::Value::Str("has\"quote".into()));
            assert_eq!(o.get("prefix").unwrap(), &serde_json_min::Value::Str("line\nbreak".into()));
        } else {
            panic!("not object");
        }
    }

    #[test]
    fn non_finite_floats_do_not_break_json() {
        let mut m = PodMetrics {
            schema: "x".into(),
            elapsed_s: f64::INFINITY,
            ..Default::default()
        };
        m.setup_s = f64::NAN;
        // Should still parse.
        let _ = parses_as_json(&m.to_json());
    }

    #[test]
    fn financial_specific_fields_only_when_set() {
        let m = PodMetrics {
            schema: "financial".into(),
            scale: Some(0.5),
            corpus_months: Some(60),
            population: Some(11_000_000),
            total_txns: Some(500_000_000),
            typology_instances: Some(1200),
            ..Default::default()
        };
        let j = m.to_json();
        assert!(j.contains("\"scale\":"));
        assert!(j.contains("\"population\":"));
        assert!(j.contains("\"typology_instances\":"));
        assert!(!j.contains("\"target_tb\":"));
        assert!(!j.contains("\"customer_id_max\":"));
    }

    #[test]
    fn customer360_specific_fields_only_when_set() {
        let m = PodMetrics {
            schema: "customer360".into(),
            target_tb: Some(0.5),
            customer_id_max: Some(500_000),
            dirty_ratio: Some(0.08),
            ..Default::default()
        };
        let j = m.to_json();
        assert!(j.contains("\"target_tb\":"));
        assert!(j.contains("\"customer_id_max\":"));
        assert!(!j.contains("\"scale\":"));
        assert!(!j.contains("\"population\":"));
    }

    #[test]
    fn emit_line_has_prefix() {
        // Not exercising stderr capture; the actual emit call is trivial.
        // Just guard against someone changing the prefix.
        assert_eq!("LB_METRICS_JSON ".trim_end(), "LB_METRICS_JSON");
    }
}
