//! Direct-to-S3 upload sink. Rayon workers build parquet into an in-memory
//! `Vec<u8>` and `put` it here, so the pod holds no local scratch. Path-style
//! addressing and a custom endpoint are set so this works against FlashBlade
//! (and any other S3-compatible store lakebench targets) without touching the
//! crate's default AWS resolution.
//!
//! object_store is async-only. We create one multi-thread tokio runtime, share
//! the AmazonS3 client across rayon workers, and bridge with `Handle::block_on`
//! from each worker. `block_on` is safe on a non-runtime OS thread (a rayon
//! worker) as long as the runtime itself has worker threads to drive the task,
//! which the multi-thread runtime does.

use std::env;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    Certificate, ClientOptions, Error as OsError, MultipartUpload, ObjectStore, PutPayload,
    RetryConfig, WriteMultipart,
};
use tokio::runtime::{Handle, Runtime};

/// Config resolved once at startup from flags/env. Everything a worker needs
/// to PUT an object with no further lookups.
pub struct S3Cfg {
    pub bucket: String,
    pub prefix: String,
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub transport: Transport,
}

/// How the client reaches the store: addressing style and TLS. These change
/// where bytes go and how, never the bytes, so they are not corpus inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transport {
    /// `S3_PATH_STYLE=false`: virtual-hosted-style requests (default path
    /// style, which FlashBlade needs).
    pub virtual_hosted: bool,
    /// `S3_VERIFY_SSL=false`: accept any server certificate.
    pub allow_invalid_certificates: bool,
    /// Plain HTTP is allowed only for an `http://` endpoint.
    pub allow_http: bool,
    /// `S3_CA_CERT`: the PEM bundle read from that path.
    pub ca_pem: Option<Vec<u8>>,
}

fn env_bool(name: &str, value: Option<&str>, default: bool) -> Result<bool, String> {
    match value.map(|v| v.trim().to_ascii_lowercase()) {
        None => Ok(default),
        Some(v) if v.is_empty() => Ok(default),
        Some(v) if v == "true" || v == "1" || v == "yes" => Ok(true),
        Some(v) if v == "false" || v == "0" || v == "no" => Ok(false),
        Some(v) => Err(format!("{name} must be true or false; got {v:?}")),
    }
}

impl Transport {
    /// Resolve from the raw environment values; a value that is not a
    /// boolean, or a CA path that cannot be read, is an error (exit 2).
    pub fn from_values(
        endpoint: &str,
        path_style: Option<&str>,
        verify_ssl: Option<&str>,
        ca_cert: Option<&str>,
    ) -> Result<Transport, String> {
        let path_style = env_bool("S3_PATH_STYLE", path_style, true)?;
        let verify = env_bool("S3_VERIFY_SSL", verify_ssl, true)?;
        let ca_pem = match ca_cert.map(str::trim).filter(|p| !p.is_empty()) {
            None => None,
            Some(path) => Some(
                std::fs::read(path)
                    .map_err(|e| format!("S3_CA_CERT={path} cannot be read ({})", e.kind()))?,
            ),
        };
        let t = Transport {
            virtual_hosted: !path_style,
            allow_invalid_certificates: !verify,
            allow_http: endpoint.trim().to_ascii_lowercase().starts_with("http://"),
            ca_pem,
        };
        t.certificates()?;
        Ok(t)
    }

    /// The CA bundle's certificates (none without S3_CA_CERT).
    pub fn certificates(&self) -> Result<Vec<Certificate>, String> {
        match &self.ca_pem {
            None => Ok(Vec::new()),
            Some(pem) => {
                let certs = Certificate::from_pem_bundle(pem)
                    .map_err(|e| format!("S3_CA_CERT is not a PEM bundle ({e})"))?;
                if certs.is_empty() {
                    return Err("S3_CA_CERT holds no certificate".into());
                }
                Ok(certs)
            }
        }
    }

    /// Client options for this transport, with the sink's timeouts.
    pub fn client_options(&self) -> Result<ClientOptions, String> {
        // Bound both connect and total request time so a stalled TCP
        // connection cannot silently wedge a worker for the whole Job timeout
        // window; a stall becomes a normal retryable error instead.
        let mut opts = ClientOptions::new()
            .with_connect_timeout(Duration::from_secs(10))
            .with_timeout(Duration::from_secs(120))
            .with_allow_http(self.allow_http)
            .with_allow_invalid_certificates(self.allow_invalid_certificates);
        for cert in self.certificates()? {
            opts = opts.with_root_certificate(cert);
        }
        Ok(opts)
    }
}

/// `scheme://bucket.host[:port]` for virtual-hosted requests
/// (`https://s3.example` and bucket `b` give `https://b.s3.example`). The
/// endpoint must be a host name with no path (a trailing slash is dropped);
/// an IP address or a path cannot carry the bucket and is refused.
pub fn virtual_hosted_endpoint(endpoint: &str, bucket: &str) -> Result<String, String> {
    let (scheme, rest) = endpoint
        .split_once("://")
        .ok_or("S3_ENDPOINT needs a scheme (http:// or https://) for virtual-hosted requests")?;
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() || bucket.is_empty() {
        return Err("S3_ENDPOINT and the bucket must be set for virtual-hosted requests".into());
    }
    if rest.contains('/') {
        return Err(
            "S3_ENDPOINT has a path; virtual-hosted requests need a bare host (set path_style: true)"
                .into(),
        );
    }
    let host = match rest.rsplit_once(':') {
        Some((h, port)) if !h.contains(']') || h.ends_with(']') => {
            if port.chars().all(|c| c.is_ascii_digit()) {
                h
            } else {
                rest
            }
        }
        _ => rest,
    };
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if bare.parse::<std::net::IpAddr>().is_ok() {
        return Err(
            "S3_ENDPOINT is an IP address; virtual-hosted requests need a host name (set path_style: true)"
                .into(),
        );
    }
    Ok(format!("{scheme}://{bucket}.{rest}"))
}

/// Write `batch` as one parquet object through multipart uploads opened by
/// `open`, retrying a failed upload or completion with
/// `with_finish_retries` (each try rebuilds the file from the same batch, so
/// the object's bytes are the same). Returns the object size.
pub fn write_parquet_retrying(
    what: &str,
    mut open: impl FnMut() -> MpuWriter,
    batch: &arrow::record_batch::RecordBatch,
    props: impl Fn() -> parquet::file::properties::WriterProperties,
    wait: impl FnMut(u64),
) -> Result<u64, String> {
    with_finish_retries(
        what,
        || {
            let mut mpu = open();
            {
                let mut w =
                    parquet::arrow::ArrowWriter::try_new(&mut mpu, batch.schema(), Some(props()))
                        .map_err(|e| e.to_string())?;
                w.write(batch).map_err(|e| e.to_string())?;
                w.close().map_err(|e| e.to_string())?;
            }
            let sz = mpu.bytes_written();
            mpu.finish().map(|_| sz)
        },
        wait,
    )
}

/// Waits before each retry of a failed multipart completion: the file is
/// rebuilt from the same batch (the bytes are deterministic) and uploaded
/// again on the same key, so the object is unchanged.
pub const FINISH_RETRY_WAITS_S: [u64; 3] = [2, 4, 8];

/// Run `attempt` until it succeeds, waiting `FINISH_RETRY_WAITS_S` between
/// tries (through `wait`, so tests do not sleep). The last error, with every
/// earlier one, is returned after the final try.
pub fn with_finish_retries<T>(
    what: &str,
    mut attempt: impl FnMut() -> Result<T, String>,
    mut wait: impl FnMut(u64),
) -> Result<T, String> {
    let mut errors = Vec::new();
    for i in 0..=FINISH_RETRY_WAITS_S.len() {
        match attempt() {
            Ok(v) => return Ok(v),
            Err(e) => {
                eprintln!("{what}: attempt {} failed: {e}", i + 1);
                errors.push(e);
                if let Some(w) = FINISH_RETRY_WAITS_S.get(i) {
                    wait(*w);
                }
            }
        }
    }
    Err(errors.join("; "))
}

impl S3Cfg {
    /// Non-panicking constructor. Called at startup, before the multi-minute
    /// world build, so a misconfigured pod (missing/bad creds, missing
    /// endpoint) exits fast with a clear message instead of after minutes of
    /// wasted setup and a panic deep in a rayon worker.
    pub fn try_from_env(bucket: String, prefix: String) -> Result<Self, String> {
        let endpoint =
            env::var("S3_ENDPOINT").map_err(|_| "S3_ENDPOINT env var required".to_string())?;
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
        let access_key = env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| "AWS_ACCESS_KEY_ID env var required".to_string())?;
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| "AWS_SECRET_ACCESS_KEY env var required".to_string())?;
        let transport = Transport::from_values(
            &endpoint,
            env::var("S3_PATH_STYLE").ok().as_deref(),
            env::var("S3_VERIFY_SSL").ok().as_deref(),
            env::var("S3_CA_CERT").ok().as_deref(),
        )?;
        Ok(Self {
            bucket,
            prefix,
            endpoint,
            region,
            access_key,
            secret_key,
            transport,
        })
    }
}

/// Async S3 client + owning tokio runtime, shared across rayon workers.
pub struct S3Sink {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    // `_rt` keeps the runtime alive for the sink's lifetime; workers call the
    // handle. Dropping the sink drops the runtime, so we do not leak threads.
    _rt: Runtime,
    handle: Handle,
}

impl S3Sink {
    /// A sink that writes `<dir>/<bucket>/<prefix>/<key>` on the local
    /// filesystem, for tests and local corpora (DG_LOCAL_DIR). Same keys and
    /// bytes as the S3 path; only the store differs.
    pub fn local(dir: &str, bucket: &str, prefix: &str) -> Self {
        let root = std::path::Path::new(dir).join(bucket);
        std::fs::create_dir_all(&root).expect("create DG_LOCAL_DIR bucket dir");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .thread_name("local-io")
            .build()
            .expect("build tokio runtime");
        let handle = rt.handle().clone();
        let store = LocalFileSystem::new_with_prefix(&root).expect("local object store");
        S3Sink {
            store: Arc::new(store),
            prefix: prefix.trim_end_matches('/').to_string(),
            _rt: rt,
            handle,
        }
    }

    /// DG_LOCAL_DIR set: a local sink (no S3 credentials needed); otherwise
    /// the S3 sink from the environment. Exits 2 on a bad S3 config.
    pub fn from_env(bucket: &str, prefix: &str) -> Self {
        if let Ok(dir) = env::var("DG_LOCAL_DIR") {
            if !dir.is_empty() {
                return Self::local(&dir, bucket, prefix);
            }
        }
        match S3Cfg::try_from_env(bucket.to_string(), prefix.to_string()) {
            Ok(c) => Self::new(&c),
            Err(msg) => {
                eprintln!("s3 config error: {}", msg);
                std::process::exit(2);
            }
        }
    }

    pub fn new(cfg: &S3Cfg) -> Self {
        // S3 upload concurrency = number of tokio worker threads. Previously
        // hardcoded to 4, which throttled aggregate upload rate to ~4 in-flight
        // PUTs even when 16+ rayon workers were calling `handle.block_on(put)`
        // concurrently -- the s3_put phase became the ceiling as pod count
        // grew (measured: 44s -> 316s thread-time as pods went 8 -> 16 in
        // c360 sweep, 2026-09-20). Bumped to 8 as a default and made env-
        // overridable via DG_S3_IO_THREADS so operators can trade tokio thread
        // count against total CPU pressure. On FlashBlade in-cluster HTTP the
        // per-PUT time is ~5-15ms; 8 concurrent PUTs sustain ~500-1500 PUT/s
        // per pod, well above what rayon can feed at typical file sizes.
        let io_threads: usize = env::var("DG_S3_IO_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|n: &usize| *n > 0)
            .unwrap_or(8);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(io_threads)
            .thread_name("s3-io")
            .build()
            .expect("build tokio runtime");
        let handle = rt.handle().clone();
        let store = Self::builder(cfg)
            .and_then(|b| b.build().map_err(|e| e.to_string()))
            .unwrap_or_else(|e| {
                eprintln!("s3 config error: {e}");
                std::process::exit(2);
            });
        S3Sink {
            store: Arc::new(store),
            prefix: cfg.prefix.trim_end_matches('/').to_string(),
            _rt: rt,
            handle,
        }
    }

    /// The S3 client builder for `cfg`: endpoint, credentials, the transport
    /// settings and the retry budget. Public so tests can read its settings
    /// back (`get_config_value`) without a network.
    pub fn builder(cfg: &S3Cfg) -> Result<AmazonS3Builder, String> {
        // object_store's own retry budget per request (parts included): 3,
        // with the 30 s ceiling; a failed completion is retried above that by
        // rebuilding the file (`with_finish_retries`).
        let retry_cfg = RetryConfig {
            max_retries: 3,
            retry_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        // With virtual-hosted requests object_store uses a custom endpoint
        // as given, bucket included, so the bucket goes into the host here;
        // otherwise every key would land under the wrong bucket.
        let endpoint = if cfg.transport.virtual_hosted {
            virtual_hosted_endpoint(&cfg.endpoint, &cfg.bucket)?
        } else {
            cfg.endpoint.clone()
        };
        Ok(AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region)
            .with_endpoint(&endpoint)
            .with_access_key_id(&cfg.access_key)
            .with_secret_access_key(&cfg.secret_key)
            .with_virtual_hosted_style_request(cfg.transport.virtual_hosted)
            .with_client_options(cfg.transport.client_options()?)
            .with_retry(retry_cfg))
    }

    /// Absolute key (bucket-relative) formed from the sink prefix and the
    /// caller's key. Extracted so single-PUT and multipart paths agree.
    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key.trim_start_matches('/'))
        }
    }

    /// Begin a multipart upload and return a synchronous `std::io::Write`
    /// adapter over it. Wrap this in `ArrowWriter` (or any streaming
    /// writer) to emit an object of arbitrary size without holding the
    /// whole thing in a single `Vec<u8>`. Caller must invoke
    /// `MpuWriter::finish()` to complete the upload; dropping without
    /// finish aborts the upload so no orphan parts accumulate on S3.
    ///
    /// LB-107: party.parquet and account.parquet at scale >= 1000 exceed
    /// the 5 GiB S3 single-PUT ceiling; this path removes that limit
    /// (S3 multipart supports up to 5 TiB per object across 10k parts).
    ///
    /// Retry (Wave 2 D5, 2026-09-28): the begin call now retries three
    /// times with exponential backoff on transient errors, matching
    /// `put`'s 3-attempt loop. Continuous mode issues ~1 MPU per bronze
    /// file, so at scale 100 (~10k files per pod) a transient 5xx is a
    /// certainty; without the retry every failure crashed the pod.
    pub fn put_multipart(&self, key: &str) -> MpuWriter {
        let full = self.full_key(key);
        let path = Path::from(full.as_str());
        let mut last_err: Option<String> = None;
        for attempt in 0..3 {
            let store = self.store.clone();
            let path_c = path.clone();
            let res = self
                .handle
                .block_on(async move { store.put_multipart(&path_c).await });
            match res {
                Ok(upload) => return MpuWriter::from_upload(upload, self.handle.clone(), full),
                Err(e) => {
                    if is_fatal(&e) {
                        panic!(
                            "s3 put_multipart begin fatal (no retry): key={} err={}",
                            full, e
                        );
                    }
                    last_err = Some(format!("{}", e));
                    eprintln!(
                        "[s3sink] mpu begin retry {} for {}: {}",
                        attempt + 1,
                        full,
                        last_err.as_deref().unwrap_or("")
                    );
                    std::thread::sleep(Duration::from_millis(200 * (attempt as u64 + 1)));
                }
            }
        }
        panic!(
            "s3 put_multipart begin failed after 3 attempts: key={} err={}",
            full,
            last_err.unwrap_or_default()
        );
    }

    pub fn put(&self, key: &str, bytes: Vec<u8>) {
        let full = self.full_key(key);
        let path = Path::from(full.as_str());
        let payload = PutPayload::from(Bytes::from(bytes));
        let store = self.store.clone();
        let mut last_err: Option<String> = None;
        for attempt in 0..3 {
            let payload_c = payload.clone();
            let path_c = path.clone();
            let store_c = store.clone();
            let res = self
                .handle
                .block_on(async move { store_c.put(&path_c, payload_c).await });
            match res {
                Ok(_) => return,
                Err(e) => {
                    // Auth / permission errors are deterministic -- retrying
                    // just burns 32 workers' retry budgets in parallel. Bail
                    // immediately so the pod fails fast and a rotated key is
                    // obvious from the first line of pod logs.
                    if is_fatal(&e) {
                        panic!("s3 put fatal (no retry): key={} err={}", full, e);
                    }
                    last_err = Some(format!("{}", e));
                    eprintln!(
                        "[s3sink] retry {} for {}: {}",
                        attempt + 1,
                        full,
                        last_err.as_deref().unwrap_or("")
                    );
                    std::thread::sleep(Duration::from_millis(200 * (attempt as u64 + 1)));
                }
            }
        }
        panic!(
            "s3 put failed after 3 attempts: key={} err={}",
            full,
            last_err.unwrap_or_default()
        );
    }
}

/// Non-retryable object_store errors. Anything auth/perm/config-shaped bails
/// out of the retry loop so a bad credential rotation surfaces on the first
/// PUT, not after 3 x 32 workers each burn their retry budget.
fn is_fatal(e: &OsError) -> bool {
    match e {
        OsError::PermissionDenied { .. } => true,
        OsError::Unauthenticated { .. } => true,
        // NotFound on a PUT would mean the bucket does not exist -- also
        // deterministic; no point retrying.
        OsError::NotFound { .. } => true,
        _ => false,
    }
}

/// Sync `std::io::Write` adapter over `object_store::WriteMultipart`.
///
/// Rust callers stream into it (`ArrowWriter` calls `Write::write`);
/// internally each call bridges into the tokio runtime the owning
/// `S3Sink` holds and enqueues 5 MiB parts against a live S3 multipart
/// upload. The whole object never sits in a single `Vec<u8>`, which is
/// what lifts party.parquet / account.parquet past the 5 GiB single-PUT
/// ceiling.
///
/// Lifecycle: after the writer has produced its last byte, call
/// `finish()` to commit the upload. On any error (or if the caller
/// simply drops the writer, e.g. on panic) the destructor issues an
/// abort so partially-uploaded parts don't accumulate on FlashBlade.
/// Abort on drop is best-effort; explicit `finish()` or `abort()` is
/// the correct path for error handling.
pub struct MpuWriter {
    /// Inner upload; `None` after `finish()` or `abort()` consumed it,
    /// or if `Drop` cleaned it up.
    inner: Option<WriteMultipart>,
    handle: Handle,
    key: String,
    bytes_written: u64,
}

impl MpuWriter {
    /// Wrap an already-started multipart upload. Public so tests can
    /// build one against `object_store::memory::InMemory` without going
    /// through `S3Sink`. Chunk size is object_store's default 5 MiB
    /// (matches the S3 multipart minimum for non-final parts).
    pub fn from_upload(upload: Box<dyn MultipartUpload>, handle: Handle, key: String) -> Self {
        Self {
            inner: Some(WriteMultipart::new(upload)),
            handle,
            key,
            bytes_written: 0,
        }
    }

    /// Bytes handed to `write()` so far. Used by the emit binary to
    /// aggregate ref-zone throughput without needing the underlying
    /// object size returned by S3.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Commit the multipart upload. On error, `WriteMultipart::finish`
    /// itself calls `abort()` internally before returning the error, so
    /// we don't need to duplicate the abort here -- but we do surface
    /// the failure to the caller (currently by panic in `generate.rs`
    /// to match the single-PUT path's fail-loud semantics).
    ///
    /// finish() itself does not retry: WriteMultipart consumes itself into
    /// finish(), and on error it aborts the upload, so there is no live
    /// upload left to retry against. A caller that can rebuild the object
    /// retries the whole file (`write_parquet_retrying`); the other
    /// retryable moments are begin (`S3Sink::put_multipart`) and each part
    /// upload (object_store's RetryConfig).
    pub fn finish(mut self) -> Result<(), String> {
        let Some(w) = self.inner.take() else {
            return Err(format!("mpu writer key={} already finalized", self.key));
        };
        self.handle
            .block_on(w.finish())
            .map(|_| ())
            .map_err(|e| format!("mpu finish key={}: {}", self.key, e))
    }

    /// Explicitly abort the upload. Consumes self. Used on error paths
    /// where the caller wants to surface an explicit abort result
    /// rather than rely on `Drop`'s best-effort cleanup.
    pub fn abort(mut self) -> Result<(), String> {
        let Some(w) = self.inner.take() else {
            return Ok(());
        };
        self.handle
            .block_on(w.abort())
            .map_err(|e| format!("mpu abort key={}: {}", self.key, e))
    }
}

/// Upper bound on concurrent in-flight upload parts before Write::write
/// blocks. Also functions as the error-surfacing checkpoint: at each
/// call we drain any completed tasks (via `wait_for_capacity`), which
/// is the only place `object_store::WriteMultipart` reports part
/// failures to the caller. Small enough to bound memory (~5 MiB * N),
/// large enough to keep S3 pipelined.
const MPU_MAX_CONCURRENT_PARTS: usize = 4;

impl std::io::Write for MpuWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let Some(w) = self.inner.as_mut() else {
            return Err(std::io::Error::other(format!(
                "mpu write after finalize (key={})",
                self.key
            )));
        };
        // WriteMultipart::write spawns upload tasks on the current
        // tokio runtime. We're being called from a sync rayon worker
        // that isn't itself inside a runtime, so enter the sink's
        // handle for the duration of the call so `tokio::spawn` /
        // `JoinSet::spawn` resolve to the right runtime.
        let _guard = self.handle.enter();
        // Back-pressure: cap in-flight parts at MPU_MAX_CONCURRENT_PARTS
        // so buffered RSS stays bounded (~N * chunk_size = ~20 MiB at
        // N=4). Note this is capacity-bound only; when fewer than N
        // parts are in flight, `poll_for_capacity` returns immediately
        // WITHOUT polling the JoinSet, so a 5xx from part #1 does not
        // surface here. Error surfacing happens in `flush()` at the
        // parquet row-group boundary (worst-case delay: one row group,
        // typically 128 MiB) and unconditionally in `finish()`.
        self.handle
            .block_on(w.wait_for_capacity(MPU_MAX_CONCURRENT_PARTS))
            .map_err(|e| {
                std::io::Error::other(format!("mpu part upload failed (key={}): {}", self.key, e))
            })?;
        w.write(buf);
        self.bytes_written += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // ArrowWriter calls flush() between parquet row groups; use
        // this as the natural checkpoint to force-drain any completed
        // part tasks and surface errors. `wait_for_capacity(1)` blocks
        // until at most one task remains, so any failed part
        // (regardless of position in the JoinSet) is polled and its
        // Err returned. This bounds delayed-error surfacing to one
        // row group (~128 MiB) instead of "until finish() panics".
        // We do NOT force chunk-flushing to S3 -- WriteMultipart
        // buffers a partial chunk internally and emits it on finish;
        // that's the correct trade for streaming parquet writes.
        let Some(w) = self.inner.as_mut() else {
            return Ok(());
        };
        let _guard = self.handle.enter();
        self.handle.block_on(w.wait_for_capacity(1)).map_err(|e| {
            std::io::Error::other(format!(
                "mpu part upload failed on flush (key={}): {}",
                self.key, e
            ))
        })
    }
}

impl Drop for MpuWriter {
    fn drop(&mut self) {
        // Best-effort abort if the caller neither finished nor aborted
        // (typically because they panicked mid-write). Prevents orphan
        // parts on FlashBlade. Errors here are swallowed -- we're
        // already unwinding or exiting.
        if let Some(w) = self.inner.take() {
            let _ = self.handle.block_on(w.abort());
        }
    }
}
