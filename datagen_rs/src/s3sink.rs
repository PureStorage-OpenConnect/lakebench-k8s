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
    ClientOptions, Error as OsError, MultipartUpload, ObjectStore, PutPayload, RetryConfig,
    WriteMultipart,
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
        Ok(Self {
            bucket,
            prefix,
            endpoint,
            region,
            access_key,
            secret_key,
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
        // Bound both connect and total request time so a stalled TCP connection
        // to FlashBlade cannot silently wedge a worker for the whole Job
        // timeout window; a stall becomes a normal retryable error instead.
        let client_opts = ClientOptions::new()
            .with_connect_timeout(Duration::from_secs(10))
            .with_timeout(Duration::from_secs(120))
            // FlashBlade is in-cluster HTTP; opt in explicitly so the builder
            // does not refuse a non-https endpoint.
            .with_allow_http(true);
        // Cap object_store's internal retry budget to a single retry so a real
        // outage doesn't stack our 3-attempt loop on top of its default of 10.
        // Keeping a small nonzero value here means the client still handles the
        // one-shot 5xx / connection-reset case, and our loop handles longer
        // outages with backoff -- matches what the previous disk-write path did.
        let retry_cfg = RetryConfig {
            max_retries: 1,
            retry_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let store = AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region)
            .with_endpoint(&cfg.endpoint)
            .with_access_key_id(&cfg.access_key)
            .with_secret_access_key(&cfg.secret_key)
            // FlashBlade needs path-style; virtual-hosted would resolve into a
            // DNS name that does not exist.
            .with_virtual_hosted_style_request(false)
            .with_client_options(client_opts)
            .with_retry(retry_cfg)
            .build()
            .expect("build object_store client");
        S3Sink {
            store: Arc::new(store),
            prefix: cfg.prefix.trim_end_matches('/').to_string(),
            _rt: rt,
            handle,
        }
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
    pub fn put_multipart(&self, key: &str) -> MpuWriter {
        let full = self.full_key(key);
        let path = Path::from(full.as_str());
        let store = self.store.clone();
        let path_c = path.clone();
        let upload = self
            .handle
            .block_on(async move { store.put_multipart(&path_c).await })
            .unwrap_or_else(|e| panic!("s3 put_multipart begin key={} err={}", full, e));
        MpuWriter::from_upload(upload, self.handle.clone(), full)
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
