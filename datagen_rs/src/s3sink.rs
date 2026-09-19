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
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::{ClientOptions, Error as OsError, ObjectStore, PutPayload, RetryConfig};
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
        let endpoint = env::var("S3_ENDPOINT").map_err(|_| "S3_ENDPOINT env var required".to_string())?;
        let region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
        let access_key = env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| "AWS_ACCESS_KEY_ID env var required".to_string())?;
        let secret_key = env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| "AWS_SECRET_ACCESS_KEY env var required".to_string())?;
        Ok(Self { bucket, prefix, endpoint, region, access_key, secret_key })
    }
}

/// Async S3 client + owning tokio runtime, shared across rayon workers.
pub struct S3Sink {
    store: Arc<AmazonS3>,
    prefix: String,
    // `_rt` keeps the runtime alive for the sink's lifetime; workers call the
    // handle. Dropping the sink drops the runtime, so we do not leak threads.
    _rt: Runtime,
    handle: Handle,
}

impl S3Sink {
    pub fn new(cfg: &S3Cfg) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
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

    pub fn put(&self, key: &str, bytes: Vec<u8>) {
        let full = if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix, key.trim_start_matches('/'))
        };
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
                        panic!(
                            "s3 put fatal (no retry): key={} err={}",
                            full, e
                        );
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
