# Component: S3 Object Storage

## Overview

S3-compatible object storage holds all pipeline data across three medallion-architecture buckets: bronze (raw ingested data), silver (cleaned and enriched), and gold (aggregated business-level tables). Lakebench uses [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) internally and supports any S3-compatible store, including AWS S3, Pure Storage FlashBlade, and MinIO.

The S3 client (`src/lakebench/s3/client.py`) provides high-level operations for connectivity testing, credential validation, bucket creation, bucket sizing, and cleanup. During deployment, Lakebench validates the endpoint, authenticates, and optionally creates the three layer buckets before any Spark jobs run.

## YAML Configuration

All S3 settings live under `platform.storage.s3` in your Lakebench config file:

```yaml
platform:
  storage:
    s3:
      endpoint: ""                       # REQUIRED: e.g. http://your-s3-endpoint:80
      region: "us-east-1"               # AWS region (required even for non-AWS)
      path_style: true                   # Required for FlashBlade, MinIO
      access_key: ""                     # Inline S3 access key
      secret_key: ""                     # Inline S3 secret key
      secret_ref: ""                     # OR: name of existing K8s Secret
      buckets:
        bronze: "lakebench-bronze"       # Bronze layer bucket name
        silver: "lakebench-silver"       # Silver layer bucket name
        gold: "lakebench-gold"           # Gold layer bucket name
      create_buckets: true               # Auto-create buckets if missing
```

### Field Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | string | `""` (required) | Full URL of the S3 endpoint, including scheme and port. Example: `http://10.0.1.50:80`. |
| `region` | string | `us-east-1` | AWS region for signature compatibility. Required even for non-AWS endpoints because boto3 uses it for S3v4 signing. |
| `path_style` | bool | `true` | Use path-style bucket addressing (`http://endpoint/bucket`) instead of virtual-hosted style (`http://bucket.endpoint`). Must be `true` for FlashBlade and MinIO. |
| `access_key` | string | `""` | S3 access key provided inline. Mutually exclusive with `secret_ref`. |
| `secret_key` | string | `""` | S3 secret key provided inline. Mutually exclusive with `secret_ref`. |
| `secret_ref` | string | `""` | Name of an existing Kubernetes Secret containing `access-key` and `secret-key` data fields. Preferred for production deployments. |
| `buckets.bronze` | string | `lakebench-bronze` | Bucket name for the bronze (raw) data layer. |
| `buckets.silver` | string | `lakebench-silver` | Bucket name for the silver (enriched) data layer. |
| `buckets.gold` | string | `lakebench-gold` | Bucket name for the gold (aggregated) data layer. |
| `create_buckets` | bool | `true` | Automatically create buckets that do not exist. Set to `false` if buckets are pre-provisioned or if the credentials lack `CreateBucket` permission. |

## Credential Management

Lakebench supports two credential strategies:

1. **Inline credentials** -- Set `access_key` and `secret_key` directly in the YAML file. Convenient for development but exposes secrets in plaintext.

2. **Kubernetes Secret reference** -- Set `secret_ref` to the name of a pre-existing Secret in the target namespace. The Secret must contain `access-key` and `secret-key` data fields. This is the recommended approach for production because credentials never appear in config files or version control.

If neither inline credentials nor a `secret_ref` is provided, validation is deferred to runtime. The deployment will fail when the S3 client attempts to authenticate.

## FlashBlade Specifics

When using Pure Storage FlashBlade as the S3 backend, keep the following in mind:

- **HTTP, not HTTPS.** FlashBlade object-store endpoints typically serve on port 80 over plain HTTP. Use `http://<IP>:80` as the endpoint.
- **Path-style access is required.** FlashBlade does not support virtual-hosted bucket addressing. Always set `path_style: true`.
- **Multipart upload ghost objects.** After failed or aborted uploads, FlashBlade may report non-zero object counts in its management UI even though `list_objects_v2` returns nothing. These are incomplete multipart upload artifacts that FlashBlade garbage-collects asynchronously. The `empty_bucket()` method in the S3 client handles this by aborting all incomplete multipart uploads and then entering a retry-verify loop: it re-checks both `list_objects_v2` and `list_multipart_uploads` until both return empty, waiting up to `max_wait` seconds (default 300) for FlashBlade's async GC to catch up.

## Spark S3A Integration

Spark jobs access the S3 buckets through the Hadoop S3A connector. Lakebench injects proven S3A tuning parameters (connection pool size, multipart upload size, retry settings) into every Spark job automatically. These defaults are defined under `spark.conf` in the config schema and have been battle-tested at 1TB+ scale on FlashBlade. They can be overridden in the `spark.conf` section of the YAML file if needed.

## See Also

- [Configuration Reference](configuration.md) -- Full YAML schema documentation
- [Troubleshooting](troubleshooting.md) -- Common S3 connectivity and credential errors
- [Architecture Overview](architecture.md) -- How S3 fits into the overall Lakebench stack
