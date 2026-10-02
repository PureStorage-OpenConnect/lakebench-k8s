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
      endpoint: ""                       # REQUIRED: e.g. http://your-s3:80 or https://your-s3:443
      region: "us-east-1"               # AWS region (required even for non-AWS)
      path_style: true                   # Required for FlashBlade, MinIO
      access_key: ""                     # Inline S3 access key
      secret_key: ""                     # Inline S3 secret key
      ca_cert: ""                        # PEM CA cert path (for HTTPS with self-signed CA)
      verify_ssl: true                   # Set false to skip SSL verification (dev only)
      buckets:
        bronze: "<name>-bronze"          # Bronze layer bucket (default: deployment name + layer)
        silver: "<name>-silver"          # Silver layer bucket
        gold: "<name>-gold"              # Gold layer bucket
      create_buckets: true               # Auto-create buckets if missing
```

### Field Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `endpoint` | string | `""` (required) | Full URL of the S3 endpoint, including scheme and port. Example: `http://10.0.1.50:80` or `https://10.0.1.50:443`. |
| `region` | string | `us-east-1` | AWS region for signature compatibility. Required even for non-AWS endpoints because boto3 uses it for S3v4 signing. |
| `path_style` | bool | `true` | Use path-style bucket addressing (`http://endpoint/bucket`) instead of virtual-hosted style (`http://bucket.endpoint`). Must be `true` for FlashBlade and MinIO. |
| `access_key` | string | `""` | S3 access key provided inline. |
| `secret_key` | string | `""` | S3 secret key provided inline. |
| `buckets.bronze` | string | `<name>-bronze` | Bucket name for the bronze (raw) data layer. When unset, derived from the deployment `name`. |
| `buckets.silver` | string | `<name>-silver` | Bucket name for the silver (enriched) data layer. When unset, derived from the deployment `name`. |
| `buckets.gold` | string | `<name>-gold` | Bucket name for the gold (aggregated) data layer. When unset, derived from the deployment `name`. |
| `create_buckets` | bool | `true` | Automatically create buckets that do not exist. Set to `false` if buckets are pre-provisioned or if the credentials lack `CreateBucket` permission. |
| `ca_cert` | string | `""` | Path to a PEM CA certificate bundle for HTTPS endpoints with self-signed or private CAs. At deploy time, the PEM content is read and embedded into a Kubernetes Secret for all components. Empty = system default CAs. |
| `verify_ssl` | bool | `true` | Verify SSL certificates for HTTPS endpoints. Set `false` only for development when you don't have the CA certificate file. |

## Credential Management

Lakebench reads S3 credentials only from `access_key` and `secret_key`. `deploy` renders the `lakebench-s3-credentials` Secret from them and the CLI's S3 client uses the same fields. To keep keys out of the file, use `${VAR}` environment substitution in the YAML (for example `access_key: "${S3_ACCESS_KEY}"`).

`secret_ref` (the name of an existing Kubernetes Secret) was removed in v1.7: nothing ever read an existing Secret. The commands that change data refuse a config that sets it; `destroy`, `status` and the read-only commands load it and drop the key with a note.

If no credentials are provided, the config still loads, and `lakebench deploy` refuses to start.

## Minimum S3 Permissions (IAM Policy)

The S3 credentials used by Lakebench must have the following permissions.
This applies regardless of the S3 provider (AWS, FlashBlade, MinIO, Ceph).

### Required Permissions

| Permission | Used By | Purpose |
|---|---|---|
| `s3:ListAllMyBuckets` | CLI (boto3) | Connectivity test during `lakebench config validate` and pre-flight checks |
| `s3:HeadBucket` | CLI (boto3) | Check whether buckets exist before creating them |
| `s3:ListBucket` | CLI (boto3), Spark (S3A), Trino, DuckDB | List objects for verification, cleanup, and query engine reads |
| `s3:GetObject` | Spark (S3A), Trino, DuckDB | Read data files from bronze, silver, and gold layers |
| `s3:PutObject` | Spark (S3A) | Write data files to silver and gold layers, write Iceberg/Delta metadata |
| `s3:DeleteObject` | CLI (boto3), Spark (S3A) | Cleanup during `lakebench destroy`, Iceberg orphan file removal |
| `s3:PutBucketTagging` | CLI (boto3) | Stamp the `lakebench.deployment` ownership tag on every bucket during `lakebench deploy` |
| `s3:GetBucketTagging` | CLI (boto3) | Read the tag back after deploy, and check ownership before `destroy` or `clean` empties a bucket |

On a backend that does not implement bucket tagging (FlashBlade returns
`NotImplemented`), lakebench falls back to a name-and-record check (see
[storage-backends.md](storage-backends.md)). On a backend that implements
tagging, an access-denied response on either tagging call is an error, not
a reason to fall back.

### Required for Multipart Upload Cleanup

Large files (Parquet, ORC) are written via S3 multipart uploads. If a Spark
job fails mid-write, incomplete uploads must be cleaned up to avoid ghost
objects (especially on FlashBlade).

| Permission | Used By | Purpose |
|---|---|---|
| `s3:ListMultipartUploadParts` | CLI (boto3) | Discover parts of incomplete uploads |
| `s3:AbortMultipartUpload` | CLI (boto3) | Abort incomplete multipart uploads during cleanup |
| `s3:ListBucketMultipartUploads` | CLI (boto3) | List all incomplete uploads in a bucket |

### Optional

| Permission | Used By | Purpose |
|---|---|---|
| `s3:CreateBucket` | CLI (boto3) | Auto-create bronze/silver/gold buckets during `lakebench deploy`. Only needed when `create_buckets: true` (the default). Set `create_buckets: false` if buckets are pre-provisioned or credentials lack this permission. |
| `s3:DeleteBucket` | CLI (boto3) | Delete the buckets deploy created during `lakebench destroy`. Required when `create_buckets: true` unless you pass `--keep-buckets`: without it the bucket delete fails, destroy reports FAILED, and the namespace is kept as the buckets' ownership record. |

### Example IAM Policy (AWS Format)

This policy works on AWS S3, MinIO, and any S3-compatible store that supports
AWS-style IAM policies:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LakebenchListBuckets",
      "Effect": "Allow",
      "Action": "s3:ListAllMyBuckets",
      "Resource": "*"
    },
    {
      "Sid": "LakebenchBucketOps",
      "Effect": "Allow",
      "Action": [
        "s3:HeadBucket",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:CreateBucket",
        "s3:DeleteBucket",
        "s3:GetBucketTagging",
        "s3:PutBucketTagging"
      ],
      "Resource": [
        "arn:aws:s3:::lakebench-*"
      ]
    },
    {
      "Sid": "LakebenchObjectOps",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListMultipartUploadParts",
        "s3:AbortMultipartUpload"
      ],
      "Resource": [
        "arn:aws:s3:::lakebench-*/*"
      ]
    }
  ]
}
```

Adjust the `Resource` ARNs to match your bucket names. Unset bucket names
default to `<deployment name>-bronze`, `-silver` and `-gold`, so a deployment
named `my-project` uses `my-project-bronze` and the pattern must cover it.

For **FlashBlade**, IAM policies are managed through the FlashBlade management
UI or CLI under the S3 account settings. The permission model is simpler --
FlashBlade typically grants full S3 access per account. Ensure the account has
read/write/delete permissions on the target buckets.

For **MinIO**, use `mc admin policy` to create and attach a custom policy with
the permissions listed above.

## FlashBlade Specifics

When using Pure Storage FlashBlade as the S3 backend, keep the following in mind:

- **HTTP or HTTPS.** FlashBlade object-store endpoints serve on port 80 (HTTP) or port 443 (HTTPS). Use `http://<IP>:80` for HTTP or `https://<IP>:443` for HTTPS.
- **HTTPS with self-signed CA.** FlashBlade uses a self-signed certificate by default. To use HTTPS, extract the CA certificate and set `ca_cert`:

  ```bash
  # Extract the CA certificate from FlashBlade
  openssl s_client -connect <IP>:443 -showcerts </dev/null 2>/dev/null \
    | openssl x509 -outform PEM > flashblade-ca.pem
  ```

  ```yaml
  platform:
    storage:
      s3:
        endpoint: https://<IP>:443
        ca_cert: ./flashblade-ca.pem
  ```

  Lakebench automatically distributes the certificate to all components (Spark, Trino, Polaris, Hive, datagen) via a Kubernetes Secret and JVM truststore injection.

- **Path-style access is required.** FlashBlade does not support virtual-hosted bucket addressing. Always set `path_style: true`.
- **Multipart upload ghost objects.** After failed or aborted uploads, FlashBlade may report non-zero object counts in its management UI even though `list_objects_v2` returns nothing. These are incomplete multipart upload artifacts that FlashBlade garbage-collects asynchronously. The `empty_bucket()` method in the S3 client handles this by aborting all incomplete multipart uploads and then entering a retry-verify loop: it re-checks both `list_objects_v2` and `list_multipart_uploads` until both return empty, waiting up to `max_wait` seconds (default 300) for FlashBlade's async GC to catch up.

## Spark S3A Integration

Spark jobs access the S3 buckets through the Hadoop S3A connector. Lakebench injects proven S3A tuning parameters into every Spark job, proven at 1TB+ scale on FlashBlade. The multipart part size, upload blocks and retry settings are job defaults that the `spark.conf` section of the YAML file can override; the connection pool, thread count and upload buffer are set by Lakebench for every job and are refused in `spark.conf` (see [Spark Reference](component-spark.md)).

## See Also

- [Configuration Reference](configuration.md) -- Full YAML schema documentation
- [Troubleshooting](troubleshooting.md) -- Common S3 connectivity and credential errors
- [Architecture Overview](architecture.md) -- How S3 fits into the overall Lakebench stack
