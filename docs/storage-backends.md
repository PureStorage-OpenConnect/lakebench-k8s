# S3 storage

Reference: configure and check an S3 store: credentials, permissions, FlashBlade settings, which stores work, the conformance check and bucket tagging.

S3-compatible object storage holds all pipeline data in three buckets: bronze (raw), silver (cleaned and enriched) and gold (aggregated tables). Every recipe uses it.

- The CLI talks to S3 with [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html). The S3 client (`src/lakebench/s3/client.py`) tests connectivity, validates credentials, and creates, sizes and empties buckets.
- Before any Spark job runs, deploy validates the endpoint, authenticates and, unless told not to, creates the three buckets.
- S3 implementations differ in ways that stay hidden until something breaks. `lakebench config storage lakebench.yaml` checks a store before you deploy. **Backend supported** means the store will work. Otherwise the output names the missing operation and what breaks.

## Configuration keys

All keys live under `platform.storage.s3`. Defaults from `S3Config` in `config/schema.py`.

| Key | Default | Effect |
|---|---|---|
| `endpoint` | `""` (required) | Full URL with scheme and port, e.g. `http://10.0.1.50:80` or `https://10.0.1.50:443`. |
| `region` | `us-east-1` | Region for S3v4 signing. Required even for non-AWS endpoints. |
| `path_style` | `true` | Path-style addressing (`http://endpoint/bucket`) instead of virtual-hosted (`http://bucket.endpoint`). Must be `true` for FlashBlade and MinIO. |
| `access_key` | `""` | S3 access key |
| `secret_key` | `""` | S3 secret key |
| `buckets.bronze`, `.silver`, `.gold` | `<name>-bronze`, `-silver`, `-gold` | Layer bucket names. Unset, derived from the deployment `name`. |
| `create_buckets` | `true` | Create buckets that do not exist. Set `false` when buckets are pre-provisioned or credentials lack `CreateBucket`. `lakebench reproduce` refuses `false` (exit 2): it measures only against buckets it creates. |
| `ca_cert` | `""` | Path to a PEM CA bundle for HTTPS with a self-signed or private CA. Deploy reads it and puts it in a Kubernetes Secret for all components. Empty: system CAs. |
| `verify_ssl` | `true` | Verify HTTPS certificates. Set `false` only in development without the CA file. |

## Credentials

- Lakebench reads S3 credentials only from `access_key` and `secret_key`. `deploy` renders the `lakebench-s3-credentials` Secret from them; the CLI's S3 client uses the same fields.
- Keep keys out of the file with `${VAR}` substitution, e.g. `access_key: "${LAKEBENCH_S3_ACCESS_KEY}"`.
- Without credentials the config loads, and `lakebench deploy` refuses to start.
- `secret_ref` is removed ([UPGRADING-1.7.md](../UPGRADING-1.7.md#config-fields-nothing-read-are-removed)): nothing ever read an existing Secret.

## Required permissions

These apply to every provider (AWS, FlashBlade, MinIO, Ceph).

| Permission | Used by | Purpose |
|---|---|---|
| `s3:ListAllMyBuckets` | CLI | Connectivity test in `lakebench config validate` and pre-flight checks |
| `s3:HeadBucket` | CLI | Check whether buckets exist before creating them |
| `s3:ListBucket` | CLI, Spark (S3A), Trino, DuckDB | List objects for verification, cleanup and query reads |
| `s3:GetObject` | Spark, Trino, DuckDB | Read data files from all layers |
| `s3:PutObject` | Spark | Write silver and gold data files and Iceberg/Delta metadata |
| `s3:DeleteObject` | CLI, Spark | Cleanup in `lakebench destroy`, Iceberg orphan file removal |
| `s3:PutBucketTagging` | CLI | Stamp the `lakebench.deployment` ownership tag on every bucket in `lakebench deploy` |
| `s3:GetBucketTagging` | CLI | Read the tag back after deploy, and check ownership before `destroy` or `clean` empties a bucket |
| `s3:ListMultipartUploadParts` | CLI | Find parts of incomplete uploads |
| `s3:AbortMultipartUpload` | CLI | Abort incomplete uploads in cleanup |
| `s3:ListBucketMultipartUploads` | CLI | List incomplete uploads in a bucket |

- Large files (Parquet, ORC) are written as multipart uploads. A Spark job that fails mid-write leaves incomplete uploads. Cleanup must remove them to avoid ghost objects (especially on FlashBlade).
- A backend without bucket tagging (FlashBlade returns `NotImplemented`) falls back to a name-and-record check ([below](#bucket-tagging)). On a backend with tagging, access denied on either tagging call is an error, not a fallback.

Optional:

| Permission | Used by | Purpose |
|---|---|---|
| `s3:CreateBucket` | CLI | Create the buckets in `lakebench deploy`. Needed only with `create_buckets: true` (the default). |
| `s3:DeleteBucket` | CLI | Delete the buckets deploy created in `lakebench destroy`. Required with `create_buckets: true` unless you pass `--keep-buckets`. Without it the bucket delete fails, destroy reports FAILED, and the namespace is kept as the buckets' ownership record. |

### Example IAM policy (AWS format)

Works on AWS S3, MinIO and any store with AWS-style IAM policies:

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

- Match the `Resource` ARNs to your bucket names. A deployment named `my-project` uses `my-project-bronze`, `-silver` and `-gold`, and the pattern must cover them.
- **FlashBlade:** manage policies in the FlashBlade UI or CLI under the S3 account settings. FlashBlade typically grants full S3 access per account; the account needs read, write and delete on the target buckets.
- **MinIO:** create and attach a policy with `mc admin policy`.

## FlashBlade settings

- **Endpoint:** port 80 (HTTP) or 443 (HTTPS): `http://<IP>:80` or `https://<IP>:443`.
- **Path-style is required.** FlashBlade has no virtual-hosted addressing; keep `path_style: true`.
- **HTTPS uses a self-signed certificate by default.** Extract the CA and set `ca_cert`:

  ```bash
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

- **CA distribution:** Lakebench gives the certificate to Spark, Trino, Polaris, Hive and datagen through a Kubernetes Secret and JVM truststore injection. Datagen pods get it as `S3_CA_CERT` and `SSL_CERT_FILE`. The second replaces the pod's system CA store, so while `ca_cert` is set datagen trusts only the CAs in that file.
- **Multipart ghost objects.** `empty_bucket()` in the S3 client aborts all incomplete uploads. It then re-checks `list_objects_v2` and `list_multipart_uploads` until both are empty, for up to `max_wait` seconds (default 300). See [FlashBlade shows objects after bucket cleanup](troubleshooting.md#flashblade-shows-objects-after-bucket-cleanup).
- **No bucket tagging:** see [Bucket tagging](#bucket-tagging).

## Spark S3A

Spark reaches the buckets through the Hadoop S3A connector, with S3A tuning proven at 1TB+ on FlashBlade.

- Part size, upload blocks and retries are job defaults that `spark.conf` can override.
- Connection pool, thread count and upload buffer are set by Lakebench and refused in `spark.conf`.
- Both lists: [Spark conf](component-spark.md#spark-conf-s3a-shuffle-memory).

## Validated backends

"Validated" means the backend passed the conformance checks on the date shown.
An unlisted store is checked at runtime, never refused.

| Backend | Status | Region strict | Bucket tagging | Notes |
|---|---|---|---|---|
| **Pure Storage FlashBlade** | Validated 2026-07-25, tagging re-checked 2026-09-21 | No | **No** | Reference platform. Path-style required. |
| **Garage** 1.0.1+ | Validated 2026-07-25 | Yes | -- | Default for local mode. Apache 2.0, 21.7 MB image. |
| **AWS S3** | Not yet validated | Yes | Yes | Set `path_style: false` for virtual-hosted addressing. |
| **MinIO** | Not yet validated | -- | Yes | Community edition is maintenance-only since 2025. |
| **SeaweedFS** | **Not supported** | No | -- | Bucket enumeration is broken. See below. |

Ceph RGW, Dell ECS and other S3-compatible stores are expected to work but
have not been checked. Run `lakebench config storage` to find out.

### Why SeaweedFS is not supported

- SeaweedFS returns an empty `ListAllMyBucketsResult` while `head_bucket`
  returns 200 and objects read back correctly. Only enumeration is broken.
- Lakebench's connectivity check then reports `overall_success: True` with an
  empty bucket list, so prerequisites pass.
- The failure stays silent until `destroy` cannot find the buckets it must
  empty. Data is left behind with no error.

## What lakebench requires

The checks are graded. A failure blocks only when it breaks a Lakebench code
path.

### Required

A backend that fails any of these cannot run Lakebench.

| Check | Operations | What breaks without it |
|---|---|---|
| `connectivity` | `list_buckets` | Nothing else can run. |
| `bucket-enumeration` | `list_buckets` returns created buckets | Deploy cannot verify buckets; `destroy` cannot clean up. |
| `object-operations` | `put_object`, `get_object`, `list_objects_v2` with prefix, `delete_objects` | Datagen cannot write; Spark cannot read. |
| `multipart-upload` | `create_multipart_upload`, `upload_part`, `list_multipart_uploads`, `abort_multipart_upload` | `empty_bucket()` cannot clear incomplete uploads, so buckets never empty. |

### Advisory

Recorded because it changes how Lakebench configures itself, not as a defect.

| Property | Meaning | Lakebench's response |
|---|---|---|
| `region-strictness` | Whether the backend validates the sigv4 region scope | Sets `spark.hadoop.fs.s3a.endpoint.region` from `s3.region` on every job. Automatic. |
| `bucket-tagging` | Whether `GetBucketTagging` / `PutBucketTagging` work at all | Uses tags for cross-team ownership when available; falls back to bucket-name matching on backends that return `NotImplemented`. |

FlashBlade accepts any region; Garage rejects a mismatch. Setting the region
explicitly makes both work.

### Bucket tagging

Destroy rules for every backend: [Bucket ownership](deployment.md#bucket-ownership). What differs by backend:

- Lakebench tags every bucket it creates with `lakebench.deployment`. On backends with bucket tagging (AWS S3, MinIO, Garage) the tag is the authoritative check: `destroy` refuses to empty another deployment's bucket.
- FlashBlade returns HTTP 501 `NotImplemented` for `GetBucketTagging` and `PutBucketTagging`. Lakebench detects this at runtime.
- `lakebench deploy` logs a warning once per run when it takes the fallback below. `lakebench config storage` reports tagging support in the `bucket-tagging` ADVISORY row.

The FlashBlade fallback is a two-part check:

1. The bucket name is exactly the deployment name or starts with `{deployment_name}-` (the longest matching deployment name wins).
2. The namespace records the bucket in `lakebench.deployment/created-buckets` (destroy empties and deletes it) or `lakebench.deployment/adopted-empty-buckets` (destroy empties it, never deletes it).

On FlashBlade:

- A bucket that matches the name but is in neither record is left in place and reported. `--force-legacy` empties it and never deletes it.
- A bucket named outside the convention is refused on destroy; pass `--force-legacy` to proceed. The example configs and `lakebench init` follow the convention.
- Deploy adopts a pre-existing empty bucket only with `--force-legacy`, also with `create_buckets: false`. From one cluster, an empty bucket looks the same as another cluster's unwritten bucket. Adopting it would let the second cluster's destroy empty the first one's data.
- Without the flag, the bucket is used for reads and writes, but destroy, `clean` and the continuous reset leave its data alone. Buckets deploy creates are unaffected.
- A bucket 1.6 recorded as adopted-empty is not emptied by destroy, `clean` or `--regenerate` on that record alone: 1.6 wrote the same record for another cluster's bucket. An owner claims it once with `lakebench admin reclaim-bucket`, which writes its owner marker.
- The owner marker `.lakebench/owner.json` names the deployment and the cluster. A same-named deployment on another cluster sharing the FlashBlade refuses the bucket instead of adopting it.
- The marker is written with a conditional PUT when the backend enforces `IfNoneMatch`, and with a read-back check otherwise.
- `.lakebench/` keys never count as data. `clean` keeps them; destroy removes them only with the bucket.

## Running the check

```bash
# Against the endpoint in your config
lakebench config storage lakebench.yaml

# Read-only: no temporary bucket is created
lakebench config storage lakebench.yaml --no-full
```

By default the check creates a temporary bucket `lb-conformance-<random>`,
exercises it and deletes it. This needs create-bucket permission.

### If you cannot create buckets

Locked-down accounts often deny `CreateBucket`. That is a permissions limit,
not a backend defect, and is not a failure.

- `--no-full` runs read-only checks against the configured bronze bucket.
- Write and multipart checks are reported as **skipped**, not failed, and the
  output says coverage was partial:

  ```
  Degraded run: No permission to create buckets. Ran read-only checks against
  existing bucket 'my-lakehouse-bronze'. Write and multipart checks were skipped.
  ```

- A degraded run shows the backend is reachable and enumerates buckets. It
  cannot show that multipart abort works, so run the full check at least once
  in a non-production account.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | No required check failed. Safe to deploy. |
| 1 | A required check failed, or the config could not be loaded. |

The command is diagnostic. **It does not gate `deploy` or `run`.** An unknown
backend is checked and reported on, never refused.

## Interpreting output

- A table lists each check with `pass`, `FAIL` or `skip` and a detail, such as
  "Endpoint reachable and credentials accepted". Advisory passes are labelled
  `(advisory)`.
- Each failed required check prints an `Impact:` line after the table. Example
  detail: `Bucket 'lb-conformance-711610f674' exists but list_buckets did not
  return it`. Its impact line: "Bucket enumeration is silently broken.
  Connectivity checks report success with an empty bucket list, and destroy
  cannot reliably clean up. This is the SeaweedFS failure mode."
- A known backend's notes follow the table.
- The last line is one of:
  - `Backend supported. <n> passed, 0 failed, <n> skipped`
  - `No blocking failures, but coverage was partial.` (degraded run)
  - `Backend not usable by lakebench. <n> passed, <n> failed, <n> skipped` (exit 1)

## What the check does not cover

A pass means Lakebench's code paths work against the store. It is not a
performance, durability or suitability judgement:

- **Performance is not measured.** A backend can pass and be far too slow to
  benchmark against.
- **Durability and consistency are not tested.** Single-node test deployments
  pass the same checks as a production cluster.
- **Backend-specific behaviour is not covered.** FlashBlade's asynchronous
  multipart cleanup (ghost object counts in its UI after an abort) is handled
  by a retry loop in `empty_bucket()`, which the check does not exercise.

## Local mode: bundled Garage

With no cluster or external object store, Lakebench runs Garage as a podman or
docker container: the object-store half of local mode. `lakebench deploy
--local` starts it. The deployer can also be driven directly:

```python
from lakebench.runtime.container import ContainerRuntime
from lakebench.deploy.garage import GarageDeployer

runtime = ContainerRuntime(namespace="lakebench")
creds = GarageDeployer(runtime, config_dir="~/.lakebench/garage").deploy()
# creds.endpoint, creds.access_key, creds.secret_key, creds.region
```

- Deploy takes about two seconds and creates the three medallion buckets.
- Podman is preferred when both CLIs are present.
- Redeploying returns the same credentials, also after a full container
  delete and redeploy.

Two Garage behaviours the deployer handles:

- **`garage key create` is not idempotent.** It creates a duplicate key with
  the same name every time; after two runs `garage key info <name>` fails with
  "2 matching keys". The deployer checks `key list` first and addresses keys
  by ID.
- **Garage state must outlive the container.** Metadata (including access
  keys) and data are bind-mounted to the host config directory. Without that,
  a container recreate mints new credentials and orphans every bucket.
  `ContainerRuntime.apply()` also reuses a running container with a matching
  image rather than recreating it.

## Adding a backend

1. Run `lakebench config storage` against it and confirm no required check
   fails.
2. Add an entry to `KNOWN_BACKENDS` in `src/lakebench/s3/conformance.py` with
   the validation date, region strictness and anything a user needs to know.

The registry is advisory metadata for better messages. An entry does not grant
access, and a missing one does not deny it.

## Troubleshooting

- [S3A requests fail with a 400 and a null message](troubleshooting.md#s3a-requests-fail-with-a-400-and-a-null-message)
- [FlashBlade shows objects after bucket cleanup](troubleshooting.md#flashblade-shows-objects-after-bucket-cleanup)
- [Troubleshooting](troubleshooting.md) for other S3 errors

## See also

[Configuration](configuration.md), [Architecture](architecture.md).
