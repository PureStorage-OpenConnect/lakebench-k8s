# Storage Backends

Lakebench stores all pipeline data in an S3-compatible object store. Any store
that implements the operations below will work, but S3 implementations differ in
ways that are not visible until something breaks, so lakebench ships a
conformance check you can run before you deploy.

## Quick answer

```bash
lakebench config storage lakebench.yaml
```

If it says **Backend supported**, your store will work. If it does not, it tells
you which operation is missing and what will break.

## Validated backends

"Validated" means the backend was run against the conformance checks on the date
shown, not that it is the only thing that works. An unlisted store is checked at
runtime, never refused.

| Backend | Status | Region strict | Notes |
|---|---|---|---|
| **Pure Storage FlashBlade** | Validated 2026-07-25 | No | Reference platform. Path-style required. |
| **Garage** 1.0.1+ | Validated 2026-07-25 | Yes | Default for local mode. Apache 2.0, 21.7 MB image. |
| **AWS S3** | Not yet validated | Yes | Set `path_style: false` for virtual-hosted addressing. |
| **MinIO** | Not yet validated | -- | Community edition is maintenance-only since 2025. |
| **SeaweedFS** | **Not supported** | No | Bucket enumeration is broken. See below. |

Ceph RGW, Dell ECS, and other S3-compatible stores are expected to work but have
not been run against the checks. Run `lakebench config storage` to find out.

### Why SeaweedFS is not supported

SeaweedFS returns an empty `ListAllMyBucketsResult` while `head_bucket` returns
200 and objects read back correctly. Buckets work; only enumeration is broken.

That combination is dangerous rather than merely limited. Lakebench's
connectivity check reports `overall_success: True` with an empty bucket list, so
prerequisites pass and the failure stays silent until `destroy` cannot find the
buckets it needs to empty. Data is left behind with no error.

## What lakebench requires

The conformance checks are graded. A failure only blocks when it breaks a
lakebench code path.

### Required

A backend that fails any of these cannot run lakebench.

| Check | Operations | What breaks without it |
|---|---|---|
| `connectivity` | `list_buckets` | Nothing else can run. |
| `bucket-enumeration` | `list_buckets` returns created buckets | Deploy cannot verify buckets; `destroy` cannot clean up. |
| `object-operations` | `put_object`, `get_object`, `list_objects_v2` with prefix, `delete_objects` | Datagen cannot write; Spark cannot read. |
| `multipart-upload` | `create_multipart_upload`, `upload_part`, `list_multipart_uploads`, `abort_multipart_upload` | `empty_bucket()` cannot clear incomplete uploads, so buckets never empty. |

### Advisory

Recorded because it changes how lakebench configures itself, not because it is a
defect.

| Property | Meaning | Lakebench's response |
|---|---|---|
| `region-strictness` | Whether the backend validates the sigv4 region scope | Sets `spark.hadoop.fs.s3a.endpoint.region` from `s3.region` on every job. Automatic; no action needed. |

Both behaviours are legitimate. FlashBlade accepts any region; Garage rejects a
mismatch. Lakebench sets the region explicitly so both work.

## Running the check

```bash
# Against the endpoint in your config
lakebench config storage lakebench.yaml

# Read-only: no temporary bucket is created
lakebench config storage lakebench.yaml --no-full
```

By default the check creates a temporary bucket named `lb-conformance-<random>`,
exercises it, and deletes it. This needs create-bucket permission.

### If you cannot create buckets

Locked-down production accounts often deny `CreateBucket`. That is a permissions
constraint, not a backend defect, and lakebench does not treat it as a failure.

Pass `--no-full` and the check runs read-only against your configured bronze
bucket. Write and multipart checks are reported as **skipped**, not failed, and
the output says coverage was partial:

```
Degraded run: No permission to create buckets. Ran read-only checks against
existing bucket 'lakebench-bronze'. Write and multipart checks were skipped.
```

A degraded run tells you the backend is reachable and enumerates buckets. It
cannot tell you multipart abort works, so run the full check at least once in a
non-production account.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | No required check failed. Safe to deploy. |
| 1 | A required check failed, or the config could not be loaded. |

The command is diagnostic. **It does not gate `deploy` or `run`.** A backend
lakebench has never seen is checked and reported on, never refused, so a store
that works is never blocked because it is unlisted.

## Interpreting output

A supported backend:

```
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Check              ┃ Result ┃ Detail                                      ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ connectivity       │ pass   │ Endpoint reachable and credentials accepted │
│ bucket-enumeration │ pass   │ list_buckets returns created buckets        │
│ object-operations  │ pass   │ put, get, prefix-list, batch-delete work    │
│ multipart-upload   │ pass   │ create, list, and abort all work            │
│ region-strictness  │ pass   │ Backend accepts any region (permissive)     │
└────────────────────┴────────┴─────────────────────────────────────────────┘

Backend supported. 5 passed, 0 failed, 0 skipped
```

A backend that will not work states the consequence, not just the failure:

```
│ bucket-enumeration │ FAIL   │ Bucket 'lb-conformance-711610f674' exists
│                    │        │ but list_buckets did not return it

Impact: Bucket enumeration is silently broken. Connectivity checks report
success with an empty bucket list, and destroy cannot reliably clean up.

Backend not usable by lakebench. 4 passed, 1 failed, 0 skipped
```

## What the check does not cover

Passing means lakebench's code paths work against the store. It is not a
performance, durability, or suitability judgement:

- **Performance is not measured.** A backend can pass every check and be far too
  slow to benchmark against.
- **Durability and consistency are not tested.** Single-node test deployments
  pass the same checks as a production cluster.
- **Backend-specific behaviour is not covered.** FlashBlade's asynchronous
  multipart cleanup (which can show ghost object counts in its UI after an
  abort) is handled by a retry loop in `empty_bucket()` and is not something the
  conformance check exercises.

Treat a pass as "lakebench will run here," not as an endorsement of the store for
benchmarking.

## Local mode: bundled Garage

When no cluster or external object store is available, lakebench can deploy
Garage as a container via podman or docker. This is the object store half of
local mode.

```python
from lakebench.runtime.container import ContainerRuntime
from lakebench.deploy.garage import GarageDeployer

runtime = ContainerRuntime(namespace="lakebench")
creds = GarageDeployer(runtime, config_dir="~/.lakebench/garage").deploy()
# creds.endpoint, creds.access_key, creds.secret_key, creds.region
```

Deploy takes roughly two seconds and creates the three medallion buckets.
Podman is preferred when both CLIs are present.

**Two behaviours worth knowing**, both of which caused real bugs during
development:

- **`garage key create` is not idempotent.** It succeeds every time and creates
  a duplicate key with the same name. After two runs, `garage key info <name>`
  fails with "2 matching keys". The deployer checks `key list` first and
  addresses keys by ID rather than name.
- **Garage state must outlive the container.** Metadata (including access keys)
  and data are bind-mounted to the host config directory. Without that, a
  container recreate silently mints new credentials and orphans every existing
  bucket. `ContainerRuntime.apply()` also reuses a running container with a
  matching image rather than recreating it.

Redeploying returns the same credentials, and they survive a full container
delete and redeploy.

## Adding a backend

1. Run `lakebench config storage` against it and confirm no required check fails.
2. Run the live suite for wider coverage:
   ```bash
   LB_S3_ENDPOINT=... LB_S3_ACCESS_KEY=... LB_S3_SECRET_KEY=... \
     pytest tests/test_s3_conformance.py -m integration -v
   ```
3. Add an entry to `KNOWN_BACKENDS` in `src/lakebench/s3/conformance.py` with the
   validation date, region strictness, and anything a user needs to know.

The registry is advisory metadata for better messages. Adding an entry does not
grant access, and omitting one does not deny it.

## Related

- [Configuration](configuration.md) for `platform.storage.s3` fields
- [Troubleshooting](troubleshooting.md) for S3 error diagnosis
- [Component: S3](component-s3.md) for how lakebench uses object storage
