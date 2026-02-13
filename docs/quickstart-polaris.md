# Polaris Catalog Quick Start

Lakebench supports [Apache Polaris](https://github.com/apache/polaris) as an
alternative to Hive Metastore for Iceberg catalog management. Polaris is an
open-source REST catalog that provides OAuth2 authentication, fine-grained
access control, and a standards-based Iceberg REST API.

Switching to Polaris requires a single configuration change. The rest of the
workflow -- deploy, generate, run, destroy -- stays exactly the same.

---

## Requirements

| Component | Minimum version | Default in Lakebench |
|-----------|----------------|---------------------|
| Apache Polaris | **1.3.0-incubating** | 1.3.0-incubating |
| Trino | **454** | 479 |

**Why 1.3.0?** Polaris versions 1.1.0 and 1.2.0 have a credential vending
bug ([apache/polaris#379](https://github.com/apache/polaris/issues/379))
where the `TaskFileIOSupplier` class ignores the `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`
flag, causing server-side S3 operations to fail on non-AWS storage. The fix
([PR #400](https://github.com/apache/polaris/pull/400)) shipped in 1.3.0.

**Why Trino 454+?** The `iceberg.rest-catalog.oauth2.scope` property was
added in Trino 454
([PR #22961](https://github.com/trinodb/trino/pull/22961)). Without it,
Trino sends `scope=catalog` which Polaris rejects as `invalid_scope`.
Lakebench defaults to Trino 479, which includes this property plus the
native S3 file system required for current Trino releases.

---

## Configuration

Start from an existing config file (or generate one with `lakebench init`).
The only change needed is setting the catalog type:

```yaml
architecture:
  catalog:
    type: polaris
```

That is it. Lakebench uses the default Polaris version (1.3.0-incubating) and
Trino version (479), both of which satisfy the minimum requirements above.

### What changes under the hood

When `catalog.type: polaris` is set, Lakebench automatically:

- **Deploys a Polaris server** (Deployment + Service on port 8181) backed by
  the same PostgreSQL instance used for Hive, but in a separate `polaris`
  database.
- **Bootstraps the catalog** via a Kubernetes Job that creates the Polaris
  realm, root principal, catalog with S3 storage config
  (`stsUnavailable: true`, `pathStyleAccess: true`), and the required
  principal roles.
- **Configures Spark** to use the Iceberg REST catalog
  (`catalog-impl=org.apache.iceberg.rest.RESTCatalog`) with OAuth2
  client credentials and static S3 access keys.
- **Configures Trino** to use `iceberg.catalog.type=rest` with
  `oauth2.scope=PRINCIPAL_ROLE:ALL` and the native S3 file system
  (`fs.native-s3.enabled=true`).
- **Updates init containers** so Trino waits for Polaris readiness instead
  of Hive Metastore.

---

## Step by Step

### 1. Generate or modify your config

```bash
# New config
lakebench init --name polaris-test --scale 1

# Then edit lakebench.yaml:
```

```yaml
name: polaris-test

platform:
  storage:
    s3:
      endpoint: http://your-s3-endpoint:80
      access_key: YOUR_ACCESS_KEY
      secret_key: YOUR_SECRET_KEY

architecture:
  catalog:
    type: polaris    # <-- the only change from the default
  workload:
    datagen:
      scale: 1
```

### 2. Validate

```bash
lakebench validate lakebench.yaml
```

Validation confirms the `(polaris, iceberg, trino)` component combination is
supported and checks Kubernetes and S3 connectivity.

### 3. Deploy

```bash
lakebench deploy lakebench.yaml
```

You will see Polaris-specific steps in the output: PostgreSQL database
creation for Polaris, Polaris server deployment, bootstrap job execution, and
Trino configured with the REST catalog connector.

### 4. Generate and run

```bash
lakebench generate lakebench.yaml --wait
lakebench run lakebench.yaml
```

The pipeline stages (bronze-verify, silver-build, gold-finalize) and the
Trino benchmark work identically regardless of catalog type. Spark registers
Iceberg tables through the Polaris REST API instead of the Hive Thrift
protocol; Trino queries them through the same REST endpoint.

### 5. View results and clean up

```bash
lakebench report
lakebench destroy lakebench.yaml
```

Destroy handles Polaris-specific cleanup: the bootstrap job, Polaris
deployment, service, ConfigMap, and the `polaris` PostgreSQL database are all
removed.

---

## Troubleshooting

### "invalid_scope: The scope is invalid"

Trino sends `scope=catalog` by default, which Polaris rejects. This means
Trino is older than 454 or the `oauth2.scope` property is missing from the
Trino catalog configuration.

**Fix:** Ensure your Trino image is version 454 or later. Lakebench defaults
to 479, so this only happens if you have overridden the Trino image:

```yaml
images:
  trino: trinodb/trino:479    # Must be 454+
```

The required property `iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL`
is set automatically by Lakebench in the Trino ConfigMap template.

### "Failed to get subscoped credentials" / STS 400 errors

This occurs when Polaris tries to call AWS STS for credential vending, which
fails on non-AWS storage (FlashBlade, MinIO). Lakebench handles this by
creating the catalog with `stsUnavailable: true` in the storage config.

If you see this error, the most likely cause is running a Polaris version
older than 1.3.0-incubating. Versions 1.1.0 and 1.2.0 have a bug where the
credential subscoping flag is ignored in `TaskFileIOSupplier`.

**Fix:** Use Polaris 1.3.0-incubating or later (the default):

```yaml
architecture:
  catalog:
    polaris:
      version: "1.3.0-incubating"
```

### Bootstrap job fails with "already been bootstrapped"

The Polaris bootstrap command is not idempotent. If you redeploy without a
full destroy first, the bootstrap job may fail because the realm already
exists.

**Fix:** Run `lakebench destroy lakebench.yaml` before redeploying, or
manually delete the bootstrap job:

```bash
kubectl delete job lakebench-polaris-bootstrap -n <namespace>
```

Lakebench's deploy engine automatically deletes stale bootstrap jobs before
recreating them.

### Docker image tags require `-incubating` suffix

Polaris Docker images on Docker Hub use the full incubating tag. Using
`apache/polaris:1.3.0` (without `-incubating`) results in
`ImagePullBackOff` with `manifest unknown`.

**Fix:** Always include the `-incubating` suffix in the version string.
Lakebench defaults to `1.3.0-incubating` so this only affects manual
overrides.

---

## Further Reading

- [Polaris Reference](polaris-reference.md) -- full integration details,
  credential chain analysis, server configuration, and the complete list of
  bugs discovered during live deployment
- [Operators and Catalogs](operators-and-catalogs.md) -- catalog comparison
  and Stackable Hive Operator setup
