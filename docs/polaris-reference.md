# Apache Polaris Integration -- State & Reference

This document captures the full state of the Polaris REST catalog integration
with lakebench, including all bugs found, their root causes, workarounds, and
upstream references. It serves as the definitive reference for anyone working
on the Polaris recipe.

---

## Current State (2026-02-09)

| Item | Status |
|------|--------|
| Config schema | Done -- `PolarisConfig`, `PolarisResourcesConfig`, 3 supported combos |
| PolarisDeployer | Done -- deploy, DB creation, bootstrap, destroy |
| Spark catalog config | Done -- REST catalog with OAuth2 + static S3 creds |
| Trino catalog config | Done -- REST catalog with OAuth2 + native S3 FS |
| K8s templates | Done -- configmap, deployment, service, bootstrap-job |
| Unit tests | 402 passing (17 Polaris-specific) |
| Live deployment | Deployed at scale 10 on OpenShift + FlashBlade |
| **Pipeline run** | **Passed** -- all 3 stages completed on first attempt |
| **Trino benchmark** | **Passed** -- all 8 queries return data (QpH: 71.4) |

### Pipeline Results (Scale 10)

```
bronze-verify:  75s   (1 attempt)
silver-build:  270s   (1 attempt)
gold-finalize: 165s   (1 attempt)
Total:         510s
Benchmark:     403s   (8/8 queries PASS)
Power QpH:     71.4
```

### Trino Version Requirement: 479 (minimum 454)

Trino **479** is the default version. Minimum **454** is required for Polaris
OAuth2 scope support. The `iceberg.rest-catalog.oauth2.scope` property was
added in [Trino PR #22961](https://github.com/trinodb/trino/pull/22961)
(milestone 454). Without it, Trino sends `scope=catalog` which Polaris
rejects as `invalid_scope`.

Trino 479 requires the native S3 file system (`fs.native-s3.enabled=true`
+ `s3.*` properties). Legacy `hive.s3.*` properties were deprecated in Trino 470
and removed by 479.

### Minimum Version: 1.3.0-incubating

Polaris **1.3.0-incubating** is the minimum required version for FlashBlade
deployments. Versions 1.1.0 and 1.2.0 have a bug where
`TaskFileIOSupplier` ignores `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`,
and 1.3.0 is required for correct `stsUnavailable` catalog-level support.

- **Upstream bug:** [apache/polaris#379](https://github.com/apache/polaris/issues/379)
- **Upstream fix:** [PR #400](https://github.com/apache/polaris/pull/400) (merged Dec 18, 2024)
- **Why not 1.2.0?** Bug was reported Oct 17, 2024; 1.2.0 released Oct 28, 2024
  but does NOT include the fix (PR merged Dec 18, after 1.2.0 release).

### Version Timeline

| Version | Release Date | Has Fix? |
|---------|-------------|----------|
| 1.1.0-incubating | Sep 19, 2024 | No -- `TaskFileIOSupplier` ignores the flag |
| 1.2.0-incubating | Oct 28, 2024 | No -- bug reported but fix not yet merged |
| 1.3.0-incubating | Jan 16, 2026 | **Yes** -- PR #400 merged into main Dec 18, 2024 |

---

## Architecture

### Component Topology

```
                  ┌──────────────────┐
                  │   PostgreSQL     │
                  │ DB: polaris      │
                  │ (metadata store) │
                  └────────┬─────────┘
                           │ JDBC
┌─────────────┐     ┌──────┴───────┐     ┌──────────────────┐
│  FlashBlade │◄────│   Polaris     │◄────│  ConfigMap       │
│  (S3 API)   │     │   Server     │     │  application.    │
│  HTTP :80   │     │  8181 (REST) │     │  properties      │
│  path-style │     │  8182 (health│     └──────────────────┘
│  no STS     │     └──────┬───────┘
│             │            │ OAuth2 (client credentials)
│             │     ┌──────┴───────┐
│             │◄────│   Spark      │  catalog-impl=RESTCatalog
│             │     │  (S3A I/O)   │  uri=polaris:8181/api/catalog
│             │     └──────────────┘  credential=client:secret
│             │
│             │     ┌──────────────┐
│             │◄────│   Trino      │  iceberg.catalog.type=rest
│             │     │  (S3 I/O)    │  rest-catalog.uri=polaris:8181
│             │     └──────────────┘  oauth2.credential=client:secret
└─────────────┘
```

### Credential Flow (FlashBlade -- No STS)

FlashBlade does not support AWS STS. This means Polaris **cannot vend
temporary credentials** to clients. Every component needs static S3 credentials.

| Component | Auth to Polaris | Auth to FlashBlade S3 |
|-----------|----------------|-----------------------|
| Polaris server | N/A | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars |
| Spark | OAuth2 `credential=client_id:client_secret` | `s3.access-key-id` / `s3.secret-access-key` on catalog config |
| Trino | OAuth2 `oauth2.credential=client_id:client_secret` | `s3.aws-access-key` / `s3.aws-secret-key` env vars (native S3 FS) |

### FlashBlade S3 Settings

| Setting | Value | Why |
|---------|-------|-----|
| `endpoint` | `http://<IP>:80` | HTTP, not HTTPS. Port 80 is FlashBlade default |
| `pathStyleAccess` | `true` | FlashBlade doesn't support virtual-hosted bucket addressing |
| `stsUnavailable` | `true` | FlashBlade has no AWS STS |
| `region` | `us-east-1` | Required by AWS SDK but meaningless for FlashBlade |
| `allowedLocations` | `["s3://lb-bronze/", "s3://lb-silver/", "s3://lb-gold/"]` | Constrains catalog's S3 path scope |

### PostgreSQL Sharing

Polaris reuses the existing `lakebench-postgres` StatefulSet with a separate database:
- `hive` DB, `hive` user -- for Hive Metastore
- `polaris` DB, `polaris` user -- for Polaris (auto-created by `PolarisDeployer._create_polaris_db()`)

### Bootstrap Sequence

1. **Init container** (`apache/polaris-admin-tool:1.3.0-incubating`):
   `java -jar /deployments/polaris-admin-tool.jar bootstrap -r POLARIS -c "POLARIS,lakebench,<secret>"`
   Creates realm + root principal via direct DB access.

2. **Main container** (`curlimages/curl`):
   Obtains OAuth2 token, creates catalog via REST API with `stsUnavailable: true`,
   `pathStyleAccess: true`, and `endpoint` in the catalog storage config.

---

## The S3FileIO Credential Chain (Critical for FlashBlade)

This section documents the most complex and error-prone aspect of the Polaris +
FlashBlade integration: how Polaris server-side S3FileIO obtains credentials
and endpoint configuration.

### Background: When Does Polaris Access S3 Server-Side?

Polaris accesses S3 during **table commit operations** -- when Spark sends a
`POST /api/catalog/v1/{warehouse}/namespaces/{ns}/tables/{table}` request to
create or update a table, Polaris needs to read/write Iceberg metadata files
(metadata.json, manifest lists) on S3. It creates an `S3FileIO` instance for this.

### The S3FileIO Initialization Chain

```
Spark sends CreateTableRequest / UpdateTableRequest
  → IcebergCatalogHandler
    → IcebergCatalog.loadFileIOForTableLike()
      → StorageAccessConfigProvider.getStorageAccessConfig()
        → AwsCredentialsStorageIntegration.getSubscopedCreds()
      → DefaultFileIOFactory.loadFileIO(storageAccessConfig, ioImplClassName, tableProperties)
        → CatalogUtil.loadFileIO("org.apache.iceberg.aws.s3.S3FileIO", mergedProperties)
```

### What `StorageAccessConfig` Contains

The `StorageAccessConfig` returned by `getStorageAccessConfig()` has three maps:

| Map | Contents | Merged Order |
|-----|----------|-------------|
| `credentials()` | `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token` | First (lowest priority) |
| `extraProperties()` | `s3.endpoint`, `s3.path-style-access`, `client.region` | Second |
| `internalProperties()` | `s3.endpoint` (internal override) | Last (highest priority) |

These are merged onto `tableProperties` in `DefaultFileIOFactory.loadFileIO()`.

### The Two Feature Flags and Their Effects

#### `stsUnavailable=true` (Catalog-Level) -- **CORRECT for FlashBlade**

Set on the catalog's `AwsStorageConfigurationInfo` during creation.

When `stsUnavailable=true`:
- **STS AssumeRole is SKIPPED** -- no call to `sts.amazonaws.com`
- **Endpoint IS passed** to `StorageAccessConfig` (`s3.endpoint` from catalog config)
- **pathStyleAccess IS passed** (`s3.path-style-access` from catalog config)
- **Region IS passed** (`client.region` from catalog config)
- **Credentials are NOT set** -- the SDK falls back to the default credential chain
  (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars)

This is the correct configuration because the S3FileIO gets the FlashBlade
endpoint + path-style config, and picks up credentials from env vars.

#### `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true` (Server-Level) -- **WRONG for FlashBlade**

Set as a Polaris feature flag (server-wide).

When `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true`:
- Returns an **EMPTY** `StorageAccessConfig.builder().build()`
- **NO endpoint** -- S3FileIO defaults to `s3.amazonaws.com`
- **NO pathStyleAccess** -- defaults to virtual-hosted style
- **NO region** -- defaults to SDK's region resolution
- **NO credentials** -- falls back to default credential chain

This flag is designed for single-tenant AWS deployments where the server's
default credentials and endpoint are sufficient. It is **incompatible** with
non-AWS S3 storage because it drops the endpoint and path-style config.

### Why `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` Fails on FlashBlade

```
SKIP=true → StorageAccessConfig is EMPTY
  → S3FileIO has no endpoint → uses s3.amazonaws.com
  → S3FileIO sends FlashBlade access key to AWS S3
  → AWS/FlashBlade says "Access Key Id does not exist"
```

Even when the S3FileIO does reach FlashBlade (via some other mechanism),
the lack of `pathStyleAccess=true` can cause bucket resolution failures.

### Why `stsUnavailable=true` Works

```
stsUnavailable=true → STS skipped, BUT StorageAccessConfig populated with:
  - s3.endpoint = http://<FLASHBLADE_IP>:80
  - s3.path-style-access = true
  - client.region = us-east-1
  → S3FileIO connects to FlashBlade with correct settings
  → Credentials from AWS_ACCESS_KEY_ID env var
  → Works!
```

### Summary: Feature Flag Comparison

| Property | `stsUnavailable=true` | `SKIP_CREDENTIAL=true` |
|----------|----------------------|----------------------|
| STS AssumeRole | Skipped | Skipped (whole StorageAccess skipped) |
| S3 endpoint | From catalog config | **NOT SET** (defaults to AWS) |
| Path-style access | From catalog config | **NOT SET** (defaults to false) |
| Region | From catalog config | **NOT SET** |
| S3 credentials | SDK default chain (env vars) | SDK default chain (env vars) |
| **Works with FlashBlade?** | **YES** | **NO** |

### The `polaris.storage.aws.access-key` / `secret-key` Properties

These properties (`application.properties`) configure the AWS credentials
used to create the **STS client** for AssumeRole. They are NOT passed to
the S3FileIO. When `stsUnavailable=true`, they are not used for STS but
serve as a fallback credential source for the AWS SDK default chain.

### SmallRye Config Env Var Limitation

The `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` flag can be set via env var
as `POLARIS_FEATURES__SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true` (double
underscore for the quoted key boundary in SmallRye Config). However, SmallRye
Config's env var reverse-mapping for dynamic map keys (`@ConfigMapping`
`Map<String, String>`) is unreliable -- the property must be "known" from
another config source for the reverse lookup to work. This means:

- **Properties file** (`application.properties`): Reliable
- **Env vars**: Unreliable for map keys like feature flags
- **Java system properties** (`-D`): Reliable

For the FlashBlade use case, none of these matter because
`SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` should **not** be used at all.

---

## Spark Configuration

### Polaris (REST Catalog)

```properties
spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.catalog-impl=org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.lakehouse.uri=http://lakebench-polaris.<ns>.svc.cluster.local:8181/api/catalog
spark.sql.catalog.lakehouse.warehouse=lakehouse
spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakehouse.s3.endpoint=http://<FLASHBLADE_IP>:80
spark.sql.catalog.lakehouse.s3.path-style-access=true
spark.sql.catalog.lakehouse.credential=lakebench:<polaris_client_secret>
spark.sql.catalog.lakehouse.scope=PRINCIPAL_ROLE:ALL
spark.sql.catalog.lakehouse.token-refresh-enabled=true
# Static S3 credentials (FlashBlade has no STS -- Polaris can't vend them)
spark.sql.catalog.lakehouse.s3.access-key-id=<access_key>
spark.sql.catalog.lakehouse.s3.secret-access-key=<secret_key>
```

### Hive (for comparison)

```properties
spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type=hive
spark.sql.catalog.lakehouse.uri=thrift://lakebench-hive-metastore.<ns>.svc.cluster.local:9083
spark.sql.catalog.lakehouse.warehouse=s3a://lb-silver/warehouse/
spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lakehouse.s3.endpoint=http://<FLASHBLADE_IP>:80
spark.sql.catalog.lakehouse.s3.path-style-access=true
spark.sql.catalog.lakehouse.hive.metastore-timeout=5m
```

### Key Differences

| Property | Hive | Polaris |
|----------|------|---------|
| `type` | `hive` | _(not set)_ |
| `catalog-impl` | _(not set)_ | `org.apache.iceberg.rest.RESTCatalog` |
| `uri` | `thrift://...metastore:9083` | `http://...polaris:8181/api/catalog` |
| `warehouse` | `s3a://bucket/warehouse/` | `lakehouse` (Polaris catalog name) |
| `credential` | _(not set)_ | `lakebench:<secret>` (OAuth2) |
| `scope` | _(not set)_ | `PRINCIPAL_ROLE:ALL` |
| `s3.access-key-id` | _(not set)_ | Static FlashBlade key (no STS) |
| `s3.secret-access-key` | _(not set)_ | Static FlashBlade secret (no STS) |

---

## Trino Configuration

### Polaris (REST) -- Trino 479+

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakebench-polaris.<ns>.svc.cluster.local:8181/api/catalog
iceberg.rest-catalog.warehouse=lakehouse
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=lakebench:<polaris_client_secret>
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
# S3 I/O -- native S3 FS (legacy hive.s3.* removed in Trino 479)
fs.native-s3.enabled=true
s3.endpoint=http://<FLASHBLADE_IP>:80
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
```

### Hive (for comparison) -- Trino 479+

```properties
connector.name=iceberg
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://lakebench-hive-metastore:9083
fs.native-s3.enabled=true
s3.endpoint=http://<FLASHBLADE_IP>:80
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
```

### Trino Version Notes

- **`oauth2.scope`** -- Required for Polaris. Added in Trino 454 ([PR #22961](https://github.com/trinodb/trino/pull/22961)).
  Without it, Trino sends `scope=catalog` which Polaris rejects. Trino 435 cannot work with Polaris.
- **Native S3 FS** -- `fs.native-s3.enabled=true` + `s3.*` props required in Trino 479.
  Legacy `hive.s3.*` properties deprecated in 470, removed by 479.

---

## Polaris Server Configuration

### Env Vars (Deployment template)

| Env Var | Value | Purpose |
|---------|-------|---------|
| `POLARIS_PERSISTENCE_TYPE` | `relational-jdbc` | PostgreSQL persistence |
| `QUARKUS_DATASOURCE_DB_KIND` | `postgresql` | DB driver |
| `QUARKUS_DATASOURCE_USERNAME` | `polaris` | DB user |
| `QUARKUS_DATASOURCE_PASSWORD` | `lakebench-polaris-2024` | DB password |
| `QUARKUS_DATASOURCE_JDBC_URL` | `jdbc:postgresql://...postgres:5432/polaris` | JDBC URL |
| `AWS_ACCESS_KEY_ID` | From Secret | FlashBlade S3 access key |
| `AWS_SECRET_ACCESS_KEY` | From Secret | FlashBlade S3 secret key |
| `AWS_REGION` | `us-east-1` | Required by AWS SDK |

### application.properties (ConfigMap)

Mounted at `/deployments/config/application.properties` via a ConfigMap
(`lakebench-polaris-config`).

```properties
# S3 credentials for server-side FileIO
polaris.storage.aws.access-key=<access_key>
polaris.storage.aws.secret-key=<secret_key>
```

**Important:** Do NOT set `polaris.features."SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION"=true`
in this file. See the "S3FileIO Credential Chain" section above for why.

### Catalog Storage Config

The catalog must be created with `stsUnavailable: true` in the storage config.
If upgrading from a version where this wasn't set, update via the REST API:

```bash
curl -X PUT .../api/management/v1/catalogs/lakehouse \
  -d '{"currentEntityVersion": N, "storageConfigInfo": {"stsUnavailable": true, ...}}'
```

---

## Docker Images

| Image | Purpose | Notes |
|-------|---------|-------|
| `apache/polaris:1.3.0-incubating` | Polaris server | All tags have `-incubating` suffix |
| `apache/polaris-admin-tool:1.3.0-incubating` | Bootstrap CLI | JAR at `/deployments/polaris-admin-tool.jar` |

**Tags without `-incubating` do NOT exist on Docker Hub** -- `apache/polaris:1.1.0`
returns `manifest unknown`.

The admin-tool has **no `polaris` binary** in PATH. Must invoke as:
```
java -jar /deployments/polaris-admin-tool.jar bootstrap ...
```

### SDK Versions in Polaris 1.3.0

| Library | Version | Notes |
|---------|---------|-------|
| AWS SDK | v2 (`software.amazon.awssdk:s3:2.39.2`) | Different signing from Spark's SDK v1 |
| Iceberg | 1.10.0 (`org.apache.iceberg:iceberg-aws`) | Server-side FileIO |
| Quarkus | SmallRye Config | Dynamic map key env vars are unreliable |

Spark uses AWS SDK v1 (`com.amazonaws:aws-java-sdk-bundle:1.12.262`). The
different signing implementations mean credential/endpoint behavior differs
between the two -- what works for Spark may not work for Polaris server-side.

---

## Deployment Bugs (Complete List)

All bugs discovered during live deployment on OpenShift + FlashBlade. Each is
fixed in the codebase.

### BUG-P01: Docker image tag requires `-incubating` suffix
- **Symptom:** `ImagePullBackOff` -- `manifest unknown`
- **Fix:** Default version set to `1.3.0-incubating` (minimum for FlashBlade)

### BUG-P02: Admin-tool entrypoint is `java -jar`, not `polaris`
- **Symptom:** `CreateContainerError` -- `executable file 'polaris' not found`
- **Fix:** `java -jar /deployments/polaris-admin-tool.jar bootstrap ...`

### BUG-P03: Bootstrap command is NOT idempotent
- **Symptom:** `IllegalArgumentException: already been bootstrapped`
- **Fix:** Shell wrapper catches error + deployer deletes old K8s Job before recreation

### BUG-P04: PostgreSQL `\gexec` doesn't work via Kubernetes exec
- **Symptom:** Silent failure -- no DB/user created, then `password authentication failed`
- **Fix:** `DO $$ BEGIN...END $$;` blocks + SELECT-then-CREATE pattern

### BUG-P05: STS credential subscoping env var name
- **Symptom:** `RESTException: Failed to get subscoped credentials (Sts, 400)`
- **Cause:** Pre-Quarkus name silently ignored; correct name uses double underscore
- **Fix:** `POLARIS_FEATURES__SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true`
- **BUT:** Even with correct env var, flag is ignored in 1.1.0/1.2.0 (BUG-P09)
- **AND:** Even when the flag works (1.3.0), it's wrong for FlashBlade (BUG-P10)

### BUG-P06: Trino 435 rejects `oauth2.scope` and `oauth2.server-uri`
- **Symptom:** Trino 435 crashes at startup -- `Configuration property was not used`
- **Original fix:** Removed both properties from Trino configmap template
- **Superseded by BUG-P11:** Upgraded to Trino 479 which supports `oauth2.scope` (added in 454).
  `oauth2.scope=PRINCIPAL_ROLE:ALL` is now required and present in the template.

### BUG-P07: Trino init container waits for Hive when using Polaris
- **Symptom:** Trino pod hangs in init
- **Fix:** Jinja2 conditional: `wait-for-polaris` vs `wait-for-hive`

### BUG-P08: Spark scripts use Python 3.10+ syntax on Python 3.9
- **Symptom:** `TypeError: 'type' object is not subscriptable`
- **Fix:** `from __future__ import annotations` in silver_build.py and gold_finalize.py

### BUG-P09: `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` ignored in TaskFileIOSupplier (UPSTREAM)
- **Symptom:** Same as BUG-P05 but persists even with correct env var
- **Cause:** `TaskFileIOSupplier` class always tries to get subscoped credentials,
  ignoring the feature flag. Known upstream bug.
- **Upstream issue:** [apache/polaris#379](https://github.com/apache/polaris/issues/379)
- **Upstream fix:** [apache/polaris PR #400](https://github.com/apache/polaris/pull/400)
  (merged Dec 18, 2024 into `main`)
- **Affected versions:** 1.1.0-incubating, 1.2.0-incubating
- **Fixed in:** **1.3.0-incubating** (released Jan 16, 2026)
- **Note:** Even with this fix, the flag should NOT be used on FlashBlade (see BUG-P10)

### BUG-P10: `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` drops endpoint + path-style config
- **Symptom:** `The AWS Access Key Id you provided does not exist in our records.
  (Service: S3, Status Code: 403)` on table commit
- **Cause:** When `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true`,
  `StorageAccessConfigProvider.getStorageAccessConfig()` returns an EMPTY
  `StorageAccessConfig.builder().build()`. This means the S3FileIO receives:
  - NO `s3.endpoint` → defaults to `s3.amazonaws.com`
  - NO `s3.path-style-access` → defaults to virtual-hosted
  - NO `client.region` → defaults to SDK region resolution
  - NO credentials → falls back to AWS SDK default chain (env vars DO work)
- **Root cause in source code:**
  ```java
  // StorageAccessConfigProvider.java
  if (skipCredentialSubscopingIndirection) {
      return StorageAccessConfig.builder().build(); // EMPTY -- drops everything
  }
  ```
- **Verified:** Same behavior on `main` branch (not just 1.3.0)
- **Impact:** The FlashBlade access key is sent to `s3.amazonaws.com` instead of
  FlashBlade, causing the "Access Key Id does not exist" error
- **Fix:** Do NOT use `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`. Instead, use
  `stsUnavailable=true` on the catalog storage config, which skips STS but
  preserves the endpoint and path-style config
- **Status:** This is a design choice in Polaris, not a bug. The flag is
  designed for single-tenant AWS deployments. For non-AWS S3, use `stsUnavailable`.

### BUG-P10 Investigation Timeline

This bug consumed significant debugging time. Here's the full timeline:

1. **Initial symptom (1.1.0):** STS 400 error on table creation. Set
   `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true` via env var.
2. **BUG-P05:** Env var name was wrong (single underscore). Fixed to double underscore.
3. **BUG-P09:** Flag still ignored -- upstream bug in `TaskFileIOSupplier`.
4. **Upgrade to 1.3.0:** PR #400 fix included. Flag now actually works.
5. **New error:** STS error gone, but "Access Key Id does not exist" appears.
   Error comes from FlashBlade (confirmed by 16-char hex request IDs).
6. **Hypothesis 1:** SmallRye Config env var mapping unreliable. Tried both
   single and double underscore env vars. Switched to `application.properties`
   file mount. Still failed.
7. **Hypothesis 2:** `polaris.storage.aws.access-key`/`secret-key` needed.
   Added to properties file. Still failed.
8. **Hypothesis 3:** Catalog properties need S3 credentials. Added
   `s3.access-key-id` etc. to catalog properties via REST API. Still failed.
9. **Root cause found:** Source code analysis revealed that when `SKIP=true`,
   `StorageAccessConfigProvider` returns an EMPTY `StorageAccessConfig`. No
   endpoint, no path-style, no credentials. S3FileIO defaults to AWS S3.
10. **Solution:** Remove `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`. Use only
    `stsUnavailable=true` on catalog. This preserves endpoint + path-style
    while skipping STS.
11. **Verified:** Pipeline completes all 3 stages on first attempt.

### BUG-P11: Trino OAuth2 scope incompatibility with Polaris

- **Symptom:** `Malformed request: invalid_scope: The scope is invalid`. Trino benchmark
  queries all return 0 rows.
- **Cause:** Trino's Iceberg REST connector sends `scope=catalog` (hardcoded in Iceberg's
  `OAuth2Properties.CATALOG_SCOPE` constant). Polaris requires `PRINCIPAL_ROLE:<role_name>`
  format. Trino 435 has no property to override this scope. Confirmed by Polaris logs:
  `Invalid scope provided. scopes=catalogscopes=catalog`.
- **Fix:** Upgraded Trino from 435 to 479. Trino 454+
  ([PR #22961](https://github.com/trinodb/trino/pull/22961)) added
  `iceberg.rest-catalog.oauth2.scope`. Set to `PRINCIPAL_ROLE:ALL` in configmap template.
- **Minimum version:** Trino **454** (we use 479 for additional improvements).

### BUG-P12: Trino 479 removed legacy `hive.s3.*` properties

- **Symptom:** `Configuration property 'hive.s3.aws-access-key' was not used` -- Trino
  coordinator crashes at startup after upgrade to 479.
- **Cause:** Trino deprecated legacy S3 file system properties (`hive.s3.*`) in version 470
  and removed them by 479. The native S3 file system is required.
- **Fix:** Updated configmap template from `hive.s3.*` to native S3 properties:
  - `fs.native-s3.enabled=true`
  - `s3.endpoint=<flashblade_url>`
  - `s3.path-style-access=true`
  - `s3.region=us-east-1`
  - `s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}`
  - `s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}`
- **Note:** Both Hive and Polaris catalog paths use the same native S3 properties.

---

## Files Changed for Polaris Support

| File | Change |
|------|--------|
| `config/schema.py` | `PolarisResourcesConfig` + `PolarisConfig`, 3 supported combos, Trino 479 default |
| `deploy/polaris.py` | PolarisDeployer (deploy, DB creation, bootstrap, destroy) |
| `deploy/engine.py` | Polaris context vars, deploy step, destroy block, ConfigMap cleanup |
| `deploy/__init__.py` | Export PolarisDeployer |
| `spark/job.py` | Catalog-type dispatch (Hive vs Polaris), static S3 creds |
| `spark/scripts/silver_build.py` | `from __future__ import annotations` (Python 3.9) |
| `spark/scripts/gold_finalize.py` | `from __future__ import annotations` (Python 3.9) |
| `templates/polaris/configmap.yaml.j2` | Polaris application.properties ConfigMap |
| `templates/polaris/deployment.yaml.j2` | Polaris Deployment with PostgreSQL + S3 env + config mount |
| `templates/polaris/service.yaml.j2` | Polaris ClusterIP Service |
| `templates/polaris/bootstrap-job.yaml.j2` | Bootstrap Job (realm, catalog, roles) |
| `templates/trino/configmap.yaml.j2` | Conditional REST vs hive_metastore, OAuth2 scope, native S3 FS |
| `config/loader.py` | Updated example config Trino image to 479 |
| `templates/trino/coordinator.yaml.j2` | Conditional wait-for-polaris vs wait-for-hive |
| `cli.py` | Polaris in logs command |
| `tests/test_config.py` | Polaris accepted, defaults, delta-rejected |
| `tests/test_spark.py` | Polaris REST catalog config test |
| `tests/test_deploy.py` | PolarisDeployer test class |

---

## Upstream References

- [Apache Polaris GitHub](https://github.com/apache/polaris)
- [Apache Polaris Releases](https://github.com/apache/polaris/releases) --1.3.0 is latest as of Feb 2026
- [Issue #379: SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION ignored in TaskFileIOSupplier](https://github.com/apache/polaris/issues/379)
- [PR #400: Fix -- pass flag to TaskFileIOSupplier](https://github.com/apache/polaris/pull/400)
- [Discussion #405: Production and Polaris credentials](https://github.com/apache/polaris/discussions/405)
- [Configuration reference (unreleased)](https://polaris.apache.org/in-dev/unreleased/configuration/)
- [SmallRye Config Environment Variables](https://smallrye.io/smallrye-config/Main/config/environment-variables/)
- [Docker Hub: apache/polaris](https://hub.docker.com/r/apache/polaris/tags) -- tags require `-incubating` suffix
- [Docker Hub: apache/polaris-admin-tool](https://hub.docker.com/r/apache/polaris-admin-tool/tags)
- [Trino PR #22961: Add scope field for Iceberg REST metastore](https://github.com/trinodb/trino/pull/22961) -- adds `oauth2.scope` in Trino 454
- [Trino blog: Out with the old file system](https://trino.io/blog/2025/02/10/old-file-system.html) -- `hive.s3.*` deprecated in 470, removed by 479

### Source Code References (Polaris 1.3.0 / main)

These files were analyzed to understand the S3FileIO credential chain:

- `StorageAccessConfigProvider.java` -- `getStorageAccessConfig()`: returns EMPTY when `SKIP=true`
- `DefaultFileIOFactory.java` -- `loadFileIO()`: merges `StorageAccessConfig` maps onto table properties
- `AwsCredentialsStorageIntegration.java` -- `getSubscopedCreds()`: populates endpoint/pathStyle when `stsUnavailable=true`
- `AwsStorageConfigurationInfo.java` -- data model for `endpoint`, `pathStyleAccess`, `stsUnavailable`
- `StorageAccessProperty.java` -- property name mappings (`s3.endpoint`, `s3.path-style-access`)
- `StorageConfiguration.java` -- `polaris.storage.aws.access-key`/`secret-key` (for STS client, NOT FileIO)
