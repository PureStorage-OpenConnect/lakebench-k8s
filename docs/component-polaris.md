# Polaris

Reference: configure the Apache Polaris REST catalog: config keys, version limits, sizing and deploy behaviour.

## What it does

[Apache Polaris](https://github.com/apache/polaris) is an Iceberg REST catalog with OAuth2 authentication and fine-grained access control. It is the alternative to Hive Metastore.

- `lakebench init` writes the Polaris recipe `polaris-iceberg-spark-trino` by default.
- In an existing config, set `recipe: polaris-iceberg-spark-trino`. Setting only `architecture.catalog.type: polaris` under a `hive-*` recipe is refused at load.
- `lakebench deploy` deploys Polaris and `lakebench destroy` removes it. No Hive Metastore is deployed.
- Commands, stages, table names and benchmark queries are the same as with Hive. Spark registers tables through the Polaris REST API instead of the Hive Thrift protocol. Trino queries through the same REST endpoint.
- Polaris serves only catalog metadata. It processes no query data.
- Recipes: `polaris-iceberg-spark-trino`, `polaris-iceberg-spark-thrift`, `polaris-iceberg-spark-duckdb`, `polaris-iceberg-spark-none` ([Recipes](recipes.md)).

## Version and image

`images.polaris` and `images.polaris_admin_tool` set the images (defaults: [version matrix](compatibility-matrix.md#component-version-matrix)). Keep both tags on the same release.

| Component | Minimum | Default | Why the minimum |
|---|---|---|---|
| Apache Polaris | 1.3.0-incubating | 1.6.0 | 1.1.0 and 1.2.0 attempt STS even when told not to. |
| Trino | 454 | 483 | 454 added `iceberg.rest-catalog.oauth2.scope`. |

- **Polaris 1.3.0.** In 1.1.0 and 1.2.0, `TaskFileIOSupplier` tries STS credential subscoping even when told not to ([apache/polaris#379](https://github.com/apache/polaris/issues/379)). Server-side S3 operations then fail on non-AWS storage. 1.3.0 and later contain the fix ([PR #400](https://github.com/apache/polaris/pull/400)).
- **Tag suffix:** only 1.3.0 carries `-incubating` (`apache/polaris:1.3.0` does not exist). 1.4.0 and later have no suffix.
- **Trino 454** added `oauth2.scope` ([PR #22961](https://github.com/trinodb/trino/pull/22961)). Without it, Trino sends `scope=catalog`, which Polaris rejects as `invalid_scope`. The default, 483, also has the native S3 file system that current Trino releases need.
- **STS skip:** the bootstrap creates the catalog with `stsUnavailable: true` and `pathStyleAccess: true`, because FlashBlade and most on-premises stores have no STS. This is the only supported way to skip STS on non-AWS S3.
- Do not add a server-wide credential-subscoping override. It drops the endpoint and path-style settings, and server-side S3FileIO falls back to `s3.amazonaws.com`.

## Configuration keys

Defaults from `PolarisConfig` and `PolarisResourcesConfig` in `config/schema.py`.

```yaml
recipe: polaris-iceberg-spark-trino
architecture:
  catalog:
    type: polaris            # optional: the recipe sets it
    polaris:
      # client_secret: optional; deploy generates one per deployment
      port: 8181
      resources:
        cpu: "1"
        memory: "2Gi"
```

| Key | Default | Effect |
|---|---|---|
| `architecture.catalog.type` | `hive` | `polaris` deploys Polaris instead of Hive Metastore. |
| `polaris.client_secret` | `""` | OAuth2 client secret for the `lakebench` root principal. See [Client secret](#client-secret). |
| `polaris.port` | `8181` | REST API port. Rarely needs changing. |
| `polaris.resources.cpu` | `"1"` | CPU request and limit. Raise to `"2"` at scale 100+ if table commits or metadata reads slow down. |
| `polaris.resources.memory` | `"2Gi"` | Memory request and limit (JVM heap). Raise to `"4Gi"` if Polaris OOMs during concurrent table commits. |
| `images.polaris`, `images.polaris_admin_tool` | [matrix](compatibility-matrix.md#component-version-matrix) | Server and bootstrap init-container images. |

### Client secret

- Empty: `deploy` generates one per deployment, once, and stores it in the Secret `lakebench-polaris-client` in the namespace. `run`, `benchmark`, Trino and Spark Thrift read it from there.
- Set before the first deploy (for example `"${LAKEBENCH_POLARIS_CLIENT_SECRET}"`): it is used as is and stored in that Secret.
- A bootstrapped Polaris keeps its secret. `deploy` refuses a config value that differs from the stored one. It stops when neither the config nor the Secret holds it.
- The Polaris DB password is per deployment too (Secret `lakebench-polaris-db`). Each deploy sets the `polaris` role to it.

## Sizing

The defaults (1 CPU, 2Gi) handle pipelines up to ~1 TB. Needs grow with concurrent catalog operations (table commits from many Spark executors), not data volume.

| Scale factor | CPU | Memory |
|---|---|---|
| 1-50 | 1 | 2Gi |
| 51 and above | 2 | 4Gi |

## Deploy and destroy

`lakebench deploy` creates:

- **Deployment** `lakebench-polaris` (1 replica): REST server on port 8181 (API) and 8182 (health and metrics). Readiness probe `/q/health/ready`, liveness `/q/health/live`, both on 8182.
- **Service** `lakebench-polaris` (ClusterIP): `lakebench-polaris.<namespace>.svc.cluster.local:8181`.
- **Bootstrap Job** `lakebench-polaris-bootstrap`: runs once after the server is healthy and the REST API answers on 8181.
  1. Init container: `polaris-admin-tool bootstrap` creates the realm and root principal.
  2. Main container: gets an OAuth2 token. It creates the principal roles, the catalog, and namespaces `default`, `bronze`, `silver`, `gold` via the REST API.
- Metadata goes into a `polaris` database the deployer creates on the deployment's PostgreSQL.

The engines are set up to match:

| Engine | Catalog settings |
|---|---|
| Spark | Iceberg REST catalog (`catalog-impl=org.apache.iceberg.rest.RESTCatalog`), OAuth2 client credentials, static S3 access keys. |
| Trino | `iceberg.catalog.type=rest`, `oauth2.scope=PRINCIPAL_ROLE:ALL`, native S3 file system (`fs.native-s3.enabled=true`). Init containers wait for Polaris instead of the Hive Metastore. |

The bootstrap is not idempotent: re-running it on an existing realm fails. Lakebench deletes stale bootstrap jobs before creating new ones and treats "already bootstrapped" errors as success. To redeploy Polaris, run `lakebench destroy` first.

`lakebench destroy`:

- Deletes the bootstrap Job and the Polaris Deployment, Service and ConfigMap.
- Does not drop the `polaris` database on its own. It lives on the PostgreSQL PVC, which destroy deletes.
- With `platform.kubernetes.create_namespace: false`, leaves the namespace. It still unregisters the tables from the catalog first.

## Troubleshooting

- [Polaris bootstrap Job fails or times out](troubleshooting.md#polaris-bootstrap-job-fails-or-times-out)
- [Polaris credential vending fails](troubleshooting.md#polaris-credential-vending-fails)
- [Polaris 1.3.0 image fails with ImagePullBackOff](troubleshooting.md#polaris-130-image-fails-with-imagepullbackoff)
- [Trino queries fail with Polaris: "scope not valid"](troubleshooting.md#trino-queries-fail-with-polaris-scope-not-valid)

## See also

[Getting started](getting-started.md), [Choosing a catalog](recipes.md#choosing-a-catalog), [Hive Metastore](component-hive.md), [Trino](component-trino.md), [Configuration](configuration.md).
