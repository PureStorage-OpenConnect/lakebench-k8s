# Component Reference: Apache Polaris

## Overview

Apache Polaris is an alternative catalog to Hive Metastore. It provides a
REST-based Iceberg catalog with OAuth2 authentication. In Lakebench, Polaris
replaces Hive Metastore as the metadata service -- Spark jobs and Trino both
connect to Polaris instead of the Hive Thrift protocol.

When `architecture.catalog.type` is set to `polaris`, Lakebench deploys
Polaris automatically during `lakebench deploy` and tears it down during
`lakebench destroy`. The user workflow is identical to Hive -- all commands,
table names, and benchmark queries work the same way.

## Architecture

Lakebench deploys Polaris as three Kubernetes resources:

- **Deployment** -- `lakebench-polaris` (1 replica). Runs the Polaris REST
  server on port 8181 (API) and port 8182 (health/metrics).
- **Service** -- `lakebench-polaris` (ClusterIP). Exposes the REST API at
  `lakebench-polaris.<namespace>.svc.cluster.local:8181`.
- **Bootstrap Job** -- `lakebench-polaris-bootstrap`. Creates the catalog
  realm, root principal, catalog, and namespaces (default, bronze, silver,
  gold). Runs once after the Polaris server is healthy.

Polaris stores its metadata in the shared PostgreSQL instance, using a
separate `polaris` database alongside the `hive` database.

### Health checks

- **Readiness**: HTTP GET on `/q/health/ready` (port 8182)
- **Liveness**: HTTP GET on `/q/health/live` (port 8182)

The deployer additionally waits for the REST API to respond on port 8181
before running the bootstrap job.

### Bootstrap behavior

The bootstrap job is a two-stage process:

1. **Init container** -- runs `polaris-admin-tool bootstrap` to create the
   realm and root principal.
2. **Main container** -- obtains an OAuth2 token and creates the catalog and
   namespaces via the REST API.

The bootstrap is not idempotent -- re-running it on an existing realm fails.
Lakebench handles this by deleting stale bootstrap jobs before creating new
ones, and by treating "already bootstrapped" errors as success. If you need
to redeploy Polaris, run `lakebench destroy` first.

## YAML Configuration

Polaris settings live under `architecture.catalog` in the config file.
The image is set under `images`.

```yaml
images:
  polaris: "apache/polaris:1.3.0-incubating"

architecture:
  catalog:
    type: polaris
    polaris:
      version: "1.3.0-incubating"    # Docker image tag
      port: 8181                      # REST API port
      resources:
        cpu: "1"                      # CPU request and limit
        memory: "2Gi"                 # Memory request and limit
```

### Field reference

| Field | Default | Description |
|---|---|---|
| `catalog.type` | `hive` | Set to `polaris` to deploy Polaris instead of Hive Metastore. |
| `polaris.version` | `1.3.0-incubating` | Polaris image tag. Must include the `-incubating` suffix -- `apache/polaris:1.3.0` does not exist. |
| `polaris.port` | `8181` | REST API port. Rarely needs changing. |
| `polaris.resources.cpu` | `"1"` | CPU request and limit for the Polaris pod. |
| `polaris.resources.memory` | `"2Gi"` | Memory request and limit for the Polaris pod. |

### What the overrides do

| Override | Effect | When to change |
|---|---|---|
| `polaris.resources.cpu` | Controls how much CPU Polaris gets for handling catalog requests. | Increase to `"2"` at scale 100+ if catalog operations (table commits, metadata reads) become slow. |
| `polaris.resources.memory` | JVM heap for the Polaris server. | Increase to `"4Gi"` if Polaris OOMs during concurrent table commits from multiple Spark executors. |
| `polaris.version` | Pins the Polaris image tag. | Only change if you need a newer Polaris release. Minimum is `1.3.0-incubating`. |

## Version Constraints

- **Minimum Polaris version**: 1.3.0-incubating. Versions 1.1.0 and 1.2.0
  have a credential vending bug where `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`
  is ignored, causing S3 failures on non-AWS storage.
- **Minimum Trino version**: 454 (for `oauth2.scope` support). Default is 479.
- **Image tag suffix**: all Polaris Docker tags carry `-incubating`. Omitting
  it causes an image pull failure.

## Sizing Guidance

Polaris is lightweight. The defaults (1 CPU, 2Gi) handle medallion pipeline
workloads up to ~1 TB. Polaris does not process query data -- it only serves
catalog metadata. Resource needs grow with concurrent catalog operations
(table commits from multiple Spark executors), not with data volume.

| Scale factor | Recommended CPU | Recommended memory |
|---|---|---|
| 1--50 | 1 | 2Gi |
| 51--500 | 2 | 4Gi |
| 500+ | 2 | 4Gi |

## Recipes

Polaris is used by these recipes:

- `polaris-iceberg-spark-trino`
- `polaris-iceberg-spark-thrift`
- `polaris-iceberg-spark-duckdb`
- `polaris-iceberg-spark-none`

See the [Recipes Guide](recipes.md) for all combinations.

## See Also

- [Hive Metastore](component-hive.md) -- the alternative catalog
- [Polaris Quickstart](quickstart-polaris.md) -- step-by-step setup guide
- [Trino](component-trino.md) -- query engine that connects to Polaris
- [Recipes](recipes.md) -- all supported component combinations
- [Configuration](configuration.md) -- full YAML schema reference
- [Troubleshooting](troubleshooting.md) -- common deployment issues
