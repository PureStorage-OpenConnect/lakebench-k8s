# Observability

Reference: enable and read the optional Prometheus and Grafana stack, the per-deployment Pushgateway, local metrics and reports.

## What it does

- **Always on:** every run writes `metrics.json` and `report.html` locally. Neither needs Prometheus or Grafana.
- **Optional:** `observability.enabled: true` deploys its own Prometheus, Grafana and Pushgateway in the deployment namespace. Two deployments on the same cluster never collide; each one's stack goes with its namespace on destroy.
- The one cluster-scoped grant is a ClusterRole (`lakebench-kubelet-metrics`) that Prometheus binds to for the kubelet cAdvisor endpoint, applied once by `lakebench admin install --component observability`. Without it the stack still deploys; platform CPU and memory read `not collected` in the report.

## Version and image

- Prometheus and Grafana are pinned by digest in the config schema: `observability.prometheus_image`, `observability.grafana_image`. Pushgateway: `observability.pushgateway_image`. See the [version matrix](compatibility-matrix.md#component-version-matrix).
- The JMX exporter sidecar image is `images.jmx_exporter`, pinned by digest.

## Configuration keys

`observability` is a flat model; nested YAML such as `metrics.prometheus.enabled` is rejected. Defaults from `ObservabilityConfig` in `config/schema.py`.

| Key | Default | Effect |
|---|---|---|
| `enabled` | `false` | Deploy Prometheus, Grafana and the Pushgateway in the deployment namespace. |
| `dashboards_enabled` | `true` | Apply the Lakebench Overview dashboard ConfigMap. Grafana itself still runs. |
| `retention` | `"7d"` | Prometheus data retention. |
| `storage` | `"10Gi"` | Prometheus PVC size. |
| `prometheus_image` | [matrix](compatibility-matrix.md#component-version-matrix) | Prometheus image. |
| `grafana_image` | [matrix](compatibility-matrix.md#component-version-matrix) | Grafana image. |
| `prometheus_storage_class` | `""` | StorageClass of the Prometheus PVC; empty uses the cluster default. |
| `pushgateway_enabled` | `true` | Per-deployment Pushgateway for live datagen and pipeline metrics. |
| `pushgateway_image` | [matrix](compatibility-matrix.md#component-version-matrix) | Pushgateway image. |
| `pushgateway_storage` | `"1Gi"` | Pushgateway PVC size. |
| `pushgateway_storage_class` | `"px-csi-scratch"` | Pushgateway PVC StorageClass. |

- Removed keys are listed under 1.7.2 in the [changelog](../CHANGELOG.md).

## Deploy and destroy

Everything in the stack lives in the deployment namespace (ownership category 1), except one cluster-scoped ClusterRole and the per-deployment ClusterRoleBinding deploy creates to attach to it.

- **Admin install:** a cluster admin runs `lakebench admin install --component observability <config>` once per cluster. It creates the `lakebench-kubelet-metrics` ClusterRole. No namespace is created, no CRDs, no Helm chart. Idempotent: an existing role is left alone.
- **Deploy:** `lakebench deploy` applies Prometheus, Grafana, the Pushgateway and (when the ClusterRole is present) a per-deployment ClusterRoleBinding named `lakebench-kubelet-metrics-<namespace>`. It waits until the Prometheus pod is Ready; a Grafana that is slow to start is a warning, not a failure. A missing ClusterRole is a warning; the stack still deploys and platform CPU and memory read `not collected` in the report.
- **Destroy:** deletes the per-deployment ClusterRoleBinding; the rest goes with the namespace or by the category1 step when the namespace survives.
- **Removing the ClusterRole** when no deployment uses it: a cluster admin runs `kubectl delete clusterrole lakebench-kubelet-metrics`.

Service names are fixed, so two deployments never clash:

```bash
kubectl get svc -n <namespace> lakebench-prometheus lakebench-grafana lakebench-pushgateway
```

### Prometheus scrape targets

Prometheus runs with a scrape config that targets this deployment only:

- The Pushgateway in this namespace (`honor_labels: true` so pushed `run_id`, `stage` and `job` labels survive).
- Trino coordinator and workers in this namespace (JMX exporter on port 9090).
- The kubelet cAdvisor endpoint on every node, filtered at relabel time to this namespace. The per-pod CPU, memory, network and filesystem families are kept; the rest is dropped.

Spark pipeline jobs run with the Spark UI off (it guards driver memory at large scale). Spark serves its Prometheus endpoint from the UI, so no Spark engine metrics (GC, shuffle, spill, S3A I/O) are collected. Spark stages report rows, bytes and elapsed time through the Pushgateway and `metrics.json`.

### Pushgateway

Each deployment gets a Deployment, a Service, a 1Gi PVC on `px-csi-scratch` by default, in its own namespace. The deployment's Prometheus scrapes it with `honor_labels: true`.

- Datagen pods push live progress, because a batch pod can finish between two Prometheus scrapes.
- Spark stages that push their metrics: AML bronze-verify, silver-build, gold-finalize and silver-stream; Customer 360 gold-finalize.
- Stages that do not push: Customer 360 bronze-verify, silver-build and continuous stages; bronze-ingest; gold-refresh. Their panels stay empty, and continuous lag and per-cycle cost are not shown live.
- The push is best-effort. A failed push never affects a run; `metrics.json` stays the record.

## Grafana

- Included when `dashboards_enabled` is `true`. Grafana itself runs either way.
- User `admin`. Deploy generates a per-deployment password into the Secret `lakebench-grafana` (key `admin-password`).
- Access:

```bash
kubectl port-forward -n <namespace> svc/lakebench-grafana 3000:3000
kubectl get secret -n <namespace> lakebench-grafana -o jsonpath='{.data.admin-password}' | base64 -d
```

The **Lakebench Overview** dashboard is provisioned from a ConfigMap in the deployment namespace. A `run_id` variable selects among the runs that landed in this namespace. Panels:

- datagen throughput (MB/s per pod, labelled as capped by the pod resources Lakebench sets), rows written by pod, phase seconds
- bronze/silver stage rows (input vs output), silver per-table row counts, pipeline stage elapsed seconds
- Trino running queries and query throughput
- CPU usage by pod (containers only, each counted once)

The datagen and pipeline panels read Pushgateway series, so they are empty with `pushgateway_enabled: false`. The `run_id` variable comes from the pipeline stage series, so on a fresh deployment the datagen panels stay empty until the first Spark stage reports.

## Local metrics

Every `lakebench run` writes `lakebench-output/runs/run-<id>/metrics.json`. It holds:

- **Pipeline scores:** time-to-value, throughput and efficiency (batch); freshness, sustained throughput and latency profile (continuous).
- **Per-stage metrics:** elapsed time, input/output sizes, row counts, throughput and allocated executor resources for bronze, silver, gold and query.
- **Query results:** per-query elapsed time, row counts and pass/fail.
- **Config snapshot:** the full run configuration, for cross-run comparison.

`MetricsCollector` (`metrics/collector.py`) records job, query and streaming metrics and measured bucket sizes. `build_pipeline_benchmark()` turns the job list into the stage matrix and computes aggregate scores.

### Platform metrics

After a run, `PlatformCollector` queries the deployment's own Prometheus (`http://lakebench-prometheus.<namespace>.svc:9090`) over the run window: CPU and memory per pod, and Trino completed and failed queries. The report's Platform Metrics section shows them, or why they were not collected (observability off, Prometheus not reachable, kubelet role not bound, or no pod series). No S3 I/O and no Spark engine metrics are collected.

### S3 metrics

No S3 I/O metrics are collected; the report and `metrics.json` record S3 request counts and latency as `null` (not collected).

## Reports

- Each run writes `report.html` into its run directory once, at the end. It is the shareable artifact.
- `lakebench report` prints the summary for a run. `--render` writes a fresh `lakebench-output/reports/report-<run-id>-<ts>.html` and never touches the delivered `report.html`. Flags: [cli-reference.md](cli-reference.md#report).
- Report layout: [HTML report layout](benchmarking/html-report.md).

## Troubleshooting

- [Trino metrics show only JVM metrics](troubleshooting.md#trino-metrics-show-only-jvm-metrics)

## See also

[Benchmarking](benchmarking.md), [Running pipelines](running-pipelines.md), [CLI reference](cli-reference.md), [Configuration](configuration.md).
