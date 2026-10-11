# Observability

Reference: enable and read the optional Prometheus and Grafana stack, per-deployment Pushgateway, local metrics and reports.

## What it does

- **Always on:** every run writes `metrics.json` and `report.html` locally. Neither needs Prometheus or Grafana.
- **Optional:** `observability.enabled: true` uses one shared `kube-prometheus-stack` Helm release (Prometheus, Grafana, kube-state-metrics, node-exporter) and gives each deployment a Pushgateway and PodMonitors.
- Lakebench disables node-exporter on OpenShift, where it needs host access the SCCs block.
- The stack is independent of the recipe.

## Version and image

- Prometheus and Grafana have no version keys. They come from the chart `observability.chart_version` names. Defaults for it and `pushgateway_image`: [version matrix](compatibility-matrix.md#component-version-matrix).
- The JMX exporter sidecar image is `images.jmx_exporter`, pinned by digest.

## Configuration keys

`observability` is a flat model; nested YAML such as `metrics.prometheus.enabled` is rejected. Defaults from `ObservabilityConfig` in `config/schema.py`.

| Key | Default | Effect |
|---|---|---|
| `enabled` | `false` | Use the shared stack and deploy the per-deployment Pushgateway and PodMonitors. Always the full stack. |
| `dashboards_enabled` | `true` | Sets the chart's `grafana.enabled` on a fresh admin install. |
| `retention` | `"7d"` | Prometheus retention on a fresh admin install. |
| `storage` | `"10Gi"` | Prometheus PVC size on a fresh admin install. The PVC uses the cluster default StorageClass. |
| `chart_version` | [matrix](compatibility-matrix.md#component-version-matrix) | `kube-prometheus-stack` chart a fresh admin install uses. |
| `pushgateway_enabled` | `true` | Per-deployment Pushgateway for live datagen and pipeline metrics. |
| `pushgateway_image` | [matrix](compatibility-matrix.md#component-version-matrix) | Pushgateway image |
| `pushgateway_storage` | `"1Gi"` | Pushgateway PVC size |
| `pushgateway_storage_class` | `"px-csi-scratch"` | Pushgateway PVC StorageClass |

- An installed release keeps the values it was installed with.
- `observability.reports`, `storage_class`, `prometheus_stack_enabled`, `s3_metrics_enabled` and `spark_metrics_enabled` are removed ([UPGRADING-1.7.md](../UPGRADING-1.7.md#config-fields-nothing-read-are-removed)). `observability.reports` (including `include.platform_metrics`) had no effect.

## Deploy and destroy

The chart installs cluster-wide objects (CRDs, cluster roles, admission webhooks), so the release named `lakebench-observability` is a shared cluster component, not part of a deployment.

- **Install:** a cluster admin runs `lakebench admin install --component observability <config>` once. It installs into the `lakebench-observability` namespace, under the cluster lease, only when no release named `lakebench-observability` exists anywhere on the cluster. An existing release is never upgraded or modified.
- **Dashboard:** the same command applies the shared dashboard ConfigMap, and re-applies it when it differs from this Lakebench's.
- **Deploy:** `lakebench deploy` only checks that the release exists (a missing one fails the step with the install command). It applies the deployment's PodMonitors and Pushgateway in its own namespace.
- **Destroy:** never uninstalls the release; another deployment may use it. PodMonitors and Pushgateway go with the deployment's namespace. The dashboard ConfigMap stays in `lakebench-observability`.
- **Exception:** a release an older Lakebench installed into the deployment's own namespace is removed by destroy, because it served only that namespace.
- **Removing the shared stack** when no deployment uses it: `helm uninstall lakebench-observability -n lakebench-observability`.

The chart shortens service names, so list them:

```bash
kubectl get svc -n lakebench-observability -l release=lakebench-observability
```

### Prometheus targets

- Prometheus picks up the PodMonitors of every Lakebench namespace. ServiceMonitor and PodMonitor CRDs handle target discovery.
- The Spark and Trino PodMonitors are applied whenever the stack is enabled. Only the Trino one finds a target.
- Spark pipeline jobs run with the Spark UI off (it guards driver memory at large scale). Spark serves its Prometheus endpoint from the UI, so no Spark engine metrics (GC, shuffle, spill, S3A I/O) are collected. Spark stages report rows, bytes and elapsed time through the Pushgateway and `metrics.json`.

### Pushgateway

Each deployment gets a Deployment, a Service, a 1Gi PVC on `px-csi-scratch` by default, and a PodMonitor that scrapes it, in its own namespace.

- Datagen pods push live progress, because a batch pod can finish between two Prometheus scrapes.
- Spark stages that push their metrics: AML bronze-verify, silver-build, gold-finalize and silver-stream; Customer 360 gold-finalize.
- Stages that do not push: Customer 360 bronze-verify, silver-build and continuous stages; bronze-ingest; gold-refresh. Their panels stay empty, and continuous lag and per-cycle cost are not shown live.
- The push is best-effort. A failed push never affects a run; `metrics.json` stays the record.

## Grafana

- Included when `dashboards_enabled` is `true`.
- User `admin`. The chart generates the password per install into the Secret `lakebench-observability-grafana` (key `admin-password`). An install made by 1.6 keeps its `lakebench` password.
- Access: `kubectl port-forward svc/lakebench-observability-grafana 3000:80 -n lakebench-observability`.

The **Lakebench Overview** dashboard comes from one ConfigMap in `lakebench-observability`. `namespace` and `run_id` variables select the deployment and run. Panels:

- datagen throughput (MB/s per pod, labelled as capped by the pod resources Lakebench sets), rows written by pod, phase seconds
- bronze/silver stage rows (input vs output), silver per-table row counts, pipeline stage elapsed seconds
- Trino running queries and query throughput
- CPU usage by pod (containers only, each counted once)

The datagen and pipeline panels read Pushgateway series, so they are empty with `pushgateway_enabled: false`. The `namespace` and `run_id` variables come from the pipeline stage series, so on a fresh deployment the datagen panels stay empty until the first Spark stage reports.

## Local metrics

Every `lakebench run` writes `lakebench-output/runs/run-<id>/metrics.json`. It holds:

- **Pipeline scores:** time-to-value, throughput and efficiency (batch); freshness, sustained throughput and latency profile (continuous).
- **Per-stage metrics:** elapsed time, input/output sizes, row counts, throughput and allocated executor resources for bronze, silver, gold and query.
- **Query results:** per-query elapsed time, row counts and pass/fail.
- **Config snapshot:** the full run configuration, for cross-run comparison.

`MetricsCollector` (`metrics/collector.py`) records job, query and streaming metrics and measured bucket sizes. `build_pipeline_benchmark()` turns the job list into the stage matrix and computes aggregate scores.

### Platform metrics

After a run, `PlatformCollector` queries the shared Prometheus for the deployment's namespace over the run window: CPU and memory per pod, and Trino completed and failed queries. The report's Platform Metrics section shows them, or why they were not collected (observability off, Prometheus not found or not reachable, no pod series). No S3 I/O and no Spark engine metrics are collected.

### S3 metrics

`observability/s3_metrics.py` defines `S3MetricsWrapper` with `lakebench_s3_request_duration_seconds` (latency histogram), `lakebench_s3_requests_total` and `lakebench_s3_errors_total` (by operation). No code path instantiates it, so these series are not emitted, and the report and `metrics.json` record S3 request counts and latency as `null` (not collected). They would cover CLI operations (list, head, delete), not Spark or Trino data I/O.

## Reports

- Each run writes `report.html` into its run directory once, at the end. It is the shareable artifact.
- `lakebench report` prints the summary for a run. `--render` writes a fresh `lakebench-output/reports/report-<run-id>-<ts>.html` and never touches the delivered `report.html`. Flags: [cli-reference.md](cli-reference.md#report).
- Report layout: [HTML report layout](benchmarking/html-report.md).

## Troubleshooting

- [Trino metrics show only JVM metrics](troubleshooting.md#trino-metrics-show-only-jvm-metrics)

## See also

[Benchmarking](benchmarking.md), [Running pipelines](running-pipelines.md), [CLI reference](cli-reference.md), [Configuration](configuration.md).
