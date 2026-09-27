# Observability

Lakebench deploys observability via the `kube-prometheus-stack` Helm chart, which bundles Prometheus, Grafana, node-exporter, and kube-state-metrics in a single install. Prometheus and Grafana versions are not independently configurable -- they come from whatever `observability.chart_version` (default `87.19.2`, currently bundling Prometheus v3.13.1 and Grafana v13.1.x) resolves to.

HTML reports are generated from local metrics and do not require Prometheus or Grafana.

## YAML Configuration

All observability settings live under the `observability` key as a flat model:

```yaml
observability:
  enabled: false                     # Master switch for the observability stack
  prometheus_stack_enabled: true     # Deploy kube-prometheus-stack
  dashboards_enabled: true           # Enable Grafana dashboards
  retention: "7d"                    # Prometheus data retention period
  storage: "10Gi"                    # Prometheus PVC size
  storage_class: ""                  # StorageClass (empty = cluster default)
  chart_version: "87.19.2"           # kube-prometheus-stack chart version (pins Prometheus + Grafana)
```

`observability.reports` has no effect and is removed in v1.7: every run writes
`report.html` into its run directory, and `lakebench report` regenerates it.

`s3_metrics_enabled` and `spark_metrics_enabled` exist in the schema but nothing reads
them; setting either prints a warning. Spark and S3 metrics are collected whenever the
stack is enabled.

Set `observability.enabled: true` to deploy the stack. All sub-flags (`prometheus_stack_enabled`, `dashboards_enabled`, etc.) default to `true` when the stack is enabled.

## Local Metrics (Always On)

Every `lakebench run` writes a `metrics.json` file to `lakebench-output/runs/run-<id>/`. No setup is needed. The file contains:

- **Pipeline benchmark scores** -- time-to-value, throughput, and efficiency for batch runs; freshness, sustained throughput, and latency profile for continuous runs.
- **Per-stage metrics** -- elapsed time, input/output sizes, row counts, throughput, and allocated executor resources for each pipeline stage (bronze, silver, gold, query).
- **Query results** -- per-query elapsed time, row counts, and pass/fail status from the benchmark.
- **Config snapshot** -- the full configuration used for the run, enabling cross-run comparison.

The `MetricsCollector` class in `metrics/collector.py` records job metrics, query metrics, streaming metrics, and measured S3 bucket sizes during the run. After completion, `build_pipeline_benchmark()` converts the flat job list into the unified stage-matrix view and computes aggregate scores.

## Prometheus

When `observability.enabled` is `true`, Lakebench uses one shared `kube-prometheus-stack` Helm release for the whole cluster. The chart installs cluster-wide objects (CRDs, cluster roles, admission webhooks), so it is a shared cluster component, not part of a deployment:

- `deploy` installs it into the `lakebench-observability` namespace only when no release named `lakebench-observability` exists anywhere on the cluster, under the cluster lease. An existing release is reused and never upgraded or modified.
- `destroy` never uninstalls it; another deployment may be using it. Each deployment's PodMonitors and dashboard ConfigMap live in its own namespace and go with it. The one exception is a release an older lakebench installed into the deployment's own namespace, which destroy removes because it served only that namespace.
- To remove the shared stack when no deployment uses it: `helm uninstall lakebench-observability -n lakebench-observability`.

The stack provides:

- Prometheus server that picks up the PodMonitors of every lakebench namespace
- Node-exporter and kube-state-metrics for cluster-level visibility
- ServiceMonitor and PodMonitor CRDs for automatic target discovery

The Helm release is named `lakebench-observability`. The chart shortens service names, so list them rather than guessing:

```bash
kubectl get svc -n lakebench-observability -l release=lakebench-observability
```

To deploy the observability stack alongside infrastructure, set `observability.enabled: true` in your config YAML, or pass the `--include-observability` flag:

```bash
lakebench deploy test-config.yaml --include-observability
```

## Grafana

Grafana is included in the kube-prometheus-stack install when `dashboards_enabled` is `true`. Default credentials are `admin` / `lakebench`.

Three built-in dashboards are provisioned:

| Dashboard | Panels |
|---|---|
| Lakebench Pipeline Overview | Pipeline stage durations, throughput, end-to-end timing |
| Lakebench Spark Detail | Job duration, executor utilization, shuffle I/O |
| Lakebench Storage I/O | S3 read/write throughput, object counts |

Access Grafana via port-forward:

```bash
kubectl port-forward svc/lakebench-observability-grafana 3000:80 -n lakebench-observability
```

## S3 Metrics

When `s3_metrics_enabled` is `true`, the `S3MetricsWrapper` instruments CLI-side boto3 operations with Prometheus counters and histograms:

- `lakebench_s3_request_duration_seconds` -- request latency histogram
- `lakebench_s3_requests_total` -- total request count by operation
- `lakebench_s3_errors_total` -- error count by operation

These metrics cover CLI operations (list, head, delete) -- not Spark/Trino data-path I/O.

## Platform Metrics Collection

After a benchmark run completes, the `PlatformCollector` queries the in-cluster Prometheus to snapshot infrastructure metrics (CPU, memory per pod, S3 I/O rates). These are included in the HTML report under the Platform Metrics tab when `reports.include.platform_metrics` is `true`.

## Reports

The `lakebench report` command generates an HTML report from collected metrics. Reports are written into the per-run directory as `report.html`.

```bash
# Generate report for the latest run
lakebench report

# Generate report for a specific run
lakebench report --run <run-id>

# List available runs
lakebench report --list
```

The report includes:

- **Summary cards** -- total time, job pass/fail counts, data processed, throughput, QpH score, and pipeline-level scores (time-to-value for batch, data freshness for continuous).
- **Pipeline benchmark table** -- the stage matrix showing per-stage elapsed time, input/output sizes, throughput, executor allocation, and status.
- **Job performance table** -- per-Spark-job duration, input size, output rows, throughput, and CPU-seconds allocated.
- **Query breakdown** -- per-query duration, rows returned, and pass/fail status.
- **Platform metrics** -- CPU and memory usage per pod, S3 I/O rates (when observability is enabled).
- **Configuration snapshot** -- scale factor, S3 endpoint, executor sizing, catalog type, and image versions.

`lakebench destroy` leaves the shared observability release in place (see [Prometheus](#prometheus)).

## See Also

- [Scoring and Benchmarking](benchmarking.md) -- pipeline scorecard and query engine benchmark
- [Running Pipelines](running-pipelines.md) -- deploy, generate, and run workflow
- [CLI Reference](cli-reference.md) -- full command and flag reference
- [Configuration](configuration.md) -- complete YAML schema documentation
- [Architecture](architecture.md) -- system design and component overview
