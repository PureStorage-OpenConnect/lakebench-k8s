# Observability

Lakebench deploys observability via the `kube-prometheus-stack` Helm chart, which bundles Prometheus, Grafana, kube-state-metrics and node-exporter in a single install (lakebench disables node-exporter on OpenShift, where it needs host access the SCCs block). Prometheus and Grafana versions are not independently configurable -- they come from whatever `observability.chart_version` (default `87.19.2`, currently bundling Prometheus v3.13.1 and Grafana v13.1.x) resolves to.

HTML reports are generated from local metrics and do not require Prometheus or Grafana.

## YAML Configuration

All observability settings live under the `observability` key as a flat model:

```yaml
observability:
  enabled: false                     # Master switch for the observability stack
  dashboards_enabled: true           # Enable Grafana dashboards
  retention: "7d"                    # Prometheus data retention period
  storage: "10Gi"                    # Prometheus PVC size (cluster default StorageClass)
  chart_version: "87.19.2"           # kube-prometheus-stack chart version (pins Prometheus + Grafana)
  pushgateway_enabled: true          # Per-deployment Pushgateway for live datagen and pipeline metrics
  pushgateway_image: "prom/pushgateway:v1.11.1"
  pushgateway_storage: "1Gi"         # Pushgateway persistence PVC size
  pushgateway_storage_class: "px-csi-scratch"
```

With observability enabled, each deployment also gets its own Prometheus
Pushgateway (a Deployment, a Service, a 1Gi PVC on `px-csi-scratch` by
default, and a PodMonitor that scrapes it) in the deployment's namespace,
removed with the namespace by `destroy`. Datagen pods push live progress to
it, because a batch pod can finish between two Prometheus scrapes, and
Spark stages push their final metrics: all three AML batch stages and
Customer 360 gold-finalize. Customer 360 bronze-verify, silver-build and
continuous stages do not push yet, so their stage panels stay empty. The push is best-effort: a failed push never affects a
run, and `metrics.json` stays the source of record. Set
`pushgateway_enabled: false` to skip it.

v1.7 removed `observability.reports`, `observability.storage_class`,
`prometheus_stack_enabled`, `s3_metrics_enabled` and `spark_metrics_enabled`,
which nothing read. Every
run writes `report.html` into its run directory once, and `lakebench report
--render` writes a fresh copy to `lakebench-output/reports/` without
overwriting the delivered file; the Prometheus volume claim uses the cluster
default StorageClass. A config that carries these keys at their old defaults
loads with a note; any other value is refused by the commands that change
data.

The Spark and Trino PodMonitors are applied whenever the stack is enabled.
`observability.enabled: true` always installs (or reuses) the full stack, and the
Prometheus PVC uses the chart's default StorageClass. `dashboards_enabled` sets the
chart's `grafana.enabled`, and `retention` and `storage` set the Prometheus retention
and PVC size.

Set `observability.enabled: true` to deploy the stack.

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
- `destroy` never uninstalls it; another deployment may be using it. Each deployment's PodMonitors and Pushgateway live in its own namespace and go with it. The Lakebench Overview dashboard ConfigMap is shared: it lives in `lakebench-observability` and destroy leaves it in place. The one exception is a release an older lakebench installed into the deployment's own namespace, which destroy removes because it served only that namespace.
- To remove the shared stack when no deployment uses it: `helm uninstall lakebench-observability -n lakebench-observability`.

The stack provides:

- Prometheus server that picks up the PodMonitors of every lakebench namespace
- kube-state-metrics, and node-exporter except on OpenShift, for cluster-level visibility
- ServiceMonitor and PodMonitor CRDs for automatic target discovery

The Helm release is named `lakebench-observability`. The chart shortens service names, so list them rather than guessing:

```bash
kubectl get svc -n lakebench-observability -l release=lakebench-observability
```

To deploy the observability stack alongside infrastructure, set `observability.enabled: true` in your config YAML, then deploy as usual:

```bash
lakebench deploy test-config.yaml
```

## Grafana

Grafana is included in the kube-prometheus-stack install when `dashboards_enabled` is `true`. Default credentials are `admin` / `lakebench`.

One built-in dashboard, **Lakebench Overview**, is provisioned from a single
ConfigMap in the shared `lakebench-observability` namespace, applied on every
deploy and left in place by destroy. `namespace` and `run_id` variables select
the deployment and run. Its panels: datagen throughput (MB/s per pod, labelled as capped by the Lakebench-set
pod resources), datagen rows written by pod, datagen phase seconds,
bronze/silver stage rows (input vs output), silver per-table row counts,
pipeline stage elapsed seconds, Trino running queries, Trino query
throughput, and node CPU usage by pod. The
datagen and pipeline panels read the Pushgateway series, so they are empty
with `pushgateway_enabled: false`.

Access Grafana via port-forward:

```bash
kubectl port-forward svc/lakebench-observability-grafana 3000:80 -n lakebench-observability
```

## S3 Metrics

`observability/s3_metrics.py` defines an `S3MetricsWrapper` with these Prometheus metrics for CLI-side boto3 operations:

- `lakebench_s3_request_duration_seconds` -- request latency histogram
- `lakebench_s3_requests_total` -- total request count by operation
- `lakebench_s3_errors_total` -- error count by operation

No code path instantiates the wrapper today, so these series are not emitted. They would cover CLI operations (list, head, delete), not Spark/Trino data-path I/O.

## Platform Metrics Collection

After a benchmark run completes, the `PlatformCollector` queries the in-cluster Prometheus to snapshot infrastructure metrics (CPU, memory per pod, S3 I/O rates). These are included in the HTML report under the Platform Metrics tab when collected. The `observability.reports` block (including `include.platform_metrics`) has no effect.

## Reports

Every run delivers an HTML report to its run directory as `report.html`. That
file is the shareable artifact and is written once, at the end of the run.
The `lakebench report` command reads saved metrics and either prints a summary
or, with `--render`, writes a fresh timestamped HTML file to
`lakebench-output/reports/report-<run-id>-<ts>.html` without touching the
delivered `report.html`.

```bash
# Print the summary for the latest run (does not touch report.html)
lakebench report

# Print the summary for a specific run
lakebench report --run <run-id>

# Regenerate a fresh HTML report (new file under lakebench-output/reports/)
lakebench report --run <run-id> --render

# Overwrite a specific pre-existing HTML at a caller-chosen path
lakebench report --run <run-id> --render --output my.html --force

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
