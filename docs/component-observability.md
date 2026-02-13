# Observability

Lakebench deploys observability via the `kube-prometheus-stack` Helm chart, which bundles Prometheus, Grafana, node-exporter, and kube-state-metrics in a single install.

HTML reports are generated from local metrics and do not require Prometheus or Grafana.

## YAML Configuration

All observability settings live under the `observability` key as a flat model:

```yaml
observability:
  enabled: false                     # Master switch for the observability stack
  prometheus_stack_enabled: true     # Deploy kube-prometheus-stack
  s3_metrics_enabled: true           # Collect S3 throughput metrics
  spark_metrics_enabled: true        # Collect Spark job metrics
  dashboards_enabled: true           # Enable Grafana dashboards
  retention: "7d"                    # Prometheus data retention period
  storage: "10Gi"                    # Prometheus PVC size
  storage_class: ""                  # StorageClass (empty = cluster default)

  reports:
    enabled: true                    # Enable HTML report generation
    output_dir: "./lakebench-output/runs"
    format: html                     # html | json | both
    include:
      summary: true                  # Include summary cards
      stage_breakdown: true          # Include per-stage breakdown
      storage_metrics: true          # Include S3 sizing data
      resource_utilization: true     # Include CPU/memory allocation data
      recommendations: true          # Include sizing recommendations
      platform_metrics: true         # Include platform metrics tab
```

Set `observability.enabled: true` to deploy the stack. All sub-flags (`prometheus_stack_enabled`, `dashboards_enabled`, etc.) default to `true` when the stack is enabled.

## Local Metrics (Always On)

Every `lakebench run` writes a `metrics.json` file to `lakebench-output/runs/run-<id>/`. No setup is needed. The file contains:

- **Pipeline benchmark scores** -- time-to-value, throughput, and efficiency for batch runs; freshness, sustained throughput, and latency profile for continuous runs.
- **Per-stage metrics** -- elapsed time, input/output sizes, row counts, throughput, and allocated executor resources for each pipeline stage (bronze, silver, gold, query).
- **Query results** -- per-query elapsed time, row counts, and pass/fail status from the benchmark.
- **Config snapshot** -- the full configuration used for the run, enabling cross-run comparison.

The `MetricsCollector` class in `metrics/collector.py` records job metrics, query metrics, streaming metrics, and measured S3 bucket sizes during the run. After completion, `build_pipeline_benchmark()` converts the flat job list into the unified stage-matrix view and computes aggregate scores.

## Prometheus

When `observability.enabled` is `true`, Lakebench installs the `kube-prometheus-stack` Helm chart into the deployment namespace. This provides:

- Prometheus server with namespace-scoped scrape configuration
- Node-exporter and kube-state-metrics for cluster-level visibility
- ServiceMonitor and PodMonitor CRDs for automatic target discovery

The Helm release is named `lakebench-observability`. Prometheus is accessible in-cluster at:

```
http://lakebench-observability-prometheus.<namespace>.svc:9090
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
kubectl port-forward svc/lakebench-observability-grafana 3000:80 -n <namespace>
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

The `include` block in the YAML controls which sections appear. Set any field to `false` to omit that section from the generated report. The `format` field supports `html`, `json`, or `both`.

Cleanup with `lakebench destroy` removes the observability Helm release and all associated resources.

## See Also

- [Benchmarking](benchmarking.md) -- scoring methodology and query benchmark details
- [Running Pipelines](running-pipelines.md) -- deploy, generate, and run workflow
- [CLI Reference](cli-reference.md) -- full command and flag reference
- [Configuration](configuration.md) -- complete YAML schema documentation
- [Architecture](architecture.md) -- system design and component overview
