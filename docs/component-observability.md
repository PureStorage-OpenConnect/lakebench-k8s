# Observability

Reference: the optional per-deployment Prometheus, Grafana and Pushgateway stack, local metrics and reports.

## What it does

- **Always on:** every run writes `metrics.json` and `report.html` locally. Neither needs Prometheus or Grafana.
- **Optional:** `observability.enabled: true` deploys its own Prometheus, Grafana and Pushgateway in the deployment namespace. Stacks of different deployments do not collide, and each goes with its namespace on destroy.
- The one cluster-scoped grant is the ClusterRole `lakebench-kubelet-metrics` (kubelet cAdvisor access), applied once by `lakebench admin install --component observability`. Without it the stack still deploys and platform CPU and memory read `not collected` in the report.

## Version and image

- Images are tag-pinned: `observability.prometheus_image`, `observability.grafana_image`, `observability.pushgateway_image`. Each also accepts a `name:tag@sha256:...` value. See the [version matrix](compatibility-matrix.md#component-version-matrix).
- The JMX exporter sidecar image is `images.jmx_exporter`, tag-pinned.

## Configuration keys

`observability` is a flat model; nested YAML such as `metrics.prometheus.enabled` is rejected.

| Key | Default | Effect |
|---|---|---|
| `enabled` | `false` | Deploy Prometheus, Grafana and the Pushgateway in the deployment namespace. |
| `dashboards_enabled` | `true` | Apply the Lakebench Overview dashboard ConfigMap. Grafana itself still runs. |
| `grafana_route` | `true` | OpenShift only: create an edge-TLS Route for Grafana. |
| `retention` | `"7d"` | Prometheus data retention. |
| `storage` | `"10Gi"` | Prometheus PVC size. |
| `prometheus_image` | [matrix](compatibility-matrix.md#component-version-matrix) | Prometheus image. |
| `grafana_image` | [matrix](compatibility-matrix.md#component-version-matrix) | Grafana image. |
| `prometheus_storage_class` | `""` | StorageClass of the Prometheus PVC; empty uses the cluster default. |
| `pushgateway_enabled` | `true` | Per-deployment Pushgateway for live datagen and pipeline metrics. |
| `pushgateway_image` | [matrix](compatibility-matrix.md#component-version-matrix) | Pushgateway image. |
| `pushgateway_storage` | `"1Gi"` | Pushgateway PVC size. |
| `pushgateway_storage_class` | `"px-csi-scratch"` | Pushgateway PVC StorageClass. |

- Removed keys (`chart_version`, `storage_class`) are listed under 1.7.2 in the [changelog](../CHANGELOG.md).

## Deploy and destroy

The stack lives in the deployment namespace, except the ClusterRole and the per-deployment ClusterRoleBinding that attaches to it.

- **Admin install:** a cluster admin runs `lakebench admin install --component observability <config>` once per cluster. It creates the `lakebench-kubelet-metrics` ClusterRole and nothing else (no namespace, CRDs or Helm chart). An existing role is left alone.
- **Deploy:** `lakebench deploy` applies Prometheus, Grafana, the Pushgateway and (when the ClusterRole is present) a per-deployment ClusterRoleBinding named `lakebench-kubelet-metrics-<namespace>`. It waits for the Prometheus pod to be Ready; a slow Grafana is a warning. A missing ClusterRole is also a warning.
- **Destroy:** deletes the per-deployment ClusterRoleBinding; the rest goes with the namespace, or is deleted by destroy when the namespace survives.
- **Removing the ClusterRole** when no deployment uses it: a cluster admin runs `kubectl delete clusterrole lakebench-kubelet-metrics`.

Service names are fixed:

```bash
kubectl get svc -n <namespace> lakebench-prometheus lakebench-grafana lakebench-pushgateway
```

### Prometheus scrape targets

Prometheus runs with a scrape config that targets this deployment only:

- The Pushgateway in this namespace (`honor_labels: true` so pushed `run_id`, `stage` and `job` labels survive).
- Trino coordinator and workers in this namespace (JMX exporter on port 9090).
- The kubelet cAdvisor endpoint on every node, filtered at relabel time to this namespace. The per-pod CPU, memory, network and filesystem families are kept; the rest is dropped.

Spark jobs run with the Spark UI off, which also turns off Spark's Prometheus endpoint, so no Spark engine metrics (GC, shuffle, spill) are collected; executor and driver series come from cAdvisor. Each executor also runs the JMX exporter agent (`images.jmx_exporter`); the stack scrapes its task output bytes and S3A read bytes every 5 s (job `spark-executors`), and the run's end stores them in the record's `platform_metrics.spark_io`. Spark stages report rows, bytes and elapsed time through the Pushgateway and `metrics.json`.

### Pushgateway

Each deployment gets a Deployment, a Service, a 1Gi PVC on `px-csi-scratch` by default, in its own namespace. The deployment's Prometheus scrapes it with `honor_labels: true`.

- Datagen pods push live progress, because a batch pod can finish between two Prometheus scrapes.
- Spark stages that push their metrics: AML bronze-verify, silver-build, gold-finalize and silver-stream; Customer 360 gold-finalize.
- Stages that do not push: Customer 360 bronze-verify, silver-build and continuous stages; bronze-ingest; gold-refresh. Their panels stay empty, and continuous lag and per-cycle cost are not shown live.
- The push is best-effort. A failed push never affects a run; `metrics.json` stays the record.

## Grafana

- The dashboard is applied when `dashboards_enabled` is `true`; Grafana runs either way.
- User `admin`. Deploy generates a per-deployment password into the Secret `lakebench-grafana` (key `admin-password`).
- `deploy` prints the Grafana URL and the login when it finishes.
- On OpenShift, deploy creates a TLS Route (`lakebench-grafana`, edge termination, HTTP redirected to HTTPS) and prints its `https://` URL. Set `grafana_route: false` to skip it. The Route goes with the namespace. Prometheus and the Pushgateway have no login and never get a Route.
- Elsewhere deploy prints the port-forward:

```bash
kubectl port-forward -n <namespace> svc/lakebench-grafana 3000:3000   # then http://localhost:3000
```

- To query Prometheus, use Grafana: Explore, data source **Prometheus**. Prometheus has no login and no URL; `kubectl port-forward -n <namespace> svc/lakebench-prometheus 9090:9090` reaches its raw UI.
- To read the password again:

```bash
kubectl get secret -n <namespace> lakebench-grafana -o jsonpath='{.data.admin-password}' | base64 -d
```

The **Lakebench Overview** dashboard is provisioned from a ConfigMap in the deployment namespace. A `run_id` variable selects among the runs that landed in this namespace. Rows:

- **Resources by component** (cAdvisor, so they work for batch and continuous runs): CPU, memory, network receive and transmit for datagen, each Spark stage (driver plus executors), Trino coordinator and workers, Polaris, Postgres, the observability pods and the dependency server; Spark executors running by stage; datagen pods running.
- **Trino** (JMX exporter): queries running and queued, completed and failed per minute, active tasks and queued executor tasks, JVM heap and GC time per pod.
- **Datagen** (Pushgateway): throughput, rows written, bytes written and elapsed seconds per pod.
- **Pipeline stages** (Pushgateway): stage rows in and out, stage input size, silver per-table row counts, stage elapsed seconds.

The pipeline-stage panels stay empty in a continuous run, and the datagen and pipeline panels are empty with `pushgateway_enabled: false`. The `run_id` variable comes from the pipeline stage series, so on a fresh deployment the datagen panels stay empty until the first Spark stage reports.

## Local metrics

Every `lakebench run` writes `lakebench-output/runs/run-<id>/metrics.json`. It holds:

- **Pipeline scores:** time-to-value, throughput and efficiency (batch); freshness, sustained throughput and latency profile (continuous).
- **Per-stage metrics:** elapsed time, input/output sizes, row counts, throughput and allocated executor resources for bronze, silver, gold and query.
- **Query results:** per-query elapsed time, row counts and pass/fail.
- **Config snapshot:** the full run configuration, for cross-run comparison.

`MetricsCollector` (`metrics/collector.py`) records job, query and streaming metrics and measured bucket sizes. `build_pipeline_benchmark()` turns the job list into the stage matrix and computes aggregate scores.

### Platform metrics

After a run, `PlatformCollector` queries the deployment's Prometheus (`http://lakebench-prometheus.<namespace>.svc:9090`) over the run window for CPU and memory per pod and Trino completed and failed queries. It also stores `platform_metrics.series`: CPU cores, memory bytes, pod network bytes per second (receive and transmit) and the number of pods running, summed per group of pods (each Spark stage, datagen, the query engine, the catalog, other) every 15 s. The deployment's own Prometheus, Grafana and Pushgateway pods are left out. Pod network includes executor-to-executor shuffle, so it is not storage traffic. The report's replay draws the lanes from it. The report's Platform Metrics section shows them, or why they were not collected (observability off, Prometheus unreachable, kubelet role not bound, or no pod series).

### Storage I/O

S3 request counts and latency are recorded as `null` (not collected). Storage bytes read and written come from the engines and are recorded in the run record's `io` block: Trino benchmark queries and the datagen total. Spark stages come from the executors' counters, with the limits in [Run records](benchmarking/records.md#storage-io).

## Reports

- Each run writes `report.html` into its run directory once, at the end. It is the shareable artifact.
- `lakebench report` prints the summary for a run. `--render` writes a fresh `lakebench-output/reports/report-<run-id>-<ts>.html` and never touches the delivered `report.html`. Flags: [cli-reference.md](cli-reference.md#report).
- Report layout: [HTML report layout](benchmarking/html-report.md).

## Troubleshooting

- [Trino metrics show only JVM metrics](troubleshooting.md#trino-metrics-show-only-jvm-metrics)

## See also

[Benchmarking](benchmarking.md), [Running pipelines](running-pipelines.md), [CLI reference](cli-reference.md), [Configuration](configuration.md).
