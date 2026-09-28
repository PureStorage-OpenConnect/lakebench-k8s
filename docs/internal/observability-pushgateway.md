# Datagen + bronze->silver live observability (Pushgateway)

Maintainer material. Closes the gap that datagen and the bronze->silver
transform have no live Prometheus/Grafana signal: today every pipeline number
(datagen rows/bytes/throughput/phase-timings, bronze/silver input/output rows,
per-table counts, stage durations) is captured only as stdout log lines parsed
post-run into `metrics.json`. Grafana shows Trino query metrics and node/pod
CPU only; the shipped Spark panels sit empty during batch ETL because
`spark.ui.enabled=false` (the LB-049 driver-OOM fix) kills the shared-Jetty
PrometheusServlet the Spark PodMonitor targets.

This design was revised after two adversarial reviews (2026-09-28); the sections
below already incorporate their findings. The pushgateway is a best-effort
EPHEMERAL live view only. `metrics.json` remains the single authoritative
artifact for every published number (DESIGN.md invariant 5): nothing is ever
published off the dashboard.

## Only when monitoring is requested

Prometheus + Grafana (and therefore this pushgateway) deploy only when the YAML
sets `observability.enabled: true`; the default is False
(`config/schema.py` ObservabilityConfig.enabled), and `ObservabilityDeployer.deploy()`
returns SKIPPED otherwise (`deploy/observability.py`), symmetric with
`destroy` (`deploy/destroy.py`). When monitoring is off there is no gateway,
`LB_PUSHGATEWAY_URL` is unset, every pusher no-ops, and the pipeline behaves
exactly as today with metrics still landing in `metrics.json`.

## Why a Pushgateway, and its strict scope

Datagen pods and Spark stages are short-to-medium-lived batch jobs; Prometheus
pull misses a job that finishes inside a scrape interval. A Pushgateway is the
standard primitive for batch-job metrics. But a pushgateway is NOT a metrics
store -- pushed groups persist in memory until deleted (review F1). So its use
here is strictly scoped:

- It holds only the CURRENT/most-recent run's in-progress series for live
  viewing, keyed by `run_id` (below).
- It is never a source of published numbers; those come from `metrics.json`.
- Batch stages are completion-only (review F3): they emit once, at the end
  (`log_job_metrics` runs once on the driver). Only continuous stages
  (silver_stream, gold_refresh) and the live datagen push produce in-run
  curves. Panels for batch-stage metrics are labeled completion-only and never
  use `rate()`/`increase()` over the final-total gauges.

## Ownership

Per-deployment (ownership category 1): a Deployment + Service in the deployment
namespace, created by `deploy`, torn down by `destroy` with the namespace. The
shared kube-prometheus-stack Prometheus in `lakebench-observability` scrapes it
via a PodMonitor rendered into the deployment namespace. That PodMonitor MUST
carry `release: lakebench-observability` (like the Spark/Trino ones) or the
shared Prometheus will not select it, and MUST set `honorLabels: true` (review
F2) so the pushed `job`/`instance`/`run_id` labels survive the scrape instead of
being renamed to `exported_*`. Adding it does not mutate the shared category-4
`podMonitorSelector` (the selector already matches the release label), so no
cluster-lock-guarded shared mutation is introduced.

Gateway pod: small CPU/memory request AND limit (review F5), and
`--persistence.file` on a small `px-csi-scratch` PVC (review F6) so a restart
does not silently drop a completed stage's final push before Prometheus scrapes
it.

Service DNS in-namespace: `http://lakebench-pushgateway.<namespace>.svc:9091`.

## Staleness control (review F1, F5)

- Every series carries a `run_id` label (the lakebench run id). Grouping keys
  use stable indices only (`node_id`, `stage`) -- never pod name/UID/restart
  count, which would make cardinality unbounded.
- The Grafana dashboard has a `run_id` template variable defaulting to the most
  recent, so a viewer sees exactly one run and phantom groups from prior runs or
  dead pods are never rendered as "current".
- DELETE-on-completion: the in-cluster driver (Spark) and the datagen pod issue
  an HTTP DELETE of their run's groups on clean exit; the orchestrator's k8s
  client is the backstop that deletes groups older than the current run. This
  plus `run_id` scoping keeps the gateway bounded and free of stale reads.

## Push mechanics (review F4, F2)

- Mandatory short connect+read timeout (~2s). Fire-and-forget on a daemon
  thread with a single bounded retry; never synchronous on the datagen
  generation loop or the stage-completion path (a hang there would slow the very
  throughput being measured -- the observer effect).
- Emit-then-push: the push is added AFTER the existing `log(...)`/`eprintln!`
  emission and wrapped in try/except (Python) / a swallowed Result (Rust), so a
  pusher exception can never short-circuit the authoritative log line that feeds
  `metrics.json`. Push failures log at debug and are swallowed.
- `honorLabels: true` on the PodMonitor (above) so the `job` label the panels
  filter on is the pushed one.

## Metric contract

All series carry `namespace`, `workload` (financial|customer360), `mode`
(batch|continuous) and `run_id`.

### Datagen (pushed from the Rust pods; PodMetrics in datagen_rs/src/metrics.rs)

Chosen mechanism (owner decision 2026-09-28): a dependency-free raw HTTP/1.1
`POST` written over `std::net::TcpStream` -- NOT hyper. hyper is only a
transitive dep (via object_store) and adopting it would violate the crate's
"hand-build it, no serde" principle and risk `--locked` CI (review #2). The
exposition-format body is hand-built exactly as the `LB_METRICS_JSON` line
already is. Pushed periodically during the run (live curves) and once at
completion (authoritative totals).

Grouping labels: `job="datagen"`, `node_id`, `schema`, `run_id`.

- `lakebench_datagen_rows_written` (gauge) -- per-pod, safe to sum across pods.
- `lakebench_datagen_bytes_written` / `_files_written` / `_total_files` (gauge)
- `lakebench_datagen_throughput_mbps` (gauge, derived) -- CAP-LABELED in Grafana
  (datagen is hard-locked at 4 CPU / 4Gi batch, 24Gi continuous; invariant 6).
- `lakebench_datagen_elapsed_seconds` (gauge)
- `lakebench_datagen_phase_seconds{phase=...}` -- setup|world|typology|gen|
  reference|build_batch|encode_parquet|s3_put (None phases omitted per schema).
- `lakebench_datagen_cpu_seconds` (gauge, derived, k8s-request-based)
- `lakebench_datagen_corpus_total_txns` -- corpus-wide CONSTANT. Pushed ONCE
  under a single grouping key with NO `node_id` (review F2), so it is exactly
  one series and `sum()` cannot fan it out to N x total_txns.

### Spark stages (pushed from the driver in spark/scripts/common.py:log_job_metrics)

Driver-only (review #4): executors do not compute these and do not push. stdlib
`urllib` with the timeout/fire-and-forget rules above.

Grouping labels: `job="spark_stage"`, `stage`, `job_type`, `run_id`.

- `lakebench_stage_input_rows` / `lakebench_stage_output_rows` (gauge)
- `lakebench_stage_input_size_gb` (gauge)
- `lakebench_stage_elapsed_seconds` (gauge)
- `lakebench_silver_table_rows{table=...}` -- the per-table counts
  `log_job_metrics` already emits (silver.transactions, .entities, .accounts,
  .account_statements, .counterparty_edges, .entity_profiles).
- Batch: pushed once at completion (completion-only). Continuous
  (silver_stream/gold_refresh): pushed per tick -> live.

## Grafana (review #1)

Render the dashboard ONCE, cluster-wide, into `lakebench-observability` -- NOT
per-namespace. The current per-namespace render with a fixed
`uid: lakebench-overview` clobbers to one nondeterministic dashboard in the
shared Grafana (an existing latent bug, logged separately as LB-NNN); this fix
also resolves that. Give it real Grafana template variables `namespace` and
`run_id` sourced from `label_values(...)`, and change every panel expr to
`namespace=~"$namespace", run_id=~"$run_id"` instead of Jinja-baked literals.
Remove `dashboard-configmap.yaml.j2` from the per-namespace PODMONITOR_TEMPLATES
path.

Panels to add: datagen throughput (MB/s, cap-labeled) and rows by pod; datagen
phase-time breakdown (stacked); bronze/silver stage input-vs-output rows and
per-silver-table counts; stage elapsed-seconds bars. Capped panels carry an
explicit annotation ("generator rate -- capped: 4 CPU/4Gi per pod"; "at executor
cap N"), mirroring how `metrics/experiment.py` already labels caps.

## Env injection points (review #3)

- Datagen: add a context var guarded by `if cfg.observability.enabled` in
  `deploy/datagen.py:_build_datagen_context`, and a conditional
  `- name: LB_PUSHGATEWAY_URL` (plus `LB_RUN_ID`) env entry in
  `templates/datagen/job.yaml.j2`. `DatagenDeployer` already holds `self.config`
  and the namespace, so both are reachable.
- Spark driver: inject `LB_PUSHGATEWAY_URL` + `LB_RUN_ID` the same way the other
  `LB_*` driver env vars are set, guarded by `observability.enabled`.

## Sequencing

1. Pushgateway component: template (Deployment+Service+PVC), config flag,
   deployer wiring in observability.py, PodMonitor (release label +
   honorLabels), env injection. Unit tests: rendering, the PodMonitor label set
   (assert honorLabels + release), no-op-when-unset.
2. Spark push: stdlib pusher in common.py (timeout, daemon thread, try/except)
   called AFTER log emission; unit-test payload format, single-series corpus
   handling, and the no-op-when-unset path.
3. Datagen push: std::net raw HTTP writer in the Rust crate, periodic + final,
   guarded by LB_PUSHGATEWAY_URL; Rust unit test the exposition-format
   serializer and the swallow-on-error path. Its own brief adversarial review.
4. Grafana: single cluster-wide dashboard with namespace + run_id variables and
   cap-labeled panels; remove from per-namespace render path.
5. Live scale-1 validation (observability on): run datagen + bronze->silver,
   confirm Prometheus has the series with rows > 0 under the current run_id, the
   dashboard populates, corpus_total_txns is a single series, and a re-run does
   not show phantom pods. Non-degenerate assertion, not just HTTP 200.
