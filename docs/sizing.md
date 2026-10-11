# Sizing

Reference: the minimum cluster per workload, mode and scale, and how
`lakebench` checks that a cluster has room.

The minimum depends on the workload, the pipeline mode and the scale factor.
The table is for `hive-iceberg-spark-trino`. It comes from
`lakebench.config.sizing.plan_requirements`, which also drives
`lakebench config show`, `lakebench config recommend` and the `run` capacity
preflight. It needs no cluster.

On a real cluster the preflight first sizes datagen and Trino against that
cluster, as `run` does. Its datagen figure can therefore differ from the
default-parallelism column.

A continuous row is the cluster that carries the scale's offered load
balanced (see [Running Pipelines](running-pipelines.md)). A smaller cluster
runs with fewer executors and a "cannot balance" warning naming the stage,
and that run fails the balance check. Use the figure, or lower the scale.

<!-- BEGIN GENERATED: sizing-detail -->
<!-- Generated from the code by `python3.11 scripts/gen_sizing_tables.py`; do not edit by hand. -->

| Workload | Mode | Scale | Minimum CPU | Minimum RAM | Spark peak | Datagen (default parallelism) | Always on | Scratch PVC (if enabled) | Largest pod |
|:---|:---|---:|---:|---:|:---|:---|:---|---:|---:|
| Customer 360 | batch | 1 | 41 cores | 544 GB | 36 cores / 525 GB | 2 pods, 16 cores / 8 GB | 5 cores / 19 GB | 400 Gi | 8 cores / 60 GB |
| Customer 360 | batch | 10 | 48 cores | 572 GB | 36 cores / 525 GB | 4 pods, 32 cores / 16 GB | 12 cores / 47 GB | 600 Gi | 8 cores / 60 GB |
| Customer 360 | batch | 100 | 114 cores | 1,340 GB | 76 cores / 1,125 GB | 10 pods, 80 cores / 40 GB | 38 cores / 215 GB | 5,400 Gi | 8 cores / 60 GB |
| Customer 360 | continuous | 1 | 46 cores | 363 GB | 38 cores / 282 GB | in always on | 6 cores / 23 GB | 640 Gi | 4 cores / 40 GB |
| Customer 360 | continuous | 10 | 54 cores | 391 GB | 38 cores / 282 GB | in always on | 13 cores / 51 GB | 640 Gi | 4 cores / 40 GB |
| Customer 360 | continuous | 100 | 222 cores | 1,896 GB | 158 cores / 1,286 GB | in always on | 48 cores / 223 GB | 3,220 Gi | 8 cores / 80 GB |
| AML | batch | 1 | 41 cores | 544 GB | 36 cores / 525 GB | 2 pods, 16 cores / 14 GB | 5 cores / 19 GB | 400 Gi | 8 cores / 60 GB |
| AML | batch | 10 | 48 cores | 572 GB | 36 cores / 525 GB | 4 pods, 32 cores / 28 GB | 12 cores / 47 GB | 600 Gi | 8 cores / 60 GB |
| AML | batch | 100 | 114 cores | 1,340 GB | 76 cores / 1,125 GB | 10 pods, 80 cores / 80 GB | 38 cores / 215 GB | 5,400 Gi | 8 cores / 60 GB |
| AML | continuous | 1 | 135 cores | 1,254 GB | 118 cores / 990 GB | in always on | 6 cores / 26 GB | 2,300 Gi | 4 cores / 40 GB |
| AML | continuous | 10 | 183 cores | 1,682 GB | 154 cores / 1,350 GB | in always on | 14 cores / 54 GB | 3,200 Gi | 4 cores / 40 GB |
| AML | continuous | 100 | 817 cores | 7,815 GB | 690 cores / 6,110 GB | in always on | 53 cores / 231 GB | 14,600 Gi | 16 cores / 160 GB |

- AML continuous scale 100 cannot balance at any cluster size: silver-stream needs ~45 executors x 16 cores to carry the offered load; the executor cap allows 28, so its lag will grow and the run will fail the balance check. The minimum is the cluster that runs it at the executor cap.

<!-- END GENERATED: sizing-detail -->

For your own config, `lakebench config recommend lakebench.yaml` prints the
largest scale the cluster holds. `lakebench config show lakebench.yaml`
prints the request at the configured scale.

## Reading the table

- **Minimum CPU and RAM** is what must fit at once.
  - Batch: datagen runs first and the Spark jobs after it. The datagen Job
    is elastic: pods the cluster cannot place wait and run as others finish.
    Only one datagen pod has to fit, so the minimum is the Spark peak plus
    the always-on pods.
  - Continuous: the three stream jobs run at once. The minimum is their sum
    plus the always-on pods. Datagen counts in the always-on line while its
    Job runs.
- **Spark peak** comes from the Spark job profiles
  (`compute_peak_requirements()`).
  - Per-executor sizing is fixed. Executor counts grow with scale above
    scale 10, up to 28 per job.
  - In batch, `silver-build` sets the peak: 8 executors of 4 cores and 60 GB
    (48 GB heap plus 12 GB overhead) at scale 10 and below.
- **Datagen** is the default `workload.datagen.parallelism`, before cluster
  scaling.
  - `run` caps it to fit the cluster it finds.
  - Above scale 50, `run` raises it to use about 90% of the CPU left after
    the always-on pods. One AML scale-100 generate ran 44 pods at 8 cores,
    about 350 cores at once (measured, n=1).
  - When the whole Job does not fit at once, the preflight passes with a
    warning that some pods queue.
- **Always on** is:
  - the query engine (here Trino, sized by scale tier)
  - the Hive Metastore and Postgres: their memory requests, and one core
    between them as the autosizer budgets it
  - the deployment's dependency server (`lb-deps`, 1 core and 2 GiB)

  Other recipes change this line. `lakebench config show` prints it for
  your config.
- **Scratch PVC** is the Spark scratch request when
  `platform.storage.scratch` is enabled: the largest batch stage's total.
  - At scale 100 that is silver-build, 18 executors of 300 Gi (5,400 Gi), for
    both workloads. AML `bronze-verify` needs 11 x 455 Gi (5,005 Gi) for its
    CTAS fallback.
  - Batch scratch per executor is the stage's per-scale need split across
    its executors, between 50Gi and the profile's size; see
    [Spark](component-spark.md#batch-jobs).
  - With scratch off ([when](operations.md#installing-the-shared-pieces)),
    no PVC is requested.
- **Largest pod** must fit on one node. A cluster with 512 GB spread across
  sixteen 32 GB nodes has enough memory on paper but cannot schedule a 60 GB
  `silver-build` executor. The 8 cores are a datagen pod.
- Per-job executor overrides (`silver_executors` and the like) are not yet
  counted in these figures. `config show` says so when a config sets one.

## Continuous sizing

Continuous sizes bronze-ingest and silver-stream to carry the scale's
offered load with 20% headroom. At AML scale 10, silver-stream gets about 19
executors x 4 cores (see [Running Pipelines](running-pipelines.md)).
gold-refresh is sized from its profile: AML 12 x 4 at scale 10, growing with
scale to the 28-executor cap near scale 23.

On a smaller cluster:

- A continuous run caps the stream jobs to what fits and warns, naming each
  capped job.
- Every stage that wants an executor gets one. Each further executor goes to
  the stage holding the smallest share of what it needs, because the
  pipeline runs at the pace of its slowest stage.
- A capped stage cannot carry the offered load, so the run fails the balance
  check.
- The capacity preflight passes such a cluster with a WARNING naming the
  capped stages, as long as the capped request plus Trino, Hive/Postgres and
  datagen fits. It fails only when even that does not fit, or when a single
  pod fits no node.
- An explicit `*_executors` count is not capped and is counted as set.

### Continuous above scale 50

Above scale 50, a continuous run that generates its own corpus is refused,
or admitted only with its streams capped hard (one bronze-ingest executor,
for example). Which one happens depends on the cluster. The datagen Job is
sized to about 90% of the CPU left after the always-on pods, and the
preflight counts it beside the streams.

To run the streams at full size:

1. Generate the corpus first: `lakebench generate`.
2. Start the streams with `lakebench run --skip-generate` within an hour of
   generation finishing.

A finished datagen Job is not counted, but Kubernetes deletes it after
3,600 s, and an absent Job is counted as still running.
`lakebench config recommend` prints the largest scale for both ways.

## The capacity check

The prerequisite phase of `lakebench run` compares the minimum against what
the cluster can still take:

- **Capacity counted:** the allocatable capacity of the schedulable nodes,
  minus what pods in other namespaces already request. Schedulable means
  Ready, not cordoned, and no `NoSchedule` or `NoExecute` taint. An
  untainted control-plane node counts.
- **On a shortfall:** the run fails at once with the shortfall, naming the
  need, the free amount and the allocatable amount. It does not leave pods
  `Pending` until the job times out.
- **Fails closed:** when the node or pod list cannot be read (no
  permission, an error, a pod on a node the list does not show), the run is
  refused with exit 4, "capacity could not be read".
- **Scratch:** with scratch enabled, the check also compares the scratch
  request with the `CSIStorageCapacity` the StorageClass publishes. When
  none is published the run goes ahead with a warning, and the record's
  `provenance.preflight.scratch` says `not_measurable`.
- **Limit:** free capacity is summed across nodes, so a cluster whose free
  cores are spread thin can pass and still leave executors `Pending`. Only
  the largest pod is checked against one node.
- **Skipping:** only `--skip-preflight` skips the check (`--skip-deploy`
  still runs it). The record then says `capacity: skipped`, and the verdict
  carries "capacity not checked".
- **Datagen:** a batch `run` counts datagen only when it creates datagen
  pods: with `--generate` (and not `--skip-generate`), or in a multi-cycle
  run without `--skip-generate`. A plain batch `run` over data from an
  earlier `lakebench generate` checks the Spark peak and the always-on pods.

`lakebench deploy` runs the same check, without datagen, before it creates
anything. So do `run --deploy-only`, `--generate-only` and run's auto-deploy.

- It refuses with exit 4 when free capacity, or the largest free node,
  cannot hold the pipeline and the always-on pods. It sizes against the
  worker nodes' allocatable. It has no flag to skip the check.
- When only the pod side cannot be read (a pod list it may not read, a pod
  on a node the list does not show), it checks the workers' allocatable
  instead.
- When capacity cannot be read at all (an unreachable cluster, a node list
  it may not read, no worker or no schedulable node), it warns and goes on.
  `run`'s preflight refuses until it can read them.
- A worker node quantity it cannot read refuses.
- `deploy --dry-run` prints the result without refusing.
