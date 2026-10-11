"""One sizing source for the capacity preflight, info, config show,
recommend and plan.

Every caller sizes a config through ``config.sizing.plan_requirements``.
These tests hold the callers to it and pin five cells to figures derived by
hand from ``_JOB_PROFILES`` and the autosizer guidance (independently of
the code under test).
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.cli._prerequisites import _check_cluster_capacity
from lakebench.config.sizing import (
    TABLE_MODES,
    TABLE_SCALES,
    TABLE_WORKLOADS,
    check_capacity,
    default_sizing_config,
    largest_fitting_scale,
    plan_requirements,
)
from lakebench.config.support import DATAGEN_SCALE_BANDS
from lakebench.k8s.client import ClusterCapacity

GIB = 1024**3
CELLS = [(wl, m, s) for wl in TABLE_WORKLOADS for m in TABLE_MODES for s in TABLE_SCALES]
# The reference cluster: 434 cores / 4,349 GB allocatable.
REFERENCE = ClusterCapacity(434_000, 4349 * GIB, 10, 64_000, 512 * GIB)


def _free_from_total(k8s_mock):
    """The preflight reads free capacity: make the mock report the capacity
    its get_cluster_capacity returns as both free and allocatable, with no
    published scratch capacity."""
    from lakebench.k8s.client import FreeCapacity, ScratchCapacity

    def _free(**_kw):
        cap = k8s_mock.get_cluster_capacity.return_value
        # One node with the largest node's resources free, for the
        # one-pod-on-one-node check.
        node = (cap.largest_node_cpu_millicores, cap.largest_node_memory_bytes)
        return FreeCapacity(free=cap, allocatable=cap, free_by_node=(node,))

    k8s_mock.get_free_capacity.side_effect = _free
    k8s_mock.get_scratch_capacity.return_value = ScratchCapacity(None, "none published (test)")


def _cap(cores: int, gb: int, node_cores: int = 64, node_gb: int = 512) -> ClusterCapacity:
    return ClusterCapacity(cores * 1000, gb * GIB, 8, node_cores * 1000, node_gb * GIB)


# Hand-derived from _JOB_PROFILES, _SCHEMA_PROFILE_OVERRIDES, the autosizer
# datagen memory model and full_compute_guidance (default recipe: Hive,
# Trino). Catalog and Postgres memory: Hive 4 GiB + Postgres 1 GiB = 5 GB;
# their CPU is the autosizer's one core. A driver pod requests its heap plus
# 40% overhead (Spark on Kubernetes, Python driver), and each job's memory
# rounds up to a whole GB.
#
# c360 batch s1: silver-build 8 x 4 + 4 = 36 cores, 8 x (48 + 12) + 32 x 1.4 =
#   524.8 -> 525 GB;
#   datagen 2 pods x 8 cores, 4 GiB each (2.2 GiB x 1.25 -> the 4 GiB floor);
#   Trino 1 worker x 2 + coordinator 1 + catalog/Postgres 1 + lb-deps 1 = 5
#   cores, 8 + 4 + 5 + 2 = 19 GB. Floor max(36, one 8-core pod) + 5 = 41
#   cores, max(525, 4) + 19 = 544 GB. Scratch, the largest stage's: each
#   executor's share of the stage's GiB per scale x scale, between 50 Gi and
#   the profile's scratch_size: silver-build 60 x 1 / 8 -> 50, 8 x 50 = 400.
# c360 batch s10: Spark as s1 (8 executors at scale <= 10); Trino 2 workers x 4
#   + coordinator 2 + 1 + lb-deps 1 = 12 cores, 2 x 16 + 8 + 5 + 2 = 47 GB.
#   Floor 48 / 572. Scratch: silver-build 60 x 10 / 8 = 75, 8 x 75 = 600.
# AML batch s100: silver-build 8 + 90 x 12 // 100 = 18 executors, 76 cores,
#   18 x 60 + 32 x 1.4 = 1,124.8 -> 1,125 GB;
#   datagen 10 pods (scale // 10) x 8 cores, 8 GiB (6.22 x 1.25 = 7.8 -> 8);
#   Trino 4 workers x 8 + 4 + 1 + lb-deps 1 = 38 cores, 4 x 48 + 16 + 5 + 2 =
#   215 GB. Floor max(76, 8) + 38 = 114, max(1,125, 8) + 215 = 1,340; all ten
#   datagen pods at once: max(76, 80) + 38 = 118 cores, max(1,125, 80) + 215 =
#   1,340 GB.
#   Scratch: silver-build 60 x 100 / 18 = 334 -> the 300 Gi ceiling,
#   18 x 300 = 5,400 (AML bronze-verify 50 x 100 / 11 = 455, 11 x 455 = 5,005).
# Continuous: the floor is the smallest cluster whose concurrent budget,
#   0.9 x (cores - Trino 2 + 2 x 4 - catalog/Postgres 1 - datagen - drivers),
#   holds every stream's executor cores; memory alike at the streams' largest
#   GiB per executor core (heap + overhead), after the always-on pods and the
#   driver pods (heap x 1.4).
# c360 continuous s10: bronze-ingest 2 x 2, silver-stream 4 x 4, gold-refresh
#   2 x 4 (each at least its balance need, 1 datagen core at 104 MB/s) = 28
#   cores; drivers 2 + 4 + 4 = 10; datagen 1. (C - 11 - 1 - 10) x 0.9 >= 28:
#   C = 54. 10 GiB per core (silver, gold 32g + 8g over 4); always on 51 GB,
#   drivers 5.6 + 11.2 + 11.2 = 28. (M - 51 - 28) x 0.9 / 10 >= 28: M = 391.
#   Scratch 2 x 20 + 4 x 100 + 2 x 100 = 640.
# AML continuous s10: datagen 1.5 cores x 28 MB/s = 42 MB/s; silver-stream
#   ceil(42 / (0.7 x 0.8) / 4) = 19 x 4, bronze-ingest 5 x 4 (its profile),
#   gold-refresh 12 x 4 (its profile at scale 10) = 144 cores; drivers 10.
#   (C - 11 - 1.5 - 10) x 0.9 >= 144: C = 183. 10 GiB per core; always on
#   54 GB, drivers 28. (M - 54 - 28) x 0.9 / 10 >= 144: M = 1,682.
#   Scratch 5 x 20 + 19 x 100 + 12 x 100 = 3,200.
HAND_DERIVED = {
    ("customer360", "batch", 1): (41, 544, 400),
    ("customer360", "batch", 10): (48, 572, 600),
    ("financial", "batch", 100): (114, 1340, 5400),
    ("customer360", "continuous", 10): (54, 391, 640),
    ("financial", "continuous", 10): (183, 1682, 3200),
}


@pytest.mark.parametrize("cell", sorted(HAND_DERIVED))
def test_hand_derived_cells(cell):
    p = plan_requirements(default_sizing_config(*cell))
    assert (p.floor.cpu_cores, p.floor.memory_gb, p.scratch_gb) == HAND_DERIVED[cell]


def test_batch_full_request_counts_every_datagen_pod():
    p = plan_requirements(default_sizing_config("financial", "batch", 100))
    assert (p.full.cpu_cores, p.full.memory_gb) == (118, 1340)
    assert p.datagen is not None and (p.datagen.pods, p.datagen.cpu_cores) == (10, 80)


def _write_cfg(tmp_path: Path, wl: str, mode: str, scale: int) -> Path:
    path = tmp_path / f"{wl}-{mode}-{scale}.yaml"
    path.write_text(
        "name: sizing\n"
        f"workload:\n  schema: {wl}\n  datagen:\n    scale: {scale}\n"
        f"architecture:\n  pipeline:\n    mode: {mode}\n"
    )
    return path


def _preflight_need(cfg, capacity: ClusterCapacity) -> tuple[int, int]:
    with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = capacity
        _free_from_total(get_client.return_value)
        res = _check_cluster_capacity(cfg)
    m = re.search(r"needs ~(\d+) cores / (\d+) GB", res.message)
    assert m, res.message
    return int(m.group(1)), int(m.group(2))


@pytest.mark.parametrize("wl,mode,scale", CELLS)
def test_preflight_prints_the_plan_figure(wl, mode, scale):
    """The preflight's "needs" figure is plan_requirements for the capacity it
    sizes with; offline (no capacity) it is the floor. Continuous above scale
    50 differs from the offline floor because the autosizer raises datagen to
    about 90% of the CPU left, which the floor then counts beside the
    streams."""
    cfg = default_sizing_config(wl, mode, scale)
    floor = plan_requirements(cfg).floor
    need = _preflight_need(cfg, REFERENCE)
    fitted = plan_requirements(cfg, capacity=REFERENCE).floor
    assert need == (fitted.cpu_cores, fitted.memory_gb)
    if mode == "batch" or scale <= 50:
        assert need == (floor.cpu_cores, floor.memory_gb)


@pytest.mark.parametrize(
    "wl,mode,scale", [("customer360", "batch", 10), ("financial", "continuous", 10)]
)
def test_cli_callers_print_the_plan_figure(tmp_path, wl, mode, scale):
    """info, config show and recommend --scale print the floor that
    plan_requirements gives the same config (one batch, one continuous cell)."""
    plan = plan_requirements(default_sizing_config(wl, mode, scale))
    floor = f"{plan.floor.cpu_cores} cores / {plan.floor.memory_gb} GB memory / {plan.scratch_gb} Gi scratch"
    path = _write_cfg(tmp_path, wl, mode, scale)
    runner = CliRunner()
    for argv in (["info", str(path)], ["config", "show", str(path)]):
        with mock.patch("lakebench.cli.get_k8s_client", side_effect=RuntimeError("no cluster")):
            res = runner.invoke(app, argv, env={"COLUMNS": "400"})
        assert res.exit_code == 0, res.output
        assert floor in " ".join(res.output.split()), argv
    res = runner.invoke(
        app,
        ["recommend", "--scale", str(scale), "--mode", mode, "--schema", wl],
        env={"COLUMNS": "400"},
    )
    assert res.exit_code == 0, res.output
    assert floor in " ".join(res.output.split())


def test_preflight_warns_when_batch_datagen_queues():
    """Eight datagen pods of 100 GiB on a cluster that holds the Spark jobs but
    not all eight at once pass with a warning that the Indexed Job queues the
    rest; without datagen in the run there is nothing to warn about."""
    from tests.conftest import make_config

    cfg = make_config(workload={"datagen": {"scale": 10, "parallelism": 8, "memory": "100Gi"}})
    plan = plan_requirements(cfg)
    assert plan.full.memory_gb > plan.floor.memory_gb + 200
    cap = _cap(400, plan.floor.memory_gb + 50, node_gb=128)
    with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = cap
        _free_from_total(get_client.return_value)
        res = _check_cluster_capacity(cfg)
        skipped = _check_cluster_capacity(cfg, datagen_runs=False)
    assert res.passed, res.message
    assert skipped.passed
    assert "WARNING:" in res.message
    assert "WARNING:" not in skipped.message


def test_cluster_scaled_datagen_never_refuses_a_batch_run():
    """AML batch s60 on 1,500 cores / 1,500 GiB is admitted: Spark needs 97
    cores / 1,080 GB and the cluster-scaled datagen pods queue."""
    cfg = default_sizing_config("financial", "batch", 60)
    cap = _cap(1500, 1500)
    verdict = check_capacity(cfg, cap)
    assert verdict.admitted, verdict.shortfalls
    assert verdict.plan.datagen is not None and verdict.plan.datagen.pods > 100


def test_preflight_largest_pod_counts_datagen():
    """6-core nodes are refused: the 8-core datagen pod would never schedule."""
    from tests.conftest import make_config

    cfg = make_config(workload={"datagen": {"scale": 1}})
    with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = _cap(400, 4000, node_cores=6)
        _free_from_total(get_client.return_value)
        res = _check_cluster_capacity(cfg)
    assert not res.passed
    assert "Largest pod needs 8 cores (datagen pod)" in res.hint


def test_batch_run_without_generate_counts_no_datagen_pod():
    """A batch run without generate creates no datagen pod, so 7.9-core nodes
    that hold every Spark pod pass; with datagen they do not."""
    from tests.conftest import make_config

    cfg = make_config(workload={"datagen": {"scale": 1}})
    cap = ClusterCapacity(16 * 7900, 16 * 61 * GIB, 16, 7900, 61 * GIB)
    with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = cap
        _free_from_total(get_client.return_value)
        assert _check_cluster_capacity(cfg, datagen_runs=False).passed
        assert not _check_cluster_capacity(cfg).passed


def test_preflight_sizes_against_the_capacity_run_sized_with():
    """With sizing_capacity the preflight checks the plan run sized, not a
    re-sized copy against its own capacity fetch."""
    cfg = default_sizing_config("customer360", "continuous", 100)
    cap = _cap(32, 960)
    own = check_capacity(cfg, cap)
    as_run = check_capacity(cfg, cap, sizing_capacity=None)
    assert as_run.plan == plan_requirements(cfg)
    assert own.plan == plan_requirements(cfg, capacity=cap)
    assert as_run.plan.floor.cpu_cores > own.plan.floor.cpu_cores
    with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = cap
        _free_from_total(get_client.return_value)
        res = _check_cluster_capacity(cfg, sizing_capacity=None)
    assert f"needs ~{as_run.plan.floor.cpu_cores} cores" in res.message


def test_thrift_pod_memory_counts_its_overhead():
    """The Spark Thrift pod requests heap + max(10%, 1 GiB), not the heap."""
    from lakebench.config.sizing import co_resident_request
    from tests.conftest import make_config

    cfg = make_config(
        recipe="hive-iceberg-spark-thrift",
        architecture={"query_engine": {"spark_thrift": {"memory": "30g"}}},
    )
    co = co_resident_request(cfg, False)
    assert co.memory_gb == 30 + 3 + 5 + 2  # heap, 10% overhead, Hive + Postgres, lb-deps
    pod = plan_requirements(cfg).largest_pod
    assert pod.memory_gb >= 33
    # A heap deploy refuses ("30Gi" is a Kubernetes unit) is sized as
    # written instead of raising inside the preflight's catch-all.
    odd = make_config(
        recipe="hive-iceberg-spark-thrift",
        architecture={"query_engine": {"spark_thrift": {"memory": "30Gi"}}},
    )
    assert co_resident_request(odd, False).memory_gb == 30 + 5 + 2  # lb-deps


def test_duckdb_recipes_leave_memory_to_the_autosizer():
    """A recipe does not mark the DuckDB memory user-set: it is autosized per
    schema, and a value the user sets is kept."""
    from lakebench.config.autosizer import resolve_auto_sizing
    from tests.conftest import make_config

    for recipe in ("hive-iceberg-spark-duckdb", "polaris-iceberg-spark-duckdb"):
        aml = make_config(recipe=recipe, architecture={"workload": {"schema": "financial"}})
        resolve_auto_sizing(aml)
        c360 = make_config(recipe=recipe)
        resolve_auto_sizing(c360)
        assert (
            aml.architecture.query_engine.duckdb.memory
            != c360.architecture.query_engine.duckdb.memory
        ), recipe
        user = make_config(
            recipe=recipe,
            architecture={
                "workload": {"schema": "financial"},
                "query_engine": {"duckdb": {"memory": "7g"}},
            },
        )
        resolve_auto_sizing(user)
        assert user.architecture.query_engine.duckdb.memory == "7g", recipe


@pytest.mark.parametrize(
    "field,value,delta",
    [
        # driver pod 64 GB x 1.4 (rounded up with the 8 x 60 GB executors) vs 32 GB x 1.4
        ("driver_memory", "64g", (0, 45)),
        ("driver_cores", 8, (4, 0)),
        # 12 more executors of 4 cores and 60 GB each
        ("silver_executors", 20, (48, 720)),
    ],
)
def test_overrides_are_counted(field, value, delta):
    from tests.conftest import make_config

    base = plan_requirements(make_config()).spark
    cfg = make_config(platform={"compute": {"spark": {field: value}}})
    got = plan_requirements(cfg).spark
    assert (got.cpu_cores - base.cpu_cores, got.memory_gb - base.memory_gb) == delta


def test_floor_driver_names_a_datagen_pod_when_it_sets_the_floor():
    from tests.conftest import make_config

    cfg = make_config(workload={"datagen": {"scale": 1, "cpu": "64"}})
    plan = plan_requirements(cfg)
    assert plan.floor_driver == "one datagen pod"
    assert plan_requirements(cfg, datagen_runs=False).floor_driver == plan.spark.driving_job


def test_plan_does_not_mutate_the_config():
    from tests.conftest import make_config

    cfg = make_config(workload={"datagen": {"scale": 100}})
    before = cfg.model_dump()
    plan_requirements(cfg, capacity=REFERENCE)
    assert cfg.model_dump() == before


def test_resolved_config_sizes_the_same():
    """The preflight runs on a config run already auto-sized in place;
    re-sizing the copy must give the same datagen request."""
    from lakebench.config.autosizer import resolve_auto_sizing
    from tests.conftest import make_config

    for scale in (1, 10, 60, 100, 300):
        cfg = make_config(workload={"schema": "financial", "datagen": {"scale": scale}})
        fresh = plan_requirements(cfg, capacity=REFERENCE)
        resolve_auto_sizing(cfg, REFERENCE)
        again = plan_requirements(cfg, capacity=REFERENCE)
        assert again == fresh, scale
        assert again.datagen.pods == cfg.architecture.workload.datagen.parallelism


def test_scratch_disabled_is_labelled():
    from lakebench.config.sizing import floor_text

    cfg = default_sizing_config("customer360", "batch", 1)
    assert "(not requested: scratch disabled)" in floor_text(plan_requirements(cfg))


def test_skip_generate_leaves_datagen_out_in_both_modes():
    for mode in ("batch", "continuous"):
        cfg = default_sizing_config("financial", mode, 10)
        with_dg = plan_requirements(cfg)
        without = plan_requirements(cfg, datagen_runs=False)
        assert without.datagen is None and not without.co_resident.includes_datagen
        assert "datagen" not in without.largest_pod.cpu_from
        if mode == "continuous":
            assert without.floor.cpu_cores < with_dg.floor.cpu_cores
        else:  # batch s10: Spark (36) is above datagen (32), so the floor holds
            assert without.floor == with_dg.floor


def _admitted_at(wl: str, mode: str, capacity: ClusterCapacity, check_pod: bool = True):
    """Admission per scale, as recommend decides it: the preflight's
    check_capacity, with continuous sized for a pre-generated corpus."""
    return lambda s: (
        check_capacity(
            default_sizing_config(wl, mode, s),
            capacity,
            check_pod=check_pod,
            datagen_runs=mode != "continuous",
        ).admitted
    )


def test_largest_fitting_scale_is_contiguous():
    """A predicate that holds at 501 but not at 500 answers 499."""
    assert largest_fitting_scale(lambda s: s != 500, upper=600) == 499
    assert largest_fitting_scale(lambda s: True, upper=600) == 600
    assert largest_fitting_scale(lambda s: False, upper=600) == 0


def _recommend_answer(wl: str, mode: str, capacity: ClusterCapacity) -> int:
    """``recommend``'s printed answer on a detected cluster (the CLI, not a
    re-implementation of its predicate). Continuous: the corpus-generated-
    first answer."""
    with mock.patch("lakebench.cli.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = capacity
        _free_from_total(get_client.return_value)
        res = CliRunner().invoke(
            app, ["recommend", "--mode", mode, "--schema", wl], env={"COLUMNS": "400"}
        )
    assert res.exit_code == 0, res.output
    out = " ".join(res.output.split())
    if "below the minimum for scale 1" in out:
        return 0
    label = ", corpus generated first:" if mode == "continuous" else ":"
    m = re.search(rf"Largest scale that fits{label} ([\d,]+)", out)
    assert m, out
    return int(m.group(1).replace(",", ""))


@pytest.mark.parametrize("wl,mode", [(w, m) for w in TABLE_WORKLOADS for m in TABLE_MODES])
def test_recommend_monotonic(wl, mode):
    """The largest scale ``recommend`` prints never shrinks as the cluster
    grows, the preflight admits every scale up to it and refuses the next
    (or it is the datagen ceiling). Clusters from 40 to 1,000 cores reach
    scales 1 to the ceiling."""
    ceiling = int(DATAGEN_SCALE_BANDS[wl][1])
    dg = mode != "continuous"
    previous = 0
    for cores in (40, 250, 1000):
        cap = _cap(cores, cores * 12)
        best = _recommend_answer(wl, mode, cap)
        assert best >= previous, (cores, best, previous)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            _free_from_total(get_client.return_value)
            for s in sorted({1, max(1, best // 2), best}) if best else ():
                cfg = default_sizing_config(wl, mode, s)
                assert _check_cluster_capacity(cfg, datagen_runs=dg).passed, (cores, s)
            if best < ceiling:
                cfg = default_sizing_config(wl, mode, best + 1)
                assert not _check_cluster_capacity(cfg, datagen_runs=dg).passed, (cores, best)
        previous = best
    assert previous == ceiling  # 1,000 cores / 12,000 GB holds every scale


def test_continuous_recommend_prints_both_answers():
    """Continuous recommend prints the plain-run answer, bounded by the
    preflight's datagen-counted decision, beside the generate-first one."""
    res = CliRunner().invoke(
        app,
        ["recommend", "--cores", "434", "--memory", "4349", "--mode", "continuous"],
        env={"COLUMNS": "400"},
    )
    assert res.exit_code == 0, res.output
    out = " ".join(res.output.split())
    plain = int(re.search(r"Largest scale that fits, plain run: ([\d,]+)", out).group(1))
    first = int(
        re.search(r"Largest scale that fits, corpus generated first: ([\d,]+)", out).group(1)
    )
    plain_ok = largest_fitting_scale(
        lambda s: (
            check_capacity(
                default_sizing_config("customer360", "continuous", s), _cap(434, 4349)
            ).admitted
        ),
        upper=600,
    )
    assert plain == plain_ok
    assert first >= plain


def test_recommend_agrees_with_the_preflight_on_the_reference_cluster():
    """Review finding: recommend (sized offline) answered 389 for C360 batch
    on 434 cores / 4,349 GB, where the preflight admits every scale to the
    600 ceiling."""
    res = CliRunner().invoke(
        app, ["recommend", "--cores", "434", "--memory", "4349"], env={"COLUMNS": "300"}
    )
    assert res.exit_code == 0, res.output
    out = " ".join(res.output.split())
    assert "Largest scale that fits: 600" in out
    assert "datagen ceiling of 600" in out


def test_config_recommend_sizes_the_config_itself(tmp_path, monkeypatch):
    """config recommend sizes the user's config (its query engine and
    datagen), not a default config of the same mode."""
    path = tmp_path / "duck.yaml"
    path.write_text(
        "name: duck\nrecipe: hive-iceberg-spark-duckdb\n"
        "workload:\n  schema: financial\n  datagen:\n    scale: 1\n"
    )
    from lakebench.config import load_config

    cap = _cap(120, 1500, node_cores=64, node_gb=512)
    monkeypatch.setattr(
        "lakebench.k8s.get_k8s_client",
        lambda *a, **k: mock.MagicMock(get_cluster_capacity=lambda: cap),
    )
    base = load_config(path)

    def admitted(s):
        c = base.model_copy(deep=True)
        object.__setattr__(c.architecture.workload.datagen, "scale", float(s))
        return check_capacity(c, cap).admitted

    best = largest_fitting_scale(admitted, upper=800)
    default_best = largest_fitting_scale(_admitted_at("financial", "batch", cap), upper=800)
    assert best != default_best  # the DuckDB config is not the default recipe
    res = CliRunner().invoke(app, ["config", "recommend", str(path)], env={"COLUMNS": "300"})
    assert res.exit_code == 0, res.output
    out = " ".join(res.output.split())
    assert f"Largest scale that fits: {best:,}" in out
    assert "default recipe" not in out


def test_recommend_scale_zero_is_a_usage_error():
    res = CliRunner().invoke(app, ["recommend", "--scale", "0"])
    assert res.exit_code == 2


def test_recommend_unknown_schema_refused():
    res = CliRunner().invoke(app, ["recommend", "--scale", "1", "--schema", "iot"])
    assert res.exit_code == 2  # USAGE: a bad argument
    assert "Unknown workload schema" in res.output


def test_recommend_context_conflict_exits_3(monkeypatch):
    """A context conflict while recommend reads capacity is exit 3
    (context.changed), not a fallback to the reference table and exit 0."""
    from lakebench.k8s.target import ContextConflictError

    def boom(*_a, **_k):
        raise ContextConflictError("this process already uses context A; refusing context B")

    monkeypatch.setattr("lakebench.cli.get_k8s_client", boom)
    res = CliRunner().invoke(app, ["recommend"])
    assert res.exit_code == 3, res.output
    assert "Cluster sizing reference" not in res.output


def test_recommend_with_cores_and_memory_makes_no_cluster_call(monkeypatch):
    called = []

    def spy(*_a, **_k):
        called.append(1)
        raise AssertionError("no cluster call expected")

    monkeypatch.setattr("lakebench.cli.get_k8s_client", spy)
    monkeypatch.setattr("lakebench.k8s.get_k8s_client", spy)
    monkeypatch.setattr("lakebench.k8s.client.get_k8s_client", spy)
    monkeypatch.setattr("lakebench.k8s.target.ClusterTarget.current", spy)
    res = CliRunner().invoke(app, ["recommend", "--cores", "434", "--memory", "4349"])
    assert res.exit_code == 0, res.output
    assert called == []
