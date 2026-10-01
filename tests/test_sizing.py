"""CC-22: one sizing source for plan, the capacity preflight, info,
config show, recommend and the docs tables (CLI-3).

Every caller sizes a config through ``config.sizing.plan_requirements``.
These tests hold the callers to it, pin four cells to figures derived by
hand from ``_JOB_PROFILES`` and the autosizer guidance (independently of
the code under test), and name the cases that fail with CC-22 reverted.
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
REPO = Path(__file__).resolve().parents[1]
CELLS = [(wl, m, s) for wl in TABLE_WORKLOADS for m in TABLE_MODES for s in TABLE_SCALES]
# The reference cluster: 434 cores / 4,349 GB allocatable.
REFERENCE = ClusterCapacity(434_000, 4349 * GIB, 10, 64_000, 512 * GIB)


def _cap(cores: int, gb: int, node_cores: int = 64, node_gb: int = 512) -> ClusterCapacity:
    return ClusterCapacity(cores * 1000, gb * GIB, 8, node_cores * 1000, node_gb * GIB)


# Hand-derived from _JOB_PROFILES, _SCHEMA_PROFILE_OVERRIDES, the autosizer
# datagen memory model and full_compute_guidance (default recipe: Hive,
# Trino). Catalog and Postgres memory: Hive 4 GiB + Postgres 1 GiB = 5 GB;
# their CPU is the autosizer's one core.
#
# c360 batch s1: silver-build 8 x 4 + 4 = 36 cores, 8 x (48 + 12) + 32 = 512 GB;
#   datagen 2 pods x 8 cores, 4 GiB each (2.2 GiB x 1.25 -> the 4 GiB floor);
#   Trino 1 worker x 2 + coordinator 1 + catalog/Postgres 1 = 4 cores,
#   8 + 4 + 5 = 17 GB. Floor max(36, one 8-core pod) + 4 = 40 cores,
#   max(512, 4) + 17 = 529 GB. Scratch 8 x 300 = 2,400.
# c360 batch s10: Spark as s1 (8 executors at scale <= 10); Trino 2 workers x 4
#   + coordinator 2 + 1 = 11 cores, 2 x 16 + 8 + 5 = 45 GB. Floor 47 / 557.
# AML continuous s1: bronze-ingest 5 x 4 + 2 = 22, 5 x 16 + 4 = 84; silver-stream
#   10 x 4 + 4 = 44, 10 x 40 + 8 = 408; gold-refresh 12 x 4 + 4 = 52, 12 x 40 + 8 =
#   488; streams 118 / 980. Always on: Trino 4 / 17 plus datagen 2 x 8 cores,
#   7 GiB each (5.36 x 1.25 = 6.7 -> 7) = 20 / 31. Floor 138 / 1,011.
#   Scratch 5 x 20 + 10 x 100 + 12 x 100 = 2,300.
# AML batch s100: silver-build 8 + 90 x 12 // 100 = 18 executors, 76 / 1,112;
#   datagen 10 pods (scale // 10) x 8 cores, 8 GiB (6.22 x 1.25 = 7.8 -> 8);
#   Trino 4 workers x 8 + 4 + 1 = 37 cores, 4 x 48 + 16 + 5 = 213 GB.
#   Floor max(76, 8) + 37 = 113, max(1,112, 8) + 213 = 1,325; all ten datagen
#   pods at once: max(76, 80) + 37 = 117. Scratch: AML bronze-verify
#   4 + 90 x 8 // 100 = 11 executors x 500 Gi = 5,500.
# c360 continuous s100: bronze-ingest 5 x 2 + 2 = 12, 5 x 6 + 4 = 34;
#   silver-stream 11 x 4 + 4 = 48, 11 x 40 + 8 = 448; gold-refresh 5 x 4 + 4 =
#   24, 5 x 40 + 8 = 208; streams 84 / 690. Always on: Trino 37 / 213 plus
#   datagen 10 x 8 = 80 cores, 10 x 4 GiB = 40 GB. Floor 201 / 943.
#   Scratch 5 x 20 + 11 x 100 + 5 x 100 = 1,700.
HAND_DERIVED = {
    ("customer360", "batch", 1): (40, 529, 2400),
    ("customer360", "batch", 10): (47, 557, 2400),
    ("financial", "continuous", 1): (138, 1011, 2300),
    ("financial", "batch", 100): (113, 1325, 5500),
    ("customer360", "continuous", 100): (201, 943, 1700),
}


@pytest.mark.parametrize("cell", sorted(HAND_DERIVED))
def test_hand_derived_cells(cell):
    p = plan_requirements(default_sizing_config(*cell))
    assert (p.floor.cpu_cores, p.floor.memory_gb, p.scratch_gb) == HAND_DERIVED[cell]


def test_batch_full_request_counts_every_datagen_pod():
    p = plan_requirements(default_sizing_config("financial", "batch", 100))
    assert (p.full.cpu_cores, p.full.memory_gb) == (117, 1325)
    assert p.datagen is not None and (p.datagen.pods, p.datagen.cpu_cores) == (10, 80)


def test_docs_spark_peak_matches_compute_peak_requirements():
    """Independent of the renderer: the Spark peak column of the
    getting-started table equals compute_peak_requirements() for each cell."""
    from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

    labels = {"Customer 360": "customer360", "AML": "financial"}
    row = re.compile(
        r"^\| (Customer 360|AML) \| (batch|continuous) \| (\d+) \| [^|]+\| [^|]+\| "
        r"([\d,]+) cores / ([\d,]+) GB \|",
        re.MULTILINE,
    )
    rows = row.findall((REPO / "docs" / "getting-started.md").read_text())
    assert len(rows) == 12
    for wl, mode, scale, cores, mem in rows:
        peak = compute_peak_requirements(int(scale), mode, labels[wl])
        assert (peak.cpu_cores, peak.memory_gb) == (
            int(cores.replace(",", "")),
            int(mem.replace(",", "")),
        ), (wl, mode, scale)


def _write_cfg(tmp_path: Path, wl: str, mode: str, scale: int) -> Path:
    path = tmp_path / f"{wl}-{mode}-{scale}.yaml"
    path.write_text(
        "name: sizing\n"
        f"workload:\n  schema: {wl}\n  datagen:\n    scale: {scale}\n"
        f"architecture:\n  pipeline:\n    mode: {mode}\n"
    )
    return path


def _doc_rows(rel: str) -> dict[tuple[str, str, int], tuple[int, int, int]]:
    labels = {"Customer 360": "customer360", "AML": "financial"}
    row = re.compile(
        r"^\| (Customer 360|AML) \| (batch|continuous) \| (\d+) \| ([\d,]+) cores \| "
        r"([\d,]+) GB \|(?:[^\n]*?\|)?? ([\d,]+) Gi \|",
        re.MULTILINE,
    )
    out = {}
    for wl, mode, scale, cores, mem, scratch in row.findall((REPO / rel).read_text()):
        out[(labels[wl], mode, int(scale))] = tuple(
            int(v.replace(",", "")) for v in (cores, mem, scratch)
        )
    return out


def _preflight_need(cfg, capacity: ClusterCapacity) -> tuple[int, int]:
    with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = capacity
        res = _check_cluster_capacity(cfg)
    m = re.search(r"needs ~(\d+) cores / (\d+) GB", res.message)
    assert m, res.message
    return int(m.group(1)), int(m.group(2))


def test_sizing_one_source(tmp_path):
    """plan_requirements, the preflight, info, config show, recommend --scale
    and both docs tables agree on all 12 cells (CLI-3 acceptance).

    Offline (no cluster) every caller prints the table's figure. Against a
    cluster the preflight prints plan_requirements with that capacity; that
    equals the table for every batch cell and for continuous at scale 50 or
    below. Continuous above scale 50 differs: the autosizer raises datagen
    to about 90% of the cluster's CPU and the floor counts it beside the
    streams (an autosizer defect older than CC-22, reported to the main
    lane). The equality with plan_requirements still holds there.
    """
    readme = _doc_rows("README.md")
    started = _doc_rows("docs/getting-started.md")
    assert set(readme) == set(started) == set(CELLS)
    runner = CliRunner()
    for wl, mode, scale in CELLS:
        cfg = default_sizing_config(wl, mode, scale)
        plan = plan_requirements(cfg)
        want = (plan.floor.cpu_cores, plan.floor.memory_gb, plan.scratch_gb)
        assert readme[(wl, mode, scale)] == want, (wl, mode, scale)
        assert started[(wl, mode, scale)] == want, (wl, mode, scale)

        need = _preflight_need(cfg, REFERENCE)
        fitted = plan_requirements(cfg, capacity=REFERENCE).floor
        assert need == (fitted.cpu_cores, fitted.memory_gb), (wl, mode, scale)
        if mode == "batch" or scale <= 50:
            assert need == want[:2], (wl, mode, scale)

        path = _write_cfg(tmp_path, wl, mode, scale)
        floor = f"{want[0]} cores / {want[1]} GB memory / {want[2]} GB scratch"
        for argv in (["info", str(path)], ["config", "show", str(path)]):
            with mock.patch("lakebench.cli.get_k8s_client", side_effect=RuntimeError("no cluster")):
                res = runner.invoke(app, argv, env={"COLUMNS": "400"})
            assert res.exit_code == 0, res.output
            assert floor in " ".join(res.output.split()), (argv, wl, mode, scale)

        res = runner.invoke(
            app,
            ["recommend", "--scale", str(scale), "--mode", mode, "--schema", wl],
            env={"COLUMNS": "400"},
        )
        assert res.exit_code == 0, res.output
        assert floor in " ".join(res.output.split()), (wl, mode, scale)


def test_preflight_warns_when_batch_datagen_queues():
    """Fails with CC-22 reverted: the preflight never looked at batch datagen.
    Eight datagen pods of 100 GiB on a cluster that holds the Spark jobs but
    not all eight at once pass (the Indexed Job queues the rest) with a
    warning that names the queueing, and are not refused."""
    from tests.conftest import make_config

    cfg = make_config(workload={"datagen": {"scale": 10, "parallelism": 8, "memory": "100Gi"}})
    plan = plan_requirements(cfg)
    assert plan.full.memory_gb > plan.floor.memory_gb + 200
    cap = _cap(400, plan.floor.memory_gb + 50, node_gb=128)
    with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = cap
        res = _check_cluster_capacity(cfg)
        skipped = _check_cluster_capacity(cfg, datagen_runs=False)
    assert res.passed, res.message
    assert "datagen: 8 pods need" in res.message and "queue" in res.message
    assert skipped.passed and "datagen" not in skipped.message.split("WARNING")[-1]


def test_cluster_scaled_datagen_never_refuses_a_batch_run():
    """Review finding: counting every cluster-scaled datagen pod in the floor
    refused AML batch s60 on 1,500 cores / 1,500 GiB, where Spark needs 97
    cores / 1,080 GB."""
    cfg = default_sizing_config("financial", "batch", 60)
    cap = _cap(1500, 1500)
    verdict = check_capacity(cfg, cap)
    assert verdict.admitted, verdict.shortfalls
    assert verdict.plan.datagen is not None and verdict.plan.datagen.pods > 100


def test_preflight_largest_pod_counts_datagen():
    """Fails with CC-22 reverted: the largest-pod check saw only Spark pods
    (4 cores), so 6-core nodes passed and the 8-core datagen pods never
    scheduled."""
    from tests.conftest import make_config

    cfg = make_config(workload={"datagen": {"scale": 1}})
    with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = _cap(400, 4000, node_cores=6)
        res = _check_cluster_capacity(cfg)
    assert not res.passed
    assert "Largest pod (datagen pod) needs 8 cores" in res.hint


def test_info_shows_batch_datagen_offline(tmp_path):
    """Fails with CC-22 reverted: info left batch datagen out of its sizing
    lines and counted no catalog or Postgres memory."""
    path = _write_cfg(tmp_path, "financial", "batch", 100)
    with mock.patch("lakebench.cli.get_k8s_client", side_effect=RuntimeError("no cluster")):
        res = CliRunner().invoke(app, ["info", str(path)], env={"COLUMNS": "400"})
    assert res.exit_code == 0, res.output
    out = " ".join(res.output.split())
    assert "113 cores / 1325 GB memory" in out
    assert "datagen 10 pods, 80 cores / 80 GB (before cluster scaling" in out


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


def test_floor_is_not_monotonic_in_scale():
    """Why recommend scans rather than bisects: the tier guidance gives 50
    datagen pods and 20 Trino workers at scale 500, 16 and 10 at 501."""
    a = plan_requirements(default_sizing_config("customer360", "batch", 500)).floor
    b = plan_requirements(default_sizing_config("customer360", "batch", 501)).floor
    assert b.cpu_cores < a.cpu_cores and b.memory_gb < a.memory_gb


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


@pytest.mark.parametrize("wl,mode", [(w, m) for w in TABLE_WORKLOADS for m in TABLE_MODES])
def test_recommend_monotonic(wl, mode):
    """The largest scale that fits never shrinks as the cluster grows, the
    next scale is refused (or it is the datagen ceiling), and the answer
    equals the preflight's decision. Clusters from 40 to 1,000 cores reach
    scales 1 to the ceiling."""
    ceiling = int(DATAGEN_SCALE_BANDS[wl][1])
    previous = 0
    for cores in (40, 100, 160, 250, 400, 600, 1000):
        cap = _cap(cores, cores * 12)
        admitted = _admitted_at(wl, mode, cap)
        best = largest_fitting_scale(admitted, upper=ceiling)
        assert best >= previous, (cores, best, previous)
        if best < ceiling:
            cfg = default_sizing_config(wl, mode, best + 1)
            with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
                get_client.return_value.get_cluster_capacity.return_value = cap
                res = _check_cluster_capacity(cfg, datagen_runs=mode != "continuous")
                assert not res.passed, (cores, best + 1)
        previous = best
    assert previous == ceiling  # 1,000 cores / 12,000 GB holds every scale


def test_continuous_recommend_says_generate_first():
    """Above scale 50 a continuous run that generates its own corpus is
    refused on any cluster (autosizer defect, reported); recommend names
    the workaround instead of reporting a scale that shrinks with the
    cluster."""
    res = CliRunner().invoke(
        app,
        ["recommend", "--cores", "434", "--memory", "4349", "--mode", "continuous"],
        env={"COLUMNS": "300"},
    )
    assert res.exit_code == 0, res.output
    out = " ".join(res.output.split())
    assert "generate first, then run --skip-generate" in out
    cfg = default_sizing_config("customer360", "continuous", 100)
    with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
        get_client.return_value.get_cluster_capacity.return_value = REFERENCE
        refused = _check_cluster_capacity(cfg)
        admitted = _check_cluster_capacity(cfg, datagen_runs=False)
    assert not refused.passed and "--skip-generate" in refused.hint
    assert admitted.passed


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


def test_recommend_cores_memory_answers_largest_fitting_scale():
    """Fails with CC-22 reverted: the old model added a 4-core infra guess
    and 15% to C360 dimensions for every schema."""
    cap = _cap(200, 2000, node_cores=200, node_gb=2000)
    best = largest_fitting_scale(
        _admitted_at("financial", "batch", cap, check_pod=False), upper=800
    )
    res = CliRunner().invoke(
        app,
        ["recommend", "--cores", "200", "--memory", "2000", "--schema", "financial"],
        env={"COLUMNS": "300"},
    )
    assert res.exit_code == 0, res.output
    assert f"Largest scale that fits: {best:,}" in " ".join(res.output.split())


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
    assert res.exit_code == 1
    assert "Unknown workload schema" in res.output
