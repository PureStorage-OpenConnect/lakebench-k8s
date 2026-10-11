"""Tests for peak resource computation and the cluster capacity preflight check.

Covers ``compute_peak_requirements()`` (the single source of truth for
documented minimums) and ``_check_cluster_capacity()`` (prerequisite check 9).
"""

import re
from unittest import mock

import pytest

from lakebench.cli._prerequisites import _check_cluster_capacity
from lakebench.config.autosizer import resolve_auto_sizing
from lakebench.config.sizing import co_resident_request
from lakebench.k8s.client import ClusterCapacity
from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

GIB = 1024**3


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


class TestComputePeakRequirements:
    """Peak resource derivation from _JOB_PROFILES."""

    def test_batch_peak_is_max_not_sum(self):
        """Batch jobs run sequentially, so the peak is the largest job."""
        peak = compute_peak_requirements(100)
        assert peak.memory_gb == max(r.memory_gb for r in peak.per_job)
        assert peak.memory_gb < sum(r.memory_gb for r in peak.per_job)

    def test_sustained_peak_is_sum_not_max(self):
        """Streaming jobs run concurrently, so their needs add up."""
        peak = compute_peak_requirements(10, "sustained")
        assert peak.memory_gb == sum(r.memory_gb for r in peak.per_job)


def _cfg(scale=1, mode="batch", schema="customer360"):
    """A real config with no query engine, so only catalog/Postgres (1 core)
    and, in continuous mode, datagen sit beside the Spark jobs.

    A real model rather than a MagicMock: the check sizes the config through
    ``config.sizing.plan_requirements``, which auto-sizes a deep copy.
    """
    from tests.conftest import make_config

    return make_config(
        workload={"schema": schema, "datagen": {"scale": scale}},
        architecture={"pipeline": {"mode": mode}, "query_engine": {"type": "none"}},
    )


@pytest.fixture
def patched_capacity():
    """Patch get_k8s_client so the check sees a controlled ClusterCapacity."""

    def _run(capacity, cfg=None):
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = capacity
            _free_from_total(get_client.return_value)
            return _check_cluster_capacity(cfg or _cfg())

    return _run


class TestClusterCapacityCheck:
    """Prerequisite check 9."""

    @pytest.mark.parametrize(
        ("capacity", "passed"),
        [
            (ClusterCapacity(652_000, 4000 * GIB, 20, 64_000, 256 * GIB), True),
            # the old README claim (8 CPU / 32 GB) is rejected
            (ClusterCapacity(8_000, 32 * GIB, 2, 4_000, 16 * GIB), False),
            # enough in total, but no 16 GB node holds a 60 GB executor
            (ClusterCapacity(200_000, 600 * GIB, 20, 8_000, 16 * GIB), False),
        ],
    )
    def test_verdict(self, patched_capacity, capacity, passed):
        result = patched_capacity(capacity)
        assert result.passed is passed
        assert result.name == "cluster-capacity"

    def test_unreadable_capacity_refuses(self):
        """Fail closed: capacity that cannot be read is a failed check, with
        the read-access or --skip-preflight way through (it used to pass)."""
        from lakebench.k8s.client import CapacityUnknown

        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_free_capacity.return_value = CapacityUnknown(
                "listing nodes failed (403 Forbidden)"
            )
            result = _check_cluster_capacity(_cfg())
        assert not result.passed
        assert "--skip-preflight" in result.hint

    def test_unreachable_cluster_refuses(self):
        """A client that cannot be built is unreadable capacity, not a pass."""
        with mock.patch("lakebench.k8s.get_k8s_client", side_effect=RuntimeError("boom")):
            result = _check_cluster_capacity(_cfg())
        assert not result.passed

    def test_capacity_is_sized_for_the_workload_schema(self, patched_capacity):
        """The check sizes the financial schema's continuous jobs, not the
        customer360 profiles: the reported need is larger for financial."""
        capacity = ClusterCapacity(1_000_000, 8000 * GIB, 20, 64_000, 256 * GIB)

        def needed_cores(schema):
            result = patched_capacity(capacity, _cfg(scale=10, mode="continuous", schema=schema))
            assert result.passed, result.message
            return int(re.search(r"needs ~(\d+) cores", result.message).group(1))

        assert needed_cores("financial") > needed_cores("customer360")


class TestCoResidentPodsAreCounted:
    """The check skipped Trino, catalog/Postgres and datagen whenever
    the uncapped pipeline request fit, so c360 continuous s10 passed on a
    40-core cluster that needs 49."""

    @staticmethod
    def _real_cfg(mode, scale=10):
        from tests.conftest import make_config

        return make_config(
            workload={"datagen": {"scale": scale}},
            architecture={"pipeline": {"mode": mode}},
        )

    def _check(self, cfg, cores, memory_gb=4000, **kw):
        cap = ClusterCapacity(cores * 1000, memory_gb * GIB, 8, 64_000, 256 * GIB)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            _free_from_total(get_client.return_value)
            return _check_cluster_capacity(cfg, **kw)

    def test_batch_counts_engine_but_not_datagen(self):
        cfg = self._real_cfg("batch")
        resolve_auto_sizing(cfg)
        batch = co_resident_request(cfg, sustained=False)
        cont = co_resident_request(cfg, sustained=True)
        assert "datagen" not in batch.label and "datagen" in cont.label
        assert cont.cpu_cores > batch.cpu_cores > 1
        peak = compute_peak_requirements(10, "batch", "customer360")
        assert not self._check(cfg, peak.cpu_cores).passed
        assert self._check(cfg, peak.cpu_cores + batch.cpu_cores).passed

    def test_memory_counts_co_residents(self):
        cfg = self._real_cfg("batch")
        resolve_auto_sizing(cfg)
        peak = compute_peak_requirements(10, "batch", "customer360")
        co_gb = co_resident_request(cfg, sustained=False).memory_gb
        assert co_gb > 0
        assert not self._check(cfg, 1000, memory_gb=peak.memory_gb).passed
        assert self._check(cfg, 1000, memory_gb=peak.memory_gb + co_gb).passed

    def test_sustained_flag_overrides_a_batch_config(self):
        """`run --sustained` does not write the mode back to the config; the
        check must size the continuous run it will actually start."""
        cfg = self._real_cfg("batch")
        cap = ClusterCapacity(40_000, 4000 * GIB, 8, 64_000, 256 * GIB)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            _free_from_total(get_client.return_value)
            res = _check_cluster_capacity(cfg, sustained=True)
        assert not res.passed and "(continuous)" in res.message

    def test_skip_generate_leaves_datagen_out(self):
        cfg = self._real_cfg("sustained")
        resolve_auto_sizing(cfg)
        with_dg = co_resident_request(cfg, True)
        without = co_resident_request(cfg, True, datagen_runs=False)
        assert "datagen" not in without.label and without.cpu_cores < with_dg.cpu_cores
        # The smallest cluster without datagen: datagen's pods do not fit.
        cores = next(c for c in range(1, 1000) if self._check(cfg, c, datagen_runs=False).passed)
        assert not self._check(cfg, cores).passed

    @pytest.fixture
    def config_files(self, tmp_path):
        base = (
            "name: cap-flag\n"
            "platform:\n  storage:\n    s3:\n      endpoint: http://127.0.0.1:1\n"
            "      access_key: x\n      secret_key: y\n"
        )
        single = tmp_path / "c.yaml"
        single.write_text(base)
        cycles = tmp_path / "cycles.yaml"
        cycles.write_text(
            base.replace("name: cap-flag\n", "name: cap-cycles\n")
            + "architecture:\n  pipeline:\n    cycles: 3\n"
        )
        return {"single": single, "cycles": cycles}

    @pytest.mark.parametrize(
        ("config", "args", "datagen_state", "expected"),
        [
            # --skip-generate drops datagen from the continuous reservation only
            # when its Job finished; absent, unfinished or unknown keep it.
            ("single", ["--sustained", "--skip-generate"], "finished", (True, False)),
            ("single", ["--sustained", "--skip-generate"], "absent", (True, True)),
            ("single", ["--sustained", "--skip-generate"], "unfinished", (True, True)),
            ("single", ["--sustained", "--skip-generate"], "unknown", (True, True)),
            # Batch creates datagen pods only with --generate or in a
            # multi-cycle run, and a multi-cycle --skip-generate reuses its corpus.
            ("single", [], "finished", (False, False)),
            ("single", ["--generate"], "finished", (False, True)),
            ("cycles", [], "finished", (False, True)),
            ("cycles", ["--skip-generate"], "finished", (False, False)),
            # --generate with --skip-generate is refused before the preflight.
            ("single", ["--generate", "--skip-generate"], "finished", None),
        ],
    )
    def test_run_passes_the_resolved_mode_to_the_check(
        self, config_files, monkeypatch, config, args, datagen_state, expected
    ):
        from typer.testing import CliRunner

        from lakebench.cli import app

        seen: dict = {}

        def fake_prereqs(cfg, **kw):
            # The capacity run sized cfg against; None here (no cluster).
            assert kw.pop("sizing_capacity") is None
            seen.update(kw)
            raise SystemExit(3)

        monkeypatch.setattr("lakebench.cli._prerequisites.run_prerequisites", fake_prereqs)
        monkeypatch.setattr(
            "lakebench.cli._sustained._datagen_job_state", lambda ns: (datagen_state, "")
        )
        # No cluster: auto-sizing runs without capacity.
        monkeypatch.setattr(
            "lakebench.k8s.get_k8s_client", mock.MagicMock(side_effect=RuntimeError("no cluster"))
        )
        res = CliRunner().invoke(app, ["run", str(config_files[config]), *args, "--yes"])
        if expected is None:
            assert res.exit_code == 2 and seen == {}
        else:
            assert seen == {"sustained": expected[0], "datagen_runs": expected[1]}


class TestPrerequisiteWiring:
    """The check must actually be registered in run_prerequisites()."""

    def test_unreadable_capacity_blocks_the_run(self):
        from lakebench.cli._prerequisites import run_prerequisites

        # run_prerequisites reads only attributes here; a MagicMock keeps the
        # catalog off the Hive-only Stackable check without a Polaris secret.
        cfg = mock.MagicMock()
        cfg.architecture.catalog.type.value = "polaris"
        cfg.platform.storage.s3.endpoint = ""
        cfg.platform.storage.s3.access_key = ""
        cfg.platform.kubernetes.create_namespace = True

        with mock.patch("lakebench.k8s.get_k8s_client", side_effect=RuntimeError("no cluster")):
            report = run_prerequisites(cfg)

        assert "cluster-capacity" in {c.name for c in report.failed}
