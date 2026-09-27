"""Evidence defects from the lb16 validation run (lane evidence-polish).

1. The settle wait ran after a DuckDB round that ran no statement, and a
   probe query with ~15% of its own noise was held to 10% (783 s wait, run
   20260927-001340-6ab705).
2. Orphan removal was merged into one "expire" row with the expire
   retention; its own 1450m retention was recorded nowhere.
3. The experiment block recorded unique_customers and date_range_days as
   null while the correctness check knew 100,000 customers.

Each test fails with its fix reverted.
"""

from __future__ import annotations

import ast
import inspect
from unittest.mock import MagicMock, patch

from rich.console import Console

from lakebench.benchmark.settle import (
    MAX_NOISE_TOLERANCE_PCT,
    effective_tolerance_pct,
    wait_for_settle,
)
from tests.conftest import make_config


class _Clock:
    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.t += s


def _wait(times, **kw):
    clock = _Clock()
    it = iter(times)

    def probe(remaining: float) -> float:
        t = next(it)
        clock.t += t
        return t

    args = {
        "probe_query": "Q1",
        "started_at": clock(),
        "max_seconds": 2700,
        "interval_seconds": 60,
        "tolerance_pct": 10.0,
        "clock": clock,
        "sleep": clock.sleep,
    }
    args.update(kw)
    return wait_for_settle(probe, **args)


# -- 1. settle -------------------------------------------------------------


def test_noisy_probe_settles_on_its_own_noise_not_after_13_minutes():
    """Pre samples 2.4/2.6/3.0 s (2 x MAD = 15% of the 2.6 s median); probes
    2.8-3.1 s as on run 20260927-001340-6ab705. Held to 10% of the median the
    pair 2.8/2.9 fails (2.9 > 2.86); with the bound widened to the query's
    noise it settles."""
    probes = [3.1, 2.8, 2.9, 2.8] + [2.9] * 50
    r = _wait(probes, reference_seconds=2.6, reference_samples=[2.4, 2.6, 3.0])
    assert r.settled and r.verified
    assert len(r.probes) == 3
    assert r.effective_tolerance_pct > 10.0
    d = r.to_dict()
    assert d["reference_samples"] == [2.4, 2.6, 3.0]
    assert d["effective_tolerance_pct"] == round(r.effective_tolerance_pct, 1)


def test_one_outlier_does_not_widen_the_bound():
    """One fast or one slow pre sample must not open the bound. With the
    slowest sample setting it, [2.5, 2.6, 3.15] opened it to 20% and a store
    still recovering (3.1 s then 2.95 s against 2.6 s) settled."""
    assert effective_tolerance_pct(10.0, [2.0, 2.6, 2.65]) == 10.0
    assert effective_tolerance_pct(10.0, [2.5, 2.6, 3.15]) == 10.0
    r = _wait([3.1, 2.95] * 50, reference_seconds=2.6, reference_samples=[2.5, 2.6, 3.15])
    assert not r.settled
    r = _wait([2.95] * 100, reference_seconds=2.6, reference_samples=[2.0, 2.6, 2.65])
    assert not r.settled


def test_two_samples_do_not_widen_the_bound():
    assert effective_tolerance_pct(10.0, [2.6, 3.2]) == 10.0


def test_pair_agreement_stays_at_the_configured_tolerance():
    """2.9 s then 2.6 s differ 11.5%: both are inside the widened 15% bound
    against the median, but they are not a stable pair at the configured
    10%."""
    r = _wait(
        [2.9, 2.6] * 50,
        reference_seconds=2.6,
        reference_samples=[2.4, 2.6, 3.0],
        max_seconds=600,
    )
    assert r.effective_tolerance_pct > 11.5
    assert not r.settled


def test_widened_tolerance_is_capped_so_a_slow_plateau_still_fails():
    """A reference with 100% spread must not accept the 33%-slow LB-150
    plateau: the widening stops at MAX_NOISE_TOLERANCE_PCT."""
    assert MAX_NOISE_TOLERANCE_PCT < 27.0
    assert effective_tolerance_pct(10.0, [5.0, 10.0, 15.0]) == MAX_NOISE_TOLERANCE_PCT  # 50% upward
    r = _wait([13.3] * 100, reference_seconds=10.0, reference_samples=[5.0, 10.0, 15.0])
    assert r.capped and not r.settled


def test_tight_reference_keeps_the_configured_tolerance():
    assert effective_tolerance_pct(10.0, [10.0, 10.1, 10.2]) == 10.0
    assert effective_tolerance_pct(10.0, [10.0]) == 10.0
    assert effective_tolerance_pct(10.0, None) == 10.0
    # 12% slow against a tight reference: still not settled.
    r = _wait([11.2] * 100, reference_seconds=10.0, reference_samples=[9.9, 10.0, 10.1])
    assert not r.settled


def test_statements_attempted_counts_what_ran():
    from lakebench.cli._run import _maintenance_statements_attempted as att

    duckdb = [
        {"kind": "expire", "skipped": "DuckDB cannot run maintenance"},
        {"kind": "compaction", "skipped": "DuckDB cannot run compaction"},
        {"kind": "compaction", "files_before": 367, "files_after": 367},
    ]
    assert att(duckdb) == 0
    trino = [
        {"kind": "expire", "total": 4, "succeeded": 4, "failed": 0, "timed_out": 0},
        {"kind": "compaction", "total": 2, "succeeded": 0, "failed": 1, "timed_out": 1},
    ]
    assert att(trino) == 6
    # Failed and timed-out statements count: they may have changed files.
    assert att([{"kind": "expire", "total": 2, "succeeded": 0, "failed": 2}]) == 2
    # A phase that raised mid-way: unknown, so the caller still waits.
    assert att([{"kind": "maintenance", "error": "x", "before_statements": False}]) is None
    assert att([]) is None and att(None) is None


def test_run_skips_the_settle_wait_only_when_no_statement_ran():
    import lakebench.cli._run as run_mod

    tree = ast.parse(inspect.getsource(run_mod))
    tests = [ast.unparse(n.test) for n in ast.walk(tree) if isinstance(n, ast.If)]
    assert "maint_elapsed > 0 and _maint_end is not None and (_attempted == 0)" in tests
    src = inspect.getsource(run_mod)
    assert "pb.maintenance_settle = _settle_skip" in src
    # Nothing ran, so there is no maintenance value either.
    assert "maint_elapsed if _settle_skip is None else 0.0" in src


# -- 2. per-operation retention -------------------------------------------


def _trino_maintenance(cfg, fail_orphan_on: str | None = None):
    from lakebench.cli._sustained import _run_iceberg_maintenance

    def fake_exec(engine, k8s, pod, ns, sql, timeout):
        if fail_orphan_on and "remove_orphan_files" in sql and fail_orphan_on in sql:
            raise RuntimeError("boom")

    outcomes: list = []
    with (
        patch(
            "lakebench.deploy.iceberg.find_maintenance_engine",
            return_value=("trino", "trino-0", "lakehouse"),
        ),
        patch("lakebench.deploy.iceberg.exec_sql", side_effect=fake_exec),
    ):
        _run_iceberg_maintenance(
            cfg,
            MagicMock(),
            Console(quiet=True),
            MagicMock(),
            "0s",
            timeout=600,
            outcomes=outcomes,
        )
    return outcomes


def test_orphan_removal_is_recorded_with_its_own_retention():
    cfg = make_config(architecture={"workload": {"schema": "customer360"}})
    outcomes = _trino_maintenance(cfg, fail_orphan_on="gold")
    (row,) = [o for o in outcomes if o["kind"] == "expire"]
    ops = {o["operation"]: o for o in row["operations"]}
    assert set(ops) == {"expire_snapshots", "remove_orphan_files"}
    assert ops["expire_snapshots"]["retention"] == "0s"
    assert ops["remove_orphan_files"]["retention"] == "1450m"
    assert ops["expire_snapshots"]["succeeded"] == ops["expire_snapshots"]["total"] == 2
    assert ops["remove_orphan_files"] == {
        "operation": "remove_orphan_files",
        "retention": "1450m",
        "total": 2,
        "succeeded": 1,
        "failed": 1,
        "timed_out": 0,
        "not_attempted": 0,
    }

    from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance

    e = effective_maintenance(
        MAINTENANCE_POLICY_ID,
        table_format="iceberg",
        query_engine="trino",
        mode="batch",
        outcomes=[*outcomes, {"kind": "compaction", "total": 2, "succeeded": 2}],
    )
    by_op = e["detail"]["operations"]
    assert by_op["expire_snapshots"]["applied_retention"] == ["0s"]
    assert by_op["remove_orphan_files"]["applied_retention"] == ["1450m"]
    assert by_op["remove_orphan_files"]["succeeded"] == 1
    # Identity is unchanged by the extra detail.
    assert e["id"] == f"{MAINTENANCE_POLICY_ID}:expire=ran,compaction=ran"


# -- 3. resolved workload parameters --------------------------------------


def test_c360_parameters_record_the_resolved_customers_not_null():
    from lakebench.metrics.c360_correctness import expected_context
    from lakebench.metrics.experiment import experiment_inputs

    cfg = make_config(architecture={"workload": {"schema": "customer360", "datagen": {"scale": 1}}})
    inp = experiment_inputs(cfg)
    c360 = inp["workload"]["parameters"]["customer360"]
    ctx = expected_context(cfg)
    assert c360["unique_customers"] == ctx["customers"] == 100_000
    assert c360["unique_customers_source"] == "scale"
    assert c360["event_window"] == {"start": ctx["window_start"], "end": ctx["window_end"]}
    assert "date_range_days" not in c360
    assert inp["corpus"]["unique_customers"] == 100_000
    assert "date_range_days" not in inp["corpus"]
    assert None not in c360.values()


def test_identity_hashes_are_unchanged_by_the_resolved_values():
    """parameters_id and corpus id still hash the declared overrides, so
    runs recorded before this stay comparable."""
    from lakebench.metrics.experiment import _short_hash, experiment_inputs

    cfg = make_config(architecture={"workload": {"schema": "customer360", "datagen": {"scale": 1}}})
    inp = experiment_inputs(cfg)
    declared = {"customer360": {"unique_customers": None, "date_range_days": None}}
    assert inp["workload"]["parameters_id"] == _short_hash(declared)

    override = make_config(
        architecture={
            "workload": {
                "schema": "customer360",
                "datagen": {"scale": 1},
                "customer360": {"unique_customers": 5000},
            }
        }
    )
    o = experiment_inputs(override)
    assert o["workload"]["parameters"]["customer360"]["unique_customers"] == 5000
    assert o["workload"]["parameters"]["customer360"]["unique_customers_source"] == (
        "customer360.unique_customers"
    )
    assert o["workload"]["parameters_id"] != inp["workload"]["parameters_id"]
    assert o["corpus"]["id"] != inp["corpus"]["id"]
