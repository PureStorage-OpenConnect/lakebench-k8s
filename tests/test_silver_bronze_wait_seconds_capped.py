"""A3 (silver-plan): silver-stream's bronze wait is capped at run_duration/4.

The old hardcoded ``_TABLE_WAIT_MAX = 1800`` was longer than the default
``run_duration`` (1800), which meant a stalled bronze-ingest could spend
the entire streaming window in the wait loop and let the run exit-0 with
zero silver rows -- the exact LB-044 class the plan closes elsewhere.

This test covers three surfaces:
  * SustainedConfig default resolution and explicit override.
  * job.py exports ``LB_SILVER_BRONZE_WAIT_SECONDS`` for SILVER_STREAM only.
  * silver_stream.py / silver_stream_delta.py's module constant reads the
    env var and raises SilverAbort at the cap, not SystemExit.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lakebench.config.schema import SustainedConfig
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.test_spark import _make_config, _mock_k8s


def test_default_wait_is_run_duration_over_four():
    """Unset: derived from run_duration."""
    cfg = SustainedConfig(run_duration=1800)
    assert cfg.silver_bronze_wait_seconds is None
    assert cfg.effective_silver_bronze_wait_seconds() == 450


def test_default_wait_scales_with_run_duration():
    """A shorter run must get a shorter wait; the whole point of A3."""
    cfg = SustainedConfig(run_duration=200)
    assert cfg.effective_silver_bronze_wait_seconds() == 50


def test_default_wait_floors_at_ten_seconds():
    """Guarantees a positive wait even for a minimum-length run."""
    cfg = SustainedConfig(run_duration=60)
    # 60 // 4 = 15, above the 10s floor.
    assert cfg.effective_silver_bronze_wait_seconds() == 15


def test_explicit_override_wins():
    """An operator-set value bypasses the auto derivation."""
    cfg = SustainedConfig(run_duration=1800, silver_bronze_wait_seconds=25)
    assert cfg.effective_silver_bronze_wait_seconds() == 25


def test_wait_never_exceeds_run_window():
    """The plan's core regression guard: 1800 == default run_duration
    let the wait consume the entire window, so no LB-044 gate ever fired.
    """
    for rd in (60, 100, 300, 1800, 7200):
        cfg = SustainedConfig(run_duration=rd)
        assert cfg.effective_silver_bronze_wait_seconds() <= rd


def _module_wait_constant(name: str, env_val: str | None) -> int:
    """Evaluate ``silver_stream[_delta].py``'s ``_TABLE_WAIT_MAX`` in isolation.

    silver_stream.py imports pyspark at module level; unit tests here run in
    an environment without pyspark, so we cannot ``import`` the module. Parse
    the source with ``ast``, find the assignment (or annotated assignment) to
    ``_TABLE_WAIT_MAX``, and exec that node in a minimal namespace.
    """
    import ast
    import os

    src = (Path(__file__).resolve().parents[1] / f"src/lakebench/spark/scripts/{name}").read_text()
    tree = ast.parse(src)
    assign = next(
        node
        for node in tree.body
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "_TABLE_WAIT_MAX" for t in node.targets)
        )
        or (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "_TABLE_WAIT_MAX"
        )
    )
    module = ast.Module(body=[assign], type_ignores=[])
    ns: dict = {"os": os}
    if env_val is None:
        os.environ.pop("LB_SILVER_BRONZE_WAIT_SECONDS", None)
    else:
        os.environ["LB_SILVER_BRONZE_WAIT_SECONDS"] = env_val
    try:
        exec(compile(module, str(name), "exec"), ns)
    finally:
        os.environ.pop("LB_SILVER_BRONZE_WAIT_SECONDS", None)
    return ns["_TABLE_WAIT_MAX"]


def test_module_constant_reads_env_var():
    """silver_stream.py's ``_TABLE_WAIT_MAX`` picks up LB_SILVER_BRONZE_WAIT_SECONDS."""
    assert _module_wait_constant("silver_stream.py", "25") == 25


def test_module_constant_reads_env_var_delta():
    """silver_stream_delta.py reads the same env var."""
    assert _module_wait_constant("silver_stream_delta.py", "40") == 40


def test_module_constant_default_when_unset():
    """No env var: fall back to 1800s so a bare submit still works."""
    assert _module_wait_constant("silver_stream.py", None) == 1800
    assert _module_wait_constant("silver_stream_delta.py", None) == 1800


def test_source_uses_silverabort_not_systemexit():
    """silver_stream.py raises SilverAbort at the cap.

    Uniform exit path with A1's LB-044 gate: silver failures surface as
    one exception class (SilverAbort) so downstream tooling sees one shape.
    """
    root = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"
    for name in ("silver_stream.py", "silver_stream_delta.py"):
        src = (root / name).read_text()
        # The wait-cap raise is now SilverAbort with the wait-window message.
        assert "raise SilverAbort(" in src, f"{name} missing SilverAbort raise"
        assert "bronze did not appear in wait window" in src, (
            f"{name} missing the wait-window abort message"
        )
        # The old SystemExit(1) at the wait-cap branch is gone.
        cap_block = src.split("Bronze table {bronze_tbl} not found", 1)[1].split("Waiting for", 1)[
            0
        ]
        assert "raise SystemExit(1)" not in cap_block, (
            f"{name} still raises SystemExit(1) in the wait-cap branch"
        )


def _env(job_type: JobType) -> dict[str, str]:
    manifest = SparkJobManager(_make_config(), _mock_k8s())._build_manifest(job_type)
    return {e["name"]: e["value"] for e in manifest["spec"]["driver"]["env"] if "value" in e}


def test_env_exported_for_silver_stream():
    """SILVER_STREAM env carries LB_SILVER_BRONZE_WAIT_SECONDS."""
    env = _env(JobType.SILVER_STREAM)
    assert "LB_SILVER_BRONZE_WAIT_SECONDS" in env
    # _make_config uses SustainedConfig defaults (run_duration=1800 -> 450).
    assert int(env["LB_SILVER_BRONZE_WAIT_SECONDS"]) == 450


@pytest.mark.parametrize("job_type", [JobType.BRONZE_INGEST, JobType.GOLD_REFRESH])
def test_env_not_set_for_other_streaming_jobs(job_type):
    """Bronze-ingest and gold-refresh don't wait for bronze; env stays clean."""
    env = _env(job_type)
    assert "LB_SILVER_BRONZE_WAIT_SECONDS" not in env
