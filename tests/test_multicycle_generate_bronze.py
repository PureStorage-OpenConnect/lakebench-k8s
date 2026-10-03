"""A multi-cycle run generates one slice per cycle, never a whole corpus
first (LB-255).

``run --generate`` on a multi-cycle config ran Phase 3's single-shot
generate (``deploy()``) before the cycle loop, whose ``deploy_cycle(0)`` then
generated cycle 0 again.
datagen_rs names cycle-0 files like a single-shot corpus
(``part-{fid:06}.parquet``, ``datagen_rs::cycle::c360_key``), so:

- on a bronze bucket this deployment owns, cycle 0 cleared the whole
  corpus first (a wasted generate);
- on one it did not create, with ``--allow-stale-bronze``, cycle 0 could not
  clear and wrote its N/c files over the first N/c of the corpus's N; cycle 0's
  silver (``common.c360_bronze_path``: ``part-[0-9]*.parquet``) then read all
  N, and the later cycles appended theirs: about (2 - 1/c) times the rows;
- on one it did not create, without the flag, cycle 0 refused the files
  Phase 3 had just written.

``run`` now refuses ``--generate`` on a multi-cycle run (exit 2, before any
cluster call), as the v1.7 design has it: a multi-cycle ``run`` generates in
its cycles without the flag. On the code before the refusal, the first test
here read 12 files at cycle 0 instead of 4 (3 cycles), with exit 0.

The tests drive the real ``run`` through the QA-9 harness with bronze held
as keys: the fake datagen writes the keys datagen_rs would, and cycle 0
calls the real ``DatagenDeployer._clear_bronze_prefix_if_fresh`` and the
real bronze gate against a key-level S3.
"""

from __future__ import annotations

import dataclasses
import fnmatch
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.harness import run_harness
from tests.harness.run_harness import SCENARIOS, invoke_scenario

PREFIX = "customer/interactions"
#: Files a single-shot corpus has at the scenario's scale (any N divisible
#: by the cycle count shows the effect).
FILES = 12
CYCLES = 3


def _common():
    path = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts/common.py"
    spec = importlib.util.spec_from_file_location("lb_common_for_test", path)
    if spec is None or spec.loader is None:  # pragma: no cover
        pytest.skip("cannot load common.py")
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except ImportError:  # pyspark-only imports
        pytest.skip("common.py needs pyspark")
    return mod


def c360_key(fid: int, cycle: int) -> str:
    """datagen_rs/src/cycle.rs ``c360_key``."""
    return f"part-{fid:06}.parquet" if cycle == 0 else f"part-c{cycle:03}-{fid:06}.parquet"


class KeyS3:
    """The bronze bucket as keys, for the bronze gate and cycle 0's clear."""

    _init_error = None

    def __init__(self, keys: set[str]):
        self.keys = keys
        self.markers: dict = {}

    def bucket_exists(self, bucket):
        return True

    def has_user_objects(self, bucket, prefix=""):
        return any(k.startswith(prefix) and not k.startswith(".lakebench/") for k in self.keys)

    def get_bucket_size(self, bucket, prefix=""):
        n = sum(1 for k in self.keys if k.startswith(prefix))
        return SimpleNamespace(object_count=n, size_bytes=n * 1000)

    @property
    def raw_client(self):
        """The corpus series marker, kept apart from the part-file keys."""
        from tests.fixtures.memory_s3 import MemoryBoto

        return MemoryBoto(self.markers)

    def delete_prefix(self, bucket, prefix, abort_multipart=False, keep_keys=frozenset()):
        gone = {k for k in self.keys if k.startswith(prefix.rstrip("/") + "/")}
        self.keys -= gone
        return len(gone)


def _run(tmp_path, monkeypatch, *, owned: bool, argv: list[str]):
    import lakebench.deploy.datagen as datagen_mod
    from lakebench.deploy.engine import DeploymentResult, DeploymentStatus

    keys: set[str] = set()
    s3 = KeyS3(keys)
    calls: list[str] = []
    monkeypatch.setattr(datagen_mod, "_s3_client_for", lambda cfg: s3)
    monkeypatch.setattr(datagen_mod, "deployment_may_empty", lambda *a, **k: owned)

    fake = run_harness.FakeDatagenDeployer

    def deploy(self, *a, **k):
        calls.append("deploy")
        keys.update(f"{PREFIX}/{c360_key(f, 0)}" for f in range(FILES))
        self._rec.add("Datagen", "deploy")
        return DeploymentResult(component="datagen", status=DeploymentStatus.SUCCESS, message="")

    def deploy_cycle(self, cycle_index, total_cycles):
        calls.append(f"deploy_cycle:{cycle_index}")
        # The real cycle-0 decision: clear an owned prefix, refuse or allow
        # stale objects on any other bucket.
        stub = SimpleNamespace(config=self._cfg, allow_stale_bronze=self._allow)
        datagen_mod.DatagenDeployer._clear_bronze_prefix_if_fresh(stub, cycle_index, PREFIX)
        per = FILES // total_cycles
        keys.update(f"{PREFIX}/{c360_key(f, cycle_index)}" for f in range(per))
        return DeploymentResult(
            component="datagen",
            status=DeploymentStatus.SUCCESS,
            message="",
            details={"timestamp_start": "2024-01-01", "timestamp_end": "2024-02-01"},
        )

    def wait_for_completion(self, *a, **k):
        return DeploymentResult(component="datagen", status=DeploymentStatus.SUCCESS, message="")

    real_init = fake.__init__

    def init(self, rec, *args, **kwargs):
        real_init(self, rec, *args, **kwargs)
        engine = args[0] if args else kwargs.get("engine")
        self._cfg = engine.config
        self._allow = bool(kwargs.get("allow_stale_bronze", False))

    monkeypatch.setattr(fake, "__init__", init)
    monkeypatch.setattr(fake, "deploy", deploy)
    monkeypatch.setattr(fake, "deploy_cycle", deploy_cycle, raising=False)
    monkeypatch.setattr(fake, "wait_for_completion", wait_for_completion, raising=False)

    # The harness replays single-cycle driver logs (bronze 2,478,560 rows);
    # the expected-results check would expect a multi-cycle count. Bronze
    # contents are what this test checks, from the keys.
    from lakebench.metrics import c360_correctness as c360

    real_ctx = c360.expected_context
    monkeypatch.setattr(
        c360,
        "expected_context",
        lambda cfg: {**real_ctx(cfg), "bronze_rows_expected": {"snappy": 2_478_560}},
    )

    config = dict(SCENARIOS["batch_c360"].config)
    config["architecture"] = {"pipeline": {"mode": "batch", "cycles": CYCLES}}
    scenario = dataclasses.replace(SCENARIOS["batch_c360"], argv=argv, config=config)
    result, rec = invoke_scenario(scenario, tmp_path, monkeypatch)
    _run.rec = rec  # type: ignore[attr-defined]
    return result, keys, calls


def _cycle0_silver_reads(keys: set[str], monkeypatch) -> int:
    common = _common()
    monkeypatch.setenv("LB_BRONZE_CYCLE", "0")
    glob = common.c360_bronze_path("s3a://b/", appending=False)[len("s3a://b/") :]
    return sum(1 for k in keys if fnmatch.fnmatchcase(k, glob))


def _seed(monkeypatch, n):
    real = KeyS3.__init__

    def seeded(self, keys):
        real(self, keys)
        keys.update(f"{PREFIX}/{c360_key(f, 0)}" for f in range(n))

    monkeypatch.setattr(KeyS3, "__init__", seeded)


@pytest.mark.parametrize(
    "argv", [["--generate", "--allow-stale-bronze", "--yes"], ["--generate", "--yes"]]
)
def test_generate_on_a_multicycle_run_is_refused_before_bronze_is_touched(
    tmp_path, monkeypatch, argv
):
    result, keys, calls = _run(tmp_path, monkeypatch, owned=False, argv=argv)
    assert result.exit_code == 2, result.output
    assert "--generate does not apply to a multi-cycle run" in result.output
    assert calls == [] and keys == set()


def test_foreign_bucket_with_allow_stale_bronze_holds_only_the_cycle_slices(tmp_path, monkeypatch):
    """The multi-cycle run generates without --generate: on a bucket this
    deployment did not create, cycle 0's silver reads cycle 0's slice."""
    result, keys, calls = _run(
        tmp_path, monkeypatch, owned=False, argv=["--allow-stale-bronze", "--yes"]
    )
    assert result.exit_code == 0, result.output
    assert (_cycle0_silver_reads(keys, monkeypatch), len(keys)) == (FILES // CYCLES, FILES)
    assert calls == [f"deploy_cycle:{i}" for i in range(CYCLES)]


def test_foreign_empty_bucket_without_the_flag_generates(tmp_path, monkeypatch):
    result, keys, calls = _run(tmp_path, monkeypatch, owned=False, argv=["--yes"])
    assert result.exit_code == 0, result.output
    assert calls == [f"deploy_cycle:{i}" for i in range(CYCLES)]
    assert len(keys) == FILES


def test_owned_corpus_is_refused_without_regenerate(tmp_path, monkeypatch):
    """An owned bronze holding a corpus is not cleared by a plain multi-cycle
    run (1.6 cleared it silently): the run is refused (exit 3) and the corpus
    stays (DESIGN ch05 7.1 rule 3)."""
    _seed(monkeypatch, FILES)
    result, keys, calls = _run(tmp_path, monkeypatch, owned=True, argv=["--yes"])
    assert result.exit_code == 3, result.output
    assert "--regenerate" in result.output
    assert calls == [] and len(keys) == FILES


def test_owned_corpus_is_replaced_by_the_cycle_slices_with_regenerate(tmp_path, monkeypatch):
    """With --regenerate, an owned bronze holding a corpus is cleared before
    cycle 0 and ends up holding the cycle slices only."""
    _seed(monkeypatch, FILES)
    result, keys, calls = _run(tmp_path, monkeypatch, owned=True, argv=["--regenerate", "--yes"])
    assert result.exit_code == 0, result.output
    assert (_cycle0_silver_reads(keys, monkeypatch), len(keys)) == (FILES // CYCLES, FILES)
    assert calls == [f"deploy_cycle:{i}" for i in range(CYCLES)]


def test_multicycle_run_completes_its_cycle_datagen(tmp_path, monkeypatch):
    """The cycle datagen wait read a ``_time`` that only the single-shot
    generate bound, so every multi-cycle run stopped at cycle 1 with
    "cannot access local variable" (exit 1)."""
    result, keys, calls = _run(tmp_path, monkeypatch, owned=True, argv=["--yes"])
    assert "_time" not in result.output, result.output
    assert result.exit_code == 0, result.output
    assert calls == [f"deploy_cycle:{i}" for i in range(CYCLES)]
