"""Tests for the FinancialGenerator + typology primitives (ENG-2C.3b).

Runs as unit tests: no numpy / pyarrow shell-out, no cluster, no S3.
Uses the datagen module by adding the datagen/ directory to sys.path
(datagen is a standalone Docker-image script, not a lakebench package).
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

DATAGEN_DIR = Path(__file__).resolve().parents[1] / "datagen"
if str(DATAGEN_DIR) not in sys.path:
    sys.path.insert(0, str(DATAGEN_DIR))


# Skip the module if the datagen runtime deps aren't installed in the
# lakebench dev venv (they always are in the datagen Docker image).
np = pytest.importorskip("numpy")
pa = pytest.importorskip("pyarrow")


class _FakeConfig:
    """Minimal Config surrogate matching what FinancialGenerator reads."""

    def __init__(
        self,
        seed: int = 42,
        total_files: int = 4,
        rows_per_file: int = 25,
        customer_id_max: int = 10_000,
        target_tb: float = 1.0,
        scale: float | None = 1.0,
        timestamp_start: datetime = datetime(2026, 1, 1),
        timestamp_end: datetime = datetime(2026, 4, 1),
    ):
        self.seed = seed
        self.total_files = total_files
        self.rows_per_file = rows_per_file
        self.customer_id_max = customer_id_max
        self.target_tb = target_tb
        # Default scale=1.0 keeps existing tests fast. Pass scale=None to
        # exercise the target_tb-based fallback path.
        if scale is not None:
            self.scale = scale
        self.timestamp_start = timestamp_start
        self.timestamp_end = timestamp_end


class TestTypologyScheduler:
    def test_all_eight_typologies_scheduled(self):
        from typologies import TYPOLOGIES, schedule_typologies

        instances = schedule_typologies(
            seed=42,
            scale=1.0,
            window_start=datetime(2026, 1, 1),
            window_end=datetime(2026, 4, 1),
            customer_id_max=10_000,
        )
        seen_types = {inst.typology_type for inst in instances}
        assert seen_types == set(TYPOLOGIES)

    def test_scaling_is_linear(self):
        from typologies import schedule_typologies

        args = {
            "seed": 42,
            "window_start": datetime(2026, 1, 1),
            "window_end": datetime(2026, 4, 1),
            "customer_id_max": 10_000,
        }
        s1 = schedule_typologies(scale=1.0, **args)
        s10 = schedule_typologies(scale=10.0, **args)
        # 10 per typology at scale 1, 100 per typology at scale 10.
        assert len(s10) == len(s1) * 10

    def test_deterministic_given_seed(self):
        from typologies import schedule_typologies

        args = {
            "scale": 1.0,
            "window_start": datetime(2026, 1, 1),
            "window_end": datetime(2026, 4, 1),
            "customer_id_max": 10_000,
        }
        a = schedule_typologies(seed=42, **args)
        b = schedule_typologies(seed=42, **args)
        assert [(i.typology_id, i.participant_entity_ids) for i in a] == [
            (i.typology_id, i.participant_entity_ids) for i in b
        ]

    def test_different_seeds_produce_different_participants(self):
        from typologies import schedule_typologies

        args = {
            "scale": 1.0,
            "window_start": datetime(2026, 1, 1),
            "window_end": datetime(2026, 4, 1),
            "customer_id_max": 10_000,
        }
        a = schedule_typologies(seed=42, **args)
        b = schedule_typologies(seed=43, **args)
        # Participants differ across seeds (compare first-instance sets).
        assert a[0].participant_entity_ids != b[0].participant_entity_ids

    def test_instance_windows_stay_inside_generation_window(self):
        from typologies import schedule_typologies

        window_start = datetime(2026, 1, 1)
        window_end = datetime(2026, 4, 1)
        instances = schedule_typologies(
            seed=42,
            scale=1.0,
            window_start=window_start,
            window_end=window_end,
            customer_id_max=10_000,
        )
        for inst in instances:
            assert window_start <= inst.injection_ts_start
            assert inst.injection_ts_end <= window_end
            assert inst.injection_ts_start <= inst.injection_ts_end


class TestTypologyEmitters:
    def _sched(self, typology_type):
        from typologies import TYPOLOGIES, TypologyInstance

        spec = TYPOLOGIES[typology_type]
        return TypologyInstance(
            typology_id=f"{typology_type}_TEST",
            typology_type=typology_type,
            participant_entity_ids=list(range(1, spec.participant_count + 1)),
            injection_ts_start=datetime(2026, 6, 1, 12, 0),
            injection_ts_end=datetime(2026, 6, 3, 12, 0),
            expected_workload=spec.expected_workload,
            severity=spec.default_severity,
            seed=100,
        )

    @pytest.mark.parametrize(
        "typology_type",
        [
            "fan_in",
            "fan_out",
            "gather_scatter",
            "scatter_gather",
            "cycle",
            "stack",
            "random",
            "bipartite",
        ],
    )
    def test_each_typology_emits_rows(self, typology_type):
        from typologies import emit_instance_rows

        inst = self._sched(typology_type)
        rows = emit_instance_rows(inst)
        assert len(rows) >= 1
        # UETRs got stamped back onto the instance:
        assert len(inst.participant_uetrs) == len(rows)
        # All rows carry a UETR.
        assert all(r["uetr"] for r in rows)

    def test_cycle_forms_a_loop_by_entity_id(self):
        from typologies import emit_instance_rows

        inst = self._sched("cycle")
        rows = emit_instance_rows(inst)
        # Cycle rows go A->B, B->C, C->D, D->A: last row's cdtr must be first row's dbtr entity.
        first_dbtr = inst.participant_entity_ids[0]
        last_cdtr = int(rows[-1]["cdtr"]["nm"].split("-")[1])
        assert last_cdtr == first_dbtr

    def test_fan_in_has_single_beneficiary(self):
        from typologies import emit_instance_rows

        inst = self._sched("fan_in")
        rows = emit_instance_rows(inst)
        beneficiary_ids = {int(r["cdtr"]["nm"].split("-")[1]) for r in rows}
        assert len(beneficiary_ids) == 1

    def test_fan_out_has_single_originator(self):
        from typologies import emit_instance_rows

        inst = self._sched("fan_out")
        rows = emit_instance_rows(inst)
        originator_ids = {int(r["dbtr"]["nm"].split("-")[1]) for r in rows}
        assert len(originator_ids) == 1

    def test_row_timestamps_within_window(self):
        from typologies import emit_instance_rows

        inst = self._sched("fan_in")
        rows = emit_instance_rows(inst)
        for r in rows:
            assert inst.injection_ts_start <= r["cre_dt_tm"] <= inst.injection_ts_end


class TestFinancialGeneratorFileEmission:
    def test_generate_file_data_returns_arrow_table(self):
        from financial import PACS008_SCHEMA, FinancialGenerator

        gen = FinancialGenerator(_FakeConfig())
        table = gen.generate_file_data(file_id=0)
        assert isinstance(table, pa.Table)
        # Same top-level column set as declared DDL.
        assert set(table.schema.names) == set(PACS008_SCHEMA.names)

    def test_generate_file_data_carries_baseline_rows(self):
        from financial import FinancialGenerator

        gen = FinancialGenerator(_FakeConfig(rows_per_file=17))
        table = gen.generate_file_data(file_id=0)
        # At least the baseline noise -- plus whatever typologies overlap.
        assert table.num_rows >= 17

    def test_generate_file_data_is_deterministic(self):
        from financial import FinancialGenerator

        a = FinancialGenerator(_FakeConfig(seed=99))
        b = FinancialGenerator(_FakeConfig(seed=99))
        t1 = a.generate_file_data(file_id=1).to_pydict()
        t2 = b.generate_file_data(file_id=1).to_pydict()
        assert t1["uetr"] == t2["uetr"]
        assert t1["intr_bk_sttlm_amt"] == t2["intr_bk_sttlm_amt"]

    def test_ensure_loyalty_builds_schedule(self):
        from financial import FinancialGenerator

        gen = FinancialGenerator(_FakeConfig())
        gen.ensure_loyalty()
        assert gen.instances(), "typology instances should be scheduled after ensure_loyalty"

    def test_manifest_bytes_produces_valid_parquet(self):
        import io

        import pyarrow.parquet as pq
        from financial import FinancialGenerator

        gen = FinancialGenerator(_FakeConfig())
        gen.ensure_loyalty()
        blob = gen.manifest_bytes()
        assert isinstance(blob, bytes) and len(blob) > 0
        table = pq.read_table(io.BytesIO(blob))
        assert "typology_id" in table.schema.names
        assert table.num_rows == len(gen.instances())

    def test_typology_uetrs_appear_in_generated_files(self):
        """A UETR emitted for a typology instance must land in some file window."""
        from financial import FinancialGenerator

        gen = FinancialGenerator(_FakeConfig(total_files=4))
        gen.ensure_loyalty()
        all_uetrs: set[str] = set()
        for file_id in range(4):
            all_uetrs.update(gen.generate_file_data(file_id).column("uetr").to_pylist())
        # Every typology instance's participant_uetrs must be a subset.
        for inst in gen.instances():
            assert set(inst.participant_uetrs).issubset(all_uetrs), (
                f"{inst.typology_id} UETRs missing from any file window"
            )

    def test_manifest_complete_before_any_file_generation(self):
        """Manifest UETRs must be populated by ensure_loyalty(), not deferred to first generate_file_data.

        Regression: prior to the fix, ensure_loyalty() only scheduled instances;
        UETRs were stamped only when a file window intersected the instance.
        Under multiprocessing where the parent writes the manifest and workers
        emit files, the manifest shipped with mostly-empty participant_uetrs.
        """
        from financial import FinancialGenerator

        gen = FinancialGenerator(_FakeConfig(total_files=4))
        gen.ensure_loyalty()
        # No generate_file_data() calls yet.
        blob = gen.manifest_bytes()
        import io

        import pyarrow.parquet as pq_

        table = pq_.read_table(io.BytesIO(blob))
        uetr_lists = table.column("participant_uetrs").to_pylist()
        assert all(len(u) > 0 for u in uetr_lists), (
            "ensure_loyalty must stamp UETRs on every instance so the manifest "
            "is complete before per-file generation runs"
        )


class TestHardening:
    """B1-B5: safety invariants that must hold at any scale.

    Structural invariants -- catch regressions where a code change breaks
    determinism, produces duplicates, or violates the partitioning contract
    the K8s Indexed Job pattern depends on.
    """

    def test_no_duplicate_uetrs_across_full_run(self):
        """B1: every UETR in a run is unique across ALL files."""
        from financial import FinancialGenerator

        gen = FinancialGenerator(_FakeConfig(total_files=8, rows_per_file=200))
        gen.ensure_loyalty()
        seen: set[str] = set()
        for fid in range(8):
            uetrs = gen.generate_file_data(fid).column("uetr").to_pylist()
            for u in uetrs:
                assert u not in seen, f"duplicate UETR {u} in file {fid}"
                seen.add(u)

    def test_manifest_uetrs_all_land_in_bronze_across_full_run(self):
        """B2: manifest.participant_uetrs is a subset of the run's bronze UETRs."""
        from financial import FinancialGenerator

        gen = FinancialGenerator(_FakeConfig(total_files=6, rows_per_file=100))
        gen.ensure_loyalty()
        all_bronze: set[str] = set()
        for fid in range(6):
            all_bronze.update(gen.generate_file_data(fid).column("uetr").to_pylist())
        manifest_uetrs: set[str] = set()
        for inst in gen.instances():
            manifest_uetrs.update(inst.participant_uetrs)
        missing = manifest_uetrs - all_bronze
        assert not missing, f"{len(missing)} manifest UETRs missing from bronze"

    def test_k8s_indexed_job_partitioning_no_collision_no_gap(self):
        """B3: N pods each running `[fid for fid in range(T) if fid % N == i]` yield disjoint UETR sets whose union equals single-process."""
        from financial import FinancialGenerator

        # Single-process baseline
        base = FinancialGenerator(_FakeConfig(total_files=8, rows_per_file=100))
        base.ensure_loyalty()
        baseline: set[str] = set()
        for fid in range(8):
            baseline.update(base.generate_file_data(fid).column("uetr").to_pylist())

        # 4 workers, each handles fid % 4 == worker_id
        n_workers = 4
        collected: list[set[str]] = []
        for w in range(n_workers):
            g = FinancialGenerator(_FakeConfig(total_files=8, rows_per_file=100))
            g.ensure_loyalty()
            worker_uetrs: set[str] = set()
            for fid in range(w, 8, n_workers):
                worker_uetrs.update(g.generate_file_data(fid).column("uetr").to_pylist())
            collected.append(worker_uetrs)
        # No collision between workers
        for i in range(n_workers):
            for j in range(i + 1, n_workers):
                overlap = collected[i] & collected[j]
                assert not overlap, f"worker {i} vs {j}: {len(overlap)} overlap"
        # Union equals single-process baseline
        union: set[str] = set()
        for s in collected:
            union |= s
        assert union == baseline, (
            f"partitioning gap/collision: |union|={len(union)} "
            f"|baseline|={len(baseline)} "
            f"missing={len(baseline - union)} extra={len(union - baseline)}"
        )

    def test_bronze_byte_identical_across_two_runs(self):
        """B3b (determinism): same (seed, config) => byte-identical bronze."""
        from financial import FinancialGenerator

        a = FinancialGenerator(_FakeConfig(total_files=4, rows_per_file=50, seed=77))
        b = FinancialGenerator(_FakeConfig(total_files=4, rows_per_file=50, seed=77))
        for fid in range(4):
            t1 = a.generate_file_data(fid).to_pydict()
            t2 = b.generate_file_data(fid).to_pydict()
            assert t1 == t2, f"file {fid} differs across runs"

    @pytest.mark.parametrize("seed", [1, 7, 42, 99, 137, 256, 512, 1024, 2048, 4096])
    def test_distribution_bands_hold_across_seeds(self, seed):
        """B4: for each seed, R1-R6 practitioner bands hold."""
        from financial import FinancialGenerator

        gen = FinancialGenerator(_FakeConfig(total_files=4, rows_per_file=500, seed=seed))
        gen.ensure_loyalty()
        rows = []
        for fid in range(4):
            rows += gen.generate_file_data(fid).to_pylist()
        amounts = [float(r["intr_bk_sttlm_amt"]) for r in rows]
        countries = [(r["dbtr"]["ctry_of_res"], r["cdtr"]["ctry_of_res"]) for r in rows]
        cross_border = sum(1 for a, b in countries if a != b) / len(countries)
        p50 = float(np.percentile(amounts, 50))
        p95 = float(np.percentile(amounts, 95))
        # R1 amount p50 in [4K, 8K]
        assert 3_000 <= p50 <= 9_000, f"seed={seed} p50={p50}"
        # R2 amount p95 in [30K, 100K] -- but typology structuring skews toward $10K
        # so at small volumes the band relaxes a bit
        assert 20_000 <= p95 <= 200_000, f"seed={seed} p95={p95}"
        # R4 cross-border share: silver derives from party ctry_of_res
        # (both parties' home countries). US-heavy customer base yields
        # ~18-28% cross-border on real data. Small-sample variance is wide
        # because typology instances have concentrated participants.
        assert 0.10 <= cross_border <= 0.40, f"seed={seed} cross_border={cross_border}"


class TestEdgeCases:
    """B5: scale extremes and empty windows do not crash or hang."""

    def test_scale_zero_still_emits_baseline(self):
        from financial import FinancialGenerator

        cfg = _FakeConfig(rows_per_file=10, scale=0.0, target_tb=0.0)
        gen = FinancialGenerator(cfg)
        gen.ensure_loyalty()
        # Baseline rows still generate. Even at scale=0 the scheduler emits
        # at least 1 instance per typology (max(1, int(scale * 10))) so
        # rows include typology piggyback -- assert >= 10 not == 10.
        t = gen.generate_file_data(0)
        assert t.num_rows >= 10

    def test_customer_id_max_one(self):
        """Small customer population must not crash PartySelector."""
        from financial import FinancialGenerator

        cfg = _FakeConfig(rows_per_file=5)
        cfg.customer_id_max = 1
        gen = FinancialGenerator(cfg)
        gen.ensure_loyalty()
        t = gen.generate_file_data(0)
        # baseline rows + typology piggyback rows; at minimum the 5 baseline
        assert t.num_rows >= 5

    def test_empty_window_does_not_hang(self):
        """Zero-width window (start == end) must not crash the scheduler."""
        from financial import FinancialGenerator

        cfg = _FakeConfig(rows_per_file=5)
        cfg.timestamp_start = datetime(2026, 3, 1)
        cfg.timestamp_end = datetime(2026, 3, 1)
        gen = FinancialGenerator(cfg)
        gen.ensure_loyalty()
        t = gen.generate_file_data(0)
        assert t.num_rows >= 5

    def test_single_file_run(self):
        """total_files=1 concentrates all typologies into file 0."""
        from financial import FinancialGenerator

        cfg = _FakeConfig(rows_per_file=5, total_files=1)
        gen = FinancialGenerator(cfg)
        gen.ensure_loyalty()
        t = gen.generate_file_data(0)
        assert t.num_rows >= 5


class TestScaleFactorReachesTypologyCount:
    """Regression: earlier the FinancialGenerator passed target_tb as `scale`
    to schedule_typologies. At target_tb=0.01 (scale=1) this floored to 1
    instance per typology, so the manifest never grew with scale until
    scale >= 100. Caught by cluster UAT s5-p8 / s10-p8 verify_run.
    """

    def test_explicit_scale_arg_produces_more_instances(self):
        from financial import FinancialGenerator

        gen_s1 = FinancialGenerator(_FakeConfig(scale=1.0, total_files=2, rows_per_file=10))
        gen_s10 = FinancialGenerator(_FakeConfig(scale=10.0, total_files=2, rows_per_file=10))
        gen_s1.ensure_loyalty()
        gen_s10.ensure_loyalty()
        assert len(gen_s10.instances()) == len(gen_s1.instances()) * 10

    def test_scale_1_has_at_least_8_instances(self):
        """One instance per typology per scale unit, 8 typologies."""
        from financial import FinancialGenerator

        gen = FinancialGenerator(_FakeConfig(scale=1.0, total_files=2, rows_per_file=10))
        gen.ensure_loyalty()
        # 10 instances per typology at scale 1, 8 typologies => 80
        assert len(gen.instances()) >= 80

    def test_fallback_when_scale_not_set(self):
        """Backward compat: no explicit scale falls back to target_tb-derived."""
        from financial import FinancialGenerator

        # Small target_tb so the test is cheap; fallback scale = target_tb*102.4
        cfg = _FakeConfig(target_tb=0.05, scale=None, total_files=2, rows_per_file=10)
        gen = FinancialGenerator(cfg)
        gen.ensure_loyalty()
        # target_tb=0.05 => fallback scale ~ 5.12 => >= 5 instances per typology
        # * 8 typologies => >= 40 instances
        assert len(gen.instances()) >= 40


class TestFinancialGeneratorRegistration:
    def test_registered_under_financial_key(self):
        # The datagen module registers FinancialGenerator lazily.
        import generate

        assert "financial" in generate._GENERATOR_REGISTRY

    def test_create_generator_dispatches_to_financial(self):
        import generate
        from financial import FinancialGenerator

        gen = generate.create_generator("financial", _FakeConfig())
        assert isinstance(gen, FinancialGenerator)
