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
        timestamp_start: datetime = datetime(2026, 1, 1),
        timestamp_end: datetime = datetime(2026, 4, 1),
    ):
        self.seed = seed
        self.total_files = total_files
        self.rows_per_file = rows_per_file
        self.customer_id_max = customer_id_max
        self.target_tb = target_tb
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
