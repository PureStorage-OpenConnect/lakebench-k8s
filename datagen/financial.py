"""FinancialGenerator: pacs.008 synthetic data emitter with typology injection.

Implements the datagen ``Generator`` protocol from ``generate.py``. One
file per generation cycle window; deterministic in ``(config.seed,
file_id)``. Each file bundles baseline noise transactions plus any
typology instances whose injection window overlaps the file's time
window.

Ground truth (which UETRs belong to which typology instance) is
accumulated into an in-process ``TypologyInstance`` list and flushed to
a Parquet manifest sidecar by ``finalise_manifest``. FinancialGenerator
does not upload the manifest itself -- ``generate.py`` orchestrates the
upload once all files complete, so the manifest reflects the full run.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import numpy as np
import pyarrow as pa
from manifest import TypologyInstance, serialise_manifest
from typologies import emit_instance_rows, schedule_typologies

# ---------------------------------------------------------------------------
# Arrow schema (mirrors BRONZE_PACS008_DDL)
# ---------------------------------------------------------------------------


def _agent_type() -> pa.StructType:
    return pa.struct(
        [
            ("bicfi", pa.string()),
            ("lei", pa.string()),
            ("nm", pa.string()),
        ]
    )


def _addr_type() -> pa.StructType:
    return pa.struct(
        [
            ("strt_nm", pa.string()),
            ("twn_nm", pa.string()),
            ("ctry", pa.string()),
        ]
    )


def _acct_type() -> pa.StructType:
    return pa.struct(
        [
            ("iban", pa.string()),
            ("othr", pa.string()),
            ("ccy", pa.string()),
        ]
    )


def _party_type() -> pa.StructType:
    return pa.struct(
        [
            ("nm", pa.string()),
            ("pstl_adr", _addr_type()),
            (
                "id",
                pa.struct([("any_bic", pa.string()), ("lei", pa.string())]),
            ),
            ("ctry_of_res", pa.string()),
        ]
    )


def _ultmt_type() -> pa.StructType:
    return pa.struct([("nm", pa.string()), ("lei", pa.string()), ("ctry", pa.string())])


def build_pacs008_schema() -> pa.Schema:
    """Arrow schema aligned to BRONZE_PACS008_DDL in financial_ddl.py."""
    return pa.schema(
        [
            ("msg_id", pa.string()),
            ("cre_dt_tm", pa.timestamp("us")),
            ("nb_of_txs", pa.int32()),
            ("ctrl_sum", pa.decimal128(18, 5)),
            ("ttl_intr_bk_sttlm_amt", pa.decimal128(18, 5)),
            ("intr_bk_sttlm_dt", pa.date32()),
            ("sttlm_inf", pa.struct([("sttlm_mtd", pa.string())])),
            (
                "pmt_tp_inf",
                pa.struct(
                    [
                        ("instr_prty", pa.string()),
                        ("clr_chanl", pa.string()),
                        ("svc_lvl", pa.string()),
                        ("lcl_instrm", pa.string()),
                        ("ctgy_purp", pa.string()),
                    ]
                ),
            ),
            ("instg_agt", _agent_type()),
            ("instd_agt", _agent_type()),
            ("txn_id", pa.string()),
            ("instr_id", pa.string()),
            ("end_to_end_id", pa.string()),
            ("uetr", pa.string()),
            ("clr_sys_ref", pa.string()),
            ("intr_bk_sttlm_amt", pa.decimal128(18, 5)),
            ("intr_bk_sttlm_ccy", pa.string()),
            ("instd_amt", pa.decimal128(18, 5)),
            ("instd_ccy", pa.string()),
            ("xchg_rate", pa.decimal128(11, 10)),
            ("chrg_br", pa.string()),
            ("intrmy_agt_1", _agent_type()),
            ("intrmy_agt_2", _agent_type()),
            ("intrmy_agt_3", _agent_type()),
            ("prvs_instg_agt_1", _agent_type()),
            ("prvs_instg_agt_2", _agent_type()),
            ("prvs_instg_agt_3", _agent_type()),
            ("ultmt_dbtr", _ultmt_type()),
            (
                "initg_pty",
                pa.struct([("nm", pa.string()), ("lei", pa.string())]),
            ),
            ("dbtr", _party_type()),
            ("dbtr_acct", _acct_type()),
            ("dbtr_agt", _agent_type()),
            ("cdtr_agt", _agent_type()),
            ("cdtr", _party_type()),
            ("cdtr_acct", _acct_type()),
            ("ultmt_cdtr", _ultmt_type()),
            ("purp_cd", pa.string()),
            ("purp_prtry", pa.string()),
            (
                "rgltry_rptg",
                pa.list_(
                    pa.struct(
                        [
                            ("dbt_cdt_rptg_ind", pa.string()),
                            ("authrty_nm", pa.string()),
                            ("authrty_ctry", pa.string()),
                            ("details", pa.list_(pa.string())),
                        ]
                    )
                ),
            ),
            ("rmt_inf_ustrd", pa.list_(pa.string())),
            (
                "rmt_inf_strd",
                pa.list_(pa.struct([("ref_doc", pa.string()), ("amt", pa.decimal128(18, 5))])),
            ),
        ]
    )


PACS008_SCHEMA = build_pacs008_schema()


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


class FinancialGenerator:
    """pacs.008 synthetic data generator.

    Deterministic in ``(config.seed, file_id)``. Emits Iceberg-friendly
    Arrow tables plus a shared TypologyInstance manifest.

    Time model: the configured
    ``(timestamp_start, timestamp_end)`` range is divided evenly across
    ``config.total_files`` file_ids. Each file window covers roughly
    ``span_seconds / total_files`` seconds and contains that file's
    baseline pacs.008 rows plus any typology instances whose scheduled
    injection window overlaps.
    """

    schema_name = "financial"

    def __init__(self, config: Any):
        self.config = config
        self._instances: list[TypologyInstance] | None = None
        self._instances_by_window: dict[int, list[TypologyInstance]] | None = None

    # -- protocol methods --

    def ensure_loyalty(self) -> None:
        """Warm the typology schedule before fork/pickle.

        Not a real 'loyalty' cache -- name kept for Generator protocol
        parity with Customer360Generator. Building the schedule here
        means every worker process inherits an identical instance list
        via copy-on-write rather than each rebuilding it.
        """
        self._ensure_schedule()

    def generate_file_data(self, file_id: int) -> pa.Table:
        """Return a Parquet-ready Arrow table for one file window."""
        self._ensure_schedule()
        rng = np.random.default_rng(seed=self.config.seed + file_id)
        window_start, window_end = self._file_window(file_id)
        rows = self._emit_baseline_rows(rng, file_id, window_start, window_end)
        for instance in self._instances_by_window.get(file_id, []):
            rows.extend(emit_instance_rows(instance))
        return self._rows_to_table(rows)

    # -- manifest access (called from generate.py after all files done) --

    def instances(self) -> list[TypologyInstance]:
        """All scheduled typology instances (post ensure_loyalty)."""
        self._ensure_schedule()
        return self._instances or []

    def manifest_bytes(self) -> bytes:
        """Serialise the manifest for upload as a Parquet sidecar."""
        return serialise_manifest(self.instances())

    # -- internals --

    def _ensure_schedule(self) -> None:
        if self._instances is not None:
            return
        instances = schedule_typologies(
            seed=self.config.seed,
            scale=self.config.target_tb,  # scale proxy: target_tb == v1 scale surrogate
            window_start=self.config.timestamp_start,
            window_end=self.config.timestamp_end,
            customer_id_max=self.config.customer_id_max,
        )
        # Pre-stamp participant_uetrs on every instance so the manifest is
        # complete regardless of which file windows a worker process
        # actually generates. Emitters are seed-deterministic, so the
        # UETRs computed here match the ones later emitted into files.
        # Needed because manifest writes happen in the parent process
        # where per-worker file generation hasn't touched every instance.
        for instance in instances:
            emit_instance_rows(instance)
        self._instances = instances
        # Map each instance to the file windows it overlaps.
        by_window: dict[int, list[TypologyInstance]] = {}
        for instance in instances:
            for fid in self._file_ids_overlapping(
                instance.injection_ts_start, instance.injection_ts_end
            ):
                by_window.setdefault(fid, []).append(instance)
        self._instances_by_window = by_window

    def _span_seconds(self) -> float:
        return max(
            1.0,
            (self.config.timestamp_end - self.config.timestamp_start).total_seconds(),
        )

    def _file_window(self, file_id: int) -> tuple[datetime, datetime]:
        total_files = max(1, self.config.total_files)
        step_s = self._span_seconds() / total_files
        window_start = self.config.timestamp_start + timedelta(seconds=step_s * file_id)
        window_end = self.config.timestamp_start + timedelta(seconds=step_s * (file_id + 1))
        return window_start, window_end

    def _file_ids_overlapping(self, start: datetime, end: datetime) -> list[int]:
        total_files = max(1, self.config.total_files)
        step_s = self._span_seconds() / total_files
        start_s = (start - self.config.timestamp_start).total_seconds()
        end_s = (end - self.config.timestamp_start).total_seconds()
        first = max(0, int(start_s // step_s))
        last = min(total_files - 1, int(end_s // step_s))
        return list(range(first, last + 1))

    def _emit_baseline_rows(
        self,
        rng: np.random.Generator,
        file_id: int,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict]:
        """Baseline non-typology transactions for this file's window."""
        # Import here to avoid a circular at module-load time.
        from typologies import _base_row

        rows_per_file = self.config.rows_per_file
        window_span_s = max(1.0, (window_end - window_start).total_seconds())
        rows: list[dict] = []
        for _ in range(rows_per_file):
            offset = float(rng.uniform(0, window_span_s))
            ts = window_start + timedelta(seconds=offset)
            originator = int(rng.integers(1, self.config.customer_id_max + 1))
            beneficiary = int(rng.integers(1, self.config.customer_id_max + 1))
            amount = Decimal(str(round(float(rng.uniform(50, 20_000)), 2)))
            currency = "USD"
            rows.append(_base_row(rng, ts, originator, beneficiary, amount, currency, "TRAD"))
        return rows

    def _rows_to_table(self, rows: list[dict]) -> pa.Table:
        """Reshape row-dicts into columnar arrays under PACS008_SCHEMA."""
        columns: dict[str, list] = {field.name: [] for field in PACS008_SCHEMA}
        for row in rows:
            for field in PACS008_SCHEMA:
                columns[field.name].append(row.get(field.name))
        return pa.Table.from_pydict(columns, schema=PACS008_SCHEMA)
