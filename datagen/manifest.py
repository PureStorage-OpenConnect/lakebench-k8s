"""Manifest sidecar for the Financial (FinServ-Crime, AML) datagen.

Ground-truth record of which synthetic transactions belong to which
injected typology instance. Read by score_financial.py to compute
recall against workload detections. Written by FinancialGenerator once
per generation run.

Format: single Parquet file at ``{prefix}/manifest/manifest-{run_id}.parquet``.
Recommended in docs/lakebench.next-spec-eng-2c3-memo.md section 3.
"""

from __future__ import annotations

import io
from dataclasses import asdict, dataclass, field
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

MANIFEST_SCHEMA = pa.schema(
    [
        ("typology_id", pa.string()),
        ("typology_type", pa.string()),
        ("participant_entity_ids", pa.list_(pa.int64())),
        ("participant_uetrs", pa.list_(pa.string())),
        ("injection_ts_start", pa.timestamp("us")),
        ("injection_ts_end", pa.timestamp("us")),
        ("expected_workload", pa.string()),
        ("severity", pa.string()),
        ("seed", pa.int64()),
    ]
)


@dataclass
class TypologyInstance:
    """One scheduled/injected typology occurrence.

    ``participant_uetrs`` is populated by the typology's ``inject``
    method as it emits transactions; the scheduler leaves it empty.
    """

    typology_id: str
    typology_type: str
    participant_entity_ids: list[int]
    injection_ts_start: datetime
    injection_ts_end: datetime
    expected_workload: str
    severity: str
    seed: int
    participant_uetrs: list[str] = field(default_factory=list)

    def to_row(self) -> dict:
        return asdict(self)


def build_manifest_table(instances: list[TypologyInstance]) -> pa.Table:
    """Assemble typology instances into a Parquet-ready Arrow table."""
    if not instances:
        return pa.Table.from_pydict(
            {field.name: [] for field in MANIFEST_SCHEMA}, schema=MANIFEST_SCHEMA
        )

    data: dict[str, list] = {name: [] for name in MANIFEST_SCHEMA.names}
    for inst in instances:
        row = inst.to_row()
        for name in MANIFEST_SCHEMA.names:
            data[name].append(row[name])
    return pa.Table.from_pydict(data, schema=MANIFEST_SCHEMA)


def serialise_manifest(instances: list[TypologyInstance]) -> bytes:
    """Return manifest Parquet bytes for upload."""
    table = build_manifest_table(instances)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()
