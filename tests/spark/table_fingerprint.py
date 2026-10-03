"""An order-independent fingerprint of a table's rows, for parity tests.

Not collected by pytest. Shared by the multi-cycle equivalence scenario
(C36-2) and the gold batch/stream parity scenario (V16-7): sha256 over the
sorted JSON rows of the business columns, so two tables with the same rows
in any order and any file layout have the same fingerprint, and one changed
value changes it. Imported by Spark children, so it does not import pyspark.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

#: Columns that differ between two correct builds of the same rows: when the
#: row was processed, which cycle or micro-batch wrote it, and the random
#: payload bytes.
NOT_BUSINESS = frozenset({"silver_processing_timestamp", "_batch_id", "interaction_payload"})


def table_fingerprint(df: Any, excluded: frozenset[str] = NOT_BUSINESS) -> dict[str, Any]:
    """``{rows, columns, sha256}`` of a DataFrame's business columns."""
    cols = sorted(c for c in df.columns if c not in excluded)
    rows = sorted(
        json.dumps(r.asDict(), sort_keys=True, default=str) for r in df.select(*cols).collect()
    )
    return {
        "rows": len(rows),
        "columns": cols,
        "sha256": hashlib.sha256("\n".join(rows).encode()).hexdigest(),
    }
