"""The report's words, in one place.

Every label that names a mode or a measurement condition reads from here,
so the page, the terminal report and the end-of-run panel say the same
thing. Lakebench modes are batch and continuous: older records store
continuous as ``sustained``, and the page never shows that word, nor
"streaming".
"""

from __future__ import annotations

BATCH = "batch"
CONTINUOUS = "continuous"
#: One benchmark pass run while a continuous pipeline is writing.
IN_STREAM_ROUND = "in-stream round"
#: The intake rate the trickle released: a Lakebench setting, not a capacity.
OFFERED_LOAD = "offered load"

_MODES = {"batch": BATCH, "sustained": CONTINUOUS, "continuous": CONTINUOUS}

_WORKLOADS = {"customer360": "Customer 360", "financial": "AML"}


def mode_label(mode: str | None) -> str:
    """``batch`` or ``continuous`` for a stored pipeline mode."""
    return _MODES.get(str(mode or "batch"), str(mode))


def workload_label(schema: str | None) -> str:
    return _WORKLOADS.get(str(schema or "customer360"), str(schema))
