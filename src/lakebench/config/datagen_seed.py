"""The top-level datagen seed a deployment generates with.

The seed names the corpus: the AML reference job reports it (LB_DATAGEN_SEED)
and the AML pre-registration assigns seeds to roles (calibration, evaluation,
robustness) and retires seeds that have been looked at (AML-GOALS R3, section 9
#38). A cluster run used to hard-code 42 in three places, which is the spent D0
seed, so every cluster corpus silently reused it.

Resolution:

- ``datagen.seed`` set in the config: that seed. For the financial schema a
  seed the pre-registration lists in ``corpora.spent_seeds`` is refused, so a
  retired corpus cannot be regenerated and scored by accident.
- unset, financial schema: the pre-registration's calibration seed. Tuning
  runs are what an unconfigured deployment is for; the evaluation and
  robustness seeds are only ever used when named explicitly.
- unset, any other schema: 42, the seed those corpora have always used, so
  their data does not change.

Only the standard library is imported, so config validation stays cheap.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

#: Seed for schemas the AML pre-registration does not govern (Customer 360,
#: IoT). Unchanged from the old hard-coded value so their corpora stay the same.
NON_AML_DEFAULT_SEED = 42

_PREREG_PATH = (
    Path(__file__).resolve().parent.parent / "spark" / "data" / "aml" / "aml_preregistration.json"
)


@lru_cache(maxsize=1)
def _corpora() -> dict:
    with open(_PREREG_PATH, encoding="utf-8") as f:
        return json.load(f)["corpora"]


def spent_seeds() -> frozenset[int]:
    """Seeds the AML pre-registration has retired (looked at, or voided)."""
    return frozenset(int(s) for s in _corpora().get("spent_seeds", []))


def calibration_seed() -> int:
    return int(_corpora()["calibration_seed"])


def check_seed(seed: int, schema: str) -> None:
    """Raise ValueError for a financial-schema seed the pre-registration retired."""
    if schema == "financial" and seed in spent_seeds():
        raise ValueError(
            f"datagen.seed {seed} is listed as spent in the AML pre-registration "
            "(corpora.spent_seeds): a corpus from it has already been looked at or "
            "voided and must not be regenerated. Leave datagen.seed unset for the "
            f"calibration seed ({calibration_seed()}) or name another seed."
        )


def resolve_seed(seed: int | None, schema: str) -> int:
    """The seed a deployment generates with (see the module docstring)."""
    if seed is None:
        seed = calibration_seed() if schema == "financial" else NON_AML_DEFAULT_SEED
    check_seed(seed, schema)
    return seed


def config_seed(cfg) -> int:
    """``resolve_seed`` for a LakebenchConfig."""
    workload = cfg.architecture.workload
    return resolve_seed(workload.datagen.seed, workload.schema_type.value)
