"""c360 multi-cycle correctness (E2).

- gold INCREMENTAL appended a last_updated column the other strategies do not
  write, failing cycle 2 on a schema mismatch; its strict > watermark dropped
  rows on the last processed date.
- silver SALTED ignored incremental mode (createOrReplace wiped earlier
  cycles) and salted nothing.
- a failed cycle datagen was a warning, so the pipeline rebuilt the cycle
  from the previous cycle's bronze and incremental silver appended it twice.
"""

from __future__ import annotations

from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def test_cycle_datagen_failure_is_fatal():
    src = (Path(__file__).resolve().parents[1] / "src/lakebench/cli/_run.py").read_text()
    i = src.index("datagen_result = _cycle_datagen.deploy_cycle")
    window = src[i : src.index("# Cycle env vars for incremental mode", i)]
    assert 'print_warning(f"Datagen cycle' not in window
    assert window.count("pipeline_success = False") >= 3
