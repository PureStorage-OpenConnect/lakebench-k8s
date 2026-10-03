"""The four AML batch/stream protocol guards go red when one business column
of a stream MERGE source is nulled (design 04 V16-2, parity proof).

Each case runs a guard's own Spark child with ``LB_PARITY_MUTATE`` set
(``_parity_mutation`` nulls the column in that MERGE's materialised source)
and asserts three things:

- the shim ran and changed the source rows (its record file has an entry
  whose AM-15a fingerprint differs before and after);
- the child finished and printed its result, so the guard did not fail by
  crashing;
- the guard's own ``problems()`` names the failure the mutation must cause.

The guards' own tests are the unmutated controls for the three child guards
(their children call the same ``install()`` with the variable unset); the
profiles guard runs in process, so its child runs here unmutated too.

Each guard is shown sensitive to one column; the shim reaches only
materialised MERGE sources (not the statements or transactions writes).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.requires_jars("iceberg")

HERE = Path(__file__).resolve().parent

# (guard module, mutation, replayed run only, failures problems() must name)
CASES = [
    ("test_aml_stream_dimension_parity_full", "entities.country", None, {"entities_match"}),
    (
        "test_aml_batch_stream_statements_parity",
        "accounts.current_balance",
        None,
        {"current_balance"},
    ),
    (
        "test_aml_stream_statements_replay_idempotent",
        "accounts.current_balance",
        "stream-run-2",
        {"current_balance"},
    ),
    ("test_aml_batch_stream_profiles_parity", "profiles.first_seen_ts", None, {"first_seen_ts"}),
]


def _named(module, out):
    """The failures the guard reports for the child's JSON."""
    if module == "test_aml_batch_stream_profiles_parity":
        names = set()
        for group in ("coverage", "sums", "nulls", "values"):
            for line in out[group]:
                names.add(line.split(":", 1)[0].split(".", 1)[-1])
        return names
    mod = __import__(module)
    return set(mod.problems(out))


@pytest.mark.parametrize(
    ("module", "mutation", "run", "expected"),
    CASES,
    ids=[f"{m.split('test_', 1)[1]}-{mut}" for m, mut, _r, _e in CASES],
)
def test_guard_goes_red_under_mutation(
    module, mutation, run, expected, spark_subprocess, spark_jars, tmp_path
):
    record = tmp_path / "mutations.jsonl"
    env = {"LB_PARITY_MUTATE": mutation, "LB_PARITY_MUTATE_RECORD": str(record)}
    if run:
        env["LB_PARITY_MUTATE_RUN"] = run
    proc = spark_subprocess(
        HERE / f"{module}.py", spark_jars.classpath, env=env, timeout=900, check=False
    )
    assert proc.returncode == 0, proc.stderr[-4000:]

    assert record.exists(), "the mutation never reached a MERGE source"
    applied = [json.loads(line) for line in record.read_text().splitlines()]
    assert {a["mutation"] for a in applied} == {mutation}, applied
    assert any(a["rows"] > 0 and a["fp_before"] != a["fp_after"] for a in applied), applied

    out = json.loads(proc.stdout.strip().splitlines()[-1])
    named = _named(module, out)
    assert expected <= named, f"{module} under {mutation} reported {named}: {out}"


def test_profiles_child_unmutated_is_clean(spark_subprocess, spark_jars):
    proc = spark_subprocess(
        HERE / "test_aml_batch_stream_profiles_parity.py", spark_jars.classpath, timeout=900
    )
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    assert out == {"coverage": [], "sums": [], "nulls": [], "values": []}, out
