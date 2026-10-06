"""LB-239: no message the AML seed guard raises or returns, nor the CLI text
built from it, names a protected seed (held out or spent) or a held-out
member of the spent list. Every refusal names a role or a kind instead.

A sweep over the guard's entry points (config load, the seed checks, the
manifest verdicts, the hash-file and pre-registration readers, the look
ledger, the experiment block's seed error and the commands that print a
config's resolution). TEST VALUES ONLY: tests/fixtures/heldout_test.json."""

from __future__ import annotations

import json
import re

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.config import datagen_seed as ds
from tests.fixtures import heldout_test_seeds as ts
from tests.fixtures import protected_corpus as pc

PROTECTED = (pc.EV, pc.RB, pc.SPENT)
_DIGITS = re.compile(r"\d+")


def _clean(text: str) -> None:
    """No digit run in *text* contains a protected seed."""
    wanted = {str(s) for s in PROTECTED}
    hits = [t for t in _DIGITS.findall(text) if any(w in t for w in wanted)]
    assert hits == [], f"a protected seed reached the message: {text[:300]!r}"


@pytest.fixture
def held(monkeypatch):
    return pc.use_heldout(monkeypatch)


def _message(fn, *a, **kw) -> str:
    """The message *fn* returns (a str: a refusal reason) or raises. A
    non-text return (a resolved seed, a set of recovered seeds) is data the
    caller asked for, not a message, and is not checked."""
    try:
        out = fn(*a, **kw)
    except Exception as e:  # noqa: BLE001 -- the message is what is checked
        return f"{type(e).__name__}: {e}"
    return out if isinstance(out, str) else ""


@pytest.mark.parametrize("seed", PROTECTED, ids=["evaluation", "robustness", "spent"])
@pytest.mark.parametrize("role", [None, "calibration", "evaluation", "robustness"])
def test_seed_checks_name_no_protected_seed(held, seed, role):
    for text in (
        _message(ds.check_seed, seed, "financial", role),
        _message(ds.resolve_seed, seed, "financial", role),
        _message(ds.aml_seed_error, pc.CORPORA, seed, role),
        _message(ds.aml_seed_error, pc.CORPORA, 43, role, matched=[seed]),
        _message(ds.aml_seed_error, pc.CORPORA, seed, role, matched=[pc.EV, pc.RB]),
        _message(ds.aml_seed_error, {**pc.CORPORA, "registered_looks_open": False}, seed, role),
    ):
        _clean(text)


@pytest.mark.parametrize("seed", PROTECTED, ids=["evaluation", "robustness", "spent"])
def test_manifest_verdicts_name_no_protected_seed(held, seed):
    rows = ts.manifest_rows(seed, 12) + ts.screening_rows(seed, 4)
    _clean(_message(ds.manifest_protected_reason, iter(rows), spent=[pc.SPENT]))
    mixed = ts.manifest_rows(pc.EV, 6) + ts.manifest_rows(pc.RB, 6) + [("x_1_0000001", seed)]
    _clean(_message(ds.recover_corpus_seeds, iter(mixed)))


def test_spent_list_readers_name_no_member(held):
    # A held-out seed appended to the pre-registration's spent list by
    # mistake, next to a non-integer: the refusal names types only.
    _clean(_message(ds.spent_from, {"spent_seeds": [pc.EV, "x"]}))
    _clean(_message(ds.spent_from, {"spent_seeds": pc.EV}))
    _clean(repr(held))
    doc = json.loads(ts.FIXTURE.read_text())
    doc["spent"].append(pc.EV)
    doc["spent"].append(2**64)
    path_problems = ds._heldout_doc_problems(doc)
    _clean(json.dumps(path_problems))
    old = json.loads(ts.FIXTURE.read_text())
    _clean(json.dumps(ds.heldout_history_problems(old, doc, {"looks": []})))


def test_a_role_conflict_names_roles_only(held):
    conflict = ds.HeldOut(
        salt=held.salt,
        roles={
            "evaluation": frozenset({ds.seed_hash(held.salt, pc.EV)}),
            "robustness": frozenset(),
        },
        spent=frozenset(),
        absence_check=held.absence_check,
        floor_salt=held.salt,
        floor={
            "evaluation": frozenset(),
            "robustness": frozenset({ds.seed_hash(held.salt, pc.EV)}),
        },
    )
    _clean(_message(ds.heldout_role, pc.EV, conflict))
    _clean(_message(ds.seed_is_protected, pc.EV, conflict))


def test_look_ledger_names_the_role_not_the_seed(held, tmp_path, monkeypatch):
    ledger = tmp_path / "looks.jsonl"
    ledger.write_text(json.dumps({"role": "evaluation", "seed": pc.EV}) + "\n")
    monkeypatch.setenv("LB_AML_LOOKS_LEDGER", str(ledger))
    _clean(_message(ds.seed_ever_recorded, pc.EV))
    ledger.write_text("not json\n")
    _clean(_message(ds.seed_ever_recorded, pc.EV))


@pytest.mark.parametrize("seed", PROTECTED, ids=["evaluation", "robustness", "spent"])
def test_experiment_seed_error_names_no_seed(held, tmp_path, seed):
    from lakebench.config import LoadPurpose, load_config
    from lakebench.metrics.experiment import experiment_inputs

    path = pc.financial_config(tmp_path / "c.yaml", seed=seed)
    cfg = load_config(path, purpose=LoadPurpose.INSPECT, print_notes=False)
    _clean(json.dumps(experiment_inputs(cfg), default=str))


#: Verbs that must refuse a protected corpus (exit 2); the others load it
#: (inspect, plan, deploy a registered look's namespace) and must still not
#: print the seed.
REFUSING = {"run", "generate"}


@pytest.mark.parametrize("seed", PROTECTED, ids=["evaluation", "robustness", "spent"])
@pytest.mark.parametrize("role", [None, "registered"], ids=["no-role", "registered-role"])
@pytest.mark.parametrize(
    "argv",
    [
        ["plan", "{cfg}", "--offline"],
        ["plan", "{cfg}", "--offline", "--json"],
        ["config", "show", "{cfg}"],
        ["config", "validate", "{cfg}"],
        ["run", "{cfg}", "--yes"],
        ["generate", "{cfg}", "--yes"],
        ["deploy", "{cfg}", "--yes", "--dry-run"],
    ],
    ids=lambda a: " ".join(a[:2]),
)
def test_commands_print_no_protected_seed(held, tmp_path, monkeypatch, caplog, seed, role, argv):
    import logging

    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "placeholder")
    monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", "placeholder")
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    # The registered role of a held-out seed (looks are open in the fixture):
    # the config loads, so the per-verb guards and the planned experiment are
    # what run. A spent seed has no registered role and is refused at load.
    declared = None
    if role == "registered":
        declared = {pc.EV: "evaluation", pc.RB: "robustness"}.get(seed)
        if declared is None:
            pytest.skip("a spent seed has no registered role")
    cfg = pc.financial_config(tmp_path / "c.yaml", seed=seed, role=declared)
    result = CliRunner().invoke(app, [a.replace("{cfg}", str(cfg)) for a in argv])
    _clean(result.output)
    _clean(str(result.exception or ""))
    _clean(caplog.text)
    if argv[0] in REFUSING:
        assert result.exit_code == 2, result.output
        said = " ".join(result.output.split()).lower()
        assert any(w in said for w in ("protected", "registered", "spent")), said
