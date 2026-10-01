"""CFG-4 (CC-14): bounds, did-you-mean, the nearest recipe and wrong-workload notes.

Each case is a config under tests/fixtures/config-messages/ and the message it
must produce, in a .txt beside it. The expected messages were written by hand
from the requirement text, not generated from this code; a case whose name
starts with ``note-`` loads and must carry the message as a load note, every
other case must be refused with it as one error line.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest

from lakebench.config import load_config
from lakebench.config.loader import (
    ConfigValidationError,
    LoadPurpose,
    load_notes,
)

CASES = Path(__file__).parent / "fixtures" / "config-messages"
NAMES = sorted(p.stem for p in CASES.glob("*.yaml"))


def test_cases_found():
    assert len(NAMES) >= 15
    for name in NAMES:
        assert (CASES / f"{name}.txt").exists(), name


@pytest.mark.parametrize("name", NAMES)
def test_config_message_golden(name):
    expected = (CASES / f"{name}.txt").read_text().rstrip("\n")
    path = CASES / f"{name}.yaml"
    if name.startswith("note-"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cfg = load_config(path, purpose=LoadPurpose.READ)
        assert expected in load_notes(cfg).texts()
        return
    with pytest.raises(ConfigValidationError) as e:
        load_config(path)
    assert expected in str(e.value).splitlines()


def test_unknown_key_error_keeps_its_location_in_errors(tmp_path):
    with pytest.raises(ConfigValidationError) as e:
        load_config(CASES / "workload-typo.yaml")
    (err,) = e.value.errors
    assert tuple(err["loc"]) == ("workload", "datagen", "scael")
    assert err["type"] == "extra_forbidden"


def test_unknown_recipe_error_is_rooted_at_recipe():
    with pytest.raises(ConfigValidationError) as e:
        load_config(CASES / "recipe-near.yaml")
    (err,) = e.value.errors
    assert tuple(err["loc"]) == ("recipe",)


def test_wrong_workload_note_is_not_a_deprecation(tmp_path):
    # A note, not a refusal, and not a DeprecationWarning: no identity moves
    # and a docs block with the key still loads under the strict docs test.
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        cfg = load_config(CASES / "note-wrong-workload-financial.yaml")
    assert any(n.kind == "wrong-workload" for n in load_notes(cfg))


def test_default_values_raise_no_wrong_workload_note(tmp_path):
    # save_config writes every field; the defaults must stay quiet.
    from lakebench.config.loader import save_config

    cfg = load_config(CASES / "note-wrong-workload-financial.yaml")
    cfg.architecture.workload.customer360.unique_customers = None
    out = tmp_path / "saved.yaml"
    save_config(cfg, out)
    reloaded = load_config(out)
    assert not [n for n in load_notes(reloaded) if n.kind == "wrong-workload"]


def test_retention_keys_are_not_wrong_workload_under_c360(tmp_path):
    # The pre-benchmark maintenance reads them for every workload.
    path = tmp_path / "c.yaml"
    path.write_text("name: t\nworkload:\n  retention_workload: true\n  retention_months: 24\n")
    cfg = load_config(path)
    assert not [n for n in load_notes(cfg) if n.kind == "wrong-workload"]


def test_timestamps_under_financial_get_no_note(tmp_path):
    # The financial generator ignores them, but they set silver's data clock.
    path = tmp_path / "c.yaml"
    path.write_text(
        "name: t\nworkload:\n  schema: financial\n  datagen:\n    timestamp_end: '2025-06-01'\n"
    )
    cfg = load_config(path)
    assert not [n for n in load_notes(cfg) if n.kind == "wrong-workload"]
