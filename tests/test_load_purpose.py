"""LoadPurpose: what each command may load.

A config with no name cannot change data, a removed key or a setting nothing
honours is refused by the commands that change data and dropped with a note
by the others, and no read-only command writes a file.
"""

from __future__ import annotations

import importlib
import re
import shutil
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.config import LakebenchConfig, load_config
from lakebench.config.loader import (
    ConfigFileNotFoundError,
    ConfigNameRequired,
    ConfigValidationError,
    LoadPurpose,
    load_notes,
    name_resolution,
)

runner = CliRunner()

FIXTURES = Path(__file__).parent / "fixtures"
NAMELESS = {"endpoint": "http://127.0.0.1:9", "access_key": "k", "secret_key": "s", "scale": 1}


def _write(directory: Path, data: dict, filename: str = "lakebench.yaml") -> Path:
    path = directory / filename
    path.write_text(yaml.safe_dump(data))
    return path


def _listing(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


# -- nameless configs ---------------------------------------------


@pytest.mark.parametrize("purpose", [LoadPurpose.MUTATE, LoadPurpose.RUN])
def test_nameless_config_refused_for_commands_that_change_data(tmp_path, purpose):
    cfg_path = _write(tmp_path, NAMELESS)
    with pytest.raises(ConfigNameRequired) as e:
        load_config(cfg_path, purpose=purpose)
    assert e.value.resolution.source == "suggested"
    assert _listing(tmp_path) == ["lakebench.yaml"]


def test_nameless_teardown_without_state_refused(tmp_path):
    # No v1.6 state and no name: a deployment under the suggested name was
    # made by some other config, so destroy and admin must not target it.
    cfg_path = _write(tmp_path, NAMELESS)
    with pytest.raises(ConfigNameRequired) as e:
        load_config(cfg_path, purpose=LoadPurpose.TEARDOWN)
    assert e.value.resolution.source == "suggested"
    # With an explicit override it loads.
    assert load_config(cfg_path, purpose=LoadPurpose.TEARDOWN, name_override="x").name == "x"


def test_v16_state_name_read_verbatim(tmp_path):
    # The v1.6 name is read, never written. A v1.6 directory without --name
    # is refused, so teardown and read loads refuse and name it; the --name
    # override and INSPECT load under it.
    (tmp_path / ".lakebench").mkdir()
    shutil.copy(FIXTURES / "v16-state" / "state.json", tmp_path / ".lakebench" / "state.json")
    before = (tmp_path / ".lakebench" / "state.json").read_bytes()
    cfg_path = _write(tmp_path, NAMELESS)
    for purpose in (LoadPurpose.TEARDOWN, LoadPurpose.READ, LoadPurpose.MUTATE):
        with pytest.raises(ConfigNameRequired) as e:
            load_config(cfg_path, purpose=purpose)
        assert "name: lb-20260915-101530" in str(e.value)
        assert e.value.resolution.source == "legacy-state"
    for purpose in (LoadPurpose.INSPECT,):
        cfg = load_config(cfg_path, purpose=purpose)
        assert cfg.name == "lb-20260915-101530"
        res = name_resolution(cfg)
        assert res is not None and res.source == "legacy-state"
    over = load_config(cfg_path, purpose=LoadPurpose.TEARDOWN, name_override="lb-20260915-101530")
    assert over.name == "lb-20260915-101530"
    assert (tmp_path / ".lakebench" / "state.json").read_bytes() == before
    assert _listing(tmp_path) == [".lakebench", ".lakebench/state.json", "lakebench.yaml"]


def test_corrupt_legacy_state_is_left_alone(tmp_path):
    (tmp_path / ".lakebench").mkdir()
    (tmp_path / ".lakebench" / "state.json").write_text("{not json")
    cfg = load_config(_write(tmp_path, NAMELESS), purpose=LoadPurpose.READ)
    res = name_resolution(cfg)
    assert res is not None and res.source == "suggested"
    assert (tmp_path / ".lakebench" / "state.json").read_text() == "{not json"


def test_name_override_must_match_config_name(tmp_path):
    cfg_path = _write(tmp_path, {**NAMELESS, "name": "mine"})
    assert load_config(cfg_path, purpose=LoadPurpose.READ, name_override="mine").name == "mine"
    with pytest.raises(ConfigValidationError, match="does not match the config's name"):
        load_config(cfg_path, purpose=LoadPurpose.READ, name_override="other")


# -- removed keys ----------------------------------------------

REMOVED_KEY = {
    "name": "rk",
    "recipe": "hive-iceberg-spark-trino",
    "images": {"pull_secrets": ["regcred"]},
}


@pytest.mark.parametrize("purpose", [LoadPurpose.MUTATE, LoadPurpose.RUN])
def test_removed_key_refused_under_mutate(tmp_path, purpose):
    with pytest.raises(ConfigValidationError) as e:
        load_config(_write(tmp_path, REMOVED_KEY), purpose=purpose)
    assert "pull_secrets" in str(e.value)


@pytest.mark.parametrize("purpose", [LoadPurpose.TEARDOWN, LoadPurpose.READ])
def test_removed_key_loads_for_destroy(tmp_path, purpose):
    cfg = load_config(_write(tmp_path, REMOVED_KEY), purpose=purpose)
    assert not hasattr(cfg.images, "pull_secrets")
    notes = load_notes(cfg)
    assert [n.kind for n in notes] == ["removed"]
    assert "pull_secrets" in notes[0].text


def test_default_purpose_is_mutate(tmp_path):
    with pytest.raises(ConfigValidationError, match="'pull_secrets' was removed"):
        load_config(_write(tmp_path, REMOVED_KEY))
    with pytest.raises(ConfigNameRequired):
        load_config(_write(tmp_path, NAMELESS, "nameless.yaml"))


def test_allow_long_names_alone_means_teardown(tmp_path):
    cfg = load_config(_write(tmp_path, REMOVED_KEY), allow_long_names=True)
    notes = load_notes(cfg)
    assert len(notes) == 1
    assert notes[0].kind == "removed"


def test_name_length_check_per_purpose(tmp_path):
    for purpose, skips in [
        (LoadPurpose.MUTATE, False),
        (LoadPurpose.RUN, False),
        (LoadPurpose.TEARDOWN, True),
        (LoadPurpose.READ, True),
    ]:
        path = _write(
            tmp_path, {"name": "ov-perf-c360-continuous-s10", "recipe": "hive-iceberg-spark-trino"}
        )
        if skips:
            assert load_config(path, purpose=purpose).name == "ov-perf-c360-continuous-s10"
        else:
            with pytest.raises(ConfigValidationError, match="at most 23"):
                load_config(path, purpose=purpose)


# -- LoadNotes ---------------------------------------------------------------


def test_old_file_size_follows_the_removed_key_rule(tmp_path):
    for purpose, loads in [
        (LoadPurpose.MUTATE, False),
        (LoadPurpose.RUN, False),
        (LoadPurpose.TEARDOWN, True),
        (LoadPurpose.READ, True),
    ]:
        path = _write(tmp_path, {"name": "fs", "workload": {"datagen": {"file_size": "128mb"}}})
        if loads:
            with pytest.warns(DeprecationWarning, match="fixed at 64mb"):
                cfg = load_config(path, purpose=purpose)
            assert cfg.workload.datagen.file_size == "64mb"
        else:
            with pytest.raises(ConfigValidationError, match="fixed at 64mb"):
                load_config(path, purpose=purpose, allow_long_names=True)


# -- Which purpose each verb loads with --------------------------------------

VERB_PURPOSES = [
    ("lakebench.cli._deploy", ["deploy"], LoadPurpose.MUTATE, False),
    ("lakebench.cli._generate", ["generate"], LoadPurpose.MUTATE, False),
    ("lakebench.cli._run", ["run"], LoadPurpose.RUN, False),
    ("lakebench.cli._clean", ["clean", "silver"], LoadPurpose.MUTATE, True),
    ("lakebench.cli", ["stop"], LoadPurpose.TEARDOWN, False),
    ("lakebench.cli._destroy", ["destroy"], LoadPurpose.TEARDOWN, False),
    (
        "lakebench.cli._financial",
        ["financial", "reference-score", "--manifest", "s3://m", "--output-prefix", "s3://o"],
        LoadPurpose.MUTATE,
        False,
    ),
    ("lakebench.cli._admin", ["admin", "migrate-deployment", "ns1"], LoadPurpose.TEARDOWN, False),
    ("lakebench.cli._admin", ["admin", "reclaim-bucket"], LoadPurpose.TEARDOWN, False),
]


@pytest.mark.parametrize(
    ("module", "argv", "purpose", "long_names"),
    VERB_PURPOSES,
    ids=[" ".join(v[1]) for v in VERB_PURPOSES],
)
def test_cli_verbs_load_with_their_purpose(
    module, argv, purpose, long_names, tmp_path, monkeypatch
):
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)
    seen: list[dict] = []

    def spy(path, **kwargs):
        seen.append(kwargs)
        raise ConfigFileNotFoundError(str(path))

    mod = importlib.import_module(module)
    # Modules that import load_config inside the command read it from
    # lakebench.config at call time.
    target = mod if hasattr(mod, "load_config") else importlib.import_module("lakebench.config")
    monkeypatch.setattr(target, "load_config", spy)
    cfg_path = _write(tmp_path, {**NAMELESS, "name": "verb"})
    runner.invoke(app, [str(cfg_path) if a == "CFG" else a for a in argv] + [str(cfg_path)])
    assert seen, f"{argv} never loaded the config"
    assert seen[0].get("purpose") == purpose
    assert bool(seen[0].get("allow_long_names")) is long_names


@pytest.mark.parametrize(
    "argv",
    [
        ["deploy"],
        ["generate"],
        ["run"],
        ["clean", "silver"],
        ["destroy"],
        ["admin", "reclaim-bucket"],
    ],
)
def test_validation_errors_print_brackets_verbatim(argv, tmp_path, monkeypatch):
    """A validation message holding ``[...]`` is printed as written and the
    command exits USAGE: unescaped, Rich swallows ``[main]`` and raises on
    ``[/tmp]``."""
    import lakebench.config

    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)

    def invalid(path, **kwargs):
        raise ConfigValidationError(
            "invalid", errors=[{"loc": ("platform", "[main]"), "msg": "bad path [/tmp]"}]
        )

    monkeypatch.setattr(lakebench.config, "load_config", invalid)
    cfg_path = _write(tmp_path, {**NAMELESS, "name": "verb"})
    result = runner.invoke(app, [*argv, str(cfg_path)])
    assert result.exit_code == 2, result.output
    assert "platform.[main]: bad path [/tmp]" in result.output


# -- v1.6 directories with several nameless configs --------------------------


def _v16_dir(tmp_path: Path) -> Path:
    (tmp_path / ".lakebench").mkdir()
    shutil.copy(FIXTURES / "v16-state" / "state.json", tmp_path / ".lakebench" / "state.json")
    return _write(tmp_path, NAMELESS, "a.yaml")


def test_v16_state_teardown_refused_when_siblings_share_the_name(tmp_path):
    # v1.6 gave a.yaml and b.yaml one name. destroy b.yaml must not reach the
    # deployment a.yaml made.
    a = _v16_dir(tmp_path)
    b = _write(tmp_path, NAMELESS, "b.yaml")
    for path, other in ((a, "b.yaml"), (b, "a.yaml")):
        with pytest.raises(ConfigNameRequired) as e:
            load_config(path, purpose=LoadPurpose.TEARDOWN)
        msg = " ".join(str(e.value).split())
        assert f"nameless {other}" in msg
        assert "name: lb-20260915-101530" in msg
        assert e.value.siblings == [tmp_path / other]
    # status refuses as destroy does.
    with pytest.raises(ConfigNameRequired):
        load_config(a, purpose=LoadPurpose.READ)
    # Naming the one that deployed it makes it loadable as itself, and the
    # other, now the only nameless config, still does not reach that
    # deployment.
    named = _write(tmp_path, {**NAMELESS, "name": "lb-20260915-101530"}, "a.yaml")
    assert load_config(named, purpose=LoadPurpose.TEARDOWN).name == "lb-20260915-101530"
    for purpose in (LoadPurpose.TEARDOWN, LoadPurpose.READ):
        with pytest.raises(ConfigNameRequired) as e:
            load_config(b, purpose=purpose)
        assert e.value.siblings == []


def test_sibling_scan_ignores_files_that_are_not_nameless_configs(tmp_path):
    import os

    from lakebench.config.deploy_state import other_nameless_configs

    a = _v16_dir(tmp_path)
    _write(tmp_path, {**NAMELESS, "name": "other"}, "named.yaml")
    _write(tmp_path, {"apiVersion": "v1", "kind": "ConfigMap"}, "manifest.yaml")
    (tmp_path / "broken.yml").write_text("{not: [yaml")
    # yaml.safe_load raises a plain ValueError on an impossible date.
    (tmp_path / "dated.yaml").write_text("release: 2026-02-30\n")
    (tmp_path / "huge.yaml").write_text(yaml.safe_dump(NAMELESS) + "#" * (1 << 20))
    (tmp_path / "notes.txt").write_text(yaml.safe_dump(NAMELESS))
    (tmp_path / "alias.yaml").symlink_to(a)
    os.link(a, tmp_path / "hard.yaml")
    (tmp_path / "sub").mkdir()
    _write(tmp_path / "sub", NAMELESS, "c.yaml")
    assert other_nameless_configs(a) == []
    # The links are the same file, not a second config.
    assert other_nameless_configs(tmp_path / "alias.yaml") == []
    assert other_nameless_configs(tmp_path / "hard.yaml") == []


def test_v16_state_mutate_refusal_does_not_offer_a_shared_name(tmp_path):
    # Offering "name: X" when another nameless config may have made X steers
    # the user into redeploying over that deployment.
    a = _v16_dir(tmp_path)
    with pytest.raises(ConfigNameRequired) as alone:
        load_config(a, purpose=LoadPurpose.MUTATE)
    assert alone.value.resolution.source == "legacy-state"
    _write(tmp_path, NAMELESS, "b.yaml")
    with pytest.raises(ConfigNameRequired) as shared:
        load_config(a, purpose=LoadPurpose.MUTATE)
    assert shared.value.siblings == [tmp_path / "b.yaml"]
    suggestions = re.findall(r"'name: ([^']+)'", str(shared.value))
    assert suggestions
    assert suggestions[0] != "lb-20260915-101530"


@pytest.mark.parametrize(
    "sibling",
    [
        {"secret_ref": "s3-creds", "mode": "batch"},  # flat keys only
        {**NAMELESS, "name": "${LB_TEST_NAME:-}"},
        {**NAMELESS, "name": "${LB_TEST_NAME}"},
    ],
    ids=["flat-only", "env-name-default", "env-name"],
)
def test_sibling_scan_counts_every_nameless_form(tmp_path, monkeypatch, sibling):
    # An env-reference name may have resolved to nothing when v1.6 deployed,
    # whatever the environment says now, so it counts as nameless.
    from lakebench.config.deploy_state import other_nameless_configs

    monkeypatch.setenv("LB_TEST_NAME", "other")
    a = _v16_dir(tmp_path)
    _write(tmp_path, sibling, "b.yaml")
    assert other_nameless_configs(a) == [tmp_path / "b.yaml"]


# -- settings nothing honours ------------------------------------------------

_BASE = {
    "name": "unhonoured-t",
    "platform": {
        "storage": {
            "s3": {
                "endpoint": "http://minio:9000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
            }
        }
    },
}


def _write_over_base(tmp_path: Path, extra: dict) -> Path:
    data = yaml.safe_load(yaml.safe_dump(_BASE))

    def merge(dst: dict, src: dict) -> None:
        for k, v in src.items():
            if isinstance(v, dict) and isinstance(dst.get(k), dict):
                merge(dst[k], v)
            else:
                dst[k] = v

    merge(data, extra)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


_EXECUTOR = {"platform": {"compute": {"spark": {"executor": {"instances": 16, "memory": "8g"}}}}}
_SPARK_OP = {"platform": {"compute": {"spark": {"operator": {"install": True}}}}}
_HIVE_OP = {"architecture": {"catalog": {"hive": {"operator": {"install": True}}}}}


@pytest.mark.parametrize(
    ("extra", "loc"),
    [
        (_EXECUTOR, ("platform", "compute", "spark")),
        (_HIVE_OP, ("architecture", "catalog", "hive", "operator")),
    ],
)
def test_unhonoured_setting_refused_on_data_changing_commands(tmp_path, extra, loc):
    with pytest.raises(ConfigValidationError) as e:
        load_config(
            _write_over_base(tmp_path, extra), purpose=LoadPurpose.MUTATE, print_notes=False
        )
    assert [err["loc"] for err in e.value.errors] == [loc]


def test_operator_install_coerced_true_refused(tmp_path):
    path = _write_over_base(
        tmp_path, {"platform": {"compute": {"spark": {"operator": {"install": "yes"}}}}}
    )
    with pytest.raises(ConfigValidationError, match="install: true' is refused"):
        load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)


@pytest.mark.parametrize("extra", [_SPARK_OP, _HIVE_OP])
def test_operator_install_true_loads_false_for_teardown(tmp_path, extra):
    path = _write_over_base(tmp_path, extra)
    cfg = load_config(path, purpose=LoadPurpose.TEARDOWN, print_notes=False)
    assert cfg.platform.compute.spark.operator.install is False
    assert cfg.architecture.catalog.hive.operator.install is False
    assert "install: true' is ignored" in " ".join(load_notes(cfg).texts())


# -- run-only benchmark refusals ----------------------------------------------


def test_run_refuses_what_it_does_not_run(tmp_path):
    for bench, needle in [
        ({"cache": "cold"}, "lakebench benchmark --cold"),
        ({"mode": "throughput"}, "lakebench benchmark --mode"),
        ({"mode": "composite"}, "lakebench benchmark --mode"),
        ({"streams": 4}, "lakebench benchmark --streams"),
    ]:
        path = _write_over_base(tmp_path, {"architecture": {"benchmark": bench}})
        with pytest.raises(ConfigValidationError) as e:
            load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
        assert needle in str(e.value)
        # `lakebench benchmark` (MUTATE) honours all of them.
        cfg = load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)
        assert cfg.architecture.benchmark.model_dump(include=set(bench)) == bench


def test_run_refuses_explicit_streams(tmp_path):
    # An explicit value above 1 is refused for every workload and mode; the
    # schema default (4, used by `lakebench benchmark`) is not.
    for schema, mode in (("customer360", "batch"), ("financial", "continuous")):
        path = _write_over_base(
            tmp_path,
            {
                "architecture": {
                    "benchmark": {"streams": 2},
                    "workload": {"schema": schema},
                    "pipeline": {"mode": mode},
                },
            },
        )
        with pytest.raises(ConfigValidationError, match="benchmark.streams 2"):
            load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    # A benchmark block without streams keeps the default 4 unset: allowed.
    unset = _write_over_base(tmp_path, {"architecture": {"benchmark": {"iterations": 3}}})
    cfg = load_config(unset, purpose=LoadPurpose.RUN, print_notes=False)
    assert cfg.architecture.benchmark.streams == 4
    one = _write_over_base(tmp_path, {"architecture": {"benchmark": {"streams": 1}}})
    load_config(one, purpose=LoadPurpose.RUN, print_notes=False)


def test_run_snapshot_records_power_hot_1(tmp_path):
    from lakebench.metrics.collector import build_config_snapshot

    path = _write_over_base(
        tmp_path,
        {"architecture": {"benchmark": {"mode": "extended", "iterations": 5}}},
    )
    cfg = load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    bench = build_config_snapshot(cfg, run_mode="batch")["benchmark"]
    assert {k: bench[k] for k in ("mode", "streams", "cache", "iterations")} == {
        "mode": "power",
        "streams": 1,
        "cache": "hot",
        "iterations": 5,
    }
    # A batch config run with --continuous records the in-stream rounds.
    cont = build_config_snapshot(cfg, run_mode="continuous")["benchmark"]
    assert cont == {"mode": "power", "streams": 1, "cache": "hot", "iterations": 1}


# -- errors never echo their input ------------------------------------------

_SEED = 4343434343


def _all_error_text(exc: BaseException) -> str:
    """Every text a caller prints: the message, the repr and, for the
    loader's error, the error dicts the CLI iterates."""
    parts = [str(exc), repr(exc)]
    errors = getattr(exc, "errors", None)
    if isinstance(errors, list):
        parts.append(repr(errors))
    return "\n".join(parts)


@pytest.mark.parametrize(
    "bad",
    [
        # A list where the workload block belongs: the error's input is the
        # whole list, seed included.
        {"architecture": {"workload": [{"datagen": {"seed": _SEED}}]}},
        # A model-level failure on the architecture block (an unsupported
        # combination), whose input is every architecture key.
        {
            "architecture": {
                "workload": {"datagen": {"seed": _SEED}},
                "catalog": {"type": "unity"},
                "table_format": {"type": "iceberg"},
            }
        },
    ],
    ids=["type-error", "model-validator"],
)
def test_validation_error_never_echoes_a_seed(tmp_path, bad):
    data = {**_BASE, **bad}
    with pytest.raises(ValidationError) as e:
        LakebenchConfig.model_validate(data)
    assert str(_SEED) not in _all_error_text(e.value)

    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(ConfigValidationError) as ce:
        load_config(path, purpose=LoadPurpose.READ, print_notes=False)
    assert str(_SEED) not in _all_error_text(ce.value)
    for err in ce.value.errors:
        assert "input" not in err
    # The pydantic error (which keeps its input) is not chained on.
    assert ce.value.__cause__ is None and ce.value.__suppress_context__
