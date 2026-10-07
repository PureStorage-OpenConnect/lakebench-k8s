"""CFG-5 and CFG-8 (CC-15): what ``init`` writes, recipe conflicts at load,
and the default-recipe deprecation.

The first-day config holds a unique name, the recipe once, the workload and
scale, the endpoint and the two S3 credentials as ``${VAR}`` references, in
no more than 12 non-comment lines. A component written against its recipe
is refused at load, naming both keys; before v1.7 the written value won
silently, so ``init --recipe polaris-*`` deployed Hive. A config with no
recipe, or ``recipe: default``, still resolves as before, with a note.
"""

from __future__ import annotations

import re
import warnings

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.cli._init import LINE_BUDGET, default_name, first_day_config
from lakebench.config import ConfigValidationError, load_config
from lakebench.config.recipes import RECIPES, recipe_components, user_set
from lakebench.config.schema import LakebenchConfig
from lakebench.config.support import recipe_names

runner = CliRunner()

SENTINEL_AK = "SENTINEL-ACCESS-7Q"
SENTINEL_SK = "SENTINEL-SECRET-9Z"


def _code_lines(text: str) -> list[str]:
    return [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]


@pytest.fixture
def creds(monkeypatch):
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", SENTINEL_AK)
    monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", SENTINEL_SK)


def _components(cfg: LakebenchConfig) -> dict[str, str]:
    arch = cfg.architecture
    return {
        "architecture.catalog.type": arch.catalog.type.value,
        "architecture.table_format.type": arch.table_format.type.value,
        "architecture.pipeline_engine": arch.pipeline_engine.value,
        "architecture.query_engine.type": arch.query_engine.type.value,
    }


# -- init output -------------------------------------------------------------


@pytest.mark.parametrize("workload", ["customer360", "financial"])
@pytest.mark.parametrize("recipe", recipe_names())
def test_init_all_recipes_load(tmp_path, creds, recipe, workload):
    out = tmp_path / "lakebench.yaml"
    r = runner.invoke(app, ["init", "-r", recipe, "-w", workload, "-o", str(out)])
    if workload == "financial" and "-delta-" in recipe:
        # The financial workload is Iceberg-only: init refuses rather than
        # write a file that does not load.
        assert r.exit_code == 2, r.output
        assert "iceberg" in r.output and not out.exists()
        return
    assert r.exit_code == 0, r.output
    text = out.read_text()
    raw = yaml.safe_load(text)

    cfg = load_config(out)
    assert cfg.recipe == recipe
    assert _components(cfg) == recipe_components(recipe)
    assert cfg.architecture.workload.schema_type.value == workload
    assert cfg.architecture.workload.datagen.scale == 1

    s3 = raw["platform"]["storage"]["s3"]
    assert s3["access_key"] == "${LAKEBENCH_S3_ACCESS_KEY}"
    assert s3["secret_key"] == "${LAKEBENCH_S3_SECRET_KEY}"
    assert SENTINEL_AK not in text and SENTINEL_SK not in text
    assert "client_secret" not in text
    assert "architecture" not in raw  # recipe-owned keys are commented
    assert len(re.findall(r"(?m)^recipe:", text)) == 1
    assert len(_code_lines(text)) <= LINE_BUDGET, text
    # The load is quiet: no deprecation or upgrade note on a fresh file.
    assert list(cfg._load_notes) == []


def test_uncommented_component_block_still_loads(tmp_path, creds):
    """A user who uncomments the recipe's own values gets no conflict."""
    text = first_day_config(name="unc-t")
    text = "\n".join(
        ln[2:] if ln.startswith("# architecture:") or ln.startswith("#   ") else ln
        for ln in text.splitlines()
    )
    p = tmp_path / "c.yaml"
    p.write_text(text)
    assert load_config(p).architecture.catalog.type.value == "polaris"


def test_default_name_is_unique_and_fits_hive(tmp_path):
    names = {default_name() for _ in range(20)}
    assert len(names) > 1
    long_user = default_name(user="Some.Very_Long-User Name", token="beef")
    assert long_user == "lb-some-very-beef"
    assert re.fullmatch(r"lb-[a-z0-9-]+-[0-9a-f]{4}", long_user)
    assert len(default_name(user="x" * 50)) <= 23
    assert default_name(user="!!!", token="0000") == "lb-user-0000"
    # A Hive recipe with the longest default name loads (LB-153 limit).
    text = first_day_config(name=default_name(user="y" * 40), recipe="hive-iceberg-spark-trino")
    data = yaml.safe_load(text)
    data["platform"]["storage"]["s3"].update(access_key="a", secret_key="b")
    LakebenchConfig.model_validate(data)


@pytest.mark.parametrize("flag", ["--access-key", "--secret-key"])
def test_plaintext_credential_flags_are_refused_without_echo(tmp_path, flag):
    out = tmp_path / "c.yaml"
    r = runner.invoke(app, ["init", "-o", str(out), flag, SENTINEL_AK])
    assert r.exit_code == 2
    assert SENTINEL_AK not in r.output and SENTINEL_AK not in r.stderr
    assert "export LAKEBENCH_S3_" in r.output
    assert not out.exists()


def test_scale_and_workload_flags_are_written(tmp_path, creds):
    out = tmp_path / "s.yaml"
    r = runner.invoke(app, ["init", "-o", str(out), "-s", "0.5", "-w", "financial"])
    assert r.exit_code == 0, r.output
    cfg = load_config(out)
    assert cfg.architecture.workload.datagen.scale == 0.5
    assert cfg.architecture.workload.schema_type.value == "financial"
    # The autosizer scales datagen parallelism only when it is unset.
    assert "parallelism" not in cfg.architecture.workload.datagen.model_fields_set


def test_existing_file_needs_overwrite(tmp_path):
    out = tmp_path / "x.yaml"
    out.write_text("old: true\n")
    r = runner.invoke(app, ["init", "-o", str(out)])
    assert r.exit_code == 2 and "--overwrite" in r.output
    assert out.read_text() == "old: true\n"


# -- recipe conflicts at load (CFG-5) ----------------------------------------


def test_recipe_conflict_refused(tmp_path):
    for block, key, wrote, recipe_sets in [
        ({"catalog": {"type": "hive"}}, "architecture.catalog.type", "hive", "polaris"),
        ({"table_format": {"type": "delta"}}, "architecture.table_format.type", "delta", "iceberg"),
        ({"query_engine": {"type": "duckdb"}}, "architecture.query_engine.type", "duckdb", "trino"),
    ]:
        p = tmp_path / "conflict.yaml"
        p.write_text(
            yaml.safe_dump(
                {"name": "conf-t", "recipe": "polaris-iceberg-spark-trino", "architecture": block}
            )
        )
        with pytest.raises(ConfigValidationError) as exc:
            load_config(p)
        msg = str(exc.value)
        assert (
            f"{key} is '{wrote}' but recipe 'polaris-iceberg-spark-trino' sets '{recipe_sets}'"
            in msg
        )
        assert "delete one of them" in msg
        assert "  - : " not in msg


def test_recipe_conflict_refused_for_a_constructed_config():
    with pytest.raises(ValueError, match="recipe 'hive-delta-spark-trino' sets 'delta'"):
        LakebenchConfig.model_validate(
            {
                "name": "t",
                "recipe": "hive-delta-spark-trino",
                "architecture": {"table_format": {"type": "iceberg"}},
            }
        )


# -- recipe-injected fields --------------------------------------------------


def test_recipe_injected_fields_are_not_user_set():
    cfg = LakebenchConfig.model_validate(
        {"name": "t", "recipe": "polaris-iceberg-spark-trino", "images": {"datagen": "x:1"}}
    )
    assert not user_set(cfg, "images.spark")
    assert not user_set(cfg, "images.postgres")
    assert not user_set(cfg, "architecture.catalog.type")
    assert user_set(cfg, "images.datagen")
    assert not user_set(cfg, "images.trino")


def test_user_written_value_equal_to_the_recipe_is_user_set():
    cfg = LakebenchConfig.model_validate(
        {
            "name": "t",
            "recipe": "polaris-iceberg-spark-trino",
            "images": {"spark": "apache/spark:4.0.2-python3"},
            "workload": {"datagen": {"scale": 2}},
        }
    )
    assert user_set(cfg, "images.spark")
    assert user_set(cfg, "workload.datagen.scale")
    assert not user_set(cfg, "images.postgres")


def test_recipe_expansion_never_aliases_the_recipe_table():
    cfg = LakebenchConfig.model_validate({"name": "t", "recipe": "hive-iceberg-spark-duckdb"})
    cfg2 = LakebenchConfig.model_validate({"name": "t", "recipe": "hive-iceberg-spark-duckdb"})
    assert cfg == cfg2
    data: dict = {"name": "t", "recipe": "hive-iceberg-spark-duckdb"}
    LakebenchConfig.model_validate(data)
    assert (
        data["architecture"]["query_engine"]
        is not (RECIPES["hive-iceberg-spark-duckdb"]["architecture"]["query_engine"])
    )


def test_dump_round_trip_keeps_the_values():
    cfg = LakebenchConfig.model_validate({"name": "t", "recipe": "polaris-iceberg-spark-trino"})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        again = LakebenchConfig.model_validate(cfg.model_dump(mode="json"))
    assert again.model_dump() == cfg.model_dump()
    # The dump writes every field, so nothing is recipe-injected any more.
    assert again._recipe_injected == frozenset()
    assert user_set(again, "images.spark") and not user_set(cfg, "images.spark")


# -- CFG-8: no recipe, or recipe: default ------------------------------------


@pytest.mark.parametrize("recipe_line", ["", "recipe: default\n"])
def test_default_recipe_warns_resolves_hive(tmp_path, recipe_line):
    p = tmp_path / "nr.yaml"
    p.write_text(f"name: nr-t\n{recipe_line}")
    with pytest.warns(DeprecationWarning, match="resolves to hive-iceberg-spark-trino"):
        cfg = load_config(p)
    assert _components(cfg) == recipe_components("hive-iceberg-spark-trino")
    notes = cfg._load_notes.texts()
    assert any("required in v1.8" in t for t in notes), notes
    said = "recipe 'default'" if recipe_line else "no recipe"
    assert any(t.startswith(said) for t in notes), notes


def test_recipe_less_components_note_names_their_recipe(tmp_path):
    """Spec issue 12: a v1.6 init output set catalog.type with no recipe."""
    p = tmp_path / "nr.yaml"
    p.write_text("name: nr-t\narchitecture:\n  catalog:\n    type: polaris\n")
    with pytest.warns(DeprecationWarning, match="resolve to polaris-iceberg-spark-trino"):
        cfg = load_config(p)
    assert cfg.architecture.catalog.type.value == "polaris"


# -- review fixes ------------------------------------------------------------

V16_POLARIS_INIT = (
    # What v1.6 `init --recipe polaris-iceberg-spark-trino` wrote: the recipe
    # plus an uncommented catalog line from the template. v1.6 deployed Hive.
    "name: v16-pol\n"
    "recipe: polaris-iceberg-spark-trino\n"
    "architecture:\n"
    "  catalog:\n"
    "    type: hive\n"
)


def test_v16_conflicting_config_refused_for_deploy_with_the_recipe_to_keep(tmp_path):
    p = tmp_path / "v16.yaml"
    p.write_text(V16_POLARIS_INIT)
    with pytest.raises(ConfigValidationError) as exc:
        load_config(p)
    msg = str(exc.value)
    assert "sets 'polaris'" in msg
    assert "a deployment made from this file is hive-iceberg-spark-trino" in msg
    assert "write recipe: hive-iceberg-spark-trino" in msg
    assert exc.value.errors[0]["loc"] == ("recipe",)


@pytest.mark.parametrize("purpose", ["teardown", "read", "inspect"])
def test_v16_conflicting_config_loads_as_deployed_for_destroy_and_status(tmp_path, purpose):
    from lakebench.config import LoadPurpose

    p = tmp_path / "v16.yaml"
    p.write_text(V16_POLARIS_INIT)
    cfg = load_config(p, purpose=LoadPurpose(purpose))
    # The architecture v1.6 deployed, not the one the recipe name says.
    assert _components(cfg) == recipe_components("hive-iceberg-spark-trino")
    assert cfg.recipe == "hive-iceberg-spark-trino"
    (note,) = [n for n in cfg._load_notes if n.kind == "conflict"]
    assert "Loaded as hive-iceberg-spark-trino" in note.text
    assert "deploy and run refuse it" in note.text


def test_env_reference_in_a_comment_is_not_required(tmp_path, monkeypatch):
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)
    monkeypatch.setenv("LB_TEST_SCALE", "3")
    p = tmp_path / "c.yaml"
    p.write_text(
        "name: c-t\nrecipe: hive-iceberg-spark-trino\n"
        "# old: ${NOT_SET_ANYWHERE}\n"
        "workload:\n  datagen:\n    scale: ${LB_TEST_SCALE}\n"
        "platform:\n  storage:\n    s3:\n      endpoint: ${LB_TEST_EP:-http://s3:80}\n"
    )
    cfg = load_config(p)
    assert cfg.architecture.workload.datagen.scale == 3
    assert cfg.platform.storage.s3.endpoint == "http://s3:80"


def test_overwrite_keeps_the_existing_name(tmp_path):
    out = tmp_path / "o.yaml"
    out.write_text("name: my-lakehouse\nrecipe: polaris-iceberg-spark-trino\n")
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite"])
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(out.read_text())["name"] == "my-lakehouse"
    assert "kept from the file it replaced" in r.stderr
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite", "--name", "other-n"])
    assert yaml.safe_load(out.read_text())["name"] == "other-n"


def test_overwrite_that_moves_the_deployment_is_refused(tmp_path):
    for old, moved in [
        (
            "name: team\nrecipe: polaris-iceberg-spark-trino\n"
            "platform:\n  kubernetes:\n    namespace: team-ns\n",
            "namespace 'team-ns' -> 'team'",
        ),
        (
            "name: team\nrecipe: polaris-iceberg-spark-trino\n"
            "platform:\n  storage:\n    s3:\n      buckets:\n        bronze: shared-b\n",
            "buckets.bronze 'shared-b' -> 'team-bronze'",
        ),
        ("name: team\nrecipe: hive-iceberg-spark-trino\n", "recipe 'hive-iceberg-spark-trino'"),
        (
            # v1.6 init -r polaris-*: the written catalog won, so it deployed Hive.
            "name: team\nrecipe: polaris-iceberg-spark-trino\n"
            "architecture:\n  catalog:\n    type: hive\n",
            "recipe 'hive-iceberg-spark-trino' -> 'polaris-iceberg-spark-trino'",
        ),
        ("name: team\n", "recipe 'hive-iceberg-spark-trino'"),
    ]:
        out = tmp_path / "o.yaml"
        out.write_text(old)
        r = runner.invoke(app, ["init", "-o", str(out), "--overwrite"])
        assert r.exit_code == 3, r.output
        assert moved in r.output and "--name" in r.output
        assert out.read_text() == old
        # Naming a new deployment is the way through.
        r = runner.invoke(app, ["init", "-o", str(out), "--overwrite", "--name", "fresh-n"])
        assert r.exit_code == 0, r.output


@pytest.mark.parametrize("name", ["0x1f", "2024-01-01", "yes", "null", "12"])
def test_names_yaml_would_retype_are_quoted(name):
    text = first_day_config(name=name)
    assert yaml.safe_load(text)["name"] == name


def _load_text(tmp_path, text):
    p = tmp_path / "env.yaml"
    p.write_text(text)
    return load_config(p)


def test_overwrite_of_a_nameless_v16_config_needs_a_name(tmp_path):
    (tmp_path / ".lakebench").mkdir()
    (tmp_path / ".lakebench" / "state.json").write_text('{"name": "v16-auto"}')
    out = tmp_path / "lakebench.yaml"
    out.write_text("recipe: hive-iceberg-spark-trino\n")
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite"])
    assert r.exit_code == 2, r.output
    assert "--name v16-auto" in r.output
    assert out.read_text() == "recipe: hive-iceberg-spark-trino\n"


def test_user_set_maps_config_aliases():
    cfg = LakebenchConfig.model_validate(
        {"name": "t", "recipe": "hive-iceberg-spark-trino", "workload": {"schema": "financial"}}
    )
    assert user_set(cfg, "workload.schema")
    assert user_set(cfg, "workload.schema_type")
    assert not user_set(cfg, "workload.datagen.scale")


# -- differential: ${VAR} loads as v1.6 did ---------------------------------
#
# v1.6 substituted the raw text and then parsed it. The expected value of a
# plain scalar is computed that way here, independently of load_yaml; a
# quoted scalar is expected verbatim (the deliberate change, so a secret is
# never retyped or truncated).

_ENV_VALUES = [
    "0042", "010", "42", "-7", "1_000", "0x1F", "12:30", "3.5", "1e3", ".inf",
    "true", "yes", "off", "null", "~", "", " lb-ns ", "lb-ns\n", "2024-01-01",
    "abc", "a b", "lb-ns", "42\xa0", "\u3000ns", "1:30", "=", "<<",
]  # fmt: skip

_PLAIN_FORMS = [
    "k: ${LB_DIFF}",
    "k: ${LB_DIFF_UNSET:-%s}",
    "k: pre-${LB_DIFF}",
    "k: ${LB_DIFF}${LB_DIFF}",
]


def _v16_expected(form: str, value: str):
    import os

    from lakebench.config.loader import _ENV_PATTERN

    text = form % value if "%s" in form else form

    def rep(m):
        got = os.environ.get(m.group(1))
        return got if got is not None else (m.group(2) or "")

    doc = yaml.safe_load(_ENV_PATTERN.sub(rep, text) + "\n")
    return doc.get("j", doc["k"])


_ENV_LOAD_CASES = [
    # (env to set, env to unset, yaml text, attribute path, expected)
    pytest.param(
        {"LB_EMPTY": ""}, (),
        "name: e-t\nrecipe: hive-iceberg-spark-trino\n"
        "platform:\n  compute:\n    spark:\n      driver_memory: ${LB_EMPTY}\n"
        "  storage:\n    s3:\n      region: x-${LB_EMPTY}\n",
        {"platform.compute.spark.driver_memory": None, "platform.storage.s3.region": "x-"},
        id="empty-whole-value-is-null",
    ),
    pytest.param(
        {}, ("LB_UNSET_R",),
        "name: q-t\nrecipe: hive-iceberg-spark-trino\n"
        'platform:\n  storage:\n    s3:\n      region: "${LB_UNSET_R:-}"\n',
        {"platform.storage.s3.region": ""},
        id="quoted-empty-default-is-empty-string",
    ),
    pytest.param(
        {"LB_B": "bkt"}, (),
        "name: k-t\nrecipe: hive-iceberg-spark-trino\nspark:\n  conf:\n    a.${LB_B}.endpoint: x\n",
        {"spark.conf[a.bkt.endpoint]": "x"},
        id="reference-in-a-key",
    ),
    pytest.param(
        {}, (),
        "name: s-t\nrecipe: hive-iceberg-spark-trino\nspark:\n  conf:\n    spark.x: ${env:HOME}/x\n",
        {"spark.conf[spark.x]": "${env:HOME}/x"},
        id="spark-env-syntax-passes-through",
    ),
]  # fmt: skip


def _attr(cfg, path):
    if path.endswith("]"):
        head, key = path[:-1].split("[", 1)
        obj = cfg
        for part in head.split("."):
            obj = getattr(obj, part)
        return obj[key]
    obj = cfg
    for part in path.split("."):
        obj = getattr(obj, part)
    return obj


@pytest.mark.parametrize(("env", "unset", "text", "expected"), _ENV_LOAD_CASES)
def test_env_reference_loads(tmp_path, monkeypatch, env, unset, text, expected):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    for k in unset:
        monkeypatch.delenv(k, raising=False)
    cfg = _load_text(tmp_path, text)
    for path, want in expected.items():
        assert _attr(cfg, path) == want, path


def test_default_cut_by_a_comment_is_refused(tmp_path, monkeypatch):
    from lakebench.config import ConfigError

    monkeypatch.setenv("LB_RG", "eu-1")
    with pytest.raises(ConfigError):
        _load_text(
            tmp_path,
            "name: c-t\nplatform:\n  storage:\n    s3:\n      region: ${LB_RG:-us-east-1 #x}\n",
        )


def test_secret_with_yaml_syntax_arrives_verbatim(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "ak")
    p = tmp_path / "s.yaml"
    p.write_text(first_day_config(name="sec-t"))
    for secret in ["x #tail", "!abc", "*star", '"quoted', "'single", "123456", "a: b", "{brace}"]:
        monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", secret)
        assert load_config(p).platform.storage.s3.secret_key == secret


def test_plain_reference_loads_as_v16(tmp_path, monkeypatch):
    """Every value in every plain form loads as v1.6 loaded it, value and type."""
    from lakebench.config.loader import load_yaml

    monkeypatch.delenv("LB_DIFF_UNSET", raising=False)
    p = tmp_path / "d.yaml"
    checked = 0
    for form in _PLAIN_FORMS:
        for value in _ENV_VALUES:
            if "%s" in form and (value.strip() != value or value in ("<<", "=")):
                continue  # a default cannot carry surrounding whitespace in a plain scalar
            monkeypatch.setenv("LB_DIFF", value)
            text = form % value if "%s" in form else form
            p.write_text(text + "\n")
            try:
                expected = _v16_expected(form, value)
            except yaml.YAMLError:
                continue  # v1.6 could not parse this substitution at all
            doc = load_yaml(p)
            got = doc.get("j", doc["k"])
            assert got == expected and type(got) is type(expected), (text, value, got, expected)
            checked += 1
    assert checked > 60


def test_quoted_reference_arrives_verbatim(tmp_path, monkeypatch):
    from lakebench.config.loader import load_yaml

    p = tmp_path / "q.yaml"
    values = [*_ENV_VALUES, "x #tail", '"q', "'s", "a\\tb", "!x", "*y", "a: b", "123456"]
    for quote in ('"', "'"):
        p.write_text(f"k: {quote}${{LB_DIFF}}{quote}\n")
        for value in values:
            monkeypatch.setenv("LB_DIFF", value)
            assert load_yaml(p)["k"] == value, (quote, value)


def test_leading_zero_seed_keeps_its_v16_value(tmp_path, monkeypatch):
    """A seed from the environment must reach the same corpus as in v1.6."""
    monkeypatch.setenv("LB_SEED", "0042")
    cfg = _load_text(
        tmp_path,
        "name: s-t\nrecipe: hive-iceberg-spark-trino\n"
        "workload:\n  datagen:\n    seed: ${LB_SEED}\n",
    )
    assert cfg.architecture.workload.datagen.seed == 34


def test_overwrite_guard_reads_the_flat_namespace(tmp_path):
    out = tmp_path / "o.yaml"
    old = "name: alice\nrecipe: polaris-iceberg-spark-trino\nnamespace: team-ns\n"
    out.write_text(old)
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite"])
    assert r.exit_code == 3, r.output
    assert "namespace 'team-ns' -> 'alice'" in r.output
    assert out.read_text() == old


def test_overwrite_guard_runs_when_the_same_name_is_passed(tmp_path):
    out = tmp_path / "o.yaml"
    old = "name: alice\nrecipe: hive-iceberg-spark-trino\n"
    out.write_text(old)
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite", "--name", "alice"])
    assert r.exit_code == 3, r.output
    assert "recipe 'hive-iceberg-spark-trino' -> 'polaris-iceberg-spark-trino'" in r.output
    r = runner.invoke(
        app,
        [
            "init",
            "-o",
            str(out),
            "--overwrite",
            "--name",
            "alice",
            "-r",
            "hive-iceberg-spark-trino",
        ],
    )
    assert r.exit_code == 0, r.output


def test_overwrite_guard_resolves_env_defaults(tmp_path, monkeypatch):
    monkeypatch.delenv("LB_G_NS", raising=False)
    monkeypatch.delenv("LB_G_NAME", raising=False)
    out = tmp_path / "o.yaml"
    out.write_text(
        "name: ${LB_G_NAME:-alice}\nrecipe: polaris-iceberg-spark-trino\n"
        "platform:\n  kubernetes:\n    namespace: ${LB_G_NS:-alice}\n"
    )
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite"])
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(out.read_text())["name"] == "alice"


def test_overwrite_of_an_unreadable_named_config_needs_a_new_name(tmp_path, monkeypatch):
    monkeypatch.delenv("LB_G_UNSET", raising=False)
    out = tmp_path / "o.yaml"
    old = "name: alice\nrecipe: polaris-iceberg-spark-trino\nplatform:\n  kubernetes:\n    namespace: ${LB_G_UNSET}\n"
    out.write_text(old)
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite"])
    assert r.exit_code == 3, r.output
    assert "cannot be read" in r.output and out.read_text() == old


def test_rerunning_init_over_its_own_file_before_export_works(tmp_path, monkeypatch):
    monkeypatch.delenv("LAKEBENCH_S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("LAKEBENCH_S3_SECRET_KEY", raising=False)
    out = tmp_path / "c.yaml"
    assert runner.invoke(app, ["init", "-o", str(out)]).exit_code == 0
    first = yaml.safe_load(out.read_text())["name"]
    # Filling in the endpoint the first file left empty is not a move.
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite", "--endpoint", "http://s3:80"])
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(out.read_text())["name"] == first
    # Changing a set endpoint under the same name is.
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite", "--endpoint", "http://other:80"])
    assert r.exit_code == 3 and "endpoint 'http://s3:80' -> 'http://other:80'" in r.output
    assert "LAKEBENCH_S3_ACCESS_KEY" not in __import__("os").environ


def test_overwrite_of_a_file_with_a_typed_reference_does_not_crash(tmp_path, monkeypatch):
    monkeypatch.setenv("LB_T_SEED", "7")
    out = tmp_path / "t.yaml"
    out.write_text(
        "name: tseed\nrecipe: polaris-iceberg-spark-trino\n"
        "workload:\n  datagen:\n    seed: !!int ${LB_T_SEED}\n"
    )
    r = runner.invoke(app, ["init", "-o", str(out), "--overwrite"])
    assert r.exception is None or isinstance(r.exception, SystemExit), r.output
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(out.read_text())["name"] == "tseed"
