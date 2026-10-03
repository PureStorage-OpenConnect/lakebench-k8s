"""CFG-6 (CC-16): ``lakebench init --from OLD -o NEW``.

The new file keeps OLD's name (or the name 1.6 recorded in the directory)
and its buckets, never expands a ``${VAR}``, moves a plaintext secret to a
reference without printing it, and lists every moved, dropped or derived
key. Loaded for a read-only command, it is the config OLD loads to, minus
the listed drops; ``init --from`` never writes over OLD.

The round trips here load both files with ``load_config`` and compare the
models directly, so they do not lean on the converter's own check.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.config import LoadPurpose, load_config
from lakebench.config.loader import load_notes
from lakebench.config.refused_keys import REFUSED_KEYS
from lakebench.config.schema import LakebenchConfig
from lakebench.config.support import recipe_names
from lakebench.metrics.experiment import planned_experiment

runner = CliRunner()
ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)")
_SECRET_LINE = re.compile(r"^\s*secret:\s+(\S+?):", re.M)

FLAT_V12 = """\
name: flat12
recipe: hive-iceberg-spark-trino
endpoint: http://10.0.1.50:80
access_key: plaintext-access
secret_key: plaintext-secret
scale: 2
namespace: flat12-ns
mode: batch
spark_image: apache/spark:4.0.2-python3
"""

# Every deprecated spelling 1.6 still loaded, in one file.
OLD_SPELLINGS = """\
name: old-spellings
architecture:
  catalog:
    type: hive
    hive:
      operator: {install: true}
  workload:
    schema: customer360
    datagen: {scale: 3}
  processing:
    mode: sustained
    sustained:
      run_duration: 1800
platform:
  compute:
    spark:
      operator: {install: true}
  storage:
    s3:
      endpoint: http://10.0.1.50:80
      access_key: ${LAKEBENCH_S3_ACCESS_KEY}
      secret_key: ${LAKEBENCH_S3_SECRET_KEY}
"""


def _flatten(node, path=()):
    if isinstance(node, dict) and node:
        out = {}
        for k, v in node.items():
            out.update(_flatten(v, (*path, str(k))))
        return out
    return {path: node}


def _model_path(dotted: str) -> tuple[str, ...]:
    path = tuple(dotted.split("."))
    if path[:2] == ("spark", "conf"):
        return ("spark", "conf", dotted[len("spark.conf.") :])
    return path


def _set_refs(monkeypatch, *texts: str) -> None:
    """Every referenced variable set to a placeholder, or unset when its
    references carry a default (``${LB_SCALE:-2}`` is not text)."""
    for text in texts:
        for m in re.finditer(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-)?", text):
            if m.group(2):
                monkeypatch.delenv(m.group(1), raising=False)
            else:
                monkeypatch.setenv(m.group(1), f"ph-{m.group(1).lower().replace('_', '-')}")


def _convert(tmp_path, monkeypatch, old_text: str, *extra: str, name: str = "old.yaml"):
    old = tmp_path / name
    old.write_text(old_text)
    new = tmp_path / "new.yaml"
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init", "--from", str(old), "-o", str(new), *extra])
    return old, new, result


def _assert_round_trip(monkeypatch, old: Path, new: Path, output: str, *, name=None):
    """NEW loads, read-only, to OLD's model and planned experiment, apart
    from the credentials the output says it moved; and NEW loads clean."""
    old_text, new_text = old.read_text(), new.read_text()
    _set_refs(monkeypatch, old_text, new_text)
    old_cfg = load_config(old, purpose=LoadPurpose.READ, name_override=name, print_notes=False)
    new_cfg = load_config(new, purpose=LoadPurpose.READ, print_notes=False)
    moved = {_model_path(p) for p in _SECRET_LINE.findall(output)}
    for path in moved:
        assert re.search(
            r"(access_key|secret_key|client_secret|secret\.key|access\.key)$", ".".join(path)
        )
    a, b = _flatten(old_cfg.model_dump(mode="json")), _flatten(new_cfg.model_dump(mode="json"))
    differ = sorted(
        ".".join(p) for p in set(a) | set(b) if p not in moved and a.get(p, a) != b.get(p, b)
    )
    assert differ == []
    assert json.dumps(planned_experiment(old_cfg), sort_keys=True, default=str) == json.dumps(
        planned_experiment(new_cfg), sort_keys=True, default=str
    )
    # Nothing left to move or drop: no removed key and no deprecated
    # spelling (a config with no recipe keeps its recipe note).
    notes = [t for t in load_notes(new_cfg).texts() if "recipe" not in t]
    assert notes == [], notes
    # No plaintext secret left in NEW: every credential is a reference.
    s3 = yaml.safe_load(new_text)["platform"]["storage"]["s3"]
    for key in ("access_key", "secret_key"):
        assert s3.get(key, "") in ("",) or str(s3[key]).startswith("${"), key
    return old_cfg, new_cfg


# -- round trips ------------------------------------------------------------------

V16_EXAMPLES = sorted((FIXTURES / "v16-examples").glob("*.yaml"))
EXAMPLES = sorted((ROOT / "examples").glob("*.yaml"))
V16_INIT = sorted((FIXTURES / "v16-init").glob("*.yaml"))


def test_fixture_sets_are_complete():
    assert len(EXAMPLES) == 13
    # The 1.6.0 examples that differ from today's; the other seven are unchanged.
    assert len(V16_EXAMPLES) == 6
    assert {p.name for p in V16_EXAMPLES} <= {p.name for p in EXAMPLES}
    assert len(V16_INIT) == 3


@pytest.mark.parametrize(
    "path",
    [*EXAMPLES, *V16_EXAMPLES, *V16_INIT, FIXTURES / "v14user.yaml"],
    ids=lambda p: f"{p.parent.name}/{p.name}",
)
def test_init_from_round_trip(tmp_path, monkeypatch, path):
    old, new, r = _convert(tmp_path, monkeypatch, path.read_text())
    assert r.exit_code == 0, r.output
    _assert_round_trip(monkeypatch, old, new, r.output)


def test_init_from_round_trip_v16_saved_config(tmp_path, monkeypatch):
    """A 1.6 ``save_config`` file carries every field, at 1.6 defaults."""
    old, new, r = _convert(tmp_path, monkeypatch, (FIXTURES / "v16-saved-c360.yaml").read_text())
    assert r.exit_code == 0, r.output
    _assert_round_trip(monkeypatch, old, new, r.output)
    # benchmark.streams: 4 (the default, written by save_config) is dropped,
    # so run accepts the new file; it is the value benchmark uses anyway.
    assert "architecture.benchmark.streams: was 4" in r.output
    assert "streams" not in yaml.safe_load(new.read_text())["architecture"]["benchmark"]
    assert "still refuse" not in r.output


def test_init_from_round_trip_flat_v12(tmp_path, monkeypatch):
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12)
    assert r.exit_code == 0, r.output
    _, cfg = _assert_round_trip(monkeypatch, old, new, r.output)
    raw = yaml.safe_load(new.read_text())
    for flat in ("endpoint", "access_key", "secret_key", "scale", "namespace", "mode"):
        assert flat not in raw
        assert f"moved:   {flat} -> " in r.output
    assert raw["workload"]["datagen"]["scale"] == 2
    assert cfg.get_namespace() == "flat12-ns"


@pytest.mark.parametrize("workload", ["customer360", "financial"])
@pytest.mark.parametrize("recipe", recipe_names())
def test_init_from_round_trip_of_init_output(tmp_path, monkeypatch, recipe, workload):
    monkeypatch.chdir(tmp_path)
    first = tmp_path / "first.yaml"
    r = runner.invoke(app, ["init", "-r", recipe, "-w", workload, "-o", str(first)])
    if r.exit_code != 0:
        assert workload == "financial" and "-delta-" in recipe  # Iceberg-only workload
        return
    new = tmp_path / "new.yaml"
    r = runner.invoke(app, ["init", "--from", str(first), "-o", str(new)])
    assert r.exit_code == 0, r.output
    _assert_round_trip(monkeypatch, first, new, r.output)
    # init's own output has nothing to move or drop; only the buckets are
    # written out.
    assert "moved:" not in r.output and "dropped:" not in r.output


def test_init_from_moves_every_old_spelling(tmp_path, monkeypatch):
    old, new, r = _convert(tmp_path, monkeypatch, OLD_SPELLINGS)
    assert r.exit_code == 0, r.output
    _assert_round_trip(monkeypatch, old, new, r.output)
    raw = yaml.safe_load(new.read_text())
    assert raw["workload"] == {"schema": "customer360", "datagen": {"scale": 3}}
    assert raw["architecture"]["pipeline"]["mode"] == "continuous"
    assert raw["architecture"]["pipeline"]["continuous"] == {"run_duration": 1800}
    assert "processing" not in raw["architecture"] and "workload" not in raw["architecture"]
    # The operator keys are dropped with the admin install text, and the
    # empty blocks they leave are gone.
    assert "operator" not in raw["architecture"]["catalog"].get("hive", {})
    assert "compute" not in raw["platform"]
    out = " ".join(r.output.split())
    for component in ("spark-operator", "stackable"):
        assert f"lakebench admin install --component {component}" in out
    # The new file is accepted by the commands that change data.
    _set_refs(monkeypatch, new.read_text())
    load_config(new, purpose=LoadPurpose.RUN, print_notes=False)


def test_init_from_resolves_a_recipe_conflict_as_v16_deployed(tmp_path, monkeypatch):
    """1.6 ``init --recipe polaris-*`` wrote ``catalog.type: hive``, and the
    written component won: the deployment is Hive."""
    path = FIXTURES / "v16-init" / "polaris-plaintext-keys.yaml"
    old, new, r = _convert(tmp_path, monkeypatch, path.read_text())
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(new.read_text())["recipe"] == "hive-iceberg-spark-trino"
    _set_refs(monkeypatch, new.read_text())
    cfg = load_config(new, purpose=LoadPurpose.MUTATE, print_notes=False)
    assert cfg.architecture.catalog.type.value == "hive"


# -- never in place, overwrite -------------------------------------------------------


def test_init_from_never_in_place(tmp_path, monkeypatch):
    old = tmp_path / "old.yaml"
    old.write_text(FLAT_V12)
    before = old.read_bytes()
    monkeypatch.chdir(tmp_path)
    for target in ("old.yaml", str(old), "./sub/../old.yaml"):
        (tmp_path / "sub").mkdir(exist_ok=True)
        r = runner.invoke(app, ["init", "--from", str(old), "-o", target, "--overwrite"])
        assert r.exit_code == 2, r.output
        assert "same file" in r.output
    os.link(old, tmp_path / "hard.yaml")
    r = runner.invoke(app, ["init", "--from", "old.yaml", "-o", "hard.yaml", "--overwrite"])
    assert r.exit_code == 2 and "same file" in r.output
    assert old.read_bytes() == before


def test_init_from_needs_overwrite_for_an_existing_file(tmp_path, monkeypatch):
    (tmp_path / "new.yaml").write_text("keep me\n")
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12)
    assert r.exit_code == 2 and "--overwrite" in r.output
    assert new.read_text() == "keep me\n"


def test_init_from_overwrite_that_moves_the_deployment_is_refused(tmp_path, monkeypatch):
    (tmp_path / "new.yaml").write_text(FLAT_V12.replace("flat12-ns", "other-ns"))
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12, "--overwrite")
    assert r.exit_code == 3, r.output
    assert "namespace 'other-ns' -> 'flat12-ns'" in " ".join(r.output.split())
    assert "other-ns" in new.read_text()
    assert not list(tmp_path.glob(".new.yaml.*"))  # no temporary file left


def test_init_from_overwrite_of_the_same_deployment(tmp_path, monkeypatch):
    (tmp_path / "new.yaml").write_text(FLAT_V12)
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12, "--overwrite")
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(new.read_text())["name"] == "flat12"


@pytest.mark.parametrize("flag", [["--recipe", "hive-iceberg-spark-trino"], ["--scale", "2"]])
def test_init_from_refuses_content_flags(tmp_path, monkeypatch, flag):
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12, *flag)
    assert r.exit_code == 2 and flag[0] in r.output
    assert not new.exists()


def test_init_from_missing_or_unparsable_old(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    r = runner.invoke(app, ["init", "--from", "nope.yaml", "-o", "new.yaml"])
    assert r.exit_code == 2 and "no such file" in r.output
    (tmp_path / "list.yaml").write_text("- a\n- b\n")
    r = runner.invoke(app, ["init", "--from", "list.yaml", "-o", "new.yaml"])
    assert r.exit_code == 2 and "not a config" in r.output
    assert not (tmp_path / "new.yaml").exists()


def test_init_from_old_that_does_not_load_is_refused(tmp_path, monkeypatch):
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12 + "unknown_key: 1\n")
    assert r.exit_code == 2, r.output
    assert "does not load" in r.output and "unknown_key" in r.output
    assert not new.exists()


# -- the name and the buckets ---------------------------------------------------------


def _legacy_dir(tmp_path: Path, name: str = "lb-20260101-120000") -> None:
    (tmp_path / ".lakebench").mkdir()
    (tmp_path / ".lakebench" / "state.json").write_text(json.dumps({"name": name}))


NAMELESS = FLAT_V12.replace("name: flat12\n", "").replace("namespace: flat12-ns\n", "")


def test_init_from_keeps_legacy_name(tmp_path, monkeypatch):
    _legacy_dir(tmp_path)
    old, new, r = _convert(tmp_path, monkeypatch, NAMELESS)
    assert r.exit_code == 0, r.output
    raw = yaml.safe_load(new.read_text())
    assert raw["name"] == "lb-20260101-120000"
    assert raw["platform"]["storage"]["s3"]["buckets"] == {
        "bronze": "lb-20260101-120000-bronze",
        "silver": "lb-20260101-120000-silver",
        "gold": "lb-20260101-120000-gold",
    }
    assert "state.json" in r.output
    _assert_round_trip(monkeypatch, old, new, r.output, name="lb-20260101-120000")


def test_init_from_legacy_name_with_a_nameless_sibling_needs_name(tmp_path, monkeypatch):
    """1.6 gave every nameless config in the directory the one name: which
    one deployed it cannot be told, so the name is not handed to either."""
    _legacy_dir(tmp_path)
    (tmp_path / "sibling.yaml").write_text(NAMELESS)
    old, new, r = _convert(tmp_path, monkeypatch, NAMELESS)
    assert r.exit_code == 3, r.output
    assert "--name lb-20260101-120000" in " ".join(r.output.split())
    assert not new.exists()
    r = runner.invoke(
        app, ["init", "--from", str(old), "-o", str(new), "--name", "lb-20260101-120000"]
    )
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(new.read_text())["name"] == "lb-20260101-120000"


def test_init_from_new_name_when_nothing_was_recorded(tmp_path, monkeypatch):
    old, new, r = _convert(tmp_path, monkeypatch, NAMELESS)
    assert r.exit_code == 0, r.output
    name = yaml.safe_load(new.read_text())["name"]
    assert re.fullmatch(r"lb-[a-z0-9-]+-[0-9a-f]{4}", name)
    assert "no deployment was recorded for this config" in r.output


def test_init_from_name_flag_must_match_a_named_config(tmp_path, monkeypatch):
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12, "--name", "other")
    assert r.exit_code == 2 and "does not match" in r.output
    assert not new.exists()


def test_init_from_keeps_explicit_buckets(tmp_path, monkeypatch):
    text = FLAT_V12 + "platform:\n  storage:\n    s3:\n      buckets: {silver: my-silver}\n"
    old, new, r = _convert(tmp_path, monkeypatch, text)
    assert r.exit_code == 0, r.output
    buckets = yaml.safe_load(new.read_text())["platform"]["storage"]["s3"]["buckets"]
    assert buckets == {"silver": "my-silver", "bronze": "flat12-bronze", "gold": "flat12-gold"}


# -- references and secrets -------------------------------------------------------------

REFS = """\
name: ${LB_NAME}
recipe: hive-iceberg-spark-trino
platform:
  kubernetes:
    namespace: "${LB_NS:-refs-ns}"
  storage:
    s3:
      endpoint: http://${LB_HOST}:80
      access_key: "${LAKEBENCH_S3_ACCESS_KEY}"
      secret_key: ${LAKEBENCH_S3_SECRET_KEY}
workload:
  datagen:
    scale: ${LB_SCALE:-2}
"""


def test_init_from_never_expands_vars(tmp_path, monkeypatch):
    monkeypatch.setenv("LB_NAME", "expanded-name")
    monkeypatch.setenv("LB_HOST", "expanded-host")
    old, new, r = _convert(tmp_path, monkeypatch, REFS)
    assert r.exit_code == 0, r.output
    text = new.read_text()
    assert "expanded" not in text and "expanded" not in r.output
    lines = {ln.strip() for ln in text.splitlines()}
    # Plain stays plain (typed after substitution), quoted stays quoted (text).
    assert "name: ${LB_NAME}" in lines
    assert 'namespace: "${LB_NS:-refs-ns}"' in lines
    assert "endpoint: http://${LB_HOST}:80" in lines
    assert 'access_key: "${LAKEBENCH_S3_ACCESS_KEY}"' in lines
    assert "secret_key: ${LAKEBENCH_S3_SECRET_KEY}" in lines
    assert "scale: ${LB_SCALE:-2}" in lines
    assert "bronze: ${LB_NAME}-bronze" in lines
    _assert_round_trip(monkeypatch, old, new, r.output)
    _set_refs(monkeypatch, text)
    monkeypatch.delenv("LB_SCALE", raising=False)
    cfg = load_config(new, purpose=LoadPurpose.READ, print_notes=False)
    assert cfg.architecture.workload.datagen.scale == 2  # typed after substitution


SENTINEL_AK = "SENTINEL-ACCESS-4F2"
SENTINEL_SK = "SENTINEL-SECRET-8K1"
SENTINEL_PC = "SENTINEL-POLARIS-3D9"
SENTINEL_SC = "SENTINEL-SPARKCONF-6H7"
SENTINEL_DEF = "SENTINEL-DEFAULT-2B5"

SECRETS = f"""\
name: secrets
recipe: polaris-iceberg-spark-trino
platform:
  storage:
    s3:
      endpoint: http://10.0.1.50:80
      access_key: {SENTINEL_AK}
      secret_key: "{SENTINEL_SK}"
architecture:
  catalog:
    polaris:
      client_secret: {SENTINEL_PC}
spark:
  conf:
    spark.hadoop.fs.s3a.secret.key: {SENTINEL_SC}
    spark.hadoop.fs.s3a.access.key: ${{MY_KEY:-{SENTINEL_DEF}}}
    spark.hadoop.fs.s3a.bucket.archive.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
"""


def test_init_from_no_secret_in_output(tmp_path, monkeypatch):
    for var, value in (
        ("LAKEBENCH_S3_ACCESS_KEY", SENTINEL_AK),
        ("LAKEBENCH_S3_SECRET_KEY", SENTINEL_SK),
        ("LAKEBENCH_POLARIS_CLIENT_SECRET", SENTINEL_PC),
        ("MY_KEY", SENTINEL_DEF),
    ):
        monkeypatch.setenv(var, value)
    old, new, r = _convert(tmp_path, monkeypatch, SECRETS)
    assert r.exit_code == 0, r.output
    text = new.read_text()
    for sentinel in (SENTINEL_AK, SENTINEL_SK, SENTINEL_PC, SENTINEL_SC, SENTINEL_DEF):
        assert sentinel not in text
        assert sentinel not in r.output
    raw = yaml.safe_load(text)
    assert raw["platform"]["storage"]["s3"]["access_key"] == "${LAKEBENCH_S3_ACCESS_KEY}"
    assert raw["platform"]["storage"]["s3"]["secret_key"] == "${LAKEBENCH_S3_SECRET_KEY}"
    assert raw["architecture"]["catalog"]["polaris"]["client_secret"] == (
        "${LAKEBENCH_POLARIS_CLIENT_SECRET}"
    )
    conf = raw["spark"]["conf"]
    assert conf["spark.hadoop.fs.s3a.secret.key"] == "${LAKEBENCH_SPARK_HADOOP_FS_S3A_SECRET_KEY}"
    assert conf["spark.hadoop.fs.s3a.access.key"] == "${MY_KEY}"
    # A credentials *provider* is a class name, not a secret.
    assert conf["spark.hadoop.fs.s3a.bucket.archive.aws.credentials.provider"].startswith(
        "org.apache"
    )
    assert "export it" in r.output
    _assert_round_trip(monkeypatch, old, new, r.output)


# -- the converter's own check ----------------------------------------------------------


def test_init_from_refuses_a_conversion_that_loads_differently(tmp_path, monkeypatch):
    """A translation that would move a bucket is refused, naming the path
    and not the value; nothing is written."""
    import lakebench.config.init_from as init_from

    real = init_from._set_buckets

    def wrong(data, name, changes):
        real(data, name, changes)
        data["platform"]["storage"]["s3"]["buckets"]["gold"] = "elsewhere-gold"

    monkeypatch.setattr(init_from, "_set_buckets", wrong)
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12)
    assert r.exit_code == 3, r.output
    out = " ".join(r.output.split())
    assert "platform.storage.s3.buckets.gold" in out
    assert "elsewhere-gold" not in out and "flat12-gold" not in out
    assert not new.exists()


def test_init_from_lists_what_run_still_refuses(tmp_path, monkeypatch):
    text = FLAT_V12 + "platform:\n  compute:\n    spark:\n      silver_executors: 40\n"
    old, new, r = _convert(tmp_path, monkeypatch, text)
    assert r.exit_code == 0, r.output
    out = " ".join(r.output.split())
    assert "deploy and run still refuse it: platform.compute.spark.silver_executors" in out
    assert yaml.safe_load(new.read_text())["platform"]["compute"]["spark"]["silver_executors"] == 40


# -- the refused-key table and the removed keys' fix text ---------------------------------


def _with(path: str, value) -> dict:
    data: dict = {
        "name": "rk",
        "platform": {"storage": {"s3": {"endpoint": "http://10.0.1.50:80"}}},
    }
    node = data
    parts = path.split(".")
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value
    return data


@pytest.mark.parametrize("row", REFUSED_KEYS, ids=lambda r: r.subject)
def test_refused_key_rows_match_their_validators(row):
    data = _with(row.key, row.example)
    refusing = [LoadPurpose.RUN] + ([LoadPurpose.MUTATE] if row.refused_by != "run" else [])
    for purpose in refusing:
        with pytest.raises(Exception) as e:
            LakebenchConfig.model_validate(data, context={"purpose": purpose})
        assert " ".join(row.fix.split()) in " ".join(str(e.value).split()), purpose
    loading = [LoadPurpose.READ, LoadPurpose.TEARDOWN]
    if row.refused_by == "run":
        loading.append(LoadPurpose.MUTATE)
    for purpose in loading:
        LakebenchConfig.model_validate(data, context={"purpose": purpose})


def test_every_removed_key_has_fix_text():
    import typing

    from pydantic import BaseModel

    seen: set[type] = set()
    missing: list[str] = []

    def walk(model: type) -> None:
        if model in seen:
            return
        seen.add(model)
        for key, text in (getattr(model, "_removed_keys", {}) or {}).items():
            if not isinstance(text, str) or len(text.split()) < 4:
                missing.append(f"{model.__name__}.{key}")
        for f in model.model_fields.values():
            for arg in typing.get_args(f.annotation) or (f.annotation,):
                if isinstance(arg, type) and issubclass(arg, BaseModel):
                    walk(arg)

    walk(LakebenchConfig)
    assert len(seen) > 20
    assert missing == []
