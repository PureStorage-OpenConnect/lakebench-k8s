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

import hashlib
import json
import os
import re
import stat
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
            r"(?i)(access_key|secret_key|client_secret|secret\.key|access\.key|password"
            r"|encryption\.key)$",
            ".".join(path),
        )
    if re.search(r"derived: recipe: [a-z]", output):
        moved.add(("recipe",))  # written for a recipe-less OLD; its effect is compared
    a, b = _flatten(old_cfg.model_dump(mode="json")), _flatten(new_cfg.model_dump(mode="json"))
    differ = sorted(
        ".".join(p) for p in set(a) | set(b) if p not in moved and a.get(p, a) != b.get(p, b)
    )
    assert differ == []
    assert json.dumps(planned_experiment(old_cfg), sort_keys=True, default=str) == json.dumps(
        planned_experiment(new_cfg), sort_keys=True, default=str
    )
    # Every leaf of OLD is in NEW at the same path, or under a path the
    # output lists (a moved, dropped, derived or secret line): nothing is
    # dropped without a line, which a read-only load would not show.
    listed = re.findall(r"^\s*(?:moved|dropped|derived|secret):\s+(\S+?)(?::| ->)", output, re.M)
    old_leaves = _flatten(yaml.safe_load(old_text))
    new_leaves = _flatten(yaml.safe_load(new_text))
    unlisted = [
        dotted
        for dotted in (".".join(p) for p in old_leaves if p not in new_leaves)
        if not any(dotted == x or dotted.startswith(x + ".") for x in listed)
    ]
    assert unlisted == [], unlisted
    # Nothing left to move or drop: no removed key and no deprecated
    # spelling (a config whose recipe could not be derived keeps its note).
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


#: The seven examples unchanged since 1.6.0, by the sha256 of their 1.6.0
#: text (`git show v1.6.0:examples/<name>`): the round trip over today's
#: copy is the round trip over 1.6's. A change to one of them fails here;
#: copy its 1.6.0 text into tests/fixtures/v16-examples/ first.
V16_UNCHANGED = {
    "hive-delta-spark-none.yaml": "eb7b96cb3a0588c8e568d5d021f041144ca6b27f6c8766077855b6c815798297",
    "hive-delta-spark-thrift.yaml": "bb1f2bac628f7c37b5ba12f0dc4c4e9978ddb684d48cf93ea54a8667576669aa",
    "hive-delta-spark-trino.yaml": "06e0d562869fee20fcaa4ea16e526cd276caf521436dfe72626c5aabdc0037cc",
    "hive-iceberg-spark-duckdb.yaml": "a30d86e042e1a4822d006dc4039429b715c8ea4a6004e3b317cdf1f1ead54ca2",
    "hive-iceberg-spark-none.yaml": "a99b836f646089bd48b6d4b6e54cf91771c7b1e94e0b9772fdfad5283b2dc327",
    "hive-iceberg-spark-thrift.yaml": "48ced34e8e87e929a0d938200ae65dd47a01ff37e8327ae9024ec4f46c4e5f02",
    "hive-iceberg-spark-trino.yaml": "c079c9dc9207670b0a11c210bebe680414676e0db0438f54e034a462d3508345",
}


def test_fixture_sets_cover_every_v16_example():
    assert len(EXAMPLES) == 13
    v16 = {p.name for p in V16_EXAMPLES} | set(V16_UNCHANGED)
    assert v16 == {p.name for p in EXAMPLES}
    for name, digest in V16_UNCHANGED.items():
        assert hashlib.sha256((ROOT / "examples" / name).read_bytes()).hexdigest() == digest, name
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
    assert "no .lakebench/state.json beside it" in " ".join(r.output.split())


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
    spark.ssl.keyStorePassword: {SENTINEL_SC}-ks
    spark.hadoop.fs.s3a.encryption.key: {SENTINEL_SC}-sse
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
    assert "run still refuses it: platform.compute.spark.silver_executors" in out
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


# -- review fixes ------------------------------------------------------------------------


def test_init_from_reads_the_legacy_name_where_v16_did(tmp_path, monkeypatch):
    """1.6 read .lakebench/state.json beside the path it was given, so a
    config reached through a link used the link's directory. Two different
    recorded names are refused; one is used."""
    lab, shared = tmp_path / "lab", tmp_path / "shared"
    lab.mkdir()
    shared.mkdir()
    _legacy_dir(lab, "lb-mine")
    (shared / "real.yaml").write_text(NAMELESS)
    (lab / "lakebench.yaml").symlink_to(shared / "real.yaml")
    monkeypatch.chdir(tmp_path)
    r = runner.invoke(app, ["init", "--from", "lab/lakebench.yaml", "-o", "new.yaml"])
    assert r.exit_code == 0, r.output
    assert yaml.safe_load((tmp_path / "new.yaml").read_text())["name"] == "lb-mine"
    _legacy_dir(shared, "lb-someone-else")
    r = runner.invoke(app, ["init", "--from", "lab/lakebench.yaml", "-o", "new2.yaml"])
    assert r.exit_code == 3, r.output
    out = " ".join(r.output.split())
    assert "lb-mine" in out and "lb-someone-else" in out
    assert not (tmp_path / "new2.yaml").exists()


def test_init_from_name_flag_says_what_was_recorded(tmp_path, monkeypatch):
    _legacy_dir(tmp_path)
    old, new, r = _convert(tmp_path, monkeypatch, NAMELESS, "--name", "fresh")
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(new.read_text())["name"] == "fresh"
    assert "'lb-20260101-120000'" in r.output and "does not address" in " ".join(r.output.split())


def test_init_from_overwrite_guard_sees_a_reference_name(tmp_path, monkeypatch):
    """OLD names its deployment through ${LB_NAME}; the file being replaced
    resolves to that same name in another namespace: refused."""
    monkeypatch.setenv("LB_NAME", "p9")
    (tmp_path / "new.yaml").write_text(FLAT_V12.replace("flat12", "p9").replace("p9-ns", "prod-ns"))
    text = FLAT_V12.replace("name: flat12", "name: ${LB_NAME}").replace("flat12-ns", "p9")
    old, new, r = _convert(tmp_path, monkeypatch, text, "--overwrite")
    assert r.exit_code == 3, r.output
    assert "prod-ns" in new.read_text()


def test_init_from_typed_reference_converts_with_the_shells_value(tmp_path, monkeypatch):
    monkeypatch.setenv("LB_SCALE", "2")
    text = FLAT_V12.replace("scale: 2", "scale: ${LB_SCALE}")
    old, new, r = _convert(tmp_path, monkeypatch, text)
    assert r.exit_code == 0, r.output
    assert "scale: ${LB_SCALE}" in new.read_text()


def test_init_from_typed_reference_unset_names_the_variable(tmp_path, monkeypatch):
    monkeypatch.delenv("LB_SCALE", raising=False)
    text = FLAT_V12.replace("scale: 2", "scale: ${LB_SCALE}")
    old, new, r = _convert(tmp_path, monkeypatch, text)
    assert r.exit_code == 2, r.output
    assert "LB_SCALE (export them)" in " ".join(r.output.split())
    assert not new.exists()


def test_init_from_yaml_error_does_not_echo_the_line(tmp_path, monkeypatch):
    text = FLAT_V12.replace("secret_key: plaintext-secret", f"secret_key: {SENTINEL_SK}: x")
    old, new, r = _convert(tmp_path, monkeypatch, text)
    assert r.exit_code == 2, r.output
    assert "not YAML" in r.output and "line 5" in r.output
    assert SENTINEL_SK not in r.output


def test_init_from_new_file_is_no_more_readable_than_old(tmp_path, monkeypatch):
    old = tmp_path / "old.yaml"
    old.write_text(FLAT_V12)
    old.chmod(0o600)
    monkeypatch.chdir(tmp_path)
    r = runner.invoke(app, ["init", "--from", "old.yaml", "-o", "new.yaml"])
    assert r.exit_code == 0, r.output
    assert stat.S_IMODE((tmp_path / "new.yaml").stat().st_mode) == 0o600


def test_init_from_keeps_a_medallion_block_that_moved_bronze(tmp_path, monkeypatch):
    text = FLAT_V12 + (
        "architecture:\n  pipeline:\n    medallion:\n      bronze:\n"
        "        path_template: custom/prefix\n"
    )
    old, new, r = _convert(tmp_path, monkeypatch, text)
    assert r.exit_code == 0, r.output
    raw = yaml.safe_load(new.read_text())
    assert raw["architecture"]["pipeline"]["medallion"]["bronze"]["path_template"] == (
        "custom/prefix"
    )
    assert "run still refuses it" in r.output and "medallion" in r.output


def test_init_from_drops_a_default_medallion_block(tmp_path, monkeypatch):
    text = FLAT_V12 + (
        "architecture:\n  pipeline:\n    medallion:\n      bronze:\n"
        "        path_template: customer/interactions\n"
    )
    old, new, r = _convert(tmp_path, monkeypatch, text)
    assert r.exit_code == 0, r.output
    assert "medallion" not in new.read_text()
    assert "dropped: architecture.pipeline.medallion" in r.output


def test_init_from_empty_platform_block(tmp_path, monkeypatch):
    """An empty block is refused as OLD's own load refuses it, not a crash."""
    old, new, r = _convert(tmp_path, monkeypatch, "name: empty\nplatform:\n")
    assert r.exit_code == 2, r.output
    assert "does not load" in r.output and not new.exists()
    old, new, r = _convert(tmp_path, monkeypatch, "name: empty\nplatform:\n  kubernetes: {}\n")
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(new.read_text())["platform"]["storage"]["s3"]["buckets"] == {
        "bronze": "empty-bronze",
        "silver": "empty-silver",
        "gold": "empty-gold",
    }


def test_init_from_writes_the_recipe_a_recipe_less_config_resolves_to(tmp_path, monkeypatch):
    old, new, r = _convert(
        tmp_path, monkeypatch, (FIXTURES / "v16-init" / "default.yaml").read_text()
    )
    assert r.exit_code == 0, r.output
    assert yaml.safe_load(new.read_text())["recipe"] == "hive-iceberg-spark-trino"
    _set_refs(monkeypatch, new.read_text())
    cfg = load_config(new, purpose=LoadPurpose.READ, print_notes=False)
    assert not [t for t in load_notes(cfg).texts() if "recipe" in t]


def test_init_from_takes_back_a_recipe_that_changes_settings(tmp_path, monkeypatch):
    """A derived recipe whose defaults would change a setting is not written."""
    import lakebench.config.recipes as recipes

    patched = {k: dict(v) for k, v in recipes.RECIPES.items()}
    patched["hive-iceberg-spark-trino"]["images"] = {"trino": "example/trino:other"}
    monkeypatch.setattr(recipes, "RECIPES", patched)
    old, new, r = _convert(tmp_path, monkeypatch, "name: plain\n")
    assert r.exit_code == 0, r.output
    assert "recipe" not in yaml.safe_load(new.read_text())
    assert "not written" in r.output and "images.trino" in r.output


def test_init_from_output_directory_is_refused(tmp_path, monkeypatch):
    (tmp_path / "dir.yaml").mkdir()
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12)
    r = runner.invoke(app, ["init", "--from", str(old), "-o", "dir.yaml", "--overwrite"])
    assert r.exit_code == 2 and "is a directory" in r.output


def test_init_from_notes_a_credential_variable_already_set(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "something-else")
    old, new, r = _convert(tmp_path, monkeypatch, FLAT_V12)
    assert r.exit_code == 0, r.output
    assert "LAKEBENCH_S3_ACCESS_KEY is already set" in r.output
    assert "something-else" not in r.output


#: Every schema validator that refuses by load purpose, and the refused-key
#: rows it raises (by subject), or why it has none.
PURPOSE_VALIDATORS = {
    "_refuse_operator_install": [
        "platform.compute.spark.operator.install true",
        "architecture.catalog.hive.operator.install true",
    ],
    "_driver_memory_is_a_spark_size": ["platform.compute.spark.driver_memory not a Spark size"],
    "_override_within_bounds": [
        "platform.compute.spark.silver_executors above 28",
        "platform.compute.spark.driver_cores above 16",
    ],
    "_refuse_what_run_does_not_do": [
        "architecture.benchmark.mode throughput or composite",
        "architecture.benchmark.cache cold",
        "architecture.benchmark.streams above 1",
    ],
    "refuse_unrunnable_gold_strategy": [
        "spark.conf spark.lb.gold.strategy other than auto, simple_agg or two_phase_agg"
    ],
    "refuse_owned_spark_conf": ["spark.conf a key Lakebench owns"],
    "_drop_removed_keys": "removed keys: each model's _removed_keys, listed by the schema walk",
    "_fixed_file_size": "1.6 already refused a file size other than 64mb",
    "apply_recipe_defaults": "a recipe conflict: init --from rewrites it; its own breaking entry",
}


def test_every_purpose_refusal_has_a_refused_key_row():
    import ast

    tree = ast.parse((ROOT / "src" / "lakebench" / "config" / "schema.py").read_text())
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            names = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
            attrs = {n.attr for n in ast.walk(node) if isinstance(n, ast.Attribute)}
            if "CHANGES_DATA" in names or ("RUN" in attrs and "LoadPurpose" in names):
                found.add(node.name)
    assert found == set(PURPOSE_VALIDATORS), (
        "a schema validator refuses by purpose: add its values to "
        "config/refused_keys.py (and docs/upgrading/breaking-1.7.yaml), or an exemption here"
    )
    subjects = {row.subject for row in REFUSED_KEYS}
    for rows in PURPOSE_VALIDATORS.values():
        if isinstance(rows, list):
            assert set(rows) <= subjects, rows


@pytest.mark.parametrize("unset_side", ["old", "replaced"])
def test_init_from_overwrite_guard_refuses_an_unset_reference_name(
    tmp_path, monkeypatch, unset_side
):
    monkeypatch.delenv("LB_NAME", raising=False)
    ref = FLAT_V12.replace("name: flat12", "name: ${LB_NAME}")
    replaced = ref if unset_side == "replaced" else FLAT_V12.replace("flat12-ns", "other-ns")
    (tmp_path / "new.yaml").write_text(replaced)
    old_text = FLAT_V12 if unset_side == "replaced" else ref
    old, new, r = _convert(tmp_path, monkeypatch, old_text, "--overwrite")
    assert r.exit_code == 3, r.output
    assert "has not set" in " ".join(r.output.split())
    assert new.read_text() == replaced


def test_spark_secret_references_are_not_credentials():
    from lakebench.config.init_from import is_credential_key

    assert not is_credential_key("spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY")
    assert not is_credential_key("spark.kubernetes.executor.secrets.s3-secret")
    assert is_credential_key("spark.hadoop.fs.s3a.secret.key")
