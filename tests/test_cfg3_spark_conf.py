"""CFG-3 (CC-13): spark.conf merges over the job defaults; owned keys are refused.

Each job's conf is SPARK_CONF_DEFAULTS, then the user's spark.conf, then the
keys Lakebench owns. A user value for an owned key would be overwritten, so
the config refuses it. Before v1.7 the defaults lived in the schema default
of spark.conf, so setting any key dropped all of them.
"""

from __future__ import annotations

import itertools
import warnings
from unittest.mock import MagicMock, patch

import pytest
import yaml

from lakebench.config import LoadPurpose, load_config
from lakebench.config.loader import ConfigValidationError, load_notes
from lakebench.modules.pipeline_engines.spark import job as job_mod
from lakebench.modules.pipeline_engines.spark.conf_keys import (
    LAKEBENCH_OWNED_SPARK_KEYS,
    OWNED_AHEAD_OF_WRITER,
    SPARK_CONF_DEFAULTS,
    USER_OVERRIDABLE_SPARK_KEYS,
    V16_DEFAULT_SPARK_CONF,
)
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.conftest import make_config


def _conf(cfg, job_type: JobType) -> dict[str, str]:
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    manager = SparkJobManager(cfg, k8s)
    with patch.object(SparkJobManager, "_build_env_vars", return_value=[]):
        return manager._build_manifest(job_type)["spec"]["sparkConf"]


def _write(tmp_path, conf: dict, **extra) -> object:
    data = {
        "name": "cfg3",
        "platform": {
            "storage": {
                "s3": {"endpoint": "http://minio:9000", "access_key": "a", "secret_key": "b"}
            }
        },
        "spark": {"conf": conf},
        **extra,
    }
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_spark_conf_defaults_are_the_v16_runtime_values():
    # The values that ran under v1.6 for the keys the job does not own.
    assert SPARK_CONF_DEFAULTS == {
        "spark.hadoop.fs.s3a.multipart.size": "268435456",
        "spark.hadoop.fs.s3a.fast.upload.active.blocks": "16",
        "spark.hadoop.fs.s3a.attempts.maximum": "20",
        "spark.hadoop.fs.s3a.retry.limit": "10",
        "spark.hadoop.fs.s3a.retry.interval": "500ms",
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
    }


def test_conf_merges_over_defaults():
    user = {"spark.speculation": "true", "spark.memory.fraction": "0.6"}
    conf = _conf(make_config(spark={"conf": user}), JobType.SILVER_BUILD)
    # A user key is added and a default the user set is replaced ...
    assert conf["spark.speculation"] == "true"
    assert conf["spark.memory.fraction"] == "0.6"
    # ... and the other defaults stay (v1.6 dropped them all).
    for key, value in SPARK_CONF_DEFAULTS.items():
        if key != "spark.memory.fraction":
            assert conf[key] == value, key
    # Owned keys are Lakebench's whatever the user map holds.
    assert conf["spark.sql.shuffle.partitions"] == "64"
    assert conf["spark.hadoop.fs.s3a.connection.maximum"] == "200"


def test_default_config_conf_is_unchanged_by_the_merge():
    # With no user conf the manifest is what v1.6 built from its default
    # spark.conf: the same keys and values.
    conf = _conf(make_config(), JobType.SILVER_BUILD)
    for key, value in V16_DEFAULT_SPARK_CONF.items():
        if key not in ("spark.sql.shuffle.partitions", "spark.default.parallelism"):
            want = {
                "spark.hadoop.fs.s3a.connection.maximum": "200",
                "spark.hadoop.fs.s3a.threads.max": "100",
            }.get(key, value)
            assert conf[key] == want, key


@pytest.mark.parametrize(
    ("key", "needle"),
    [
        ("spark.jars", "resolved dependency set"),
        ("spark.jars.packages", "resolved dependency set"),
        ("spark.jars.repositories", "Lakebench writes it"),
        ("spark.jars.ivy", "Lakebench writes it"),
        ("spark.jars.ivySettings", "reserved for Lakebench"),
        ("spark.submit.pyFiles", "reserved for Lakebench"),
        ("spark.sql.shuffle.partitions", "<job>_executors"),
        ("spark.executor.memory", "job profiles"),
        ("spark.executor.memoryOverheadFactor", "capacity check"),
        ("spark.driver.memoryOverhead", "capacity check"),
        ("spark.memory.offHeap.size", "capacity check"),
        ("spark.executor.pyspark.memory", "capacity check"),
        ("spark.sql.session.timeZone", "a job script sets it"),
        ("spark.sql.autoBroadcastJoinThreshold", "a job script sets it"),
        ("spark.kubernetes.executor.podNamePrefix", "reserved for Lakebench"),
        ("spark.kubernetes.node.selector.zone", "reserved for Lakebench"),
        ("spark.hadoop.fs.s3a.endpoint", "platform.storage.s3.endpoint"),
        ("spark.sql.catalog.lakehouse.uri", "Lakebench writes it"),
    ],
)
@pytest.mark.parametrize("purpose", [LoadPurpose.RUN, LoadPurpose.MUTATE])
def test_owned_key_refused(tmp_path, key, needle, purpose):
    path = _write(tmp_path, {key: "x"})
    with pytest.raises(ConfigValidationError) as e:
        load_config(path, purpose=purpose, print_notes=False)
    assert f"{key} is owned by Lakebench" in str(e.value)
    assert needle in str(e.value)


def test_refusal_is_located_at_spark(tmp_path):
    path = _write(tmp_path, {"spark.sql.shuffle.partitions": "400"})
    with pytest.raises(ConfigValidationError) as e:
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    assert [tuple(err["loc"]) for err in e.value.errors] == [("spark",)]


def test_record_never_carries_a_secret_or_location():
    from lakebench.metrics.experiment import experiment_inputs

    cfg = make_config(
        spark={
            "conf": {
                "spark.hadoop.fs.s3a.secret.key": "SUPERSECRET",
                "spark.hadoop.fs.azure.account.key.acct.dfs.core.windows.net": "AZSECRET",
                "spark.hadoop.fs.s3a.bucket.b.endpoint": "http://10.9.9.9:80",
                "spark.speculation": "true",
            }
        }
    )
    arch = experiment_inputs(cfg, run_mode="batch")["architecture"]
    text = repr(arch)
    for leaked in ("SUPERSECRET", "AZSECRET", "10.9.9.9"):
        assert leaked not in text
    assert arch["spark_conf_user"]["spark.speculation"] == "true"
    assert arch["spark_conf_user"]["spark.hadoop.fs.s3a.secret.key"] == "<redacted>"


def test_local_run_records_no_spark_conf():
    # The local runner builds no Spark job manifest: the conf never runs.
    from lakebench.metrics.experiment import experiment_inputs

    cfg = make_config(spark={"conf": {"spark.speculation": "true"}})
    arch = experiment_inputs(cfg, run_mode="batch", system="local")["architecture"]
    assert "spark_conf_user" not in arch


def test_owned_catalog_key_follows_the_catalog_name(tmp_path):
    extra = {"architecture": {"query_engine": {"trino": {"catalog_name": "cat2"}}}}
    path = _write(tmp_path, {"spark.sql.catalog.cat2.warehouse": "s3a://x/"}, **extra)
    with pytest.raises(ConfigValidationError, match="spark.sql.catalog.cat2.warehouse is owned"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    # A key under another catalog name is not Lakebench's.
    ok = _write(tmp_path, {"spark.sql.catalog.lakehouse.cache-enabled": "false"}, **extra)
    load_config(ok, purpose=LoadPurpose.RUN, print_notes=False)


@pytest.mark.parametrize("purpose", [LoadPurpose.TEARDOWN, LoadPurpose.READ])
def test_owned_key_dropped_with_a_note_for_teardown(tmp_path, purpose):
    path = _write(tmp_path, {"spark.sql.shuffle.partitions": "400", "spark.speculation": "true"})
    cfg = load_config(path, purpose=purpose, print_notes=False)
    assert cfg.spark.conf == {"spark.speculation": "true"}
    assert any("spark.sql.shuffle.partitions" in t for t in load_notes(cfg).texts())


def test_v16_default_conf_is_inert(tmp_path):
    # A config carrying the v1.6 schema default of spark.conf (owned keys at
    # values v1.6 overwrote) changed nothing: it loads for run with notes.
    path = _write(tmp_path, dict(V16_DEFAULT_SPARK_CONF))
    cfg = load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    assert all(k in SPARK_CONF_DEFAULTS or k not in V16_DEFAULT_SPARK_CONF for k in cfg.spark.conf)
    notes = " ".join(load_notes(cfg).texts())
    assert "spark.sql.shuffle.partitions" in notes and "v1.6 default" in notes


@pytest.mark.parametrize(
    "job_type", [JobType.SILVER_BUILD, JobType.SILVER_STREAM], ids=["silver-build", "silver-stream"]
)
def test_user_conf_reaches_aml_silver(job_type):
    cfg = make_config(
        recipe="polaris-iceberg-spark-trino",
        architecture={"workload": {"schema": "financial", "datagen": {"seed": 43}}},
        spark={"conf": {"spark.sql.broadcastTimeout": "900"}},
    )
    assert _conf(cfg, job_type)["spark.sql.broadcastTimeout"] == "900"


def test_default_identity_unchanged():
    from lakebench.metrics.experiment import experiment_inputs

    def inputs(**kw):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            return experiment_inputs(make_config(**kw), run_mode="batch")

    base = inputs()
    assert "spark_conf_user" not in base["architecture"]
    # The old default map, and user keys at their default values, change
    # nothing that runs, so they record nothing.
    assert inputs(spark={"conf": dict(SPARK_CONF_DEFAULTS)}) == base
    custom = inputs(spark={"conf": {"spark.speculation": "true", "spark.memory.fraction": "0.8"}})
    assert custom["architecture"]["spark_conf_user"] == {"spark.speculation": "true"}
    assert {k: v for k, v in custom["architecture"].items() if k != "spark_conf_user"} == (
        base["architecture"]
    )


# -- drift: the owned set is exactly what the manifest writes ------------------

_RECIPES = ("hive-iceberg-spark-trino", "polaris-iceberg-spark-trino", "hive-delta-spark-trino")
_SPARK = (
    "apache/spark:3.5.8-java17-python3",
    "apache/spark:4.0.2-python3",
    "apache/spark:4.1.1-python3",
)


_SENTINEL = "lb-sentinel"


def _written_keys(catalog_name: str = "lakehouse") -> set[str]:
    """Every key any manifest writes. Each build also carries every default
    set to a sentinel in the user conf, which must survive: a later owned
    write of a default key would silently replace the user's value."""
    written: set[str] = set()
    for recipe, schema, image, obs, ca, scratch in itertools.product(
        _RECIPES, ("customer360", "financial"), _SPARK, (False, True), (False, True), (False, True)
    ):
        if "delta" in recipe and (schema == "financial" or image.startswith("apache/spark:3.5")):
            continue
        s3 = {"endpoint": "http://minio:9000", "access_key": "a", "secret_key": "b"}
        if ca:
            s3["ca_cert"] = "/etc/ssl/ca.pem"
        cfg = make_config(
            recipe=recipe,
            images={"spark": image},
            observability={"enabled": obs},
            platform={"storage": {"s3": s3, "scratch": {"enabled": scratch}}},
            architecture={
                "workload": {"schema": schema, "datagen": {"seed": 43}},
                "query_engine": {"trino": {"catalog_name": catalog_name}},
            },
            spark={"conf": dict.fromkeys(SPARK_CONF_DEFAULTS, _SENTINEL)},
        )
        for jt in JobType:
            conf = _conf(cfg, jt)
            overwritten = [k for k in SPARK_CONF_DEFAULTS if conf.get(k) != _SENTINEL]
            assert not overwritten, (recipe, schema, image, jt, overwritten)
            written.update(conf)
    return written


def test_owned_keys_match_what_the_manifest_writes():
    written = _written_keys() - set(SPARK_CONF_DEFAULTS) - USER_OVERRIDABLE_SPARK_KEYS
    expand = {k.replace("{catalog}", "lakehouse") for k in LAKEBENCH_OWNED_SPARK_KEYS}
    ahead = {k.replace("{catalog}", "lakehouse") for k in OWNED_AHEAD_OF_WRITER}
    prefixed = {k for k in written if k.startswith("spark.kubernetes.")}
    assert written - prefixed == expand - ahead
    # Every written key is refused from user conf (or is a default/overridable).
    from lakebench.modules.pipeline_engines.spark.conf_keys import is_owned_spark_key

    assert all(is_owned_spark_key(k) for k in written)
    # The catalog keys follow the catalog name.
    renamed = _written_keys("cat2") - set(SPARK_CONF_DEFAULTS) - USER_OVERRIDABLE_SPARK_KEYS
    assert all(is_owned_spark_key(k, "cat2") for k in renamed)
    assert not [k for k in renamed if k.startswith("spark.sql.catalog.lakehouse")]


def test_job_py_names_the_owned_set():
    # The design names the constants in job.py; it re-exports them.
    assert job_mod.LAKEBENCH_OWNED_SPARK_KEYS is LAKEBENCH_OWNED_SPARK_KEYS
    assert job_mod.SPARK_CONF_DEFAULTS is SPARK_CONF_DEFAULTS


def test_keys_the_job_scripts_set_are_owned():
    """A key a job script sets with spark.conf.set (always, or while a step
    runs) would not hold for the whole job, so it is owned too."""
    import re
    from pathlib import Path

    from lakebench.modules.pipeline_engines.spark.conf_keys import is_owned_spark_key

    scripts = Path(job_mod.__file__).resolve().parents[3] / "spark" / "scripts"
    found: set[str] = set()
    for f in scripts.glob("*.py"):
        text = f.read_text()
        found.update(re.findall(r'spark\.conf\.set\(\s*"([^"]+)"', text))
        if re.search(r"spark\.conf\.set\(\s*key\b", text):
            found.update(re.findall(r'\bkey\s*=\s*"(spark\.[^"]+)"', text))
    assert found
    assert not [k for k in sorted(found) if not is_owned_spark_key(k)]
